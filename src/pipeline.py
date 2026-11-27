"""CLI research pipeline: ingest -> state machine -> synthesis -> audit -> atomic save & KV publish."""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import re
import sys
from datetime import datetime, time, timezone
from pathlib import Path
from typing import Any

import httpx
from pydantic import BaseModel

from src.config import TZ_CN, TZ_ET, format_utc_iso, get_market_close, load_settings
from src.ingestion import IngestionBundle, run_ingestion
from src.models.schemas import LiveResponse, NewsResponse, StateResponse
from src.research import ResearchResult, load_previous_state, run_research_cycle

logger = logging.getLogger(__name__)

DEFAULT_CF_KV_NAMESPACE_ID = "abc99f1cc4ae4314a085daa5d0006076"
_KV_RETRYABLE_STATUSES = frozenset({408, 429, 500, 502, 503, 504})
_REBALANCE_HISTORY_TYPES = frozenset({"晋升主线", "降级淘汰", "主线复盘"})
_HISTORY_TS_RE = re.compile(r"^(\d{4})[-/](\d{2})[-/](\d{2})[T ](\d{2}):(\d{2})(?::(\d{2}))?")


def _kv_timeout(timeout_sec: float) -> httpx.Timeout:
    return httpx.Timeout(
        connect=min(5.0, timeout_sec),
        read=timeout_sec,
        write=timeout_sec,
        pool=min(5.0, timeout_sec),
    )


def _already_rebalanced_after_close(prev_state: StateResponse | None, now_utc: datetime) -> bool:
    """Return True if prev_state already logged a post-close rebalance on the same ET trading day."""
    if prev_state is None or not prev_state.history:
        return False
    et_now = now_utc.astimezone(TZ_ET)
    close_h = get_market_close(et_now.date()).hour
    rebalance_start = time(close_h, 30)
    for entry in prev_state.history:
        if entry.type not in _REBALANCE_HISTORY_TYPES:
            continue
        m = _HISTORY_TS_RE.match(entry.time)
        if not m:
            continue
        try:
            dt_cn = datetime(
                int(m.group(1)),
                int(m.group(2)),
                int(m.group(3)),
                int(m.group(4)),
                int(m.group(5)),
                int(m.group(6) or 0),
                tzinfo=TZ_CN,
            )
        except ValueError:
            continue
        dt_et = dt_cn.astimezone(TZ_ET)
        if dt_et.date() == et_now.date() and dt_et.time() >= rebalance_start:
            return True
    return False


async def _get_kv_state_with_retry(
    client: httpx.AsyncClient,
    url: str,
    headers: dict[str, str],
    *,
    retry_delays: tuple[float, ...] = (0.25, 0.6),
) -> httpx.Response:
    total_attempts = 1 + len(retry_delays)
    last_exc: Exception | None = None
    for attempt in range(total_attempts):
        try:
            resp = await client.get(url, headers=headers)
            if resp.status_code in _KV_RETRYABLE_STATUSES and attempt < total_attempts - 1:
                logger.warning(
                    "Cloudflare KV GET state returned HTTP %d (attempt %d/%d); retrying",
                    resp.status_code,
                    attempt + 1,
                    total_attempts,
                )
                await asyncio.sleep(retry_delays[attempt])
                continue
            return resp
        except (httpx.TimeoutException, httpx.TransportError) as exc:
            last_exc = exc
            if attempt < total_attempts - 1:
                logger.warning(
                    "Cloudflare KV GET state network error (attempt %d/%d): %s; retrying",
                    attempt + 1,
                    total_attempts,
                    type(exc).__name__,
                )
                await asyncio.sleep(retry_delays[attempt])
                continue
            raise
    assert last_exc is not None
    raise last_exc


async def _put_kv_value_with_retry(
    client: httpx.AsyncClient,
    url: str,
    key: str,
    body: bytes,
    headers: dict[str, str],
    *,
    retry_delays: tuple[float, ...] = (0.35, 0.9),
) -> bool:
    total_attempts = 1 + len(retry_delays)
    last_err: Exception | None = None
    for attempt in range(total_attempts):
        try:
            resp = await client.put(url, headers=headers, content=body)
            if resp.status_code in _KV_RETRYABLE_STATUSES and attempt < total_attempts - 1:
                logger.warning(
                    "Cloudflare KV PUT '%s' returned HTTP %d (attempt %d/%d); retrying",
                    key,
                    resp.status_code,
                    attempt + 1,
                    total_attempts,
                )
                await asyncio.sleep(retry_delays[attempt])
                continue
            if resp.status_code >= 400:
                raise RuntimeError(
                    f"Cloudflare KV PUT '{key}' failed ({resp.status_code}): {resp.text[:300]}"
                )
            parsed = resp.json()
            if not parsed.get("success", False):
                raise RuntimeError(f"Cloudflare KV PUT '{key}' rejected: {parsed}")
            return True
        except (httpx.TimeoutException, httpx.TransportError) as exc:
            last_err = exc
            if attempt < total_attempts - 1:
                logger.warning(
                    "Cloudflare KV PUT '%s' network error (attempt %d/%d): %s; retrying",
                    key,
                    attempt + 1,
                    total_attempts,
                    type(exc).__name__,
                )
                await asyncio.sleep(retry_delays[attempt])
                continue
            raise RuntimeError(f"Cloudflare KV PUT '{key}' transport failed: {exc}") from exc
    raise RuntimeError(f"Cloudflare KV PUT '{key}' failed after retries: {last_err}")


def atomic_write_json(path: Path, payload: BaseModel | dict[str, Any]) -> Path:
    """Atomically write JSON payload to disk using temp-file + os.replace."""
    path.parent.mkdir(parents=True, exist_ok=True)
    data = payload.model_dump(mode="json") if isinstance(payload, BaseModel) else payload
    serialized = json.dumps(data, ensure_ascii=False, indent=2, allow_nan=False)
    tmp_path = path.with_name(f"{path.name}.tmp")
    tmp_path.write_text(serialized, encoding="utf-8")
    os.replace(tmp_path, path)
    return path


async def publish_to_cloudflare_kv(
    *,
    state: StateResponse | None = None,
    live: LiveResponse | None = None,
    news: NewsResponse | None = None,
    account_id: str,
    api_token: str,
    namespace_id: str = DEFAULT_CF_KV_NAMESPACE_ID,
    timeout_sec: float = 20.0,
    retry_delays: tuple[float, ...] = (0.35, 0.9),
) -> dict[str, bool]:
    """Push validated state, live, and/or news payloads into Cloudflare Workers KV with retries."""
    if not account_id or not api_token:
        raise ValueError("CF_ACCOUNT_ID and CF_API_TOKEN are required for KV publishing")

    base = (
        f"https://api.cloudflare.com/client/v4/accounts/{account_id}"
        f"/storage/kv/namespaces/{namespace_id}/values"
    )
    headers = {
        "Authorization": f"Bearer {api_token}",
        "Content-Type": "application/json; charset=utf-8",
    }
    entries: dict[str, bytes] = {}
    if state is not None:
        entries["state"] = json.dumps(state.model_dump(mode="json"), ensure_ascii=False, allow_nan=False).encode("utf-8")
    if live is not None:
        entries["live"] = json.dumps(live.model_dump(mode="json"), ensure_ascii=False, allow_nan=False).encode("utf-8")
    if news is not None:
        entries["news"] = json.dumps(news.model_dump(mode="json"), ensure_ascii=False, allow_nan=False).encode("utf-8")
    if not entries:
        raise ValueError("At least one payload (state, live, or news) is required for KV publishing")

    async with httpx.AsyncClient(timeout=_kv_timeout(timeout_sec)) as client:
        keys = list(entries.keys())
        outcomes = await asyncio.gather(
            *(
                _put_kv_value_with_retry(
                    client,
                    f"{base}/{key}",
                    key,
                    entries[key],
                    headers,
                    retry_delays=retry_delays,
                )
                for key in keys
            )
        )
    return dict(zip(keys, outcomes))


def _write_snapshot(
    data_dir: Path,
    now_utc: datetime,
    bundle: IngestionBundle,
    research: ResearchResult,
) -> Path:
    """Persist an auditable point-in-time snapshot under data/snapshots/."""
    et_now = now_utc.astimezone(TZ_ET)
    snap_dir = data_dir / "snapshots"
    snap_name = f"{et_now.strftime('%Y-%m-%d_%H%M%S')}_et.json"
    audit_passed = sum(1 for c in research.audit.checks if c.passed)
    audit_total = len(research.audit.checks)
    snapshot_payload = {
        "generatedAtUtc": format_utc_iso(now_utc),
        "etTime": et_now.strftime("%Y-%m-%d %H:%M:%S %Z"),
        "engineMode": research.engine_mode,
        "modelUsed": research.model_used,
        "fallbackReason": research.fallback_reason,
        "governanceWindow": research.governance.window,
        "marketSession": research.governance.market_session,
        "allowStructuralRebalance": research.governance.allow_structural_rebalance,
        "ingestionSummary": bundle.summary_dict(),
        "auditPassed": audit_passed,
        "auditTotal": audit_total,
        "auditRepairs": research.audit.repairs,
        "auditSummary": research.audit.summary,
        "meta": research.state.meta.model_dump(mode="json"),
    }
    return atomic_write_json(snap_dir / snap_name, snapshot_payload)


async def run_pipeline(
    *,
    data_dir: Path | None = None,
    use_llm: bool = True,
    force_rebalance: bool = False,
    catchup_rebalance: bool = False,
    publish_kv: bool = False,
    include_screener: bool = True,
    now_utc: datetime | None = None,
) -> dict[str, Any]:
    """Execute the full M2 + M3 pipeline, persist artifacts, and optionally publish KV."""
    settings = load_settings()
    target_dir = data_dir or settings.data_dir
    target_dir.mkdir(parents=True, exist_ok=True)
    ts_now = now_utc or datetime.now(timezone.utc)
    if ts_now.tzinfo is None:
        ts_now = ts_now.replace(tzinfo=timezone.utc)

    kv_id = os.getenv("CF_KV_NAMESPACE_ID", "").strip() or DEFAULT_CF_KV_NAMESPACE_ID
    restored_state: StateResponse | None = None

    # CI runners are ephemeral: recover the durable accepted state before governance.
    if publish_kv:
        if not settings.cf_account_id or not settings.cf_api_token:
            raise ValueError("Cloudflare credentials required before ingestion")
        url = (f"https://api.cloudflare.com/client/v4/accounts/{settings.cf_account_id}"
               f"/storage/kv/namespaces/{kv_id}/values/state")
        async with httpx.AsyncClient(timeout=_kv_timeout(20.0)) as client:
            response = await _get_kv_state_with_retry(
                client, url, {"Authorization": f"Bearer {settings.cf_api_token}"}
            )
        if response.status_code == 404:
            if (target_dir / "state.json").exists():
                raise ValueError("Remote state missing while local state exists; refusing divergent publication")
        else:
            response.raise_for_status()
            restored_state = StateResponse.model_validate(response.json())
            atomic_write_json(target_dir / "state.json", restored_state)

    effective_force_rebalance = force_rebalance
    if catchup_rebalance and not force_rebalance:
        prior = restored_state if restored_state is not None else load_previous_state(target_dir / "state.json")
        if _already_rebalanced_after_close(prior, ts_now):
            logger.info("Skipping catchup rebalance: post-close rebalance already recorded today")
            effective_force_rebalance = False
        else:
            effective_force_rebalance = True

    bundle = await run_ingestion(now_utc=ts_now, include_screener=include_screener)
    live_path = atomic_write_json(target_dir / "live.json", bundle.live_response)
    news_path = atomic_write_json(target_dir / "news.json", bundle.news_response)

    try:
        research = await run_research_cycle(
            bundle,
            state_path=target_dir / "state.json",
            persist=True,
            force_rebalance=effective_force_rebalance,
            use_llm=use_llm,
        )
    except Exception:
        if publish_kv:
            # Preserve prior accepted research state while still keeping KV live quotes & news fresh.
            try:
                await publish_to_cloudflare_kv(
                    state=None,
                    live=bundle.live_response,
                    news=bundle.news_response,
                    account_id=settings.cf_account_id,
                    api_token=settings.cf_api_token,
                    namespace_id=kv_id,
                )
                logger.warning("Research cycle failed; published fresh live/news to KV while preserving prior state")
            except Exception as kv_exc:
                logger.warning("Fallback live/news KV publish also failed: %s", kv_exc)
        raise

    state_path = research.saved_path or (target_dir / "state.json")
    snap_path = _write_snapshot(target_dir, bundle.fetched_at_utc, bundle, research)

    kv_status: dict[str, bool] = {}
    if publish_kv:
        kv_status = await publish_to_cloudflare_kv(
            state=research.state,
            live=bundle.live_response,
            news=bundle.news_response,
            account_id=settings.cf_account_id,
            api_token=settings.cf_api_token,
            namespace_id=kv_id,
        )

    audit_passed = sum(1 for c in research.audit.checks if c.passed)
    audit_total = len(research.audit.checks)
    return {
        "engine_mode": research.engine_mode,
        "model_used": research.model_used,
        "fallback_reason": research.fallback_reason,
        "governance_window": research.governance.window,
        "market_session": research.governance.market_session,
        "quotes_count": bundle.live_response.count,
        "news_count": bundle.news_response.count,
        "audit_passed": audit_passed,
        "audit_total": audit_total,
        "state_path": str(state_path),
        "live_path": str(live_path),
        "news_path": str(news_path),
        "snapshot_path": str(snap_path),
        "kv_published": kv_status,
    }


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Stock Trading Desk v2.0 — Research Pipeline CLI",
    )
    parser.add_argument(
        "--no-llm",
        action="store_true",
        help="Disable Gemini synthesis and run deterministic quantitative fallback only",
    )
    parser.add_argument(
        "--force-rebalance",
        action="store_true",
        help="Bypass intraday/off-hours anti-jitter lock and evaluate structural rebalance",
    )
    parser.add_argument(
        "--catchup-rebalance",
        action="store_true",
        help="Run delayed post-close rebalance once if no post-close rebalance was recorded today",
    )
    parser.add_argument(
        "--publish-kv",
        action="store_true",
        help="Push generated state, live, and news payloads to Cloudflare KV (DESK_STATE)",
    )
    parser.add_argument(
        "--no-screener",
        action="store_true",
        help="Skip Yahoo predefined screener queries for faster runs",
    )
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=None,
        help="Override output data directory (defaults to DESK_DATA_DIR or ./data)",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    args = _parse_args(argv)
    settings = load_settings()
    logging.basicConfig(
        level=getattr(logging, settings.log_level, logging.INFO),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    summary = asyncio.run(
        run_pipeline(
            data_dir=args.data_dir,
            use_llm=not args.no_llm,
            force_rebalance=args.force_rebalance,
            catchup_rebalance=args.catchup_rebalance,
            publish_kv=args.publish_kv,
            include_screener=not args.no_screener,
        )
    )
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
