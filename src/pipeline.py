"""CLI research pipeline: ingest -> state machine -> synthesis -> audit -> atomic save & KV publish."""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import httpx
from pydantic import BaseModel

from src.config import TZ_ET, format_utc_iso, load_settings
from src.ingestion import IngestionBundle, run_ingestion
from src.models.schemas import LiveResponse, NewsResponse, StateResponse
from src.research import ResearchResult, run_research_cycle

logger = logging.getLogger(__name__)

DEFAULT_CF_KV_NAMESPACE_ID = "abc99f1cc4ae4314a085daa5d0006076"


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
    state: StateResponse,
    live: LiveResponse,
    news: NewsResponse,
    account_id: str,
    api_token: str,
    namespace_id: str = DEFAULT_CF_KV_NAMESPACE_ID,
    timeout_sec: float = 20.0,
) -> dict[str, bool]:
    """Push validated state, live, and news payloads into Cloudflare Workers KV."""
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
    entries = {
        "state": json.dumps(state.model_dump(mode="json"), ensure_ascii=False, allow_nan=False),
        "live": json.dumps(live.model_dump(mode="json"), ensure_ascii=False, allow_nan=False),
        "news": json.dumps(news.model_dump(mode="json"), ensure_ascii=False, allow_nan=False),
    }

    results: dict[str, bool] = {}
    async with httpx.AsyncClient(timeout=timeout_sec) as client:
        for key, body in entries.items():
            resp = await client.put(f"{base}/{key}", headers=headers, content=body.encode("utf-8"))
            if resp.status_code >= 400:
                raise RuntimeError(
                    f"Cloudflare KV PUT '{key}' failed ({resp.status_code}): {resp.text[:300]}"
                )
            parsed = resp.json()
            if not parsed.get("success", False):
                raise RuntimeError(f"Cloudflare KV PUT '{key}' rejected: {parsed}")
            results[key] = True
    return results


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

    bundle = await run_ingestion(now_utc=ts_now, include_screener=include_screener)
    research = await run_research_cycle(
        bundle,
        state_path=target_dir / "state.json",
        persist=True,
        force_rebalance=force_rebalance,
        use_llm=use_llm,
    )

    live_path = atomic_write_json(target_dir / "live.json", bundle.live_response)
    news_path = atomic_write_json(target_dir / "news.json", bundle.news_response)
    state_path = research.saved_path or (target_dir / "state.json")
    snap_path = _write_snapshot(target_dir, bundle.fetched_at_utc, bundle, research)

    kv_status: dict[str, bool] = {}
    if publish_kv:
        kv_id = os.getenv("CF_KV_NAMESPACE_ID", "").strip() or DEFAULT_CF_KV_NAMESPACE_ID
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
            publish_kv=args.publish_kv,
            include_screener=not args.no_screener,
        )
    )
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
