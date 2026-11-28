"""FastAPI always-on HTTP server serving the SPA, hot-cached /api/* endpoints, and VPS scheduler."""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import logging
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, TypeVar

import uvicorn
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse, Response

from src.config import (
    APP_VERSION,
    PROJECT_ROOT,
    format_utc_iso,
    get_market_session,
    load_settings,
)
from src.ingestion.fast_live import fetch_fast_live
from src.ingestion.market_data import fetch_market_snapshot
from src.ingestion.news_aggregator import fetch_aggregated_news
from src.models.schemas import HealthResponse, LiveResponse, NewsResponse, StateResponse
from src.pipeline import atomic_write_json, run_pipeline
from src.scheduler import DeskScheduler, SchedulerConfig, parse_cn_timestamp_utc

logger = logging.getLogger(__name__)


class ServerHealthResponse(HealthResponse):
    """Extended /api/health payload preserving full compatibility with HealthResponse."""

    stateAgeMinutes: int | None = None
    liveUpdatedAt: str | None = None
    liveAgeSeconds: int | None = None
    rotSampled: str | None = None
    rotThemes: int = 0
    runtimeMode: str = "local"
    schedulerEnabled: bool = False
    pipelineRunning: bool = False
    lastPipelineAt: str | None = None
    lastPipelineStatus: str | None = None
    lastPipelineReason: str | None = None
    lastPipelineError: str | None = None

FRONTEND_PATH = PROJECT_ROOT / "frontend" / "index.html"
CACHE_TTL_SEC = 25.0
LIVE_CACHE_TTL_ACTIVE_SEC = 25.0
LIVE_CACHE_TTL_IDLE_SEC = 90.0
NEWS_CACHE_TTL_SEC = 60.0

TModel = TypeVar("TModel")


@dataclass
class _HotEntry:
    data_dir: Path
    model: Any
    body: bytes
    etag: str
    file_sig: tuple[int, int] | None
    updated_mono: float


@dataclass
class _HtmlCache:
    file_sig: tuple[int, int]
    content: str
    etag: str


_HOT_CACHE: dict[str, _HotEntry] = {}
_HTML_CACHE: _HtmlCache | None = None
_live_lock = asyncio.Lock()
_news_lock = asyncio.Lock()
_scheduler: DeskScheduler | None = None


def reset_server_caches() -> None:
    """Clear in-memory caches (primarily for deterministic unit tests)."""
    global _HTML_CACHE
    _HOT_CACHE.clear()
    _HTML_CACHE = None


def _make_weak_etag(prefix: str, payload_bytes: bytes) -> str:
    digest = hashlib.blake2b(payload_bytes, digest_size=8).hexdigest()
    return f'W/"{prefix}-{len(payload_bytes):x}-{digest}"'


def _normalize_etag(tag: str) -> str:
    s = (tag or "").strip()
    if s.startswith(("W/", "w/")):
        return s[2:].strip()
    return s


def _etag_matches(if_none_match: str | None, etag: str) -> bool:
    if not if_none_match or not etag:
        return False
    raw = if_none_match.strip()
    if raw == "*":
        return True
    target = _normalize_etag(etag)
    return any(_normalize_etag(part) == target for part in raw.split(","))


def _file_sig(path: Path) -> tuple[int, int] | None:
    try:
        st = path.stat()
    except OSError:
        return None
    return (st.st_mtime_ns, st.st_size)


def _data_dir() -> Path:
    return load_settings().data_dir


def _is_force(val: str | None) -> bool:
    if not val:
        return False
    return val.strip().lower() in {"1", "true", "yes", "on"}


def _file_age_seconds(path: Path) -> float:
    try:
        return max(0.0, time.time() - path.stat().st_mtime)
    except OSError:
        return float("inf")


def _file_mtime_utc_iso(path: Path) -> str | None:
    try:
        mtime = path.stat().st_mtime
    except OSError:
        return None
    dt = datetime.fromtimestamp(mtime, tz=timezone.utc)
    return format_utc_iso(dt)


def _serialize_model(model: Any) -> bytes:
    payload = model.model_dump(mode="json")
    return json.dumps(payload, ensure_ascii=False, allow_nan=False).encode("utf-8")


def _store_hot(
    key: str,
    data_dir: Path,
    model: Any,
    *,
    file_sig: tuple[int, int] | None,
) -> _HotEntry:
    body = _serialize_model(model)
    entry = _HotEntry(
        data_dir=data_dir,
        model=model,
        body=body,
        etag=_make_weak_etag(key, body),
        file_sig=file_sig,
        updated_mono=time.monotonic(),
    )
    _HOT_CACHE[key] = entry
    return entry


def _read_json_model(path: Path, model_cls: type[TModel]) -> TModel | None:
    if not path.is_file():
        return None
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
        return model_cls.model_validate(raw)
    except Exception as exc:
        logger.warning("Failed to validate cached %s: %s", path.name, exc)
        return None


def _get_hot_or_disk(
    key: str,
    path: Path,
    model_cls: type[TModel],
    *,
    allow_mem_newer_than_disk: bool = False,
) -> tuple[_HotEntry | None, str]:
    """Load validated model from hot RAM cache or disk, auto-invalidating on file changes."""
    data_dir = path.parent
    disk_sig = _file_sig(path)
    existing = _HOT_CACHE.get(key)

    if existing is not None and existing.data_dir == data_dir:
        if disk_sig == existing.file_sig:
            if disk_sig is not None or allow_mem_newer_than_disk:
                return existing, "MEM-HIT"
        elif allow_mem_newer_than_disk and disk_sig is None and existing.file_sig is None:
            return existing, "MEM-HIT"

    if disk_sig is None:
        if existing is not None and existing.data_dir == data_dir and not allow_mem_newer_than_disk:
            _HOT_CACHE.pop(key, None)
        return None, "MISS"

    model = _read_json_model(path, model_cls)
    if model is None:
        if existing is not None and existing.data_dir == data_dir:
            _HOT_CACHE.pop(key, None)
        return None, "MISS"

    entry = _store_hot(key, data_dir, model, file_sig=disk_sig)
    return entry, "DISK-HIT"


def _json_response(
    model: Any,
    *,
    request: Request | None = None,
    etag: str | None = None,
    etag_prefix: str = "json",
    pre_serialized: bytes | None = None,
    cache_control: str = "no-store",
    cache_status: str | None = None,
    status_code: int = 200,
) -> Response:
    body = pre_serialized if pre_serialized is not None else _serialize_model(model)
    effective_etag = etag
    if effective_etag is None and status_code == 200 and cache_control != "no-store":
        effective_etag = _make_weak_etag(etag_prefix, body)

    headers: dict[str, str] = {"Cache-Control": cache_control}
    if effective_etag:
        headers["ETag"] = effective_etag
    if cache_status:
        headers["X-Desk-Cache"] = cache_status

    if (
        status_code == 200
        and effective_etag
        and cache_control != "no-store"
        and request is not None
        and _etag_matches(request.headers.get("if-none-match"), effective_etag)
    ):
        headers["X-Desk-Cache"] = f"{cache_status}-304" if cache_status else "304"
        return Response(status_code=304, headers=headers)

    is_head = request is not None and request.method.upper() == "HEAD"
    return Response(
        content=b"" if is_head else body,
        status_code=status_code,
        media_type="application/json; charset=utf-8",
        headers=headers,
    )


def _live_cache_ttl_sec() -> float:
    return (
        LIVE_CACHE_TTL_ACTIVE_SEC
        if get_market_session() in {"PRE_MARKET", "INTRADAY", "AFTER_HOURS"}
        else LIVE_CACHE_TTL_IDLE_SEC
    )


def _live_entry_fresh(entry: _HotEntry, path: Path, ttl_sec: float) -> bool:
    # Check both in-memory refresh age and serialized payload timestamp age.
    mem_age = time.monotonic() - entry.updated_mono
    if entry.file_sig is None:
        return mem_age <= ttl_sec
    disk_age = _file_age_seconds(path)
    ts_utc = parse_cn_timestamp_utc(getattr(entry.model, "updatedAt", None))
    if ts_utc is not None:
        ts_age = (datetime.now(timezone.utc) - ts_utc).total_seconds()
        if 0 <= ts_age <= ttl_sec:
            return True
    return min(mem_age, disk_age) <= ttl_sec


def _news_entry_fresh(entry: _HotEntry, path: Path, ttl_sec: float) -> bool:
    mem_age = time.monotonic() - entry.updated_mono
    if entry.file_sig is None:
        return mem_age <= ttl_sec
    return min(mem_age, _file_age_seconds(path)) <= ttl_sec


async def _fetch_live_upstream(cached: LiveResponse | None) -> LiveResponse:
    """Fast 2-batch CNBC + gap Yahoo live fetch with full snapshot fallback."""
    try:
        return await fetch_fast_live(cached=cached)
    except Exception as fast_exc:
        logger.warning("Fast live fetch degraded (%s); falling back to snapshot fetcher", fast_exc)
        snap = await fetch_market_snapshot(include_screener=False)
        return snap.live


async def _get_or_refresh_live(
    *,
    force: bool = False,
    persist_disk: bool = True,
) -> tuple[_HotEntry, str]:
    data_dir = _data_dir()
    live_path = data_dir / "live.json"
    ttl_sec = _live_cache_ttl_sec()

    entry, hit_layer = _get_hot_or_disk(
        "live",
        live_path,
        LiveResponse,
        allow_mem_newer_than_disk=True,
    )
    if not force and entry is not None and _live_entry_fresh(entry, live_path, ttl_sec):
        return entry, hit_layer

    async with _live_lock:
        entry, hit_layer = _get_hot_or_disk(
            "live",
            live_path,
            LiveResponse,
            allow_mem_newer_than_disk=True,
        )
        if not force and entry is not None and _live_entry_fresh(entry, live_path, ttl_sec):
            return entry, hit_layer
        cached_model: LiveResponse | None = entry.model if entry is not None else None
        try:
            fresh_live = await _fetch_live_upstream(cached_model)
            sig = entry.file_sig if entry is not None else _file_sig(live_path)
            if persist_disk:
                atomic_write_json(live_path, fresh_live)
                sig = _file_sig(live_path)
            updated = _store_hot("live", data_dir, fresh_live, file_sig=sig)
            return updated, ("FORCE-FRESH" if force else "ORIGIN-FRESH")
        except Exception as exc:
            logger.warning("Live market refresh failed: %s", exc)
            if entry is not None:
                return entry, "MEM-FALLBACK"
            raise HTTPException(
                status_code=503,
                detail={"ok": False, "error": f"Live market data unavailable: {exc}"},
            ) from exc


async def _get_or_refresh_news(*, force: bool = False) -> tuple[_HotEntry, str]:
    data_dir = _data_dir()
    news_path = data_dir / "news.json"

    entry, hit_layer = _get_hot_or_disk("news", news_path, NewsResponse)
    if not force and entry is not None and _news_entry_fresh(entry, news_path, NEWS_CACHE_TTL_SEC):
        return entry, hit_layer

    async with _news_lock:
        entry, hit_layer = _get_hot_or_disk("news", news_path, NewsResponse)
        if not force and entry is not None and _news_entry_fresh(entry, news_path, NEWS_CACHE_TTL_SEC):
            return entry, hit_layer
        try:
            agg = await fetch_aggregated_news()
            if agg.response.count > 0 or entry is None:
                atomic_write_json(news_path, agg.response)
                sig = _file_sig(news_path)
                updated = _store_hot("news", data_dir, agg.response, file_sig=sig)
                return updated, ("FORCE-FRESH" if force else "ORIGIN-FRESH")
            return entry, "MEM-FALLBACK"
        except Exception as exc:
            logger.warning("News refresh failed: %s", exc)
            if entry is not None:
                return entry, "MEM-FALLBACK"
            raise HTTPException(
                status_code=502,
                detail={"ok": False, "error": f"News upstream unavailable: {exc}"},
            ) from exc


async def _scheduler_refresh_live(persist_disk: bool) -> None:
    await _get_or_refresh_live(force=True, persist_disk=persist_disk)


async def _scheduler_refresh_news() -> None:
    await _get_or_refresh_news(force=True)


def _scheduler_load_state() -> StateResponse | None:
    state_path = _data_dir() / "state.json"
    entry, _ = _get_hot_or_disk("state", state_path, StateResponse)
    return entry.model if entry is not None else None


async def _scheduler_run_research(
    reason: str,
    force_rebalance: bool,
    catchup_rebalance: bool,
) -> dict[str, object]:
    cfg = SchedulerConfig.from_env()
    data_dir = _data_dir()
    summary = await run_pipeline(
        data_dir=data_dir,
        use_llm=cfg.use_llm,
        force_rebalance=force_rebalance,
        catchup_rebalance=catchup_rebalance,
        publish_kv=cfg.publish_kv,
        include_screener=True,
    )
    # Immediately hydrate hot RAM caches from freshly persisted files.
    _get_hot_or_disk("state", data_dir / "state.json", StateResponse)
    _get_hot_or_disk("live", data_dir / "live.json", LiveResponse)
    _get_hot_or_disk("news", data_dir / "news.json", NewsResponse)
    return summary


def _ensure_scheduler() -> DeskScheduler:
    global _scheduler
    if _scheduler is None:
        _scheduler = DeskScheduler(
            config=SchedulerConfig.from_env(),
            refresh_live_cb=_scheduler_refresh_live,
            refresh_news_cb=_scheduler_refresh_news,
            load_state_cb=_scheduler_load_state,
            run_research_cb=_scheduler_run_research,
        )
    return _scheduler


@asynccontextmanager
async def _lifespan(_app: FastAPI):
    global _scheduler
    cfg = SchedulerConfig.from_env()
    data_dir = _data_dir()
    # Pre-warm RAM caches from existing disk artifacts on startup.
    _get_hot_or_disk("state", data_dir / "state.json", StateResponse)
    _get_hot_or_disk("live", data_dir / "live.json", LiveResponse)
    _get_hot_or_disk("news", data_dir / "news.json", NewsResponse)

    _scheduler = DeskScheduler(
        config=cfg,
        refresh_live_cb=_scheduler_refresh_live,
        refresh_news_cb=_scheduler_refresh_news,
        load_state_cb=_scheduler_load_state,
        run_research_cb=_scheduler_run_research,
    )
    if cfg.enabled:
        _scheduler.start()
    try:
        yield
    finally:
        if _scheduler is not None:
            await _scheduler.stop()


app = FastAPI(
    title="美股交易链条观察 · Stock Trading Desk",
    version=APP_VERSION,
    lifespan=_lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "HEAD", "OPTIONS", "POST"],
    allow_headers=["*"],
    expose_headers=["ETag", "X-Desk-Cache"],
)


@app.middleware("http")
async def origin_secret_and_security_headers(request: Request, call_next: Any) -> Response:
    settings = load_settings()
    if settings.origin_secret and request.method.upper() != "OPTIONS":
        client_host = request.client.host if request.client else ""
        is_local = client_host in {"127.0.0.1", "::1", "localhost", "testclient"}
        if not is_local:
            provided = request.headers.get("x-desk-origin-secret", "")
            if not provided or not hmac.compare_digest(provided, settings.origin_secret):
                return JSONResponse(
                    status_code=403,
                    content={"ok": False, "detail": "Forbidden origin"},
                    headers={"Cache-Control": "no-store"},
                )

    resp = await call_next(request)
    resp.headers.setdefault("X-Content-Type-Options", "nosniff")
    resp.headers.setdefault("Referrer-Policy", "strict-origin-when-cross-origin")
    if request.url.path in ("/", "/index.html"):
        resp.headers.setdefault("X-Frame-Options", "SAMEORIGIN")
    return resp


@app.exception_handler(Exception)
async def unhandled_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    logger.exception("Unhandled error on %s %s", request.method, request.url.path)
    return JSONResponse(
        status_code=500,
        content={"ok": False, "detail": "Internal server error"},
        headers={"Cache-Control": "no-store"},
    )


def _load_html_cached() -> _HtmlCache:
    global _HTML_CACHE
    sig = _file_sig(FRONTEND_PATH)
    if sig is None:
        raise HTTPException(status_code=503, detail="frontend/index.html not built yet")
    if _HTML_CACHE is not None and _HTML_CACHE.file_sig == sig:
        return _HTML_CACHE
    content = FRONTEND_PATH.read_text(encoding="utf-8")
    etag = _make_weak_etag("html", content.encode("utf-8"))
    _HTML_CACHE = _HtmlCache(file_sig=sig, content=content, etag=etag)
    return _HTML_CACHE


@app.api_route("/", methods=["GET", "HEAD"], response_class=HTMLResponse)
@app.api_route("/index.html", methods=["GET", "HEAD"], response_class=HTMLResponse)
async def serve_index(request: Request) -> Response:
    html_entry = _load_html_cached()
    headers = {
        "Cache-Control": "public, max-age=0, must-revalidate",
        "ETag": html_entry.etag,
        "X-Desk-Cache": "MEM-HIT",
    }
    if _etag_matches(request.headers.get("if-none-match"), html_entry.etag):
        headers["X-Desk-Cache"] = "MEM-304"
        return Response(status_code=304, headers=headers)
    if request.method.upper() == "HEAD":
        return Response(status_code=200, media_type="text/html; charset=utf-8", headers=headers)
    return HTMLResponse(
        content=html_entry.content,
        media_type="text/html; charset=utf-8",
        headers=headers,
    )


@app.api_route("/favicon.ico", methods=["GET", "HEAD"])
async def favicon() -> Response:
    return Response(status_code=204)


@app.api_route("/api/health", methods=["GET", "HEAD"])
async def api_health(
    request: Request,
    force: str | None = Query(default=None),
) -> Response:
    data_dir = _data_dir()
    state_path = data_dir / "state.json"
    live_path = data_dir / "live.json"
    news_path = data_dir / "news.json"

    if _is_force(force):
        try:
            await asyncio.gather(
                _get_or_refresh_live(force=True),
                _get_or_refresh_news(force=True),
            )
        except HTTPException:
            pass

    state_entry, _ = _get_hot_or_disk("state", state_path, StateResponse)
    live_entry, _ = _get_hot_or_disk(
        "live",
        live_path,
        LiveResponse,
        allow_mem_newer_than_disk=True,
    )
    news_entry, _ = _get_hot_or_disk("news", news_path, NewsResponse)

    now_utc = datetime.now(timezone.utc)
    state_obj: StateResponse | None = state_entry.model if state_entry else None
    live_obj: LiveResponse | None = live_entry.model if live_entry else None
    news_obj: NewsResponse | None = news_entry.model if news_entry else None

    state_updated: str | None = None
    state_age_min: int | None = None
    if state_obj is not None:
        synced_dt = parse_cn_timestamp_utc(state_obj.serverSyncedAt)
        if synced_dt is not None:
            state_updated = format_utc_iso(synced_dt)
            state_age_min = max(0, int(round((now_utc - synced_dt).total_seconds() / 60.0)))
    if state_updated is None:
        state_updated = _file_mtime_utc_iso(state_path)

    live_updated_at: str | None = live_obj.updatedAt if live_obj is not None else None
    live_age_sec: int | None = None
    if live_obj is not None:
        live_dt = parse_cn_timestamp_utc(live_obj.updatedAt)
        if live_dt is not None:
            live_age_sec = max(0, int(round((now_utc - live_dt).total_seconds())))

    rot = live_obj.session_rotation if live_obj is not None else None
    sched = _scheduler
    cfg_enabled = load_settings().enable_scheduler
    tele = sched.snapshot_telemetry() if sched is not None else {
        "schedulerEnabled": cfg_enabled,
        "pipelineRunning": False,
        "lastPipelineAt": None,
        "lastPipelineStatus": None,
        "lastPipelineReason": None,
        "lastPipelineError": None,
    }

    health = ServerHealthResponse(
        ok=True,
        version=APP_VERSION,
        serverTimeUtc=format_utc_iso(now_utc),
        stateUpdatedAt=state_updated,
        stateAgeMinutes=state_age_min,
        liveUpdatedAt=live_updated_at,
        liveAgeSeconds=live_age_sec,
        rotSampled=rot.sampled_utc if rot is not None else None,
        rotThemes=len(rot.themes) if rot is not None else 0,
        newsCount=news_obj.count if news_obj is not None else 0,
        quotesCount=live_obj.count if live_obj is not None else 0,
        runtimeMode="vps-daemon" if cfg_enabled else "local",
        schedulerEnabled=bool(tele["schedulerEnabled"]),
        pipelineRunning=bool(tele["pipelineRunning"]),
        lastPipelineAt=tele["lastPipelineAt"],  # type: ignore[arg-type]
        lastPipelineStatus=tele["lastPipelineStatus"],  # type: ignore[arg-type]
        lastPipelineReason=tele["lastPipelineReason"],  # type: ignore[arg-type]
        lastPipelineError=tele["lastPipelineError"],  # type: ignore[arg-type]
    )
    return _json_response(health, request=request, cache_control="no-store")


@app.api_route("/api/live", methods=["GET", "HEAD"])
async def api_live(
    request: Request,
    force: str | None = Query(default=None),
    sym: str | None = Query(default=None),
) -> Response:
    is_forced = _is_force(force)
    entry, cache_status = await _get_or_refresh_live(force=is_forced)
    live: LiveResponse = entry.model
    cache_control = "no-store" if is_forced else "no-cache, must-revalidate"

    if sym:
        sym_clean = sym.strip().upper()
        filtered = [q for q in live.quotes if q.sym.upper() == sym_clean]
        sliced = LiveResponse(
            session_rotation=live.session_rotation,
            updatedAt=live.updatedAt,
            updatedAtEt=live.updatedAtEt,
            marketSession=live.marketSession,
            count=len(filtered),
            quotes=filtered,
        )
        return _json_response(
            sliced,
            request=request,
            etag_prefix="live",
            cache_control=cache_control,
            cache_status=cache_status,
        )

    return _json_response(
        live,
        request=request,
        etag=None if is_forced else entry.etag,
        pre_serialized=entry.body,
        cache_control=cache_control,
        cache_status=cache_status,
    )


@app.api_route("/api/state", methods=["GET", "HEAD"])
async def api_state(
    request: Request,
    force: str | None = Query(default=None),
) -> Response:
    is_forced = _is_force(force)
    state_path = _data_dir() / "state.json"
    if is_forced:
        _HOT_CACHE.pop("state", None)
    entry, cache_status = _get_hot_or_disk("state", state_path, StateResponse)
    if entry is None:
        raise HTTPException(
            status_code=503,
            detail={"ok": False, "error": "Research state not initialized yet; run pipeline first"},
        )
    return _json_response(
        entry.model,
        request=request,
        etag=None if is_forced else entry.etag,
        pre_serialized=entry.body,
        cache_control="no-store" if is_forced else "no-cache, must-revalidate",
        cache_status="FORCE-DISK" if is_forced else cache_status,
    )


@app.api_route("/api/news", methods=["GET", "HEAD"])
async def api_news(
    request: Request,
    force: str | None = Query(default=None),
    limit: str | None = Query(default=None),
) -> Response:
    is_forced = _is_force(force)
    entry, cache_status = await _get_or_refresh_news(force=is_forced)
    news: NewsResponse = entry.model
    cache_control = "no-store" if is_forced else "no-cache, must-revalidate"

    if limit is not None:
        try:
            parsed = int(limit.strip())
        except ValueError:
            parsed = None
        if parsed is not None:
            clamped = max(1, min(parsed, 50))
            sliced = news.items[:clamped]
            trimmed = NewsResponse(
                updatedAt=news.updatedAt,
                count=len(sliced),
                items=sliced,
            )
            return _json_response(
                trimmed,
                request=request,
                etag_prefix="news",
                cache_control=cache_control,
                cache_status=cache_status,
            )

    return _json_response(
        news,
        request=request,
        etag=None if is_forced else entry.etag,
        pre_serialized=entry.body,
        cache_control=cache_control,
        cache_status=cache_status,
    )


@app.post("/api/admin/run-pipeline")
async def api_admin_run_pipeline(
    request: Request,
    force_rebalance: str | None = Query(default=None),
) -> JSONResponse:
    """Protected endpoint to trigger an immediate background research cycle on the VPS."""
    expected = load_settings().admin_token
    if not expected:
        return JSONResponse(
            status_code=403,
            content={"ok": False, "detail": "Admin trigger disabled (DESK_ADMIN_TOKEN not set)"},
            headers={"Cache-Control": "no-store"},
        )
    auth = request.headers.get("authorization", "")
    bearer = auth[7:].strip() if auth.lower().startswith("bearer ") else ""
    provided = request.headers.get("x-admin-token", "").strip() or bearer
    if not provided or not hmac.compare_digest(provided, expected):
        return JSONResponse(
            status_code=403,
            content={"ok": False, "detail": "Invalid admin token"},
            headers={"Cache-Control": "no-store"},
        )

    sched = _ensure_scheduler()
    started = sched.trigger_research_background(
        reason="manual_api",
        force_rebalance=_is_force(force_rebalance),
    )
    return JSONResponse(
        status_code=202 if started else 200,
        content={
            "ok": True,
            "started": started,
            "reason": "manual_api" if started else "already_running",
            "forceRebalance": _is_force(force_rebalance),
        },
        headers={"Cache-Control": "no-store"},
    )


def main() -> None:
    settings = load_settings()
    logging.basicConfig(
        level=getattr(logging, settings.log_level, logging.INFO),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    uvicorn.run(
        "src.server:app",
        host=settings.host,
        port=settings.port,
        log_level=settings.log_level.lower(),
    )


if __name__ == "__main__":
    main()
