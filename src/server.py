"""FastAPI local HTTP server serving the 5-tab SPA and /api/* endpoints."""

from __future__ import annotations

import asyncio
import json
import logging
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import uvicorn
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse, Response

from src.config import APP_VERSION, PROJECT_ROOT, format_cn_timestamp, format_utc_iso, load_settings
from src.ingestion.market_data import fetch_market_snapshot
from src.ingestion.news_aggregator import fetch_aggregated_news
from src.models.schemas import HealthResponse, LiveResponse, NewsResponse, StateResponse
from src.pipeline import atomic_write_json

logger = logging.getLogger(__name__)

FRONTEND_PATH = PROJECT_ROOT / "frontend" / "index.html"
CACHE_TTL_SEC = 180.0

app = FastAPI(
    title="美股交易链条观察 · Stock Trading Desk",
    version=APP_VERSION,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "HEAD", "OPTIONS"],
    allow_headers=["*"],
)


@app.middleware("http")
async def add_security_headers(request: Request, call_next: Any) -> Response:
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


_live_lock = asyncio.Lock()
_news_lock = asyncio.Lock()


def _data_dir() -> Path:
    return load_settings().data_dir


def _is_force(val: str | None) -> bool:
    if not val:
        return False
    return val.strip().lower() in {"1", "true", "yes", "on"}


def _file_age_seconds(path: Path) -> float:
    if not path.is_file():
        return float("inf")
    return max(0.0, time.time() - path.stat().st_mtime)


def _file_mtime_utc_iso(path: Path) -> str | None:
    if not path.is_file():
        return None
    dt = datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)
    return format_utc_iso(dt)


def _read_json_model(path: Path, model_cls: type[Any]) -> Any | None:
    if not path.is_file():
        return None
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
        return model_cls.model_validate(raw)
    except Exception as exc:
        logger.warning("Failed to validate cached %s: %s", path.name, exc)
        return None


def _json_response(model: Any) -> JSONResponse:
    payload = model.model_dump(mode="json")
    body = json.dumps(payload, ensure_ascii=False, allow_nan=False)
    return Response(
        content=body,
        media_type="application/json; charset=utf-8",
        headers={"Cache-Control": "no-store"},
    )


async def _get_or_refresh_live(*, force: bool = False) -> LiveResponse:
    data_dir = _data_dir()
    live_path = data_dir / "live.json"
    cached = _read_json_model(live_path, LiveResponse)

    if not force and cached is not None and _file_age_seconds(live_path) <= CACHE_TTL_SEC:
        return cached

    async with _live_lock:
        # Re-check inside lock in case another concurrent request just refreshed it.
        cached = _read_json_model(live_path, LiveResponse)
        if not force and cached is not None and _file_age_seconds(live_path) <= CACHE_TTL_SEC:
            return cached
        try:
            snap = await fetch_market_snapshot(include_screener=False)
            atomic_write_json(live_path, snap.live)
            return snap.live
        except Exception as exc:
            logger.warning("Live market refresh failed: %s", exc)
            if cached is not None:
                return cached
            raise HTTPException(
                status_code=503,
                detail={"ok": False, "error": f"Live market data unavailable: {exc}"},
            ) from exc


async def _get_or_refresh_news(*, force: bool = False) -> NewsResponse:
    data_dir = _data_dir()
    news_path = data_dir / "news.json"
    cached = _read_json_model(news_path, NewsResponse)

    if not force and cached is not None and _file_age_seconds(news_path) <= CACHE_TTL_SEC:
        return cached

    async with _news_lock:
        cached = _read_json_model(news_path, NewsResponse)
        if not force and cached is not None and _file_age_seconds(news_path) <= CACHE_TTL_SEC:
            return cached
        try:
            agg = await fetch_aggregated_news()
            if agg.response.count > 0 or cached is None:
                atomic_write_json(news_path, agg.response)
                return agg.response
            return cached
        except Exception as exc:
            logger.warning("News refresh failed: %s", exc)
            if cached is not None:
                return cached
            raise HTTPException(
                status_code=502,
                detail={"ok": False, "error": f"News upstream unavailable: {exc}"},
            ) from exc


@app.api_route("/", methods=["GET", "HEAD"], response_class=HTMLResponse)
async def serve_index() -> HTMLResponse:
    if not FRONTEND_PATH.is_file():
        raise HTTPException(status_code=503, detail="frontend/index.html not built yet")
    html = FRONTEND_PATH.read_text(encoding="utf-8")
    return HTMLResponse(
        content=html,
        media_type="text/html; charset=utf-8",
        headers={"Cache-Control": "no-store"},
    )


@app.api_route("/favicon.ico", methods=["GET", "HEAD"])
async def favicon() -> Response:
    return Response(status_code=204)


@app.api_route("/api/health", methods=["GET", "HEAD"])
async def api_health(force: str | None = Query(default=None)) -> Response:
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

    live = _read_json_model(live_path, LiveResponse)
    news = _read_json_model(news_path, NewsResponse)
    state_updated = _file_mtime_utc_iso(state_path)

    health = HealthResponse(
        ok=True,
        version=APP_VERSION,
        serverTimeUtc=format_utc_iso(),
        stateUpdatedAt=state_updated,
        newsCount=news.count if news is not None else 0,
        quotesCount=live.count if live is not None else 0,
    )
    return _json_response(health)


@app.api_route("/api/live", methods=["GET", "HEAD"])
async def api_live(
    force: str | None = Query(default=None),
    sym: str | None = Query(default=None),
) -> Response:
    live = await _get_or_refresh_live(force=_is_force(force))
    if sym:
        sym_clean = sym.strip().upper()
        filtered = [q for q in live.quotes if q.sym.upper() == sym_clean]
        live = LiveResponse(
            updatedAt=live.updatedAt,
            updatedAtEt=live.updatedAtEt,
            marketSession=live.marketSession,
            count=len(filtered),
            quotes=filtered,
        )
    return _json_response(live)


@app.api_route("/api/state", methods=["GET", "HEAD"])
async def api_state(force: str | None = Query(default=None)) -> Response:
    state_path = _data_dir() / "state.json"
    state = _read_json_model(state_path, StateResponse)
    if state is None:
        raise HTTPException(
            status_code=503,
            detail={"ok": False, "error": "Research state not initialized yet; run pipeline first"},
        )
    if _is_force(force):
        state = state.model_copy(update={"serverSyncedAt": format_cn_timestamp()})
    return _json_response(state)


@app.api_route("/api/news", methods=["GET", "HEAD"])
async def api_news(
    force: str | None = Query(default=None),
    limit: str | None = Query(default=None),
) -> Response:
    news = await _get_or_refresh_news(force=_is_force(force))
    if limit is not None:
        try:
            parsed = int(limit.strip())
        except ValueError:
            parsed = None
        if parsed is not None:
            clamped = max(1, min(parsed, 50))
            sliced = news.items[:clamped]
            news = NewsResponse(
                updatedAt=news.updatedAt,
                count=len(sliced),
                items=sliced,
            )
    return _json_response(news)


def main() -> None:
    settings = load_settings()
    logging.basicConfig(
        level=getattr(logging, settings.log_level, logging.INFO),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    uvicorn.run(
        "src.server:app",
        host="0.0.0.0",
        port=settings.port,
        log_level=settings.log_level.lower(),
    )


if __name__ == "__main__":
    main()
