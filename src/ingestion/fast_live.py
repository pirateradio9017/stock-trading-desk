"""Fast 2-batch CNBC live quote + gap-only Yahoo v8 fetcher for always-on VPS server."""

from __future__ import annotations

import asyncio
import logging
import math
from datetime import datetime, timezone
from typing import Any
from urllib.parse import quote as url_quote

import httpx

from src.config import (
    AUXILIARY_BENCHMARK_SYMBOLS,
    BENCHMARK_SYMBOLS,
    CORE_SYMBOLS,
    FEED_SOURCES,
    HTTP_USER_AGENT,
    ROTATION_THEMES,
    SYMBOL_NAME_MAP,
    SYMBOL_SPEC_MAP,
    SymbolSpec,
    format_cn_timestamp,
    format_et_short,
    format_utc_iso,
    get_market_session,
)
from src.ingestion.market_data import _parse_yahoo_chart
from src.ingestion.session_rotation import (
    CNBC_BATCH_URL,
    EXTRA_SESSION_SYMBOLS,
    _parse_iso_ts,
    compare_prints,
    fetch_session_snapshot,
    parse_cnbc_print,
)
from src.models.schemas import LiveResponse, QuoteItem, SessionPrint, SessionRotation

logger = logging.getLogger(__name__)

CNBC_MACRO_MAP: tuple[tuple[str, str], ...] = (
    ("^GSPC", ".SPX"),
    ("^IXIC", ".IXIC"),
    ("^VIX", ".VIX"),
    ("^MOVE", ".MOVE"),
    ("^TNX", "US10Y"),
    ("BTC-USD", "BTC.CM="),
    ("GC=F", "@GC.1"),
    ("SI=F", "@SI.1"),
    ("HG=F", "@HG.1"),
    ("BZ=F", "@LCO.1"),
    ("CL=F", "@CL.1"),
    ("JPY=X", "JPY="),
    ("DX-Y.NYB", ".DXY"),
    ("ES=F", "@SP.1"),
    ("NQ=F", "@ND.1"),
    ("^N225", ".N225"),
    ("^KS11", ".KS11"),
    ("^TWII", ".TWII"),
    ("^HSI", ".HSI"),
    ("000001.SS", ".SSEC"),
    ("2330.TW", "2330-TW"),
    ("^GDAXI", ".GDAXI"),
    ("^STOXX50E", ".STOXX50E"),
    ("ASML.AS", "ASML-NL"),
)

_ROTATION_SYMBOLS: tuple[str, ...] = tuple(
    dict.fromkeys(("SPY", *(sym for syms in ROTATION_THEMES.values() for sym in syms)))
)
CNBC_EQUITY_SYMBOLS: tuple[str, ...] = tuple(
    dict.fromkeys((*_ROTATION_SYMBOLS, *EXTRA_SESSION_SYMBOLS))
)
CNBC_MACRO_SYMBOLS: tuple[str, ...] = tuple(cnbc for _, cnbc in CNBC_MACRO_MAP)
CNBC_TO_CANONICAL: dict[str, str] = {
    **{sym: sym for sym in CNBC_EQUITY_SYMBOLS},
    **{cnbc: canon for canon, cnbc in CNBC_MACRO_MAP},
    **{cnbc.upper(): canon for canon, cnbc in CNBC_MACRO_MAP},
}


def _ordered_live_specs() -> list[SymbolSpec]:
    seen: set[str] = set()
    out: list[SymbolSpec] = []
    for spec in (*CORE_SYMBOLS, *BENCHMARK_SYMBOLS, *AUXILIARY_BENCHMARK_SYMBOLS):
        if spec.sym not in seen:
            seen.add(spec.sym)
            out.append(spec)
    return out


LIVE_SPECS: tuple[SymbolSpec, ...] = tuple(_ordered_live_specs())


def _clean_float(raw: Any) -> float | None:
    if raw is None or isinstance(raw, bool):
        return None
    text = str(raw).replace("$", "").replace("%", "").replace(",", "").strip()
    if not text:
        return None
    try:
        val = float(text)
    except ValueError:
        return None
    return val if math.isfinite(val) else None


def _round_px(val: float | None) -> float | None:
    if val is None or not math.isfinite(val):
        return None
    return round(val, 4 if val < 10 else 2)


def parse_cnbc_live_quote(
    item: dict[str, Any],
    canonical_sym: str,
    display_name: str,
    now_iso: str,
    *,
    is_premarket: bool,
) -> QuoteItem | None:
    """Parse one CNBC FormattedQuote dict into a validated QuoteItem."""
    if not isinstance(item, dict):
        return None
    try:
        if int(item.get("code", -1)) != 0:
            return None
    except (TypeError, ValueError):
        return None

    price = _clean_float(item.get("last"))
    if price is None or price <= 0:
        return None
    chg = _clean_float(item.get("change_pct"))

    ah_price: float | None = None
    ah_chg: float | None = None
    ext = item.get("ExtendedMktQuote")
    if isinstance(ext, dict):
        ext_type = str(ext.get("type") or "").strip().upper()
        ext_allowed = (ext_type == "PRE_MKT") if is_premarket else (ext_type in {"POST_MKT", "PRE_MKT"})
        if ext_allowed:
            ep = _clean_float(ext.get("last"))
            ec = _clean_float(ext.get("change_pct"))
            if ep is not None and ep > 0:
                ah_price = ep
                ah_chg = ec if ec is not None else ((ep / price) - 1.0) * 100.0

    ts_utc = now_iso
    parsed_iso = _parse_iso_ts(item.get("last_time"))
    if parsed_iso is not None:
        ts_utc = format_utc_iso(parsed_iso)
    elif item.get("last_time_msec"):
        try:
            ms = float(item["last_time_msec"])
            if math.isfinite(ms) and ms > 1_000_000_000_000:
                ts_utc = format_utc_iso(datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc))
        except (TypeError, ValueError):
            pass
    else:
        raw_lt = str(item.get("last_time") or "").strip()
        if len(raw_lt) == 10 and raw_lt[4] == "-" and raw_lt[7] == "-":
            ts_utc = f"{raw_lt}T21:00:00Z"

    return QuoteItem(
        sym=canonical_sym,
        name=display_name,
        price=_round_px(price),
        chg=round(chg, 2) if chg is not None else None,
        ahPrice=_round_px(ah_price),
        ahChg=round(ah_chg, 2) if ah_chg is not None else None,
        r5=None,
        r20=None,
        source="cnbc_batch",
        tsUtc=ts_utc,
    )


async def _fetch_cnbc_batch(
    client: httpx.AsyncClient,
    cnbc_symbols: tuple[str, ...],
    timeout_sec: float = 4.5,
) -> list[dict[str, Any]]:
    params = {
        "symbols": "|".join(cnbc_symbols),
        "requestMethod": "itv",
        "noform": "1",
        "partnerId": "2",
        "fund": "1",
        "exthrs": "1",
        "output": "json",
        "events": "1",
    }
    try:
        resp = await client.get(
            CNBC_BATCH_URL,
            params=params,
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                "Accept": "application/json",
            },
            timeout=timeout_sec,
        )
        if resp.status_code >= 400:
            return []
        raw = (resp.json().get("FormattedQuoteResult") or {}).get("FormattedQuote")
        if isinstance(raw, list):
            return [x for x in raw if isinstance(x, dict)]
        if isinstance(raw, dict):
            return [raw]
        return []
    except Exception as exc:
        logger.debug("Fast CNBC batch failed: %s", exc)
        return []


async def _fetch_yahoo_gap_symbol(
    client: httpx.AsyncClient,
    sem: asyncio.Semaphore,
    spec: SymbolSpec,
    *,
    market_session: str,
    now_utc: datetime,
    timeout_sec: float = 4.5,
) -> QuoteItem | None:
    url = FEED_SOURCES["yahoo_v8"].url.format(symbol=url_quote(spec.sym, safe=""))
    params = {"range": "3mo", "interval": "1d", "includePrePost": "true"}
    async with sem:
        try:
            resp = await client.get(
                url,
                params=params,
                headers={"User-Agent": HTTP_USER_AGENT},
                timeout=timeout_sec,
            )
            if resp.status_code >= 400:
                return None
            series = _parse_yahoo_chart(
                spec,
                resp.json(),
                market_session=market_session,
                now_utc=now_utc,
            )
            return series.quote
        except Exception as exc:
            logger.debug("Yahoo gap fetch failed for %s: %s", spec.sym, exc)
            return None


async def fetch_fast_live(
    *,
    cached: LiveResponse | None = None,
    now_utc: datetime | None = None,
    timeout_sec: float = 6.0,
) -> LiveResponse:
    """Refresh 52 live quotes + intraday/premarket rotation in ~0.5-1.5s via 2 CNBC batches + gap Yahoo."""
    ts_now = now_utc or datetime.now(timezone.utc)
    if ts_now.tzinfo is None:
        ts_now = ts_now.replace(tzinfo=timezone.utc)

    now_iso = format_utc_iso(ts_now)
    market_session = get_market_session(ts_now)
    is_premarket = market_session == "PRE_MARKET"
    is_session_active = market_session in {"PRE_MARKET", "INTRADAY"}

    cached_by_sym: dict[str, QuoteItem] = {}
    if cached is not None:
        for q in cached.quotes:
            cached_by_sym[q.sym] = q

    timeout = httpx.Timeout(
        connect=min(3.5, timeout_sec),
        read=timeout_sec,
        write=4.0,
        pool=4.0,
    )
    limits = httpx.Limits(max_connections=24, max_keepalive_connections=12)

    fetched_by_sym: dict[str, QuoteItem] = {}
    edge_prints: dict[str, SessionPrint] = {}

    async with httpx.AsyncClient(timeout=timeout, limits=limits, follow_redirects=True) as client:
        equity_items, macro_items = await asyncio.gather(
            _fetch_cnbc_batch(client, CNBC_EQUITY_SYMBOLS, timeout_sec=min(4.5, timeout_sec)),
            _fetch_cnbc_batch(client, CNBC_MACRO_SYMBOLS, timeout_sec=min(4.5, timeout_sec)),
        )

        parsed_at = datetime.now(timezone.utc) if now_utc is None else ts_now
        for item in equity_items:
            raw_sym = str(item.get("symbol") or "").strip().upper()
            canon = CNBC_TO_CANONICAL.get(raw_sym, raw_sym)
            if canon in SYMBOL_SPEC_MAP:
                q = parse_cnbc_live_quote(
                    item,
                    canon,
                    SYMBOL_NAME_MAP.get(canon, canon),
                    now_iso,
                    is_premarket=is_premarket,
                )
                if q is not None and q.price is not None:
                    fetched_by_sym[canon] = q
            if is_session_active:
                sp = parse_cnbc_print(item, parsed_at, market_session)
                if sp is not None:
                    edge_prints[sp.sym] = sp

        for item in macro_items:
            raw_sym = str(item.get("symbol") or "").strip()
            canon = CNBC_TO_CANONICAL.get(raw_sym) or CNBC_TO_CANONICAL.get(raw_sym.upper())
            if not canon or canon in fetched_by_sym:
                continue
            q = parse_cnbc_live_quote(
                item,
                canon,
                SYMBOL_NAME_MAP.get(canon, canon),
                now_iso,
                is_premarket=is_premarket,
            )
            if q is not None and q.price is not None:
                fetched_by_sym[canon] = q

        missing_specs = [spec for spec in LIVE_SPECS if spec.sym not in fetched_by_sym]
        if missing_specs:
            sem = asyncio.Semaphore(8)
            gap_results = await asyncio.gather(
                *(
                    _fetch_yahoo_gap_symbol(
                        client,
                        sem,
                        spec,
                        market_session=market_session,
                        now_utc=ts_now,
                        timeout_sec=min(5.0, timeout_sec),
                    )
                    for spec in missing_specs
                )
            )
            for spec, q in zip(missing_specs, gap_results):
                if q is not None and q.price is not None:
                    fetched_by_sym[spec.sym] = q

    built_rotation: SessionRotation | None = None
    if is_session_active:
        if edge_prints:
            cand = compare_prints(edge_prints, parsed_at, market_session)
            if len(cand.themes) >= 2 or not (cached and cached.session_rotation and cached.session_rotation.session == market_session):
                built_rotation = cand
        if built_rotation is None or len(built_rotation.themes) < 2:
            try:
                fb_rot, fb_prints = await asyncio.wait_for(
                    fetch_session_snapshot(ts_now, market_session),
                    timeout=8.0,
                )
                for sym, sp in fb_prints.items():
                    edge_prints.setdefault(sym, sp)
                if fb_rot is not None and (
                    len(fb_rot.themes) >= 2
                    or (built_rotation is None and len(fb_rot.themes) > 0)
                ):
                    built_rotation = fb_rot
            except Exception as exc:
                logger.debug("Fast session rotation fallback skipped: %s", exc)

    fresh_count = len(fetched_by_sym)
    has_fresh_rotation = built_rotation is not None and bool(built_rotation.themes)
    if fresh_count == 0 and not has_fresh_rotation and not edge_prints:
        if cached is not None and cached.quotes:
            return cached
        raise RuntimeError("Fast live upstream returned 0 quotes")

    reusable_rotation: SessionRotation | None = None
    if is_session_active:
        if built_rotation is not None:
            reusable_rotation = built_rotation
        elif cached and cached.session_rotation and cached.session_rotation.session == market_session:
            reusable_rotation = cached.session_rotation

    session_prints: dict[str, SessionPrint] = {}
    if reusable_rotation is not None:
        if reusable_rotation.benchmark is not None:
            session_prints[reusable_rotation.benchmark.sym] = reusable_rotation.benchmark
        for theme in reusable_rotation.themes:
            for m in theme.members:
                session_prints[m.sym] = m
    session_prints.update(edge_prints)

    def _apply_premarket(q: QuoteItem) -> QuoteItem:
        if not is_premarket:
            return q
        sp = session_prints.get(q.sym)
        if sp is not None and sp.price is not None:
            return q.model_copy(update={"ahPrice": sp.price, "ahChg": sp.change})
        return q

    live_sym_set = {spec.sym for spec in LIVE_SPECS}
    quotes: list[QuoteItem] = []
    for spec in LIVE_SPECS:
        fresh = fetched_by_sym.get(spec.sym)
        old = cached_by_sym.get(spec.sym)
        if fresh is not None and fresh.price is not None:
            merged = fresh.model_copy(
                update={
                    "r5": fresh.r5 if fresh.r5 is not None else (old.r5 if old is not None else None),
                    "r20": fresh.r20 if fresh.r20 is not None else (old.r20 if old is not None else None),
                }
            )
            quotes.append(_apply_premarket(merged))
        elif old is not None:
            quotes.append(_apply_premarket(old))
        else:
            quotes.append(
                QuoteItem(
                    sym=spec.sym,
                    name=spec.name,
                    price=None,
                    chg=None,
                    source="unavailable",
                    tsUtc=now_iso,
                )
            )

    for sym, old in cached_by_sym.items():
        if sym not in live_sym_set:
            quotes.append(_apply_premarket(old))

    return LiveResponse(
        session_rotation=reusable_rotation,
        updatedAt=format_cn_timestamp(ts_now),
        updatedAtEt=format_et_short(ts_now),
        marketSession=market_session,
        count=len(quotes),
        quotes=quotes,
    )
