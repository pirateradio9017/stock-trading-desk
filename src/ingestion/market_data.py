"""Multi-region market quote, historical return, correlation, and scissor trend ingestion."""

from __future__ import annotations

import asyncio
import logging
import math
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any
from urllib.parse import quote as url_quote

import httpx
import numpy as np
import pandas as pd

from src.config import (
    ALL_SYMBOLS,
    BENCHMARK_SYMBOLS,
    CORE_SYMBOLS,
    CORR_LABELS,
    CORR_SYMBOLS,
    FEED_SOURCES,
    HTTP_USER_AGENT,
    SCISSOR_SERIES,
    SYMBOL_NAME_MAP,
    SymbolSpec,
    TZ_ET,
    format_cn_timestamp,
    format_et_short,
    format_utc_iso,
    get_market_session,
)
from src.models.schemas import (
    LinkageTrend,
    LinkageTrendSeries,
    LiveResponse,
    QuoteItem,
    RotationAsset,
)

logger = logging.getLogger(__name__)

_ROTATION_GROUPS: dict[str, str] = {
    "NVDA": "AI算力",
    "AVGO": "AI算力",
    "AMD": "AI算力",
    "LITE": "光通信",
    "SOXX": "半导体",
    "SMH": "半导体",
    "ASML": "半导体",
    "IREN": "电力算力",
    "NBIS": "新云基建",
    "CRWV": "新云基建",
    "AAPL": "大型科技",
    "QQQ": "宽基指数",
    "SPY": "宽基指数",
    "ITB": "地产链",
    "XLE": "能源",
    "XLU": "公用电力",
    "XLP": "防御消费",
    "XLF": "金融",
    "TLT": "长债",
    "GLD": "黄金",
    "HYG": "高收益债",
    "COIN": "加密链",
    "MSTR": "加密链",
    "BTC-USD": "加密链",
}


@dataclass
class SymbolSeries:
    """Daily OHLC close series and metadata for a single symbol."""

    spec: SymbolSpec
    dates: list[str] = field(default_factory=list)
    closes: list[float] = field(default_factory=list)
    quote: QuoteItem | None = None


@dataclass
class ScreenerMover:
    """Notable mover from Yahoo Finance predefined screeners."""

    category: str
    sym: str
    name: str
    price: float | None
    chg: float | None
    volume: int | None = None


@dataclass
class MarketSnapshot:
    """Aggregated market quotes, analytics series, and screener context."""

    fetched_at_utc: datetime
    live: LiveResponse
    series_map: dict[str, SymbolSeries]
    spark_labels: list[str]
    symbol_sparks: dict[str, list[float]]
    corr_labels: list[str]
    corr_matrix: list[list[float]]
    linkage_trend: LinkageTrend
    rotation_assets: list[RotationAsset]
    movers: list[ScreenerMover] = field(default_factory=list)

    def get_quote(self, sym: str) -> QuoteItem | None:
        s = self.series_map.get(sym)
        if s and s.quote:
            return s.quote
        for q in self.live.quotes:
            if q.sym == sym:
                return q
        return None


def _finite_or_none(val: Any) -> float | None:
    if val is None or isinstance(val, bool):
        return None
    try:
        f = float(val)
    except (TypeError, ValueError):
        return None
    return f if math.isfinite(f) else None


def _round_or_none(val: float | None, digits: int = 2) -> float | None:
    if val is None or not math.isfinite(val):
        return None
    return round(val, digits)


def _compute_return(closes: list[float], lookback: int, current_price: float | None) -> float | None:
    """Compute percentage return over `lookback` trading days."""
    if current_price is None or current_price <= 0 or len(closes) <= lookback:
        return None
    base = closes[-1 - lookback]
    if base is None or base <= 0:
        return None
    return round(((current_price / base) - 1.0) * 100.0, 2)


def _normalize_spark(closes: list[float], length: int = 11) -> list[float]:
    """Normalize the trailing `length` closes to base 100.0."""
    valid = [c for c in closes if c is not None and math.isfinite(c) and c > 0]
    if len(valid) < 2:
        return [100.0] * length
    tail = valid[-length:]
    if len(tail) < length:
        pad = [tail[0]] * (length - len(tail))
        tail = pad + tail
    base = tail[0]
    if base <= 0:
        return [100.0] * length
    return [round((v / base) * 100.0, 2) for v in tail]


def _parse_yahoo_chart(
    spec: SymbolSpec,
    payload: dict[str, Any],
    *,
    market_session: str,
    now_utc: datetime,
) -> SymbolSeries:
    """Parse Yahoo v8 chart JSON into SymbolSeries + QuoteItem."""
    result_list = (payload.get("chart") or {}).get("result") or []
    if not result_list:
        raise ValueError(f"Empty Yahoo chart result for {spec.sym}")

    result = result_list[0]
    meta = result.get("meta") or {}
    timestamps = result.get("timestamp") or []
    quote_block = ((result.get("indicators") or {}).get("quote") or [{}])[0]
    raw_closes = quote_block.get("close") or []

    dates: list[str] = []
    closes: list[float] = []
    seen_dates: dict[str, float] = {}
    for ts, close_val in zip(timestamps, raw_closes):
        c = _finite_or_none(close_val)
        if c is None or c <= 0:
            continue
        dt_et = datetime.fromtimestamp(int(ts), tz=timezone.utc).astimezone(TZ_ET)
        d_str = dt_et.strftime("%Y-%m-%d")
        seen_dates[d_str] = c

    for d_str in sorted(seen_dates):
        dates.append(d_str)
        closes.append(seen_dates[d_str])

    price = _finite_or_none(meta.get("regularMarketPrice"))
    if price is None and closes:
        price = closes[-1]

    # Prefer Yahoo's regularMarketChangePercent; avoid range-wide chartPreviousClose distortion.
    chg = _finite_or_none(meta.get("regularMarketChangePercent"))
    if chg is None and price is not None and len(closes) >= 2:
        prev_close = closes[-2]
        if prev_close > 0:
            chg = ((price / prev_close) - 1.0) * 100.0

    ah_price: float | None = None
    ah_chg: float | None = None
    pre_price = _finite_or_none(meta.get("preMarketPrice"))
    pre_chg = _finite_or_none(meta.get("preMarketChangePercent"))
    post_price = _finite_or_none(meta.get("postMarketPrice"))
    post_chg = _finite_or_none(meta.get("postMarketChangePercent"))

    if market_session == "PRE_MARKET" and pre_price is not None:
        ah_price = pre_price
        ah_chg = pre_chg if pre_chg is not None and price and price > 0 else pre_chg
        if ah_chg is None and price and price > 0:
            ah_chg = ((pre_price / price) - 1.0) * 100.0
    elif post_price is not None:
        ah_price = post_price
        ah_chg = post_chg
        if ah_chg is None and price and price > 0:
            ah_chg = ((post_price / price) - 1.0) * 100.0
    elif pre_price is not None:
        ah_price = pre_price
        ah_chg = pre_chg
        if ah_chg is None and price and price > 0:
            ah_chg = ((pre_price / price) - 1.0) * 100.0

    reg_ts = meta.get("regularMarketTime")
    if isinstance(reg_ts, (int, float)) and reg_ts > 0:
        ts_dt = datetime.fromtimestamp(int(reg_ts), tz=timezone.utc)
    else:
        ts_dt = now_utc

    r5 = _compute_return(closes, 5, price)
    r20 = _compute_return(closes, 20, price)

    quote = QuoteItem(
        sym=spec.sym,
        name=spec.name,
        price=_round_or_none(price, 4 if (price is not None and price < 10) else 2),
        chg=_round_or_none(chg, 2),
        ahPrice=_round_or_none(ah_price, 4 if (ah_price is not None and ah_price < 10) else 2),
        ahChg=_round_or_none(ah_chg, 2),
        r5=r5,
        r20=r20,
        source="yahoo_v8",
        tsUtc=format_utc_iso(ts_dt),
    )
    return SymbolSeries(spec=spec, dates=dates, closes=closes, quote=quote)


def _yfinance_fallback_sync(spec: SymbolSpec, now_utc: datetime) -> SymbolSeries:
    """Synchronous yfinance fallback when Yahoo v8 HTTP endpoint fails."""
    import yfinance as yf

    hist = yf.Ticker(spec.sym).history(period="3mo", interval="1d", auto_adjust=False)
    if hist is None or hist.empty or "Close" not in hist:
        raise ValueError(f"Empty yfinance history for {spec.sym}")

    dates: list[str] = []
    closes: list[float] = []
    for idx, val in hist["Close"].items():
        c = _finite_or_none(val)
        if c is None or c <= 0:
            continue
        dates.append(pd.Timestamp(idx).strftime("%Y-%m-%d"))
        closes.append(c)

    if not closes:
        raise ValueError(f"No finite closes in yfinance history for {spec.sym}")

    price = closes[-1]
    chg = ((price / closes[-2]) - 1.0) * 100.0 if len(closes) >= 2 and closes[-2] > 0 else 0.0
    r5 = _compute_return(closes, 5, price)
    r20 = _compute_return(closes, 20, price)
    quote = QuoteItem(
        sym=spec.sym,
        name=spec.name,
        price=_round_or_none(price, 2),
        chg=_round_or_none(chg, 2),
        r5=r5,
        r20=r20,
        source="yfinance",
        tsUtc=format_utc_iso(now_utc),
    )
    return SymbolSeries(spec=spec, dates=dates, closes=closes, quote=quote)


async def _fetch_single_symbol(
    client: httpx.AsyncClient,
    sem: asyncio.Semaphore,
    spec: SymbolSpec,
    *,
    market_session: str,
    now_utc: datetime,
) -> SymbolSeries:
    """Fetch a single symbol via Yahoo v8 with retry and yfinance fallback."""
    url_tpl = FEED_SOURCES["yahoo_v8"].url
    url = url_tpl.format(symbol=url_quote(spec.sym, safe=""))
    params = {"range": "3mo", "interval": "1d", "includePrePost": "true"}

    last_exc: Exception | None = None
    async with sem:
        for attempt in range(2):
            try:
                resp = await client.get(
                    url,
                    params=params,
                    headers={"User-Agent": HTTP_USER_AGENT, "Accept": "application/json"},
                )
                if resp.status_code == 200:
                    return _parse_yahoo_chart(
                        spec,
                        resp.json(),
                        market_session=market_session,
                        now_utc=now_utc,
                    )
                last_exc = RuntimeError(f"HTTP {resp.status_code} for {spec.sym}")
            except Exception as exc:
                last_exc = exc
            if attempt == 0:
                await asyncio.sleep(0.35)

    try:
        return await asyncio.to_thread(_yfinance_fallback_sync, spec, now_utc)
    except Exception as yf_exc:
        logger.warning("Failed fetching %s (%s; yf=%s)", spec.sym, last_exc, yf_exc)
        return SymbolSeries(
            spec=spec,
            dates=[],
            closes=[],
            quote=QuoteItem(
                sym=spec.sym,
                name=spec.name,
                price=None,
                chg=None,
                source="unavailable",
                tsUtc=format_utc_iso(now_utc),
            ),
        )


async def _fetch_screeners(client: httpx.AsyncClient) -> list[ScreenerMover]:
    """Best-effort fetch of Yahoo day_gainers, day_losers, most_actives."""
    targets = (
        ("day_gainers", FEED_SOURCES["yahoo_screener_gainers"].url),
        ("day_losers", FEED_SOURCES["yahoo_screener_losers"].url),
        ("most_actives", FEED_SOURCES["yahoo_screener_actives"].url),
    )

    async def _one(category: str, url: str) -> list[ScreenerMover]:
        try:
            resp = await client.get(
                url,
                params={"count": "8"},
                headers={"User-Agent": HTTP_USER_AGENT, "Accept": "application/json"},
            )
            if resp.status_code != 200:
                return []
            results = ((resp.json().get("finance") or {}).get("result") or [])
            if not results:
                return []
            quotes = results[0].get("quotes") or []
            out: list[ScreenerMover] = []
            for q in quotes[:8]:
                sym = str(q.get("symbol") or "").strip()
                if not sym:
                    continue
                out.append(
                    ScreenerMover(
                        category=category,
                        sym=sym,
                        name=str(q.get("shortName") or q.get("longName") or sym).strip(),
                        price=_round_or_none(_finite_or_none(q.get("regularMarketPrice")), 2),
                        chg=_round_or_none(_finite_or_none(q.get("regularMarketChangePercent")), 2),
                        volume=int(q["regularMarketVolume"]) if isinstance(q.get("regularMarketVolume"), (int, float)) else None,
                    )
                )
            return out
        except Exception as exc:
            logger.debug("Screener %s failed: %s", category, exc)
            return []

    batches = await asyncio.gather(*(_one(cat, url) for cat, url in targets))
    return [item for batch in batches for item in batch]


def _build_spark_labels(series_map: dict[str, SymbolSeries], now_utc: datetime) -> list[str]:
    """Extract 11 clean MM-DD labels anchored on ^GSPC or trading-day calendar."""
    ref = series_map.get("^GSPC") or series_map.get("SPY") or series_map.get("NVDA")
    if ref and len(ref.dates) >= 11:
        return [d[5:] for d in ref.dates[-11:]]
    base = now_utc.astimezone(TZ_ET).date()
    return [(base - timedelta(days=10 - i)).strftime("%m-%d") for i in range(11)]


def _build_corr_matrix(series_map: dict[str, SymbolSeries]) -> list[list[float]]:
    """Compute a 6x6 symmetric Pearson correlation matrix over aligned daily returns."""
    n = len(CORR_SYMBOLS)
    frame_data: dict[str, pd.Series] = {}
    for sym in CORR_SYMBOLS:
        s = series_map.get(sym)
        if s and len(s.dates) >= 10:
            frame_data[sym] = pd.Series(s.closes, index=pd.to_datetime(s.dates)).pct_change()

    if len(frame_data) < 2:
        return [[1.0 if i == j else 0.0 for j in range(n)] for i in range(n)]

    df = pd.DataFrame(frame_data).sort_index().ffill().tail(45)
    corr_df = df.corr(min_periods=8)

    matrix: list[list[float]] = []
    for i, sym_i in enumerate(CORR_SYMBOLS):
        row: list[float] = []
        for j, sym_j in enumerate(CORR_SYMBOLS):
            if i == j:
                row.append(1.0)
                continue
            val: float | None = None
            if sym_i in corr_df.columns and sym_j in corr_df.index:
                raw = corr_df.loc[sym_j, sym_i]
                val = _finite_or_none(raw)
            if val is None:
                val = 0.0
            row.append(round(max(-1.0, min(1.0, val)), 2))
        matrix.append(row)

    # Enforce strict symmetry.
    for i in range(n):
        matrix[i][i] = 1.0
        for j in range(i + 1, n):
            avg = round(0.5 * (matrix[i][j] + matrix[j][i]), 2)
            matrix[i][j] = avg
            matrix[j][i] = avg
    return matrix


def _build_linkage_trend(series_map: dict[str, SymbolSeries], now_utc: datetime) -> LinkageTrend:
    """Build 30-trading-day cumulative % return scissor trend aligned on ^GSPC."""
    anchor = series_map.get("^GSPC") or series_map.get("SPY")
    if anchor and len(anchor.dates) >= 5:
        anchor_dates = anchor.dates[-30:]
    else:
        today_et = now_utc.astimezone(TZ_ET).date()
        anchor_dates = [
            (today_et - timedelta(days=29 - i)).strftime("%Y-%m-%d") for i in range(30)
        ]

    labels = [d[5:] for d in anchor_dates]
    anchor_index = pd.to_datetime(anchor_dates)

    series_out: list[LinkageTrendSeries] = []
    for sym, label in SCISSOR_SERIES:
        s = series_map.get(sym)
        if s and len(s.dates) >= 2:
            raw = pd.Series(s.closes, index=pd.to_datetime(s.dates))
            aligned = raw.reindex(raw.index.union(anchor_index)).sort_index().ffill().bfill()
            vals = aligned.reindex(anchor_index).tolist()
            base = _finite_or_none(vals[0]) if vals else None
            if base and base > 0:
                pct_vals = [
                    round(((v / base) - 1.0) * 100.0, 2)
                    if (_finite_or_none(v) is not None)
                    else 0.0
                    for v in vals
                ]
            else:
                pct_vals = [0.0] * len(labels)
        else:
            pct_vals = [0.0] * len(labels)

        series_out.append(LinkageTrendSeries(sym=sym, label=label, values=pct_vals))

    return LinkageTrend(labels=labels, series=series_out)


def _build_rotation_assets(series_map: dict[str, SymbolSeries]) -> list[RotationAsset]:
    """Assemble 5d vs 20d rotation scatter points for core & sector tickers."""
    assets: list[RotationAsset] = []
    for sym, group in _ROTATION_GROUPS.items():
        s = series_map.get(sym)
        if not s or not s.quote:
            continue
        r5 = s.quote.r5
        r20 = s.quote.r20
        if r5 is None or r20 is None:
            continue
        assets.append(
            RotationAsset(
                sym=sym,
                name=SYMBOL_NAME_MAP.get(sym, s.quote.name or sym),
                group=group,
                r5=r5,
                r20=r20,
            )
        )
    return assets


async def fetch_market_snapshot(
    *,
    now_utc: datetime | None = None,
    include_screener: bool = True,
    timeout_sec: float = 12.0,
) -> MarketSnapshot:
    """Fetch multi-region live quotes, history, correlation matrix, and scissor trend."""
    ts_now = now_utc or datetime.now(timezone.utc)
    if ts_now.tzinfo is None:
        ts_now = ts_now.replace(tzinfo=timezone.utc)

    market_session = get_market_session(ts_now)
    sem = asyncio.Semaphore(8)

    async with httpx.AsyncClient(timeout=timeout_sec, follow_redirects=True) as client:
        tasks = [
            _fetch_single_symbol(
                client,
                sem,
                spec,
                market_session=market_session,
                now_utc=ts_now,
            )
            for spec in ALL_SYMBOLS
        ]
        if include_screener:
            screener_task = asyncio.create_task(_fetch_screeners(client))
            results = await asyncio.gather(*tasks)
            movers = await screener_task
        else:
            results = await asyncio.gather(*tasks)
            movers = []

    series_map: dict[str, SymbolSeries] = {res.spec.sym: res for res in results}

    # Always keep the 27 CORE_SYMBOLS first, followed by BENCHMARK_SYMBOLS.
    ordered_specs = list(CORE_SYMBOLS) + list(BENCHMARK_SYMBOLS)
    quotes: list[QuoteItem] = []
    for spec in ordered_specs:
        s = series_map.get(spec.sym)
        if s and s.quote is not None:
            quotes.append(s.quote)
        else:
            quotes.append(
                QuoteItem(
                    sym=spec.sym,
                    name=spec.name,
                    price=None,
                    chg=None,
                    source="unavailable",
                    tsUtc=format_utc_iso(ts_now),
                )
            )

    live = LiveResponse(
        updatedAt=format_cn_timestamp(ts_now),
        updatedAtEt=format_et_short(ts_now),
        marketSession=market_session,
        count=len(quotes),
        quotes=quotes,
    )

    spark_labels = _build_spark_labels(series_map, ts_now)
    symbol_sparks = {
        sym: _normalize_spark(s.closes, length=11)
        for sym, s in series_map.items()
    }
    corr_matrix = _build_corr_matrix(series_map)
    linkage_trend = _build_linkage_trend(series_map, ts_now)
    rotation_assets = _build_rotation_assets(series_map)

    return MarketSnapshot(
        fetched_at_utc=ts_now,
        live=live,
        series_map=series_map,
        spark_labels=spark_labels,
        symbol_sparks=symbol_sparks,
        corr_labels=list(CORR_LABELS),
        corr_matrix=corr_matrix,
        linkage_trend=linkage_trend,
        rotation_assets=rotation_assets,
        movers=movers,
    )
