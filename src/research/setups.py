"""Derive price, valuation, and anchor review observations from ingested data."""

from __future__ import annotations

from datetime import date, timedelta
from statistics import median
from urllib.parse import quote as url_quote

from src.config import TZ_ET
from src.ingestion.market_data import MarketSnapshot, SymbolSeries
from src.models.schemas import AnchorReview, MainlineItem, ResearchSetup, SetupValuation
from src.research.state_machine import GovernanceDecision

_DEFENSIVE_REFERENCES = {"XLE", "LMT", "XLP", "XLU", "TLT", "GLD"}
_HIGH_RISK_CATEGORIES = {"equity_cloud_power", "equity_crypto"}
_CATEGORY_LABELS = {
    "dip": "深度回撤",
    "hedge": "防御对照",
    "spec": "高波观察",
    "avoid": "风险警示",
    "watch": "待观察",
}


def _roster(mainline: MainlineItem) -> list[str]:
    pairs = (pair.partition(":")[0].strip().upper() for pair in mainline.stocks.split(",") if ":" in pair)
    return list(dict.fromkeys((mainline.anchor_sym.upper(), *pairs)))


_US_TRADED_EQUITY_OVERRIDES = {"ASML"}
_CORE_SETUP_FALLBACKS: tuple[tuple[str, str], ...] = (
    ("TSM", "M1"),
    ("AVGO", "M1"),
    ("NVDA", "M1"),
    ("ASML", "M1"),
    ("LMT", "M5"),
    ("XLE", "M5"),
    ("ITB", "M2"),
    ("TLT", "M2"),
    ("CRWV", "M4"),
    ("COIN", "M4"),
)


def _security_series(market: MarketSnapshot, symbol: str) -> SymbolSeries | None:
    series = market.series_map.get(symbol)
    if series is None or (series.spec.region != "US" and series.spec.sym not in _US_TRADED_EQUITY_OVERRIDES):
        return None
    category = series.spec.category
    return series if category.startswith("equity_") or category.endswith("etf") else None


def _same_market_kind(left: SymbolSeries, right: SymbolSeries) -> bool:
    return left.spec.category == right.spec.category or (
        left.spec.category.startswith("equity_") and right.spec.category.startswith("equity_")
    )


def _dates_and_closes(series: SymbolSeries, through: date) -> dict[str, float]:
    return {
        day: close for day, close in zip(series.dates, series.closes)
        if day <= through.isoformat() and close > 0
    }


def _recent_market_date(day: str, market_date: date) -> bool:
    return (market_date - timedelta(days=7)).isoformat() <= day <= market_date.isoformat()


_OVERSEAS_PEER_MAP: dict[str, tuple[str, str]] = {
    "TSM": ("2330.TW", "台积电台股"),
    "ASML": ("ASML.AS", "阿斯麦欧股"),
    "NVDA": ("000660.KS", "SK海力士"),
}


def _session_print_map(market: MarketSnapshot) -> dict[str, tuple[float, str]]:
    rot = market.live.session_rotation
    if rot is None:
        return {}
    out: dict[str, tuple[float, str]] = {}
    if rot.benchmark is not None:
        out[rot.benchmark.sym.upper()] = (rot.benchmark.change, rot.session)
    for theme in rot.themes:
        for member in theme.members:
            out[member.sym.upper()] = (member.change, rot.session)
    return out


def _watch_category(
    series: SymbolSeries,
    drawdown: float | None,
    r5: float | None,
    r20: float | None,
    *,
    forward_pe: float | None = None,
    pe_discount_pct: float | None = None,
    reward_risk: float | None = None,
    tnx_yield: float | None = None,
) -> tuple[str, str]:
    sym = series.spec.sym
    is_high_risk = series.spec.category in _HIGH_RISK_CATEGORIES
    has_quality_val_cushion = (
        not is_high_risk
        and drawdown is not None
        and drawdown >= 25
        and forward_pe is not None
        and 4.0 <= forward_pe <= 40.0
        and pe_discount_pct is not None
        and pe_discount_pct >= 20.0
    )
    if is_high_risk:
        if r5 is not None and r20 is not None and r5 <= -5 and r20 <= -15:
            return "avoid", "高波动板块 5 日与 20 日均深度走弱，缺乏稳健估值安全垫，严控风险。"
        return "spec", "所属板块对融资条件或加密波动较敏感，关注回撤和短期动量。"
    if has_quality_val_cushion:
        if r5 is not None and r20 is not None and r5 <= -5 and r20 <= -15:
            return "dip", f"距 52 周高点回撤 {drawdown:.1f}%，远期 PE {forward_pe:.1f}x（较历史中位折价 {pe_discount_pct:.0f}%）；美股多日仍弱，盯现货放量止跌。"
        return "dip", f"距 52 周高点回撤 {drawdown:.1f}%，远期 PE {forward_pe:.1f}x（较历史中位折价 {pe_discount_pct:.0f}%），具备估值安全垫，观察承接。"
    if r5 is not None and r20 is not None and r5 <= -5 and r20 <= -15:
        return "avoid", "5 日和 20 日均显著走弱，且暂缺充分估值折价保护，观察短期跌势是否延续。"
    if sym in {"ITB", "XHB"} and tnx_yield is not None and tnx_yield >= 4.5 and reward_risk is not None and reward_risk < 0.5:
        return "avoid", f"逼近近 20 日区间上沿（盈亏比仅 {reward_risk:.1f}:1），且 10Y 美债 {tnx_yield:.2f}% 高位压制按揭需求，不宜追高。"
    if drawdown is not None and drawdown >= 25:
        return "dip", "距 52 周高点已回撤至少 25%，观察动量能否企稳及估值比较是否支持。"
    if sym in _DEFENSIVE_REFERENCES:
        return "hedge", "作为防御或行业对照，跟踪它在大盘走弱时的相对表现。"
    return "watch", "跟踪主线内相对强弱和观察价位。"


def _setup(series: SymbolSeries, mainline_id: str, market: MarketSnapshot) -> ResearchSetup:
    sym = series.spec.sym
    market_date = market.fetched_at_utc.astimezone(TZ_ET).date()
    closes = _dates_and_closes(series, market_date)
    q = series.quote
    latest = max(closes, default="")
    has_price = bool(q and q.price and _recent_market_date(latest, market_date))
    price = q.price if q and has_price else None
    high = series.high_52w if has_price else None
    drawdown = round(100 * (1 - price / high), 2) if price and high and price <= high else None

    fund_data = market.valuations.get(sym)
    forward_pe: float | None = None
    discount: float | None = None
    if fund_data is not None:
        forward_pe = fund_data.forward_pe
        count = len(fund_data.historical_forward_pe)
        reference = round(median(fund_data.historical_forward_pe), 2) if count >= 3 else None
        discount = round(100 * (1 - forward_pe / reference), 2) if forward_pe and reference else None
        valuation = SetupValuation(
            forward_pe=forward_pe,
            reference_forward_pe=reference,
            forward_pe_discount_pct=discount,
            reference_observations=count,
            note=(
                f"Yahoo Finance 同一公司最近 {count} 个有效历史快照中位数。估值差随盈利预期变化。"
                if discount is not None else "缺少正值预期市盈率或足够的历史样本。"
            ),
            source_url=fund_data.source_url,
        )
    else:
        valuation = SetupValuation(note=(
            "暂未取得可用的预期市盈率。" if series.spec.category.startswith("equity_")
            else "ETF 用于价格观察，这里没有适用的单一公司预期市盈率。"
        ))

    recent_closes = [closes[day] for day in sorted(closes)[:-1][-20:]]
    low = upper = ratio = None
    scenario_note = "需有 20 个完整历史收盘价，且当前价处于历史区间内，才计算区间收益风险比。"
    if price and len(recent_closes) == 20:
        precision = 4 if price < 10 else 2
        lower_bound = round(min(recent_closes), precision)
        upper_bound = round(max(recent_closes), precision)
        if lower_bound < price < upper_bound:
            if (price - lower_bound) < price * 0.04:
                scenario_note = "现价紧贴近 20 日收盘低点（距下沿不足 4%），机械盈亏比易失真，先看企稳再评估。"
            else:
                low, upper = lower_bound, upper_bound
                ratio = round((upper - price) / (price - low), 2)
                scenario_note = "区间上下沿为本期之前 20 个交易日收盘价最高与最低值。比值以当前价计算，未计费用和滑点。"

    tnx_q = market.get_quote("^TNX")
    tnx_yield = tnx_q.price if (tnx_q and tnx_q.price is not None) else None
    category, summary = _watch_category(
        series,
        drawdown,
        q.r5 if has_price else None,
        q.r20 if has_price else None,
        forward_pe=forward_pe,
        pe_discount_pct=discount,
        reward_risk=ratio,
        tnx_yield=tnx_yield,
    )

    if has_price:
        cues: list[str] = []
        session_prints = _session_print_map(market)
        if sym in session_prints:
            sp_chg, sp_sess = session_prints[sym]
            stage = "盘前" if sp_sess == "PRE_MARKET" else "盘中"
            cues.append(f"{stage} {sp_chg:+.2f}%")
        elif q and q.ahChg is not None and market.live.marketSession == "PRE_MARKET":
            cues.append(f"盘前 {q.ahChg:+.2f}%")
        peer = _OVERSEAS_PEER_MAP.get(sym)
        if peer and market.live.marketSession in ("PRE_MARKET", "OFF_HOURS", "HOLIDAY"):
            pq = market.get_quote(peer[0])
            if pq and pq.chg is not None and pq.tsUtc[:10] == market.fetched_at_utc.date().isoformat():
                cues.append(f"{peer[1]} {pq.chg:+.2f}%")
        if cues:
            summary = f"{summary.rstrip('。')}；最新映射：{'、'.join(cues)}。"

    return ResearchSetup(
        symbol=sym,
        name=series.spec.name,
        mainline=mainline_id,
        category=category,
        category_label=_CATEGORY_LABELS[category],
        summary=summary if has_price else "缺少可用于本轮的近期报价，等待更新。",
        valuation=valuation,
        drawdown_52w_pct=drawdown,
        high_52w=high,
        last_price=price,
        invalidation_price=low,
        target_price=upper,
        reward_risk=ratio,
        as_of=q.tsUtc if q and has_price else None,
        source_url=f"https://finance.yahoo.com/quote/{url_quote(sym, safe='')}/history/",
        evidence_status=(
            "unavailable" if not has_price else
            "available" if drawdown is not None and ratio is not None and (
                valuation.forward_pe_discount_pct is not None or not series.spec.category.startswith("equity_")
            ) else "partial"
        ),
        scenario_note=scenario_note,
    )


def build_setups(mainlines: list[MainlineItem], market: MarketSnapshot) -> list[ResearchSetup]:
    """Cover each mainline and high-signal core equities (e.g. TSM/LMT/ASML/AVGO) up to 10 rows."""
    rosters = [
        (line.id, [series for symbol in _roster(line) if (series := _security_series(market, symbol))])
        for line in mainlines
    ]
    ownership = {line.anchor_sym.upper(): line.id for line in reversed(mainlines)}
    valid_mainline_ids = {line.id for line in mainlines}
    items: list[ResearchSetup] = []
    seen: set[str] = set()

    # Pass 1: Cover the lead security from every mainline (M1..M5) first.
    for mainline_id, roster in rosters:
        if not roster:
            continue
        series = roster[0]
        symbol = series.spec.sym
        if symbol in seen or ownership.get(symbol, mainline_id) != mainline_id:
            continue
        items.append(_setup(series, mainline_id, market))
        seen.add(symbol)
        if len(items) == 10:
            return items

    # Pass 2: Prioritize high-signal single-name equities with valuation coverage (TSM/AVGO/ASML/LMT/etc.).
    for symbol, default_ml in _CORE_SETUP_FALLBACKS:
        if len(items) >= 10:
            return items
        if symbol in seen:
            continue
        series = _security_series(market, symbol)
        if series is None:
            continue
        ml_id = ownership.get(symbol, default_ml if default_ml in valid_mainline_ids else (mainlines[0].id if mainlines else "M1"))
        items.append(_setup(series, ml_id, market))
        seen.add(symbol)

    # Pass 3: Remaining mainline roster depth if slots remain.
    for mainline_id, roster in rosters:
        for series in roster[1:]:
            if len(items) >= 10:
                return items
            symbol = series.spec.sym
            if symbol in seen or ownership.get(symbol, mainline_id) != mainline_id:
                continue
            items.append(_setup(series, mainline_id, market))
            seen.add(symbol)
    return items[:10]


def build_anchor_reviews(
    mainlines: list[MainlineItem], market: MarketSnapshot, governance: GovernanceDecision,
) -> list[AnchorReview]:
    """Flag sustained same-theme outperformance while leaving structural changes to governance."""
    market_date = market.fetched_at_utc.astimezone(TZ_ET).date()
    reviews: list[AnchorReview] = []
    for line in mainlines:
        anchor = _security_series(market, line.anchor_sym.upper())
        if anchor is None:
            continue
        anchor_closes = _dates_and_closes(anchor, market_date)
        anchor_days = sorted(anchor_closes)[-8:]
        if len(anchor_days) < 8 or not _recent_market_date(anchor_days[-1], market_date):
            continue
        best: AnchorReview | None = None
        for symbol in _roster(line)[1:]:
            alternative = _security_series(market, symbol)
            if alternative is None or not _same_market_kind(anchor, alternative):
                continue
            other_closes = _dates_and_closes(alternative, market_date)
            if sorted(other_closes)[-8:] != anchor_days:
                continue
            returns = [
                (
                    100 * (anchor_closes[day] / anchor_closes[base] - 1),
                    100 * (other_closes[day] / other_closes[base] - 1),
                )
                for base, day in zip(anchor_days, anchor_days[5:])
            ]
            anchor_r5, alternative_r5 = returns[-1]
            weak_anchor = anchor_r5 < 0 or (0 < line.flip < line.accel and anchor_closes[anchor_days[-1]] <= line.flip)
            alt_val = market.valuations.get(symbol)
            alt_has_cushion = bool(
                alt_val and alt_val.forward_pe and len(alt_val.historical_forward_pe) >= 3
                and alt_val.forward_pe <= median(alt_val.historical_forward_pe) * 0.85
            )
            alt_better_20d = (
                alternative.quote is not None and anchor.quote is not None
                and alternative.quote.r20 is not None and anchor.quote.r20 is not None
                and alternative.quote.r20 > anchor.quote.r20 + 5.0
            )
            sustained_lead = all(right - left >= 3 for left, right in returns)
            if not weak_anchor or not sustained_lead or (alternative_r5 <= 0 and not (alt_has_cushion and alt_better_20d)):
                continue
            if best is None or alternative_r5 > best.candidate_r5:
                best = AnchorReview(
                    mainline=line.id,
                    anchor_symbol=anchor.spec.sym,
                    candidate_symbol=symbol,
                    anchor_r5=round(anchor_r5, 2),
                    candidate_r5=round(alternative_r5, 2),
                    lead_sessions=len(returns),
                    governance_window=governance.window,
                    structural_change_allowed=governance.allow_structural_rebalance,
                    as_of=anchor_days[-1],
                )
        if best is not None:
            reviews.append(best)
    return reviews
