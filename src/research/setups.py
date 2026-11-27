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


def _security_series(market: MarketSnapshot, symbol: str) -> SymbolSeries | None:
    series = market.series_map.get(symbol)
    if series is None or series.spec.region != "US":
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


def _watch_category(
    series: SymbolSeries, drawdown: float | None, r5: float | None, r20: float | None,
) -> tuple[str, str]:
    if r5 is not None and r20 is not None and r5 <= -5 and r20 <= -15:
        return "avoid", "5 日和 20 日均显著走弱，观察短期跌势是否延续。"
    if series.spec.category in _HIGH_RISK_CATEGORIES:
        return "spec", "所属板块对融资条件或加密波动较敏感，关注回撤和短期动量。"
    if drawdown is not None and drawdown >= 25:
        return "dip", "距 52 周高点已回撤至少 25%，观察动量能否企稳及估值比较是否支持。"
    if series.spec.sym in _DEFENSIVE_REFERENCES:
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
    category, summary = _watch_category(series, drawdown, q.r5 if has_price else None, q.r20 if has_price else None)

    fund_data = market.valuations.get(sym)
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
            low, upper = lower_bound, upper_bound
            ratio = round((upper - price) / (price - low), 2)
            scenario_note = "区间上下沿为本期之前 20 个交易日收盘价最高与最低值。比值以当前价计算，未计费用和滑点。"

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
    """Cover each mainline before filling more rows from its same-theme roster."""
    rosters = [
        (line.id, [series for symbol in _roster(line) if (series := _security_series(market, symbol))])
        for line in mainlines
    ]
    ownership = {line.anchor_sym.upper(): line.id for line in reversed(mainlines)}
    items: list[ResearchSetup] = []
    seen: set[str] = set()
    for depth in range(3):
        for mainline_id, roster in rosters:
            if len(roster) <= depth:
                continue
            series = roster[depth]
            symbol = series.spec.sym
            if symbol in seen or ownership.get(symbol, mainline_id) != mainline_id:
                continue
            items.append(_setup(series, mainline_id, market))
            seen.add(symbol)
            if len(items) == 10:
                return items
    return items


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
            if not weak_anchor or not all(right - left >= 3 for left, right in returns) or alternative_r5 <= 0:
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
