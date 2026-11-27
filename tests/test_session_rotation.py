from datetime import datetime, timedelta, timezone

from src.config import resolve_research_schedule
from src.ingestion.session_rotation import compare_prints, parse_print

NOW = datetime(2026, 11, 27, 11, 15, tzinfo=timezone.utc)


def quote(sym, change, stamp="Nov 27, 2026 6:14 AM ET"):
    return parse_print(sym, {"marketStatus": "Pre-Market", "primaryData": {
        "lastSalePrice": "$100", "lastTradeTimestamp": stamp, "percentageChange": str(change)+"%"}},
        NOW, "PRE_MARKET")


def test_same_session_alignment_and_dynamic_leadership():
    prints = {s: quote(s, v) for s, v in (("SPY", 2), ("COHR", 7), ("LITE", 9), ("ITB", 1), ("XHB", 3))}
    ranks = compare_prints(prints, NOW, "PRE_MARKET").themes
    assert [(r.name, r.excess) for r in ranks] == [("光模块与高速互联", 6), ("住宅建筑与开发", 0)]
    prints["COHR"] = quote("COHR", -8)
    prints["LITE"] = quote("LITE", -7)
    assert compare_prints(prints, NOW, "PRE_MARKET").themes[0].name == "住宅建筑与开发"
    prints["LITE"] = quote("LITE", 9, "Nov 27, 2026 6:02 AM ET")
    assert len(compare_prints(prints, NOW, "PRE_MARKET").themes) == 1
    assert quote("XHB", 8, "Nov 25, 2026 7:59 PM ET") is None
    assert quote("ITB", 9, "Nov 27, 2026 4:01 AM ET") is None
    assert quote("ITB", 9, "Nov 27, 2026 6:18 AM ET") is None
    assert not compare_prints({}, NOW, "PRE_MARKET").themes


def test_extra_runs_respect_holidays_and_early_closes():
    assert resolve_research_schedule("schedule", "38 9 * * 1-5", NOW+timedelta(hours=3, minutes=23)).run
    assert not resolve_research_schedule("schedule", "43 4-8,10-15 * * 1-5", NOW+timedelta(hours=8)).run
    assert not resolve_research_schedule("schedule", "43 4-8,10-15 * * 1-5", NOW-timedelta(days=1)).run


def test_session_print_preserves_price_and_setups_classify_quality_dips_and_rate_traps():
    from types import SimpleNamespace
    from src.config import SymbolSpec
    from src.ingestion.market_data import EquityValuation, SymbolSeries
    from src.models.schemas import LiveResponse, QuoteItem
    from src.research.setups import _setup

    p = quote("TSM", 3.12)
    assert p is not None and p.price == 100.0

    days = [f"2026-10-{d:02d}" for d in range(1, 22)]
    days[-1] = "2026-11-25"
    tsm_closes = [430.0] * 10 + [390.0] * 5 + [360.0] * 4 + [318.0, 342.34]
    tsm_q = QuoteItem(sym="TSM", name="台积电", price=342.34, chg=-1.96, r5=-5.59, r20=-22.11, tsUtc="2026-11-25T21:00:00Z")
    tsm_s = SymbolSeries(spec=SymbolSpec("TSM", "台积电", "equity_ai"), dates=days, closes=tsm_closes, quote=tsm_q, high_52w=487.47)

    mstr_closes = [145.0] * 10 + [110.0] * 5 + [90.0] * 4 + [78.0, 79.96]
    mstr_q = QuoteItem(sym="MSTR", name="Strategy", price=79.96, chg=3.1, r5=-11.47, r20=-44.92, tsUtc="2026-11-25T21:00:00Z")
    mstr_s = SymbolSeries(spec=SymbolSpec("MSTR", "Strategy", "equity_crypto"), dates=days, closes=mstr_closes, quote=mstr_q, high_52w=198.4)

    itb_closes = [89.0] * 10 + [92.0] * 5 + [95.0] * 4 + [100.3, 98.34]
    itb_q = QuoteItem(sym="ITB", name="房屋建筑 ETF", price=98.34, chg=2.91, r5=6.13, r20=10.51, tsUtc="2026-11-25T21:00:00Z")
    itb_s = SymbolSeries(spec=SymbolSpec("ITB", "房屋建筑 ETF", "sector_etf"), dates=days, closes=itb_closes, quote=itb_q, high_52w=115.26)

    tnx_q = QuoteItem(sym="^TNX", name="10Y 美债", price=4.85, chg=-1.14, r5=-4.3, r20=-3.9, tsUtc="2026-11-25T20:00:00Z")
    tw_q = QuoteItem(sym="2330.TW", name="台积电台股", price=2180.0, chg=1.63, r5=9.0, r20=-8.0, tsUtc="2026-11-27T05:30:00Z")

    rot = compare_prints({"SPY": quote("SPY", 1.78), "TSM": quote("TSM", 3.12), "ASML": quote("ASML", 2.82)}, NOW, "PRE_MARKET")
    quotes = {"TSM": tsm_q, "MSTR": mstr_q, "ITB": itb_q, "^TNX": tnx_q, "2330.TW": tw_q}
    live = LiveResponse(session_rotation=rot, updatedAt="2026/11/27 19:15:00 UTC+8", updatedAtEt="11-27 06:15 ET",
                        marketSession="PRE_MARKET", count=len(quotes), quotes=list(quotes.values()))
    market = SimpleNamespace(
        fetched_at_utc=NOW,
        live=live,
        valuations={
            "TSM": EquityValuation(forward_pe=15.4, historical_forward_pe=(24.0, 24.5, 25.0), source_url="https://finance.yahoo.com/quote/TSM/key-statistics/"),
            "MSTR": EquityValuation(forward_pe=1.67, historical_forward_pe=(2.4, 2.5, 2.6), source_url="https://finance.yahoo.com/quote/MSTR/key-statistics/"),
        },
        get_quote=lambda sym: quotes.get(sym),
    )

    tsm_setup = _setup(tsm_s, "M1", market)
    assert tsm_setup.category == "dip"
    assert "台积电台股 +1.63%" in tsm_setup.summary and "盘前 +3.12%" in tsm_setup.summary

    mstr_setup = _setup(mstr_s, "M4", market)
    assert mstr_setup.category == "avoid"
    assert mstr_setup.reward_risk is None  # Guard degenerate knife-edge 20D low denominator

    itb_setup = _setup(itb_s, "M2", market)
    assert itb_setup.category == "avoid"
    assert "4.85%" in itb_setup.summary

