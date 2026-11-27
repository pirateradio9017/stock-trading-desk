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

