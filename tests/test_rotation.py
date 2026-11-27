from datetime import date, datetime, timedelta, timezone
from types import SimpleNamespace

from src.config import is_us_trading_day
from src.research.rotation import ranked_baskets, rotation_candidates


def test_baskets_rotate_on_aligned_sessions_and_skip_missing_components():
    # Friday premarket after Thanksgiving: Wednesday is the latest US session.
    day = date(2026, 11, 25)
    days = []
    while len(days) < 21:
        if is_us_trading_day(day):
            days.insert(0, day.isoformat())
        day -= timedelta(days=1)
    def series(rate):
        return SimpleNamespace(dates=days, closes=[100 * (1 + rate) ** i for i in range(21)])

    market = SimpleNamespace(fetched_at_utc=datetime(2026, 11, 27, 11, tzinfo=timezone.utc),
                             series_map={s: series(0) for s in ("SPY", "ITB", "XHB", "LITE", "COHR")})
    for leaders, laggards, expected in [(("ITB", "XHB"), ("LITE", "COHR"), "住宅建筑与开发"),
                                        (("LITE", "COHR"), ("ITB", "XHB"), "光模块与高速互联")]:
        market.series_map.update({s: series(.01) for s in leaders})
        market.series_map.update({s: series(-.01) for s in laggards})
        rows = ranked_baskets(market)
        candidates = rotation_candidates(rows, [], [], "2026-11-27")
        assert candidates[0].name == expected and candidates[0].tier == "upgrade"
        assert candidates[0].as_of == "2026-11-25"
        assert candidates[1].tier == "demote"
    market.series_map["COHR"].closes.pop()
    assert [r["name"] for r in ranked_baskets(market)] == ["住宅建筑与开发"]
    market.series_map["SPY"].closes.pop()
    assert not ranked_baskets(market)
