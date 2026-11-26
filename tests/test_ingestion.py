"""Unit tests for M2 ingestion modules (market_data, primary_sources, news_aggregator)."""

from __future__ import annotations

from datetime import date, datetime, timezone

import pytest

from src.config import CORE_SYMBOLS, CORR_LABELS, SymbolSpec, is_us_trading_day
from src.ingestion.market_data import (
    MarketSnapshot,
    SymbolSeries,
    _build_corr_matrix,
    _build_linkage_trend,
    _normalize_spark,
    _parse_yahoo_chart,
)
from src.ingestion.news_aggregator import (
    RawWireItem,
    _parse_finviz_html,
    _parse_wscn_payload,
    build_chinese_news_response,
    deduplicate_wire_items,
    is_predominantly_chinese,
)
from src.ingestion.primary_sources import (
    SecFilingSignal,
    build_holiday_safe_calendar,
    build_macro_proxies,
)
from src.models.schemas import LiveResponse, QuoteItem


def _make_sample_snapshot(now_utc: datetime) -> MarketSnapshot:
    series_map: dict[str, SymbolSeries] = {}
    quotes: list[QuoteItem] = []

    base_dates = [f"2026-10-{d:02d}" for d in range(1, 32)] + [
        f"2026-11-{d:02d}" for d in range(1, 26)
    ]

    for idx, spec in enumerate(CORE_SYMBOLS):
        closes = [100.0 + (i * 0.4) + (idx % 3) * 0.2 for i in range(len(base_dates))]
        if spec.sym == "^TNX":
            closes = [4.15 + (i % 5) * 0.02 for i in range(len(base_dates))]
        elif spec.sym == "^VIX":
            closes = [15.8 + (i % 4) * 0.25 for i in range(len(base_dates))]
        price = closes[-1]
        q = QuoteItem(
            sym=spec.sym,
            name=spec.name,
            price=round(price, 2),
            chg=round(((idx % 5) - 2) * 0.45, 2),
            r5=round(((idx % 7) - 2) * 0.8, 2),
            r20=round(((idx % 9) - 2) * 1.4, 2),
            source="yahoo_v8",
            tsUtc="2026-11-26T10:00:00Z",
        )
        quotes.append(q)
        series_map[spec.sym] = SymbolSeries(
            spec=spec,
            dates=base_dates,
            closes=closes,
            quote=q,
        )

    for sym, name in (
        ("SPY", "标普500 ETF"),
        ("GLD", "黄金 ETF"),
        ("HYG", "高收益债 ETF"),
        ("LQD", "投资级债 ETF"),
        ("DX-Y.NYB", "美元指数 DXY"),
        ("XLU", "公用事业 ETF"),
    ):
        closes = [90.0 + i * 0.25 for i in range(len(base_dates))]
        spec = SymbolSpec(sym, name, "etf")
        q = QuoteItem(
            sym=sym,
            name=name,
            price=round(closes[-1], 2),
            chg=0.35,
            r5=1.1,
            r20=3.4,
            source="yahoo_v8",
            tsUtc="2026-11-26T10:00:00Z",
        )
        series_map[sym] = SymbolSeries(spec=spec, dates=base_dates, closes=closes, quote=q)

    live = LiveResponse(
        updatedAt="2026/11/26 18:00:00 UTC+8",
        updatedAtEt="11-26 05:00 ET",
        marketSession="HOLIDAY",
        count=len(quotes),
        quotes=quotes,
    )
    return MarketSnapshot(
        fetched_at_utc=now_utc,
        live=live,
        series_map=series_map,
        spark_labels=[d[5:] for d in base_dates[-11:]],
        symbol_sparks={s: _normalize_spark(v.closes, 11) for s, v in series_map.items()},
        corr_labels=list(CORR_LABELS),
        corr_matrix=_build_corr_matrix(series_map),
        linkage_trend=_build_linkage_trend(series_map, now_utc),
        rotation_assets=[],
    )


def test_parse_yahoo_chart_prefers_regular_change_over_chart_prev_close() -> None:
    spec = SymbolSpec("NVDA", "英伟达", "equity_ai")
    # Simulate 25 daily closes ending at 149.30 with chartPreviousClose far away (225.07).
    base_ts = 1793000000
    timestamps = [base_ts + i * 86400 for i in range(25)]
    closes = [140.0 + i * 0.35 for i in range(24)] + [149.30]
    payload = {
        "chart": {
            "result": [
                {
                    "meta": {
                        "regularMarketPrice": 149.30,
                        "chartPreviousClose": 225.07,
                        "regularMarketChangePercent": -3.91,
                        "preMarketPrice": 150.10,
                        "regularMarketTime": timestamps[-1],
                    },
                    "timestamp": timestamps,
                    "indicators": {"quote": [{"close": closes}]},
                }
            ]
        }
    }
    now_utc = datetime(2026, 11, 26, 10, 0, tzinfo=timezone.utc)
    series = _parse_yahoo_chart(spec, payload, market_session="PRE_MARKET", now_utc=now_utc)
    assert series.quote is not None
    assert series.quote.price == 149.30
    assert series.quote.chg == -3.91
    assert series.quote.ahPrice == 150.10
    assert series.quote.ahChg is not None and series.quote.ahChg > 0
    assert series.quote.r5 is not None
    assert series.quote.r20 is not None
    assert len(_normalize_spark(series.closes, 11)) == 11


def test_corr_matrix_and_scissor_trend_invariants() -> None:
    now_utc = datetime(2026, 11, 26, 10, 0, tzinfo=timezone.utc)
    snap = _make_sample_snapshot(now_utc)
    assert len(snap.corr_labels) == 6
    assert len(snap.corr_matrix) == 6
    for i in range(6):
        assert snap.corr_matrix[i][i] == pytest.approx(1.0)
        for j in range(6):
            assert -1.0 <= snap.corr_matrix[i][j] <= 1.0
            assert snap.corr_matrix[i][j] == pytest.approx(snap.corr_matrix[j][i])

    assert len(snap.linkage_trend.labels) == 30
    assert len(snap.linkage_trend.series) == 5
    for s in snap.linkage_trend.series:
        assert len(s.values) == 30
        assert s.values[0] == 0.0


def test_macro_proxies_and_holiday_safe_calendar() -> None:
    # 2026-11-26 is US Thanksgiving Day (Thursday, NYSE closed).
    thanksgiving_utc = datetime(2026, 11, 26, 14, 0, tzinfo=timezone.utc)
    snap = _make_sample_snapshot(thanksgiving_utc)

    macros = build_macro_proxies(snap, thanksgiving_utc)
    assert [m.chain_id for m in macros] == [
        "rate",
        "credit",
        "sentiment",
        "compute",
        "semi_demand",
    ]
    for m in macros:
        assert m.source_url.startswith("http")
        assert 0 <= m.score <= 100

    filings = [
        SecFilingSignal(
            sym="NVDA",
            company="NVIDIA CORP",
            cik="0001045810",
            form="10-Q",
            filing_date="2026-11-20",
            accession_number="0001045810-26-000123",
            items="2.02",
            url="https://www.sec.gov/Archives/edgar/data/1045810/000104581026000123/0001045810-26-000123-index.htm",
        )
    ]
    cal = build_holiday_safe_calendar(thanksgiving_utc, filings)
    assert len(cal) >= 2
    assert any("感恩节" in ev.title or "Thanksgiving" in ev.title for ev in cal)
    for ev in cal:
        ev_date = date.fromisoformat(ev.day)
        assert is_us_trading_day(ev_date), f"Calendar event landed on non-trading day {ev.day}"


def test_wscn_empty_title_splitting_dedup_and_chinese_filter() -> None:
    from src.config import FEED_SOURCES

    now_utc = datetime(2026, 11, 26, 10, 0, tzinfo=timezone.utc)
    payload = {
        "data": {
            "items": [
                {
                    "title": "",
                    "content_text": "英伟达最新季报显示数据中心算力需求保持强劲，光通信供应链同步放量。",
                    "display_time": 1795701945,
                    "uri": "https://wallstreetcn.com/livenews/3206491",
                },
                {
                    "title": "",
                    "content_text": "英伟达最新季报显示数据中心算力需求保持强劲，光通信供应链同步放量!",
                    "display_time": 1795701900,
                    "uri": "https://wallstreetcn.com/livenews/3206490",
                },
            ]
        }
    }
    wscn_items = _parse_wscn_payload(FEED_SOURCES["wscn_us_stock"], payload, now_utc)
    assert len(wscn_items) == 2
    assert all(len(x.title) >= 4 for x in wscn_items)

    en_item = RawWireItem(
        event_id="",
        title="US stocks pause for Thanksgiving holiday while yields edge lower",
        summary="Bond yields moved lower ahead of the holiday.",
        tag="宏观利率",
        source="CNBC Markets",
        tier="L2_WIRE_EN",
        lang="en",
        pub_dt_utc=now_utc,
        pub_date_cn="2026-11-26 18:00 UTC+8",
        link="https://www.cnbc.com/example",
        score=60,
    )
    deduped = deduplicate_wire_items(wscn_items + [en_item])
    # Near-duplicate Chinese stories should collapse into 1 cluster + 1 English wire = 2 total.
    assert len(deduped) == 2
    assert len({x.event_id for x in deduped}) == 2

    news_resp = build_chinese_news_response(deduped, now_utc)
    assert news_resp.count == 1
    assert is_predominantly_chinese(news_resp.items[0].title)
    assert len(news_resp.items[0].summary) >= 4


def test_parse_finviz_html_extracts_valid_links() -> None:
    from src.config import FEED_SOURCES

    now_utc = datetime(2026, 11, 26, 10, 0, tzinfo=timezone.utc)
    html = """
    <html><body>
      <table class="styled-table-new">
        <tr><td><a class="nn-tab-link" href="https://example.com/news/1">Semiconductor stocks rally on strong AI capex guidance</a></td></tr>
        <tr><td><a class="nn-tab-link" href="/quote.ashx?t=NVDA">NVDA</a></td></tr>
      </table>
    </body></html>
    """
    items = _parse_finviz_html(FEED_SOURCES["finviz_news"], html, now_utc)
    assert len(items) == 1
    assert items[0].link == "https://example.com/news/1"


def test_us_desk_news_relevance_prioritizes_market_wires() -> None:
    now_utc = datetime(2026, 11, 26, 10, 0, tzinfo=timezone.utc)
    noise = RawWireItem(
        event_id="noise1111111",
        title="某地发布本地市政公园开放公告",
        summary="本地市政公园周末免费开放。",
        tag="美股快讯",
        source="华尔街见闻",
        tier="L1_WIRE_ZH",
        lang="zh",
        pub_dt_utc=now_utc,
        pub_date_cn="2026-11-26 18:00 UTC+8",
        link="https://wallstreetcn.com/livenews/1",
        score=100,
    )
    us_news = RawWireItem(
        event_id="us2222222222",
        title="美联储会议纪要显示降息节奏仍取决于核心PCE通胀回落",
        summary="市场定价12月利率路径，10Y美债收益率小幅回落。",
        tag="宏观利率",
        source="华尔街见闻",
        tier="L1_WIRE_ZH",
        lang="zh",
        pub_dt_utc=now_utc,
        pub_date_cn="2026-11-26 17:55 UTC+8",
        link="https://wallstreetcn.com/livenews/2",
        score=100,
    )
    resp = build_chinese_news_response([noise, us_news], now_utc, limit=1)
    assert resp.count == 1
    assert resp.items[0].event_id == "us2222222222"
