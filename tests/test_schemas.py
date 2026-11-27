"""Unit tests for M1 Pydantic v2 contracts and configuration catalog."""

from __future__ import annotations

import inspect
from datetime import date, datetime, timezone

import pytest
from pydantic import ValidationError

from src import config
from src.models import (
    AnalyticsData,
    CalendarEvent,
    CandidateItem,
    HealthResponse,
    HistoryEntry,
    LinkageStory,
    LinkageTrend,
    LinkageTrendSeries,
    LiveResponse,
    MacroDimItem,
    MainlineItem,
    MetaInfo,
    NewsItem,
    NewsResponse,
    QuoteItem,
    RotationAsset,
    StateResponse,
)


def _sample_meta() -> dict:
    return {
        "updated_at": "11-26 05:30 ET · 感恩节休市防抖动锁定",
        "updated_at_short": "11-26 05:30 ET",
        "badge": "高位震荡 · 算力与防御双线并行",
        "headline": "英伟达财报后算力链高位分化，防御与黄金承接轮动资金",
        "plain_guide": "主线未破但高弹性标的波动加大，关注关键支撑与催化窗口。",
        "next_gate": "11-27 半日交易量能与下周 ISM 制造业数据",
        "regime": "Risk-On 分化",
        "stance": "核心持有 + 逢低轮动",
        "vix_note": "VIX 15.2 · 处于低波区间",
        "tnx_note": "10Y 4.28% · 利率压力缓和",
        "btc_note": "BTC 95,400 · 高位整固",
        "self_review": "6维自检通过：信源/时区/日历/数值/链条/反幻觉全部核验",
    }


def _sample_mainline(**overrides) -> dict:
    base = {
        "id": "M1",
        "title": "AI 算力与光互连基建",
        "action": "核心持有",
        "action_tone": "sage",
        "first_seen": "2026-11-18",
        "score": 97,
        "status": "[First_seen: 2026-11-18 | Score: 97] 订单可见度高",
        "chain": "云厂资本开支 | GPU与定制ASIC | 800G光模块",
        "stocks": "NVDA:英伟达, AVGO:博通, LITE:Lumentum",
        "anchor_sym": "NVDA",
        "accel": 152.0,
        "flip": 145.0,
        "spark": [100.0, 100.5, 101.2, 101.8, 102.0, 101.5, 102.4, 103.0, 103.6, 104.1, 104.8],
        "plain": "下游云厂 CapEx 仍在扩张周期，光通信订单能见度延续至明年。",
        "summary": "算力链条维持全市场最强基本面支撑，回调不破关键均线维持多头结构。",
        "watch_points": "观察 NVDA 145 美元支撑及光模块板块量能变化。",
        "source_url": "https://www.sec.gov/edgar/browse/?CIK=0001045810",
    }
    base.update(overrides)
    return base


def _sample_candidate(**overrides) -> dict:
    base = {
        "id": "C1",
        "tier": "upgrade",
        "tier_label": "准备升级",
        "tone": "sage",
        "name": "数据中心电力与液冷",
        "linked_to": "M1",
        "stocks": "IREN:IREN, NBIS:Nebius",
        "r5": 4.2,
        "r20": 11.5,
        "plain": "算力机房供电瓶颈推动电力配套与高密度供电标的走强。",
        "rule": "5日相对收益连续3日居前且放量突破20日平台",
        "first_seen": "2026-11-20",
        "stale_days": 0,
        "history_log": "2026-11-20 纳入替补池",
    }
    base.update(overrides)
    return base


def _sample_macro(**overrides) -> dict:
    base = {
        "chain_id": "rate",
        "title": "利率与债市波动",
        "state": "顺风",
        "wind": "顺风",
        "score": 68,
        "feature": "10Y 美债收益率回落至 4.3% 下方",
        "concern": "若通胀粘性反弹则长端利率上行压力再起",
        "plain": "美债波动率回落叠加长端利率企稳，对成长股估值构成阶段性支撑。",
        "m1_label": "10Y 美债",
        "m1_val": "4.28%",
        "m1_date": "2026-11-25",
        "m1_sub": "5日 -11bp",
        "m2_label": "MOVE 指数",
        "m2_val": "94.2",
        "m2_date": "2026-11-25",
        "m2_sub": "处于平稳区间",
        "source_url": "https://finance.yahoo.com/quote/%5ETNX",
    }
    base.update(overrides)
    return base


def _sample_analytics() -> dict:
    labels_30 = [f"11-{i:02d}" for i in range(1, 31)]
    zeros_30 = [float(i) * 0.1 for i in range(30)]
    return {
        "sparkLabels": ["D-10", "D-9", "D-8", "D-7", "D-6", "D-5", "D-4", "D-3", "D-2", "D-1", "Now"],
        "rotationAssets": [
            {"sym": "NVDA", "name": "英伟达", "group": "M1", "r5": -2.4, "r20": 8.1},
            {"sym": "XLP", "name": "必需消费 ETF", "group": "M4", "r5": 1.6, "r20": 3.4},
        ],
        "corrLabels": ["SPY", "QQQ", "TLT", "GLD", "NVDA", "BTC"],
        "corrMatrix": [
            [1.0, 0.94, 0.15, 0.08, 0.72, 0.35],
            [0.94, 1.0, 0.12, 0.04, 0.81, 0.41],
            [0.15, 0.12, 1.0, 0.28, 0.05, -0.06],
            [0.08, 0.04, 0.28, 1.0, 0.02, 0.19],
            [0.72, 0.81, 0.05, 0.02, 1.0, 0.29],
            [0.35, 0.41, -0.06, 0.19, 0.29, 1.0],
        ],
        "linkageTrend": {
            "labels": labels_30,
            "series": [
                {"sym": "XLP", "label": "防御消费 XLP", "values": zeros_30},
                {"sym": "TLT", "label": "20Y美债 TLT", "values": zeros_30},
                {"sym": "GC=F", "label": "黄金 GC=F", "values": zeros_30},
                {"sym": "^GSPC", "label": "标普500", "values": zeros_30},
                {"sym": "NVDA", "label": "英伟达 NVDA", "values": zeros_30},
            ],
        },
    }


def test_health_response_round_trip() -> None:
    payload = {
        "ok": True,
        "version": "2.0.0",
        "serverTimeUtc": "2026-11-26T10:30:00Z",
        "stateUpdatedAt": "2026-11-26T10:25:00Z",
        "newsCount": 18,
        "quotesCount": 27,
    }
    model = HealthResponse.model_validate(payload)
    assert model.model_dump() == payload
    reloaded = HealthResponse.model_validate_json(model.model_dump_json())
    assert reloaded == model

    with pytest.raises(ValidationError):
        HealthResponse.model_validate({**payload, "newsCount": -1})


def test_live_response_round_trip_and_session_validation() -> None:
    payload = {
        "updatedAt": "2026/11/26 18:30:00 UTC+8",
        "updatedAtEt": "11-26 05:30 ET",
        "marketSession": "HOLIDAY",
        "count": 2,
        "quotes": [
            {
                "sym": "NVDA",
                "name": "英伟达",
                "price": 149.30,
                "chg": -3.91,
                "ahPrice": None,
                "ahChg": None,
                "r5": -2.4,
                "r20": 8.1,
                "source": "yahoo_v8",
                "tsUtc": "2026-11-26T10:30:00Z",
            },
            {
                "sym": "^GSPC",
                "name": "标普500",
                "price": 5998.74,
                "chg": 0.25,
                "ahPrice": None,
                "ahChg": None,
                "r5": 1.1,
                "r20": 3.8,
                "source": "yahoo_v8",
                "tsUtc": "2026-11-26T10:30:00Z",
            },
        ],
    }
    model = LiveResponse.model_validate(payload)
    assert model.model_dump() == payload
    assert LiveResponse.model_validate_json(model.model_dump_json()) == model

    with pytest.raises(ValidationError):
        LiveResponse.model_validate({**payload, "marketSession": "WEEKEND"})

    with pytest.raises(ValidationError):
        LiveResponse.model_validate({**payload, "count": 1})


def test_state_response_full_round_trip() -> None:
    payload = {
        "serverSyncedAt": "2026/11/26 18:30:00 UTC+8",
        "meta": _sample_meta(),
        "mainlines": [_sample_mainline()],
        "candidates": [_sample_candidate()],
        "linkage": [
            {
                "pair": "美债收益率 vs 科技权重",
                "corr": "-0.62",
                "tone": "sage",
                "why": "长端利率下行释放估值弹性",
                "signal": "关注 10Y 4.20% 关口",
            }
        ],
        "macro": [_sample_macro()],
        "calendar": [
            {
                "day": "2026-11-27",
                "time": "21:30 UTC+8",
                "title": "感恩节次日美股提前休市",
                "tag": "全市场流动性",
                "tone": "ochre",
                "desc": "NYSE 于 13:00 ET 提前收盘，成交清淡。",
                "source_url": "https://www.nyse.com/markets/hours-calendars",
            }
        ],
        "history": [
            {
                "time": "2026-11-26 18:00 UTC+8",
                "type": "防抖锁定",
                "tone": "slate",
                "title": "感恩节休市维持昨日主线结构",
                "detail": "休市日不触发调仓，维持既有 M1–M5 权重。",
                "lesson": "休市与盘中防抖动可避免无效换手。",
            }
        ],
        "analytics": _sample_analytics(),
    }
    model = StateResponse.model_validate(payload)
    assert model.meta.market_brief == ""
    payload["meta"]["market_brief"] = ""
    payload["mainlines"][0]["reasoning"] = None
    payload.update(setups=[], anchor_reviews=[])
    assert model.model_dump() == payload
    assert StateResponse.model_validate_json(model.model_dump_json()) == model


def test_mainline_directional_accel_and_flip() -> None:
    bullish = MainlineItem.model_validate(_sample_mainline(accel=152.0, flip=145.0))
    assert bullish.accel > bullish.flip

    inverse = MainlineItem.model_validate(
        _sample_mainline(id="M2", anchor_sym="TLT", accel=80.5, flip=82.8)
    )
    assert inverse.accel < inverse.flip

    for bad_patch in (
        {"id": "M6"},
        {"score": 101},
        {"score": -1},
        {"action_tone": "red"},
    ):
        with pytest.raises(ValidationError):
            MainlineItem.model_validate(_sample_mainline(**bad_patch))


def test_strict_iso_date_rejects_gviz_and_invalid_dates() -> None:
    with pytest.raises(ValidationError):
        MainlineItem.model_validate(_sample_mainline(first_seen="Date(2026,10,26)"))
    with pytest.raises(ValidationError):
        CandidateItem.model_validate(_sample_candidate(first_seen="Date(2026,10,26)"))
    with pytest.raises(ValidationError):
        CalendarEvent.model_validate(
            {
                "day": "Date(2026,10,26)",
                "time": "21:30 UTC+8",
                "title": "Test",
                "tag": "M1",
                "tone": "ochre",
                "desc": "Test desc",
            }
        )
    with pytest.raises(ValidationError):
        HistoryEntry.model_validate(
            {
                "time": "Date(2026,10,26,4,9,0)",
                "type": "防抖锁定",
                "tone": "slate",
                "title": "Test",
                "detail": "Detail",
            }
        )
    with pytest.raises(ValidationError):
        MetaInfo.model_validate({**_sample_meta(), "updated_at": "Date(2026,10,26,4,9,0)"})

    for bad_date in ("2026-02-30", "2026/11/26", "11-26"):
        with pytest.raises(ValidationError):
            MainlineItem.model_validate(_sample_mainline(first_seen=bad_date))

    parsed = MainlineItem.model_validate(_sample_mainline(first_seen=date(2026, 11, 26)))
    assert parsed.first_seen == "2026-11-26"


def test_macro_chain_id_enum_validation() -> None:
    for chain_id in ("rate", "credit", "sentiment", "compute", "semi_demand"):
        item = MacroDimItem.model_validate(_sample_macro(chain_id=chain_id))
        assert item.chain_id == chain_id

    for invalid_id in ("利率与债市波动", "unknown"):
        with pytest.raises(ValidationError):
            MacroDimItem.model_validate(_sample_macro(chain_id=invalid_id))


def test_spark_length_and_finite_validation() -> None:
    eleven = MainlineItem.model_validate(_sample_mainline())
    assert len(eleven.spark) == 11

    two = MainlineItem.model_validate(_sample_mainline(spark=[100.0, 101.0]))
    assert len(two.spark) == 2

    for bad_spark in ([], [100.0], [100.0, float("nan")], [100.0, float("inf")]):
        with pytest.raises(ValidationError):
            MainlineItem.model_validate(_sample_mainline(spark=bad_spark))


def test_corr_matrix_and_linkage_trend_dimension_validation() -> None:
    valid = _sample_analytics()
    assert len(AnalyticsData.model_validate(valid).corrMatrix) == 6

    with pytest.raises(ValidationError):
        AnalyticsData.model_validate({**valid, "corrMatrix": valid["corrMatrix"][:5]})

    ragged = [list(row) for row in valid["corrMatrix"]]
    ragged[0] = ragged[0][:5]
    with pytest.raises(ValidationError):
        AnalyticsData.model_validate({**valid, "corrMatrix": ragged})

    out_of_range = [list(row) for row in valid["corrMatrix"]]
    out_of_range[0][1] = 1.5
    with pytest.raises(ValidationError):
        AnalyticsData.model_validate({**valid, "corrMatrix": out_of_range})

    with pytest.raises(ValidationError):
        LinkageTrend.model_validate(
            {
                "labels": ["11-01", "11-02", "11-03"],
                "series": [
                    {"sym": "XLP", "label": "防御消费 XLP", "values": [0.0, 0.2]},
                ],
            }
        )


def test_candidate_and_meta_bounds() -> None:
    with pytest.raises(ValidationError):
        CandidateItem.model_validate(_sample_candidate(stale_days=-1))
    with pytest.raises(ValidationError):
        CandidateItem.model_validate(_sample_candidate(tier="hold"))
    with pytest.raises(ValidationError):
        MetaInfo.model_validate({**_sample_meta(), "updated_at": "x" * 121})
    with pytest.raises(ValidationError):
        MetaInfo.model_validate({**_sample_meta(), "updated_at_short": "y" * 41})


def test_news_response_empty_and_populated() -> None:
    empty = NewsResponse.model_validate(
        {"updatedAt": "2026/11/26 18:30:00 UTC+8", "count": 0, "items": []}
    )
    assert empty.count == 0
    assert empty.items == []

    items = [
        {
            "event_id": "sha1-000000000001",
            "title": "英伟达 Blackwell 机柜出货节奏跟踪",
            "summary": "供应链反馈四季度出货稳步爬坡，光互连需求维持景气。",
            "tag": "M1 算力",
            "source": "华尔街见闻",
            "pubDate": "2026-11-26 18:15 UTC+8",
            "link": "https://wallstreetcn.com/articles/100001",
        },
        {
            "event_id": "sha1-000000000002",
            "title": "美联储纪要显示降息节奏保持数据依赖",
            "summary": "利率曲线短端稳健，市场继续定价渐进式宽松。",
            "tag": "宏观利率",
            "source": "CNBC",
            "pubDate": "2026-11-26 17:45 UTC+8",
            "link": "https://www.cnbc.com/2026/11/26/fed-minutes.html",
        },
    ]
    populated = NewsResponse.model_validate(
        {"updatedAt": "2026/11/26 18:30:00 UTC+8", "count": 2, "items": items}
    )
    assert NewsResponse.model_validate_json(populated.model_dump_json()) == populated

    with pytest.raises(ValidationError):
        NewsResponse.model_validate(
            {"updatedAt": "2026/11/26 18:30:00 UTC+8", "count": 1, "items": items}
        )

    with pytest.raises(ValidationError):
        NewsItem.model_validate({**items[0], "link": "javascript:alert(1)"})


def test_config_symbols_feeds_sessions_and_no_deepseek() -> None:
    expected_core = {
        "^GSPC",
        "^IXIC",
        "^VIX",
        "^MOVE",
        "^TNX",
        "BTC-USD",
        "GC=F",
        "SI=F",
        "HG=F",
        "BZ=F",
        "JPY=X",
        "QQQ",
        "TLT",
        "XLP",
        "XLE",
        "NVDA",
        "AVGO",
        "AMD",
        "LITE",
        "SOXX",
        "ITB",
        "AAPL",
        "IREN",
        "NBIS",
        "CRWV",
        "COIN",
        "MSTR",
    }
    expected_core.update({"TSM", "LMT"})
    assert len(config.CORE_SYMBOLS) == 29
    assert {s.sym for s in config.CORE_SYMBOLS} == expected_core
    assert len(config.APAC_SYMBOLS) == 5
    assert len(config.EU_SYMBOLS) == 3
    assert len(config.CREDIT_MACRO_SYMBOLS) == 7
    assert len(config.AUXILIARY_BENCHMARK_SYMBOLS) == 2
    assert len(config.CORR_SYMBOLS) == 6
    assert len(config.CORR_LABELS) == 6
    assert len(config.SCISSOR_SERIES) == 5

    assert len(config.FEED_SOURCES) == 16
    assert "sec_edgar_submissions" in config.FEED_SOURCES
    assert config.FEED_SOURCES["sec_edgar_submissions"].requires_sec_ua is True
    assert "sec_edgar_current_rss" in config.FEED_SOURCES
    assert config.FEED_SOURCES["sec_edgar_current_rss"].requires_sec_ua is True
    assert "wscn_us_stock" in config.FEED_SOURCES
    assert "wscn_global" in config.FEED_SOURCES

    assert config.is_us_market_holiday(date(2026, 11, 26)) is True
    assert config.is_us_market_holiday(date(2026, 11, 27)) is False
    assert config.is_us_market_holiday(date(2026, 4, 3)) is True
    assert config.is_us_market_holiday(date(2026, 7, 3)) is True
    assert config.is_us_trading_day(date(2026, 11, 26)) is False
    assert config.is_us_trading_day(date(2026, 11, 27)) is True

    # 2026-11-26 is Thanksgiving (HOLIDAY)
    thanksgiving_noon = datetime(2026, 11, 26, 12, 0, tzinfo=config.TZ_ET)
    assert config.get_market_session(thanksgiving_noon) == "HOLIDAY"
    assert config.get_research_window(thanksgiving_noon) == "HOLIDAY_LOCK"

    # 2026-11-25 is Wednesday (normal trading day, EST UTC-5)
    assert (
        config.get_market_session(datetime(2026, 11, 25, 8, 0, tzinfo=config.TZ_ET))
        == "PRE_MARKET"
    )
    assert (
        config.get_market_session(datetime(2026, 11, 25, 11, 0, tzinfo=config.TZ_ET))
        == "INTRADAY"
    )
    assert (
        config.get_market_session(datetime(2026, 11, 25, 16, 35, tzinfo=config.TZ_ET))
        == "AFTER_HOURS"
    )
    assert (
        config.get_market_session(datetime(2026, 11, 25, 21, 0, tzinfo=config.TZ_ET))
        == "OFF_HOURS"
    )
    assert (
        config.get_market_session(datetime(2026, 11, 28, 12, 0, tzinfo=config.TZ_ET))
        == "OFF_HOURS"
    )

    assert (
        config.get_research_window(datetime(2026, 11, 25, 11, 0, tzinfo=config.TZ_ET))
        == "INTRADAY_LOCK"
    )
    assert (
        config.get_research_window(datetime(2026, 11, 25, 16, 35, tzinfo=config.TZ_ET))
        == "REBALANCE_WINDOW"
    )

    src_text = inspect.getsource(config)
    assert "DEEPSEEK" not in src_text.upper()
    assert "DEEPSEEK_API_KEY" not in config.ENV_VAR_NAMES

    # Verify timezone formatting helpers work reliably
    sample_utc = datetime(2026, 11, 26, 10, 30, 0, tzinfo=timezone.utc)
    assert config.format_cn_timestamp(sample_utc) == "2026/11/26 18:30:00 UTC+8"
    assert config.format_et_short(sample_utc) == "11-26 05:30 ET"
    assert config.format_utc_iso(sample_utc) == "2026-11-26T10:30:00Z"


def test_load_settings_and_holiday_edge_cases(monkeypatch: pytest.MonkeyPatch) -> None:
    for key in config.ENV_VAR_NAMES:
        monkeypatch.delenv(key, raising=False)
    defaults = config.load_settings()
    assert defaults.port == 8787
    assert defaults.log_level == "INFO"
    assert defaults.data_dir == config.DEFAULT_DATA_DIR

    monkeypatch.setenv("DESK_PORT", "9090")
    monkeypatch.setenv("DESK_LOG_LEVEL", "debug")
    monkeypatch.setenv("DESK_DATA_DIR", "E:/tmp/desk_data")
    custom = config.load_settings()
    assert custom.port == 9090
    assert custom.log_level == "DEBUG"
    assert str(custom.data_dir).endswith("desk_data")

    # Per NYSE Rule 7.2, when Jan 1 falls on a Saturday (2022-01-01, 2028-01-01),
    # the preceding Friday Dec 31 (end of monthly/yearly accounting period) is NOT a holiday.
    assert config.is_us_market_holiday(date(2021, 12, 31)) is False
    assert config.is_us_market_holiday(date(2027, 12, 31)) is False
    assert config.is_us_trading_day(date(2021, 12, 31)) is True
    assert config.is_us_trading_day(date(2027, 12, 31)) is True
    assert config.is_us_market_holiday(date(2023, 1, 2)) is True
    assert config.is_us_market_holiday(date(2026, 7, 3)) is True
    assert config.is_us_market_holiday(date(2021, 6, 18)) is False
    assert config.is_us_market_holiday(date(2022, 6, 20)) is True


def test_linkage_trend_empty_labels_rejects_nonempty_series() -> None:
    empty = LinkageTrend.model_validate({"labels": [], "series": []})
    assert empty.labels == []
    assert empty.series == []

    empty_series_vals = LinkageTrend.model_validate(
        {"labels": [], "series": [{"sym": "XLP", "label": "防御消费 XLP", "values": []}]}
    )
    assert len(empty_series_vals.series) == 1

    with pytest.raises(ValidationError):
        LinkageTrend.model_validate(
            {
                "labels": [],
                "series": [
                    {"sym": "XLP", "label": "X", "values": [0.0, 1.0]},
                    {"sym": "TLT", "label": "T", "values": [0.0]},
                ],
            }
        )

    with pytest.raises(ValidationError):
        LinkageTrend.model_validate(
            {
                "labels": [],
                "series": [{"sym": "XLP", "label": "X", "values": [0.0]}],
            }
        )


def test_strict_url_validation_rejects_hostless_and_control_chars() -> None:
    bad_urls = (
        "https://:8080/a",
        "https://user:pass@/a",
        "https:///path",
        "https://example.com/a\nb",
        "https://example.com/a b",
        "https://example.com/\x07",
    )
    sample_news = {
        "event_id": "sha1-000000000001",
        "title": "测试快讯",
        "tag": "M1 算力",
        "source": "华尔街见闻",
        "pubDate": "2026-11-26 18:15 UTC+8",
    }
    for bad_url in bad_urls:
        with pytest.raises(ValidationError):
            NewsItem.model_validate({**sample_news, "link": bad_url})
        with pytest.raises(ValidationError):
            MainlineItem.model_validate(_sample_mainline(source_url=bad_url))

    valid_localhost = NewsItem.model_validate(
        {**sample_news, "link": "https://localhost:8787/api/health"}
    )
    assert valid_localhost.link == "https://localhost:8787/api/health"


def test_strict_timestamp_and_case_insensitive_gviz() -> None:
    # HealthResponse: CleanTimestampStr on serverTimeUtc and optional stateUpdatedAt
    ok_health = HealthResponse.model_validate(
        {"serverTimeUtc": "2026-11-26T10:30:00Z", "stateUpdatedAt": None}
    )
    assert ok_health.stateUpdatedAt is None

    for bad_health_ts in (
        "   ",
        "",
        "not-a-timestamp",
        "2026-02-29 10:00",
        "2026-11-26 99:99",
        "2026-11-26 date(2026,10,26)",
    ):
        with pytest.raises(ValidationError):
            HealthResponse.model_validate({"serverTimeUtc": bad_health_ts})
        with pytest.raises(ValidationError):
            HealthResponse.model_validate(
                {"serverTimeUtc": "2026-11-26T10:30:00Z", "stateUpdatedAt": bad_health_ts}
            )

    # HistoryEntry, NewsItem, QuoteItem: reject invalid HH:MM:SS, invalid dates, lowercase date(
    for bad_ts in (
        "2026-11-26 99:99",
        "2026-11-26 24:00",
        "2026-11-26 10:60",
        "2026-11-26 10:30:99",
        "2026-02-29 10:00",
        "not-a-timestamp",
        "2026-11-26 date(2026,10,26)",
    ):
        with pytest.raises(ValidationError):
            HistoryEntry.model_validate(
                {
                    "time": bad_ts,
                    "type": "防抖锁定",
                    "tone": "slate",
                    "title": "Test",
                    "detail": "Detail",
                }
            )
        with pytest.raises(ValidationError):
            NewsItem.model_validate(
                {
                    "event_id": "1",
                    "title": "t",
                    "tag": "m",
                    "source": "s",
                    "pubDate": bad_ts,
                    "link": "https://example.com/a",
                }
            )
        with pytest.raises(ValidationError):
            QuoteItem.model_validate({"sym": "NVDA", "tsUtc": bad_ts})

    # SyncedTimestampStr: LiveResponse.updatedAt, StateResponse.serverSyncedAt, NewsResponse.updatedAt
    ok_state_iso = StateResponse.model_validate(
        {
            "serverSyncedAt": "2026-11-26T10:30:00Z",
            "meta": _sample_meta(),
            "analytics": _sample_analytics(),
        }
    )
    assert ok_state_iso.serverSyncedAt == "2026-11-26T10:30:00Z"

    for bad_synced in (
        "not-a-timestamp",
        "2026-02-29 10:00",
        "2026/02/29 10:00:00 UTC+8",
        "2026/11/26 99:99:00 UTC+8",
        "2026/11/26 date(2026,10,26)",
    ):
        with pytest.raises(ValidationError):
            LiveResponse.model_validate(
                {
                    "updatedAt": bad_synced,
                    "updatedAtEt": "11-26 05:30 ET",
                    "marketSession": "HOLIDAY",
                    "count": 0,
                    "quotes": [],
                }
            )
        with pytest.raises(ValidationError):
            StateResponse.model_validate(
                {
                    "serverSyncedAt": bad_synced,
                    "meta": _sample_meta(),
                    "analytics": _sample_analytics(),
                }
            )
        with pytest.raises(ValidationError):
            NewsResponse.model_validate({"updatedAt": bad_synced, "count": 0, "items": []})

    # EtTimestampStr: LiveResponse.updatedAtEt, MetaInfo.updated_at, MetaInfo.updated_at_short
    for bad_et in (
        "not-a-timestamp",
        "2026-02-29 10:00",
        "02-30 05:30 ET",
        "13-01 05:30 ET",
        "11-26 99:99 ET",
        "11-26 05:30 ET · date(2026,10,26)",
    ):
        with pytest.raises(ValidationError):
            LiveResponse.model_validate(
                {
                    "updatedAt": "2026/11/26 18:30:00 UTC+8",
                    "updatedAtEt": bad_et,
                    "marketSession": "HOLIDAY",
                    "count": 0,
                    "quotes": [],
                }
            )
        with pytest.raises(ValidationError):
            MetaInfo.model_validate({**_sample_meta(), "updated_at": bad_et})
        with pytest.raises(ValidationError):
            MetaInfo.model_validate({**_sample_meta(), "updated_at_short": bad_et})

    with pytest.raises(ValidationError):
        MainlineItem.model_validate(_sample_mainline(first_seen="date(2026,10,26)"))


def test_directional_ruler_bool_rejection_corr_message_and_naive_utc() -> None:
    with pytest.raises(ValidationError, match="accel and flip must differ"):
        MainlineItem.model_validate(_sample_mainline(accel=150.0, flip=150.0))

    with pytest.raises(ValidationError):
        MainlineItem.model_validate(_sample_mainline(spark=[True, False]))
    with pytest.raises(ValidationError):
        MainlineItem.model_validate(_sample_mainline(accel=True))
    with pytest.raises(ValidationError):
        QuoteItem.model_validate(
            {"sym": "NVDA", "price": False, "tsUtc": "2026-11-26T10:30:00Z"}
        )

    valid = _sample_analytics()
    out_of_range = [list(row) for row in valid["corrMatrix"]]
    out_of_range[0][1] = 1.0002
    with pytest.raises(ValidationError, match=r"\[-1\.0001, 1\.0001\]"):
        AnalyticsData.model_validate({**valid, "corrMatrix": out_of_range})

    # Naive datetime is explicitly interpreted as UTC across config helpers
    naive_dt = datetime(2026, 11, 25, 11, 0)
    assert config.get_market_session(naive_dt) == "PRE_MARKET"
    assert config.format_et_short(naive_dt) == "11-25 06:00 ET"
    assert config.format_utc_iso(naive_dt) == "2026-11-25T11:00:00Z"
    for fn in (
        config._to_utc,
        config.get_market_session,
        config.get_research_window,
        config.format_cn_timestamp,
        config.format_et_short,
        config.format_utc_iso,
    ):
        doc = (fn.__doc__ or "").lower()
        assert "naive" in doc and "utc" in doc


def test_url_and_quote_boundary_validation() -> None:
    with pytest.raises(ValidationError):
        MainlineItem.model_validate(_sample_mainline(source_url="ftp://example.com/file"))
    with pytest.raises(ValidationError):
        MacroDimItem.model_validate(_sample_macro(source_url="javascript:void(0)"))
    with pytest.raises(ValidationError):
        CalendarEvent.model_validate(
            {
                "day": "2026-11-27",
                "time": "21:30 UTC+8",
                "title": "Test",
                "tag": "M1",
                "tone": "ochre",
                "desc": "Desc",
                "source_url": "not-a-url",
            }
        )
    empty_url = MainlineItem.model_validate(_sample_mainline(source_url=""))
    assert empty_url.source_url == ""

    with pytest.raises(ValidationError):
        QuoteItem.model_validate(
            {
                "sym": "NVDA",
                "price": -1.0,
                "tsUtc": "2026-11-26T10:30:00Z",
            }
        )
    with pytest.raises(ValidationError):
        QuoteItem.model_validate(
            {
                "sym": "NVDA",
                "r5": float("nan"),
                "tsUtc": "2026-11-26T10:30:00Z",
            }
        )

