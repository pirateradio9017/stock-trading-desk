"""Unit and integration tests for M3 research engine (state_machine, analyst, auditor)."""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from pathlib import Path

import pytest

from src.config import CORE_SYMBOLS, CORR_LABELS, SymbolSpec
from src.ingestion import IngestionBundle
from src.ingestion.market_data import (
    MarketSnapshot,
    SymbolSeries,
    _build_corr_matrix,
    _build_linkage_trend,
    _build_rotation_assets,
    _normalize_spark,
)
from src.ingestion.news_aggregator import AggregatedNews, RawWireItem
from src.ingestion.primary_sources import (
    PrimarySignals,
    SecFilingSignal,
    build_holiday_safe_calendar,
    build_macro_proxies,
)
from src.models.schemas import (
    LiveResponse,
    NewsItem,
    NewsResponse,
    QuoteItem,
    StateResponse,
)
from src.research import (
    audit_and_repair_state,
    build_deterministic_draft,
    build_research_context,
    evaluate_governance_window,
    evolve_state,
    load_previous_state,
    run_research_cycle,
    save_state,
    synthesize_research_state,
)
from src.research.analyst import draft_to_state_response


def _make_test_bundle(
    now_utc: datetime,
    *,
    nvda_price: float = 150.0,
    nvda_r5: float = -2.5,
    nvda_r20: float = -18.0,
    itb_r5: float = 5.5,
    itb_r20: float = 11.2,
    vix_price: float = 24.5,
) -> IngestionBundle:
    base_dates = [f"2026-10-{d:02d}" for d in range(1, 32)] + [
        f"2026-11-{d:02d}" for d in range(1, 26)
    ]
    series_map: dict[str, SymbolSeries] = {}
    quotes: list[QuoteItem] = []

    overrides: dict[str, tuple[float, float, float, float]] = {
        "^GSPC": (7180.0, -0.35, -1.8, -6.5),
        "^IXIC": (23550.0, -0.65, -3.2, -10.4),
        "^VIX": (vix_price, -2.1, -4.0, 42.0),
        "^MOVE": (97.5, -1.0, -0.5, 1.2),
        "^TNX": (4.68, -0.9, -3.8, -2.5),
        "BTC-USD": (60800.0, 4.2, 1.2, -18.5),
        "GC=F": (3600.0, 1.1, -1.8, -9.2),
        "SI=F": (41.7, 2.5, -4.5, -17.0),
        "HG=F": (5.45, 0.7, -2.2, -14.0),
        "BZ=F": (91.2, 0.8, -0.6, -18.0),
        "TLT": (82.5, 0.9, 2.6, 1.8),
        "XLP": (92.3, 0.5, 1.4, 6.2),
        "XLE": (60.9, 0.7, -0.5, -9.5),
        "NVDA": (nvda_price, -2.8, nvda_r5, nvda_r20),
        "AVGO": (301.0, -0.2, -12.0, -14.0),
        "AMD": (406.0, -1.5, -19.0, -25.0),
        "LITE": (476.0, -2.1, -13.0, -40.0),
        "SOXX": (128.0, -0.6, -8.5, -22.0),
        "ITB": (98.3, 2.8, itb_r5, itb_r20),
        "AAPL": (355.5, 0.4, 1.2, 3.8),
        "IREN": (12.85, -0.8, -14.0, -48.0),
        "NBIS": (85.0, -2.4, -18.0, -45.0),
        "CRWV": (42.6, -0.1, -10.5, -30.0),
        "COIN": (103.7, 1.3, -5.8, -32.0),
        "MSTR": (80.0, 3.0, -10.2, -40.0),
    }

    for idx, spec in enumerate(CORE_SYMBOLS):
        p, chg, r5, r20 = overrides.get(
            spec.sym,
            (100.0 + idx, 0.25, 0.8, 2.4),
        )
        closes = [round(p * (0.94 + 0.06 * (i / (len(base_dates) - 1))), 2) for i in range(len(base_dates))]
        closes[-1] = p
        q = QuoteItem(
            sym=spec.sym,
            name=spec.name,
            price=p,
            chg=chg,
            r5=r5,
            r20=r20,
            source="yahoo_v8",
            tsUtc="2026-11-26T10:00:00Z",
        )
        quotes.append(q)
        series_map[spec.sym] = SymbolSeries(spec=spec, dates=base_dates, closes=closes, quote=q)

    extra_specs = [
        ("SPY", "标普500 ETF", 715.0, -0.3, -1.7, -6.2),
        ("GLD", "黄金 ETF", 332.0, 0.9, -1.5, -8.8),
        ("HYG", "高收益债 ETF", 76.5, 0.1, -0.2, -1.4),
        ("LQD", "投资级债 ETF", 106.2, 0.4, 1.1, 0.6),
        ("DX-Y.NYB", "美元指数 DXY", 103.6, -0.2, -0.5, 1.2),
        ("XLU", "公用事业 ETF", 43.6, 0.5, 0.2, 2.6),
        ("XLF", "金融 ETF", 52.4, 0.3, -0.8, 1.1),
        ("SMH", "半导体龙头 ETF", 240.0, -0.8, -9.0, -20.0),
        ("ASML", "阿斯麦", 1303.0, -2.4, -6.5, -21.0),
    ]
    for sym, name, p, chg, r5, r20 in extra_specs:
        spec = SymbolSpec(sym, name, "etf")
        closes = [round(p * (0.95 + 0.05 * (i / (len(base_dates) - 1))), 2) for i in range(len(base_dates))]
        closes[-1] = p
        q = QuoteItem(
            sym=sym,
            name=name,
            price=p,
            chg=chg,
            r5=r5,
            r20=r20,
            source="yahoo_v8",
            tsUtc="2026-11-26T10:00:00Z",
        )
        series_map[sym] = SymbolSeries(spec=spec, dates=base_dates, closes=closes, quote=q)

    live = LiveResponse(
        updatedAt="2026/11/26 22:00:00 UTC+8",
        updatedAtEt="11-26 09:00 ET",
        marketSession="HOLIDAY",
        count=len(quotes),
        quotes=quotes,
    )
    market = MarketSnapshot(
        fetched_at_utc=now_utc,
        live=live,
        series_map=series_map,
        spark_labels=[d[5:] for d in base_dates[-11:]],
        symbol_sparks={s: _normalize_spark(v.closes, 11) for s, v in series_map.items()},
        corr_labels=list(CORR_LABELS),
        corr_matrix=_build_corr_matrix(series_map),
        linkage_trend=_build_linkage_trend(series_map, now_utc),
        rotation_assets=_build_rotation_assets(series_map),
    )

    sec_filings = [
        SecFilingSignal(
            sym="NVDA",
            company="NVIDIA CORP",
            cik="0001045810",
            form="8-K",
            filing_date="2026-11-23",
            accession_number="0001045810-26-000090",
            items="5.02,9.01",
            url="https://www.sec.gov/Archives/edgar/data/1045810/000104581026000090/0001045810-26-000090-index.htm",
        ),
        SecFilingSignal(
            sym="MSTR",
            company="STRATEGY INC",
            cik="0001050446",
            form="8-K",
            filing_date="2026-11-23",
            accession_number="0001193125-26-470961",
            items="7.01,8.01",
            url="https://www.sec.gov/Archives/edgar/data/1050446/000119312526470961/0001193125-26-470961-index.htm",
        ),
    ]
    primary = PrimarySignals(
        fetched_at_utc=now_utc,
        sec_filings=sec_filings,
        edgar_stream=[],
        macro_proxies=build_macro_proxies(market, now_utc),
        calendar_events=build_holiday_safe_calendar(now_utc, sec_filings=sec_filings),
        apac_eu_summary={
            "^TWII": {"name": "台湾加权", "price": 39151.8, "chg": 2.5, "r5": 1.89, "r20": -15.56},
            "^GDAXI": {"name": "德国 DAX", "price": 25213.5, "chg": 2.07, "r5": -0.98, "r20": -3.17},
        },
    )

    zh_items = [
        NewsItem(
            event_id="evt001a",
            title="褐皮书与PCE双重信号考验美联储十二月利率定价",
            summary="市场关注美债收益率高位回落对科技估值与防御板块的轮动影响。",
            tag="宏观利率",
            source="华尔街见闻",
            pubDate="2026-11-26 21:30 UTC+8",
            link="https://wallstreetcn.com/livenews/3206490",
        ),
        NewsItem(
            event_id="evt002b",
            title="英伟达提交最新8-K公告并引发半导体链条资金再评估",
            summary="高波动背景下资金审视算力资本开支回报率与光通信交付节奏。",
            tag="M1 算力",
            source="华尔街见闻",
            pubDate="2026-11-26 21:10 UTC+8",
            link="https://wallstreetcn.com/livenews/3206488",
        ),
    ]
    news = AggregatedNews(
        fetched_at_utc=now_utc,
        response=NewsResponse(
            updatedAt="2026/11/26 22:00:00 UTC+8",
            count=len(zh_items),
            items=zh_items,
        ),
        raw_items=[
            RawWireItem(
                event_id="evt003c",
                title="BBVA Weighs SRT to Hedge $500M of Data Center Loans",
                summary="European lender looks to offload data center credit risk.",
                tag="电力基建",
                source="Finviz News",
                tier="L2_AGGREGATOR",
                lang="en",
                pub_dt_utc=now_utc,
                pub_date_cn="2026-11-26 21:00 UTC+8",
                link="https://finviz.com/news.ashx",
                score=85,
            )
        ],
    )
    return IngestionBundle(fetched_at_utc=now_utc, market=market, primary=primary, news=news)


def test_governance_window_rules() -> None:
    # 2026-11-26 is Thanksgiving (US full-day holiday)
    thanksgiving_utc = datetime(2026, 11, 26, 15, 0, tzinfo=timezone.utc)  # 10:00 ET
    d_hol = evaluate_governance_window(thanksgiving_utc, has_prev_state=True)
    assert d_hol.window == "HOLIDAY_LOCK"
    assert d_hol.is_holiday is True
    assert d_hol.allow_structural_rebalance is False

    # Cold start overrides lock so first build always initializes cleanly
    d_cold = evaluate_governance_window(thanksgiving_utc, has_prev_state=False)
    assert d_cold.allow_structural_rebalance is True

    # 2026-11-25 Wednesday 10:30 ET (15:30 UTC) -> INTRADAY_LOCK
    intraday_utc = datetime(2026, 11, 25, 15, 30, tzinfo=timezone.utc)
    d_intra = evaluate_governance_window(intraday_utc, has_prev_state=True)
    assert d_intra.window == "INTRADAY_LOCK"
    assert d_intra.allow_structural_rebalance is False

    # 2026-11-25 Wednesday 16:40 ET (21:40 UTC) -> REBALANCE_WINDOW
    rebal_utc = datetime(2026, 11, 25, 21, 40, tzinfo=timezone.utc)
    d_rebal = evaluate_governance_window(rebal_utc, has_prev_state=True)
    assert d_rebal.window == "REBALANCE_WINDOW"
    assert d_rebal.allow_structural_rebalance is True

    # Explicit force_rebalance overrides lock
    d_force = evaluate_governance_window(intraday_utc, has_prev_state=True, force_rebalance=True)
    assert d_force.allow_structural_rebalance is True


def test_state_machine_intraday_lock_and_rebalance_evolution(tmp_path: Path) -> None:
    # Step 1: Cold start on 2026-11-25 16:40 ET
    t0 = datetime(2026, 11, 25, 21, 40, tzinfo=timezone.utc)
    b0 = _make_test_bundle(t0)
    d0 = evaluate_governance_window(t0, has_prev_state=False)
    draft0 = build_deterministic_draft(b0, None, d0)
    state0 = evolve_state(draft_to_state_response(draft0, b0, d0), None, d0, t0, b0.market)

    assert [m.id for m in state0.mainlines] == ["M1", "M2", "M3", "M4", "M5"]
    assert [c.id for c in state0.candidates] == ["C1", "C2", "C3", "C4", "C5", "C6"]
    assert len(state0.history) == 1
    orig_titles = [m.title for m in state0.mainlines]
    orig_anchors = [m.anchor_sym for m in state0.mainlines]
    orig_scores = [m.score for m in state0.mainlines]

    # Persist & reload roundtrip
    state_file = tmp_path / "state.json"
    save_state(state0, state_file)
    reloaded = load_previous_state(state_file)
    assert reloaded is not None
    assert reloaded.mainlines[0].title == state0.mainlines[0].title

    # Step 2: Intraday lock on 2026-11-27 10:30 ET (15:30 UTC) — try feeding wildly shifted scores & order
    t1 = datetime(2026, 11, 27, 15, 30, tzinfo=timezone.utc)
    b1 = _make_test_bundle(t1, nvda_price=165.0, nvda_r5=12.0, nvda_r20=25.0, itb_r5=-8.0, itb_r20=-12.0)
    d1 = evaluate_governance_window(t1, has_prev_state=True)
    assert d1.allow_structural_rebalance is False
    draft1 = build_deterministic_draft(b1, reloaded, d1)
    state1 = evolve_state(draft_to_state_response(draft1, b1, d1), reloaded, d1, t1, b1.market)

    # Structural skeleton must remain locked, score delta clamped to +/-5
    assert [m.title for m in state1.mainlines] == orig_titles
    assert [m.anchor_sym for m in state1.mainlines] == orig_anchors
    for old_s, new_s in zip(orig_scores, [m.score for m in state1.mainlines]):
        assert abs(new_s - old_s) <= 5
    assert state1.history[0].type == "防抖锁定"

    # Repeating intraday lock on same day replaces top lock entry rather than spamming
    t1_b = datetime(2026, 11, 27, 17, 0, tzinfo=timezone.utc)
    state1_b = evolve_state(draft_to_state_response(draft1, b1, d1), state1, d1, t1_b, b1.market)
    assert len(state1_b.history) == len(state1.history)

    # Step 3: Rebalance window on 2026-11-27 16:40 ET (21:40 UTC) -> allows promotion / demotion & history <= 20
    t2 = datetime(2026, 11, 27, 21, 40, tzinfo=timezone.utc)
    d2 = evaluate_governance_window(t2, has_prev_state=True)
    assert d2.allow_structural_rebalance is True
    # Simulate stale candidate C1 with stale_days=4 advancing one more flat day -> auto demote
    stale_prev = state1_b.model_copy(
        update={
            "candidates": [
                state1_b.candidates[0].model_copy(update={"stale_days": 4, "tier": "watch"}),
                *state1_b.candidates[1:],
            ]
        }
    )
    b2 = _make_test_bundle(t2, nvda_price=168.0, nvda_r5=10.5, nvda_r20=18.0, itb_r5=-7.0, itb_r20=-10.0)
    draft2 = build_deterministic_draft(b2, stale_prev, d2)
    # Force one new anchor to verify promotion logging
    draft2_state = draft_to_state_response(draft2, b2, d2)
    promoted_m5 = draft2_state.mainlines[4].model_copy(
        update={"title": "大型科技现金牛避风港", "anchor_sym": "AAPL", "stocks": "AAPL:苹果, QQQ:纳指100 ETF"}
    )
    draft2_state = draft2_state.model_copy(update={"mainlines": [*draft2_state.mainlines[:4], promoted_m5]})
    state2 = evolve_state(draft2_state, stale_prev, d2, t2, b2.market)

    assert state2.history[0].type == "晋升主线"
    assert state2.mainlines[4].anchor_sym == "AAPL"
    assert state2.mainlines[4].first_seen == "2026-11-27"
    assert len(state2.history) <= 20


def test_auditor_catches_and_repairs_hallucinations() -> None:
    now_utc = datetime(2026, 11, 26, 14, 0, tzinfo=timezone.utc)
    bundle = _make_test_bundle(now_utc)
    decision = evaluate_governance_window(now_utc, has_prev_state=False)
    draft = build_deterministic_draft(bundle, None, decision)
    raw_state = draft_to_state_response(draft, bundle, decision)

    # Inject multiple hallucinations into M1 and history
    broken_m1 = raw_state.mainlines[0].model_copy(
        update={
            "chain": "仅一步无因果",
            "stocks": "NVDA:英伟达",
            "anchor_sym": "NVDA",
            "accel": 99999.0,  # extreme hallucinated price
            "flip": 0.01,
            "spark": [100.0, 101.0],  # wrong length (2 != 11)
            "source_url": "https://hallucinated-nonexistent-domain.example/fake",
            "plain": "TODO 后续补充 Date(2026,10,26)",
        }
    )
    broken_history = [raw_state.history[0].model_copy(update={"lesson": ""})]
    broken_state = raw_state.model_copy(
        update={
            "mainlines": [broken_m1, *raw_state.mainlines[1:]],
            "history": broken_history,
        }
    )

    repaired, report = audit_and_repair_state(broken_state, bundle, engine_mode="gemini", model_used="gemini-2.5-pro")
    assert report.passed is True
    assert len(report.checks) == 6
    assert len(report.repairs) >= 4

    fixed_m1 = repaired.mainlines[0]
    assert len(fixed_m1.spark) == 11
    assert 100.0 < fixed_m1.accel < 200.0
    assert 100.0 < fixed_m1.flip < 200.0
    assert fixed_m1.accel != fixed_m1.flip
    assert "hallucinated" not in fixed_m1.source_url
    assert "Date(" not in fixed_m1.plain
    assert "TODO" not in fixed_m1.plain
    assert repaired.history[0].lesson != ""
    assert "6维自审 6/6 通过" in repaired.meta.self_review


def test_analyst_context_and_mocked_gemini_and_fallback(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    now_utc = datetime(2026, 11, 26, 14, 0, tzinfo=timezone.utc)
    bundle = _make_test_bundle(now_utc)
    decision = evaluate_governance_window(now_utc, has_prev_state=False)

    ctx = build_research_context(bundle, None, decision)
    assert "VERIFIED_URLS" in ctx
    assert "NVDA" in ctx
    assert "SEC" in ctx
    assert "20D相关系数" in ctx

    # 1) Mock successful Gemini structured synthesis
    expected_draft = build_deterministic_draft(bundle, None, decision)
    monkeypatch.setenv("GEMINI_API_KEY", "test-fake-key")
    monkeypatch.setattr(
        "src.research.analyst._call_gemini_sync",
        lambda api_key, model_name, prompt, timeout_sec=35.0: expected_draft,
    )
    res_llm = asyncio.run(
        run_research_cycle(
            bundle,
            state_path=tmp_path / "llm_state.json",
            persist=True,
            use_llm=True,
            models=("gemini-3.8-flash",),
        )
    )
    assert res_llm.engine_mode == "gemini"
    assert res_llm.model_used == "gemini-3.8-flash"
    assert res_llm.audit.passed is True
    assert (tmp_path / "llm_state.json").exists()
    assert isinstance(res_llm.state, StateResponse)

    # 2) Mock Gemini exception -> seamless deterministic fallback
    def _raise_api_err(api_key: str, model_name: str, prompt: str, timeout_sec: float = 35.0):
        raise RuntimeError("429 RESOURCE_EXHAUSTED")

    monkeypatch.setattr("src.research.analyst._call_gemini_sync", _raise_api_err)
    res_fb = asyncio.run(
        synthesize_research_state(
            bundle,
            None,
            decision,
            use_llm=True,
            models=("gemini-3.8-flash", "gemini-3.5-flash-lite"),
        )
    )
    assert res_fb.engine_mode == "deterministic_fallback"
    assert "429" in res_fb.fallback_reason
    assert len(res_fb.draft_state.mainlines) == 5
    assert len(res_fb.draft_state.candidates) == 6
    assert len(res_fb.draft_state.linkage) == 4
    assert len(res_fb.draft_state.macro) == 5
