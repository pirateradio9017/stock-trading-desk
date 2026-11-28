"""Regression cases for the independent reliability review."""
import asyncio
from datetime import datetime, timezone
from unittest.mock import patch

import pytest
from test_research import _make_test_bundle
from src.config import get_market_session, get_research_window
from src.research import run_research_cycle
from src.research.analyst import build_deterministic_draft, draft_to_state_response, _ensure_rotation_assets
from src.research.auditor import audit_and_repair_state, _is_trusted_url, AuditReport
from src.research.state_machine import evaluate_governance_window, load_previous_state, _match_mainline
from src.ingestion.primary_sources import build_holiday_safe_calendar
from src.ingestion.market_data import _build_linkage_trend


def sample():
    b = _make_test_bundle(datetime(2026, 11, 27, 18, 40, tzinfo=timezone.utc))
    g = evaluate_governance_window(b.fetched_at_utc, has_prev_state=False)
    s = draft_to_state_response(build_deterministic_draft(b, None, g), b, g)
    return b, s


@pytest.mark.parametrize('hour,minute,session,window', [
    (17, 0, 'INTRADAY', 'INTRADAY_LOCK'),
    (18, 0, 'AFTER_HOURS', 'POST_CLOSE_SETTLE'),
    (18, 40, 'AFTER_HOURS', 'REBALANCE_WINDOW'),
    (22, 0, 'OFF_HOURS', 'OFF_HOURS'),
])
def test_early_close(hour, minute, session, window):
    t = datetime(2026, 11, 27, hour, minute, tzinfo=timezone.utc)
    assert get_market_session(t) == session
    assert get_research_window(t) == window


def test_corrupt_state_does_not_become_cold_start(tmp_path):
    p = tmp_path / 'state.json'
    p.write_text('{broken', encoding='utf-8')
    with pytest.raises(ValueError):
        load_previous_state(p)
    assert load_previous_state(tmp_path / 'missing.json') is None


def test_failed_audit_never_overwrites_accepted_state(tmp_path):
    b, s = sample()
    p = tmp_path / 'state.json'
    p.write_text(s.model_dump_json(), encoding='utf-8')
    before = p.read_bytes()
    with patch('src.research.audit_and_repair_state', return_value=(s, AuditReport(passed=False, summary='regression rejection'))):
        with pytest.raises(ValueError, match='rejected publication'):
            asyncio.run(run_research_cycle(b, state_path=p, use_llm=False))
    assert p.read_bytes() == before


def test_trusted_domain_does_not_verify_invented_document():
    assert not _is_trusted_url('https://www.sec.gov/Archives/invented.htm', set())


def test_asymmetric_matrix_repaired_from_observations():
    b, s = sample()
    s.analytics.corrMatrix[0][1] = .99
    s.analytics.corrMatrix[1][0] = -.99
    fixed, report = audit_and_repair_state(s, b)
    assert fixed.analytics.corrMatrix == b.market.corr_matrix
    assert report.passed


def test_slot_and_shared_ticker_do_not_transfer_research_identity():
    _, s = sample()
    target = s.mainlines[0]
    unrelated = target.model_copy(update={'anchor_sym': 'UNRELATED', 'score': 98})
    assert _match_mainline(target, [unrelated]) is None


def test_calendar_does_not_invent_release_dates():
    events = build_holiday_safe_calendar(datetime(2026, 11, 26, 15, tzinfo=timezone.utc), [])
    assert not any('PCE' in e.title or 'CPI' in e.title or '非农' in e.title for e in events)
    assert any('研究待办' in e.title for e in events)


def test_missing_market_history_is_not_fabricated():
    b, _ = sample()
    b.market.rotation_assets = []
    b.market.live.quotes = []
    assert _ensure_rotation_assets(b.market) == []
    assert _build_linkage_trend({}, b.fetched_at_utc).series == []


def test_research_summary_separates_evidence_and_hypothesis():
    _, s = sample()
    for m in s.mainlines:
        assert '【变化】' in m.summary and '【传导假设】' in m.summary
        assert '【反证】' in m.watch_points and '【下一验证】' in m.watch_points

def test_ci_restores_durable_state_before_ingestion(tmp_path, monkeypatch):
    from types import SimpleNamespace
    import httpx
    import src.pipeline as pipeline
    b, s = sample()
    monkeypatch.setattr(pipeline, 'load_settings', lambda: SimpleNamespace(
        data_dir=tmp_path, cf_account_id='account', cf_api_token='fake-test-token'))
    class Client:
        async def __aenter__(self): return self
        async def __aexit__(self, *args): pass
        async def get(self, url, **kwargs):
            return httpx.Response(200, json=s.model_dump(mode='json'), request=httpx.Request('GET', url))
    monkeypatch.setattr(pipeline.httpx, 'AsyncClient', lambda **kwargs: Client())
    async def stop_after_restore(**kwargs):
        restored = load_previous_state(tmp_path / 'state.json')
        assert restored == s
        raise RuntimeError('restore verified before ingestion')
    monkeypatch.setattr(pipeline, 'run_ingestion', stop_after_restore)
    with pytest.raises(RuntimeError, match='restore verified'):
        asyncio.run(pipeline.run_pipeline(data_dir=tmp_path, publish_kv=True))


def test_remote_state_error_does_not_allow_cold_start(tmp_path, monkeypatch):
    from types import SimpleNamespace
    from unittest.mock import AsyncMock
    import httpx
    import src.pipeline as pipeline
    monkeypatch.setattr(pipeline, 'load_settings', lambda: SimpleNamespace(
        data_dir=tmp_path, cf_account_id='account', cf_api_token='fake-test-token'))
    class Client:
        async def __aenter__(self): return self
        async def __aexit__(self, *args): pass
        async def get(self, url, **kwargs):
            return httpx.Response(503, request=httpx.Request('GET', url))
    monkeypatch.setattr(pipeline.httpx, 'AsyncClient', lambda **kwargs: Client())
    ingestion = AsyncMock()
    monkeypatch.setattr(pipeline, 'run_ingestion', ingestion)
    with pytest.raises(httpx.HTTPStatusError):
        asyncio.run(pipeline.run_pipeline(data_dir=tmp_path, publish_kv=True))
    ingestion.assert_not_called()

def test_deepseek_is_primary_and_result_is_validated(tmp_path, monkeypatch):
    import src.research.analyst as analyst
    b, _ = sample()
    g = evaluate_governance_window(b.fetched_at_utc, has_prev_state=False)
    draft = build_deterministic_draft(b, None, g)
    monkeypatch.setenv('DEEPSEEK_API_KEY', 'test-only')
    monkeypatch.setenv('GEMINI_API_KEY', 'test-only')
    calls = []
    def deepseek(*args):
        calls.append('deepseek')
        return draft
    def gemini(*args):
        raise AssertionError('Should not call fallback after successful primary')
    monkeypatch.setattr(analyst, '_call_deepseek_sync', deepseek)
    monkeypatch.setattr(analyst, '_call_gemini_sync', gemini)
    result = asyncio.run(run_research_cycle(b, state_path=tmp_path/'state.json', use_llm=True))
    assert result.engine_mode == 'deepseek'
    assert calls == ['deepseek']
    assert 'deepseek' in result.state.meta.self_review


def test_deepseek_auth_error_falls_back_without_secret_leak(monkeypatch):
    import src.research.analyst as analyst
    b, _ = sample()
    g = evaluate_governance_window(b.fetched_at_utc, has_prev_state=False)
    monkeypatch.setenv('DEEPSEEK_API_KEY', 'test-only')
    def fail(*args): raise RuntimeError('401 private-credential-do-not-log')
    monkeypatch.setattr(analyst, '_call_deepseek_sync', fail)
    result = asyncio.run(analyst.synthesize_research_state(b,None,g))
    assert result.engine_mode == 'deterministic_fallback'
    assert '401' in result.fallback_reason
    assert 'private-credential' not in result.fallback_reason


def test_deepseek_invalid_output_retries_with_feedback(monkeypatch):
    from src.research import analyst
    b, _ = sample()
    g = evaluate_governance_window(b.fetched_at_utc, has_prev_state=False)
    draft = build_deterministic_draft(b, None, g)
    allowed = analyst._collect_context_urls(b)[0][1]
    for mainline in draft.mainlines:
        mainline.source_url = allowed
    prompts = []
    def fake_call(key, model, prompt, timeout):
        prompts.append(prompt)
        if len(prompts) == 1:
            raise ValueError('DeepSeek response incomplete: length')
        return draft
    monkeypatch.setenv('DEEPSEEK_API_KEY', 'test-key')
    monkeypatch.setenv('DESK_LLM_PROVIDER', 'deepseek')
    monkeypatch.setattr(analyst, '_call_deepseek_sync', fake_call)
    result = asyncio.run(analyst.synthesize_research_state(b, None, g))
    assert result.engine_mode == 'deepseek'
    assert len(prompts) == 2
    assert 'DeepSeek response incomplete: length' in prompts[1]


def test_model_diagnostics_do_not_expose_validation_input():
    from src.research.analyst import ResearchDraft, _safe_model_error
    from pydantic import ValidationError
    try:
        ResearchDraft.model_validate({'meta': 'secret-model-response'})
    except ValidationError as exc:
        message = _safe_model_error(exc)
    assert 'Schema validation:' in message
    assert 'secret-model-response' not in message
    assert _safe_model_error(RuntimeError('secret-key')) == 'RuntimeError'


def test_deepseek_failure_is_bounded_and_keeps_old_research(monkeypatch, tmp_path):
    from src.research import analyst
    b, state = sample()
    path = tmp_path / 'state.json'
    path.write_text(state.model_dump_json(), encoding='utf-8')
    before = path.read_bytes()
    calls = []
    def fail(*args):
        calls.append(1)
        raise ValueError('DeepSeek response incomplete: length')
    monkeypatch.setenv('DEEPSEEK_API_KEY', 'test-key')
    monkeypatch.setenv('DESK_LLM_PROVIDER', 'deepseek')
    monkeypatch.setattr(analyst, '_call_deepseek_sync', fail)
    with pytest.raises(RuntimeError, match='preserving previous accepted research'):
        asyncio.run(run_research_cycle(b, state_path=path))
    assert len(calls) == 3
    assert path.read_bytes() == before


def test_pairwise_corr_not_collapsed_by_single_shifted_symbol():
    from src.ingestion.market_data import SymbolSeries, _build_corr_matrix
    b, _ = sample()
    sm = dict(b.market.series_map)
    btc = sm['BTC-USD']
    sm['BTC-USD'] = SymbolSeries(spec=btc.spec, dates=btc.dates[:10], closes=btc.closes[:10], quote=btc.quote)
    for eq in ('SPY', 'QQQ', 'TLT', 'GLD', 'NVDA'):
        es = sm[eq]
        sm[eq] = SymbolSeries(spec=es.spec, dates=es.dates[5:], closes=es.closes[5:], quote=es.quote)
    mat = _build_corr_matrix(sm)
    # SPY-QQQ share >20 aligned dates and should not be collapsed to 0.0 by BTC-USD's short window.
    assert mat[0][1] != 0.0
    assert mat[0][1] == mat[1][0]


def test_news_word_boundaries_priority_backfill_and_ordering():
    from datetime import timedelta
    from src.ingestion.news_aggregator import RawWireItem, _classify_tag, build_chinese_news_response
    assert _classify_tag('智利铜矿工会Copper Workers Federation呼吁恢复铜冶炼') == '能源大宗'
    assert _classify_tag('法国Caisse基础设施部门暂停发债') != 'M1 算力'
    assert _classify_tag('比特币升破6万美元关口') == '加密联动'
    now = datetime(2026, 11, 27, 4, 0, tzinfo=timezone.utc)
    rel = [
        RawWireItem(f'r{i}', f'美股英伟达财报跟踪测试标题{i}', '摘要内容足够长', 'M1 算力', '华尔街见闻', 'L1_WIRE_ZH', 'zh',
                    now - timedelta(minutes=i * 5), f'2026-11-27 11:{55 - i:02d} UTC+8', f'https://wallstreetcn.com/r{i}', 100)
        for i in range(12)
    ]
    noise = [
        RawWireItem(f'n{i}', f'本地公园赏秋摄影活动通知{i}', '社区公园活动介绍', '美股快讯', '华尔街见闻', 'L1_WIRE_ZH', 'zh',
                    now - timedelta(minutes=i), f'2026-11-27 11:5{i} UTC+8', f'https://wallstreetcn.com/n{i}', 100)
        for i in range(5)
    ]
    resp = build_chinese_news_response(noise + rel, now, limit=25)
    assert resp.count == 12
    assert all('公园赏秋' not in item.title for item in resp.items)
    dates = [item.pubDate for item in resp.items]
    assert dates == sorted(dates, reverse=True)


def test_early_close_calendar_and_fred_macro_formatting():
    from src.ingestion.primary_sources import build_macro_proxies
    b, _ = sample()
    thx = datetime(2026, 11, 26, 15, 0, tzinfo=timezone.utc)
    cal = build_holiday_safe_calendar(thx, [])
    assert any('半日市' in e.title and '13:00 ET' in e.time for e in cal)
    assert any('FOMC' in e.title for e in cal)
    macros = build_macro_proxies(b.market, thx, fred_metrics={
        'BAMLH0A0HYM2': ('2026-11-24', 3.72),
        'BAMLH0A0HYM2EY': ('2026-11-24', 8.44),
        'MORTGAGE30US': ('2026-11-25', 7.21),
    })
    credit = next(m for m in macros if m.chain_id == 'credit')
    assert credit.m1_label == 'FRED 高收益债 OAS'
    assert credit.m1_val == '3.72%'
    assert '8.44%' in credit.m1_sub
    for m in macros:
        assert not m.m1_val.endswith('$') and not m.m2_val.endswith('$')


def test_locked_window_preserves_reasoning_updates_action_and_idempotent_audit():
    from datetime import timedelta
    from src.research.state_machine import evolve_state
    b, s = sample()
    dt_lock = datetime(2026, 11, 26, 16, 0, tzinfo=timezone.utc)
    dec = evaluate_governance_window(dt_lock)
    prev_m0 = s.mainlines[0].model_copy(update={
        'action': '顺势核心跟踪', 'action_tone': 'sage',
        'source_url': 'https://wallstreetcn.com/livenews/expired-old-123',
    })
    prev_c0 = s.candidates[0].model_copy(update={
        'tier': 'demote', 'tier_label': '弱化/淘汰',
        'history_log': '初始化建档，因能源电力基建抗跌性强纳入升级候选',
    })
    prev = s.model_copy(update={
        'mainlines': [prev_m0] + list(s.mainlines[1:]),
        'candidates': [prev_c0] + list(s.candidates[1:]),
    })
    # Same-anchor update refreshes action/action_tone; mismatched anchor preserves reasoning.
    draft_same = s.model_copy(update={
        'mainlines': [s.mainlines[0].model_copy(update={'action': '控仓等企稳', 'action_tone': 'terra'})] + list(s.mainlines[1:])
    })
    ev_same = evolve_state(draft_same, prev, dec, dt_lock + timedelta(hours=2), b.market)
    assert ev_same.mainlines[0].action == '控仓等企稳'
    assert ev_same.mainlines[0].action_tone == 'terra'
    assert '纳入升级候选' not in ev_same.candidates[0].history_log

    draft_diff = s.model_copy(update={
        'mainlines': [m.model_copy(update={'anchor_sym': 'AAPL'}) for m in s.mainlines]
    })
    cur = prev
    for step in range(3):
        ev = evolve_state(draft_diff, cur, dec, dt_lock + timedelta(hours=2 * (step + 1)), b.market)
        assert ev.mainlines[0].reasoning is not None
        cur, rep = audit_and_repair_state(ev, b, engine_mode='deepseek', model_used='deepseek-flash', prev_state=None)
        assert rep.passed
        assert cur.mainlines[0].summary.count('【原引用未匹配本轮采集') <= 1


def test_asml_included_in_setups_and_dim6_tolerates_two_missing_scissor_series():
    from src.config import SymbolSpec
    from src.ingestion.market_data import SymbolSeries
    from src.research.setups import _security_series, build_setups
    b, s = sample()
    asml_old = b.market.series_map['ASML']
    b.market.series_map['ASML'] = SymbolSeries(
        spec=SymbolSpec('ASML', '阿斯麦', 'equity_semi', region='EU'),
        dates=asml_old.dates, closes=asml_old.closes, quote=asml_old.quote, high_52w=1450.0,
    )
    assert _security_series(b.market, 'ASML') is not None
    setups = build_setups(s.mainlines, b.market)
    syms = {item.symbol for item in setups}
    assert 'TSM' in syms or 'NVDA' in syms
    trimmed_lt = s.analytics.linkageTrend.model_copy(update={'series': s.analytics.linkageTrend.series[:3]})
    trimmed_state = s.model_copy(update={'analytics': s.analytics.model_copy(update={'linkageTrend': trimmed_lt})})
    _, rep = audit_and_repair_state(trimmed_state, b, engine_mode='deepseek', model_used='deepseek-flash')
    assert rep.passed


def test_resolve_research_schedule_cadence_and_delay_catchup():
    from src.config import (
        SCHEDULE_ACTIVE_ODD_CRON,
        SCHEDULE_BASELINE_CRON,
        SCHEDULE_REBALANCE_CRON,
        TZ_ET,
        resolve_research_schedule,
    )

    # Manual dispatch always runs without catchup flag.
    d_manual = resolve_research_schedule('workflow_dispatch', '', datetime(2026, 11, 26, 15, 0, tzinfo=timezone.utc))
    assert d_manual.run is True and d_manual.catchup_rebalance is False

    # Baseline 2h cron always runs even on holiday/weekend or delayed across an odd UTC hour.
    for dt in (
        datetime(2026, 11, 26, 15, 55, tzinfo=timezone.utc),  # Thanksgiving delayed into odd hour
        datetime(2026, 11, 28, 3, 40, tzinfo=timezone.utc),   # Weekend delayed into odd hour
    ):
        dec = resolve_research_schedule('schedule', SCHEDULE_BASELINE_CRON, dt)
        assert dec.run is True and dec.catchup_rebalance is False

    # Active odd-hour cron runs during US PRE_MARKET/INTRADAY/AFTER_HOURS and skips OFF_HOURS/HOLIDAY.
    assert resolve_research_schedule('schedule', SCHEDULE_ACTIVE_ODD_CRON, datetime(2026, 11, 25, 8, 13, tzinfo=TZ_ET)).run is True
    assert resolve_research_schedule('schedule', SCHEDULE_ACTIVE_ODD_CRON, datetime(2026, 11, 25, 11, 13, tzinfo=TZ_ET)).run is True
    assert resolve_research_schedule('schedule', SCHEDULE_ACTIVE_ODD_CRON, datetime(2026, 11, 25, 18, 13, tzinfo=TZ_ET)).run is True
    assert resolve_research_schedule('schedule', SCHEDULE_ACTIVE_ODD_CRON, datetime(2026, 11, 25, 22, 13, tzinfo=TZ_ET)).run is False
    assert resolve_research_schedule('schedule', SCHEDULE_ACTIVE_ODD_CRON, datetime(2026, 11, 26, 11, 13, tzinfo=TZ_ET)).run is False

    # Rebalance cron on normal close day (2026-11-25, close 16:00 ET): tolerates 95-min GH cron delay.
    assert resolve_research_schedule('schedule', SCHEDULE_REBALANCE_CRON, datetime(2026, 11, 25, 13, 32, tzinfo=TZ_ET)).run is False
    on_time = resolve_research_schedule('schedule', SCHEDULE_REBALANCE_CRON, datetime(2026, 11, 25, 16, 32, tzinfo=TZ_ET))
    assert on_time.run is True and on_time.catchup_rebalance is False
    delayed_95m = resolve_research_schedule('schedule', SCHEDULE_REBALANCE_CRON, datetime(2026, 11, 25, 18, 7, tzinfo=TZ_ET))
    assert delayed_95m.run is True and delayed_95m.catchup_rebalance is True
    assert resolve_research_schedule('schedule', SCHEDULE_REBALANCE_CRON, datetime(2026, 11, 25, 18, 50, tzinfo=TZ_ET)).run is False

    # Rebalance cron on early close day (2026-11-27, close 13:00 ET): 13:32 catches up to 15:44, 16:32 skips.
    early_on_time = resolve_research_schedule('schedule', SCHEDULE_REBALANCE_CRON, datetime(2026, 11, 27, 13, 32, tzinfo=TZ_ET))
    assert early_on_time.run is True and early_on_time.catchup_rebalance is False
    early_delayed = resolve_research_schedule('schedule', SCHEDULE_REBALANCE_CRON, datetime(2026, 11, 27, 15, 7, tzinfo=TZ_ET))
    assert early_delayed.run is True and early_delayed.catchup_rebalance is True
    assert resolve_research_schedule('schedule', SCHEDULE_REBALANCE_CRON, datetime(2026, 11, 27, 16, 32, tzinfo=TZ_ET)).run is False

    # Holiday never runs rebalance cron.
    assert resolve_research_schedule('schedule', SCHEDULE_REBALANCE_CRON, datetime(2026, 11, 26, 16, 32, tzinfo=TZ_ET)).run is False


def test_kv_publish_retries_and_fallback_live_news_on_research_failure(tmp_path, monkeypatch):
    from types import SimpleNamespace
    import httpx
    import src.pipeline as pipeline

    b, s = sample()
    attempts: dict[str, int] = {}

    class FlakyKvClient:
        async def __aenter__(self): return self
        async def __aexit__(self, *args): pass
        async def put(self, url, **kwargs):
            key = url.rsplit('/', 1)[-1]
            attempts[key] = attempts.get(key, 0) + 1
            if key == 'news' and attempts[key] == 1:
                return httpx.Response(503, text='transient', request=httpx.Request('PUT', url))
            return httpx.Response(200, json={'success': True}, request=httpx.Request('PUT', url))

    monkeypatch.setattr(pipeline.httpx, 'AsyncClient', lambda **kwargs: FlakyKvClient())
    res = asyncio.run(pipeline.publish_to_cloudflare_kv(
        state=s, live=b.live_response, news=b.news_response,
        account_id='acct', api_token='tok', retry_delays=(0.01, 0.01),
    ))
    assert res == {'state': True, 'live': True, 'news': True}
    assert attempts['news'] == 2

    # Persistent rejection must raise loudly (zero silent write drops).
    class RejectKvClient:
        async def __aenter__(self): return self
        async def __aexit__(self, *args): pass
        async def put(self, url, **kwargs):
            return httpx.Response(200, json={'success': False}, request=httpx.Request('PUT', url))

    monkeypatch.setattr(pipeline.httpx, 'AsyncClient', lambda **kwargs: RejectKvClient())
    with pytest.raises(RuntimeError, match='rejected'):
        asyncio.run(pipeline.publish_to_cloudflare_kv(
            state=s, live=b.live_response, news=b.news_response,
            account_id='acct', api_token='tok', retry_delays=(0.01,),
        ))

    # When run_research_cycle fails during publish_kv=True, fresh live+news still push while state stays untouched.
    pushed_keys: list[str] = []

    class RestoreAndFallbackClient:
        async def __aenter__(self): return self
        async def __aexit__(self, *args): pass
        async def get(self, url, **kwargs):
            return httpx.Response(200, json=s.model_dump(mode='json'), request=httpx.Request('GET', url))
        async def put(self, url, **kwargs):
            pushed_keys.append(url.rsplit('/', 1)[-1])
            return httpx.Response(200, json={'success': True}, request=httpx.Request('PUT', url))

    monkeypatch.setattr(pipeline, 'load_settings', lambda: SimpleNamespace(
        data_dir=tmp_path, cf_account_id='acct', cf_api_token='tok'))
    monkeypatch.setattr(pipeline.httpx, 'AsyncClient', lambda **kwargs: RestoreAndFallbackClient())
    async def fake_ingest(**kwargs): return b
    async def fail_research(*args, **kwargs): raise RuntimeError('Model unavailable; preserving previous accepted research')
    monkeypatch.setattr(pipeline, 'run_ingestion', fake_ingest)
    monkeypatch.setattr(pipeline, 'run_research_cycle', fail_research)

    with pytest.raises(RuntimeError, match='Model unavailable'):
        asyncio.run(pipeline.run_pipeline(data_dir=tmp_path, publish_kv=True, now_utc=b.fetched_at_utc))
    assert set(pushed_keys) == {'live', 'news'}
    assert 'state' not in pushed_keys
    assert (tmp_path / 'live.json').is_file() and (tmp_path / 'news.json').is_file()


def test_catchup_rebalance_idempotent_guard():
    from src.models.schemas import HistoryEntry
    from src.pipeline import _already_rebalanced_after_close

    _, s = sample()
    # Early-close day 2026-11-27 (close 13:00 ET): 13:35 ET = 18:35 UTC = 2026-11-28 02:35 UTC+8
    now_delayed = datetime(2026, 11, 27, 20, 5, tzinfo=timezone.utc)  # 15:05 ET
    locked_only = s.model_copy(update={'history': [
        HistoryEntry(time='2026-11-28 01:15 UTC+8', type='防抖锁定', tone='slate', title='t', detail='d'),
    ]})
    assert _already_rebalanced_after_close(locked_only, now_delayed) is False

    already_done = s.model_copy(update={'history': [
        HistoryEntry(time='2026-11-28 02:35 UTC+8', type='主线复盘', tone='ochre', title='t', detail='d'),
    ]})
    assert _already_rebalanced_after_close(already_done, now_delayed) is True


def test_screener_url_preserves_scrids_and_yfinance_fallback_is_bounded(monkeypatch):
    import time as time_mod
    import httpx
    from src.config import FEED_SOURCES, SymbolSpec
    import src.ingestion.market_data as md

    merged = md._screener_request_url(FEED_SOURCES['yahoo_screener_gainers'].url, count=8)
    assert merged.params.get('scrIds') == 'day_gainers'
    assert merged.params.get('count') == '8'

    class FailingClient:
        async def get(self, url, **kwargs):
            return httpx.Response(503, request=httpx.Request('GET', str(url)))

    received_timeouts: list[float] = []

    def slow_yf(spec, *, timeout_sec=6.0):
        received_timeouts.append(timeout_sec)
        time_mod.sleep(0.55)
        raise AssertionError('should have timed out before completing')

    monkeypatch.setattr(md, '_yfinance_fallback_sync', slow_yf)

    async def _run():
        t0 = time_mod.monotonic()
        res = await md._fetch_single_symbol(
            FailingClient(),
            asyncio.Semaphore(2),
            SymbolSpec('NVDA', '英伟达', 'equity_ai'),
            market_session='INTRADAY',
            now_utc=datetime(2026, 11, 27, 16, 0, tzinfo=timezone.utc),
            yf_sem=asyncio.Semaphore(2),
            req_timeout_sec=0.5,
            yf_timeout_sec=0.1,
        )
        return res, time_mod.monotonic() - t0

    series, elapsed = asyncio.run(_run())
    assert series.quote is not None and series.quote.source == 'unavailable'
    assert received_timeouts == [0.1]
    assert elapsed < 0.75


def test_pipeline_workflow_yaml_and_stdlib_schedule_step():
    from pathlib import Path
    import yaml
    from src.config import (
        SCHEDULE_ACTIVE_HALF_HOUR_CRON,
        SCHEDULE_ACTIVE_ODD_CRON,
        SCHEDULE_BASELINE_CRON,
        SCHEDULE_CROSS_MARKET_CRON,
        SCHEDULE_REBALANCE_CRON,
        SCHEDULE_SESSION_20M_CRON,
        resolve_research_schedule,
    )

    wf_path = Path(__file__).resolve().parent.parent / '.github' / 'workflows' / 'pipeline.yml'
    wf = yaml.safe_load(wf_path.read_text(encoding='utf-8'))
    on_cfg = wf.get('on') or wf.get(True)
    # Scheduled GitHub Actions triggers are retired now that VPS daemon runs 24/7.
    assert 'schedule' not in on_cfg
    assert 'workflow_dispatch' in on_cfg
    cm_dec = resolve_research_schedule('schedule', SCHEDULE_CROSS_MARKET_CRON, datetime(2026, 11, 27, 6, 40, tzinfo=timezone.utc))
    assert cm_dec.run is True and cm_dec.reason == 'cross_market_close_open'

    steps = wf['jobs']['run-pipeline']['steps']
    slot_step = next(st for st in steps if st.get('id') == 'research-slot')
    assert 'resolve_research_schedule' in slot_step['run']
    run_step = next(st for st in steps if 'Run research pipeline' in st.get('name', ''))
    assert run_step['env']['DESK_LLM_PROVIDER'] == 'auto'
    assert '--catchup-rebalance' in run_step['run']


def test_cross_timezone_quote_freshness_and_noise_filter():
    from src.ingestion.news_aggregator import RawWireItem, _classify_tag, select_research_wires
    from src.models.schemas import QuoteItem
    from src.research.analyst import build_research_context

    # Friday 2026-11-27 08:35 UTC = 16:35 UTC+8 = 03:35 ET (OFF_HOURS before US pre-market)
    now_utc = datetime(2026, 11, 27, 8, 35, tzinfo=timezone.utc)
    b = _make_test_bundle(now_utc)
    # Simulate stale Wednesday close for US NVDA/ASML vs fresh Friday APAC/EU/futures quotes.
    stale_ts = '2026-11-25T21:00:00Z'
    fresh_ts = '2026-11-27T08:30:00Z'
    for q in b.market.live.quotes:
        if q.sym in ('NVDA', 'ASML', 'TSM'):
            q.tsUtc = stale_ts
    extra_quotes = [
        QuoteItem(sym='ES=F', name='标普500期货', price=7329.75, chg=1.86, r5=2.1, r20=-4.0, source='yahoo_v8', tsUtc=fresh_ts),
        QuoteItem(sym='NQ=F', name='纳指100期货', price=26793.5, chg=2.66, r5=1.9, r20=-7.2, source='yahoo_v8', tsUtc=fresh_ts),
        QuoteItem(sym='2330.TW', name='台积电台股(TWD)', price=2180.0, chg=1.63, r5=4.0, r20=1.2, source='yahoo_v8', tsUtc='2026-11-27T05:30:00Z'),
        QuoteItem(sym='000660.KS', name='SK海力士(KRW)', price=1110000.0, chg=6.42, r5=5.1, r20=-2.0, source='yahoo_v8', tsUtc='2026-11-27T06:30:00Z'),
        QuoteItem(sym='ASML.AS', name='阿斯麦欧股(EUR)', price=1222.8, chg=4.67, r5=3.2, r20=-6.5, source='yahoo_v8', tsUtc=fresh_ts),
    ]
    b.market.live.quotes.extend(extra_quotes)
    b.primary.apac_eu_summary['2330.TW'] = {'name': '台积电台股(TWD)', 'price': 2180.0, 'chg': 1.63, 'r5': 4.0, 'r20': 1.2, 'tsUtc': '2026-11-27T05:30:00Z'}
    b.primary.apac_eu_summary['ASML.AS'] = {'name': '阿斯麦欧股(EUR)', 'price': 1222.8, 'chg': 4.67, 'r5': 3.2, 'r20': -6.5, 'tsUtc': fresh_ts}

    dec = evaluate_governance_window(now_utc)
    ctx = build_research_context(b, None, dec)
    assert '停盘旧价 11-25' in ctx
    assert 'ES=F' in ctx and 'NQ=F' in ctx
    assert '2330.TW' in ctx and 'ASML.AS' in ctx
    assert '今日最新 11-27' in ctx

    # Non-market social/gadget noise filtered while APAC chip & export-control catalysts remain.
    wires = [
        RawWireItem('n1', '韩国高考作弊查处量激增 涉智能设备案件增至去年3.6倍', '韩国教育部门通报高考作弊情况', _classify_tag('韩国高考作弊查处量激增'), '华尔街见闻', 'L1_WIRE_ZH', 'zh', now_utc, '2026-11-27 15:58 UTC+8', 'https://wallstreetcn.com/n1', 100),
        RawWireItem('n2', '传小米AI眼镜2代或将于12月中旬发布，客服回应', '客服称暂无官方消息', _classify_tag('传小米AI眼镜2代或将于12月中旬发布，客服回应'), '华尔街见闻', 'L1_WIRE_ZH', 'zh', now_utc, '2026-11-27 16:27 UTC+8', 'https://wallstreetcn.com/n2', 100),
        RawWireItem('n3', '新型机器人夹爪实现象鼻般的灵敏触觉', '科研团队发表论文', _classify_tag('新型机器人夹爪实现象鼻般的灵敏触觉'), '华尔街见闻', 'L1_WIRE_ZH', 'zh', now_utc, '2026-11-27 16:16 UTC+8', 'https://wallstreetcn.com/n3', 100),
        RawWireItem('n4', '11月27日航锦科技涨停：半导体，国产芯片，英伟达概念热股_个股掘金 - 证券之星', 'A股涨停播报', _classify_tag('11月27日航锦科技涨停：英伟达概念热股_个股掘金'), 'Google News 中文美股', 'L2_WIRE_ZH', 'zh', now_utc, '2026-11-27 16:40 UTC+8', 'https://example.com/n4', 80),
        RawWireItem('n5', '$英伟达 (NVDA.US)$ - Moomoo', '行情页', _classify_tag('$英伟达 (NVDA.US)$ - Moomoo'), 'Google News 中文美股', 'L2_WIRE_ZH', 'zh', now_utc, '2026-11-27 16:40 UTC+8', 'https://example.com/n5', 80),
        RawWireItem('g1', 'SK海力士大涨超6%带动韩国KOSPI收涨4.3%，台积电台股收于2180新台币', '亚太半导体板块全面走强', _classify_tag('SK海力士大涨超6%带动韩国KOSPI收涨4.3%，台积电台股收于2180新台币'), '华尔街见闻', 'L1_WIRE_ZH', 'zh', now_utc, '2026-11-27 14:35 UTC+8', 'https://wallstreetcn.com/g1', 100),
        RawWireItem('g2', '中国商务部顺延对美镓、锗、锑出口禁令暂停期至2027年1月10日', '商务部公告顺延出口管制暂停期', _classify_tag('中国商务部顺延对美镓、锗、锑出口禁令暂停期至2027年1月10日'), '华尔街见闻', 'L1_WIRE_ZH', 'zh', now_utc, '2026-11-27 14:20 UTC+8', 'https://wallstreetcn.com/g2', 100),
    ]
    selected = select_research_wires(wires, now_utc)
    selected_links = {w.link for w in selected}
    assert selected_links == {'https://wallstreetcn.com/g1', 'https://wallstreetcn.com/g2'}
    assert _classify_tag('SK海力士大涨带动芯片股走强') == 'M1 算力'
    assert _classify_tag('中国商务部顺延镓锗锑出口禁令暂停期') == '并购监管'
    from pydantic import ValidationError
    from src.research.analyst import _DraftMeta
    with pytest.raises(ValidationError):
        _DraftMeta(
            market_brief='',
            badge='b', headline='h', plain_guide='p', next_gate='n',
            regime='r', stance='s', vix_note='v', tnx_note='t', btc_note='btc',
        )
