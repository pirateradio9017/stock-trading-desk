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
