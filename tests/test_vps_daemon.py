"""Unit tests for always-on VPS daemon features: fast_live, scheduler, hot cache, and admin/origin security."""

from __future__ import annotations

import asyncio
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest
import httpx
from fastapi.testclient import TestClient

from src.config import CORE_SYMBOLS, format_cn_timestamp
from src.ingestion.fast_live import (
    CNBC_EQUITY_SYMBOLS, _fetch_nasdaq_batch, _fetch_yahoo_batch, fetch_fast_live, parse_cnbc_live_quote,
)
from src.models.schemas import (
    AnalyticsData,
    HistoryEntry,
    LiveResponse,
    MetaInfo,
    NewsItem,
    NewsResponse,
    QuoteItem,
    StateResponse,
)
from src.scheduler import ResearchRuntimeStatus, SchedulerConfig, evaluate_research_due
from src.server import app, reset_server_caches


def _make_state(synced_utc: datetime, history: list[HistoryEntry] | None = None) -> StateResponse:
    return StateResponse(
        serverSyncedAt=format_cn_timestamp(synced_utc),
        meta=MetaInfo(
            updated_at="11-28 10:00 ET",
            updated_at_short="11-28 10:00 ET",
            badge="测试",
            headline="测试主线研判标题",
            plain_guide="观察关键支撑位",
            next_gate="等待盘后确认",
        ),
        history=history or [],
        analytics=AnalyticsData(),
    )


def _make_live(updated_utc: datetime, price: float = 123.45) -> LiveResponse:
    quotes = [
        QuoteItem(
            sym=spec.sym,
            name=spec.name,
            price=price,
            chg=1.25,
            r5=2.5,
            r20=6.0,
            source="cnbc_batch",
            tsUtc=updated_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
        )
        for spec in CORE_SYMBOLS
    ]
    return LiveResponse(
        updatedAt=format_cn_timestamp(updated_utc),
        updatedAtEt="11-28 10:00 ET",
        marketSession="OFF_HOURS",
        count=len(quotes),
        quotes=quotes,
    )


def test_parse_cnbc_live_quote_handles_regular_and_premarket() -> None:
    item = {
        "symbol": "NVDA",
        "code": 0,
        "last": "142.50",
        "change_pct": "+2.35%",
        "last_time": "2026-11-27T08:15:00.000-0500",
        "ExtendedMktQuote": {
            "type": "PRE_MKT",
            "last": "144.00",
            "change_pct": "+1.05%",
        },
    }
    q = parse_cnbc_live_quote(item, "NVDA", "英伟达", "2026-11-27T13:16:00Z", is_premarket=True)
    assert q is not None
    assert q.price == 142.5
    assert q.chg == 2.35
    assert q.ahPrice == 144.0
    assert q.ahChg == 1.05
    assert q.source == "cnbc_batch"


def test_fetch_fast_live_merges_cnbc_and_preserves_r5_r20() -> None:
    now_utc = datetime(2026, 11, 27, 15, 0, 0, tzinfo=timezone.utc)
    cached = _make_live(now_utc - timedelta(minutes=5), price=100.0)

    async def fake_cnbc_batch(_client, syms, timeout_sec=4.5):
        return [
            {
                "symbol": sym,
                "code": 0,
                "curmktstatus": "REG_MKT",
                "last": "500.00",
                "change_pct": "+1.50%",
                "last_time": "2026-11-27T09:59:00.000-0500",
            }
            for sym in syms
        ]

    async def fake_yahoo_gap(_client, _sem, spec, **_kwargs):
        return QuoteItem(
            sym=spec.sym,
            name=spec.name,
            price=88.0,
            chg=0.5,
            r5=1.1,
            r20=2.2,
            source="yahoo_v8",
            tsUtc="2026-11-27T15:00:00Z",
        )

    with (
        patch("src.ingestion.fast_live._fetch_cnbc_batch", side_effect=fake_cnbc_batch),
        patch("src.ingestion.fast_live._fetch_nasdaq_batch", return_value=[]),
        patch("src.ingestion.fast_live._fetch_yahoo_batch", return_value=[]),
        patch("src.ingestion.fast_live._fetch_yahoo_gap_symbol", side_effect=fake_yahoo_gap),
    ):
        live = asyncio.run(fetch_fast_live(cached=cached, now_utc=now_utc))

    assert live.count >= 52
    nvda = next(q for q in live.quotes if q.sym == "NVDA")
    assert nvda.price == 500.0
    assert nvda.r5 == 2.5
    assert nvda.r20 == 6.0
    assert live.session_rotation is not None
    assert len(live.session_rotation.themes) >= 2


def test_nasdaq_watchlist_at_open_and_http_limit() -> None:
    now = datetime(2026, 11, 30, 14, 30, 15, tzinfo=timezone.utc)
    sizes = []
    def respond(request):
        symbols = [s.split("|")[0] for s in request.url.params.get_list("symbol")]
        sizes.append(len(symbols))
        return httpx.Response(200, json={"data": [
            {"symbol": sym, "marketStatus": "Market Open", "lastSalePrice": "$152.54",
             "percentageChange": "+1.20%", "lastTradeTimestampDateTime":
                 {"SPY": "2026-11-27T00:00:00", "NVDA": "2026-11-30T16:30:00Z"}
                 .get(sym, "2026-11-30T09:30:10")}
            for sym in symbols]})
    async def fetch():
        async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
            return await _fetch_nasdaq_batch(client, now, "INTRADAY")
    prints = asyncio.run(fetch())
    assert max(sizes) <= 20
    assert {p.sym for p in prints} == set(CNBC_EQUITY_SYMBOLS) - {"SPY", "NVDA"}
    assert all(p.trade_utc == "2026-11-30T14:30:10Z" for p in prints)


def test_cnbc_date_only_and_invalid_times() -> None:
    row = {"code": 0, "last": "98", "last_time": "2026-11-27"}
    def parse(row):
        return parse_cnbc_live_quote(row, "SPY", "SPY", "2026-11-30T14:30:00Z", is_premarket=False)
    assert parse(row).tsUtc == "2026-11-27"
    assert parse({**row, "last_time": "bad"}) is None
    assert parse({**row, "last_time": "2026-11-30T18:00:00Z"}) is None


def test_yahoo_batch_uses_trade_metadata_at_open() -> None:
    now = datetime(2026, 11, 30, 14, 30, 20, tzinfo=timezone.utc)
    def respond(request):
        assert request.url.params["range"] == "1d"
        rows = [{"response": [{"meta": {
            "symbol": sym, "regularMarketTime": now.timestamp() + offset,
            "regularMarketPrice": 102, "previousClose": 100,
        }, "indicators": {"quote": [{"close": [80]}]}}]} for sym, offset in
            [("NVDA", -2), ("QQQ", 3600), ("AAPL", -86400)]]
        return httpx.Response(200, json={"spark": {"result": rows}})
    async def fetch():
        async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
            return await _fetch_yahoo_batch(client, list(CORE_SYMBOLS), now)
    quotes = asyncio.run(fetch())
    assert [(q.sym, q.price, q.chg, q.tsUtc) for q in quotes] == [
        ("NVDA", 102, 2, "2026-11-30T14:30:18Z")
    ]


def test_open_uses_newest_trade_and_preserves_cached_newer_price() -> None:
    from src.models.schemas import SessionPrint
    now = datetime(2026, 11, 30, 14, 30, 25, tzinfo=timezone.utc)
    cached = _make_live(now - timedelta(seconds=15), price=101)
    cnbc = [{"code": 0, "symbol": sym, "last": "100", "change_pct": "+1",
             "last_time": "2026-11-30T14:30:05Z", "curmktstatus": "REG_MKT"} for sym in CNBC_EQUITY_SYMBOLS]
    newer = SessionPrint(sym="NVDA", price=103, change=2, trade_utc="2026-11-30T14:30:20Z")
    with (
        patch("src.ingestion.fast_live._fetch_cnbc_batch", side_effect=[cnbc, []]),
        patch("src.ingestion.fast_live._fetch_nasdaq_batch", return_value=[newer]),
        patch("src.ingestion.fast_live._fetch_yahoo_gap_symbol", return_value=None),
    ):
        live = asyncio.run(fetch_fast_live(cached=cached, now_utc=now))
    quotes = {q.sym: q for q in live.quotes}
    assert (quotes["NVDA"].price, quotes["NVDA"].source, quotes["NVDA"].r5) == (103, "nasdaq_watchlist", 2.5)
    assert (quotes["AAPL"].price, quotes["AAPL"].tsUtc) == (101, "2026-11-30T14:30:10Z")


def test_old_disk_snapshot_and_failed_refresh_stay_stale(tmp_path, monkeypatch) -> None:
    reset_server_caches()
    monkeypatch.setenv("DESK_DATA_DIR", str(tmp_path))
    old = _make_live(datetime.now(timezone.utc) - timedelta(days=1))
    path = tmp_path / "live.json"
    path.write_text(old.model_dump_json(), encoding="utf-8")
    mtime = path.stat().st_mtime_ns
    async def offline(cached):
        return cached
    with patch("src.server._fetch_live_upstream", side_effect=offline) as refresh:
        response = TestClient(app).get("/api/live")
    assert response.headers["X-Desk-Cache"] == "MEM-FALLBACK"
    assert response.json()["updatedAt"] == old.updatedAt
    assert path.stat().st_mtime_ns == mtime
    assert refresh.await_count == 1


def test_scheduler_decision_windows_and_failure_backoff() -> None:
    cfg = SchedulerConfig(enabled=True)
    status = ResearchRuntimeStatus()

    # Cold start triggers immediately
    now_active = datetime(2026, 11, 25, 15, 0, 0, tzinfo=timezone.utc)  # Wed 10:00 ET
    d_cold = evaluate_research_due(None, status, now_active, cfg)
    assert d_cold.should_run is True
    assert d_cold.reason == "cold_start"

    # Active intraday: 10m fresh -> skip; 25m stale -> run
    assert evaluate_research_due(_make_state(now_active - timedelta(minutes=10)), status, now_active, cfg).should_run is False
    d_active = evaluate_research_due(_make_state(now_active - timedelta(minutes=25)), status, now_active, cfg)
    assert d_active.should_run is True
    assert d_active.reason == "active_20m_due"

    # Post-close rebalance on early-close Black Friday 2026-11-27 13:35 ET (18:35 UTC)
    early_rebal_now = datetime(2026, 11, 27, 18, 35, 0, tzinfo=timezone.utc)
    d_rebal = evaluate_research_due(_make_state(early_rebal_now - timedelta(minutes=15)), status, early_rebal_now, cfg)
    assert d_rebal.should_run is True
    assert d_rebal.reason == "post_close_rebalance"
    assert d_rebal.catchup_rebalance is False

    # Already rebalanced today skips duplicate
    done_hist = [
        HistoryEntry(
            time="2026-11-28 02:32 UTC+8",  # 2026-11-27 13:32 ET
            type="主线复盘",
            tone="sage",
            title="盘后复盘",
            detail="完成复盘",
        )
    ]
    assert evaluate_research_due(
        _make_state(early_rebal_now - timedelta(minutes=3), history=done_hist),
        status,
        early_rebal_now,
        cfg,
    ).should_run is False

    # Catchup window after 14:05 ET (19:05 UTC)
    catchup_now = datetime(2026, 11, 27, 19, 5, 0, tzinfo=timezone.utc)
    d_catch = evaluate_research_due(_make_state(catchup_now - timedelta(minutes=10)), status, catchup_now, cfg)
    assert d_catch.should_run is True
    assert d_catch.reason == "post_close_rebalance_catchup"
    assert d_catch.catchup_rebalance is True

    # Cross-market APAC close window (2026-11-27 14:40 CST = 06:40 UTC)
    apac_now = datetime(2026, 11, 27, 6, 40, 0, tzinfo=timezone.utc)
    d_apac = evaluate_research_due(_make_state(apac_now - timedelta(minutes=40)), status, apac_now, cfg)
    assert d_apac.should_run is True
    assert d_apac.reason == "cross_market_apac_close"

    # Failure backoff prevents tight retry storms
    fail_status = ResearchRuntimeStatus(
        consecutive_failures=1,
        next_retry_after=now_active + timedelta(seconds=120),
    )
    d_back = evaluate_research_due(_make_state(now_active - timedelta(minutes=30)), fail_status, now_active, cfg)
    assert d_back.should_run is False
    assert d_back.reason == "failure_backoff"


def test_server_etag_304_hot_cache_and_admin_auth(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    reset_server_caches()
    monkeypatch.setenv("DESK_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("DESK_ADMIN_TOKEN", "secret-admin-xyz")

    now_utc = datetime.now(timezone.utc)
    state = _make_state(now_utc - timedelta(minutes=5))
    live = _make_live(now_utc)
    news = NewsResponse(
        updatedAt=format_cn_timestamp(now_utc),
        count=1,
        items=[
            NewsItem(
                event_id="evt-1",
                title="美股科技股午后走强测试快讯",
                summary="测试摘要",
                tag="M1 算力",
                source="华尔街见闻",
                pubDate="2026-11-28 19:00 UTC+8",
                link="https://example.com/1",
            )
        ],
    )
    (tmp_path / "state.json").write_text(json.dumps(state.model_dump(mode="json"), ensure_ascii=False), encoding="utf-8")
    (tmp_path / "live.json").write_text(json.dumps(live.model_dump(mode="json"), ensure_ascii=False), encoding="utf-8")
    (tmp_path / "news.json").write_text(json.dumps(news.model_dump(mode="json"), ensure_ascii=False), encoding="utf-8")

    client = TestClient(app)

    # First hit reads disk, second hits hot RAM, If-None-Match yields 304
    r1 = client.get("/api/state")
    assert r1.status_code == 200
    assert r1.headers.get("x-desk-cache") == "DISK-HIT"
    etag = r1.headers.get("etag")
    assert etag and etag.startswith('W/"state-')

    r2 = client.get("/api/state")
    assert r2.status_code == 200
    assert r2.headers.get("x-desk-cache") == "MEM-HIT"

    r304 = client.get("/api/state", headers={"If-None-Match": etag})
    assert r304.status_code == 304
    assert r304.text == ""

    # Health includes extended VPS telemetry fields
    rh = client.get("/api/health")
    assert rh.status_code == 200
    hj = rh.json()
    assert hj["ok"] is True
    assert hj["stateAgeMinutes"] is not None
    assert hj["quotesCount"] == len(CORE_SYMBOLS)
    assert hj["runtimeMode"] == "local"

    # Admin trigger rejects missing/wrong token and accepts valid token
    assert client.post("/api/admin/run-pipeline").status_code == 403
    assert client.post("/api/admin/run-pipeline", headers={"X-Admin-Token": "wrong"}).status_code == 403
    with patch("src.scheduler.DeskScheduler.trigger_research_background", return_value=True) as mock_trig:
        r_ok = client.post("/api/admin/run-pipeline?force_rebalance=1", headers={"X-Admin-Token": "secret-admin-xyz"})
        assert r_ok.status_code == 202
        assert r_ok.json()["started"] is True
        mock_trig.assert_called_once_with(reason="manual_api", force_rebalance=True)
