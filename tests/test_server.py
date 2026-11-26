"""Unit & contract tests for src/pipeline.py and src/server.py."""

from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest
from fastapi.testclient import TestClient

from src.config import CORE_SYMBOLS
from src.ingestion import IngestionBundle
from src.ingestion.market_data import MarketSnapshot, SymbolSeries
from src.ingestion.news_aggregator import AggregatedNews, RawWireItem
from src.ingestion.primary_sources import PrimarySignals
from src.models.schemas import (
    HealthResponse,
    LinkageTrend,
    LinkageTrendSeries,
    LiveResponse,
    NewsItem,
    NewsResponse,
    QuoteItem,
    RotationAsset,
    StateResponse,
)
from src.pipeline import run_pipeline
from src.server import app


def _make_bundle() -> IngestionBundle:
    now_utc = datetime(2026, 11, 26, 15, 0, 0, tzinfo=timezone.utc)
    quotes: list[QuoteItem] = []
    series_map: dict[str, SymbolSeries] = {}
    symbol_sparks: dict[str, list[float]] = {}
    for idx, spec in enumerate(CORE_SYMBOLS):
        px = 100.0 + idx * 2.0
        q_item = QuoteItem(
            sym=spec.sym,
            name=spec.name,
            price=px,
            chg=0.8 if idx % 2 == 0 else -0.4,
            r5=2.1,
            r20=6.4,
            source="yahoo_v8",
            tsUtc="2026-11-26T15:00:00Z",
        )
        quotes.append(q_item)
        closes = [px * (0.95 + i * 0.002) for i in range(30)]
        series_map[spec.sym] = SymbolSeries(
            spec=spec,
            dates=[f"2026-11-{i + 1:02d}" for i in range(30)],
            closes=closes,
            quote=q_item,
        )
        symbol_sparks[spec.sym] = [round(100.0 + i * 0.5, 2) for i in range(11)]

    live = LiveResponse(
        updatedAt="2026/11/26 23:00:00 UTC+8",
        updatedAtEt="11-26 10:00 ET",
        marketSession="HOLIDAY",
        count=len(quotes),
        quotes=quotes,
    )
    market = MarketSnapshot(
        fetched_at_utc=now_utc,
        live=live,
        series_map=series_map,
        spark_labels=[f"D-{10 - i}" for i in range(10)] + ["Now"],
        symbol_sparks=symbol_sparks,
        corr_labels=["SPY", "QQQ", "TLT", "GLD", "NVDA", "BTC"],
        corr_matrix=[
            [1.0 if i == j else 0.25 for j in range(6)]
            for i in range(6)
        ],
        linkage_trend=LinkageTrend(
            labels=["11-20", "11-21", "11-24", "11-25", "11-26"],
            series=[
                LinkageTrendSeries(sym="XLP", label="防御消费 XLP", values=[0.0, 0.4, 0.8, 1.1, 1.3]),
                LinkageTrendSeries(sym="TLT", label="20Y美债 TLT", values=[0.0, 0.2, 0.5, 0.6, 0.9]),
                LinkageTrendSeries(sym="GC=F", label="黄金 GC=F", values=[0.0, 0.5, 0.9, 1.4, 1.8]),
                LinkageTrendSeries(sym="^GSPC", label="标普500", values=[0.0, 0.3, 0.7, 1.0, 1.2]),
                LinkageTrendSeries(sym="NVDA", label="英伟达 NVDA", values=[0.0, 1.0, 2.0, 1.8, 2.5]),
            ],
        ),
        rotation_assets=[
            RotationAsset(sym="NVDA", name="英伟达", group="M1", r5=2.1, r20=6.4),
            RotationAsset(sym="AVGO", name="博通", group="M1", r5=1.8, r20=5.0),
            RotationAsset(sym="TLT", name="20Y+美债 ETF", group="M3", r5=0.9, r20=2.0),
            RotationAsset(sym="XLP", name="必需消费 ETF", group="M4", r5=1.3, r20=3.1),
            RotationAsset(sym="BTC-USD", name="比特币", group="M5", r5=-1.2, r20=4.5),
        ],
    )
    primary = PrimarySignals(
        fetched_at_utc=now_utc,
        sec_filings=[],
        macro_proxies=[],
        calendar_events=[],
    )
    news_resp = NewsResponse(
        updatedAt="2026/11/26 23:00:00 UTC+8",
        count=1,
        items=[
            NewsItem(
                event_id="evt1234567890",
                title="英伟达数据中心算力需求保持强劲",
                summary="对应 M1 算力主线：AI 服务器资本开支维持高位。",
                tag="M1 算力",
                source="华尔街见闻",
                pubDate="2026-11-26 22:50 UTC+8",
                link="https://wallstreetcn.com/articles/3700001",
            )
        ],
    )
    news = AggregatedNews(
        fetched_at_utc=now_utc,
        response=news_resp,
        raw_items=[
            RawWireItem(
                event_id="evt1234567890",
                title="英伟达数据中心算力需求保持强劲",
                summary="对应 M1 算力主线：AI 服务器资本开支维持高位。",
                tag="M1 算力",
                source="华尔街见闻",
                tier="L1_WIRE_ZH",
                lang="zh",
                pub_dt_utc=now_utc,
                pub_date_cn="2026-11-26 22:50 UTC+8",
                link="https://wallstreetcn.com/articles/3700001",
                score=10,
            )
        ],
        feed_status={"wscn_us_stock": 1},
    )
    return IngestionBundle(
        fetched_at_utc=now_utc,
        market=market,
        primary=primary,
        news=news,
    )


def test_pipeline_and_server_roundtrip(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DESK_DATA_DIR", str(tmp_path))
    bundle = _make_bundle()

    with patch("src.pipeline.run_ingestion", new=AsyncMock(return_value=bundle)):
        summary = asyncio.run(
            run_pipeline(
                data_dir=tmp_path,
                use_llm=False,
                force_rebalance=False,
                publish_kv=False,
                include_screener=False,
                now_utc=bundle.fetched_at_utc,
            )
        )

    assert summary["engine_mode"] == "deterministic_fallback"
    assert summary["quotes_count"] == len(CORE_SYMBOLS)
    assert summary["news_count"] == 1
    assert (tmp_path / "state.json").is_file()
    assert (tmp_path / "live.json").is_file()
    assert (tmp_path / "news.json").is_file()

    StateResponse.model_validate(json.loads((tmp_path / "state.json").read_text(encoding="utf-8")))
    LiveResponse.model_validate(json.loads((tmp_path / "live.json").read_text(encoding="utf-8")))
    NewsResponse.model_validate(json.loads((tmp_path / "news.json").read_text(encoding="utf-8")))

    client = TestClient(app)

    r_root = client.get("/")
    assert r_root.status_code == 200
    assert "text/html" in r_root.headers.get("content-type", "")
    assert "美股交易链条观察" in r_root.text

    r_health = client.get("/api/health")
    assert r_health.status_code == 200
    health = HealthResponse.model_validate(r_health.json())
    assert health.quotesCount == len(CORE_SYMBOLS)
    assert health.newsCount == 1
    assert health.stateUpdatedAt is not None

    r_live = client.get("/api/live?sym=INVALID&force=abc")
    assert r_live.status_code == 200
    assert r_live.json()["count"] == 0

    r_state = client.get("/api/state?force=1")
    assert r_state.status_code == 200
    StateResponse.model_validate(r_state.json())

    r_news = client.get("/api/news?limit=-5")
    assert r_news.status_code == 200
    NewsResponse.model_validate(r_news.json())

    r_missing = client.get("/api/nonexistent")
    assert r_missing.status_code == 404
    assert "<html" not in r_missing.text.lower()

    r_method = client.post("/api/health")
    assert r_method.status_code == 405


def test_server_returns_503_when_state_missing(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DESK_DATA_DIR", str(tmp_path))
    client = TestClient(app)
    r_state = client.get("/api/state")
    assert r_state.status_code == 503
    assert "application/json" in r_state.headers.get("content-type", "")


def test_tier5_adversarial_server_and_worker_parity(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DESK_DATA_DIR", str(tmp_path))
    bundle = _make_bundle()
    with patch("src.pipeline.run_ingestion", new=AsyncMock(return_value=bundle)):
        asyncio.run(
            run_pipeline(
                data_dir=tmp_path,
                use_llm=False,
                force_rebalance=False,
                publish_kv=False,
                include_screener=False,
                now_utc=bundle.fetched_at_utc,
            )
        )

    client = TestClient(app)

    # HEAD method parity + security headers across HTML and API routes
    for path in ("/", "/api/health", "/api/live", "/api/state", "/api/news"):
        r_head = client.head(path)
        assert r_head.status_code == 200, f"HEAD {path} returned {r_head.status_code}"
        assert r_head.headers.get("x-content-type-options") == "nosniff"
        assert r_head.headers.get("referrer-policy") == "strict-origin-when-cross-origin"
    assert client.get("/").headers.get("x-frame-options") == "SAMEORIGIN"

    # Non-numeric & extreme limit params on /api/news never 422/500
    for q in ("abc", "-99", "0", "999999999999"):
        r_n = client.get(f"/api/news?limit={q}")
        assert r_n.status_code == 200
        assert 1 <= r_n.json()["count"] <= 50

    # Corrupted state.json returns clean 503 JSON
    (tmp_path / "state.json").write_text("{bad json", encoding="utf-8")
    r_corrupt = client.get("/api/state")
    assert r_corrupt.status_code == 503
    assert "application/json" in r_corrupt.headers.get("content-type", "")

    # Worker edge parity static assertions
    worker_src = (Path(__file__).resolve().parent.parent / "worker" / "src" / "index.js").read_text(encoding="utf-8")
    assert '"XLU"' in worker_src
    assert "regularMarketChangePercent" in worker_src
    assert "meta.chartPreviousClose" not in worker_src
    assert "easterSundayUtc" in worker_src
    assert "try {" in worker_src and "} catch (err)" in worker_src
