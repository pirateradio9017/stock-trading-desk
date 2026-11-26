"""M2 Data Ingestion Engine for Stock Trading Desk v2.0."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from src.ingestion.market_data import MarketSnapshot, fetch_market_snapshot
from src.ingestion.news_aggregator import AggregatedNews, fetch_aggregated_news
from src.ingestion.primary_sources import PrimarySignals, fetch_primary_signals
from src.models.schemas import LiveResponse, NewsResponse


@dataclass
class IngestionBundle:
    """Complete verified ingestion payload handed off to the M3 research engine."""

    fetched_at_utc: datetime
    market: MarketSnapshot
    primary: PrimarySignals
    news: AggregatedNews

    @property
    def live_response(self) -> LiveResponse:
        return self.market.live

    @property
    def news_response(self) -> NewsResponse:
        return self.news.response

    def summary_dict(self) -> dict[str, Any]:
        return {
            "quotes_count": self.market.live.count,
            "live_quotes_ok": sum(1 for q in self.market.live.quotes if q.price is not None),
            "sec_filings": len(self.primary.sec_filings),
            "macro_proxies": len(self.primary.macro_proxies),
            "calendar_events": len(self.primary.calendar_events),
            "news_zh_count": self.news.response.count,
            "raw_wire_count": len(self.news.raw_items),
        }


async def run_ingestion(
    *,
    now_utc: datetime | None = None,
    include_screener: bool = True,
) -> IngestionBundle:
    """Run market data, primary L1/L2 signals, and news aggregation concurrently."""
    import asyncio

    market_task = asyncio.create_task(
        fetch_market_snapshot(now_utc=now_utc, include_screener=include_screener)
    )
    news_task = asyncio.create_task(fetch_aggregated_news(now_utc=now_utc))

    market = await market_task
    primary_task = asyncio.create_task(
        fetch_primary_signals(market=market, now_utc=now_utc)
    )
    primary, news = await asyncio.gather(primary_task, news_task)

    return IngestionBundle(
        fetched_at_utc=market.fetched_at_utc,
        market=market,
        primary=primary,
        news=news,
    )


__all__ = [
    "AggregatedNews",
    "IngestionBundle",
    "MarketSnapshot",
    "PrimarySignals",
    "fetch_aggregated_news",
    "fetch_market_snapshot",
    "fetch_primary_signals",
    "run_ingestion",
]
