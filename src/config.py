"""Core configuration, symbol universe, feed definitions, and market session rules."""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Literal
from zoneinfo import ZoneInfo

APP_VERSION = "2.0.0"
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_DATA_DIR = PROJECT_ROOT / "data"

ENV_VAR_NAMES: tuple[str, ...] = (
    "GEMINI_API_KEY",
    "CF_ACCOUNT_ID",
    "CF_API_TOKEN",
    "DESK_DATA_DIR",
    "DESK_PORT",
    "DESK_LOG_LEVEL",
)


@dataclass(frozen=True)
class DeskSettings:
    """Runtime settings loaded dynamically from environment variables."""

    gemini_api_key: str
    cf_account_id: str
    cf_api_token: str
    data_dir: Path
    port: int = 8787
    log_level: str = "INFO"


def load_settings() -> DeskSettings:
    """Load runtime settings dynamically so tests and CLI overrides apply cleanly."""
    raw_dir = os.getenv("DESK_DATA_DIR", "").strip()
    raw_port = os.getenv("DESK_PORT", "").strip()
    raw_level = os.getenv("DESK_LOG_LEVEL", "").strip()
    return DeskSettings(
        gemini_api_key=os.getenv("GEMINI_API_KEY", "").strip(),
        cf_account_id=os.getenv("CF_ACCOUNT_ID", "").strip(),
        cf_api_token=os.getenv("CF_API_TOKEN", "").strip(),
        data_dir=Path(raw_dir) if raw_dir else DEFAULT_DATA_DIR,
        port=int(raw_port) if raw_port else 8787,
        log_level=raw_level.upper() if raw_level else "INFO",
    )


@dataclass(frozen=True)
class SymbolSpec:
    """Market ticker definition."""

    sym: str
    name: str
    category: str
    region: str = "US"


CORE_SYMBOLS: tuple[SymbolSpec, ...] = (
    SymbolSpec("^GSPC", "标普500", "index"),
    SymbolSpec("^IXIC", "纳斯达克", "index"),
    SymbolSpec("^VIX", "恐慌指数 VIX", "volatility"),
    SymbolSpec("^MOVE", "美债波动率 MOVE", "volatility"),
    SymbolSpec("^TNX", "10Y 美债收益率", "rates"),
    SymbolSpec("BTC-USD", "比特币", "crypto"),
    SymbolSpec("GC=F", "黄金", "commodity"),
    SymbolSpec("SI=F", "白银", "commodity"),
    SymbolSpec("HG=F", "COMEX 铜", "commodity"),
    SymbolSpec("BZ=F", "布伦特原油", "commodity"),
    SymbolSpec("JPY=X", "美元/日元", "fx"),
    SymbolSpec("QQQ", "纳指100 ETF", "etf"),
    SymbolSpec("TLT", "20Y+美债 ETF", "rates_etf"),
    SymbolSpec("XLP", "必需消费 ETF", "sector_etf"),
    SymbolSpec("XLE", "能源 ETF", "sector_etf"),
    SymbolSpec("NVDA", "英伟达", "equity_ai"),
    SymbolSpec("AVGO", "博通", "equity_ai"),
    SymbolSpec("AMD", "超威半导体", "equity_ai"),
    SymbolSpec("LITE", "Lumentum", "equity_optical"),
    SymbolSpec("SOXX", "半导体 ETF", "sector_etf"),
    SymbolSpec("ITB", "房屋建筑 ETF", "sector_etf"),
    SymbolSpec("AAPL", "苹果", "equity_tech"),
    SymbolSpec("IREN", "IREN", "equity_cloud_power"),
    SymbolSpec("NBIS", "Nebius", "equity_cloud_power"),
    SymbolSpec("CRWV", "CoreWeave", "equity_cloud_power"),
    SymbolSpec("COIN", "Coinbase", "equity_crypto"),
    SymbolSpec("MSTR", "Strategy", "equity_crypto"),
    SymbolSpec("TSM", "台积电", "equity_ai"),
    SymbolSpec("LMT", "洛克希德·马丁", "equity_defense"),
)

US_FUTURES_SYMBOLS: tuple[SymbolSpec, ...] = (
    SymbolSpec("ES=F", "标普500期货", "index_future", region="US"),
    SymbolSpec("NQ=F", "纳指100期货", "index_future", region="US"),
)

APAC_SYMBOLS: tuple[SymbolSpec, ...] = (
    SymbolSpec("^N225", "日经225", "index", region="APAC"),
    SymbolSpec("^KS11", "韩国 KOSPI", "index", region="APAC"),
    SymbolSpec("^TWII", "台湾加权", "index", region="APAC"),
    SymbolSpec("^HSI", "恒生指数", "index", region="APAC"),
    SymbolSpec("000001.SS", "上证指数", "index", region="APAC"),
    SymbolSpec("2330.TW", "台积电台股(TWD)", "equity_apac_semi", region="APAC"),
    SymbolSpec("000660.KS", "SK海力士(KRW)", "equity_apac_semi", region="APAC"),
    SymbolSpec("005930.KS", "三星电子(KRW)", "equity_apac_semi", region="APAC"),
)

EU_SYMBOLS: tuple[SymbolSpec, ...] = (
    SymbolSpec("^GDAXI", "德国 DAX", "index", region="EU"),
    SymbolSpec("^STOXX50E", "欧洲斯托克50", "index", region="EU"),
    SymbolSpec("ASML.AS", "阿斯麦欧股(EUR)", "equity_eu_semi", region="EU"),
    SymbolSpec("ASML", "阿斯麦", "equity_semi", region="EU"),
)

CREDIT_MACRO_SYMBOLS: tuple[SymbolSpec, ...] = (
    SymbolSpec("HYG", "高收益债 ETF", "credit_etf", region="US"),
    SymbolSpec("LQD", "投资级债 ETF", "credit_etf", region="US"),
    SymbolSpec("DX-Y.NYB", "美元指数 DXY", "fx", region="US"),
    SymbolSpec("CL=F", "WTI 原油", "commodity", region="US"),
    SymbolSpec("XLU", "公用事业 ETF", "sector_etf", region="US"),
    SymbolSpec("XLF", "金融 ETF", "sector_etf", region="US"),
    SymbolSpec("SMH", "半导体龙头 ETF", "sector_etf", region="US"),
)

AUXILIARY_BENCHMARK_SYMBOLS: tuple[SymbolSpec, ...] = (
    SymbolSpec("SPY", "标普500 ETF", "etf", region="US"),
    SymbolSpec("GLD", "黄金 ETF", "commodity_etf", region="US"),
)

CORE_SYMBOL_MAP: dict[str, SymbolSpec] = {s.sym: s for s in CORE_SYMBOLS}
BENCHMARK_SYMBOLS: tuple[SymbolSpec, ...] = (
    US_FUTURES_SYMBOLS + APAC_SYMBOLS + EU_SYMBOLS + CREDIT_MACRO_SYMBOLS
)
ALL_SYMBOLS: tuple[SymbolSpec, ...] = (
    CORE_SYMBOLS + BENCHMARK_SYMBOLS + AUXILIARY_BENCHMARK_SYMBOLS
)
SYMBOL_SPEC_MAP: dict[str, SymbolSpec] = {s.sym: s for s in ALL_SYMBOLS}
SYMBOL_NAME_MAP: dict[str, str] = {s.sym: s.name for s in ALL_SYMBOLS}

CORR_ASSETS: tuple[tuple[str, str], ...] = (
    ("SPY", "SPY"),
    ("QQQ", "QQQ"),
    ("TLT", "TLT"),
    ("GLD", "GLD"),
    ("NVDA", "NVDA"),
    ("BTC-USD", "BTC"),
)
CORR_SYMBOLS: tuple[str, ...] = tuple(sym for sym, _ in CORR_ASSETS)
CORR_LABELS: tuple[str, ...] = tuple(lbl for _, lbl in CORR_ASSETS)

SCISSOR_SERIES: tuple[tuple[str, str], ...] = (
    ("XLP", "防御消费 XLP"),
    ("TLT", "20Y美债 TLT"),
    ("GC=F", "黄金 GC=F"),
    ("^GSPC", "标普500"),
    ("NVDA", "英伟达 NVDA"),
)

SEC_USER_AGENT = "StockTradingDesk/2.0 (research@stock-trading-desk.local)"
HTTP_USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) StockTradingDesk/2.0"

FeedTier = Literal[
    "L1_PRIMARY",
    "L1_MARKET",
    "L1_WIRE_ZH",
    "L2_MARKET",
    "L2_WIRE_EN",
    "L2_WIRE_ZH",
    "L2_AGGREGATOR",
]
FeedKind = Literal["json", "rss", "atom", "html"]


@dataclass(frozen=True)
class FeedSpec:
    """Upstream market, filing, or news wire feed specification."""

    key: str
    name: str
    tier: FeedTier
    url: str
    kind: FeedKind
    requires_sec_ua: bool = False


FEED_SOURCES: dict[str, FeedSpec] = {
    "yahoo_v8": FeedSpec(
        key="yahoo_v8",
        name="Yahoo Finance v8 Chart",
        tier="L1_MARKET",
        url="https://query1.finance.yahoo.com/v8/finance/chart/{symbol}",
        kind="json",
    ),
    "yahoo_screener_gainers": FeedSpec(
        key="yahoo_screener_gainers",
        name="Yahoo Screener Day Gainers",
        tier="L2_MARKET",
        url="https://query1.finance.yahoo.com/v1/finance/screener/predefined/saved?scrIds=day_gainers",
        kind="json",
    ),
    "yahoo_screener_losers": FeedSpec(
        key="yahoo_screener_losers",
        name="Yahoo Screener Day Losers",
        tier="L2_MARKET",
        url="https://query1.finance.yahoo.com/v1/finance/screener/predefined/saved?scrIds=day_losers",
        kind="json",
    ),
    "yahoo_screener_actives": FeedSpec(
        key="yahoo_screener_actives",
        name="Yahoo Screener Most Actives",
        tier="L2_MARKET",
        url="https://query1.finance.yahoo.com/v1/finance/screener/predefined/saved?scrIds=most_actives",
        kind="json",
    ),
    "sec_edgar_submissions": FeedSpec(
        key="sec_edgar_submissions",
        name="SEC EDGAR Submissions",
        tier="L1_PRIMARY",
        url="https://data.sec.gov/submissions/CIK{cik10}.json",
        kind="json",
        requires_sec_ua=True,
    ),
    "sec_edgar_current_rss": FeedSpec(
        key="sec_edgar_current_rss",
        name="SEC EDGAR Current Filings",
        tier="L1_PRIMARY",
        url="https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&CIK=&type=&company=&dateb=&owner=include&start=0&count=40&output=atom",
        kind="atom",
        requires_sec_ua=True,
    ),
    "wscn_us_stock": FeedSpec(
        key="wscn_us_stock",
        name="华尔街见闻美股快讯",
        tier="L1_WIRE_ZH",
        url="https://api-one-wscn.awtmt.com/apiv1/content/lives?channel=us-stock-channel&limit=60",
        kind="json",
    ),
    "wscn_global": FeedSpec(
        key="wscn_global",
        name="华尔街见闻全球快讯",
        tier="L1_WIRE_ZH",
        url="https://api-one-wscn.awtmt.com/apiv1/content/lives?channel=global-channel&limit=60",
        kind="json",
    ),
    "cnbc_markets": FeedSpec(
        key="cnbc_markets",
        name="CNBC Markets",
        tier="L2_WIRE_EN",
        url="https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=15839069",
        kind="rss",
    ),
    "cnbc_economy": FeedSpec(
        key="cnbc_economy",
        name="CNBC Economy",
        tier="L2_WIRE_EN",
        url="https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=20910258",
        kind="rss",
    ),
    "cnbc_world": FeedSpec(
        key="cnbc_world",
        name="CNBC World Markets",
        tier="L2_WIRE_EN",
        url="https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=100727362",
        kind="rss",
    ),
    "marketwatch_top": FeedSpec(
        key="marketwatch_top",
        name="MarketWatch Top Stories",
        tier="L2_WIRE_EN",
        url="https://feeds.content.dowjones.io/public/rss/mw_topstories",
        kind="rss",
    ),
    "investing_stocks": FeedSpec(
        key="investing_stocks",
        name="Investing.com Stocks",
        tier="L2_WIRE_EN",
        url="https://www.investing.com/rss/news_25.rss",
        kind="rss",
    ),
    "seekingalpha_currents": FeedSpec(
        key="seekingalpha_currents",
        name="Seeking Alpha Market Currents",
        tier="L2_WIRE_EN",
        url="https://seekingalpha.com/market_currents.xml",
        kind="rss",
    ),
    "nasdaq_markets": FeedSpec(
        key="nasdaq_markets",
        name="Nasdaq Markets",
        tier="L2_WIRE_EN",
        url="https://www.nasdaq.com/feed/rssoutbound?category=Markets",
        kind="rss",
    ),
    "google_news_us": FeedSpec(
        key="google_news_us",
        name="Google News US Markets",
        tier="L2_WIRE_EN",
        url="https://news.google.com/rss/search?q=US+stock+market+OR+Federal+Reserve+OR+semiconductor&hl=en-US&gl=US&ceid=US:en",
        kind="rss",
    ),
    "google_news_zh": FeedSpec(
        key="google_news_zh",
        name="Google News 中文美股",
        tier="L2_WIRE_ZH",
        url="https://news.google.com/rss/search?q=%E7%BE%8E%E8%82%A1+OR+%E8%8B%B1%E4%BC%9F%E8%BE%BE+OR+%E7%BE%8E%E8%81%94%E5%82%A8&hl=zh-CN&gl=CN&ceid=CN:zh-Hans",
        kind="rss",
    ),
    "finviz_news": FeedSpec(
        key="finviz_news",
        name="Finviz News",
        tier="L2_AGGREGATOR",
        url="https://finviz.com/news.ashx",
        kind="html",
    ),
}

SEC_TRACKED_CIKS: dict[str, str] = {
    "NVDA": "0001045810",
    "TSM": "0001046179",
    "AAPL": "0000320193",
    "AVGO": "0001730168",
    "AMD": "0000002488",
    "LMT": "0000936468",
    "COIN": "0001679788",
    "MSTR": "0001050446",
}

TZ_ET = ZoneInfo("America/New_York")
TZ_CN = ZoneInfo("Asia/Shanghai")
TZ_UTC = timezone.utc

SESSION_WINDOWS_ET: dict[str, tuple[time, time] | time] = {
    "PRE_MARKET": (time(4, 0), time(9, 30)),
    "INTRADAY": (time(9, 30), time(15, 30)),
    "MARKET_CLOSE": time(16, 0),
    "REBALANCE_WINDOW": (time(16, 30), time(17, 0)),
    "AFTER_HOURS": (time(16, 0), time(20, 0)),
}

EXTRA_US_MARKET_HOLIDAYS: dict[date, str] = {}


def _observed_holiday(d: date) -> date:
    """Shift weekend holidays to Friday (Sat) or Monday (Sun) per NYSE rules."""
    if d.weekday() == 5:
        return d - timedelta(days=1)
    if d.weekday() == 6:
        return d + timedelta(days=1)
    return d


def _nth_weekday(year: int, month: int, weekday: int, n: int) -> date:
    """Return the n-th occurrence (1-indexed) of weekday (Mon=0..Sun=6) in month."""
    first = date(year, month, 1)
    offset = (weekday - first.weekday()) % 7
    return first + timedelta(days=offset + (n - 1) * 7)


def _last_weekday(year: int, month: int, weekday: int) -> date:
    """Return the last occurrence of weekday (Mon=0..Sun=6) in month."""
    if month == 12:
        last_day = date(year, 12, 31)
    else:
        last_day = date(year, month + 1, 1) - timedelta(days=1)
    offset = (last_day.weekday() - weekday) % 7
    return last_day - timedelta(days=offset)


def _easter_sunday(year: int) -> date:
    """Compute Gregorian Easter Sunday via Meeus/Jones/Butcher algorithm."""
    a = year % 19
    b = year // 100
    c = year % 100
    d = b // 4
    e = b % 4
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i = c // 4
    k = c % 4
    ell = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * ell) // 451
    month = (h + ell - 7 * m + 114) // 31
    day = ((h + ell - 7 * m + 114) % 31) + 1
    return date(year, month, day)


def get_us_market_holidays(year: int) -> dict[date, str]:
    """Return NYSE observed full-day holidays falling within the given calendar year.

    Per NYSE Rule 7.2, when a holiday falls on a Saturday, the Exchange closes on the
    preceding Friday unless that Friday is the end of a monthly or yearly accounting
    period (specifically Dec 31 when Jan 1 of the following year falls on a Saturday,
    such as 2021-12-31 and 2027-12-31, on which NYSE remains open). Sunday holidays
    are observed on the following Monday (e.g., 2023-01-02).
    """
    holidays: dict[date, str] = {}

    nyd_obs = _observed_holiday(date(year, 1, 1))
    if nyd_obs.year == year:
        holidays[nyd_obs] = "New Year's Day"

    holidays[_nth_weekday(year, 1, 0, 3)] = "Martin Luther King Jr. Day"
    holidays[_nth_weekday(year, 2, 0, 3)] = "Washington's Birthday"
    holidays[_easter_sunday(year) - timedelta(days=2)] = "Good Friday"
    holidays[_last_weekday(year, 5, 0)] = "Memorial Day"

    if year >= 2022:
        holidays[_observed_holiday(date(year, 6, 19))] = "Juneteenth National Independence Day"

    holidays[_observed_holiday(date(year, 7, 4))] = "Independence Day"
    holidays[_nth_weekday(year, 9, 0, 1)] = "Labor Day"
    holidays[_nth_weekday(year, 11, 3, 4)] = "Thanksgiving Day"
    holidays[_observed_holiday(date(year, 12, 25))] = "Christmas Day"

    for extra_date, extra_name in EXTRA_US_MARKET_HOLIDAYS.items():
        if extra_date.year == year:
            holidays[extra_date] = extra_name

    return holidays


def get_us_holiday_name(d: date) -> str | None:
    """Return the NYSE holiday name if date d is a full-day US market holiday."""
    return get_us_market_holidays(d.year).get(d)


def is_us_market_holiday(d: date) -> bool:
    """Return True if date d is an observed NYSE full-day holiday."""
    return d in get_us_market_holidays(d.year)


def is_us_trading_day(d: date) -> bool:
    """Return True if date d is a weekday and not an NYSE full-day holiday."""
    return d.weekday() < 5 and not is_us_market_holiday(d)


def _to_utc(dt: datetime | None) -> datetime:
    """Normalize an instant to UTC; naive datetime objects are interpreted as UTC."""
    if dt is None:
        return datetime.now(TZ_UTC)
    if dt.tzinfo is None:
        return dt.replace(tzinfo=TZ_UTC)
    return dt.astimezone(TZ_UTC)


EARLY_CLOSE_DATES = {
    date(2026, 11, 27), date(2026, 12, 24),
    date(2027, 11, 26), date(2028, 7, 3), date(2028, 11, 24),
}


def get_market_close(d: date) -> time:
    return time(13, 0) if d in EARLY_CLOSE_DATES else time(16, 0)


def get_market_session(
    dt: datetime | None = None,
) -> Literal["PRE_MARKET", "INTRADAY", "AFTER_HOURS", "OFF_HOURS", "HOLIDAY"]:
    """Classify an instant into the live US market session in America/New_York.

    Naive ``datetime`` inputs are interpreted as UTC; ``None`` uses the current UTC time.
    """
    et = _to_utc(dt).astimezone(TZ_ET)
    if is_us_market_holiday(et.date()):
        return "HOLIDAY"
    if et.weekday() >= 5:
        return "OFF_HOURS"
    t = et.time()
    if time(4, 0) <= t < time(9, 30):
        return "PRE_MARKET"
    close = get_market_close(et.date())
    if time(9, 30) <= t < close:
        return "INTRADAY"
    if close <= t < (time(17, 0) if close.hour == 13 else time(20, 0)):
        return "AFTER_HOURS"
    return "OFF_HOURS"


def get_research_window(
    dt: datetime | None = None,
) -> Literal[
    "HOLIDAY_LOCK",
    "INTRADAY_LOCK",
    "POST_CLOSE_SETTLE",
    "REBALANCE_WINDOW",
    "PRE_MARKET",
    "OFF_HOURS",
]:
    """Classify an instant into the research state-machine governance window.

    Naive ``datetime`` inputs are interpreted as UTC; ``None`` uses the current UTC time.
    """
    et = _to_utc(dt).astimezone(TZ_ET)
    if is_us_market_holiday(et.date()):
        return "HOLIDAY_LOCK"
    if et.weekday() >= 5:
        return "OFF_HOURS"
    t = et.time()
    close_hour = get_market_close(et.date()).hour
    if time(9, 30) <= t < time(close_hour - 1, 30):
        return "INTRADAY_LOCK"
    if time(close_hour - 1, 30) <= t < time(close_hour, 30):
        return "POST_CLOSE_SETTLE"
    if time(close_hour, 30) <= t < time(close_hour + 1, 0):
        return "REBALANCE_WINDOW"
    if time(4, 0) <= t < time(9, 30):
        return "PRE_MARKET"
    return "OFF_HOURS"


def format_cn_timestamp(dt: datetime | None = None) -> str:
    """Format timestamp in Asia/Shanghai as 'YYYY/MM/DD HH:MM:SS UTC+8'.

    Naive ``datetime`` inputs are interpreted as UTC; ``None`` uses the current UTC time.
    """
    return _to_utc(dt).astimezone(TZ_CN).strftime("%Y/%m/%d %H:%M:%S UTC+8")


def format_et_short(dt: datetime | None = None) -> str:
    """Format timestamp in America/New_York as 'MM-DD HH:MM ET'.

    Naive ``datetime`` inputs are interpreted as UTC; ``None`` uses the current UTC time.
    """
    return _to_utc(dt).astimezone(TZ_ET).strftime("%m-%d %H:%M ET")


def format_utc_iso(dt: datetime | None = None) -> str:
    """Format timestamp in UTC as 'YYYY-MM-DDTHH:MM:SSZ'.

    Naive ``datetime`` inputs are interpreted as UTC; ``None`` uses the current UTC time.
    """
    return _to_utc(dt).strftime("%Y-%m-%dT%H:%M:%SZ")


SCHEDULE_BASELINE_CRON = "13 */2 * * *"
SCHEDULE_ACTIVE_ODD_CRON = "13 1,3,5,7,9,11,13,15,17,19,21,23 * * *"
SCHEDULE_CROSS_MARKET_CRON = "40 6,8 * * 1-5"
SCHEDULE_REBALANCE_CRON = "32 13,16 * * 1-5"


@dataclass(frozen=True)
class ScheduleSlotDecision:
    """Resolved CI schedule trigger decision (stdlib-only for pre-install step)."""

    run: bool
    catchup_rebalance: bool
    session: Literal["PRE_MARKET", "INTRADAY", "AFTER_HOURS", "OFF_HOURS", "HOLIDAY"]
    window: Literal[
        "HOLIDAY_LOCK",
        "INTRADAY_LOCK",
        "POST_CLOSE_SETTLE",
        "REBALANCE_WINDOW",
        "PRE_MARKET",
        "OFF_HOURS",
    ]
    reason: str


def resolve_research_schedule(
    event_name: str,
    schedule: str,
    dt: datetime | None = None,
) -> ScheduleSlotDecision:
    """Resolve whether a CI trigger should execute and whether delayed rebalance catchup applies.

    Naive ``datetime`` inputs are interpreted as UTC; ``None`` uses the current UTC time.
    """
    now_utc = _to_utc(dt)
    et = now_utc.astimezone(TZ_ET)
    session = get_market_session(now_utc)
    window = get_research_window(now_utc)
    sched = (schedule or "").strip()

    if event_name == "workflow_dispatch":
        return ScheduleSlotDecision(True, False, session, window, "workflow_dispatch")

    if sched == SCHEDULE_BASELINE_CRON:
        return ScheduleSlotDecision(True, False, session, window, "baseline_2h")

    if sched == SCHEDULE_CROSS_MARKET_CRON:
        return ScheduleSlotDecision(True, False, session, window, "cross_market_close_open")

    if sched == SCHEDULE_REBALANCE_CRON:
        if not is_us_trading_day(et.date()):
            return ScheduleSlotDecision(False, False, session, window, "non_trading_day")
        if window == "REBALANCE_WINDOW":
            return ScheduleSlotDecision(True, False, session, window, "rebalance_window")
        close_h = get_market_close(et.date()).hour
        # Tolerate up to ~133 min of CI cron queue delay after close_h:32 ET,
        # while ending strictly before 16:32 ET on 13:00 ET early-close days.
        if time(close_h + 1, 0) <= et.time() < time(close_h + 2, 45):
            return ScheduleSlotDecision(True, True, session, window, "rebalance_catchup")
        return ScheduleSlotDecision(False, False, session, window, "outside_rebalance_window")

    if session in {"PRE_MARKET", "INTRADAY", "AFTER_HOURS"}:
        return ScheduleSlotDecision(True, False, session, window, "active_us_hourly")
    return ScheduleSlotDecision(False, False, session, window, "off_hours_skip")
