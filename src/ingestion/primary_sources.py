"""Primary L1/L2 signal fetchers: SEC EDGAR filings, 5-dim macro proxies, and holiday-safe calendar."""

from __future__ import annotations

import asyncio
import calendar as py_calendar
import logging
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from typing import Any

import feedparser
import httpx

from src.config import (
    FEED_SOURCES,
    SEC_TRACKED_CIKS,
    SEC_USER_AGENT,
    TZ_ET,
    get_us_holiday_name,
    is_us_market_holiday,
    is_us_trading_day,
)
from src.ingestion.market_data import MarketSnapshot
from src.models.schemas import CalendarEvent, MacroChainId, MacroDimItem

logger = logging.getLogger(__name__)

_HIGH_SIGNAL_FORMS = {"8-K", "10-Q", "10-K", "S-1", "425", "SC 13D", "SC 13G", "6-K"}


@dataclass
class SecFilingSignal:
    """Verified SEC EDGAR filing record."""

    sym: str
    company: str
    cik: str
    form: str
    filing_date: str
    accession_number: str
    items: str
    url: str


@dataclass
class PrimarySignals:
    """L1/L2 primary signals bundle for the research engine."""

    fetched_at_utc: datetime
    sec_filings: list[SecFilingSignal] = field(default_factory=list)
    edgar_stream: list[dict[str, str]] = field(default_factory=list)
    macro_proxies: list[MacroDimItem] = field(default_factory=list)
    calendar_events: list[CalendarEvent] = field(default_factory=list)
    apac_eu_summary: dict[str, dict[str, Any]] = field(default_factory=dict)


def _format_metric(val: float | None, suffix: str = "", digits: int = 2) -> str:
    if val is None:
        return "DATA_PENDING"
    return f"{val:.{digits}f}{suffix}"


def _format_chg(val: float | None) -> str:
    if val is None:
        return "DATA_PENDING"
    sign = "+" if val >= 0 else ""
    return f"{sign}{val:.2f}%"


def _clamp_score(val: float) -> int:
    return int(max(10, min(95, round(val))))


async def _fetch_cik_submissions(
    client: httpx.AsyncClient,
    sem: asyncio.Semaphore,
    sym: str,
    cik: str,
    cutoff_date: date,
) -> list[SecFilingSignal]:
    """Fetch recent high-signal filings for one CIK from data.sec.gov."""
    url = FEED_SOURCES["sec_edgar_submissions"].url.format(cik10=cik)
    async with sem:
        for attempt in range(2):
            try:
                resp = await client.get(
                    url,
                    headers={"User-Agent": SEC_USER_AGENT, "Accept": "application/json"},
                )
                if resp.status_code == 200:
                    data = resp.json()
                    company = str(data.get("name") or sym).strip()
                    recent = (data.get("filings") or {}).get("recent") or {}
                    forms = recent.get("form") or []
                    dates = recent.get("filingDate") or []
                    accs = recent.get("accessionNumber") or []
                    items_list = recent.get("items") or []

                    out: list[SecFilingSignal] = []
                    for form, fdate, acc, items in zip(forms, dates, accs, items_list):
                        form_str = str(form).strip()
                        if form_str not in _HIGH_SIGNAL_FORMS:
                            continue
                        try:
                            dt = date.fromisoformat(str(fdate)[:10])
                        except ValueError:
                            continue
                        if dt < cutoff_date:
                            continue
                        acc_str = str(acc).strip()
                        acc_nodash = acc_str.replace("-", "")
                        cik_int = str(int(cik))
                        filing_url = (
                            f"https://www.sec.gov/Archives/edgar/data/"
                            f"{cik_int}/{acc_nodash}/{acc_str}-index.htm"
                        )
                        items_str = (
                            ",".join(str(x) for x in items)
                            if isinstance(items, list)
                            else str(items or "")
                        )
                        out.append(
                            SecFilingSignal(
                                sym=sym,
                                company=company,
                                cik=cik,
                                form=form_str,
                                filing_date=dt.isoformat(),
                                accession_number=acc_str,
                                items=items_str,
                                url=filing_url,
                            )
                        )
                        if len(out) >= 3:
                            break
                    return out
            except Exception as exc:
                logger.debug("SEC submissions %s attempt %d failed: %s", sym, attempt + 1, exc)
            if attempt == 0:
                await asyncio.sleep(0.35)
    return []


async def _fetch_edgar_current_atom(client: httpx.AsyncClient) -> list[dict[str, str]]:
    """Fetch recent market-wide 8-K / S-1 / 425 / 13D entries from SEC Atom feed."""
    url = FEED_SOURCES["sec_edgar_current_rss"].url
    try:
        resp = await client.get(url, headers={"User-Agent": SEC_USER_AGENT})
        if resp.status_code != 200:
            return []
        parsed = feedparser.parse(resp.text)
        out: list[dict[str, str]] = []
        for entry in parsed.entries[:25]:
            title = str(getattr(entry, "title", "") or "").strip()
            link = str(getattr(entry, "link", "") or "").strip()
            updated = str(getattr(entry, "updated", "") or "").strip()
            if not title or not link.startswith("http"):
                continue
            if any(title.startswith(f"{f} ") or f"({f})" in title for f in ("8-K", "425", "S-1", "SC 13D")):
                out.append({"title": title, "link": link, "updated": updated[:10]})
            if len(out) >= 8:
                break
        return out
    except Exception as exc:
        logger.debug("SEC current Atom feed failed: %s", exc)
        return []


def build_macro_proxies(market: MarketSnapshot, now_utc: datetime) -> list[MacroDimItem]:
    """Derive 5 quantitative macro & sector chain proxies directly from live market data."""
    et_date = now_utc.astimezone(TZ_ET).date().isoformat()

    def _q(sym: str) -> tuple[float | None, float | None, float | None, float | None, str]:
        q = market.get_quote(sym)
        if not q:
            return None, None, None, None, et_date
        return q.price, q.chg, q.r5, q.r20, q.tsUtc[:10]

    tnx_p, tnx_chg, tnx_r5, _, tnx_d = _q("^TNX")
    tlt_p, tlt_chg, tlt_r5, _, tlt_d = _q("TLT")
    move_p, move_chg, _, _, move_d = _q("^MOVE")
    hyg_p, hyg_chg, hyg_r5, _, hyg_d = _q("HYG")
    lqd_p, lqd_chg, lqd_r5, _, _ = _q("LQD")
    vix_p, vix_chg, vix_r5, _, vix_d = _q("^VIX")
    btc_p, btc_chg, btc_r5, _, btc_d = _q("BTC-USD")
    iren_p, _, iren_r5, iren_r20, iren_d = _q("IREN")
    xlu_p, _, xlu_r5, xlu_r20, xlu_d = _q("XLU")
    nvda_p, nvda_chg, nvda_r5, nvda_r20, nvda_d = _q("NVDA")
    soxx_p, soxx_chg, soxx_r5, soxx_r20, soxx_d = _q("SOXX")

    # 1) Rate & Bond Volatility
    if tnx_p is None:
        rate_score, rate_state, rate_wind = 50, "DATA_PENDING", "中性"
    elif tnx_p < 4.25 and (tlt_r5 or 0.0) >= 0:
        rate_score, rate_state, rate_wind = _clamp_score(72 - (tnx_p - 4.0) * 15), "长端利率平稳", "顺风"
    elif tnx_p >= 4.55 or (tnx_r5 or 0.0) > 3.0:
        rate_score, rate_state, rate_wind = _clamp_score(42 - (tnx_p - 4.5) * 20), "利率上行压制估值", "逆风"
    else:
        rate_score, rate_state, rate_wind = _clamp_score(58 - (tnx_p - 4.2) * 12), "区间拉锯待催化", "中性"

    rate_item = MacroDimItem(
        chain_id="rate",
        title="利率与美债波动",
        state=rate_state,
        wind=rate_wind,
        score=rate_score,
        feature=f"10Y收益率 {_format_metric(tnx_p, '%')}（5D {_format_chg(tnx_r5)}），TLT 5D {_format_chg(tlt_r5)}",
        concern="若10Y突破4.5%且MOVE走高，高估值成长股折现率将再度承压。" if tnx_p is not None else "DATA_PENDING：利率代理暂缺",
        plain=f"10Y美债当前报 {_format_metric(tnx_p, '%')}，对应长债ETF TLT 5日变化 {_format_chg(tlt_r5)}，决定科技股估值分母松紧。",
        m1_label="10Y 美债 (^TNX)",
        m1_val=_format_metric(tnx_p, "%"),
        m1_date=tnx_d,
        m1_sub=f"日涨跌 {_format_chg(tnx_chg)} | 5D {_format_chg(tnx_r5)}",
        m2_label="债市波动 (^MOVE / TLT)",
        m2_val=_format_metric(move_p, "", 1) if move_p is not None else _format_metric(tlt_p, "$"),
        m2_date=move_d if move_p is not None else tlt_d,
        m2_sub=f"MOVE {_format_chg(move_chg)} | TLT {_format_chg(tlt_chg)}",
        source_url="https://fred.stlouisfed.org/series/DGS10",
    )

    # 2) Credit Spread & Dollar Liquidity
    hyg_lqd_spread_5d = (hyg_r5 - lqd_r5) if (hyg_r5 is not None and lqd_r5 is not None) else None
    if hyg_p is None:
        credit_score, credit_state, credit_wind = 50, "DATA_PENDING", "中性"
    elif (hyg_lqd_spread_5d or 0.0) >= 0.3 and (hyg_chg or 0.0) >= -0.3:
        credit_score, credit_state, credit_wind = _clamp_score(68 + (hyg_lqd_spread_5d or 0.0) * 8), "信用利差收敛", "顺风"
    elif (hyg_lqd_spread_5d or 0.0) <= -0.8:
        credit_score, credit_state, credit_wind = _clamp_score(38 + (hyg_lqd_spread_5d or 0.0) * 8), "高收益债相对走弱", "逆风"
    else:
        credit_score, credit_state, credit_wind = 56, "信用环境温和", "中性"

    dxy_p, dxy_chg, dxy_r5, _, dxy_d = _q("DX-Y.NYB")
    credit_item = MacroDimItem(
        chain_id="credit",
        title="信用利差与美元流动性",
        state=credit_state,
        wind=credit_wind,
        score=credit_score,
        feature=f"HYG 5D {_format_chg(hyg_r5)} vs LQD 5D {_format_chg(lqd_r5)}，DXY {_format_metric(dxy_p)}",
        concern="高收益债若跑输投资级债且美元走强，往往领先风险资产去杠杆。",
        plain=f"用高收益债HYG与投资级债LQD强弱差（5D差值 {_format_chg(hyg_lqd_spread_5d)}）观测企业融资信用是否健康。",
        m1_label="高收益债 (HYG)",
        m1_val=_format_metric(hyg_p, "$"),
        m1_date=hyg_d,
        m1_sub=f"5D {_format_chg(hyg_r5)} | HYG-LQD {_format_chg(hyg_lqd_spread_5d)}",
        m2_label="美元指数 (DXY)",
        m2_val=_format_metric(dxy_p),
        m2_date=dxy_d,
        m2_sub=f"日涨跌 {_format_chg(dxy_chg)} | 5D {_format_chg(dxy_r5)}",
        source_url="https://fred.stlouisfed.org/series/BAMLH0A0HYM2",
    )

    # 3) Risk Sentiment & Crypto Liquidity
    if vix_p is None:
        sent_score, sent_state, sent_wind = 50, "DATA_PENDING", "中性"
    elif vix_p < 16.5:
        sent_score, sent_state, sent_wind = _clamp_score(78 - (vix_p - 12.0) * 3), "低波风险偏好", "顺风"
    elif vix_p >= 22.0:
        sent_score, sent_state, sent_wind = _clamp_score(36 - (vix_p - 22.0) * 2), "避险情绪升温", "逆风"
    else:
        sent_score, sent_state, sent_wind = _clamp_score(62 - (vix_p - 16.5) * 3), "情绪中性偏谨慎", "中性"

    sent_item = MacroDimItem(
        chain_id="sentiment",
        title="恐慌情绪与风险偏好",
        state=sent_state,
        wind=sent_wind,
        score=sent_score,
        feature=f"VIX {_format_metric(vix_p)}（5D {_format_chg(vix_r5)}），BTC {_format_metric(btc_p, '$', 0)}",
        concern="VIX若站上20上方配合加密资产回撤，需防范动量抱团仓位被动降波。",
        plain=f"VIX报 {_format_metric(vix_p)} 衡量美股隐含波动压力，比特币 5D {_format_chg(btc_r5)} 同步反映高风险流动性水温。",
        m1_label="恐慌指数 (VIX)",
        m1_val=_format_metric(vix_p),
        m1_date=vix_d,
        m1_sub=f"日涨跌 {_format_chg(vix_chg)} | 5D {_format_chg(vix_r5)}",
        m2_label="比特币 (BTC-USD)",
        m2_val=_format_metric(btc_p, "$", 0),
        m2_date=btc_d,
        m2_sub=f"日涨跌 {_format_chg(btc_chg)} | 5D {_format_chg(btc_r5)}",
        source_url="https://www.cboe.com/tradable_products/vix/",
    )

    # 4) Compute Infrastructure & Power Chain
    if iren_r5 is None and xlu_r5 is None:
        comp_score, comp_state, comp_wind = 50, "DATA_PENDING", "中性"
    else:
        avg_r5 = ((iren_r5 or 0.0) + (xlu_r5 or 0.0)) / 2.0
        comp_score = _clamp_score(58 + avg_r5 * 2.5)
        comp_wind = "顺风" if avg_r5 > 1.0 else ("逆风" if avg_r5 < -2.0 else "中性")
        comp_state = "算力基建扩张" if comp_wind == "顺风" else ("电算高波整固" if comp_wind == "逆风" else "电算分化轮动")

    comp_item = MacroDimItem(
        chain_id="compute",
        title="AI 电力与新云基建",
        state=comp_state,
        wind=comp_wind,
        score=comp_score,
        feature=f"IREN 20D {_format_chg(iren_r20)}，XLU 公用电力 20D {_format_chg(xlu_r20)}",
        concern="高Beta矿场转算力标的对融资利率敏感，若资本开支节奏放缓将放大回撤。",
        plain=f"追踪IREN/NBIS/CRWV与公用电力XLU的强弱，观察AI数据中心供电与机柜交付链条景气度。",
        m1_label="新云基建 (IREN)",
        m1_val=_format_metric(iren_p, "$"),
        m1_date=iren_d,
        m1_sub=f"5D {_format_chg(iren_r5)} | 20D {_format_chg(iren_r20)}",
        m2_label="公用电力 (XLU)",
        m2_val=_format_metric(xlu_p, "$"),
        m2_date=xlu_d,
        m2_sub=f"5D {_format_chg(xlu_r5)} | 20D {_format_chg(xlu_r20)}",
        source_url="https://www.sec.gov/edgar/searchedgar/companysearch",
    )

    # 5) Semiconductor & AI Hardware Demand
    if soxx_r5 is None and nvda_r5 is None:
        semi_score, semi_state, semi_wind = 50, "DATA_PENDING", "中性"
    else:
        semi_avg = ((soxx_r5 or 0.0) + (nvda_r5 or 0.0)) / 2.0
        semi_score = _clamp_score(60 + semi_avg * 2.5)
        semi_wind = "顺风" if semi_avg > 0.8 else ("逆风" if semi_avg < -2.0 else "中性")
        semi_state = "芯片链条领涨" if semi_wind == "顺风" else ("高位分歧整固" if semi_wind == "逆风" else "晶圆算力结构分化")

    semi_item = MacroDimItem(
        chain_id="semi_demand",
        title="半导体与光通信需求",
        state=semi_state,
        wind=semi_wind,
        score=semi_score,
        feature=f"NVDA 5D {_format_chg(nvda_r5)} / 20D {_format_chg(nvda_r20)}，SOXX 5D {_format_chg(soxx_r5)}",
        concern="若龙头财报后出现放量滞涨或亚太晶圆链条走弱，说明短期预期打满。",
        plain=f"以NVDA与半导体ETF SOXX为核心锚点，联合LITE光通信及台韩权重评估算力硬件真实买盘。",
        m1_label="算力核心 (NVDA)",
        m1_val=_format_metric(nvda_p, "$"),
        m1_date=nvda_d,
        m1_sub=f"5D {_format_chg(nvda_r5)} | 20D {_format_chg(nvda_r20)}",
        m2_label="半导体 (SOXX)",
        m2_val=_format_metric(soxx_p, "$"),
        m2_date=soxx_d,
        m2_sub=f"5D {_format_chg(soxx_r5)} | 20D {_format_chg(soxx_r20)}",
        source_url="https://www.sec.gov/edgar/browse/?CIK=0001045810",
    )

    return [rate_item, credit_item, sent_item, comp_item, semi_item]


def _next_valid_trading_day(start_day: date) -> date:
    d = start_day
    while not is_us_trading_day(d):
        d += timedelta(days=1)
    return d


def _first_friday_of_month(year: int, month: int) -> date:
    first = date(year, month, 1)
    offset = (4 - first.weekday()) % 7
    return _next_valid_trading_day(first + timedelta(days=offset))


def _first_business_day_of_month(year: int, month: int) -> date:
    return _next_valid_trading_day(date(year, month, 1))


def _mid_month_cpi_window(year: int, month: int) -> date:
    # CPI typically releases on a Tue/Wed/Thu around the 11th–15th trading day window.
    candidate = date(year, month, 12)
    while not is_us_trading_day(candidate) or candidate.weekday() not in (1, 2, 3):
        candidate += timedelta(days=1)
    return candidate


def _late_month_pce_window(year: int, month: int) -> date:
    last_dom = py_calendar.monthrange(year, month)[1]
    candidate = date(year, month, min(28, last_dom))
    while not is_us_trading_day(candidate):
        candidate -= timedelta(days=1)
    return candidate


def build_holiday_safe_calendar(
    now_utc: datetime,
    sec_filings: list[SecFilingSignal],
) -> list[CalendarEvent]:
    """Build an upcoming US trading-day-validated macro & catalyst calendar."""
    today_et = now_utc.astimezone(TZ_ET).date()
    events: list[CalendarEvent] = []

    holiday_name = get_us_holiday_name(today_et)
    if holiday_name:
        reopen_day = _next_valid_trading_day(today_et + timedelta(days=1))
        events.append(
            CalendarEvent(
                day=reopen_day.isoformat(),
                time="22:30 UTC+8",
                title=f"美股节后恢复开盘（{holiday_name}次日交易）",
                tag="全市场流动性恢复",
                tone="slate",
                desc=f"今日（{today_et.isoformat()}）为 {holiday_name} 全日休市；下一合法交易日为 {reopen_day.isoformat()}，注意节后早盘流动性缺口再平衡。",
                source_url="https://www.nyse.com/markets/hours-calendars",
            )
        )

    # Upcoming verified US macro release cadence (strictly placed on valid NYSE trading days).
    cur_y, cur_m = today_et.year, today_et.month
    next_y, next_m = (cur_y + 1, 1) if cur_m == 12 else (cur_y, cur_m + 1)

    cadence: list[tuple[date, str, str, str, str, str, str]] = []
    for yr, mo in ((cur_y, cur_m), (next_y, next_m)):
        cadence.append((
            _late_month_pce_window(yr, mo),
            "21:30 UTC+8",
            "美国 PCE 通胀与个人消费支出",
            "影响 M1/M3 · 利率 / 消费主线",
            "ochre",
            "核心PCE决定美联储降息空间与10Y美债方向，直接传导至高估值算力与地产链。",
            "https://www.bea.gov/news/schedule",
        ))
        cadence.append((
            _first_business_day_of_month(yr, mo),
            "23:00 UTC+8",
            "ISM 制造业 PMI",
            "影响 M3 · 宏观景气 / 工业铜",
            "sage",
            "观察新订单与价格分项是否共振走强，验证顺周期与工业金属需求。",
            "https://www.ismworld.org/supply-management-news-and-reports/reports/ism-report-on-business/",
        ))
        cadence.append((
            _first_friday_of_month(yr, mo),
            "21:30 UTC+8",
            "美国非农就业与失业率 (NFP)",
            "影响 M1/M3 · 利率 / 降息定价",
            "terra",
            "就业韧性与薪资增速若超预期将推高10Y美债收益率，考验风险资产估值。",
            "https://www.bls.gov/schedule/news_release/empsit.htm",
        ))
        cadence.append((
            _mid_month_cpi_window(yr, mo),
            "21:30 UTC+8",
            "美国 CPI 消费者物价指数",
            "影响 M3 · 利率 / 黄金 / 长债",
            "ochre",
            "观察住房与核心服务通胀粘性，对TLT、黄金及纳指敏感度最高。",
            "https://www.bls.gov/schedule/news_release/cpi.htm",
        ))

    # Include any near-term SEC filing review checkpoint on the next trading day if fresh.
    if sec_filings:
        latest_sec = sec_filings[0]
        check_day = _next_valid_trading_day(today_et + timedelta(days=1))
        sec_m_tag = "M5" if latest_sec.sym in ("COIN", "MSTR") else "M1"
        events.append(
            CalendarEvent(
                day=check_day.isoformat(),
                time="21:00 UTC+8",
                title=f"SEC 披露跟踪：{latest_sec.sym} {latest_sec.form}（{latest_sec.filing_date}）",
                tag=f"{sec_m_tag} 一手公告核验",
                tone="sage",
                desc=f"最近抓取到 {latest_sec.company} ({latest_sec.sym}) 提交 {latest_sec.form}（Items: {latest_sec.items or '常规披露'}），开盘前后跟踪机构解读与量价承接。",
                source_url=latest_sec.url,
            )
        )

    seen_titles: set[tuple[str, str]] = set()
    for ev_day, ev_time, title, tag, tone, desc, url in sorted(cadence, key=lambda x: x[0]):
        if ev_day <= today_et:
            continue
        if is_us_market_holiday(ev_day) or ev_day.weekday() >= 5:
            continue
        key = (ev_day.isoformat(), title)
        if key in seen_titles:
            continue
        seen_titles.add(key)
        events.append(
            CalendarEvent(
                day=ev_day.isoformat(),
                time=ev_time,
                title=title,
                tag=tag,
                tone=tone,  # type: ignore[arg-type]
                desc=desc,
                source_url=url,
            )
        )
        if len(events) >= 6:
            break

    return events


async def fetch_primary_signals(
    *,
    market: MarketSnapshot,
    now_utc: datetime | None = None,
    timeout_sec: float = 12.0,
) -> PrimarySignals:
    """Fetch L1 SEC EDGAR filings, compute 5-dim macro proxies, and build holiday-safe calendar."""
    ts_now = now_utc or datetime.now(timezone.utc)
    if ts_now.tzinfo is None:
        ts_now = ts_now.replace(tzinfo=timezone.utc)

    cutoff_date = ts_now.astimezone(TZ_ET).date() - timedelta(days=45)
    sem = asyncio.Semaphore(2)

    async with httpx.AsyncClient(timeout=timeout_sec, follow_redirects=True) as client:
        sec_tasks = [
            _fetch_cik_submissions(client, sem, sym, cik, cutoff_date)
            for sym, cik in SEC_TRACKED_CIKS.items()
        ]
        atom_task = asyncio.create_task(_fetch_edgar_current_atom(client))
        sec_batches = await asyncio.gather(*sec_tasks)
        edgar_stream = await atom_task

    sec_filings = sorted(
        [f for batch in sec_batches for f in batch],
        key=lambda f: f.filing_date,
        reverse=True,
    )

    macro_proxies = build_macro_proxies(market, ts_now)
    calendar_events = build_holiday_safe_calendar(ts_now, sec_filings)

    apac_eu_summary: dict[str, dict[str, Any]] = {}
    for sym in ("^N225", "^KS11", "^TWII", "^HSI", "000001.SS", "^GDAXI", "^STOXX50E", "ASML"):
        q = market.get_quote(sym)
        if q:
            apac_eu_summary[sym] = {
                "name": q.name,
                "price": q.price,
                "chg": q.chg,
                "r5": q.r5,
                "r20": q.r20,
            }

    return PrimarySignals(
        fetched_at_utc=ts_now,
        sec_filings=sec_filings,
        edgar_stream=edgar_stream,
        macro_proxies=macro_proxies,
        calendar_events=calendar_events,
        apac_eu_summary=apac_eu_summary,
    )
