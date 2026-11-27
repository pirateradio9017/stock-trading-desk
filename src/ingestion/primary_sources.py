"""Primary L1/L2 signal fetchers: SEC EDGAR filings, 5-dim macro proxies, and holiday-safe calendar."""

from __future__ import annotations

import asyncio
import logging
import math
import re
import warnings
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from typing import Any

import feedparser
import httpx
from bs4 import BeautifulSoup, XMLParsedAsHTMLWarning

from src.config import (
    EARLY_CLOSE_DATES,
    FEED_SOURCES,
    SEC_TRACKED_CIKS,
    SEC_USER_AGENT,
    TZ_ET,
    TZ_CN,
    get_market_close,
    get_us_holiday_name,
    is_us_market_holiday,
    is_us_trading_day,
)
from src.ingestion.market_data import MarketSnapshot
from src.models.schemas import CalendarEvent, MacroChainId, MacroDimItem

logger = logging.getLogger(__name__)

_HIGH_SIGNAL_FORMS = {"8-K", "10-Q", "10-K", "S-1", "425", "SC 13D", "SC 13G", "6-K"}
_FRED_SERIES: tuple[str, ...] = ("BAMLH0A0HYM2", "BAMLH0A0HYM2EY", "DGS10", "MORTGAGE30US")
_SEC_ITEM_START_RE = re.compile(r"\bItem\s+[1-9]\.\d{2}\b", re.IGNORECASE)

# Explicitly verified official upcoming events on valid US trading days (auto-gated by date window).
# Monthly CPI/PCE/NFP releases are never heuristic-guessed here; only official fixed calendars are listed.
_VERIFIED_OFFICIAL_EVENTS: tuple[tuple[date, str, str, str, str, str, str], ...] = (
    (
        date(2026, 11, 30),
        "09:05 UTC+8 / 周日 OPEC+ 落地",
        "OPEC+ 2027 产量会议落地 & 日央行行长植田和男名古屋演讲",
        "能源原油 / 日元套息",
        "ochre",
        "周日 11/29 OPEC+ 部长级会议决定 2027 产量基线（影响布油/XLE），周一 10:05 JST（09:05 UTC+8）日央行行长植田和男发表讲话，检验 12 月加息预期与日元套息传导。",
        "https://www.boj.or.jp/en/about/press/koen_2026/index.htm",
    ),
    (
        date(2026, 12, 9),
        "14:00 ET (次日 03:00 UTC+8)",
        "美联储 12 月 FOMC 利率决议与经济预测点阵图 (SEP)",
        "L1 央行议息",
        "terra",
        "美联储 12 月 8–9 日召开年度收官 FOMC 议息会议，14:00 ET 公布利率决议及季度 SEP 点阵图，14:30 ET 举行新闻发布会，直接重定价 10Y 美债与成长股估值分母。",
        "https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm",
    ),
)


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
    excerpt: str = ""


@dataclass
class PrimarySignals:
    """L1/L2 primary signals bundle for the research engine."""

    fetched_at_utc: datetime
    sec_filings: list[SecFilingSignal] = field(default_factory=list)
    edgar_stream: list[dict[str, str]] = field(default_factory=list)
    macro_proxies: list[MacroDimItem] = field(default_factory=list)
    calendar_events: list[CalendarEvent] = field(default_factory=list)
    apac_eu_summary: dict[str, dict[str, Any]] = field(default_factory=dict)
    fred_metrics: dict[str, tuple[str, float]] = field(default_factory=dict)


def _format_metric(val: float | None, suffix: str = "", digits: int = 2) -> str:
    if val is None or not math.isfinite(val):
        return "DATA_PENDING"
    if suffix == "$":
        if digits == 0 and abs(val) >= 1000:
            return f"${val:,.0f}"
        return f"${val:.{digits}f}"
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
                    primary_docs = recent.get("primaryDocument") or [""] * len(forms)

                    out: list[SecFilingSignal] = []
                    for form, fdate, acc, items, pdoc in zip(forms, dates, accs, items_list, primary_docs):
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
                        excerpt = ""
                        pdoc_str = str(pdoc or "").strip()
                        if not out and pdoc_str and form_str in ("8-K", "6-K") and pdoc_str.lower().endswith((".htm", ".html", ".txt")):
                            excerpt = await _fetch_sec_doc_excerpt(
                                client,
                                f"https://www.sec.gov/Archives/edgar/data/{cik_int}/{acc_nodash}/{pdoc_str}",
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
                                excerpt=excerpt,
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


async def _fetch_sec_doc_excerpt(client: httpx.AsyncClient, doc_url: str) -> str:
    """Fetch a bounded first-hand text excerpt from a recent SEC 8-K/6-K primary document."""
    try:
        resp = await client.get(
            doc_url,
            headers={"User-Agent": SEC_USER_AGENT, "Accept": "text/html,text/plain,*/*"},
            timeout=6.0,
        )
        if resp.status_code != 200 or not resp.text:
            return ""
        raw_head = resp.text[:65000]
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)
            clean = " ".join(BeautifulSoup(raw_head, "lxml").get_text(" ").split())
        match = _SEC_ITEM_START_RE.search(clean)
        snippet = clean[match.start():] if match else clean
        snippet = re.sub(r"\s+", " ", snippet).strip()
        return snippet[:420] + ("…" if len(snippet) > 420 else "")
    except Exception as exc:
        logger.debug("SEC document excerpt fetch failed (%s): %s", doc_url, exc)
        return ""


async def _fetch_fred_metrics(client: httpx.AsyncClient) -> dict[str, tuple[str, float]]:
    """Fetch official latest observations from public FRED CSV endpoints (no API key)."""
    sem = asyncio.Semaphore(4)

    async def _one(series_id: str) -> tuple[str, tuple[str, float] | None]:
        url = f"https://fred.stlouisfed.org/graph/fredgraph.csv?id={series_id}"
        try:
            async with sem:
                resp = await client.get(url, headers={"User-Agent": SEC_USER_AGENT}, timeout=6.0)
            if resp.status_code != 200:
                return series_id, None
            for line in reversed(resp.text.strip().splitlines()):
                parts = line.split(",")
                if len(parts) != 2 or parts[0] == "observation_date":
                    continue
                obs_date, raw_val = parts[0].strip(), parts[1].strip()
                if not raw_val or raw_val == ".":
                    continue
                val = float(raw_val)
                if math.isfinite(val):
                    return series_id, (obs_date, round(val, 2))
            return series_id, None
        except Exception as exc:
            logger.debug("FRED %s fetch failed: %s", series_id, exc)
            return series_id, None

    pairs = await asyncio.gather(*(_one(sid) for sid in _FRED_SERIES))
    return {sid: obs for sid, obs in pairs if obs is not None}


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


def build_macro_proxies(
    market: MarketSnapshot,
    now_utc: datetime,
    fred_metrics: dict[str, tuple[str, float]] | None = None,
) -> list[MacroDimItem]:
    """Derive 5 quantitative macro & sector chain dimensions from live market + FRED L1 data."""
    et_date = now_utc.astimezone(TZ_ET).date().isoformat()
    fred = fred_metrics or {}

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

    fred_hy_oas = fred.get("BAMLH0A0HYM2")
    fred_hy_ey = fred.get("BAMLH0A0HYM2EY")
    fred_mort = fred.get("MORTGAGE30US")

    # 1) Rate & Bond Volatility
    if tnx_p is None:
        rate_score, rate_state, rate_wind = 50, "DATA_PENDING", "中性"
    elif tnx_p < 4.25 and (tlt_r5 or 0.0) >= 0:
        rate_score, rate_state, rate_wind = _clamp_score(72 - (tnx_p - 4.0) * 15), "长端利率平稳", "顺风"
    elif tnx_p >= 4.55 or (tnx_r5 or 0.0) > 3.0:
        rate_score, rate_state, rate_wind = _clamp_score(42 - (tnx_p - 4.5) * 20), "利率上行压制估值", "逆风"
    else:
        rate_score, rate_state, rate_wind = _clamp_score(58 - (tnx_p - 4.2) * 12), "区间拉锯待催化", "中性"

    mort_clause = f"，FRED 30Y按揭 {fred_mort[1]:.2f}%（{fred_mort[0]}）" if fred_mort else ""
    rate_item = MacroDimItem(
        chain_id="rate",
        title="利率与美债波动",
        state=rate_state,
        wind=rate_wind,
        score=rate_score,
        feature=f"10Y收益率 {_format_metric(tnx_p, '%')}（5D {_format_chg(tnx_r5)}），TLT 5D {_format_chg(tlt_r5)}{mort_clause}",
        concern="若10Y突破5.0%且MOVE走高，高估值成长股折现率将再度承压。" if tnx_p is not None else "DATA_PENDING：利率代理暂缺",
        plain=f"10Y美债当前报 {_format_metric(tnx_p, '%')}，对应长债ETF TLT 5日变化 {_format_chg(tlt_r5)}，决定科技股估值分母松紧。",
        m1_label="10Y 美债 (^TNX)",
        m1_val=_format_metric(tnx_p, "%"),
        m1_date=tnx_d,
        m1_sub=f"日涨跌 {_format_chg(tnx_chg)} | 5D {_format_chg(tnx_r5)}" + (f" | 30Y按揭 {fred_mort[1]:.2f}%" if fred_mort else ""),
        m2_label="债市波动 (^MOVE / TLT)",
        m2_val=_format_metric(move_p, "", 1) if move_p is not None else _format_metric(tlt_p, "$"),
        m2_date=move_d if move_p is not None else tlt_d,
        m2_sub=f"MOVE {_format_chg(move_chg)} | TLT {_format_chg(tlt_chg)}",
        source_url="https://fred.stlouisfed.org/series/DGS10",
    )

    # 2) Credit Spread & Dollar Liquidity (uses official FRED OAS when fetched, else HYG/LQD proxy)
    hyg_lqd_spread_5d = (hyg_r5 - lqd_r5) if (hyg_r5 is not None and lqd_r5 is not None) else None
    if hyg_p is None and fred_hy_oas is None:
        credit_score, credit_state, credit_wind = 50, "DATA_PENDING", "中性"
    elif (hyg_lqd_spread_5d or 0.0) <= -0.8 or (fred_hy_oas is not None and fred_hy_oas[1] >= 4.5):
        base_pen = (hyg_lqd_spread_5d or 0.0) * 8
        oas_pen = max(0.0, (fred_hy_oas[1] - 4.0) * 10) if fred_hy_oas else 0.0
        credit_score, credit_state, credit_wind = _clamp_score(38 + base_pen - oas_pen), "高收益债相对走弱", "逆风"
    elif (hyg_lqd_spread_5d or 0.0) >= 0.3 and (hyg_chg or 0.0) >= -0.3 and (fred_hy_oas is None or fred_hy_oas[1] < 4.0):
        credit_score, credit_state, credit_wind = _clamp_score(68 + (hyg_lqd_spread_5d or 0.0) * 8), "信用利差收敛", "顺风"
    else:
        credit_score, credit_state, credit_wind = 56, "信用环境温和", "中性"

    dxy_p, dxy_chg, dxy_r5, _, dxy_d = _q("DX-Y.NYB")
    if fred_hy_oas is not None:
        ey_note = f"有效收益率 {fred_hy_ey[1]:.2f}% | " if fred_hy_ey else ""
        credit_m1_label = "FRED 高收益债 OAS"
        credit_m1_val = f"{fred_hy_oas[1]:.2f}%"
        credit_m1_date = fred_hy_oas[0]
        credit_m1_sub = f"{ey_note}HYG-LQD 5D {_format_chg(hyg_lqd_spread_5d)}"
        credit_feature = (
            f"FRED 高收益 OAS {fred_hy_oas[1]:.2f}%（{fred_hy_oas[0]}）"
            + (f" / 有效收益率 {fred_hy_ey[1]:.2f}%" if fred_hy_ey else "")
            + f"，HYG 5D {_format_chg(hyg_r5)} vs LQD 5D {_format_chg(lqd_r5)}，DXY {_format_metric(dxy_p)}"
        )
    else:
        credit_m1_label = "高收益债 (HYG)"
        credit_m1_val = _format_metric(hyg_p, "$")
        credit_m1_date = hyg_d
        credit_m1_sub = f"5D {_format_chg(hyg_r5)} | HYG-LQD {_format_chg(hyg_lqd_spread_5d)}"
        credit_feature = f"HYG 5D {_format_chg(hyg_r5)} vs LQD 5D {_format_chg(lqd_r5)}，DXY {_format_metric(dxy_p)}"

    credit_item = MacroDimItem(
        chain_id="credit",
        title="信用利差与美元流动性",
        state=credit_state,
        wind=credit_wind,
        score=credit_score,
        feature=credit_feature,
        concern="高收益债若跑输投资级债且美元走强，往往领先风险资产去杠杆。",
        plain=f"结合高收益债利差与 HYG/LQD 强弱差（5D 差值 {_format_chg(hyg_lqd_spread_5d)}）观测企业融资信用是否健康。",
        m1_label=credit_m1_label,
        m1_val=credit_m1_val,
        m1_date=credit_m1_date,
        m1_sub=credit_m1_sub,
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

    # 4) Compute Infrastructure & Power Chain (price-momentum proxy, not rental rate)
    if iren_r5 is None and xlu_r5 is None:
        comp_score, comp_state, comp_wind = 50, "DATA_PENDING", "中性"
    else:
        avg_r5 = ((iren_r5 or 0.0) + (xlu_r5 or 0.0)) / 2.0
        comp_score = _clamp_score(58 + avg_r5 * 2.5)
        comp_wind = "顺风" if avg_r5 > 1.0 else ("逆风" if avg_r5 < -2.0 else "中性")
        comp_state = "电算动量扩张" if comp_wind == "顺风" else ("电算高波整固" if comp_wind == "逆风" else "电算分化轮动")

    comp_item = MacroDimItem(
        chain_id="compute",
        title="AI 电力与新云基建动量",
        state=comp_state,
        wind=comp_wind,
        score=comp_score,
        feature=f"IREN 5D {_format_chg(iren_r5)} / 20D {_format_chg(iren_r20)}，XLU 公用电力 20D {_format_chg(xlu_r20)}（价格动量代理）",
        concern="高Beta矿场转算力标的对融资利率敏感，若资本开支节奏放缓将放大回撤。",
        plain="以 IREN 与公用电力 XLU 的 5 日价格动量差观测电算基建强弱分化（价格代理，非算力租金实测）。",
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

    # 5) Semiconductor & AI Hardware Price Momentum (explicitly price proxy, not order book)
    if soxx_r5 is None and nvda_r5 is None:
        semi_score, semi_state, semi_wind = 50, "DATA_PENDING", "中性"
    else:
        semi_avg = ((soxx_r5 or 0.0) + (nvda_r5 or 0.0)) / 2.0
        semi_score = _clamp_score(60 + semi_avg * 2.5)
        semi_wind = "顺风" if semi_avg > 0.8 else ("逆风" if semi_avg < -2.0 else "中性")
        semi_state = "芯片动量领涨" if semi_wind == "顺风" else ("高位分歧整固" if semi_wind == "逆风" else "晶圆算力结构分化")

    semi_item = MacroDimItem(
        chain_id="semi_demand",
        title="半导体与算力价格动量",
        state=semi_state,
        wind=semi_wind,
        score=semi_score,
        feature=f"NVDA 5D {_format_chg(nvda_r5)} / 20D {_format_chg(nvda_r20)}，SOXX 5D {_format_chg(soxx_r5)}（5D价格动量代理）",
        concern="若龙头财报后出现放量滞涨或亚太晶圆链条走弱，说明短期预期打满。",
        plain="以 NVDA 与半导体 ETF SOXX 的 5 日价格动量衡量硬件链盘面强弱（价格代理，不代表芯片订单或基本面评分）。",
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


def build_holiday_safe_calendar(
    now_utc: datetime,
    sec_filings: list[SecFilingSignal],
) -> list[CalendarEvent]:
    """Build a trading-day-validated calendar with early closes and verified official events."""
    today_et = now_utc.astimezone(TZ_ET).date()
    events: list[CalendarEvent] = []
    seen_days_titles: set[tuple[str, str]] = set()

    def _push(ev: CalendarEvent) -> None:
        key = (ev.day, ev.title)
        if key not in seen_days_titles:
            seen_days_titles.add(key)
            events.append(ev)

    holiday_name = get_us_holiday_name(today_et)
    reopen_early_handled = False
    if holiday_name:
        reopen_day = _next_valid_trading_day(today_et + timedelta(days=1))
        open_cn = datetime(reopen_day.year, reopen_day.month, reopen_day.day, 9, 30, tzinfo=TZ_ET).astimezone(TZ_CN).strftime("%H:%M UTC+8")
        if reopen_day in EARLY_CLOSE_DATES:
            reopen_early_handled = True
            close_t = get_market_close(reopen_day)
            close_cn = datetime(reopen_day.year, reopen_day.month, reopen_day.day, close_t.hour, close_t.minute, tzinfo=TZ_ET).astimezone(TZ_CN).strftime("%H:%M UTC+8")
            _push(
                CalendarEvent(
                    day=reopen_day.isoformat(),
                    time=f"{open_cn} 开盘 · {close_t.strftime('%H:%M')} ET 提前收市（{close_cn}）",
                    title=f"美股节后半日市恢复开盘（{holiday_name}次日 · 提前收市）",
                    tag="半日市 / 流动性恢复",
                    tone="ochre",
                    desc=f"今日（{today_et.isoformat()}）为 {holiday_name} 全日休市；下一交易日 {reopen_day.isoformat()} 为 NYSE 提前收市日（股票 09:30–{close_t.strftime('%H:%M')} ET，流动性偏薄），盘后研究复核提前至 {close_t.hour}:30 ET。",
                    source_url="https://www.nyse.com/trade/hours-calendars",
                )
            )
        else:
            _push(
                CalendarEvent(
                    day=reopen_day.isoformat(),
                    time=open_cn,
                    title=f"美股节后恢复开盘（{holiday_name}次日交易）",
                    tag="全市场流动性恢复",
                    tone="slate",
                    desc=f"今日（{today_et.isoformat()}）为 {holiday_name} 全日休市；下一合法交易日为 {reopen_day.isoformat()}，注意节后早盘流动性缺口再平衡。",
                    source_url="https://www.nyse.com/trade/hours-calendars",
                )
            )

    # Upcoming NYSE early-close half-days within the next 5 calendar days.
    for offset in range(0, 6):
        d = today_et + timedelta(days=offset)
        if d in EARLY_CLOSE_DATES and is_us_trading_day(d) and not (reopen_early_handled and offset == 1):
            close_t = get_market_close(d)
            open_cn = datetime(d.year, d.month, d.day, 9, 30, tzinfo=TZ_ET).astimezone(TZ_CN).strftime("%H:%M UTC+8")
            _push(
                CalendarEvent(
                    day=d.isoformat(),
                    time=f"09:30–{close_t.strftime('%H:%M')} ET（{open_cn} 开盘）",
                    title="美股提前收市半日市（NYSE 官方交易时间）",
                    tag="半日市窗口",
                    tone="ochre",
                    desc=f"{d.isoformat()} 为 NYSE 提前收市日（股票 {close_t.strftime('%H:%M')} ET 收盘），交投偏薄，盘后研究复核提前至 {close_t.hour}:30 ET。",
                    source_url="https://www.nyse.com/trade/hours-calendars",
                )
            )

    # Explicitly verified official fixed-calendar macro/policy events (never month-guessed).
    horizon = today_et + timedelta(days=21)
    for ev_day, ev_time, title, tag, tone, desc, url in _VERIFIED_OFFICIAL_EVENTS:
        if today_et <= ev_day <= horizon and is_us_trading_day(ev_day):
            _push(
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

    # Never infer monthly CPI/PCE/NFP releases from calendar heuristics; keep explicit research task.
    check_day = _next_valid_trading_day(today_et + timedelta(days=1))
    _push(
        CalendarEvent(
            day=check_day.isoformat(),
            time="待官方日历确认",
            title="宏观发布日程核验（研究待办，非数据公布）",
            tag="日程待核验",
            tone="slate",
            desc="核对 BEA/BLS 官方日历后再设置月度通胀与就业数据提醒；不按月份规律推算发布日。",
            source_url="https://www.bea.gov/news/schedule",
        )
    )

    # Include any near-term SEC filing review checkpoint on the next trading day if fresh.
    if sec_filings:
        latest_sec = sec_filings[0]
        sec_m_tag = "M4" if latest_sec.sym in ("COIN", "MSTR") else ("M5" if latest_sec.sym == "LMT" else "M1")
        excerpt_hint = f" 正文摘要：{latest_sec.excerpt[:120]}" if latest_sec.excerpt else ""
        _push(
            CalendarEvent(
                day=check_day.isoformat(),
                time="开盘前后（研究待办）",
                title=f"SEC 披露跟踪：{latest_sec.sym} {latest_sec.form}（{latest_sec.filing_date}）",
                tag=f"{sec_m_tag} 一手公告核验",
                tone="sage",
                desc=f"最近抓取到 {latest_sec.company} ({latest_sec.sym}) 提交 {latest_sec.form}（Items: {latest_sec.items or '常规披露'}），开盘前后跟踪机构解读与量价承接。{excerpt_hint}",
                source_url=latest_sec.url,
            )
        )

    events.sort(key=lambda e: e.day)
    return events


async def fetch_primary_signals(
    *,
    market: MarketSnapshot,
    now_utc: datetime | None = None,
    timeout_sec: float = 12.0,
) -> PrimarySignals:
    """Fetch L1 SEC EDGAR filings + FRED CSV metrics, compute 5-dim macro cards, and build calendar."""
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
        fred_task = asyncio.create_task(_fetch_fred_metrics(client))
        sec_batches = await asyncio.gather(*sec_tasks)
        edgar_stream = await atom_task
        fred_metrics = await fred_task

    sec_filings = sorted(
        [f for batch in sec_batches for f in batch],
        key=lambda f: f.filing_date,
        reverse=True,
    )

    macro_proxies = build_macro_proxies(market, ts_now, fred_metrics=fred_metrics)
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
        fred_metrics=fred_metrics,
    )
