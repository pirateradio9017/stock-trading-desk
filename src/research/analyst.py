"""Gemini grounded causal-chain research analyst with deterministic quantitative fallback."""

from __future__ import annotations

import asyncio
import json
import httpx
import logging
import math
import os
import re
from dataclasses import dataclass
from datetime import datetime
from statistics import median
from typing import Any, Literal

from pydantic import BaseModel, Field

from src.config import (
    CORR_LABELS,
    SYMBOL_NAME_MAP,
    TZ_CN,
    TZ_ET,
    format_cn_timestamp,
    format_et_short,
    load_settings,
)
from src.ingestion import IngestionBundle
from src.ingestion.market_data import MarketSnapshot
from src.models.schemas import (
    AnalyticsData,
    CalendarEvent,
    CandidateItem,
    CandidateTier,
    HistoryEntry,
    LinkageStory,
    MacroChainId,
    MacroDimItem,
    MainlineItem,
    MainlineReasoning,
    ResearchEvidence,
    MetaInfo,
    RotationAsset,
    StateResponse,
    Tone,
)
from src.research.setups import build_anchor_reviews
from src.research.state_machine import GovernanceDecision

logger = logging.getLogger(__name__)

DEFAULT_GEMINI_MODELS: tuple[str, ...] = (
    "gemini-3.8-flash",
    "gemini-3.5-flash-lite",
    "gemini-3.1-flash-lite",
)

_TIER_META: dict[CandidateTier, tuple[str, Tone]] = {
    "upgrade": ("准备升级", "sage"),
    "watch": ("强化跟踪", "ochre"),
    "demote": ("弱化/淘汰", "slate"),
}


# ---------------------------------------------------------------------------
# Structured LLM Output Schema (consumed by google-genai response_schema)
# ---------------------------------------------------------------------------


class _DraftMeta(BaseModel):
    market_brief: str = Field(default="", max_length=6000, description="跨市场增量快报：按事件分段，写事实与时间、相对上期变化、主线/候补映射、传导假设、待验证点；只使用提供的证据。")
    badge: str = Field(description="简短市场定调标签，如'高波去杠杆 · 防御与长债避险占优'")
    headline: str = Field(description="一句话核心交易结论，必须引用真实数据")
    plain_guide: str = Field(description="2-3句大白话行动指引（只研不交易，聚焦观察与风控）")
    next_gate: str = Field(description="下一个最关键的时间/宏观/财报催化节点与判定标准")
    regime: str = Field(description="市场阶段标签，如'避险去杠杆'、'高位震荡分化'、'风险偏好扩张'")
    stance: str = Field(description="仓位与节奏定调，如'防守优先，控仓等待波动率回落'")
    vix_note: str = Field(description="基于当前实测 VIX 的一句解读")
    tnx_note: str = Field(description="基于当前实测 10Y 美债收益率与 TLT 的一句解读")
    btc_note: str = Field(description="基于当前实测 BTC 价格与涨跌幅的一句流动性解读")


class _DraftReasoning(BaseModel):
    hypothesis: str = Field(max_length=300, description="仅写一句传导假设，区分推断与事实")
    confirmation: str = Field(max_length=200, description="一句可观察的确认条件")
    invalidation: str = Field(max_length=200, description="一句能削弱或推翻假设的条件")
    uncertainty: str = Field(max_length=200, description="一句核心证据缺口或替代解释")


class _DraftMainline(BaseModel):
    id: Literal["M1", "M2", "M3", "M4", "M5"]
    title: str = Field(description="主线标题，8-18字")
    action: str = Field(description="简洁观察定调，如'防御首选'、'控仓等企稳'、'顺势跟踪'、'逢高减压'")
    action_tone: Tone
    score: int = Field(description="10-98 整数，主线资金共识与强度评分")
    status: str = Field(description="一句话现状概述")
    chain: str = Field(description="至少3步因果传导链，严格使用 ' | ' 分隔，例如 '催化A | 传导B | 资金落脚C'")
    stocks: str = Field(description="2-4组 SYM:中文名，逗号分隔，首位必须与 anchor_sym 一致")
    anchor_sym: str = Field(description="核心锚定标的代码，必须来自给定行情列表")
    accel: float = Field(description="加速确认位（正数，须在 anchor_sym 现价附近 0.80x-1.25x，且不等于 flip）")
    flip: float = Field(description="逻辑证伪/翻转位（正数，须在 anchor_sym 现价附近 0.80x-1.25x，且不等于 accel）")
    plain: str = Field(description="1-2句大白话逻辑")
    summary: str = Field(description="2-3句专业因果与量价证据拆解")
    watch_points: str = Field(description="具体盯盘价位/成交量/公告触发条件")
    source_url: str = Field(description="必须从 Briefing 的 VERIFIED_URLS 中原样选取一条最相关 URL")
    reasoning: _DraftReasoning | None = None


class _DraftCandidate(BaseModel):
    id: Literal["C1", "C2", "C3", "C4", "C5", "C6"]
    tier: CandidateTier
    name: str = Field(description="候补主题名称")
    linked_to: str = Field(description="关联主线编号，如 M1、M2")
    stocks: str = Field(description="2-3组 SYM:中文名，逗号分隔，必须来自给定行情列表")
    plain: str = Field(description="一句话大白话说明为何纳入候补")
    rule: str = Field(description="晋升或淘汰的具体量价/催化触发条件")
    history_log: str = Field(description="简洁演进记录")


class _DraftLinkage(BaseModel):
    pair: str = Field(description="跨资产联动对名称，如 '标普500 vs 20Y美债 (SPY/TLT)'")
    corr: str = Field(description="引用给定 6x6 相关系数或强弱读数，如 '20D相关 -0.28 · 股债跷跷板'")
    tone: Tone
    why: str = Field(description="跨资产因果传导机理")
    signal: str = Field(description="当前读数释放的实盘观察信号")


class _DraftMacroNote(BaseModel):
    chain_id: MacroChainId
    feature: str = Field(description="结合实测指标的一句话特征总结")
    concern: str = Field(description="核心风险点与反证条件")
    plain: str = Field(description="大白话说明该维度对美股估值/资金的影响")


class _DraftHistoryNote(BaseModel):
    title: str
    detail: str
    lesson: str


class ResearchDraft(BaseModel):
    """Structured research synthesis returned by Gemini or deterministic fallback."""

    meta: _DraftMeta
    mainlines: list[_DraftMainline] = Field(description="严格5条主线，顺序为 M1..M5")
    candidates: list[_DraftCandidate] = Field(description="严格6条候补，顺序为 C1..C6")
    linkage: list[_DraftLinkage] = Field(description="严格4条跨资产联动故事")
    macro_notes: list[_DraftMacroNote] = Field(description="严格5条宏观解读，覆盖 rate/credit/sentiment/compute/semi_demand")
    history_note: _DraftHistoryNote


@dataclass
class SynthesisOutput:
    """Result of analyst synthesis prior to state-machine evolution & audit."""

    draft_state: StateResponse
    history_note: tuple[str, str, str]
    engine_mode: Literal["gemini", "deepseek", "deterministic_fallback"]
    model_used: str
    fallback_reason: str = ""


# ---------------------------------------------------------------------------
# High Signal-to-Noise Context Builder
# ---------------------------------------------------------------------------


def _fmt_num(val: float | None, digits: int = 2, suffix: str = "") -> str:
    if val is None or not math.isfinite(val):
        return "DATA_PENDING"
    return f"{val:.{digits}f}{suffix}"


def _fmt_pct(val: float | None) -> str:
    if val is None or not math.isfinite(val):
        return "DATA_PENDING"
    return f"{val:+.2f}%"


def _collect_context_urls(bundle: IngestionBundle) -> list[tuple[str, str]]:
    """Collect deduplicated verified URLs with short labels for grounded citation."""
    seen: set[str] = set()
    items: list[tuple[str, str]] = []

    def _add(label: str, url: str) -> None:
        u = (url or "").strip()
        if u.startswith(("http://", "https://")) and u not in seen:
            seen.add(u)
            items.append((label, u))

    for f in bundle.primary.sec_filings[:8]:
        _add(f"SEC {f.sym} {f.form} ({f.filing_date})", f.url)
    for m in bundle.primary.macro_proxies:
        _add(f"Macro {m.chain_id}", m.source_url)
    for symbol, valuation in bundle.market.valuations.items():
        if valuation.forward_pe is not None:
            _add(f"Yahoo {symbol} 预期市盈率", valuation.source_url)
    for n in bundle.news.research_items[:20]:
        _add(f"新闻[{n.tag}] {n.title[:28]}", n.link)
    return items


def build_research_context(
    bundle: IngestionBundle,
    prev_state: StateResponse | None,
    decision: GovernanceDecision,
) -> str:
    """Compress IngestionBundle into a dense, grounded briefing for Gemini synthesis."""
    market = bundle.market
    now_utc = bundle.fetched_at_utc
    et_str = format_et_short(now_utc)
    cn_str = format_cn_timestamp(now_utc)

    lines: list[str] = [
        "=== 1. 时段与治理状态 ===",
        f"美东时间: {et_str} | 北京时间: {cn_str}",
        f"交易时段: {decision.market_session} | 治理窗口: {decision.window} | 允许调仓换线: {decision.allow_structural_rebalance}",
        f"治理说明: {decision.reason}",
    ]

    if prev_state is not None:
        lines.append("\n=== 2. 上一期主线骨架（防抖参考） ===")
        for m in prev_state.mainlines:
            lines.append(
                f"  {m.id} {m.title} | anchor={m.anchor_sym} | score={m.score} | first_seen={m.first_seen} | stocks={m.stocks}"
            )
        for m in prev_state.mainlines:
            lines.append(f"  上期论点 {m.anchor_sym}: {m.summary[:500]} | 验证节点: {m.watch_points[:300]}")
        lines.append("  上一期候补: " + "; ".join(f"{c.id}:{c.name}({c.tier})" for c in prev_state.candidates))
        for review in build_anchor_reviews(prev_state.mainlines, market, decision):
            lines.append(
                f"  同主题换锚复核 {review.mainline}: {review.anchor_symbol} 5D {review.anchor_r5:+.2f}% | "
                f"{review.candidate_symbol} 5D {review.candidate_r5:+.2f}% | "
                f"连续 {review.lead_sessions} 个同日窗口领先≥3个百分点（截至 {review.as_of}）。"
            )

    lines.append("\n=== 3. 核心跨资产实测行情（禁止编造） ===")
    macro_syms = [
        "^GSPC", "^IXIC", "^VIX", "^MOVE", "^TNX", "DX-Y.NYB",
        "TLT", "HYG", "LQD", "GC=F", "SI=F", "HG=F", "BZ=F", "JPY=X", "BTC-USD",
    ]
    for sym in macro_syms:
        q = market.get_quote(sym)
        if q:
            lines.append(
                f"  {sym:8s} ({q.name}): 现价={_fmt_num(q.price)} | 日={_fmt_pct(q.chg)} | 5D={_fmt_pct(q.r5)} | 20D={_fmt_pct(q.r20)}"
            )

    lines.append("\n=== 4. 行业板块与核心个股动量（按20D强弱排序） ===")
    sector_syms = [
        "ITB", "XLP", "XLU", "AAPL", "QQQ", "SPY", "XLF", "XLE",
        "SOXX", "SMH", "ASML", "NVDA", "AVGO", "AMD", "LITE", "TSM", "LMT",
        "CRWV", "IREN", "NBIS", "COIN", "MSTR",
    ]
    sector_quotes = [market.get_quote(s) for s in sector_syms]
    sector_quotes_valid = [q for q in sector_quotes if q is not None]
    sector_quotes_valid.sort(key=lambda q: q.r20 if q.r20 is not None else -999.0, reverse=True)
    for q in sector_quotes_valid:
        valuation = market.valuations.get(q.sym)
        current_pe = valuation.forward_pe if valuation is not None else None
        samples = valuation.historical_forward_pe if valuation is not None else ()
        reference = median(samples) if len(samples) >= 3 else None
        val_info = (
            f" | Yahoo 预期市盈率={current_pe:.2f}倍"
            + (f"，同公司最近 {len(samples)} 个有效历史点中位={reference:.2f}倍" if reference is not None else "")
            if current_pe is not None else ""
        )
        lines.append(
            f"  {q.sym:6s} ({q.name}): 现价={_fmt_num(q.price)} | 日={_fmt_pct(q.chg)} | 5D={_fmt_pct(q.r5)} | 20D={_fmt_pct(q.r20)}{val_info}"
        )

    if bundle.primary.apac_eu_summary:
        lines.append("\n=== 5. 亚太与欧洲盘面联动 ===")
        for sym, info in bundle.primary.apac_eu_summary.items():
            lines.append(
                f"  {sym} ({info.get('name', sym)}): 现价={_fmt_num(info.get('price'))} | 日={_fmt_pct(info.get('chg'))} | 5D={_fmt_pct(info.get('r5'))} | 20D={_fmt_pct(info.get('r20'))}"
            )

    lines.append("\n=== 6. 6x6 跨资产相关性矩阵与30D剪刀差 ===")
    if len(market.corr_labels) == 6 and len(market.corr_matrix) == 6:
        idx_map = {lbl: i for i, lbl in enumerate(market.corr_labels)}
        key_pairs = [
            ("SPY", "TLT"), ("SPY", "GLD"), ("SPY", "NVDA"),
            ("SPY", "BTC"), ("QQQ", "TLT"), ("NVDA", "BTC"), ("TLT", "GLD"),
        ]
        pair_strs = []
        for a, b in key_pairs:
            if a in idx_map and b in idx_map:
                val = market.corr_matrix[idx_map[a]][idx_map[b]]
                pair_strs.append(f"{a}-{b}={val:+.2f}")
        lines.append("  20D相关系数: " + ", ".join(pair_strs))
    if market.linkage_trend.series:
        tail_vals = [
            f"{s.label}={s.values[-1]:+.2f}%"
            for s in market.linkage_trend.series
            if s.values
        ]
        lines.append("  30D累计涨跌剪刀差: " + ", ".join(tail_vals))

    lines.append("\n=== 7. 五维宏观代理基准与 FRED L1 官方数据（实测定量） ===")
    for p in bundle.primary.macro_proxies:
        lines.append(f"  [{p.chain_id}] {p.title}: score={p.score}, state={p.state}, wind={p.wind} | {p.feature}")
    if bundle.primary.fred_metrics:
        fred_labels = {
            "BAMLH0A0HYM2": "FRED 高收益债 OAS(%)",
            "BAMLH0A0HYM2EY": "FRED 高收益债有效收益率(%)",
            "DGS10": "FRED 10Y 美债(%)",
            "MORTGAGE30US": "FRED 30Y 固定按揭利率(%)",
        }
        for sid, (obs_date, obs_val) in bundle.primary.fred_metrics.items():
            lines.append(f"  [L1 FRED] {fred_labels.get(sid, sid)}: {obs_val:.2f}% (截至 {obs_date})")

    if bundle.primary.sec_filings:
        lines.append("\n=== 8. SEC EDGAR L1 一手公告（含已抓取正文首段摘录） ===")
        for f in bundle.primary.sec_filings[:8]:
            excerpt_note = f" | L1正文摘录: {f.excerpt}" if f.excerpt else " | 仅索引（未抓取正文）"
            lines.append(f"  {f.filing_date} | {f.sym} {f.form} (Items: {f.items or '-'}) | {f.url}{excerpt_note}")

    lines.append("\n=== 9. 近 48 小时发布时间可识别的媒体快讯 ===")
    for n in bundle.news.research_items[:20]:
        lines.append(f"  [{n.lang.upper()}|{n.source}|{n.tag}] {n.title}（来源发布时间 {n.pub_date_cn}） | 摘要（未核验原文）: {n.summary[:600]} | {n.link}")

    lines.append("\n=== 10. 未来日程（含 NYSE 提前收市与已核验官方日程） ===")
    for ev in bundle.primary.calendar_events[:8]:
        lines.append(f"  {ev.day} {ev.time} | {ev.title} [{ev.tag}] — {ev.desc[:120]}")

    lines.append("\n=== 11. VERIFIED_URLS（所有 source_url 必须从此列表中原样选取） ===")
    for label, url in _collect_context_urls(bundle):
        lines.append(f"  - {label}: {url}")

    return "\n".join(lines)


_SYSTEM_PROMPT = """你是一位服务于资深美股实盘交易员（pirateradio）的顶级买方宏观策略与产业链首席研究员。
你的任务是基于提供的全市场实时行情、6x6跨资产相关性、30D剪刀差、亚太/欧股联动、SEC EDGAR 一手公告及多源聚类快讯，输出一份严谨、客观、有因果链条、有可执行观察价位锚点的《美股交易链条观察》深度研判 JSON。

必须严格遵守以下红线与专业规范：
1. **只研不交易、实测诚实**：严禁编造任何未出现在 Briefing 中的价格、涨跌幅、财报数字或链接；数据不足直接写 DATA_PENDING 或标明待验证。
2. **严格契约**：
   - `mainlines` 必须恰好 5 条（M1..M5），按当前市场资金共识与交易优先级排序；若当前处于防抖锁定窗口且 Briefing 给出了上一期骨架，尽量沿用上一期 M1-M5 的核心锚定标的并在其上更新最新盘面证据与价位。
   - 每条主线的 `chain` 必须写出 **3-4 步真实因果传导链**，严格用 ` | ` 分隔（如：`10Y收益率高位回落 | 利率敏感地产与防御资产获资金回补 | 高估值算力资金被动切换`）。
   - 每条主线的 `stocks` 必须包含 2-4 个 `SYM:中文名`（逗号分隔），首位与 `anchor_sym` 一致，且所有 ticker 必须存在于 Briefing 的实测行情列表中。
   - `accel`（向上加速确认位/突破锚点）与 `flip`（逻辑证伪/翻转防守位）必须锚定在 `anchor_sym` 当前实测现价附近（±2% 到 ±15% 合理区间内），均为正数且 `accel != flip`。
   - `source_url` 必须从 Briefing 第 11 节 `VERIFIED_URLS` 中原样选取最相关的真实 URL，绝不能编造 URL。
3. **候补池与联动**：
   - `candidates` 必须恰好 6 条（C1..C6），覆盖 `upgrade`（准备升级）、`watch`（强化跟踪）、`demote`（弱化/淘汰）三类梯度，每条给出清晰的 `rule`（晋级/淘汰量化触发条件）与非空 `history_log`。
   - `linkage` 必须恰好 4 条，紧扣实测 6x6 相关系数与 30D 剪刀差（例如股债跷跷板 SPY/TLT、算力与大盘 SPY/NVDA、黄金避险 SPY/GLD、加密与高Beta流动性 NVDA/BTC），写清传导机理（`why`）与当下交易信号（`signal`）。
4. **研究输出结构**：
   - headline 只说本轮最重要变化；plain_guide 用三句话分别解释事实、影响、下一步观察。
   - 每条 summary 按“新增事实；较上期变化；传导假设；反证/不确定性”组织。没有新增证据就明确写“本轮无新增证据”，不能把旧闻改写成新催化。
   - plain 解释为什么值得关注，不重复价格；watch_points 同时写验证条件和失效条件，不将价格涨跌直接等同于基本面逻辑证伪。
   - 每条主线附 reasoning，hypothesis / confirmation / invalidation / uncertainty 各一句，分别写传导假设、可观察确认条件、反证条件、证据缺口，每句尽量不超过 45 字，避免复述长文。来源时间是媒体发布时间，不能当作事件发生时间。
   - 换锚复核只是候选信号。锁定期沿用已接受锚定标的；开放窗口结合当前量价、估值及独立证据复核，若接受换锚，重新写齐该标的的逻辑、确认位与失效位，禁止继承旧锚价位。
   - 区分新闻报道、企业披露、行情观察、研究推断；只有新闻标题时，必须注明未核验原文。URL 出现在列表中不代表所有相关说法已验证。
   - 相关性仅描述历史共变；股价上涨不能直接证明机构净流入、需求增长或订单兑现。
   - **代理指标与文风硬约束**：`compute`（AI 电力与新云基建动量）与 `semi_demand`（半导体与算力价格动量）的评分本质是 5 日价格动量公式，**严禁将其称为“基本面评分”或编造“价格与基本面背离”**；所有中文正文中**严禁泄露代码内部字段名**（如 `semi_demand`、`chain_id`）及漏写货币单位（统一写 `美元` 或 `$`，不得写 `元` 代替美元）；若 SEC 公告已附“L1正文摘录”，须直接引用摘录事实说明公告实质，而非一律写“正文未核验”。
   - 候补解释为何尚未成为主线、缺哪项证据，以及替代哪条主线的条件；候补的 `plain` 与 `history_log` 必须与自身 `tier`（升级/跟踪/弱化淘汰）自洽，禁止“分级为弱化淘汰、备注写纳入升级候选”。
   - market_brief 输出独立的跨市场增量快报，通常 600–1200 字，证据少时缩短，不能为了字数编造。围绕最重要的 3–5 件事分段，每段写清事件/行情时间（含时区，缺失则注明）、数据与来源名称、较上期变化、关联 M/C 编号、传导假设与下一验证。优先覆盖隔夜与亚欧关键宏观/央行/通胀/商品/半日市催化，按素材实际覆盖不强行凑齐。区分新增事件、持续跟踪和待核验线索；没有上期对应证据时不得声称新增。报告生成时间不能当作行情或事件时间。不得凭价格走势声称机构抢筹，不得凭公告索引编写财务数字。
5. **表达风格**：全部使用简体中文（代码/Ticker/术语保留英文），克制、锐利、去水词，禁止出现 `Date(...)`、`TODO`、`TBD` 等占位符。
"""


# ---------------------------------------------------------------------------
# Deterministic Quantitative Fallback Engine (Zero API Dependency)
# ---------------------------------------------------------------------------


def _pick_url_for_keywords(
    bundle: IngestionBundle,
    keywords: tuple[str, ...],
    default_url: str,
) -> str:
    kws = tuple(k.lower() for k in keywords)
    for f in bundle.primary.sec_filings:
        if f.sym.lower() in kws:
            return f.url
    for n in bundle.news.research_items:
        blob = f"{n.title} {n.summary} {n.tag}".lower()
        if any(k in blob for k in kws):
            return n.link
    return default_url


def _calc_ruler(price: float | None, r5: float | None) -> tuple[float, float]:
    if price is None or not math.isfinite(price) or price <= 0:
        return 105.0, 95.0
    # Slightly wider band for high-volatility assets or negative momentum
    up_pct = 0.045 if (r5 is not None and r5 < -5.0) else 0.035
    dn_pct = 0.045 if (r5 is not None and r5 < -5.0) else 0.038
    accel = round(price * (1.0 + up_pct), 2)
    flip = round(price * (1.0 - dn_pct), 2)
    if accel == flip:
        accel = round(price + 0.5, 2)
        flip = round(max(0.01, price - 0.5), 2)
    return accel, flip


def _basket_stats(market: MarketSnapshot, syms: list[str]) -> tuple[float, float, float]:
    chgs: list[float] = []
    r5s: list[float] = []
    r20s: list[float] = []
    for sym in syms:
        q = market.get_quote(sym)
        if not q:
            continue
        if q.chg is not None:
            chgs.append(q.chg)
        if q.r5 is not None:
            r5s.append(q.r5)
        if q.r20 is not None:
            r20s.append(q.r20)
    avg_chg = round(sum(chgs) / len(chgs), 2) if chgs else 0.0
    avg_r5 = round(sum(r5s) / len(r5s), 2) if r5s else 0.0
    avg_r20 = round(sum(r20s) / len(r20s), 2) if r20s else 0.0
    return avg_chg, avg_r5, avg_r20


def _stocks_str(syms: list[str]) -> str:
    return ", ".join(f"{s}:{SYMBOL_NAME_MAP.get(s, s)}" for s in syms)


def build_deterministic_draft(
    bundle: IngestionBundle,
    prev_state: StateResponse | None,
    decision: GovernanceDecision,
) -> ResearchDraft:
    """Build a 100% live-data-driven quantitative ResearchDraft when Gemini is unavailable."""
    market = bundle.market
    et_today = bundle.fetched_at_utc.astimezone(TZ_ET).date().isoformat()

    q_spx = market.get_quote("^GSPC")
    q_spy = market.get_quote("SPY")
    q_ixic = market.get_quote("^IXIC")
    q_qqq = market.get_quote("QQQ")
    q_vix = market.get_quote("^VIX")
    q_tnx = market.get_quote("^TNX")
    q_tlt = market.get_quote("TLT")
    q_btc = market.get_quote("BTC-USD")

    vix_p = q_vix.price if q_vix else None
    tnx_p = q_tnx.price if q_tnx else None
    spx_r20_opt = (
        q_spx.r20
        if (q_spx and q_spx.r20 is not None)
        else (q_spy.r20 if (q_spy and q_spy.r20 is not None) else None)
    )
    ixic_r20_opt = (
        q_ixic.r20
        if (q_ixic and q_ixic.r20 is not None)
        else (q_qqq.r20 if (q_qqq and q_qqq.r20 is not None) else None)
    )
    spx_r20 = spx_r20_opt if spx_r20_opt is not None else 0.0

    # Regime determination from live VIX / TNX / SPX
    if (vix_p is not None and vix_p >= 22.0) or (spx_r20_opt is not None and spx_r20_opt <= -5.0):
        regime = "高波避险去杠杆"
        badge = "高波去杠杆 · 防御与长债占优"
        stance = "防守优先，严控高 Beta 成长敞口，等待波动率回落与右侧企稳确认"
    elif (vix_p is not None and vix_p < 16.5) and (spx_r20_opt is not None and spx_r20_opt >= 2.0):
        regime = "低波风险偏好扩张"
        badge = "低波顺风 · 科技与成长领涨"
        stance = "顺势聚焦核心强势主线，以加速位与证伪位动态跟踪"
    else:
        regime = "高位震荡 · 结构轮动"
        badge = "震荡分化 · 进攻与防御双线拉锯"
        stance = "均衡配置，只做量价与催化共振链条，回避无量弱势标的"

    next_cal = bundle.primary.calendar_events[0] if bundle.primary.calendar_events else None
    next_gate = (
        f"{next_cal.day} {next_cal.time} {next_cal.title}（{next_cal.tag}）"
        if next_cal
        else "关注下一个交易日美股开盘量能及 10Y 收益率方向选择"
    )

    headline = (
        f"标普500 20D {_fmt_pct(spx_r20_opt)} / 纳指 20D {_fmt_pct(ixic_r20_opt)}，"
        f"VIX {_fmt_num(vix_p)}、10Y {_fmt_num(tnx_p, 2, '%')}，当前格局定调「{regime}」。"
    )
    plain_guide = (
        f"当前处于{decision.badge_suffix}，市场情绪由 VIX {_fmt_num(vix_p)} 与长端利率 {_fmt_num(tnx_p, 2, '%')} 主导。"
        f"策略上坚持{stance}，重点观察各主线加速确认位与证伪防守位得失。"
    )

    meta = _DraftMeta(
        badge=badge,
        headline=headline,
        plain_guide=plain_guide,
        next_gate=next_gate,
        regime=regime,
        stance=stance,
        vix_note=f"VIX 报 {_fmt_num(vix_p)}（日 {_fmt_pct(q_vix.chg if q_vix else None)}，20D {_fmt_pct(q_vix.r20 if q_vix else None)}），{'避险对冲溢价偏高' if (vix_p or 0) >= 20 else '波动率处于可控区间'}",
        tnx_note=f"10Y 美债报 {_fmt_num(tnx_p, 2, '%')}（5D {_fmt_pct(q_tnx.r5 if q_tnx else None)}），TLT 5D {_fmt_pct(q_tlt.r5 if q_tlt else None)}",
        btc_note=f"BTC 报 {f'${q_btc.price:,.0f}' if (q_btc and q_btc.price is not None) else 'DATA_PENDING'}（日 {_fmt_pct(q_btc.chg if q_btc else None)}，20D {_fmt_pct(q_btc.r20 if q_btc else None)}）",
    )

    # 5 dynamic market theme clusters ranked by live quantitative momentum & consensus
    themes_spec = [
        {
            "key": "defensive_rates",
            "title": "利率敏感与防御避险回流",
            "anchor": "ITB",
            "syms": ["ITB", "XLP", "TLT", "XLU"],
            "kws": ("利率", "美债", "通胀", "pce", "美联储", "fed", "mortgage"),
            "fallback_url": "https://fred.stlouisfed.org/series/DGS10",
            "chain_up": "长端利率边际回落与避险升温 | 资金从高波科技切入防御消费与利率敏感链 | ITB/XLP/TLT 形成正反馈剪刀差",
            "chain_dn": "10Y 美债收益率再度抬升 | 估值分母与按揭成本承压 | 防御及利率敏感资产同步回调",
            "plain_tpl": "当高波动压制进攻情绪时，资金优先抱团防御消费（XLP）、长债（TLT）与利率敏感的房屋建筑（ITB）。",
        },
        {
            "key": "ai_compute",
            "title": "AI 算力核心与半导体硬件链",
            "anchor": "NVDA",
            "syms": ["NVDA", "AVGO", "TSM", "SOXX"],
            "kws": ("nvda", "英伟达", "avgo", "amd", "半导体", "芯片", "算力", "ai"),
            "fallback_url": "https://www.sec.gov/edgar/browse/?CIK=0001045810",
            "chain_up": "云厂商 AI 资本开支维持高景气 | 核心 GPU/ASIC 与半导体 ETF 获增量买盘 | 龙头带动纳指风险偏好回暖",
            "chain_dn": "高利率与前期涨幅引发估值消化 | 算力龙头与光通信链条同步去杠杆 | 资金等待财报/公告验证再定方向",
            "plain_tpl": "NVDA/AVGO/SOXX 是全市场风偏总开关，当前直接决定科技成长能否止跌或重启主升。",
        },
        {
            "key": "power_neocloud",
            "title": "AI 电力供给与新云基建链",
            "anchor": "IREN",
            "syms": ["IREN", "NBIS", "CRWV", "XLU"],
            "kws": ("data center", "数据中心", "电力", "iren", "nebius", "coreweave", "公用事业"),
            "fallback_url": "https://www.sec.gov/edgar/searchedgar/companysearch",
            "chain_up": "数据中心供电与机柜交付紧缺 | 公用电力 XLU 托底并向新云高弹性标的传导 | 融资预期改善放大利润弹性",
            "chain_dn": "信用利差与数据中心融资担忧升温 | 高 Beta 新云基建（IREN/NBIS/CRWV）弹性放大回撤 | 仅公用电力 XLU 保持防御韧性",
            "plain_tpl": "电算基建分为高波动新云（IREN/NBIS/CRWV）与稳健公用电力（XLU），强弱分化直接反映融资与风偏水温。",
        },
        {
            "key": "crypto_beta",
            "title": "加密资产与高 Beta 流动性链",
            "anchor": "BTC-USD",
            "syms": ["BTC-USD", "COIN", "MSTR"],
            "kws": ("btc", "bitcoin", "比特币", "coin", "mstr", "crypto"),
            "fallback_url": "https://www.sec.gov/edgar/browse/?CIK=0001050446",
            "chain_up": "全球流动性预期边际转松 | 比特币率先放量反弹修复 | 交易所 COIN 与持币杠杆 MSTR 跟随扩大弹性",
            "chain_dn": "风险偏好收缩叠加美元偏强 | 比特币高位回撤引发杠杆出清 | COIN/MSTR 承压并拖累高 Beta 情绪",
            "plain_tpl": "比特币与 COIN/MSTR 是全市场最灵敏的流动性温度计，常领先美股成长股拐点。",
        },
        {
            "key": "commodities_hedge",
            "title": "大宗通胀、能源与地缘防御链",
            "anchor": "GC=F",
            "syms": ["GC=F", "XLE", "LMT", "BZ=F"],
            "kws": ("黄金", "白银", "原油", "通胀", "关税", "gold", "oil", "tariff"),
            "fallback_url": "https://fred.stlouisfed.org/series/BAMLH0A0HYM2",
            "chain_up": "地缘扰动与通胀黏性反复 | 避险买盘与大宗补库推升黄金及油气 | 能源与贵金属板块形成组合对冲",
            "chain_dn": "强势美元叠加需求放缓预期 | 贵金属与原油同步震荡去泡沫 | 能源股回吐前期通胀交易溢价",
            "plain_tpl": "黄金与原油直接定价通胀黏性与地缘风险，是判断美联储政策空间和防御对冲有效性的关键锚点。",
        },
    ]

    scored_themes: list[tuple[int, dict[str, Any], float, float, float]] = []
    for spec in themes_spec:
        chg, r5, r20 = _basket_stats(market, spec["syms"])
        # Prioritize actionable strength (strong upside leaders OR dominant macro risk drivers)
        momentum_score = 55 + r5 * 1.8 + r20 * 0.7
        if spec["key"] == "ai_compute":
            # AI compute always carries high structural market weight
            momentum_score = max(momentum_score, 62 + r5 * 0.8)
        score = int(max(28, min(95, round(momentum_score))))
        scored_themes.append((score, spec, chg, r5, r20))

    scored_themes.sort(key=lambda x: x[0], reverse=True)

    mainlines: list[_DraftMainline] = []
    for idx, (score, spec, chg, r5, r20) in enumerate(scored_themes, start=1):
        anchor = spec["anchor"]
        q_anchor = market.get_quote(anchor)
        if q_anchor is None or q_anchor.price is None:
            for alt_sym in spec["syms"]:
                alt_q = market.get_quote(alt_sym)
                if alt_q is not None and alt_q.price is not None:
                    anchor = alt_sym
                    q_anchor = alt_q
                    break
        a_price = q_anchor.price if q_anchor else None
        ordered_syms = [anchor] + [s for s in spec["syms"] if s != anchor]
        accel, flip = _calc_ruler(a_price, r5)
        bullish = (r5 >= 0.5 and r20 >= 0.0) or (r5 >= 2.0)
        bearish = r5 <= -3.0 or r20 <= -15.0

        if bullish:
            action = "顺势核心跟踪" if score >= 70 else "右侧偏强观察"
            tone: Tone = "sage"
            chain = spec["chain_up"]
        elif bearish:
            action = "控仓等企稳"
            tone = "terra"
            chain = spec["chain_dn"]
        else:
            action = "区间震荡观察"
            tone = "ochre"
            chain = spec["chain_up"] if chg >= 0 else spec["chain_dn"]

        source_url = _pick_url_for_keywords(bundle, spec["kws"], spec["fallback_url"])
        summary = (
            f"【量价观察，非事件核验】板块篮子日均 {_fmt_pct(chg)}、5D {_fmt_pct(r5)}、20D {_fmt_pct(r20)}。"
            f"锚定标的 {anchor} 现报 {_fmt_num(a_price)}，"
            f"{'呈相对强势领跑格局' if bullish else ('处于深幅消化与去杠杆阶段' if bearish else '维持区间拉锯待催化')}。"
        )
        old = next((m for m in (prev_state.mainlines if prev_state else []) if m.anchor_sym == anchor), None)
        change = f"较上期评分变化 {score - old.score:+d} 分（启发式动量评分）。" if old else "首次纳入，尚无上期可比较。"
        summary += "【变化】" + change
        event = next((n for n in bundle.news.research_items if n.link == source_url), None)
        filing = next((f for f in bundle.primary.sec_filings if f.url == source_url), None)
        if filing:
            summary += f"【已采集线索】{filing.sym} 的 {filing.form} 申报（{filing.filing_date}）；采集到申报索引不代表已核验正文或盈利影响。"
        elif event:
            summary += f"【已采集线索】{event.source}（发布于 {event.pub_date_cn}）：{event.title}。媒体内容待核验。"
        else:
            summary += "【证据缺口】本轮未取得可直接关联的事件线索，当前判断仅基于量价。"
        summary += "【传导假设】" + chain.replace("|", "→") + "；价格强弱不证明订单、现金流或机构买入。"

        watch_points = (
            f"锚定 {anchor}：{accel} / {flip} 为按价格比例生成的观察阈值，并非已验证技术支撑阻力。"
            "【反证】观察突破后能否持续、关联标的是否同向；反向运行则降低短线假设信心。"
            "【下一验证】下一交易日复核篮子相对强弱；取得公司公告后核对收入、订单及现金流，证据不足时保持观察。"
        )
        mainlines.append(
            _DraftMainline(
                id=f"M{idx}",
                title=spec["title"],
                action=action,
                action_tone=tone,
                score=score,
                status=f"篮子 5D {_fmt_pct(r5)} / 20D {_fmt_pct(r20)}，{action}",
                chain=chain,
                stocks=_stocks_str(ordered_syms),
                anchor_sym=anchor,
                accel=accel,
                flip=flip,
                plain=spec["plain_tpl"],
                summary=summary,
                watch_points=watch_points,
                source_url=source_url,
                reasoning=_DraftReasoning(
                    hypothesis=chain.replace("|", "→"),
                    confirmation=f"下一交易日核对 {anchor} 与同主线标的能否同向运行并维持相对强弱。",
                    invalidation="锚定标的反向运行且同主线表现背离时，下调短线传导判断。",
                    uncertainty="量价只能确认盘面表现；需求、订单和资金流向仍需核验。",
                ),
            )
        )

    # 6 dynamic candidates grounded in live returns
    cand_specs = [
        ("大型科技现金牛避风港", "M1", ["AAPL", "QQQ"], "高波环境中大盘权重苹果展现抗跌韧性，观察能否成为科技资金防御落脚点。", "5D 跑赢纳指 >= 3% 且 AAPL 站稳近20日高点时晋升主线。"),
        ("光通信与高速互联链", "M2", ["LITE", "AVGO"], "光模块与高速互连是 AI 集群扩容的高弹性分支，波动显著大于芯片主干。", "LITE 与 AVGO 同步放量收复 5D 均线且单周转正 >= 4% 升级观察；持续破位则淘汰。"),
        ("欧洲半导体设备与亚太晶圆链", "M2", ["ASML", "SOXX"], "ASML 与台湾加权/韩国 KOSPI 构成美股开盘前的半导体领先温度计。", "ASML 与 SOXX 5D 同步转正并伴随亚太晶圆链走强时强化跟踪。"),
        ("高收益信用债与金融流动性", "M1", ["HYG", "XLF"], "追踪企业信用利差与金融股表现，提前识别风险偏好拐点或信用收紧信号。", "HYG 相对 LQD 5D 走强 >= +0.5% 且 XLF 企稳时上调风偏预期。"),
        ("能源与工业金属周期链", "M5", ["XLE", "HG=F", "BZ=F"], "原油与工业铜同步反映地缘溢价与全球制造业景气预期。", "布油与铜价 5D 同步走强 >= 3% 叠加 ISM PMI 改善时准备升级。"),
        ("算力租赁高 Beta 反弹观察", "M3", ["CRWV", "NBIS", "IREN"], "前期深调的新云基建标的具备极高弹性，但在 VIX 高位期易受融资担忧压制。", "须待 VIX 回落至 20 下方且篮子连续 3 日放量企稳，否则维持弱化/淘汰监控。"),
    ]
    candidates: list[_DraftCandidate] = []
    for idx, (name, linked_to, syms, plain, rule) in enumerate(cand_specs, start=1):
        _, r5, r20 = _basket_stats(market, syms)
        if r5 >= 1.0 and r20 >= 2.0:
            tier: CandidateTier = "upgrade"
        elif r5 <= -5.0 or r20 <= -15.0:
            tier = "demote"
        else:
            tier = "watch"
        candidates.append(
            _DraftCandidate(
                id=f"C{idx}",
                tier=tier,
                name=name,
                linked_to=linked_to,
                stocks=_stocks_str(syms),
                plain=plain,
                rule=rule,
                history_log=f"{et_today} 定量初评：5D {r5:+.2f}% / 20D {r20:+.2f}% -> {_TIER_META[tier][0]}",
            )
        )

    # 4 dynamic cross-asset linkage stories directly backed by 6x6 correlation matrix
    corr_map: dict[tuple[str, str], float] = {}
    if len(market.corr_labels) == 6 and len(market.corr_matrix) == 6:
        for i, la in enumerate(market.corr_labels):
            for j, lb in enumerate(market.corr_labels):
                corr_map[(la, lb)] = market.corr_matrix[i][j]

    c_spy_tlt = corr_map.get(("SPY", "TLT"), -0.25)
    c_spy_nvda = corr_map.get(("SPY", "NVDA"), 0.75)
    c_spy_gld = corr_map.get(("SPY", "GLD"), 0.20)
    c_nvda_btc = corr_map.get(("NVDA", "BTC"), 0.30)

    linkage = [
        _DraftLinkage(
            pair="标普500 vs 20Y美债 (SPY / TLT)",
            corr=f"20D相关 {c_spy_tlt:+.2f} · {'股债负相关避险恢复' if c_spy_tlt < -0.15 else '股债同向受利率主导'}",
            tone="sage" if c_spy_tlt < -0.15 else "ochre",
            why="当增长担忧或高波避险升温时，长债 TLT 发挥避险缓冲作用；若通胀失控则演变为股债双杀。",
            signal=f"当前相关系数 {c_spy_tlt:+.2f}，TLT 5D {_fmt_pct(q_tlt.r5 if q_tlt else None)}，{'债市正提供有效组合对冲' if (q_tlt and (q_tlt.r5 or 0) > 0) else '警惕利率上行再度抽离估值'}。",
        ),
        _DraftLinkage(
            pair="标普500 vs 英伟达 (SPY / NVDA)",
            corr=f"20D相关 {c_spy_nvda:+.2f} · 科技权重核心锚",
            tone="terra" if (market.get_quote("NVDA") and (market.get_quote("NVDA").r20 or 0) < -10) else "sage",
            why="AI 算力龙头高度主导纳指与标普权重动量，其资金流向直接放大指数波动。",
            signal=f"SPY-NVDA 相关性达 {c_spy_nvda:+.2f}，大盘若要结束调整并重回趋势，必须先看到 NVDA 与 SOXX 放量止跌。",
        ),
        _DraftLinkage(
            pair="标普500 vs 黄金 (SPY / GLD)",
            corr=f"20D相关 {c_spy_gld:+.2f} · 流动性与避险共振",
            tone="ochre",
            why="黄金兼具实际利率定价与地缘避险属性，与美股相关性转正往往意味着共同受流动性驱动。",
            signal=f"当前相关系数 {c_spy_gld:+.2f}，黄金 5D {_fmt_pct(market.get_quote('GC=F').r5 if market.get_quote('GC=F') else None)}，观察后续通胀数据对实际利率的二次冲击。",
        ),
        _DraftLinkage(
            pair="英伟达 vs 比特币 (NVDA / BTC)",
            corr=f"20D相关 {c_nvda_btc:+.2f} · 高 Beta 流动性温度计",
            tone="sage" if (q_btc and (q_btc.chg or 0) > 2.0) else "slate",
            why="加密资产与 AI 高弹性科技股共享边际美元流动性池，常在风险偏好拐点出现领先背离。",
            signal=f"当前相关性 {c_nvda_btc:+.2f}，BTC 日内 {_fmt_pct(q_btc.chg if q_btc else None)}，关注加密先行反弹能否向高 Beta 科技扩散。",
        ),
    ]

    macro_notes = [
        _DraftMacroNote(
            chain_id=p.chain_id,
            feature=p.feature,
            concern=p.concern,
            plain=p.plain,
        )
        for p in bundle.primary.macro_proxies
    ]

    history_note = _DraftHistoryNote(
        title=f"定量研判同步 · {badge}",
        detail=f"完成 {market.live.count} 只行情、{len(bundle.primary.sec_filings)} 条 SEC 公告与 {bundle.news.response.count} 条中文快讯聚合分析，当前窗口 {decision.window}。",
        lesson="当大模型通道降级时，定量引擎严格锚定实测价位与跨资产剪刀差输出可验证观察框架，绝不编造观点。",
    )

    return ResearchDraft(
        meta=meta,
        mainlines=mainlines,
        candidates=candidates,
        linkage=linkage,
        macro_notes=macro_notes,
        history_note=history_note,
    )


# ---------------------------------------------------------------------------
# Gemini Grounded Synthesis via google-genai SDK
# ---------------------------------------------------------------------------


def _safe_model_error(exc: Exception) -> str:
    """Log validation locations without exposing model text or credentials."""
    from pydantic import ValidationError
    if isinstance(exc, ValidationError):
        errors = exc.errors(include_url=False, include_context=False, include_input=False)
        return "Schema validation: " + "; ".join(
            ".".join(map(str, e["loc"])) + ": " + e["type"] for e in errors[:8]
        )
    message = str(exc)
    if type(exc) is ValueError and message.startswith((
        "DeepSeek response incomplete:", "DeepSeek returned incorrect research card counts:",
        "Model returned a source outside the supplied evidence set",
    )):
        return message
    return type(exc).__name__


def _call_deepseek_sync(api_key: str, model_name: str, prompt: str, timeout_sec: float = 120.0) -> ResearchDraft:
    """Bounded paid request; validate JSON locally before accepting research."""
    schema = json.dumps(ResearchDraft.model_json_schema(), ensure_ascii=False)
    with httpx.Client(timeout=httpx.Timeout(timeout_sec, connect=15.0)) as client:
        response = client.post("https://api.deepseek.com/chat/completions", headers={
            "Authorization": "Bearer " + api_key,
        }, json={
            "model": model_name,
            "messages": [
                {"role": "system", "content": _SYSTEM_PROMPT + "\n在保证证据、传导与反证完整的前提下避免重复，所有中文叙述合计控制在6000字以内，优先完整输出全部必填字段。\nReturn only a JSON object matching this JSON Schema:\n" + schema},
                {"role": "user", "content": prompt},
            ],
            "response_format": {"type": "json_object"},
            "temperature": 0.25,
            "max_tokens": 24000,
            "stream": False,
        })
        response.raise_for_status()
        choice = response.json()["choices"][0]
        if choice.get("finish_reason") != "stop":
            raise ValueError("DeepSeek response incomplete: " + str(choice.get("finish_reason")))
        draft = ResearchDraft.model_validate_json(choice["message"].get("content") or "")
        if (
            len(draft.mainlines) != 5
            or len(draft.candidates) != 6
            or len(draft.linkage) != 4
            or len(draft.macro_notes) != 5
            or len({m.id for m in draft.mainlines}) != 5
            or len({c.id for c in draft.candidates}) != 6
        ):
            raise ValueError(
                f"DeepSeek returned incorrect research card counts: "
                f"M={len(draft.mainlines)} C={len(draft.candidates)} L={len(draft.linkage)} Mac={len(draft.macro_notes)}"
            )
        return draft


def _call_gemini_sync(
    api_key: str,
    model_name: str,
    prompt: str,
    timeout_sec: float = 35.0,
) -> ResearchDraft:
    from google import genai
    from google.genai import types

    client = genai.Client(
        api_key=api_key,
        http_options=types.HttpOptions(timeout=max(5_000, int(timeout_sec * 1000))),
    )
    response = client.models.generate_content(
        model=model_name,
        contents=prompt,
        config=types.GenerateContentConfig(
            system_instruction=_SYSTEM_PROMPT,
            response_mime_type="application/json",
            response_schema=ResearchDraft,
            temperature=0.25,
        ),
    )

    parsed = getattr(response, "parsed", None)
    draft: ResearchDraft | None = None
    if isinstance(parsed, ResearchDraft):
        draft = parsed
    elif isinstance(parsed, dict):
        draft = ResearchDraft.model_validate(parsed)

    if draft is None:
        raw_text = (getattr(response, "text", None) or "").strip()
        if not raw_text:
            raise ValueError(f"Gemini model {model_name} returned empty response text")
        if raw_text.startswith("```"):
            raw_text = re.sub(r"^```(?:json)?\s*", "", raw_text)
            raw_text = re.sub(r"\s*```$", "", raw_text)
        draft = ResearchDraft.model_validate_json(raw_text)

    if len(draft.mainlines) != 5 or len(draft.candidates) != 6 or len(draft.linkage) != 4:
        raise ValueError(
            f"Gemini model {model_name} returned wrong card counts: "
            f"M={len(draft.mainlines)} C={len(draft.candidates)} L={len(draft.linkage)}"
        )
    return draft


def _resolve_gemini_models() -> tuple[str, ...]:
    env_model = os.getenv("DESK_GEMINI_MODEL", "").strip()
    if env_model:
        out = [env_model]
        for m in DEFAULT_GEMINI_MODELS:
            if m not in out:
                out.append(m)
        return tuple(out)
    return DEFAULT_GEMINI_MODELS


# ---------------------------------------------------------------------------
# Draft -> StateResponse Merger (Grounds Quantitative Fields in Live Data)
# ---------------------------------------------------------------------------


def _ensure_rotation_assets(market: MarketSnapshot) -> list[RotationAsset]:
    if len(market.rotation_assets) >= 5:
        return list(market.rotation_assets)
    assets: list[RotationAsset] = []
    for q in market.live.quotes:
        if q.r5 is not None and q.r20 is not None:
            assets.append(
                RotationAsset(
                    sym=q.sym,
                    name=q.name or q.sym,
                    group="核心追踪",
                    r5=round(float(q.r5), 2),
                    r20=round(float(q.r20), 2),
                )
            )
        if len(assets) >= 12:
            break
    return assets


def _ensure_spark_labels(market: MarketSnapshot) -> list[str]:
    if len(market.spark_labels) == 11:
        return list(market.spark_labels)
    return [f"D-{10 - i}" for i in range(10)] + ["Now"]


def _extract_syms(stocks: str) -> list[str]:
    return [p.split(":", 1)[0].strip() for p in (stocks or "").split(",") if ":" in p]


def _mainline_evidence(anchor: str, url: str, bundle: IngestionBundle) -> list[ResearchEvidence]:
    evidence: list[ResearchEvidence] = []
    quote = bundle.market.get_quote(anchor)
    if quote and quote.price is not None:
        evidence.append(ResearchEvidence(
            kind="market", text=f"{anchor} 价格 {_fmt_num(quote.price)}，5 日变化 {_fmt_pct(quote.r5)}。",
            source="Yahoo Finance", as_of=quote.tsUtc,
            source_url=f"https://finance.yahoo.com/quote/{anchor}",
        ))
    for filing in bundle.primary.sec_filings:
        if filing.url == url:
            evidence.append(ResearchEvidence(
                kind="filing", text=f"{filing.sym} 提交 {filing.form}，采集到申报索引。",
                source="SEC EDGAR", as_of=str(filing.filing_date), source_url=url,
            ))
            break
    for report in bundle.news.research_items:
        if report.link == url:
            evidence.append(ResearchEvidence(
                kind="report", text=report.title[:450], source=report.source[:120],
                as_of=report.pub_date_cn, source_url=url,
            ))
            break
    return evidence


def draft_to_state_response(
    draft: ResearchDraft,
    bundle: IngestionBundle,
    decision: GovernanceDecision,
) -> StateResponse:
    """Merge ResearchDraft narratives with strictly grounded live market analytics & proxies."""
    now_utc = bundle.fetched_at_utc
    market = bundle.market
    et_today = now_utc.astimezone(TZ_ET).date().isoformat()
    et_short = format_et_short(now_utc)
    cn_ts = now_utc.astimezone(TZ_CN).strftime("%Y-%m-%d %H:%M UTC+8")

    meta = MetaInfo(
        updated_at=f"{et_short} · {decision.badge_suffix}"[:120],
        updated_at_short=et_short,
        badge=draft.meta.badge,
        headline=draft.meta.headline,
        plain_guide=draft.meta.plain_guide,
        market_brief=draft.meta.market_brief,
        next_gate=draft.meta.next_gate,
        regime=draft.meta.regime,
        stance=draft.meta.stance,
        vix_note=draft.meta.vix_note,
        tnx_note=draft.meta.tnx_note,
        btc_note=draft.meta.btc_note,
        self_review="待六维自审校验",
    )

    mainlines: list[MainlineItem] = []
    for idx, dm in enumerate(draft.mainlines[:5], start=1):
        anchor = dm.anchor_sym.strip()
        if market.get_quote(anchor) is None:
            syms = _extract_syms(dm.stocks)
            anchor = next((s for s in syms if market.get_quote(s) is not None), "NVDA")

        q_anchor = market.get_quote(anchor)
        a_price = q_anchor.price if q_anchor else None
        accel = float(dm.accel)
        flip = float(dm.flip)
        if accel <= 0 or flip <= 0 or accel == flip:
            accel, flip = _calc_ruler(a_price, q_anchor.r5 if q_anchor else None)

        spark = market.symbol_sparks.get(anchor)
        if not spark or len(spark) != 11:
            spark = [100.0] * 11

        mainlines.append(
            MainlineItem(
                id=f"M{idx}",
                title=dm.title,
                action=dm.action,
                action_tone=dm.action_tone,
                first_seen=et_today,
                score=int(max(10, min(98, dm.score))),
                status=f"[First_seen: {et_today} | Score: {int(dm.score)}] {dm.status}",
                chain=dm.chain,
                stocks=dm.stocks,
                anchor_sym=anchor,
                accel=round(accel, 2),
                flip=round(flip, 2),
                spark=[round(float(x), 2) for x in spark],
                plain=dm.plain,
                summary=dm.summary,
                watch_points=dm.watch_points,
                source_url=dm.source_url,
                reasoning=MainlineReasoning(
                    **(dm.reasoning.model_dump() if anchor == dm.anchor_sym and dm.reasoning else {}),
                    evidence=_mainline_evidence(anchor, dm.source_url, bundle),
                ),
            )
        )

    candidates: list[CandidateItem] = []
    for idx, dc in enumerate(draft.candidates[:6], start=1):
        syms = _extract_syms(dc.stocks)
        _, r5, r20 = _basket_stats(market, syms)
        tier_label, tone = _TIER_META[dc.tier]
        candidates.append(
            CandidateItem(
                id=f"C{idx}",
                tier=dc.tier,
                tier_label=tier_label,
                tone=tone,
                name=dc.name,
                linked_to=dc.linked_to,
                stocks=dc.stocks,
                r5=r5,
                r20=r20,
                plain=dc.plain,
                rule=dc.rule,
                first_seen=et_today,
                stale_days=0,
                history_log=dc.history_log or f"{et_today} 纳入候补池（5D {r5:+.2f}%）",
            )
        )

    linkage = [
        LinkageStory(
            pair=dl.pair,
            corr=dl.corr,
            tone=dl.tone,
            why=dl.why,
            signal=dl.signal,
        )
        for dl in draft.linkage[:4]
    ]

    # Merge macro commentary into quantitative primary.macro_proxies (never overwrite measured metrics!)
    notes_by_id = {n.chain_id: n for n in draft.macro_notes}
    macro: list[MacroDimItem] = []
    for proxy in bundle.primary.macro_proxies:
        note = notes_by_id.get(proxy.chain_id)
        if note is not None:
            macro.append(
                proxy.model_copy(
                    update={
                        "feature": note.feature or proxy.feature,
                        "concern": note.concern or proxy.concern,
                        "plain": note.plain or proxy.plain,
                    }
                )
            )
        else:
            macro.append(proxy)

    calendar_events: list[CalendarEvent] = list(bundle.primary.calendar_events)
    if not calendar_events:
        calendar_events = [
            CalendarEvent(
                day=et_today,
                time="待官方日历确认",
                title="常规盘面与宏观日历跟踪",
                tag="宏观观察",
                tone="slate",
                desc="未取得已核验的发布日程；此项为研究待办，不能据此判断当日没有重要事件。",
                source_url="https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm",
            )
        ]

    analytics = AnalyticsData(
        sparkLabels=_ensure_spark_labels(market),
        rotationAssets=_ensure_rotation_assets(market),
        corrLabels=list(market.corr_labels) if len(market.corr_labels) == 6 else list(CORR_LABELS),
        corrMatrix=[list(row) for row in market.corr_matrix],
        linkageTrend=market.linkage_trend,
    )

    init_history = [
        HistoryEntry(
            time=cn_ts,
            type="主线复盘",
            tone="sage",
            title=draft.history_note.title,
            detail=draft.history_note.detail,
            lesson=draft.history_note.lesson,
        )
    ]

    return StateResponse(
        serverSyncedAt=format_cn_timestamp(now_utc),
        meta=meta,
        mainlines=mainlines,
        candidates=candidates,
        linkage=linkage,
        macro=macro,
        calendar=calendar_events,
        history=init_history,
        analytics=analytics,
    )


async def synthesize_research_state(
    bundle: IngestionBundle,
    prev_state: StateResponse | None,
    decision: GovernanceDecision,
    *,
    use_llm: bool = True,
    models: tuple[str, ...] | None = None,
    timeout_sec: float = 35.0,
) -> SynthesisOutput:
    """Synthesize grounded research state via Gemini SDK, with deterministic quantitative fallback."""
    settings = load_settings()
    api_key = settings.gemini_api_key
    model_chain = models or _resolve_gemini_models()

    deepseek_key = os.getenv("DEEPSEEK_API_KEY", "").strip()
    provider = os.getenv("DESK_LLM_PROVIDER", "auto").strip().lower()
    attempts = []
    if deepseek_key and provider in ("auto", "deepseek"):
        attempts.extend([("deepseek", os.getenv("DEEPSEEK_MODEL", "deepseek-flash"), deepseek_key, _call_deepseek_sync, 180.0)] * 3)
    if api_key and provider in ("auto", "gemini"):
        attempts.extend(("gemini", m, api_key, _call_gemini_sync, timeout_sec) for m in model_chain[:2])
    if use_llm and attempts:
        briefing = build_research_context(bundle, prev_state, decision)
        prompt = (
            "以下是最新实测全市场投研 Briefing，请严格遵循系统规范与 JSON Schema 输出深度因果研判：\n\n"
            + briefing
        )
        last_err = ""
        for engine, model_name, key, call, call_timeout in attempts:
            try:
                draft = await asyncio.wait_for(
                    asyncio.to_thread(call, key, model_name, prompt, call_timeout),
                    timeout=call_timeout + 5.0,
                )
                allowed_urls = {url for _, url in _collect_context_urls(bundle)}
                if any(m.source_url not in allowed_urls for m in draft.mainlines):
                    raise ValueError("Model returned a source outside the supplied evidence set")
                draft_state = draft_to_state_response(draft, bundle, decision)
                return SynthesisOutput(
                    draft_state=draft_state,
                    history_note=(
                        draft.history_note.title,
                        draft.history_note.detail,
                        draft.history_note.lesson,
                    ),
                    engine_mode=engine,
                    model_used=model_name,
                )
            except Exception as exc:
                status = getattr(getattr(exc, "response", None), "status_code", None)
                if status is None:
                    match = re.search(r"\b(401|402|429|500|502|503|504)\b", str(exc))
                    status = match.group(1) if match else ""
                detail = _safe_model_error(exc)
                last_err = f"{engine}/{model_name}: {detail} {status}"
                logger.warning("Synthesis failed on %s/%s: %s", engine, model_name, detail)
                if status and str(status) in ("400", "401", "402", "403", "404"):
                    break
                prompt = ("上一轮输出未通过校验：" + detail +
                          "。请重新生成完整且精简的 JSON，严格满足所有字段和卡片数量，"
                          "source_url 必须原样复制 VERIFIED_URLS，不得添加其他网址。\n\n" + briefing)
                await asyncio.sleep(2)

        fallback_reason = f"模型调用失败回退定量引擎 ({last_err})"
    else:
        fallback_reason = "未配置可用模型或已禁用 LLM"

    det_draft = build_deterministic_draft(bundle, prev_state, decision)
    det_state = draft_to_state_response(det_draft, bundle, decision)
    return SynthesisOutput(
        draft_state=det_state,
        history_note=(
            det_draft.history_note.title,
            det_draft.history_note.detail,
            det_draft.history_note.lesson,
        ),
        engine_mode="deterministic_fallback",
        model_used="deterministic_quant_v2",
        fallback_reason=fallback_reason,
    )
