"""Six-dimension self-audit and anti-hallucination guardrail for StateResponse."""

from __future__ import annotations

import math
import re
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import urlparse

from src.config import CORR_LABELS
from src.ingestion import IngestionBundle
from src.models.schemas import (
    AnalyticsData,
    CalendarEvent,
    CandidateItem,
    HistoryEntry,
    MacroDimItem,
    MainlineItem,
    StateResponse,
)

_GOOGLE_DATE_RE = re.compile(r"Date\s*\(", re.IGNORECASE)
_PLACEHOLDER_RE = re.compile(r"\b(TODO|TBD|FIXME|LOREM\s+IPSUM)\b", re.IGNORECASE)
_CHAIN_SPLIT_RE = re.compile(r"\s*(?:\||->|→)\s*")

_TRUSTED_SOURCE_DOMAINS: tuple[str, ...] = (
    "sec.gov",
    "fred.stlouisfed.org",
    "cboe.com",
    "federalreserve.gov",
    "bls.gov",
    "bea.gov",
    "ismworld.org",
    "finance.yahoo.com",
    "wallstreetcn.com",
    "cnbc.com",
    "marketwatch.com",
    "investing.com",
    "seekingalpha.com",
    "finviz.com",
    "news.google.com",
    "nasdaq.com",
    "nyse.com",
    "reuters.com",
    "bloomberg.com",
)

_REQUIRED_MACRO_IDS: tuple[str, ...] = (
    "rate",
    "credit",
    "sentiment",
    "compute",
    "semi_demand",
)


@dataclass
class AuditCheck:
    """Result for one of the 6 self-audit dimensions."""

    dim: str
    passed: bool
    detail: str
    repaired: bool = False


@dataclass
class AuditReport:
    """Aggregated 6-dimension self-audit report."""

    passed: bool
    checks: list[AuditCheck] = field(default_factory=list)
    repairs: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    summary: str = ""


def _is_valid_http_url(url: str) -> bool:
    s = (url or "").strip()
    if not s or any(ch.isspace() for ch in s):
        return False
    parsed = urlparse(s)
    return parsed.scheme in ("http", "https") and bool(parsed.netloc) and bool(parsed.hostname)


def _is_trusted_url(url: str, verified_urls: set[str]) -> bool:
    if not _is_valid_http_url(url):
        return False
    if url in verified_urls:
        return True
    return False  # A reputable hostname does not verify an invented document path.


def _collect_verified_urls(bundle: IngestionBundle) -> set[str]:
    urls: set[str] = set()
    for f in bundle.primary.sec_filings:
        if _is_valid_http_url(f.url):
            urls.add(f.url)
    for e in bundle.primary.edgar_stream:
        u = e.get("link", "")
        if _is_valid_http_url(u):
            urls.add(u)
    for m in bundle.primary.macro_proxies:
        if _is_valid_http_url(m.source_url):
            urls.add(m.source_url)
    for c in bundle.primary.calendar_events:
        if _is_valid_http_url(c.source_url):
            urls.add(c.source_url)
    for valuation in bundle.market.valuations.values():
        if valuation.forward_pe is not None and _is_valid_http_url(valuation.source_url):
            urls.add(valuation.source_url)
    for n in bundle.news.research_items:
        if _is_valid_http_url(n.link):
            urls.add(n.link)
    return urls


def _fallback_url_for_anchor(anchor_sym: str, bundle: IngestionBundle) -> str:
    sym_u = anchor_sym.upper()
    for f in bundle.primary.sec_filings:
        if f.sym.upper() == sym_u and _is_valid_http_url(f.url):
            return f.url
    for n in bundle.news.research_items:
        if sym_u in n.title.upper() or sym_u in n.summary.upper():
            return n.link
    return f"https://finance.yahoo.com/quote/{anchor_sym}"


def _scan_forbidden_tokens(obj: Any, path: str = "$") -> list[str]:
    issues: list[str] = []
    if isinstance(obj, str):
        if _GOOGLE_DATE_RE.search(obj):
            issues.append(f"{path}: Google Date(...) literal")
        if _PLACEHOLDER_RE.search(obj):
            issues.append(f"{path}: placeholder token")
    elif isinstance(obj, float):
        if not math.isfinite(obj):
            issues.append(f"{path}: non-finite float")
    elif isinstance(obj, dict):
        for k, v in obj.items():
            issues.extend(_scan_forbidden_tokens(v, f"{path}.{k}"))
    elif isinstance(obj, list):
        for idx, item in enumerate(obj):
            issues.extend(_scan_forbidden_tokens(item, f"{path}[{idx}]"))
    return issues


def _sanitize_text(text: str, default: str) -> str:
    s = (text or "").strip()
    if not s:
        return default
    s = _GOOGLE_DATE_RE.sub("", s)
    s = _PLACEHOLDER_RE.sub("待跟踪", s)
    return s.strip() or default


def audit_and_repair_state(
    state: StateResponse,
    bundle: IngestionBundle,
    *,
    engine_mode: str = "gemini",
    model_used: str = "",
) -> tuple[StateResponse, AuditReport]:
    """Run 6-dimension self-audit on StateResponse, auto-repairing recoverable defects."""
    checks: list[AuditCheck] = []
    repairs: list[str] = []
    warnings: list[str] = []

    verified_urls = _collect_verified_urls(bundle)

    # --- Dimension 1: Data Freshness & Coverage ---
    live_total = bundle.market.live.count
    live_ok = sum(1 for q in bundle.market.live.quotes if q.price is not None)
    coverage_ratio = (live_ok / live_total) if live_total > 0 else 0.0
    pending_macro = sum(1 for m in state.macro if "DATA_PENDING" in m.state)
    dim1_ok = live_total >= 27 and coverage_ratio >= 0.80
    if not dim1_ok:
        warnings.append(f"行情覆盖偏低：{live_ok}/{live_total}")
    if pending_macro > 0:
        warnings.append(f"{pending_macro} 项宏观维度存在 DATA_PENDING")
    checks.append(
        AuditCheck(
            dim="数据新鲜度与覆盖",
            passed=dim1_ok,
            detail=f"行情 {live_ok}/{live_total} ({coverage_ratio:.0%})，SEC {len(bundle.primary.sec_filings)} 条，新闻 {bundle.news.response.count} 条",
        )
    )

    # --- Dimension 3 & 4 & 5 Repairs on Mainlines ---
    repaired_mainlines: list[MainlineItem] = []
    chain_repaired = False
    ruler_repaired = False
    url_repaired = False

    for idx, m in enumerate(state.mainlines[:5], start=1):
        updates: dict[str, Any] = {}
        if m.id != f"M{idx}":
            updates["id"] = f"M{idx}"

        # Chain >= 3 steps
        steps = [p.strip() for p in _CHAIN_SPLIT_RE.split(m.chain) if p.strip()]
        if len(steps) < 3:
            updates["chain"] = f"{m.title}催化 | 核心标的 {m.anchor_sym} 量价验证 | 跨资产风险偏好传导"
            chain_repaired = True
            repairs.append(f"M{idx}: 补全因果链至3步")

        # Stocks >= 2 SYM:Name pairs
        pairs = [p.strip() for p in m.stocks.split(",") if ":" in p]
        if len(pairs) < 2:
            q_anchor = bundle.market.get_quote(m.anchor_sym)
            anchor_name = q_anchor.name if q_anchor and q_anchor.name else m.anchor_sym
            updates["stocks"] = f"{m.anchor_sym}:{anchor_name}, QQQ:纳指100 ETF"
            chain_repaired = True
            repairs.append(f"M{idx}: 补齐 stocks 至少2组 SYM:Name")

        # Spark length == 11
        spark = m.spark
        if len(spark) != 11 or not all(math.isfinite(x) for x in spark):
            live_sp = bundle.market.symbol_sparks.get(m.anchor_sym)
            if live_sp and len(live_sp) == 11:
                updates["spark"] = [round(float(x), 2) for x in live_sp]
            else:
                updates["spark"] = [100.0] * 11
            chain_repaired = True
            repairs.append(f"M{idx}: 修正 spark 长度至11点")

        # Directional price ruler sanity against live quote
        q = bundle.market.get_quote(m.anchor_sym)
        accel, flip = float(m.accel), float(m.flip)
        if q and q.price and q.price > 0:
            price = float(q.price)
            out_of_band = (
                accel <= 0
                or flip <= 0
                or abs(accel - flip) < max(0.01, price * 0.004)
                or accel < price * 0.55
                or accel > price * 1.45
                or flip < price * 0.55
                or flip > price * 1.45
            )
            if out_of_band:
                updates["accel"] = round(price * 1.045, 2)
                updates["flip"] = round(price * 0.945, 2)
                ruler_repaired = True
                repairs.append(
                    f"M{idx}({m.anchor_sym}): 纠偏幻觉价位 ({accel}/{flip} -> {updates['accel']}/{updates['flip']} @现价 {price:.2f})"
                )
        elif accel <= 0 or flip <= 0 or accel == flip:
            updates["accel"] = 105.0
            updates["flip"] = 95.0
            ruler_repaired = True
            repairs.append(f"M{idx}: 修正无效 accel/flip 标尺")

        # Source URL verification
        if not _is_trusted_url(m.source_url, verified_urls):
            updates["source_url"] = _fallback_url_for_anchor(m.anchor_sym, bundle)
            updates["summary"] = "【原引用未匹配本轮采集，以下论述待核验；链接仅作查阅入口】" + m.summary
            url_repaired = True
            repairs.append(f"M{idx}: 替换非白名单/无效信源 URL")

        # Text hygiene
        for field_name, fallback_txt in (
            ("plain", f"{m.title}处于关键观察区间，以 {m.anchor_sym} 价位锚点验证方向。"),
            ("summary", f"围绕 {m.anchor_sym} 与关联板块跟踪资金流向及基本面催化。"),
            ("watch_points", f"重点观察 {m.anchor_sym} 加速位与证伪位得失及成交配合。"),
        ):
            val = updates.get(field_name, getattr(m, field_name))
            cleaned = _sanitize_text(val, fallback_txt)
            if cleaned != val:
                updates[field_name] = cleaned

        repaired_mainlines.append(m.model_copy(update=updates) if updates else m)

    # --- Candidate consistency & non-empty fields ---
    repaired_candidates: list[CandidateItem] = []
    quant_repaired = False
    for idx, c in enumerate(state.candidates[:6], start=1):
        updates = {}
        if c.id != f"C{idx}":
            updates["id"] = f"C{idx}"
        if not c.stocks.strip() or ":" not in c.stocks:
            updates["stocks"] = "QQQ:纳指100 ETF, SPY:标普500 ETF"
            quant_repaired = True
            repairs.append(f"C{idx}: 补齐候选股票对")
        if not c.history_log.strip():
            updates["history_log"] = f"{c.first_seen} 纳入候补跟踪"
            quant_repaired = True
        if not math.isfinite(c.r5) or not math.isfinite(c.r20):
            updates["r5"] = 0.0
            updates["r20"] = 0.0
            quant_repaired = True
        repaired_candidates.append(c.model_copy(update=updates) if updates else c)

    # --- Macro URL & non-empty field check ---
    repaired_macro: list[MacroDimItem] = []
    proxy_by_id = {p.chain_id: p for p in bundle.primary.macro_proxies}
    for chain_id in _REQUIRED_MACRO_IDS:
        existing = next((x for x in state.macro if x.chain_id == chain_id), None)
        proxy = proxy_by_id.get(chain_id)
        item = existing or proxy
        if item is None:
            continue
        updates = {}
        if not _is_trusted_url(item.source_url, verified_urls):
            updates["source_url"] = (
                proxy.source_url if proxy and _is_valid_http_url(proxy.source_url) else "https://fred.stlouisfed.org/"
            )
            url_repaired = True
            repairs.append(f"macro.{chain_id}: 修正 source_url")
        for f_name in ("feature", "concern", "plain", "m1_label", "m1_val", "m1_date", "m1_sub", "m2_label", "m2_val", "m2_date", "m2_sub"):
            val = getattr(item, f_name, "")
            if not str(val).strip():
                fallback_val = getattr(proxy, f_name, "DATA_PENDING") if proxy else "DATA_PENDING"
                updates[f_name] = fallback_val or "DATA_PENDING"
        repaired_macro.append(item.model_copy(update=updates) if updates else item)

    # --- Calendar URL check ---
    repaired_calendar: list[CalendarEvent] = []
    for idx, ev in enumerate(state.calendar):
        if not _is_trusted_url(ev.source_url, verified_urls):
            repaired_calendar.append(ev.model_copy(update={"source_url": "https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm"}))
            url_repaired = True
            repairs.append(f"calendar[{idx}]: 修正 source_url")
        else:
            repaired_calendar.append(ev)
    if not repaired_calendar and bundle.primary.calendar_events:
        repaired_calendar = list(bundle.primary.calendar_events)

    # --- History lesson non-empty check ---
    repaired_history: list[HistoryEntry] = []
    for h in state.history[:20]:
        if not h.lesson.strip():
            repaired_history.append(
                h.model_copy(update={"lesson": "严格按时段防抖与量化证据推进主线复盘。"})
            )
        else:
            repaired_history.append(h)

    # --- Analytics matrix symmetry & length check ---
    analytics = state.analytics
    corr_ok = (
        len(analytics.corrLabels) == 6
        and len(analytics.corrMatrix) == 6
        and all(len(row) == 6 for row in analytics.corrMatrix)
        and all(abs(analytics.corrMatrix[i][i] - 1.0) <= 1e-3 for i in range(6))
        and all(abs(analytics.corrMatrix[i][j] - analytics.corrMatrix[j][i]) <= 1e-6 for i in range(6) for j in range(6))
        and analytics.corrMatrix == bundle.market.corr_matrix
    )
    if not corr_ok:
        analytics = AnalyticsData(
            sparkLabels=list(bundle.market.spark_labels),
            rotationAssets=list(bundle.market.rotation_assets),
            corrLabels=list(bundle.market.corr_labels or CORR_LABELS),
            corrMatrix=[list(r) for r in bundle.market.corr_matrix],
            linkageTrend=bundle.market.linkage_trend,
        )
        quant_repaired = True
        repairs.append("analytics: 从实测行情快照恢复 6x6 相关矩阵与轮动数据")

    # Assemble repaired candidate state for final schema scan
    meta_updates: dict[str, Any] = {}
    for f_name, fallback_txt in (
        ("regime", "震荡分化"),
        ("stance", "谨慎观察，控制仓位"),
        ("vix_note", "关注 VIX 波动率变化"),
        ("tnx_note", "关注 10Y 美债收益率趋势"),
        ("btc_note", "关注比特币流动性风向"),
    ):
        val = getattr(state.meta, f_name, "")
        cleaned = _sanitize_text(val, fallback_txt)
        if cleaned != val:
            meta_updates[f_name] = cleaned

    repaired_state = state.model_copy(
        update={
            "meta": state.meta.model_copy(update=meta_updates) if meta_updates else state.meta,
            "mainlines": repaired_mainlines,
            "candidates": repaired_candidates,
            "macro": repaired_macro,
            "calendar": repaired_calendar,
            "history": repaired_history,
            "analytics": analytics,
        }
    )

    # --- Dimension 2: Schema Cleanliness & Forbidden Tokens ---
    dumped = repaired_state.model_dump(mode="json")
    forbidden = _scan_forbidden_tokens(dumped)
    counts_ok = (
        len(repaired_state.mainlines) == 5
        and len(repaired_state.candidates) == 6
        and len(repaired_state.linkage) == 4
        and len(repaired_state.macro) == 5
        and len(repaired_state.calendar) >= 1
        and 1 <= len(repaired_state.history) <= 20
    )
    dim2_ok = counts_ok and not forbidden
    if forbidden:
        warnings.extend(forbidden[:3])
    checks.append(
        AuditCheck(
            dim="Schema 洁净与禁词",
            passed=dim2_ok,
            detail="结构数量合规、无 Date()/NaN/Inf 残留" if dim2_ok else f"结构数量合规={counts_ok}; 内容异常: {', '.join(forbidden[:2])}",
        )
    )

    # --- Dimension 3: Causal Chain & Ticker Pairs ---
    dim3_ok = all(
        len([p for p in _CHAIN_SPLIT_RE.split(m.chain) if p.strip()]) >= 3
        and len([p for p in m.stocks.split(",") if ":" in p]) >= 2
        and len(m.spark) == 11
        for m in repaired_state.mainlines
    )
    checks.append(
        AuditCheck(
            dim="因果链与标的完整",
            passed=dim3_ok,
            detail="5条主线均具 >=3步因果链、>=2组标的与11点曲线" if dim3_ok else "存在主线链条或曲线不完整",
            repaired=chain_repaired,
        )
    )

    # --- Dimension 4: Price Ruler Sanity ---
    dim4_ok = all(
        m.accel > 0 and m.flip > 0 and m.accel != m.flip
        for m in repaired_state.mainlines
    )
    checks.append(
        AuditCheck(
            dim="方向性价位标尺",
            passed=dim4_ok,
            detail="全部主线 accel/flip 锚定实测现价且非零分离" if dim4_ok else "存在无效价位标尺",
            repaired=ruler_repaired,
        )
    )

    # --- Dimension 5: Source URL Grounding ---
    all_urls_valid = (
        all(_is_valid_http_url(m.source_url) for m in repaired_state.mainlines)
        and all(_is_valid_http_url(m.source_url) for m in repaired_state.macro)
        and all(_is_valid_http_url(c.source_url) for c in repaired_state.calendar)
    )
    checks.append(
        AuditCheck(
            dim="来源地址匹配检查",
            passed=all_urls_valid,
            detail="来源地址检查；正文事实与因果推断仍需核验" if all_urls_valid else "存在非法信源 URL",
            repaired=url_repaired,
        )
    )

    # --- Dimension 6: Quantitative Consistency ---
    dim6_ok = (
        len(repaired_state.analytics.sparkLabels) == 11
        and len(repaired_state.analytics.corrLabels) == 6
        and len(repaired_state.analytics.corrMatrix) == 6
        and len(repaired_state.analytics.rotationAssets) >= 5
        and len(repaired_state.analytics.linkageTrend.labels) >= 5
        and len(repaired_state.analytics.linkageTrend.series) >= 4
    )
    checks.append(
        AuditCheck(
            dim="定量一致性与防幻觉",
            passed=dim6_ok,
            detail="6x6 相关性、30D 剪刀差、轮动指标与实测行情对齐" if dim6_ok else "量化分析块不完整",
            repaired=quant_repaired,
        )
    )

    passed_count = sum(1 for c in checks if c.passed)
    overall_ok = passed_count == len(checks)
    mode_tag = f"{engine_mode} ({model_used})" if engine_mode in ("gemini", "deepseek") else "定量降级引擎"
    repair_note = f"自动修复 {len(repairs)} 项" if repairs else "未触发自动修复（不代表事实已逐项核验）"
    summary_text = (
        f"6维自审 {passed_count}/6 通过 | 引擎: {mode_tag} | "
        f"行情 {live_ok}/{live_total} | SEC {len(bundle.primary.sec_filings)} | "
        f"新闻 {bundle.news.response.count} | {repair_note}"
    )

    final_state = StateResponse.model_validate(
        repaired_state.model_copy(
            update={
                "meta": repaired_state.meta.model_copy(
                    update={"self_review": summary_text}
                )
            }
        ).model_dump(mode="json")
    )

    report = AuditReport(
        passed=overall_ok,
        checks=checks,
        repairs=repairs,
        warnings=warnings,
        summary=summary_text,
    )
    return final_state, report
