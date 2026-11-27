"""Stateful M1-M5 mainline & C1-C6 candidate governance state machine with ET anti-jitter locks."""

from __future__ import annotations

import json
import logging
import math
import os
import re
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Literal

from src.config import (
    TZ_CN,
    TZ_ET,
    format_cn_timestamp,
    format_et_short,
    get_market_session,
    get_market_close,
    get_research_window,
    get_us_holiday_name,
    is_us_market_holiday,
    load_settings,
)
from src.ingestion.market_data import MarketSnapshot
from src.models.schemas import (
    CandidateItem,
    HistoryEntry,
    MainlineItem,
    MarketSession,
    StateResponse,
)

logger = logging.getLogger(__name__)

GovernanceWindow = Literal[
    "HOLIDAY_LOCK",
    "INTRADAY_LOCK",
    "POST_CLOSE_SETTLE",
    "REBALANCE_WINDOW",
    "PRE_MARKET",
    "OFF_HOURS",
]

_STATUS_PREFIX_RE = re.compile(r"^\[First_seen:[^\]]*\]\s*")

_TIER_LABELS: dict[str, tuple[str, Literal["sage", "ochre", "slate"]]] = {
    "upgrade": ("准备升级", "sage"),
    "watch": ("强化跟踪", "ochre"),
    "demote": ("弱化/淘汰", "slate"),
}


@dataclass(frozen=True)
class GovernanceDecision:
    """Resolved session governance window and anti-jitter lock policy."""

    window: GovernanceWindow
    market_session: MarketSession
    allow_structural_rebalance: bool
    is_holiday: bool
    holiday_name: str | None
    reason: str
    badge_suffix: str


def evaluate_governance_window(
    now_utc: datetime,
    *,
    has_prev_state: bool = True,
    force_rebalance: bool = False,
) -> GovernanceDecision:
    """Evaluate ET market session rules to determine whether structural rebalance is allowed.

    Rules:
      - Cold start (no previous state): always allow initial mainline construction.
      - HOLIDAY_LOCK (NYSE full-day holiday): strict anti-jitter lock unless force_rebalance=True.
      - INTRADAY_LOCK (09:30-15:30 ET): strict anti-jitter lock unless force_rebalance=True.
      - POST_CLOSE_SETTLE (15:30-16:30 ET): hold structure until 16:30 ET rebalance window.
      - REBALANCE_WINDOW (16:30-17:00 ET): allow full structural rebalance, promotions, and demotions.
      - PRE_MARKET / OFF_HOURS: preserve prior structure unless cold start or force_rebalance=True.
    """
    et_dt = now_utc.astimezone(TZ_ET)
    close_hour = get_market_close(et_dt.date()).hour
    window = get_research_window(now_utc)
    session = get_market_session(now_utc)
    holiday_name = get_us_holiday_name(et_dt.date())
    is_holiday = holiday_name is not None

    if not has_prev_state:
        suffix = f"{holiday_name}休市建档" if is_holiday else "冷启动初始化建档"
        return GovernanceDecision(
            window=window,
            market_session=session,
            allow_structural_rebalance=True,
            is_holiday=is_holiday,
            holiday_name=holiday_name,
            reason="首次运行无历史 state.json，执行主线与候补池初始化建档",
            badge_suffix=suffix,
        )

    if force_rebalance:
        suffix = "强制复盘重排"
        return GovernanceDecision(
            window=window,
            market_session=session,
            allow_structural_rebalance=True,
            is_holiday=is_holiday,
            holiday_name=holiday_name,
            reason="用户或调度器显式触发强制调仓评估（force_rebalance=True）",
            badge_suffix=suffix,
        )

    if window == "HOLIDAY_LOCK":
        h_label = holiday_name or "美股休市"
        return GovernanceDecision(
            window=window,
            market_session=session,
            allow_structural_rebalance=False,
            is_holiday=True,
            holiday_name=holiday_name,
            reason=f"{h_label}全天休市，锁定 M1-M5 主线与候补骨架，仅刷新跨市场/新闻上下文",
            badge_suffix=f"{h_label}休市防抖动锁定",
        )

    if window == "INTRADAY_LOCK":
        return GovernanceDecision(
            window=window,
            market_session=session,
            allow_structural_rebalance=False,
            is_holiday=False,
            holiday_name=None,
            reason=f"美股盘中 09:30-{close_hour-1}:30 ET 防抖窗口，锁定主线骨架防止盘中噪音频繁换线",
            badge_suffix=f"盘中防抖动锁定 (09:30-{close_hour-1}:30 ET)",
        )

    if window == "POST_CLOSE_SETTLE":
        return GovernanceDecision(
            window=window,
            market_session=session,
            allow_structural_rebalance=False,
            is_holiday=False,
            holiday_name=None,
            reason=f"尾盘及盘后结算期 ({close_hour-1}:30-{close_hour}:30 ET)，等待研究调整窗口定型",
            badge_suffix="尾盘结算等待调仓",
        )

    if window == "REBALANCE_WINDOW":
        return GovernanceDecision(
            window=window,
            market_session=session,
            allow_structural_rebalance=True,
            is_holiday=False,
            holiday_name=None,
            reason=f"美股收盘后 {close_hour}:30-{close_hour+1}:00 ET 研究窗口，允许主线晋级、淘汰与权重重排",
            badge_suffix=f"盘后研究评估窗口 ({close_hour}:30 ET)",
        )

    if window == "PRE_MARKET":
        return GovernanceDecision(
            window=window,
            market_session=session,
            allow_structural_rebalance=False,
            is_holiday=False,
            holiday_name=None,
            reason="盘前流动性较薄 (04:00-09:30 ET)，锁定昨日主线骨架并更新盘前观察要点",
            badge_suffix="盘前防抖观察锁定",
        )

    return GovernanceDecision(
        window=window,
        market_session=session,
        allow_structural_rebalance=False,
        is_holiday=False,
        holiday_name=None,
        reason="非交易/周末时段防抖锁定，维持主线结构并跟踪跨市场与公告催化",
        badge_suffix="盘后/周末防抖动锁定",
    )


def default_state_path() -> Path:
    """Return default persisted state.json path from dynamic settings."""
    return load_settings().data_dir / "state.json"


def load_previous_state(state_path: Path | None = None) -> StateResponse | None:
    """Load and validate previous state.json if it exists; return None on cold start or corruption."""
    path = state_path or default_state_path()
    if not path.exists():
        return None
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
        return StateResponse.model_validate(raw)
    except Exception as exc:
        logger.warning("Failed to load previous state from %s: %s", path, exc)
        raise ValueError(f"Invalid previous research state: {path}") from exc


def save_state(state: StateResponse, state_path: Path | None = None) -> Path:
    """Atomically persist StateResponse to state.json."""
    path = state_path or default_state_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(".json.tmp")
    payload = state.model_dump(mode="json")
    tmp_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    os.replace(tmp_path, path)
    return path


def _extract_syms(stocks_str: str) -> list[str]:
    syms: list[str] = []
    for part in (stocks_str or "").split(","):
        part = part.strip()
        if ":" in part:
            sym = part.split(":", 1)[0].strip()
            if sym:
                syms.append(sym)
    return syms


def _strip_status_prefix(status: str) -> str:
    cleaned = _STATUS_PREFIX_RE.sub("", (status or "").strip()).strip()
    return cleaned or "持续跟踪量价与催化"


def _format_status(first_seen: str, score: int, body: str) -> str:
    clean_body = _strip_status_prefix(body)
    return f"[First_seen: {first_seen} | Score: {score}] {clean_body}"


def _refresh_spark(anchor_sym: str, fallback_spark: list[float], market: MarketSnapshot) -> list[float]:
    live_spark = market.symbol_sparks.get(anchor_sym)
    if live_spark and len(live_spark) == 11 and all(math.isfinite(x) for x in live_spark):
        return [round(float(x), 2) for x in live_spark]
    valid = [round(float(x), 2) for x in fallback_spark if math.isfinite(x)]
    if len(valid) == 11:
        return valid
    if len(valid) >= 2:
        if len(valid) > 11:
            return valid[-11:]
        return [valid[0]] * (11 - len(valid)) + valid
    return [100.0] * 11


def _compute_basket_returns(
    stocks_str: str,
    market: MarketSnapshot,
    fallback_r5: float,
    fallback_r20: float,
) -> tuple[float, float]:
    r5_vals: list[float] = []
    r20_vals: list[float] = []
    for sym in _extract_syms(stocks_str):
        q = market.get_quote(sym)
        if q:
            if q.r5 is not None and math.isfinite(q.r5):
                r5_vals.append(float(q.r5))
            if q.r20 is not None and math.isfinite(q.r20):
                r20_vals.append(float(q.r20))
    r5 = round(sum(r5_vals) / len(r5_vals), 2) if r5_vals else round(float(fallback_r5), 2)
    r20 = round(sum(r20_vals) / len(r20_vals), 2) if r20_vals else round(float(fallback_r20), 2)
    return r5, r20


def _ensure_ruler_levels(
    anchor_sym: str,
    accel: float,
    flip: float,
    market: MarketSnapshot,
) -> tuple[float, float]:
    q = market.get_quote(anchor_sym)
    price = q.price if (q and q.price and q.price > 0) else None
    a = float(accel) if (math.isfinite(accel) and accel > 0) else 0.0
    f = float(flip) if (math.isfinite(flip) and flip > 0) else 0.0

    if price is not None:
        if a <= 0 or a < price * 0.6 or a > price * 1.4:
            a = round(price * 1.04, 2)
        if f <= 0 or f < price * 0.6 or f > price * 1.4:
            f = round(price * 0.95, 2)
        if abs(a - f) < max(0.01, price * 0.005):
            a = round(price * 1.04, 2)
            f = round(price * 0.95, 2)
    else:
        if a <= 0:
            a = 105.0
        if f <= 0 or abs(a - f) < 0.01:
            f = round(max(0.01, a * 0.92), 2)

    return round(a, 2), round(f, 2)


def _prev_state_et_date(prev_state: StateResponse) -> date | None:
    """Extract previous state's America/New_York trading date from serverSyncedAt (UTC+8)."""
    m = re.match(
        r"^(\d{4})[-/](\d{2})[-/](\d{2})[T ](\d{2}):(\d{2})(?::(\d{2}))?",
        prev_state.serverSyncedAt,
    )
    if m:
        try:
            dt_cn = datetime(
                int(m.group(1)),
                int(m.group(2)),
                int(m.group(3)),
                int(m.group(4)),
                int(m.group(5)),
                int(m.group(6) or 0),
                tzinfo=TZ_CN,
            )
            return dt_cn.astimezone(TZ_ET).date()
        except ValueError:
            pass
    if prev_state.history:
        m2 = re.match(r"^(\d{4})-(\d{2})-(\d{2})", prev_state.history[0].time)
        if m2:
            try:
                return date(int(m2.group(1)), int(m2.group(2)), int(m2.group(3)))
            except ValueError:
                pass
    return None


def _match_mainline(target: MainlineItem, pool: list[MainlineItem]) -> MainlineItem | None:
    for item in pool:
        if item.anchor_sym.upper() == target.anchor_sym.upper():
            return item
    return None


def _match_candidate(target: CandidateItem, pool: list[CandidateItem]) -> CandidateItem | None:
    # Slots and overlapping tickers are not stable research identities.
    return next((item for item in pool if item.name == target.name
                 and set(_extract_syms(item.stocks)) == set(_extract_syms(target.stocks))), None)



def evolve_state(
    draft_state: StateResponse,
    prev_state: StateResponse | None,
    decision: GovernanceDecision,
    now_utc: datetime,
    market: MarketSnapshot,
    *,
    history_note: tuple[str, str, str] | None = None,
) -> StateResponse:
    """Apply stateful M1-M5/C1-C6 evolution, anti-jitter locks, and history[20] governance."""
    et_today = now_utc.astimezone(TZ_ET).date()
    et_today_iso = et_today.isoformat()
    cn_ts = now_utc.astimezone(TZ_CN).strftime("%Y-%m-%d %H:%M UTC+8")
    et_short = format_et_short(now_utc)
    updated_at_full = f"{et_short} · {decision.badge_suffix}"[:120]

    meta = draft_state.meta.model_copy(
        update={
            "updated_at": updated_at_full,
            "updated_at_short": et_short,
        }
    )

    # Case 1: Cold start (no previous state)
    if prev_state is None:
        evolved_mainlines: list[MainlineItem] = []
        for idx, m in enumerate(draft_state.mainlines[:5], start=1):
            score = int(max(10, min(98, round(m.score))))
            accel, flip = _ensure_ruler_levels(m.anchor_sym, m.accel, m.flip, market)
            evolved_mainlines.append(
                m.model_copy(
                    update={
                        "id": f"M{idx}",
                        "first_seen": et_today_iso,
                        "score": score,
                        "status": _format_status(et_today_iso, score, m.status),
                        "accel": accel,
                        "flip": flip,
                        "spark": _refresh_spark(m.anchor_sym, m.spark, market),
                    }
                )
            )

        evolved_candidates: list[CandidateItem] = []
        for idx, c in enumerate(draft_state.candidates[:6], start=1):
            r5, r20 = _compute_basket_returns(c.stocks, market, c.r5, c.r20)
            tier = c.tier
            if r5 <= -6.0 and r20 <= -12.0:
                tier = "demote"
            elif tier == "upgrade" and r5 < 0.0 and r20 < 0.0:
                tier = "watch"
            tier_label, default_tone = _TIER_LABELS[tier]
            log_text = c.history_log.strip() or f"{et_today_iso} 初始纳入候补观察（5D {r5:+.2f}% / 20D {r20:+.2f}%）"
            evolved_candidates.append(
                c.model_copy(
                    update={
                        "id": f"C{idx}",
                        "tier": tier,
                        "tier_label": tier_label,
                        "tone": default_tone,
                        "r5": r5,
                        "r20": r20,
                        "first_seen": et_today_iso,
                        "stale_days": 0,
                        "history_log": log_text,
                    }
                )
            )

        note_title, note_detail, note_lesson = history_note or (
            f"初始化建档 · {meta.badge}",
            f"基于 {market.live.count} 只跨市场实时行情与一手公告完成 M1-M5 主线及 C1-C6 候补池首次建档。",
            "首版建档锚定量价与因果传导证据，后续盘中与休市期启用防抖锁定，仅在收盘后研究窗口评估换线。",
        )
        init_entry = HistoryEntry(
            time=cn_ts,
            type="主线复盘",
            tone="sage",
            title=note_title,
            detail=note_detail,
            lesson=note_lesson,
        )
        return draft_state.model_copy(
            update={
                "serverSyncedAt": format_cn_timestamp(now_utc),
                "meta": meta,
                "mainlines": evolved_mainlines,
                "candidates": evolved_candidates,
                "history": [init_entry],
            }
        )

    prev_date = _prev_state_et_date(prev_state)
    day_advanced = prev_date is not None and et_today > prev_date and et_today.weekday() < 5 and not is_us_market_holiday(et_today)

    # Case 2: Locked governance window (anti-jitter lock preserves M1-M5 structural slots)
    if not decision.allow_structural_rebalance:
        locked_mainlines: list[MainlineItem] = []
        for idx, prev_m in enumerate(prev_state.mainlines[:5], start=1):
            matched = _match_mainline(prev_m, draft_state.mainlines)
            if matched is not None:
                raw_delta = matched.score - prev_m.score
                clamped_delta = max(-5, min(5, raw_delta))
                new_score = int(max(10, min(98, prev_m.score + clamped_delta)))
                same_anchor = matched.anchor_sym.upper() == prev_m.anchor_sym.upper()
                new_chain = matched.chain if same_anchor else prev_m.chain
                new_plain = matched.plain if same_anchor else prev_m.plain
                new_summary = matched.summary if same_anchor else prev_m.summary
                new_watch = matched.watch_points if same_anchor else prev_m.watch_points
                new_url = (matched.source_url or prev_m.source_url) if same_anchor else prev_m.source_url
                new_reasoning = matched.reasoning if same_anchor else None
                cand_accel = matched.accel if same_anchor else prev_m.accel
                cand_flip = matched.flip if same_anchor else prev_m.flip
            else:
                new_score = prev_m.score
                new_chain = prev_m.chain
                new_plain = prev_m.plain
                new_summary = prev_m.summary
                new_watch = prev_m.watch_points
                new_url = prev_m.source_url
                new_reasoning = None
                cand_accel = prev_m.accel
                cand_flip = prev_m.flip

            accel, flip = _ensure_ruler_levels(prev_m.anchor_sym, cand_accel, cand_flip, market)
            status_body = f"防抖锁定保持骨架（{decision.window}）"
            locked_mainlines.append(
                prev_m.model_copy(
                    update={
                        "id": f"M{idx}",
                        "score": new_score,
                        "status": _format_status(prev_m.first_seen, new_score, status_body),
                        "chain": new_chain,
                        "accel": accel,
                        "flip": flip,
                        "spark": _refresh_spark(prev_m.anchor_sym, prev_m.spark, market),
                        "plain": new_plain,
                        "summary": new_summary,
                        "watch_points": new_watch,
                        "source_url": new_url,
                        "reasoning": new_reasoning,
                    }
                )
            )

        locked_candidates: list[CandidateItem] = []
        for idx, prev_c in enumerate(prev_state.candidates[:6], start=1):
            r5, r20 = _compute_basket_returns(prev_c.stocks, market, prev_c.r5, prev_c.r20)
            stale = prev_c.stale_days
            if day_advanced:
                stale = stale + 1 if abs(r5) < 1.5 else 0
            tier = prev_c.tier
            # Structural demotion is deferred until the rebalance window.
            tier_label, default_tone = _TIER_LABELS[tier]
            log_text = prev_c.history_log.strip() or f"{prev_c.first_seen} 持续跟踪"
            locked_candidates.append(
                prev_c.model_copy(
                    update={
                        "id": f"C{idx}",
                        "tier": tier,
                        "tier_label": tier_label,
                        "tone": default_tone if tier != prev_c.tier else prev_c.tone,
                        "r5": r5,
                        "r20": r20,
                        "stale_days": stale,
                        "history_log": log_text,
                    }
                )
            )

        lock_entry = HistoryEntry(
            time=cn_ts,
            type="防抖锁定",
            tone="slate",
            title=f"{decision.badge_suffix} · 维持 M1-M5 主线骨架",
            detail=f"{decision.reason}。最新行情与跨市场动态已同步，主线排名与归属等待收盘后研究窗口统一复核。",
            lesson="遵守时段纪律：盘中噪音与休市无量期不随意切换主线，避免追涨杀跌导致观察框架漂移。",
        )
        history = list(prev_state.history)
        same_et_day = prev_date == et_today
        if (
            history
            and history[0].type == "防抖锁定"
            and same_et_day
            and decision.badge_suffix in history[0].title
        ):
            history[0] = lock_entry
        else:
            history.insert(0, lock_entry)

        return draft_state.model_copy(
            update={
                "serverSyncedAt": format_cn_timestamp(now_utc),
                "meta": meta,
                "mainlines": locked_mainlines,
                "candidates": locked_candidates,
                "history": history[:20],
            }
        )

    # Case 3: Rebalance window (16:30 ET or force_rebalance=True)
    prev_anchors = {m.anchor_sym.upper(): m for m in prev_state.mainlines}
    prev_cand_anchors = {
        sym.upper(): c
        for c in prev_state.candidates
        for sym in _extract_syms(c.stocks)
    }

    rebalanced_mainlines: list[MainlineItem] = []
    promoted_titles: list[str] = []
    new_anchors: set[str] = set()

    for idx, m in enumerate(draft_state.mainlines[:5], start=1):
        matched_prev = _match_mainline(m, prev_state.mainlines)
        anchor_upper = m.anchor_sym.upper()
        new_anchors.add(anchor_upper)

        if matched_prev is not None and matched_prev.anchor_sym.upper() == anchor_upper:
            first_seen = matched_prev.first_seen
            status_body = _strip_status_prefix(m.status)
        else:
            first_seen = et_today_iso
            promoted_titles.append(f"{m.title}({m.anchor_sym})")
            from_cand = prev_cand_anchors.get(anchor_upper)
            status_body = (
                f"由候补 {from_cand.id} 晋升主线 · {_strip_status_prefix(m.status)}"
                if from_cand
                else f"新晋主线 · {_strip_status_prefix(m.status)}"
            )

        score = int(max(10, min(98, round(m.score))))
        accel, flip = _ensure_ruler_levels(m.anchor_sym, m.accel, m.flip, market)
        rebalanced_mainlines.append(
            m.model_copy(
                update={
                    "id": f"M{idx}",
                    "first_seen": first_seen,
                    "score": score,
                    "status": _format_status(first_seen, score, status_body),
                    "accel": accel,
                    "flip": flip,
                    "spark": _refresh_spark(m.anchor_sym, m.spark, market),
                }
            )
        )

    dropped_titles = [
        f"{pm.title}({pm.anchor_sym})"
        for anchor, pm in prev_anchors.items()
        if anchor not in new_anchors
    ]

    rebalanced_candidates: list[CandidateItem] = []
    demoted_cands: list[str] = []
    for idx, c in enumerate(draft_state.candidates[:6], start=1):
        matched_prev_c = _match_candidate(c, prev_state.candidates)
        r5, r20 = _compute_basket_returns(c.stocks, market, c.r5, c.r20)
        if matched_prev_c is not None:
            first_seen = matched_prev_c.first_seen
            stale = matched_prev_c.stale_days
            if day_advanced:
                if abs(r5) < 1.5 and r20 < 3.0:
                    stale += 1
                elif abs(r5) >= 2.5:
                    stale = 0
        else:
            first_seen = et_today_iso
            stale = 0

        tier = c.tier
        if stale >= 5 or (r5 <= -6.0 and r20 <= -12.0):
            tier = "demote"
            demoted_cands.append(c.name)
        elif r5 >= 2.5 and r20 >= 6.0 and stale == 0:
            tier = "upgrade"

        tier_label, default_tone = _TIER_LABELS[tier]
        prev_log = (matched_prev_c.history_log if matched_prev_c else "").strip()
        step_note = f"{et_today_iso}:{tier_label}(5D {r5:+.1f}%)"
        combined_log = f"{prev_log} -> {step_note}" if prev_log and step_note not in prev_log else (c.history_log.strip() or step_note)
        combined_log = combined_log[-180:]

        rebalanced_candidates.append(
            c.model_copy(
                update={
                    "id": f"C{idx}",
                    "tier": tier,
                    "tier_label": tier_label,
                    "tone": default_tone,
                    "r5": r5,
                    "r20": r20,
                    "first_seen": first_seen,
                    "stale_days": stale,
                    "history_log": combined_log,
                }
            )
        )

    history = list(prev_state.history)
    if promoted_titles:
        ev_type = "晋升主线"
        ev_tone: Literal["sage", "terra", "ochre", "slate"] = "sage"
        ev_title = f"盘后研究晋升：{', '.join(promoted_titles[:2])}"
        drop_clause = f"；同时移出 {', '.join(dropped_titles[:2])}" if dropped_titles else ""
        ev_detail = f"收盘定型确认资金强弱切换，新晋主线 {', '.join(promoted_titles)}{drop_clause}。"
        ev_lesson = "主线轮动必须由收盘后多日量价与基本面催化共同确认，淘汰转弱旧线才能保持组合锐度。"
    elif dropped_titles or demoted_cands:
        ev_type = "降级淘汰"
        ev_tone = "terra"
        targets = dropped_titles or demoted_cands
        ev_title = f"调仓弱化/淘汰：{', '.join(targets[:2])}"
        ev_detail = f"动量衰减或持续停滞触发降级规则（{', '.join(targets[:3])}），收紧跟踪权重。"
        ev_lesson = "对长期停滞或显著跑输基准的伪主线果断降级，把注意力集中在资金真实共识链条。"
    else:
        ev_type = "主线复盘"
        ev_tone = "ochre"
        if history_note:
            ev_title, ev_detail, ev_lesson = history_note
        else:
            ev_title = f"收盘复盘定型 · {meta.badge}"
            ev_detail = f"盘后研究复核 M1-M5 主线与 C1-C6 候补强弱，核心主线结构延续，更新评分与触发锚点。"
            ev_lesson = "趋势延续期以关键加速位/证伪位跟踪为主，不因单日波动破坏主线框架。"

    history.insert(
        0,
        HistoryEntry(
            time=cn_ts,
            type=ev_type,
            tone=ev_tone,
            title=ev_title,
            detail=ev_detail,
            lesson=ev_lesson,
        ),
    )

    return draft_state.model_copy(
        update={
            "serverSyncedAt": format_cn_timestamp(now_utc),
            "meta": meta,
            "mainlines": rebalanced_mainlines,
            "candidates": rebalanced_candidates,
            "history": history[:20],
        }
    )
