"""M3 Research Engine: stateful governance, Gemini causal synthesis, and 6-dim self-audit."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from src.ingestion import IngestionBundle
from src.models.schemas import StateResponse, ThemeComparison
from src.research.analyst import (
    ResearchDraft,
    SynthesisOutput,
    build_deterministic_draft,
    build_research_context,
    synthesize_research_state,
)
from src.research.auditor import AuditCheck, AuditReport, audit_and_repair_state
from src.config import TZ_ET
from src.research.rotation import THEMES, overseas_watch, ranked_baskets, rotation_candidates
from src.research.setups import build_anchor_reviews, build_setups
from src.research.state_machine import (
    GovernanceDecision,
    default_state_path,
    evaluate_governance_window,
    evolve_state,
    load_previous_state,
    save_state,
)


@dataclass
class ResearchResult:
    """Complete output of one M3 research cycle."""

    state: StateResponse
    audit: AuditReport
    governance: GovernanceDecision
    engine_mode: Literal["gemini", "deepseek", "deterministic_fallback"]
    model_used: str
    fallback_reason: str = ""
    saved_path: Path | None = None


async def run_research_cycle(
    bundle: IngestionBundle,
    *,
    state_path: Path | None = None,
    persist: bool = True,
    force_rebalance: bool = False,
    use_llm: bool = True,
    models: tuple[str, ...] | None = None,
    timeout_sec: float = 35.0,
) -> ResearchResult:
    """Execute full M3 pipeline: load prev -> govern -> synthesize -> evolve -> audit -> save."""
    target_path = state_path or default_state_path()
    prev_state = load_previous_state(target_path)
    now_utc = bundle.fetched_at_utc

    governance = evaluate_governance_window(
        now_utc,
        has_prev_state=prev_state is not None,
        force_rebalance=force_rebalance,
    )

    synthesis = await synthesize_research_state(
        bundle,
        prev_state,
        governance,
        use_llm=use_llm,
        models=models,
        timeout_sec=timeout_sec,
    )

    if use_llm and prev_state is not None and synthesis.engine_mode == "deterministic_fallback":
        raise RuntimeError("Model unavailable; preserving previous accepted research: " + synthesis.fallback_reason)

    evolved = evolve_state(
        synthesis.draft_state,
        prev_state,
        governance,
        now_utc,
        bundle.market,
        history_note=synthesis.history_note,
    )
    ranks = ranked_baskets(bundle.market)
    if len(ranks) >= 6:
        candidates = rotation_candidates(
            ranks,
            evolved.mainlines,
            prev_state.candidates if prev_state else [],
            now_utc.astimezone(TZ_ET).date().isoformat(),
            market=bundle.market,
        )
        sig = bundle.market.live.session_rotation
        note = f"美股收盘动量截至 {ranks[0]['as_of']} · {len(ranks)}/{len(THEMES)} 个主题数据齐全（1日40%/5日45%/20日15%相对标普加权，展示前六）。"
        if sig and sig.themes and sig.themes[0].name != ranks[0]["name"]:
            stage = "盘前" if sig.session == "PRE_MARKET" else "盘中"
            note = (
                f"口径对照：今日{stage}实时领涨为「{sig.themes[0].name}」（{sig.themes[0].change:+.2f}%）；"
                f"下方多日收盘动量截至 {ranks[0]['as_of']}，榜首为「{ranks[0]['name']}」（5日 {ranks[0]['r5']:+.2f}%），两者分歧代表短线修复与中期防御切换。"
            )
    else:
        candidates = prev_state.candidates if prev_state else evolved.candidates
        note = f"本轮仅 {len(ranks)}/{len(THEMES)} 个主题数据齐全，暂停横向排名。" + ("展示上期候补。" if prev_state else "展示初始研究候补。")
    evolved = evolved.model_copy(
        update={
            "candidates": candidates,
            "rotation_note": note,
            "session_rotation": bundle.market.live.session_rotation,
            "theme_comparison": [ThemeComparison(name=r["name"], members=list(r["members"]), as_of=r["as_of"],
                                                 r5=r["r5"], r20=r["r20"], excess5=r["rs5"]) for r in ranks],
            "overseas_watch": overseas_watch(bundle.market),
            "setups": build_setups(evolved.mainlines, bundle.market),
            "anchor_reviews": build_anchor_reviews(evolved.mainlines, bundle.market, governance),
        }
    )

    audited_state, audit_report = audit_and_repair_state(
        evolved,
        bundle,
        engine_mode=synthesis.engine_mode,
        model_used=synthesis.model_used,
        prev_state=prev_state,
    )

    if not audit_report.passed:
        raise ValueError("Research audit rejected publication: " + audit_report.summary)

    # Re-validate after recomputing setups/anchor_reviews if audit adjusted mainlines.
    audited_state = StateResponse.model_validate(
        audited_state.model_copy(
            update={
                "setups": build_setups(audited_state.mainlines, bundle.market),
                "anchor_reviews": build_anchor_reviews(audited_state.mainlines, bundle.market, governance),
            }
        ).model_dump(mode="json")
    )

    saved: Path | None = None
    if persist:
        saved = save_state(audited_state, target_path)

    return ResearchResult(
        state=audited_state,
        audit=audit_report,
        governance=governance,
        engine_mode=synthesis.engine_mode,
        model_used=synthesis.model_used,
        fallback_reason=synthesis.fallback_reason,
        saved_path=saved,
    )


__all__ = [
    "AuditCheck",
    "AuditReport",
    "GovernanceDecision",
    "ResearchDraft",
    "ResearchResult",
    "SynthesisOutput",
    "audit_and_repair_state",
    "build_deterministic_draft",
    "build_research_context",
    "default_state_path",
    "evaluate_governance_window",
    "evolve_state",
    "load_previous_state",
    "run_research_cycle",
    "save_state",
    "synthesize_research_state",
]
