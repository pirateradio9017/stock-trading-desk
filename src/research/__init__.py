"""M3 Research Engine: stateful governance, Gemini causal synthesis, and 6-dim self-audit."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from src.ingestion import IngestionBundle
from src.models.schemas import StateResponse
from src.research.analyst import (
    ResearchDraft,
    SynthesisOutput,
    build_deterministic_draft,
    build_research_context,
    synthesize_research_state,
)
from src.research.auditor import AuditCheck, AuditReport, audit_and_repair_state
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

    audited_state, audit_report = audit_and_repair_state(
        evolved,
        bundle,
        engine_mode=synthesis.engine_mode,
        model_used=synthesis.model_used,
    )

    if not audit_report.passed:
        raise ValueError("Research audit rejected publication: " + audit_report.summary)

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
