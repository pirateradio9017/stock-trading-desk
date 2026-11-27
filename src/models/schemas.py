"""Unified Pydantic v2 API and state contracts for Stock Trading Desk v2.0."""

from __future__ import annotations

import math
import re
from datetime import date, datetime
from typing import Annotated, Literal
from urllib.parse import urlparse

from pydantic import (
    AfterValidator,
    BaseModel,
    BeforeValidator,
    ConfigDict,
    Field,
    model_validator,
)

Tone = Literal["sage", "terra", "ochre", "slate"]
MarketSession = Literal["OFF_HOURS", "PRE_MARKET", "INTRADAY", "AFTER_HOURS", "HOLIDAY"]
CandidateTier = Literal["upgrade", "watch", "demote"]
MacroChainId = Literal["rate", "credit", "sentiment", "compute", "semi_demand"]

_ISO_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_CLEAN_TS_RE = re.compile(
    r"^(\d{4}-\d{2}-\d{2})"
    r"(?:[T ](\d{2}):(\d{2})(?::(\d{2}))?(?:\.\d+)?"
    r"(?:\s*(?:Z|[+-]\d{2}:?\d{2}|UTC(?:[+-]\d{1,2}(?::\d{2})?)?|ET|EST|EDT))?"
    r"(?:\s*·.*)?)?$"
)
_SYNCED_TS_RE = re.compile(
    r"^(?:(\d{4})-(\d{2})-(\d{2})|(\d{4})/(\d{2})/(\d{2}))"
    r"(?:[T ](\d{2}):(\d{2})(?::(\d{2}))?(?:\.\d+)?"
    r"(?:\s*(?:Z|[+-]\d{2}:?\d{2}|UTC(?:[+-]\d{1,2}(?::\d{2})?)?|ET|EST|EDT))?"
    r"(?:\s*·.*)?)?$"
)
_ET_SHORT_TS_RE = re.compile(
    r"^(\d{2})-(\d{2})[T ](\d{2}):(\d{2})(?::(\d{2}))?"
    r"(?:\s+(?:ET|EST|EDT|UTC(?:[+-]\d{1,2}(?::\d{2})?)?)(?:\s+.*)?)?$"
)


def _reject_gviz(v: str) -> str:
    if "date(" in v.lower():
        raise ValueError("Google gviz Date(...) syntax is forbidden")
    return v


def _validate_hms(
    hour_str: str | None,
    minute_str: str | None,
    second_str: str | None,
) -> None:
    if hour_str is None or minute_str is None:
        return
    hh = int(hour_str)
    mm = int(minute_str)
    ss = int(second_str) if second_str is not None else 0
    if not (0 <= hh <= 23 and 0 <= mm <= 59 and 0 <= ss <= 59):
        raise ValueError(
            f"Time component out of bounds: {hour_str}:{minute_str}"
            + (f":{second_str}" if second_str is not None else "")
        )


def _validate_iso_date(v: object) -> str:
    if isinstance(v, datetime):
        raise ValueError("Expected YYYY-MM-DD date, got datetime")
    if isinstance(v, date):
        return v.isoformat()
    if not isinstance(v, str):
        raise ValueError("Expected YYYY-MM-DD date string")
    s = v.strip()
    _reject_gviz(s)
    if not _ISO_DATE_RE.match(s):
        raise ValueError(f"Expected YYYY-MM-DD ISO date, got {s!r}")
    date.fromisoformat(s)
    return s


def _validate_clean_timestamp(v: str) -> str:
    s = v.strip()
    if not s:
        raise ValueError("Timestamp must not be empty")
    _reject_gviz(s)
    m = _CLEAN_TS_RE.match(s)
    if not m:
        raise ValueError(f"Expected clean YYYY-MM-DD timestamp, got {s!r}")
    date.fromisoformat(m.group(1))
    _validate_hms(m.group(2), m.group(3), m.group(4))
    return s


def _validate_synced_timestamp(v: str) -> str:
    s = v.strip()
    if not s:
        raise ValueError("Timestamp must not be empty")
    _reject_gviz(s)
    m = _SYNCED_TS_RE.match(s)
    if not m:
        raise ValueError(f"Expected YYYY/MM/DD or YYYY-MM-DD timestamp, got {s!r}")
    if m.group(1) is not None:
        year, month, day = int(m.group(1)), int(m.group(2)), int(m.group(3))
    else:
        year, month, day = int(m.group(4)), int(m.group(5)), int(m.group(6))
    date(year, month, day)
    _validate_hms(m.group(7), m.group(8), m.group(9))
    return s


def _validate_et_timestamp(v: str) -> str:
    s = v.strip()
    if not s:
        raise ValueError("Timestamp must not be empty")
    _reject_gviz(s)
    m_full = _CLEAN_TS_RE.match(s)
    if m_full:
        date.fromisoformat(m_full.group(1))
        _validate_hms(m_full.group(2), m_full.group(3), m_full.group(4))
        return s
    m_et = _ET_SHORT_TS_RE.match(s)
    if not m_et:
        raise ValueError(
            f"Expected 'MM-DD HH:MM ET...' or 'YYYY-MM-DD...' timestamp, got {s!r}"
        )
    month, day = int(m_et.group(1)), int(m_et.group(2))
    # Use leap year 2024 to validate calendar month/day without rejecting Feb 29.
    date(2024, month, day)
    _validate_hms(m_et.group(3), m_et.group(4), m_et.group(5))
    return s


def _validate_no_gviz_str(v: str) -> str:
    s = v.strip()
    if not s:
        raise ValueError("String must not be empty")
    return _reject_gviz(s)


def _check_http_url(s: str) -> str:
    if any(ch.isspace() or ord(ch) < 32 for ch in s):
        raise ValueError(f"URL must not contain whitespace or control characters: {s!r}")
    parsed = urlparse(s)
    if parsed.scheme not in ("http", "https") or not parsed.netloc or not parsed.hostname:
        raise ValueError(f"Expected valid http(s) URL with hostname, got {s!r}")
    return s


def _validate_optional_http_url(v: str) -> str:
    s = v.strip()
    if not s:
        return ""
    return _check_http_url(s)


def _validate_required_http_url(v: str) -> str:
    s = v.strip()
    if not s:
        raise ValueError("Expected non-empty http(s) URL, got ''")
    return _check_http_url(s)


def _reject_bool_float(v: object) -> object:
    if isinstance(v, bool):
        raise ValueError("Boolean value is not allowed for float field")
    return v


def _validate_finite_float(v: float) -> float:
    if math.isnan(v) or math.isinf(v):
        raise ValueError("Value must be finite (NaN/Inf forbidden)")
    return v


IsoDateStr = Annotated[str, BeforeValidator(_validate_iso_date)]
CleanTimestampStr = Annotated[str, AfterValidator(_validate_clean_timestamp)]
SyncedTimestampStr = Annotated[str, AfterValidator(_validate_synced_timestamp)]
EtTimestampStr = Annotated[str, AfterValidator(_validate_et_timestamp)]
NoGvizStr = Annotated[str, AfterValidator(_validate_no_gviz_str)]
OptionalHttpUrl = Annotated[str, AfterValidator(_validate_optional_http_url)]
RequiredHttpUrl = Annotated[str, AfterValidator(_validate_required_http_url)]
FiniteFloat = Annotated[
    float,
    BeforeValidator(_reject_bool_float),
    AfterValidator(_validate_finite_float),
]


class _DeskModel(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True)


class HealthResponse(_DeskModel):
    ok: bool = True
    version: str = Field(default="2.0.0", min_length=1)
    serverTimeUtc: CleanTimestampStr
    stateUpdatedAt: CleanTimestampStr | None = None
    newsCount: int = Field(default=0, ge=0)
    quotesCount: int = Field(default=0, ge=0)


class QuoteItem(_DeskModel):
    sym: str = Field(min_length=1)
    name: str = ""
    price: FiniteFloat | None = Field(default=None, ge=0)
    chg: FiniteFloat | None = None
    ahPrice: FiniteFloat | None = Field(default=None, ge=0)
    ahChg: FiniteFloat | None = None
    r5: FiniteFloat | None = None
    r20: FiniteFloat | None = None
    source: str = Field(default="yahoo_v8", min_length=1)
    tsUtc: CleanTimestampStr


class SessionPrint(_DeskModel):
    sym: str
    change: FiniteFloat
    trade_utc: CleanTimestampStr
    price: FiniteFloat | None = None


class SessionTheme(_DeskModel):
    name: str
    change: FiniteFloat
    excess: FiniteFloat
    outperformers: int
    members: list[SessionPrint]


class SessionRotation(_DeskModel):
    session: Literal["PRE_MARKET", "INTRADAY"]
    sampled_utc: CleanTimestampStr
    benchmark: SessionPrint | None
    themes: list[SessionTheme]
    universe: int


class LiveResponse(_DeskModel):
    session_rotation: SessionRotation | None = None
    updatedAt: SyncedTimestampStr
    updatedAtEt: EtTimestampStr
    marketSession: MarketSession
    count: int = Field(ge=0)
    quotes: list[QuoteItem] = Field(default_factory=list)

    @model_validator(mode="after")
    def _check_count_matches_quotes(self) -> LiveResponse:
        if self.count != len(self.quotes):
            raise ValueError(
                f"LiveResponse count ({self.count}) != len(quotes) ({len(self.quotes)})"
            )
        return self


class MetaInfo(_DeskModel):
    updated_at: EtTimestampStr = Field(min_length=1, max_length=120)
    updated_at_short: EtTimestampStr = Field(min_length=1, max_length=40)
    badge: str = Field(min_length=1)
    headline: str = Field(min_length=1)
    plain_guide: str = Field(min_length=1)
    next_gate: str = Field(min_length=1)
    regime: str = ""
    stance: str = ""
    vix_note: str = ""
    tnx_note: str = ""
    btc_note: str = ""
    self_review: str = ""
    market_brief: str = ""


class ResearchEvidence(_DeskModel):
    kind: Literal["market", "filing", "report"]
    text: str = Field(min_length=1, max_length=450)
    source: str = Field(min_length=1, max_length=120)
    as_of: str | None = Field(default=None, max_length=45)
    source_url: RequiredHttpUrl


class MainlineReasoning(_DeskModel):
    evidence: list[ResearchEvidence] = Field(default_factory=list, max_length=5)
    hypothesis: str | None = Field(default=None, max_length=500)
    confirmation: str | None = Field(default=None, max_length=500)
    invalidation: str | None = Field(default=None, max_length=500)
    uncertainty: str | None = Field(default=None, max_length=500)


class MainlineItem(_DeskModel):
    id: str = Field(pattern=r"^M[1-5]$")
    title: str = Field(min_length=1)
    action: str = Field(min_length=1)
    action_tone: Tone
    first_seen: IsoDateStr
    score: int = Field(ge=0, le=100)
    status: str = Field(min_length=1)
    chain: str = Field(min_length=1)
    stocks: str = Field(min_length=1)
    anchor_sym: str = Field(min_length=1)
    accel: FiniteFloat
    flip: FiniteFloat
    spark: list[FiniteFloat] = Field(min_length=2, max_length=60)
    plain: str = Field(min_length=1)
    summary: str = Field(min_length=1)
    watch_points: str = Field(min_length=1)
    source_url: OptionalHttpUrl = ""
    reasoning: MainlineReasoning | None = None

    @model_validator(mode="after")
    def _check_directional_ruler(self) -> MainlineItem:
        if self.accel == self.flip:
            raise ValueError("accel and flip must differ for a non-zero directional ruler")
        return self


class CandidateItem(_DeskModel):
    id: str = Field(pattern=r"^C[1-9]\d*$")
    tier: CandidateTier
    tier_label: str = Field(min_length=1)
    tone: Tone
    name: str = Field(min_length=1)
    linked_to: str = Field(min_length=1)
    stocks: str = ""
    r5: FiniteFloat
    r20: FiniteFloat
    plain: str = Field(min_length=1)
    rule: str = Field(min_length=1)
    first_seen: IsoDateStr
    stale_days: int = Field(default=0, ge=0)
    history_log: str = ""
    as_of: IsoDateStr | None = None
    excess5: FiniteFloat | None = None


class LinkageStory(_DeskModel):
    pair: str = Field(min_length=1)
    corr: str = Field(min_length=1)
    tone: Tone
    why: str = Field(min_length=1)
    signal: str = Field(min_length=1)


class MacroDimItem(_DeskModel):
    chain_id: MacroChainId
    title: str = Field(min_length=1)
    state: str = Field(min_length=1)
    wind: str = Field(min_length=1)
    score: int = Field(ge=0, le=100)
    feature: str = ""
    concern: str = ""
    plain: str = Field(min_length=1)
    m1_label: str = ""
    m1_val: str = ""
    m1_date: str = ""
    m1_sub: str = ""
    m2_label: str = ""
    m2_val: str = ""
    m2_date: str = ""
    m2_sub: str = ""
    source_url: OptionalHttpUrl = ""


class CalendarEvent(_DeskModel):
    day: IsoDateStr
    time: str = Field(min_length=1)
    title: str = Field(min_length=1)
    tag: str = Field(min_length=1)
    tone: Tone
    desc: str = Field(min_length=1)
    source_url: OptionalHttpUrl = ""


class HistoryEntry(_DeskModel):
    time: CleanTimestampStr
    type: str = Field(min_length=1)
    tone: Tone
    title: str = Field(min_length=1)
    detail: str = Field(min_length=1)
    lesson: str = ""


class RotationAsset(_DeskModel):
    sym: str = Field(min_length=1)
    name: str = Field(min_length=1)
    group: str = Field(min_length=1)
    r5: FiniteFloat
    r20: FiniteFloat


class LinkageTrendSeries(_DeskModel):
    sym: str = Field(min_length=1)
    label: str = Field(min_length=1)
    values: list[FiniteFloat] = Field(default_factory=list)


class LinkageTrend(_DeskModel):
    labels: list[str] = Field(default_factory=list)
    series: list[LinkageTrendSeries] = Field(default_factory=list)

    @model_validator(mode="after")
    def _check_series_lengths(self) -> LinkageTrend:
        expected_len = len(self.labels)
        for item in self.series:
            if len(item.values) != expected_len:
                raise ValueError(
                    f"Series {item.sym!r} values length ({len(item.values)}) "
                    f"!= labels length ({expected_len})"
                )
        return self


class AnalyticsData(_DeskModel):
    sparkLabels: list[str] = Field(default_factory=list)
    rotationAssets: list[RotationAsset] = Field(default_factory=list)
    corrLabels: list[str] = Field(default_factory=list)
    corrMatrix: list[list[FiniteFloat]] = Field(default_factory=list)
    linkageTrend: LinkageTrend = Field(default_factory=LinkageTrend)

    @model_validator(mode="after")
    def _check_corr_matrix_square_and_bounded(self) -> AnalyticsData:
        n = len(self.corrLabels)
        if len(self.corrMatrix) != n:
            raise ValueError(
                f"corrMatrix row count ({len(self.corrMatrix)}) != corrLabels ({n})"
            )
        for row_idx, row in enumerate(self.corrMatrix):
            if len(row) != n:
                raise ValueError(
                    f"corrMatrix row {row_idx} length ({len(row)}) != corrLabels ({n})"
                )
            for col_idx, val in enumerate(row):
                if not (-1.0001 <= val <= 1.0001):
                    raise ValueError(
                        f"corrMatrix[{row_idx}][{col_idx}]={val} outside [-1.0001, 1.0001]"
                    )
        return self


class SetupValuation(_DeskModel):
    forward_pe: FiniteFloat | None = Field(default=None, gt=0)
    reference_forward_pe: FiniteFloat | None = Field(default=None, gt=0)
    forward_pe_discount_pct: FiniteFloat | None = None
    reference_observations: int = Field(default=0, ge=0, le=5)
    note: str = Field(min_length=1)
    source_url: OptionalHttpUrl = ""


class ResearchSetup(_DeskModel):
    symbol: str = Field(min_length=1)
    name: str = Field(min_length=1)
    mainline: str = Field(pattern=r"^M[1-5]$")
    category: Literal["dip", "hedge", "spec", "avoid", "watch"]
    category_label: str
    summary: str
    valuation: SetupValuation
    drawdown_52w_pct: FiniteFloat | None = Field(default=None, ge=0, le=100)
    high_52w: FiniteFloat | None = Field(default=None, gt=0)
    last_price: FiniteFloat | None = Field(default=None, gt=0)
    invalidation_price: FiniteFloat | None = Field(default=None, gt=0)
    target_price: FiniteFloat | None = Field(default=None, gt=0)
    reward_risk: FiniteFloat | None = Field(default=None, ge=0)
    as_of: CleanTimestampStr | None = None
    source_url: RequiredHttpUrl
    evidence_status: Literal["available", "partial", "unavailable"]
    scenario_note: str

    @model_validator(mode="after")
    def _check_scenario(self) -> ResearchSetup:
        levels = (self.invalidation_price, self.target_price, self.reward_risk)
        if all(level is None for level in levels):
            return self
        if any(level is None for level in levels) or self.last_price is None:
            raise ValueError("Scenario requires both levels, price, and reward/risk")
        if not self.invalidation_price < self.last_price < self.target_price:
            raise ValueError("Scenario must bracket the observed price")
        expected = (self.target_price - self.last_price) / (self.last_price - self.invalidation_price)
        if not math.isclose(self.reward_risk, round(expected, 2), abs_tol=0.005):
            raise ValueError("Reward/risk must agree with scenario levels")
        return self


class AnchorReview(_DeskModel):
    mainline: str = Field(pattern=r"^M[1-5]$")
    anchor_symbol: str
    candidate_symbol: str
    anchor_r5: FiniteFloat
    candidate_r5: FiniteFloat
    lead_sessions: int = Field(ge=3)
    governance_window: str
    structural_change_allowed: bool
    as_of: IsoDateStr


class ThemeComparison(_DeskModel):
    name: str
    members: list[str]
    as_of: IsoDateStr
    r5: FiniteFloat
    r20: FiniteFloat
    excess5: FiniteFloat


class StateResponse(_DeskModel):
    session_rotation: SessionRotation | None = None
    theme_comparison: list[ThemeComparison] = Field(default_factory=list)
    serverSyncedAt: SyncedTimestampStr
    meta: MetaInfo
    mainlines: list[MainlineItem] = Field(default_factory=list)
    overseas_watch: str = ""
    rotation_note: str = ""
    candidates: list[CandidateItem] = Field(default_factory=list)
    linkage: list[LinkageStory] = Field(default_factory=list)
    macro: list[MacroDimItem] = Field(default_factory=list)
    calendar: list[CalendarEvent] = Field(default_factory=list)
    history: list[HistoryEntry] = Field(default_factory=list)
    analytics: AnalyticsData
    setups: list[ResearchSetup] = Field(default_factory=list, max_length=10)
    anchor_reviews: list[AnchorReview] = Field(default_factory=list, max_length=5)


class NewsItem(_DeskModel):
    event_id: str = Field(min_length=1)
    title: str = Field(min_length=1)
    summary: str = ""
    tag: str = Field(min_length=1)
    source: str = Field(min_length=1)
    pubDate: CleanTimestampStr | Literal["发布时间未提供"]
    link: RequiredHttpUrl


class NewsResponse(_DeskModel):
    updatedAt: SyncedTimestampStr
    count: int = Field(ge=0)
    items: list[NewsItem] = Field(default_factory=list)

    @model_validator(mode="after")
    def _check_count_matches_items(self) -> NewsResponse:
        if self.count != len(self.items):
            raise ValueError(
                f"NewsResponse count ({self.count}) != len(items) ({len(self.items)})"
            )
        return self
