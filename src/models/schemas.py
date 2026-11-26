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
_CLEAN_TS_RE = re.compile(r"^(\d{4}-\d{2}-\d{2})(?:[T ]\d{2}:\d{2}(?::\d{2})?.*)?$")


def _reject_gviz(v: str) -> str:
    if "Date(" in v:
        raise ValueError("Google gviz Date(...) syntax is forbidden")
    return v


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
    return s


def _validate_no_gviz_str(v: str) -> str:
    s = v.strip()
    if not s:
        raise ValueError("String must not be empty")
    return _reject_gviz(s)


def _validate_optional_http_url(v: str) -> str:
    s = v.strip()
    if not s:
        return ""
    parsed = urlparse(s)
    if parsed.scheme not in ("http", "https") or not parsed.netloc:
        raise ValueError(f"Expected http(s) URL, got {s!r}")
    return s


def _validate_required_http_url(v: str) -> str:
    s = v.strip()
    parsed = urlparse(s)
    if parsed.scheme not in ("http", "https") or not parsed.netloc:
        raise ValueError(f"Expected non-empty http(s) URL, got {s!r}")
    return s


def _validate_finite_float(v: float) -> float:
    if math.isnan(v) or math.isinf(v):
        raise ValueError("Value must be finite (NaN/Inf forbidden)")
    return v


IsoDateStr = Annotated[str, BeforeValidator(_validate_iso_date)]
CleanTimestampStr = Annotated[str, AfterValidator(_validate_clean_timestamp)]
NoGvizStr = Annotated[str, AfterValidator(_validate_no_gviz_str)]
OptionalHttpUrl = Annotated[str, AfterValidator(_validate_optional_http_url)]
RequiredHttpUrl = Annotated[str, AfterValidator(_validate_required_http_url)]
FiniteFloat = Annotated[float, AfterValidator(_validate_finite_float)]


class _DeskModel(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True)


class HealthResponse(_DeskModel):
    ok: bool = True
    version: str = Field(default="2.0.0", min_length=1)
    serverTimeUtc: NoGvizStr = Field(min_length=1)
    stateUpdatedAt: str | None = None
    newsCount: int = Field(default=0, ge=0)
    quotesCount: int = Field(default=0, ge=0)

    @model_validator(mode="after")
    def _check_optional_state_ts(self) -> HealthResponse:
        if self.stateUpdatedAt is not None:
            _reject_gviz(self.stateUpdatedAt)
        return self


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
    tsUtc: NoGvizStr = Field(min_length=1)


class LiveResponse(_DeskModel):
    updatedAt: NoGvizStr = Field(min_length=1)
    updatedAtEt: NoGvizStr = Field(min_length=1)
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
    updated_at: NoGvizStr = Field(min_length=1, max_length=120)
    updated_at_short: NoGvizStr = Field(min_length=1, max_length=40)
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
        if self.labels:
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
                        f"corrMatrix[{row_idx}][{col_idx}]={val} outside [-1.0, 1.0]"
                    )
        return self


class StateResponse(_DeskModel):
    serverSyncedAt: NoGvizStr = Field(min_length=1)
    meta: MetaInfo
    mainlines: list[MainlineItem] = Field(default_factory=list)
    candidates: list[CandidateItem] = Field(default_factory=list)
    linkage: list[LinkageStory] = Field(default_factory=list)
    macro: list[MacroDimItem] = Field(default_factory=list)
    calendar: list[CalendarEvent] = Field(default_factory=list)
    history: list[HistoryEntry] = Field(default_factory=list)
    analytics: AnalyticsData


class NewsItem(_DeskModel):
    event_id: str = Field(min_length=1)
    title: str = Field(min_length=1)
    summary: str = ""
    tag: str = Field(min_length=1)
    source: str = Field(min_length=1)
    pubDate: NoGvizStr = Field(min_length=1)
    link: RequiredHttpUrl


class NewsResponse(_DeskModel):
    updatedAt: NoGvizStr = Field(min_length=1)
    count: int = Field(ge=0)
    items: list[NewsItem] = Field(default_factory=list)

    @model_validator(mode="after")
    def _check_count_matches_items(self) -> NewsResponse:
        if self.count != len(self.items):
            raise ValueError(
                f"NewsResponse count ({self.count}) != len(items) ({len(self.items)})"
            )
        return self
