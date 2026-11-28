"""Always-on VPS background scheduler for live quotes, news, and precision research runs."""

from __future__ import annotations

import asyncio
import logging
import os
import re
from dataclasses import dataclass
from datetime import datetime, time, timedelta, timezone
from typing import Awaitable, Callable

from src.config import (
    TZ_CN,
    TZ_ET,
    format_utc_iso,
    get_market_close,
    get_market_session,
    is_us_trading_day,
)
from src.models.schemas import StateResponse
from src.pipeline import _already_rebalanced_after_close

logger = logging.getLogger(__name__)

_CN_TS_RE = re.compile(r"^(\d{4})[-/](\d{2})[-/](\d{2})[T ](\d{2}):(\d{2})(?::(\d{2}))?")


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name, "").strip().lower()
    if not raw:
        return default
    return raw in {"1", "true", "yes", "on"}


def _env_float(name: str, default: float, *, minimum: float = 1.0) -> float:
    raw = os.getenv(name, "").strip()
    if not raw:
        return default
    try:
        return max(minimum, float(raw))
    except ValueError:
        return default


def _env_int(name: str, default: int, *, minimum: int = 1) -> int:
    raw = os.getenv(name, "").strip()
    if not raw:
        return default
    try:
        return max(minimum, int(raw))
    except ValueError:
        return default


def parse_cn_timestamp_utc(raw: str | None) -> datetime | None:
    """Parse 'YYYY/MM/DD HH:MM:SS UTC+8' or 'YYYY-MM-DD HH:MM UTC+8' into UTC datetime."""
    if not raw:
        return None
    m = _CN_TS_RE.match(raw.strip())
    if not m:
        return None
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
    except ValueError:
        return None
    return dt_cn.astimezone(timezone.utc)


@dataclass(frozen=True)
class SchedulerConfig:
    """Tuning knobs for the always-on VPS daemon loops."""

    enabled: bool = False
    live_active_sec: float = 20.0
    live_idle_sec: float = 90.0
    live_persist_sec: float = 60.0
    news_active_sec: float = 60.0
    news_idle_sec: float = 180.0
    research_active_min: int = 20
    research_weekday_idle_min: int = 60
    research_idle_min: int = 120
    research_cooldown_min: int = 10
    research_retry_base_sec: int = 180
    research_retry_max_sec: int = 900
    research_check_sec: float = 30.0
    publish_kv: bool = False
    use_llm: bool = True

    @classmethod
    def from_env(cls) -> SchedulerConfig:
        return cls(
            enabled=_env_bool("DESK_ENABLE_SCHEDULER", False),
            live_active_sec=_env_float("DESK_LIVE_ACTIVE_SEC", 20.0, minimum=5.0),
            live_idle_sec=_env_float("DESK_LIVE_IDLE_SEC", 90.0, minimum=15.0),
            live_persist_sec=_env_float("DESK_LIVE_PERSIST_SEC", 60.0, minimum=10.0),
            news_active_sec=_env_float("DESK_NEWS_ACTIVE_SEC", 60.0, minimum=20.0),
            news_idle_sec=_env_float("DESK_NEWS_IDLE_SEC", 180.0, minimum=30.0),
            research_active_min=_env_int("DESK_RESEARCH_ACTIVE_MIN", 20, minimum=5),
            research_weekday_idle_min=_env_int("DESK_RESEARCH_WEEKDAY_IDLE_MIN", 60, minimum=15),
            research_idle_min=_env_int("DESK_RESEARCH_IDLE_MIN", 120, minimum=30),
            research_cooldown_min=_env_int("DESK_RESEARCH_COOLDOWN_MIN", 10, minimum=1),
            research_retry_base_sec=_env_int("DESK_RESEARCH_RETRY_BASE_SEC", 180, minimum=30),
            research_retry_max_sec=_env_int("DESK_RESEARCH_RETRY_MAX_SEC", 900, minimum=60),
            research_check_sec=_env_float("DESK_RESEARCH_CHECK_SEC", 30.0, minimum=5.0),
            publish_kv=_env_bool("DESK_SCHEDULER_PUBLISH_KV", False),
            use_llm=_env_bool("DESK_SCHEDULER_USE_LLM", True),
        )


@dataclass
class ResearchRuntimeStatus:
    """Mutable runtime telemetry for background research pipeline executions."""

    running: bool = False
    last_started_at: datetime | None = None
    last_finished_at: datetime | None = None
    last_success_at: datetime | None = None
    last_status: str | None = None
    last_reason: str | None = None
    last_error: str | None = None
    consecutive_failures: int = 0
    next_retry_after: datetime | None = None


@dataclass(frozen=True)
class ResearchScheduleDecision:
    """Deterministic scheduling decision for whether a VPS research cycle should start now."""

    should_run: bool
    reason: str
    force_rebalance: bool = False
    catchup_rebalance: bool = False
    state_age_min: int | None = None


def _is_post_close_rebalance_due(
    state: StateResponse | None,
    now_utc: datetime,
) -> tuple[bool, bool]:
    """Return (due, catchup_rebalance) for the ET post-close governance window."""
    et = now_utc.astimezone(TZ_ET)
    if not is_us_trading_day(et.date()):
        return False, False
    close_h = get_market_close(et.date()).hour
    rebal_start = time(close_h, 30)
    rebal_native_end = time(close_h + 1, 0)
    rebal_catchup_end = time(close_h + 2, 30)
    if not (rebal_start <= et.time() < rebal_catchup_end):
        return False, False
    if _already_rebalanced_after_close(state, now_utc):
        return False, False
    return True, et.time() >= rebal_native_end


def _is_cross_market_window_due(
    state_synced_utc: datetime | None,
    now_utc: datetime,
) -> tuple[bool, str]:
    """Check weekday CST windows for APAC close (14:35) and EU open (16:05)."""
    cn = now_utc.astimezone(TZ_CN)
    if cn.weekday() >= 5:
        return False, ""
    cn_min = cn.hour * 60 + cn.minute
    if 14 * 60 + 35 <= cn_min < 15 * 60 + 15:
        win_start = cn.replace(hour=14, minute=35, second=0, microsecond=0).astimezone(timezone.utc)
        win_reason = "cross_market_apac_close"
    elif 16 * 60 + 5 <= cn_min < 16 * 60 + 40:
        win_start = cn.replace(hour=16, minute=5, second=0, microsecond=0).astimezone(timezone.utc)
        win_reason = "cross_market_eu_open"
    else:
        return False, ""

    if state_synced_utc is None or state_synced_utc < win_start:
        return True, win_reason
    return False, ""


def evaluate_research_due(
    state: StateResponse | None,
    status: ResearchRuntimeStatus,
    now_utc: datetime,
    config: SchedulerConfig | None = None,
) -> ResearchScheduleDecision:
    """Decide whether the background research pipeline should execute at `now_utc`."""
    cfg = config or SchedulerConfig()
    if now_utc.tzinfo is None:
        now_utc = now_utc.replace(tzinfo=timezone.utc)

    synced_utc = parse_cn_timestamp_utc(state.serverSyncedAt) if state is not None else None
    age_sec = (now_utc - synced_utc).total_seconds() if synced_utc is not None else None
    state_age_min = max(0, int(round(age_sec / 60.0))) if age_sec is not None and age_sec >= 0 else None

    if status.running:
        return ResearchScheduleDecision(False, "already_running", state_age_min=state_age_min)

    if status.next_retry_after is not None and now_utc < status.next_retry_after:
        return ResearchScheduleDecision(False, "failure_backoff", state_age_min=state_age_min)

    # Guard against runaway loops if state timestamp parsing ever lags behind a recent successful finish.
    if status.last_started_at is not None and status.consecutive_failures == 0:
        since_start = (now_utc - status.last_started_at).total_seconds()
        if 0 <= since_start < cfg.research_cooldown_min * 60:
            return ResearchScheduleDecision(False, "cooldown", state_age_min=state_age_min)

    if state is None:
        return ResearchScheduleDecision(True, "cold_start", state_age_min=None)

    rebal_due, is_catchup = _is_post_close_rebalance_due(state, now_utc)
    if rebal_due:
        return ResearchScheduleDecision(
            True,
            "post_close_rebalance_catchup" if is_catchup else "post_close_rebalance",
            force_rebalance=False,
            catchup_rebalance=is_catchup,
            state_age_min=state_age_min,
        )

    cross_due, cross_reason = _is_cross_market_window_due(synced_utc, now_utc)
    if cross_due:
        return ResearchScheduleDecision(True, cross_reason, state_age_min=state_age_min)

    session = get_market_session(now_utc)
    active = session in {"PRE_MARKET", "INTRADAY", "AFTER_HOURS"}
    et = now_utc.astimezone(TZ_ET)
    is_workday = is_us_trading_day(et.date())
    max_age_min = (
        cfg.research_active_min
        if active
        else (cfg.research_weekday_idle_min if is_workday else cfg.research_idle_min)
    )
    if age_sec is None or age_sec < 0 or age_sec >= max_age_min * 60:
        reason = (
            f"active_{cfg.research_active_min}m_due"
            if active
            else ("weekday_hourly_due" if is_workday else "idle_2h_due")
        )
        return ResearchScheduleDecision(True, reason, state_age_min=state_age_min)

    return ResearchScheduleDecision(False, "fresh", state_age_min=state_age_min)


class DeskScheduler:
    """In-process asyncio scheduler keeping live quotes, news, and research fresh 24/7."""

    def __init__(
        self,
        *,
        config: SchedulerConfig,
        refresh_live_cb: Callable[[bool], Awaitable[None]],
        refresh_news_cb: Callable[[], Awaitable[None]],
        load_state_cb: Callable[[], StateResponse | None],
        run_research_cb: Callable[[str, bool, bool], Awaitable[dict[str, object]]],
    ) -> None:
        self.config = config
        self._refresh_live_cb = refresh_live_cb
        self._refresh_news_cb = refresh_news_cb
        self._load_state_cb = load_state_cb
        self._run_research_cb = run_research_cb
        self.status = ResearchRuntimeStatus()
        self._tasks: list[asyncio.Task[None]] = []
        self._stop_event = asyncio.Event()
        self._research_lock = asyncio.Lock()
        self._last_live_persist_mono: float = 0.0

    @property
    def is_running(self) -> bool:
        return any(not t.done() for t in self._tasks)

    def start(self) -> None:
        if not self.config.enabled or self.is_running:
            return
        self._stop_event.clear()
        self._tasks = [
            asyncio.create_task(self._live_loop(), name="desk-live-loop"),
            asyncio.create_task(self._news_loop(), name="desk-news-loop"),
            asyncio.create_task(self._research_loop(), name="desk-research-loop"),
        ]
        logger.info(
            "VPS DeskScheduler started (live=%ss/%ss, news=%ss/%ss, research=%sm/%sm/%sm)",
            int(self.config.live_active_sec),
            int(self.config.live_idle_sec),
            int(self.config.news_active_sec),
            int(self.config.news_idle_sec),
            self.config.research_active_min,
            self.config.research_weekday_idle_min,
            self.config.research_idle_min,
        )

    async def stop(self) -> None:
        self._stop_event.set()
        for task in self._tasks:
            task.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks = []

    async def _sleep_or_stop(self, seconds: float) -> bool:
        try:
            await asyncio.wait_for(self._stop_event.wait(), timeout=max(0.5, seconds))
            return True
        except asyncio.TimeoutError:
            return False

    async def _live_loop(self) -> None:
        loop = asyncio.get_running_loop()
        while not self._stop_event.is_set():
            try:
                now_mono = loop.time()
                should_persist = (
                    self._last_live_persist_mono == 0.0
                    or (now_mono - self._last_live_persist_mono) >= self.config.live_persist_sec
                )
                await self._refresh_live_cb(should_persist)
                if should_persist:
                    self._last_live_persist_mono = now_mono
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning("Background live refresh error: %s", exc)

            session = get_market_session()
            active = session in {"PRE_MARKET", "INTRADAY", "AFTER_HOURS"}
            delay = self.config.live_active_sec if active else self.config.live_idle_sec
            if await self._sleep_or_stop(delay):
                break

    async def _news_loop(self) -> None:
        # Stagger slightly behind live warm-up on boot.
        if await self._sleep_or_stop(2.0):
            return
        while not self._stop_event.is_set():
            try:
                await self._refresh_news_cb()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning("Background news refresh error: %s", exc)

            session = get_market_session()
            active = session in {"PRE_MARKET", "INTRADAY", "AFTER_HOURS"}
            delay = self.config.news_active_sec if active else self.config.news_idle_sec
            if await self._sleep_or_stop(delay):
                break

    async def execute_research_once(
        self,
        *,
        reason: str,
        force_rebalance: bool = False,
        catchup_rebalance: bool = False,
    ) -> bool:
        """Run one serialized research cycle and update runtime telemetry."""
        if self._research_lock.locked():
            return False
        async with self._research_lock:
            now_start = datetime.now(timezone.utc)
            self.status.running = True
            self.status.last_started_at = now_start
            self.status.last_reason = reason
            logger.info(
                "Starting background research pipeline (reason=%s, force_rebalance=%s, catchup=%s)",
                reason,
                force_rebalance,
                catchup_rebalance,
            )
            try:
                summary = await self._run_research_cb(reason, force_rebalance, catchup_rebalance)
                now_done = datetime.now(timezone.utc)
                self.status.last_finished_at = now_done
                self.status.last_success_at = now_done
                self.status.last_status = f"ok:{summary.get('engine_mode', 'unknown')}"
                self.status.last_error = None
                self.status.consecutive_failures = 0
                self.status.next_retry_after = None
                logger.info(
                    "Background research succeeded (reason=%s, mode=%s, model=%s, audit=%s/%s)",
                    reason,
                    summary.get("engine_mode"),
                    summary.get("model_used"),
                    summary.get("audit_passed"),
                    summary.get("audit_total"),
                )
                return True
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                now_fail = datetime.now(timezone.utc)
                self.status.last_finished_at = now_fail
                self.status.last_status = "error"
                self.status.last_error = f"{type(exc).__name__}: {str(exc)[:240]}"
                self.status.consecutive_failures += 1
                backoff = min(
                    self.config.research_retry_max_sec,
                    self.config.research_retry_base_sec * (2 ** (self.status.consecutive_failures - 1)),
                )
                self.status.next_retry_after = now_fail + timedelta(seconds=backoff)
                logger.exception(
                    "Background research failed (attempt=%d, retry in %ds): %s",
                    self.status.consecutive_failures,
                    backoff,
                    exc,
                )
                return False
            finally:
                self.status.running = False

    def trigger_research_background(
        self,
        *,
        reason: str = "manual_api",
        force_rebalance: bool = False,
    ) -> bool:
        """Schedule an immediate background research cycle if none is currently running."""
        if self._research_lock.locked() or self.status.running:
            return False
        asyncio.create_task(
            self.execute_research_once(
                reason=reason,
                force_rebalance=force_rebalance,
                catchup_rebalance=False,
            ),
            name=f"desk-research-{reason}",
        )
        return True

    async def _research_loop(self) -> None:
        # Let live/news warm up first on cold boot.
        if await self._sleep_or_stop(5.0):
            return
        while not self._stop_event.is_set():
            try:
                now_utc = datetime.now(timezone.utc)
                state = self._load_state_cb()
                decision = evaluate_research_due(state, self.status, now_utc, self.config)
                if decision.should_run:
                    await self.execute_research_once(
                        reason=decision.reason,
                        force_rebalance=decision.force_rebalance,
                        catchup_rebalance=decision.catchup_rebalance,
                    )
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.exception("Research scheduler tick error: %s", exc)

            if await self._sleep_or_stop(self.config.research_check_sec):
                break

    def snapshot_telemetry(self) -> dict[str, object]:
        st = self.status
        return {
            "schedulerEnabled": bool(self.config.enabled),
            "pipelineRunning": bool(st.running),
            "lastPipelineAt": format_utc_iso(st.last_finished_at) if st.last_finished_at else None,
            "lastPipelineStatus": st.last_status,
            "lastPipelineReason": st.last_reason,
            "lastPipelineError": st.last_error,
        }
