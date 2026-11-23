"""PortfolioPnlSnapshotter — async Phase-5B equity/drawdown sample writer.

Owns ONE :class:`Repository` for its lifetime and mirrors
:class:`DbStatsSnapshotter` exactly: an interruptible pre-sleep loop, an
``app.state`` pre-init contract, a disabled mode that constructs but parks, and a
symmetric reverse-order stop. Single-writer safety comes from the instance-0
lifespan gate plus the sample DAL's per-scope advisory lock.

A tick discovers executor-backed spot scopes with an active USD live anchor.
It resumes each scope's durable correction frontier before selecting new work.
A generation fixes the finalized tip, its evidence horizon and the pending
execution watermark. Each tick commits at most one bounded chunk per scope.
The sample edits, causal peak, detection baseline and next minute commit
together, including when a chunk writes no sample rows.

A new generation scans forward or starts at the earliest late fill, retryable
incomplete minute or outstanding repair. Its execution baseline advances only
when that generation is reconciled. A restarted process reads the persisted
frontier rather than inferring progress from a later sample.

Correction-flow limitation (D2): 5B v1 does NOT detect a corrected mark candle —
a finalized 1m close a venue later revises. An already-``complete`` persisted
sample is revisited only by a late-fill recompute-forward (driven by the 5A
engine's earliest-affected-minute boundary) or by a bounded, retryable
``incomplete`` self-heal; a silent candle restatement alone triggers neither, so
that historical row is not rewritten. This is safe because the served P&L curve is
recomputed on read from current evidence, never from these persisted rows.

Configuration: ``PNL_SNAPSHOTTER_INTERVAL_SECONDS`` (default 60) and
``PNL_SNAPSHOTTER_ENABLED`` (default false — DISABLED so a production rollout is an
explicit operator flip). Read directly from ``os.environ.get(...)``.
"""

import asyncio
import contextlib
import itertools
import json
import logging
import os
import time
from bisect import bisect_right
from collections.abc import Callable
from collections.abc import Mapping
from collections.abc import Sequence
from dataclasses import dataclass
from dataclasses import replace
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from typing import Final
from uuid import uuid7

from sqlalchemy.exc import DBAPIError
from sqlalchemy.exc import SQLAlchemyError

from snapper.application.portfolio import pnl_snapshotter_checkpoint as checkpoint_dispatch
from snapper.application.portfolio.basket_valuation import CryptoUsdCandle
from snapper.application.portfolio.basket_valuation import PositionInventoryEntry
from snapper.application.portfolio.basket_valuation import ValuationEvidence
from snapper.application.portfolio.fx_conversion_shadow import FxShadowPinContext
from snapper.application.portfolio.fx_conversion_shadow import activate_fx_shadow_context
from snapper.application.portfolio.pnl_correction_planner import PlannedPnlCorrection
from snapper.application.portfolio.pnl_correction_planner import PnlCorrectionPlanInputs
from snapper.application.portfolio.pnl_correction_planner import plan_correction_chunk
from snapper.application.portfolio.pnl_opening_checkpoint_capture import PnlCheckpointCaptureRequest
from snapper.application.portfolio.pnl_opening_checkpoint_certificate import (
    PnlCheckpointWriteRefusalError,
)
from snapper.application.portfolio.pnl_opening_checkpoint_metrics import (
    CHECKPOINT_SUMMARY_EVERY_TICKS,
)
from snapper.application.portfolio.pnl_opening_checkpoint_metrics import (
    advance_checkpoint_summary_tick,
)
from snapper.application.portfolio.pnl_opening_checkpoint_metrics import checkpoint_wallet_alias
from snapper.application.portfolio.pnl_opening_checkpoint_metrics import prune_checkpoint_metrics
from snapper.application.portfolio.pnl_opening_checkpoint_metrics import (
    record_checkpoint_capture_committed,
)
from snapper.application.portfolio.pnl_opening_checkpoint_metrics import (
    record_checkpoint_resume_attempted,
)
from snapper.application.portfolio.pnl_opening_checkpoint_metrics import (
    record_checkpoint_resume_succeeded,
)
from snapper.application.portfolio.pnl_opening_checkpoint_metrics import (
    record_checkpoint_shadow_comparison,
)
from snapper.application.portfolio.pnl_opening_checkpoint_metrics import (
    record_checkpoint_shadow_failure,
)
from snapper.application.portfolio.pnl_opening_checkpoint_metrics import (
    record_checkpoint_shadow_mismatch,
)
from snapper.application.portfolio.pnl_opening_checkpoint_metrics import (
    record_checkpoint_shadow_skipped,
)
from snapper.application.portfolio.pnl_opening_checkpoint_verifier import PnlCheckpointResume
from snapper.application.portfolio.pnl_snapshot_planner import FINALIZATION_LAG
from snapper.application.portfolio.pnl_snapshot_planner import SELF_HEAL_LOOKBACK
from snapper.application.portfolio.pnl_snapshot_planner import ChunkWindow
from snapper.application.portfolio.pnl_snapshot_planner import MinuteInputs
from snapper.application.portfolio.pnl_snapshot_planner import PlannedSample
from snapper.application.portfolio.pnl_snapshot_planner import PositionVersion
from snapper.application.portfolio.pnl_snapshot_planner import SelfHealCandidate
from snapper.application.portfolio.pnl_snapshot_planner import WithheldMinute
from snapper.application.portfolio.pnl_snapshot_planner import plan_late_fill_recompute
from snapper.application.portfolio.pnl_snapshot_planner import plan_self_heal_minutes
from snapper.application.portfolio.pnl_snapshotter_checkpoint import PnlCheckpointResumeRequest
from snapper.application.portfolio.pnl_snapshotter_checkpoint import capture_request_for
from snapper.application.portfolio.pnl_snapshotter_checkpoint import certified_checkpoint_write
from snapper.application.portfolio.pnl_snapshotter_checkpoint import delete_checkpoint
from snapper.application.portfolio.pnl_snapshotter_checkpoint import handle_resume_fallback
from snapper.application.portfolio.pnl_snapshotter_checkpoint import prepare_checkpoint_resume
from snapper.application.portfolio.pnl_snapshotter_checkpoint_shadow import (
    PnlCheckpointShadowMismatch,
)
from snapper.application.portfolio.pnl_snapshotter_checkpoint_shadow import PnlCheckpointShadowSide
from snapper.application.portfolio.pnl_snapshotter_checkpoint_shadow import (
    PnlCheckpointShadowWindow,
)
from snapper.application.portfolio.pnl_snapshotter_checkpoint_shadow import (
    compare_checkpoint_shadow_sides,
)
from snapper.application.portfolio.pnl_snapshotter_config import CHECKPOINT_MODE_ENV_VAR
from snapper.application.portfolio.pnl_snapshotter_config import ENABLED_ENV_VAR
from snapper.application.portfolio.pnl_snapshotter_config import FX_SHADOW_PINNING_ENV_VAR
from snapper.application.portfolio.pnl_snapshotter_config import INTERVAL_ENV_VAR
from snapper.application.portfolio.pnl_snapshotter_config import CheckpointMode
from snapper.application.portfolio.pnl_snapshotter_config import resolve_checkpoint_modes
from snapper.application.portfolio.pnl_snapshotter_config import resolve_enabled
from snapper.application.portfolio.pnl_snapshotter_config import resolve_interval
from snapper.application.portfolio.pnl_timeline import PnlTimelineWorkBudgetError
from snapper.application.portfolio.pnl_timeline_service import PnlFrozenReplayEvidenceChangedError
from snapper.application.portfolio.pnl_timeline_service import PnlSeriesReadPolicy
from snapper.application.portfolio.pnl_timeline_service import PnlSeriesReplayMetadata
from snapper.application.portfolio.pnl_timeline_service import PnlSeriesReplayOptions
from snapper.application.portfolio.pnl_timeline_service import PnlWalletSeriesResult
from snapper.application.portfolio.pnl_timeline_service import build_wallet_pnl_series
from snapper.application.portfolio.pnl_timeline_service import load_basket_fiat_evidence_by_minute
from snapper.application.process_manager.executor_topology import MINT_WALLET_PIN_SETTING_KEY
from snapper.application.process_manager.executor_topology import ExecutorCredentialDisposition
from snapper.application.process_manager.executor_topology import ExecutorTopology
from snapper.application.process_manager.executor_topology import resolve_executor_topology
from snapper.application.services.settings import SettingsService
from snapper.core.json_types import JsonValue
from snapper.data.pnl_correction_state import MAX_CORRECTION_CHUNK_MINUTES
from snapper.data.pnl_correction_state import PnlCorrectionEvidence
from snapper.data.pnl_correction_state import PnlCorrectionFrontier
from snapper.data.pnl_correction_state import PnlCorrectionKey
from snapper.data.pnl_correction_state import PnlCorrectionRequest
from snapper.data.pnl_correction_state import PortfolioPnlCorrectionConflictError
from snapper.data.repository import PortfolioPnlSampleQuery
from snapper.data.repository import PortfolioPnlSampleScope
from snapper.data.repository import Repository
from snapper.data.repository_types import PNL_SAMPLE_CALC_VERSION
from snapper.data.repository_types import PortfolioPnlAnchorRow
from snapper.data.repository_types import PortfolioPnlSampleRow
from snapper.data.repository_types import VenueAccountObservationAttemptRow
from snapper.data.repository_types import WalletCredentialRow
from snapper.infrastructure.exchanges.reconciliation_policy import get_reconciliation_method_policy

logger = logging.getLogger(__name__)

MAX_SHADOW_COMPARISONS_PER_TICK: Final[int] = 2
"""Maximum verified checkpoint comparisons and resumed replays permitted in one tick."""

SHADOW_TICK_RESERVE_FRACTION: Final[float] = 0.25
"""Share of the tick interval withheld from shadow work for the rest of the tick.

The authoritative anchor plan commits before any shadow work starts, so shadow
work can never block it. Shadow work still runs under its own nested deadline
that ends this fraction of the interval before the tick deadline, so a slow
comparison is cut short as an isolated shadow failure instead of consuming the
time later wallets in the same tick need.
"""

_LIVE_MODE: Final = "live"
_USD: Final[str] = "USD"
_FUTURES_POSITION_METHOD: Final[str] = "futures_position"
_PAPER_CREDENTIAL_TYPE: Final[str] = "paper"
_SCOPE_SKIP_MINT_IDENTITY: Final[str] = "mint_identity_executor_topology"
_SCOPE_SKIP_NO_ANCHOR: Final[str] = "no_active_usd_anchor"
_SCOPE_SKIP_NO_SPOT_VENUES: Final[str] = "no_spot_venues"
_SCOPE_SKIP_PERSISTENT_FILL_GAP: Final[str] = "persistent_fill_gap_skip"
_MINUTE: Final[timedelta] = timedelta(minutes=1)
_REPAIR_FROM_LAG_THRESHOLD: Final[timedelta] = timedelta(hours=24)
"""Age beyond which a pinned ``repair_from`` emits a rate-limited lag warning."""
_CRYPTO_PLANE_LIMIT_SQLSTATES: Final[frozenset[str]] = frozenset({"57014", "55P03"})
"""PostgreSQL SQLSTATEs raised when the crypto-plane read exceeds its shared P&L budget.

``57014`` is a statement timeout (``query_canceled``) and ``55P03`` a lock timeout
(``lock_not_available``); both are expected budget outcomes logged once per streak.
"""

_CHUNK_RECOVERY_STREAK: Final[int] = 3
"""Consecutive fast committed chunks required before a reduced chunk limit doubles."""

_CHUNK_RECOVERY_BUDGET_FRACTION: Final[float] = 0.25
"""Share of the tick budget below which a full-width committed chunk counts toward recovery.

Doubling a chunk that spanned the whole current limit and finished within a
quarter of the tick keeps the doubled chunk within about half of it, so recovery
cannot oscillate against the timeout that originally halved the limit. A chunk
narrower than the limit, clipped at the frontier tip or split after a work-budget
overflow, proves nothing about a full-width chunk and never counts.
"""

_FAILURE_MESSAGES: Final[dict[str, str]] = {
    "sample_failed": "PortfolioPnlSnapshotter: wallet %s sample failed; continuing",
    "crypto_plane_timeout": (
        "PortfolioPnlSnapshotter: wallet %s crypto-plane query timed out (statement or lock limit) "
        "for chunk %s through %s; not advancing baseline"
    ),
    "supersede_cas": "PortfolioPnlSnapshotter: wallet %s supersede CAS lost at %s; retrying next tick",
    "retract_cas": "PortfolioPnlSnapshotter: wallet %s retract CAS lost at %s; retrying next tick",
    "recompute_insert": (
        "PortfolioPnlSnapshotter: wallet %s recompute-insert conflicts at %s; retrying next tick"
    ),
    "catchup_conflict": (
        "PortfolioPnlSnapshotter: wallet %s catch-up conflicts at %s; not advancing baseline"
    ),
}
"""Per-failure-class log-once message templates (B4/C2): the first ``%s`` is the
wallet, the second (conflict classes only) the offending minute(s)."""


def is_futures_class_exchange(exchange: str) -> bool:
    """Return whether an exchange is a FUTURES-class venue (decision R11).

    The single, documented futures signal: the exchange's registered concrete
    reconciliation adapter proves ``futures_position`` as its structural default
    (only ``kraken_futures`` does). This reads the authoritative venue-capability
    registry rather than matching an exchange-name list, so a new futures venue is
    excluded the moment its adapter is registered.

    Args:
        exchange: Canonical exchange identifier.

    Returns:
        Whether the exchange values as futures and is excluded from the spot scope.
    """
    return get_reconciliation_method_policy(exchange).structural_default == _FUTURES_POSITION_METHOD


def resolve_spot_venues(
    credentials: Sequence[WalletCredentialRow],
    topology: ExecutorTopology,
) -> dict[str, frozenset[str]]:
    """Group active credentials into each wallet's expected SPOT venue set (R11).

    A credential contributes its exchange to its wallet's denominator only when it
    is neither a paper credential nor a futures-class venue and the shared executor
    topology says its order executor runs. A wallet whose every credential is
    unsupported or executor-excluded resolves to an empty set. The venue account
    observer runs inside that executor, so an executor-less wallet can never emit a
    ``venue_account_observations`` row. Leaving such a venue in the denominator
    would therefore make every minute fail ``basket_missing_venue`` forever.

    Args:
        credentials: Active wallet credential rows at the tick horizon.
        topology: Shared executor topology resolved from the same catalogue.

    Returns:
        Every seen wallet mapped to its (possibly empty) spot venue set.
    """
    grouped: dict[str, set[str]] = {}
    for credential in credentials:
        wallet = credential["wallet_public_id"]
        grouped.setdefault(wallet, set())
        if not _is_spot_credential(credential):
            continue
        if (
            topology.disposition_for(credential)
            == ExecutorCredentialDisposition.MINT_WALLET_EXCLUDED
        ):
            continue
        grouped[wallet].add(credential["exchange"])
    return {wallet: frozenset(venues) for wallet, venues in grouped.items()}


def _is_spot_credential(credential: WalletCredentialRow) -> bool:
    """Return whether a credential contributes a spot valuation venue."""
    if credential["credential_type"] == _PAPER_CREDENTIAL_TYPE:
        return False
    return not is_futures_class_exchange(credential["exchange"])


def _mint_excluded_spot_wallets(
    credentials: Sequence[WalletCredentialRow], topology: ExecutorTopology
) -> frozenset[str]:
    """Return wallets whose otherwise-spot credential topology excludes."""
    return frozenset(
        credential["wallet_public_id"]
        for credential in credentials
        if _is_spot_credential(credential)
        and topology.disposition_for(credential)
        == ExecutorCredentialDisposition.MINT_WALLET_EXCLUDED
    )


@dataclass(frozen=True, slots=True)
class _ScopeContext:
    """The immutable per-scope binding one tick threads through its helpers."""

    wallet_public_id: str
    scope: PortfolioPnlSampleScope
    query: PortfolioPnlSampleQuery
    t0: datetime
    expected_venues: frozenset[str]
    as_of: datetime
    anchor: PortfolioPnlAnchorRow


@dataclass(frozen=True, slots=True)
class _CheckpointReplayOptions:
    """Optional capture and verified resume inputs for one replay attempt."""

    capture: PnlCheckpointCaptureRequest | None = None
    resume: PnlCheckpointResume | None = None


@dataclass(frozen=True, slots=True)
class _CheckpointDispatchRequest:
    """Complete immutable state for one non-off replay dispatch."""

    repo: Repository
    ctx: _ScopeContext
    frontier: PnlCorrectionFrontier
    chunk: ChunkWindow
    started: float
    capture: PnlCheckpointCaptureRequest | None
    resume: PnlCheckpointResume | None


@dataclass(frozen=True, slots=True)
class _PlanningInputs:
    """Observation, valuation and position evidence read once for one chunk."""

    observations: dict[datetime, dict[str, VenueAccountObservationAttemptRow]]
    valuation: ValuationEvidence
    positions: list[PositionVersion]


@dataclass(frozen=True, slots=True)
class _PlanBasis:
    """Physical correction evidence plus optional pre-read planning inputs.

    Shadow mode plans the cold anchor and the resumed side over the same
    window. Both planners receive one shared ``inputs`` snapshot so that a
    write committed between two separate reads cannot surface as a false
    mismatch that deletes a valid checkpoint.
    """

    evidence: PnlCorrectionEvidence
    inputs: _PlanningInputs | None = None


@dataclass(frozen=True)
class _ChunkPlanRequest:
    """Bundled inputs for planning one physical correction chunk."""

    repo: Repository
    ctx: _ScopeContext
    result: PnlWalletSeriesResult
    basis: _PlanBasis


class PortfolioPnlSnapshotter:
    """Async Phase-5B equity/drawdown sample writer with disabled-mode support."""

    def __init__(
        self,
        *,
        repo: Repository | None,
        interval_seconds: int | None = None,
        disabled: bool | None = None,
        clock: Callable[[], datetime] | None = None,
        settings_service: SettingsService | None = None,
    ) -> None:
        """Wire dependencies.

        Args:
            repo: Async :class:`Repository`. ``None`` when parked in disabled mode.
            interval_seconds: Loop period. ``None`` reads
                ``PNL_SNAPSHOTTER_INTERVAL_SECONDS`` (default 60).
            disabled: Disable override; ``None`` reads ``PNL_SNAPSHOTTER_ENABLED``
                (default disabled).
            clock: Injectable UTC ``now`` for deterministic tests.
            settings_service: Runtime settings reader used for a fresh mint pin
                lookup on every tick. ``None`` means no declared exclusion.
        """
        if interval_seconds is None:
            interval_seconds = resolve_interval(os.environ.get(INTERVAL_ENV_VAR))
        if disabled is None:
            disabled = not resolve_enabled(os.environ.get(ENABLED_ENV_VAR))
        self._repo = repo if not disabled else None
        self._interval_seconds = interval_seconds
        self._disabled = disabled
        self._clock: Callable[[], datetime] = clock or (lambda: datetime.now(UTC))
        self._monotonic: Callable[[], float] = time.monotonic
        self._settings_service = settings_service
        self._stopping = asyncio.Event()
        self._loop_task: asyncio.Task[None] | None = None
        self._session_id = str(uuid7())
        self._sequence = 0
        self._last_scheduled_wallet: str | None = None
        self._replay_chunk_limits: dict[str, int] = {}
        self._replay_chunk_streaks: dict[str, int] = {}
        self._unsupported_logged: set[str] = set()
        self._mint_identity_logged: set[str] = set()
        self._no_anchor_logged: set[str] = set()
        self._failure_logged: set[tuple[str, str]] = set()
        self._self_heal_logged: dict[str, tuple[datetime, tuple[str, ...]]] = {}
        self._withheld_logged: dict[str, tuple[datetime, tuple[str, ...]]] = {}
        self._persistent_fill_gap_skip_logged: set[tuple[str, str, str, str]] = set()
        self._repair_from_lag_logged: dict[tuple[str, str, str, str], datetime] = {}
        self._shadow_failure_logged: set[str] = set()
        self._fx_shadow_pinning_enabled = resolve_enabled(os.environ.get(FX_SHADOW_PINNING_ENV_VAR))
        self._fx_shadow_context: FxShadowPinContext | None = None
        checkpoint_resolution = resolve_checkpoint_modes(os.environ.get(CHECKPOINT_MODE_ENV_VAR))
        self._configured_checkpoint_mode: CheckpointMode = checkpoint_resolution.configured
        self._checkpoint_mode: CheckpointMode = checkpoint_resolution.effective
        self._checkpoint_summary_ticks: int = 0
        self._tick_recaptures: int = 0
        self._tick_shadow_comparisons: int = 0
        self._tick_last_budgeted_wallet: str | None = None
        self._tick_deadline: float | None = None

    @property
    def disabled(self) -> bool:
        """Return whether the snapshotter is parked in disabled mode.

        Returns:
            ``True`` iff the loop is skipped because the feature is not enabled.
        """
        return self._disabled

    @property
    def interval_seconds(self) -> int:
        """Return the configured loop period in seconds.

        Returns:
            Seconds the loop sleeps between ticks.
        """
        return self._interval_seconds

    @property
    def checkpoint_mode(self) -> CheckpointMode:
        """Return the effective opening-checkpoint mode resolved at startup.

        Returns:
            Effective checkpoint mode: ``"off"``, ``"shadow"``, or ``"active"``.
        """
        return self._checkpoint_mode

    @property
    def configured_checkpoint_mode(self) -> CheckpointMode:
        """Return the parsed configured opening-checkpoint mode from startup env.

        Returns:
            Parsed configured mode before the kill switch:
            ``"off"``, ``"shadow"``, or ``"active"``.
        """
        return self._configured_checkpoint_mode

    async def start(self) -> None:
        """Spawn the tick loop. No-op in disabled mode."""
        if self._disabled or self._repo is None:
            logger.info(
                "PortfolioPnlSnapshotter: disabled (PNL_SNAPSHOTTER_ENABLED not set); skipping start"
            )
            await asyncio.sleep(0)
            return
        self._stopping.clear()
        self._loop_task = asyncio.create_task(self._loop())
        await asyncio.sleep(0)

    async def stop(self) -> None:
        """Signal the loop to exit, cancel + await its task."""
        self._stopping.set()
        task = self._loop_task
        if task is not None and not task.done():
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
        self._loop_task = None

    async def _loop(self) -> None:
        """Sleep then tick; repeat until cancelled.

        Pre-sleep on every iteration so an ephemeral test app that lives shorter
        than ``interval_seconds`` never pays for a tick. A defensive ``try/except``
        keeps the loop alive across unexpected exceptions.
        """
        while not self._stopping.is_set():
            try:
                await asyncio.wait_for(self._stopping.wait(), timeout=self._interval_seconds)
                return
            except TimeoutError:
                pass
            if self._stopping.is_set():
                return
            try:
                await self._tick_once()
            except Exception:
                logger.exception("PortfolioPnlSnapshotter._tick_once raised; continuing")

    async def _tick_once(self) -> None:
        """Discover every USD-anchored scope and sample it under failure isolation."""
        repo = self._repo
        if repo is None:
            raise RuntimeError("PortfolioPnlSnapshotter._tick_once called in disabled mode")
        as_of = self._clock()
        self._fx_shadow_context = (
            FxShadowPinContext(calculation_version=PNL_SAMPLE_CALC_VERSION, evaluations=[])
            if self._fx_shadow_pinning_enabled
            else None
        )
        try:
            with activate_fx_shadow_context(self._fx_shadow_context):
                self._tick_deadline = self._monotonic() + self._interval_seconds
                async with asyncio.timeout(self._interval_seconds):
                    await self._tick_valuation(repo, as_of)
        except TimeoutError:
            logger.warning("PortfolioPnlSnapshotter: replay tick exceeded its time budget")
        finally:
            self._tick_deadline = None
            context = self._fx_shadow_context
            self._fx_shadow_context = None
            if context is not None:
                await context.flush_bounded(repo)
        self._checkpoint_summary_ticks = advance_checkpoint_summary_tick(
            self._checkpoint_summary_ticks,
            self._checkpoint_mode,
            logger,
            every_ticks=CHECKPOINT_SUMMARY_EVERY_TICKS,
        )

    async def _tick_valuation(self, repo: Repository, as_of: datetime) -> None:
        """Run one tick's valuation and durable sample work before shadow flushing."""
        self._tick_recaptures = 0
        self._tick_shadow_comparisons = 0
        self._tick_last_budgeted_wallet = None
        credentials = await repo.list_active_wallet_credentials(as_of)
        raw_pin = await self._read_mint_wallet_pin()
        topology = await resolve_executor_topology(repo, credentials, raw_pin, self._clock)
        venues_by_wallet = resolve_spot_venues(credentials, topology)
        mint_excluded_wallets = _mint_excluded_spot_wallets(credentials, topology)
        self._prune_absent_scopes(set(venues_by_wallet))
        failures = 0
        order = self._wallet_order(venues_by_wallet)
        for wallet in order:
            self._last_scheduled_wallet = wallet
            try:
                conflicted = await self._process_wallet(
                    repo,
                    wallet,
                    venues_by_wallet[wallet],
                    as_of,
                    wallet in mint_excluded_wallets,
                )
            except asyncio.CancelledError:
                self._reduce_replay_chunk(wallet)
                raise
            except Exception as exc:
                if isinstance(exc, DBAPIError) and getattr(exc.orig, "sqlstate", None) == "57014":
                    self._reduce_replay_chunk(wallet)
                failures += 1
                self._log_class(wallet, "sample_failed", is_exception=True)
                continue
            if conflicted:
                failures += 1
            else:
                self._reset_scope_logs(wallet)
        if order:
            self._last_scheduled_wallet = self._tick_last_budgeted_wallet or order[0]
        if failures:
            logger.warning(
                "PortfolioPnlSnapshotter: %s scope(s) failed this tick; continuing", failures
            )

    def _wallet_order(self, wallets: Mapping[str, frozenset[str]]) -> list[str]:
        """Resume after the last scheduled scope so a slow wallet cannot starve its peers."""
        ordered = sorted(wallets)
        previous = self._last_scheduled_wallet
        offset = bisect_right(ordered, previous) if previous is not None else 0
        return ordered[offset:] + ordered[:offset]

    def _replay_chunk_limit(self, wallet: str) -> int:
        """Return the scope's retry limit without changing its durable cursor."""
        return self._replay_chunk_limits.get(wallet, MAX_CORRECTION_CHUNK_MINUTES)

    def _reduce_replay_chunk(self, wallet: str) -> None:
        """Retry less valuation work after cancellation or a database query deadline."""
        self._replay_chunk_limits[wallet] = max(1, self._replay_chunk_limit(wallet) // 2)
        self._replay_chunk_streaks.pop(wallet, None)

    def _record_chunk_outcome(self, wallet: str, elapsed: float, replayed: ChunkWindow) -> None:
        """Grow a reduced chunk limit back after consecutive fast full-width commits.

        A halved limit otherwise persists for the life of the process, so one
        transient deadline would multiply every later chunk's fixed prefix cost.
        Only committed chunks reach this method; rows stay width-independent
        because valuation is per point, so recovery changes only throughput.
        A slow chunk resets the streak whatever its width, which is conservative.
        A fast chunk counts only when it spanned the whole current limit, because
        a tip-clipped or budget-split chunk says nothing about a full-width one.

        Args:
            wallet: Wallet whose replay chunk has just committed.
            elapsed: Monotonic wall time of that chunk, in seconds.
            replayed: The window actually replayed and committed.
        """
        limit = self._replay_chunk_limit(wallet)
        if limit >= MAX_CORRECTION_CHUNK_MINUTES:
            self._replay_chunk_streaks.pop(wallet, None)
            return
        if elapsed >= self._interval_seconds * _CHUNK_RECOVERY_BUDGET_FRACTION:
            self._replay_chunk_streaks[wallet] = 0
            return
        if (replayed.end - replayed.start) // _MINUTE + 1 < limit:
            return
        streak = self._replay_chunk_streaks.get(wallet, 0) + 1
        if streak < _CHUNK_RECOVERY_STREAK:
            self._replay_chunk_streaks[wallet] = streak
            return
        self._replay_chunk_streaks.pop(wallet, None)
        grown = min(limit * 2, MAX_CORRECTION_CHUNK_MINUTES)
        if grown >= MAX_CORRECTION_CHUNK_MINUTES:
            self._replay_chunk_limits.pop(wallet, None)
        else:
            self._replay_chunk_limits[wallet] = grown

    async def _read_mint_wallet_pin(self) -> JsonValue:
        """Read the current mint pin directly from storage, failing open."""
        if self._settings_service is None:
            return ""
        try:
            return await self._settings_service.get_setting_fresh(MINT_WALLET_PIN_SETTING_KEY)
        except Exception:
            logger.exception(
                "PortfolioPnlSnapshotter: mint-pin lookup failed; applying unrestricted topology"
            )
            return ""

    def _log_class(
        self, wallet: str, failure_class: str, *args: object, is_exception: bool = False
    ) -> None:
        """Log one scope failure class at most once per failing streak (B4/C2).

        The four reconcile/catch-up conflict warnings and the per-scope exception
        route through the same per-``(wallet, failure_class)`` log-once set, reset
        on a clean scope; the tick still emits one aggregate warning per tick.
        """
        key = (wallet, failure_class)
        if key in self._failure_logged:
            return
        self._failure_logged.add(key)
        message = _FAILURE_MESSAGES[failure_class]
        if is_exception:
            logger.exception(message, wallet)
        else:
            logger.warning(message, wallet, *args)

    def _reset_scope_logs(self, wallet: str) -> None:
        """Clear a wallet's failure streak once it completes a clean tick (B4/C2)."""
        self._failure_logged = {
            (logged_wallet, failure_class)
            for logged_wallet, failure_class in self._failure_logged
            if logged_wallet != wallet
        }
        self._self_heal_logged.pop(wallet, None)

    def _prune_absent_scopes(self, current_wallets: set[str]) -> None:
        """Drop in-memory state for wallets absent from this discovery pass (B5)."""
        prune_checkpoint_metrics(current_wallets)
        self._unsupported_logged.intersection_update(current_wallets)
        self._mint_identity_logged.intersection_update(current_wallets)
        self._no_anchor_logged.intersection_update(current_wallets)
        self._failure_logged = {
            (wallet, failure_class)
            for wallet, failure_class in self._failure_logged
            if wallet in current_wallets
        }
        self._self_heal_logged = {
            wallet: value
            for wallet, value in self._self_heal_logged.items()
            if wallet in current_wallets
        }
        self._withheld_logged = {
            wallet: value
            for wallet, value in self._withheld_logged.items()
            if wallet in current_wallets
        }
        self._replay_chunk_limits = {
            wallet: limit
            for wallet, limit in self._replay_chunk_limits.items()
            if wallet in current_wallets
        }
        self._replay_chunk_streaks = {
            wallet: streak
            for wallet, streak in self._replay_chunk_streaks.items()
            if wallet in current_wallets
        }
        self._persistent_fill_gap_skip_logged = {
            scope_key
            for scope_key in self._persistent_fill_gap_skip_logged
            if scope_key[0] in current_wallets
        }
        self._repair_from_lag_logged = {
            scope_key: repair_from
            for scope_key, repair_from in self._repair_from_lag_logged.items()
            if scope_key[0] in current_wallets
        }

    async def _process_wallet(
        self,
        repo: Repository,
        wallet: str,
        expected_venues: frozenset[str],
        as_of: datetime,
        mint_identity_excluded: bool,
    ) -> bool:
        """Resolve one wallet's anchored USD scope and sample it, or skip honestly.

        Returns whether the scope hit a reconcile or catch-up conflict this tick
        (so the tick counts it toward the aggregate and never resets its streak).
        """
        anchor = await repo.get_portfolio_pnl_anchor(wallet, _LIVE_MODE, _USD, None)
        if anchor is None:
            self._log_once(
                self._no_anchor_logged,
                wallet,
                "no active USD anchor; skipping",
                reason=_SCOPE_SKIP_NO_ANCHOR,
            )
            return False
        if not expected_venues:
            if mint_identity_excluded:
                self._log_once(
                    self._mint_identity_logged,
                    wallet,
                    "no executor-backed spot venues (pinned equities token-mint identity); "
                    "unsupported",
                    reason=_SCOPE_SKIP_MINT_IDENTITY,
                )
            else:
                self._log_once(
                    self._unsupported_logged,
                    wallet,
                    "no spot venues (futures-only); unsupported",
                    reason=_SCOPE_SKIP_NO_SPOT_VENUES,
                )
            return False
        ctx = self._build_scope_context(wallet, anchor, expected_venues, as_of)
        return await self._sample_scope(repo, ctx)

    @staticmethod
    def _build_scope_context(
        wallet: str,
        anchor: PortfolioPnlAnchorRow,
        expected_venues: frozenset[str],
        as_of: datetime,
    ) -> _ScopeContext:
        """Bind checkpoint reads and financial writes to the same active anchor."""
        epoch = anchor["epoch_public_id"]
        t0 = anchor["point_time"]
        scope = PortfolioPnlSampleScope(
            wallet_public_id=wallet,
            mode=_LIVE_MODE,
            valuation_ccy=_USD,
            epoch_public_id=epoch,
            anchor_point_time=t0,
        )
        query = PortfolioPnlSampleQuery(
            wallet_public_id=wallet,
            mode=_LIVE_MODE,
            valuation_ccy=_USD,
            epoch_public_id=epoch,
            calc_version=PNL_SAMPLE_CALC_VERSION,
        )
        return _ScopeContext(
            wallet_public_id=wallet,
            scope=scope,
            query=query,
            t0=t0,
            expected_venues=expected_venues,
            as_of=as_of,
            anchor=anchor,
        )

    def _log_once(self, seen: set[str], wallet: str, detail: str, *, reason: str) -> None:
        """Log one wallet-scoped lifecycle notice at most once per process."""
        if wallet in seen:
            return
        seen.add(wallet)
        logger.info(
            "PortfolioPnlSnapshotter: wallet %s %s",
            wallet,
            detail,
            extra={"scope_skip_reason": reason},
        )

    async def _sample_scope(self, repo: Repository, ctx: _ScopeContext) -> bool:
        """Commit at most one replay chunk, resuming durable work before probing new evidence."""
        try:
            frontier = await repo.get_portfolio_pnl_correction_frontier(ctx.scope)
            self._log_repair_from_lag(ctx, frontier)
            if frontier is not None and ctx.as_of < frontier.as_of:
                return False
            if (
                frontier is None
                or frontier.next_minute is None
                or frontier.raw_execution_watermarks is None
                or frontier.execution_manifest_digest is None
                or frontier.has_fill_gap is None
                or frontier.expected_venues is None
                or frontier.venue_event_watermarks is None
            ):
                frontier = await self._start_replay(repo, ctx, frontier)
            if frontier is not None:
                await self._advance_replay(repo, ctx, frontier)
        except PortfolioPnlCorrectionConflictError, PnlFrozenReplayEvidenceChangedError:
            self._log_class(ctx.wallet_public_id, "catchup_conflict", "durable frontier")
            return True
        return False

    async def _start_replay(
        self,
        repo: Repository,
        ctx: _ScopeContext,
        frontier: PnlCorrectionFrontier | None,
        *,
        rewind: bool = False,
    ) -> PnlCorrectionFrontier | None:
        """Select an inclusive finalized target and freeze its execution evidence prefix."""
        tip = ctx.as_of.replace(second=0, microsecond=0) - FINALIZATION_LAG
        if tip <= ctx.t0:
            return None
        if frontier is not None:
            tip = max(tip, frontier.tip)
        pending = await self._probe_replay(repo, ctx, frontier, tip)
        witness = pending.fill_witness_cut
        if witness is None:
            raise PortfolioPnlCorrectionConflictError(
                "P&L replay could not certify its fill witnesses"
            )
        start = (
            ctx.t0 + _MINUTE
            if rewind
            else await self._replay_start(repo, ctx, frontier, tip, pending)
        )
        if start is None:
            return None
        first_minute = await repo.get_portfolio_pnl_correction_evidence(ctx.scope, start, start)
        started = await repo.invalidate_portfolio_pnl_correction(
            ctx.scope,
            PnlCorrectionRequest(
                start=start,
                tip=tip,
                as_of=ctx.as_of,
                baseline_watermarks=frontier.baseline_watermarks if frontier is not None else {},
                pending_watermarks=_detection_watermarks(pending),
                prior_peak=first_minute.prior_peak,
                raw_execution_watermarks=pending.raw_execution_watermarks,
                execution_manifest_digest=pending.execution_manifest_digest,
                has_fill_gap=pending.has_fill_gap,
                expected_venues=ctx.expected_venues,
                venue_event_watermarks=witness["watermarks"],
                fill_witness_digest=witness["digest"],
            ),
            frontier,
        )
        self._clear_fill_gap_skip_streak(ctx.scope)
        return started

    async def _probe_replay(
        self,
        repo: Repository,
        ctx: _ScopeContext,
        frontier: PnlCorrectionFrontier | None,
        tip: datetime,
    ) -> PnlSeriesReplayMetadata:
        """Read a single finalized minute to detect late executions even at a stationary tip."""
        options = PnlSeriesReplayOptions(
            baseline_watermarks=dict(frontier.pending_watermarks) if frontier is not None else {},
            offload_valuation=True,
            previous_replay_as_of=frontier.as_of if frontier is not None else None,
            verify_replay_probe=True,
            metadata_only=True,
        )
        result = await self._series(repo, ctx, ChunkWindow(tip, tip), options)
        metadata = result.replay_metadata
        if (
            metadata is None
            or metadata.raw_execution_watermarks is None
            or metadata.execution_manifest_digest is None
            or metadata.has_fill_gap is None
        ):
            raise PortfolioPnlCorrectionConflictError("P&L replay could not certify its evidence")
        return metadata

    async def _replay_start(
        self,
        repo: Repository,
        ctx: _ScopeContext,
        frontier: PnlCorrectionFrontier | None,
        tip: datetime,
        pending: PnlSeriesReplayMetadata,
    ) -> datetime | None:
        """Recompute the oldest unresolved minute through the selected finalized tip."""
        first = ctx.t0 + _MINUTE
        if (
            frontier is None
            or frontier.raw_execution_watermarks is None
            or frontier.venue_event_watermarks is None
            or frontier.execution_manifest_digest != pending.execution_manifest_digest
            or frontier.has_fill_gap != pending.has_fill_gap
            or frontier.expected_venues != ctx.expected_venues
        ):
            return first
        if pending.has_fill_gap:
            if await self._can_skip_persistent_fill_gap(repo, ctx, frontier, tip, pending):
                self._log_persistent_fill_gap_skip(ctx, frontier)
                return None
            return first
        late = plan_late_fill_recompute(pending.earliest_affected_minute, frontier.tip)
        healed = await self._self_heal_start(repo, ctx, frontier.tip)
        forward = frontier.tip + _MINUTE if frontier.tip < tip else None
        candidates = (
            minute for minute in (late, healed, frontier.repair_from, forward) if minute is not None
        )
        start = min(candidates, default=None)
        return max(start, first) if start is not None else None

    @staticmethod
    def _scope_streak_key(scope: PortfolioPnlSampleScope) -> tuple[str, str, str, str]:
        """Return the scope tuple identifying one persistent fill-gap pause streak."""
        return (
            scope.wallet_public_id,
            scope.mode,
            scope.valuation_ccy,
            scope.epoch_public_id,
        )

    def _clear_fill_gap_skip_streak(self, scope: PortfolioPnlSampleScope) -> None:
        """End a scope's persistent fill-gap pause streak once replay resumes."""
        self._persistent_fill_gap_skip_logged.discard(self._scope_streak_key(scope))

    def _log_persistent_fill_gap_skip(
        self, ctx: _ScopeContext, frontier: PnlCorrectionFrontier
    ) -> None:
        """Emit one liveness log per pause streak for a persistently gapped scope."""
        scope_key = self._scope_streak_key(ctx.scope)
        if scope_key in self._persistent_fill_gap_skip_logged:
            return
        self._persistent_fill_gap_skip_logged.add(scope_key)
        gap_age = ctx.as_of - frontier.as_of
        logger.info(
            "PortfolioPnlSnapshotter: wallet %s paused replay on persistent fill gap "
            "(mode=%s ccy=%s epoch=%s frontier_tip=%s frontier_as_of=%s as_of=%s gap_age=%s)",
            ctx.scope.wallet_public_id,
            ctx.scope.mode,
            ctx.scope.valuation_ccy,
            ctx.scope.epoch_public_id,
            frontier.tip.isoformat(),
            frontier.as_of.isoformat(),
            ctx.as_of.isoformat(),
            gap_age,
            extra={
                "scope_skip_reason": _SCOPE_SKIP_PERSISTENT_FILL_GAP,
                "wallet_public_id": ctx.scope.wallet_public_id,
                "mode": ctx.scope.mode,
                "valuation_ccy": ctx.scope.valuation_ccy,
                "epoch_public_id": ctx.scope.epoch_public_id,
                "frontier_tip": frontier.tip,
                "frontier_as_of": frontier.as_of,
                "as_of": ctx.as_of,
                "gap_age": gap_age,
            },
        )

    def _drop_superseded_epoch_lag_logs(self, scope_key: tuple[str, str, str, str]) -> None:
        """Forget lag-log entries of earlier epochs once a scope re-anchors.

        :meth:`_prune_absent_scopes` prunes by wallet, so a wallet that keeps its
        credential and venues across a re-anchor would otherwise keep its old
        epoch's entry for the life of the process, one stale entry per re-anchor.

        Args:
            scope_key: The ``(wallet, mode, valuation_ccy, epoch)`` scope now logged.
        """
        self._repair_from_lag_logged = {
            key: repair_from
            for key, repair_from in self._repair_from_lag_logged.items()
            if key[:3] != scope_key[:3] or key == scope_key
        }

    def _log_repair_from_lag(
        self, ctx: _ScopeContext, frontier: PnlCorrectionFrontier | None
    ) -> None:
        """Emit one structured liveness log per scope and distinct ``repair_from`` past 24 h.

        Entries for earlier epochs of the same wallet, mode and currency are dropped
        first, so a re-anchored scope keeps at most one entry.

        Args:
            ctx: Active scope binding and tick clock.
            frontier: Persisted or newly started correction frontier, if any.
        """
        scope_key = self._scope_streak_key(ctx.scope)
        self._drop_superseded_epoch_lag_logs(scope_key)
        if frontier is None or frontier.repair_from is None:
            self._repair_from_lag_logged.pop(scope_key, None)
            return
        repair_from = frontier.repair_from
        lag = frontier.tip - repair_from
        if lag <= _REPAIR_FROM_LAG_THRESHOLD:
            self._repair_from_lag_logged.pop(scope_key, None)
            return
        if self._repair_from_lag_logged.get(scope_key) == repair_from:
            return
        self._repair_from_lag_logged[scope_key] = repair_from
        logger.warning(
            "PortfolioPnlSnapshotter: wallet %s repair_from lags frontier tip by %s "
            "(mode=%s ccy=%s epoch=%s repair_from=%s frontier_tip=%s as_of=%s)",
            ctx.scope.wallet_public_id,
            lag,
            ctx.scope.mode,
            ctx.scope.valuation_ccy,
            ctx.scope.epoch_public_id,
            repair_from.isoformat(),
            frontier.tip.isoformat(),
            ctx.as_of.isoformat(),
            extra={
                "wallet_public_id": ctx.scope.wallet_public_id,
                "mode": ctx.scope.mode,
                "valuation_ccy": ctx.scope.valuation_ccy,
                "epoch_public_id": ctx.scope.epoch_public_id,
                "repair_from": repair_from,
                "frontier_tip": frontier.tip,
                "as_of": ctx.as_of,
                "repair_from_lag": lag,
            },
        )

    @staticmethod
    async def _can_skip_persistent_fill_gap(
        repo: Repository,
        ctx: _ScopeContext,
        frontier: PnlCorrectionFrontier,
        tip: datetime,
        pending: PnlSeriesReplayMetadata,
    ) -> bool:
        """Prove a settled gapped generation can skip re-replay without changing truth."""
        if not _has_stationary_persistent_fill_gap(ctx, frontier, pending):
            return False
        return not await repo.has_active_portfolio_pnl_samples(ctx.scope, ctx.t0 + _MINUTE, tip)

    async def _self_heal_start(
        self, repo: Repository, ctx: _ScopeContext, last_minute: datetime
    ) -> datetime | None:
        """Return the earliest retryable incomplete minute in the lookback (D10)."""
        cutoff = ctx.as_of.replace(second=0, microsecond=0) - SELF_HEAL_LOOKBACK
        incompletes = await repo.get_portfolio_pnl_samples(
            ctx.query, cutoff, last_minute, status="incomplete"
        )
        candidates = [
            SelfHealCandidate(
                point_time=row["point_time"],
                reason_codes=_extract_reason_codes(row["audit_json"]),
            )
            for row in incompletes
        ]
        minutes = plan_self_heal_minutes(candidates, ctx.as_of)
        if minutes:
            selected = next(
                candidate for candidate in candidates if candidate.point_time == minutes[0]
            )
            key = (minutes[0], tuple(sorted(selected.reason_codes)))
            if self._self_heal_logged.get(ctx.wallet_public_id) != key:
                logger.info(
                    "PortfolioPnlSnapshotter: wallet %s self-heal window starts at %s "
                    "with reasons %s",
                    ctx.wallet_public_id,
                    key[0],
                    key[1],
                )
                self._self_heal_logged[ctx.wallet_public_id] = key
        return minutes[0] if minutes else None

    @staticmethod
    def _split_chunk(chunk: ChunkWindow) -> list[ChunkWindow]:
        """Split one chunk into two minute-halves, refusing a single minute.

        Args:
            chunk: The over-budget chunk to subdivide.

        Returns:
            The two covering half-chunks.

        Raises:
            PnlTimelineWorkBudgetError: When a single minute already overflows.
        """
        if chunk.start == chunk.end:
            raise PnlTimelineWorkBudgetError(
                "single-minute portfolio P&L chunk exceeds the work budget"
            )
        total_minutes = int((chunk.end - chunk.start) / _MINUTE) + 1
        left_end = chunk.start + (total_minutes // 2 - 1) * _MINUTE
        return [
            ChunkWindow(start=chunk.start, end=left_end),
            ChunkWindow(start=left_end + _MINUTE, end=chunk.end),
        ]

    async def _series(
        self,
        repo: Repository,
        ctx: _ScopeContext,
        chunk: ChunkWindow,
        options: PnlSeriesReplayOptions,
    ) -> PnlWalletSeriesResult:
        """Run the 5A engine over one chunk at the specified knowledge horizon."""
        return await build_wallet_pnl_series(
            repo,
            ctx.wallet_public_id,
            _LIVE_MODE,
            chunk.start,
            chunk.end,
            "1m",
            ctx.as_of,
            valuation_ccy=_USD,
            policy=PnlSeriesReadPolicy(
                allow_anchor_creation=False,
                current_truth_horizon=False,
            ),
            options=replace(options, pointwise_exact_fx=True),
        )

    async def _advance_replay(
        self, repo: Repository, ctx: _ScopeContext, frontier: PnlCorrectionFrontier
    ) -> None:
        """Resume one bounded chunk at the generation's fixed evidence horizon."""
        self._log_repair_from_lag(ctx, frontier)
        start = frontier.next_minute
        venues = frontier.expected_venues
        if start is None or venues is None:
            raise PortfolioPnlCorrectionConflictError("P&L replay has no certified pending window")
        self._clear_fill_gap_skip_streak(ctx.scope)
        limit = self._replay_chunk_limit(ctx.wallet_public_id)
        end = min(start + (limit - 1) * _MINUTE, frontier.tip)
        frozen = replace(ctx, as_of=frontier.as_of, expected_venues=venues)
        started = self._monotonic()
        try:
            if self._checkpoint_mode != "off":
                await self._advance_checkpoint(
                    repo,
                    frozen,
                    frontier,
                    ChunkWindow(start, end),
                    started,
                )
                return
            chunk, result = await self._replay_series(
                repo, frozen, ChunkWindow(start, end), frontier
            )
        except PnlFrozenReplayEvidenceChangedError:
            await self._start_replay(repo, ctx, frontier, rewind=True)
            return
        evidence = await repo.get_portfolio_pnl_correction_evidence(
            ctx.scope, chunk.start, chunk.end
        )
        if evidence.prior_peak != frontier.prior_peak:
            await self._rewind_replay(repo, frozen, frontier)
            return
        plan = await self._plan_chunk(repo, frozen, result, _PlanBasis(evidence))
        committed = await repo.commit_portfolio_pnl_correction_chunk(
            ctx.scope, frontier, plan.chunk
        )
        self._record_chunk_outcome(ctx.wallet_public_id, self._monotonic() - started, chunk)
        self._log_repair_from_lag(ctx, committed)
        self._report_withheld(ctx.wallet_public_id, plan.withheld, bool(plan.chunk.edits))

    def _record_recapture_consumed(self, wallet: str) -> None:
        """Advance the tick recapture budget and mark the consuming wallet for rotation."""
        self._tick_recaptures += 1
        self._tick_last_budgeted_wallet = wallet

    def _record_shadow_comparison_consumed(self, wallet: str) -> None:
        """Advance the tick comparison budget and mark the consuming wallet for rotation."""
        self._tick_shadow_comparisons += 1
        self._tick_last_budgeted_wallet = wallet

    def _can_shadow_compare(self) -> bool:
        """Return whether this tick still has budget for verification and resumed replay."""
        return self._tick_shadow_comparisons < MAX_SHADOW_COMPARISONS_PER_TICK

    def _can_recapture(self) -> bool:
        """Return whether this tick still has budget for a full anchor recapture."""
        return self._tick_recaptures < checkpoint_dispatch.MAX_RECAPTURES_PER_TICK

    def _consume_recapture(
        self,
        request: _CheckpointDispatchRequest,
        end: datetime,
    ) -> PnlCheckpointCaptureRequest | None:
        """Issue one tip recapture request when the per-tick recapture budget permits."""
        ctx, frontier = request.ctx, request.frontier
        if not self._can_recapture():
            if (
                checkpoint_dispatch.checkpoint_capture_permitted(frontier.key.calc_version)
                and frontier.next_minute is not None
                and end == frontier.tip
                and frontier.has_fill_gap is False
            ):
                checkpoint_dispatch.record_checkpoint_write_refusal(
                    ctx.scope,
                    "capture_recapture_budget_exceeded",
                )
            return None
        recapture = capture_request_for(frontier, ctx.scope, ctx.anchor, end)
        if recapture is not None:
            self._record_recapture_consumed(ctx.wallet_public_id)
        return recapture

    @staticmethod
    def _shadow_window(chunk: ChunkWindow, key: PnlCorrectionKey) -> PnlCheckpointShadowWindow:
        """Bind window bounds, exact four-part scope identity, and calc version."""
        return PnlCheckpointShadowWindow(
            from_time=chunk.start,
            to_time=chunk.end,
            scope=(
                key.wallet_public_id,
                key.mode,
                key.valuation_ccy,
                key.epoch_public_id,
            ),
            calc_version=key.calc_version,
        )

    async def _record_shadow_mismatch(
        self,
        repo: Repository,
        ctx: _ScopeContext,
        resume: PnlCheckpointResume,
        mismatch: PnlCheckpointShadowMismatch,
    ) -> None:
        """Count, delete, and log a genuine shadow divergence using an aliased wallet."""
        record_checkpoint_shadow_mismatch(ctx.scope)
        await delete_checkpoint(repo, ctx.scope, resume.record)
        logger.error(
            "checkpoint_shadow_mismatch wallet=%s generation=%s point=%s field=%s",
            checkpoint_wallet_alias(ctx.wallet_public_id),
            resume.record.certificate.source_generation,
            mismatch.point_time,
            mismatch.field,
            extra={
                "checkpoint_shadow_mismatch": True,
                "point_time": mismatch.point_time,
                "field": mismatch.field,
            },
        )

    async def _compare_shadow_branch(
        self,
        request: _CheckpointDispatchRequest,
        basis: _PlanBasis,
        anchor_pair: tuple[ChunkWindow, PnlWalletSeriesResult, PlannedPnlCorrection],
        resumed_pair: tuple[ChunkWindow, PnlWalletSeriesResult],
    ) -> None:
        """Plan resumed shadow work without side effects and compare identical windows."""
        ctx, resume = request.ctx, request.resume
        assert resume is not None
        anchor_chunk, anchor, anchor_plan = anchor_pair
        resumed_chunk, resumed = resumed_pair
        anchor_window = self._shadow_window(anchor_chunk, request.frontier.key)
        resumed_window = self._shadow_window(resumed_chunk, resume.record.certificate.key)
        if anchor_window != resumed_window:
            record_checkpoint_shadow_skipped("window_mismatch", ctx.scope)
            return
        resumed_plan = await self._plan_shadow_chunk(request.repo, ctx, resumed, basis)
        outcome = compare_checkpoint_shadow_sides(
            PnlCheckpointShadowSide(anchor_window, anchor, anchor_plan),
            PnlCheckpointShadowSide(resumed_window, resumed, resumed_plan),
        )
        record_checkpoint_shadow_comparison(ctx.scope)
        if outcome.mismatch is not None:
            await self._record_shadow_mismatch(request.repo, ctx, resume, outcome.mismatch)

    async def _advance_checkpoint(
        self,
        repo: Repository,
        ctx: _ScopeContext,
        frontier: PnlCorrectionFrontier,
        chunk: ChunkWindow,
        started: float,
    ) -> None:
        """Dispatch one non-off chunk through capture, shadow, or active resume."""
        request = _CheckpointDispatchRequest(repo, ctx, frontier, chunk, started, None, None)
        if self._checkpoint_mode == "shadow" and not self._can_shadow_compare():
            await self._advance_active(request)
            return
        decision = await prepare_checkpoint_resume(
            PnlCheckpointResumeRequest(
                repo,
                ctx.scope,
                ctx.anchor,
                frontier,
                chunk.start,
                self._checkpoint_mode,
                recapture_budget_exhausted=not self._can_recapture(),
            )
        )
        resume = decision.resume
        capture: PnlCheckpointCaptureRequest | None = None
        if decision.recapture is not None:
            capture = capture_request_for(frontier, ctx.scope, ctx.anchor, chunk.end)
            if capture is not None:
                self._record_recapture_consumed(ctx.wallet_public_id)
        request = _CheckpointDispatchRequest(repo, ctx, frontier, chunk, started, capture, resume)
        if self._checkpoint_mode == "shadow" and (
            resume is not None or decision.verification_attempted
        ):
            self._record_shadow_comparison_consumed(ctx.wallet_public_id)
        if self._checkpoint_mode == "shadow" and resume is not None:
            await self._advance_shadow(request)
            return
        await self._advance_active(request)

    async def _advance_active(self, request: _CheckpointDispatchRequest) -> None:
        """Commit a resumed plan, or an anchor plan after a soft resume fallback."""
        repo, ctx, frontier = request.repo, request.ctx, request.frontier
        capture, resume = request.capture, request.resume
        if resume is not None:
            record_checkpoint_resume_attempted(ctx.scope)
        chunk, result = await self._replay_series(
            repo,
            ctx,
            request.chunk,
            frontier,
            _CheckpointReplayOptions(capture if resume is None else None, resume),
        )
        fallback = result.checkpoint_resume_fallback
        if resume is not None and fallback is not None:
            await handle_resume_fallback(repo, ctx.scope, frontier, resume, fallback)
            recapture = capture or self._consume_recapture(request, chunk.end)
            if recapture is not None:
                chunk, result = await self._replay_series(
                    repo,
                    ctx,
                    chunk,
                    frontier,
                    _CheckpointReplayOptions(capture=recapture),
                )
        elif resume is not None:
            record_checkpoint_resume_succeeded(ctx.scope)
        await self._plan_and_commit_checkpoint(request, chunk, result)

    def _record_shadow_failure(self, ctx: _ScopeContext, exc: Exception) -> None:
        """Count every isolated failure and log once per scope failure episode."""
        wallet = ctx.wallet_public_id
        reason = f"shadow_error:{type(exc).__name__}"
        record_checkpoint_shadow_failure(reason, ctx.scope)
        if wallet in self._shadow_failure_logged:
            return
        self._shadow_failure_logged.add(wallet)
        logger.warning(
            "PortfolioPnlSnapshotter: checkpoint shadow failed; wallet=%s error_type=%s",
            checkpoint_wallet_alias(wallet),
            type(exc).__name__,
            extra={"checkpoint_shadow_error": reason},
        )

    async def _run_shadow_branch(
        self,
        request: _CheckpointDispatchRequest,
        basis: _PlanBasis,
        anchor_pair: tuple[ChunkWindow, PnlWalletSeriesResult, PlannedPnlCorrection],
    ) -> None:
        """Run only non-authoritative resumed comparison and fallback retirement work.

        A resume fallback retires the stored checkpoint without an inline
        recapture: the authoritative chunk has already committed, so the next
        tick's ``absent`` path captures a fresh checkpoint on the cold path.
        """
        repo, ctx, frontier = request.repo, request.ctx, request.frontier
        resume = request.resume
        assert resume is not None
        anchor_chunk = anchor_pair[0]
        record_checkpoint_resume_attempted(ctx.scope)
        with activate_fx_shadow_context(None):
            resumed_chunk, resumed = await self._replay_series(
                repo,
                ctx,
                anchor_chunk,
                frontier,
                _CheckpointReplayOptions(resume=resume),
            )
        fallback = resumed.checkpoint_resume_fallback
        if fallback is not None:
            await handle_resume_fallback(repo, ctx.scope, frontier, resume, fallback)
            return
        record_checkpoint_resume_succeeded(ctx.scope)
        await self._compare_shadow_branch(
            request,
            basis,
            anchor_pair,
            (resumed_chunk, resumed),
        )

    async def _advance_shadow(self, request: _CheckpointDispatchRequest) -> None:
        """Commit the cold anchor plan first, then run isolated shadow work.

        Committing before any shadow await means no shadow outcome, including
        a valuation worker that cannot be cancelled, can skip or delay the
        authoritative write. Shadow work then runs under its own budget and
        every non-cancellation failure is recorded as an isolated shadow error.
        """
        repo, ctx, frontier = request.repo, request.ctx, request.frontier
        anchor_chunk, anchor = await self._replay_series(repo, ctx, request.chunk, frontier)
        evidence = await repo.get_portfolio_pnl_correction_evidence(
            ctx.scope, anchor_chunk.start, anchor_chunk.end
        )
        if evidence.prior_peak != frontier.prior_peak:
            await self._rewind_replay(repo, ctx, frontier)
            return
        inputs = await self._load_planning_inputs(
            repo, ctx, ChunkWindow(evidence.start, evidence.end)
        )
        basis = _PlanBasis(evidence, inputs)
        anchor_plan = await self._plan_chunk(repo, ctx, anchor, basis)
        await self._commit_checkpoint_plan(request, anchor_chunk, anchor, anchor_plan)
        budget = self._shadow_budget_seconds()
        if budget is not None and budget <= 0:
            record_checkpoint_shadow_skipped("tick_budget", ctx.scope)
            return
        try:
            async with asyncio.timeout(budget):
                await self._run_shadow_branch(request, basis, (anchor_chunk, anchor, anchor_plan))
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self._record_shadow_failure(ctx, exc)
        else:
            self._shadow_failure_logged.discard(ctx.wallet_public_id)

    def _shadow_budget_seconds(self) -> float | None:
        """Return the shadow branch's time budget, or ``None`` outside a deadline-bound tick.

        The budget ends ``SHADOW_TICK_RESERVE_FRACTION`` of the interval
        before the tick deadline, so a slow shadow branch is cut short as an
        isolated shadow failure and leaves time for the tick's later wallets.
        A non-positive value means shadow work must be skipped.
        """
        if self._tick_deadline is None:
            return None
        reserve = self._interval_seconds * SHADOW_TICK_RESERVE_FRACTION
        return self._tick_deadline - self._monotonic() - reserve

    async def _plan_and_commit_checkpoint(
        self,
        request: _CheckpointDispatchRequest,
        chunk: ChunkWindow,
        result: PnlWalletSeriesResult,
    ) -> None:
        """Plan a selected checkpoint-mode replay against exact physical evidence."""
        repo, ctx, frontier = request.repo, request.ctx, request.frontier
        evidence = await repo.get_portfolio_pnl_correction_evidence(
            ctx.scope, chunk.start, chunk.end
        )
        if evidence.prior_peak != frontier.prior_peak:
            await self._rewind_replay(repo, ctx, frontier)
            return
        plan = await self._plan_chunk(repo, ctx, result, _PlanBasis(evidence))
        await self._commit_checkpoint_plan(request, chunk, result, plan)

    async def _commit_checkpoint_plan(
        self,
        request: _CheckpointDispatchRequest,
        chunk: ChunkWindow,
        result: PnlWalletSeriesResult,
        plan: PlannedPnlCorrection,
    ) -> None:
        """Commit the selected plan with only a pre-certified checkpoint write."""
        repo, ctx, frontier = request.repo, request.ctx, request.frontier
        write = certified_checkpoint_write(frontier, ctx.scope, plan, result)
        try:
            committed = await repo.commit_portfolio_pnl_correction_chunk(
                ctx.scope,
                frontier,
                plan.chunk,
                checkpoint=write,
            )
        except PnlCheckpointWriteRefusalError, SQLAlchemyError:
            if write is None:
                raise
            checkpoint_dispatch.record_checkpoint_write_refusal(
                ctx.scope,
                "capture_write_failed",
            )
            committed = await repo.commit_portfolio_pnl_correction_chunk(
                ctx.scope,
                frontier,
                plan.chunk,
                checkpoint=None,
            )
            write = None
        write_refusal = repo.consume_last_pnl_checkpoint_write_refusal()
        if write is not None:
            if write_refusal is None:
                record_checkpoint_capture_committed(ctx.scope)
            else:
                checkpoint_dispatch.record_checkpoint_write_refusal(ctx.scope, write_refusal)
        self._record_chunk_outcome(ctx.wallet_public_id, self._monotonic() - request.started, chunk)
        self._log_repair_from_lag(ctx, committed)
        self._report_withheld(ctx.wallet_public_id, plan.withheld, bool(plan.chunk.edits))

    async def _replay_series(
        self,
        repo: Repository,
        ctx: _ScopeContext,
        chunk: ChunkWindow,
        frontier: PnlCorrectionFrontier,
        checkpoint: _CheckpointReplayOptions | None = None,
    ) -> tuple[ChunkWindow, PnlWalletSeriesResult]:
        """Subdivide work-budget overflow without scheduling another successful chunk."""
        checkpoint = checkpoint or _CheckpointReplayOptions()
        venue_watermarks, witness_digest = (
            frontier.venue_event_watermarks,
            frontier.fill_witness_digest,
        )
        if venue_watermarks is None or witness_digest is None:
            raise PortfolioPnlCorrectionConflictError(
                "P&L replay has no certified fill-witness cut"
            )
        options = PnlSeriesReplayOptions(
            baseline_watermarks=dict(frontier.baseline_watermarks),
            offload_valuation=True,
            frozen_execution_watermarks=frontier.raw_execution_watermarks,
            frozen_execution_manifest_digest=frontier.execution_manifest_digest,
            frozen_has_fill_gap=frontier.has_fill_gap,
            frozen_fill_witness_cut={
                "watermarks": dict(venue_watermarks),
                "digest": witness_digest,
            },
            replay_generation_tip=frontier.tip,
            checkpoint_capture=checkpoint.capture,
            checkpoint_resume=checkpoint.resume,
        )
        while True:
            try:
                result = await self._series(repo, ctx, chunk, options)
            except PnlTimelineWorkBudgetError:
                chunk = self._split_chunk(chunk)[0]
                continue
            if result.replay_metadata is None:
                raise PortfolioPnlCorrectionConflictError("P&L replay anchor is unavailable")
            return chunk, result

    async def _rewind_replay(
        self, repo: Repository, ctx: _ScopeContext, frontier: PnlCorrectionFrontier
    ) -> None:
        """Rebuild the causal prefix after a concurrent change to an earlier peak."""
        await repo.invalidate_portfolio_pnl_correction(
            ctx.scope,
            PnlCorrectionRequest(
                start=ctx.t0 + _MINUTE,
                tip=frontier.tip,
                as_of=frontier.as_of,
                baseline_watermarks=frontier.baseline_watermarks,
                pending_watermarks=frontier.pending_watermarks,
                prior_peak=None,
                raw_execution_watermarks=frontier.raw_execution_watermarks,
                execution_manifest_digest=frontier.execution_manifest_digest,
                has_fill_gap=frontier.has_fill_gap,
                expected_venues=frontier.expected_venues,
                venue_event_watermarks=frontier.venue_event_watermarks,
                fill_witness_digest=frontier.fill_witness_digest,
            ),
            frontier,
        )

    def _report_withheld(
        self, wallet: str, withheld: Sequence[WithheldMinute], committed: bool
    ) -> None:
        """Alarm on minutes a scope planned and could not value, once per streak.

        Deliberately NOT routed through :meth:`_log_class`. That set is cleared by
        :meth:`_reset_scope_logs` on any tick that reports no conflict — and a
        scope withholding every minute reports no conflict at all, so the alarm
        would re-fire every tick and become the noise it exists to escape.

        The streak is keyed on what an operator would act on: the earliest hole
        and its causal reasons. A growing count under an unchanged cause is the
        same incident and stays quiet; a new earliest minute or a new reason is a
        new incident and speaks. The count still rides on the message, because a
        window that grows while the cause holds is the signature of a scope that
        is falling behind rather than one that briefly stumbled.

        Args:
            wallet: The scope's wallet public id.
            withheld: Every minute this tick planned and could not value.
            committed: Whether this tick wrote any row for this scope.
        """
        if not withheld:
            if committed:
                self._withheld_logged.pop(wallet, None)
            return
        earliest = min(minute.point_time for minute in withheld)
        reasons = tuple(sorted({reason for minute in withheld for reason in minute.reasons}))
        signature = (earliest, reasons)
        if self._withheld_logged.get(wallet) == signature:
            return
        self._withheld_logged[wallet] = signature
        logger.warning(
            "PortfolioPnlSnapshotter: wallet %s withheld %s planned minute(s) from %s; "
            "wrote_rows=%s reasons=%s",
            wallet,
            len(withheld),
            earliest,
            committed,
            reasons,
        )

    async def _plan_chunk_impl(
        self,
        request: _ChunkPlanRequest,
        *,
        advance_sequence: bool,
    ) -> PlannedPnlCorrection:
        """Plan each minute against exact financial versions with isolated sequence choice."""
        repo, ctx, result, physical_evidence = (
            request.repo,
            request.ctx,
            request.result,
            request.basis.evidence,
        )
        chunk = ChunkWindow(physical_evidence.start, physical_evidence.end)
        inputs = request.basis.inputs or await self._load_planning_inputs(repo, ctx, chunk)
        observations, valuation, positions = (
            inputs.observations,
            inputs.valuation,
            inputs.positions,
        )
        shadow_sequence = itertools.count(1)
        return plan_correction_chunk(
            PnlCorrectionPlanInputs(
                evidence=physical_evidence,
                minute_inputs=[
                    MinuteInputs(
                        point=point,
                        attempts=observations.get(point.point_time, {}),
                        evidence=valuation,
                    )
                    for point in result.points
                    if chunk.start <= point.point_time <= chunk.end
                ],
                expected_venues=ctx.expected_venues,
                position_versions=positions,
                make_row=lambda sample: self._to_row(
                    sample,
                    ctx,
                    _watermarks_json(result.replay_metadata, sample.point_time),
                    sequence_id=None if advance_sequence else next(shadow_sequence),
                ),
            )
        )

    async def _plan_chunk(
        self,
        repo: Repository,
        ctx: _ScopeContext,
        result: PnlWalletSeriesResult,
        basis: _PlanBasis,
    ) -> PlannedPnlCorrection:
        """Plan each minute against the exact financial versions and causal prefix read."""
        return await self._plan_chunk_impl(
            _ChunkPlanRequest(repo, ctx, result, basis),
            advance_sequence=True,
        )

    async def _plan_shadow_chunk(
        self,
        repo: Repository,
        ctx: _ScopeContext,
        result: PnlWalletSeriesResult,
        basis: _PlanBasis,
    ) -> PlannedPnlCorrection:
        """Plan a shadow comparison chunk without mutating ``_sequence`` or shared cursors."""
        return await self._plan_chunk_impl(
            _ChunkPlanRequest(repo, ctx, result, basis),
            advance_sequence=False,
        )

    async def _load_planning_inputs(
        self, repo: Repository, ctx: _ScopeContext, chunk: ChunkWindow
    ) -> _PlanningInputs:
        """Read one chunk's observation, valuation and position evidence in one pass."""
        observations, valuation = await self._load_chunk_evidence(repo, ctx, chunk)
        positions = await self._load_position_versions(repo, ctx, chunk)
        return _PlanningInputs(observations, valuation, positions)

    async def _load_position_versions(
        self, repo: Repository, ctx: _ScopeContext, chunk: ChunkWindow
    ) -> list[PositionVersion]:
        """Load the chunk's temporal position versions for the per-minute partition."""
        rows = await repo.get_pnl_scope_position_inventory_window(
            ctx.wallet_public_id, _LIVE_MODE, chunk.start, chunk.end
        )
        return [
            PositionVersion(
                entry=PositionInventoryEntry(
                    exchange=row["exchange"],
                    base_currency=row["base_currency"],
                    quantity=row["quantity"],
                    is_spot_margin=row["is_spot_margin"],
                ),
                valid_from=row["valid_from"],
                valid_to=row["valid_to"],
            )
            for row in rows
        ]

    async def _load_chunk_evidence(
        self,
        repo: Repository,
        ctx: _ScopeContext,
        chunk: ChunkWindow,
    ) -> tuple[
        dict[datetime, dict[str, VenueAccountObservationAttemptRow]],
        ValuationEvidence,
    ]:
        """Load the temporal basket per minute and the shared crypto price plane."""
        attempts = await repo.get_venue_account_observation_attempt_stream(
            ctx.wallet_public_id,
            sorted(ctx.expected_venues),
            _LIVE_MODE,
            chunk.start,
            chunk.end,
        )
        observations, currencies = _observation_grid(attempts, chunk)
        crypto_planes = await self._load_crypto_planes(repo, ctx, chunk, currencies)
        fiat_rates, fiat_venues, fiat_versions = await load_basket_fiat_evidence_by_minute(
            repo,
            frozenset(currencies),
            chunk.start,
            chunk.end,
            ctx.as_of,
        )
        evidence = ValuationEvidence(
            fiat_rates=fiat_rates,
            fiat_venues={},
            fiat_versions=fiat_versions,
            crypto_planes=crypto_planes,
            fiat_venues_by_minute=fiat_venues,
        )
        return observations, evidence

    async def _load_crypto_planes(
        self,
        repo: Repository,
        ctx: _ScopeContext,
        chunk: ChunkWindow,
        currencies: set[str],
    ) -> dict[tuple[str, datetime], list[CryptoUsdCandle]]:
        """Load and index the crypto→USD candle plane for the chunk's currencies."""
        if not currencies:
            return {}
        try:
            rows = await repo.get_pnl_crypto_usd_plane_candles(
                sorted(currencies), chunk.start - _MINUTE, chunk.end - _MINUTE, ctx.as_of
            )
        except DBAPIError as error:
            if getattr(error.orig, "sqlstate", None) in _CRYPTO_PLANE_LIMIT_SQLSTATES:
                self._log_class(
                    ctx.wallet_public_id, "crypto_plane_timeout", chunk.start, chunk.end
                )
            raise
        planes: dict[tuple[str, datetime], list[CryptoUsdCandle]] = {}
        for row in rows:
            minute = row["open_at"] + _MINUTE
            candle = CryptoUsdCandle(
                base=row["base"],
                quote=row["quote"],
                exchange=row["exchange"],
                native_symbol=row["native_symbol"],
                instrument_public_id=row["instrument_public_id"],
                candle_id=row["candle_id"],
                candle_public_id=row["candle_public_id"],
                candle_open_at=row["open_at"],
                candle_timestamp=row["candle_timestamp"],
                close=row["close"],
            )
            planes.setdefault((row["base"], minute), []).append(candle)
        return planes

    def _to_row(
        self,
        sample: PlannedSample,
        ctx: _ScopeContext,
        watermarks_json: str,
        *,
        sequence_id: int | None = None,
    ) -> PortfolioPnlSampleRow:
        """Stamp write-time provenance and watermarks onto a planned sample row."""
        if sequence_id is None:
            self._sequence += 1
            assigned_sequence = self._sequence
        else:
            assigned_sequence = sequence_id
        return {
            "public_id": str(uuid7()),
            "session_id": self._session_id,
            "sequence_id": assigned_sequence,
            "timestamp": ctx.as_of,
            "wallet_public_id": ctx.wallet_public_id,
            "mode": _LIVE_MODE,
            "valuation_ccy": _USD,
            "point_time": sample.point_time,
            "point_kind": "sample",
            "epoch_public_id": ctx.scope.epoch_public_id,
            "calc_version": PNL_SAMPLE_CALC_VERSION,
            "valuation_status": sample.valuation_status,
            "realized_pnl": sample.realized_pnl,
            "fee_pnl": sample.fee_pnl,
            "accrual_pnl": sample.accrual_pnl,
            "external_flow_adjustment": 0.0,
            "unrealized_pnl": sample.unrealized_pnl,
            "cash_usd": sample.cash_usd,
            "position_value_usd": sample.position_value_usd,
            "drawdown": sample.drawdown,
            "mark_source": sample.mark_source,
            "mark_time": sample.mark_time,
            "audit_json": sample.audit_json,
            "watermarks_json": watermarks_json,
        }


def _has_stationary_persistent_fill_gap(
    ctx: _ScopeContext,
    frontier: PnlCorrectionFrontier,
    pending: PnlSeriesReplayMetadata,
) -> bool:
    """Return whether settled frontier metadata and probe evidence remain identically gapped."""
    first = ctx.t0 + _MINUTE
    witness = pending.fill_witness_cut
    return (
        frontier.next_minute is None
        and frontier.window_start == first
        and frontier.repair_from == first
        and frontier.has_fill_gap is True
        and pending.has_fill_gap is True
        and frontier.raw_execution_watermarks is not None
        and pending.raw_execution_watermarks is not None
        and frontier.raw_execution_watermarks == pending.raw_execution_watermarks
        and frontier.execution_manifest_digest is not None
        and pending.execution_manifest_digest is not None
        and frontier.execution_manifest_digest == pending.execution_manifest_digest
        and frontier.expected_venues is not None
        and frontier.expected_venues == ctx.expected_venues
        and witness is not None
        and frontier.venue_event_watermarks is not None
        and frontier.venue_event_watermarks == witness["watermarks"]
        and frontier.fill_witness_digest is not None
        and frontier.fill_witness_digest == witness["digest"]
        and plan_late_fill_recompute(pending.earliest_affected_minute, frontier.tip) is None
    )


def _detection_watermarks(metadata: PnlSeriesReplayMetadata) -> Mapping[str, int]:
    """Use the certified durable prefix when a previous checkpoint was supplied."""
    if metadata.detection_watermarks is not None:
        return metadata.detection_watermarks
    return metadata.max_scope_sequence_by_exchange


def _watermarks_json(metadata: PnlSeriesReplayMetadata | None, minute: datetime) -> str:
    """Serialize the causal execution evidence stamped on one sample minute."""
    watermarks: Mapping[str, int] = {}
    if metadata is not None:
        by_minute = metadata.sample_watermarks_by_minute
        watermarks = (
            metadata.max_scope_sequence_by_exchange if by_minute is None else by_minute[minute]
        )
    return json.dumps(watermarks, allow_nan=False, separators=(",", ":"), sort_keys=True)


def _observation_grid(
    attempts: Sequence[VenueAccountObservationAttemptRow], chunk: ChunkWindow
) -> tuple[dict[datetime, dict[str, VenueAccountObservationAttemptRow]], set[str]]:
    """Fold the ordered attempt stream into one latest observation per exchange per minute."""
    stream = iter(attempts)
    upcoming = next(stream, None)
    latest: dict[str, VenueAccountObservationAttemptRow] = {}
    grid: dict[datetime, dict[str, VenueAccountObservationAttemptRow]] = {}
    currencies: set[str] = set()
    minute = chunk.start
    while minute <= chunk.end:
        while upcoming is not None and upcoming["timestamp"] <= minute:
            latest[upcoming["exchange"]] = upcoming
            upcoming = next(stream, None)
        grid[minute] = latest.copy()
        for attempt in latest.values():
            currencies.update(_observed_currencies(attempt))
        minute += _MINUTE
    return grid, currencies


def _observed_currencies(attempt: VenueAccountObservationAttemptRow) -> set[str]:
    """Best-effort collect the non-USD currencies one attempt reports.

    Used only to size the crypto price-plane load; a malformed or unobserved
    attempt contributes nothing and the planner still fails the minute closed.

    Args:
        attempt: One observation attempt row.

    Returns:
        The non-USD currency codes in the attempt's balances, or an empty set.
    """
    currencies = _observed_balance_currencies(attempt)
    currencies |= _observed_position_quote_currencies(attempt)
    return currencies


def _observed_balance_currencies(attempt: VenueAccountObservationAttemptRow) -> set[str]:
    """Collect valid non-USD balance currencies without certifying the payload."""
    balances_json = attempt["balances_json"] if attempt["balance_status"] == "observed" else None
    if balances_json is None:
        return set()
    try:
        payload = json.loads(balances_json)
    except ValueError, TypeError:
        return set()
    if not isinstance(payload, list):
        return set()
    currencies: set[str] = set()
    for entry in payload:
        if isinstance(entry, dict):
            currency = entry.get("currency")
            if isinstance(currency, str) and currency and currency != _USD:
                currencies.add(currency)
    return currencies


def _observed_position_quote_currencies(
    attempt: VenueAccountObservationAttemptRow,
) -> set[str]:
    """Collect canonical non-USD position quotes for evidence preloading."""
    positions_json = (
        attempt["open_positions_json"] if attempt["position_status"] == "observed" else None
    )
    if positions_json is None:
        return set()
    try:
        positions = json.loads(positions_json)
    except ValueError, TypeError:
        return set()
    if not isinstance(positions, list):
        return set()
    currencies: set[str] = set()
    for entry in positions:
        if not isinstance(entry, dict):
            continue
        symbol = entry.get("symbol")
        if not isinstance(symbol, str):
            continue
        parts = symbol.split("-")
        if len(parts) == 2 and parts[1] != _USD:
            currencies.add(parts[1])
    return currencies


def _extract_reason_codes(audit_json: str) -> frozenset[str]:
    """Extract the reason codes from a persisted sample's audit JSON, verbatim.

    Tokens are returned exactly as persisted, never narrowed to a canonical
    member: a row this binary did not write may carry a code a newer writer
    introduced, and the eligibility decision belongs to
    :func:`plan_self_heal_minutes`' FINAL deny-list, not to this reader. Mapping
    an unrecognised token onto a terminal code here would silently strand every
    minute a newer writer produced.

    Args:
        audit_json: The ``incomplete`` sample's audit envelope.

    Returns:
        The reason codes recorded on the sample, empty on a missing or malformed
        envelope (the planner never persisted such a row, so it never self-heals).
    """
    try:
        payload = json.loads(audit_json)
    except ValueError, TypeError:
        return frozenset()
    if not isinstance(payload, dict):
        return frozenset()
    raw = payload.get("reason_codes", [])
    if not isinstance(raw, list):
        return frozenset()
    return frozenset(code for code in raw if isinstance(code, str))
