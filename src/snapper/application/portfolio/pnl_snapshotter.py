"""PortfolioPnlSnapshotter — async Phase-5B equity/drawdown sample writer.

Owns ONE :class:`Repository` for its lifetime and mirrors
:class:`DbStatsSnapshotter` exactly: an interruptible pre-sleep loop, an
``app.state`` pre-init contract, a disabled mode that constructs but parks, and a
symmetric reverse-order stop. Single-writer safety comes from the instance-0
lifespan gate plus the sample DAL's per-scope advisory lock.

Per tick the snapshotter discovers every scope that already holds an ACTIVE USD
live activation anchor (decision R7 — it NEVER creates one), resolves the scope's
expected SPOT venue set (active wallet credentials minus paper minus futures-class
venues, decision R11), and for each scope:

  1. reads the durable progress (the last active sample);
  2. runs the EXISTING 5A series engine over the finalized catch-up window in
     budget-bounded chunks (decision D1/R5), passing the in-memory baseline
     watermark map so the engine reports the late-fill boundary (R3);
  3. loads the temporal observation basket and the crypto→USD price evidence;
  4. hands the pure planner every typed input and persists its rows through
     :meth:`Repository.record_portfolio_pnl_samples`;
  5. recomputes-forward and supersedes any minute a late fill invalidated (D9),
     and self-heals retryable ``incomplete`` minutes inside the 15-minute
     lookback (D10).

Every scope is isolated: one scope's failure logs at most once per failing streak
and never aborts the tick, and a work-budget overflow subdivides the chunk instead
of crash-looping.

The full basket prices off the crypto→USD plane, the USD identity AND the fiat
forex plane (walutomat EUR-PLN and any fiat leg), and the position/cash partition
uses the temporal, spec-proven spot inventory active AT EACH minute. The durable
cold-start baseline is each sample's persisted per-exchange watermark map, so a
restart neither rewrites history nor misses a late fill that landed during
downtime. Every correction (late fill or self-heal) recomputes forward from the
earliest affected minute through the tip under an optimistic supersede CAS, and a
batch conflict or a lost CAS leaves the durable progress untouched for a clean
retry next tick.

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
import json
import logging
import os
from collections import deque
from collections.abc import Callable
from collections.abc import Mapping
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from typing import Final
from typing import cast
from uuid import uuid7

from snapper.application.portfolio.basket_valuation import CryptoUsdCandle
from snapper.application.portfolio.basket_valuation import PositionInventoryEntry
from snapper.application.portfolio.basket_valuation import ValuationEvidence
from snapper.application.portfolio.pnl_snapshot_planner import SELF_HEAL_LOOKBACK
from snapper.application.portfolio.pnl_snapshot_planner import ChunkPlan
from snapper.application.portfolio.pnl_snapshot_planner import ChunkWindow
from snapper.application.portfolio.pnl_snapshot_planner import MinuteInputs
from snapper.application.portfolio.pnl_snapshot_planner import PlannedSample
from snapper.application.portfolio.pnl_snapshot_planner import PositionVersion
from snapper.application.portfolio.pnl_snapshot_planner import SelfHealCandidate
from snapper.application.portfolio.pnl_snapshot_planner import plan_catchup_chunks
from snapper.application.portfolio.pnl_snapshot_planner import plan_catchup_window
from snapper.application.portfolio.pnl_snapshot_planner import plan_chunk_samples
from snapper.application.portfolio.pnl_snapshot_planner import plan_late_fill_recompute
from snapper.application.portfolio.pnl_snapshot_planner import plan_self_heal_minutes
from snapper.application.portfolio.pnl_snapshotter_config import ENABLED_ENV_VAR
from snapper.application.portfolio.pnl_snapshotter_config import INTERVAL_ENV_VAR
from snapper.application.portfolio.pnl_snapshotter_config import resolve_enabled
from snapper.application.portfolio.pnl_snapshotter_config import resolve_interval
from snapper.application.portfolio.pnl_timeline_service import PNL_TIMELINE_MAX_WORK_UNITS
from snapper.application.portfolio.pnl_timeline_service import PnlSeriesReadPolicy
from snapper.application.portfolio.pnl_timeline_service import PnlSeriesReplayMetadata
from snapper.application.portfolio.pnl_timeline_service import PnlSeriesReplayOptions
from snapper.application.portfolio.pnl_timeline_service import PnlTimelineWorkBudgetError
from snapper.application.portfolio.pnl_timeline_service import PnlWalletSeriesResult
from snapper.application.portfolio.pnl_timeline_service import build_wallet_pnl_series
from snapper.application.portfolio.pnl_timeline_service import load_basket_fiat_evidence
from snapper.data.repository import PortfolioPnlSampleConflictError
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

_LIVE_MODE: Final = "live"
_USD: Final[str] = "USD"
_FUTURES_POSITION_METHOD: Final[str] = "futures_position"
_PAPER_CREDENTIAL_TYPE: Final[str] = "paper"
_MINUTE: Final[timedelta] = timedelta(minutes=1)
_POOL_KEY_ESTIMATE: Final[int] = 1
"""Chunk-sizing pool estimate; the work-budget split is the true safety net."""

_FAILURE_MESSAGES: Final[dict[str, str]] = {
    "sample_failed": "PortfolioPnlSnapshotter: wallet %s sample failed; continuing",
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
) -> dict[str, frozenset[str]]:
    """Group active credentials into each wallet's expected SPOT venue set (R11).

    A credential contributes its exchange to its wallet's denominator only when it
    is neither a paper credential nor a futures-class venue. A wallet whose every
    credential is paper or futures resolves to an empty set (unsupported).

    Args:
        credentials: Active wallet credential rows at the tick horizon.

    Returns:
        Every seen wallet mapped to its (possibly empty) spot venue set.
    """
    grouped: dict[str, set[str]] = {}
    for credential in credentials:
        wallet = credential["wallet_public_id"]
        grouped.setdefault(wallet, set())
        if credential["credential_type"] == _PAPER_CREDENTIAL_TYPE:
            continue
        exchange = credential["exchange"]
        if is_futures_class_exchange(exchange):
            continue
        grouped[wallet].add(exchange)
    return {wallet: frozenset(venues) for wallet, venues in grouped.items()}


@dataclass(frozen=True, slots=True)
class _ScopeContext:
    """The immutable per-scope binding one tick threads through its helpers."""

    wallet_public_id: str
    scope: PortfolioPnlSampleScope
    query: PortfolioPnlSampleQuery
    t0: datetime
    expected_venues: frozenset[str]
    as_of: datetime
    last_minute: datetime | None
    durable_baseline: dict[str, int]


@dataclass(frozen=True, slots=True)
class _Catchup:
    """One tick's held catch-up engine runs and their advanced watermark."""

    chunks: list[tuple[ChunkWindow, PnlWalletSeriesResult]]
    metadata: PnlSeriesReplayMetadata | None


class PortfolioPnlSnapshotter:
    """Async Phase-5B equity/drawdown sample writer with disabled-mode support."""

    def __init__(
        self,
        *,
        repo: Repository | None,
        interval_seconds: int | None = None,
        disabled: bool | None = None,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        """Wire dependencies.

        Args:
            repo: Async :class:`Repository`. ``None`` when parked in disabled mode.
            interval_seconds: Loop period. ``None`` reads
                ``PNL_SNAPSHOTTER_INTERVAL_SECONDS`` (default 60).
            disabled: Disable override; ``None`` reads ``PNL_SNAPSHOTTER_ENABLED``
                (default disabled).
            clock: Injectable UTC ``now`` for deterministic tests.
        """
        if interval_seconds is None:
            interval_seconds = resolve_interval(os.environ.get(INTERVAL_ENV_VAR))
        if disabled is None:
            disabled = not resolve_enabled(os.environ.get(ENABLED_ENV_VAR))
        self._repo = repo if not disabled else None
        self._interval_seconds = interval_seconds
        self._disabled = disabled
        self._clock: Callable[[], datetime] = clock or (lambda: datetime.now(UTC))
        self._stopping = asyncio.Event()
        self._loop_task: asyncio.Task[None] | None = None
        self._session_id = str(uuid7())
        self._sequence = 0
        self._baselines: dict[str, dict[str, int]] = {}
        self._unsupported_logged: set[str] = set()
        self._no_anchor_logged: set[str] = set()
        self._failure_logged: set[tuple[str, str]] = set()
        self._self_heal_logged: dict[str, tuple[datetime, tuple[str, ...]]] = {}

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
        credentials = await repo.list_active_wallet_credentials(as_of)
        venues_by_wallet = resolve_spot_venues(credentials)
        self._prune_absent_scopes(set(venues_by_wallet))
        failures = 0
        for wallet in sorted(venues_by_wallet):
            try:
                conflicted = await self._process_wallet(
                    repo, wallet, venues_by_wallet[wallet], as_of
                )
            except Exception:
                failures += 1
                self._log_class(wallet, "sample_failed", is_exception=True)
                continue
            if conflicted:
                failures += 1
            else:
                self._reset_scope_logs(wallet)
        if failures:
            logger.warning(
                "PortfolioPnlSnapshotter: %s scope(s) failed this tick; continuing", failures
            )

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
        self._unsupported_logged.intersection_update(current_wallets)
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
        self._baselines = {
            key: value
            for key, value in self._baselines.items()
            if key.split("|", 1)[0] in current_wallets
        }

    async def _process_wallet(
        self,
        repo: Repository,
        wallet: str,
        expected_venues: frozenset[str],
        as_of: datetime,
    ) -> bool:
        """Resolve one wallet's anchored USD scope and sample it, or skip honestly.

        Returns whether the scope hit a reconcile or catch-up conflict this tick
        (so the tick counts it toward the aggregate and never resets its streak).
        """
        anchor = await repo.get_portfolio_pnl_anchor(wallet, _LIVE_MODE, _USD, None)
        if anchor is None:
            self._log_once(self._no_anchor_logged, wallet, "no active USD anchor; skipping")
            return False
        if not expected_venues:
            self._log_once(
                self._unsupported_logged, wallet, "no spot venues (futures-only); unsupported"
            )
            return False
        ctx = await self._build_scope_context(repo, wallet, anchor, expected_venues, as_of)
        return await self._sample_scope(repo, ctx)

    async def _build_scope_context(
        self,
        repo: Repository,
        wallet: str,
        anchor: PortfolioPnlAnchorRow,
        expected_venues: frozenset[str],
        as_of: datetime,
    ) -> _ScopeContext:
        """Assemble the immutable per-scope binding, reading durable progress."""
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
        latest = await repo.get_latest_portfolio_pnl_sample(query)
        last_minute = latest["point_time"] if latest is not None else None
        durable_baseline = _parse_baseline(latest["watermarks_json"]) if latest is not None else {}
        return _ScopeContext(
            wallet_public_id=wallet,
            scope=scope,
            query=query,
            t0=t0,
            expected_venues=expected_venues,
            as_of=as_of,
            last_minute=last_minute,
            durable_baseline=durable_baseline,
        )

    def _log_once(self, seen: set[str], wallet: str, detail: str) -> None:
        """Log one wallet-scoped lifecycle notice at most once per process."""
        if wallet in seen:
            return
        seen.add(wallet)
        logger.info("PortfolioPnlSnapshotter: wallet %s %s", wallet, detail)

    def _scope_key(self, ctx: _ScopeContext) -> str:
        """Return the in-memory baseline key for one scope."""
        return "|".join((ctx.wallet_public_id, _LIVE_MODE, _USD, ctx.query.epoch_public_id))

    async def _sample_scope(self, repo: Repository, ctx: _ScopeContext) -> bool:
        """Catch up and recompute-forward any late-fill or self-heal correction.

        The durable baseline (the last sample's persisted watermark map, cached in
        memory) drives late-fill detection, so a restart no longer treats every
        post-anchor execution as a late fill (B2). Any correction — a late fill or
        a healed retryable minute — recomputes FORWARD from the earliest affected
        minute through the tip (A1). If ANY reconcile conflict occurs (a lost
        supersede/retract CAS or a restored-minute insert conflict) the scope ABORTS
        BEFORE catch-up (decision C1): no catch-up rows are written and the durable
        baseline is left untouched, so the whole flow retries from durable state
        next tick and the late fill is not silently skipped. Catch-up minutes are
        inserted last so their causal peak reflects the corrections.

        Returns:
            Whether this scope hit a reconcile or catch-up conflict this tick.
        """
        scope_key = self._scope_key(ctx)
        baseline = self._baselines.get(scope_key, ctx.durable_baseline)
        window = plan_catchup_window(ctx.as_of, ctx.last_minute, ctx.t0)
        catchup = (
            await self._compute_catchup(repo, ctx, window, baseline) if window is not None else None
        )
        recompute_from = await self._recompute_start(repo, ctx, catchup)
        if recompute_from is not None and ctx.last_minute is not None:
            if await self._recompute_forward(repo, ctx, recompute_from, ctx.last_minute, baseline):
                return True
        if catchup is not None:
            return await self._insert_catchup(repo, ctx, scope_key, catchup)
        return False

    async def _compute_catchup(
        self,
        repo: Repository,
        ctx: _ScopeContext,
        window: ChunkWindow,
        baseline: Mapping[str, int],
    ) -> _Catchup:
        """Run the engine over the catch-up window, holding points for late planning.

        Points are held (not yet planned) so their causal peak is read AFTER any
        recompute-forward commits. Budget overflows subdivide the chunk.
        """
        pending: deque[ChunkWindow] = deque(
            plan_catchup_chunks(window, _POOL_KEY_ESTIMATE, work_budget=PNL_TIMELINE_MAX_WORK_UNITS)
        )
        chunks: list[tuple[ChunkWindow, PnlWalletSeriesResult]] = []
        metadata: PnlSeriesReplayMetadata | None = None
        while pending:
            chunk = pending.popleft()
            try:
                result = await self._series(repo, ctx, chunk, baseline)
            except PnlTimelineWorkBudgetError:
                pending.extendleft(reversed(self._split_chunk(chunk)))
                continue
            chunks.append((chunk, result))
            if result.replay_metadata is not None:
                metadata = result.replay_metadata
        return _Catchup(chunks=chunks, metadata=metadata)

    async def _recompute_start(
        self, repo: Repository, ctx: _ScopeContext, catchup: _Catchup | None
    ) -> datetime | None:
        """Return the earliest minute a late fill or self-heal must recompute from."""
        earliest_affected = (
            catchup.metadata.earliest_affected_minute
            if catchup is not None and catchup.metadata is not None
            else None
        )
        late_start = plan_late_fill_recompute(earliest_affected, ctx.last_minute)
        self_heal_start = await self._self_heal_start(repo, ctx)
        candidates = [start for start in (late_start, self_heal_start) if start is not None]
        return min(candidates) if candidates else None

    async def _self_heal_start(self, repo: Repository, ctx: _ScopeContext) -> datetime | None:
        """Return the earliest retryable incomplete minute in the lookback (D10)."""
        if ctx.last_minute is None:
            return None
        cutoff = ctx.as_of.replace(second=0, microsecond=0) - SELF_HEAL_LOOKBACK
        incompletes = await repo.get_portfolio_pnl_samples(
            ctx.query, cutoff, ctx.last_minute, status="incomplete"
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
        baseline: Mapping[str, int],
    ) -> PnlWalletSeriesResult:
        """Run the 5A engine over one chunk with the R3 baseline watermark map."""
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
                current_truth_horizon=True,
            ),
            options=PnlSeriesReplayOptions(baseline_watermarks=dict(baseline)),
        )

    async def _recompute_forward(
        self,
        repo: Repository,
        ctx: _ScopeContext,
        start: datetime,
        tip: datetime,
        baseline: Mapping[str, int],
    ) -> bool:
        """Recompute ``[start .. tip]`` and reconcile the changed minutes under CAS.

        A changed equity at ``start`` shifts the causal peak of every later minute,
        so the whole suffix through the tip is recomputed. On the FIRST reconcile
        conflict the recompute STOPS (leaving the tip's watermark untouched) and
        returns ``True`` so the scope skips catch-up and retries next tick (C1).

        Returns:
            Whether any chunk hit a reconcile conflict.
        """
        current = {
            row["point_time"]: row
            for row in await repo.get_portfolio_pnl_samples(ctx.query, start, tip)
        }
        pending: deque[ChunkWindow] = deque(
            plan_catchup_chunks(
                ChunkWindow(start=start, end=tip),
                _POOL_KEY_ESTIMATE,
                work_budget=PNL_TIMELINE_MAX_WORK_UNITS,
            )
        )
        while pending:
            chunk = pending.popleft()
            try:
                result = await self._series(repo, ctx, chunk, baseline)
            except PnlTimelineWorkBudgetError:
                pending.extendleft(reversed(self._split_chunk(chunk)))
                continue
            if await self._reconcile_changed(repo, ctx, result, chunk, current):
                return True
        return False

    async def _reconcile_changed(
        self,
        repo: Repository,
        ctx: _ScopeContext,
        result: PnlWalletSeriesResult,
        chunk: ChunkWindow,
        current: Mapping[datetime, PortfolioPnlSampleRow],
    ) -> bool:
        """Reconcile one recomputed chunk against its active rows under CAS.

        A full three-way merge over the recompute window: a planned minute absent
        from the persisted rows is INSERTED (a formerly-untrusted minute that a
        late execution restored — catch-up can never revisit it, decision P1); a
        planned minute that changed is superseded; a persisted minute the recompute
        no longer produces is RETRACTED so no stale monetary row keeps serving or
        contaminating the causal peak (N1); an identical recompute is skipped. The
        FIRST conflict (lost CAS or batch conflict) aborts and returns ``True`` (C1).

        Returns:
            Whether a reconcile conflict occurred.
        """
        watermarks_json = _watermarks_json(result.replay_metadata)
        prior_peak = await repo.get_portfolio_pnl_sample_peak(ctx.query, chunk.start)
        plan = await self._plan_chunk(repo, ctx, result, chunk, prior_peak)
        planned = {sample.point_time: sample for sample in plan.samples}
        for point_time, existing in sorted(current.items()):
            if not chunk.start <= point_time <= chunk.end:
                continue
            sample = planned.get(point_time)
            if sample is None:
                if await self._retract(repo, ctx, point_time, existing):
                    return True
                continue
            replacement = self._to_row(sample, ctx, watermarks_json)
            if _sample_values_equal(existing, replacement):
                continue
            if await self._supersede(repo, ctx, replacement, existing):
                return True
        inserts = [
            self._to_row(sample, ctx, watermarks_json)
            for point_time, sample in sorted(planned.items())
            if point_time not in current
        ]
        return bool(inserts) and await self._record_recompute_inserts(repo, ctx, inserts)

    async def _record_recompute_inserts(
        self, repo: Repository, ctx: _ScopeContext, rows: list[PortfolioPnlSampleRow]
    ) -> bool:
        """Insert the restored minutes via the batch writer; report any conflict (P1/B1)."""
        outcome = await repo.record_portfolio_pnl_samples(rows, ctx.scope)
        if outcome["conflicts"]:
            self._log_class(ctx.wallet_public_id, "recompute_insert", outcome["conflicts"])
            return True
        return False

    async def _supersede(
        self,
        repo: Repository,
        ctx: _ScopeContext,
        replacement: PortfolioPnlSampleRow,
        existing: PortfolioPnlSampleRow,
    ) -> bool:
        """CAS-supersede one changed minute; report a lost CAS for retry (B3/C1).

        Returns:
            Whether the optimistic CAS was lost.
        """
        try:
            await repo.supersede_portfolio_pnl_sample(
                ctx.scope,
                replacement,
                derived_suffix_reconciliation=True,
                expected_public_id=existing["public_id"],
            )
            return False
        except PortfolioPnlSampleConflictError:
            self._log_class(ctx.wallet_public_id, "supersede_cas", replacement["point_time"])
            return True

    async def _retract(
        self,
        repo: Repository,
        ctx: _ScopeContext,
        point_time: datetime,
        existing: PortfolioPnlSampleRow,
    ) -> bool:
        """CAS-retract one now-untrusted minute; report a lost CAS for retry (N1/C1).

        Returns:
            Whether the optimistic CAS was lost.
        """
        try:
            await repo.retract_portfolio_pnl_sample(
                ctx.scope,
                point_time,
                expected_public_id=existing["public_id"],
                bus_time=ctx.as_of,
            )
            return False
        except PortfolioPnlSampleConflictError:
            self._log_class(ctx.wallet_public_id, "retract_cas", point_time)
            return True

    async def _insert_catchup(
        self, repo: Repository, ctx: _ScopeContext, scope_key: str, catchup: _Catchup
    ) -> bool:
        """Plan and insert the held catch-up chunks; advance baseline if conflict-free.

        STOPS at the FIRST batch conflict — no LATER chunk may be written once any
        chunk conflicts, so durable progress never crosses a gap (a chunk N
        conflict must not let chunk N+1 commit its advanced watermark). The scope
        retries next tick and re-plans from the last contiguous committed chunk.
        The baseline advances only when every chunk committed cleanly (B1/C1).

        Returns:
            Whether a chunk's batch write reported a conflict.
        """
        for chunk, result in catchup.chunks:
            watermarks_json = _watermarks_json(result.replay_metadata)
            prior_peak = await repo.get_portfolio_pnl_sample_peak(ctx.query, chunk.start)
            plan = await self._plan_chunk(repo, ctx, result, chunk, prior_peak)
            rows = [self._to_row(sample, ctx, watermarks_json) for sample in plan.samples]
            if not rows:
                continue
            outcome = await repo.record_portfolio_pnl_samples(rows, ctx.scope)
            if outcome["conflicts"]:
                self._log_class(ctx.wallet_public_id, "catchup_conflict", outcome["conflicts"])
                return True
        self._baselines[scope_key] = (
            catchup.metadata.max_scope_sequence_by_exchange if catchup.metadata is not None else {}
        )
        return False

    async def _plan_chunk(
        self,
        repo: Repository,
        ctx: _ScopeContext,
        result: PnlWalletSeriesResult,
        chunk: ChunkWindow,
        prior_peak: float | None,
    ) -> ChunkPlan:
        """Load a chunk's observation, price and position evidence and plan it."""
        observations, evidence = await self._load_chunk_evidence(repo, ctx, chunk)
        position_versions = await self._load_position_versions(repo, ctx, chunk)
        minute_inputs = [
            MinuteInputs(
                point=point,
                attempts=observations.get(point.point_time, {}),
                evidence=evidence,
            )
            for point in result.points
            if chunk.start <= point.point_time <= chunk.end
        ]
        return plan_chunk_samples(minute_inputs, ctx.expected_venues, position_versions, prior_peak)

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
        exchanges = sorted(ctx.expected_venues)
        observations: dict[datetime, dict[str, VenueAccountObservationAttemptRow]] = {}
        currencies: set[str] = set()
        minute = chunk.start
        while minute <= chunk.end:
            attempts = await repo.get_venue_account_observation_attempts_at(
                ctx.wallet_public_id, exchanges, _LIVE_MODE, minute
            )
            observations[minute] = attempts
            for attempt in attempts.values():
                currencies |= _observed_currencies(attempt)
            minute += _MINUTE
        crypto_planes = await self._load_crypto_planes(repo, ctx, chunk, currencies)
        fiat_rates, fiat_venues, fiat_versions = await load_basket_fiat_evidence(
            repo, frozenset(currencies), chunk.start, chunk.end, ctx.as_of
        )
        evidence = ValuationEvidence(
            fiat_rates=fiat_rates,
            fiat_venues=fiat_venues,
            fiat_versions=fiat_versions,
            crypto_planes=crypto_planes,
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
        rows = await repo.get_pnl_crypto_usd_plane_candles(
            sorted(currencies), chunk.start - _MINUTE, chunk.end - _MINUTE, ctx.as_of
        )
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
        self, sample: PlannedSample, ctx: _ScopeContext, watermarks_json: str
    ) -> PortfolioPnlSampleRow:
        """Stamp write-time provenance and watermarks onto a planned sample row."""
        self._sequence += 1
        return {
            "public_id": str(uuid7()),
            "session_id": self._session_id,
            "sequence_id": self._sequence,
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


_SAMPLE_VALUE_KEYS: Final[tuple[str, ...]] = (
    "valuation_status",
    "realized_pnl",
    "fee_pnl",
    "accrual_pnl",
    "external_flow_adjustment",
    "unrealized_pnl",
    "cash_usd",
    "position_value_usd",
    "drawdown",
    "mark_source",
    "mark_time",
    "audit_json",
    "watermarks_json",
)
"""The value columns that decide whether a recompute actually changed a sample."""


def _watermarks_json(metadata: PnlSeriesReplayMetadata | None) -> str:
    """Serialize one run's advanced per-exchange watermark map canonically (B2)."""
    watermarks = metadata.max_scope_sequence_by_exchange if metadata is not None else {}
    return json.dumps(watermarks, allow_nan=False, separators=(",", ":"), sort_keys=True)


def _parse_baseline(watermarks_json: str) -> dict[str, int]:
    """Parse a persisted watermark map into the durable cold-start baseline (B2).

    A malformed or non-map payload yields an empty baseline (fail-safe: the tick
    then treats the whole suffix as potentially late, all idempotent).

    Args:
        watermarks_json: The latest active sample's persisted watermark map.

    Returns:
        The per-exchange watermark map, empty on any parse fault.
    """
    try:
        payload = json.loads(watermarks_json)
    except ValueError, TypeError:
        return {}
    if not isinstance(payload, dict):
        return {}
    return {
        exchange: sequence
        for exchange, sequence in payload.items()
        if isinstance(exchange, str)
        and isinstance(sequence, int)
        and not isinstance(sequence, bool)
    }


def _sample_values_equal(
    existing: PortfolioPnlSampleRow, replacement: PortfolioPnlSampleRow
) -> bool:
    """Return whether a recompute produced a byte-identical value for one minute."""
    left = cast(Mapping[str, object], existing)
    right = cast(Mapping[str, object], replacement)
    return all(left[key] == right[key] for key in _SAMPLE_VALUE_KEYS)


def _observed_currencies(attempt: VenueAccountObservationAttemptRow) -> set[str]:
    """Best-effort collect the non-USD currencies one attempt reports.

    Used only to size the crypto price-plane load; a malformed or unobserved
    attempt contributes nothing and the planner still fails the minute closed.

    Args:
        attempt: One observation attempt row.

    Returns:
        The non-USD currency codes in the attempt's balances, or an empty set.
    """
    if attempt["balance_status"] != "observed":
        return set()
    balances_json = attempt["balances_json"]
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
