"""DbStatsSnapshotter — async per-table row-count sampler.

Owns ONE :class:`Repository` (async) for the lifetime of the
snapshotter. Per tick:

  1. Iterate :data:`TABLES_TO_SAMPLE` (STATE-first, then EVENT, both
     alphabetical) calling :meth:`Repository.count_table_stats` per
     table.
  2. Per-table query is wrapped in :func:`asyncio.wait_for` with the
     module-level ``PER_TABLE_TIMEOUT_SECONDS`` budget. On timeout or
     exception the snapshotter clones the prior :class:`TableStats`
     with ``is_stale=True`` (or emits null counters when no prior
     sample exists). Per-table failures NEVER abort the tick.
  3. Atomically swap ``_latest_snapshot`` once all tables are visited.

Disabled mode mirrors :class:`RetentionScheduler` exactly: the
snapshotter is still constructed and assigned to
``app.state.db_stats_snapshotter`` so the metrics route can distinguish
disabled-via-env from missing-init from no-sample-yet, but the
underlying repo is ``None`` and :meth:`start` / :meth:`stop` are
no-ops. Operators with PostgreSQL deployments lacking ``psycopg2`` in
deps therefore boot cleanly when ``DB_METRICS_DISABLED=true``.

Configuration: ``DB_METRICS_INTERVAL_SECONDS`` (default 60),
``DB_METRICS_DISABLED`` (default false). Read directly from
``os.environ.get(...)``. Per-table timeout is a module constant
(not env-configurable for v1).
"""

import asyncio
import contextlib
import logging
import os
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC
from datetime import date
from datetime import datetime
from typing import Final
from typing import Literal

from sqlalchemy.orm import DeclarativeBase

from snapper.application.retention.policies import RETENTION_POLICIES
from snapper.application.retention.window import compute_retention_window
from snapper.data.archiver import EVENT_TABLES
from snapper.data.archiver import STATE_TABLES
from snapper.data.db_stats_types import TableEntry
from snapper.data.models import Candle
from snapper.data.repository import Repository

logger = logging.getLogger(__name__)

DEFAULT_INTERVAL_SECONDS: Final[int] = 60
INTERVAL_MIN_SECONDS: Final[int] = 10
INTERVAL_MAX_SECONDS: Final[int] = 3600
PER_TABLE_TIMEOUT_SECONDS: Final[float] = 30.0
_INTERVAL_ENV_VAR: Final[str] = "DB_METRICS_INTERVAL_SECONDS"
_DISABLED_ENV_VAR: Final[str] = "DB_METRICS_DISABLED"
_TRUTHY_ENV_VALUES: Final[frozenset[str]] = frozenset({"1", "true", "yes"})
ENV_VARS: Final[frozenset[str]] = frozenset({_INTERVAL_ENV_VAR, _DISABLED_ENV_VAR})
"""Public allowlist of env vars db_stats reads via ``os.environ``.

Consumed by :mod:`snapper.config.env_contract` to validate ``.env`` keys
against the union of every subsystem's contract.
"""


def resolve_interval(env_value: str | None) -> int:
    """Coerce ``DB_METRICS_INTERVAL_SECONDS`` to an int in [10, 3600] seconds.

    Empty / unset values fall back to :data:`DEFAULT_INTERVAL_SECONDS`
    (60). Anything else that parses as an integer outside the
    [``INTERVAL_MIN_SECONDS``, ``INTERVAL_MAX_SECONDS``] range raises
    :class:`ValueError` so a malformed env var fails loud at lifespan
    startup (the helper catches the exception and the route falls
    through to 503 ``not initialized``).

    Args:
        env_value: Raw env-var value, or ``None`` when unset.

    Returns:
        Loop interval in seconds.

    Raises:
        ValueError: When the raw value is not parseable as an integer
            or parses outside the supported range.
    """
    if env_value is None or env_value.strip() == "":
        return DEFAULT_INTERVAL_SECONDS
    try:
        value = int(env_value.strip())
    except ValueError as exc:
        raise ValueError(f"DB_METRICS_INTERVAL_SECONDS={env_value!r} is not an integer") from exc
    if value < INTERVAL_MIN_SECONDS or value > INTERVAL_MAX_SECONDS:
        raise ValueError(
            "DB_METRICS_INTERVAL_SECONDS="
            f"{value} out of range [{INTERVAL_MIN_SECONDS}, {INTERVAL_MAX_SECONDS}]"
        )
    return value


def resolve_disabled(env_value: str | None) -> bool:
    """Parse ``DB_METRICS_DISABLED`` as a boolean.

    Truthy values: ``"1"``, ``"true"``, ``"yes"`` (case-insensitive).
    Everything else (including ``None``) is ``False``.

    Args:
        env_value: Raw env-var value, or ``None`` when unset.

    Returns:
        ``True`` iff the snapshotter should park (no DB queries, no
        loop). ``False`` otherwise.
    """
    if env_value is None:
        return False
    return env_value.strip().lower() in _TRUTHY_ENV_VALUES


_STATE_MODELS: Final[dict[str, type[DeclarativeBase]]] = {
    **{name: spec.model for name, spec in STATE_TABLES.items()},
    "candles": Candle,
}
"""State-kind tables to sample: the archiver registry plus ``candles``.

``candles`` deliberately lives outside the archiver's ``STATE_TABLES`` /
``EVENT_TABLES`` registries — those dicts also drive archive/purge
behavior and candles has a bespoke per-instrument export path — but
operators still need its row counts on the #health dashboard. It samples
as ``state``: the model is SCD2-versioned via ``TemporalMixin``, so
``current`` / ``closed`` derive from ``known_to``.
"""

_STATE_CURRENT_ESTIMATE_INDEXES: Final[dict[str, str]] = {"candles": "uq_candle_itf_open"}
"""State tables whose PostgreSQL ``current`` count uses active-index stats.

The named index must contain exactly the active SCD2 rows. SQLite keeps
the exact ``known_to`` current count for every state table.
"""

TABLES_TO_SAMPLE: Final[tuple[TableEntry, ...]] = (
    *(
        TableEntry(
            name=name,
            kind="state",
            model=model,
            current_estimate_index=_STATE_CURRENT_ESTIMATE_INDEXES.get(name),
        )
        for name, model in sorted(_STATE_MODELS.items())
    ),
    *(
        TableEntry(name=name, kind="event", model=spec.model)
        for name, spec in sorted(EVENT_TABLES.items())
    ),
)


@dataclass(frozen=True, slots=True)
class TableStats:
    """One row in :class:`DbStatsSnapshot.tables`.

    ``total`` is dialect-aware: on PostgreSQL it is a planner ESTIMATE
    (``pg_class.reltuples``, accurate within autovacuum drift), on
    SQLite an exact count. ``current`` and ``archivable`` are exact
    except for explicit PostgreSQL active-index current estimates such
    as ``candles``. ``closed`` on state tables is derived after
    clamping ``total`` no lower than ``current``. Consumers must not
    treat PostgreSQL estimated axes as exact.
    """

    table: str
    table_kind: Literal["event", "state"]
    total: int | None
    current: int | None
    closed: int | None
    archivable: int | None
    is_stale: bool
    last_sampled_at: datetime


@dataclass(frozen=True, slots=True)
class DbStatsSnapshot:
    """Atomic per-table row-count snapshot published by the sampler."""

    snapshot_started_at: datetime
    snapshot_completed_at: datetime
    interval_seconds: int
    tables: tuple[TableStats, ...]

    def find(self, name: str) -> TableStats | None:
        """Return the :class:`TableStats` for ``name``, or ``None``.

        Args:
            name: Table name to look up.

        Returns:
            The matching :class:`TableStats` row, or ``None`` when the
            snapshot does not contain that table.
        """
        for table in self.tables:
            if table.table == name:
                return table
        return None


class DbStatsSnapshotter:
    """Async per-table row-count sampler with disabled-mode support."""

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
            repo: Async :class:`Repository`. ``None`` when the
                snapshotter is parked in disabled mode.
            interval_seconds: Loop period. ``None`` reads
                ``DB_METRICS_INTERVAL_SECONDS`` (default 60).
            disabled: Disable flag override; ``None`` reads
                ``DB_METRICS_DISABLED``.
            clock: Injectable UTC ``now`` for deterministic tests.
                Defaults to ``lambda: datetime.now(UTC)``.
        """
        if interval_seconds is None:
            interval_seconds = resolve_interval(os.environ.get(_INTERVAL_ENV_VAR))
        if disabled is None:
            disabled = resolve_disabled(os.environ.get(_DISABLED_ENV_VAR))
        self._repo = repo if not disabled else None
        self._interval_seconds = interval_seconds
        self._disabled = disabled
        self._clock: Callable[[], datetime] = clock or (lambda: datetime.now(UTC))
        self._latest_snapshot: DbStatsSnapshot | None = None
        self._stopping = asyncio.Event()
        self._loop_task: asyncio.Task[None] | None = None

    @property
    def disabled(self) -> bool:
        """Return whether the snapshotter is parked in disabled mode.

        Returns:
            ``True`` iff ``DB_METRICS_DISABLED`` was truthy at
            construction time; the loop is skipped and the metrics
            route reports the disabled detail.
        """
        return self._disabled

    @property
    def interval_seconds(self) -> int:
        """Return the configured loop period in seconds.

        Returns:
            Seconds the loop sleeps between samples.
        """
        return self._interval_seconds

    @property
    def latest_snapshot(self) -> DbStatsSnapshot | None:
        """Return the most recently published snapshot, or ``None``.

        Atomic read — readers see either a fully-constructed
        :class:`DbStatsSnapshot` or ``None`` (the prior snapshot until a
        new one is fully built and assigned).

        Returns:
            The latest :class:`DbStatsSnapshot` if at least one sample
            has completed, else ``None``.
        """
        return self._latest_snapshot

    async def start(self) -> None:
        """Spawn the sampler loop. No-op in disabled mode.

        Lifespan startup is unaffected — ``start()`` is non-blocking
        and the first sample only runs after the first ``sleep``. This
        keeps tests lightweight: an ephemeral test app that lives
        shorter than ``interval_seconds`` never pays for a DB sample
        run. Operators pay an up-to-``interval_seconds`` cold-start
        503 window before the first ``GET /api/metrics/db/tables``
        returns 200.
        """
        if self._disabled or self._repo is None:
            logger.info("DbStatsSnapshotter: disabled (DB_METRICS_DISABLED=true); skipping start")
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
        """Sleep then sample; repeat until cancelled.

        Pre-sleep on every iteration (mirrors the system-metrics
        sampler pattern) so an ephemeral test app that lives shorter than
        ``interval_seconds`` never pays for a DB sample. Defensive
        ``try/except`` keeps the loop alive across unexpected
        exceptions from the sampler path.
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
                self._latest_snapshot = await self._sample_once()
            except Exception:
                logger.exception("DbStatsSnapshotter._sample_once raised; continuing")

    async def _sample_once(self) -> DbStatsSnapshot:
        """Build ONE :class:`DbStatsSnapshot` covering every registered table.

        Per-table failures (timeout / exception) clone the prior
        :class:`TableStats` with ``is_stale=True``; if no prior snapshot
        exists the failed-table row carries all-null counters with the
        loop's ``last_sampled_at`` timestamp.
        """
        repo = self._repo
        if repo is None:
            raise RuntimeError("DbStatsSnapshotter._sample_once called in disabled mode")
        started_at = self._clock()
        prior = self._latest_snapshot
        today_utc = started_at.date()
        policies_by_table = {p.table: p for p in RETENTION_POLICIES}
        rows: list[TableStats] = []
        for entry in TABLES_TO_SAMPLE:
            policy = policies_by_table.get(entry.name)
            archivable_window: tuple[date, date] | None = (
                compute_retention_window(today_utc, policy) if policy is not None else None
            )
            sampled_at = self._clock()
            try:
                counters = await asyncio.wait_for(
                    repo.count_table_stats(entry, archivable_window=archivable_window),
                    timeout=PER_TABLE_TIMEOUT_SECONDS,
                )
                rows.append(
                    TableStats(
                        table=entry.name,
                        table_kind=entry.kind,
                        total=counters.total,
                        current=counters.current,
                        closed=counters.closed,
                        archivable=counters.archivable,
                        is_stale=False,
                        last_sampled_at=sampled_at,
                    )
                )
            except Exception as exc:
                logger.warning(
                    "DbStatsSnapshotter: %s sample failed (%s); reusing prior",
                    entry.name,
                    exc,
                )
                rows.append(self._stale_row_for(entry, prior=prior, sampled_at=sampled_at))
        completed_at = self._clock()
        return DbStatsSnapshot(
            snapshot_started_at=started_at,
            snapshot_completed_at=completed_at,
            interval_seconds=self._interval_seconds,
            tables=tuple(rows),
        )

    @staticmethod
    def _stale_row_for(
        entry: TableEntry,
        *,
        prior: DbStatsSnapshot | None,
        sampled_at: datetime,
    ) -> TableStats:
        """Build the stale-row clone or all-null fallback for ``entry``."""
        prior_row = prior.find(entry.name) if prior is not None else None
        if prior_row is not None:
            return TableStats(
                table=prior_row.table,
                table_kind=prior_row.table_kind,
                total=prior_row.total,
                current=prior_row.current,
                closed=prior_row.closed,
                archivable=prior_row.archivable,
                is_stale=True,
                last_sampled_at=prior_row.last_sampled_at,
            )
        return TableStats(
            table=entry.name,
            table_kind=entry.kind,
            total=None,
            current=None,
            closed=None,
            archivable=None,
            is_stale=True,
            last_sampled_at=sampled_at,
        )
