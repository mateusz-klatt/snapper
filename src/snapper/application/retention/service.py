"""Async retention service that wraps the sync :class:`EventArchiver`.

Owns ONE :class:`DatabaseRepository` + ONE :class:`EventArchiver` for
the lifetime of the scheduler. Per-tick:

  1. Compute the per-policy ``[earliest_scanned_day, last_eligible_day]``
     window using the formula in
     ``proprietary/plans/plan_observability_cluster_c.md`` §3.6.
  2. Wrap the sync ``EventArchiver.export(...)`` in
     ``asyncio.to_thread`` so the event loop stays responsive.
  3. Capture per-policy results (``files_written``, ``rows_exported``,
     ``rows_purged``, ``error``); a raise from one policy NEVER
     propagates — the loop continues with the next policy.

Threading contract: ONE ``to_thread`` boundary, inside
:meth:`evaluate_policy`. The scheduler simply ``await`` -s the result;
it does NOT add a second ``to_thread`` layer.

Lifecycle: :meth:`close` is ``async def`` and internally wraps the
sync :meth:`DatabaseRepository.dispose` with ``asyncio.to_thread``,
so the scheduler can ``await self._service.close()`` without knowing
the underlying repo is sync.
"""

import asyncio
import logging
import os
from datetime import UTC
from datetime import date
from datetime import datetime
from datetime import timedelta
from pathlib import Path
from typing import TypedDict

from snapper.application.retention.policies import RETENTION_POLICIES
from snapper.application.retention.policies import RetentionPolicy
from snapper.application.retention.policies import resolve_dry_run
from snapper.data.archiver import EventArchiver
from snapper.data.repository import DatabaseRepository

logger = logging.getLogger(__name__)

_DRY_RUN_ENV_VAR = "RETENTION_DRY_RUN"


class RetentionPolicyRunResult(TypedDict):
    """Per-policy outcome captured on every tick.

    ``day_start`` / ``day_end`` are ISO-8601 dates (``YYYY-MM-DD``);
    ``None`` only when the policy raised before the window was
    computed. ``error`` is ``str(exception)`` on failure; ``None`` on
    success. Counters are zero when the archiver was skipped or the
    policy raised.
    """

    table: str
    retain_days: int
    backlog_lookback_days: int
    day_start: str | None
    day_end: str | None
    archived_rows: int
    purged_rows: int
    files_written: int
    error: str | None


class RetentionRunSummary(TypedDict):
    """Aggregate result of one full ``run_once`` pass.

    ``dry_run`` snapshots the value of ``RETENTION_DRY_RUN`` at run
    start so a mid-run env-var flip cannot create an inconsistent
    summary.
    """

    run_started_at: datetime
    run_completed_at: datetime
    dry_run: bool
    results: list[RetentionPolicyRunResult]


class RetentionService:
    """Owns the sync repo + archiver; orchestrates per-policy ticks."""

    def __init__(self, *, db_url: str, base_dir: Path) -> None:
        """Build the sync repo + archiver wired to ``db_url`` + ``base_dir``.

        Args:
            db_url: SQLAlchemy URL for the sync repository.
            base_dir: Filesystem root for the CSV archive layout
                (matches the ``snapper archive`` CLI default).
        """
        self._db_url = db_url
        self._base_dir = base_dir
        self._repo = DatabaseRepository(db_url)
        self._archiver = EventArchiver(self._repo, base_dir)
        self._last_run_summary: RetentionRunSummary | None = None

    @property
    def last_run_summary(self) -> RetentionRunSummary | None:
        """Return the most recent run's summary, or ``None`` before the first run.

        Returns:
            The latest :class:`RetentionRunSummary` populated by
            :meth:`run_once`, or ``None`` until the eager first run
            completes.
        """
        return self._last_run_summary

    async def evaluate_policy(
        self,
        policy: RetentionPolicy,
        *,
        dry_run: bool | None = None,
    ) -> RetentionPolicyRunResult:
        """Archive + (optionally) purge eligible rows for one policy.

        Computes the per-tick window per the formula in
        ``proprietary/plans/plan_observability_cluster_c.md`` §3.6 and
        delegates to :meth:`EventArchiver.export` via
        ``asyncio.to_thread``. Any exception is captured into the
        returned :class:`RetentionPolicyRunResult` ``error`` field; it
        is NEVER re-raised.

        Args:
            policy: The retention rule to apply.
            dry_run: Override for the dry-run flag — when ``None``,
                :meth:`evaluate_policy` reads ``RETENTION_DRY_RUN`` from
                the environment. :meth:`run_once` resolves the value
                ONCE at run start and passes it through here so a
                mid-run env-var flip cannot make the summary's
                ``dry_run`` field disagree with what individual
                policies actually ran with.

        Returns:
            Per-policy outcome with ``day_start`` / ``day_end`` /
            counters / ``error``.
        """
        today_utc = datetime.now(UTC).date()
        try:
            day_start, day_end = _compute_window(today_utc, policy)
        except (ValueError, OverflowError) as exc:
            logger.exception(
                "RetentionService: window computation failed for table=%s", policy.table
            )
            return _failure_result(policy, day_start=None, day_end=None, error=str(exc))
        if dry_run is None:
            dry_run = resolve_dry_run(os.environ.get(_DRY_RUN_ENV_VAR))
        try:
            result = await asyncio.to_thread(
                self._archiver.export,
                table=policy.table,
                day_start=day_start,
                day_end=day_end,
                dry_run=False,
                purge=not dry_run,
            )
        except Exception as exc:
            logger.exception(
                "RetentionService: archiver raised for table=%s day_start=%s day_end=%s",
                policy.table,
                day_start.isoformat(),
                day_end.isoformat(),
            )
            return _failure_result(
                policy,
                day_start=day_start.isoformat(),
                day_end=day_end.isoformat(),
                error=str(exc),
            )
        logger.info(
            "RetentionService: archived table=%s day_start=%s day_end=%s "
            "files=%d rows=%d purged=%d",
            policy.table,
            day_start.isoformat(),
            day_end.isoformat(),
            result.files_written,
            result.rows_exported,
            result.rows_purged,
        )
        return RetentionPolicyRunResult(
            table=policy.table,
            retain_days=policy.retain_days,
            backlog_lookback_days=policy.backlog_lookback_days,
            day_start=day_start.isoformat(),
            day_end=day_end.isoformat(),
            archived_rows=result.rows_exported,
            purged_rows=result.rows_purged,
            files_written=result.files_written,
            error=None,
        )

    async def run_once(self) -> RetentionRunSummary:
        """Iterate every policy in :data:`RETENTION_POLICIES` once.

        Captures :meth:`evaluate_policy` per-policy results into a
        single :class:`RetentionRunSummary`. Stamps
        ``run_started_at`` + ``run_completed_at`` + ``dry_run`` at the
        boundaries. Replaces :attr:`last_run_summary`.

        Returns:
            The aggregate summary, also stored on the instance.
        """
        run_started_at = datetime.now(UTC)
        dry_run = resolve_dry_run(os.environ.get(_DRY_RUN_ENV_VAR))
        results: list[RetentionPolicyRunResult] = []
        for policy in RETENTION_POLICIES:
            results.append(await self.evaluate_policy(policy, dry_run=dry_run))
        run_completed_at = datetime.now(UTC)
        summary = RetentionRunSummary(
            run_started_at=run_started_at,
            run_completed_at=run_completed_at,
            dry_run=dry_run,
            results=results,
        )
        self._last_run_summary = summary
        return summary

    async def close(self) -> None:
        """Dispose the underlying SQLAlchemy engine.

        Wraps the sync :meth:`DatabaseRepository.dispose` in
        ``asyncio.to_thread`` so the scheduler can simply
        ``await service.close()``.
        """
        await asyncio.to_thread(self._repo.dispose)


def _compute_window(today_utc: date, policy: RetentionPolicy) -> tuple[date, date]:
    """Map ``policy`` to a per-tick ``(day_start, day_end)`` window.

    Per the formula in
    ``proprietary/plans/plan_observability_cluster_c.md`` §3.6:

    .. code-block:: python

        oldest_kept_day      = today_utc - timedelta(days=policy.retain_days)
        last_eligible_day    = oldest_kept_day - timedelta(days=1)
        earliest_scanned_day = last_eligible_day - timedelta(days=policy.backlog_lookback_days)

    Args:
        today_utc: UTC date to anchor the window against (typically
            ``datetime.now(UTC).date()``).
        policy: Retention rule with ``retain_days`` +
            ``backlog_lookback_days``.

    Returns:
        ``(earliest_scanned_day, last_eligible_day)`` — the
        ``(day_start, day_end)`` pair to pass to
        :meth:`EventArchiver.export` (whole-day inclusive).
    """
    oldest_kept_day = today_utc - timedelta(days=policy.retain_days)
    last_eligible_day = oldest_kept_day - timedelta(days=1)
    earliest_scanned_day = last_eligible_day - timedelta(days=policy.backlog_lookback_days)
    return earliest_scanned_day, last_eligible_day


def _failure_result(
    policy: RetentionPolicy,
    *,
    day_start: str | None,
    day_end: str | None,
    error: str,
) -> RetentionPolicyRunResult:
    """Build a zero-counters :class:`RetentionPolicyRunResult` carrying ``error``.

    Args:
        policy: Source policy (for ``table`` + retention parameters).
        day_start: ISO-8601 date string when computed; ``None`` when the
            window itself failed to compute.
        day_end: Same, for the upper bound.
        error: ``str(exception)`` describing the failure.

    Returns:
        A failure-shaped :class:`RetentionPolicyRunResult`.
    """
    return RetentionPolicyRunResult(
        table=policy.table,
        retain_days=policy.retain_days,
        backlog_lookback_days=policy.backlog_lookback_days,
        day_start=day_start,
        day_end=day_end,
        archived_rows=0,
        purged_rows=0,
        files_written=0,
        error=error,
    )
