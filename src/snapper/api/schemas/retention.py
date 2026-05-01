"""Pydantic schemas for the ``GET /api/metrics/retention`` route.

Mirrors the in-memory :class:`RetentionPolicyRunResult` +
:class:`RetentionRunSummary` TypedDicts as wire-strict Pydantic
bodies, wrapped in the standard ``PayloadResponse`` envelope.

Field semantics (units, ``None`` cases) match the TypedDicts in
:mod:`snapper.application.retention.service`.
"""

from datetime import datetime
from typing import Literal

from snapper.api.schemas.base import PayloadResponse
from snapper.api.schemas.base import StrictBody
from snapper.api.schemas.base import StrictDataSchema


class RetentionPolicyResult(StrictBody):
    """Per-policy outcome for one scheduler tick.

    ``day_start`` / ``day_end`` are ISO-8601 dates (``YYYY-MM-DD``);
    ``None`` only when the policy raised before the window was
    computed. ``error`` is ``str(exception)`` on failure; ``None`` on
    success.

    Attributes:
        table: Event-table name targeted by the policy.
        retain_days: Configured DB residency in whole UTC days.
        backlog_lookback_days: Per-tick cap on days walked backward.
        day_start: ISO-8601 lower bound of the archived window;
            ``None`` when the window itself failed to compute.
        day_end: ISO-8601 upper bound; same ``None`` semantics.
        archived_rows: Rows written to CSV during this tick.
        purged_rows: Rows deleted from the DB during this tick.
            ``0`` when ``RETENTION_DRY_RUN=true`` is in effect.
        files_written: CSV files written this tick.
        error: ``str(exception)`` when the policy raised; ``None`` on
            success.
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


class RetentionRunData(StrictDataSchema[Literal["retention_run"]]):
    """Aggregate result of one ``RetentionService.run_once`` pass.

    Attributes:
        type: Payload item type discriminator.
        run_started_at: UTC timestamp when the tick began.
        run_completed_at: UTC timestamp when the last policy result
            was captured.
        dry_run: Snapshot of ``RETENTION_DRY_RUN`` taken at run start;
            ``True`` means every policy ran with ``purge=False``.
        results: Per-policy outcomes in policy-list order.
    """

    type: Literal["retention_run"] = "retention_run"
    run_started_at: datetime
    run_completed_at: datetime
    dry_run: bool
    results: list[RetentionPolicyResult]


class RetentionRunResponse(PayloadResponse[Literal["retention_run_response"], RetentionRunData]):
    """Envelope-wrapped response for ``GET /api/metrics/retention``."""

    type: Literal["retention_run_response"] = "retention_run_response"
