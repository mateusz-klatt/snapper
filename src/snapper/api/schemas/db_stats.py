"""Pydantic schemas for the ``GET /api/metrics/db/tables`` route (Cluster B).

Mirrors the in-memory :class:`TableStats` + :class:`DbStatsSnapshot`
dataclasses as wire-strict Pydantic bodies, wrapped in the standard
``PayloadResponse`` envelope.

Field semantics (units, ``None`` cases) match
:mod:`snapper.application.db_stats.snapshotter`. ``current``/``closed``
are ``None`` for event tables (no SCD2 lifecycle); ``archivable`` is
``None`` when no retention policy applies (semantically distinct from
``0``).
"""

from datetime import datetime
from typing import Literal

from snapper.api.schemas.base import PayloadResponse
from snapper.api.schemas.base import StrictBody
from snapper.api.schemas.base import StrictDataSchema


class TableStatsItem(StrictBody):
    """Per-table four-counter row from the latest sampler snapshot.

    Attributes:
        table: Table name (key in ``EVENT_TABLES`` or ``STATE_TABLES``).
        table_kind: ``"event"`` (append-only) or ``"state"``
            (SCD2-versioned).
        total: Total row count. ``None`` only on per-table query
            failure with no prior sample to clone.
        current: Active SCD2 versions for state tables; ``None`` for
            event tables.
        closed: Superseded SCD2 versions for state tables; ``None``
            for event tables.
        archivable: Row count in the policy retention window when a
            policy is registered; ``None`` when no policy applies.
        is_stale: ``True`` when the row was reused from a prior sample
            after a per-table query timeout or exception.
        last_sampled_at: UTC timestamp of the row's source sample.
            On a stale clone this is the original timestamp, NOT the
            current cycle's clock.
    """

    table: str
    table_kind: Literal["event", "state"]
    total: int | None
    current: int | None
    closed: int | None
    archivable: int | None
    is_stale: bool
    last_sampled_at: datetime


class DbStatsData(StrictDataSchema[Literal["db_stats"]]):
    """Latest sampled DB row-count snapshot.

    Attributes:
        type: Payload item type discriminator.
        snapshot_started_at: UTC timestamp when the sampler began the
            cycle that produced this snapshot.
        snapshot_completed_at: UTC timestamp when the sampler finished
            the cycle.
        interval_seconds: Echo of the configured sampler cadence.
        tables: Per-table counters in the sampler's deterministic
            order (STATE alphabetical, then EVENT alphabetical).
    """

    type: Literal["db_stats"] = "db_stats"
    snapshot_started_at: datetime
    snapshot_completed_at: datetime
    interval_seconds: int
    tables: list[TableStatsItem]


class DbStatsResponse(PayloadResponse[Literal["db_stats_response"], DbStatsData]):
    """Envelope-wrapped response for ``GET /api/metrics/db/tables``."""

    type: Literal["db_stats_response"] = "db_stats_response"
