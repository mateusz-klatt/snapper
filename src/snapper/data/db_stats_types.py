"""Shared types for the per-table DB-stats counter (Cluster B).

``TableEntry`` describes ONE table the snapshotter samples (name, kind,
ORM model). ``TableCounters`` is the four-counter result returned by
:meth:`Repository.count_table_stats` for a single table.

The dataclasses live in the ``data`` layer (alongside SQLAlchemy ORM
models) so :class:`Repository` can declare the abstract primitive
without importing from ``application``. Cluster B's
:class:`DbStatsSnapshotter` imports from here too — single source of
truth, no circular dependency.

``TableKind`` distinguishes append-only event tables (``current`` /
``closed`` are semantically null — these tables have no SCD2 lifecycle)
from SCD2 state tables (``current`` counts rows whose ``known_to`` equals
the SCD2 sentinel; ``closed`` counts rows whose ``known_to`` is below
the sentinel).
"""

from dataclasses import dataclass
from typing import Any
from typing import Literal

TableKind = Literal["event", "state"]


@dataclass(frozen=True, slots=True)
class TableEntry:
    """One table the per-table stats snapshotter samples.

    Attributes:
        name: Table name (key in ``EVENT_TABLES`` or ``STATE_TABLES``).
        kind: ``"event"`` (append-only) or ``"state"`` (SCD2-versioned).
        model: SQLAlchemy ORM model class. ``type[Any]`` follows the
            project boundary-exception rule for SQLAlchemy expression
            internals.
    """

    name: str
    kind: TableKind
    model: type[Any]


@dataclass(frozen=True, slots=True)
class TableCounters:
    """Four-counter result from :meth:`Repository.count_table_stats`.

    Attributes:
        total: Total row count. ``None`` only on per-table query failure
            (the snapshotter then reuses prior values via
            ``dataclasses.replace(prior, is_stale=True)``).
        current: Active SCD2 versions (rows whose ``known_to`` equals
            the SCD2 sentinel) for state tables; ``None`` for event
            tables (semantics).
        closed: Superseded SCD2 versions (rows whose ``known_to`` is
            below the sentinel) for state tables; ``None`` for event
            tables.
        archivable: Row count in the policy retention window when a
            policy is registered for the table; ``None`` when no policy
            applies (semantically distinct from ``0``).
    """

    total: int | None
    current: int | None
    closed: int | None
    archivable: int | None
