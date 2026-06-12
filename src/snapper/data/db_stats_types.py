"""Shared types for the per-table DB-stats counter.

``TableEntry`` describes ONE table the snapshotter samples (name, kind,
ORM model). ``TableCounters`` is the four-counter result returned by
:meth:`Repository.count_table_stats` for a single table.

The dataclasses live in the ``data`` layer (alongside SQLAlchemy ORM
models) so :class:`Repository` can declare the abstract primitive
without importing from ``application``. The
:class:`DbStatsSnapshotter` imports from here too — single source of
truth, no circular dependency.

``TableKind`` distinguishes append-only event tables (``current`` /
``closed`` are semantically null — these tables have no SCD2 lifecycle)
from SCD2 state tables (``current`` counts rows whose ``known_to`` equals
the SCD2 sentinel; ``closed`` is derived as ``max(0, total - current)``
rather than counted directly — see :class:`TableCounters`).
"""

from dataclasses import dataclass
from typing import Literal

from sqlalchemy.orm import DeclarativeBase

TableKind = Literal["event", "state"]


@dataclass(frozen=True, slots=True)
class TableEntry:
    """One table the per-table stats snapshotter samples.

    Attributes:
        name: Table name (key in ``EVENT_TABLES`` or ``STATE_TABLES``).
        kind: ``"event"`` (append-only) or ``"state"`` (SCD2-versioned).
        model: SQLAlchemy ORM model class — must inherit
            :class:`sqlalchemy.orm.DeclarativeBase` so the count
            primitive can resolve ``model.known_to`` /
            ``model.timestamp`` columns at runtime.
    """

    name: str
    kind: TableKind
    model: type[DeclarativeBase]


@dataclass(frozen=True, slots=True)
class TableCounters:
    """Four-counter result from :meth:`Repository.count_table_stats`.

    Attributes:
        total: Total row count — dialect-aware: on PostgreSQL a planner
            ESTIMATE (``pg_class.reltuples``, accurate within
            autovacuum drift), on SQLite exact. ``None`` only on
            per-table query failure (the snapshotter then reuses prior
            values via ``dataclasses.replace(prior, is_stale=True)``).
        current: Active SCD2 versions (rows whose ``known_to`` equals
            the SCD2 sentinel; always an exact count) for state tables;
            ``None`` for event tables (semantics).
        closed: Superseded SCD2 versions for state tables, derived as
            ``max(0, total - current)`` to skip a slow full scan —
            inherits the PG estimate error; ``None`` for event tables.
        archivable: Row count in the policy retention window when a
            policy is registered for the table; ``None`` when no policy
            applies (semantically distinct from ``0``).
    """

    total: int | None
    current: int | None
    closed: int | None
    archivable: int | None
