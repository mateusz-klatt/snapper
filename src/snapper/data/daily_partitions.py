"""Daily PostgreSQL partition lifecycle for Snapper market-data tables.

The lifecycle is deliberately separate from row-retention policies. It works
only on the static ``ticks``, ``candles``, and ``trades`` allowlist, performs
catalog inspection before every mutation, and returns the exact DDL it planned
or executed. Adoption preserves the ordinary table as ``*_legacy``, constructs
a minimal partitioned parent independently of Alembic, pre-creates fourteen
daily leaves and an anomaly-buffer DEFAULT partition, then transfers the
existing sequence ownership to the parent.

Mutation defaults are dry-run. Callers must pass ``dry_run=False`` explicitly.
"""

import re
import time
from dataclasses import dataclass
from datetime import UTC
from datetime import date
from datetime import datetime
from datetime import timedelta
from enum import StrEnum
from typing import Final
from typing import Literal
from typing import cast

from sqlalchemy import bindparam
from sqlalchemy import text
from sqlalchemy.engine import Connection
from sqlalchemy.exc import DBAPIError

MarketDataTable = Literal["ticks", "candles", "trades"]

FUTURE_LEAF_TARGET: Final[int] = 14
FUTURE_LEAF_ALARM: Final[int] = 7
DETACH_RETRIES: Final[int] = 3
LOCK_TIMEOUT: Final[str] = "2s"
CUTOVER_STATEMENT_TIMEOUT: Final[str] = "30s"
VALIDATION_STATEMENT_TIMEOUT: Final[str] = "12h"
INDEX_BUILD_STATEMENT_TIMEOUT: Final[str] = "12h"
INDEX_BUILD_MAINTENANCE_WORK_MEM: Final[str] = "64MB"
_ACTIVE_KNOWN_TO: Final[datetime] = datetime(9999, 12, 31, 23, 59, 59, tzinfo=UTC)
_ANCHOR_PATTERN: Final[re.Pattern[str]] = re.compile(r"\d{4}-\d{2}-\d{2}T00:00:00\+00:00")
_SET_LOCAL_TIME_ZONE_UTC: Final[str] = "SET LOCAL TIME ZONE 'UTC'"
_TIMESTAMPTZ: Final[str] = "timestamp with time zone"
_DOUBLE_PRECISION: Final[str] = "double precision"
_COMPACT_STRIP_PATTERN: Final[str] = r'[\s()"]'
_QUOTED_LITERAL_PATTERN: Final[str] = r"'([^']+)'"


class DailyPartitionError(RuntimeError):
    """Raised when catalog state makes a lifecycle operation unsafe."""


class RelationState(StrEnum):
    """Supported catalog states for one allowlisted market-data relation."""

    MISSING = "missing"
    ORDINARY = "ordinary"
    PARTITIONED = "partitioned"
    OTHER = "other"


class LifecycleAction(StrEnum):
    """Outcome of one requested lifecycle operation."""

    PLANNED = "planned"
    ADOPTED = "adopted"
    ENSURED = "ensured"
    DETACHED = "detached"
    NOOP = "noop"


class ConstraintRole(StrEnum):
    """Catalog role controlling one relation's exact non-CHECK manifest."""

    ORDINARY = "ordinary"
    PREPARED = "prepared"
    PARENT = "parent"
    LEGACY = "legacy"
    LEAF = "leaf"


@dataclass(frozen=True, slots=True)
class IndexSpec:
    """Expected definition of one partitioned parent index.

    Attributes:
        name: Stable parent index name.
        columns: Ordered SQL expressions forming the index key.
        unique: Whether PostgreSQL enforces uniqueness.
        predicate: Optional partial-index predicate.
    """

    name: str
    columns: tuple[str, ...]
    unique: bool = False
    predicate: str | None = None


@dataclass(frozen=True, slots=True)
class TableSpec:
    """Static lifecycle contract for one market-data table.

    Attributes:
        table: Allowlisted parent relation name.
        partition_key: Event-time range partition key.
        retention_days: Minimum number of complete days retained.
        local_public_id: Whether leaves retain active-public-id uniqueness.
        parent_indexes: Exact minimal-parent index manifest.
    """

    table: MarketDataTable
    partition_key: str
    retention_days: int
    local_public_id: bool
    parent_indexes: tuple[IndexSpec, ...]


@dataclass(frozen=True, slots=True)
class PartitionRef:
    """One direct child and its PostgreSQL partition bound.

    Attributes:
        name: Child relation name.
        bound: Canonical ``pg_get_expr`` partition-bound rendering.
        schema: Child namespace.
        relation_kind: PostgreSQL relation kind.
        is_partition: Whether the child has partition identity.
        has_partition_key: Whether the expected leaf is itself partitioned.
        inheritance_sequence: Direct ``pg_inherits`` edge position.
    """

    name: str
    bound: str
    schema: str = "public"
    relation_kind: str = "r"
    is_partition: bool = True
    has_partition_key: bool = False
    inheritance_sequence: int = 1


@dataclass(frozen=True, slots=True)
class PartitionInspection:
    """Read-only topology report for one allowlisted table.

    Attributes:
        table: Inspected allowlisted relation.
        state: Ordinary, partitioned, missing, or unsupported relation state.
        partition_key: PostgreSQL partition-key definition when partitioned.
        partitions: Direct children ordered by relation name.
        default_attached: Whether the required DEFAULT child is attached.
        default_has_rows: Whether the DEFAULT anomaly buffer contains any row.
        future_leaf_count: Daily leaves at or after the inspection anchor.
        future_leaf_alarm: Whether fewer than seven future leaves remain.
    """

    table: MarketDataTable
    state: RelationState
    partition_key: str | None
    partitions: tuple[PartitionRef, ...]
    default_attached: bool
    default_has_rows: bool
    future_leaf_count: int
    future_leaf_alarm: bool


@dataclass(frozen=True, slots=True)
class LifecycleResult:
    """Exact outcome and DDL evidence from one lifecycle request.

    Attributes:
        table: Affected allowlisted relation.
        action: Planned, applied, detached, or no-op outcome.
        statements: Ordered mutation statements planned or executed.
        future_leaf_count: Resulting or planned number of future leaves.
        future_leaf_alarm: Whether that count is below the alarm threshold.
    """

    table: MarketDataTable
    action: LifecycleAction
    statements: tuple[str, ...]
    future_leaf_count: int
    future_leaf_alarm: bool


@dataclass(frozen=True, slots=True)
class ConstraintInfo:
    """Catalog definition and validation state for one CHECK constraint.

    Attributes:
        definition: PostgreSQL canonical constraint definition.
        validated: Whether existing rows have been validated.
    """

    definition: str
    validated: bool


@dataclass(frozen=True, slots=True)
class ConstraintShape:
    """Exact catalog shape of one non-CHECK table constraint.

    Attributes:
        name: Stable constraint name.
        kind: PostgreSQL one-letter constraint type.
        definition: Canonical ``pg_get_constraintdef`` output.
        validated: Whether PostgreSQL considers the constraint validated.
        deferrable: Whether enforcement may be deferred.
        initially_deferred: Whether enforcement starts deferred.
        index_name: Name of the exact backing index, when present.
    """

    name: str
    kind: str
    definition: str
    validated: bool
    deferrable: bool
    initially_deferred: bool
    index_name: str | None


@dataclass(frozen=True, slots=True)
class IndexShape:
    """Structural catalog shape of one index.

    Attributes:
        unique: Whether the index enforces uniqueness.
        valid: Whether PostgreSQL considers the index valid.
        ready: Whether inserts maintain the index.
        live: Whether PostgreSQL considers the index usable in catalog work.
        columns: Ordered key expressions.
        predicate: Optional canonical predicate expression.
        constraint_backed: Whether a ``pg_constraint`` owns the index.
        access_method: PostgreSQL index access method.
        no_expressions: Whether every key is a plain table attribute.
        no_include: Whether the index has no INCLUDE attributes.
        nulls_distinct: Whether unique NULL values remain distinct.
        default_options: Whether every key uses ascending default NULL ordering.
        default_opclasses: Whether every key uses its default btree operator class.
        attribute_collations: Whether key collations equal their table attributes.
        default_reloptions: Whether the index has no relation storage options.
        default_tablespace: Whether the index inherits the database tablespace.
        parent_indexes: Existing direct index-parent edges.
    """

    unique: bool
    valid: bool
    ready: bool
    live: bool
    columns: tuple[str, ...]
    predicate: str | None
    constraint_backed: bool
    access_method: str
    no_expressions: bool
    no_include: bool
    nulls_distinct: bool
    default_options: bool
    default_opclasses: bool
    attribute_collations: bool
    default_reloptions: bool
    default_tablespace: bool
    parent_indexes: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class ColumnContract:
    """Expected ordinary-table column catalog shape.

    Attributes:
        name: Stable column name and ordinal-position identity.
        data_type: Canonical PostgreSQL ``format_type`` output.
        nullable: Required nullability, or no restriction during preparation.
        default_kind: Expected default category.
    """

    name: str
    data_type: str
    nullable: bool | None
    default_kind: str | None = None


@dataclass(frozen=True, slots=True)
class OrdinaryIndexContract:
    """Expected pre-adoption ordinary index shape.

    Attributes:
        name: Stable legacy index name.
        columns: Ordered key expressions.
        unique: Whether the index enforces uniqueness.
        active_predicate: Whether it is the active-row partial.
        constraint_backed: Whether a constraint owns the index.
    """

    name: str
    columns: tuple[str, ...]
    unique: bool
    active_predicate: bool
    constraint_backed: bool


_TABLE_SPECS: Final[dict[str, TableSpec]] = {
    "ticks": TableSpec(
        table="ticks",
        partition_key="timestamp",
        retention_days=7,
        local_public_id=False,
        parent_indexes=(
            IndexSpec(
                name="ticks_p_ix_instr_ts",
                columns=("instrument_public_id", "timestamp"),
            ),
        ),
    ),
    "candles": TableSpec(
        table="candles",
        partition_key="open_at",
        retention_days=30,
        local_public_id=True,
        parent_indexes=(
            IndexSpec(
                name="candles_p_uq_itf_open",
                columns=("instrument_public_id", "timeframe", "open_at"),
                unique=True,
                predicate="known_to = TIMESTAMPTZ '9999-12-31 23:59:59+00:00'",
            ),
            IndexSpec(
                name="candles_p_ix_instr_open",
                columns=("instrument_public_id", "open_at"),
            ),
        ),
    ),
    "trades": TableSpec(
        table="trades",
        partition_key="executed_at",
        retention_days=30,
        local_public_id=True,
        parent_indexes=(
            IndexSpec(
                name="trades_p_uq_instr_tid_exec",
                columns=("instrument_public_id", "trade_id", "executed_at"),
                unique=True,
            ),
            IndexSpec(
                name="trades_p_ix_instr_ts",
                columns=("instrument_public_id", "timestamp"),
            ),
            IndexSpec(name="trades_p_ix_ts", columns=("timestamp",)),
            IndexSpec(name="trades_p_ix_exec", columns=("executed_at",)),
        ),
    ),
}


def parse_anchor(value: str) -> datetime:
    """Parse the one accepted deterministic partition-anchor representation.

    Args:
        value: Candidate ``YYYY-MM-DDT00:00:00+00:00`` value.

    Returns:
        A timezone-aware UTC midnight.

    Raises:
        ValueError: If the representation is not exact or is not a valid date.
    """
    if _ANCHOR_PATTERN.fullmatch(value) is None:
        raise ValueError("partition anchor must use YYYY-MM-DDT00:00:00+00:00 exactly")
    parsed = datetime.fromisoformat(value)
    return _validate_anchor(parsed)


def current_utc_anchor() -> datetime:
    """Return the current UTC day's deterministic lower boundary.

    Returns:
        Current UTC midnight with no fractional seconds.
    """
    now = datetime.now(UTC)
    return datetime(now.year, now.month, now.day, tzinfo=UTC)


def market_data_table(value: str) -> MarketDataTable:
    """Validate a table name against the immutable lifecycle allowlist.

    Args:
        value: Candidate relation name.

    Returns:
        The narrowed allowlisted table name.

    Raises:
        ValueError: If the value is outside the three-table allowlist.
    """
    if value not in _TABLE_SPECS:
        allowed = ", ".join(sorted(_TABLE_SPECS))
        raise ValueError(f"market-data table must be one of: {allowed}")
    return cast(MarketDataTable, value)


def inspect(
    connection: Connection,
    table: MarketDataTable,
    anchor: datetime | None = None,
) -> PartitionInspection:
    """Inspect one market-data table without mutating catalog or data.

    Args:
        connection: SQLAlchemy connection to the target PostgreSQL database.
        table: Statically allowlisted market-data table.
        anchor: UTC midnight used to classify future daily leaves.

    Returns:
        A typed topology and anomaly-buffer report.

    Raises:
        DailyPartitionError: If the connection is not PostgreSQL.
        ValueError: If the anchor is not a UTC midnight.
    """
    _require_postgresql(connection)
    _require_public_schema(connection)
    spec = _spec(table)
    effective_anchor = current_utc_anchor() if anchor is None else _validate_anchor(anchor)
    connection.execute(text(_SET_LOCAL_TIME_ZONE_UTC))
    relation_row = connection.execute(
        text("""
            SELECT c.relkind::text,
                   c.relispartition
            FROM pg_class AS c
            JOIN pg_namespace AS n ON n.oid = c.relnamespace
            WHERE n.nspname = current_schema()
              AND c.relname = :table
            """),
        {"table": spec.table},
    ).one_or_none()
    relation_kind = cast(str | None, relation_row[0] if relation_row is not None else None)
    is_partition = cast(bool, relation_row[1]) if relation_row is not None else False
    state = RelationState.OTHER if is_partition else _relation_state(relation_kind)
    if state is not RelationState.PARTITIONED:
        return PartitionInspection(
            table=spec.table,
            state=state,
            partition_key=None,
            partitions=(),
            default_attached=False,
            default_has_rows=False,
            future_leaf_count=0,
            future_leaf_alarm=True,
        )
    partition_key = cast(
        str,
        connection.scalar(
            text("""
                SELECT pg_get_partkeydef(c.oid)
                FROM pg_class AS c
                JOIN pg_namespace AS n ON n.oid = c.relnamespace
                WHERE n.nspname = current_schema()
                  AND c.relname = :table
                """),
            {"table": spec.table},
        ),
    )
    partitions = _direct_partitions(connection, spec)
    default_name = f"{spec.table}_default"
    default_attached = any(item.name == default_name for item in partitions)
    default_has_rows = _relation_has_rows(connection, default_name) if default_attached else False
    future_leaf_count = _future_leaf_count(spec, partitions, effective_anchor)
    return PartitionInspection(
        table=spec.table,
        state=state,
        partition_key=partition_key,
        partitions=partitions,
        default_attached=default_attached,
        default_has_rows=default_has_rows,
        future_leaf_count=future_leaf_count,
        future_leaf_alarm=future_leaf_count < FUTURE_LEAF_ALARM,
    )


def adopt(
    connection: Connection,
    table: MarketDataTable,
    anchor: datetime,
    *,
    dry_run: bool = True,
) -> LifecycleResult:
    """Adopt an ordinary market-data table as a daily partitioned parent.

    The manual implementation is intentionally independent of migration SQL.
    It prepares a validated legacy bound, builds the trades U3 index
    concurrently when needed, and performs rename, parent construction, leaf
    creation, legacy ATTACH, and sequence transfer in one atomic cutover.

    Args:
        connection: Clean SQLAlchemy connection to PostgreSQL.
        table: Statically allowlisted market-data table.
        anchor: Exact UTC midnight separating legacy and daily leaves.
        dry_run: When true, return all required DDL without executing it.

    Returns:
        Ordered planned or executed DDL and future-leaf status.

    Raises:
        DailyPartitionError: If state or catalog shape is unsafe or ambiguous.
        ValueError: If the anchor is not a UTC midnight.
    """
    _require_clean_connection(connection)
    _require_postgresql(connection)
    _require_public_schema(connection)
    spec = _spec(table)
    checked_anchor = _validate_anchor(anchor)
    initial = inspect(connection, table, checked_anchor)
    connection.rollback()
    if initial.state is RelationState.PARTITIONED:
        _verify_partitioned(connection, spec, checked_anchor)
        connection.rollback()
        return LifecycleResult(
            table=spec.table,
            action=LifecycleAction.NOOP,
            statements=(),
            future_leaf_count=initial.future_leaf_count,
            future_leaf_alarm=initial.future_leaf_alarm,
        )
    if initial.state is not RelationState.ORDINARY:
        raise DailyPartitionError(
            f"refused: {spec.table} is in unsupported state {initial.state.value}"
        )
    connection.execute(text(_SET_LOCAL_TIME_ZONE_UTC))
    _verify_ordinary_schema(connection, spec, checked_anchor)
    _verify_sequence_owner(connection, spec)
    ordinary_checks = _ordinary_check_constraints(connection, spec)
    trade_u3 = _trade_u3_statement(connection, spec)
    range_statements = _range_preparation_statements(connection, spec, checked_anchor)
    connection.rollback()
    cutover_statements = _cutover_statements(spec, checked_anchor, ordinary_checks)
    statements = tuple(
        statement for statement in (trade_u3, *range_statements, *cutover_statements) if statement
    )
    if dry_run:
        return LifecycleResult(
            table=spec.table,
            action=LifecycleAction.PLANNED,
            statements=statements,
            future_leaf_count=FUTURE_LEAF_TARGET,
            future_leaf_alarm=False,
        )
    if trade_u3:
        _create_trade_u3_concurrently(connection, trade_u3)
    _prepare_legacy_range(connection, spec, checked_anchor)
    final = _execute_cutover(
        connection,
        spec,
        checked_anchor,
        cutover_statements,
    )
    return LifecycleResult(
        table=spec.table,
        action=LifecycleAction.ADOPTED,
        statements=statements,
        future_leaf_count=final.future_leaf_count,
        future_leaf_alarm=final.future_leaf_alarm,
    )


def ensure_future_leaves(
    connection: Connection,
    table: MarketDataTable,
    anchor: datetime | None = None,
    *,
    dry_run: bool = True,
) -> LifecycleResult:
    """Create every missing daily leaf in the fourteen-day future window.

    The DEFAULT child is locked behind the parent's ACCESS EXCLUSIVE lock and
    rechecked for rows before any partition creation. A nonempty anomaly
    buffer therefore causes a refusal instead of an unbounded overlap scan.

    Args:
        connection: Clean SQLAlchemy connection to PostgreSQL.
        table: Statically allowlisted market-data table.
        anchor: First UTC day that must have a leaf.
        dry_run: When true, report DDL without executing it.

    Returns:
        Ordered leaf DDL and the planned or resulting future count.

    Raises:
        DailyPartitionError: If topology or DEFAULT state is unsafe.
        ValueError: If the anchor is not a UTC midnight.
    """
    _require_clean_connection(connection)
    _require_postgresql(connection)
    _require_public_schema(connection)
    spec = _spec(table)
    effective_anchor = current_utc_anchor() if anchor is None else _validate_anchor(anchor)
    topology = inspect(connection, table, effective_anchor)
    connection.rollback()
    _require_partitioned_topology(connection, spec, effective_anchor)
    connection.rollback()
    if not topology.default_attached:
        raise DailyPartitionError(
            f"refused: required DEFAULT partition {spec.table}_default is not attached"
        )
    if topology.default_has_rows:
        raise DailyPartitionError(
            f"refused: {spec.table}_default contains rows and must be drained first"
        )
    attached = {item.name for item in topology.partitions}
    _verify_desired_daily_bounds(spec, topology.partitions, effective_anchor)
    missing_days = tuple(
        effective_anchor + timedelta(days=offset)
        for offset in range(FUTURE_LEAF_TARGET)
        if _daily_name(spec, effective_anchor + timedelta(days=offset)) not in attached
    )
    statements = tuple(
        statement for lower in missing_days for statement in _daily_leaf_statements(spec, lower)
    )
    planned_count = topology.future_leaf_count + len(missing_days)
    if not statements:
        return LifecycleResult(
            table=spec.table,
            action=LifecycleAction.NOOP,
            statements=(),
            future_leaf_count=topology.future_leaf_count,
            future_leaf_alarm=topology.future_leaf_alarm,
        )
    if dry_run:
        return LifecycleResult(
            table=spec.table,
            action=LifecycleAction.PLANNED,
            statements=statements,
            future_leaf_count=planned_count,
            future_leaf_alarm=planned_count < FUTURE_LEAF_ALARM,
        )
    _execute_ensure(connection, spec, statements)
    final = inspect(connection, table, effective_anchor)
    connection.rollback()
    return LifecycleResult(
        table=spec.table,
        action=LifecycleAction.ENSURED,
        statements=statements,
        future_leaf_count=final.future_leaf_count,
        future_leaf_alarm=final.future_leaf_alarm,
    )


def detach(
    connection: Connection,
    table: MarketDataTable,
    day: date,
    anchor: datetime | None = None,
    *,
    dry_run: bool = True,
) -> LifecycleResult:
    """Detach one expired daily leaf with plain bounded-lock PostgreSQL DDL.

    ``CONCURRENTLY`` is deliberately absent because PostgreSQL cannot combine
    it with the retained DEFAULT partition. The child is detached but never
    dropped; archival and destructive removal remain separate operator acts.

    Args:
        connection: Clean SQLAlchemy connection to PostgreSQL.
        table: Statically allowlisted market-data table.
        day: UTC event day represented by the daily leaf.
        anchor: Current UTC retention boundary.
        dry_run: When true, report the plain DETACH without executing it.

    Returns:
        Planned or executed DETACH evidence.

    Raises:
        DailyPartitionError: If the day is retained, missing, or lock retries fail.
        ValueError: If the anchor is not a UTC midnight.
    """
    _require_clean_connection(connection)
    _require_postgresql(connection)
    _require_public_schema(connection)
    spec = _spec(table)
    effective_anchor = current_utc_anchor() if anchor is None else _validate_anchor(anchor)
    cutoff = effective_anchor.date() - timedelta(days=spec.retention_days)
    if day >= cutoff:
        raise DailyPartitionError(
            f"refused: {day.isoformat()} is inside the {spec.retention_days}-day "
            f"{spec.table} retention horizon"
        )
    topology = inspect(connection, table, effective_anchor)
    connection.rollback()
    _require_partitioned_topology(connection, spec, effective_anchor)
    connection.rollback()
    if not topology.default_attached:
        raise DailyPartitionError(
            f"refused: required DEFAULT partition {spec.table}_default is not attached"
        )
    leaf = _daily_name(spec, datetime(day.year, day.month, day.day, tzinfo=UTC))
    partition = next((item for item in topology.partitions if item.name == leaf), None)
    if partition is None:
        raise DailyPartitionError(f"refused: {leaf} is not an attached partition")
    lower = datetime(day.year, day.month, day.day, tzinfo=UTC)
    if not _range_bound_matches(partition.bound, lower, lower + timedelta(days=1)):
        raise DailyPartitionError(f"refused: {leaf} has an unexpected partition bound")
    statement = f"ALTER TABLE {spec.table} DETACH PARTITION {leaf}"
    if dry_run:
        return LifecycleResult(
            table=spec.table,
            action=LifecycleAction.PLANNED,
            statements=(statement,),
            future_leaf_count=topology.future_leaf_count,
            future_leaf_alarm=topology.future_leaf_alarm,
        )
    _detach_with_retries(connection, spec, leaf, lower)
    return LifecycleResult(
        table=spec.table,
        action=LifecycleAction.DETACHED,
        statements=(statement,),
        future_leaf_count=topology.future_leaf_count,
        future_leaf_alarm=topology.future_leaf_alarm,
    )


def _spec(table: MarketDataTable) -> TableSpec:
    """Resolve one statically typed table to its immutable SQL contract.

    Args:
        table: Allowlisted market-data table.

    Returns:
        Immutable table lifecycle specification.
    """
    try:
        return _TABLE_SPECS[table]
    except KeyError as error:
        allowed = ", ".join(sorted(_TABLE_SPECS))
        raise ValueError(f"market-data table must be one of: {allowed}") from error


def _validate_anchor(anchor: datetime) -> datetime:
    """Require an aware UTC midnight suitable for deterministic bounds.

    Args:
        anchor: Candidate datetime boundary.

    Returns:
        The same instant normalized to the canonical UTC timezone.

    Raises:
        ValueError: If timezone, time-of-day, or precision is unsafe.
    """
    if anchor.tzinfo is None or anchor.utcoffset() != timedelta(0):
        raise ValueError("partition anchor must be timezone-aware UTC")
    normalized = anchor.astimezone(UTC)
    if (
        normalized.hour,
        normalized.minute,
        normalized.second,
        normalized.microsecond,
    ) != (0, 0, 0, 0):
        raise ValueError("partition anchor must be an exact UTC midnight")
    return normalized


def _require_postgresql(connection: Connection) -> None:
    """Reject lifecycle use against SQLite or another SQL dialect.

    Args:
        connection: Candidate SQLAlchemy connection.

    Raises:
        DailyPartitionError: If the connection dialect is not PostgreSQL.
    """
    if connection.dialect.name != "postgresql":
        raise DailyPartitionError("daily market partitions require PostgreSQL")


def _require_public_schema(connection: Connection) -> None:
    """Require the migration's fixed ``public`` schema before catalog access.

    Args:
        connection: Candidate PostgreSQL connection.

    Raises:
        DailyPartitionError: If ``search_path`` resolves another current schema.
    """
    current = cast(str | None, connection.scalar(text("SELECT current_schema()")))
    if current != "public":
        raise DailyPartitionError(f"refused: current_schema() is {current!r}, expected 'public'")


def _require_clean_connection(connection: Connection) -> None:
    """Require mutation entry points to own their transaction boundaries.

    Args:
        connection: Candidate SQLAlchemy connection.

    Raises:
        DailyPartitionError: If caller work is already in progress.
    """
    if connection.in_transaction():
        raise DailyPartitionError(
            "daily partition mutations require a connection with no active transaction"
        )


def _relation_state(kind: str | None) -> RelationState:
    """Map PostgreSQL ``relkind`` to the lifecycle's closed state machine.

    Args:
        kind: PostgreSQL relation kind or no matching relation.

    Returns:
        Supported lifecycle state.
    """
    if kind is None:
        return RelationState.MISSING
    if kind == "r":
        return RelationState.ORDINARY
    if kind == "p":
        return RelationState.PARTITIONED
    return RelationState.OTHER


def _direct_partitions(
    connection: Connection,
    spec: TableSpec,
) -> tuple[PartitionRef, ...]:
    """Read every immediate table child and canonical partition bound.

    Args:
        connection: PostgreSQL catalog connection.
        spec: Parent table contract.

    Returns:
        Direct children ordered by name.
    """
    rows = connection.execute(
        text("""
            SELECT child_namespace.nspname::text,
                   child.relname::text,
                   pg_get_expr(child.relpartbound, child.oid),
                   child.relkind::text,
                   child.relispartition,
                   child_partitioning.partrelid IS NOT NULL,
                   inheritance.inhseqno::integer
            FROM pg_inherits AS inheritance
            JOIN pg_class AS parent ON parent.oid = inheritance.inhparent
            JOIN pg_namespace AS parent_ns ON parent_ns.oid = parent.relnamespace
            JOIN pg_class AS child ON child.oid = inheritance.inhrelid
            JOIN pg_namespace AS child_namespace
              ON child_namespace.oid = child.relnamespace
            LEFT JOIN pg_partitioned_table AS child_partitioning
              ON child_partitioning.partrelid = child.oid
            WHERE parent_ns.nspname = current_schema()
              AND parent.relname = :table
            ORDER BY child_namespace.nspname, child.relname
            """),
        {"table": spec.table},
    )
    return tuple(
        PartitionRef(
            schema=cast(str, row[0]),
            name=cast(str, row[1]),
            bound=cast(str, row[2]),
            relation_kind=cast(str, row[3]),
            is_partition=cast(bool, row[4]),
            has_partition_key=cast(bool, row[5]),
            inheritance_sequence=cast(int, row[6]),
        )
        for row in rows
    )


def _relation_has_rows(connection: Connection, relation: str) -> bool:
    """Probe row existence without materializing or counting a relation.

    Args:
        connection: PostgreSQL catalog connection.
        relation: Internally generated allowlisted child name.

    Returns:
        Whether at least one row exists.
    """
    return bool(connection.scalar(text(f"SELECT EXISTS (SELECT 1 FROM {relation} LIMIT 1)")))


def _future_leaf_count(
    spec: TableSpec,
    partitions: tuple[PartitionRef, ...],
    anchor: datetime,
) -> int:
    """Count correctly named daily leaves at or after one anchor.

    Args:
        spec: Parent table contract.
        partitions: Direct child catalog rows.
        anchor: First day classified as future.

    Returns:
        Number of future daily leaves.
    """
    prefix = f"{spec.table}_d"
    count = 0
    for partition in partitions:
        if not partition.name.startswith(prefix):
            continue
        suffix = partition.name.removeprefix(prefix)
        if len(suffix) != 8 or not suffix.isdigit():
            continue
        leaf_day = datetime.strptime(suffix, "%Y%m%d").date()
        if leaf_day >= anchor.date():
            count += 1
    return count


def _verify_desired_daily_bounds(
    spec: TableSpec,
    partitions: tuple[PartitionRef, ...],
    anchor: datetime,
) -> None:
    """Reject a desired leaf name that is attached with another range.

    Args:
        spec: Parent table contract.
        partitions: Direct child catalog rows.
        anchor: First day in the desired fourteen-leaf window.

    Raises:
        DailyPartitionError: If an existing desired name has a wrong bound.
    """
    by_name = {item.name: item for item in partitions}
    for offset in range(FUTURE_LEAF_TARGET):
        lower = anchor + timedelta(days=offset)
        name = _daily_name(spec, lower)
        existing = by_name.get(name)
        if existing is not None and not _range_bound_matches(
            existing.bound, lower, lower + timedelta(days=1)
        ):
            raise DailyPartitionError(f"refused: {name} exists with an unexpected partition bound")


def _verify_ordinary_schema(
    connection: Connection,
    spec: TableSpec,
    anchor: datetime,
    *,
    prepared: bool = False,
) -> None:
    """Fail closed unless the ordinary 0042 schema is exactly recognizable.

    Args:
        connection: PostgreSQL catalog connection.
        spec: Expected ordinary table contract.
        anchor: Expected bound for an optional resumed legacy CHECK.
        prepared: Whether the partition key has already been hardened.

    Raises:
        DailyPartitionError: If columns, CHECKs, or indexes drift from 0042.
    """
    _verify_adoption_names_available(connection, spec, anchor)
    _verify_relation_columns(
        connection,
        spec,
        spec.table,
        partition_key_not_null=prepared,
    )
    _verify_ordinary_checks(connection, spec)
    _verify_ordinary_indexes(connection, spec)
    role = ConstraintRole.PREPARED if prepared else ConstraintRole.ORDINARY
    _verify_noncheck_constraints(connection, spec, spec.table, role)
    range_info = _constraint_info(
        connection,
        spec.table,
        _range_constraint_name(spec),
    )
    if range_info is not None and not _range_constraint_matches(
        range_info.definition,
        spec,
        anchor,
    ):
        raise DailyPartitionError(
            f"refused: {_range_constraint_name(spec)} has an unexpected definition"
        )


def _verify_adoption_names_available(
    connection: Connection,
    spec: TableSpec,
    anchor: datetime,
) -> None:
    """Refuse every target relation-name collision before heavy preparation.

    Args:
        connection: PostgreSQL catalog connection.
        spec: Expected ordinary table contract.
        anchor: Exact first daily partition boundary.

    Raises:
        DailyPartitionError: If any cutover-created relation name already exists.
    """
    leaves = tuple(
        _daily_name(spec, anchor + timedelta(days=offset)) for offset in range(FUTURE_LEAF_TARGET)
    ) + (f"{spec.table}_default",)
    names = {
        f"{spec.table}_legacy",
        *(index.name for index in spec.parent_indexes),
        *leaves,
    }
    for leaf in leaves:
        names.add(f"{leaf}_pkey")
        names.update(f"{leaf}_{'_'.join(index.columns)}_idx" for index in spec.parent_indexes)
        if spec.local_public_id:
            names.add(f"{leaf}_public_id")
    statement = text("""
        SELECT relation.relname::text
        FROM pg_class AS relation
        JOIN pg_namespace AS namespace ON namespace.oid = relation.relnamespace
        WHERE namespace.nspname = current_schema()
          AND relation.relname IN :names
        ORDER BY relation.relname
        """).bindparams(bindparam("names", expanding=True))
    collisions = tuple(
        cast(str, name)
        for name in connection.execute(
            statement,
            {"names": tuple(sorted(names))},
        ).scalars()
    )
    if collisions:
        raise DailyPartitionError(
            f"refused: target partition relation names already exist: {collisions!r}"
        )


def _verify_prepared_ordinary_schema(
    connection: Connection,
    spec: TableSpec,
    anchor: datetime,
) -> None:
    """Revalidate the complete prepared source while holding its cutover lock.

    Args:
        connection: PostgreSQL catalog connection holding ACCESS EXCLUSIVE.
        spec: Expected prepared ordinary table contract.
        anchor: Exact validated legacy upper bound.

    Raises:
        DailyPartitionError: If the locked source differs from the prepared shape.
    """
    topology = inspect(connection, spec.table, anchor)
    if topology.state is not RelationState.ORDINARY:
        raise DailyPartitionError(
            f"refused: locked {spec.table} is not the expected ordinary table"
        )
    _verify_ordinary_schema(connection, spec, anchor, prepared=True)
    range_info = _constraint_info(
        connection,
        spec.table,
        _range_constraint_name(spec),
    )
    if (
        range_info is None
        or not range_info.validated
        or not _range_constraint_matches(range_info.definition, spec, anchor)
    ):
        raise DailyPartitionError(
            f"refused: {_range_constraint_name(spec)} is not the exact validated bound"
        )
    if spec.table == "trades":
        u3 = _index_shape(connection, spec.table, _trade_u3_contract().name)
        if u3 is None or not _ordinary_index_matches(u3, _trade_u3_contract()):
            raise DailyPartitionError(
                "refused: prepared trades U3 is absent or has an unexpected shape"
            )
    _verify_sequence_owner(connection, spec)


def _verify_relation_columns(
    connection: Connection,
    spec: TableSpec,
    relation: str,
    *,
    partition_key_not_null: bool,
) -> None:
    """Compare every column on one known relation to the exact table contract.

    Args:
        connection: PostgreSQL catalog connection.
        spec: Expected market-data column contract.
        relation: Parent, ordinary, or attached legacy relation name.
        partition_key_not_null: Whether preparation must have hardened the key.

    Raises:
        DailyPartitionError: If any column catalog property differs.
    """
    rows = connection.execute(
        text("""
            SELECT attribute.attname::text,
                   format_type(attribute.atttypid, attribute.atttypmod),
                   NOT attribute.attnotnull,
                   pg_get_expr(default_row.adbin, default_row.adrelid),
                   attribute.attidentity::text,
                   attribute.attgenerated::text
            FROM pg_attribute AS attribute
            JOIN pg_class AS relation ON relation.oid = attribute.attrelid
            JOIN pg_namespace AS namespace ON namespace.oid = relation.relnamespace
            LEFT JOIN pg_attrdef AS default_row
              ON default_row.adrelid = attribute.attrelid
             AND default_row.adnum = attribute.attnum
            WHERE namespace.nspname = current_schema()
              AND relation.relname = :table
              AND attribute.attnum > 0
              AND NOT attribute.attisdropped
            ORDER BY attribute.attnum
            """),
        {"table": relation},
    )
    observed = tuple(
        (
            cast(str, row[0]),
            cast(str, row[1]),
            cast(bool, row[2]),
            cast(str | None, row[3]),
            cast(str, row[4]),
            cast(str, row[5]),
        )
        for row in rows
    )
    expected = _ordinary_column_contract(spec)
    if len(observed) != len(expected):
        raise DailyPartitionError(
            f"refused: {relation} has {len(observed)} columns, expected {len(expected)}"
        )
    for actual, contract in zip(observed, expected, strict=True):
        name, data_type, nullable, default, identity, generated = actual
        if name != contract.name or data_type != contract.data_type:
            raise DailyPartitionError(
                f"refused: unexpected column {name} {data_type} on {relation}"
            )
        expected_nullable = contract.nullable
        if partition_key_not_null and name == spec.partition_key:
            expected_nullable = False
        if expected_nullable is not None and nullable != expected_nullable:
            raise DailyPartitionError(f"refused: unexpected nullability for {relation}.{name}")
        if not _column_default_matches(default, contract, spec):
            raise DailyPartitionError(f"refused: unexpected default for {relation}.{name}")
        if identity or generated:
            raise DailyPartitionError(
                f"refused: unexpected identity or generated state for {relation}.{name}"
            )


def _ordinary_column_contract(spec: TableSpec) -> tuple[ColumnContract, ...]:
    """Return the exact post-0042 column contract for one ordinary table.

    Args:
        spec: Expected ordinary table contract.

    Returns:
        Ordered column manifest including preparation-tolerant trades nullability.
    """
    common_start = (
        ColumnContract("id", "bigint", False, "sequence"),
        ColumnContract("public_id", "uuid", False),
        ColumnContract("instrument_public_id", "uuid", False),
    )
    common_end = (
        ColumnContract("session_id", "uuid", False),
        ColumnContract("sequence_id", "integer", False),
        ColumnContract("timestamp", _TIMESTAMPTZ, False),
        ColumnContract("known_to", _TIMESTAMPTZ, False),
    )
    if spec.table == "ticks":
        return (
            common_start
            + (
                ColumnContract("bid", _DOUBLE_PRECISION, True),
                ColumnContract("ask", _DOUBLE_PRECISION, True),
                ColumnContract("last", _DOUBLE_PRECISION, True),
                ColumnContract("volume", _DOUBLE_PRECISION, False),
            )
            + common_end
        )
    if spec.table == "candles":
        return (
            common_start
            + (
                ColumnContract("timeframe", "character varying(8)", False),
                ColumnContract("open_at", _TIMESTAMPTZ, False),
                ColumnContract("open", _DOUBLE_PRECISION, False),
                ColumnContract("high", _DOUBLE_PRECISION, False),
                ColumnContract("low", _DOUBLE_PRECISION, False),
                ColumnContract("close", _DOUBLE_PRECISION, False),
                ColumnContract("volume", _DOUBLE_PRECISION, False),
                ColumnContract("vwap", _DOUBLE_PRECISION, True),
                ColumnContract("trades", "integer", True),
            )
            + common_end
            + (
                ColumnContract("source", "character varying(16)", False, "native"),
                ColumnContract("complete", "boolean", False, "true"),
                ColumnContract("price_basis", "character varying(16)", True),
            )
        )
    return (
        common_start
        + (
            ColumnContract("trade_id", "character varying(64)", True),
            ColumnContract("price", _DOUBLE_PRECISION, False),
            ColumnContract("size", _DOUBLE_PRECISION, False),
            ColumnContract("side", "character varying(4)", False),
            ColumnContract("executed_at", _TIMESTAMPTZ, None),
        )
        + common_end
    )


def _column_default_matches(
    observed: str | None,
    contract: ColumnContract,
    spec: TableSpec,
) -> bool:
    """Compare one canonical default to its narrow expected category.

    Args:
        observed: Canonical ``pg_get_expr`` default or no default.
        contract: Expected column contract.
        spec: Owning table contract.

    Returns:
        Whether the default is absent or exactly recognizable.
    """
    if contract.default_kind is None:
        return observed is None
    if observed is None:
        return False
    compact = re.sub(r'[\s"]', "", observed).lower()
    if contract.default_kind == "sequence":
        return compact in {
            f"nextval('{spec.table}_id_seq'::regclass)",
            f"nextval('public.{spec.table}_id_seq'::regclass)",
        }
    if contract.default_kind == "native":
        return compact == "'native'::charactervarying"
    return compact == "true"


def _verify_ordinary_checks(
    connection: Connection,
    spec: TableSpec,
    relation: str | None = None,
    *,
    exclude_legacy_range: bool = True,
) -> None:
    """Require the exact validated ordinary CHECK manifest.

    Args:
        connection: PostgreSQL catalog connection.
        spec: Expected ordinary table contract.
        relation: Known relation name, defaulting to the ordinary table.
        exclude_legacy_range: Whether the prepared legacy bound is allowed.

    Raises:
        DailyPartitionError: If a CHECK is missing, extra, invalid, or malformed.
    """
    target = relation or spec.table
    checks = _ordinary_check_constraints(
        connection,
        spec,
        target,
        exclude_legacy_range=exclude_legacy_range,
    )
    expected_names = (
        ("ck_candle_source", "ck_candles_sequence_id")
        if spec.table == "candles"
        else (f"ck_{spec.table}_sequence_id",)
    )
    if tuple(name for name, definition in checks) != expected_names:
        raise DailyPartitionError(f"refused: {target} ordinary CHECK manifest drifted")
    definitions = dict(checks)
    sequence_name = f"ck_{spec.table}_sequence_id"
    if _compact_check(definitions[sequence_name]) != "checksequence_id>0":
        raise DailyPartitionError(f"refused: {sequence_name} has an unexpected definition")
    if spec.table == "candles":
        source = _compact_check(definitions["ck_candle_source"])
        expected = "checksource=anyarray['native','calculated','synthesized']"
        if source != expected:
            raise DailyPartitionError("refused: ck_candle_source has an unexpected definition")


def _compact_check(definition: str) -> str:
    """Normalize one known PostgreSQL CHECK rendering for exact comparison.

    Args:
        definition: Canonical ``pg_get_constraintdef`` output.

    Returns:
        Whitespace-, parenthesis-, quote-, and cast-free comparison text.
    """
    compact = re.sub(_COMPACT_STRIP_PATTERN, "", definition).lower()
    compact = compact.replace("::charactervarying", "")
    compact = compact.replace("::text[]", "")
    return compact.replace("::text", "")


def _verify_ordinary_indexes(connection: Connection, spec: TableSpec) -> None:
    """Require the exact ordinary index manifest, allowing prepared trades U3.

    Args:
        connection: PostgreSQL catalog connection.
        spec: Expected ordinary table contract.

    Raises:
        DailyPartitionError: If any index is missing, extra, invalid, or malformed.
    """
    contracts = _ordinary_index_contract(spec)
    names = _relation_index_names(connection, spec.table)
    expected = {contract.name for contract in contracts}
    optional_u3 = spec.table == "trades" and "uq_trade_instr_tid_exec" in names
    if optional_u3:
        expected.add("uq_trade_instr_tid_exec")
    if set(names) != expected or len(names) != len(expected):
        raise DailyPartitionError(f"refused: {spec.table} ordinary index manifest is {names!r}")
    for contract in contracts:
        shape = _index_shape(connection, spec.table, contract.name)
        if shape is None or not _ordinary_index_matches(shape, contract):
            raise DailyPartitionError(
                f"refused: ordinary index {contract.name} has an unexpected shape"
            )


def _relation_index_names(
    connection: Connection,
    relation: str,
) -> tuple[str, ...]:
    """Read the complete direct index manifest for one known relation.

    Args:
        connection: PostgreSQL catalog connection.
        relation: Parent, ordinary, legacy, or leaf relation name.

    Returns:
        Direct index relation names in stable lexical order.
    """
    rows = connection.execute(
        text("""
            SELECT index_relation.relname::text
            FROM pg_index AS index_row
            JOIN pg_class AS index_relation
              ON index_relation.oid = index_row.indexrelid
            JOIN pg_class AS table_relation
              ON table_relation.oid = index_row.indrelid
            JOIN pg_namespace AS namespace
              ON namespace.oid = table_relation.relnamespace
            WHERE namespace.nspname = current_schema()
              AND table_relation.relname = :table
            ORDER BY index_relation.relname
            """),
        {"table": relation},
    )
    return tuple(cast(str, row[0]) for row in rows)


def _ordinary_index_matches(
    shape: IndexShape,
    contract: OrdinaryIndexContract,
    parent_indexes: tuple[str, ...] = (),
) -> bool:
    """Compare an observed ordinary index to its exact static contract.

    Args:
        shape: Observed structural index shape.
        contract: Expected ordinary index shape.
        parent_indexes: Exact direct index parents expected in this state.

    Returns:
        Whether keys, flags, ownership, and predicate match.
    """
    if (
        shape.columns != contract.columns
        or shape.unique != contract.unique
        or shape.constraint_backed != contract.constraint_backed
        or not _index_storage_matches(shape, parent_indexes)
    ):
        return False
    if contract.active_predicate:
        return shape.predicate is not None and _active_predicate_matches(shape.predicate)
    return shape.predicate is None


def _index_storage_matches(
    shape: IndexShape,
    parent_indexes: tuple[str, ...],
) -> bool:
    """Require the physical properties needed for zero-build index adoption.

    Args:
        shape: Observed structural index shape.
        parent_indexes: Exact direct index parents expected in this state.

    Returns:
        Whether PostgreSQL can adopt this plain default-btree shape unchanged.
    """
    return (
        shape.access_method == "btree"
        and shape.valid
        and shape.ready
        and shape.live
        and shape.no_expressions
        and shape.no_include
        and shape.nulls_distinct
        and shape.default_options
        and shape.default_opclasses
        and shape.attribute_collations
        and shape.default_reloptions
        and shape.default_tablespace
        and shape.parent_indexes == parent_indexes
    )


def _ordinary_index_contract(
    spec: TableSpec,
) -> tuple[OrdinaryIndexContract, ...]:
    """Return the exact post-0042 ordinary index contract.

    Args:
        spec: Expected ordinary table contract.

    Returns:
        Ordered static index manifest excluding optional prepared trades U3.
    """
    primary = OrdinaryIndexContract(
        f"{spec.table}_pkey",
        ("id",),
        True,
        False,
        True,
    )
    if spec.table == "ticks":
        contracts = [
            primary,
            OrdinaryIndexContract(
                "ix_tick_instrument_ts",
                ("instrument_public_id", "timestamp"),
                False,
                False,
                False,
            ),
        ]
    elif spec.table == "candles":
        contracts = [
            primary,
            OrdinaryIndexContract(
                "ix_candle_instrument_open",
                ("instrument_public_id", "open_at"),
                False,
                False,
                False,
            ),
            OrdinaryIndexContract(
                "ix_candles_public_id",
                ("public_id",),
                True,
                True,
                False,
            ),
            OrdinaryIndexContract(
                "uq_candle_itf_open",
                ("instrument_public_id", "timeframe", "open_at"),
                True,
                True,
                False,
            ),
        ]
    else:
        contracts = [
            primary,
            OrdinaryIndexContract(
                "ix_trade_instrument_ts",
                ("instrument_public_id", "timestamp"),
                False,
                False,
                False,
            ),
            OrdinaryIndexContract(
                "ix_trades_executed_at",
                ("executed_at",),
                False,
                False,
                False,
            ),
            OrdinaryIndexContract(
                "ix_trades_public_id",
                ("public_id",),
                True,
                True,
                False,
            ),
            OrdinaryIndexContract(
                "ix_trades_timestamp",
                ("timestamp",),
                False,
                False,
                False,
            ),
            OrdinaryIndexContract(
                "uq_trade_instrument_trade_id",
                ("instrument_public_id", "trade_id"),
                True,
                False,
                True,
            ),
        ]
    return tuple(contracts)


def _verify_sequence_owner(connection: Connection, spec: TableSpec) -> None:
    """Require the exact bigint id sequence parameters and ownership edge.

    Args:
        connection: PostgreSQL catalog connection.
        spec: Ordinary table contract.

    Raises:
        DailyPartitionError: If sequence ownership cannot be transferred exactly.
    """
    exact = cast(
        bool,
        connection.scalar(
            text("""
                SELECT count(*) = 1
                FROM pg_class AS sequence_relation
                JOIN pg_namespace AS sequence_namespace
                  ON sequence_namespace.oid = sequence_relation.relnamespace
                JOIN pg_sequence AS sequence_parameters
                  ON sequence_parameters.seqrelid = sequence_relation.oid
                JOIN pg_depend AS dependency
                  ON dependency.classid = 'pg_class'::regclass
                 AND dependency.objid = sequence_relation.oid
                 AND dependency.objsubid = 0
                 AND dependency.refclassid = 'pg_class'::regclass
                 AND dependency.deptype = 'a'
                JOIN pg_class AS owner_relation
                  ON owner_relation.oid = dependency.refobjid
                JOIN pg_namespace AS owner_namespace
                  ON owner_namespace.oid = owner_relation.relnamespace
                JOIN pg_attribute AS owner_attribute
                  ON owner_attribute.attrelid = owner_relation.oid
                 AND owner_attribute.attnum = dependency.refobjsubid
                WHERE sequence_namespace.nspname = current_schema()
                  AND sequence_relation.relname = :sequence
                  AND sequence_relation.relkind = 'S'
                  AND owner_namespace.nspname = current_schema()
                  AND owner_relation.relname = :owner
                  AND owner_attribute.attname = 'id'
                  AND sequence_parameters.seqtypid = 'bigint'::regtype
                  AND sequence_parameters.seqstart = 1
                  AND sequence_parameters.seqincrement = 1
                  AND sequence_parameters.seqmax = 9223372036854775807
                  AND sequence_parameters.seqmin = 1
                  AND sequence_parameters.seqcache = 1
                  AND NOT sequence_parameters.seqcycle
                """),
            {
                "sequence": f"{spec.table}_id_seq",
                "owner": spec.table,
            },
        ),
    )
    if not exact:
        raise DailyPartitionError(
            f"refused: {spec.table}_id_seq parameters or ownership are not exact"
        )


def _ordinary_check_constraints(
    connection: Connection,
    spec: TableSpec,
    relation: str | None = None,
    *,
    exclude_legacy_range: bool = True,
) -> tuple[tuple[str, str], ...]:
    """Capture validated ordinary CHECKs for independent parent construction.

    Args:
        connection: PostgreSQL catalog connection.
        spec: Ordinary table contract.
        relation: Known relation name, defaulting to the ordinary table.
        exclude_legacy_range: Whether to suppress the prepared legacy bound.

    Returns:
        Constraint names and canonical definitions, excluding the legacy bound.

    Raises:
        DailyPartitionError: If an ordinary CHECK is not validated.
    """
    range_name = _range_constraint_name(spec)
    target = relation or spec.table
    rows = connection.execute(
        text("""
            SELECT constraint_row.conname::text,
                   pg_get_constraintdef(constraint_row.oid, true),
                   constraint_row.convalidated
            FROM pg_constraint AS constraint_row
            JOIN pg_class AS relation ON relation.oid = constraint_row.conrelid
            JOIN pg_namespace AS namespace ON namespace.oid = relation.relnamespace
            WHERE namespace.nspname = current_schema()
              AND relation.relname = :table
              AND constraint_row.contype = 'c'
            ORDER BY constraint_row.conname
            """),
        {"table": target},
    )
    checks: list[tuple[str, str]] = []
    for row in rows:
        name = cast(str, row[0])
        if exclude_legacy_range and name == range_name:
            continue
        if not cast(bool, row[2]):
            raise DailyPartitionError(
                f"refused: ordinary CHECK {name} on {target} is not validated"
            )
        checks.append((name, cast(str, row[1])))
    return tuple(checks)


def _verify_noncheck_constraints(
    connection: Connection,
    spec: TableSpec,
    relation: str,
    role: ConstraintRole,
) -> None:
    """Require exact NOT NULL, primary, and legacy U2 constraints.

    Args:
        connection: PostgreSQL catalog connection.
        spec: Expected market-data table contract.
        relation: Parent, ordinary, legacy, or daily relation name.
        role: Role selecting required local constraints and key nullability.

    Raises:
        DailyPartitionError: If a constraint is missing, extra, or malformed.
    """
    observed = _noncheck_constraints(connection, relation)
    expected = _expected_noncheck_constraints(spec, relation, role)
    if tuple(shape.name for shape in observed) != tuple(item[0] for item in expected):
        raise DailyPartitionError(f"refused: {relation} relation constraint manifest drifted")
    for shape, contract in zip(observed, expected, strict=True):
        name, kind, definition, index_name = contract
        if (
            shape.name != name
            or shape.kind != kind
            or _compact_key_constraint(shape.definition) != definition
            or not shape.validated
            or shape.deferrable
            or shape.initially_deferred
            or shape.index_name != index_name
        ):
            raise DailyPartitionError(
                f"refused: local constraint {name} on {relation} is malformed"
            )


def _noncheck_constraints(
    connection: Connection,
    relation: str,
) -> tuple[ConstraintShape, ...]:
    """Read every exact non-CHECK constraint on one known relation.

    Args:
        connection: PostgreSQL catalog connection.
        relation: Parent, ordinary, or attached legacy relation name.

    Returns:
        Stable catalog shapes ordered by constraint name.
    """
    rows = connection.execute(
        text("""
            SELECT constraint_row.conname::text,
                   constraint_row.contype::text,
                   pg_get_constraintdef(constraint_row.oid, true),
                   constraint_row.convalidated,
                   constraint_row.condeferrable,
                   constraint_row.condeferred,
                   index_relation.relname::text
            FROM pg_constraint AS constraint_row
            JOIN pg_class AS relation
              ON relation.oid = constraint_row.conrelid
            JOIN pg_namespace AS namespace
              ON namespace.oid = relation.relnamespace
            LEFT JOIN pg_class AS index_relation
              ON index_relation.oid = constraint_row.conindid
            WHERE namespace.nspname = current_schema()
              AND relation.relname = :table
              AND constraint_row.contype <> 'c'
            ORDER BY constraint_row.conname
            """),
        {"table": relation},
    )
    return tuple(
        ConstraintShape(
            name=cast(str, row[0]),
            kind=cast(str, row[1]),
            definition=cast(str, row[2]),
            validated=cast(bool, row[3]),
            deferrable=cast(bool, row[4]),
            initially_deferred=cast(bool, row[5]),
            index_name=cast(str | None, row[6]),
        )
        for row in rows
    )


def _expected_noncheck_constraints(
    spec: TableSpec,
    relation: str,
    role: ConstraintRole,
) -> tuple[tuple[str, str, str, str | None], ...]:
    """Return exact relation constraints ordered by stable constraint name.

    Args:
        spec: Expected market-data table contract.
        relation: Relation whose generated constraint names are expected.
        role: Role controlling partition-key nullability and local arbiters.

    Returns:
        Name, kind, compact definition, and backing index tuples.
    """
    local_prefix = relation if role is ConstraintRole.LEAF else spec.table
    expected: list[tuple[str, str, str, str | None]] = []
    for column in _ordinary_column_contract(spec):
        required = column.nullable is False
        if column.name == spec.partition_key and role is not ConstraintRole.ORDINARY:
            required = True
        if required:
            expected.append(
                (
                    f"{spec.table}_{column.name}_not_null",
                    "n",
                    f"notnull{column.name}",
                    None,
                )
            )
    if role is ConstraintRole.PARENT:
        return tuple(sorted(expected))
    primary = (
        f"{local_prefix}_pkey",
        "p",
        "primarykeyid",
        f"{local_prefix}_pkey",
    )
    expected.append(primary)
    if spec.table == "trades" and role is not ConstraintRole.LEAF:
        expected.append(
            (
                "uq_trade_instrument_trade_id",
                "u",
                "uniqueinstrument_public_id,trade_id",
                "uq_trade_instrument_trade_id",
            )
        )
    return tuple(sorted(expected))


def _compact_key_constraint(definition: str) -> str:
    """Normalize one primary or unique definition for exact comparison.

    Args:
        definition: Canonical ``pg_get_constraintdef`` output.

    Returns:
        Whitespace-, quote-, and parenthesis-free definition.
    """
    return re.sub(_COMPACT_STRIP_PATTERN, "", definition).lower()


def _trade_u3_statement(connection: Connection, spec: TableSpec) -> str:
    """Return missing trades U3 DDL after validating any existing index.

    Args:
        connection: PostgreSQL catalog connection.
        spec: Ordinary table contract.

    Returns:
        Concurrent U3 creation SQL, or an empty string when already exact.

    Raises:
        DailyPartitionError: If the named index exists with another shape.
    """
    if spec.table != "trades":
        return ""
    name = "uq_trade_instr_tid_exec"
    shape = _index_shape(connection, spec.table, name)
    if shape is None:
        return (
            "CREATE UNIQUE INDEX CONCURRENTLY uq_trade_instr_tid_exec "
            "ON trades (instrument_public_id, trade_id, executed_at)"
        )
    if not _ordinary_index_matches(shape, _trade_u3_contract()):
        raise DailyPartitionError(
            f"refused: existing {name} does not match the standalone U3 contract"
        )
    return ""


def _trade_u3_contract() -> OrdinaryIndexContract:
    """Return the standalone trades U3 legacy index contract.

    Returns:
        Exact pre-ATTACH U3 structure.
    """
    return OrdinaryIndexContract(
        "uq_trade_instr_tid_exec",
        ("instrument_public_id", "trade_id", "executed_at"),
        True,
        False,
        False,
    )


def _range_preparation_statements(
    connection: Connection,
    spec: TableSpec,
    anchor: datetime,
) -> tuple[str, ...]:
    """Plan only the missing legacy-bound preparation statements.

    Args:
        connection: PostgreSQL catalog connection.
        spec: Ordinary table contract.
        anchor: Exclusive legacy upper bound.

    Returns:
        Ordered ADD, VALIDATE, and NOT NULL statements still required.

    Raises:
        DailyPartitionError: If an existing named range CHECK has another shape.
    """
    name = _range_constraint_name(spec)
    info = _constraint_info(connection, spec.table, name)
    statements: list[str] = []
    if info is None:
        statements.append(_add_range_constraint_sql(spec, anchor))
        statements.append(f"ALTER TABLE {spec.table} VALIDATE CONSTRAINT {name}")
    else:
        if not _range_constraint_matches(info.definition, spec, anchor):
            raise DailyPartitionError(f"refused: existing {name} has an unexpected definition")
        if not info.validated:
            statements.append(f"ALTER TABLE {spec.table} VALIDATE CONSTRAINT {name}")
    if _column_is_nullable(connection, spec.table, spec.partition_key):
        statements.append(
            f"ALTER TABLE {spec.table} ALTER COLUMN "
            f"{_quote_identifier(spec.partition_key)} SET NOT NULL"
        )
    return tuple(statements)


def _cutover_statements(
    spec: TableSpec,
    anchor: datetime,
    ordinary_checks: tuple[tuple[str, str], ...],
) -> tuple[str, ...]:
    """Build the independent atomic manual-adoption DDL sequence.

    Args:
        spec: Ordinary table contract.
        anchor: Exclusive legacy bound and first daily lower bound.
        ordinary_checks: Validated non-range CHECKs copied to the parent.

    Returns:
        Ordered rename, parent, leaf, ATTACH, and sequence DDL.

    Raises:
        DailyPartitionError: If the ordinary table has no recognized CHECKs.
    """
    if not ordinary_checks:
        raise DailyPartitionError(
            f"refused: {spec.table} has no ordinary CHECK constraints to preserve"
        )
    legacy = f"{spec.table}_legacy"
    key = _quote_identifier(spec.partition_key)
    statements = [
        f"ALTER TABLE {spec.table} RENAME TO {legacy}",
        (
            f"CREATE TABLE {spec.table} "
            f"(LIKE {legacy} INCLUDING DEFAULTS INCLUDING GENERATED "
            f"INCLUDING STORAGE INCLUDING COMPRESSION) "
            f"PARTITION BY RANGE ({key})"
        ),
    ]
    statements.extend(_parent_check_statements(spec))
    statements.extend(_parent_index_sql(spec, index) for index in spec.parent_indexes)
    for offset in range(FUTURE_LEAF_TARGET):
        statements.extend(_daily_leaf_statements(spec, anchor + timedelta(days=offset)))
    statements.extend(_default_leaf_statements(spec))
    statements.append(
        f"ALTER TABLE {spec.table} ATTACH PARTITION {legacy} "
        f"FOR VALUES FROM (MINVALUE) TO ({_timestamp_literal(anchor)})"
    )
    statements.append(f"ALTER SEQUENCE {spec.table}_id_seq OWNED BY {spec.table}.id")
    return tuple(statements)


def _parent_check_statements(spec: TableSpec) -> tuple[str, ...]:
    """Render ordinary parent CHECKs from raw independent source expressions.

    Re-executing ``pg_get_constraintdef`` output is unsafe for the candle
    vocabulary CHECK because PostgreSQL deparses ``IN`` as a cast-bearing
    ``ANY`` expression whose reparsed tree differs from the legacy constraint.

    Args:
        spec: Parent table contract.

    Returns:
        Exact raw CHECK creation DDL matching the 0042 expression trees.
    """
    sequence_statement = (
        f"ALTER TABLE {spec.table} ADD CONSTRAINT "
        f"ck_{spec.table}_sequence_id CHECK (sequence_id > 0)"
    )
    statements = [sequence_statement]
    if spec.table == "candles":
        statements.append(
            "ALTER TABLE candles ADD CONSTRAINT ck_candle_source "
            "CHECK (source IN ('native', 'calculated', 'synthesized'))"
        )
    return tuple(statements)


def _daily_leaf_statements(
    spec: TableSpec,
    lower: datetime,
) -> tuple[str, ...]:
    """Build one daily child and its leaf-local guarantees.

    Args:
        spec: Parent table contract.
        lower: Inclusive UTC lower bound.

    Returns:
        Ordered PARTITION OF, local PK, and optional partial-index DDL.
    """
    upper = lower + timedelta(days=1)
    leaf = _daily_name(spec, lower)
    create_statement = (
        f"CREATE TABLE {leaf} PARTITION OF {spec.table} "
        f"FOR VALUES FROM ({_timestamp_literal(lower)}) "
        f"TO ({_timestamp_literal(upper)})"
    )
    statements = [
        create_statement,
        f"ALTER TABLE {leaf} ADD CONSTRAINT {leaf}_pkey PRIMARY KEY (id)",
    ]
    if spec.local_public_id:
        statements.append(_local_public_id_sql(leaf))
    return tuple(statements)


def _default_leaf_statements(spec: TableSpec) -> tuple[str, ...]:
    """Build the zero-row DEFAULT anomaly buffer and local guarantees.

    Args:
        spec: Parent table contract.

    Returns:
        Ordered DEFAULT, local PK, and optional partial-index DDL.
    """
    leaf = f"{spec.table}_default"
    statements = [
        f"CREATE TABLE {leaf} PARTITION OF {spec.table} DEFAULT",
        f"ALTER TABLE {leaf} ADD CONSTRAINT {leaf}_pkey PRIMARY KEY (id)",
    ]
    if spec.local_public_id:
        statements.append(_local_public_id_sql(leaf))
    return tuple(statements)


def _parent_index_sql(spec: TableSpec, index: IndexSpec) -> str:
    """Render one standalone partitioned-parent index.

    Args:
        spec: Parent table contract.
        index: Exact parent index contract.

    Returns:
        ``CREATE INDEX`` SQL that never creates a UNIQUE constraint.
    """
    unique = "UNIQUE " if index.unique else ""
    columns = ", ".join(_quote_identifier(column) for column in index.columns)
    predicate = f" WHERE {index.predicate}" if index.predicate else ""
    return f"CREATE {unique}INDEX {index.name} ON {spec.table} ({columns}){predicate}"


def _local_public_id_sql(leaf: str) -> str:
    """Render one leaf-local active-public-id partial unique index.

    Args:
        leaf: Internally generated daily or DEFAULT relation name.

    Returns:
        Exact standalone partial unique-index DDL.
    """
    return (
        f"CREATE UNIQUE INDEX {leaf}_public_id ON {leaf} (public_id) "
        "WHERE known_to = TIMESTAMPTZ '9999-12-31 23:59:59+00:00'"
    )


def _daily_name(spec: TableSpec, lower: datetime) -> str:
    """Return the canonical relation name for one UTC event day.

    Args:
        spec: Parent table contract.
        lower: Daily UTC lower bound.

    Returns:
        Stable ``{table}_dYYYYMMDD`` relation name.
    """
    return f"{spec.table}_d{lower.strftime('%Y%m%d')}"


def _range_constraint_name(spec: TableSpec) -> str:
    """Return the stable legacy range-CHECK name.

    Args:
        spec: Parent table contract.

    Returns:
        Stable legacy-only constraint name.
    """
    return f"ck_{spec.table}_legacy_range"


def _add_range_constraint_sql(spec: TableSpec, anchor: datetime) -> str:
    """Render the NOT VALID implication proof used by legacy ATTACH.

    Args:
        spec: Ordinary table contract.
        anchor: Exclusive legacy upper bound.

    Returns:
        Exact CHECK creation DDL.
    """
    key = _quote_identifier(spec.partition_key)
    name = _range_constraint_name(spec)
    return (
        f"ALTER TABLE {spec.table} ADD CONSTRAINT {name} "
        f"CHECK ({key} IS NOT NULL AND {key} < {_timestamp_literal(anchor)}) "
        "NOT VALID"
    )


def _timestamp_literal(value: datetime) -> str:
    """Render one already-validated UTC timestamp with an explicit offset.

    Args:
        value: UTC-aware timestamp.

    Returns:
        PostgreSQL ``TIMESTAMPTZ`` literal.
    """
    normalized = value.astimezone(UTC).isoformat(timespec="seconds")
    return f"TIMESTAMPTZ '{normalized}'"


def _quote_identifier(value: str) -> str:
    """Quote a trusted or catalog-sourced PostgreSQL identifier.

    Args:
        value: Identifier text.

    Returns:
        Double-quoted SQL identifier.
    """
    return '"' + value.replace('"', '""') + '"'


def _constraint_info(
    connection: Connection,
    table: str,
    constraint: str,
) -> ConstraintInfo | None:
    """Read one named CHECK's canonical definition and validation state.

    Args:
        connection: PostgreSQL catalog connection.
        table: Allowlisted ordinary or legacy relation.
        constraint: Expected constraint name.

    Returns:
        Catalog information or no matching constraint.
    """
    row = connection.execute(
        text("""
            SELECT pg_get_constraintdef(constraint_row.oid, true),
                   constraint_row.convalidated
            FROM pg_constraint AS constraint_row
            JOIN pg_class AS relation ON relation.oid = constraint_row.conrelid
            JOIN pg_namespace AS namespace ON namespace.oid = relation.relnamespace
            WHERE namespace.nspname = current_schema()
              AND relation.relname = :table
              AND constraint_row.conname = :constraint
              AND constraint_row.contype = 'c'
            """),
        {"table": table, "constraint": constraint},
    ).one_or_none()
    if row is None:
        return None
    return ConstraintInfo(
        definition=cast(str, row[0]),
        validated=cast(bool, row[1]),
    )


def _range_constraint_matches(
    definition: str,
    spec: TableSpec,
    anchor: datetime,
) -> bool:
    """Compare a canonical CHECK to the exact legacy implication contract.

    Args:
        definition: ``pg_get_constraintdef`` output.
        spec: Expected table contract.
        anchor: Expected exclusive upper bound.

    Returns:
        Whether key nullability and the sole timestamp bound match exactly.
    """
    literals = re.findall(_QUOTED_LITERAL_PATTERN, definition)
    if len(literals) != 1:
        return False
    try:
        literal = datetime.fromisoformat(literals[0].replace(" ", "T"))
    except ValueError:
        return False
    if literal.tzinfo is None or literal.astimezone(UTC) != anchor:
        return False
    replaced = definition.replace(f"'{literals[0]}'", "'anchor'")
    compact = re.sub(_COMPACT_STRIP_PATTERN, "", replaced).lower()
    compact = compact.replace("::timestampwithtimezone", "")
    expected = f"check{spec.partition_key.lower()}isnotnulland{spec.partition_key.lower()}<'anchor'"
    return compact == expected


def _column_is_nullable(
    connection: Connection,
    table: str,
    column: str,
) -> bool:
    """Read one column's PostgreSQL NOT NULL catalog bit.

    Args:
        connection: PostgreSQL catalog connection.
        table: Allowlisted ordinary relation.
        column: Expected partition-key column.

    Returns:
        Whether the column currently permits NULL.

    Raises:
        DailyPartitionError: If the expected column is absent.
    """
    not_null = cast(
        bool | None,
        connection.scalar(
            text("""
                SELECT attribute.attnotnull
                FROM pg_attribute AS attribute
                JOIN pg_class AS relation ON relation.oid = attribute.attrelid
                JOIN pg_namespace AS namespace ON namespace.oid = relation.relnamespace
                WHERE namespace.nspname = current_schema()
                  AND relation.relname = :table
                  AND attribute.attname = :column
                  AND attribute.attnum > 0
                  AND NOT attribute.attisdropped
                """),
            {"table": table, "column": column},
        ),
    )
    if not_null is None:
        raise DailyPartitionError(f"refused: expected column {table}.{column} is absent")
    return not not_null


def _index_shape(
    connection: Connection,
    table: str,
    index: str,
) -> IndexShape | None:
    """Read structural index properties without comparing formatted DDL.

    Args:
        connection: PostgreSQL catalog connection.
        table: Expected owning relation.
        index: Expected index relation name.

    Returns:
        Structural shape or no matching index.
    """
    row = connection.execute(
        text("""
            SELECT index_row.indisunique,
                   index_row.indisvalid,
                   index_row.indisready,
                   index_row.indislive,
                   pg_get_expr(index_row.indpred, index_row.indrelid),
                   ARRAY(
                       SELECT pg_get_indexdef(
                           index_row.indexrelid,
                           key_position,
                           true
                       )
                       FROM generate_series(
                           1,
                           index_row.indnkeyatts
                       ) AS key_position
                       ORDER BY key_position
                   ),
                   EXISTS (
                       SELECT 1
                       FROM pg_constraint AS constraint_row
                       WHERE constraint_row.conindid = index_row.indexrelid
                   ),
                   access_method.amname::text,
                   index_row.indexprs IS NULL,
                   index_row.indnatts = index_row.indnkeyatts,
                   NOT index_row.indnullsnotdistinct,
                   NOT EXISTS (
                       SELECT 1
                       FROM generate_series(
                           0,
                           index_row.indnkeyatts - 1
                       ) AS key_position
                       WHERE index_row.indoption[key_position] <> 0
                   ),
                   NOT EXISTS (
                       SELECT 1
                       FROM generate_series(
                           0,
                           index_row.indnkeyatts - 1
                       ) AS key_position
                       LEFT JOIN pg_opclass AS operator_class
                         ON operator_class.oid =
                            index_row.indclass[key_position]
                       WHERE NOT COALESCE(
                           operator_class.opcdefault
                           AND operator_class.opcmethod =
                               index_relation.relam,
                           false
                       )
                   ),
                   NOT EXISTS (
                       SELECT 1
                       FROM generate_series(
                           0,
                           index_row.indnkeyatts - 1
                       ) AS key_position
                       LEFT JOIN pg_attribute AS key_attribute
                         ON key_attribute.attrelid = index_row.indrelid
                        AND key_attribute.attnum =
                            index_row.indkey[key_position]
                       WHERE key_attribute.attnum IS NULL
                          OR index_row.indcollation[key_position] <>
                             key_attribute.attcollation
                   ),
                   index_relation.reloptions IS NULL,
                   index_relation.reltablespace = 0,
                   ARRAY(
                       SELECT parent_index.relname::text
                       FROM pg_inherits AS index_inheritance
                       JOIN pg_class AS parent_index
                         ON parent_index.oid =
                            index_inheritance.inhparent
                       WHERE index_inheritance.inhrelid =
                             index_row.indexrelid
                       ORDER BY parent_index.relname
                   )
            FROM pg_index AS index_row
            JOIN pg_class AS index_relation
              ON index_relation.oid = index_row.indexrelid
            JOIN pg_am AS access_method
              ON access_method.oid = index_relation.relam
            JOIN pg_class AS table_relation
              ON table_relation.oid = index_row.indrelid
            JOIN pg_namespace AS namespace
              ON namespace.oid = table_relation.relnamespace
            WHERE namespace.nspname = current_schema()
              AND table_relation.relname = :table
              AND index_relation.relname = :index
            """),
        {"table": table, "index": index},
    ).one_or_none()
    if row is None:
        return None
    return IndexShape(
        unique=cast(bool, row[0]),
        valid=cast(bool, row[1]),
        ready=cast(bool, row[2]),
        live=cast(bool, row[3]),
        columns=tuple(_normalize_index_expression(value) for value in cast(list[str], row[5])),
        predicate=cast(str | None, row[4]),
        constraint_backed=cast(bool, row[6]),
        access_method=cast(str, row[7]),
        no_expressions=cast(bool, row[8]),
        no_include=cast(bool, row[9]),
        nulls_distinct=cast(bool, row[10]),
        default_options=cast(bool, row[11]),
        default_opclasses=cast(bool, row[12]),
        attribute_collations=cast(bool, row[13]),
        default_reloptions=cast(bool, row[14]),
        default_tablespace=cast(bool, row[15]),
        parent_indexes=tuple(cast(list[str], row[16])),
    )


def _normalize_index_expression(value: str) -> str:
    """Normalize PostgreSQL quoting around one simple index key.

    Args:
        value: ``pg_get_indexdef`` key expression.

    Returns:
        Unquoted simple identifier or the unchanged complex expression.
    """
    if re.fullmatch(r'"[^"]+"', value) is not None:
        return value[1:-1].replace('""', '"')
    return value


def _create_trade_u3_concurrently(
    connection: Connection,
    statement: str,
) -> None:
    """Build the populated legacy trades arbiter without blocking writers.

    Args:
        connection: Clean caller connection whose engine supplies a sibling.
        statement: Exact concurrent standalone U3 creation DDL.
    """
    with connection.engine.connect().execution_options(isolation_level="AUTOCOMMIT") as concurrent:
        concurrent.execute(text("SET TIME ZONE 'UTC'"))
        concurrent.execute(text(f"SET statement_timeout = '{INDEX_BUILD_STATEMENT_TIMEOUT}'"))
        concurrent.execute(text(f"SET maintenance_work_mem = '{INDEX_BUILD_MAINTENANCE_WORK_MEM}'"))
        concurrent.execute(text("SET max_parallel_maintenance_workers = 0"))
        concurrent.execute(text(statement))


def _prepare_legacy_range(
    connection: Connection,
    spec: TableSpec,
    anchor: datetime,
) -> None:
    """Add, validate, and apply the legacy implication proof in safe stages.

    Args:
        connection: Clean PostgreSQL connection.
        spec: Ordinary table contract.
        anchor: Exclusive legacy upper bound.
    """
    statements = _range_preparation_statements(connection, spec, anchor)
    connection.rollback()
    for statement in statements:
        if " VALIDATE CONSTRAINT " in statement:
            _execute_validation_transaction(connection, statement)
        else:
            _execute_transaction(connection, (statement,))


def _execute_transaction(
    connection: Connection,
    statements: tuple[str, ...],
) -> None:
    """Execute an ordered DDL stage with UTC and bounded lock acquisition.

    Args:
        connection: Clean PostgreSQL connection.
        statements: Ordered SQL statements in one atomic transaction.
    """
    with connection.begin():
        connection.execute(text(_SET_LOCAL_TIME_ZONE_UTC))
        connection.execute(text(f"SET LOCAL lock_timeout = '{LOCK_TIMEOUT}'"))
        connection.execute(text(f"SET LOCAL statement_timeout = '{CUTOVER_STATEMENT_TIMEOUT}'"))
        for statement in statements:
            connection.execute(text(statement))


def _execute_cutover(
    connection: Connection,
    spec: TableSpec,
    anchor: datetime,
    statements: tuple[str, ...],
) -> PartitionInspection:
    """Lock, revalidate, cut over, and verify before the transaction commits.

    Args:
        connection: Clean PostgreSQL connection.
        spec: Prepared ordinary table contract.
        anchor: Exact legacy upper partition bound.
        statements: Ordered atomic cutover DDL.

    Returns:
        Verified partitioned topology observed inside the cutover transaction.
    """
    with connection.begin():
        connection.execute(text(_SET_LOCAL_TIME_ZONE_UTC))
        connection.execute(text(f"SET LOCAL lock_timeout = '{LOCK_TIMEOUT}'"))
        connection.execute(text(f"SET LOCAL statement_timeout = '{CUTOVER_STATEMENT_TIMEOUT}'"))
        connection.execute(text(f"LOCK TABLE {spec.table} IN ACCESS EXCLUSIVE MODE"))
        _verify_prepared_ordinary_schema(connection, spec, anchor)
        for statement in statements:
            connection.execute(text(statement))
        return _verify_partitioned(connection, spec, anchor)


def _execute_validation_transaction(
    connection: Connection,
    statement: str,
) -> None:
    """Validate a populated legacy bound under an explicit work ceiling.

    Args:
        connection: Clean PostgreSQL connection.
        statement: Exact legacy CHECK validation DDL.
    """
    with connection.begin():
        connection.execute(text(_SET_LOCAL_TIME_ZONE_UTC))
        connection.execute(text(f"SET LOCAL lock_timeout = '{LOCK_TIMEOUT}'"))
        connection.execute(text(f"SET LOCAL statement_timeout = '{VALIDATION_STATEMENT_TIMEOUT}'"))
        connection.execute(text(statement))


def _execute_ensure(
    connection: Connection,
    spec: TableSpec,
    statements: tuple[str, ...],
) -> None:
    """Create future leaves only after locking and proving DEFAULT empty.

    Args:
        connection: Clean PostgreSQL connection.
        spec: Partitioned parent contract.
        statements: Ordered missing-leaf DDL.

    Raises:
        DailyPartitionError: If DEFAULT detached or gained a row before the lock.
    """
    with connection.begin():
        connection.execute(text(_SET_LOCAL_TIME_ZONE_UTC))
        connection.execute(text(f"SET LOCAL lock_timeout = '{LOCK_TIMEOUT}'"))
        connection.execute(text(f"SET LOCAL statement_timeout = '{CUTOVER_STATEMENT_TIMEOUT}'"))
        connection.execute(text(f"LOCK TABLE {spec.table} IN ACCESS EXCLUSIVE MODE"))
        default_name = f"{spec.table}_default"
        default_ref = next(
            (
                partition
                for partition in _direct_partitions(connection, spec)
                if partition.name == default_name
            ),
            None,
        )
        if (
            default_ref is None
            or default_ref.schema != "public"
            or default_ref.relation_kind != "r"
            or not default_ref.is_partition
            or default_ref.has_partition_key
            or default_ref.inheritance_sequence != 1
            or default_ref.bound.strip().upper() != "DEFAULT"
        ):
            raise DailyPartitionError(f"refused: {default_name} is no longer attached as DEFAULT")
        if _relation_has_rows(connection, default_name):
            raise DailyPartitionError(
                f"refused: {default_name} contains rows and must be drained first"
            )
        for statement in statements:
            connection.execute(text(statement))


def _execute_detach_attempt(
    connection: Connection,
    spec: TableSpec,
    leaf: str,
    lower: datetime,
) -> None:
    """Lock and revalidate one exact daily child before plain DETACH.

    Args:
        connection: Clean PostgreSQL connection.
        spec: Partitioned parent contract.
        leaf: Expected generated daily relation name.
        lower: Exact inclusive UTC day boundary.

    Raises:
        DailyPartitionError: If the locked attachment or bound has changed.
    """
    with connection.begin():
        connection.execute(text(_SET_LOCAL_TIME_ZONE_UTC))
        connection.execute(text(f"SET LOCAL lock_timeout = '{LOCK_TIMEOUT}'"))
        connection.execute(text(f"SET LOCAL statement_timeout = '{CUTOVER_STATEMENT_TIMEOUT}'"))
        connection.execute(text(f"LOCK TABLE {spec.table} IN ACCESS EXCLUSIVE MODE"))
        observed = next(
            (
                partition
                for partition in _direct_partitions(connection, spec)
                if partition.name == leaf
            ),
            None,
        )
        if (
            observed is None
            or observed.schema != "public"
            or observed.relation_kind != "r"
            or not observed.is_partition
            or observed.has_partition_key
            or observed.inheritance_sequence != 1
            or not _range_bound_matches(
                observed.bound,
                lower,
                lower + timedelta(days=1),
            )
        ):
            raise DailyPartitionError(
                f"refused: locked attachment for {leaf} no longer has its exact daily bound"
            )
        connection.execute(text(f"ALTER TABLE {spec.table} DETACH PARTITION {leaf}"))


def _detach_with_retries(
    connection: Connection,
    spec: TableSpec,
    leaf: str,
    lower: datetime,
) -> None:
    """Run plain DETACH under a fixed lock timeout and bounded retries.

    Args:
        connection: Clean PostgreSQL connection.
        spec: Partitioned parent contract.
        leaf: Expected generated daily relation name.
        lower: Exact inclusive UTC day boundary.

    Raises:
        DailyPartitionError: If a non-lock error occurs or retries are exhausted.
    """
    for attempt in range(DETACH_RETRIES):
        try:
            _execute_detach_attempt(connection, spec, leaf, lower)
            return
        except DBAPIError as error:
            if _sqlstate(error) != "55P03" or attempt == DETACH_RETRIES - 1:
                raise DailyPartitionError(
                    f"plain DETACH failed after {attempt + 1} attempt(s): {error}"
                ) from error
            time.sleep(0.25 * (attempt + 1))
    raise DailyPartitionError("plain DETACH exhausted its bounded retry loop")


def _sqlstate(error: DBAPIError) -> str | None:
    """Extract PostgreSQL SQLSTATE without depending on one DBAPI driver.

    Args:
        error: SQLAlchemy-wrapped database failure.

    Returns:
        Five-character SQLSTATE when exposed by the driver.
    """
    for attribute in ("sqlstate", "pgcode"):
        value = getattr(error.orig, attribute, None)
        if isinstance(value, str) and re.fullmatch(r"[0-9A-Z]{5}", value) is not None:
            return value
    return None


def _require_partitioned_topology(
    connection: Connection,
    spec: TableSpec,
    anchor: datetime,
) -> None:
    """Require an exact adopted parent before steady-state lifecycle DDL.

    Args:
        connection: PostgreSQL catalog connection.
        spec: Expected parent contract.
        anchor: Original legacy and initial daily boundary.

    Raises:
        DailyPartitionError: If the relation is ordinary or topology is ambiguous.
    """
    topology = inspect(connection, spec.table, anchor)
    if topology.state is not RelationState.PARTITIONED:
        raise DailyPartitionError(f"refused: {spec.table} is not a partitioned parent")
    expected_key = f"range({spec.partition_key})"
    actual_key = re.sub(r'[\s"]', "", topology.partition_key or "").lower()
    if actual_key != expected_key:
        raise DailyPartitionError(
            f"refused: {spec.table} partition key is {topology.partition_key!r}"
        )
    _verify_direct_partition_shapes(spec, topology)
    _verify_parent_indexes(connection, spec)


def _verify_direct_partition_shapes(
    spec: TableSpec,
    topology: PartitionInspection,
) -> None:
    """Reject foreign, non-leaf, or nonpartition direct children.

    Args:
        spec: Expected parent contract.
        topology: Direct-child catalog inspection.

    Raises:
        DailyPartitionError: If any child is outside the exact leaf shape.
    """
    for partition in topology.partitions:
        if (
            partition.schema != "public"
            or partition.relation_kind != "r"
            or not partition.is_partition
            or partition.has_partition_key
            or partition.inheritance_sequence != 1
        ):
            raise DailyPartitionError(
                f"refused: {spec.table} direct child "
                f"{partition.schema}.{partition.name} has an unexpected relation shape"
            )


def _verify_partitioned(
    connection: Connection,
    spec: TableSpec,
    anchor: datetime,
) -> PartitionInspection:
    """Verify the complete required adoption topology before a no-op or return.

    Args:
        connection: PostgreSQL catalog connection.
        spec: Expected parent contract.
        anchor: Original legacy and first daily boundary.

    Returns:
        Fully verified partitioned topology.

    Raises:
        DailyPartitionError: If any required child, bound, index, or local object drifts.
    """
    _require_partitioned_topology(connection, spec, anchor)
    topology = inspect(connection, spec.table, anchor)
    expected_names = {
        f"{spec.table}_legacy",
        f"{spec.table}_default",
        *(
            _daily_name(spec, anchor + timedelta(days=offset))
            for offset in range(FUTURE_LEAF_TARGET)
        ),
    }
    observed_names = tuple(partition.name for partition in topology.partitions)
    if set(observed_names) != expected_names or len(observed_names) != len(expected_names):
        raise DailyPartitionError(
            f"refused: {spec.table} direct partition manifest is {observed_names!r}"
        )
    by_name = {item.name: item for item in topology.partitions}
    legacy = f"{spec.table}_legacy"
    legacy_ref = by_name.get(legacy)
    if legacy_ref is None or not _range_bound_matches(legacy_ref.bound, None, anchor):
        raise DailyPartitionError(f"refused: {legacy} is missing or has an unexpected bound")
    default_name = f"{spec.table}_default"
    default_ref = by_name.get(default_name)
    if default_ref is None or default_ref.bound.strip().upper() != "DEFAULT":
        raise DailyPartitionError(f"refused: {default_name} is missing or is not DEFAULT")
    for offset in range(FUTURE_LEAF_TARGET):
        lower = anchor + timedelta(days=offset)
        leaf = _daily_name(spec, lower)
        ref = by_name.get(leaf)
        if ref is None or not _range_bound_matches(ref.bound, lower, lower + timedelta(days=1)):
            raise DailyPartitionError(f"refused: {leaf} is missing or has an unexpected bound")
        _verify_leaf_local_objects(connection, spec, leaf)
    _verify_leaf_local_objects(connection, spec, default_name)
    _verify_legacy_index_parentage(connection, spec, legacy)
    _verify_partitioned_relation_shapes(connection, spec, legacy, anchor)
    _verify_sequence_owner(connection, spec)
    return topology


def _verify_legacy_index_parentage(
    connection: Connection,
    spec: TableSpec,
    legacy: str,
) -> None:
    """Require each matching legacy index to be adopted by its exact parent.

    Args:
        connection: PostgreSQL catalog connection.
        spec: Expected parent table contract.
        legacy: Attached legacy relation name.

    Raises:
        DailyPartitionError: If an edge or adoptable index shape differs.
    """
    for contract, parent in _legacy_parent_index_contracts(spec):
        shape = _index_shape(connection, legacy, contract.name)
        if shape is None or not _ordinary_index_matches(
            shape,
            contract,
            (parent,),
        ):
            raise DailyPartitionError(
                f"refused: legacy index {contract.name} was not adopted by {parent}"
            )


def _verify_partitioned_relation_shapes(
    connection: Connection,
    spec: TableSpec,
    legacy: str,
    anchor: datetime,
) -> None:
    """Require exact parent and legacy columns, checks, and local objects.

    Args:
        connection: PostgreSQL catalog connection.
        spec: Expected partitioned table contract.
        legacy: Attached legacy relation name.
        anchor: Exact legacy upper range bound.

    Raises:
        DailyPartitionError: If either relation has catalog drift.
    """
    _verify_relation_columns(
        connection,
        spec,
        spec.table,
        partition_key_not_null=True,
    )
    _verify_relation_columns(
        connection,
        spec,
        legacy,
        partition_key_not_null=True,
    )
    _verify_ordinary_checks(connection, spec, spec.table)
    _verify_ordinary_checks(connection, spec, legacy)
    parent_range = _constraint_info(
        connection,
        spec.table,
        _range_constraint_name(spec),
    )
    if parent_range is not None:
        raise DailyPartitionError(
            f"refused: partitioned parent carries {_range_constraint_name(spec)}"
        )
    legacy_range = _constraint_info(
        connection,
        legacy,
        _range_constraint_name(spec),
    )
    if (
        legacy_range is None
        or not legacy_range.validated
        or not _range_constraint_matches(legacy_range.definition, spec, anchor)
    ):
        raise DailyPartitionError(f"refused: {legacy} lacks the exact validated legacy range CHECK")
    _verify_noncheck_constraints(connection, spec, spec.table, ConstraintRole.PARENT)
    _verify_noncheck_constraints(connection, spec, legacy, ConstraintRole.LEGACY)
    _verify_legacy_index_manifest(connection, spec, legacy)
    _verify_legacy_local_objects(connection, spec, legacy)


def _verify_legacy_index_manifest(
    connection: Connection,
    spec: TableSpec,
    legacy: str,
) -> None:
    """Require every and only the expected legacy index relation names.

    Args:
        connection: PostgreSQL catalog connection.
        spec: Expected parent table contract.
        legacy: Attached legacy relation name.

    Raises:
        DailyPartitionError: If any legacy index is missing, extra, or duplicated.
    """
    expected = {contract.name for contract in _ordinary_index_contract(spec)}
    if spec.table == "trades":
        expected.add(_trade_u3_contract().name)
    names = _relation_index_names(connection, legacy)
    if set(names) != expected or len(names) != len(expected):
        raise DailyPartitionError(f"refused: {legacy} index manifest is {names!r}")


def _verify_legacy_local_objects(
    connection: Connection,
    spec: TableSpec,
    legacy: str,
) -> None:
    """Require exact legacy-only PK, partial, and trades U2 indexes.

    Args:
        connection: PostgreSQL catalog connection.
        spec: Expected parent table contract.
        legacy: Attached legacy relation name.

    Raises:
        DailyPartitionError: If a local legacy index is missing or malformed.
    """
    contracts = {contract.name: contract for contract in _ordinary_index_contract(spec)}
    local_names = [f"{spec.table}_pkey"]
    if spec.local_public_id:
        local_names.append(f"ix_{spec.table}_public_id")
    if spec.table == "trades":
        local_names.append("uq_trade_instrument_trade_id")
    for name in local_names:
        contract = contracts[name]
        shape = _index_shape(connection, legacy, name)
        if shape is None or not _ordinary_index_matches(shape, contract):
            raise DailyPartitionError(f"refused: legacy local index {name} has an unexpected shape")


def _legacy_parent_index_contracts(
    spec: TableSpec,
) -> tuple[tuple[OrdinaryIndexContract, str], ...]:
    """Map each adoptable legacy index to its exact partitioned parent.

    Args:
        spec: Expected parent table contract.

    Returns:
        Legacy contracts paired with required direct parent index names.
    """
    contracts = {contract.name: contract for contract in _ordinary_index_contract(spec)}
    if spec.table == "ticks":
        parent_contracts = [(contracts["ix_tick_instrument_ts"], "ticks_p_ix_instr_ts")]
    elif spec.table == "candles":
        parent_contracts = [
            (
                contracts["uq_candle_itf_open"],
                "candles_p_uq_itf_open",
            ),
            (
                contracts["ix_candle_instrument_open"],
                "candles_p_ix_instr_open",
            ),
        ]
    else:
        parent_contracts = [
            (
                _trade_u3_contract(),
                "trades_p_uq_instr_tid_exec",
            ),
            (
                contracts["ix_trade_instrument_ts"],
                "trades_p_ix_instr_ts",
            ),
            (
                contracts["ix_trades_timestamp"],
                "trades_p_ix_ts",
            ),
            (
                contracts["ix_trades_executed_at"],
                "trades_p_ix_exec",
            ),
        ]
    return tuple(parent_contracts)


def _verify_parent_indexes(connection: Connection, spec: TableSpec) -> None:
    """Verify exact standalone parent index names and structural definitions.

    Args:
        connection: PostgreSQL catalog connection.
        spec: Expected parent contract.

    Raises:
        DailyPartitionError: If the parent index manifest drifts.
    """
    rows = connection.execute(
        text("""
            SELECT index_relation.relname::text
            FROM pg_index AS index_row
            JOIN pg_class AS index_relation
              ON index_relation.oid = index_row.indexrelid
            JOIN pg_class AS table_relation
              ON table_relation.oid = index_row.indrelid
            JOIN pg_namespace AS namespace
              ON namespace.oid = table_relation.relnamespace
            WHERE namespace.nspname = current_schema()
              AND table_relation.relname = :table
            ORDER BY index_relation.relname
            """),
        {"table": spec.table},
    )
    names = tuple(cast(str, row[0]) for row in rows)
    expected_names = tuple(sorted(index.name for index in spec.parent_indexes))
    if tuple(sorted(names)) != expected_names:
        raise DailyPartitionError(f"refused: {spec.table} parent index manifest is {names!r}")
    for expected in spec.parent_indexes:
        shape = _index_shape(connection, spec.table, expected.name)
        if shape is None or not _index_matches(shape, expected):
            raise DailyPartitionError(
                f"refused: parent index {expected.name} has an unexpected shape"
            )


def _index_matches(
    shape: IndexShape,
    expected: IndexSpec,
    parent_indexes: tuple[str, ...] = (),
) -> bool:
    """Compare one structural catalog index to its static contract.

    Args:
        shape: Observed catalog shape.
        expected: Static parent index definition.
        parent_indexes: Exact direct index parents required for this relation.

    Returns:
        Whether uniqueness, validity, keys, predicate, and ownership all match.
    """
    if (
        shape.unique != expected.unique
        or shape.columns != expected.columns
        or shape.constraint_backed
        or not _index_storage_matches(shape, parent_indexes)
    ):
        return False
    if expected.predicate is None:
        return shape.predicate is None
    return shape.predicate is not None and _active_predicate_matches(shape.predicate)


def _active_predicate_matches(predicate: str) -> bool:
    """Recognize only the exact active-row ``known_to`` predicate.

    Args:
        predicate: Canonical ``pg_get_expr`` predicate.

    Returns:
        Whether the predicate is exactly ``known_to = active infinity``.
    """
    literals = re.findall(_QUOTED_LITERAL_PATTERN, predicate)
    if len(literals) != 1:
        return False
    try:
        value = datetime.fromisoformat(literals[0].replace(" ", "T"))
    except ValueError:
        return False
    if value.tzinfo is None or value.astimezone(UTC) != _ACTIVE_KNOWN_TO:
        return False
    replaced = predicate.replace(f"'{literals[0]}'", "'active'")
    compact = re.sub(_COMPACT_STRIP_PATTERN, "", replaced).lower()
    compact = compact.replace("::timestampwithtimezone", "")
    return compact == "known_to='active'"


def _verify_leaf_local_objects(
    connection: Connection,
    spec: TableSpec,
    leaf: str,
) -> None:
    """Require each leaf-local PK and active-public-id partial.

    Args:
        connection: PostgreSQL catalog connection.
        spec: Expected parent contract.
        leaf: Expected daily or DEFAULT child name.

    Raises:
        DailyPartitionError: If a local object is absent or malformed.
    """
    _verify_relation_columns(
        connection,
        spec,
        leaf,
        partition_key_not_null=True,
    )
    _verify_ordinary_checks(
        connection,
        spec,
        leaf,
        exclude_legacy_range=False,
    )
    _verify_noncheck_constraints(connection, spec, leaf, ConstraintRole.LEAF)
    _verify_leaf_index_manifest(connection, spec, leaf)
    primary = _index_shape(connection, leaf, f"{leaf}_pkey")
    if (
        primary is None
        or not primary.unique
        or primary.columns != ("id",)
        or not primary.constraint_backed
        or not _index_storage_matches(primary, ())
    ):
        raise DailyPartitionError(f"refused: {leaf} local primary key is malformed")
    if not spec.local_public_id:
        return
    public_id = _index_shape(connection, leaf, f"{leaf}_public_id")
    if (
        public_id is None
        or not public_id.unique
        or public_id.columns != ("public_id",)
        or public_id.constraint_backed
        or public_id.predicate is None
        or not _active_predicate_matches(public_id.predicate)
        or not _index_storage_matches(public_id, ())
    ):
        raise DailyPartitionError(f"refused: {leaf} active-public-id partial is malformed")


def _verify_leaf_index_manifest(
    connection: Connection,
    spec: TableSpec,
    leaf: str,
) -> None:
    """Require every local and inherited leaf index, with exact parentage.

    Args:
        connection: PostgreSQL catalog connection.
        spec: Expected parent contract.
        leaf: Expected daily or DEFAULT child name.

    Raises:
        DailyPartitionError: If names, definitions, or parent edges drift.
    """
    inherited = {f"{leaf}_{'_'.join(index.columns)}_idx": index for index in spec.parent_indexes}
    expected_names = set(inherited)
    expected_names.add(f"{leaf}_pkey")
    if spec.local_public_id:
        expected_names.add(f"{leaf}_public_id")
    names = _relation_index_names(connection, leaf)
    if set(names) != expected_names or len(names) != len(expected_names):
        raise DailyPartitionError(f"refused: {leaf} index manifest is {names!r}")
    for child_name, parent in inherited.items():
        shape = _index_shape(connection, leaf, child_name)
        if shape is None or not _index_matches(shape, parent, (parent.name,)):
            raise DailyPartitionError(
                f"refused: {child_name} is not the exact child of {parent.name}"
            )


def _range_bound_matches(
    bound: str,
    lower: datetime | None,
    upper: datetime,
) -> bool:
    """Compare ``pg_get_expr`` range output to exact UTC boundaries.

    Args:
        bound: Canonical PostgreSQL partition-bound expression.
        lower: Inclusive lower bound, or MINVALUE for legacy.
        upper: Exclusive upper bound.

    Returns:
        Whether the catalog bound exactly matches.
    """
    values = re.findall(_QUOTED_LITERAL_PATTERN, bound)
    expected_count = 1 if lower is None else 2
    if len(values) != expected_count:
        return False
    parsed: list[datetime] = []
    for value in values:
        try:
            item = datetime.fromisoformat(value.replace(" ", "T"))
        except ValueError:
            return False
        if item.tzinfo is None:
            return False
        parsed.append(item.astimezone(UTC))
    if lower is None:
        return "MINVALUE" in bound.upper() and parsed == [upper]
    return parsed == [lower, upper]
