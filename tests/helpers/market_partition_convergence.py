"""PostgreSQL 18.4 convergence support for daily market partitions.

The helper creates one private, Unix-socket-only scratch cluster and two
databases. The manual database reaches revision 0042 before calling the public
runtime adoption API for each market table. The fresh database reaches 0001
before Alembic independently constructs the final topology through 0043.

Catalog rows are normalized into immutable tuples without retaining OIDs,
physical file identifiers, or Alembic's version row. Mutation helpers alter
one throwaway branch at a time inside caller-owned transactions. Two
mutation probes write PostgreSQL catalogs directly because PostgreSQL has no
supported command for detaching one inherited index or changing an index key
in place. The private cluster enables those writes, and every test rolls them
back before yielding control.
"""

import os
import shutil
import signal
import subprocess
import tempfile
import time
from argparse import Namespace
from collections.abc import Callable
from collections.abc import Generator
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC
from datetime import datetime
from enum import StrEnum
from pathlib import Path
from typing import Final

import sqlalchemy as sa
from alembic import command
from alembic.config import Config
from sqlalchemy.engine import Connection
from sqlalchemy.engine import Engine
from sqlalchemy.engine import RowMapping

from snapper.data.daily_partitions import DailyPartitionError
from snapper.data.daily_partitions import adopt

type RelationRow = tuple[str, str, bool, str | None, str | None]
type InheritanceRow = tuple[str, str, int]
type PartitionBoundRow = tuple[str, str]
type IndexRow = tuple[str, str, str, bool, bool, bool, str | None]
type ConstraintRow = tuple[str, str, str, str, bool, bool, bool]
type ColumnRow = tuple[str, int, str, str, bool, str | None, str, str]
type SequenceRow = tuple[
    str,
    str,
    int,
    int,
    int,
    int,
    bool,
    int,
    str | None,
    str | None,
    str | None,
    str | None,
]

_PROJECT_ROOT = Path(__file__).resolve().parents[2]
_ALEMBIC_INI = _PROJECT_ROOT / "alembic.ini"
_PG_BIN = Path("/usr/lib/postgresql/18/bin")
_PG_ROLE = "snapper_partition_test"
_PG_PORT = 65439
_HOST_CORES = 4
_MAX_LOAD_PER_CORE = 1.5
_LOAD_WAIT_SECONDS = 45.0
_LOAD_POLL_SECONDS = 1.0
_ANCHOR_TEXT = "2026-07-30T00:00:00+00:00"
_ANCHOR = datetime(2026, 7, 30, tzinfo=UTC)
_ROOT_TABLES: Final[tuple[str, ...]] = ("ticks", "candles", "trades")
_DATABASE_ENVIRONMENT: Final[tuple[str, ...]] = (
    "DATABASE_URL",
    "DB_URL",
    "PGDATABASE",
    "PGHOST",
    "PGPASSFILE",
    "PGPORT",
    "PGSERVICE",
    "PGUSER",
    "TEST_DB_URL",
)

_TOPOLOGY_CTE = """
WITH RECURSIVE topology(oid) AS (
    SELECT relation.oid
    FROM pg_class AS relation
    JOIN pg_namespace AS namespace ON namespace.oid = relation.relnamespace
    WHERE namespace.nspname = 'public'
      AND relation.relname IN ('ticks', 'candles', 'trades')
      AND relation.relkind IN ('r', 'p')
    UNION
    SELECT
        CASE
            WHEN inheritance.inhparent = topology.oid THEN inheritance.inhrelid
            ELSE inheritance.inhparent
        END
    FROM pg_inherits AS inheritance
    JOIN topology
      ON topology.oid = inheritance.inhparent
      OR topology.oid = inheritance.inhrelid
)
"""

_INDEX_GRAPH_CTE = _TOPOLOGY_CTE + """
, selected_indexes(index_oid) AS (
    SELECT index_catalog.indexrelid
    FROM pg_index AS index_catalog
    JOIN topology ON topology.oid = index_catalog.indrelid
    UNION
    SELECT
        CASE
            WHEN inheritance.inhparent = selected_indexes.index_oid
                THEN inheritance.inhrelid
            ELSE inheritance.inhparent
        END
    FROM pg_inherits AS inheritance
    JOIN selected_indexes
      ON selected_indexes.index_oid = inheritance.inhparent
      OR selected_indexes.index_oid = inheritance.inhrelid
    JOIN pg_index AS endpoint
      ON endpoint.indexrelid = CASE
          WHEN inheritance.inhparent = selected_indexes.index_oid
              THEN inheritance.inhrelid
          ELSE inheritance.inhparent
      END
)
"""

_RELATIONS_SQL = _INDEX_GRAPH_CTE + """
, selected_relations(oid) AS (
    SELECT oid FROM topology
    UNION
    SELECT index_oid FROM selected_indexes
    UNION
    SELECT sequence.oid
    FROM pg_class AS sequence
    JOIN pg_namespace AS namespace ON namespace.oid = sequence.relnamespace
    WHERE namespace.nspname = 'public'
      AND sequence.relname IN ('ticks_id_seq', 'candles_id_seq', 'trades_id_seq')
      AND sequence.relkind = 'S'
)
SELECT
    relation.relname AS relation_name,
    relation.relkind::text AS relation_kind,
    relation.relispartition,
    partitioned.partstrat::text AS partition_strategy,
    CASE
        WHEN relation.relkind = 'p' THEN pg_get_partkeydef(relation.oid)
        ELSE NULL
    END AS partition_key
FROM selected_relations
JOIN pg_class AS relation ON relation.oid = selected_relations.oid
LEFT JOIN pg_partitioned_table AS partitioned ON partitioned.partrelid = relation.oid
ORDER BY relation.relname
"""

_TABLE_INHERITANCE_SQL = _TOPOLOGY_CTE + """
SELECT
    parent.relname AS parent_name,
    child.relname AS child_name,
    inheritance.inhseqno
FROM pg_inherits AS inheritance
JOIN pg_class AS parent ON parent.oid = inheritance.inhparent
JOIN pg_class AS child ON child.oid = inheritance.inhrelid
JOIN topology AS parent_topology ON parent_topology.oid = parent.oid
JOIN topology AS child_topology ON child_topology.oid = child.oid
ORDER BY parent.relname, child.relname, inheritance.inhseqno
"""

_PARTITION_BOUNDS_SQL = _TOPOLOGY_CTE + """
SELECT
    relation.relname AS relation_name,
    pg_get_expr(relation.relpartbound, relation.oid, true) AS partition_bound
FROM topology
JOIN pg_class AS relation ON relation.oid = topology.oid
WHERE relation.relispartition
ORDER BY relation.relname
"""

_INDEXES_SQL = _INDEX_GRAPH_CTE + """
SELECT
    table_relation.relname AS table_name,
    index_relation.relname AS index_name,
    pg_get_indexdef(index_relation.oid) AS index_definition,
    index_catalog.indisprimary,
    index_catalog.indisunique,
    index_catalog.indisvalid,
    pg_get_expr(index_catalog.indpred, index_catalog.indrelid, true) AS predicate
FROM selected_indexes
JOIN pg_index AS index_catalog ON index_catalog.indexrelid = selected_indexes.index_oid
JOIN pg_class AS table_relation ON table_relation.oid = index_catalog.indrelid
JOIN pg_class AS index_relation ON index_relation.oid = index_catalog.indexrelid
ORDER BY table_relation.relname, index_relation.relname
"""

_INDEX_INHERITANCE_SQL = _INDEX_GRAPH_CTE + """
SELECT
    parent_index.relname AS parent_name,
    child_index.relname AS child_name,
    inheritance.inhseqno
FROM pg_inherits AS inheritance
JOIN pg_class AS parent_index ON parent_index.oid = inheritance.inhparent
JOIN pg_class AS child_index ON child_index.oid = inheritance.inhrelid
JOIN pg_index AS parent_catalog ON parent_catalog.indexrelid = parent_index.oid
JOIN pg_index AS child_catalog ON child_catalog.indexrelid = child_index.oid
JOIN selected_indexes AS parent_selected
  ON parent_selected.index_oid = parent_catalog.indexrelid
JOIN selected_indexes AS child_selected
  ON child_selected.index_oid = child_catalog.indexrelid
ORDER BY parent_index.relname, child_index.relname, inheritance.inhseqno
"""

_CONSTRAINTS_SQL = _TOPOLOGY_CTE + """
SELECT
    table_relation.relname AS table_name,
    constraint_catalog.conname AS constraint_name,
    constraint_catalog.contype::text AS constraint_type,
    pg_get_constraintdef(constraint_catalog.oid, true) AS constraint_definition,
    constraint_catalog.convalidated,
    constraint_catalog.condeferrable,
    constraint_catalog.condeferred
FROM topology
JOIN pg_class AS table_relation ON table_relation.oid = topology.oid
JOIN pg_constraint AS constraint_catalog ON constraint_catalog.conrelid = table_relation.oid
ORDER BY table_relation.relname, constraint_catalog.conname
"""

_COLUMNS_SQL = _TOPOLOGY_CTE + """
SELECT
    table_relation.relname AS table_name,
    attribute.attnum AS ordinal,
    attribute.attname AS column_name,
    format_type(attribute.atttypid, attribute.atttypmod) AS column_type,
    NOT attribute.attnotnull AS nullable,
    pg_get_expr(default_catalog.adbin, default_catalog.adrelid, true) AS column_default,
    attribute.attidentity::text AS identity_state,
    attribute.attgenerated::text AS generated_state
FROM topology
JOIN pg_class AS table_relation ON table_relation.oid = topology.oid
JOIN pg_attribute AS attribute ON attribute.attrelid = table_relation.oid
LEFT JOIN pg_attrdef AS default_catalog
    ON default_catalog.adrelid = attribute.attrelid
   AND default_catalog.adnum = attribute.attnum
WHERE attribute.attnum > 0
  AND NOT attribute.attisdropped
ORDER BY table_relation.relname, attribute.attnum
"""

_SEQUENCES_SQL = """
SELECT
    sequence_relation.relname AS sequence_name,
    format_type(sequence_catalog.seqtypid, NULL) AS data_type,
    sequence_catalog.seqstart,
    sequence_catalog.seqmin,
    sequence_catalog.seqmax,
    sequence_catalog.seqincrement,
    sequence_catalog.seqcycle,
    sequence_catalog.seqcache,
    owned_namespace.nspname AS owned_schema,
    owned_table.relname AS owned_table,
    owned_column.attname AS owned_column,
    ownership.deptype::text AS dependency_type
FROM pg_class AS sequence_relation
JOIN pg_namespace AS namespace ON namespace.oid = sequence_relation.relnamespace
JOIN pg_sequence AS sequence_catalog ON sequence_catalog.seqrelid = sequence_relation.oid
LEFT JOIN pg_depend AS ownership
    ON ownership.classid = 'pg_class'::regclass
   AND ownership.objid = sequence_relation.oid
   AND ownership.objsubid = 0
   AND ownership.refclassid = 'pg_class'::regclass
   AND ownership.refobjsubid > 0
   AND ownership.deptype IN ('a', 'i')
LEFT JOIN pg_class AS owned_table ON owned_table.oid = ownership.refobjid
LEFT JOIN pg_namespace AS owned_namespace
    ON owned_namespace.oid = owned_table.relnamespace
LEFT JOIN pg_attribute AS owned_column
    ON owned_column.attrelid = ownership.refobjid
   AND owned_column.attnum = ownership.refobjsubid
WHERE namespace.nspname = 'public'
  AND sequence_relation.relname IN ('ticks_id_seq', 'candles_id_seq', 'trades_id_seq')
  AND sequence_relation.relkind = 'S'
ORDER BY
    sequence_relation.relname,
    owned_namespace.nspname,
    owned_table.relname,
    owned_column.attname
"""


class ScratchClusterUnavailableError(RuntimeError):
    """Report that the exact private PostgreSQL fixture cannot be built."""


class UnsafeHostLoadError(RuntimeError):
    """Refuse scratch-cluster work while the four-core host is too busy."""


class CatalogMutation(StrEnum):
    """Name each required adversarial change to one catalog branch."""

    PARTITION_BOUND = "partition-bound"
    LOCAL_ACTIVE_PUBLIC_ID_PARTIAL = "local-active-public-id-partial"
    INDEX_NAME = "index-name"
    INDEX_PARENTAGE = "index-parentage"
    PARTITION_KEY_NULLABILITY = "partition-key-nullability"
    TRADE_U3_DEFINITION = "trade-u3-definition"
    SEQUENCE_OWNERSHIP = "sequence-ownership"


@dataclass(frozen=True)
class CatalogFingerprint:
    """Store every logical catalog feature in deterministic relation-name order."""

    relations: tuple[RelationRow, ...]
    table_inheritance: tuple[InheritanceRow, ...]
    partition_bounds: tuple[PartitionBoundRow, ...]
    indexes: tuple[IndexRow, ...]
    index_inheritance: tuple[InheritanceRow, ...]
    constraints: tuple[ConstraintRow, ...]
    columns: tuple[ColumnRow, ...]
    sequences: tuple[SequenceRow, ...]


@dataclass(frozen=True)
class RefusalEvidence:
    """Record fail-closed observations made before the manual branch converges."""

    populated_revision: str
    populated_root_kinds: tuple[tuple[str, str, bool], ...]
    populated_legacy_relations: tuple[str, ...]
    runtime_constraint_error: str
    runtime_constraint_name: str
    runtime_constraint_restored: bool
    runtime_sequence_error: str
    runtime_sequence_cache: int
    runtime_sequence_restored: bool
    runtime_leaf_error: str
    runtime_leaf_index_name: str
    runtime_leaf_restored: bool
    unexpected_partition_revision: str
    unexpected_partition_error: str
    unexpected_partition: tuple[str, str, str, bool, str, str, str]
    unexpected_partition_removed: bool
    storage_drift_revision: str
    storage_drift_error: str
    storage_drift_reloptions: str
    storage_drift_restored: bool
    malformed_revision: str
    malformed_index_name: str


@dataclass(frozen=True)
class PostgresBranches:
    """Expose both converged scratch engines and their pre-adoption refusal proof."""

    manual: Engine
    fresh: Engine
    refusals: RefusalEvidence


@dataclass(frozen=True)
class _PostgresTools:
    """Hold the exact PostgreSQL 18.4 programs used by the private fixture."""

    initdb: Path
    createdb: Path
    postgres: Path
    pg_isready: Path


def _required_text(value: object) -> str:
    """Normalize a required textual catalog value.

    Args:
        value: Raw driver value.

    Returns:
        Text with catalog formatting whitespace collapsed.

    Raises:
        TypeError: If PostgreSQL returned a non-text value.
    """
    if not isinstance(value, str):
        raise TypeError(f"expected catalog text, received {type(value).__name__}")
    return " ".join(value.split())


def _optional_text(value: object) -> str | None:
    """Normalize an optional textual catalog value.

    Args:
        value: Raw driver value.

    Returns:
        Normalized text or ``None``.
    """
    if value is None:
        return None
    return _required_text(value)


def _integer(value: object) -> int:
    """Return a catalog integer without accepting booleans.

    Args:
        value: Raw driver value.

    Returns:
        Integer value.

    Raises:
        TypeError: If PostgreSQL returned a non-integer value.
    """
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"expected catalog integer, received {type(value).__name__}")
    return value


def _boolean(value: object) -> bool:
    """Return a catalog boolean without truthiness coercion.

    Args:
        value: Raw driver value.

    Returns:
        Boolean value.

    Raises:
        TypeError: If PostgreSQL returned a non-boolean value.
    """
    if not isinstance(value, bool):
        raise TypeError(f"expected catalog boolean, received {type(value).__name__}")
    return value


def _rows(connection: Connection, statement: str) -> list[RowMapping]:
    """Execute one static catalog query and return mapping rows.

    Args:
        connection: Scratch PostgreSQL connection.
        statement: Static catalog SQL.

    Returns:
        Materialized mapping rows.
    """
    return list(connection.exec_driver_sql(statement).mappings())


def _relations(connection: Connection) -> tuple[RelationRow, ...]:
    """Collect relation identity and partitioning metadata."""
    return tuple(
        (
            _required_text(row["relation_name"]),
            _required_text(row["relation_kind"]),
            _boolean(row["relispartition"]),
            _optional_text(row["partition_strategy"]),
            _optional_text(row["partition_key"]),
        )
        for row in _rows(connection, _RELATIONS_SQL)
    )


def _inheritance(connection: Connection, statement: str) -> tuple[InheritanceRow, ...]:
    """Collect table or index ``pg_inherits`` edges."""
    return tuple(
        (
            _required_text(row["parent_name"]),
            _required_text(row["child_name"]),
            _integer(row["inhseqno"]),
        )
        for row in _rows(connection, statement)
    )


def _partition_bounds(connection: Connection) -> tuple[PartitionBoundRow, ...]:
    """Collect every named child partition bound."""
    return tuple(
        (
            _required_text(row["relation_name"]),
            _required_text(row["partition_bound"]),
        )
        for row in _rows(connection, _PARTITION_BOUNDS_SQL)
    )


def _indexes(connection: Connection) -> tuple[IndexRow, ...]:
    """Collect full index definitions, flags, and predicates."""
    return tuple(
        (
            _required_text(row["table_name"]),
            _required_text(row["index_name"]),
            _required_text(row["index_definition"]),
            _boolean(row["indisprimary"]),
            _boolean(row["indisunique"]),
            _boolean(row["indisvalid"]),
            _optional_text(row["predicate"]),
        )
        for row in _rows(connection, _INDEXES_SQL)
    )


def _constraints(connection: Connection) -> tuple[ConstraintRow, ...]:
    """Collect all constraint names, definitions, and enforcement flags."""
    return tuple(
        (
            _required_text(row["table_name"]),
            _required_text(row["constraint_name"]),
            _required_text(row["constraint_type"]),
            _required_text(row["constraint_definition"]),
            _boolean(row["convalidated"]),
            _boolean(row["condeferrable"]),
            _boolean(row["condeferred"]),
        )
        for row in _rows(connection, _CONSTRAINTS_SQL)
    )


def _columns(connection: Connection) -> tuple[ColumnRow, ...]:
    """Collect logical column shape, defaults, identity, and generation state."""
    return tuple(
        (
            _required_text(row["table_name"]),
            _integer(row["ordinal"]),
            _required_text(row["column_name"]),
            _required_text(row["column_type"]),
            _boolean(row["nullable"]),
            _optional_text(row["column_default"]),
            _required_text(row["identity_state"]),
            _required_text(row["generated_state"]),
        )
        for row in _rows(connection, _COLUMNS_SQL)
    )


def _sequences(connection: Connection) -> tuple[SequenceRow, ...]:
    """Collect sequence parameters and exact owned-column dependencies."""
    return tuple(
        (
            _required_text(row["sequence_name"]),
            _required_text(row["data_type"]),
            _integer(row["seqstart"]),
            _integer(row["seqmin"]),
            _integer(row["seqmax"]),
            _integer(row["seqincrement"]),
            _boolean(row["seqcycle"]),
            _integer(row["seqcache"]),
            _optional_text(row["owned_schema"]),
            _optional_text(row["owned_table"]),
            _optional_text(row["owned_column"]),
            _optional_text(row["dependency_type"]),
        )
        for row in _rows(connection, _SEQUENCES_SQL)
    )


def catalog_fingerprint(connection: Connection) -> CatalogFingerprint:
    """Build one complete normalized market-partition catalog fingerprint.

    Args:
        connection: Scratch PostgreSQL connection.

    Returns:
        Immutable catalog fingerprint.
    """
    connection.exec_driver_sql("SET LOCAL TIME ZONE 'UTC'")
    return CatalogFingerprint(
        relations=_relations(connection),
        table_inheritance=_inheritance(connection, _TABLE_INHERITANCE_SQL),
        partition_bounds=_partition_bounds(connection),
        indexes=_indexes(connection),
        index_inheritance=_inheritance(connection, _INDEX_INHERITANCE_SQL),
        constraints=_constraints(connection),
        columns=_columns(connection),
        sequences=_sequences(connection),
    )


def fingerprint_sections(
    fingerprint: CatalogFingerprint,
) -> tuple[tuple[str, object], ...]:
    """Return comparator sections in their fail-fast diagnostic order.

    Args:
        fingerprint: Catalog fingerprint to expose.

    Returns:
        Named immutable sections.
    """
    return (
        ("relations", fingerprint.relations),
        ("table_inheritance", fingerprint.table_inheritance),
        ("partition_bounds", fingerprint.partition_bounds),
        ("indexes", fingerprint.indexes),
        ("index_inheritance", fingerprint.index_inheritance),
        ("constraints", fingerprint.constraints),
        ("columns", fingerprint.columns),
        ("sequences", fingerprint.sequences),
    )


def assert_catalogs_identical(
    manual: CatalogFingerprint,
    fresh: CatalogFingerprint,
) -> None:
    """Assert exact equality and name the first differing catalog section.

    Args:
        manual: Runtime-adoption branch fingerprint.
        fresh: Alembic fresh-install branch fingerprint.

    Raises:
        AssertionError: If any normalized section differs.
    """
    paired = zip(fingerprint_sections(manual), fingerprint_sections(fresh), strict=True)
    for (manual_name, manual_value), (fresh_name, fresh_value) in paired:
        if manual_name != fresh_name:
            raise AssertionError(
                f"catalog comparator section mismatch: {manual_name}, {fresh_name}"
            )
        if manual_value != fresh_value:
            raise AssertionError(
                f"catalog fingerprint differs in {manual_name}: "
                f"manual={manual_value!r}, fresh={fresh_value!r}"
            )


def alembic_revision(connection: Connection) -> str:
    """Read the branch revision without including it in the catalog oracle.

    Args:
        connection: Scratch PostgreSQL connection.

    Returns:
        Current single Alembic revision.
    """
    value = connection.exec_driver_sql("SELECT version_num FROM alembic_version").scalar_one()
    return _required_text(value)


def _load_average() -> float:
    """Read the host's one-minute load average from the kernel interface."""
    first_field = Path("/proc/loadavg").read_text(encoding="utf-8").split(maxsplit=1)[0]
    return float(first_field)


def _require_safe_load(operation: str) -> None:
    """Refuse a heavy fixture phase above 1.5 load per host CPU.

    Args:
        operation: Human-readable phase about to begin.

    Raises:
        UnsafeHostLoadError: If the four-core host is already too busy.
    """
    load = _load_average()
    limit = _HOST_CORES * _MAX_LOAD_PER_CORE
    if load > limit:
        raise UnsafeHostLoadError(
            f"refusing {operation}: one-minute load {load:.2f} exceeds "
            f"{_MAX_LOAD_PER_CORE:.1f} per CPU on {_HOST_CORES} cores"
        )


def _await_safe_load(
    operation: str,
    *,
    load_reader: Callable[[], float] = _load_average,
    clock: Callable[[], float] = time.monotonic,
    sleeper: Callable[[float], None] = time.sleep,
    wait_seconds: float = _LOAD_WAIT_SECONDS,
) -> None:
    """Wait idly and boundedly for one already-admitted heavy phase.

    Args:
        operation: Human-readable phase about to begin.
        load_reader: One-minute host load provider.
        clock: Monotonic deadline clock.
        sleeper: Idle wait function.
        wait_seconds: Maximum total idle wait.

    Raises:
        UnsafeHostLoadError: If load remains unsafe through the deadline.
        ValueError: If the wait bound is not positive.
    """
    if wait_seconds <= 0.0:
        raise ValueError("load wait duration must be positive")
    limit = _HOST_CORES * _MAX_LOAD_PER_CORE
    load = load_reader()
    if load <= limit:
        return
    deadline = clock() + wait_seconds
    while load > limit:
        remaining = deadline - clock()
        if remaining <= 0.0:
            raise UnsafeHostLoadError(
                f"refusing {operation}: one-minute load {load:.2f} exceeds "
                f"{_MAX_LOAD_PER_CORE:.1f} per CPU on {_HOST_CORES} cores"
            )
        sleeper(min(_LOAD_POLL_SECONDS, remaining, 1.0))
        load = load_reader()


def _postgres_tools() -> _PostgresTools:
    """Resolve and verify the exact PostgreSQL 18.4 server toolchain.

    Returns:
        PostgreSQL binary paths.

    Raises:
        ScratchClusterUnavailableError: If PostgreSQL 18.4 is unavailable.
    """
    tools = _PostgresTools(
        initdb=_PG_BIN / "initdb",
        createdb=_PG_BIN / "createdb",
        postgres=_PG_BIN / "postgres",
        pg_isready=_PG_BIN / "pg_isready",
    )
    paths = (tools.initdb, tools.createdb, tools.postgres, tools.pg_isready)
    missing = tuple(str(path) for path in paths if not path.is_file())
    if missing:
        raise ScratchClusterUnavailableError(
            f"PostgreSQL 18.4 scratch binaries unavailable: {', '.join(missing)}"
        )
    version = _run_process([str(tools.postgres), "--version"], low_priority=False).stdout.strip()
    if "postgres (PostgreSQL) 18.4" not in version:
        raise ScratchClusterUnavailableError(f"expected PostgreSQL 18.4, received {version!r}")
    return tools


def _low_priority_prefix() -> list[str]:
    """Return parent binding plus CPU and I/O wrappers for heavy subprocesses."""
    setpriv = shutil.which("setpriv")
    nice = shutil.which("nice")
    ionice = shutil.which("ionice")
    if setpriv is None or nice is None or ionice is None:
        raise ScratchClusterUnavailableError(
            "setpriv, nice, and ionice are required for scratch-cluster work"
        )
    return [setpriv, "--pdeathsig", "KILL", nice, "-n", "19", ionice, "-c", "3"]


def _lower_process_priority() -> None:
    """Apply nice 19 and idle I/O scheduling to in-process migration work."""
    os.setpriority(os.PRIO_PROCESS, 0, 19)
    ionice = shutil.which("ionice")
    if ionice is None:
        raise ScratchClusterUnavailableError("ionice is required for scratch-cluster work")
    result = _run_process(
        [ionice, "-c", "3", "-p", str(os.getpid())],
        low_priority=False,
    )
    if result.returncode != 0 or os.getpriority(os.PRIO_PROCESS, 0) != 19:
        raise ScratchClusterUnavailableError("could not lower scratch fixture process priority")


def _run_process(
    arguments: list[str],
    *,
    low_priority: bool = True,
    environment: dict[str, str] | None = None,
) -> subprocess.CompletedProcess[str]:
    """Run one bounded fixture subprocess and preserve diagnostic output.

    Args:
        arguments: Program and arguments without a shell.
        low_priority: Whether to apply ``nice`` and ``ionice``.
        environment: Sanitized process environment.

    Returns:
        Successful completed process.

    Raises:
        ScratchClusterUnavailableError: If the command fails.
    """
    command_line = [*_low_priority_prefix(), *arguments] if low_priority else arguments
    result = subprocess.run(
        command_line,
        cwd=_PROJECT_ROOT,
        env=environment,
        check=False,
        capture_output=True,
        text=True,
        timeout=120,
    )
    if result.returncode != 0:
        raise ScratchClusterUnavailableError(
            f"scratch command failed with {result.returncode}: {command_line!r}\n"
            f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
        )
    return result


@contextmanager
def _without_database_environment() -> Iterator[dict[str, str]]:
    """Remove every ambient database selector while the scratch fixture runs."""
    saved = {name: os.environ[name] for name in _DATABASE_ENVIRONMENT if name in os.environ}
    for name in _DATABASE_ENVIRONMENT:
        os.environ.pop(name, None)
    sanitized = dict(os.environ)
    try:
        yield sanitized
    finally:
        for name in _DATABASE_ENVIRONMENT:
            os.environ.pop(name, None)
        os.environ.update(saved)


def _append_server_configuration(data_directory: Path, socket_directory: Path) -> None:
    """Configure a small Unix-socket-only PostgreSQL scratch server.

    Args:
        data_directory: Fresh ``initdb`` output directory.
        socket_directory: Private socket directory.
    """
    settings = (
        "\n"
        "listen_addresses = ''\n"
        f"port = {_PG_PORT}\n"
        f"unix_socket_directories = '{socket_directory}'\n"
        "unix_socket_permissions = 0700\n"
        "allow_system_table_mods = on\n"
        "max_connections = 20\n"
        "shared_buffers = '32MB'\n"
        "autovacuum = off\n"
        "fsync = off\n"
        "full_page_writes = off\n"
        "synchronous_commit = off\n"
    )
    configuration = data_directory / "postgresql.conf"
    with configuration.open("a", encoding="utf-8") as stream:
        stream.write(settings)


def _connection_url(driver: str, database: str, socket_directory: Path) -> str:
    """Build an explicit Unix-socket URL without environment fallbacks.

    Args:
        driver: SQLAlchemy PostgreSQL driver name.
        database: Scratch database name.
        socket_directory: Private server socket directory.

    Returns:
        Explicit SQLAlchemy URL.
    """
    return f"postgresql+{driver}://{_PG_ROLE}@/{database}?host={socket_directory}&port={_PG_PORT}"


def _alembic_config(database: str, socket_directory: Path) -> Config:
    """Build Alembic configuration with the explicit anchor x-argument.

    Args:
        database: Scratch database name.
        socket_directory: Private server socket directory.

    Returns:
        Fully isolated Alembic configuration.
    """
    config = Config(str(_ALEMBIC_INI))
    config.set_main_option(
        "sqlalchemy.url",
        _connection_url("asyncpg", database, socket_directory),
    )
    config.cmd_opts = Namespace(x=[f"partition_anchor={_ANCHOR_TEXT}"])
    return config


def _upgrade(database: str, socket_directory: Path, revision: str) -> None:
    """Run one low-priority Alembic upgrade after a fresh load check.

    Args:
        database: Scratch database name.
        socket_directory: Private server socket directory.
        revision: Alembic target revision.
    """
    _await_safe_load(f"Alembic upgrade of {database} to {revision}")
    command.upgrade(_alembic_config(database, socket_directory), revision)


def _expect_upgrade_refusal(
    database: str,
    socket_directory: Path,
    expected_text: str,
    diagnostic_engine: Engine,
) -> str:
    """Require migration 0043 to refuse the current database state.

    Args:
        database: Scratch database name.
        socket_directory: Private server socket directory.
        expected_text: Required fail-closed diagnostic.
        diagnostic_engine: Engine used only for exact catalog evidence.

    Returns:
        Exact migration refusal text.

    Raises:
        AssertionError: If migration 0043 accepts the invalid state or refuses
            for a different reason.
    """
    try:
        _upgrade(database, socket_directory, "0043")
    except UnsafeHostLoadError:
        raise
    except RuntimeError as exc:
        refusal = str(exc)
        if expected_text not in refusal:
            evidence = _constraint_evidence(diagnostic_engine)
            raise AssertionError(
                f"migration 0043 refused for the wrong reason: {exc}; constraints={evidence!r}"
            ) from exc
        return refusal
    raise AssertionError(f"migration 0043 unexpectedly accepted invalid state in {database}")


def _constraint_evidence(engine: Engine) -> tuple[tuple[object, ...], ...]:
    """Read concise raw PG18 constraints for representative topology tables."""
    with engine.connect() as connection:
        connection.exec_driver_sql("SET LOCAL TIME ZONE 'UTC'")
        rows = connection.exec_driver_sql("""
            SELECT relation.relname,
                   constraint_row.conname,
                   constraint_row.contype::text,
                   pg_get_constraintdef(constraint_row.oid, true),
                   constraint_row.convalidated,
                   constraint_row.condeferrable,
                   constraint_row.condeferred
            FROM pg_class AS relation
            JOIN pg_namespace AS namespace ON namespace.oid = relation.relnamespace
            JOIN pg_constraint AS constraint_row ON constraint_row.conrelid = relation.oid
            WHERE namespace.nspname = 'public'
              AND relation.relname IN (
                  'ticks',
                  'ticks_legacy',
                  'ticks_d20260730',
                  'ticks_default',
                  'candles',
                  'candles_legacy',
                  'candles_d20260730',
                  'candles_default',
                  'trades',
                  'trades_legacy',
                  'trades_d20260730',
                  'trades_default'
              )
            ORDER BY relation.relname, constraint_row.conname
            """)
        return tuple(tuple(row) for row in rows)


def _insert_populated_tick(connection: Connection) -> None:
    """Insert one valid scratch tick for the populated-table refusal probe."""
    connection.exec_driver_sql("""
        INSERT INTO ticks (
            public_id,
            instrument_public_id,
            bid,
            ask,
            last,
            volume,
            session_id,
            sequence_id,
            timestamp,
            known_to
        ) VALUES (
            '00000000-0000-7000-8000-000000000043',
            '00000000-0000-7000-8000-000000000044',
            1.0,
            1.1,
            1.05,
            2.0,
            '00000000-0000-7000-8000-000000000045',
            1,
            '2026-07-29T23:00:00+00:00',
            'infinity'
        )
        """)


def _root_kinds(connection: Connection) -> tuple[tuple[str, str, bool], ...]:
    """Read ordinary-versus-partitioned root state for refusal evidence."""
    rows = connection.exec_driver_sql("""
        SELECT relname, relkind::text, relispartition
        FROM pg_class
        JOIN pg_namespace ON pg_namespace.oid = pg_class.relnamespace
        WHERE pg_namespace.nspname = 'public'
          AND relname IN ('ticks', 'candles', 'trades')
        ORDER BY relname
        """).mappings()
    return tuple(
        (
            _required_text(row["relname"]),
            _required_text(row["relkind"]),
            _boolean(row["relispartition"]),
        )
        for row in rows
    )


def _legacy_relations(connection: Connection) -> tuple[str, ...]:
    """Read any market legacy relation names after a refused migration."""
    values = connection.exec_driver_sql("""
        SELECT relname
        FROM pg_class
        JOIN pg_namespace ON pg_namespace.oid = pg_class.relnamespace
        WHERE pg_namespace.nspname = 'public'
          AND relname IN ('ticks_legacy', 'candles_legacy', 'trades_legacy')
        ORDER BY relname
        """).scalars()
    return tuple(_required_text(value) for value in values)


def _prove_populated_refusal(
    engine: Engine,
    socket_directory: Path,
) -> tuple[str, tuple[tuple[str, str, bool], ...], tuple[str, ...]]:
    """Prove a populated ordinary table fails closed before manual adoption."""
    with engine.begin() as connection:
        _insert_populated_tick(connection)
    _expect_upgrade_refusal(
        "manual_branch",
        socket_directory,
        "ticks is populated; 0043 refuses automatic adoption",
        engine,
    )
    with engine.connect() as connection:
        revision = alembic_revision(connection)
        root_kinds = _root_kinds(connection)
        legacy_relations = _legacy_relations(connection)
    with engine.begin() as connection:
        connection.exec_driver_sql(
            "DELETE FROM ticks WHERE public_id = '00000000-0000-7000-8000-000000000043'"
        )
    return revision, root_kinds, legacy_relations


def _expect_runtime_adoption_refusal(
    engine: Engine,
    table: str,
    expected_text: str,
) -> str:
    """Require the public manual path to reject one ordinary catalog drift.

    Args:
        engine: Scratch manual-branch engine.
        table: Allowlisted ordinary table under test.
        expected_text: Required refusal diagnostic fragment.

    Returns:
        Exact runtime refusal message.

    Raises:
        AssertionError: If adoption succeeds or refuses for another reason.
    """
    with engine.connect() as connection:
        try:
            adopt(connection, table, _ANCHOR, dry_run=False)
        except DailyPartitionError as exc:
            refusal = str(exc)
            if expected_text not in refusal:
                raise AssertionError(
                    f"runtime adoption refused {table} for the wrong reason: {exc}"
                ) from exc
            return refusal
    raise AssertionError(f"runtime adoption unexpectedly accepted drifted {table}")


def _not_null_constraint_name(
    connection: Connection,
    table: str,
    column: str,
) -> str:
    """Return one PostgreSQL 18 NOT NULL constraint name.

    Args:
        connection: Scratch catalog connection.
        table: Public table relation name.
        column: Required column name.

    Returns:
        Exact catalog constraint name.
    """
    value = connection.execute(
        sa.text("""
            SELECT constraint_row.conname
            FROM pg_constraint AS constraint_row
            JOIN pg_class AS relation ON relation.oid = constraint_row.conrelid
            JOIN pg_namespace AS namespace ON namespace.oid = relation.relnamespace
            JOIN pg_attribute AS attribute
              ON attribute.attrelid = relation.oid
             AND attribute.attnum = constraint_row.conkey[1]
            WHERE namespace.nspname = 'public'
              AND relation.relname = :table
              AND attribute.attname = :column
              AND constraint_row.contype = 'n'
            """),
        {"table": table, "column": column},
    ).scalar_one()
    return _required_text(value)


def _sequence_cache(connection: Connection, sequence: str) -> int:
    """Return one sequence cache parameter from the scratch catalog.

    Args:
        connection: Scratch catalog connection.
        sequence: Public sequence relation name.

    Returns:
        Exact positive cache value.

    Raises:
        TypeError: If the driver returns a noninteger catalog value.
    """
    value = connection.execute(
        sa.text("""
            SELECT sequence_parameters.seqcache
            FROM pg_sequence AS sequence_parameters
            JOIN pg_class AS relation
              ON relation.oid = sequence_parameters.seqrelid
            JOIN pg_namespace AS namespace ON namespace.oid = relation.relnamespace
            WHERE namespace.nspname = 'public'
              AND relation.relname = :sequence
            """),
        {"sequence": sequence},
    ).scalar_one()
    if not isinstance(value, int):
        raise TypeError(f"expected integer sequence cache, observed {value!r}")
    return value


def _prove_runtime_constraint_refusal(
    engine: Engine,
) -> tuple[str, str, bool]:
    """Prove adoption rejects and preserves a renamed NOT NULL constraint.

    Args:
        engine: Scratch manual-branch engine at revision 0042.

    Returns:
        Refusal text, preserved drift name, and restoration result.
    """
    table = "ticks"
    original = "ticks_timestamp_not_null"
    drifted = "ticks_timestamp_not_null_drifted"
    with engine.connect() as connection:
        baseline = catalog_fingerprint(connection)
        if _not_null_constraint_name(connection, table, "timestamp") != original:
            raise AssertionError(f"{original} is absent before the refusal probe")
    with engine.begin() as connection:
        connection.exec_driver_sql(
            f"ALTER TABLE public.{table} RENAME CONSTRAINT {original} TO {drifted}"
        )
    error = _expect_runtime_adoption_refusal(
        engine,
        table,
        "ticks relation constraint manifest drifted",
    )
    with engine.connect() as connection:
        observed = _not_null_constraint_name(connection, table, "timestamp")
    with engine.begin() as connection:
        connection.exec_driver_sql(
            f"ALTER TABLE public.{table} RENAME CONSTRAINT {drifted} TO {original}"
        )
    with engine.connect() as connection:
        restored_name = _not_null_constraint_name(connection, table, "timestamp")
        restored = catalog_fingerprint(connection)
    assert_catalogs_identical(baseline, restored)
    return error, observed, restored_name == original


def _prove_runtime_sequence_refusal(
    engine: Engine,
) -> tuple[str, int, bool]:
    """Prove adoption rejects and preserves sequence-parameter drift.

    Args:
        engine: Scratch manual-branch engine at revision 0042.

    Returns:
        Refusal text, preserved cache value, and restoration result.
    """
    sequence = "ticks_id_seq"
    with engine.connect() as connection:
        baseline = catalog_fingerprint(connection)
        if _sequence_cache(connection, sequence) != 1:
            raise AssertionError(f"{sequence} has a nondefault cache before the refusal probe")
    with engine.begin() as connection:
        connection.exec_driver_sql(f"ALTER SEQUENCE public.{sequence} CACHE 100")
    error = _expect_runtime_adoption_refusal(
        engine,
        "ticks",
        "ticks_id_seq parameters or ownership are not exact",
    )
    with engine.connect() as connection:
        observed = _sequence_cache(connection, sequence)
    with engine.begin() as connection:
        connection.exec_driver_sql(f"ALTER SEQUENCE public.{sequence} CACHE 1")
    with engine.connect() as connection:
        restored_cache = _sequence_cache(connection, sequence)
        restored = catalog_fingerprint(connection)
    assert_catalogs_identical(baseline, restored)
    return error, observed, restored_cache == 1


def _prove_runtime_leaf_index_refusal(
    engine: Engine,
) -> tuple[str, str, bool]:
    """Prove adopted no-op rejects and preserves one renamed leaf index.

    Args:
        engine: Scratch manual-branch engine after public adoption.

    Returns:
        Refusal text, preserved drift name, and restoration result.
    """
    original = "ticks_d20260730_instrument_public_id_timestamp_idx"
    drifted = "ticks_d20260730_instrument_public_id_timestamp_drifted"
    with engine.connect() as connection:
        baseline = catalog_fingerprint(connection)
    with engine.begin() as connection:
        connection.exec_driver_sql(f"ALTER INDEX public.{original} RENAME TO {drifted}")
    error = _expect_runtime_adoption_refusal(
        engine,
        "ticks",
        "ticks_d20260730 index manifest",
    )
    with engine.connect() as connection:
        observed = _required_text(
            connection.exec_driver_sql(
                f"SELECT relname FROM pg_class WHERE oid = 'public.{drifted}'::regclass"
            ).scalar_one()
        )
    with engine.begin() as connection:
        connection.exec_driver_sql(f"ALTER INDEX public.{drifted} RENAME TO {original}")
    with engine.connect() as connection:
        restored_name = _required_text(
            connection.exec_driver_sql(
                f"SELECT relname FROM pg_class WHERE oid = 'public.{original}'::regclass"
            ).scalar_one()
        )
        restored = catalog_fingerprint(connection)
    assert_catalogs_identical(baseline, restored)
    return error, observed, restored_name == original


def _ordinary_index_evidence(
    connection: Connection,
    table: str,
) -> tuple[tuple[object, ...], ...]:
    """Read raw PG18 index deparse fields for a failed strict preflight."""
    rows = connection.execute(
        sa.text("""
            SELECT index_relation.relname::text,
                   index_row.indisunique,
                   index_row.indisvalid,
                   pg_get_expr(index_row.indpred, index_row.indrelid),
                   ARRAY(
                       SELECT pg_get_indexdef(index_row.indexrelid, position, true)
                       FROM generate_series(1, index_row.indnkeyatts) AS position
                       ORDER BY position
                   ),
                   EXISTS (
                       SELECT 1
                       FROM pg_constraint
                       WHERE pg_constraint.conindid = index_row.indexrelid
                   )
            FROM pg_index AS index_row
            JOIN pg_class AS index_relation ON index_relation.oid = index_row.indexrelid
            JOIN pg_class AS table_relation ON table_relation.oid = index_row.indrelid
            JOIN pg_namespace AS namespace ON namespace.oid = table_relation.relnamespace
            WHERE namespace.nspname = current_schema()
              AND table_relation.relname = :table
            ORDER BY index_relation.relname
            """),
        {"table": table},
    )
    return tuple(tuple(row) for row in rows)


def _manual_adoption(engine: Engine) -> None:
    """Invoke the public runtime adoption path independently for all roots."""
    for table in _ROOT_TABLES:
        _await_safe_load(f"runtime adoption of {table}")
        with engine.connect() as connection:
            try:
                adopt(connection, table, _ANCHOR, dry_run=False)
            except DailyPartitionError as exc:
                evidence = _ordinary_index_evidence(connection, table)
                raise AssertionError(
                    f"runtime adoption refused {table}: {exc}; raw indexes={evidence!r}"
                ) from exc


def _unexpected_partition_evidence(
    connection: Connection,
) -> tuple[str, str, str, bool, str, str, str]:
    """Read the exact nested direct-child state used by the refusal probe."""
    connection.exec_driver_sql("SET LOCAL TIME ZONE 'UTC'")
    row = connection.exec_driver_sql("""
        SELECT
            child_namespace.nspname,
            child.relname,
            child.relkind::text,
            child.relispartition,
            parent.relname,
            pg_get_expr(child.relpartbound, child.oid, true),
            pg_get_partkeydef(child.oid)
        FROM pg_inherits AS inheritance
        JOIN pg_class AS parent ON parent.oid = inheritance.inhparent
        JOIN pg_namespace AS parent_namespace
          ON parent_namespace.oid = parent.relnamespace
        JOIN pg_class AS child ON child.oid = inheritance.inhrelid
        JOIN pg_namespace AS child_namespace
          ON child_namespace.oid = child.relnamespace
        WHERE parent_namespace.nspname = 'public'
          AND parent.relname = 'trades'
          AND child_namespace.nspname = 'partition_probe'
          AND child.relname = 'trades_unexpected_partitioned'
        """).one()
    return (
        _required_text(row[0]),
        _required_text(row[1]),
        _required_text(row[2]),
        _boolean(row[3]),
        _required_text(row[4]),
        _required_text(row[5]),
        _required_text(row[6]),
    )


def _prove_unexpected_partitioned_child_refusal(
    engine: Engine,
    socket_directory: Path,
) -> tuple[str, str, tuple[str, str, str, bool, str, str, str], bool]:
    """Prove 0043 rejects and preserves a foreign-schema partitioned child."""
    with engine.connect() as connection:
        baseline = catalog_fingerprint(connection)
    with engine.begin() as connection:
        connection.exec_driver_sql("CREATE SCHEMA partition_probe")
        connection.exec_driver_sql("""
            CREATE TABLE partition_probe.trades_unexpected_partitioned
            PARTITION OF public.trades
            FOR VALUES FROM (TIMESTAMPTZ '2026-09-01 00:00:00+00')
            TO (TIMESTAMPTZ '2026-09-02 00:00:00+00')
            PARTITION BY RANGE (executed_at)
            """)
    with engine.connect() as connection:
        observed = _unexpected_partition_evidence(connection)
    refusal = _expect_upgrade_refusal(
        "manual_branch",
        socket_directory,
        "trades partition bounds or table edges differ",
        engine,
    )
    with engine.connect() as connection:
        revision = alembic_revision(connection)
        preserved = _unexpected_partition_evidence(connection)
    if preserved != observed:
        raise AssertionError(
            f"refused migration changed unexpected partition: "
            f"before={observed!r}, after={preserved!r}"
        )
    with engine.begin() as connection:
        connection.exec_driver_sql("DROP TABLE partition_probe.trades_unexpected_partitioned")
        connection.exec_driver_sql("DROP SCHEMA partition_probe")
    with engine.connect() as connection:
        removed = _boolean(
            connection.exec_driver_sql(
                "SELECT to_regclass('partition_probe.trades_unexpected_partitioned') IS NULL"
            ).scalar_one()
        )
        restored = catalog_fingerprint(connection)
    if not removed:
        raise AssertionError("unexpected nested partition was not removed from the scratch branch")
    assert_catalogs_identical(baseline, restored)
    return revision, refusal, observed, removed


def _index_reloptions(connection: Connection, index: str) -> str:
    """Return stable comma-delimited storage options for one public index."""
    value = connection.execute(
        sa.text("""
            SELECT COALESCE(array_to_string(relation.reloptions, ','), '')
            FROM pg_class AS relation
            JOIN pg_namespace AS namespace ON namespace.oid = relation.relnamespace
            WHERE namespace.nspname = 'public'
              AND relation.relname = :index
            """),
        {"index": index},
    ).scalar_one()
    return _required_text(value)


def _prove_legacy_index_storage_refusal(
    engine: Engine,
    socket_directory: Path,
) -> tuple[str, str, str, bool]:
    """Prove 0043 rejects and preserves nondefault adopted-index storage."""
    index = "ix_trades_timestamp"
    with engine.connect() as connection:
        baseline = catalog_fingerprint(connection)
        if _index_reloptions(connection, index):
            raise AssertionError(f"{index} unexpectedly has storage options before drift")
    with engine.begin() as connection:
        connection.exec_driver_sql(f"ALTER INDEX public.{index} SET (fillfactor = 70)")
    with engine.connect() as connection:
        observed = _index_reloptions(connection, index)
    refusal = _expect_upgrade_refusal(
        "manual_branch",
        socket_directory,
        "trades_legacy index manifest is not exact",
        engine,
    )
    with engine.connect() as connection:
        revision = alembic_revision(connection)
        preserved = _index_reloptions(connection, index)
    if preserved != observed:
        raise AssertionError(
            f"refused migration changed {index} storage: "
            f"before={observed!r}, after={preserved!r}"
        )
    with engine.begin() as connection:
        connection.exec_driver_sql(f"ALTER INDEX public.{index} RESET (fillfactor)")
    with engine.connect() as connection:
        restored_options = _index_reloptions(connection, index)
        restored = catalog_fingerprint(connection)
    storage_restored = restored_options == ""
    if not storage_restored:
        raise AssertionError(f"{index} storage options remain after RESET: {restored_options!r}")
    assert_catalogs_identical(baseline, restored)
    return revision, refusal, observed, storage_restored


def _prove_malformed_partitioned_refusal(
    engine: Engine,
    socket_directory: Path,
) -> tuple[str, str]:
    """Prove verify/no-op refuses a partitioned root with a renamed index."""
    malformed_name = "trades_p_ix_ts_malformed"
    with engine.begin() as connection:
        connection.exec_driver_sql(f"ALTER INDEX public.trades_p_ix_ts RENAME TO {malformed_name}")
    _expect_upgrade_refusal(
        "manual_branch",
        socket_directory,
        "trades index manifest is not exact",
        engine,
    )
    with engine.connect() as connection:
        revision = alembic_revision(connection)
        observed = connection.exec_driver_sql(
            "SELECT relname FROM pg_class WHERE oid = "
            "'public.trades_p_ix_ts_malformed'::regclass"
        ).scalar_one()
    with engine.begin() as connection:
        connection.exec_driver_sql(f"ALTER INDEX public.{malformed_name} RENAME TO trades_p_ix_ts")
    return revision, _required_text(observed)


def _build_manual_branch(engine: Engine, socket_directory: Path) -> RefusalEvidence:
    """Build manual adoption, its two refusals, and the final 0043 no-op proof."""
    _upgrade("manual_branch", socket_directory, "0042")
    populated_revision, root_kinds, legacy_relations = _prove_populated_refusal(
        engine,
        socket_directory,
    )
    (
        runtime_constraint_error,
        runtime_constraint_name,
        runtime_constraint_restored,
    ) = _prove_runtime_constraint_refusal(engine)
    (
        runtime_sequence_error,
        runtime_sequence_cache,
        runtime_sequence_restored,
    ) = _prove_runtime_sequence_refusal(engine)
    _manual_adoption(engine)
    (
        runtime_leaf_error,
        runtime_leaf_index_name,
        runtime_leaf_restored,
    ) = _prove_runtime_leaf_index_refusal(engine)
    (
        unexpected_partition_revision,
        unexpected_partition_error,
        unexpected_partition,
        unexpected_partition_removed,
    ) = _prove_unexpected_partitioned_child_refusal(engine, socket_directory)
    (
        storage_drift_revision,
        storage_drift_error,
        storage_drift_reloptions,
        storage_drift_restored,
    ) = _prove_legacy_index_storage_refusal(engine, socket_directory)
    malformed_revision, malformed_name = _prove_malformed_partitioned_refusal(
        engine,
        socket_directory,
    )
    with engine.connect() as connection:
        before_noop = catalog_fingerprint(connection)
    _upgrade("manual_branch", socket_directory, "0043")
    with engine.connect() as connection:
        after_noop = catalog_fingerprint(connection)
    assert_catalogs_identical(before_noop, after_noop)
    return RefusalEvidence(
        populated_revision=populated_revision,
        populated_root_kinds=root_kinds,
        populated_legacy_relations=legacy_relations,
        runtime_constraint_error=runtime_constraint_error,
        runtime_constraint_name=runtime_constraint_name,
        runtime_constraint_restored=runtime_constraint_restored,
        runtime_sequence_error=runtime_sequence_error,
        runtime_sequence_cache=runtime_sequence_cache,
        runtime_sequence_restored=runtime_sequence_restored,
        runtime_leaf_error=runtime_leaf_error,
        runtime_leaf_index_name=runtime_leaf_index_name,
        runtime_leaf_restored=runtime_leaf_restored,
        unexpected_partition_revision=unexpected_partition_revision,
        unexpected_partition_error=unexpected_partition_error,
        unexpected_partition=unexpected_partition,
        unexpected_partition_removed=unexpected_partition_removed,
        storage_drift_revision=storage_drift_revision,
        storage_drift_error=storage_drift_error,
        storage_drift_reloptions=storage_drift_reloptions,
        storage_drift_restored=storage_drift_restored,
        malformed_revision=malformed_revision,
        malformed_index_name=malformed_name,
    )


def _build_fresh_branch(socket_directory: Path) -> None:
    """Build the independent Alembic branch explicitly from 0001 through 0043."""
    _upgrade("fresh_branch", socket_directory, "0001")
    _upgrade("fresh_branch", socket_directory, "0043")


def _create_database(
    tools: _PostgresTools,
    socket_directory: Path,
    database: str,
    environment: dict[str, str],
) -> None:
    """Create one explicitly addressed scratch database.

    Args:
        tools: PostgreSQL 18.4 binaries.
        socket_directory: Private server socket directory.
        database: Database to create.
        environment: Sanitized process environment.
    """
    _await_safe_load(f"creation of {database}")
    _run_process(
        [
            str(tools.createdb),
            "-h",
            str(socket_directory),
            "-p",
            str(_PG_PORT),
            "-U",
            _PG_ROLE,
            database,
        ],
        environment=environment,
    )


def _verify_private_server(engine: Engine, socket_directory: Path) -> None:
    """Verify version, port, and absence of a TCP listener."""
    with engine.connect() as connection:
        values = connection.exec_driver_sql("""
            SELECT
                current_setting('server_version_num'),
                current_setting('port'),
                current_setting('listen_addresses'),
                current_setting('unix_socket_directories')
            """).one()
    observed = tuple(_required_text(value) for value in values)
    expected = ("180004", str(_PG_PORT), "", str(socket_directory))
    if observed != expected:
        raise ScratchClusterUnavailableError(
            f"scratch server isolation mismatch: expected {expected!r}, observed {observed!r}"
        )


def _start_cluster(
    tools: _PostgresTools,
    data_directory: Path,
    socket_directory: Path,
    log_path: Path,
    environment: dict[str, str],
) -> subprocess.Popen[bytes]:
    """Initialize and start a parent-bound private PostgreSQL postmaster."""
    _await_safe_load("PostgreSQL 18.4 initdb")
    _run_process(
        [
            str(tools.initdb),
            "-D",
            str(data_directory),
            "--no-locale",
            "--encoding=UTF8",
            "--auth-local=trust",
            "--auth-host=reject",
            f"--username={_PG_ROLE}",
        ],
        environment=environment,
    )
    socket_directory.mkdir(mode=0o700)
    _append_server_configuration(data_directory, socket_directory)
    _await_safe_load("PostgreSQL 18.4 server startup")
    with log_path.open("ab") as log_stream:
        postmaster = subprocess.Popen(
            [
                *_low_priority_prefix(),
                str(tools.postgres),
                "-D",
                str(data_directory),
            ],
            cwd=_PROJECT_ROOT,
            env=environment,
            stdout=log_stream,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
    try:
        if os.getsid(postmaster.pid) != postmaster.pid:
            raise ScratchClusterUnavailableError(
                "scratch postmaster is not leader of its private process session"
            )
        deadline = time.monotonic() + 30.0
        while time.monotonic() < deadline:
            returncode = postmaster.poll()
            if returncode is not None:
                log_output = log_path.read_text(encoding="utf-8", errors="replace")
                raise ScratchClusterUnavailableError(
                    f"scratch postmaster exited with {returncode}:\n{log_output}"
                )
            pid_file = data_directory / "postmaster.pid"
            if pid_file.exists():
                recorded_pid = int(pid_file.read_text(encoding="utf-8").splitlines()[0])
                if recorded_pid != postmaster.pid:
                    raise ScratchClusterUnavailableError(
                        "scratch postmaster PID file does not match owned process"
                    )
                ready = subprocess.run(
                    [
                        str(tools.pg_isready),
                        "-h",
                        str(socket_directory),
                        "-p",
                        str(_PG_PORT),
                        "-U",
                        _PG_ROLE,
                        "-d",
                        "postgres",
                    ],
                    cwd=_PROJECT_ROOT,
                    env=environment,
                    check=False,
                    capture_output=True,
                    text=True,
                    timeout=10,
                )
                if ready.returncode == 0:
                    return postmaster
            time.sleep(0.05)
        raise ScratchClusterUnavailableError("scratch postmaster readiness timed out")
    except BaseException:
        if postmaster.poll() is None:
            os.killpg(postmaster.pid, signal.SIGKILL)
        postmaster.wait(timeout=5)
        raise


def _stop_cluster(postmaster: subprocess.Popen[bytes] | None) -> None:
    """Stop and reap only the directly owned private postmaster session."""
    if postmaster is None:
        return
    if postmaster.poll() is None:
        postmaster.send_signal(signal.SIGQUIT)
        try:
            postmaster.wait(timeout=30)
        except subprocess.TimeoutExpired:
            os.killpg(postmaster.pid, signal.SIGKILL)
            try:
                postmaster.wait(timeout=5)
            except subprocess.TimeoutExpired as exc:
                raise ScratchClusterUnavailableError(
                    "owned scratch postmaster could not be reaped"
                ) from exc
    elif postmaster.returncode is None:
        postmaster.wait(timeout=5)


@contextmanager
def postgres_branches() -> Generator[PostgresBranches]:
    """Build and yield both independent branches in one private PG 18.4 cluster.

    Yields:
        Converged branch engines and fail-closed evidence.

    Raises:
        ScratchClusterUnavailableError: If PostgreSQL 18.4 or isolation controls fail.
        UnsafeHostLoadError: If the host exceeds the accepted load before heavy work.
    """
    if os.environ.get("PYTEST_XDIST_WORKER"):
        raise ScratchClusterUnavailableError(
            "the PostgreSQL convergence fixture must run serially without pytest-xdist"
        )
    _require_safe_load("private PostgreSQL convergence fixture")
    with _without_database_environment() as environment:
        tools = _postgres_tools()
        _lower_process_priority()
        with tempfile.TemporaryDirectory(prefix="snapper-pg0043-", dir="/tmp") as temporary:
            fixture_root = Path(temporary)
            data_directory = fixture_root / "data"
            socket_directory = fixture_root / "socket"
            log_path = fixture_root / "postgresql.log"
            postmaster: subprocess.Popen[bytes] | None = None
            try:
                postmaster = _start_cluster(
                    tools,
                    data_directory,
                    socket_directory,
                    log_path,
                    environment,
                )
                _create_database(tools, socket_directory, "manual_branch", environment)
                _create_database(tools, socket_directory, "fresh_branch", environment)
                manual = sa.create_engine(
                    _connection_url("psycopg2", "manual_branch", socket_directory)
                )
                fresh = sa.create_engine(
                    _connection_url("psycopg2", "fresh_branch", socket_directory)
                )
                try:
                    _verify_private_server(manual, socket_directory)
                    refusals = _build_manual_branch(manual, socket_directory)
                    _build_fresh_branch(socket_directory)
                    yield PostgresBranches(manual=manual, fresh=fresh, refusals=refusals)
                finally:
                    manual.dispose()
                    fresh.dispose()
            finally:
                _stop_cluster(postmaster)


def _require_one_catalog_change(result: sa.CursorResult[object], mutation: CatalogMutation) -> None:
    """Require a direct scratch-catalog mutation to affect one row.

    Args:
        result: SQLAlchemy mutation result.
        mutation: Mutation being applied.

    Raises:
        AssertionError: If the target was absent or ambiguous.
    """
    if result.rowcount != 1:
        raise AssertionError(f"{mutation.value} changed {result.rowcount} catalog rows, expected 1")


def _mutate_partition_bound(connection: Connection) -> None:
    """Shorten one empty daily partition while preserving its lower bound."""
    connection.exec_driver_sql("ALTER TABLE public.ticks DETACH PARTITION public.ticks_d20260730")
    connection.exec_driver_sql("""
        ALTER TABLE public.ticks
        ATTACH PARTITION public.ticks_d20260730
        FOR VALUES FROM ('2026-07-30T00:00:00+00:00')
        TO ('2026-07-30T12:00:00+00:00')
        """)


def _mutate_local_partial(connection: Connection) -> None:
    """Change one local active-public-id predicate without changing its name."""
    connection.exec_driver_sql("DROP INDEX public.trades_d20260730_public_id")
    connection.exec_driver_sql("""
        CREATE UNIQUE INDEX trades_d20260730_public_id
        ON public.trades_d20260730 USING btree (public_id)
        WHERE known_to = 'infinity'::timestamp with time zone
          AND public_id IS NOT NULL
        """)


def _mutate_index_name(connection: Connection) -> None:
    """Rename one unattached local partial index."""
    connection.exec_driver_sql("""
        ALTER INDEX public.trades_d20260730_public_id
        RENAME TO trades_d20260730_public_id_mutated
        """)


def _mutate_index_parentage(connection: Connection) -> None:
    """Delete exactly one inherited-index edge in the throwaway catalog."""
    result = connection.exec_driver_sql("""
        DELETE FROM pg_inherits
        WHERE inhparent = 'public.trades_p_uq_instr_tid_exec'::regclass
          AND inhrelid = (
              SELECT child_index.oid
              FROM pg_inherits AS edge
              JOIN pg_class AS child_index ON child_index.oid = edge.inhrelid
              JOIN pg_index AS child_catalog ON child_catalog.indexrelid = child_index.oid
              WHERE edge.inhparent = 'public.trades_p_uq_instr_tid_exec'::regclass
                AND child_catalog.indrelid = 'public.trades_d20260730'::regclass
          )
        """)
    _require_one_catalog_change(result, CatalogMutation.INDEX_PARENTAGE)


def _mutate_partition_key_nullability(connection: Connection) -> None:
    """Drop parent key nullability in one throwaway catalog row."""
    result = connection.exec_driver_sql("""
        UPDATE pg_attribute
        SET attnotnull = false
        WHERE attrelid = 'public.trades'::regclass
          AND attname = 'executed_at'
          AND attnotnull
        """)
    _require_one_catalog_change(result, CatalogMutation.PARTITION_KEY_NULLABILITY)


def _mutate_trade_u3(connection: Connection) -> None:
    """Reorder the parent U3 key inside the throwaway system catalog."""
    result = connection.exec_driver_sql("""
        UPDATE pg_index
        SET indkey = concat_ws(
            ' ',
            (string_to_array(indkey::text, ' '))[1],
            (string_to_array(indkey::text, ' '))[3],
            (string_to_array(indkey::text, ' '))[2]
        )::int2vector
        WHERE indexrelid = 'public.trades_p_uq_instr_tid_exec'::regclass
          AND indnkeyatts = 3
          AND cardinality(string_to_array(indkey::text, ' ')) = 3
        """)
    _require_one_catalog_change(result, CatalogMutation.TRADE_U3_DEFINITION)


def _mutate_sequence_ownership(connection: Connection) -> None:
    """Move one sequence ownership dependency from parent to legacy."""
    connection.exec_driver_sql(
        "ALTER SEQUENCE public.trades_id_seq OWNED BY public.trades_legacy.id"
    )


def apply_catalog_mutation(connection: Connection, mutation: CatalogMutation) -> None:
    """Apply one required adversarial change to a throwaway transaction.

    Args:
        connection: Fresh-branch scratch connection.
        mutation: Required mutation to apply.
    """
    mutations = {
        CatalogMutation.PARTITION_BOUND: _mutate_partition_bound,
        CatalogMutation.LOCAL_ACTIVE_PUBLIC_ID_PARTIAL: _mutate_local_partial,
        CatalogMutation.INDEX_NAME: _mutate_index_name,
        CatalogMutation.INDEX_PARENTAGE: _mutate_index_parentage,
        CatalogMutation.PARTITION_KEY_NULLABILITY: _mutate_partition_key_nullability,
        CatalogMutation.TRADE_U3_DEFINITION: _mutate_trade_u3,
        CatalogMutation.SEQUENCE_OWNERSHIP: _mutate_sequence_ownership,
    }
    mutations[mutation](connection)
