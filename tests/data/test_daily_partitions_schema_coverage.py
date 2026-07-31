"""Mutation-sensitive schema and catalog tests for daily partitions."""

from dataclasses import replace
from datetime import UTC
from datetime import date
from datetime import datetime
from datetime import timedelta
from types import SimpleNamespace
from typing import cast
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest
from sqlalchemy.engine import Connection

import snapper.data.daily_partitions as lifecycle

_ANCHOR = datetime(2026, 8, 1, tzinfo=UTC)

_PARTITION_KEYS: dict[lifecycle.MarketDataTable, str] = {
    "ticks": "timestamp",
    "candles": "open_at",
    "trades": "executed_at",
}

_COLUMN_MANIFESTS: dict[
    lifecycle.MarketDataTable,
    tuple[tuple[str, str, bool, str | None, str, str], ...],
] = {
    "ticks": (
        ("id", "bigint", False, "nextval('ticks_id_seq'::regclass)", "", ""),
        ("public_id", "uuid", False, None, "", ""),
        ("instrument_public_id", "uuid", False, None, "", ""),
        ("bid", "double precision", True, None, "", ""),
        ("ask", "double precision", True, None, "", ""),
        ("last", "double precision", True, None, "", ""),
        ("volume", "double precision", False, None, "", ""),
        ("session_id", "uuid", False, None, "", ""),
        ("sequence_id", "integer", False, None, "", ""),
        ("timestamp", "timestamp with time zone", False, None, "", ""),
        ("known_to", "timestamp with time zone", False, None, "", ""),
    ),
    "candles": (
        ("id", "bigint", False, "nextval('candles_id_seq'::regclass)", "", ""),
        ("public_id", "uuid", False, None, "", ""),
        ("instrument_public_id", "uuid", False, None, "", ""),
        ("timeframe", "character varying(8)", False, None, "", ""),
        ("open_at", "timestamp with time zone", False, None, "", ""),
        ("open", "double precision", False, None, "", ""),
        ("high", "double precision", False, None, "", ""),
        ("low", "double precision", False, None, "", ""),
        ("close", "double precision", False, None, "", ""),
        ("volume", "double precision", False, None, "", ""),
        ("vwap", "double precision", True, None, "", ""),
        ("trades", "integer", True, None, "", ""),
        ("session_id", "uuid", False, None, "", ""),
        ("sequence_id", "integer", False, None, "", ""),
        ("timestamp", "timestamp with time zone", False, None, "", ""),
        ("known_to", "timestamp with time zone", False, None, "", ""),
        ("source", "character varying(16)", False, "'native'::character varying", "", ""),
        ("complete", "boolean", False, "true", "", ""),
        ("price_basis", "character varying(16)", True, None, "", ""),
    ),
    "trades": (
        ("id", "bigint", False, "nextval('trades_id_seq'::regclass)", "", ""),
        ("public_id", "uuid", False, None, "", ""),
        ("instrument_public_id", "uuid", False, None, "", ""),
        ("trade_id", "character varying(64)", True, None, "", ""),
        ("price", "double precision", False, None, "", ""),
        ("size", "double precision", False, None, "", ""),
        ("side", "character varying(4)", False, None, "", ""),
        ("executed_at", "timestamp with time zone", True, None, "", ""),
        ("session_id", "uuid", False, None, "", ""),
        ("sequence_id", "integer", False, None, "", ""),
        ("timestamp", "timestamp with time zone", False, None, "", ""),
        ("known_to", "timestamp with time zone", False, None, "", ""),
    ),
}

_NONCHECK_CONSTRAINT_MANIFESTS: dict[
    lifecycle.MarketDataTable,
    tuple[lifecycle.ConstraintShape, ...],
] = {
    "ticks": (
        lifecycle.ConstraintShape(
            name="ticks_pkey",
            kind="p",
            definition="PRIMARY KEY (id)",
            validated=True,
            deferrable=False,
            initially_deferred=False,
            index_name="ticks_pkey",
        ),
    ),
    "candles": (
        lifecycle.ConstraintShape(
            name="candles_pkey",
            kind="p",
            definition="PRIMARY KEY (id)",
            validated=True,
            deferrable=False,
            initially_deferred=False,
            index_name="candles_pkey",
        ),
    ),
    "trades": (
        lifecycle.ConstraintShape(
            name="trades_pkey",
            kind="p",
            definition="PRIMARY KEY (id)",
            validated=True,
            deferrable=False,
            initially_deferred=False,
            index_name="trades_pkey",
        ),
        lifecycle.ConstraintShape(
            name="uq_trade_instrument_trade_id",
            kind="u",
            definition="UNIQUE (instrument_public_id, trade_id)",
            validated=True,
            deferrable=False,
            initially_deferred=False,
            index_name="uq_trade_instrument_trade_id",
        ),
    ),
}


def _literal_noncheck_manifest(
    table: lifecycle.MarketDataTable,
    role: lifecycle.ConstraintRole,
) -> tuple[lifecycle.ConstraintShape, ...]:
    """Build the independently pinned ordinary or parent constraint manifest.

    Args:
        table: Table whose literal non-CHECK constraints are required.
        role: Ordinary or parent role selecting local key constraints.

    Returns:
        Exact sorted structural constraint shapes for the requested role.
    """
    shapes = [
        lifecycle.ConstraintShape(
            name=f"{table}_{name}_not_null",
            kind="n",
            definition=f"NOT NULL {name}",
            validated=True,
            deferrable=False,
            initially_deferred=False,
            index_name=None,
        )
        for name, _data_type, nullable, _default, _identity, _generated in (
            _COLUMN_MANIFESTS[table]
        )
        if not nullable
        or (name == _PARTITION_KEYS[table] and role is not lifecycle.ConstraintRole.ORDINARY)
    ]
    if role is not lifecycle.ConstraintRole.PARENT:
        shapes.extend(_NONCHECK_CONSTRAINT_MANIFESTS[table])
    return tuple(sorted(shapes, key=lambda shape: shape.name))


def _connection_double(
    rows: tuple[tuple[object, ...], ...] = (),
) -> Connection:
    """Build a clean PostgreSQL connection double with iterable catalog rows.

    Args:
        rows: Rows returned by every unconfigured execute call.

    Returns:
        A PostgreSQL-shaped SQLAlchemy connection double.
    """
    connection = MagicMock(spec=Connection)
    connection.in_transaction.return_value = False
    connection.dialect = SimpleNamespace(name="postgresql")
    connection.scalar.return_value = "public"
    connection.execute.return_value = rows
    return cast(Connection, connection)


def _partitioned_report(
    table: lifecycle.MarketDataTable,
    partitions: tuple[lifecycle.PartitionRef, ...] = (),
    *,
    default_attached: bool = True,
) -> lifecycle.PartitionInspection:
    """Build a deterministic partitioned topology report.

    Args:
        table: Allowlisted table represented by the report.
        partitions: Attached direct children.
        default_attached: Whether the required DEFAULT child is attached.

    Returns:
        A partitioned inspection suitable for lifecycle entry-point tests.
    """
    return lifecycle.PartitionInspection(
        table=table,
        state=lifecycle.RelationState.PARTITIONED,
        partition_key=f"RANGE ({lifecycle._spec(table).partition_key})",
        partitions=partitions,
        default_attached=default_attached,
        default_has_rows=False,
        future_leaf_count=lifecycle._future_leaf_count(
            lifecycle._spec(table),
            partitions,
            _ANCHOR,
        ),
        future_leaf_alarm=True,
    )


def _ordinary_report(
    table: lifecycle.MarketDataTable,
    state: lifecycle.RelationState = lifecycle.RelationState.ORDINARY,
) -> lifecycle.PartitionInspection:
    """Build a deterministic nonpartitioned topology report.

    Args:
        table: Allowlisted table represented by the report.
        state: Required nonpartitioned relation state.

    Returns:
        A topology report with no children.
    """
    return lifecycle.PartitionInspection(
        table=table,
        state=state,
        partition_key=None,
        partitions=(),
        default_attached=False,
        default_has_rows=False,
        future_leaf_count=0,
        future_leaf_alarm=True,
    )


def _safe_index_shape(
    columns: tuple[str, ...],
    *,
    unique: bool,
    constraint_backed: bool,
    predicate: str | None = None,
) -> lifecycle.IndexShape:
    """Build an exact default-btree index shape.

    Args:
        columns: Ordered index keys.
        unique: Whether the index is unique.
        constraint_backed: Whether a constraint owns the index.
        predicate: Optional partial-index predicate.

    Returns:
        A structurally safe index shape.
    """
    return lifecycle.IndexShape(
        unique=unique,
        valid=True,
        ready=True,
        live=True,
        columns=columns,
        predicate=predicate,
        constraint_backed=constraint_backed,
        access_method="btree",
        no_expressions=True,
        no_include=True,
        nulls_distinct=True,
        default_options=True,
        default_opclasses=True,
        attribute_collations=True,
        default_reloptions=True,
        default_tablespace=True,
        parent_indexes=(),
    )


_ORDINARY_INDEX_MANIFESTS: dict[
    lifecycle.MarketDataTable,
    tuple[tuple[str, lifecycle.IndexShape], ...],
] = {
    "ticks": (
        (
            "ix_tick_instrument_ts",
            _safe_index_shape(
                ("instrument_public_id", "timestamp"),
                unique=False,
                constraint_backed=False,
            ),
        ),
        (
            "ticks_pkey",
            _safe_index_shape(("id",), unique=True, constraint_backed=True),
        ),
    ),
    "candles": (
        (
            "candles_pkey",
            _safe_index_shape(("id",), unique=True, constraint_backed=True),
        ),
        (
            "ix_candle_instrument_open",
            _safe_index_shape(
                ("instrument_public_id", "open_at"),
                unique=False,
                constraint_backed=False,
            ),
        ),
        (
            "ix_candles_public_id",
            _safe_index_shape(
                ("public_id",),
                unique=True,
                constraint_backed=False,
                predicate="known_to = '9999-12-31 23:59:59+00'::timestamp with time zone",
            ),
        ),
        (
            "uq_candle_itf_open",
            _safe_index_shape(
                ("instrument_public_id", "timeframe", "open_at"),
                unique=True,
                constraint_backed=False,
                predicate="known_to = '9999-12-31 23:59:59+00'::timestamp with time zone",
            ),
        ),
    ),
    "trades": (
        (
            "ix_trade_instrument_ts",
            _safe_index_shape(
                ("instrument_public_id", "timestamp"),
                unique=False,
                constraint_backed=False,
            ),
        ),
        (
            "ix_trades_executed_at",
            _safe_index_shape(
                ("executed_at",),
                unique=False,
                constraint_backed=False,
            ),
        ),
        (
            "ix_trades_public_id",
            _safe_index_shape(
                ("public_id",),
                unique=True,
                constraint_backed=False,
                predicate="known_to = '9999-12-31 23:59:59+00'::timestamp with time zone",
            ),
        ),
        (
            "ix_trades_timestamp",
            _safe_index_shape(
                ("timestamp",),
                unique=False,
                constraint_backed=False,
            ),
        ),
        (
            "trades_pkey",
            _safe_index_shape(("id",), unique=True, constraint_backed=True),
        ),
        (
            "uq_trade_instr_tid_exec",
            _safe_index_shape(
                ("instrument_public_id", "trade_id", "executed_at"),
                unique=True,
                constraint_backed=False,
            ),
        ),
        (
            "uq_trade_instrument_trade_id",
            _safe_index_shape(
                ("instrument_public_id", "trade_id"),
                unique=True,
                constraint_backed=True,
            ),
        ),
    ),
}


def _literal_ordinary_index_shape(
    connection: Connection,
    relation: str,
    index_name: str,
) -> lifecycle.IndexShape | None:
    """Return a literal observed index shape without consulting source contracts.

    Args:
        connection: Unused catalog double required by the patched call signature.
        relation: Allowlisted ordinary table name.
        index_name: Exact catalog index name requested by the verifier.

    Returns:
        Independently pinned shape or no shape for an unexpected index name.
    """
    del connection
    table = cast(lifecycle.MarketDataTable, relation)
    return dict(_ORDINARY_INDEX_MANIFESTS[table]).get(index_name)


def _column_rows(
    table: lifecycle.MarketDataTable,
    *,
    prepared: bool = False,
) -> tuple[tuple[str, str, bool, str | None, str, str], ...]:
    """Return an independently pinned catalog manifest.

    Args:
        table: Table whose literal post-0042 manifest is required.
        prepared: Whether the trades range key has been hardened to NOT NULL.

    Returns:
        Ordered pg_attribute-shaped rows.
    """
    rows = _COLUMN_MANIFESTS[table]
    if not prepared:
        return rows
    return tuple(
        (
            name,
            data_type,
            False if name == "executed_at" else nullable,
            default,
            identity,
            generated,
        )
        for name, data_type, nullable, default, identity, generated in rows
    )


def test_current_anchor_is_an_exact_utc_midnight() -> None:
    """The implicit anchor must retain UTC date and zero time precision."""
    before = datetime.now(UTC).date()

    anchor = lifecycle.current_utc_anchor()

    after = datetime.now(UTC).date()
    assert anchor.date() in {before, after}
    assert anchor.tzinfo is UTC
    assert anchor.timetz().replace(tzinfo=None) == datetime.min.time()


@pytest.mark.parametrize(
    ("kind", "expected"),
    [
        (None, lifecycle.RelationState.MISSING),
        ("r", lifecycle.RelationState.ORDINARY),
        ("p", lifecycle.RelationState.PARTITIONED),
        ("v", lifecycle.RelationState.OTHER),
    ],
)
def test_relation_state_maps_the_closed_catalog_vocabulary(
    kind: str | None,
    expected: lifecycle.RelationState,
) -> None:
    """Every PostgreSQL relation kind must enter one explicit lifecycle state."""
    assert lifecycle._relation_state(kind) is expected


@pytest.mark.parametrize("kind", [None, "r", "v"])
def test_inspect_returns_a_closed_nonpartitioned_report(kind: str | None) -> None:
    """Inspection must stop before child queries for every nonparent state."""
    connection = _connection_double()
    relation = MagicMock()
    relation.one_or_none.return_value = None if kind is None else (kind, False)
    connection.execute.side_effect = ((), relation)

    report = lifecycle.inspect(connection, "ticks", _ANCHOR)

    assert report.state is lifecycle._relation_state(kind)
    assert report.partition_key is None
    assert report.partitions == ()
    assert report.future_leaf_count == 0
    assert report.future_leaf_alarm is True
    assert connection.scalar.call_count == 1
    assert connection.execute.call_count == 2


def test_inspect_reports_partition_key_default_rows_and_future_alarm() -> None:
    """Partition inspection must derive all topology fields from catalog facts."""
    connection = _connection_double()
    relation = MagicMock()
    relation.one_or_none.return_value = ("p", False)
    connection.execute.side_effect = ((), relation)
    connection.scalar.side_effect = ("public", "RANGE (executed_at)")
    partitions = (
        lifecycle.PartitionRef(
            "trades_d20260801",
            "FOR VALUES FROM ('2026-08-01 00:00:00+00') TO ('2026-08-02 00:00:00+00')",
        ),
        lifecycle.PartitionRef("trades_default", "DEFAULT"),
    )

    with (
        patch.object(lifecycle, "_direct_partitions", return_value=partitions),
        patch.object(lifecycle, "_relation_has_rows", return_value=True) as row_probe,
    ):
        report = lifecycle.inspect(connection, "trades", _ANCHOR)

    assert report.state is lifecycle.RelationState.PARTITIONED
    assert report.partition_key == "RANGE (executed_at)"
    assert report.default_attached is True
    assert report.default_has_rows is True
    assert report.future_leaf_count == 1
    assert report.future_leaf_alarm is True
    row_probe.assert_called_once_with(connection, "trades_default")


def test_inspect_skips_the_row_probe_without_an_attached_default() -> None:
    """A missing DEFAULT edge must not trigger SQL against an unrelated name."""
    connection = _connection_double()
    relation = MagicMock()
    relation.one_or_none.return_value = ("p", False)
    connection.execute.side_effect = ((), relation)
    connection.scalar.side_effect = ("public", "RANGE (timestamp)")

    with (
        patch.object(lifecycle, "_direct_partitions", return_value=()),
        patch.object(lifecycle, "_relation_has_rows") as row_probe,
    ):
        report = lifecycle.inspect(connection, "ticks", _ANCHOR)

    assert report.default_attached is False
    assert report.default_has_rows is False
    row_probe.assert_not_called()


def test_adopt_returns_a_verified_noop_for_an_existing_parent() -> None:
    """Adoption must verify a parent before treating it as an idempotent no-op."""
    connection = _connection_double()
    topology = _partitioned_report("ticks")

    with (
        patch.object(lifecycle, "inspect", return_value=topology),
        patch.object(lifecycle, "_verify_partitioned") as verify,
    ):
        result = lifecycle.adopt(connection, "ticks", _ANCHOR)

    assert result.action is lifecycle.LifecycleAction.NOOP
    assert result.statements == ()
    verify.assert_called_once_with(connection, lifecycle._spec("ticks"), _ANCHOR)
    assert connection.rollback.call_count == 2


@pytest.mark.parametrize(
    "state",
    [lifecycle.RelationState.MISSING, lifecycle.RelationState.OTHER],
)
def test_adopt_refuses_every_unsupported_source_state(
    state: lifecycle.RelationState,
) -> None:
    """Adoption must fail closed when no recognizable ordinary table exists."""
    connection = _connection_double()

    with (
        patch.object(lifecycle, "inspect", return_value=_ordinary_report("ticks", state)),
        pytest.raises(lifecycle.DailyPartitionError, match=state.value),
    ):
        lifecycle.adopt(connection, "ticks", _ANCHOR)


@pytest.mark.parametrize("trade_u3", ["", "CREATE UNIQUE INDEX CONCURRENTLY u3"])
def test_live_adopt_executes_only_the_planned_preparation(
    trade_u3: str,
) -> None:
    """Live adoption must conditionally build U3 and return final verified status."""
    connection = _connection_double()
    final = replace(
        _partitioned_report("trades"),
        future_leaf_count=14,
        future_leaf_alarm=False,
    )

    with (
        patch.object(lifecycle, "inspect", return_value=_ordinary_report("trades")),
        patch.object(lifecycle, "_verify_ordinary_schema"),
        patch.object(lifecycle, "_verify_sequence_owner"),
        patch.object(
            lifecycle,
            "_ordinary_check_constraints",
            return_value=(("ck_trades_sequence_id", "CHECK (sequence_id > 0)"),),
        ),
        patch.object(lifecycle, "_trade_u3_statement", return_value=trade_u3),
        patch.object(
            lifecycle,
            "_range_preparation_statements",
            return_value=("ALTER TABLE trades PREPARE",),
        ),
        patch.object(
            lifecycle,
            "_cutover_statements",
            return_value=("ALTER TABLE trades CUTOVER",),
        ),
        patch.object(lifecycle, "_create_trade_u3_concurrently") as create_u3,
        patch.object(lifecycle, "_prepare_legacy_range") as prepare,
        patch.object(lifecycle, "_execute_cutover", return_value=final) as cutover,
    ):
        result = lifecycle.adopt(
            connection,
            "trades",
            _ANCHOR,
            dry_run=False,
        )

    assert result.action is lifecycle.LifecycleAction.ADOPTED
    assert result.future_leaf_count == 14
    assert result.statements[-1] == "ALTER TABLE trades CUTOVER"
    if trade_u3:
        create_u3.assert_called_once_with(connection, trade_u3)
    else:
        create_u3.assert_not_called()
    prepare.assert_called_once_with(connection, lifecycle._spec("trades"), _ANCHOR)
    cutover.assert_called_once()


def test_ensure_future_leaves_refuses_a_missing_default() -> None:
    """Leaf maintenance must refuse before planning when DEFAULT is detached."""
    connection = _connection_double()
    topology = _partitioned_report("ticks", default_attached=False)

    with (
        patch.object(lifecycle, "inspect", return_value=topology),
        patch.object(lifecycle, "_require_partitioned_topology"),
        pytest.raises(lifecycle.DailyPartitionError, match="not attached"),
    ):
        lifecycle.ensure_future_leaves(connection, "ticks", _ANCHOR)


def test_ensure_future_leaves_returns_noop_for_a_complete_window() -> None:
    """A complete fourteen-day window must produce no redundant DDL."""
    spec = lifecycle._spec("ticks")
    partitions = tuple(
        lifecycle.PartitionRef(
            lifecycle._daily_name(spec, _ANCHOR + timedelta(days=offset)),
            lifecycle._daily_leaf_statements(
                spec,
                _ANCHOR + timedelta(days=offset),
            )[0].split(
                "FOR VALUES ", maxsplit=1
            )[1],
        )
        for offset in range(lifecycle.FUTURE_LEAF_TARGET)
    )
    topology = replace(
        _partitioned_report("ticks", partitions),
        future_leaf_alarm=False,
    )
    connection = _connection_double()

    with (
        patch.object(lifecycle, "inspect", return_value=topology),
        patch.object(lifecycle, "_require_partitioned_topology"),
    ):
        result = lifecycle.ensure_future_leaves(connection, "ticks", _ANCHOR)

    assert result.action is lifecycle.LifecycleAction.NOOP
    assert result.statements == ()
    assert result.future_leaf_count == lifecycle.FUTURE_LEAF_TARGET


def test_ensure_future_leaves_plans_every_missing_leaf() -> None:
    """Dry-run maintenance must report the exact complete missing window."""
    connection = _connection_double()
    topology = _partitioned_report("candles")

    with (
        patch.object(lifecycle, "inspect", return_value=topology),
        patch.object(lifecycle, "_require_partitioned_topology"),
    ):
        result = lifecycle.ensure_future_leaves(connection, "candles", _ANCHOR)

    assert result.action is lifecycle.LifecycleAction.PLANNED
    assert result.future_leaf_count == lifecycle.FUTURE_LEAF_TARGET
    assert result.future_leaf_alarm is False
    assert len(result.statements) == lifecycle.FUTURE_LEAF_TARGET * 3


def test_ensure_future_leaves_executes_and_reports_fresh_topology() -> None:
    """Live maintenance must execute the plan and trust only a fresh inspection."""
    connection = _connection_double()
    initial = _partitioned_report("ticks")
    final = replace(initial, future_leaf_count=14, future_leaf_alarm=False)

    with (
        patch.object(lifecycle, "inspect", side_effect=(initial, final)),
        patch.object(lifecycle, "_require_partitioned_topology"),
        patch.object(lifecycle, "_execute_ensure") as execute,
    ):
        result = lifecycle.ensure_future_leaves(
            connection,
            "ticks",
            _ANCHOR,
            dry_run=False,
        )

    assert result.action is lifecycle.LifecycleAction.ENSURED
    assert result.future_leaf_count == 14
    execute.assert_called_once()
    assert connection.rollback.call_count == 3


def test_detach_refuses_a_missing_expired_leaf() -> None:
    """DETACH must reject an expired name without a current attachment edge."""
    connection = _connection_double()

    with (
        patch.object(lifecycle, "inspect", return_value=_partitioned_report("trades")),
        patch.object(lifecycle, "_require_partitioned_topology"),
        pytest.raises(lifecycle.DailyPartitionError, match="not an attached partition"),
    ):
        lifecycle.detach(connection, "trades", date(2026, 6, 1), _ANCHOR)


def test_detach_refuses_a_mismatched_expired_leaf_bound() -> None:
    """DETACH must reject a correctly named child attached to another day."""
    connection = _connection_double()
    child = lifecycle.PartitionRef(
        "trades_d20260601",
        "FOR VALUES FROM ('2026-06-02 00:00:00+00') TO ('2026-06-03 00:00:00+00')",
    )

    with (
        patch.object(
            lifecycle,
            "inspect",
            return_value=_partitioned_report("trades", (child,)),
        ),
        patch.object(lifecycle, "_require_partitioned_topology"),
        pytest.raises(lifecycle.DailyPartitionError, match="unexpected partition bound"),
    ):
        lifecycle.detach(connection, "trades", date(2026, 6, 1), _ANCHOR)


def test_live_detach_uses_the_bounded_retry_executor() -> None:
    """Live DETACH must route its sole DDL through bounded lock retries."""
    connection = _connection_double()
    child = lifecycle.PartitionRef(
        "trades_d20260601",
        "FOR VALUES FROM ('2026-06-01 00:00:00+00') TO ('2026-06-02 00:00:00+00')",
    )

    with (
        patch.object(
            lifecycle,
            "inspect",
            return_value=_partitioned_report("trades", (child,)),
        ),
        patch.object(lifecycle, "_require_partitioned_topology"),
        patch.object(lifecycle, "_detach_with_retries") as execute,
    ):
        result = lifecycle.detach(
            connection,
            "trades",
            date(2026, 6, 1),
            _ANCHOR,
            dry_run=False,
        )

    statement = "ALTER TABLE trades DETACH PARTITION trades_d20260601"
    assert result.action is lifecycle.LifecycleAction.DETACHED
    assert result.statements == (statement,)
    execute.assert_called_once_with(
        connection,
        lifecycle._spec("trades"),
        "trades_d20260601",
        datetime(2026, 6, 1, tzinfo=UTC),
    )


def test_private_spec_rejects_a_runtime_allowlist_escape() -> None:
    """The internal resolver must retain the public allowlist boundary."""
    with pytest.raises(ValueError, match="ticks, trades"):
        lifecycle._spec(cast(lifecycle.MarketDataTable, "orders"))


def test_anchor_validation_rejects_a_naive_value() -> None:
    """Anchor validation must reject values without a usable UTC offset."""
    with pytest.raises(ValueError, match="timezone-aware UTC"):
        lifecycle._validate_anchor(datetime(2026, 8, 1))


def test_anchor_validation_rejects_nonutc_and_nonmidnight_values() -> None:
    """Both offset and precision drift must fail before SQL rendering."""
    non_utc = datetime.fromisoformat("2026-08-01T00:00:00+02:00")
    non_midnight = datetime(2026, 8, 1, 0, 0, 0, 1, tzinfo=UTC)

    with pytest.raises(ValueError, match="timezone-aware UTC"):
        lifecycle._validate_anchor(non_utc)
    with pytest.raises(ValueError, match="exact UTC midnight"):
        lifecycle._validate_anchor(non_midnight)


def test_postgresql_guard_rejects_another_dialect() -> None:
    """Catalog operations must not execute against SQLite-shaped connections."""
    connection = _connection_double()
    connection.dialect = SimpleNamespace(name="sqlite")

    with pytest.raises(lifecycle.DailyPartitionError, match="require PostgreSQL"):
        lifecycle._require_postgresql(connection)


def test_mutation_entry_points_reject_an_active_transaction() -> None:
    """Mutation ownership must remain explicit at every public entry point."""
    connection = _connection_double()
    connection.in_transaction.return_value = True

    with pytest.raises(lifecycle.DailyPartitionError, match="active transaction"):
        lifecycle._require_clean_connection(connection)


def test_direct_partition_reader_preserves_name_bound_and_order() -> None:
    """Direct partition rows must become immutable typed references unchanged."""
    rows = (
        (
            "public",
            "ticks_d20260801",
            "FOR VALUES FROM ('a') TO ('b')",
            "r",
            True,
            False,
            1,
        ),
        ("public", "ticks_default", "DEFAULT", "r", True, False, 1),
    )
    connection = _connection_double(rows)

    partitions = lifecycle._direct_partitions(connection, lifecycle._spec("ticks"))

    assert partitions == (
        lifecycle.PartitionRef("ticks_d20260801", "FOR VALUES FROM ('a') TO ('b')"),
        lifecycle.PartitionRef("ticks_default", "DEFAULT"),
    )


@pytest.mark.parametrize(("scalar", "expected"), [(0, False), (1, True)])
def test_relation_row_probe_normalizes_scalar_truth(
    scalar: int,
    expected: bool,
) -> None:
    """The DEFAULT probe must normalize PostgreSQL scalar truth exactly."""
    connection = _connection_double()
    connection.scalar.return_value = scalar

    assert lifecycle._relation_has_rows(connection, "ticks_default") is expected


def test_future_leaf_counter_rejects_names_and_days_outside_contract() -> None:
    """Only canonical nonhistoric daily names may contribute to the alarm count."""
    spec = lifecycle._spec("ticks")
    partitions = (
        lifecycle.PartitionRef("other_d20260801", "ignored"),
        lifecycle.PartitionRef("ticks_d2026081", "ignored"),
        lifecycle.PartitionRef("ticks_d2026AB01", "ignored"),
        lifecycle.PartitionRef("ticks_d20260731", "historic"),
        lifecycle.PartitionRef("ticks_d20260801", "future"),
        lifecycle.PartitionRef("ticks_default", "DEFAULT"),
    )

    assert lifecycle._future_leaf_count(spec, partitions, _ANCHOR) == 1


def test_desired_bound_verifier_accepts_missing_and_exact_children() -> None:
    """Desired names may be absent or exact, but no third state is accepted."""
    spec = lifecycle._spec("ticks")
    exact = lifecycle.PartitionRef(
        "ticks_d20260801",
        "FOR VALUES FROM ('2026-08-01 00:00:00+00') TO ('2026-08-02 00:00:00+00')",
    )

    lifecycle._verify_desired_daily_bounds(spec, (exact,), _ANCHOR)
    lifecycle._verify_desired_daily_bounds(spec, (), _ANCHOR)


def test_ordinary_schema_verifier_runs_every_independent_catalog_check() -> None:
    """Ordinary verification must compose columns, checks, indexes and constraints."""
    connection = _connection_double()
    spec = lifecycle._spec("ticks")

    with (
        patch.object(lifecycle, "_verify_adoption_names_available") as names,
        patch.object(lifecycle, "_verify_relation_columns") as columns,
        patch.object(lifecycle, "_verify_ordinary_checks") as checks,
        patch.object(lifecycle, "_verify_ordinary_indexes") as indexes,
        patch.object(lifecycle, "_verify_noncheck_constraints") as constraints,
        patch.object(lifecycle, "_constraint_info", return_value=None),
    ):
        lifecycle._verify_ordinary_schema(connection, spec, _ANCHOR)

    names.assert_called_once_with(connection, spec, _ANCHOR)
    columns.assert_called_once_with(
        connection,
        spec,
        "ticks",
        partition_key_not_null=False,
    )
    checks.assert_called_once_with(connection, spec)
    indexes.assert_called_once_with(connection, spec)
    constraints.assert_called_once_with(
        connection,
        spec,
        "ticks",
        lifecycle.ConstraintRole.ORDINARY,
    )


def test_ordinary_schema_verifier_rejects_a_resumed_wrong_range() -> None:
    """A named resumed range CHECK must match the requested anchor exactly."""
    connection = _connection_double()
    info = lifecycle.ConstraintInfo("CHECK (timestamp < 'wrong')", True)

    with (
        patch.object(lifecycle, "_verify_adoption_names_available"),
        patch.object(lifecycle, "_verify_relation_columns"),
        patch.object(lifecycle, "_verify_ordinary_checks"),
        patch.object(lifecycle, "_verify_ordinary_indexes"),
        patch.object(lifecycle, "_verify_noncheck_constraints"),
        patch.object(lifecycle, "_constraint_info", return_value=info),
        patch.object(lifecycle, "_range_constraint_matches", return_value=False),
        pytest.raises(lifecycle.DailyPartitionError, match="unexpected definition"),
    ):
        lifecycle._verify_ordinary_schema(
            connection,
            lifecycle._spec("ticks"),
            _ANCHOR,
        )


def test_prepared_schema_refuses_a_source_that_changed_relation_state() -> None:
    """Locked cutover revalidation must still require the ordinary source state."""
    connection = _connection_double()

    with (
        patch.object(
            lifecycle,
            "inspect",
            return_value=_partitioned_report("ticks"),
        ),
        pytest.raises(lifecycle.DailyPartitionError, match="expected ordinary table"),
    ):
        lifecycle._verify_prepared_ordinary_schema(
            connection,
            lifecycle._spec("ticks"),
            _ANCHOR,
        )


@pytest.mark.parametrize(
    "range_info",
    [
        None,
        lifecycle.ConstraintInfo("correct", False),
        lifecycle.ConstraintInfo("wrong", True),
    ],
)
def test_prepared_schema_requires_the_exact_validated_range(
    range_info: lifecycle.ConstraintInfo | None,
) -> None:
    """Missing, unvalidated and mismatched range proofs must all fail cutover."""
    connection = _connection_double()

    with (
        patch.object(
            lifecycle,
            "inspect",
            return_value=_ordinary_report("ticks"),
        ),
        patch.object(lifecycle, "_verify_ordinary_schema"),
        patch.object(lifecycle, "_verify_relation_columns"),
        patch.object(lifecycle, "_constraint_info", return_value=range_info),
        patch.object(
            lifecycle,
            "_range_constraint_matches",
            return_value=range_info is not None and range_info.definition == "correct",
        ),
        pytest.raises(lifecycle.DailyPartitionError, match="exact validated bound"),
    ):
        lifecycle._verify_prepared_ordinary_schema(
            connection,
            lifecycle._spec("ticks"),
            _ANCHOR,
        )


def test_prepared_ticks_schema_verifies_columns_range_and_sequence() -> None:
    """A prepared nontrade source must pass every locked catalog proof."""
    connection = _connection_double()
    spec = lifecycle._spec("ticks")
    info = lifecycle.ConstraintInfo("correct", True)

    with (
        patch.object(lifecycle, "inspect", return_value=_ordinary_report("ticks")),
        patch.object(lifecycle, "_verify_adoption_names_available"),
        patch.object(lifecycle, "_verify_relation_columns") as columns,
        patch.object(lifecycle, "_verify_ordinary_checks"),
        patch.object(lifecycle, "_verify_ordinary_indexes"),
        patch.object(lifecycle, "_verify_noncheck_constraints"),
        patch.object(lifecycle, "_constraint_info", return_value=info),
        patch.object(lifecycle, "_range_constraint_matches", return_value=True),
        patch.object(lifecycle, "_verify_sequence_owner") as sequence,
    ):
        lifecycle._verify_prepared_ordinary_schema(connection, spec, _ANCHOR)

    columns.assert_called_once_with(
        connection,
        spec,
        "ticks",
        partition_key_not_null=True,
    )
    sequence.assert_called_once_with(connection, spec)


@pytest.mark.parametrize(
    "shape", [None, _safe_index_shape(("wrong",), unique=True, constraint_backed=False)]
)
def test_prepared_trades_schema_requires_the_exact_u3(
    shape: lifecycle.IndexShape | None,
) -> None:
    """A prepared trades source cannot cut over without its standalone U3."""
    connection = _connection_double()
    info = lifecycle.ConstraintInfo("correct", True)

    with (
        patch.object(lifecycle, "inspect", return_value=_ordinary_report("trades")),
        patch.object(lifecycle, "_verify_ordinary_schema"),
        patch.object(lifecycle, "_verify_relation_columns"),
        patch.object(lifecycle, "_constraint_info", return_value=info),
        patch.object(lifecycle, "_range_constraint_matches", return_value=True),
        patch.object(lifecycle, "_index_shape", return_value=shape),
        pytest.raises(lifecycle.DailyPartitionError, match="prepared trades U3"),
    ):
        lifecycle._verify_prepared_ordinary_schema(
            connection,
            lifecycle._spec("trades"),
            _ANCHOR,
        )


def test_prepared_trades_schema_accepts_an_exact_u3() -> None:
    """An exact standalone U3 must allow the prepared trades proof to finish."""
    connection = _connection_double()
    info = lifecycle.ConstraintInfo("correct", True)
    u3_shape = dict(_ORDINARY_INDEX_MANIFESTS["trades"])["uq_trade_instr_tid_exec"]

    with (
        patch.object(lifecycle, "inspect", return_value=_ordinary_report("trades")),
        patch.object(lifecycle, "_verify_ordinary_schema"),
        patch.object(lifecycle, "_verify_relation_columns"),
        patch.object(lifecycle, "_constraint_info", return_value=info),
        patch.object(lifecycle, "_range_constraint_matches", return_value=True),
        patch.object(lifecycle, "_index_shape", return_value=u3_shape),
        patch.object(lifecycle, "_verify_sequence_owner") as sequence,
    ):
        lifecycle._verify_prepared_ordinary_schema(
            connection,
            lifecycle._spec("trades"),
            _ANCHOR,
        )

    sequence.assert_called_once()


def test_ordinary_schema_delegates_with_preparation_tolerance() -> None:
    """Ordinary columns must leave only the trade key nullability preparable."""
    connection = _connection_double()
    spec = lifecycle._spec("trades")

    with (
        patch.object(lifecycle, "_verify_adoption_names_available"),
        patch.object(lifecycle, "_verify_relation_columns") as verify,
        patch.object(lifecycle, "_verify_ordinary_checks"),
        patch.object(lifecycle, "_verify_ordinary_indexes"),
        patch.object(lifecycle, "_verify_noncheck_constraints"),
        patch.object(lifecycle, "_constraint_info", return_value=None),
    ):
        lifecycle._verify_ordinary_schema(connection, spec, _ANCHOR)

    verify.assert_called_once_with(
        connection,
        spec,
        "trades",
        partition_key_not_null=False,
    )


@pytest.mark.parametrize("table", ["ticks", "candles", "trades"])
def test_relation_column_verifier_accepts_each_exact_contract(
    table: lifecycle.MarketDataTable,
) -> None:
    """Every post-0042 table contract must be accepted in ordinal order."""
    spec = lifecycle._spec(table)
    connection = _connection_double(_column_rows(table))

    lifecycle._verify_relation_columns(
        connection,
        spec,
        table,
        partition_key_not_null=False,
    )


def test_relation_column_verifier_hardens_the_prepared_trade_key() -> None:
    """Prepared trades must require NOT NULL on the partition key."""
    spec = lifecycle._spec("trades")
    connection = _connection_double(_column_rows("trades", prepared=True))

    lifecycle._verify_relation_columns(
        connection,
        spec,
        "trades",
        partition_key_not_null=True,
    )


def test_relation_column_verifier_rejects_a_wrong_column_count() -> None:
    """Missing columns must fail before pairwise catalog comparison."""
    spec = lifecycle._spec("ticks")
    connection = _connection_double(_column_rows("ticks")[:-1])

    with pytest.raises(lifecycle.DailyPartitionError, match="has 10 columns"):
        lifecycle._verify_relation_columns(
            connection,
            spec,
            "ticks",
            partition_key_not_null=False,
        )


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        (0, "wrong_name", "unexpected column"),
        (1, "text", "unexpected column"),
        (2, True, "unexpected nullability"),
        (3, "false", "unexpected default"),
        (4, "a", "identity or generated"),
        (5, "s", "identity or generated"),
    ],
)
def test_relation_column_verifier_rejects_each_catalog_drift(
    field: int,
    value: bool | str,
    message: str,
) -> None:
    """Each independently significant pg_attribute field must fail closed."""
    spec = lifecycle._spec("ticks")
    rows = list(_column_rows("ticks"))
    first = list(rows[0])
    first[field] = value
    rows[0] = tuple(first)
    connection = _connection_double(tuple(rows))

    with pytest.raises(lifecycle.DailyPartitionError, match=message):
        lifecycle._verify_relation_columns(
            connection,
            spec,
            "ticks",
            partition_key_not_null=False,
        )


@pytest.mark.parametrize(
    ("observed", "contract", "expected"),
    [
        (None, lifecycle.ColumnContract("x", "text", True), True),
        ("false", lifecycle.ColumnContract("x", "text", True), False),
        (None, lifecycle.ColumnContract("id", "bigint", False, "sequence"), False),
        (
            "nextval('public.ticks_id_seq'::regclass)",
            lifecycle.ColumnContract("id", "bigint", False, "sequence"),
            True,
        ),
        (
            "'native'::character varying",
            lifecycle.ColumnContract("source", "character varying(16)", False, "native"),
            True,
        ),
        (
            "'other'::character varying",
            lifecycle.ColumnContract("source", "character varying(16)", False, "native"),
            False,
        ),
        ("true", lifecycle.ColumnContract("complete", "boolean", False, "true"), True),
        ("false", lifecycle.ColumnContract("complete", "boolean", False, "true"), False),
    ],
)
def test_column_default_comparison_is_category_exact(
    observed: str | None,
    contract: lifecycle.ColumnContract,
    expected: bool,
) -> None:
    """Every default category must accept only its canonical PostgreSQL shape."""
    assert (
        lifecycle._column_default_matches(
            observed,
            contract,
            lifecycle._spec("ticks"),
        )
        is expected
    )


@pytest.mark.parametrize(
    ("definition", "expected"),
    [
        ('CHECK (("sequence_id" > 0))', "checksequence_id>0"),
        (
            "CHECK ((source)::text = ANY (ARRAY['native'::character varying]::text[]))",
            "checksource=anyarray['native']",
        ),
    ],
)
def test_check_compaction_removes_only_known_catalog_noise(
    definition: str,
    expected: str,
) -> None:
    """Constraint normalization must preserve semantic tokens while removing casts."""
    assert lifecycle._compact_check(definition) == expected


def test_ordinary_check_reader_skips_range_and_rejects_unvalidated_rows() -> None:
    """Only validated ordinary CHECKs may be copied to the new parent."""
    spec = lifecycle._spec("ticks")
    rows = (
        ("ck_ticks_legacy_range", "CHECK (timestamp < 'x')", True),
        ("ck_ticks_sequence_id", "CHECK (sequence_id > 0)", True),
    )
    connection = _connection_double(rows)

    checks = lifecycle._ordinary_check_constraints(connection, spec)

    assert checks == (("ck_ticks_sequence_id", "CHECK (sequence_id > 0)"),)

    connection.execute.return_value = (("ck_ticks_sequence_id", "CHECK (sequence_id > 0)", False),)
    with pytest.raises(lifecycle.DailyPartitionError, match="not validated"):
        lifecycle._ordinary_check_constraints(connection, spec)


def test_ordinary_check_verifier_accepts_ticks_and_candles() -> None:
    """Both ordinary CHECK manifests must retain their exact semantics."""
    connection = _connection_double()

    with patch.object(
        lifecycle,
        "_ordinary_check_constraints",
        return_value=(("ck_ticks_sequence_id", "CHECK (sequence_id > 0)"),),
    ):
        lifecycle._verify_ordinary_checks(connection, lifecycle._spec("ticks"))

    candle_checks = (
        (
            "ck_candle_source",
            "CHECK (source = ANY (ARRAY['native', 'calculated', 'synthesized']))",
        ),
        ("ck_candles_sequence_id", "CHECK (sequence_id > 0)"),
    )
    with patch.object(
        lifecycle,
        "_ordinary_check_constraints",
        return_value=candle_checks,
    ):
        lifecycle._verify_ordinary_checks(
            connection,
            lifecycle._spec("candles"),
            "candles_legacy",
        )


@pytest.mark.parametrize(
    ("checks", "message"),
    [
        ((), "manifest drifted"),
        (
            (("ck_ticks_sequence_id", "CHECK (sequence_id >= 0)"),),
            "unexpected definition",
        ),
    ],
)
def test_ordinary_check_verifier_rejects_ticks_drift(
    checks: tuple[tuple[str, str], ...],
    message: str,
) -> None:
    """Missing or weakened ticks CHECKs must fail independently."""
    connection = _connection_double()

    with (
        patch.object(lifecycle, "_ordinary_check_constraints", return_value=checks),
        pytest.raises(lifecycle.DailyPartitionError, match=message),
    ):
        lifecycle._verify_ordinary_checks(connection, lifecycle._spec("ticks"))


def test_ordinary_check_verifier_rejects_candle_vocabulary_drift() -> None:
    """The candle source vocabulary cannot silently gain another value."""
    checks = (
        (
            "ck_candle_source",
            "CHECK (source = ANY (ARRAY['native', 'other']))",
        ),
        ("ck_candles_sequence_id", "CHECK (sequence_id > 0)"),
    )

    with (
        patch.object(lifecycle, "_ordinary_check_constraints", return_value=checks),
        pytest.raises(lifecycle.DailyPartitionError, match="ck_candle_source"),
    ):
        lifecycle._verify_ordinary_checks(
            _connection_double(),
            lifecycle._spec("candles"),
        )


def test_relation_index_name_reader_preserves_catalog_order() -> None:
    """Index manifest reads must preserve the query's stable lexical order."""
    connection = _connection_double((("a_index",), ("b_index",)))

    assert lifecycle._relation_index_names(connection, "ticks") == (
        "a_index",
        "b_index",
    )


@pytest.mark.parametrize("table", ["ticks", "candles", "trades"])
def test_ordinary_index_verifier_accepts_each_exact_manifest(
    table: lifecycle.MarketDataTable,
) -> None:
    """Each ordinary table must accept all and only its exact index shapes."""
    spec = lifecycle._spec(table)
    names = tuple(name for name, _shape in _ORDINARY_INDEX_MANIFESTS[table])

    with (
        patch.object(lifecycle, "_relation_index_names", return_value=names),
        patch.object(lifecycle, "_index_shape", side_effect=_literal_ordinary_index_shape),
    ):
        lifecycle._verify_ordinary_indexes(_connection_double(), spec)


def test_ordinary_index_verifier_rejects_manifest_and_shape_drift() -> None:
    """Extra names and malformed expected indexes must fail at separate boundaries."""
    connection = _connection_double()
    spec = lifecycle._spec("ticks")
    names = tuple(name for name, _shape in _ORDINARY_INDEX_MANIFESTS["ticks"])

    with (
        patch.object(
            lifecycle,
            "_relation_index_names",
            return_value=(*names, "unexpected"),
        ),
        pytest.raises(lifecycle.DailyPartitionError, match="manifest"),
    ):
        lifecycle._verify_ordinary_indexes(connection, spec)

    with (
        patch.object(lifecycle, "_relation_index_names", return_value=names),
        patch.object(lifecycle, "_index_shape", return_value=None),
        pytest.raises(lifecycle.DailyPartitionError, match="unexpected shape"),
    ):
        lifecycle._verify_ordinary_indexes(connection, spec)


@pytest.mark.parametrize(
    "exact",
    [None, False],
)
def test_sequence_owner_verifier_rejects_missing_or_wrong_ownership(
    exact: bool | None,
) -> None:
    """The source id sequence must have the exact transferable owner."""
    connection = _connection_double()
    connection.scalar.return_value = exact

    with pytest.raises(
        lifecycle.DailyPartitionError,
        match="ticks_id_seq parameters or ownership are not exact",
    ):
        lifecycle._verify_sequence_owner(connection, lifecycle._spec("ticks"))


@pytest.mark.parametrize("table", ["ticks", "candles", "trades"])
def test_sequence_owner_verifier_accepts_an_exact_catalog_proof(
    table: lifecycle.MarketDataTable,
) -> None:
    """Each exact sequence parameter and ownership proof must be accepted."""
    connection = _connection_double()
    connection.scalar.return_value = True

    lifecycle._verify_sequence_owner(connection, lifecycle._spec(table))

    parameters = connection.scalar.call_args.args[1]
    assert parameters == {
        "sequence": f"{table}_id_seq",
        "owner": table,
    }


def test_noncheck_constraint_reader_preserves_every_structural_flag() -> None:
    """Constraint catalog rows must be copied into exact immutable shapes."""
    connection = _connection_double(
        (
            (
                "ticks_pkey",
                "p",
                "PRIMARY KEY (id)",
                True,
                False,
                False,
                "ticks_pkey",
            ),
        )
    )

    assert lifecycle._noncheck_constraints(connection, "ticks") == (
        lifecycle.ConstraintShape(
            name="ticks_pkey",
            kind="p",
            definition="PRIMARY KEY (id)",
            validated=True,
            deferrable=False,
            initially_deferred=False,
            index_name="ticks_pkey",
        ),
    )


@pytest.mark.parametrize("table", ["ticks", "candles", "trades"])
def test_noncheck_constraint_verifier_accepts_exact_local_manifests(
    table: lifecycle.MarketDataTable,
) -> None:
    """Ordinary local PK and trades U2 constraints must remain exact."""
    spec = lifecycle._spec(table)

    with patch.object(
        lifecycle,
        "_noncheck_constraints",
        return_value=_literal_noncheck_manifest(
            table,
            lifecycle.ConstraintRole.ORDINARY,
        ),
    ):
        lifecycle._verify_noncheck_constraints(
            _connection_double(),
            spec,
            table,
            lifecycle.ConstraintRole.ORDINARY,
        )


def test_parent_noncheck_constraint_verifier_accepts_only_not_null_constraints() -> None:
    """A partitioned parent must omit leaf-local keys from its exact manifest."""
    manifest = _literal_noncheck_manifest(
        "trades",
        lifecycle.ConstraintRole.PARENT,
    )

    with patch.object(lifecycle, "_noncheck_constraints", return_value=manifest):
        lifecycle._verify_noncheck_constraints(
            _connection_double(),
            lifecycle._spec("trades"),
            "trades",
            lifecycle.ConstraintRole.PARENT,
        )

    assert all(shape.kind == "n" for shape in manifest)


def test_noncheck_constraint_verifier_rejects_manifest_drift() -> None:
    """A missing local primary key must fail before shape comparison."""
    manifest = tuple(
        shape
        for shape in _literal_noncheck_manifest(
            "ticks",
            lifecycle.ConstraintRole.ORDINARY,
        )
        if shape.name != "ticks_pkey"
    )

    with (
        patch.object(lifecycle, "_noncheck_constraints", return_value=manifest),
        pytest.raises(lifecycle.DailyPartitionError, match="manifest drifted"),
    ):
        lifecycle._verify_noncheck_constraints(
            _connection_double(),
            lifecycle._spec("ticks"),
            "ticks",
            lifecycle.ConstraintRole.ORDINARY,
        )


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("name", "wrong", "manifest drifted"),
        ("kind", "u", "malformed"),
        ("definition", "PRIMARY KEY (public_id)", "malformed"),
        ("validated", False, "malformed"),
        ("deferrable", True, "malformed"),
        ("initially_deferred", True, "malformed"),
        ("index_name", "wrong", "malformed"),
    ],
)
def test_noncheck_constraint_verifier_rejects_each_shape_drift(
    field: str,
    value: object,
    message: str,
) -> None:
    """Each primary-key catalog flag must independently remain fail closed."""
    manifest = list(
        _literal_noncheck_manifest(
            "ticks",
            lifecycle.ConstraintRole.ORDINARY,
        )
    )
    primary_index = next(
        index for index, shape in enumerate(manifest) if shape.name == "ticks_pkey"
    )
    shape = manifest[primary_index]
    malformed = replace(shape, **{field: value})
    manifest[primary_index] = malformed

    with (
        patch.object(lifecycle, "_noncheck_constraints", return_value=tuple(manifest)),
        pytest.raises(lifecycle.DailyPartitionError, match=message),
    ):
        lifecycle._verify_noncheck_constraints(
            _connection_double(),
            lifecycle._spec("ticks"),
            "ticks",
            lifecycle.ConstraintRole.ORDINARY,
        )


def test_expected_noncheck_constraints_match_the_literal_manifests() -> None:
    """Every ordinary table must retain its exact PK and trades U2 contract."""
    for table in ("ticks", "candles", "trades"):
        manifest = _literal_noncheck_manifest(
            table,
            lifecycle.ConstraintRole.ORDINARY,
        )
        expected = tuple(
            (
                shape.name,
                shape.kind,
                lifecycle._compact_key_constraint(shape.definition),
                shape.index_name,
            )
            for shape in manifest
        )
        assert (
            lifecycle._expected_noncheck_constraints(
                lifecycle._spec(table),
                table,
                lifecycle.ConstraintRole.ORDINARY,
            )
            == expected
        )
    assert lifecycle._compact_key_constraint('UNIQUE ("a", "b")') == "uniquea,b"


def test_trade_u3_planner_handles_nontrade_missing_and_exact_states() -> None:
    """The standalone U3 planner must distinguish all safe catalog states."""
    connection = _connection_double()
    assert lifecycle._trade_u3_statement(connection, lifecycle._spec("ticks")) == ""

    with patch.object(lifecycle, "_index_shape", return_value=None):
        statement = lifecycle._trade_u3_statement(
            connection,
            lifecycle._spec("trades"),
        )
    assert statement.startswith("CREATE UNIQUE INDEX CONCURRENTLY")

    u3_shape = dict(_ORDINARY_INDEX_MANIFESTS["trades"])["uq_trade_instr_tid_exec"]
    with patch.object(
        lifecycle,
        "_index_shape",
        return_value=u3_shape,
    ):
        assert lifecycle._trade_u3_statement(connection, lifecycle._spec("trades")) == ""


@pytest.mark.parametrize(
    ("info", "nullable", "expected"),
    [
        (
            None,
            True,
            (
                (
                    "ALTER TABLE ticks ADD CONSTRAINT ck_ticks_legacy_range "
                    'CHECK ("timestamp" IS NOT NULL AND "timestamp" < '
                    "TIMESTAMPTZ '2026-08-01T00:00:00+00:00') NOT VALID"
                ),
                "ALTER TABLE ticks VALIDATE CONSTRAINT ck_ticks_legacy_range",
                'ALTER TABLE ticks ALTER COLUMN "timestamp" SET NOT NULL',
            ),
        ),
        (
            None,
            False,
            (
                (
                    "ALTER TABLE ticks ADD CONSTRAINT ck_ticks_legacy_range "
                    'CHECK ("timestamp" IS NOT NULL AND "timestamp" < '
                    "TIMESTAMPTZ '2026-08-01T00:00:00+00:00') NOT VALID"
                ),
                "ALTER TABLE ticks VALIDATE CONSTRAINT ck_ticks_legacy_range",
            ),
        ),
        (
            lifecycle.ConstraintInfo("correct", False),
            True,
            (
                "ALTER TABLE ticks VALIDATE CONSTRAINT ck_ticks_legacy_range",
                'ALTER TABLE ticks ALTER COLUMN "timestamp" SET NOT NULL',
            ),
        ),
        (
            lifecycle.ConstraintInfo("correct", False),
            False,
            ("ALTER TABLE ticks VALIDATE CONSTRAINT ck_ticks_legacy_range",),
        ),
        (
            lifecycle.ConstraintInfo("correct", True),
            True,
            ('ALTER TABLE ticks ALTER COLUMN "timestamp" SET NOT NULL',),
        ),
        (
            lifecycle.ConstraintInfo("correct", True),
            False,
            (),
        ),
    ],
)
def test_range_preparation_plans_only_missing_proof_steps(
    info: lifecycle.ConstraintInfo | None,
    nullable: bool,
    expected: tuple[str, ...],
) -> None:
    """Resumed preparation must emit exactly the still-missing range proof steps."""
    connection = _connection_double()
    spec = lifecycle._spec("ticks")

    with (
        patch.object(lifecycle, "_constraint_info", return_value=info),
        patch.object(lifecycle, "_range_constraint_matches", return_value=True),
        patch.object(lifecycle, "_column_is_nullable", return_value=nullable),
    ):
        statements = lifecycle._range_preparation_statements(
            connection,
            spec,
            _ANCHOR,
        )

    assert statements == expected


def test_range_preparation_rejects_a_named_wrong_bound() -> None:
    """A resumed named CHECK cannot be repurposed for another anchor."""
    info = lifecycle.ConstraintInfo("wrong", True)

    with (
        patch.object(lifecycle, "_constraint_info", return_value=info),
        patch.object(lifecycle, "_range_constraint_matches", return_value=False),
        pytest.raises(lifecycle.DailyPartitionError, match="unexpected definition"),
    ):
        lifecycle._range_preparation_statements(
            _connection_double(),
            lifecycle._spec("ticks"),
            _ANCHOR,
        )
