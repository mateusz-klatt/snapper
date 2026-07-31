"""Focused contracts for the daily market-data partition lifecycle.

The PostgreSQL convergence suite proves real catalog identity. These unit
tests pin the independent runtime planner's static allowlist, deterministic
bounds and names, safe dry-run default, DEFAULT refusal, retention guard, and
plain bounded-lock DETACH shape without starting a database cluster.
"""

from collections.abc import Iterator
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
from sqlalchemy.exc import DBAPIError

import snapper.data.daily_partitions as lifecycle

_ANCHOR = datetime(2026, 8, 1, tzinfo=UTC)


class _Psycopg2LockError(RuntimeError):
    """Represent psycopg2's legacy ``pgcode`` exception surface."""

    pgcode: str = "55P03"


class _ObservedPartitions(tuple[lifecycle.PartitionRef, ...]):
    """Record direct-child names as shape validation consumes them."""

    def __init__(self, values: tuple[lifecycle.PartitionRef, ...]) -> None:
        """Initialize an empty visit record for the supplied partitions."""
        self.visited: list[str] = []

    def __iter__(self) -> Iterator[lifecycle.PartitionRef]:
        """Yield direct children while recording their traversal order."""
        for partition in super().__iter__():
            self.visited.append(partition.name)
            yield partition


def _partitioned_report(
    table: lifecycle.MarketDataTable,
    partitions: tuple[lifecycle.PartitionRef, ...],
    *,
    default_attached: bool = True,
    default_has_rows: bool = False,
) -> lifecycle.PartitionInspection:
    """Build one typed partitioned topology report for a lifecycle unit test.

    Args:
        table: Allowlisted table represented by the report.
        partitions: Direct child catalog rows.
        default_attached: Whether the DEFAULT child is attached.
        default_has_rows: Whether the DEFAULT anomaly buffer is nonempty.

    Returns:
        A partitioned inspection with deterministic future status.
    """
    return lifecycle.PartitionInspection(
        table=table,
        state=lifecycle.RelationState.PARTITIONED,
        partition_key="RANGE (executed_at)",
        partitions=partitions,
        default_attached=default_attached,
        default_has_rows=default_has_rows,
        future_leaf_count=0,
        future_leaf_alarm=True,
    )


def _connection_double() -> Connection:
    """Build a clean PostgreSQL-shaped SQLAlchemy connection double.

    Returns:
        Typed connection double accepted by public lifecycle functions.
    """
    connection = MagicMock(spec=Connection)
    connection.in_transaction.return_value = False
    connection.dialect = SimpleNamespace(name="postgresql")
    connection.scalar.return_value = "public"
    return cast(Connection, connection)


def _index_shape_double(
    columns: tuple[str, ...],
    *,
    unique: bool,
    constraint_backed: bool,
    predicate: str | None = None,
) -> lifecycle.IndexShape:
    """Build one otherwise safe structural index catalog double.

    Args:
        columns: Ordered simple index keys.
        unique: Whether the index enforces uniqueness.
        constraint_backed: Whether a PostgreSQL constraint owns the index.
        predicate: Optional canonical partial-index predicate.

    Returns:
        An index shape suitable for one deliberately narrow mutation.
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


def test_parse_anchor_accepts_only_the_exact_explicit_utc_midnight() -> None:
    """The deterministic anchor parser must accept its sole wire representation.

    Given: The exact documented UTC-midnight representation.
    When: The public anchor parser validates it.
    Then: It returns an aware UTC midnight without changing the instant.
    """
    parsed = lifecycle.parse_anchor("2026-08-01T00:00:00+00:00")

    assert parsed == _ANCHOR
    assert parsed.tzinfo is UTC


@pytest.mark.parametrize(
    "value",
    [
        "2026-08-01",
        "2026-08-01T00:00:00Z",
        "2026-08-01T01:00:00+01:00",
        "2026-08-01T00:00:00.000000+00:00",
        "2026-02-30T00:00:00+00:00",
    ],
)
def test_parse_anchor_rejects_every_ambiguous_or_noncanonical_form(value: str) -> None:
    """Every wall-clock or alternate spelling must fail before SQL is built.

    Given: A date-only, offset-shifted, fractional, shorthand, or invalid value.
    When: The strict public parser receives it.
    Then: It raises ``ValueError`` instead of deriving a flaky partition bound.
    """
    with pytest.raises(ValueError):
        lifecycle.parse_anchor(value)


@pytest.mark.parametrize("table", ["ticks", "candles", "trades"])
def test_static_table_allowlist_accepts_only_market_data_relations(table: str) -> None:
    """All and only the three settled market-data tables must be selectable.

    Given: One name from the immutable lifecycle allowlist.
    When: Runtime validation narrows it to ``MarketDataTable``.
    Then: The same value is returned for safe identifier interpolation.
    """
    assert lifecycle.market_data_table(table) == table


def test_static_table_allowlist_rejects_an_unrelated_relation() -> None:
    """An operator cannot redirect lifecycle DDL to another Snapper table.

    Given: A valid database relation that is outside partitioning scope.
    When: Runtime table validation runs.
    Then: It refuses before any SQL identifier can be constructed.
    """
    with pytest.raises(ValueError, match="ticks, trades"):
        lifecycle.market_data_table("orders")


def test_public_operations_refuse_a_nonpublic_current_schema() -> None:
    """A modified search path cannot redirect unqualified lifecycle SQL.

    Given: A PostgreSQL connection whose current schema resolves to ``scratch``.
    When: Public catalog inspection begins.
    Then: It refuses before looking up or mutating any same-named relation.
    """
    connection = _connection_double()
    connection.scalar.return_value = "scratch"

    with pytest.raises(lifecycle.DailyPartitionError, match="expected 'public'"):
        lifecycle.inspect(connection, "ticks", _ANCHOR)


def test_inspect_classifies_a_nested_root_as_unsupported() -> None:
    """A same-named relation cannot masquerade as an independent root.

    Given: PostgreSQL reports the allowlisted root as a partition of another
        relation even though its relkind is partitioned-table.
    When: Read-only lifecycle inspection classifies that root.
    Then: It returns the closed OTHER state so adoption refuses before any
        preparation or DDL can begin.
    """
    connection = _connection_double()
    timezone_result = MagicMock()
    relation_result = MagicMock()
    relation_result.one_or_none.return_value = ("p", True)
    connection.execute.side_effect = (timezone_result, relation_result)

    report = lifecycle.inspect(connection, "trades", _ANCHOR)

    assert report.state is lifecycle.RelationState.OTHER
    assert report.partitions == ()
    assert connection.execute.call_count == 2


@pytest.mark.parametrize("table", ["ticks", "candles", "trades"])
def test_manual_cutover_plan_preserves_legacy_and_builds_fourteen_days(
    table: lifecycle.MarketDataTable,
) -> None:
    """Every table's adoption plan must follow the convergent catalog order.

    Given: An ordinary 0042 table and one explicit anchor.
    When: The independent runtime cutover planner renders its atomic DDL.
    Then: It renames rather than drops legacy, creates fourteen ascending daily
        leaves plus DEFAULT, attaches legacy last, and transfers the sequence.
    """
    spec = lifecycle._spec(table)

    statements = lifecycle._cutover_statements(
        spec,
        _ANCHOR,
        ((f"ck_{table}_sequence_id", "CHECK (sequence_id > 0)"),),
    )

    assert statements[0] == f"ALTER TABLE {table} RENAME TO {table}_legacy"
    assert all("DROP " not in statement for statement in statements)
    if table == "candles":
        assert (
            "ALTER TABLE candles ADD CONSTRAINT ck_candle_source "
            "CHECK (source IN ('native', 'calculated', 'synthesized'))"
        ) in statements
    daily_creates = [
        statement
        for statement in statements
        if statement.startswith(f"CREATE TABLE {table}_d20") and " PARTITION OF " in statement
    ]
    assert len(daily_creates) == lifecycle.FUTURE_LEAF_TARGET
    assert f"{table}_d20260801" in daily_creates[0]
    assert f"{table}_d20260814" in daily_creates[-1]
    assert f"CREATE TABLE {table}_default PARTITION OF {table} DEFAULT" in statements
    assert statements[-2].startswith(f"ALTER TABLE {table} ATTACH PARTITION {table}_legacy")
    assert statements[-1] == (f"ALTER SEQUENCE {table}_id_seq OWNED BY {table}.id")


def test_manual_cutover_refuses_to_drop_every_ordinary_check() -> None:
    """Adoption must not silently discard an unrecognized CHECK manifest.

    Given: A validated ordinary table whose CHECK catalog resolved to no rows.
    When: The independent runtime cutover planner is asked to render DDL.
    Then: It refuses before emitting a parent or renaming the source table.
    """
    with pytest.raises(
        lifecycle.DailyPartitionError,
        match="no ordinary CHECK constraints to preserve",
    ):
        lifecycle._cutover_statements(lifecycle._spec("ticks"), _ANCHOR, ())


def test_parent_unique_indexes_are_standalone_and_keep_the_exact_keys() -> None:
    """Unique parent arbiters must use index form and table-specific semantics.

    Given: The settled candles and trades parent manifests.
    When: Runtime renders their independent parent-index DDL.
    Then: Candle active uniqueness still includes ``open_at`` and trades uses
        U3, while neither statement creates a UNIQUE constraint.
    """
    candles = lifecycle._spec("candles")
    trades = lifecycle._spec("trades")

    candle_sql = lifecycle._parent_index_sql(candles, candles.parent_indexes[0])
    trade_sql = lifecycle._parent_index_sql(trades, trades.parent_indexes[0])

    assert candle_sql.startswith("CREATE UNIQUE INDEX candles_p_uq_itf_open ON candles")
    assert '"instrument_public_id", "timeframe", "open_at"' in candle_sql
    assert "WHERE known_to = TIMESTAMPTZ" in candle_sql
    assert trade_sql == (
        "CREATE UNIQUE INDEX trades_p_uq_instr_tid_exec ON trades "
        '("instrument_public_id", "trade_id", "executed_at")'
    )
    assert "ADD CONSTRAINT" not in candle_sql
    assert "ADD CONSTRAINT" not in trade_sql


def test_relation_constraint_manifest_tracks_postgresql_18_not_null_names() -> None:
    """Constraint roles must distinguish ordinary, prepared, and leaf shapes.

    Given: Trades has a nullable 0042 partition key that preparation hardens.
    When: Runtime builds exact non-CHECK manifests for each relation role.
    Then: Prepared and leaf manifests add canonical NOT NULL names, while a
        daily leaf has its own PK name and never inherits legacy U2.
    """
    spec = lifecycle._spec("trades")

    ordinary = lifecycle._expected_noncheck_constraints(
        spec,
        "trades",
        lifecycle.ConstraintRole.ORDINARY,
    )
    prepared = lifecycle._expected_noncheck_constraints(
        spec,
        "trades",
        lifecycle.ConstraintRole.PREPARED,
    )
    leaf = lifecycle._expected_noncheck_constraints(
        spec,
        "trades_d20260801",
        lifecycle.ConstraintRole.LEAF,
    )
    ordinary_names = {item[0] for item in ordinary}
    prepared_names = {item[0] for item in prepared}
    leaf_names = {item[0] for item in leaf}

    assert "trades_executed_at_not_null" not in ordinary_names
    assert "trades_executed_at_not_null" in prepared_names
    assert "trades_executed_at_not_null" in leaf_names
    assert "trades_d20260801_pkey" in leaf_names
    assert "uq_trade_instrument_trade_id" not in leaf_names


def test_sequence_preflight_refuses_parameter_drift() -> None:
    """Sequence ownership alone cannot hide nondefault sequence parameters.

    Given: The exact sequence catalog query reports a mismatch.
    When: Runtime verifies the ordinary ticks id sequence before adoption.
    Then: It refuses with the parameter-or-ownership contract instead of
        accepting a name-only ownership match.
    """
    connection = _connection_double()
    connection.scalar.return_value = False

    with pytest.raises(
        lifecycle.DailyPartitionError,
        match="ticks_id_seq parameters or ownership are not exact",
    ):
        lifecycle._verify_sequence_owner(connection, lifecycle._spec("ticks"))

    statement = str(connection.scalar.call_args.args[0])
    assert "sequence_parameters.seqcache = 1" in statement
    assert "dependency.deptype = 'a'" in statement


def test_adoption_refuses_target_name_collisions_before_preparation() -> None:
    """Cutover names must be free before a populated-index build can start.

    Given: A public relation already occupies the planned ticks legacy name.
    When: Runtime evaluates adoption target names at the ordinary preflight.
    Then: It refuses immediately and reports the exact colliding relation.
    """
    connection = _connection_double()
    connection.execute.return_value.scalars.return_value = iter(("ticks_legacy",))

    with pytest.raises(lifecycle.DailyPartitionError) as error:
        lifecycle._verify_adoption_names_available(
            connection,
            lifecycle._spec("ticks"),
            _ANCHOR,
        )

    assert str(error.value) == (
        "refused: target partition relation names already exist: ('ticks_legacy',)"
    )
    parameters = connection.execute.call_args.args[1]
    assert "ticks_legacy" in parameters["names"]
    assert "ticks_d20260801" in parameters["names"]
    assert "ticks_d20260814_instrument_public_id_timestamp_idx" in parameters["names"]
    assert not any(name.endswith("_public_id") for name in parameters["names"])


@pytest.mark.parametrize(
    ("table", "local_public_id"),
    [
        ("ticks", False),
        ("candles", True),
        ("trades", True),
    ],
)
def test_adoption_accepts_available_names_for_each_public_id_contract(
    table: lifecycle.MarketDataTable,
    local_public_id: bool,
) -> None:
    """Free cutover names must allow adoption preparation to continue.

    Given: No public relation occupies any planned cutover name for one table.
    When: Runtime checks names for a table with or without leaf-local indexes.
    Then: It returns normally after one exact candidate-name catalog query.
    """
    connection = _connection_double()
    connection.execute.return_value.scalars.return_value = iter(())
    spec = lifecycle._spec(table)
    leaves = tuple(
        f"{table}_d{(_ANCHOR + timedelta(days=offset)):%Y%m%d}"
        for offset in range(lifecycle.FUTURE_LEAF_TARGET)
    ) + (f"{table}_default",)
    expected_names = {
        f"{table}_legacy",
        *(index.name for index in spec.parent_indexes),
        *leaves,
        *(f"{leaf}_pkey" for leaf in leaves),
        *(
            f"{leaf}_{'_'.join(index.columns)}_idx"
            for leaf in leaves
            for index in spec.parent_indexes
        ),
    }
    if local_public_id:
        expected_names.update(f"{leaf}_public_id" for leaf in leaves)

    lifecycle._verify_adoption_names_available(
        connection,
        spec,
        _ANCHOR,
    )

    names = cast(tuple[str, ...], connection.execute.call_args.args[1]["names"])
    assert names == tuple(sorted(expected_names))
    connection.execute.return_value.scalars.assert_called_once_with()


def test_generated_leaf_does_not_hide_a_reserved_legacy_range_check() -> None:
    """The legacy range name is special only on source and legacy relations.

    Given: A generated ticks leaf carries an extra validated CHECK using the
        reserved legacy-range constraint name.
    When: CHECK collection runs for source mode and exact leaf mode.
    Then: Source mode suppresses the preparation artifact, while leaf mode
        returns it so the exact manifest verifier refuses the extra CHECK.
    """
    connection = _connection_double()
    definition = (
        "CHECK (timestamp IS NOT NULL AND "
        "timestamp < '2026-08-01 00:00:00+00'::timestamp with time zone)"
    )
    connection.execute.return_value = [
        ("ck_ticks_legacy_range", definition, True),
    ]
    spec = lifecycle._spec("ticks")

    source_checks = lifecycle._ordinary_check_constraints(
        connection,
        spec,
        "ticks",
    )
    leaf_checks = lifecycle._ordinary_check_constraints(
        connection,
        spec,
        "ticks_d20260801",
        exclude_legacy_range=False,
    )

    assert source_checks == ()
    assert leaf_checks == (("ck_ticks_legacy_range", definition),)


def test_trade_u3_preflight_refuses_an_index_that_is_not_ready() -> None:
    """A named U3 cannot pass merely because its visible definition matches.

    Given: A standalone valid-looking U3 whose ``indisready`` bit is false.
    When: The manual adoption path validates its prebuilt legacy arbiter.
    Then: It refuses before ATTACH can build a replacement on the huge table.
    """
    connection = _connection_double()
    unsafe = replace(
        _index_shape_double(
            ("instrument_public_id", "trade_id", "executed_at"),
            unique=True,
            constraint_backed=False,
        ),
        ready=False,
    )
    with (
        patch.object(lifecycle, "_index_shape", return_value=unsafe),
        pytest.raises(
            lifecycle.DailyPartitionError,
            match="standalone U3 contract",
        ),
    ):
        lifecycle._trade_u3_statement(
            connection,
            lifecycle._spec("trades"),
        )


@pytest.mark.parametrize(
    "unsafe",
    [
        replace(
            _index_shape_double(
                ("instrument_public_id", "trade_id", "executed_at"),
                unique=True,
                constraint_backed=False,
            ),
            default_reloptions=False,
        ),
        replace(
            _index_shape_double(
                ("instrument_public_id", "trade_id", "executed_at"),
                unique=True,
                constraint_backed=False,
            ),
            default_tablespace=False,
        ),
    ],
)
def test_trade_u3_preflight_refuses_nondefault_index_storage(
    unsafe: lifecycle.IndexShape,
) -> None:
    """Physical index storage drift cannot pass a definition-only comparison.

    Given: A structurally matching U3 with reloptions or tablespace drift.
    When: The ordinary adoption preflight evaluates its storage contract.
    Then: It refuses before the index could be considered adoptable.
    """
    connection = _connection_double()

    with (
        patch.object(lifecycle, "_index_shape", return_value=unsafe),
        pytest.raises(
            lifecycle.DailyPartitionError,
            match="standalone U3 contract",
        ),
    ):
        lifecycle._trade_u3_statement(
            connection,
            lifecycle._spec("trades"),
        )


def test_cutover_verification_failure_rolls_back_before_commit() -> None:
    """Final topology verification must remain inside the DDL transaction.

    Given: A locked prepared source whose DDL runs but final verification fails.
    When: The atomic cutover helper reaches its verification boundary.
    Then: The transaction exits with that exception instead of committing DDL.
    """
    connection = _connection_double()
    transaction = MagicMock()
    transaction.__exit__.return_value = False
    connection.begin.return_value = transaction
    failure = lifecycle.DailyPartitionError("malformed final topology")
    statement = "ALTER TABLE ticks RENAME TO ticks_legacy"

    with (
        patch.object(lifecycle, "_verify_prepared_ordinary_schema") as preflight,
        patch.object(lifecycle, "_verify_partitioned", side_effect=failure) as verify,
        pytest.raises(lifecycle.DailyPartitionError, match="malformed final topology"),
    ):
        lifecycle._execute_cutover(
            connection,
            lifecycle._spec("ticks"),
            _ANCHOR,
            (statement,),
        )

    executed = [str(call.args[0]) for call in connection.execute.call_args_list]
    assert "LOCK TABLE ticks IN ACCESS EXCLUSIVE MODE" in executed
    assert statement in executed
    preflight.assert_called_once_with(
        connection,
        lifecycle._spec("ticks"),
        _ANCHOR,
    )
    verify.assert_called_once_with(
        connection,
        lifecycle._spec("ticks"),
        _ANCHOR,
    )
    assert transaction.__exit__.call_args.args[0] is lifecycle.DailyPartitionError
    assert transaction.__exit__.call_args.args[1] is failure


def test_cutover_revalidates_the_locked_source_before_first_ddl() -> None:
    """A source index mutation cannot slip between preparation and rename.

    Given: ACCESS EXCLUSIVE is acquired but locked catalog revalidation fails.
    When: The cutover helper is asked to rename the prepared ordinary table.
    Then: It exits the transaction without executing any cutover statement.
    """
    connection = _connection_double()
    transaction = MagicMock()
    transaction.__exit__.return_value = False
    connection.begin.return_value = transaction
    failure = lifecycle.DailyPartitionError("ordinary index drifted")
    statement = "ALTER TABLE ticks RENAME TO ticks_legacy"

    with (
        patch.object(
            lifecycle,
            "_verify_prepared_ordinary_schema",
            side_effect=failure,
        ) as preflight,
        patch.object(lifecycle, "_verify_partitioned") as verify,
        pytest.raises(lifecycle.DailyPartitionError, match="ordinary index drifted"),
    ):
        lifecycle._execute_cutover(
            connection,
            lifecycle._spec("ticks"),
            _ANCHOR,
            (statement,),
        )

    executed = [str(call.args[0]) for call in connection.execute.call_args_list]
    assert "LOCK TABLE ticks IN ACCESS EXCLUSIVE MODE" in executed
    assert statement not in executed
    preflight.assert_called_once_with(
        connection,
        lifecycle._spec("ticks"),
        _ANCHOR,
    )
    verify.assert_not_called()
    assert transaction.__exit__.call_args.args[0] is lifecycle.DailyPartitionError
    assert transaction.__exit__.call_args.args[1] is failure


def test_partitioned_noop_refuses_a_malformed_legacy_primary_key() -> None:
    """A legacy PK cannot disappear behind an otherwise valid parent topology.

    Given: An attached ticks legacy PK whose key column has drifted from ``id``.
    When: The partitioned no-op verifier checks legacy-local objects.
    Then: It refuses instead of reporting an already-adopted no-op.
    """
    connection = _connection_double()
    malformed = _index_shape_double(
        ("public_id",),
        unique=True,
        constraint_backed=True,
    )

    with (
        patch.object(lifecycle, "_index_shape", return_value=malformed),
        pytest.raises(
            lifecycle.DailyPartitionError,
            match="legacy local index ticks_pkey",
        ),
    ):
        lifecycle._verify_legacy_local_objects(
            connection,
            lifecycle._spec("ticks"),
            "ticks_legacy",
        )


def test_partitioned_noop_refuses_a_nested_or_foreign_direct_child() -> None:
    """Every direct child must remain a public ordinary partition leaf.

    Given: A valid public ticks leaf precedes a foreign child that is partitioned.
    When: Runtime validates direct-child relation shapes.
    Then: It continues past the valid leaf and refuses the malformed second one.
    """
    valid = lifecycle.PartitionRef(
        name="ticks_legacy",
        bound="FOR VALUES FROM (MINVALUE) TO ('2026-08-01 00:00:00+00')",
    )
    nested = lifecycle.PartitionRef(
        name="ticks_d20260801",
        bound="FOR VALUES FROM ('2026-08-01 00:00:00+00') TO ('2026-08-02 00:00:00+00')",
        schema="partition_probe",
        relation_kind="p",
        has_partition_key=True,
    )
    report = _partitioned_report("ticks", (valid, nested))

    with pytest.raises(lifecycle.DailyPartitionError) as error:
        lifecycle._verify_direct_partition_shapes(lifecycle._spec("ticks"), report)

    assert str(error.value) == (
        "refused: ticks direct child partition_probe.ticks_d20260801 "
        "has an unexpected relation shape"
    )


def test_partitioned_shape_verifier_accepts_multiple_exact_leaf_shapes() -> None:
    """Every valid direct child must pass the complete leaf-shape contract.

    Given: Two public ordinary partitions satisfying all five shape conditions.
    When: Runtime validates every direct-child relation shape.
    Then: It returns normally after visiting both children in order.
    """
    legacy = lifecycle.PartitionRef(
        name="ticks_legacy",
        bound="FOR VALUES FROM (MINVALUE) TO ('2026-08-01 00:00:00+00')",
        schema="public",
        relation_kind="r",
        is_partition=True,
        has_partition_key=False,
        inheritance_sequence=1,
    )
    default = lifecycle.PartitionRef(
        name="ticks_default",
        bound="DEFAULT",
        schema="public",
        relation_kind="r",
        is_partition=True,
        has_partition_key=False,
        inheritance_sequence=1,
    )
    partitions = _ObservedPartitions((legacy, default))
    report = _partitioned_report("ticks", partitions)

    lifecycle._verify_direct_partition_shapes(lifecycle._spec("ticks"), report)

    assert partitions.visited == ["ticks_legacy", "ticks_default"]


def test_partitioned_noop_refuses_a_malformed_legacy_active_partial() -> None:
    """A malformed active-public partial cannot pass the adopted no-op path.

    Given: A valid candles legacy PK and a public-id index with a wrong predicate.
    When: The partitioned no-op verifier checks both legacy-local objects.
    Then: It refuses the partial instead of weakening active-row uniqueness.
    """
    connection = _connection_double()
    primary = _index_shape_double(
        ("id",),
        unique=True,
        constraint_backed=True,
    )
    malformed = _index_shape_double(
        ("public_id",),
        unique=True,
        constraint_backed=False,
        predicate="known_to IS NULL",
    )

    with (
        patch.object(lifecycle, "_index_shape", side_effect=(primary, malformed)),
        pytest.raises(
            lifecycle.DailyPartitionError,
            match="legacy local index ix_candles_public_id",
        ),
    ):
        lifecycle._verify_legacy_local_objects(
            connection,
            lifecycle._spec("candles"),
            "candles_legacy",
        )


def test_partitioned_noop_refuses_a_malformed_legacy_trade_u2() -> None:
    """The original trades U2 must remain exact and leaf-local after adoption.

    Given: Valid legacy PK and partial indexes but a U2 widened to ``executed_at``.
    When: The partitioned no-op verifier checks all trades local arbiters.
    Then: It refuses the widened U2 rather than mistaking it for the original.
    """
    connection = _connection_double()
    primary = _index_shape_double(
        ("id",),
        unique=True,
        constraint_backed=True,
    )
    public_id = _index_shape_double(
        ("public_id",),
        unique=True,
        constraint_backed=False,
        predicate="known_to = '9999-12-31 23:59:59+00'::timestamp with time zone",
    )
    malformed = _index_shape_double(
        ("instrument_public_id", "trade_id", "executed_at"),
        unique=True,
        constraint_backed=True,
    )

    with (
        patch.object(
            lifecycle,
            "_index_shape",
            side_effect=(primary, public_id, malformed),
        ),
        pytest.raises(
            lifecycle.DailyPartitionError,
            match="legacy local index uq_trade_instrument_trade_id",
        ),
    ):
        lifecycle._verify_legacy_local_objects(
            connection,
            lifecycle._spec("trades"),
            "trades_legacy",
        )


def test_leaf_local_uniqueness_uses_the_canonical_relation_based_names() -> None:
    """Leaf-only PK and active-public-id names must match the catalog contract.

    Given: The first candles day and the candles DEFAULT child.
    When: Runtime renders their leaf-local objects.
    Then: Daily and DEFAULT names follow the settled rehearsal convention and
        the partial predicate remains active-row-only.
    """
    spec = lifecycle._spec("candles")

    daily = lifecycle._daily_leaf_statements(spec, _ANCHOR)
    default = lifecycle._default_leaf_statements(spec)

    assert (
        daily[1]
        == "ALTER TABLE candles_d20260801 ADD CONSTRAINT candles_d20260801_pkey PRIMARY KEY (id)"
    )
    assert daily[2].startswith(
        "CREATE UNIQUE INDEX candles_d20260801_public_id ON candles_d20260801 (public_id)"
    )
    assert len(default) == 3
    assert default[1].endswith("candles_default_pkey PRIMARY KEY (id)")
    assert default[2].startswith("CREATE UNIQUE INDEX candles_default_public_id")


def test_public_adopt_defaults_to_a_read_only_plan() -> None:
    """The public manual path must require an explicit false dry-run argument.

    Given: An ordinary table with a valid sequence and catalog prerequisites.
    When: ``adopt`` is called without overriding ``dry_run``.
    Then: It returns the complete plan and never enters preparation or cutover.
    """
    connection = _connection_double()
    ordinary = lifecycle.PartitionInspection(
        table="ticks",
        state=lifecycle.RelationState.ORDINARY,
        partition_key=None,
        partitions=(),
        default_attached=False,
        default_has_rows=False,
        future_leaf_count=0,
        future_leaf_alarm=True,
    )
    with (
        patch.object(lifecycle, "inspect", return_value=ordinary),
        patch.object(lifecycle, "_verify_ordinary_schema"),
        patch.object(lifecycle, "_verify_sequence_owner"),
        patch.object(
            lifecycle,
            "_ordinary_check_constraints",
            return_value=(("ck_ticks_sequence_id", "CHECK (sequence_id > 0)"),),
        ),
        patch.object(lifecycle, "_trade_u3_statement", return_value=""),
        patch.object(
            lifecycle,
            "_range_preparation_statements",
            return_value=("ALTER TABLE ticks PREPARE",),
        ),
        patch.object(lifecycle, "_prepare_legacy_range") as prepare,
        patch.object(lifecycle, "_execute_transaction") as execute,
    ):
        result = lifecycle.adopt(connection, "ticks", _ANCHOR)

    assert result.action is lifecycle.LifecycleAction.PLANNED
    assert result.statements[0] == "ALTER TABLE ticks PREPARE"
    connection.execute.assert_called_once()
    assert str(connection.execute.call_args.args[0]) == "SET LOCAL TIME ZONE 'UTC'"
    prepare.assert_not_called()
    execute.assert_not_called()


def test_ensure_future_refuses_a_nonempty_default_before_building_sql() -> None:
    """A DEFAULT anomaly must block leaf creation before PostgreSQL scans it.

    Given: An adopted trades parent whose DEFAULT partition contains one row.
    When: Future-leaf maintenance runs, even in dry-run mode.
    Then: It refuses with a drain-first message and emits no DDL.
    """
    connection = _connection_double()
    report = _partitioned_report("trades", (), default_has_rows=True)
    with (
        patch.object(lifecycle, "inspect", return_value=report),
        patch.object(lifecycle, "_require_partitioned_topology"),
        pytest.raises(lifecycle.DailyPartitionError, match="must be drained first"),
    ):
        lifecycle.ensure_future_leaves(connection, "trades", _ANCHOR)


def test_ensure_future_refuses_a_name_with_the_wrong_partition_bound() -> None:
    """An attached desired name cannot hide a gap by carrying another bound.

    Given: ``trades_d20260801`` attached to the following day's range.
    When: Future-leaf maintenance evaluates its desired window.
    Then: It fails closed instead of counting the misleading relation name.
    """
    connection = _connection_double()
    wrong = lifecycle.PartitionRef(
        name="trades_d20260801",
        bound="FOR VALUES FROM ('2026-08-02 00:00:00+00') TO ('2026-08-03 00:00:00+00')",
    )
    report = _partitioned_report("trades", (wrong,))
    with (
        patch.object(lifecycle, "inspect", return_value=report),
        patch.object(lifecycle, "_require_partitioned_topology"),
        pytest.raises(lifecycle.DailyPartitionError, match="unexpected partition bound"),
    ):
        lifecycle.ensure_future_leaves(connection, "trades", _ANCHOR)


def test_ensure_rechecks_default_attachment_under_the_parent_lock() -> None:
    """A detached DEFAULT cannot slip between planning and locked execution.

    Given: The parent lock is acquired after its DEFAULT edge disappeared.
    When: The atomic future-leaf executor rechecks the direct child catalog.
    Then: It refuses before probing rows or executing any leaf DDL.
    """
    connection = _connection_double()
    statement = "CREATE TABLE trades_d20260801 PARTITION OF trades"

    with (
        patch.object(lifecycle, "_direct_partitions", return_value=()),
        patch.object(lifecycle, "_relation_has_rows") as has_rows,
        pytest.raises(
            lifecycle.DailyPartitionError,
            match="no longer attached as DEFAULT",
        ),
    ):
        lifecycle._execute_ensure(
            connection,
            lifecycle._spec("trades"),
            (statement,),
        )

    executed = [str(call.args[0]) for call in connection.execute.call_args_list]
    assert "LOCK TABLE trades IN ACCESS EXCLUSIVE MODE" in executed
    assert statement not in executed
    has_rows.assert_not_called()


def test_detach_is_plain_and_retention_bounded_with_default_present() -> None:
    """Expired retention uses plain DETACH and leaves DROP out of this module.

    Given: An expired trades day attached with its exact daily bound.
    When: The public detach path is requested in safe dry-run mode.
    Then: It emits one non-concurrent DETACH, with no DROP or row deletion.
    """
    connection = _connection_double()
    day = date(2026, 6, 1)
    lower = datetime(day.year, day.month, day.day, tzinfo=UTC)
    leaf = lifecycle.PartitionRef(
        name="trades_d20260601",
        bound="FOR VALUES FROM ('2026-06-01 00:00:00+00') TO ('2026-06-02 00:00:00+00')",
    )
    report = _partitioned_report("trades", (leaf,))
    with (
        patch.object(lifecycle, "inspect", return_value=report),
        patch.object(lifecycle, "_require_partitioned_topology"),
    ):
        result = lifecycle.detach(
            connection,
            "trades",
            day,
            _ANCHOR,
        )

    assert result.statements == ("ALTER TABLE trades DETACH PARTITION trades_d20260601",)
    assert "CONCURRENTLY" not in result.statements[0]
    assert "DROP" not in result.statements[0]
    assert lower + timedelta(days=1) < _ANCHOR


def test_detach_retries_a_psycopg2_pgcode_lock_timeout() -> None:
    """The CLI's psycopg2 SQLSTATE spelling must retain bounded DETACH retries.

    Given: SQLAlchemy wraps a lock timeout exposed only through ``pgcode``.
    When: Plain DETACH fails once and succeeds on its second bounded attempt.
    Then: The lock error is classified as retryable and one backoff occurs.
    """
    connection = _connection_double()
    original = _Psycopg2LockError("lock timeout")
    failure = DBAPIError(
        "ALTER TABLE trades DETACH PARTITION trades_d20260601",
        None,
        original,
        False,
    )
    lower = datetime(2026, 6, 1, tzinfo=UTC)
    spec = lifecycle._spec("trades")
    leaf = "trades_d20260601"

    with (
        patch.object(
            lifecycle,
            "_execute_detach_attempt",
            side_effect=(failure, None),
        ) as execute,
        patch.object(lifecycle.time, "sleep") as sleep,
    ):
        lifecycle._detach_with_retries(connection, spec, leaf, lower)

    assert execute.call_count == 2
    execute.assert_called_with(connection, spec, leaf, lower)
    sleep.assert_called_once_with(0.25)


def test_detach_revalidates_the_exact_bound_under_the_parent_lock() -> None:
    """A replaced leaf name cannot redirect a previously approved DETACH.

    Given: The parent lock reveals the selected daily name now carries another
        range after the public preflight completed.
    When: One bounded DETACH attempt rechecks its target under that lock.
    Then: It refuses before emitting DETACH against the replacement relation.
    """
    connection = _connection_double()
    spec = lifecycle._spec("trades")
    leaf = "trades_d20260601"
    lower = datetime(2026, 6, 1, tzinfo=UTC)
    replacement = lifecycle.PartitionRef(
        name=leaf,
        bound="FOR VALUES FROM ('2026-05-31 00:00:00+00') TO ('2026-06-01 00:00:00+00')",
    )

    with (
        patch.object(lifecycle, "_direct_partitions", return_value=(replacement,)),
        pytest.raises(
            lifecycle.DailyPartitionError,
            match="no longer has its exact daily bound",
        ),
    ):
        lifecycle._execute_detach_attempt(connection, spec, leaf, lower)

    executed = [str(call.args[0]) for call in connection.execute.call_args_list]
    assert "LOCK TABLE trades IN ACCESS EXCLUSIVE MODE" in executed
    assert f"ALTER TABLE trades DETACH PARTITION {leaf}" not in executed


def test_detach_refuses_when_the_settled_default_partition_is_missing() -> None:
    """Plain DETACH is valid only while the DEFAULT anomaly buffer is retained.

    Given: An otherwise partitioned trades parent without its required DEFAULT.
    When: An expired daily leaf is selected for DETACH.
    Then: The lifecycle refuses before issuing partition DDL.
    """
    connection = _connection_double()
    day = date(2026, 6, 1)
    leaf = lifecycle.PartitionRef(
        name="trades_d20260601",
        bound="FOR VALUES FROM ('2026-06-01 00:00:00+00') TO ('2026-06-02 00:00:00+00')",
    )
    report = _partitioned_report(
        "trades",
        (leaf,),
        default_attached=False,
    )
    with (
        patch.object(lifecycle, "inspect", return_value=report),
        patch.object(lifecycle, "_require_partitioned_topology"),
        pytest.raises(lifecycle.DailyPartitionError, match="DEFAULT partition"),
    ):
        lifecycle.detach(connection, "trades", day, _ANCHOR)


@pytest.mark.parametrize(
    ("table", "retention_days"),
    [("ticks", 7), ("candles", 30), ("trades", 30)],
)
def test_detach_refuses_a_day_inside_each_tables_retention_horizon(
    table: lifecycle.MarketDataTable,
    retention_days: int,
) -> None:
    """Daily DETACH cannot be aimed at data still inside its table horizon.

    Given: A day one short of each table's seven- or thirty-day horizon.
    When: The public detach path applies the table-specific policy.
    Then: Every table refuses before inspecting or locking its parent.
    """
    connection = _connection_double()
    day = _ANCHOR.date() - timedelta(days=retention_days - 1)

    with pytest.raises(
        lifecycle.DailyPartitionError,
        match=rf"{retention_days}-day",
    ):
        lifecycle.detach(
            connection,
            table,
            day,
            _ANCHOR,
        )
