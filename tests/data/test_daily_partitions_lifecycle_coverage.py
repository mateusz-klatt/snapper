"""Mutation-sensitive unit coverage for partition lifecycle execution and verification."""

import re
from dataclasses import replace
from datetime import UTC
from datetime import datetime
from functools import partial
from types import SimpleNamespace
from typing import cast
from unittest.mock import MagicMock
from unittest.mock import call
from unittest.mock import patch

import pytest
from sqlalchemy.engine import Connection
from sqlalchemy.exc import DBAPIError

import snapper.data.daily_partitions as lifecycle

_ANCHOR = datetime(2026, 8, 1, tzinfo=UTC)
_NEXT_DAY = datetime(2026, 8, 2, tzinfo=UTC)
_ACTIVE_PREDICATE = "known_to = '9999-12-31 23:59:59+00'::timestamp with time zone"


class _LockError(RuntimeError):
    """Expose a retryable SQLSTATE through the modern DBAPI spelling."""

    sqlstate: str = "55P03"


class _PersistentError(RuntimeError):
    """Expose a valid but non-retryable SQLSTATE."""

    sqlstate: str = "40001"


class _MalformedStateError(RuntimeError):
    """Expose values that must not be accepted as PostgreSQL SQLSTATEs."""

    sqlstate: str = "55p03"
    pgcode: int = 55003


def _connection_double() -> Connection:
    """Return a PostgreSQL-shaped SQLAlchemy connection double."""
    connection = MagicMock(spec=Connection)
    connection.in_transaction.return_value = False
    connection.dialect = SimpleNamespace(name="postgresql")
    connection.scalar.return_value = "public"
    return cast(Connection, connection)


def _shape(
    columns: tuple[str, ...],
    *,
    unique: bool,
    constraint_backed: bool,
    predicate: str | None = None,
    parent_indexes: tuple[str, ...] = (),
) -> lifecycle.IndexShape:
    """Return an otherwise canonical btree index shape."""
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
        parent_indexes=parent_indexes,
    )


def _partitioned(
    partitions: tuple[lifecycle.PartitionRef, ...],
) -> lifecycle.PartitionInspection:
    """Return a trades partition topology for verification tests."""
    return lifecycle.PartitionInspection(
        table="trades",
        state=lifecycle.RelationState.PARTITIONED,
        partition_key="RANGE (executed_at)",
        partitions=partitions,
        default_attached=True,
        default_has_rows=False,
        future_leaf_count=1,
        future_leaf_alarm=True,
    )


def _legacy_ref() -> lifecycle.PartitionRef:
    """Return the exact initial legacy range."""
    return lifecycle.PartitionRef(
        name="trades_legacy",
        bound="FOR VALUES FROM (MINVALUE) TO ('2026-08-01 00:00:00+00')",
    )


def _default_ref() -> lifecycle.PartitionRef:
    """Return the settled DEFAULT anomaly buffer."""
    return lifecycle.PartitionRef(name="trades_default", bound="DEFAULT")


def _daily_ref() -> lifecycle.PartitionRef:
    """Return the first exact daily leaf."""
    return lifecycle.PartitionRef(
        name="trades_d20260801",
        bound=("FOR VALUES FROM ('2026-08-01 00:00:00+00') TO ('2026-08-02 00:00:00+00')"),
    )


def _range_definition() -> str:
    """Return PostgreSQL's canonical validated legacy CHECK rendering."""
    return (
        "CHECK ((executed_at IS NOT NULL) AND "
        "(executed_at < '2026-08-01 00:00:00+00'::timestamp with time zone))"
    )


def _dbapi_error(original: BaseException) -> DBAPIError:
    """Wrap a driver failure in SQLAlchemy's public DBAPI exception."""
    return DBAPIError("ALTER TABLE trades", None, original, False)


def test_range_constraint_rendering_preserves_the_exact_utc_implication() -> None:
    """The preparation DDL must bind the named CHECK to the UTC anchor."""
    spec = lifecycle._spec("trades")

    assert lifecycle._range_constraint_name(spec) == "ck_trades_legacy_range"
    assert lifecycle._add_range_constraint_sql(spec, _ANCHOR) == (
        "ALTER TABLE trades ADD CONSTRAINT ck_trades_legacy_range "
        'CHECK ("executed_at" IS NOT NULL AND '
        "\"executed_at\" < TIMESTAMPTZ '2026-08-01T00:00:00+00:00') NOT VALID"
    )
    assert lifecycle._quote_identifier('event"time') == '"event""time"'


@pytest.mark.parametrize(
    ("definition", "expected"),
    [
        (_range_definition(), True),
        ("CHECK (executed_at IS NOT NULL)", False),
        (
            (
                "CHECK (executed_at < '2026-08-01 00:00:00+00' "
                "AND executed_at > '2026-07-01 00:00:00+00')"
            ),
            False,
        ),
        ("CHECK (executed_at < 'not-a-date')", False),
        ("CHECK (executed_at < '2026-08-01 00:00:00')", False),
        ("CHECK (executed_at < '2026-08-02 00:00:00+00')", False),
        (
            (
                "CHECK (executed_at IS NULL AND "
                "executed_at < '2026-08-01 00:00:00+00'::timestamp with time zone)"
            ),
            False,
        ),
    ],
)
def test_range_constraint_matcher_rejects_every_weakened_shape(
    definition: str,
    expected: bool,
) -> None:
    """Only the exact non-null exclusive legacy bound is accepted."""
    assert (
        lifecycle._range_constraint_matches(
            definition,
            lifecycle._spec("trades"),
            _ANCHOR,
        )
        is expected
    )


def test_constraint_info_distinguishes_absence_from_catalog_shape() -> None:
    """Constraint lookup must preserve both definition and validation state."""
    connection = MagicMock(spec=Connection)
    result = MagicMock()
    connection.execute.return_value = result
    typed = cast(Connection, connection)

    result.one_or_none.return_value = None
    assert lifecycle._constraint_info(typed, "trades", "ck_trades_legacy_range") is None

    result.one_or_none.return_value = (_range_definition(), True)
    assert lifecycle._constraint_info(
        typed,
        "trades",
        "ck_trades_legacy_range",
    ) == lifecycle.ConstraintInfo(definition=_range_definition(), validated=True)


@pytest.mark.parametrize(
    ("catalog_value", "expected"),
    [(True, False), (False, True)],
)
def test_column_nullability_inverts_the_catalog_not_null_bit(
    catalog_value: bool,
    expected: bool,
) -> None:
    """The catalog helper must report nullable rather than ``attnotnull``."""
    connection = _connection_double()
    connection.scalar.return_value = catalog_value

    assert lifecycle._column_is_nullable(connection, "trades", "executed_at") is expected


def test_column_nullability_refuses_an_absent_partition_key() -> None:
    """A missing expected column cannot be mistaken for nullable."""
    connection = _connection_double()
    connection.scalar.return_value = None

    with pytest.raises(lifecycle.DailyPartitionError, match="trades.executed_at is absent"):
        lifecycle._column_is_nullable(connection, "trades", "executed_at")


def test_index_shape_maps_every_catalog_safety_bit_and_normalizes_simple_keys() -> None:
    """Index inspection must retain every storage and inheritance property."""
    connection = MagicMock(spec=Connection)
    result = MagicMock()
    connection.execute.return_value = result
    typed = cast(Connection, connection)

    result.one_or_none.return_value = None
    assert lifecycle._index_shape(typed, "trades", "missing") is None

    result.one_or_none.return_value = (
        True,
        True,
        False,
        True,
        _ACTIVE_PREDICATE,
        ['"public_id"', "lower(trade_id)"],
        False,
        "btree",
        True,
        False,
        True,
        False,
        True,
        False,
        True,
        False,
        ["trades_p_uq_instr_tid_exec"],
    )
    shape = lifecycle._index_shape(typed, "trades", "trades_d20260801_public_id")

    assert shape == lifecycle.IndexShape(
        unique=True,
        valid=True,
        ready=False,
        live=True,
        columns=("public_id", "lower(trade_id)"),
        predicate=_ACTIVE_PREDICATE,
        constraint_backed=False,
        access_method="btree",
        no_expressions=True,
        no_include=False,
        nulls_distinct=True,
        default_options=False,
        default_opclasses=True,
        attribute_collations=False,
        default_reloptions=True,
        default_tablespace=False,
        parent_indexes=("trades_p_uq_instr_tid_exec",),
    )
    assert lifecycle._normalize_index_expression("executed_at") == "executed_at"
    assert lifecycle._normalize_index_expression('"executed_at"') == "executed_at"


def test_concurrent_u3_build_uses_an_autocommit_sibling_with_fixed_resources() -> None:
    """The populated U3 build must remain outside the caller transaction."""
    connection = MagicMock()
    sibling = MagicMock(spec=Connection)
    context = connection.engine.connect.return_value.execution_options.return_value
    context.__enter__.return_value = sibling
    statement = "CREATE UNIQUE INDEX CONCURRENTLY uq_trade_instr_tid_exec ON trades"

    lifecycle._create_trade_u3_concurrently(cast(Connection, connection), statement)

    connection.engine.connect.return_value.execution_options.assert_called_once_with(
        isolation_level="AUTOCOMMIT"
    )
    executed = [str(call.args[0]) for call in sibling.execute.call_args_list]
    assert executed == [
        "SET TIME ZONE 'UTC'",
        "SET statement_timeout = '12h'",
        "SET maintenance_work_mem = '64MB'",
        "SET max_parallel_maintenance_workers = 0",
        statement,
    ]


def test_prepare_legacy_range_dispatches_validation_to_its_long_ceiling() -> None:
    """Only CHECK validation may use the twelve-hour statement timeout."""
    connection = _connection_double()
    add = "ALTER TABLE trades ADD CONSTRAINT range NOT VALID"
    validate = "ALTER TABLE trades VALIDATE CONSTRAINT range"
    not_null = "ALTER TABLE trades ALTER COLUMN executed_at SET NOT NULL"

    with (
        patch.object(
            lifecycle,
            "_range_preparation_statements",
            return_value=(add, validate, not_null),
        ),
        patch.object(lifecycle, "_execute_transaction") as execute,
        patch.object(lifecycle, "_execute_validation_transaction") as validate_execute,
    ):
        lifecycle._prepare_legacy_range(
            connection,
            lifecycle._spec("trades"),
            _ANCHOR,
        )

    connection.rollback.assert_called_once_with()
    assert execute.call_args_list[0].args == (connection, (add,))
    assert execute.call_args_list[1].args == (connection, (not_null,))
    validate_execute.assert_called_once_with(connection, validate)


def test_execute_transaction_applies_every_statement_under_bounded_settings() -> None:
    """Ordinary DDL stages must share one UTC bounded transaction."""
    connection = _connection_double()
    transaction = connection.begin.return_value
    statements = ("ALTER TABLE ticks ONE", "ALTER TABLE ticks TWO")

    lifecycle._execute_transaction(connection, statements)

    connection.begin.assert_called_once_with()
    transaction.__enter__.assert_called_once_with()
    transaction.__exit__.assert_called_once_with(None, None, None)
    executed = [str(call.args[0]) for call in connection.execute.call_args_list]
    assert executed == [
        "SET LOCAL TIME ZONE 'UTC'",
        "SET LOCAL lock_timeout = '2s'",
        "SET LOCAL statement_timeout = '30s'",
        *statements,
    ]


def test_execute_validation_uses_the_dedicated_twelve_hour_ceiling() -> None:
    """A populated CHECK validation must not inherit the cutover ceiling."""
    connection = _connection_double()
    transaction = connection.begin.return_value
    statement = "ALTER TABLE trades VALIDATE CONSTRAINT ck_trades_legacy_range"

    lifecycle._execute_validation_transaction(connection, statement)

    connection.begin.assert_called_once_with()
    transaction.__enter__.assert_called_once_with()
    transaction.__exit__.assert_called_once_with(None, None, None)
    executed = [str(call.args[0]) for call in connection.execute.call_args_list]
    assert executed == [
        "SET LOCAL TIME ZONE 'UTC'",
        "SET LOCAL lock_timeout = '2s'",
        "SET LOCAL statement_timeout = '12h'",
        statement,
    ]


@pytest.mark.parametrize("validation", [False, True])
def test_transaction_context_receives_failures_for_automatic_rollback(
    validation: bool,
) -> None:
    """Each transaction helper must leave failure rollback to its entered context."""
    connection = _connection_double()
    transaction = connection.begin.return_value
    failure = RuntimeError("DDL failed")
    connection.execute.side_effect = (None, None, None, failure)
    invoke_failing_helper = (
        partial(
            lifecycle._execute_validation_transaction,
            connection,
            "ALTER TABLE trades VALIDATE CONSTRAINT ck_trades_legacy_range",
        )
        if validation
        else partial(
            lifecycle._execute_transaction,
            connection,
            ("ALTER TABLE trades ADD CONSTRAINT broken",),
        )
    )

    with pytest.raises(RuntimeError, match="DDL failed") as raised:
        invoke_failing_helper()

    connection.begin.assert_called_once_with()
    transaction.__enter__.assert_called_once_with()
    transaction.__exit__.assert_called_once()
    exception_type, exception, traceback = transaction.__exit__.call_args.args
    assert exception_type is RuntimeError
    assert exception is failure
    assert exception is raised.value
    assert traceback is not None
    expected_helper = "_execute_validation_transaction" if validation else "_execute_transaction"
    assert traceback.tb_frame.f_code.co_name == expected_helper


def test_execute_ensure_refuses_rows_that_arrive_after_planning() -> None:
    """A DEFAULT row appearing under the parent lock must block leaf creation."""
    connection = _connection_double()
    statement = "CREATE TABLE trades_d20260801 PARTITION OF trades"
    trades_spec = lifecycle._spec("trades")

    with (
        patch.object(lifecycle, "_direct_partitions", return_value=(_default_ref(),)),
        patch.object(lifecycle, "_relation_has_rows", return_value=True),
        pytest.raises(lifecycle.DailyPartitionError, match="must be drained first"),
    ):
        lifecycle._execute_ensure(
            connection,
            trades_spec,
            (statement,),
        )

    assert statement not in [str(call.args[0]) for call in connection.execute.call_args_list]


def test_execute_ensure_creates_every_leaf_only_after_locked_default_recheck() -> None:
    """A stable empty DEFAULT permits the complete ordered leaf stage."""
    connection = _connection_double()
    statements = (
        "CREATE TABLE trades_d20260801 PARTITION OF trades",
        "ALTER TABLE trades_d20260801 ADD PRIMARY KEY (id)",
    )

    with (
        patch.object(lifecycle, "_direct_partitions", return_value=(_default_ref(),)),
        patch.object(lifecycle, "_relation_has_rows", return_value=False),
    ):
        lifecycle._execute_ensure(
            connection,
            lifecycle._spec("trades"),
            statements,
        )

    executed = [str(call.args[0]) for call in connection.execute.call_args_list]
    assert executed[-2:] == list(statements)


def test_detach_rejects_a_non_lock_database_failure_without_retry() -> None:
    """Only PostgreSQL lock-not-available is retryable."""
    connection = _connection_double()
    failure = _dbapi_error(_PersistentError("serialization"))
    spec = lifecycle._spec("trades")

    with (
        patch.object(lifecycle, "_execute_detach_attempt", side_effect=failure) as execute,
        patch.object(lifecycle.time, "sleep") as sleep,
        pytest.raises(lifecycle.DailyPartitionError, match="after 1 attempt"),
    ):
        lifecycle._detach_with_retries(connection, spec, "trades_d20260801", _ANCHOR)

    execute.assert_called_once_with(connection, spec, "trades_d20260801", _ANCHOR)
    sleep.assert_not_called()


def test_detach_exhausts_exactly_three_lock_attempts_and_two_backoffs() -> None:
    """Bounded DETACH must surface the third lock timeout rather than spin."""
    connection = _connection_double()
    failure = _dbapi_error(_LockError("lock timeout"))
    spec = lifecycle._spec("trades")

    with (
        patch.object(lifecycle, "_execute_detach_attempt", side_effect=failure) as execute,
        patch.object(lifecycle.time, "sleep") as sleep,
        pytest.raises(lifecycle.DailyPartitionError, match="after 3 attempt"),
    ):
        lifecycle._detach_with_retries(connection, spec, "trades_d20260801", _ANCHOR)

    assert execute.call_count == lifecycle.DETACH_RETRIES
    execute.assert_called_with(connection, spec, "trades_d20260801", _ANCHOR)
    assert [call.args[0] for call in sleep.call_args_list] == [0.25, 0.5]


def test_detach_zero_retry_guard_still_fails_closed() -> None:
    """A corrupted retry budget cannot make DETACH report success."""
    connection = _connection_double()
    spec = lifecycle._spec("trades")

    with (
        patch.object(lifecycle, "DETACH_RETRIES", 0),
        pytest.raises(lifecycle.DailyPartitionError, match="exhausted its bounded retry loop"),
    ):
        lifecycle._detach_with_retries(connection, spec, "trades_d20260801", _ANCHOR)


def test_detach_attempt_executes_plain_detach_after_locked_exact_bound() -> None:
    """An unchanged daily attachment must permit plain DETACH under its parent lock.

    Given: The locked catalog still exposes the selected leaf with its exact UTC day bound.
    When: One bounded DETACH attempt revalidates that attachment.
    Then: It executes plain DETACH only after the bounded settings and parent lock.
    """
    connection = _connection_double()
    spec = lifecycle._spec("trades")

    with patch.object(lifecycle, "_direct_partitions", return_value=(_daily_ref(),)):
        lifecycle._execute_detach_attempt(
            connection,
            spec,
            "trades_d20260801",
            _ANCHOR,
        )

    executed = [str(item.args[0]) for item in connection.execute.call_args_list]
    assert executed == [
        "SET LOCAL TIME ZONE 'UTC'",
        "SET LOCAL lock_timeout = '2s'",
        "SET LOCAL statement_timeout = '30s'",
        "LOCK TABLE trades IN ACCESS EXCLUSIVE MODE",
        "ALTER TABLE trades DETACH PARTITION trades_d20260801",
    ]


def test_sqlstate_rejects_malformed_driver_attributes() -> None:
    """Invalid SQLSTATE spellings and types must not enter retry classification."""
    assert lifecycle._sqlstate(_dbapi_error(_MalformedStateError("bad state"))) is None


def test_partition_topology_requirement_refuses_an_ordinary_relation() -> None:
    """Steady-state DDL cannot run against the pre-adoption table."""
    connection = _connection_double()
    report = replace(
        _partitioned(()),
        state=lifecycle.RelationState.ORDINARY,
        partition_key=None,
    )
    trades_spec = lifecycle._spec("trades")

    with (
        patch.object(lifecycle, "inspect", return_value=report),
        pytest.raises(lifecycle.DailyPartitionError, match="not a partitioned parent"),
    ):
        lifecycle._require_partitioned_topology(
            connection,
            trades_spec,
            _ANCHOR,
        )


def test_partition_topology_requirement_refuses_the_wrong_partition_key() -> None:
    """A RANGE parent on another timestamp cannot pass by relation kind alone."""
    connection = _connection_double()
    report = replace(_partitioned(()), partition_key="RANGE (timestamp)")
    trades_spec = lifecycle._spec("trades")

    with (
        patch.object(lifecycle, "inspect", return_value=report),
        pytest.raises(lifecycle.DailyPartitionError, match="partition key"),
    ):
        lifecycle._require_partitioned_topology(
            connection,
            trades_spec,
            _ANCHOR,
        )


def test_partition_topology_requirement_accepts_quoted_spacing_and_checks_indexes() -> None:
    """Canonical RANGE formatting differences must not weaken index verification."""
    connection = _connection_double()
    report = replace(_partitioned(()), partition_key='RANGE ( "executed_at" )')

    with (
        patch.object(lifecycle, "inspect", return_value=report),
        patch.object(lifecycle, "_verify_parent_indexes") as verify,
    ):
        lifecycle._require_partitioned_topology(
            connection,
            lifecycle._spec("trades"),
            _ANCHOR,
        )

    verify.assert_called_once_with(connection, lifecycle._spec("trades"))


@pytest.mark.parametrize(
    ("partitions", "message"),
    [
        (
            (_default_ref(), _daily_ref()),
            (
                "refused: trades direct partition manifest is "
                "('trades_default', 'trades_d20260801')"
            ),
        ),
        (
            (_legacy_ref(), _daily_ref()),
            (
                "refused: trades direct partition manifest is "
                "('trades_legacy', 'trades_d20260801')"
            ),
        ),
        (
            (_legacy_ref(), _default_ref()),
            "refused: trades direct partition manifest is ('trades_legacy', 'trades_default')",
        ),
        (
            (
                replace(
                    _legacy_ref(),
                    bound="FOR VALUES FROM (MINVALUE) TO ('2026-08-02 00:00:00+00')",
                ),
                _default_ref(),
                _daily_ref(),
            ),
            "refused: trades_legacy is missing or has an unexpected bound",
        ),
        (
            (
                _legacy_ref(),
                replace(
                    _default_ref(),
                    bound=(
                        "FOR VALUES FROM ('2026-08-01 00:00:00+00') "
                        "TO ('2026-08-02 00:00:00+00')"
                    ),
                ),
                _daily_ref(),
            ),
            "refused: trades_default is missing or is not DEFAULT",
        ),
        (
            (
                _legacy_ref(),
                _default_ref(),
                replace(
                    _daily_ref(),
                    bound=(
                        "FOR VALUES FROM ('2026-08-02 00:00:00+00') TO ('2026-08-03 00:00:00+00')"
                    ),
                ),
            ),
            "refused: trades_d20260801 is missing or has an unexpected bound",
        ),
    ],
)
def test_partitioned_verification_refuses_each_required_child_gap(
    partitions: tuple[lifecycle.PartitionRef, ...],
    message: str,
) -> None:
    """Legacy, DEFAULT, and exact future bounds are independently mandatory."""
    connection = _connection_double()
    trades_spec = lifecycle._spec("trades")

    with (
        patch.object(lifecycle, "FUTURE_LEAF_TARGET", 1),
        patch.object(lifecycle, "_require_partitioned_topology"),
        patch.object(lifecycle, "inspect", return_value=_partitioned(partitions)),
        pytest.raises(
            lifecycle.DailyPartitionError,
            match=rf"^{re.escape(message)}$",
        ),
    ):
        lifecycle._verify_partitioned(
            connection,
            trades_spec,
            _ANCHOR,
        )


def test_partitioned_verification_runs_every_deep_contract_before_return() -> None:
    """A valid child manifest is not accepted until all deep contracts pass."""
    connection = _connection_double()
    report = _partitioned((_legacy_ref(), _default_ref(), _daily_ref()))

    with (
        patch.object(lifecycle, "FUTURE_LEAF_TARGET", 1),
        patch.object(lifecycle, "_require_partitioned_topology") as require,
        patch.object(lifecycle, "inspect", return_value=report),
        patch.object(lifecycle, "_verify_leaf_local_objects") as leaf_verify,
        patch.object(lifecycle, "_verify_legacy_index_parentage") as parentage,
        patch.object(lifecycle, "_verify_partitioned_relation_shapes") as shapes,
        patch.object(lifecycle, "_verify_sequence_owner") as sequence,
    ):
        result = lifecycle._verify_partitioned(
            connection,
            lifecycle._spec("trades"),
            _ANCHOR,
        )

    assert result is report
    require.assert_called_once_with(connection, lifecycle._spec("trades"), _ANCHOR)
    assert [call.args[2] for call in leaf_verify.call_args_list] == [
        "trades_d20260801",
        "trades_default",
    ]
    parentage.assert_called_once_with(connection, lifecycle._spec("trades"), "trades_legacy")
    shapes.assert_called_once_with(
        connection,
        lifecycle._spec("trades"),
        "trades_legacy",
        _ANCHOR,
    )
    sequence.assert_called_once_with(connection, lifecycle._spec("trades"))


@pytest.mark.parametrize("shape_present", [False, True])
def test_legacy_index_parentage_refuses_absence_and_malformed_edges(
    shape_present: bool,
) -> None:
    """An adopted index must exist and name its one exact parent."""
    connection = _connection_double()
    contract = lifecycle._trade_u3_contract()
    shape = (
        _shape(
            contract.columns,
            unique=True,
            constraint_backed=False,
            parent_indexes=("wrong_parent",),
        )
        if shape_present
        else None
    )
    trades_spec = lifecycle._spec("trades")

    with (
        patch.object(
            lifecycle,
            "_legacy_parent_index_contracts",
            return_value=((contract, "trades_p_uq_instr_tid_exec"),),
        ),
        patch.object(lifecycle, "_index_shape", return_value=shape),
        patch.object(lifecycle, "_ordinary_index_matches", return_value=False),
        pytest.raises(lifecycle.DailyPartitionError, match="was not adopted"),
    ):
        lifecycle._verify_legacy_index_parentage(
            connection,
            trades_spec,
            "trades_legacy",
        )


def test_legacy_index_parentage_accepts_every_exact_edge() -> None:
    """All adoptable legacy indexes must complete the parentage loop."""
    connection = _connection_double()
    contract = lifecycle._trade_u3_contract()
    shape = _shape(
        contract.columns,
        unique=True,
        constraint_backed=False,
        parent_indexes=("trades_p_uq_instr_tid_exec",),
    )

    with (
        patch.object(
            lifecycle,
            "_legacy_parent_index_contracts",
            return_value=((contract, "trades_p_uq_instr_tid_exec"),),
        ),
        patch.object(lifecycle, "_index_shape", return_value=shape),
        patch.object(lifecycle, "_ordinary_index_matches", return_value=True) as matches,
    ):
        lifecycle._verify_legacy_index_parentage(
            connection,
            lifecycle._spec("trades"),
            "trades_legacy",
        )

    matches.assert_called_once_with(
        shape,
        contract,
        ("trades_p_uq_instr_tid_exec",),
    )


def test_partitioned_relation_shapes_refuse_a_range_check_on_the_parent() -> None:
    """The adoption-only range proof must remain legacy-local."""
    connection = _connection_double()
    parent_range = lifecycle.ConstraintInfo(definition=_range_definition(), validated=True)
    trades_spec = lifecycle._spec("trades")

    with (
        patch.object(lifecycle, "_verify_relation_columns"),
        patch.object(lifecycle, "_verify_ordinary_checks"),
        patch.object(lifecycle, "_constraint_info", return_value=parent_range),
        pytest.raises(lifecycle.DailyPartitionError, match="partitioned parent carries"),
    ):
        lifecycle._verify_partitioned_relation_shapes(
            connection,
            trades_spec,
            "trades_legacy",
            _ANCHOR,
        )


@pytest.mark.parametrize(
    "legacy_range",
    [
        None,
        lifecycle.ConstraintInfo(definition=_range_definition(), validated=False),
        lifecycle.ConstraintInfo(
            definition=(
                "CHECK (executed_at IS NOT NULL AND executed_at < '2026-08-02 00:00:00+00')"
            ),
            validated=True,
        ),
    ],
)
def test_partitioned_relation_shapes_refuse_each_invalid_legacy_range_state(
    legacy_range: lifecycle.ConstraintInfo | None,
) -> None:
    """Missing, unvalidated, and shifted legacy bounds must all fail closed."""
    connection = _connection_double()
    trades_spec = lifecycle._spec("trades")

    with (
        patch.object(lifecycle, "_verify_relation_columns"),
        patch.object(lifecycle, "_verify_ordinary_checks"),
        patch.object(lifecycle, "_constraint_info", side_effect=(None, legacy_range)),
        pytest.raises(lifecycle.DailyPartitionError, match="lacks the exact validated"),
    ):
        lifecycle._verify_partitioned_relation_shapes(
            connection,
            trades_spec,
            "trades_legacy",
            _ANCHOR,
        )


def test_partitioned_relation_shapes_runs_all_local_and_manifest_checks() -> None:
    """An exact legacy range permits every remaining deep verifier."""
    connection = _connection_double()
    spec = lifecycle._spec("trades")
    legacy_range = lifecycle.ConstraintInfo(
        definition=_range_definition(),
        validated=True,
    )

    with (
        patch.object(lifecycle, "_verify_relation_columns") as columns,
        patch.object(lifecycle, "_verify_ordinary_checks") as checks,
        patch.object(
            lifecycle,
            "_constraint_info",
            side_effect=(None, legacy_range),
        ) as constraint_info,
        patch.object(lifecycle, "_verify_noncheck_constraints") as constraints,
        patch.object(lifecycle, "_verify_legacy_index_manifest") as manifest,
        patch.object(lifecycle, "_verify_legacy_local_objects") as local,
    ):
        lifecycle._verify_partitioned_relation_shapes(
            connection,
            spec,
            "trades_legacy",
            _ANCHOR,
        )

    assert columns.call_args_list == [
        call(connection, spec, "trades", partition_key_not_null=True),
        call(connection, spec, "trades_legacy", partition_key_not_null=True),
    ]
    assert checks.call_args_list == [
        call(connection, spec, "trades"),
        call(connection, spec, "trades_legacy"),
    ]
    constraint_name = lifecycle._range_constraint_name(spec)
    assert constraint_info.call_args_list == [
        call(connection, "trades", constraint_name),
        call(connection, "trades_legacy", constraint_name),
    ]
    assert constraints.call_args_list == [
        call(connection, spec, "trades", lifecycle.ConstraintRole.PARENT),
        call(connection, spec, "trades_legacy", lifecycle.ConstraintRole.LEGACY),
    ]
    manifest.assert_called_once_with(connection, spec, "trades_legacy")
    local.assert_called_once_with(connection, spec, "trades_legacy")


@pytest.mark.parametrize(
    ("table", "expected_names"),
    [
        ("ticks", ("ticks_pkey", "ix_tick_instrument_ts")),
        (
            "candles",
            (
                "candles_pkey",
                "ix_candle_instrument_open",
                "ix_candles_public_id",
                "uq_candle_itf_open",
            ),
        ),
        (
            "trades",
            (
                "trades_pkey",
                "ix_trade_instrument_ts",
                "ix_trades_executed_at",
                "ix_trades_public_id",
                "ix_trades_timestamp",
                "uq_trade_instrument_trade_id",
            ),
        ),
    ],
)
def test_ordinary_index_contracts_preserve_exact_tuple_manifests(
    table: lifecycle.MarketDataTable,
    expected_names: tuple[str, ...],
) -> None:
    """Verify every ordinary index manifest retains its exact tuple contract.

    Given: Each supported partitioned market-data table and its ordered index names,
    When: The ordinary index contract is constructed,
    Then: Its tuple type and ordered names exactly match the table-specific manifest.
    """
    contracts = lifecycle._ordinary_index_contract(lifecycle._spec(table))

    assert isinstance(contracts, tuple)
    assert tuple(contract.name for contract in contracts) == expected_names


@pytest.mark.parametrize("table", ["ticks", "trades"])
def test_legacy_index_manifest_accepts_the_exact_table_specific_names(
    table: lifecycle.MarketDataTable,
) -> None:
    """Trades alone must add U3 to the unchanged ordinary index manifest."""
    connection = _connection_double()
    spec = lifecycle._spec(table)
    expected = {contract.name for contract in lifecycle._ordinary_index_contract(spec)}
    if table == "trades":
        expected.add(lifecycle._trade_u3_contract().name)

    with patch.object(
        lifecycle,
        "_relation_index_names",
        return_value=tuple(sorted(expected)),
    ):
        lifecycle._verify_legacy_index_manifest(connection, spec, f"{table}_legacy")


@pytest.mark.parametrize("duplicate", [False, True])
def test_legacy_index_manifest_refuses_extra_and_duplicate_names(
    duplicate: bool,
) -> None:
    """Set equality and relation count independently prevent catalog drift."""
    connection = _connection_double()
    spec = lifecycle._spec("ticks")
    expected = tuple(contract.name for contract in lifecycle._ordinary_index_contract(spec))
    names = expected + ((expected[0],) if duplicate else ("unexpected_index",))

    with (
        patch.object(lifecycle, "_relation_index_names", return_value=names),
        pytest.raises(lifecycle.DailyPartitionError, match="index manifest"),
    ):
        lifecycle._verify_legacy_index_manifest(connection, spec, "ticks_legacy")


def test_legacy_local_objects_accept_the_complete_trades_manifest() -> None:
    """The success path must verify PK, active partial, and retained U2."""
    connection = _connection_double()
    shape = _shape(("id",), unique=True, constraint_backed=True)

    with (
        patch.object(lifecycle, "_index_shape", return_value=shape) as index_shape,
        patch.object(lifecycle, "_ordinary_index_matches", return_value=True),
    ):
        lifecycle._verify_legacy_local_objects(
            connection,
            lifecycle._spec("trades"),
            "trades_legacy",
        )

    assert index_shape.call_count == 3


def test_legacy_parent_index_contracts_cover_each_table_shape() -> None:
    """Ticks, candles, and trades must map to their distinct parent indexes."""
    tick_names = lifecycle._legacy_parent_index_contracts(lifecycle._spec("ticks"))
    candle_names = lifecycle._legacy_parent_index_contracts(lifecycle._spec("candles"))
    trade_names = lifecycle._legacy_parent_index_contracts(lifecycle._spec("trades"))

    assert isinstance(tick_names, tuple)
    assert isinstance(candle_names, tuple)
    assert isinstance(trade_names, tuple)
    assert [(item[0].name, item[1]) for item in tick_names] == [
        ("ix_tick_instrument_ts", "ticks_p_ix_instr_ts")
    ]
    assert [(item[0].name, item[1]) for item in candle_names] == [
        ("uq_candle_itf_open", "candles_p_uq_itf_open"),
        ("ix_candle_instrument_open", "candles_p_ix_instr_open"),
    ]
    assert [item[1] for item in trade_names] == [
        "trades_p_uq_instr_tid_exec",
        "trades_p_ix_instr_ts",
        "trades_p_ix_ts",
        "trades_p_ix_exec",
    ]


def test_parent_index_verifier_refuses_a_manifest_name_mismatch() -> None:
    """A structurally plausible extra parent index must block lifecycle DDL."""
    connection = MagicMock(spec=Connection)
    connection.execute.return_value = [("unexpected_parent",)]

    typed_connection = cast(Connection, connection)
    ticks_spec = lifecycle._spec("ticks")
    with pytest.raises(lifecycle.DailyPartitionError, match="parent index manifest"):
        lifecycle._verify_parent_indexes(
            typed_connection,
            ticks_spec,
        )


@pytest.mark.parametrize("shape_present", [False, True])
def test_parent_index_verifier_refuses_absent_and_malformed_shapes(
    shape_present: bool,
) -> None:
    """Correct names alone cannot hide an absent or malformed index relation."""
    connection = MagicMock(spec=Connection)
    spec = lifecycle._spec("ticks")
    expected = spec.parent_indexes[0]
    connection.execute.return_value = [(expected.name,)]
    shape = (
        _shape(expected.columns, unique=False, constraint_backed=False) if shape_present else None
    )
    typed_connection = cast(Connection, connection)

    with (
        patch.object(lifecycle, "_index_shape", return_value=shape),
        patch.object(lifecycle, "_index_matches", return_value=False),
        pytest.raises(lifecycle.DailyPartitionError, match="unexpected shape"),
    ):
        lifecycle._verify_parent_indexes(typed_connection, spec)


def test_parent_index_verifier_accepts_every_named_exact_shape() -> None:
    """The complete expected parent manifest must finish its verification loop."""
    connection = MagicMock(spec=Connection)
    spec = lifecycle._spec("trades")
    connection.execute.return_value = [(index.name,) for index in spec.parent_indexes]
    shape = _shape(
        spec.parent_indexes[0].columns,
        unique=True,
        constraint_backed=False,
    )

    with (
        patch.object(lifecycle, "_index_shape", return_value=shape) as index_shape,
        patch.object(lifecycle, "_index_matches", return_value=True) as matches,
    ):
        lifecycle._verify_parent_indexes(cast(Connection, connection), spec)

    assert index_shape.call_count == len(spec.parent_indexes)
    assert matches.call_count == len(spec.parent_indexes)


@pytest.mark.parametrize(
    "mutation",
    [
        {"unique": False},
        {"columns": ("trade_id",)},
        {"constraint_backed": True},
        {"valid": False},
    ],
)
def test_parent_index_shape_comparison_rejects_each_structural_mutation(
    mutation: dict[str, object],
) -> None:
    """Uniqueness, keys, ownership, and storage safety are all mandatory."""
    expected = lifecycle._spec("trades").parent_indexes[0]
    canonical = _shape(
        expected.columns,
        unique=True,
        constraint_backed=False,
    )
    malformed = replace(canonical, **mutation)

    assert lifecycle._index_matches(malformed, expected) is False


def test_parent_index_shape_comparison_preserves_predicate_semantics() -> None:
    """Absent and active predicates must match their exact static contracts."""
    trade = lifecycle._spec("trades").parent_indexes[0]
    candle = lifecycle._spec("candles").parent_indexes[0]
    trade_shape = _shape(trade.columns, unique=True, constraint_backed=False)
    unexpected_partial = replace(trade_shape, predicate=_ACTIVE_PREDICATE)
    candle_shape = _shape(
        candle.columns,
        unique=True,
        constraint_backed=False,
        predicate=_ACTIVE_PREDICATE,
    )

    assert lifecycle._index_matches(trade_shape, trade) is True
    assert lifecycle._index_matches(unexpected_partial, trade) is False
    assert lifecycle._index_matches(replace(candle_shape, predicate=None), candle) is False
    assert lifecycle._index_matches(candle_shape, candle) is True
    assert (
        lifecycle._index_matches(
            replace(candle_shape, predicate="known_to IS NULL"),
            candle,
        )
        is False
    )


@pytest.mark.parametrize(
    "mutation",
    [
        {"access_method": "hash"},
        {"valid": False},
        {"ready": False},
        {"live": False},
        {"no_expressions": False},
        {"no_include": False},
        {"nulls_distinct": False},
        {"default_options": False},
        {"default_opclasses": False},
        {"attribute_collations": False},
        {"default_reloptions": False},
        {"default_tablespace": False},
        {"parent_indexes": ()},
        {"parent_indexes": ("wrong_parent",)},
    ],
)
def test_index_storage_matcher_rejects_each_independent_catalog_mutation(
    mutation: dict[str, object],
) -> None:
    """Every physical adoption flag and direct parent edge is load-bearing."""
    expected_parents = ("trades_p_uq_instr_tid_exec",)
    canonical = _shape(
        ("instrument_id", "trade_id", "executed_at"),
        unique=True,
        constraint_backed=False,
        parent_indexes=expected_parents,
    )

    assert lifecycle._index_storage_matches(canonical, expected_parents) is True
    assert (
        lifecycle._index_storage_matches(
            replace(canonical, **mutation),
            expected_parents,
        )
        is False
    )


@pytest.mark.parametrize(
    ("predicate", "expected"),
    [
        (_ACTIVE_PREDICATE, True),
        ("known_to IS NULL", False),
        ("known_to = 'not-a-date'", False),
        ("known_to = '9999-12-31 23:59:59'", False),
        ("known_to = '2026-08-01 00:00:00+00'", False),
        (
            "other_column = '9999-12-31 23:59:59+00'::timestamp with time zone",
            False,
        ),
    ],
)
def test_active_predicate_matcher_accepts_only_the_infinity_equality(
    predicate: str,
    expected: bool,
) -> None:
    """Every malformed or shifted active-row predicate must be rejected."""
    assert lifecycle._active_predicate_matches(predicate) is expected


def test_leaf_index_manifest_accepts_exact_local_names_and_parent_edges() -> None:
    """The leaf manifest must bind every inherited index to its exact parent.

    Given: A candles leaf with both local names and exact inherited index shapes.
    When: The leaf index manifest is verified.
    Then: Every inherited index is accepted only through its named parent edge.
    """
    connection = _connection_double()
    spec = lifecycle._spec("candles")
    leaf = "candles_d20260801"
    inherited_names = tuple(
        f"{leaf}_{'_'.join(index.columns)}_idx" for index in spec.parent_indexes
    )
    shapes = tuple(
        _shape(
            index.columns,
            unique=index.unique,
            constraint_backed=False,
            predicate=_ACTIVE_PREDICATE if index.predicate is not None else None,
            parent_indexes=(index.name,),
        )
        for index in spec.parent_indexes
    )
    names = (*inherited_names, f"{leaf}_pkey", f"{leaf}_public_id")

    with (
        patch.object(lifecycle, "_relation_index_names", return_value=names),
        patch.object(lifecycle, "_index_shape", side_effect=shapes) as index_shape,
    ):
        lifecycle._verify_leaf_index_manifest(connection, spec, leaf)

    assert index_shape.call_args_list == [
        call(connection, leaf, child_name) for child_name in inherited_names
    ]


def test_leaf_index_manifest_refuses_exact_names_with_the_wrong_parent_edge() -> None:
    """The inherited child name cannot disguise a wrong index-parent edge.

    Given: An exact ticks leaf name manifest whose inherited index names the wrong parent.
    When: The leaf index manifest verifies the inherited index shape.
    Then: It emits the exact refusal naming both the child and required parent.
    """
    connection = _connection_double()
    spec = lifecycle._spec("ticks")
    leaf = "ticks_d20260801"
    parent = spec.parent_indexes[0]
    child_name = f"{leaf}_{'_'.join(parent.columns)}_idx"
    names = (child_name, f"{leaf}_pkey")
    shape = _shape(
        parent.columns,
        unique=parent.unique,
        constraint_backed=False,
        parent_indexes=("wrong_parent",),
    )
    message = f"refused: {child_name} is not the exact child of {parent.name}"

    with (
        patch.object(lifecycle, "_relation_index_names", return_value=names),
        patch.object(lifecycle, "_index_shape", return_value=shape),
        pytest.raises(
            lifecycle.DailyPartitionError,
            match=rf"^{re.escape(message)}$",
        ),
    ):
        lifecycle._verify_leaf_index_manifest(connection, spec, leaf)


def test_leaf_index_manifest_refuses_an_extra_relation_name_exactly() -> None:
    """The complete name manifest must reject even one extra leaf index.

    Given: A ticks leaf index catalog containing its required names and one extra name.
    When: The leaf index manifest is verified.
    Then: It emits the exact refusal with the observed ordered name tuple.
    """
    connection = _connection_double()
    spec = lifecycle._spec("ticks")
    leaf = "ticks_d20260801"
    parent = spec.parent_indexes[0]
    names = (
        f"{leaf}_{'_'.join(parent.columns)}_idx",
        f"{leaf}_pkey",
        "unexpected_index",
    )
    message = f"refused: {leaf} index manifest is {names!r}"

    with (
        patch.object(lifecycle, "_relation_index_names", return_value=names),
        pytest.raises(
            lifecycle.DailyPartitionError,
            match=rf"^{re.escape(message)}$",
        ),
    ):
        lifecycle._verify_leaf_index_manifest(connection, spec, leaf)


@pytest.mark.parametrize(
    "primary",
    [
        None,
        _shape(("id",), unique=False, constraint_backed=True),
        _shape(("public_id",), unique=True, constraint_backed=True),
        _shape(("id",), unique=True, constraint_backed=False),
        replace(
            _shape(("id",), unique=True, constraint_backed=True),
            default_tablespace=False,
        ),
    ],
)
def test_leaf_verifier_rejects_every_malformed_primary_shape(
    primary: lifecycle.IndexShape | None,
) -> None:
    """Leaf identity requires an exact constraint-backed local ``id`` PK."""
    connection = _connection_double()
    candles_spec = lifecycle._spec("candles")

    with (
        patch.object(lifecycle, "_verify_relation_columns"),
        patch.object(lifecycle, "_verify_ordinary_checks"),
        patch.object(lifecycle, "_verify_noncheck_constraints"),
        patch.object(lifecycle, "_verify_leaf_index_manifest"),
        patch.object(lifecycle, "_index_shape", return_value=primary),
        pytest.raises(lifecycle.DailyPartitionError, match="local primary key"),
    ):
        lifecycle._verify_leaf_local_objects(
            connection,
            candles_spec,
            "candles_d20260801",
        )


def test_leaf_verifier_returns_after_the_ticks_primary_key() -> None:
    """Ticks intentionally have no active-public-id partial."""
    connection = _connection_double()
    primary = _shape(("id",), unique=True, constraint_backed=True)

    with (
        patch.object(lifecycle, "_verify_relation_columns"),
        patch.object(lifecycle, "_verify_ordinary_checks"),
        patch.object(lifecycle, "_verify_noncheck_constraints"),
        patch.object(lifecycle, "_verify_leaf_index_manifest"),
        patch.object(lifecycle, "_index_shape", return_value=primary) as index_shape,
    ):
        lifecycle._verify_leaf_local_objects(
            connection,
            lifecycle._spec("ticks"),
            "ticks_d20260801",
        )

    index_shape.assert_called_once_with(connection, "ticks_d20260801", "ticks_d20260801_pkey")


@pytest.mark.parametrize(
    "public_id",
    [
        None,
        _shape(
            ("public_id",),
            unique=False,
            constraint_backed=False,
            predicate=_ACTIVE_PREDICATE,
        ),
        _shape(
            ("id",),
            unique=True,
            constraint_backed=False,
            predicate=_ACTIVE_PREDICATE,
        ),
        _shape(
            ("public_id",),
            unique=True,
            constraint_backed=True,
            predicate=_ACTIVE_PREDICATE,
        ),
        _shape(("public_id",), unique=True, constraint_backed=False),
        _shape(
            ("public_id",),
            unique=True,
            constraint_backed=False,
            predicate="known_to IS NULL",
        ),
        replace(
            _shape(
                ("public_id",),
                unique=True,
                constraint_backed=False,
                predicate=_ACTIVE_PREDICATE,
            ),
            valid=False,
        ),
    ],
)
def test_leaf_verifier_rejects_every_malformed_active_public_id_shape(
    public_id: lifecycle.IndexShape | None,
) -> None:
    """Every part of the leaf-local active-public-id arbiter is mandatory."""
    connection = _connection_double()
    primary = _shape(("id",), unique=True, constraint_backed=True)
    candles_spec = lifecycle._spec("candles")

    with (
        patch.object(lifecycle, "_verify_relation_columns"),
        patch.object(lifecycle, "_verify_ordinary_checks"),
        patch.object(lifecycle, "_verify_noncheck_constraints"),
        patch.object(lifecycle, "_verify_leaf_index_manifest"),
        patch.object(lifecycle, "_index_shape", side_effect=(primary, public_id)),
        pytest.raises(lifecycle.DailyPartitionError, match="active-public-id partial"),
    ):
        lifecycle._verify_leaf_local_objects(
            connection,
            candles_spec,
            "candles_d20260801",
        )


def test_leaf_verifier_accepts_both_exact_local_objects() -> None:
    """A canonical PK and active partial complete leaf verification."""
    connection = _connection_double()
    primary = _shape(("id",), unique=True, constraint_backed=True)
    public_id = _shape(
        ("public_id",),
        unique=True,
        constraint_backed=False,
        predicate=_ACTIVE_PREDICATE,
    )

    with (
        patch.object(lifecycle, "_verify_relation_columns"),
        patch.object(lifecycle, "_verify_ordinary_checks"),
        patch.object(lifecycle, "_verify_noncheck_constraints"),
        patch.object(lifecycle, "_verify_leaf_index_manifest"),
        patch.object(lifecycle, "_index_shape", side_effect=(primary, public_id)),
    ):
        lifecycle._verify_leaf_local_objects(
            connection,
            lifecycle._spec("candles"),
            "candles_d20260801",
        )


@pytest.mark.parametrize(
    ("bound", "lower", "upper", "expected"),
    [
        (
            "FOR VALUES FROM (MINVALUE) TO ('2026-08-01 00:00:00+00')",
            None,
            _ANCHOR,
            True,
        ),
        (
            ("FOR VALUES FROM ('2026-08-01 00:00:00+00') TO ('2026-08-02 00:00:00+00')"),
            _ANCHOR,
            _NEXT_DAY,
            True,
        ),
        ("DEFAULT", None, _ANCHOR, False),
        (
            "FOR VALUES FROM (MINVALUE) TO ('not-a-date')",
            None,
            _ANCHOR,
            False,
        ),
        (
            "FOR VALUES FROM (MINVALUE) TO ('2026-08-01 00:00:00')",
            None,
            _ANCHOR,
            False,
        ),
        (
            ("FOR VALUES FROM ('2026-08-01 00:00:00+00') TO ('2026-08-03 00:00:00+00')"),
            _ANCHOR,
            _NEXT_DAY,
            False,
        ),
        (
            ("FOR VALUES FROM ('2026-08-01 00:00:00+00') TO ('2026-08-02 00:00:00+00')"),
            None,
            _ANCHOR,
            False,
        ),
    ],
)
def test_range_bound_matcher_requires_exact_aware_boundaries(
    bound: str,
    lower: datetime | None,
    upper: datetime,
    expected: bool,
) -> None:
    """Wrong arity, syntax, awareness, or instant must fail closed."""
    assert lifecycle._range_bound_matches(bound, lower, upper) is expected
