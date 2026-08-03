"""Fail-closed unit coverage for PostgreSQL migration 0043."""

import importlib
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest

migration = importlib.import_module("snapper.data.migrations.versions.0043_daily_market_partitions")

_ANCHOR = datetime(2026, 8, 2, tzinfo=UTC)


def _bind(*scalar_values: object, dialect: str = "postgresql") -> MagicMock:
    """Build a connection double with deterministic catalog scalar results."""
    bind = MagicMock()
    bind.dialect.name = dialect
    if scalar_values:
        bind.scalar.side_effect = scalar_values
    else:
        bind.scalar.return_value = True
    return bind


def _operations(bind: MagicMock) -> tuple[MagicMock, MagicMock]:
    """Build an Alembic operations double with a functional batch context."""
    batch = MagicMock()
    batch_context = MagicMock()
    batch_context.__enter__.return_value = batch
    batch_context.__exit__.return_value = False
    operations = MagicMock()
    operations.get_bind.return_value = bind
    operations.batch_alter_table.return_value = batch_context
    return operations, batch


def test_identifier_partition_and_index_renderers_are_canonical() -> None:
    """Generated SQL names quote inputs and keep deterministic daily bounds."""
    ticks = migration._TABLES[0]
    index = ticks.matching_indexes[0]

    assert migration._identifier('tick"name') == '"tick""name"'
    assert migration._qualified("ticks") == '"public"."ticks"'
    assert migration._sql_text_array(("normal", "it's")) == ("ARRAY['normal', 'it''s']::text[]")
    assert migration._anchor_literal(_ANCHOR) == "TIMESTAMPTZ '2026-08-02T00:00:00+00:00'"
    assert migration._leaf_name("ticks", _ANCHOR) == "ticks_d20260802"
    assert migration._daily_names(ticks, _ANCHOR)[-1] == "ticks_d20260815"
    assert migration._generated_index_name("ticks_d20260802", index) == (
        "ticks_d20260802_instrument_public_id_timestamp_idx"
    )

    with pytest.raises(RuntimeError, match="NAMEDATALEN"):
        migration._generated_index_name("x" * 63, index)


def test_explicit_anchor_accepts_absence_and_canonical_utc() -> None:
    """Only an absent or exactly canonical UTC-midnight x-argument is accepted."""
    with patch.object(migration.context, "get_x_argument", return_value=[]):
        assert migration._explicit_anchor() is None

    with patch.object(
        migration.context,
        "get_x_argument",
        return_value=["partition_anchor=2026-08-02T00:00:00+00:00"],
    ):
        assert migration._explicit_anchor() == _ANCHOR


@pytest.mark.parametrize(
    ("arguments", "message"),
    [
        (
            [
                "partition_anchor=2026-08-02T00:00:00+00:00",
                "partition_anchor=2026-08-03T00:00:00+00:00",
            ],
            "at most once",
        ),
        (["partition_anchor"], "at most once"),
        (["partition_anchor:2026-08-02"], "at most once"),
        (["partition_anchor=2026-08-02"], "exactly match"),
        (["partition_anchor=2026-02-30T00:00:00+00:00"], "valid calendar date"),
    ],
)
def test_explicit_anchor_rejects_ambiguous_or_malformed_values(
    arguments: list[str],
    message: str,
) -> None:
    """Ambiguous spelling and invalid calendar dates fail before catalog work."""
    with (
        patch.object(migration.context, "get_x_argument", return_value=arguments),
        pytest.raises(RuntimeError, match=message),
    ):
        migration._explicit_anchor()


def test_explicit_anchor_rejects_non_utc_parse_result() -> None:
    """The parsed instant must retain the exact UTC offset required by the contract."""
    parsed = datetime(2026, 8, 2, tzinfo=timezone(timedelta(hours=1)))
    datetime_double = MagicMock()
    datetime_double.fromisoformat.return_value = parsed

    with (
        patch.object(
            migration.context,
            "get_x_argument",
            return_value=["partition_anchor=2026-08-02T00:00:00+00:00"],
        ),
        patch.object(migration, "datetime", datetime_double),
        pytest.raises(RuntimeError, match="UTC midnight"),
    ):
        migration._explicit_anchor()


def test_inferred_anchor_requires_one_utc_midnight_bound() -> None:
    """Legacy parent inference normalizes offsets and refuses missing or partial days."""
    ticks = migration._TABLES[0]
    bind = _bind(datetime(2026, 8, 2, 2, tzinfo=timezone(timedelta(hours=2))))

    assert migration._infer_parent_anchor(bind, ticks) == _ANCHOR

    bind_without_upper_bound = _bind(None)
    with pytest.raises(RuntimeError, match="upper bound is unavailable"):
        migration._infer_parent_anchor(bind_without_upper_bound, ticks)
    bind_with_offset_upper_bound = _bind(_ANCHOR + timedelta(minutes=1))
    with pytest.raises(RuntimeError, match="not UTC midnight"):
        migration._infer_parent_anchor(bind_with_offset_upper_bound, ticks)


def test_resolve_anchor_prefers_explicit_then_consistent_inference() -> None:
    """Anchor choice is explicit, fresh, or one value shared by all parents."""
    bind = _bind()
    with patch.object(migration, "_explicit_anchor", return_value=_ANCHOR):
        assert migration._resolve_anchor(bind) == _ANCHOR

    datetime_double = MagicMock()
    datetime_double.now.return_value = _ANCHOR + timedelta(hours=19)
    with (
        patch.object(migration, "_explicit_anchor", return_value=None),
        patch.object(migration, "_relation_kind", return_value="r:false"),
        patch.object(migration, "datetime", datetime_double),
    ):
        assert migration._resolve_anchor(bind) == _ANCHOR

    with (
        patch.object(migration, "_explicit_anchor", return_value=None),
        patch.object(migration, "_relation_kind", return_value="p:false"),
        patch.object(migration, "_infer_parent_anchor", return_value=_ANCHOR),
    ):
        assert migration._resolve_anchor(bind) == _ANCHOR

    with (
        patch.object(migration, "_explicit_anchor", return_value=None),
        patch.object(migration, "_relation_kind", return_value="p:false"),
        patch.object(
            migration,
            "_infer_parent_anchor",
            side_effect=[_ANCHOR, _ANCHOR + timedelta(days=1), _ANCHOR],
        ),
        pytest.raises(RuntimeError, match="do not share"),
    ):
        migration._resolve_anchor(bind)


def test_relation_and_requirement_catalog_guards() -> None:
    """Compact relation state and every catalog assertion fail closed."""
    bind = _bind("p:false")
    assert migration._relation_kind(bind, "ticks") == "p:false"
    migration._require(_bind(True), MagicMock(), "unused")

    drifted_bind = _bind(False)
    drift_context = MagicMock()
    with pytest.raises(RuntimeError, match="catalog drift"):
        migration._require(drifted_bind, drift_context, "catalog drift")


def test_column_and_constraint_contract_renderers_cover_all_roles() -> None:
    """Ordinary, parent, legacy, and leaf manifests remain structurally distinct."""
    ticks, candles, trades = migration._TABLES
    ordinary_trade_columns = migration._column_values(trades, ordinary=True)
    partitioned_trade_columns = migration._column_values(trades, ordinary=False)
    migration._column_values(ticks, ordinary=True)

    assert "'executed_at', 'timestamp with time zone', FALSE" in ordinary_trade_columns
    assert "'executed_at', 'timestamp with time zone', TRUE" in partitioned_trade_columns
    assert "synthesized" in migration._check_definition("ck_candle_source")
    assert migration._check_definition("ck_ticks_sequence_id") == "CHECK (sequence_id > 0)"

    candle_not_null = migration._not_null_constraint_rows(candles, ordinary=False)
    trade_not_null = migration._not_null_constraint_rows(trades, ordinary=True)
    migration._not_null_constraint_rows(ticks, ordinary=True)
    assert any('NOT NULL "timestamp"' in row[2] for row in candle_not_null)
    assert all("executed_at" not in row[0] for row in trade_not_null)

    assert not any(
        row[1] == "p"
        for row in migration._constraint_rows(ticks, "ticks", legacy=False, anchor=_ANCHOR)
    )
    leaf_rows = migration._constraint_rows(
        candles,
        "candles_d20260802",
        legacy=False,
        anchor=_ANCHOR,
    )
    assert any(row[1] == "p" for row in leaf_rows)
    legacy_rows = migration._constraint_rows(
        trades,
        "trades_legacy",
        legacy=True,
        anchor=_ANCHOR,
    )
    assert any(row[0] == "uq_trade_instrument_trade_id" for row in legacy_rows)
    assert any(row[0] == "ck_trades_legacy_range" for row in legacy_rows)

    with pytest.raises(RuntimeError, match="requires an anchor"):
        migration._constraint_rows(trades, "trades_legacy", legacy=True, anchor=None)


def test_index_contract_renderers_cover_parent_legacy_leaf_and_ordinary() -> None:
    """Every supported index role renders its exact structural manifest."""
    ticks, candles, trades = migration._TABLES
    parent_rows = migration._index_shape_rows(trades, "trades", parent=True, legacy=False)
    legacy_rows = migration._index_shape_rows(
        trades,
        "trades_legacy",
        parent=False,
        legacy=True,
    )
    leaf_rows = migration._index_shape_rows(
        candles,
        "candles_d20260802",
        parent=False,
        legacy=False,
    )
    tick_leaf_rows = migration._index_shape_rows(
        ticks,
        "ticks_d20260802",
        parent=False,
        legacy=False,
    )
    ticks_ordinary = migration._ordinary_index_rows(ticks, has_trade_u3=False)
    candles_ordinary = migration._ordinary_index_rows(candles, has_trade_u3=False)
    trades_without_u3 = migration._ordinary_index_rows(trades, has_trade_u3=False)
    trades_with_u3 = migration._ordinary_index_rows(trades, has_trade_u3=True)

    assert len(parent_rows) == len(trades.matching_indexes)
    assert any("uq_trade_instrument_trade_id" in row for row in legacy_rows)
    assert any("candles_d20260802_public_id" in row for row in leaf_rows)
    assert len(tick_leaf_rows) == 1
    assert len(ticks_ordinary) == 2
    assert any("ix_candles_public_id" in row for row in candles_ordinary)
    assert len(trades_with_u3) == len(trades_without_u3) + 1


def test_low_level_catalog_verifiers_accept_matching_results() -> None:
    """Each exact PostgreSQL catalog query accepts a complete matching world."""
    bind = _bind()
    ticks, candles, trades = migration._TABLES
    candle_relations = ("candles", "candles_legacy", "candles_d20260802")

    migration._verify_columns(bind, trades, ("trades",), ordinary=True)
    migration._verify_constraint_manifest(
        bind,
        trades,
        "trades_legacy",
        legacy=True,
        anchor=_ANCHOR,
    )
    migration._verify_check_definitions(bind, candles, candle_relations, _ANCHOR)
    migration._verify_named_indexes(
        bind,
        "ticks",
        migration._index_shape_rows(ticks, "ticks", parent=True, legacy=False),
    )
    migration._verify_generated_partition_indexes(
        bind,
        candles,
        ("candles_d20260802",),
    )
    migration._verify_leaf_local_indexes(bind, ticks, "ticks_d20260802")
    migration._verify_partial_predicates(bind, ticks, ("ticks",), parent=True)
    migration._verify_partial_predicates(bind, candles, ("candles",), parent=True)
    migration._verify_partial_predicates(bind, candles, ("candles_d20260802",))
    migration._verify_standalone_unique_indexes(bind, ticks, "ticks_legacy")
    migration._verify_standalone_unique_indexes(bind, candles, "candles_legacy")
    migration._verify_standalone_unique_indexes(bind, trades, "trades_legacy")
    migration._verify_sequence(bind, trades, "trades")

    assert bind.scalar.call_count == 19


def test_partition_tree_and_complete_partitioned_verifier() -> None:
    """A matching topology traverses all children, constraints, and index roles."""
    bind = _bind()
    ticks, candles, trades = migration._TABLES

    tick_children = migration._verify_partition_tree(bind, ticks, _ANCHOR)
    assert tick_children[0] == "ticks_legacy"
    assert tick_children[-1] == "ticks_default"
    assert len(tick_children) == 16

    migration._verify_partitioned(bind, candles, _ANCHOR)
    migration._verify_partitioned(bind, trades, _ANCHOR)

    assert str(bind.execute.call_args_list[0].args[0]) == (
        'LOCK TABLE ONLY "public"."candles" IN ACCESS SHARE MODE'
    )


def test_ordinary_verifier_handles_trade_u3_presence_and_absence() -> None:
    """An ordinary table accepts only its exact revision-0042 index variants."""
    bind = _bind()
    ticks, candles, trades = migration._TABLES

    with patch.object(migration, "_relation_kind", return_value=None):
        migration._verify_ordinary(bind, ticks)
        migration._verify_ordinary(bind, candles)
        migration._verify_ordinary(bind, trades)
    with patch.object(migration, "_relation_kind", return_value="i:false"):
        migration._verify_ordinary(bind, trades)


def test_name_availability_guard_checks_every_future_object() -> None:
    """Ordinary adoption reserves legacy, default, daily, and parent-index names."""
    bind = _bind()
    migration._assert_names_available(bind, migration._TABLES[1], _ANCHOR)

    statement = str(bind.scalar.call_args.args[0])
    assert "candles_legacy" in statement
    assert "candles_default" in statement
    assert "candles_d20260815" in statement
    assert "candles_p_uq_itf_open" in statement


def test_preflight_accepts_exact_partitioned_and_empty_ordinary_states() -> None:
    """Only verified parents and twice-empty locked ordinary tables may proceed."""
    bind = _bind()
    ticks = migration._TABLES[0]

    with (
        patch.object(migration, "_relation_kind", return_value="p:false"),
        patch.object(migration, "_verify_partitioned") as verify_partitioned,
    ):
        assert migration._preflight(bind, ticks, _ANCHOR) == "partitioned"
    verify_partitioned.assert_called_once_with(bind, ticks, _ANCHOR)

    ordinary_bind = _bind(False, False)
    with (
        patch.object(migration, "_relation_kind", return_value="r:false"),
        patch.object(migration, "_verify_ordinary") as verify_ordinary,
        patch.object(migration, "_assert_names_available") as names_available,
    ):
        assert migration._preflight(ordinary_bind, ticks, _ANCHOR) == "ordinary"
    verify_ordinary.assert_called_once_with(ordinary_bind, ticks)
    names_available.assert_called_once_with(ordinary_bind, ticks, _ANCHOR)
    ordinary_bind.execute.assert_called_once()


def test_preflight_rejects_missing_populated_and_racing_tables() -> None:
    """Unsupported identity, existing rows, and lock races all fail closed."""
    ticks = migration._TABLES[0]
    missing_relation_bind = _bind()
    with (
        patch.object(migration, "_relation_kind", return_value=None),
        pytest.raises(RuntimeError, match="observed missing"),
    ):
        migration._preflight(missing_relation_bind, ticks, _ANCHOR)

    populated_bind = _bind(True)
    with (
        patch.object(migration, "_relation_kind", return_value="r:false"),
        pytest.raises(RuntimeError, match="is populated"),
    ):
        migration._preflight(populated_bind, ticks, _ANCHOR)

    racing_bind = _bind(False, True)
    with (
        patch.object(migration, "_relation_kind", return_value="r:false"),
        pytest.raises(RuntimeError, match="became populated"),
    ):
        migration._preflight(racing_bind, ticks, _ANCHOR)


def test_creation_helpers_emit_complete_parent_leaf_and_legacy_ddl() -> None:
    """Independent adoption DDL includes checks, indexes, leaves, and sequence ownership."""
    bind = _bind()
    operations, _batch = _operations(bind)
    ticks, candles, trades = migration._TABLES

    with patch.object(migration, "op", operations):
        migration._create_parent_checks(ticks)
        migration._create_parent_checks(candles)
        migration._create_parent_indexes(trades)
        migration._create_local_leaf_objects(ticks, "ticks_d20260802")
        migration._create_local_leaf_objects(candles, "candles_d20260802")
        migration._create_partitions(candles, _ANCHOR)
        migration._prepare_legacy(ticks, _ANCHOR)
        with patch.object(migration, "_relation_kind", return_value=None):
            migration._prepare_legacy(trades, _ANCHOR)
        with patch.object(migration, "_relation_kind", return_value="i:false"):
            migration._prepare_legacy(trades, _ANCHOR)
        with patch.object(migration, "_verify_partitioned") as verify_partitioned:
            migration._adopt(bind, ticks, _ANCHOR)

    ddl = "\n".join(str(item.args[0]) for item in operations.execute.call_args_list)
    assert "ADD CONSTRAINT ck_candle_source" in ddl
    assert "candles_d20260815" in ddl
    assert "candles_default" in ddl
    assert "CREATE UNIQUE INDEX uq_trade_instr_tid_exec" in ddl
    assert "ALTER SEQUENCE" in ddl
    verify_partitioned.assert_called_once_with(bind, ticks, _ANCHOR)


def test_sqlite_tightening_refuses_nulls_and_emits_exact_batch_contract() -> None:
    """SQLite refuses null event time and otherwise swaps U2 for the U3 arbiter."""
    operations, batch = _operations(_bind(False, dialect="sqlite"))
    with patch.object(migration, "op", operations):
        migration._upgrade_sqlite(operations.get_bind.return_value)

    batch.drop_constraint.assert_called_once_with(
        "uq_trade_instrument_trade_id",
        type_="unique",
    )
    batch.alter_column.assert_called_once()
    operations.create_index.assert_called_once_with(
        "uq_trade_instr_tid_exec",
        "trades",
        ["instrument_public_id", "trade_id", "executed_at"],
        unique=True,
    )

    sqlite_bind_with_null_rows = _bind(True, dialect="sqlite")
    with pytest.raises(RuntimeError, match="contains NULL"):
        migration._upgrade_sqlite(sqlite_bind_with_null_rows)


def test_public_upgrade_routes_supported_dialects_and_adopts_only_ordinary() -> None:
    """Upgrade rejects foreign dialects and adopts exactly ordinary PostgreSQL roots."""
    unsupported = _bind(dialect="mysql")
    operations, _batch = _operations(unsupported)
    with (
        patch.object(migration, "op", operations),
        pytest.raises(RuntimeError, match="supports only"),
    ):
        migration.upgrade()

    sqlite_bind = _bind(dialect="sqlite")
    sqlite_operations, _sqlite_batch = _operations(sqlite_bind)
    with (
        patch.object(migration, "op", sqlite_operations),
        patch.object(migration, "_upgrade_sqlite") as upgrade_sqlite,
    ):
        migration.upgrade()
    upgrade_sqlite.assert_called_once_with(sqlite_bind)

    postgres = _bind(dialect="postgresql")
    postgres_operations, _postgres_batch = _operations(postgres)
    with (
        patch.object(migration, "op", postgres_operations),
        patch.object(migration, "_resolve_anchor", return_value=_ANCHOR),
        patch.object(
            migration,
            "_preflight",
            side_effect=["ordinary", "partitioned", "ordinary"],
        ),
        patch.object(migration, "_adopt") as adopt,
    ):
        migration.upgrade()

    assert postgres.execute.call_count == 3
    assert [item.args[1].name for item in adopt.call_args_list] == ["ticks", "trades"]


def test_public_downgrade_reverses_sqlite_and_refuses_postgresql() -> None:
    """Downgrade is implemented only for the ordinary SQLite compatibility slice."""
    sqlite = _bind(dialect="sqlite")
    operations, batch = _operations(sqlite)
    with patch.object(migration, "op", operations):
        migration.downgrade()

    operations.drop_index.assert_called_once_with(
        "uq_trade_instr_tid_exec",
        table_name="trades",
    )
    batch.create_unique_constraint.assert_called_once_with(
        "uq_trade_instrument_trade_id",
        ["instrument_public_id", "trade_id"],
    )

    postgres = _bind(dialect="postgresql")
    postgres_operations, _postgres_batch = _operations(postgres)
    with (
        patch.object(migration, "op", postgres_operations),
        pytest.raises(RuntimeError, match="intentionally fail-closed"),
    ):
        migration.downgrade()
