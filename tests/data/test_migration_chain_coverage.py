"""Semantic coverage for the complete Alembic migration chain.

The worker template proves that every upgrade can run on an empty SQLite
database, but a downgrade reached incidentally by an unrelated test does not
prove that its own artifact disappeared.  This module pins the compact schema
deltas that have no dedicated migration test and proves that the entire chain
can return to ``base`` and be rebuilt to ``head`` without residue.
"""

import importlib
from datetime import UTC
from datetime import datetime
from io import StringIO
from pathlib import Path
from types import SimpleNamespace
from typing import Protocol
from typing import cast
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest
import sqlalchemy as sa
from alembic import command
from alembic.config import Config
from alembic.migration import MigrationContext
from alembic.operations import Operations
from alembic.script import ScriptDirectory
from sqlalchemy.sql.elements import TextClause

_ALEMBIC_INI = Path(__file__).resolve().parents[2] / "alembic.ini"
_ACTIVE_TICK_INDEXES = {
    "ix_ticks_instrument_public_id",
    "ix_ticks_public_id",
}


class _FillBackfillMigration(Protocol):
    """Typed surface used from the numerically named migration module."""

    def upgrade(self) -> None:
        """Run the fill-truth backfill."""

    def _dedup_additive_and_legacy(
        self,
        fill_rows: list[
            tuple[
                str,
                float | None,
                float | None,
                float | None,
                str | None,
                str | None,
                str | None,
            ]
        ],
        order_exchange_order_id: str | None,
    ) -> tuple[float, float | None]:
        """Return the migration's deduplicated additive and legacy totals."""


class _ReversibleMigration(Protocol):
    """Common callable surface of a reversible Alembic revision."""

    def upgrade(self) -> None:
        """Apply the revision."""

    def downgrade(self) -> None:
        """Reverse the revision."""


class _InitialMigration(_ReversibleMigration, Protocol):
    """Typed helper surface specific to revision 0001."""

    def _bind_datetime_literal(self, value: datetime, dialect_name: str) -> datetime | str:
        """Return the dialect-appropriate datetime bind value."""


class _CalculatedSourceMigration(_ReversibleMigration, Protocol):
    """Typed helper surface specific to revision 0012."""

    def _retag_calculated_to_native_batched(self) -> None:
        """Retag calculated rows through bounded id windows."""


class _BoundsResult:
    """Minimal result exposing the aggregate bounds read by revision 0012."""

    def __init__(self, bounds: tuple[int | None, int | None]) -> None:
        self._bounds = bounds

    def one(self) -> tuple[int | None, int | None]:
        """Return the configured minimum and maximum ids."""
        return self._bounds


class _BoundsConnection:
    """Minimal connection for the revision 0012 bounds query."""

    def __init__(self, bounds: tuple[int | None, int | None]) -> None:
        self._bounds = bounds

    def execute(self, _statement: object) -> _BoundsResult:
        """Return the configured aggregate result."""
        return _BoundsResult(self._bounds)


class _RetagOperations:
    """Record the batched UPDATE statements emitted by revision 0012."""

    def __init__(self, bounds: tuple[int | None, int | None]) -> None:
        self._bind = _BoundsConnection(bounds)
        self.statements: list[object] = []

    def get_bind(self) -> _BoundsConnection:
        """Return the bounds-query connection."""
        return self._bind

    def execute(self, statement: object) -> None:
        """Record one id-window UPDATE."""
        self.statements.append(statement)


def _config(db_url: str) -> Config:
    """Build an Alembic configuration for one throwaway SQLite database."""
    config = Config(str(_ALEMBIC_INI))
    config.set_main_option("sqlalchemy.url", db_url)
    return config


def _postgresql_operations() -> tuple[Operations, StringIO]:
    """Build an offline PostgreSQL operation recorder."""
    output = StringIO()
    context = MigrationContext.configure(
        dialect_name="postgresql",
        opts={"as_sql": True, "output_buffer": output},
    )
    return Operations(context), output


def _tables(engine: sa.Engine) -> set[str]:
    """Return the current user-visible table names."""
    return set(sa.inspect(engine).get_table_names())


def _columns(engine: sa.Engine, table: str) -> set[str]:
    """Return the named columns currently present on one table."""
    return {str(column["name"]) for column in sa.inspect(engine).get_columns(table)}


def _indexes(engine: sa.Engine, table: str) -> set[str]:
    """Return the named indexes currently present on one table."""
    return {
        str(index["name"])
        for index in sa.inspect(engine).get_indexes(table)
        if index["name"] is not None
    }


def _revision(engine: sa.Engine) -> str | None:
    """Return the current Alembic revision, or ``None`` at base."""
    with engine.connect() as connection:
        value = connection.execute(
            sa.text("SELECT version_num FROM alembic_version")
        ).scalar_one_or_none()
    return None if value is None else str(value)


def _head_revision() -> str:
    """Return the chain head declared by the migration scripts themselves.

    Derived rather than written down: a literal goes stale the moment a
    revision is added and then fails as a regression that never happened.

    Returns:
        The single head revision of the configured script directory.
    """
    head = ScriptDirectory.from_config(Config(str(_ALEMBIC_INI))).get_current_head()
    assert head is not None
    return head


def _assert_head_artifacts(engine: sa.Engine) -> None:
    """Require every compact migration artifact that lacks a direct test."""
    assert _revision(engine) == _head_revision()
    assert "ix_venue_events_cid_event_type" in _indexes(engine, "venue_events")
    assert "ix_trade_commands_client_order_id" in _indexes(engine, "trade_commands")
    assert "ix_peg_status_timestamp" in _indexes(engine, "paired_execution_groups")
    assert "ix_trades_executed_at" in _indexes(engine, "trades")
    assert "stop_price" in _columns(engine, "trade_commands")
    assert {
        "ix_epd_outbox_public_id",
        "ix_epd_outbox_decision_public_id",
        "ix_epd_outbox_ready",
        "ix_epd_outbox_plan_public_id",
    } <= _indexes(engine, "execution_plan_decision_outbox")
    assert {
        "venue_account_observations",
        "venue_account_states",
        "trade_integrity_worklog",
        "trade_integrity_monitor_cursors",
    } <= _tables(engine)
    assert not (_ACTIVE_TICK_INDEXES & _indexes(engine, "ticks"))
    assert "ix_candles_instrument_public_id" not in _indexes(engine, "candles")
    assert "price_basis" in _columns(engine, "candles")
    assert "projection_calc_version" in _columns(engine, "trade_projection_checkpoints")
    assert {
        "ix_trade_integrity_worklog_m1_pending",
        "ix_trade_integrity_worklog_m2_pending",
    } <= _indexes(engine, "trade_integrity_worklog")


@pytest.mark.timeout(60)
def test_sqlite_chain_downgrades_each_unpinned_delta_to_base_and_rebuilds_head(
    tmp_path: Path,
) -> None:
    """Every compact delta disappears at its boundary and base leaves no residue.

    Given: A blank SQLite database migrated through the real Alembic head,
    When: It crosses the otherwise unpinned downgrade boundaries, reaches
        ``base``, and is upgraded through the complete chain again,
    Then: Each migration removes or restores its own artifact, base contains
        only Alembic metadata, and the rebuilt head has the identical required
        schema contract.
    """
    db_url = f"sqlite:///{tmp_path / 'migration-chain.db'}"
    config = _config(db_url)
    engine = sa.create_engine(db_url)

    command.upgrade(config, "head")
    _assert_head_artifacts(engine)

    command.downgrade(config, "0041")
    assert _revision(engine) == "0041"
    assert "projection_calc_version" not in _columns(engine, "trade_projection_checkpoints")

    command.downgrade(config, "0040")
    assert _revision(engine) == "0040"
    assert "trade_integrity_worklog" not in _tables(engine)
    assert "trade_integrity_monitor_cursors" not in _tables(engine)

    command.downgrade(config, "0039")
    assert _revision(engine) == "0039"
    assert "price_basis" not in _columns(engine, "candles")

    command.downgrade(config, "0031")
    assert _revision(engine) == "0031"
    assert _indexes(engine, "ticks") >= _ACTIVE_TICK_INDEXES
    assert "ix_candles_instrument_public_id" in _indexes(engine, "candles")

    command.downgrade(config, "0020")
    assert _revision(engine) == "0020"
    assert "venue_account_observations" not in _tables(engine)
    assert "venue_account_states" not in _tables(engine)

    command.downgrade(config, "0008")
    assert _revision(engine) == "0008"
    assert "execution_plan_decision_outbox" not in _tables(engine)
    assert "stop_price" in _columns(engine, "trade_commands")

    command.downgrade(config, "0007")
    assert _revision(engine) == "0007"
    assert "stop_price" not in _columns(engine, "trade_commands")

    command.downgrade(config, "0006")
    assert _revision(engine) == "0006"
    assert "ix_trades_executed_at" not in _indexes(engine, "trades")

    command.downgrade(config, "0005")
    assert _revision(engine) == "0005"
    assert "ix_peg_status_timestamp" not in _indexes(engine, "paired_execution_groups")

    command.downgrade(config, "0004")
    assert _revision(engine) == "0004"
    assert "ix_trade_commands_client_order_id" not in _indexes(engine, "trade_commands")

    command.downgrade(config, "0003")
    assert _revision(engine) == "0003"
    assert "ix_venue_events_cid_event_type" not in _indexes(engine, "venue_events")

    command.downgrade(config, "base")
    assert _revision(engine) is None
    assert _tables(engine) == {"alembic_version"}

    command.upgrade(config, "head")
    _assert_head_artifacts(engine)
    engine.dispose()


def test_0017_rejects_a_foreign_venue_order_id_but_accepts_idless_evidence() -> None:
    """A shared client id cannot leak a different venue order's fill.

    Given: Two fill rows already matched on client id, wallet, mode, and
        exchange, where the first names another venue order and the second is
        legitimate id-less evidence,
    When: Migration 0017 computes the repair total for the owned order,
    Then: The foreign row contributes neither additive nor legacy truth while
        the id-less row remains usable.
    """
    migration = cast(
        _FillBackfillMigration,
        importlib.import_module("snapper.data.migrations.versions.0017_orders_fill_truth_backfill"),
    )
    rows = [
        ("shared-cid", 9.0, 100.0, 9.0, "foreign-exec", None, "venue-order-b"),
        ("shared-cid", 0.25, 101.0, 0.25, "owned-exec", None, None),
    ]

    additive, legacy = migration._dedup_additive_and_legacy(rows, "venue-order-a")

    assert additive == 0.25
    assert legacy == 0.25


def test_0001_preserves_native_datetime_objects_for_postgresql() -> None:
    """PostgreSQL seed binds retain timezone-aware datetime values.

    Given: The aware active-row sentinel used by revision 0001,
    When: The migration prepares it for PostgreSQL,
    Then: The original datetime object is retained instead of being flattened
        into SQLite's text storage representation.
    """
    module = importlib.import_module("snapper.data.migrations.versions.0001_init")
    migration = cast(_InitialMigration, module)
    value = datetime(9999, 12, 31, 23, 59, 59, tzinfo=UTC)

    assert migration._bind_datetime_literal(value, "postgresql") is value


@pytest.mark.parametrize(
    ("module_name", "required_ddl"),
    (
        (
            "0010_schema_hygiene_batch",
            (
                "ALTER TABLE orders ADD CONSTRAINT ck_orders_mode",
                "ALTER TABLE continuous_contract_configs ALTER COLUMN known_to DROP DEFAULT",
                "ALTER TABLE orders DROP CONSTRAINT ck_orders_mode",
                "ALTER TABLE continuous_contract_configs ALTER COLUMN known_to SET DEFAULT",
            ),
        ),
        (
            "0011_candle_provenance",
            (
                "ALTER TABLE candles ADD COLUMN source VARCHAR(16) DEFAULT 'native' NOT NULL",
                "ALTER TABLE candles ADD CONSTRAINT ck_candle_source",
                "ALTER TABLE candles DROP CONSTRAINT ck_candle_source",
                "ALTER TABLE candles DROP COLUMN source",
            ),
        ),
        (
            "0018_instrument_source_exchange",
            (
                "ALTER TABLE instruments ADD COLUMN source_exchange VARCHAR(32)",
                "ALTER TABLE instruments ADD CONSTRAINT ck_instrument_source_exchange",
                "ALTER TABLE instruments DROP CONSTRAINT ck_instrument_source_exchange",
                "ALTER TABLE instruments DROP COLUMN source_exchange",
            ),
        ),
        (
            "0020_positions_truth_provenance",
            (
                "ALTER TABLE positions ALTER COLUMN average_price DROP NOT NULL",
                "ALTER TABLE positions ADD COLUMN marked_at TIMESTAMP WITH TIME ZONE",
                "UPDATE positions SET average_price = COALESCE(average_price, 0.0)",
                "ALTER TABLE positions ALTER COLUMN average_price SET NOT NULL",
            ),
        ),
    ),
)
def test_postgresql_direct_ddl_branches_round_trip_exact_contracts(
    module_name: str,
    required_ddl: tuple[str, ...],
) -> None:
    """Direct PostgreSQL ALTER paths emit their upgrade and downgrade proofs.

    Args:
        module_name: Numerically named migration module under test.
        required_ddl: Load-bearing PostgreSQL fragments from both directions.
    """
    module = importlib.import_module(f"snapper.data.migrations.versions.{module_name}")
    migration = cast(_ReversibleMigration, module)
    operations, output = _postgresql_operations()

    with patch.object(module, "op", operations):
        migration.upgrade()
        migration.downgrade()

    ddl = output.getvalue()
    for fragment in required_ddl:
        assert fragment in ddl


def test_0012_postgresql_retag_batches_cover_empty_and_wide_id_ranges() -> None:
    """The downgrade retag neither invents work nor skips its final id window.

    Given: First an empty candles table and then ids spanning three 100k windows,
    When: Revision 0012 builds its PostgreSQL retag updates,
    Then: Empty history emits nothing and the populated range emits three
        contiguous half-open updates, including the final boundary id.
    """
    module = importlib.import_module(
        "snapper.data.migrations.versions.0012_candle_source_calculated"
    )
    migration = cast(_CalculatedSourceMigration, module)
    empty_operations = _RetagOperations((None, None))
    with patch.object(module, "op", empty_operations):
        migration._retag_calculated_to_native_batched()
    assert empty_operations.statements == []

    populated_operations = _RetagOperations((5, 200_005))
    with patch.object(module, "op", populated_operations):
        migration._retag_calculated_to_native_batched()
    parameters = [
        cast(TextClause, statement).compile().params
        for statement in populated_operations.statements
    ]
    assert parameters == [
        {"lo": 5, "hi": 100_005},
        {"lo": 100_005, "hi": 200_005},
        {"lo": 200_005, "hi": 300_005},
    ]


def test_0012_postgresql_constraint_swap_retags_before_restoring_old_vocabulary() -> None:
    """PostgreSQL widens and narrows the candle-source CHECK explicitly.

    Given: An offline PostgreSQL operations context with the data retag isolated,
    When: Revision 0012 upgrades and downgrades,
    Then: Both NOT VALID constraints are validated and downgrade invokes the
        batched retag before the old vocabulary can become authoritative.
    """
    module = importlib.import_module(
        "snapper.data.migrations.versions.0012_candle_source_calculated"
    )
    migration = cast(_CalculatedSourceMigration, module)
    operations, output = _postgresql_operations()

    with (
        patch.object(module, "op", operations),
        patch.object(module, "_retag_calculated_to_native_batched") as retag,
    ):
        migration.upgrade()
        migration.downgrade()

    ddl = output.getvalue()
    assert "source IN ('native', 'calculated', 'synthesized')) NOT VALID" in ddl
    assert "VALIDATE CONSTRAINT ck_candle_source" in ddl
    assert "source IN ('native', 'synthesized')) NOT VALID" in ddl
    retag.assert_called_once_with()


def test_0017_postgresql_backfill_pins_utc_and_typed_active_binds() -> None:
    """The PostgreSQL fill repair uses aware values through TIMESTAMPTZ binds.

    Given: An empty legacy PostgreSQL ledger represented by a recording bind,
    When: Revision 0017 runs its data repair,
    Then: UTC is pinned before every read and both active-row queries carry an
        aware datetime through an explicitly timezone-aware bind parameter.
    """
    module = importlib.import_module(
        "snapper.data.migrations.versions.0017_orders_fill_truth_backfill"
    )
    migration = cast(_FillBackfillMigration, module)
    connection = MagicMock()
    connection.dialect.name = "postgresql"
    connection.execute.return_value.fetchall.return_value = []
    operations = MagicMock()
    operations.get_bind.return_value = connection

    with patch.object(module, "op", operations):
        migration.upgrade()

    calls = connection.execute.call_args_list
    assert str(calls[0].args[0]) == "SET LOCAL TIME ZONE 'UTC'"
    assert len(calls) == 4
    for call_index in (2, 3):
        statement = cast(TextClause, calls[call_index].args[0])
        active_bind = statement._bindparams["active"]
        assert isinstance(active_bind.type, sa.DateTime)
        assert active_bind.type.timezone is True
        parameters = cast(dict[str, object], calls[call_index].args[1])
        active = parameters["active"]
        assert isinstance(active, datetime)
        assert active.tzinfo is UTC


def test_0019_postgresql_backlog_is_typed_and_schema_round_trips() -> None:
    """Replay classification cannot depend on the server's inherited timezone.

    Given: A recording PostgreSQL operation and connection surface,
    When: Revision 0019 upgrades and downgrades,
    Then: It pins UTC, classifies backlog through a TIMESTAMPTZ bind, and uses
        direct PostgreSQL DDL for all three provenance columns in both directions.
    """
    module = importlib.import_module(
        "snapper.data.migrations.versions.0019_trade_commands_replay_provenance"
    )
    migration = cast(_ReversibleMigration, module)
    connection = MagicMock()
    connection.dialect.name = "postgresql"
    operations = MagicMock()
    operations.get_bind.return_value = connection

    with patch.object(module, "op", operations):
        migration.upgrade()
        migration.downgrade()

    calls = connection.execute.call_args_list
    assert str(calls[0].args[0]) == "SET LOCAL TIME ZONE 'UTC'"
    backlog = cast(TextClause, calls[1].args[0])
    active_bind = backlog._bindparams["active"]
    assert isinstance(active_bind.type, sa.DateTime)
    assert active_bind.type.timezone is True
    parameters = cast(dict[str, object], calls[1].args[1])
    active = parameters["active"]
    assert isinstance(active, datetime)
    assert active.tzinfo is UTC
    assert operations.add_column.call_count == 3
    operations.create_check_constraint.assert_called_once_with(
        "ck_trade_commands_origin",
        "trade_commands",
        "origin IN ('live', 'replay')",
    )
    assert operations.drop_column.call_count == 3


def test_0031_postgresql_direct_alters_round_trip_the_certified_anchor() -> None:
    """The PostgreSQL anchor migration emits every direct ALTER in both directions.

    Given: An offline PostgreSQL DDL recorder with the online emptiness read isolated,
    When: Revision 0031 upgrades and downgrades,
    Then: Nine evidence columns and the certified checks are added before the
        old checks and columns are restored in reverse.
    """
    module = importlib.import_module(
        "snapper.data.migrations.versions.0031_spot_anchor_venue_cursor"
    )
    migration = cast(_ReversibleMigration, module)
    operations, output = _postgresql_operations()

    with (
        patch.object(module, "op", operations),
        patch.object(module, "_assert_anchor_table_empty"),
    ):
        migration.upgrade()
        migration.downgrade()

    ddl = output.getvalue()
    assert ddl.count("ADD COLUMN") == 9
    assert "ADD COLUMN source_chain_tip VARCHAR(64) NOT NULL" in ddl
    assert "CHECK (source_watermark_kind = 'scope_sequence' AND source_watermark >= 1)" in ddl
    assert "CHECK (source_watermark_kind = 'scope_sequence' AND source_watermark >= 0)" in ddl
    assert ddl.count("DROP COLUMN") == 9


@pytest.mark.parametrize(
    "revision",
    ("0036", "0037", "0039"),
)
def test_trigger_migrations_refuse_offline_rendering_before_any_ddl(revision: str) -> None:
    """Trigger-bearing migrations fail closed when no live dialect bind exists.

    Args:
        revision: Migration whose shared trigger installer requires a live bind.
    """
    suffixes = {
        "0036": "ai_research_persistence",
        "0037": "execution_annulments",
        "0039": "execution_annulment_visibility",
    }
    module = importlib.import_module(
        f"snapper.data.migrations.versions.{revision}_{suffixes[revision]}"
    )
    migration = cast(_ReversibleMigration, module)
    operations, output = _postgresql_operations()

    with (
        patch.object(module, "op", operations),
        pytest.raises(RuntimeError, match=f"migration {revision} requires an online connection"),
    ):
        migration.upgrade()

    assert output.getvalue() == ""


class _SequenceRepairMigration(_ReversibleMigration, Protocol):
    """Typed helper surface specific to revision 0046."""

    def _rename_temporary_sequence(self, table: str) -> None:
        """Restore one table's canonical identity sequence."""


class _ScalarResult:
    """Minimal result exposing a single scalar to a migration probe."""

    def __init__(self, value: object) -> None:
        self._value = value

    def scalar_one(self) -> object:
        """Return the configured scalar."""
        return self._value


class _RegclassConnection:
    """Answer ``to_regclass`` probes from a fixed set of existing relations."""

    def __init__(self, dialect_name: str, existing: set[str]) -> None:
        self.dialect = SimpleNamespace(name=dialect_name)
        self._existing = existing

    def execute(
        self, _statement: object, parameters: dict[str, str] | None = None
    ) -> _ScalarResult:
        """Report whether the probed relation name exists."""
        name = "" if parameters is None else parameters.get("name", "")
        return _ScalarResult(name in self._existing)


class _SequenceOperations:
    """Record the DDL revision 0046 emits against a PostgreSQL bind."""

    def __init__(self, dialect_name: str, existing: set[str]) -> None:
        self._bind = _RegclassConnection(dialect_name, existing)
        self.statements: list[str] = []

    def get_bind(self) -> _RegclassConnection:
        """Return the probe connection."""
        return self._bind

    def execute(self, statement: object) -> None:
        """Record one emitted statement."""
        self.statements.append(str(statement))


class _CarriedCountConnection:
    """Report how many proofs carry a mark, for the revision 0045 guard."""

    def __init__(self, carried: int) -> None:
        self._carried = carried

    def execute(self, _statement: object) -> _ScalarResult:
        """Return the configured carried-proof count."""
        return _ScalarResult(self._carried)


class _CarriedCountOperations:
    """Expose only the bind revision 0045's downgrade guard reads."""

    def __init__(self, carried: int) -> None:
        self._bind = _CarriedCountConnection(carried)

    def get_bind(self) -> _CarriedCountConnection:
        """Return the counting connection."""
        return self._bind


class _CarriedIdentityConnection:
    """Answer revision 0047's active-carried count on a named dialect."""

    def __init__(self, dialect_name: str, carried: int) -> None:
        self.dialect = SimpleNamespace(name=dialect_name)
        self._carried = carried

    def execute(self, _statement: object) -> _ScalarResult:
        """Return the configured active carried-election count."""
        return _ScalarResult(self._carried)


class _CarriedIdentityOperations:
    """Expose only the bind revision 0047's downgrade guard reads."""

    def __init__(self, dialect_name: str, carried: int) -> None:
        self._bind = _CarriedIdentityConnection(dialect_name, carried)

    def get_bind(self) -> _CarriedIdentityConnection:
        """Return the counting connection."""
        return self._bind


def _expected_rename_ddl(table: str) -> tuple[str, str, str]:
    """Return the three statements a canonical sequence restore must emit.

    Args:
        table: Table whose identity sequence is restored.

    Returns:
        Rename, ownership and column-default statements, in emission order.
    """
    temporary = f'"_alembic_tmp_{table}_id_seq"'
    canonical = f'"{table}_id_seq"'
    return (
        f"ALTER SEQUENCE {temporary} RENAME TO {canonical}",
        f'ALTER SEQUENCE {canonical} OWNED BY "{table}".id',
        f"ALTER TABLE \"{table}\" ALTER COLUMN id SET DEFAULT nextval('{canonical}')",
    )


def _sequence_repair() -> _SequenceRepairMigration:
    """Import revision 0046 through its typed helper surface."""
    module = importlib.import_module(
        "snapper.data.migrations.versions.0046_fx_conversion_sequence_repair"
    )
    return cast(_SequenceRepairMigration, module)


def test_sequence_repair_is_inert_off_postgresql() -> None:
    """SQLite has no sequences, so the repair must emit nothing there.

    Given: A bind reporting a non-PostgreSQL dialect
    When: Revision 0046 upgrades
    Then: No DDL is emitted at all
    """
    migration = _sequence_repair()
    operations = _SequenceOperations("sqlite", set())

    with patch.object(migration, "op", operations):
        migration.upgrade()

    assert operations.statements == []


def test_sequence_repair_renames_every_surviving_temporary_sequence() -> None:
    """Both rebuilt tables regain a canonically named, owned sequence.

    Given: A PostgreSQL bind where only the temporary sequences exist
    When: Revision 0046 upgrades
    Then: Each table is renamed, re-owned and repointed, in that order
    """
    migration = _sequence_repair()
    existing = {
        "_alembic_tmp_fx_conversion_elections_id_seq",
        "_alembic_tmp_fx_conversion_proofs_id_seq",
    }
    operations = _SequenceOperations("postgresql", existing)

    with patch.object(migration, "op", operations):
        migration.upgrade()

    assert operations.statements == [
        statement
        for table in ("fx_conversion_elections", "fx_conversion_proofs")
        for statement in _expected_rename_ddl(table)
    ]


def test_sequence_repair_skips_a_database_that_never_lost_the_name() -> None:
    """A database whose 0045 run kept canonical names is left untouched.

    Given: A PostgreSQL bind with no temporary sequence present
    When: One table is repaired
    Then: Nothing is emitted
    """
    migration = _sequence_repair()
    operations = _SequenceOperations("postgresql", set())

    with patch.object(migration, "op", operations):
        migration._rename_temporary_sequence("fx_conversion_proofs")

    assert operations.statements == []


def test_sequence_repair_refuses_to_clobber_an_occupied_canonical_name() -> None:
    """A canonical name already in use is never renamed over.

    Given: Both the temporary and the canonical sequence exist
    When: One table is repaired
    Then: The repair declines rather than colliding
    """
    migration = _sequence_repair()
    operations = _SequenceOperations(
        "postgresql",
        {"_alembic_tmp_fx_conversion_proofs_id_seq", "fx_conversion_proofs_id_seq"},
    )

    with patch.object(migration, "op", operations):
        migration._rename_temporary_sequence("fx_conversion_proofs")

    assert operations.statements == []


def test_sequence_repair_downgrade_keeps_the_canonical_names() -> None:
    """Reintroducing a temporary name has no value, so downgrade is inert.

    Given: Revision 0046 applied
    When: It is reversed
    Then: The reversal completes without emitting DDL
    """
    migration = _sequence_repair()
    operations = _SequenceOperations("postgresql", set())

    with patch.object(migration, "op", operations):
        migration.downgrade()

    assert operations.statements == []


def test_carry_forward_downgrade_refuses_to_orphan_carried_evidence() -> None:
    """Restoring the exact-minute rule must not drop carried proofs.

    Given: A database holding proofs that carry a mark across a gap
    When: Revision 0045 is reversed
    Then: It refuses, naming how much evidence a replay would lose
    """
    module = importlib.import_module(
        "snapper.data.migrations.versions.0045_fx_conversion_carry_forward"
    )
    migration = cast(_ReversibleMigration, module)
    operations = _CarriedCountOperations(3)

    with patch.object(module, "op", operations), pytest.raises(RuntimeError, match="3 proof"):
        migration.downgrade()


def test_carried_identity_downgrade_refuses_to_strand_active_elections() -> None:
    """Narrowing the resolved indexes must not orphan carried elections.

    Given: A database holding active carried elections
    When: Revision 0047 is reversed
    Then: It refuses, naming how many rows would lose their uniqueness key
    """
    module = importlib.import_module(
        "snapper.data.migrations.versions.0047_fx_conversion_carried_identity"
    )
    migration = cast(_ReversibleMigration, module)
    operations = _CarriedIdentityOperations("postgresql", 4)

    with patch.object(module, "op", operations), pytest.raises(RuntimeError, match="4 active"):
        migration.downgrade()


def test_carry_bound_downgrade_refuses_to_strand_wider_carries() -> None:
    """Tightening the carried-minutes ceiling must not orphan wider proofs.

    Given: A database holding proofs carried further than the historical bound
    When: Revision 0048 is reversed
    Then: It refuses, naming how much evidence a replay would lose
    """
    module = importlib.import_module(
        "snapper.data.migrations.versions.0048_fx_conversion_carry_bound_20"
    )
    migration = cast(_ReversibleMigration, module)
    operations = _CarriedCountOperations(2)

    with patch.object(module, "op", operations), pytest.raises(RuntimeError, match="2 proof"):
        migration.downgrade()
