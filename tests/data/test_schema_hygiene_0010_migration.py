"""Tests for the 0010 schema-hygiene migration.

Verifies that ``alembic upgrade head`` on a fresh SQLite database adds
the enum CHECK constraints to orders / trade_commands / executions, the
``ck_bc_pairing_mode`` constraint, and the ``ix_executions_wallet_ts``
index; that those CHECKs reject out-of-vocabulary values while accepting
every value the persistence layer actually writes (including the
dual-era order_type spellings and the WIRE ``canceled`` order status,
but NOT the domain ``cancelled``); that the batch recreation preserved
the pre-existing partial indexes; and that the downgrade removes the
constraints/index and restores the dropped ``known_to`` default.
"""

from collections.abc import Iterator
from pathlib import Path

import pytest
import sqlalchemy as sa
from alembic import command
from alembic.config import Config

ALEMBIC_INI = Path(__file__).resolve().parents[2] / "alembic.ini"
_TS = "2026-06-13 12:00:00.000000"
_ACTIVE = "9999-12-31 23:59:59.000000"


def _make_alembic_config(db_url: str) -> Config:
    """Build an Alembic config pointed at the supplied database URL."""
    cfg = Config(str(ALEMBIC_INI))
    cfg.set_main_option("sqlalchemy.url", db_url)
    return cfg


@pytest.fixture
def migrated_db(tmp_path: Path) -> Iterator[tuple[sa.Engine, Config]]:
    """Provide a SQLite database upgraded through the latest migration."""
    db_path = tmp_path / "hygiene.db"
    db_url = f"sqlite:///{db_path}"
    cfg = _make_alembic_config(db_url)
    command.upgrade(cfg, "head")
    engine = sa.create_engine(db_url, future=True)
    try:
        yield engine, cfg
    finally:
        engine.dispose()


def _insert_order(
    engine: sa.Engine, *, side: str = "buy", order_type: str = "market", status: str = "open"
) -> None:
    """Insert one active orders row, overriding the CHECK'd columns."""
    with engine.begin() as conn:
        conn.execute(
            sa.text(
                "INSERT INTO orders (public_id, instrument_public_id, mode, side, "
                "order_type, size, status, created_at, wallet_public_id, session_id, "
                "sequence_id, timestamp, known_to) VALUES (:pid, 'inst-1', 'live', "
                ":side, :ot, 1.0, :status, :ts, 'wal-1', 'sess-1', 1, :ts, :active)"
            ),
            {
                "pid": f"o-{side}-{order_type}-{status}",
                "side": side,
                "ot": order_type,
                "status": status,
                "ts": _TS,
                "active": _ACTIVE,
            },
        )


def _insert_execution(engine: sa.Engine, *, side: str = "buy", status: str = "filled") -> None:
    """Insert one active executions row, overriding the CHECK'd columns."""
    with engine.begin() as conn:
        conn.execute(
            sa.text(
                "INSERT INTO executions (public_id, order_public_id, side, status, "
                "price, size, fee, fee_asset, wallet_public_id, session_id, sequence_id, "
                "timestamp, known_to) VALUES (:pid, 'o-1', :side, :status, 1.0, 1.0, "
                "0.0, 'USD', 'wal-1', 'sess-1', 1, :ts, :active)"
            ),
            {
                "pid": f"e-{side}-{status}",
                "side": side,
                "status": status,
                "ts": _TS,
                "active": _ACTIVE,
            },
        )


def _insert_trade_command(
    engine: sa.Engine,
    *,
    command_type: str = "create",
    side: str = "buy",
    order_type: str = "market",
    status: str = "created",
    mode: str = "live",
) -> None:
    """Insert one active trade_commands row, overriding the CHECK'd columns."""
    with engine.begin() as conn:
        conn.execute(
            sa.text(
                "INSERT INTO trade_commands (public_id, session_id, sequence_id, "
                "timestamp, known_to, command_type, shard_key, exchange, instrument, "
                "mode, strategy_id, client_order_id, venue_client_id, side, order_type, "
                "quantity, status, created_at, correlation_id, wallet_public_id) VALUES "
                "(:pid, 'sess-1', 1, :ts, :active, :ct, 'kraken.BTC-USD.live', 'kraken', "
                "'BTC-USD', :mode, 'strat-1', :pid, :pid, :side, :ot, 1.0, :status, :ts, "
                "'corr-1', 'wal-1')"
            ),
            {
                "pid": f"tc-{command_type}-{order_type}-{status}",
                "ct": command_type,
                "mode": mode,
                "side": side,
                "ot": order_type,
                "status": status,
                "ts": _TS,
                "active": _ACTIVE,
            },
        )


def _insert_comparison(engine: sa.Engine, *, pairing_mode: str = "auto") -> None:
    """Insert one active backtest_comparisons row with the given pairing_mode."""
    with engine.begin() as conn:
        conn.execute(
            sa.text(
                "INSERT INTO backtest_comparisons (public_id, session_id, sequence_id, "
                "timestamp, known_to, wallet_public_id, run_a_public_id, run_b_public_id, "
                "pairing_mode) VALUES (:pid, 'sess-1', 1, :ts, :active, 'wal-1', 'ra', "
                "'rb', :pm)"
            ),
            {"pid": f"bc-{pairing_mode}", "pm": pairing_mode, "ts": _TS, "active": _ACTIVE},
        )


def _names(engine: sa.Engine, kind: str, like: str) -> set[str]:
    """Return sqlite_master object names of a kind matching a LIKE pattern."""
    with engine.begin() as conn:
        rows = conn.execute(
            sa.text("SELECT name FROM sqlite_master WHERE type = :kind AND name LIKE :like"),
            {"kind": kind, "like": like},
        ).all()
    return {row[0] for row in rows}


class TestSchemaHygiene0010Upgrade:
    """Upgrade adds the constraints, index, and drops the default."""

    def test_executions_wallet_ts_index_created(
        self, migrated_db: tuple[sa.Engine, Config]
    ) -> None:
        """The composite wallet+timestamp index exists after upgrade."""
        engine, _ = migrated_db
        assert "ix_executions_wallet_ts" in _names(engine, "index", "ix_executions_%")

    def test_preexisting_partial_indexes_preserved(
        self, migrated_db: tuple[sa.Engine, Config]
    ) -> None:
        """Batch recreate kept the orders/executions partial indexes intact.

        The CHECK adds recreate these tables on SQLite; this guards
        against the recreate silently dropping the active-row partial
        unique indexes (the failure the plan called out).
        """
        engine, _ = migrated_db
        assert {"uq_orders_client_oid", "uq_orders_exchange_oid", "ix_orders_public_id"} <= _names(
            engine, "index", "%orders%"
        )
        assert {
            "uq_executions_order_exec",
            "uq_executions_order_trade",
            "ix_executions_public_id",
        } <= _names(engine, "index", "%executions%")

    def test_known_to_default_dropped(self, migrated_db: tuple[sa.Engine, Config]) -> None:
        """continuous_contract_configs.known_to no longer carries a server default."""
        engine, _ = migrated_db
        ddl = (
            engine.connect()
            .execute(
                sa.text(
                    "SELECT sql FROM sqlite_master WHERE type='table' AND name='continuous_contract_configs'"
                )
            )
            .scalar_one()
        )
        known_to_line = next(ln for ln in ddl.splitlines() if "known_to" in ln)
        assert "DEFAULT" not in known_to_line.upper()


class TestSchemaHygiene0010Positive:
    """Every value the persistence layer writes survives the CHECKs."""

    @pytest.mark.parametrize("command_type", ["create", "submit", "cancel", "replace"])
    def test_all_command_types_accepted(
        self, migrated_db: tuple[sa.Engine, Config], command_type: str
    ) -> None:
        """All four intentional command_type vocabulary values insert."""
        engine, _ = migrated_db
        _insert_trade_command(engine, command_type=command_type)

    @pytest.mark.parametrize("order_type", ["stop", "trailing-stop", "stop-loss-limit"])
    def test_dual_era_order_types_accepted(
        self, migrated_db: tuple[sa.Engine, Config], order_type: str
    ) -> None:
        """A CORE value, a wire spelling, and a max-width (15-char) value all insert."""
        engine, _ = migrated_db
        _insert_order(engine, order_type=order_type)
        _insert_trade_command(engine, order_type=order_type)

    def test_wire_canceled_order_status_accepted(
        self, migrated_db: tuple[sa.Engine, Config]
    ) -> None:
        """orders.status accepts the WIRE spelling 'canceled'."""
        engine, _ = migrated_db
        _insert_order(engine, status="canceled")

    def test_paper_mode_accepted(self, migrated_db: tuple[sa.Engine, Config]) -> None:
        """orders/trade_commands accept mode 'paper' alongside 'live'."""
        engine, _ = migrated_db
        _insert_trade_command(engine, mode="paper")

    @pytest.mark.parametrize("pairing_mode", ["auto", "manual"])
    def test_pairing_modes_accepted(
        self, migrated_db: tuple[sa.Engine, Config], pairing_mode: str
    ) -> None:
        """backtest_comparisons accepts both runtime pairing modes."""
        engine, _ = migrated_db
        _insert_comparison(engine, pairing_mode=pairing_mode)


class TestSchemaHygiene0010Negative:
    """The CHECKs reject out-of-vocabulary values."""

    def test_domain_cancelled_rejected_on_orders(
        self, migrated_db: tuple[sa.Engine, Config]
    ) -> None:
        """orders.status rejects the DOMAIN spelling 'cancelled' (wire-only column)."""
        engine, _ = migrated_db
        with pytest.raises(sa.exc.IntegrityError):
            _insert_order(engine, status="cancelled")

    def test_bad_order_type_rejected(self, migrated_db: tuple[sa.Engine, Config]) -> None:
        """orders.order_type rejects an unknown type."""
        engine, _ = migrated_db
        with pytest.raises(sa.exc.IntegrityError):
            _insert_order(engine, order_type="bogus")

    def test_bad_command_type_rejected(self, migrated_db: tuple[sa.Engine, Config]) -> None:
        """trade_commands.command_type rejects an unknown command."""
        engine, _ = migrated_db
        with pytest.raises(sa.exc.IntegrityError):
            _insert_trade_command(engine, command_type="amend")

    def test_bad_trade_command_status_rejected(self, migrated_db: tuple[sa.Engine, Config]) -> None:
        """trade_commands.status rejects an unknown status."""
        engine, _ = migrated_db
        with pytest.raises(sa.exc.IntegrityError):
            _insert_trade_command(engine, status="bogus")

    def test_bad_execution_status_rejected(self, migrated_db: tuple[sa.Engine, Config]) -> None:
        """executions.status rejects a non-domain fill status."""
        engine, _ = migrated_db
        with pytest.raises(sa.exc.IntegrityError):
            _insert_execution(engine, status="open")

    def test_bad_pairing_mode_rejected(self, migrated_db: tuple[sa.Engine, Config]) -> None:
        """backtest_comparisons.pairing_mode rejects a stale enum value."""
        engine, _ = migrated_db
        with pytest.raises(sa.exc.IntegrityError):
            _insert_comparison(engine, pairing_mode="walk_forward")

    def test_bad_side_rejected(self, migrated_db: tuple[sa.Engine, Config]) -> None:
        """orders.side rejects a non buy/sell value."""
        engine, _ = migrated_db
        with pytest.raises(sa.exc.IntegrityError):
            _insert_order(engine, side="long")


class TestSchemaHygiene0010Downgrade:
    """Downgrade removes the constraints/index and restores the default."""

    def test_downgrade_round_trip(self, migrated_db: tuple[sa.Engine, Config]) -> None:
        """0010 -> 0009 drops the CHECKs + index and restores the known_to default."""
        engine, cfg = migrated_db
        command.downgrade(cfg, "0009")
        assert "ix_executions_wallet_ts" not in _names(engine, "index", "ix_executions_%")
        orders_ddl = (
            engine.connect()
            .execute(sa.text("SELECT sql FROM sqlite_master WHERE type='table' AND name='orders'"))
            .scalar_one()
        )
        assert "ck_orders_status" not in orders_ddl
        ccc_ddl = (
            engine.connect()
            .execute(
                sa.text(
                    "SELECT sql FROM sqlite_master WHERE type='table' AND name='continuous_contract_configs'"
                )
            )
            .scalar_one()
        )
        known_to_line = next(ln for ln in ccc_ddl.splitlines() if "known_to" in ln)
        assert "DEFAULT" in known_to_line.upper()
        with engine.begin() as conn:
            conn.execute(
                sa.text(
                    "INSERT INTO orders (public_id, instrument_public_id, mode, side, "
                    "order_type, size, status, created_at, wallet_public_id, session_id, "
                    "sequence_id, timestamp, known_to) VALUES ('o-x', 'inst-1', 'live', "
                    "'buy', 'bogus', 1.0, 'cancelled', :ts, 'wal-1', 'sess-1', 1, :ts, :active)"
                ),
                {"ts": _TS, "active": _ACTIVE},
            )
