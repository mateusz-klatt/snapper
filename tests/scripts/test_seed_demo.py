"""Tests for the ``scripts/seed_demo.py`` one-shot demo data seeder.

Bootstraps a real schema via ``alembic upgrade head`` against an
ephemeral SQLite database, seeds the minimal pre-requisites that a
production ``make migrate-dev`` would produce (default operator,
paper wallet, four instruments), and runs the seeder end-to-end.

Tests cover:

- The :func:`_sync_db_url` URL transformer.
- The happy path that inserts 4 orders, 2 executions, 2 positions,
  4 backtest runs (BTC RSI/MACD perp + CLM6 RSI + GCM6 MACD daily),
  and one pending CLM6 ``ai_review``.
- Idempotency: re-running with existing orders is a no-op.
- Hard-fail branches when required pre-reqs (paper wallet, default
  operator, BTC-USD-PERP, ETH-USD-PERP) are missing.
- Optional-instrument branches: CLM6/GCM6 absence skips their
  backtests; CLM6 absence also skips the AI-review seed.
"""

from collections.abc import Iterator
from datetime import UTC
from datetime import datetime
from pathlib import Path
from uuid import uuid7

import pytest
import sqlalchemy as sa
from alembic import command
from alembic.config import Config
from sqlalchemy import text
from sqlalchemy.engine import Connection

from scripts import seed_demo

ALEMBIC_INI = Path(__file__).resolve().parents[2] / "alembic.ini"
KNOWN_TO_MAX_STR = "9999-12-31 23:59:59.000000"


def _make_alembic_config(db_url: str) -> Config:
    """Build an Alembic config pointed at the supplied database URL."""
    cfg = Config(str(ALEMBIC_INI))
    cfg.set_main_option("sqlalchemy.url", db_url)
    return cfg


@pytest.fixture
def migrated_db(tmp_path: Path) -> Iterator[tuple[sa.Engine, str]]:
    """Provide a SQLite database upgraded through the latest migration."""
    db_path = tmp_path / "seed_demo.db"
    db_url = f"sqlite:///{db_path}"
    cfg = _make_alembic_config(db_url)
    command.upgrade(cfg, "head")
    engine = sa.create_engine(db_url, future=True)
    try:
        yield engine, db_url
    finally:
        engine.dispose()


def _seed_paper_wallet(conn: Connection) -> str:
    """Insert a paper wallet matching the ``_lookup_paper_wallet`` predicate."""
    public_id = str(uuid7())
    conn.execute(
        text(
            "INSERT INTO wallets (public_id, label, description, is_paper, "
            "timestamp, known_to, session_id, sequence_id) "
            "VALUES (:pid, 'paper', NULL, 1, :ts, :ka, 's', 1)"
        ),
        {"pid": public_id, "ts": str(datetime.now(UTC)), "ka": KNOWN_TO_MAX_STR},
    )
    return public_id


def _seed_default_operator(conn: Connection) -> str:
    """Insert a default operator matching the ``_lookup_operator`` predicate."""
    public_id = str(uuid7())
    conn.execute(
        text(
            "INSERT INTO operators (public_id, label, description, "
            "timestamp, known_to, session_id, sequence_id) "
            "VALUES (:pid, 'default', NULL, :ts, :ka, 's', 1)"
        ),
        {"pid": public_id, "ts": str(datetime.now(UTC)), "ka": KNOWN_TO_MAX_STR},
    )
    return public_id


def _seed_instrument(
    conn: Connection,
    *,
    native_symbol: str,
    base: str,
    quote: str | None,
    exchange: str,
    asset_type: str = "crypto",
) -> str:
    """Insert symbol + instrument rows so ``_lookup_instrument`` can find them.

    Uses the SQLite literal known_to format the production seeder
    embeds (``9999-12-31 23:59:59.000000``) so the active-row partial
    index predicate matches.
    """
    symbol_pid = str(uuid7())
    now_iso = str(datetime.now(UTC))
    conn.execute(
        text(
            "INSERT INTO symbols (public_id, native_symbol, base, quote, asset_type, "
            "created_at, session_id, sequence_id, timestamp, known_to) "
            "VALUES (:spid, :sym, :base, :quote, :asset, :ts, 's', 1, :ts, :ka)"
        ),
        {
            "spid": symbol_pid,
            "sym": native_symbol,
            "base": base,
            "quote": quote,
            "asset": asset_type,
            "ts": now_iso,
            "ka": KNOWN_TO_MAX_STR,
        },
    )
    inst_pid = str(uuid7())
    conn.execute(
        text(
            "INSERT INTO instruments (public_id, symbol_public_id, exchange, "
            "requires_ai_review, session_id, sequence_id, timestamp, known_to) "
            "VALUES (:ipid, :spid, :ex, 0, 's', 1, :ts, :ka)"
        ),
        {
            "ipid": inst_pid,
            "spid": symbol_pid,
            "ex": exchange,
            "ts": now_iso,
            "ka": KNOWN_TO_MAX_STR,
        },
    )
    return inst_pid


def _seed_required_instruments(conn: Connection) -> None:
    """Insert BTC-USD-PERP + ETH-USD-PERP on kraken_futures (the hard-required pair)."""
    _seed_instrument(
        conn,
        native_symbol="BTC-USD-PERP",
        base="BTC",
        quote="USD",
        exchange="kraken_futures",
    )
    _seed_instrument(
        conn,
        native_symbol="ETH-USD-PERP",
        base="ETH",
        quote="USD",
        exchange="kraken_futures",
    )


def _seed_optional_commodity_instruments(conn: Connection) -> None:
    """Insert CLM6-NYMEX + GCM6-COMEX on kraken_equities (the optional commodity pair)."""
    _seed_instrument(
        conn,
        native_symbol="CLM6-NYMEX",
        base="CL",
        quote=None,
        exchange="kraken_equities",
        asset_type="commodity",
    )
    _seed_instrument(
        conn,
        native_symbol="GCM6-COMEX",
        base="GC",
        quote=None,
        exchange="kraken_equities",
        asset_type="commodity",
    )


class TestSyncDbUrl:
    """Tests for the :func:`seed_demo._sync_db_url` URL transformer."""

    def test_converts_aiosqlite_to_sqlite(self) -> None:
        """``sqlite+aiosqlite://`` becomes ``sqlite://``.

        Given: an async-driver SQLite URL,
        When: ``_sync_db_url`` is called,
        Then: the driver suffix is stripped so a sync ``create_engine``
        can use the same database file.
        """
        result = seed_demo._sync_db_url("sqlite+aiosqlite:///./data/snapper.db")
        assert result == "sqlite:///./data/snapper.db"

    def test_passes_plain_sqlite_through(self) -> None:
        """A plain ``sqlite://`` URL is returned unchanged.

        Given: a URL already using the sync SQLite driver,
        When: ``_sync_db_url`` is called,
        Then: the URL is returned as-is.
        """
        url = "sqlite:///./data/snapper.db"
        assert seed_demo._sync_db_url(url) == url

    def test_passes_postgres_through(self) -> None:
        """Non-SQLite URLs are returned unchanged.

        Given: a Postgres URL (production deployments switch via DB_URL),
        When: ``_sync_db_url`` is called,
        Then: no transformation is applied.
        """
        url = "postgresql+psycopg2://user:pass@host/db"
        assert seed_demo._sync_db_url(url) == url


class TestMain:
    """End-to-end tests for :func:`seed_demo.main`."""

    def test_full_demo_set_inserted(
        self,
        migrated_db: tuple[sa.Engine, str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Happy path inserts the full demo trajectory.

        Given: an empty migrated DB seeded with paper wallet, default
            operator, BTC/ETH perps and CLM6/GCM6 commodity instruments,
        When: ``main()`` runs,
        Then: 4 orders, 2 executions, 2 positions, 4 backtest runs,
            and one pending ``ai_review`` are inserted; the function
            returns 0.
        """
        engine, db_url = migrated_db
        with engine.begin() as conn:
            _seed_paper_wallet(conn)
            _seed_default_operator(conn)
            _seed_required_instruments(conn)
            _seed_optional_commodity_instruments(conn)

        monkeypatch.setenv("DB_URL", db_url)
        rc = seed_demo.main()
        assert rc == 0

        with engine.connect() as conn:
            assert conn.execute(text("SELECT COUNT(*) FROM orders")).scalar() == 4
            assert conn.execute(text("SELECT COUNT(*) FROM executions")).scalar() == 2
            assert conn.execute(text("SELECT COUNT(*) FROM positions")).scalar() == 2
            assert conn.execute(text("SELECT COUNT(*) FROM backtest_runs")).scalar() == 4
            assert conn.execute(text("SELECT COUNT(*) FROM ai_reviews")).scalar() == 1
            assert conn.execute(text("SELECT COUNT(*) FROM ai_delegates")).scalar() == 1
            assert conn.execute(text("SELECT COUNT(*) FROM ai_review_events")).scalar() == 2

    def test_idempotent_skip_when_orders_exist(
        self,
        migrated_db: tuple[sa.Engine, str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Re-running with existing orders is a no-op.

        Given: a DB with the pre-reqs seeded AND one order row already present,
        When: ``main()`` runs,
        Then: the function returns 0 without inserting new rows
            (the screenshot tool is intentionally idempotent so re-runs
            after an aborted demo don't double up the dataset).
        """
        engine, db_url = migrated_db
        with engine.begin() as conn:
            wallet = _seed_paper_wallet(conn)
            operator = _seed_default_operator(conn)
            _seed_required_instruments(conn)
            inst_pid = str(uuid7())
            conn.execute(
                text(
                    "INSERT INTO orders "
                    "(public_id, instrument_public_id, mode, wallet_public_id, "
                    " operator_public_id, client_order_id, exchange_order_id, "
                    " created_at, updated_at, side, order_type, price, size, "
                    " status, time_in_force, filled_size, average_price, error, "
                    " leverage, reduce_only, plan_public_id, "
                    " timestamp, known_to, session_id, sequence_id) "
                    "VALUES (:pid, :inst, 'paper', :wal, :op, "
                    " 'pre-existing', NULL, :ts, :ts, 'buy', 'market', "
                    " 100.0, 1.0, 'filled', 'gtc', 1.0, 100.0, NULL, "
                    " NULL, 0, NULL, "
                    " :ts, :ka, 's', 1)"
                ),
                {
                    "pid": str(uuid7()),
                    "inst": inst_pid,
                    "wal": wallet,
                    "op": operator,
                    "ts": str(datetime.now(UTC)),
                    "ka": KNOWN_TO_MAX_STR,
                },
            )

        monkeypatch.setenv("DB_URL", db_url)
        rc = seed_demo.main()
        assert rc == 0

        with engine.connect() as conn:
            assert conn.execute(text("SELECT COUNT(*) FROM orders")).scalar() == 1
            assert conn.execute(text("SELECT COUNT(*) FROM executions")).scalar() == 0
            assert conn.execute(text("SELECT COUNT(*) FROM positions")).scalar() == 0
            assert conn.execute(text("SELECT COUNT(*) FROM backtest_runs")).scalar() == 0

    def test_raises_when_paper_wallet_missing(
        self,
        migrated_db: tuple[sa.Engine, str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Missing paper wallet causes a hard fail with a migrate-dev hint.

        Given: a DB with operator + instruments but no paper wallet,
        When: ``main()`` runs,
        Then: ``RuntimeError`` is raised pointing at ``make migrate-dev``.
        """
        engine, db_url = migrated_db
        with engine.begin() as conn:
            _seed_default_operator(conn)
            _seed_required_instruments(conn)
        monkeypatch.setenv("DB_URL", db_url)
        with pytest.raises(RuntimeError, match="paper wallet or default operator"):
            seed_demo.main()

    def test_raises_when_default_operator_missing(
        self,
        migrated_db: tuple[sa.Engine, str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Missing default operator causes a hard fail.

        Given: a DB with paper wallet + instruments but no operator,
        When: ``main()`` runs,
        Then: ``RuntimeError`` is raised pointing at ``make migrate-dev``.
        """
        engine, db_url = migrated_db
        with engine.begin() as conn:
            _seed_paper_wallet(conn)
            _seed_required_instruments(conn)
        monkeypatch.setenv("DB_URL", db_url)
        with pytest.raises(RuntimeError, match="paper wallet or default operator"):
            seed_demo.main()

    def test_raises_when_btc_perp_missing(
        self,
        migrated_db: tuple[sa.Engine, str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Missing BTC-USD-PERP causes a hard fail with a run-static hint.

        Given: a DB with wallet + operator and only ETH-USD-PERP,
        When: ``main()`` runs,
        Then: ``RuntimeError`` is raised pointing at ``make run-static``.
        """
        engine, db_url = migrated_db
        with engine.begin() as conn:
            _seed_paper_wallet(conn)
            _seed_default_operator(conn)
            _seed_instrument(
                conn,
                native_symbol="ETH-USD-PERP",
                base="ETH",
                quote="USD",
                exchange="kraken_futures",
            )
        monkeypatch.setenv("DB_URL", db_url)
        with pytest.raises(RuntimeError, match="required instruments not found"):
            seed_demo.main()

    def test_raises_when_eth_perp_missing(
        self,
        migrated_db: tuple[sa.Engine, str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Missing ETH-USD-PERP causes a hard fail.

        Given: a DB with wallet + operator and only BTC-USD-PERP,
        When: ``main()`` runs,
        Then: ``RuntimeError`` is raised pointing at ``make run-static``.
        """
        engine, db_url = migrated_db
        with engine.begin() as conn:
            _seed_paper_wallet(conn)
            _seed_default_operator(conn)
            _seed_instrument(
                conn,
                native_symbol="BTC-USD-PERP",
                base="BTC",
                quote="USD",
                exchange="kraken_futures",
            )
        monkeypatch.setenv("DB_URL", db_url)
        with pytest.raises(RuntimeError, match="required instruments not found"):
            seed_demo.main()

    def test_omits_optional_commodity_branches(
        self,
        migrated_db: tuple[sa.Engine, str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Without CLM6 / GCM6 the seed skips commodity backtests + AI review.

        Given: a DB with wallet + operator + the required BTC/ETH perps but
            no CLM6-NYMEX or GCM6-COMEX,
        When: ``main()`` runs,
        Then: orders / executions / positions match the happy path
            (the BTC/ETH legs are independent of commodities), only the
            two BTC backtest runs are inserted, and no ``ai_review`` row
            is seeded because the AI review path is gated on CLM6.
        """
        engine, db_url = migrated_db
        with engine.begin() as conn:
            _seed_paper_wallet(conn)
            _seed_default_operator(conn)
            _seed_required_instruments(conn)
        monkeypatch.setenv("DB_URL", db_url)
        rc = seed_demo.main()
        assert rc == 0
        with engine.connect() as conn:
            assert conn.execute(text("SELECT COUNT(*) FROM orders")).scalar() == 4
            assert conn.execute(text("SELECT COUNT(*) FROM executions")).scalar() == 2
            assert conn.execute(text("SELECT COUNT(*) FROM positions")).scalar() == 2
            assert conn.execute(text("SELECT COUNT(*) FROM backtest_runs")).scalar() == 2
            assert conn.execute(text("SELECT COUNT(*) FROM ai_reviews")).scalar() == 0
            assert conn.execute(text("SELECT COUNT(*) FROM ai_delegates")).scalar() == 0

    def test_seeds_clm6_branches_when_only_gcm6_missing(
        self,
        migrated_db: tuple[sa.Engine, str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """CLM6 present + GCM6 missing seeds 3 backtests and the AI review.

        Given: a DB with wallet + operator + BTC/ETH/CLM6 (no GCM6),
        When: ``main()`` runs,
        Then: backtests cover BTC RSI + BTC MACD + CLM6 RSI (no Gold MACD)
            and the AI review gated on CLM6 is inserted.
        """
        engine, db_url = migrated_db
        with engine.begin() as conn:
            _seed_paper_wallet(conn)
            _seed_default_operator(conn)
            _seed_required_instruments(conn)
            _seed_instrument(
                conn,
                native_symbol="CLM6-NYMEX",
                base="CL",
                quote=None,
                exchange="kraken_equities",
                asset_type="commodity",
            )
        monkeypatch.setenv("DB_URL", db_url)
        rc = seed_demo.main()
        assert rc == 0
        with engine.connect() as conn:
            assert conn.execute(text("SELECT COUNT(*) FROM backtest_runs")).scalar() == 3
            assert conn.execute(text("SELECT COUNT(*) FROM ai_reviews")).scalar() == 1
