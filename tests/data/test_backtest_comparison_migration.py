"""Tests for the squashed ``uq_bc_active_pair_per_wallet`` index.

Verifies the partial unique index rejects a second active
``backtest_comparisons`` row for the same
``(wallet_public_id, run_a_public_id, run_b_public_id)`` triplet and
allows cross-wallet duplicates + same-pair replacements after the
active row is SCD2-closed.
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

ALEMBIC_INI = Path(__file__).resolve().parents[2] / "alembic.ini"
KNOWN_TO_MAX_LITERAL = "9999-12-31 23:59:59.000000"


def _make_alembic_config(db_url: str) -> Config:
    """Build an Alembic config pointed at the supplied database URL."""
    cfg = Config(str(ALEMBIC_INI))
    cfg.set_main_option("sqlalchemy.url", db_url)
    return cfg


def _insert_comparison(
    engine: sa.Engine,
    *,
    wallet_public_id: str,
    run_a_public_id: str,
    run_b_public_id: str,
    known_to: str = KNOWN_TO_MAX_LITERAL,
) -> str:
    """Insert one backtest_comparisons row with the supplied SCD2 state."""
    public_id = str(uuid7())
    now = datetime.now(UTC).isoformat(sep=" ")
    with engine.begin() as conn:
        conn.execute(
            sa.text(
                "INSERT INTO backtest_comparisons "
                "(public_id, wallet_public_id, run_a_public_id, run_b_public_id, "
                "pairing_mode, session_id, sequence_id, timestamp, known_to) "
                "VALUES (:public_id, :wallet, :run_a, :run_b, 'manual', "
                ":session, 1, :ts, :known_to)"
            ),
            {
                "public_id": public_id,
                "wallet": wallet_public_id,
                "run_a": run_a_public_id,
                "run_b": run_b_public_id,
                "session": str(uuid7()),
                "ts": now,
                "known_to": known_to,
            },
        )
    return public_id


def _index_exists(engine: sa.Engine) -> bool:
    """Return True iff the comparison active-pair partial index is present."""
    with engine.begin() as conn:
        rows = conn.execute(
            sa.text(
                "SELECT name FROM sqlite_master "
                "WHERE type='index' AND name='uq_bc_active_pair_per_wallet'"
            )
        ).all()
    return len(rows) == 1


@pytest.fixture
def migrated_db(tmp_path: Path) -> Iterator[tuple[sa.Engine, Config]]:
    """Provide a SQLite database upgraded through the latest migration."""
    db_path = tmp_path / "comparison_index.db"
    db_url = f"sqlite:///{db_path}"
    cfg = _make_alembic_config(db_url)
    command.upgrade(cfg, "head")
    engine = sa.create_engine(db_url, future=True)
    try:
        yield engine, cfg
    finally:
        engine.dispose()


class TestBacktestComparisonUniquePairMigration:
    """Upgrade / downgrade / round-trip behaviours for the active-pair index."""

    def test_upgrade_creates_partial_index(self, migrated_db: tuple[sa.Engine, Config]) -> None:
        """Given a freshly-migrated DB, the partial index is present.

        Given:
            ``alembic upgrade head`` on a fresh SQLite DB,

        When:
            Querying sqlite_master for the index name,

        Then:
            Exactly one row is returned for
            ``uq_bc_active_pair_per_wallet``.
        """
        engine, _ = migrated_db
        assert _index_exists(engine)

    def test_duplicate_active_pair_same_wallet_is_rejected(
        self, migrated_db: tuple[sa.Engine, Config]
    ) -> None:
        """Two active comparisons for the same wallet + pair → IntegrityError.

        Given:
            An active comparison row for (wallet W, run A, run B) with
            ``known_to=KNOWN_TO_MAX``,

        When:
            A second insert with the same triplet and the same
            ``known_to`` is attempted,

        Then:
            The second insert raises ``IntegrityError`` — the partial
            unique index enforces at most one active comparison per
            (wallet, normalised pair) and closes the race window
            between the SELECT-then-INSERT idempotency check and the
            commit of two concurrent POSTs.
        """
        engine, _ = migrated_db
        wallet = str(uuid7())
        run_a = str(uuid7())
        run_b = str(uuid7())
        _insert_comparison(
            engine,
            wallet_public_id=wallet,
            run_a_public_id=run_a,
            run_b_public_id=run_b,
        )
        with pytest.raises(sa.exc.IntegrityError):
            _insert_comparison(
                engine,
                wallet_public_id=wallet,
                run_a_public_id=run_a,
                run_b_public_id=run_b,
            )

    def test_cross_wallet_same_pair_allowed(self, migrated_db: tuple[sa.Engine, Config]) -> None:
        """Same normalised pair on different wallets → both inserts succeed.

        Given:
            The index is partial on ``wallet_public_id`` as the leading
            column,

        When:
            Two inserts carry the same (run_a, run_b) but differ on
            ``wallet_public_id``,

        Then:
            Both succeed — comparisons are wallet-scoped and the
            index deliberately does not prevent cross-tenant dupes
            (each wallet still sees its own comparison independently).
        """
        engine, _ = migrated_db
        run_a = str(uuid7())
        run_b = str(uuid7())
        _insert_comparison(
            engine,
            wallet_public_id=str(uuid7()),
            run_a_public_id=run_a,
            run_b_public_id=run_b,
        )
        _insert_comparison(
            engine,
            wallet_public_id=str(uuid7()),
            run_a_public_id=run_a,
            run_b_public_id=run_b,
        )

    def test_closed_row_does_not_block_active_replacement(
        self, migrated_db: tuple[sa.Engine, Config]
    ) -> None:
        """Closed (non-active) row on the same pair → new active insert allowed.

        Given:
            A row that has been SCD2-closed (``known_to`` set to a past
            timestamp, not KNOWN_TO_MAX),

        When:
            A fresh active row for the same wallet + pair is inserted,

        Then:
            The insert succeeds — the partial index only covers
            active rows, so historical versions are free to coexist
            with a new active row.
        """
        engine, _ = migrated_db
        wallet = str(uuid7())
        run_a = str(uuid7())
        run_b = str(uuid7())
        _insert_comparison(
            engine,
            wallet_public_id=wallet,
            run_a_public_id=run_a,
            run_b_public_id=run_b,
            known_to="2020-01-01 00:00:00.000000",
        )
        _insert_comparison(
            engine,
            wallet_public_id=wallet,
            run_a_public_id=run_a,
            run_b_public_id=run_b,
        )

    def test_downgrade_to_0001_keeps_index_and_rejects_duplicates(
        self, migrated_db: tuple[sa.Engine, Config]
    ) -> None:
        """Downgrade to revision 0001 is a no-op in the squashed chain."""
        engine, cfg = migrated_db
        command.downgrade(cfg, "0001")
        assert _index_exists(engine)
        wallet = str(uuid7())
        run_a = str(uuid7())
        run_b = str(uuid7())
        _insert_comparison(
            engine,
            wallet_public_id=wallet,
            run_a_public_id=run_a,
            run_b_public_id=run_b,
        )
        with pytest.raises(sa.exc.IntegrityError):
            _insert_comparison(
                engine,
                wallet_public_id=wallet,
                run_a_public_id=run_a,
                run_b_public_id=run_b,
            )

    def test_round_trip_restores_enforcement(self, migrated_db: tuple[sa.Engine, Config]) -> None:
        """Given upgrade -> downgrade -> upgrade, the index is reinstated.

        Given:
            A DB that has been through upgrade, then
            ``downgrade 0001``, then ``upgrade head``,

        When:
            Two active comparisons with the same triplet are inserted,

        Then:
            The second raises ``IntegrityError`` — the index was
            re-created by the second upgrade.
        """
        engine, cfg = migrated_db
        command.downgrade(cfg, "0001")
        command.upgrade(cfg, "head")
        assert _index_exists(engine)
        wallet = str(uuid7())
        run_a = str(uuid7())
        run_b = str(uuid7())
        _insert_comparison(
            engine,
            wallet_public_id=wallet,
            run_a_public_id=run_a,
            run_b_public_id=run_b,
        )
        with pytest.raises(sa.exc.IntegrityError):
            _insert_comparison(
                engine,
                wallet_public_id=wallet,
                run_a_public_id=run_a,
                run_b_public_id=run_b,
            )
