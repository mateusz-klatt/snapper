"""Tests for the 0017 orders fill-truth backfill migration.

Seeds a pre-backfill database at revision 0016 with orders whose fill
truth lives only in ``executions``/``venue_events``, upgrades to head,
and verifies: venue cumulative wins over the execution sum, the VWAP
is derived only on a sum match, evidence-less orders stay untouched,
and closed historical versions are never rewritten.
"""

from collections.abc import Iterator
from pathlib import Path

import pytest
import sqlalchemy as sa
from alembic import command
from alembic.config import Config

ALEMBIC_INI = Path(__file__).resolve().parents[2] / "alembic.ini"
_ACTIVE = "9999-12-31 23:59:59.000000"
_TS = "2026-07-11 08:00:00.000000"


def _seed_identity(engine: sa.Engine) -> None:
    """Seed the symbol + instrument rows the identity join requires.

    Args:
        engine: Engine bound to the migrated database.
    """
    with engine.begin() as conn:
        conn.execute(
            sa.text(
                "INSERT INTO symbols (public_id, native_symbol, base, quote, "
                "asset_type, created_at, session_id, sequence_id, timestamp, "
                "known_to) VALUES ('sym-1', 'BTC-USD', 'BTC', 'USD', 'crypto', "
                ":ts, 'sess-1', 1, :ts, :kt)"
            ),
            {"ts": _TS, "kt": _ACTIVE},
        )
        conn.execute(
            sa.text(
                "INSERT INTO instruments (public_id, symbol_public_id, exchange, "
                "requires_ai_review, session_id, sequence_id, timestamp, "
                "known_to) VALUES ('inst-1', 'sym-1', 'kraken', 0, 'sess-1', 1, "
                ":ts, :kt)"
            ),
            {"ts": _TS, "kt": _ACTIVE},
        )


def _make_alembic_config(db_url: str) -> Config:
    """Build an Alembic config pointed at the supplied database URL.

    Args:
        db_url: SQLAlchemy URL of the throwaway SQLite database.

    Returns:
        Configured :class:`Config` bound to ``db_url``.
    """
    cfg = Config(str(ALEMBIC_INI))
    cfg.set_main_option("sqlalchemy.url", db_url)
    return cfg


def _insert_order(
    engine: sa.Engine,
    *,
    public_id: str,
    client_order_id: str,
    known_to: str = _ACTIVE,
    filled_size: float = 0.0,
) -> None:
    """Insert one order version row via literal SQL.

    Args:
        engine: Engine bound to the migrated database.
        public_id: Stable order identity.
        client_order_id: Venue client id (venue_events scope key).
        known_to: SCD2 close marker (active by default).
        filled_size: Pre-backfill filled size.
    """
    with engine.begin() as conn:
        conn.execute(
            sa.text(
                "INSERT INTO orders (public_id, instrument_public_id, mode, "
                "wallet_public_id, client_order_id, created_at, updated_at, "
                "side, order_type, size, filled_size, status, session_id, "
                "sequence_id, timestamp, known_to) VALUES "
                "(:pid, 'inst-1', 'live', 'wal-1', :cid, :ts, :ts, 'buy', "
                "'market', 1.0, :filled, 'open', 'sess-1', 1, :ts, :kt)"
            ),
            {
                "pid": public_id,
                "cid": client_order_id,
                "ts": _TS,
                "filled": filled_size,
                "kt": known_to,
            },
        )


def _insert_execution(
    engine: sa.Engine,
    *,
    order_public_id: str,
    price: float,
    size: float,
    seq: int,
    known_to: str = _ACTIVE,
) -> None:
    """Insert one execution delta row via literal SQL.

    Args:
        engine: Engine bound to the migrated database.
        order_public_id: Order the fill belongs to.
        price: Delta fill price.
        size: Delta fill size.
        seq: Bus sequence uniquifier.
        known_to: SCD2 close marker (active by default).
    """
    with engine.begin() as conn:
        conn.execute(
            sa.text(
                "INSERT INTO executions (public_id, order_public_id, timestamp, "
                "side, status, price, size, fee, fee_asset, wallet_public_id, "
                "session_id, sequence_id, known_to) VALUES "
                "(:pid, :opid, :ts, 'buy', 'partial', :price, :size, 0.0, "
                "'USD', 'wal-1', 'sess-1', :seq, :kt)"
            ),
            {
                "pid": f"exec-{order_public_id}-{seq}",
                "opid": order_public_id,
                "ts": _TS,
                "price": price,
                "size": size,
                "seq": seq,
                "kt": known_to,
            },
        )


def _insert_fill_event(
    engine: sa.Engine,
    *,
    client_order_id: str,
    seq: int,
    fill_size: float | None = None,
    cum: float | None = None,
    exec_id: str | None = None,
    trade_id: str | None = None,
    wallet: str = "wal-1",
    price: float | None = None,
) -> None:
    """Insert one durable fill_observed venue event.

    Args:
        engine: Engine bound to the migrated database.
        client_order_id: Scope key linking the event to its order.
        seq: Bus sequence uniquifier.
        fill_size: Additive delta recorded on the row.
        cum: Venue-reported cumulative (legacy evidence).
        exec_id: Venue execution identity (dedup key).
        trade_id: Venue trade identity (alternate dedup key).
        wallet: Owning wallet (identity scope).
        price: Fill price recorded on the row (fallback dedup key).
    """
    with engine.begin() as conn:
        conn.execute(
            sa.text(
                "INSERT INTO venue_events (public_id, event_type, shard_key, "
                "exchange, instrument, mode, client_order_id, fill_size, "
                "fill_price, cum_fill_size, exec_id, trade_id, "
                "wallet_public_id, received_at, session_id, sequence_id, "
                "timestamp, known_to) "
                "VALUES (:pid, 'fill_observed', 'kraken.BTC-USD.live', "
                "'kraken', 'BTC-USD', 'live', :cid, :fsize, :fprice, :cum, "
                ":eid, :tid, :wallet, :ts, 'sess-1', :seq, :ts, :kt)"
            ),
            {
                "pid": f"ve-{client_order_id}-{seq}",
                "cid": client_order_id,
                "fsize": fill_size,
                "fprice": price,
                "cum": cum,
                "eid": exec_id,
                "tid": trade_id,
                "wallet": wallet,
                "ts": _TS,
                "seq": seq,
                "kt": _ACTIVE,
            },
        )


def _fill_columns(engine: sa.Engine, public_id: str, known_to: str = _ACTIVE) -> sa.Row:
    """Read back (filled_size, average_price) for one order version.

    Args:
        engine: Engine bound to the migrated database.
        public_id: Stable order identity.
        known_to: Version selector (active by default).

    Returns:
        Row of the two fill columns.
    """
    with engine.begin() as conn:
        return conn.execute(
            sa.text(
                "SELECT filled_size, average_price FROM orders "
                "WHERE public_id = :pid AND known_to = :kt"
            ),
            {"pid": public_id, "kt": known_to},
        ).one()


@pytest.fixture
def pre_backfill_db(tmp_path: Path) -> Iterator[tuple[sa.Engine, Config]]:
    """Provide a SQLite database migrated to 0016 (pre-backfill).

    Args:
        tmp_path: Pytest-provided temporary directory.

    Yields:
        Tuple of engine bound to the database and the config.
    """
    db_path = tmp_path / "fill_backfill.db"
    db_url = f"sqlite:///{db_path}"
    cfg = _make_alembic_config(db_url)
    command.upgrade(cfg, "0016")
    engine = sa.create_engine(db_url, future=True)
    try:
        yield engine, cfg
    finally:
        engine.dispose()


class TestOrdersFillBackfillMigration:
    """Repair behaviours of the 0017 data migration."""

    def test_additive_dedup_sum_wins_and_vwap_matches(
        self, pre_backfill_db: tuple[sa.Engine, Config]
    ) -> None:
        """Deduplicated fill_observed deltas set the cumulative + VWAP.

        Given: two execution deltas (0.3 @ 100, 0.1 @ 104), matching
            fill_observed rows plus one exec_id redelivery AND one
            trade-id-only redelivery (both must count once), and a
            LOWER legacy cumulative (0.3 — the delta-only fabrication),
        When: the backfill runs,
        Then: filled_size uses the additive dedup sum 0.4 (not the
            under-reporting legacy cum) and average_price the VWAP.
        """
        engine, cfg = pre_backfill_db
        _seed_identity(engine)
        _insert_order(engine, public_id="ord-1", client_order_id="cid-1")
        _insert_execution(engine, order_public_id="ord-1", price=100.0, size=0.3, seq=1)
        _insert_execution(engine, order_public_id="ord-1", price=104.0, size=0.1, seq=2)
        _insert_fill_event(
            engine, client_order_id="cid-1", seq=3, fill_size=0.3, cum=0.3, exec_id="ex-a"
        )
        _insert_fill_event(
            engine,
            client_order_id="cid-1",
            seq=4,
            fill_size=0.1,
            cum=0.3,
            exec_id="ex-b",
            trade_id="t-b",
        )
        _insert_fill_event(
            engine, client_order_id="cid-1", seq=5, fill_size=0.1, cum=0.3, exec_id="ex-b"
        )
        _insert_fill_event(engine, client_order_id="cid-1", seq=6, fill_size=0.1, trade_id="t-b")
        command.upgrade(cfg, "head")
        row = _fill_columns(engine, "ord-1")
        assert row[0] == pytest.approx(0.4)
        assert row[1] == pytest.approx(101.0)

    def test_missed_execution_keeps_null_average(
        self, pre_backfill_db: tuple[sa.Engine, Config]
    ) -> None:
        """A cumulative/exec-sum mismatch backfills size but not average.

        Given: fill_observed evidence summing to 0.4 but only one
            persisted execution delta (0.3 — a publish failure skipped
            the executions write),
        When: the backfill runs,
        Then: filled_size uses the durable additive truth and
            average_price stays NULL — a skewed VWAP is never written.
        """
        engine, cfg = pre_backfill_db
        _seed_identity(engine)
        _insert_order(engine, public_id="ord-2", client_order_id="cid-2")
        _insert_execution(engine, order_public_id="ord-2", price=100.0, size=0.3, seq=1)
        _insert_fill_event(engine, client_order_id="cid-2", seq=2, fill_size=0.3, exec_id="ex-a")
        _insert_fill_event(engine, client_order_id="cid-2", seq=3, fill_size=0.1, exec_id="ex-b")
        command.upgrade(cfg, "head")
        row = _fill_columns(engine, "ord-2")
        assert row[0] == pytest.approx(0.4)
        assert row[1] is None

    def test_legacy_cum_used_without_fill_observed_rows(
        self, pre_backfill_db: tuple[sa.Engine, Config]
    ) -> None:
        """Pre-fill_observed history falls back to MAX(cum_fill_size).

        Given: only a fill_observed row carrying cum_fill_size with a
            NULL fill_size (pre-delta-recording history) and no
            executions,
        When: the backfill runs,
        Then: filled_size uses the legacy cumulative with NULL average.
        """
        engine, cfg = pre_backfill_db
        _seed_identity(engine)
        _insert_order(engine, public_id="ord-3", client_order_id="cid-3")
        _insert_fill_event(engine, client_order_id="cid-3", seq=1, cum=0.7, exec_id="ex-l")
        command.upgrade(cfg, "head")
        row = _fill_columns(engine, "ord-3")
        assert row[0] == pytest.approx(0.7)
        assert row[1] is None

    def test_execution_sum_used_without_venue_evidence(
        self, pre_backfill_db: tuple[sa.Engine, Config]
    ) -> None:
        """Orders evidenced only by executions use the delta sum + VWAP.

        Given: two execution deltas and no venue events at all,
        When: the backfill runs,
        Then: filled_size is the delta sum and average_price the VWAP.
        """
        engine, cfg = pre_backfill_db
        _seed_identity(engine)
        _insert_order(engine, public_id="ord-4", client_order_id="cid-4")
        _insert_execution(engine, order_public_id="ord-4", price=200.0, size=0.5, seq=1)
        _insert_execution(engine, order_public_id="ord-4", price=210.0, size=0.5, seq=2)
        command.upgrade(cfg, "head")
        row = _fill_columns(engine, "ord-4")
        assert row[0] == pytest.approx(1.0)
        assert row[1] == pytest.approx(205.0)

    def test_orders_without_evidence_stay_untouched(
        self, pre_backfill_db: tuple[sa.Engine, Config]
    ) -> None:
        """Evidence-less orders keep their pre-backfill columns.

        Given: an active order with neither executions nor venue fill
            events,
        When: the backfill runs,
        Then: filled_size stays 0.0 and average_price NULL.
        """
        engine, cfg = pre_backfill_db
        _seed_identity(engine)
        _insert_order(engine, public_id="ord-5", client_order_id="cid-5")
        command.upgrade(cfg, "head")
        row = _fill_columns(engine, "ord-5")
        assert row[0] == pytest.approx(0.0)
        assert row[1] is None

    def test_closed_historical_version_never_rewritten(
        self, pre_backfill_db: tuple[sa.Engine, Config]
    ) -> None:
        """Only the active SCD2 version is repaired.

        Given: a closed historical version and an active version of
            the same order, both with fill evidence,
        When: the backfill runs,
        Then: the active version is repaired while the closed version
            keeps its original zero columns.
        """
        engine, cfg = pre_backfill_db
        _seed_identity(engine)
        closed_at = "2026-07-11 09:00:00.000000"
        _insert_order(engine, public_id="ord-6", client_order_id="cid-6", known_to=closed_at)
        _insert_order(engine, public_id="ord-6", client_order_id="cid-6")
        _insert_execution(engine, order_public_id="ord-6", price=50.0, size=0.2, seq=1)
        command.upgrade(cfg, "head")
        active = _fill_columns(engine, "ord-6")
        closed = _fill_columns(engine, "ord-6", known_to=closed_at)
        assert active[0] == pytest.approx(0.2)
        assert active[1] == pytest.approx(50.0)
        assert closed[0] == pytest.approx(0.0)
        assert closed[1] is None

    def test_foreign_identity_events_never_leak(
        self, pre_backfill_db: tuple[sa.Engine, Config]
    ) -> None:
        """Events sharing a cid under a DIFFERENT mode never count.

        Given: an active LIVE order and fill_observed evidence recorded
            under the same cid but mode='paper' (a foreign identity),
        When: the backfill runs,
        Then: the live order stays untouched — correlation demands the
            full order identity, never bare cid.
        """
        engine, cfg = pre_backfill_db
        _seed_identity(engine)
        _insert_order(engine, public_id="ord-7", client_order_id="cid-7")
        with engine.begin() as conn:
            conn.execute(
                sa.text(
                    "INSERT INTO venue_events (public_id, event_type, shard_key, "
                    "exchange, instrument, mode, client_order_id, fill_size, "
                    "exec_id, wallet_public_id, received_at, session_id, "
                    "sequence_id, timestamp, known_to) "
                    "VALUES ('ve-foreign', 'fill_observed', 'paper.BTC-USD.paper', "
                    "'paper', 'BTC-USD', 'paper', 'cid-7', 0.9, 'ex-f', 'wal-1', "
                    ":ts, 'sess-1', 1, :ts, :kt)"
                ),
                {"ts": _TS, "kt": _ACTIVE},
            )
        command.upgrade(cfg, "head")
        row = _fill_columns(engine, "ord-7")
        assert row[0] == pytest.approx(0.0)
        assert row[1] is None


class TestIdlessFallbackDedup:
    """Canonical fallback-key semantics for identity-less fills."""

    def test_same_size_different_price_fills_both_count(
        self, pre_backfill_db: tuple[sa.Engine, Config]
    ) -> None:
        """Identity-less fills differing only by price are distinct.

        Given: two id-less fill_observed rows of size 0.5 at DIFFERENT
            prices plus an exact duplicate of the first (same size AND
            price),
        When: the backfill runs,
        Then: the two distinct fills count (1.0) while the exact
            duplicate collapses — the fallback key includes the price.
        """
        engine, cfg = pre_backfill_db
        _seed_identity(engine)
        _insert_order(engine, public_id="ord-8", client_order_id="cid-8")
        _insert_fill_event(engine, client_order_id="cid-8", seq=1, fill_size=0.5, price=100.0)
        _insert_fill_event(engine, client_order_id="cid-8", seq=2, fill_size=0.5, price=101.0)
        _insert_fill_event(engine, client_order_id="cid-8", seq=3, fill_size=0.5, price=100.0)
        command.upgrade(cfg, "head")
        row = _fill_columns(engine, "ord-8")
        assert row[0] == pytest.approx(1.0)
