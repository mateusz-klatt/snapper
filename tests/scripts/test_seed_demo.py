"""Tests for the ``scripts/seed_demo.py`` one-shot demo data seeder.

Bootstraps a real schema via ``alembic upgrade head`` against an
ephemeral SQLite database, seeds the minimal pre-requisites that a
production ``make migrate-dev`` would produce (default operator,
paper wallet, four instruments), and runs the seeder end-to-end.

Tests cover:

- The :func:`_sync_db_url` URL transformer.
- The :func:`_require_sqlite_dialect` fail-closed gate — the script
  supports SQLite only and must refuse anything else before any DML.
- The :func:`_acquire_execution_fence` SQLite write reservation (the
  seed's fused ``max+1`` execution inserts must hold the SAME fence
  production ingest uses, so a concurrent service writer can never
  collide with the seed on ``uq_executions_scope_sequence``).
- The :func:`_canonical_wallet` single canonicalization site, including
  the end-to-end proof that ONE wallet spelling reaches every consumer
  and that production ingest CONTINUES the seeded counter scope rather
  than opening a rival one.
- The happy path that inserts 4 orders, 2 executions, 2 positions,
  4 backtest runs (BTC RSI/MACD perp + CLM6 RSI + GCM6 MACD daily),
  and one pending CLM6 ``ai_review``.
- Idempotency: re-running with existing orders is a no-op.
- Hard-fail branches when required pre-reqs (paper wallet, default
  operator, BTC-USD-PERP, ETH-USD-PERP) are missing.
- Optional-instrument branches: CLM6/GCM6 absence skips their
  backtests; CLM6 absence also skips the AI-review seed.

The alias-spelling constants deliberately carry ``a-f`` hex letters so
case-change assertions exercise a REAL difference instead of passing
tautologically.
"""

from collections.abc import Iterator
from datetime import UTC
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock
from uuid import uuid7

import pytest
import sqlalchemy as sa
from alembic import command
from alembic.config import Config
from sqlalchemy import text
from sqlalchemy.engine import Connection

from scripts import seed_demo
from snapper.data.repository import SQLAlchemyRepository

ALEMBIC_INI = Path(__file__).resolve().parents[2] / "alembic.ini"
SEED_DEMO_SOURCE = Path(seed_demo.__file__)
KNOWN_TO_MAX_STR = "9999-12-31 23:59:59.000000"

ALIAS_WALLET = "0000FACE-0000-7000-8000-00000000C101"
"""An UPPERCASE (alias-spelled) wallet identity carrying real ``a-f`` letters.

SQLite's ``UUIDColumn`` is a ``String(36)`` that stores text verbatim, so
a hand-inserted row can legitimately hold this spelling while production
ingest canonicalizes to :data:`CANONICAL_WALLET`. Any consumer that binds
the raw spelling opens a counter scope the unique index cannot tell apart
from the canonical one.
"""

CANONICAL_WALLET = "0000face-0000-7000-8000-00000000c101"
"""The canonical ``str(UUID(...))`` spelling of :data:`ALIAS_WALLET`."""

LIVE_FILL_SESSION_ID = "00000000-0000-7000-8000-000000000301"
"""Provenance session for the simulated post-seed production fill.

Deliberately NOT the seeder's ``_DEMO_SESSION_ID``: the interop proof
models a live writer arriving after the screenshots were seeded.
"""


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


def _seed_paper_wallet(conn: Connection, public_id: str | None = None) -> str:
    """Insert a paper wallet matching the ``_lookup_paper_wallet`` predicate.

    ``public_id`` defaults to a fresh canonical ``uuid7`` (what
    ``make migrate-dev`` produces). Tests pass an explicit value to
    reproduce a hand-inserted row whose stored text is NOT canonical.
    """
    public_id = public_id if public_id is not None else str(uuid7())
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


def _seed_demo_users(conn: Connection) -> dict[str, str]:
    """Insert admin / operator / viewer users so the alerts seed loop finds them.

    Returns a mapping from role name to the inserted ``public_id`` so
    individual tests can verify per-user alert counts.
    """
    out: dict[str, str] = {}
    for role in ("admin", "operator", "viewer"):
        public_id = str(uuid7())
        out[role] = public_id
        conn.execute(
            text(
                "INSERT INTO users "
                "(public_id, username, email, password_hash, role, is_active, "
                " created_at, timestamp, known_to, session_id, sequence_id) "
                "VALUES (:pid, :username, :email, 'x', :role, 1, "
                " :ts, :ts, :ka, 's', 1)"
            ),
            {
                "pid": public_id,
                "username": role,
                "email": f"{role}@snapper.local",
                "role": role,
                "ts": str(datetime.now(UTC)),
                "ka": KNOWN_TO_MAX_STR,
            },
        )
    return out


def _seed_sealed_execution(
    conn: Connection,
    *,
    wallet: str,
    scope_sequence: int,
) -> str:
    """Insert one sealed (append-only ledger) execution row and return its public_id.

    Written directly with an explicit ``scope_sequence`` and a caller-chosen
    ``wallet_public_id`` spelling so a test can assert the seeder leaves the
    physical ledger row byte-identical. ``exchange``/``mode`` mirror the
    seeder's own ``kraken_futures``/``paper`` scope so a rewrite — if one
    leaked — would land the row in the same partition the seed writes.
    """
    public_id = str(uuid7())
    conn.execute(
        text(
            "INSERT INTO executions "
            "(public_id, order_public_id, wallet_public_id, operator_public_id, "
            " exchange, mode, scope_sequence, "
            " exec_id, trade_id, side, status, price, size, fee, fee_asset, "
            " executed_at, liquidity_role, "
            " timestamp, known_to, session_id, sequence_id) "
            "VALUES "
            "(:pid, :opid, :wallet, :operator, "
            " 'kraken_futures', 'paper', :seq, "
            " :exid, :tid, 'buy', 'filled', 100.0, 1.0, 0.1, 'USD', "
            " :ts, 'taker', "
            " :ts, :ka, :sid, 1)"
        ),
        {
            "pid": public_id,
            "opid": str(uuid7()),
            "wallet": wallet,
            "operator": str(uuid7()),
            "seq": scope_sequence,
            "exid": f"exec-{public_id[:8]}",
            "tid": f"trade-{public_id[:8]}",
            "ts": str(datetime.now(UTC)),
            "ka": KNOWN_TO_MAX_STR,
            "sid": "pre-existing-ledger",
        },
    )
    return public_id


def _seed_spot_reconciliation_anchor(conn: Connection, *, wallet: str) -> str:
    """Insert one immutable spot reconciliation anchor row and return its public_id.

    Satisfies every anchor CHECK (live mode, lowercase exchange, ordered
    ten-instant read chain, ``scope_sequence`` watermark unit, non-empty
    evidence text, certified statuses, exchange-bound venue cursor and
    lowercase chain tip) so the row is a legitimate sealed anchor the
    seeder must never re-key.
    """
    public_id = str(uuid7())
    conn.execute(
        text(
            "INSERT INTO portfolio_spot_reconciliation_anchors "
            "(public_id, wallet_public_id, exchange, mode, "
            " venue_account_state_public_id, balance_observation_id, "
            " source_watermark_kind, source_watermark, balances_json, "
            " first_request_started_at, first_request_completed_at, "
            " second_request_started_at, second_request_completed_at, "
            " boundary_status, inventory_status, margin_status, provenance, "
            " source_chain_tip, venue_cursor_kind, venue_cursor_scheme, "
            " venue_cursor_value, venue_cursor_requested_at, "
            " venue_cursor_observed_at, venue_cursor_confirmed_at, "
            " source_watermark_requested_at, source_watermark_captured_at, "
            " timestamp, known_to, session_id, sequence_id) "
            "VALUES "
            "(:pid, :wallet, 'kraken', 'live', "
            " :vas, 1, "
            " 'scope_sequence', 3, '{\"BTC\": \"1.0\"}', "
            " '2026-05-01 10:00:00.000000', '2026-05-01 10:00:01.000000', "
            " '2026-05-01 10:00:02.000000', '2026-05-01 10:00:03.000000', "
            " 'cursor_certified', 'venue_reported_full', 'cash', 'demo-anchor', "
            " 'e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0c1d2e3f4a5b6c7d8e9f0a1b2c3d4e5f6', "
            " 'account_history_item_id', 'kraken:ccxt:account/history:v1', "
            " '1', '2026-05-01 09:59:56.000000', "
            " '2026-05-01 09:59:57.000000', '2026-05-01 10:00:03.000000', "
            " '2026-05-01 09:59:58.000000', '2026-05-01 09:59:59.000000', "
            " '2026-05-01 10:00:04.000000', :ka, :sid, 1)"
        ),
        {
            "pid": public_id,
            "wallet": wallet,
            "vas": str(uuid7()),
            "ka": KNOWN_TO_MAX_STR,
            "sid": "pre-existing-ledger",
        },
    )
    return public_id


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


WALLET_SCOPED_SCHEMA_TABLES = frozenset(
    {
        "accrual_ledger",
        "ai_reviews",
        "alert_deliveries",
        "alert_events",
        "backtest_comparisons",
        "backtest_runs",
        "device_alert_prefs",
        "execution_plans",
        "executions",
        "orders",
        "paired_execution_groups",
        "paired_execution_halts",
        "paired_execution_legs",
        "portfolio_drift_episodes",
        "portfolio_pnl_points",
        "portfolio_reconciliation_method_configs",
        "portfolio_reconciliation_observations",
        "portfolio_reconciliation_states",
        "portfolio_spot_reconciliation_anchors",
        "position_cycles",
        "positions",
        "process_runs",
        "signals",
        "trade_commands",
        "trade_projection_checkpoints",
        "venue_account_observations",
        "venue_account_states",
        "venue_events",
        "wallet_credentials",
        "wallet_operator_scope_grants",
    }
)
"""EVERY ``wallet_public_id``-bearing table in the migrated schema.

This is the pinned enumeration of the physical wallet-scoped surface, NOT the
subset the seed writes. :func:`_wallet_scoped_schema_tables` rediscovers the
set from the live schema and the alias proof asserts the two agree, so ANY new
``wallet_public_id``-bearing migration — populated by the seed or not — breaks
this test and forces an explicit decision: does the new table belong in
:data:`WALLET_SCOPED_SEED_TABLES` (and get seeded + spelling-asserted), or is
it deliberately out of the seed's scope. That forced classification is the
theorem. Discovering only *populated* tables would let a new EMPTY wallet-scoped
table vanish from the comparison and pass vacuously — reopening the D1 overclaim
in a new shape, where a rival seed path targets a fresh wallet-scoped table that
the alias fixture happens to leave empty.
"""

WALLET_SCOPED_SEED_TABLES = frozenset(
    {
        "orders",
        "executions",
        "positions",
        "backtest_runs",
        "alert_events",
        "wallet_operator_scope_grants",
        "ai_reviews",
    }
)
"""The wallet-scoped tables a complete ``main()`` run POPULATES.

A declared subset of :data:`WALLET_SCOPED_SCHEMA_TABLES`.
:func:`_populated_wallet_scoped_tables` proves each member is actually seeded
with the canonical spelling. Membership is a standing obligation: a declared
table that stops being seeded fails the populated-coverage check rather than
silently going unproven.

``wallet_operator_scope_grants`` and ``ai_reviews`` are reachable only when
CLM6-NYMEX exists, so the alias test must seed the optional commodity pair —
without it both tables stay empty and their wallet spelling is never asserted.
"""


def _wallet_scoped_schema_tables(conn: Connection) -> set[str]:
    """Return EVERY migrated table carrying a ``wallet_public_id`` column.

    Introspects the schema alone — row counts are irrelevant, so a
    wallet-scoped table that holds no rows is still discovered. The caller
    asserts this set equals :data:`WALLET_SCOPED_SEED_TABLES`, which turns
    "the seed writes every wallet-scoped table" into a theorem over the
    physical schema: a newly added ``wallet_public_id``-bearing table forces a
    frozenset update EVEN IF the fixture leaves it empty. A populated-only
    discovery would omit that empty table and let the equality pass silently.
    """
    inspector = sa.inspect(conn)
    return {
        name
        for name in inspector.get_table_names()
        if "wallet_public_id" in {column["name"] for column in inspector.get_columns(name)}
    }


def _populated_wallet_scoped_tables(conn: Connection) -> dict[str, list[str]]:
    """Map each populated wallet-scoped table to the distinct spellings it holds.

    Introspects the migrated schema under test rather than a hand-kept list.
    A wallet-scoped table holding no rows is omitted here BY DESIGN — this
    helper answers "which spelling did each seeded table store", not "is every
    wallet-scoped table present". Completeness against the schema is proven
    separately by :func:`_wallet_scoped_schema_tables`; the caller cross-checks
    both, so a declared table that stops being seeded surfaces as a missing
    key here while a new empty wallet-scoped table surfaces there.
    """
    populated: dict[str, list[str]] = {}
    for name in sorted(_wallet_scoped_schema_tables(conn)):
        spellings = [
            row[0] for row in conn.execute(text(f"SELECT DISTINCT wallet_public_id FROM {name}"))
        ]
        if spellings:
            populated[name] = spellings
    return populated


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

        Given: a Postgres URL,
        When: ``_sync_db_url`` is called,
        Then: no transformation is applied. The transformer rewrites only
        the SQLite driver; a PostgreSQL URL therefore survives to
        ``engine.connect()``, where :func:`seed_demo._require_sqlite_dialect`
        refuses it. (This URL form is NOT what deployments configure —
        the documented production ``DB_URL`` is ``postgresql+asyncpg://``,
        which a synchronous ``create_engine`` cannot open at all.)
        """
        url = "postgresql+psycopg2://user:pass@host/db"
        assert seed_demo._sync_db_url(url) == url


class TestRequireSqliteDialect:
    """Tests for the :func:`seed_demo._require_sqlite_dialect` fail-closed gate.

    The script is a screenshot tool whose only working backend is SQLite.
    A PostgreSQL fence, its isolation level, and its native-UUID handling
    cannot be proven correct by a mocked connection — a mock returns
    whatever it was told to — so the script refuses the dialect outright
    instead of shipping an unprovable claim.
    """

    def test_refuses_postgresql(self) -> None:
        """PostgreSQL is refused rather than seeded through a dead fence.

        Given: a mocked connection reporting the ``postgresql`` dialect,
        When: the gate runs,
        Then: ``RuntimeError`` naming the dialect is raised and NO SQL is
        executed — the script must not begin work it cannot serialize.
        """
        conn = MagicMock()
        conn.dialect.name = "postgresql"
        with pytest.raises(RuntimeError, match="sqlite only.*postgresql"):
            seed_demo._require_sqlite_dialect(conn)
        conn.execute.assert_not_called()

    def test_refuses_unsupported_dialect(self) -> None:
        """Any other dialect fails closed too.

        Given: a mocked connection reporting an unsupported dialect,
        When: the gate runs,
        Then: ``RuntimeError`` is raised and no SQL is executed — seeding
        without allocation serialization would risk colliding with a live
        service writer on ``uq_executions_scope_sequence``.
        """
        conn = MagicMock()
        conn.dialect.name = "mysql"
        with pytest.raises(RuntimeError, match="sqlite only.*mysql"):
            seed_demo._require_sqlite_dialect(conn)
        conn.execute.assert_not_called()

    def test_admits_sqlite(self) -> None:
        """SQLite — the one supported backend — passes the gate silently.

        Given: a mocked connection reporting the ``sqlite`` dialect,
        When: the gate runs,
        Then: it returns without raising and without executing SQL.
        """
        conn = MagicMock()
        conn.dialect.name = "sqlite"
        seed_demo._require_sqlite_dialect(conn)
        conn.execute.assert_not_called()

    def test_module_carries_no_postgresql_fence_statement(self) -> None:
        """No PostgreSQL advisory-lock statement survives anywhere in the module.

        Given: the seeder's source text,
        When: it is scanned for the PostgreSQL fence primitive,
        Then: ``pg_advisory_xact_lock`` does not appear. This asserts on
        source rather than behaviour deliberately: a ``MagicMock`` can
        never prove what a real PostgreSQL server does, so the previous
        mock-based fence test certified a branch it had not exercised —
        exactly the false authoritative verdict this suite must not
        produce. With the dialect refused, absence of the statement is
        the honest, checkable invariant.
        """
        source = SEED_DEMO_SOURCE.read_text(encoding="utf-8")
        assert "pg_advisory_xact_lock" not in source


class TestCanonicalWallet:
    """Tests for the :func:`seed_demo._canonical_wallet` single canonicalization site."""

    def test_alias_spelling_becomes_canonical(self) -> None:
        """An UPPERCASE alias spelling collapses to the canonical form.

        Given: a hand-inserted wallet stored in an alias spelling,
        When: it is canonicalized,
        Then: the canonical lowercase-hyphenated spelling is returned —
        the SAME value ``insert_execution`` derives, which is what makes
        the seed's counter scope and production's the one scope.
        """
        assert seed_demo._canonical_wallet(ALIAS_WALLET) == CANONICAL_WALLET

    def test_canonical_spelling_is_unchanged(self) -> None:
        """An already-canonical identity round-trips unchanged.

        Given: the canonical spelling ``make migrate-dev`` produces,
        When: it is canonicalized,
        Then: it is returned byte-identical (the normal path is inert).
        """
        assert seed_demo._canonical_wallet(CANONICAL_WALLET) == CANONICAL_WALLET

    def test_non_uuid_identity_is_refused(self) -> None:
        """A non-UUID wallet identity refuses the seed instead of writing rows.

        Given: a wallet whose stored ``public_id`` is not a UUID (SQLite
            stores ``String(36)`` text verbatim, so this can be
            hand-inserted),
        When: it is canonicalized,
        Then: ``RuntimeError`` is raised. Production ingest refuses such a
        wallet outright (``invalid_execution_wallet_identity``), so seeding
        under it would build a scope no live fill could ever extend.
        """
        with pytest.raises(RuntimeError, match="not a valid UUID"):
            seed_demo._canonical_wallet("not-a-uuid")


class TestAcquireExecutionFence:
    """Unit tests for the :func:`seed_demo._acquire_execution_fence` helper.

    Additionally exercised end-to-end by every ``main()`` happy-path test.
    """

    def test_sqlite_takes_the_write_reservation(self) -> None:
        """The fence opens the BEGIN IMMEDIATE write reservation.

        Given: a mocked connection,
        When: the fence is acquired,
        Then: ``BEGIN IMMEDIATE`` is executed so the reservation covers
        every subsequent fused ``max+1`` allocation until the script's
        single commit.
        """
        conn = MagicMock()
        seed_demo._acquire_execution_fence(conn)
        assert str(conn.execute.call_args.args[0]) == "BEGIN IMMEDIATE"


class TestNormalizeWalletIdentity:
    """Primitive-level tests for :func:`seed_demo._normalize_wallet_identity`.

    ``main`` binds the canonical spelling to every row it INSERTS, so the
    only surface these tests can exercise that the end-to-end proofs cannot
    is a wallet that ALREADY has references when normalization runs: the
    root ``wallets`` row it never inserts, and any pre-existing
    ``wallet_public_id`` reference. Calling the primitive directly with a
    seeded reference proves the rewrite reaches the physical row, not just
    the rows a particular ``main`` run happens to write.
    """

    def test_rewrites_root_row_and_existing_reference(
        self,
        migrated_db: tuple[sa.Engine, str],
    ) -> None:
        """A noncanonical root row and its references move to canonical together.

        Given: a wallet hand-inserted in the UPPERCASE alias spelling and a
            pre-existing ``wallet_public_id`` reference in the SAME
            spelling (a wallet-scoped table populated before the seed runs),
        When: :func:`seed_demo._normalize_wallet_identity` runs for that
            wallet,
        Then: BOTH the root ``wallets`` row and the reference carry the
            canonical spelling — the rewrite is schema-driven, so a table
            it does not hard-code is still reached.

        This is the theorem the end-to-end alias proof cannot state on its
        own: there, references are written canonically by ``main`` AFTER
        normalization, so the reference rewrite executes against zero rows.
        Here the reference exists first, so the assertion fails unless the
        primitive actually rewrites it.
        """
        engine, _ = migrated_db
        with engine.begin() as conn:
            _seed_paper_wallet(conn, ALIAS_WALLET)
            conn.execute(
                text(
                    "CREATE TABLE _preexisting_wallet_scoped ("
                    "id INTEGER PRIMARY KEY, wallet_public_id TEXT NOT NULL)"
                )
            )
            conn.execute(
                text("INSERT INTO _preexisting_wallet_scoped (wallet_public_id) VALUES (:w)"),
                {"w": ALIAS_WALLET},
            )

        with engine.begin() as conn:
            seed_demo._normalize_wallet_identity(conn, ALIAS_WALLET, CANONICAL_WALLET)

        with engine.connect() as conn:
            root = [row[0] for row in conn.execute(text("SELECT public_id FROM wallets"))]
            reference = [
                row[0]
                for row in conn.execute(
                    text("SELECT wallet_public_id FROM _preexisting_wallet_scoped")
                )
            ]

        assert root == [CANONICAL_WALLET]
        assert reference == [CANONICAL_WALLET]

    def test_already_canonical_leaves_rows_untouched(
        self,
        migrated_db: tuple[sa.Engine, str],
    ) -> None:
        """An already-canonical identity rewrites nothing.

        Given: a canonical root wallet plus a reference row deliberately
            carrying a DIFFERENT (alias) spelling,
        When: normalization runs with ``stored == canonical``,
        Then: the early return leaves every row byte-identical — the guard
            never rewrites unrelated rows on the production ``migrate-dev``
            path where the wallet was created canonically.
        """
        engine, _ = migrated_db
        with engine.begin() as conn:
            _seed_paper_wallet(conn, CANONICAL_WALLET)
            conn.execute(
                text(
                    "CREATE TABLE _unrelated_wallet_scoped ("
                    "id INTEGER PRIMARY KEY, wallet_public_id TEXT NOT NULL)"
                )
            )
            conn.execute(
                text("INSERT INTO _unrelated_wallet_scoped (wallet_public_id) VALUES (:w)"),
                {"w": ALIAS_WALLET},
            )

        with engine.begin() as conn:
            seed_demo._normalize_wallet_identity(conn, CANONICAL_WALLET, CANONICAL_WALLET)

        with engine.connect() as conn:
            root = [row[0] for row in conn.execute(text("SELECT public_id FROM wallets"))]
            reference = [
                row[0]
                for row in conn.execute(
                    text("SELECT wallet_public_id FROM _unrelated_wallet_scoped")
                )
            ]

        assert root == [CANONICAL_WALLET]
        assert reference == [ALIAS_WALLET]


class TestImmutableLedgerExclusion:
    """Tests that the seeder never rewrites an append-only ledger row.

    The normalization loop rewrites ``wallet_public_id`` across EVERY
    wallet-scoped table it discovers by introspection. ``executions`` and
    ``portfolio_spot_reconciliation_anchors`` carry that column but are
    immutable ledgers whose ``(wallet, exchange, mode, scope_sequence)``
    identity and certified inventory are theorem inputs to the
    authoritative reconciliation verdict — re-keying either would split a
    counter scope or restate a sealed inventory. These tests prove the
    exclusion holds against the PHYSICAL table the ORM maps.
    """

    def test_excluded_names_are_the_physical_ledger_tables(
        self,
        migrated_db: tuple[sa.Engine, str],
    ) -> None:
        """The exclusion set equals the mapped ledger tables AND is wallet-scoped.

        Given: the migrated schema,
        When: :func:`seed_demo._immutable_ledger_table_names` is compared
            against the physical schema,
        Then: it names exactly ``executions`` and the spot reconciliation
            anchor, and BOTH carry a ``wallet_public_id`` column — so absent
            the skip the introspection loop WOULD rewrite them. Deriving the
            set from the mapped models' ``__table__.name`` binds the guard to
            the physical table, not a call-site string that a rename could
            desync.
        """
        engine, _ = migrated_db
        excluded = seed_demo._immutable_ledger_table_names()
        assert excluded == {"executions", "portfolio_spot_reconciliation_anchors"}
        with engine.connect() as conn:
            wallet_scoped = _wallet_scoped_schema_tables(conn)
        assert excluded <= wallet_scoped, (
            "the excluded ledgers must actually carry wallet_public_id — otherwise "
            "the exclusion is vacuous and proves nothing"
        )

    def test_sealed_execution_and_anchor_survive_seeding_byte_identical(
        self,
        migrated_db: tuple[sa.Engine, str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A pre-seeded sealed execution and anchor are untouched by ``main()``.

        Given: an alias-spelled paper wallet, plus a sealed execution row and
            a spot reconciliation anchor row BOTH hand-written in that same
            alias spelling with a fixed ``scope_sequence``,
        When: ``main()`` runs (which canonicalizes the root wallet and every
            NON-ledger reference),
        Then: the ledger rows keep their EXACT alias ``wallet_public_id`` and
            ``scope_sequence`` — the seeder issues no ``UPDATE`` against
            either physical table — while the root ``wallets`` row and the
            seeder's OWN freshly inserted executions carry the canonical
            spelling and remain a contiguous ``1, 2`` scope.

        Reverting the ledger exclusion fails this test: the pre-seeded
        execution's wallet comes back canonical (proving it was rewritten)
        and its scope would then collide against the seed's own rows.
        """
        engine, db_url = migrated_db
        with engine.begin() as conn:
            _seed_paper_wallet(conn, ALIAS_WALLET)
            _seed_default_operator(conn)
            _seed_required_instruments(conn)
            sealed_execution_pid = _seed_sealed_execution(
                conn, wallet=ALIAS_WALLET, scope_sequence=7
            )
            anchor_pid = _seed_spot_reconciliation_anchor(conn, wallet=ALIAS_WALLET)

        monkeypatch.setenv("DB_URL", db_url)
        assert seed_demo.main() == 0

        with engine.connect() as conn:
            sealed = conn.execute(
                text(
                    "SELECT wallet_public_id, scope_sequence FROM executions WHERE public_id = :pid"
                ),
                {"pid": sealed_execution_pid},
            ).one()
            anchor_wallet = conn.execute(
                text(
                    "SELECT wallet_public_id FROM portfolio_spot_reconciliation_anchors "
                    "WHERE public_id = :pid"
                ),
                {"pid": anchor_pid},
            ).scalar_one()
            root_wallet = conn.execute(text("SELECT public_id FROM wallets")).scalar_one()
            seeded_scope = conn.execute(
                text(
                    "SELECT wallet_public_id, scope_sequence FROM executions "
                    "WHERE session_id = :sid ORDER BY scope_sequence ASC"
                ),
                {"sid": seed_demo._DEMO_SESSION_ID},
            ).all()

        assert sealed == (ALIAS_WALLET, 7), (
            "the sealed execution ledger row must be byte-identical after seeding; "
            f"got wallet={sealed[0]!r} scope_sequence={sealed[1]}"
        )
        assert (
            anchor_wallet == ALIAS_WALLET
        ), f"the sealed anchor ledger row must be untouched; got {anchor_wallet!r}"
        assert root_wallet == CANONICAL_WALLET
        assert [row[1] for row in seeded_scope] == [1, 2]
        assert {row[0] for row in seeded_scope} == {CANONICAL_WALLET}

    def test_root_wallet_converges_on_a_skipped_rerun(
        self,
        migrated_db: tuple[sa.Engine, str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A re-run still canonicalizes the root wallet even when the seed skips.

        Given: an already-seeded database (an order carrying the demo
            ``session_id`` trips the idempotency gate) whose root ``wallets``
            row and a NON-ledger reference are still in the alias spelling,
        When: ``main()`` runs again,
        Then: the normalization runs BEFORE the skip gate and commits, so the
            root wallet and the reference converge to the canonical spelling
            while NO new demo rows are inserted — a first-run alias is not
            frozen forever behind the idempotency early return.
        """
        engine, db_url = migrated_db
        with engine.begin() as conn:
            wallet = _seed_paper_wallet(conn, ALIAS_WALLET)
            operator = _seed_default_operator(conn)
            _seed_required_instruments(conn)
            conn.execute(
                text(
                    "CREATE TABLE _preexisting_wallet_scoped ("
                    "id INTEGER PRIMARY KEY, wallet_public_id TEXT NOT NULL)"
                )
            )
            conn.execute(
                text("INSERT INTO _preexisting_wallet_scoped (wallet_public_id) VALUES (:w)"),
                {"w": ALIAS_WALLET},
            )
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
                    " 'prior-demo', NULL, :ts, :ts, 'buy', 'market', "
                    " 100.0, 1.0, 'filled', 'gtc', 1.0, 100.0, NULL, "
                    " NULL, 0, NULL, "
                    " :ts, :ka, :sid, 1)"
                ),
                {
                    "pid": str(uuid7()),
                    "inst": inst_pid,
                    "wal": wallet,
                    "op": operator,
                    "ts": str(datetime.now(UTC)),
                    "ka": KNOWN_TO_MAX_STR,
                    "sid": seed_demo._DEMO_SESSION_ID,
                },
            )

        monkeypatch.setenv("DB_URL", db_url)
        assert seed_demo.main() == 0

        with engine.connect() as conn:
            root = conn.execute(text("SELECT public_id FROM wallets")).scalar_one()
            reference = conn.execute(
                text("SELECT wallet_public_id FROM _preexisting_wallet_scoped")
            ).scalar_one()
            demo_orders = conn.execute(
                text("SELECT COUNT(*) FROM orders WHERE session_id = :sid"),
                {"sid": seed_demo._DEMO_SESSION_ID},
            ).scalar()

        assert root == CANONICAL_WALLET
        assert reference == CANONICAL_WALLET
        assert demo_orders == 1, "the skip gate must not insert a second demo dataset"


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
            _seed_demo_users(conn)

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
            assert conn.execute(text("SELECT COUNT(*) FROM alert_events")).scalar() == 21
            for role in ("admin", "operator", "viewer"):
                row_count = conn.execute(
                    text(
                        "SELECT COUNT(*) FROM alert_events "
                        "WHERE user_public_id = (SELECT public_id FROM users WHERE role = :role)"
                    ),
                    {"role": role},
                ).scalar()
                assert row_count == 7, f"expected 7 alerts for role {role}, got {row_count}"

    def test_refuses_non_sqlite_dialect_before_touching_the_database(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A non-SQLite connection is refused before ANY statement runs.

        Given: a connection reporting the ``postgresql`` dialect,
        When: ``main()`` runs,
        Then: ``RuntimeError`` naming the dialect is raised, and the
            connection executed NOTHING and committed NOTHING — proving
            the gate is ordered ahead of every lookup and every insert,
            not merely present somewhere in the function.

        The connection is mocked deliberately and only to pin OUR
        statement ordering — which a mock CAN establish. It makes no
        claim about real PostgreSQL behaviour; that is precisely the
        claim the seeder no longer makes.
        """
        conn = MagicMock()
        conn.dialect.name = "postgresql"
        engine = MagicMock()
        engine.connect.return_value.__enter__.return_value = conn
        monkeypatch.setattr(seed_demo, "create_engine", MagicMock(return_value=engine))
        monkeypatch.setenv("DB_URL", "sqlite:///./unused-by-this-test.db")

        with pytest.raises(RuntimeError, match="sqlite only.*postgresql"):
            seed_demo.main()

        conn.execute.assert_not_called()
        conn.commit.assert_not_called()

    def test_refuses_non_uuid_paper_wallet(
        self,
        migrated_db: tuple[sa.Engine, str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A hand-inserted non-UUID wallet refuses the seed instead of writing rows.

        Given: a paper wallet whose ``public_id`` is not a UUID (SQLite
            stores ``String(36)`` verbatim, so this survives insertion),
        When: ``main()`` runs,
        Then: ``RuntimeError`` is raised and no orders are written —
            production ingest refuses that identity outright, so any rows
            seeded under it would form a scope no live fill could extend.
        """
        engine, db_url = migrated_db
        with engine.begin() as conn:
            _seed_paper_wallet(conn, "not-a-uuid")
            _seed_default_operator(conn)
            _seed_required_instruments(conn)

        monkeypatch.setenv("DB_URL", db_url)
        with pytest.raises(RuntimeError, match="not a valid UUID"):
            seed_demo.main()

        with engine.connect() as conn:
            assert conn.execute(text("SELECT COUNT(*) FROM orders")).scalar() == 0

    def test_empty_wallet_scoped_table_breaks_schema_completeness(
        self,
        migrated_db: tuple[sa.Engine, str],
    ) -> None:
        """A new EMPTY wallet-scoped table fails the proof instead of passing silently.

        Given: the migrated schema, plus one extra table carrying a
            ``wallet_public_id`` column that holds NO rows (modelling a
            newly added conditional seed path whose fixture leaves it empty),
        When: the completeness helpers run,
        Then: the schema helper DISCOVERS the empty table — so the alias
            proof's ``_wallet_scoped_schema_tables(conn) == WALLET_SCOPED_SCHEMA_TABLES``
            assertion would FAIL and demand a frozenset update — while the
            populated helper OMITS it.

        This is the anti-regression for the D1 overclaim: the earlier
        populated-only discovery would have dropped the empty table from the
        comparison, letting a rival wallet-scoped table slip in unproven. The
        two-part check makes coverage a theorem over the physical schema, not
        a probability over whichever tables happened to get rows.
        """
        engine, _ = migrated_db
        with engine.begin() as conn:
            conn.execute(
                text(
                    "CREATE TABLE _phantom_wallet_scoped ("
                    "id INTEGER PRIMARY KEY, wallet_public_id TEXT NOT NULL)"
                )
            )

        with engine.connect() as conn:
            schema_tables = _wallet_scoped_schema_tables(conn)
            populated = _populated_wallet_scoped_tables(conn)

        assert "_phantom_wallet_scoped" in schema_tables
        assert schema_tables != WALLET_SCOPED_SCHEMA_TABLES
        assert "_phantom_wallet_scoped" not in populated

    def test_alias_spelled_wallet_reaches_every_row_canonically(
        self,
        migrated_db: tuple[sa.Engine, str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """ONE canonical wallet spelling reaches every table the seed writes.

        Given: a paper wallet hand-inserted in an UPPERCASE alias spelling
            (SQLite stores ``UUIDColumn`` text verbatim),
        When: ``main()`` runs,
        Then: every wallet-scoped table holds EXACTLY the canonical
            lowercase spelling and nothing else.

        This is the same-value proof: the seeder previously canonicalized
        only the PostgreSQL fence key while binding the raw spelling to
        the ``max+1`` subselect and to every persisted row, so the fence,
        the counter scope, and the stored rows could drift apart. Against
        that code this test fails — the rows come back UPPERCASE.

        The optional commodity pair is seeded because
        ``wallet_operator_scope_grants`` and ``ai_reviews`` are gated on
        CLM6-NYMEX; without it those two binds execute against nothing and
        their spelling goes unasserted while the lines still read as
        covered. That gap is not cosmetic — ``repository.py`` locks and
        overlap-checks scope grants BY WALLET TEXT, so an alias-spelled
        grant would be invisible to canonical overlap detection.

        The root ``wallets`` row is asserted alongside the child rows: it
        is the ONE row ``main`` does not insert (it is hand-seeded here in
        the alias spelling), so canonicalizing only the seeded children
        would leave the root ``public_id`` uppercase.
        ``repository.py`` resolves non-admin scope by joining
        ``wallets.public_id`` against each grant's ``wallet_public_id`` by
        text, so a divergent root row hides the seeded wallet from every
        non-admin operator. Reverting
        :func:`seed_demo._normalize_wallet_identity` fails the final
        assertion — the root row comes back UPPERCASE.
        """
        engine, db_url = migrated_db
        with engine.begin() as conn:
            _seed_paper_wallet(conn, ALIAS_WALLET)
            _seed_default_operator(conn)
            _seed_required_instruments(conn)
            _seed_optional_commodity_instruments(conn)
            _seed_demo_users(conn)

        monkeypatch.setenv("DB_URL", db_url)
        assert seed_demo.main() == 0

        assert WALLET_SCOPED_SEED_TABLES <= WALLET_SCOPED_SCHEMA_TABLES, (
            "the seeded set must be a subset of the pinned schema surface; "
            f"stray members: {sorted(WALLET_SCOPED_SEED_TABLES - WALLET_SCOPED_SCHEMA_TABLES)}"
        )

        with engine.connect() as conn:
            schema_tables = _wallet_scoped_schema_tables(conn)
            populated = _populated_wallet_scoped_tables(conn)
            root_wallet_spellings = [
                row[0] for row in conn.execute(text("SELECT public_id FROM wallets"))
            ]

        assert schema_tables == WALLET_SCOPED_SCHEMA_TABLES, (
            "wallet-scoped SCHEMA drifted — every table carrying a wallet_public_id "
            "column must be enumerated; a new one (even left empty) forces a decision "
            "on whether it belongs in WALLET_SCOPED_SEED_TABLES. "
            f"Schema-carried: {sorted(schema_tables)}"
        )
        assert set(populated) == WALLET_SCOPED_SEED_TABLES, (
            "wallet-scoped seed coverage drifted — every declared seeded table "
            f"must actually be populated; populated: {sorted(populated)}"
        )
        for table, spellings in sorted(populated.items()):
            assert spellings == [CANONICAL_WALLET], f"{table} stored {spellings}"
        assert root_wallet_spellings == [
            CANONICAL_WALLET
        ], f"root wallets row must carry the canonical spelling; stored {root_wallet_spellings}"

    async def test_production_ingest_continues_the_seeded_execution_scope(
        self,
        migrated_db: tuple[sa.Engine, str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A live fill CONTINUES the seed's counter scope instead of rivalling it.

        Given: an alias-spelled paper wallet and a completed ``main()``
            run (two seeded ``kraken_futures``/``paper`` executions),
        When: production ``insert_execution`` persists a fill for the SAME
            logical wallet, addressed by its alias spelling,
        Then: the ledger holds ``scope_sequence`` 1, 2, 3 under ONE
            canonical wallet spelling.

        This is the interop proof and the assertion that matters. The
        failure it closes is silent: with the seed binding the raw
        uppercase spelling, its rows land in a scope that production's
        canonical ``max+1`` read cannot see, so the live fill allocates a
        SECOND ``scope_sequence = 1``. Both rows COMMIT — the unique
        index compares different wallet text — and the canonical
        watermark capture never observes the seeded rows. Asserting
        merely that no ``IntegrityError`` is raised would therefore prove
        nothing; only the contiguous 1/2/3 under a single spelling does.
        """
        engine, db_url = migrated_db
        with engine.begin() as conn:
            _seed_paper_wallet(conn, ALIAS_WALLET)
            _seed_default_operator(conn)
            _seed_required_instruments(conn)

        monkeypatch.setenv("DB_URL", db_url)
        assert seed_demo.main() == 0

        with engine.connect() as conn:
            order_public_id = conn.execute(
                text(
                    "SELECT public_id FROM orders WHERE session_id = :sid "
                    "AND status = 'filled' ORDER BY id ASC LIMIT 1"
                ),
                {"sid": seed_demo._DEMO_SESSION_ID},
            ).scalar_one()

        repository = SQLAlchemyRepository(db_url.replace("sqlite://", "sqlite+aiosqlite://"))
        try:
            await repository.insert_execution(
                order_public_id=str(order_public_id),
                wallet_public_id=ALIAS_WALLET,
                timestamp=datetime(2026, 5, 6, 12, 0, tzinfo=UTC),
                side="buy",
                status="filled",
                price=79781.0,
                size=0.01,
                fee=0.8,
                fee_asset="USD",
                session_id=LIVE_FILL_SESSION_ID,
                sequence_id=1,
            )
        finally:
            await repository.engine.dispose()

        with engine.connect() as conn:
            ledger = conn.execute(
                text(
                    "SELECT wallet_public_id, scope_sequence FROM executions "
                    "ORDER BY scope_sequence ASC"
                )
            ).all()

        assert [row[1] for row in ledger] == [1, 2, 3]
        assert {row[0] for row in ledger} == {CANONICAL_WALLET}

    def test_idempotent_skip_when_demo_session_exists(
        self,
        migrated_db: tuple[sa.Engine, str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Re-running after a previous demo seed run is a no-op.

        Given: a DB where ``main()`` has already produced the demo dataset
            (rows tagged with the well-known ``_DEMO_SESSION_ID``),
        When: ``main()`` runs again,
        Then: the function returns 0 without inserting new rows because
            the skip gate matches the demo-session marker.
        """
        engine, db_url = migrated_db
        with engine.begin() as conn:
            _seed_paper_wallet(conn)
            _seed_default_operator(conn)
            _seed_required_instruments(conn)
            _seed_optional_commodity_instruments(conn)
            _seed_demo_users(conn)

        monkeypatch.setenv("DB_URL", db_url)
        first_rc = seed_demo.main()
        assert first_rc == 0

        with engine.connect() as conn:
            first_orders = conn.execute(text("SELECT COUNT(*) FROM orders")).scalar()
            first_alerts = conn.execute(text("SELECT COUNT(*) FROM alert_events")).scalar()
            assert first_orders == 4
            assert first_alerts == 21

        second_rc = seed_demo.main()
        assert second_rc == 0

        with engine.connect() as conn:
            assert conn.execute(text("SELECT COUNT(*) FROM orders")).scalar() == first_orders
            assert conn.execute(text("SELECT COUNT(*) FROM alert_events")).scalar() == first_alerts

    def test_runs_alongside_unrelated_orders(
        self,
        migrated_db: tuple[sa.Engine, str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Pre-existing non-demo orders do not block the demo seed.

        Given: a DB with the pre-reqs seeded AND one unrelated order row
            (different ``session_id`` than ``_DEMO_SESSION_ID``),
        When: ``main()`` runs,
        Then: the function still inserts the full demo dataset on top of
            the unrelated row (5 orders total). This is the new behaviour
            after the skip gate moved from ``EXISTS(orders)`` to a
            session-id match — manual e2e test rows on a shared dev DB
            no longer prevent screenshots / iOS UAT data from landing.
        """
        engine, db_url = migrated_db
        with engine.begin() as conn:
            wallet = _seed_paper_wallet(conn)
            operator = _seed_default_operator(conn)
            _seed_required_instruments(conn)
            _seed_demo_users(conn)
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
                    " :ts, :ka, 'unrelated-session', 1)"
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
            assert conn.execute(text("SELECT COUNT(*) FROM orders")).scalar() == 5
            assert conn.execute(text("SELECT COUNT(*) FROM executions")).scalar() == 2
            assert (
                conn.execute(
                    text("SELECT COUNT(*) FROM orders WHERE session_id = :sid"),
                    {"sid": seed_demo._DEMO_SESSION_ID},
                ).scalar()
                == 4
            )
            assert conn.execute(text("SELECT COUNT(*) FROM alert_events")).scalar() == 21

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
