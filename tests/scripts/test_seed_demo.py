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
- The happy path that inserts 4 orders, 2 executions, 2 canonical fill
  witnesses, 2 positions,
  4 backtest runs (BTC RSI/MACD perp + CLM6 RSI + GCM6 MACD daily),
  and one pending CLM6 ``ai_review``.
- Idempotency: re-running with existing orders is a no-op.
- Legacy/incomplete P&L rows fail loudly because append-only execution
  identities cannot be repaired in place.
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
from datetime import timedelta
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
from snapper.messaging.infrastructure.publisher import SequenceTracker

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


def _seed_demo_users(conn: Connection, operator_public_id: str) -> dict[str, str]:
    """Insert the three human users and attach each to the default desk.

    Returns a mapping from role name to the inserted ``public_id`` so
    individual tests can verify per-user alert counts and desk visibility.
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
        conn.execute(
            text(
                "INSERT INTO user_operator_memberships "
                "(public_id, user_public_id, operator_public_id, is_primary, "
                " timestamp, known_to, session_id, sequence_id) "
                "VALUES (:public_id, :user, :operator, 1, "
                " :timestamp, :known_to, 's', :sequence_id)"
            ),
            {
                "public_id": str(uuid7()),
                "user": public_id,
                "operator": operator_public_id,
                "timestamp": str(datetime.now(UTC)),
                "known_to": KNOWN_TO_MAX_STR,
                "sequence_id": len(out),
            },
        )
    return out


def _seed_additional_viewer(
    conn: Connection,
    username: str,
    operator_public_id: str | None,
) -> str:
    """Insert another active viewer with an optional default-desk membership."""
    public_id = str(uuid7())
    now = str(datetime.now(UTC))
    conn.execute(
        text(
            "INSERT INTO users "
            "(public_id, username, email, password_hash, role, is_active, "
            " created_at, timestamp, known_to, session_id, sequence_id) "
            "VALUES (:public_id, :username, :email, 'x', 'viewer', 1, "
            " :timestamp, :timestamp, :known_to, 's', 1)"
        ),
        {
            "public_id": public_id,
            "username": username,
            "email": f"{username}@snapper.local",
            "timestamp": now,
            "known_to": KNOWN_TO_MAX_STR,
        },
    )
    if operator_public_id is not None:
        conn.execute(
            text(
                "INSERT INTO user_operator_memberships "
                "(public_id, user_public_id, operator_public_id, is_primary, "
                " timestamp, known_to, session_id, sequence_id) "
                "VALUES (:public_id, :user, :operator, 1, "
                " :timestamp, :known_to, 's', 1)"
            ),
            {
                "public_id": str(uuid7()),
                "user": public_id,
                "operator": operator_public_id,
                "timestamp": now,
                "known_to": KNOWN_TO_MAX_STR,
            },
        )
    return public_id


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


def _seed_sealed_venue_event(conn: Connection, *, wallet: str) -> str:
    """Insert one append-only venue event whose wallet spelling must not change."""
    public_id = str(uuid7())
    observed_at = datetime(2026, 4, 20, 8, 0, tzinfo=UTC)
    conn.execute(
        text(
            "INSERT INTO venue_events "
            "(public_id, event_type, shard_key, wallet_public_id, command_public_id, "
            " exchange, instrument, mode, exchange_order_id, client_order_id, "
            " venue_client_id, side, status, fill_price, fill_size, cum_fill_size, "
            " fee, fee_asset, exec_id, trade_id, error, venue_timestamp, received_at, "
            " payload_json, liquidity_role, paired_group_id, "
            " timestamp, known_to, session_id, sequence_id) "
            "VALUES "
            "(:public_id, 'order_accepted', :shard_key, :wallet, NULL, "
            " 'kraken_futures', 'BTC-USD-PERP', 'paper', 'sealed-order', "
            " 'sealed-client-order', 'sealed-venue-client', 'buy', 'open', "
            " NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, :observed_at, "
            " :observed_at, NULL, 'unknown', NULL, :observed_at, :known_to, "
            " 'sealed-session', 1)"
        ),
        {
            "public_id": public_id,
            "shard_key": f"kraken_futures.BTC-USD-PERP.paper.{wallet}",
            "wallet": wallet,
            "observed_at": str(observed_at),
            "known_to": KNOWN_TO_MAX_STR,
        },
    )
    return public_id


def _seed_legacy_demo_execution(
    conn: Connection,
    *,
    wallet: str,
    order_public_id: str,
    scope_sequence: int,
    executed_at: datetime,
) -> str:
    """Insert one old-style demo execution with a truncated shared identity."""
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
            "(:public_id, :order_public_id, :wallet, NULL, "
            " 'kraken_futures', 'paper', :scope_sequence, "
            " 'exec-019fb1f7', 'trade-019fb1f7', "
            " 'buy', 'filled', 100.0, 1.0, 0.1, 'USD', "
            " :executed_at, 'taker', "
            " :executed_at, :known_to, :session_id, :sequence_id)"
        ),
        {
            "public_id": public_id,
            "order_public_id": order_public_id,
            "wallet": wallet,
            "scope_sequence": scope_sequence,
            "executed_at": str(executed_at),
            "known_to": KNOWN_TO_MAX_STR,
            "session_id": seed_demo._DEMO_SESSION_ID,
            "sequence_id": scope_sequence,
        },
    )
    return public_id


def _seed_legacy_demo_pnl_rows(
    conn: Connection,
    wallet: str,
    operator: str,
) -> None:
    """Recreate the old four-order, duplicate-identity, witness-free seed."""
    tracker = SequenceTracker()
    tracker._session_id = seed_demo._DEMO_SESSION_ID
    instrument = str(
        conn.execute(
            text(
                "SELECT i.public_id FROM instruments i "
                "JOIN symbols s ON s.public_id = i.symbol_public_id "
                "WHERE s.native_symbol = 'BTC-USD-PERP' "
                "AND i.exchange = 'kraken_futures' "
                "AND i.known_to = :known_to AND s.known_to = :known_to"
            ),
            {"known_to": KNOWN_TO_MAX_STR},
        ).scalar_one()
    )
    executed_at = datetime(2026, 4, 21, 9, 14, 32, tzinfo=UTC)
    order_public_ids: list[str] = []
    for index in range(4):
        filled = index < 2
        order_public_ids.append(
            seed_demo._insert_order(
                conn,
                tracker,
                wallet=wallet,
                operator=operator,
                instrument=instrument,
                side="buy",
                order_type="market",
                price=None,
                size=1.0,
                status="filled" if filled else "open",
                filled_size=1.0 if filled else 0.0,
                average_price=100.0 if filled else None,
                created_at=executed_at + timedelta(seconds=index),
            )
        )
    for scope_sequence, order_public_id in enumerate(order_public_ids[:2], start=1):
        _seed_legacy_demo_execution(
            conn,
            wallet=wallet,
            order_public_id=order_public_id,
            scope_sequence=scope_sequence,
            executed_at=executed_at + timedelta(seconds=scope_sequence),
        )


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


def _seed_other_desk_underlying_grant(
    conn: Connection,
    wallet: str,
    instrument: str,
    grantor: str,
) -> str:
    """Give another desk an underlying scope that covers one instrument."""
    now = str(datetime.now(UTC))
    operator = str(uuid7())
    underlying = str(uuid7())
    conn.execute(
        text(
            "INSERT INTO operators "
            "(public_id, label, description, timestamp, known_to, session_id, sequence_id) "
            "VALUES (:public_id, 'desk-b', NULL, :timestamp, :known_to, 's', 1)"
        ),
        {
            "public_id": operator,
            "timestamp": now,
            "known_to": KNOWN_TO_MAX_STR,
        },
    )
    conn.execute(
        text(
            "INSERT INTO underlying_assets "
            "(public_id, name, ticker, asset_class, sector, description, "
            " timestamp, known_to, session_id, sequence_id) "
            "VALUES (:public_id, :name, 'CL', 'commodity', NULL, NULL, "
            " :timestamp, :known_to, 's', 1)"
        ),
        {
            "public_id": underlying,
            "name": '{"en":"Crude oil"}',
            "timestamp": now,
            "known_to": KNOWN_TO_MAX_STR,
        },
    )
    conn.execute(
        text(
            "INSERT INTO instrument_underlying_mappings "
            "(public_id, instrument_public_id, underlying_public_id, relationship_type, "
            " contract_family, timestamp, known_to, session_id, sequence_id) "
            "VALUES (:public_id, :instrument, :underlying, 'derivative', "
            " 'CL', :timestamp, :known_to, 's', 1)"
        ),
        {
            "public_id": str(uuid7()),
            "instrument": instrument,
            "underlying": underlying,
            "timestamp": now,
            "known_to": KNOWN_TO_MAX_STR,
        },
    )
    conn.execute(
        text(
            "INSERT INTO wallet_operator_scope_grants "
            "(public_id, operator_public_id, wallet_public_id, granted_by_user_public_id, "
            " scope_kind, underlying_public_id, instrument_public_id, note, "
            " timestamp, known_to, session_id, sequence_id) "
            "VALUES (:public_id, :operator, :wallet, :grantor, "
            " 'underlying', :underlying, NULL, 'other desk CL family', "
            " :timestamp, :known_to, 's', 1)"
        ),
        {
            "public_id": str(uuid7()),
            "operator": operator,
            "wallet": wallet,
            "grantor": grantor,
            "underlying": underlying,
            "timestamp": now,
            "known_to": KNOWN_TO_MAX_STR,
        },
    )
    return operator


WALLET_SCOPED_SCHEMA_TABLES = frozenset(
    {
        "accrual_ledger",
        "ai_reviews",
        "alert_deliveries",
        "alert_events",
        "backtest_comparisons",
        "backtest_runs",
        "device_alert_prefs",
        "execution_annulment_visibility",
        "execution_annulments",
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
        "wallet_user_read_grants",
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
        "venue_events",
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

``wallet_operator_scope_grants`` is always populated with the required BTC
demo desk scope. ``ai_reviews`` remains reachable only when CLM6-NYMEX exists,
so the alias test seeds the optional commodity pair to exercise that table.

``execution_annulments`` is deliberately EXCLUDED. Its only writer is the
guarded ``record_execution_annulment`` maintenance surface, which exists to
repudiate a production booking defect after an operator diagnosed it; a demo
seed that manufactured corrections would be inventing evidence of a defect that
never happened. The table therefore stays empty in every seeded database, and
its wallet spelling is proven by the repository writer's own tests instead.

``execution_annulment_visibility`` is deliberately EXCLUDED for the same
reason as the manifest it observes: its rows are appended only by the guarded
correction protocol, and a demo seed that manufactured durability observations
would be inventing proof for corrections that never happened. It stays empty in
every seeded database, and its wallet spelling is proven by the repository
writer's own tests instead.

``wallet_user_read_grants`` is deliberately EXCLUDED. The demo seed already
gives its viewer wallet visibility through the operator plane, so a read grant
would add a second, redundant route to the same wallet and blur which plane a
demo visibility result came from. The read plane's own tests cover the grant
surface and its spelling.
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
    wallet-scoped table it discovers by introspection. ``executions``,
    ``venue_events`` and ``portfolio_spot_reconciliation_anchors`` carry
    that column but are immutable ledgers whose counter scope, shard
    identity and certified inventory are theorem inputs to the
    authoritative reconciliation verdict. These tests prove the exclusion
    holds against the PHYSICAL table the ORM maps.
    """

    def test_excluded_names_are_the_physical_ledger_tables(
        self,
        migrated_db: tuple[sa.Engine, str],
    ) -> None:
        """The exclusion set equals the mapped ledger tables AND is wallet-scoped.

        Given: the migrated schema,
        When: :func:`seed_demo._immutable_ledger_table_names` is compared
            against the physical schema,
        Then: it names exactly ``executions``, ``venue_events`` and the
            spot reconciliation anchor, and all carry a
            ``wallet_public_id`` column — so absent the skip the
            introspection loop WOULD rewrite them. Deriving the set from
            mapped models binds the guard to the physical table, not a
            call-site string that a rename could desync.
        """
        engine, _ = migrated_db
        excluded = seed_demo._immutable_ledger_table_names()
        assert excluded == {
            "executions",
            "portfolio_spot_reconciliation_anchors",
            "venue_events",
        }
        with engine.connect() as conn:
            wallet_scoped = _wallet_scoped_schema_tables(conn)
        assert excluded <= wallet_scoped, (
            "the excluded ledgers must actually carry wallet_public_id — otherwise "
            "the exclusion is vacuous and proves nothing"
        )

    def test_sealed_ledgers_survive_seeding_byte_identical(
        self,
        migrated_db: tuple[sa.Engine, str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Pre-seeded execution, venue-event and anchor rows remain untouched.

        Given: an alias-spelled paper wallet plus one row in each immutable
            wallet-scoped ledger, all carrying that exact alias,
        When: ``main()`` runs (which canonicalizes the root wallet and every
            NON-ledger reference),
        Then: all ledger rows keep their exact wallet spelling and the
            execution keeps its ``scope_sequence`` while the root wallet and
            seeder's OWN freshly inserted executions carry the canonical
            spelling and remain a contiguous ``1, 2`` scope.

        Reverting the ledger exclusion fails this test: the pre-seeded
        execution's wallet comes back canonical (proving it was rewritten)
        and its scope would then collide against the seed's own rows.
        """
        engine, db_url = migrated_db
        with engine.begin() as conn:
            _seed_paper_wallet(conn, ALIAS_WALLET)
            operator = _seed_default_operator(conn)
            _seed_required_instruments(conn)
            _seed_demo_users(conn, operator)
            sealed_execution_pid = _seed_sealed_execution(
                conn, wallet=ALIAS_WALLET, scope_sequence=7
            )
            sealed_event_pid = _seed_sealed_venue_event(conn, wallet=ALIAS_WALLET)
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
            event_wallet = conn.execute(
                text("SELECT wallet_public_id FROM venue_events WHERE public_id = :pid"),
                {"pid": sealed_event_pid},
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
        assert (
            event_wallet == ALIAS_WALLET
        ), f"the sealed venue event must be untouched; got {event_wallet!r}"
        assert root_wallet == CANONICAL_WALLET
        assert [row[1] for row in seeded_scope] == [1, 2]
        assert {row[0] for row in seeded_scope} == {CANONICAL_WALLET}

    def test_root_wallet_converges_on_a_skipped_rerun(
        self,
        migrated_db: tuple[sa.Engine, str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A re-run still canonicalizes the root wallet even when the seed skips.

        Given: a complete current-version demo seed whose root ``wallets``
            row and a NON-ledger reference are later put back into the alias
            spelling,
        When: ``main()`` runs again,
        Then: the complete-state preflight admits the idempotent skip and
            normalization commits, so the
            root wallet and the reference converge to the canonical spelling
            while NO new demo rows are inserted — a first-run alias is not
            frozen forever behind the idempotency early return.
        """
        engine, db_url = migrated_db
        with engine.begin() as conn:
            _seed_paper_wallet(conn, ALIAS_WALLET)
            operator = _seed_default_operator(conn)
            _seed_required_instruments(conn)
            _seed_demo_users(conn, operator)

        monkeypatch.setenv("DB_URL", db_url)
        assert seed_demo.main() == 0

        with engine.begin() as conn:
            conn.execute(
                text("UPDATE wallets SET public_id = :alias WHERE public_id = :canonical"),
                {
                    "alias": ALIAS_WALLET,
                    "canonical": CANONICAL_WALLET,
                },
            )
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
            demo_venue_events = conn.execute(
                text("SELECT COUNT(*) FROM venue_events WHERE session_id = :sid"),
                {"sid": seed_demo._DEMO_SESSION_ID},
            ).scalar()

        assert root == CANONICAL_WALLET
        assert reference == CANONICAL_WALLET
        assert demo_orders == 4, "the skip gate must not insert a second demo dataset"
        assert demo_venue_events == 2


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
            2 canonical ``fill_observed`` witnesses, and one pending
            ``ai_review`` are inserted; the function returns 0.
        """
        engine, db_url = migrated_db
        with engine.begin() as conn:
            _seed_paper_wallet(conn)
            operator = _seed_default_operator(conn)
            _seed_required_instruments(conn)
            _seed_optional_commodity_instruments(conn)
            _seed_demo_users(conn, operator)

        monkeypatch.setenv("DB_URL", db_url)
        rc = seed_demo.main()
        assert rc == 0

        with engine.connect() as conn:
            assert conn.execute(text("SELECT COUNT(*) FROM orders")).scalar() == 4
            assert conn.execute(text("SELECT COUNT(*) FROM executions")).scalar() == 2
            assert conn.execute(text("SELECT COUNT(*) FROM venue_events")).scalar() == 2
            assert conn.execute(text("SELECT COUNT(*) FROM positions")).scalar() == 2
            assert conn.execute(text("SELECT COUNT(*) FROM backtest_runs")).scalar() == 4
            assert conn.execute(text("SELECT COUNT(*) FROM ai_reviews")).scalar() == 1
            assert conn.execute(text("SELECT COUNT(*) FROM ai_delegates")).scalar() == 1
            assert conn.execute(text("SELECT COUNT(*) FROM ai_review_events")).scalar() == 2
            assert conn.execute(text("SELECT COUNT(*) FROM alert_events")).scalar() == 21
            assert (
                conn.execute(text("SELECT COUNT(*) FROM wallet_operator_scope_grants")).scalar()
                == 2
            )
            assert conn.execute(text("SELECT COUNT(*) FROM wallet_user_read_grants")).scalar() == 0
            for role in ("admin", "operator", "viewer"):
                row_count = conn.execute(
                    text(
                        "SELECT COUNT(*) FROM alert_events "
                        "WHERE user_public_id = (SELECT public_id FROM users WHERE role = :role)"
                    ),
                    {"role": role},
                ).scalar()
                assert row_count == 7, f"expected 7 alerts for role {role}, got {row_count}"

    def test_optional_ai_scope_refuses_cross_desk_underlying_overlap(
        self,
        migrated_db: tuple[sa.Engine, str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """The optional CLM6 grant obeys the same cross-kind ownership preflight."""
        engine, db_url = migrated_db
        with engine.begin() as conn:
            wallet = _seed_paper_wallet(conn)
            operator = _seed_default_operator(conn)
            _seed_required_instruments(conn)
            _seed_optional_commodity_instruments(conn)
            users = _seed_demo_users(conn, operator)
            clm6 = seed_demo._lookup_instrument(
                conn,
                "CLM6-NYMEX",
                "kraken_equities",
            )
            assert clm6 is not None
            other_operator = _seed_other_desk_underlying_grant(
                conn,
                wallet,
                clm6,
                users["admin"],
            )

        monkeypatch.setenv("DB_URL", db_url)
        with pytest.raises(
            RuntimeError,
            match="demo instrument scope overlaps.*another desk",
        ):
            seed_demo.main()

        with engine.connect() as conn:
            owners = {
                str(row[0])
                for row in conn.execute(
                    text("SELECT operator_public_id FROM wallet_operator_scope_grants")
                ).all()
            }
            assert owners == {other_operator}
            assert conn.execute(text("SELECT COUNT(*) FROM orders")).scalar_one() == 0
            assert conn.execute(text("SELECT COUNT(*) FROM ai_reviews")).scalar_one() == 0

    async def test_viewer_membership_inherits_demo_desk_wallet_without_read_grant(
        self,
        migrated_db: tuple[sa.Engine, str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """The required BTC scope makes membership sufficient on both read planes.

        Given: a fresh default desk with admin/operator/viewer members,
        When: the demo seed provisions its required BTC instrument scope,
        Then: the viewer sees the paper wallet through both the readable and
            operator-accessible repository planes, with zero personal grants.
        """
        engine, db_url = migrated_db
        with engine.begin() as conn:
            wallet = _seed_paper_wallet(conn)
            operator = _seed_default_operator(conn)
            _seed_required_instruments(conn)
            users = _seed_demo_users(conn, operator)

        monkeypatch.setenv("DB_URL", db_url)
        assert seed_demo.main() == 0

        with engine.connect() as conn:
            membership_count = conn.execute(
                text(
                    "SELECT COUNT(*) FROM user_operator_memberships "
                    "WHERE user_public_id = :viewer AND operator_public_id = :operator"
                ),
                {"viewer": users["viewer"], "operator": operator},
            ).scalar_one()
            scope_count = conn.execute(
                text(
                    "SELECT COUNT(*) FROM wallet_operator_scope_grants "
                    "WHERE wallet_public_id = :wallet AND operator_public_id = :operator"
                ),
                {"wallet": wallet, "operator": operator},
            ).scalar_one()
            personal_count = conn.execute(
                text("SELECT COUNT(*) FROM wallet_user_read_grants WHERE user_public_id = :viewer"),
                {"viewer": users["viewer"]},
            ).scalar_one()
        assert membership_count == 1
        assert scope_count == 1
        assert personal_count == 0

        repository = SQLAlchemyRepository(db_url.replace("sqlite://", "sqlite+aiosqlite://"))
        as_of = datetime.now(UTC) + timedelta(seconds=1)
        try:
            readable = await repository.list_readable_wallets_for_user(
                users["viewer"],
                [operator],
                as_of,
            )
            accessible = await repository.list_accessible_wallets_for_operators(
                [operator],
                as_of,
            )
        finally:
            await repository.engine.dispose()
        assert [row["public_id"] for row in readable] == [wallet]
        assert [row["public_id"] for row in accessible] == [wallet]

    def test_exact_viewer_username_drives_membership_check(
        self,
        migrated_db: tuple[sa.Engine, str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """An earlier unrelated viewer cannot replace the seeded viewer identity."""
        engine, db_url = migrated_db
        with engine.begin() as conn:
            _seed_paper_wallet(conn)
            operator = _seed_default_operator(conn)
            _seed_required_instruments(conn)
            _seed_additional_viewer(conn, "observer", None)
            _seed_demo_users(conn, operator)

        monkeypatch.setenv("DB_URL", db_url)
        assert seed_demo.main() == 0

    def test_missing_seeded_admin_or_viewer_fails_before_demo_writes(
        self,
        migrated_db: tuple[sa.Engine, str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Desk visibility requires both exact seeded human identities.

        Given: A paper wallet, default desk, and instruments but no seeded
            admin or viewer identity,
        When: The demo seed prepares viewer desk visibility,
        Then: It fails before inserting a scope grant or any P&L row.
        """
        engine, db_url = migrated_db
        with engine.begin() as conn:
            _seed_paper_wallet(conn)
            _seed_default_operator(conn)
            _seed_required_instruments(conn)

        monkeypatch.setenv("DB_URL", db_url)
        with pytest.raises(RuntimeError, match="active admin/viewer users not found"):
            seed_demo.main()
        with engine.connect() as conn:
            assert (
                conn.execute(text("SELECT COUNT(*) FROM wallet_operator_scope_grants")).scalar_one()
                == 0
            )
            assert conn.execute(text("SELECT COUNT(*) FROM orders")).scalar_one() == 0

    def test_existing_viewer_without_membership_fails_with_attach_path(
        self,
        migrated_db: tuple[sa.Engine, str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """The demo seed never attaches an established viewer by username.

        Given: active human users but the viewer's default-desk membership
            has been removed,
        When: the demo seed tries to provision desk visibility,
        Then: it fails before inserting scope or P&L rows and names the
            supported runtime attachment path.
        """
        engine, db_url = migrated_db
        with engine.begin() as conn:
            _seed_paper_wallet(conn)
            operator = _seed_default_operator(conn)
            _seed_required_instruments(conn)
            users = _seed_demo_users(conn, operator)
            _seed_additional_viewer(conn, "observer", operator)
            conn.execute(
                text("DELETE FROM user_operator_memberships WHERE user_public_id = :viewer"),
                {"viewer": users["viewer"]},
            )

        monkeypatch.setenv("DB_URL", db_url)
        with pytest.raises(
            RuntimeError,
            match=r"POST /api/auth/desks/\{operator_public_id\}/members/\{username\}",
        ):
            seed_demo.main()
        with engine.connect() as conn:
            assert conn.execute(text("SELECT COUNT(*) FROM orders")).scalar_one() == 0
            assert (
                conn.execute(text("SELECT COUNT(*) FROM wallet_operator_scope_grants")).scalar_one()
                == 0
            )

    def test_inactive_optional_alert_recipient_is_skipped(
        self,
        migrated_db: tuple[sa.Engine, str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """An absent optional alert recipient does not block the canonical demo.

        Given: Exact seeded admin and viewer identities but an inactive
            operator recipient,
        When: The demo alert fanout is inserted,
        Then: The two active recipients receive their seven alerts and the
            remaining demo state still commits.
        """
        engine, db_url = migrated_db
        with engine.begin() as conn:
            _seed_paper_wallet(conn)
            operator = _seed_default_operator(conn)
            _seed_required_instruments(conn)
            _seed_demo_users(conn, operator)
            conn.execute(text("UPDATE users SET is_active = 0 WHERE username = 'operator'"))

        monkeypatch.setenv("DB_URL", db_url)
        assert seed_demo.main() == 0
        with engine.connect() as conn:
            assert conn.execute(text("SELECT COUNT(*) FROM alert_events")).scalar_one() == 14
            assert conn.execute(text("SELECT COUNT(*) FROM orders")).scalar_one() == 4

    async def test_seeded_executions_have_injective_fill_witnesses(
        self,
        migrated_db: tuple[sa.Engine, str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Two seeded executions certify against two distinct venue witnesses.

        Given: the required demo wallet, operator, and BTC/ETH instruments,
        When: ``main()`` inserts its two completed executions,
        Then: each row carries the full execution UUID in distinct
            ``exec_id`` and ``trade_id`` values, exactly one economically
            identical ``fill_observed`` event owns that identity, and the
            real P&L prefix loader consumes both witnesses injectively.
        """
        engine, db_url = migrated_db
        with engine.begin() as conn:
            _seed_paper_wallet(conn)
            operator = _seed_default_operator(conn)
            _seed_required_instruments(conn)
            _seed_demo_users(conn, operator)

        monkeypatch.setenv("DB_URL", db_url)
        assert seed_demo.main() == 0

        with engine.connect() as conn:
            witness_rows = (
                conn.execute(
                    text(
                        "SELECT "
                        "e.public_id AS execution_public_id, "
                        "e.wallet_public_id AS execution_wallet, "
                        "e.exchange AS execution_exchange, e.mode AS execution_mode, "
                        "e.exec_id, e.trade_id, e.side AS execution_side, "
                        "e.status AS execution_status, e.price AS execution_price, "
                        "e.size AS execution_size, e.fee AS execution_fee, "
                        "e.executed_at, o.client_order_id, o.exchange_order_id, "
                        "v.public_id AS venue_event_public_id, v.event_type, v.shard_key, "
                        "v.instrument AS event_instrument, "
                        "v.wallet_public_id AS event_wallet, "
                        "v.exchange AS event_exchange, v.mode AS event_mode, "
                        "v.client_order_id AS event_client_order_id, "
                        "v.exchange_order_id AS event_exchange_order_id, "
                        "v.venue_client_id, v.exec_id AS event_exec_id, "
                        "v.trade_id AS event_trade_id, v.side AS event_side, "
                        "v.status AS event_status, v.fill_price, v.fill_size, "
                        "v.cum_fill_size, v.fee AS event_fee, v.venue_timestamp, "
                        "v.received_at, v.timestamp AS event_timestamp, "
                        "v.known_to, v.session_id, v.sequence_id "
                        "FROM executions e "
                        "JOIN orders o ON o.public_id = e.order_public_id "
                        "JOIN venue_events v ON v.wallet_public_id = e.wallet_public_id "
                        "AND v.exchange = e.exchange AND v.mode = e.mode "
                        "AND v.exec_id = e.exec_id AND v.trade_id = e.trade_id "
                        "WHERE e.session_id = :session_id "
                        "ORDER BY e.scope_sequence ASC"
                    ),
                    {"session_id": seed_demo._DEMO_SESSION_ID},
                )
                .mappings()
                .all()
            )
            venue_event_count = conn.execute(
                text("SELECT COUNT(*) FROM venue_events WHERE session_id = :session_id"),
                {"session_id": seed_demo._DEMO_SESSION_ID},
            ).scalar_one()

        assert venue_event_count == 2
        assert len(witness_rows) == 2
        assert len({row["exec_id"] for row in witness_rows}) == 2
        assert len({row["trade_id"] for row in witness_rows}) == 2
        assert len({row["venue_event_public_id"] for row in witness_rows}) == 2
        for row in witness_rows:
            assert row["exec_id"] == f"exec-{row['execution_public_id']}"
            assert row["trade_id"] == f"trade-{row['execution_public_id']}"
            assert row["event_exec_id"] == row["exec_id"]
            assert row["event_trade_id"] == row["trade_id"]
            assert row["event_type"] == "fill_observed"
            assert row["event_wallet"] == row["execution_wallet"]
            assert row["event_exchange"] == row["execution_exchange"]
            assert row["event_mode"] == row["execution_mode"]
            assert row["shard_key"] == seed_demo.compute_shard_key(
                instrument=str(row["event_instrument"]),
                exchange=seed_demo.ExchangeEnum(str(row["event_exchange"])),
                mode=seed_demo.ExecutionModeEnum(str(row["event_mode"])),
                wallet_public_id=str(row["event_wallet"]),
                strategy_tag=None,
            )
            assert row["event_client_order_id"] == row["client_order_id"]
            assert row["event_exchange_order_id"] == row["exchange_order_id"]
            assert row["venue_client_id"] == f"venue-{row['client_order_id']}"
            assert row["event_side"] == row["execution_side"]
            assert row["event_status"] == row["execution_status"]
            assert row["fill_price"] == row["execution_price"]
            assert row["fill_size"] == row["execution_size"]
            assert row["cum_fill_size"] == row["execution_size"]
            assert row["event_fee"] == row["execution_fee"]
            assert row["venue_timestamp"] == row["executed_at"]
            assert row["received_at"] == row["executed_at"]
            assert row["event_timestamp"] == row["executed_at"]
            assert row["known_to"] == KNOWN_TO_MAX_STR
            assert row["session_id"] == seed_demo._DEMO_SESSION_ID
        assert [row["sequence_id"] for row in witness_rows] == [1, 1]

        repository = SQLAlchemyRepository(db_url.replace("sqlite://", "sqlite+aiosqlite://"))
        try:
            prefix = await repository.get_pnl_timeline_execution_prefix(
                str(witness_rows[0]["execution_wallet"]),
                "paper",
                None,
            )
        finally:
            await repository.engine.dispose()

        assert prefix["watermarks"] == {"kraken_futures": 2}
        assert prefix["annulments"] == []
        assert {row["public_id"]: row["shard_key"] for row in prefix["executions"]} == {
            str(row["execution_public_id"]): str(row["shard_key"]) for row in witness_rows
        }

    @pytest.mark.parametrize(
        ("corruption", "expected_exact", "expected_canonical_shards"),
        [
            ("duplicate_first_witness", 1, 1),
            ("wrong_shard", 2, 1),
        ],
    )
    def test_corrupt_fill_witness_state_refuses_idempotent_skip(
        self,
        migrated_db: tuple[sa.Engine, str],
        monkeypatch: pytest.MonkeyPatch,
        corruption: str,
        expected_exact: int,
        expected_canonical_shards: int,
    ) -> None:
        """A count-complete but non-injective or cross-shard seed fails loud.

        Given: a freshly complete demo seed whose two event rows are then
            corrupted either into duplicate witnesses for one execution or
            by moving one witness to a non-canonical shard,
        When: ``main()`` performs its locked preflight,
        Then: it refuses the idempotent skip even though the raw row counts
            remain 4 orders, 2 executions and 2 venue events.
        """
        engine, db_url = migrated_db
        with engine.begin() as conn:
            _seed_paper_wallet(conn)
            operator = _seed_default_operator(conn)
            _seed_required_instruments(conn)
            _seed_demo_users(conn, operator)

        monkeypatch.setenv("DB_URL", db_url)
        assert seed_demo.main() == 0

        with engine.begin() as conn:
            if corruption == "duplicate_first_witness":
                conn.execute(
                    text(
                        "UPDATE venue_events SET "
                        "shard_key = (SELECT shard_key FROM venue_events ORDER BY id LIMIT 1), "
                        "instrument = (SELECT instrument FROM venue_events ORDER BY id LIMIT 1), "
                        "exchange_order_id = (SELECT exchange_order_id FROM venue_events "
                        " ORDER BY id LIMIT 1), "
                        "client_order_id = (SELECT client_order_id FROM venue_events "
                        " ORDER BY id LIMIT 1), "
                        "venue_client_id = (SELECT venue_client_id FROM venue_events "
                        " ORDER BY id LIMIT 1), "
                        "side = (SELECT side FROM venue_events ORDER BY id LIMIT 1), "
                        "status = (SELECT status FROM venue_events ORDER BY id LIMIT 1), "
                        "fill_price = (SELECT fill_price FROM venue_events ORDER BY id LIMIT 1), "
                        "fill_size = (SELECT fill_size FROM venue_events ORDER BY id LIMIT 1), "
                        "cum_fill_size = (SELECT cum_fill_size FROM venue_events "
                        " ORDER BY id LIMIT 1), "
                        "fee = (SELECT fee FROM venue_events ORDER BY id LIMIT 1), "
                        "fee_asset = (SELECT fee_asset FROM venue_events ORDER BY id LIMIT 1), "
                        "exec_id = (SELECT exec_id FROM venue_events ORDER BY id LIMIT 1), "
                        "trade_id = (SELECT trade_id FROM venue_events ORDER BY id LIMIT 1), "
                        "venue_timestamp = (SELECT venue_timestamp FROM venue_events "
                        " ORDER BY id LIMIT 1), "
                        "received_at = (SELECT received_at FROM venue_events ORDER BY id LIMIT 1), "
                        "timestamp = (SELECT timestamp FROM venue_events ORDER BY id LIMIT 1) "
                        "WHERE id = (SELECT MAX(id) FROM venue_events)"
                    )
                )
            else:
                conn.execute(
                    text(
                        "UPDATE venue_events SET shard_key = 'corrupt.cross-scope' "
                        "WHERE id = (SELECT MAX(id) FROM venue_events)"
                    )
                )

        with engine.connect() as conn:
            state = seed_demo._read_demo_pnl_seed_state(conn)
        assert state == seed_demo._DemoPnlSeedState(
            orders=4,
            executions=2,
            venue_events=2,
            canonical_execution_ids=2,
            exact_witnesses=expected_exact,
            canonical_shards=expected_canonical_shards,
        )

        with pytest.raises(
            RuntimeError,
            match=f"exact_witnesses={expected_exact} canonical_shards={expected_canonical_shards}",
        ):
            seed_demo.main()

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
            operator = _seed_default_operator(conn)
            _seed_required_instruments(conn)
            _seed_optional_commodity_instruments(conn)
            _seed_demo_users(conn, operator)

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
            operator = _seed_default_operator(conn)
            _seed_required_instruments(conn)
            _seed_demo_users(conn, operator)

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
            operator = _seed_default_operator(conn)
            _seed_required_instruments(conn)
            _seed_optional_commodity_instruments(conn)
            _seed_demo_users(conn, operator)

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

    def test_legacy_demo_seed_refuses_silent_skip(
        self,
        migrated_db: tuple[sa.Engine, str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """An old witness-free append-only ledger requires a local DB rebuild.

        Given: the exact legacy P&L shape of four demo orders, two
            executions sharing the old eight-character ``exec_id`` and
            ``trade_id`` suffix, and zero ``fill_observed`` rows,
        When: ``main()`` is rerun,
        Then: it raises an actionable error instead of reporting a
            successful idempotent skip, because immutable execution
            identities cannot be repaired safely in place.
        """
        engine, db_url = migrated_db
        with engine.begin() as conn:
            wallet = _seed_paper_wallet(conn)
            operator = _seed_default_operator(conn)
            _seed_required_instruments(conn)
            _seed_legacy_demo_pnl_rows(conn, wallet, operator)

        monkeypatch.setenv("DB_URL", db_url)
        with pytest.raises(
            RuntimeError,
            match=(
                "legacy or incomplete demo P&L seed.*"
                "orders=4 executions=2 fill_observed=0 "
                "canonical_execution_ids=0 exact_witnesses=0.*"
                "append-only"
            ),
        ):
            seed_demo.main()

        with engine.connect() as conn:
            state = seed_demo._read_demo_pnl_seed_state(conn)
            distinct_exec_ids = conn.execute(
                text(
                    "SELECT COUNT(DISTINCT exec_id) FROM executions WHERE session_id = :session_id"
                ),
                {"session_id": seed_demo._DEMO_SESSION_ID},
            ).scalar_one()

        assert state == seed_demo._DemoPnlSeedState(
            orders=4,
            executions=2,
            venue_events=0,
            canonical_execution_ids=0,
            exact_witnesses=0,
            canonical_shards=0,
        )
        assert distinct_exec_ids == 1

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
            _seed_demo_users(conn, operator)
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
            operator = _seed_default_operator(conn)
            _seed_required_instruments(conn)
            _seed_demo_users(conn, operator)
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
            operator = _seed_default_operator(conn)
            _seed_required_instruments(conn)
            _seed_demo_users(conn, operator)
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
