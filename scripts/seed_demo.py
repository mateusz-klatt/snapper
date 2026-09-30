"""One-shot demo data seeder for README/LinkedIn screenshots.

Inserts a small but realistic set of facts on top of an already-migrated
``dev`` profile database (paper wallet, default operator, instruments).
Idempotent: skips when ``orders`` already contains rows stamped with this
script's stable demo ``session_id``, so unrelated local rows do not block
the demo data from being inserted.

Demo set as of 2026-05-05 (entries chosen to match REAL Kraken Futures
``market_snapshots`` so unrealized P&L is plausible against the live mark):

- LONG BTC-USD-PERP opened 2026-04-21 @ $76,820.50 (mid-70k post-dip);
  current mark $79,781 → unrealized +$420.79 on 0.1421 BTC.
- SHORT ETH-USD-PERP opened 2026-04-25 @ $2,820.40 (pre-crash level);
  current mark $2,345.60 → unrealized +$2,231.56 on 4.7 ETH = +16.8% on
  notional $13,256. Relative-value: ETH lagged BTC's bounce hard.
- One open limit buy BTC + one canceled stop sell ETH
- Two completed BTC futures backtest runs, plus CLM6/GCM6 Kraken Equities
  backtest rows when those instruments are available, so the
  Backtests/Compare page renders meaningful rows.

Bypasses fact -> projection chain: positions are inserted directly so the
screenshots don't require a running paper executor / strategy runtime.
This is a screenshot tool, not a production loader.

SQLite only: ``main`` opens a SYNCHRONOUS engine, while every documented
PostgreSQL ``DB_URL`` is ``postgresql+asyncpg://``, so a server database
was never reachable here (see :func:`_require_sqlite_dialect`). The
script refuses any other dialect before touching a row rather than
pretending to support one it cannot serialize allocations against.
"""

import hashlib
import json
import secrets
from dataclasses import dataclass
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from typing import cast
from uuid import UUID
from uuid import uuid5
from uuid import uuid7

import bcrypt
from sqlalchemy import create_engine
from sqlalchemy import inspect
from sqlalchemy import text
from sqlalchemy.engine import Connection
from sqlalchemy.pool import NullPool

from snapper.application.engine.service import compute_shard_key
from snapper.config.bootstrap import BootstrapSettingsLoader
from snapper.core.types import ExchangeEnum
from snapper.core.types import ExecutionModeEnum
from snapper.core.types import OrderExchange
from snapper.data.models import Execution
from snapper.data.models import PortfolioSpotReconciliationAnchor
from snapper.data.models import VenueEvent
from snapper.messaging.infrastructure.publisher import SequenceTracker

KNOWN_TO_MAX_STR = "9999-12-31 23:59:59.000000"

_DEMO_SESSION_ID = "019e0500-0000-0000-0000-0000000d3e70"
"""Stable session_id stamped on every row this script inserts.

The skip gate in ``main`` checks for rows carrying this exact session_id
to detect a previous seed run. Manually-inserted orders / e2e-test rows
use different session_ids, so they no longer trip the skip gate and
the demo data lands as a complete additive set on top of them.
"""

_DEMO_VENUE_EVENT_NAMESPACE = UUID("019e0500-0000-0000-0000-0000000d3e71")
"""Stable namespace for execution-derived demo venue-event identities."""


@dataclass(frozen=True, slots=True)
class _SeedExecutionFill:
    """Exact execution facts used to append its canonical fill witness."""

    public_id: str
    order_public_id: str
    wallet_public_id: str
    exchange: str
    mode: str
    instrument: str
    exec_id: str
    trade_id: str
    side: str
    status: str
    price: float
    size: float
    fee: float
    executed_at: datetime


@dataclass(frozen=True, slots=True)
class _DemoPnlSeedState:
    """Counts proving whether the append-only demo P&L seed is complete."""

    orders: int
    executions: int
    venue_events: int
    canonical_execution_ids: int
    exact_witnesses: int
    canonical_shards: int


@dataclass(frozen=True, slots=True)
class _DemoInstrumentScope:
    """One exact desk grant requested by the additive demo seed."""

    operator: str
    wallet: str
    grantor: str
    instrument: str
    note: str


@dataclass(frozen=True, slots=True)
class _DemoAlertEvent:
    """One demo notification exactly as the iOS Alerts tab reads it back.

    Groups the alert's own domain facts so the writer takes the row as
    one value instead of thirteen loose keyword arguments. Provenance
    (``session_id``, ``sequence_id``, ``known_to``) is NOT held here: it
    comes from the ``SequenceTracker`` the writer is handed, mirroring
    the production sidecar bypass writer.

    Attributes:
        user_public_id: Recipient whose Alerts tab shows this row.
        alert_type: ``AlertType`` literal driving the iOS presentation.
        priority: Delivery urgency (``medium`` / ``high``).
        is_safety_critical: Whether the alert bypasses quiet hours.
        title: Short headline rendered in the alert list.
        body: Human-readable detail line.
        payload: Structured detail serialized to JSON in the row.
        dedup_key: Stable key collapsing repeat deliveries.
        thread_key: Grouping key threading related alerts together.
        source_topic: ZMQ topic a production frame would have carried.
        occurred_at: Domain time of the alert, also its row timestamp.
        wallet_public_id: Wallet the alert concerns, when wallet-scoped.
        operator_public_id: Desk the alert concerns, when desk-scoped.
    """

    user_public_id: str
    alert_type: str
    priority: str
    is_safety_critical: bool
    title: str
    body: str
    payload: dict[str, object]
    dedup_key: str
    thread_key: str
    source_topic: str
    occurred_at: datetime
    wallet_public_id: str | None = None
    operator_public_id: str | None = None


@dataclass(frozen=True, slots=True)
class _DemoBacktestRun:
    """One finished backtest as the Backtests/Compare page renders it.

    Holds only the run's domain facts. The demo-fixed execution columns
    (``mode``, ``created_by_user_id``, ``execution_mode``, ``fill_model``,
    slippage and commission) stay literals in the writer's INSERT because
    no caller varies them, and provenance comes from the tracker.

    Attributes:
        wallet: Wallet the run is booked against.
        instrument: Instrument public_id the run traded.
        exchange: Venue the instrument belongs to.
        strategy_name: Strategy class name shown in the run list.
        strategy_params: Strategy tuning serialized to JSON in the row.
        timeframe: Candle timeframe the run consumed.
        start: First simulated date, also the row's timestamp.
        end: Last simulated date.
        initial_cash: Starting equity of the simulation.
        status: Terminal run status (``completed``).
        started_at: Wall-clock time the run began.
        completed_at: Wall-clock finish time, or ``None`` while unfinished.
    """

    wallet: str
    instrument: str
    exchange: str
    strategy_name: str
    strategy_params: dict[str, object]
    timeframe: str
    start: datetime
    end: datetime
    initial_cash: float
    status: str
    started_at: datetime
    completed_at: datetime | None


_EMPTY_DEMO_PNL_SEED = _DemoPnlSeedState(0, 0, 0, 0, 0, 0)
"""State of a database that has never received the demo P&L rows."""

_COMPLETE_DEMO_PNL_SEED = _DemoPnlSeedState(4, 2, 2, 2, 2, 2)
"""State produced by the current atomic demo P&L seed."""


def _sync_db_url(url: str) -> str:
    """Convert async sqlite URL to sync for direct engine use."""
    if "aiosqlite" in url:
        return url.replace("sqlite+aiosqlite://", "sqlite://")
    return url


def _require_sqlite_dialect(conn: Connection) -> None:
    """Refuse any backend other than SQLite, before a single row is written.

    The PostgreSQL branch this guard replaces could never execute:
    ``main`` opens a SYNCHRONOUS ``create_engine``, while every
    documented PostgreSQL ``DB_URL`` is ``postgresql+asyncpg://`` and
    ``_sync_db_url`` rewrites only the SQLite driver — so a real
    deployment's URL died at ``engine.connect()`` with ``MissingGreenlet``
    long before any fence ran. Reaching that branch required hand-writing
    a ``psycopg2`` URL the application itself cannot open.

    Keeping it alive meant maintaining a fence key, a transaction
    isolation level, and a native-UUID conversion whose correctness no
    test could establish without a real server — and a mocked connection
    proves nothing about any of them, because it returns whatever the
    mock was told to return. Refusing outright removes the unprovable
    claim instead of making it subtly correct, and turns a confusing
    ``MissingGreenlet`` into a stated limitation. Restoring PostgreSQL
    seeding is deliberate separate work: an asyncpg -> psycopg2 rewrite
    in ``_sync_db_url`` plus a real PostgreSQL integration test.

    Args:
        conn: The script's single connection, freshly opened.

    Raises:
        RuntimeError: When the connection is not SQLite.
    """
    dialect = conn.dialect.name
    if dialect != "sqlite":
        raise RuntimeError(
            f"seed_demo supports sqlite only — refusing dialect={dialect}. "
            "This is a screenshot tool: point DB_URL at the local dev SQLite "
            "database, or load a server database through the application."
        )


def _canonical_wallet(wallet: str) -> str:
    """Canonicalize the paper wallet identity ONCE, for every consumer.

    ``insert_execution`` canonicalizes with ``str(UUID(...))`` and uses
    that ONE spelling for its fence key, its ``max+1`` read, and the
    persisted row. This script must agree exactly, because SQLite's
    ``UUIDColumn`` is a ``String(36)`` storing whatever text it is given:
    an alias-spelled wallet (an uppercase hand-inserted row) would open a
    SECOND counter scope that the unique index reads as different text.
    The seed's row and a later production fill would then BOTH commit
    ``scope_sequence = 1`` — no error is raised, the ledger simply splits
    in two and the canonical watermark capture never sees the seed's rows.

    Canonicalizing at the single lookup site, rather than at each
    consumer, is what makes that agreement structural: every fence,
    query, and write downstream binds this one returned value.

    Args:
        wallet: The ``public_id`` text read from the ``wallets`` row.

    Returns:
        The canonical lowercase-hyphenated UUID spelling.

    Raises:
        RuntimeError: When the stored identity is not a UUID. Production
            ingest refuses such a wallet outright
            (``invalid_execution_wallet_identity``), so seeding rows under
            it would build a scope no live fill could ever extend.
    """
    try:
        return str(UUID(wallet))
    except ValueError as exc:
        raise RuntimeError(f"paper wallet public_id is not a valid UUID: {wallet!r}") from exc


def _acquire_execution_fence(conn: Connection) -> None:
    """Serialize the seed's fused ``max+1`` execution inserts with live writers.

    ``_insert_execution`` allocates ``scope_sequence`` as a fused
    ``INSERT ... SELECT COALESCE(MAX(scope_sequence), 0) + 1`` — sound
    only while no other connection can allocate concurrently. "One
    connection" describes this script, not the database: a concurrently
    running service writer takes the repository's per-wallet execution
    fence and would race the seed's read-then-write, rejecting either
    the seed or a published live fill on ``uq_executions_scope_sequence``.
    ``BEGIN IMMEDIATE`` takes SQLite's database write reservation — the
    same primitive ``insert_execution`` opens its own transaction with —
    and must run before the script's first DML so the reservation covers
    every allocation through to the script's single commit.

    No per-dialect branching remains: ``main`` refuses every non-SQLite
    dialect via :func:`_require_sqlite_dialect` before reaching this
    helper, so SQLite is the only backend whose fence this script can
    take — and the only one whose fence a test can actually prove.

    Args:
        conn: The script's single connection (one transaction, committed
            once at the end of ``main``).
    """
    conn.execute(text("BEGIN IMMEDIATE"))


def _immutable_ledger_table_names() -> frozenset[str]:
    """Return the PHYSICAL table names the seeder must never rewrite.

    ``executions``, ``venue_events`` and
    ``portfolio_spot_reconciliation_anchors`` are append-only ledgers:
    an execution row's
    ``(wallet_public_id, exchange, mode, scope_sequence)`` tuple and an
    event's shard key are one fused certification identity; an anchor
    row's certified inventory is another theorem input to the
    authoritative reconciliation verdict. Re-keying any of them by a
    screenshot tool would either split a counter scope, detach a durable
    witness from its shard, or restate sealed inventory — the exact
    false-authoritative failure this program exists to prevent.
    Execution-scope alias normalization is migration 0029's job
    exclusively (``_normalize_wallet_aliases``), taken under the
    migration write fence; the seeder must not duplicate it.

    The names are read from the mapped models' ``__tablename__`` rather
    than hard-coded, so the exclusion binds the PHYSICAL table the ORM
    maps regardless of how the ledger is expressed — a future
    ``__tablename__`` rename moves the guard with it. The caller compares
    these against the physical names ``inspect(conn).get_table_names()``
    returns from the live schema, which is the same physical identity, so
    no alias, mapper, or Table-object spelling can slip a ledger table
    past the skip.
    """
    return frozenset(
        {
            Execution.__tablename__,
            PortfolioSpotReconciliationAnchor.__tablename__,
            VenueEvent.__tablename__,
        }
    )


def _normalize_wallet_identity(conn: Connection, stored: str, canonical: str) -> None:
    """Rewrite a noncanonical root wallet row and its references to canonical.

    :func:`_canonical_wallet` gives every row this script INSERTS the one
    canonical spelling, but the root ``wallets`` row keeps whatever text
    was hand-inserted (SQLite's ``UUIDColumn`` is a ``String(36)`` that
    stores verbatim). Nothing downstream reads the root row's spelling
    during the seed, yet ``repository.py`` resolves non-admin scope by
    joining ``wallets.public_id`` against the ``wallet_public_id`` of the
    active scope grants BY TEXT: with the root row left uppercase and the
    seeded grant written canonical, that join no longer matches and the
    seeded wallet becomes invisible to every non-admin operator.

    Canonicalizing only the seeded child rows would therefore make the
    guarantee hold at the INSERT call sites while the physical root row
    still diverged. This closes it at the primitive instead: after this
    runs, the ``wallets`` row and every existing ``wallet_public_id``
    reference carry the canonical spelling, so the theorem "the seed's
    wallet is addressable by exactly one spelling everywhere" holds
    against the database, not merely against the rows ``main`` happens to
    write. There are no database-level foreign keys on ``wallets`` (every
    wallet link is a text join), so the references must be rewritten
    explicitly; the schema is introspected rather than hard-coded so a
    future wallet-scoped table cannot silently escape the rewrite.

    Runs under the caller's ``BEGIN IMMEDIATE`` write reservation, so the
    root row and all references move to the canonical spelling atomically
    with the seed's inserts — a concurrent reader never observes a split
    identity.

    An already-canonical ``stored`` (the production ``make migrate-dev``
    case, where the wallet was created via ``str(uuid7())``) is a no-op:
    the update statements would touch nothing, so the work is skipped.

    The append-only ledgers named by :func:`_immutable_ledger_table_names`
    are EXCLUDED from the reference rewrite even though they carry a
    ``wallet_public_id`` column: re-keying a sealed execution or anchor
    row would corrupt certification input (see that helper). Their alias
    normalization is migration 0029's sole responsibility, so the seeder
    issues no ``UPDATE`` against either physical table.

    Args:
        conn: The script's single connection, already holding the fence.
        stored: The verbatim ``public_id`` read from the ``wallets`` row.
        canonical: The :func:`_canonical_wallet` spelling every seeded row
            binds.
    """
    if stored == canonical:
        return
    ledger_tables = _immutable_ledger_table_names()
    inspector = inspect(conn)
    for table_name in inspector.get_table_names():
        if table_name in ledger_tables:
            continue
        columns = {column["name"] for column in inspector.get_columns(table_name)}
        if "wallet_public_id" not in columns:
            continue
        conn.execute(
            text(
                f"UPDATE {table_name} SET wallet_public_id = :canon "
                "WHERE wallet_public_id = :stored"
            ),
            {"canon": canonical, "stored": stored},
        )
    conn.execute(
        text("UPDATE wallets SET public_id = :canon WHERE public_id = :stored"),
        {"canon": canonical, "stored": stored},
    )


def _lookup_instrument(conn: Connection, native_symbol: str, exchange: str) -> str | None:
    """Find the active instrument public_id for a (symbol, exchange) pair."""
    row = conn.execute(
        text(
            "SELECT i.public_id FROM instruments i "
            "JOIN symbols s ON i.symbol_public_id = s.public_id "
            "WHERE s.native_symbol = :sym AND i.exchange = :ex "
            "AND i.known_to = :ka AND s.known_to = :ka LIMIT 1"
        ),
        {"sym": native_symbol, "ex": exchange, "ka": KNOWN_TO_MAX_STR},
    ).first()
    return row[0] if row else None


def _lookup_paper_wallet(conn: Connection) -> str | None:
    """Find the paper wallet public_id seeded from dev.toml."""
    row = conn.execute(
        text(
            "SELECT public_id FROM wallets "
            "WHERE label = 'paper' AND is_paper = 1 "
            "ORDER BY id ASC LIMIT 1"
        ),
    ).first()
    return row[0] if row else None


def _lookup_seeded_user(conn: Connection, username: str, role: str) -> str | None:
    """Find one exact active seeded human identity.

    The demo alerts seed loops over the three seeded roles so each
    iOS Alerts tab renders a non-empty list when the matching user
    is the authenticated principal. Username and role are both bound:
    another human with the same role must never stand in for the
    profile-owned ``admin``, ``operator`` or ``viewer`` account.
    The lookup intentionally compares
    ``known_to`` with a strftime-derived sentinel rather than
    ``KNOWN_TO_MAX_STR`` because seeded users are written with a
    timezone-aware string (``9999-12-31 23:59:59+00:00``) while other
    insert paths use the bare ``9999-12-31 23:59:59.000000`` form.
    """
    row = conn.execute(
        text(
            "SELECT public_id FROM users "
            "WHERE username = :username AND role = :role AND is_active = 1 "
            "AND known_to > strftime('%Y-%m-%dT%H:%M:%S','now') "
            "ORDER BY id ASC LIMIT 1"
        ),
        {"username": username, "role": role},
    ).first()
    return row[0] if row else None


def _lookup_operator(conn: Connection) -> str | None:
    """Find the default operator public_id seeded by multi-tenant bootstrap."""
    row = conn.execute(
        text("SELECT public_id FROM operators WHERE label = 'default' ORDER BY id ASC LIMIT 1"),
    ).first()
    return row[0] if row else None


def _ensure_demo_viewer_desk_scope(
    conn: Connection,
    tracker: SequenceTracker,
    *,
    operator: str,
    wallet: str,
    instrument: str,
) -> int:
    """Ensure the seeded viewer can see the demo wallet through its desk.

    Membership supplies the viewer's desk identity; one operator scope
    grant on the wallet supplies that desk's wallet visibility. This
    helper never creates a personal read grant and never silently attaches
    an existing user: a missing viewer membership must be repaired through
    the supported administration endpoint and takes effect on next login.

    An existing instrument or underlying grant from the same operator
    that covers the required instrument already proves visibility and is
    left untouched. An overlap owned by a different operator fails loud
    instead of bypassing the runtime grant service's exclusivity rules.

    Returns:
        One when a BTC demo scope grant was inserted, otherwise zero.
    """
    admin = _lookup_seeded_user(conn, "admin", "admin")
    viewer = _lookup_seeded_user(conn, "viewer", "viewer")
    if admin is None or viewer is None:
        raise RuntimeError(
            "active admin/viewer users not found — run `make migrate-dev` against "
            "a fresh database before `scripts/seed_demo.py`"
        )
    membership = conn.execute(
        text(
            "SELECT 1 FROM user_operator_memberships "
            "WHERE user_public_id = :viewer AND operator_public_id = :operator "
            "AND known_to > strftime('%Y-%m-%dT%H:%M:%S','now') LIMIT 1"
        ),
        {"viewer": viewer, "operator": operator},
    ).first()
    if membership is None:
        raise RuntimeError(
            "viewer is not attached to the default desk; use "
            "`POST /api/auth/desks/{operator_public_id}/members/{username}` "
            "with the resolved default operator and viewer username, then log in again"
        )
    return _ensure_demo_instrument_scope(
        conn,
        tracker,
        _DemoInstrumentScope(
            operator=operator,
            wallet=wallet,
            grantor=admin,
            instrument=instrument,
            note="Demo BTC desk scope for seeded viewer visibility",
        ),
    )


def _demo_instrument_scope_owners(
    conn: Connection,
    wallet: str,
    instrument: str,
) -> set[str]:
    """Return every active desk whose scope covers one wallet instrument."""
    rows = conn.execute(
        text(
            "SELECT grants.operator_public_id "
            "FROM wallet_operator_scope_grants AS grants "
            "WHERE grants.wallet_public_id = :wallet "
            "AND grants.instrument_public_id = :instrument "
            "AND grants.known_to > strftime('%Y-%m-%dT%H:%M:%S','now') "
            "UNION "
            "SELECT grants.operator_public_id "
            "FROM wallet_operator_scope_grants AS grants "
            "JOIN instrument_underlying_mappings AS mappings "
            "ON mappings.underlying_public_id = grants.underlying_public_id "
            "AND mappings.instrument_public_id = :instrument "
            "AND mappings.known_to > strftime('%Y-%m-%dT%H:%M:%S','now') "
            "WHERE grants.wallet_public_id = :wallet "
            "AND grants.known_to > strftime('%Y-%m-%dT%H:%M:%S','now') "
        ),
        {"wallet": wallet, "instrument": instrument},
    ).all()
    return {str(row[0]) for row in rows}


def _ensure_demo_instrument_scope(
    conn: Connection,
    tracker: SequenceTracker,
    scope: _DemoInstrumentScope,
) -> int:
    """Insert one non-overlapping demo instrument grant or reuse its desk owner."""
    owners = _demo_instrument_scope_owners(conn, scope.wallet, scope.instrument)
    conflicting_owners = sorted(owners - {scope.operator})
    if conflicting_owners:
        raise RuntimeError(
            "demo instrument scope overlaps an active grant owned by another desk; "
            f"wallet={scope.wallet} instrument={scope.instrument} "
            f"operators={conflicting_owners}"
        )
    if owners:
        return 0
    now = datetime.now(tz=UTC)
    conn.execute(
        text(
            "INSERT INTO wallet_operator_scope_grants "
            "(public_id, operator_public_id, wallet_public_id, granted_by_user_public_id, "
            " scope_kind, underlying_public_id, instrument_public_id, note, "
            " timestamp, known_to, session_id, sequence_id) "
            "VALUES (:public_id, :operator, :wallet, :grantor, "
            " 'instrument', NULL, :instrument, :note, "
            " :timestamp, :known_to, :session_id, :sequence_id)"
        ),
        {
            "public_id": str(uuid7()),
            "operator": scope.operator,
            "wallet": scope.wallet,
            "grantor": scope.grantor,
            "instrument": scope.instrument,
            "note": scope.note,
            "timestamp": str(now),
            "known_to": KNOWN_TO_MAX_STR,
            "session_id": tracker.session_id,
            "sequence_id": tracker.next_sequence("wallet_operator_scope_grants"),
        },
    )
    return 1


def _prepare_demo_instruments(
    conn: Connection,
    tracker: SequenceTracker,
    operator: str,
    wallet: str,
) -> tuple[str, str, str | None, str | None]:
    """Resolve required demo instruments and provision viewer desk visibility."""
    btc_perp = _lookup_instrument(conn, "BTC-USD-PERP", "kraken_futures")
    eth_perp = _lookup_instrument(conn, "ETH-USD-PERP", "kraken_futures")
    clm6 = _lookup_instrument(conn, "CLM6-NYMEX", "kraken_equities")
    gcm6 = _lookup_instrument(conn, "GCM6-COMEX", "kraken_equities")
    if not btc_perp or not eth_perp:
        raise RuntimeError(
            f"required instruments not found — btc_perp={btc_perp} eth_perp={eth_perp}; "
            "run `make run-static` to populate symbols"
        )
    _ensure_demo_viewer_desk_scope(
        conn,
        tracker,
        operator=operator,
        wallet=wallet,
        instrument=btc_perp,
    )
    return btc_perp, eth_perp, clm6, gcm6


def _insert_order(
    conn: Connection,
    tracker: SequenceTracker,
    *,
    wallet: str,
    operator: str,
    instrument: str,
    side: str,
    order_type: str,
    price: float | None,
    size: float,
    status: str,
    filled_size: float,
    average_price: float | None,
    created_at: datetime,
) -> str:
    """Insert one order row and return its public_id."""
    public_id = str(uuid7())
    conn.execute(
        text(
            "INSERT INTO orders "
            "(public_id, instrument_public_id, mode, wallet_public_id, operator_public_id, "
            " client_order_id, exchange_order_id, created_at, updated_at, side, order_type, "
            " price, size, status, time_in_force, filled_size, average_price, error, "
            " leverage, reduce_only, plan_public_id, "
            " timestamp, known_to, session_id, sequence_id) "
            "VALUES "
            "(:public_id, :instrument, 'paper', :wallet, :operator, "
            " :coid, :exoid, :created_at, :updated_at, :side, :order_type, "
            " :price, :size, :status, 'gtc', :filled_size, :avg_price, NULL, "
            " NULL, 0, NULL, "
            " :ts, :known_to, :sid, :seq)"
        ),
        {
            "public_id": public_id,
            "instrument": instrument,
            "wallet": wallet,
            "operator": operator,
            "coid": _demo_client_order_id(public_id),
            "exoid": _demo_exchange_order_id(public_id),
            "created_at": str(created_at),
            "updated_at": str(created_at),
            "side": side,
            "order_type": order_type,
            "price": price,
            "size": size,
            "status": status,
            "filled_size": filled_size,
            "avg_price": average_price,
            "ts": str(created_at),
            "known_to": KNOWN_TO_MAX_STR,
            "sid": tracker.session_id,
            "seq": tracker.next_sequence("orders"),
        },
    )
    return public_id


def _demo_client_order_id(order_public_id: str) -> str:
    """Return the stable client-order identity shared by order and fill."""
    return f"demo-{order_public_id}"


def _demo_exchange_order_id(order_public_id: str) -> str:
    """Return the stable exchange-order identity shared by order and fill."""
    return f"ex-{order_public_id}"


def _lookup_order_native_symbol(conn: Connection, order_public_id: str) -> str:
    """Resolve the active native symbol owned by one freshly seeded order."""
    native_symbol: str = conn.execute(
        text(
            "SELECT s.native_symbol FROM orders o "
            "JOIN instruments i ON i.public_id = o.instrument_public_id "
            "JOIN symbols s ON s.public_id = i.symbol_public_id "
            "WHERE o.public_id = :order_public_id "
            "AND o.known_to = :known_to AND i.known_to = :known_to "
            "AND s.known_to = :known_to"
        ),
        {
            "order_public_id": order_public_id,
            "known_to": KNOWN_TO_MAX_STR,
        },
    ).scalar_one()
    return str(native_symbol)


def _insert_fill_observed(
    conn: Connection,
    tracker: SequenceTracker,
    fill: _SeedExecutionFill,
) -> None:
    """Append exactly one canonical venue witness for one seeded execution."""
    event_public_id = str(uuid5(_DEMO_VENUE_EVENT_NAMESPACE, fill.public_id))
    client_order_id = _demo_client_order_id(fill.order_public_id)
    exchange_order_id = _demo_exchange_order_id(fill.order_public_id)
    shard_key = compute_shard_key(
        instrument=fill.instrument,
        exchange=cast(OrderExchange, ExchangeEnum(fill.exchange)),
        mode=ExecutionModeEnum(fill.mode),
        wallet_public_id=fill.wallet_public_id,
        strategy_tag=None,
    )
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
            "(:public_id, 'fill_observed', :shard_key, :wallet, NULL, "
            " :exchange, :instrument, :mode, :exchange_order_id, :client_order_id, "
            " :venue_client_id, :side, :status, :price, :size, :size, "
            " :fee, 'USD', :exec_id, :trade_id, NULL, :event_time, :event_time, "
            " NULL, 'taker', NULL, "
            " :event_time, :known_to, :session_id, :sequence_id)"
        ),
        {
            "public_id": event_public_id,
            "shard_key": shard_key,
            "wallet": fill.wallet_public_id,
            "exchange": fill.exchange,
            "instrument": fill.instrument,
            "mode": fill.mode,
            "exchange_order_id": exchange_order_id,
            "client_order_id": client_order_id,
            "venue_client_id": f"venue-{client_order_id}",
            "side": fill.side,
            "status": fill.status,
            "price": fill.price,
            "size": fill.size,
            "fee": fill.fee,
            "exec_id": fill.exec_id,
            "trade_id": fill.trade_id,
            "event_time": str(fill.executed_at),
            "known_to": KNOWN_TO_MAX_STR,
            "session_id": tracker.session_id,
            "sequence_id": tracker.next_sequence(f"venue.{shard_key}"),
        },
    )


def _read_demo_pnl_seed_state(conn: Connection) -> _DemoPnlSeedState:
    """Read the exact append-only P&L evidence shape owned by this seed."""
    row = conn.execute(
        text(
            "SELECT "
            "(SELECT COUNT(*) FROM orders WHERE session_id = :session_id), "
            "(SELECT COUNT(*) FROM executions WHERE session_id = :session_id), "
            "(SELECT COUNT(*) FROM venue_events WHERE session_id = :session_id), "
            "(SELECT COUNT(*) FROM executions "
            " WHERE session_id = :session_id "
            " AND exec_id = 'exec-' || public_id "
            " AND trade_id = 'trade-' || public_id)"
        ),
        {
            "session_id": _DEMO_SESSION_ID,
        },
    ).one()
    witness_rows = conn.execute(
        text(
            "SELECT e.public_id, e.wallet_public_id, e.exchange, e.mode, "
            "s.native_symbol, v.shard_key "
            "FROM executions e "
            "JOIN orders o ON o.public_id = e.order_public_id "
            "JOIN instruments i ON i.public_id = o.instrument_public_id "
            " AND i.known_to = :known_to "
            "JOIN symbols s ON s.public_id = i.symbol_public_id "
            " AND s.known_to = :known_to "
            "JOIN venue_events v ON v.session_id = :session_id "
            " AND v.event_type = 'fill_observed' "
            " AND v.wallet_public_id = e.wallet_public_id "
            " AND v.exchange = e.exchange AND v.mode = e.mode "
            " AND v.instrument = s.native_symbol "
            " AND v.client_order_id = o.client_order_id "
            " AND v.exchange_order_id = o.exchange_order_id "
            " AND v.exec_id = e.exec_id AND v.trade_id = e.trade_id "
            " AND v.side = e.side AND v.status = e.status "
            " AND v.fill_price = e.price AND v.fill_size = e.size "
            " AND v.cum_fill_size = e.size AND v.fee = e.fee "
            " AND v.fee_asset = e.fee_asset "
            " AND v.venue_timestamp = e.executed_at "
            " AND v.received_at = e.executed_at AND v.timestamp = e.timestamp "
            " AND v.known_to = e.known_to "
            "WHERE e.session_id = :session_id "
            "AND o.known_to = :known_to"
        ),
        {
            "session_id": _DEMO_SESSION_ID,
            "known_to": KNOWN_TO_MAX_STR,
        },
    ).all()
    exact_execution_ids = {str(witness[0]) for witness in witness_rows}
    canonical_shard_execution_ids = {
        str(execution_public_id)
        for (
            execution_public_id,
            wallet_public_id,
            exchange,
            mode,
            instrument,
            shard_key,
        ) in witness_rows
        if shard_key
        == compute_shard_key(
            instrument=str(instrument),
            exchange=cast(OrderExchange, ExchangeEnum(str(exchange))),
            mode=ExecutionModeEnum(str(mode)),
            wallet_public_id=str(wallet_public_id),
            strategy_tag=None,
        )
    }
    return _DemoPnlSeedState(
        orders=int(row[0]),
        executions=int(row[1]),
        venue_events=int(row[2]),
        canonical_execution_ids=int(row[3]),
        exact_witnesses=len(exact_execution_ids),
        canonical_shards=len(canonical_shard_execution_ids),
    )


def _require_repairable_demo_pnl_seed(state: _DemoPnlSeedState) -> None:
    """Refuse legacy or partial rows that append-only execution IDs cannot repair."""
    if state in {_EMPTY_DEMO_PNL_SEED, _COMPLETE_DEMO_PNL_SEED}:
        return
    raise RuntimeError(
        "legacy or incomplete demo P&L seed detected — refusing silent skip: "
        f"orders={state.orders} executions={state.executions} "
        f"fill_observed={state.venue_events} "
        f"canonical_execution_ids={state.canonical_execution_ids} "
        f"exact_witnesses={state.exact_witnesses} "
        f"canonical_shards={state.canonical_shards}. "
        "Executions are append-only, so this script cannot repair old or duplicate "
        "venue identities in place. Rebuild the configured local SQLite database, "
        "run `make migrate-dev`, then rerun `scripts/seed_demo.py`."
    )


def _insert_execution(
    conn: Connection,
    tracker: SequenceTracker,
    *,
    wallet: str,
    operator: str,
    order_public_id: str,
    exchange: str,
    mode: str,
    side: str,
    status: str,
    price: float,
    size: float,
    fee: float,
    executed_at: datetime,
) -> str:
    """Insert one execution row and return its public_id.

    ``exchange``/``mode`` are the fill's immutable certification scope
    (the caller supplies the order's mode and its instrument's exchange,
    mirroring what production ingest resolves from lineage) and
    ``scope_sequence`` is allocated inline as the committed per-scope
    max + 1 — the same allocation rule ``insert_execution`` applies,
    race-free because ``main`` acquired the same per-wallet execution
    fence (``_acquire_execution_fence``) before the first insert and
    holds it to the script's single commit. The full execution UUID is
    retained in both venue identities, and the same transaction appends
    exactly one canonical ``fill_observed`` witness carrying the stored
    execution economics and order lineage.
    """
    public_id = str(uuid7())
    exec_id = f"exec-{public_id}"
    trade_id = f"trade-{public_id}"
    conn.execute(
        text(
            "INSERT INTO executions "
            "(public_id, order_public_id, wallet_public_id, operator_public_id, "
            " exchange, mode, scope_sequence, "
            " exec_id, trade_id, side, status, price, size, fee, fee_asset, "
            " executed_at, liquidity_role, "
            " timestamp, known_to, session_id, sequence_id) "
            "VALUES "
            "(:public_id, :opid, :wallet, :operator, "
            " :exchange, :mode, "
            " (SELECT COALESCE(MAX(scope_sequence), 0) + 1 FROM executions "
            "  WHERE wallet_public_id = :wallet AND exchange = :exchange "
            "  AND mode = :mode), "
            " :exid, :tid, :side, :status, :price, :size, :fee, 'USD', "
            " :exec_at, 'taker', "
            " :ts, :known_to, :sid, :seq)"
        ),
        {
            "public_id": public_id,
            "opid": order_public_id,
            "wallet": wallet,
            "operator": operator,
            "exchange": exchange,
            "mode": mode,
            "exid": exec_id,
            "tid": trade_id,
            "side": side,
            "status": status,
            "price": price,
            "size": size,
            "fee": fee,
            "exec_at": str(executed_at),
            "ts": str(executed_at),
            "known_to": KNOWN_TO_MAX_STR,
            "sid": tracker.session_id,
            "seq": tracker.next_sequence("executions"),
        },
    )
    _insert_fill_observed(
        conn,
        tracker,
        _SeedExecutionFill(
            public_id=public_id,
            order_public_id=order_public_id,
            wallet_public_id=wallet,
            exchange=exchange,
            mode=mode,
            instrument=_lookup_order_native_symbol(conn, order_public_id),
            exec_id=exec_id,
            trade_id=trade_id,
            side=side,
            status=status,
            price=price,
            size=size,
            fee=fee,
            executed_at=executed_at,
        ),
    )
    return public_id


def _insert_position(
    conn: Connection,
    tracker: SequenceTracker,
    *,
    wallet: str,
    instrument: str,
    quantity: float,
    average_price: float,
    unrealized_pnl: float,
    realized_pnl: float,
    ts: datetime,
) -> str:
    """Insert one position projection row and return its public_id."""
    public_id = str(uuid7())
    conn.execute(
        text(
            "INSERT INTO positions "
            "(public_id, instrument_public_id, mode, wallet_public_id, "
            " quantity, average_price, unrealized_pnl, realized_pnl, "
            " timestamp, known_to, session_id, sequence_id) "
            "VALUES "
            "(:public_id, :instrument, 'paper', :wallet, "
            " :qty, :avg, :upnl, :rpnl, "
            " :ts, :known_to, :sid, :seq)"
        ),
        {
            "public_id": public_id,
            "instrument": instrument,
            "wallet": wallet,
            "qty": quantity,
            "avg": average_price,
            "upnl": unrealized_pnl,
            "rpnl": realized_pnl,
            "ts": str(ts),
            "known_to": KNOWN_TO_MAX_STR,
            "sid": tracker.session_id,
            "seq": tracker.next_sequence("positions"),
        },
    )
    return public_id


def _insert_alert_event(
    conn: Connection,
    tracker: SequenceTracker,
    alert: _DemoAlertEvent,
) -> str:
    """Insert one temporal (SCD2) alert_events row and return its public_id.

    Mirrors what the production sidecar bypass writer does when an
    ``alerts.{user}.{alert_type}`` ZMQ frame is consumed: every row
    gets ``known_to = KNOWN_TO_MAX_STR`` (active) and provenance
    pulled from ``tracker``. The iOS Alerts tab reads these rows
    via ``Repository.list_recent_alerts_for_user`` filtered on
    ``user_public_id == principal.user_public_id``.

    Args:
        conn: Open SQLite connection inside the seeder's transaction.
        tracker: Provenance source for ``session_id`` / ``sequence_id``.
        alert: Domain facts of the notification to persist.

    Returns:
        The freshly generated ``public_id`` of the inserted row.
    """
    public_id = str(uuid7())
    conn.execute(
        text(
            "INSERT INTO alert_events "
            "(public_id, user_public_id, operator_public_id, wallet_public_id, "
            " alert_type, priority, is_safety_critical, title, body, payload, "
            " dedup_key, thread_key, source_topic, "
            " timestamp, known_to, session_id, sequence_id) "
            "VALUES "
            "(:public_id, :user, :operator, :wallet, "
            " :atype, :priority, :critical, :title, :body, :payload, "
            " :dedup, :thread, :topic, "
            " :ts, :known_to, :sid, :seq)"
        ),
        {
            "public_id": public_id,
            "user": alert.user_public_id,
            "operator": alert.operator_public_id,
            "wallet": alert.wallet_public_id,
            "atype": alert.alert_type,
            "priority": alert.priority,
            "critical": alert.is_safety_critical,
            "title": alert.title,
            "body": alert.body,
            "payload": json.dumps(alert.payload),
            "dedup": alert.dedup_key,
            "thread": alert.thread_key,
            "topic": alert.source_topic,
            "ts": str(alert.occurred_at),
            "known_to": KNOWN_TO_MAX_STR,
            "sid": tracker.session_id,
            "seq": tracker.next_sequence("alert_events"),
        },
    )
    return public_id


def _seed_demo_alerts_for_user(
    conn: Connection,
    tracker: SequenceTracker,
    *,
    user_public_id: str,
    operator_public_id: str,
    wallet_public_id: str,
    base_time: datetime,
) -> int:
    """Insert one realistic alert per ``AlertType`` for the given user.

    Seven rows total (one per ``AlertType`` literal): order_fill_full,
    order_rejected, order_unknown, position_stop_loss_fired,
    margin_warning, critical_system_error, drift. Spaced by 30 minutes so
    the iOS Alerts tab renders chronologically. Returns the count
    inserted.
    """
    short = user_public_id[:8]
    alerts: list[tuple[str, str, bool, str, str, dict[str, object]]] = [
        (
            "order_fill_full",
            "medium",
            False,
            "Order filled",
            "BUY 0.1421 BTC-USD-PERP @ $76,820.50 filled on kraken_futures",
            {
                "client_order_id": f"demo-{short}-1",
                "exchange_order_id": f"demo-exch-{short}-1",
                "instrument": "BTC-USD-PERP",
                "exchange": "kraken_futures",
                "side": "buy",
                "size": 0.1421,
                "price": 76820.50,
            },
        ),
        (
            "order_rejected",
            "high",
            False,
            "Order rejected",
            "BUY 2.0 BTC-USD-PERP rejected: insufficient margin",
            {
                "client_order_id": f"demo-{short}-2",
                "exchange_order_id": f"demo-exch-{short}-2",
                "instrument": "BTC-USD-PERP",
                "exchange": "kraken_futures",
                "reason": "insufficient_margin",
            },
        ),
        (
            "order_unknown",
            "high",
            True,
            "Order state unknown",
            "SELL 0.5 ETH-USD-PERP submit outcome ambiguous — venue verification in progress",
            {
                "client_order_id": f"demo-{short}-3",
                "instrument": "ETH-USD-PERP",
                "exchange": "kraken_futures",
                "reason": "venue_timeout",
                "deep_link_path": f"/orders/demo-{short}-3",
            },
        ),
        (
            "position_stop_loss_fired",
            "high",
            False,
            "Stop-loss fired",
            "ETH-USD-PERP stop @ $3,000 triggered, position reduced to 0",
            {
                "instrument": "ETH-USD-PERP",
                "exchange": "kraken_futures",
                "stop_price": 3000.0,
            },
        ),
        (
            "margin_warning",
            "high",
            False,
            "Margin warning",
            "Wallet margin utilisation 82% — consider reducing exposure",
            {"utilisation_pct": 82.0},
        ),
        (
            "critical_system_error",
            "high",
            True,
            "Trader heartbeat stale",
            "ZMQ trader has not produced a heartbeat in 60 seconds",
            {"component": "trader", "stale_seconds": 60},
        ),
        (
            "drift",
            "high",
            True,
            "Portfolio drift detected",
            "LIVE kraken_futures portfolio drift detected after 3 consecutive mismatches",
            {
                "episode_public_id": str(uuid7()),
                "lifecycle": "opened",
                "wallet_public_id": wallet_public_id,
                "operator_public_id": operator_public_id,
                "exchange": "kraken_futures",
                "mode": "live",
                "mismatch_count": 3,
            },
        ),
    ]
    for offset, (alert_type, priority, critical, title, body, payload) in enumerate(alerts):
        occurred = base_time + timedelta(minutes=30 * offset)
        _insert_alert_event(
            conn,
            tracker,
            _DemoAlertEvent(
                user_public_id=user_public_id,
                wallet_public_id=wallet_public_id,
                operator_public_id=operator_public_id,
                alert_type=alert_type,
                priority=priority,
                is_safety_critical=critical,
                title=title,
                body=body,
                payload=payload,
                dedup_key=f"demo.{short}.{alert_type}",
                thread_key=f"snapper.demo.{short}",
                source_topic=f"alerts.{user_public_id}.{alert_type}",
                occurred_at=occurred,
            ),
        )
    return len(alerts)


def _insert_backtest_run(
    conn: Connection,
    tracker: SequenceTracker,
    run: _DemoBacktestRun,
) -> str:
    """Insert one backtest_runs row and return its public_id.

    Args:
        conn: Open SQLite connection inside the seeder's transaction.
        tracker: Provenance source for ``session_id`` / ``sequence_id``.
        run: Domain facts of the finished backtest to persist.

    Returns:
        The freshly generated ``public_id`` of the inserted row.
    """
    public_id = str(uuid7())
    conn.execute(
        text(
            "INSERT INTO backtest_runs "
            "(public_id, wallet_public_id, operator_public_id, "
            " strategy_name, strategy_params, "
            " instrument_public_id, exchange, mode, timeframe, "
            " start_date, end_date, initial_cash, status, "
            " created_by_user_id, started_at, completed_at, error, "
            " process_name, execution_mode, fill_model, "
            " slippage_bps, commission_bps, config_hash, target_execution_exchange, "
            " timestamp, known_to, session_id, sequence_id) "
            "VALUES "
            "(:public_id, :wallet, NULL, "
            " :strategy, :params, "
            " :instrument, :exchange, 'paper', :tf, "
            " :start, :end, :cash, :status, "
            " 'demo', :started, :completed, NULL, "
            " NULL, 'direct_db', 'market', "
            " 5.0, 8.0, NULL, NULL, "
            " :ts, :known_to, :sid, :seq)"
        ),
        {
            "public_id": public_id,
            "wallet": run.wallet,
            "strategy": run.strategy_name,
            "params": json.dumps(run.strategy_params),
            "instrument": run.instrument,
            "exchange": run.exchange,
            "tf": run.timeframe,
            "start": str(run.start),
            "end": str(run.end),
            "cash": run.initial_cash,
            "status": run.status,
            "started": str(run.started_at),
            "completed": str(run.completed_at) if run.completed_at else None,
            "ts": str(run.start),
            "known_to": KNOWN_TO_MAX_STR,
            "sid": tracker.session_id,
            "seq": tracker.next_sequence("backtest_runs"),
        },
    )
    return public_id


def _seed_ai_delegate_review(
    conn: Connection,
    tracker: SequenceTracker,
    *,
    operator: str,
    wallet: str,
    instrument: str,
) -> str:
    """Seed one ``ai_delegate`` user + pending ``ai_review`` row.

    Creates an ``ai_demo`` login with a fresh random password, the matching
    ``ai_delegates`` row, an operator membership, an instrument scope grant,
    and a pending review on the supplied instrument with a Strait-of-Hormuz
    oil-volatility rationale embedded in the signal envelope. The caller
    reports the once-shown demo credential only after the transaction commits.

    Returns:
        The freshly generated demo password stored by this transaction.
    """
    now = datetime.now(tz=UTC)
    delegate_user_pid = str(uuid7())
    password = secrets.token_urlsafe(24)
    pwd_hash = bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()
    conn.execute(
        text(
            "INSERT INTO users "
            "(public_id, username, email, password_hash, role, is_active, "
            " created_at, timestamp, known_to, session_id, sequence_id) "
            "VALUES (:pid, 'ai_demo', 'ai_demo@snapper.local', :pw, 'ai_delegate', 1, "
            " :now, :now, :ka, :sid, :seq)"
        ),
        {
            "pid": delegate_user_pid,
            "pw": pwd_hash,
            "now": str(now),
            "ka": KNOWN_TO_MAX_STR,
            "sid": tracker.session_id,
            "seq": tracker.next_sequence("users"),
        },
    )
    delegate_pid = str(uuid7())
    conn.execute(
        text(
            "INSERT INTO ai_delegates "
            "(public_id, user_public_id, last_seen_at, active_reviews_count, created_at, updated_at) "
            "VALUES (:pid, :uid, :now, 1, :now, :now)"
        ),
        {"pid": delegate_pid, "uid": delegate_user_pid, "now": str(now)},
    )

    conn.execute(
        text(
            "INSERT INTO user_operator_memberships "
            "(public_id, user_public_id, operator_public_id, is_primary, "
            " timestamp, known_to, session_id, sequence_id) "
            "VALUES (:pid, :uid, :op, 1, :now, :ka, :sid, :seq)"
        ),
        {
            "pid": str(uuid7()),
            "uid": delegate_user_pid,
            "op": operator,
            "now": str(now),
            "ka": KNOWN_TO_MAX_STR,
            "sid": tracker.session_id,
            "seq": tracker.next_sequence("user_operator_memberships"),
        },
    )

    _ensure_demo_instrument_scope(
        conn,
        tracker,
        _DemoInstrumentScope(
            operator=operator,
            wallet=wallet,
            grantor=delegate_user_pid,
            instrument=instrument,
            note="Demo CLM6 oil scope for ai_demo delegate",
        ),
    )

    review_pid = str(uuid7())
    created = now - timedelta(minutes=4)
    fanout_after = now - timedelta(minutes=1)
    deadline = now + timedelta(minutes=11)
    envelope = {
        "type": "signal",
        "side": "buy",
        "symbol": "CLM6-NYMEX",
        "timeframe": "1h",
        "strategy": "HormuzCrudeShock",
        "thesis": (
            "Breakout continuation above $104 after Strait-of-Hormuz news flow; "
            "WTI crude intraday range $99.30 – $109.86 on supply-route risk."
        ),
        "strength": 0.71,
        "news_anchors": [
            "newsnow.co.uk: Brent crude headlines",
            "Reuters: Hormuz route disruption fears",
        ],
    }
    metadata = {
        "symbol": "CLM6-NYMEX",
        "venue": "NYMEX",
        "last_price": 104.27,
        "intraday_low": 99.30,
        "intraday_high": 109.86,
        "theme": "Strait-of-Hormuz volatility",
    }
    snap_hash = hashlib.sha256(json.dumps(envelope, sort_keys=True).encode()).hexdigest()
    conn.execute(
        text(
            "INSERT INTO ai_reviews "
            "(public_id, session_id, sequence_id, "
            " user_public_id, operator_public_id, wallet_public_id, "
            " instrument_public_id, strategy_public_id, "
            " selected_delegate_public_id, responding_delegate_public_id, "
            " resolution_mode, status, signal_envelope, signal_snapshot_hash, "
            " instrument_metadata, deadline, fanout_after, decision, rationale, "
            " dispatch_version, counter_decremented_at, "
            " created_at, updated_at, resolved_at) "
            "VALUES (:pid, :sid, :seq, "
            " :uid, :op, :wal, "
            " :inst, :strat, "
            " :dlg, NULL, "
            " NULL, 'pending', :env, :snap, "
            " :meta, :deadline, :fanout, NULL, NULL, "
            " 0, NULL, "
            " :created, :now, NULL)"
        ),
        {
            "pid": review_pid,
            "sid": tracker.session_id,
            "seq": tracker.next_sequence("ai_reviews"),
            "uid": delegate_user_pid,
            "op": operator,
            "wal": wallet,
            "inst": instrument,
            "strat": str(uuid7()),
            "dlg": delegate_pid,
            "env": json.dumps(envelope),
            "snap": snap_hash,
            "meta": json.dumps(metadata),
            "deadline": str(deadline),
            "fanout": str(fanout_after),
            "created": str(created),
            "now": str(now),
        },
    )

    conn.execute(
        text(
            "INSERT INTO ai_review_events "
            "(public_id, review_public_id, event_type, actor_delegate_public_id, "
            " previous_status, new_status, payload, occurred_at) "
            "VALUES (:pid, :rev, 'created', NULL, NULL, 'pending', :payload, :ts)"
        ),
        {
            "pid": str(uuid7()),
            "rev": review_pid,
            "payload": json.dumps({"selected_delegate": delegate_pid}),
            "ts": str(created),
        },
    )
    conn.execute(
        text(
            "INSERT INTO ai_review_events "
            "(public_id, review_public_id, event_type, actor_delegate_public_id, "
            " previous_status, new_status, payload, occurred_at) "
            "VALUES (:pid, :rev, 'fanout_dispatched', :dlg, 'pending', 'fanout_dispatched', :p, :ts)"
        ),
        {
            "pid": str(uuid7()),
            "rev": review_pid,
            "dlg": delegate_pid,
            "p": json.dumps({"reason": "primary_quorum_reached", "delegates_pinged": 1}),
            "ts": str(fanout_after),
        },
    )
    return password


def _report_demo_ai_login(password: str | None) -> None:
    """Print a freshly committed demo credential when the optional user exists."""
    if password is not None:
        print(f"demo AI login: ai_demo / {password}")


def _report_existing_demo_ai_login_recovery() -> None:
    """Explain how to rotate a demo credential that cannot be reprinted."""
    print(
        "demo AI login password was shown once; "
        "run `snapper reset-password ai_demo` to rotate it"
    )


def _finish_existing_demo_seed(conn: Connection, seed_state: _DemoPnlSeedState) -> int:
    """Commit an idempotent rerun and report credential recovery guidance."""
    conn.commit()
    print(f"demo seed already inserted ({seed_state.orders} demo orders), skipping")
    if _lookup_seeded_user(conn, "ai_demo", "ai_delegate") is not None:
        _report_existing_demo_ai_login_recovery()
    conn.engine.dispose()
    return 0


def main() -> int:
    """Run the demo seed end-to-end.

    The write fence protects an exact P&L seed-state preflight before
    either insertion or the idempotency skip. A current complete seed
    may skip, while legacy duplicate execution identities, missing
    ``fill_observed`` witnesses, and every other partial shape fail
    loudly because the append-only execution ledger cannot be repaired
    in place.

    The non-ledger root-wallet normalization then runs before either
    insertion or a valid skip, and the skip path commits, so a re-run
    against an already-seeded database still converges the root
    ``wallets`` row and every non-ledger ``wallet_public_id`` reference
    to the canonical spelling instead of leaving a first-run alias
    frozen forever. The append-only ledgers (``executions``,
    ``venue_events`` and the spot reconciliation anchor) are excluded
    from that normalization — their alias handling belongs to migration
    0029 alone.

    Returns:
        ``0`` on successful completion. Errors raise ``RuntimeError``
        before reaching the return statement.
    """
    db_url = _sync_db_url(BootstrapSettingsLoader().db_url)
    engine = create_engine(db_url, poolclass=NullPool)
    tracker = SequenceTracker()
    tracker._session_id = _DEMO_SESSION_ID

    with engine.connect() as conn:
        _require_sqlite_dialect(conn)
        wallet_identity = _lookup_paper_wallet(conn)
        operator = _lookup_operator(conn)
        if not wallet_identity or not operator:
            raise RuntimeError(
                "paper wallet or default operator not found — run `make migrate-dev` first"
            )
        wallet = _canonical_wallet(wallet_identity)

        _acquire_execution_fence(conn)
        seed_state = _read_demo_pnl_seed_state(conn)
        _require_repairable_demo_pnl_seed(seed_state)
        _normalize_wallet_identity(conn, wallet_identity, wallet)

        btc_perp, eth_perp, clm6, gcm6 = _prepare_demo_instruments(
            conn,
            tracker,
            operator,
            wallet,
        )
        if seed_state == _COMPLETE_DEMO_PNL_SEED:
            return _finish_existing_demo_seed(conn, seed_state)

        order1_t = datetime(2026, 4, 21, 9, 14, 32, tzinfo=UTC)
        order1 = _insert_order(
            conn,
            tracker,
            wallet=wallet,
            operator=operator,
            instrument=btc_perp,
            side="buy",
            order_type="market",
            price=None,
            size=0.1421,
            status="filled",
            filled_size=0.1421,
            average_price=76820.50,
            created_at=order1_t,
        )
        _insert_execution(
            conn,
            tracker,
            wallet=wallet,
            operator=operator,
            order_public_id=order1,
            exchange="kraken_futures",
            mode="paper",
            side="buy",
            status="filled",
            price=76820.50,
            size=0.1421,
            fee=10.92,
            executed_at=order1_t,
        )
        _insert_position(
            conn,
            tracker,
            wallet=wallet,
            instrument=btc_perp,
            quantity=0.1421,
            average_price=76820.50,
            unrealized_pnl=420.79,
            realized_pnl=0.0,
            ts=order1_t,
        )

        order2_t = datetime(2026, 4, 25, 14, 18, 42, tzinfo=UTC)
        order2 = _insert_order(
            conn,
            tracker,
            wallet=wallet,
            operator=operator,
            instrument=eth_perp,
            side="sell",
            order_type="market",
            price=None,
            size=4.7,
            status="filled",
            filled_size=4.7,
            average_price=2820.40,
            created_at=order2_t,
        )
        _insert_execution(
            conn,
            tracker,
            wallet=wallet,
            operator=operator,
            order_public_id=order2,
            exchange="kraken_futures",
            mode="paper",
            side="sell",
            status="filled",
            price=2820.40,
            size=4.7,
            fee=10.61,
            executed_at=order2_t,
        )
        _insert_position(
            conn,
            tracker,
            wallet=wallet,
            instrument=eth_perp,
            quantity=-4.7,
            average_price=2820.40,
            unrealized_pnl=2231.56,
            realized_pnl=0.0,
            ts=order2_t,
        )

        order3_t = datetime(2026, 5, 4, 22, 11, 7, tzinfo=UTC)
        _insert_order(
            conn,
            tracker,
            wallet=wallet,
            operator=operator,
            instrument=btc_perp,
            side="buy",
            order_type="limit",
            price=77500.0,
            size=0.0843,
            status="open",
            filled_size=0.0,
            average_price=None,
            created_at=order3_t,
        )

        order4_t = datetime(2026, 5, 1, 16, 8, 52, tzinfo=UTC)
        _insert_order(
            conn,
            tracker,
            wallet=wallet,
            operator=operator,
            instrument=eth_perp,
            side="sell",
            order_type="stop",
            price=2950.0,
            size=2.5,
            status="canceled",
            filled_size=0.0,
            average_price=None,
            created_at=order4_t,
        )

        bt_start = datetime(2025, 11, 1, tzinfo=UTC)
        bt_end = datetime(2026, 4, 30, tzinfo=UTC)

        _insert_backtest_run(
            conn,
            tracker,
            _DemoBacktestRun(
                wallet=wallet,
                instrument=btc_perp,
                exchange="kraken_futures",
                strategy_name="RsiReversion",
                strategy_params={"period": 14, "oversold": 28, "overbought": 72},
                timeframe="1h",
                start=bt_start,
                end=bt_end,
                initial_cash=10000.0,
                status="completed",
                started_at=datetime(2026, 5, 4, 21, 0, 0, tzinfo=UTC),
                completed_at=datetime(2026, 5, 4, 21, 14, 32, tzinfo=UTC),
            ),
        )

        _insert_backtest_run(
            conn,
            tracker,
            _DemoBacktestRun(
                wallet=wallet,
                instrument=btc_perp,
                exchange="kraken_futures",
                strategy_name="MacdCrossover",
                strategy_params={"fast": 12, "slow": 26, "signal": 9},
                timeframe="1h",
                start=bt_start,
                end=bt_end,
                initial_cash=10000.0,
                status="completed",
                started_at=datetime(2026, 5, 4, 21, 14, 33, tzinfo=UTC),
                completed_at=datetime(2026, 5, 4, 21, 28, 11, tzinfo=UTC),
            ),
        )

        if clm6:
            _insert_backtest_run(
                conn,
                tracker,
                _DemoBacktestRun(
                    wallet=wallet,
                    instrument=clm6,
                    exchange="kraken_equities",
                    strategy_name="RsiReversion",
                    strategy_params={"period": 14, "oversold": 30, "overbought": 70},
                    timeframe="1d",
                    start=datetime(2025, 5, 1, tzinfo=UTC),
                    end=bt_end,
                    initial_cash=10000.0,
                    status="completed",
                    started_at=datetime(2026, 5, 4, 21, 28, 12, tzinfo=UTC),
                    completed_at=datetime(2026, 5, 4, 21, 39, 50, tzinfo=UTC),
                ),
            )

        if gcm6:
            _insert_backtest_run(
                conn,
                tracker,
                _DemoBacktestRun(
                    wallet=wallet,
                    instrument=gcm6,
                    exchange="kraken_equities",
                    strategy_name="MacdCrossover",
                    strategy_params={"fast": 8, "slow": 21, "signal": 5},
                    timeframe="1d",
                    start=datetime(2025, 5, 1, tzinfo=UTC),
                    end=bt_end,
                    initial_cash=10000.0,
                    status="completed",
                    started_at=datetime(2026, 5, 4, 21, 39, 51, tzinfo=UTC),
                    completed_at=datetime(2026, 5, 4, 21, 51, 33, tzinfo=UTC),
                ),
            )

        demo_ai_password = (
            _seed_ai_delegate_review(
                conn,
                tracker,
                operator=operator,
                wallet=wallet,
                instrument=clm6,
            )
            if clm6
            else None
        )

        alerts_base = datetime(2026, 5, 7, 9, 0, tzinfo=UTC)
        alerts_inserted = 0
        for role in ("admin", "operator", "viewer"):
            user_pid = _lookup_seeded_user(conn, role, role)
            if user_pid:
                alerts_inserted += _seed_demo_alerts_for_user(
                    conn,
                    tracker,
                    user_public_id=user_pid,
                    operator_public_id=operator,
                    wallet_public_id=wallet,
                    base_time=alerts_base,
                )

        conn.commit()
    engine.dispose()
    _report_demo_ai_login(demo_ai_password)
    print(
        "demo seed inserted: 4 orders, 2 executions, 2 positions, "
        f"3 backtests, {alerts_inserted} alerts"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
