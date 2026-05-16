"""Initial database schema migration.

Creates all tables for the Snapper trading system: instruments, candles,
trades, orders, executions, positions, signals, users, settings, market
snapshots, trade runtime tables, multi-tenant foundation (wallets,
operators, credentials, memberships, scope grants), funding rates, accrual
ledger, continuous contracts, execution plans, position cycles, and
supporting reference tables. Seeds symbols, aliases, and exchange
capabilities.
"""

from collections.abc import Sequence
from datetime import UTC
from datetime import datetime
from uuid import uuid7

import sqlalchemy as sa
from alembic import op
from sqlalchemy import text

_CK_EXCHANGE_LOWER = "exchange = LOWER(exchange)"
_KNOWN_TO_MAX = datetime(9999, 12, 31, 23, 59, 59, tzinfo=UTC)


def _bind_datetime_literal(value: datetime, dialect_name: str) -> datetime | str:
    """Return SQLite-safe bind values while preserving real datetimes elsewhere."""
    if dialect_name == "sqlite":
        return value.isoformat(sep=" ")
    return value


_KNOWN_TO_ACTIVE_PG = "known_to = '9999-12-31T23:59:59+00:00'"
_KNOWN_TO_ACTIVE_SQLITE = "known_to = '9999-12-31 23:59:59.000000'"
_NOTIFICATION_DEVICE_ACTIVE_PG = (
    "known_to = '9999-12-31T23:59:59+00:00' AND token_status = 'active'"
)
_NOTIFICATION_DEVICE_ACTIVE_SQLITE = (
    "known_to = '9999-12-31 23:59:59.000000' AND token_status = 'active'"
)
_CK_NOTIFICATION_DEVICE_TOKEN_STATUS = (
    "token_status IN ('active', 'unregistered', 'user_unregistered')"
)
_CK_SESSION_ID = "session_id != ''"
_CK_SEQUENCE_ID = "sequence_id > 0"
_CK_SESSION_ID_NONEMPTY = "length(session_id) > 0"
_CK_SEQUENCE_ID_NONNEG = "sequence_id >= 0"

revision: str = "0001"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None
SYMBOL_CATALOG = [
    ("BTC-USD", "BTC", "USD", "crypto"),
    ("BTC-EUR", "BTC", "EUR", "crypto"),
    ("ETH-USD", "ETH", "USD", "crypto"),
    ("ETH-EUR", "ETH", "EUR", "crypto"),
    ("ETH-BTC", "ETH", "BTC", "crypto"),
    ("EUR-USD", "EUR", "USD", "forex"),
    ("USD-PLN", "USD", "PLN", "forex"),
    ("EUR-PLN", "EUR", "PLN", "forex"),
    ("GBP-PLN", "GBP", "PLN", "forex"),
    ("GBP-USD", "GBP", "USD", "forex"),
    ("EUR-GBP", "EUR", "GBP", "forex"),
]
SYMBOL_ALIASES = [
    ("BTC-USD", "kraken", "ws", "BTC/USD"),
    ("BTC-USD", "kraken", "rest", "XXBTZUSD"),
    ("BTC-USD", "kraken", "ccxt", "BTC/USD"),
    ("BTC-USD", "polygon", "rest", "X:BTCUSD"),
    ("BTC-EUR", "kraken", "ws", "BTC/EUR"),
    ("BTC-EUR", "kraken", "rest", "XXBTZEUR"),
    ("BTC-EUR", "kraken", "ccxt", "BTC/EUR"),
    ("BTC-EUR", "polygon", "rest", "X:BTCEUR"),
    ("ETH-USD", "kraken", "ws", "ETH/USD"),
    ("ETH-USD", "kraken", "rest", "XETHZUSD"),
    ("ETH-USD", "kraken", "ccxt", "ETH/USD"),
    ("ETH-USD", "polygon", "rest", "X:ETHUSD"),
    ("ETH-EUR", "kraken", "ws", "ETH/EUR"),
    ("ETH-EUR", "kraken", "rest", "XETHZEUR"),
    ("ETH-EUR", "kraken", "ccxt", "ETH/EUR"),
    ("ETH-BTC", "kraken", "ws", "ETH/BTC"),
    ("ETH-BTC", "kraken", "rest", "XETHXXBT"),
    ("ETH-BTC", "kraken", "ccxt", "ETH/BTC"),
    ("ETH-BTC", "polygon", "rest", "X:ETHBTC"),
    ("EUR-USD", "kraken", "ws", "EUR/USD"),
    ("EUR-USD", "kraken", "rest", "ZEURZUSD"),
    ("EUR-USD", "kraken", "ccxt", "EUR/USD"),
    ("EUR-USD", "polygon", "rest", "C:EURUSD"),
    ("EUR-USD", "walutomat", "ws", "EUR_USD"),
    ("EUR-USD", "walutomat", "rest", "EURUSD"),
    ("USD-PLN", "polygon", "rest", "C:USDPLN"),
    ("USD-PLN", "walutomat", "ws", "USD_PLN"),
    ("USD-PLN", "walutomat", "rest", "USDPLN"),
    ("EUR-PLN", "polygon", "rest", "C:EURPLN"),
    ("EUR-PLN", "walutomat", "ws", "EUR_PLN"),
    ("EUR-PLN", "walutomat", "rest", "EURPLN"),
    ("GBP-PLN", "polygon", "rest", "C:GBPPLN"),
    ("GBP-PLN", "walutomat", "ws", "GBP_PLN"),
    ("GBP-PLN", "walutomat", "rest", "GBPPLN"),
    ("GBP-USD", "polygon", "rest", "C:GBPUSD"),
    ("EUR-GBP", "polygon", "rest", "C:EURGBP"),
]
_EXCHANGE_CAPABILITIES: dict[str, tuple[bool, bool]] = {
    "kraken": (True, True),
    "polygon": (True, False),
    "walutomat": (True, True),
}
SYMBOL_CAPABILITIES: list[tuple[str, str, bool, bool]] = []
_seen_pairs: set[tuple[str, str]] = set()
for _alias in SYMBOL_ALIASES:
    _pair = (_alias[0], _alias[1])
    if _pair not in _seen_pairs:
        _seen_pairs.add(_pair)
        _can_md, _can_trade = _EXCHANGE_CAPABILITIES[_alias[1]]
        SYMBOL_CAPABILITIES.append((_alias[0], _alias[1], _can_md, _can_trade))


def upgrade() -> None:
    """Create all database tables, indexes, and seed reference data."""
    op.create_table(
        "symbols",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("public_id", sa.String(36), nullable=False),
        sa.Column("native_symbol", sa.String(32), nullable=False),
        sa.Column("base", sa.String(16), nullable=False),
        sa.Column("quote", sa.String(16), nullable=True),
        sa.Column("asset_type", sa.String(16), server_default="crypto", nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("session_id", sa.String(36), nullable=False),
        sa.Column("sequence_id", sa.Integer(), nullable=False),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("known_to", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.CheckConstraint(
            "asset_type IN ('crypto', 'forex', 'equity', 'index', 'commodity', 'yield')",
            name="ck_symbol_asset_type",
        ),
        sa.CheckConstraint(
            "asset_type IN ('equity', 'index', 'commodity', 'yield') OR quote IS NOT NULL",
            name="ck_symbol_quote_required",
        ),
        sa.CheckConstraint(_CK_SESSION_ID, name="ck_symbols_session_id"),
        sa.CheckConstraint(_CK_SEQUENCE_ID, name="ck_symbols_sequence_id"),
    )
    op.create_index(
        "uq_symbols_active_native",
        "symbols",
        ["native_symbol"],
        unique=True,
        sqlite_where=text(_KNOWN_TO_ACTIVE_SQLITE),
        postgresql_where=text(_KNOWN_TO_ACTIVE_PG),
    )
    op.create_index(
        "ix_symbols_public_id",
        "symbols",
        ["public_id"],
        unique=True,
        sqlite_where=text(_KNOWN_TO_ACTIVE_SQLITE),
        postgresql_where=text(_KNOWN_TO_ACTIVE_PG),
    )
    op.create_table(
        "symbol_aliases",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("public_id", sa.String(36), nullable=False),
        sa.Column("symbol_public_id", sa.String(36), nullable=False),
        sa.Column("exchange", sa.String(20), nullable=False),
        sa.Column("channel", sa.String(10), nullable=False),
        sa.Column("exchange_symbol", sa.String(40), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("session_id", sa.String(36), nullable=False),
        sa.Column("sequence_id", sa.Integer(), nullable=False),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("known_to", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.CheckConstraint(
            _CK_EXCHANGE_LOWER,
            name="ck_symbol_alias_exchange_lower",
        ),
        sa.CheckConstraint(
            "channel IN ('ws', 'rest', 'ccxt')",
            name="ck_symbol_alias_channel",
        ),
        sa.CheckConstraint(_CK_SESSION_ID, name="ck_symbol_aliases_session_id"),
        sa.CheckConstraint(_CK_SEQUENCE_ID, name="ck_symbol_aliases_sequence_id"),
    )
    op.create_index(
        "ix_symbol_aliases_public_id",
        "symbol_aliases",
        ["public_id"],
        unique=True,
        sqlite_where=text(_KNOWN_TO_ACTIVE_SQLITE),
        postgresql_where=text(_KNOWN_TO_ACTIVE_PG),
    )
    op.create_index(
        "uq_alias_spid_exchange_channel",
        "symbol_aliases",
        ["symbol_public_id", "exchange", "channel"],
        unique=True,
        sqlite_where=text(_KNOWN_TO_ACTIVE_SQLITE),
        postgresql_where=text(_KNOWN_TO_ACTIVE_PG),
    )
    op.create_index(
        "uq_alias_exchange_channel_symbol",
        "symbol_aliases",
        ["exchange", "channel", "exchange_symbol"],
        unique=True,
        sqlite_where=text(_KNOWN_TO_ACTIVE_SQLITE),
        postgresql_where=text(_KNOWN_TO_ACTIVE_PG),
    )
    op.create_index("ix_symbol_aliases_symbol_public_id", "symbol_aliases", ["symbol_public_id"])
    op.create_table(
        "symbol_exchange_capabilities",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("public_id", sa.String(36), nullable=False),
        sa.Column("symbol_public_id", sa.String(36), nullable=False),
        sa.Column("exchange", sa.String(20), nullable=False),
        sa.Column("can_market_data", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("can_trade", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("source", sa.String(50), nullable=True),
        sa.Column("reason", sa.String(1024), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("session_id", sa.String(36), nullable=False),
        sa.Column("sequence_id", sa.Integer(), nullable=False),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("known_to", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.CheckConstraint(
            _CK_EXCHANGE_LOWER,
            name="ck_sec_exchange_lower",
        ),
        sa.CheckConstraint(_CK_SESSION_ID, name="ck_symbol_exchange_capabilities_session_id"),
        sa.CheckConstraint(_CK_SEQUENCE_ID, name="ck_symbol_exchange_capabilities_sequence_id"),
    )
    op.create_index(
        "ix_symbol_exchange_capabilities_public_id",
        "symbol_exchange_capabilities",
        ["public_id"],
        unique=True,
        sqlite_where=text(_KNOWN_TO_ACTIVE_SQLITE),
        postgresql_where=text(_KNOWN_TO_ACTIVE_PG),
    )
    op.create_index(
        "uq_sec_symbol_exchange",
        "symbol_exchange_capabilities",
        ["symbol_public_id", "exchange"],
        unique=True,
        sqlite_where=text(_KNOWN_TO_ACTIVE_SQLITE),
        postgresql_where=text(_KNOWN_TO_ACTIVE_PG),
    )
    op.create_index("ix_sec_exchange", "symbol_exchange_capabilities", ["exchange"])
    op.create_index(
        "ix_symbol_exchange_capabilities_symbol_public_id",
        "symbol_exchange_capabilities",
        ["symbol_public_id"],
    )
    op.create_index(
        "ix_sec_exchange_trade",
        "symbol_exchange_capabilities",
        ["exchange", "can_trade"],
        sqlite_where=text("can_trade = 1"),
    )
    op.create_index(
        "ix_sec_exchange_md",
        "symbol_exchange_capabilities",
        ["exchange", "can_market_data"],
        sqlite_where=text("can_market_data = 1"),
    )
    op.create_table(
        "instruments",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("public_id", sa.String(36), nullable=False),
        sa.Column("symbol_public_id", sa.String(36), nullable=False),
        sa.Column("exchange", sa.String(20), nullable=False),
        sa.Column(
            "requires_ai_review",
            sa.Boolean(),
            nullable=False,
            server_default=sa.text("false"),
        ),
        sa.Column("session_id", sa.String(36), nullable=False),
        sa.Column("sequence_id", sa.Integer(), nullable=False),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("known_to", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.CheckConstraint(_CK_EXCHANGE_LOWER, name="ck_instrument_exchange_lower"),
        sa.CheckConstraint(_CK_SESSION_ID, name="ck_instruments_session_id"),
        sa.CheckConstraint(_CK_SEQUENCE_ID, name="ck_instruments_sequence_id"),
    )
    op.create_index(
        "ix_instruments_public_id",
        "instruments",
        ["public_id"],
        unique=True,
        sqlite_where=text(_KNOWN_TO_ACTIVE_SQLITE),
        postgresql_where=text(_KNOWN_TO_ACTIVE_PG),
    )
    op.create_index(
        "uq_instrument_spid_exchange",
        "instruments",
        ["symbol_public_id", "exchange"],
        unique=True,
        sqlite_where=text(_KNOWN_TO_ACTIVE_SQLITE),
        postgresql_where=text(_KNOWN_TO_ACTIVE_PG),
    )
    op.create_index("ix_instruments_symbol_public_id", "instruments", ["symbol_public_id"])
    op.create_index("ix_instruments_exchange", "instruments", ["exchange"])
    op.create_table(
        "candles",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("public_id", sa.String(36), nullable=False),
        sa.Column("instrument_public_id", sa.String(36), nullable=False),
        sa.Column("timeframe", sa.String(8), nullable=False),
        sa.Column("open_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("open", sa.Float(), nullable=False),
        sa.Column("high", sa.Float(), nullable=False),
        sa.Column("low", sa.Float(), nullable=False),
        sa.Column("close", sa.Float(), nullable=False),
        sa.Column("volume", sa.Float(), nullable=False),
        sa.Column("vwap", sa.Float(), nullable=True),
        sa.Column("trades", sa.Integer(), nullable=True),
        sa.Column("session_id", sa.String(36), nullable=False),
        sa.Column("sequence_id", sa.Integer(), nullable=False),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("known_to", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.CheckConstraint(_CK_SESSION_ID, name="ck_candles_session_id"),
        sa.CheckConstraint(_CK_SEQUENCE_ID, name="ck_candles_sequence_id"),
    )
    op.create_index(
        "ix_candles_public_id",
        "candles",
        ["public_id"],
        unique=True,
        sqlite_where=text(_KNOWN_TO_ACTIVE_SQLITE),
        postgresql_where=text(_KNOWN_TO_ACTIVE_PG),
    )
    op.create_index(
        "uq_candle_itf_open",
        "candles",
        ["instrument_public_id", "timeframe", "open_at"],
        unique=True,
        sqlite_where=text(_KNOWN_TO_ACTIVE_SQLITE),
        postgresql_where=text(_KNOWN_TO_ACTIVE_PG),
    )
    op.create_index("ix_candles_instrument_public_id", "candles", ["instrument_public_id"])
    op.create_index("ix_candle_instrument_open", "candles", ["instrument_public_id", "open_at"])
    op.create_table(
        "trades",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("public_id", sa.String(36), nullable=False),
        sa.Column("instrument_public_id", sa.String(36), nullable=False),
        sa.Column("trade_id", sa.String(64), nullable=True),
        sa.Column("price", sa.Float(), nullable=False),
        sa.Column("size", sa.Float(), nullable=False),
        sa.Column("side", sa.String(4), nullable=False),
        sa.Column("executed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("session_id", sa.String(36), nullable=False),
        sa.Column("sequence_id", sa.Integer(), nullable=False),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("known_to", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "instrument_public_id", "trade_id", name="uq_trade_instrument_trade_id"
        ),
        sa.CheckConstraint(_CK_SESSION_ID, name="ck_trades_session_id"),
        sa.CheckConstraint(_CK_SEQUENCE_ID, name="ck_trades_sequence_id"),
    )
    op.create_index(
        "ix_trades_public_id",
        "trades",
        ["public_id"],
        unique=True,
        sqlite_where=text(_KNOWN_TO_ACTIVE_SQLITE),
        postgresql_where=text(_KNOWN_TO_ACTIVE_PG),
    )
    op.create_index("ix_trades_instrument_public_id", "trades", ["instrument_public_id"])
    op.create_index("ix_trades_timestamp", "trades", ["timestamp"])
    op.create_index("ix_trade_instrument_ts", "trades", ["instrument_public_id", "timestamp"])
    op.create_table(
        "ticks",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("public_id", sa.String(36), nullable=False),
        sa.Column("instrument_public_id", sa.String(36), nullable=False),
        sa.Column("bid", sa.Float(), nullable=True),
        sa.Column("ask", sa.Float(), nullable=True),
        sa.Column("last", sa.Float(), nullable=True),
        sa.Column("volume", sa.Float(), nullable=False),
        sa.Column("session_id", sa.String(36), nullable=False),
        sa.Column("sequence_id", sa.Integer(), nullable=False),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("known_to", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.CheckConstraint(_CK_SESSION_ID, name="ck_ticks_session_id"),
        sa.CheckConstraint(_CK_SEQUENCE_ID, name="ck_ticks_sequence_id"),
    )
    op.create_index(
        "ix_ticks_public_id",
        "ticks",
        ["public_id"],
        unique=True,
        sqlite_where=text(_KNOWN_TO_ACTIVE_SQLITE),
        postgresql_where=text(_KNOWN_TO_ACTIVE_PG),
    )
    op.create_index("ix_ticks_instrument_public_id", "ticks", ["instrument_public_id"])
    op.create_index("ix_tick_instrument_ts", "ticks", ["instrument_public_id", "timestamp"])
    op.create_table(
        "orders",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("public_id", sa.String(36), nullable=False),
        sa.Column("instrument_public_id", sa.String(36), nullable=False),
        sa.Column("client_order_id", sa.String(64), nullable=True),
        sa.Column("exchange_order_id", sa.String(64), nullable=True),
        sa.Column("side", sa.String(4), nullable=False),
        sa.Column("order_type", sa.String(16), nullable=False),
        sa.Column("price", sa.Float(), nullable=True),
        sa.Column("size", sa.Float(), nullable=False),
        sa.Column("filled_size", sa.Float(), nullable=False, server_default="0"),
        sa.Column("mode", sa.String(8), server_default="live", nullable=False),
        sa.Column("average_price", sa.Float(), nullable=True),
        sa.Column("status", sa.String(16), nullable=False),
        sa.Column("time_in_force", sa.String(16), nullable=True),
        sa.Column("error", sa.String(512), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("leverage", sa.Integer(), nullable=True),
        sa.Column("reduce_only", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("wallet_public_id", sa.String(36), nullable=False),
        sa.Column("operator_public_id", sa.String(36), nullable=True),
        sa.Column("plan_public_id", sa.String(36), nullable=True),
        sa.Column("session_id", sa.String(36), nullable=False),
        sa.Column("sequence_id", sa.Integer(), nullable=False),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("known_to", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.CheckConstraint(_CK_SESSION_ID, name="ck_orders_session_id"),
        sa.CheckConstraint(_CK_SEQUENCE_ID, name="ck_orders_sequence_id"),
    )
    op.create_index(
        "ix_orders_public_id",
        "orders",
        ["public_id"],
        unique=True,
        sqlite_where=text(_KNOWN_TO_ACTIVE_SQLITE),
        postgresql_where=text(_KNOWN_TO_ACTIVE_PG),
    )
    op.create_index("ix_orders_instrument_public_id", "orders", ["instrument_public_id"])
    op.create_index("ix_orders_client_order_id", "orders", ["client_order_id"])
    op.create_index("ix_orders_exchange_order_id", "orders", ["exchange_order_id"])
    op.create_index(
        "uq_orders_client_oid",
        "orders",
        ["instrument_public_id", "mode", "client_order_id"],
        unique=True,
        sqlite_where=text("client_order_id IS NOT NULL AND " + _KNOWN_TO_ACTIVE_SQLITE),
        postgresql_where=text("client_order_id IS NOT NULL AND " + _KNOWN_TO_ACTIVE_PG),
    )
    op.create_index(
        "uq_orders_exchange_oid",
        "orders",
        ["instrument_public_id", "mode", "exchange_order_id"],
        unique=True,
        sqlite_where=text("exchange_order_id IS NOT NULL AND " + _KNOWN_TO_ACTIVE_SQLITE),
        postgresql_where=text("exchange_order_id IS NOT NULL AND " + _KNOWN_TO_ACTIVE_PG),
    )
    op.create_index("ix_orders_plan_public_id", "orders", ["plan_public_id"])
    op.create_index(
        "ix_orders_wallet_public_id_created_at",
        "orders",
        ["wallet_public_id", "created_at"],
        sqlite_where=text(_KNOWN_TO_ACTIVE_SQLITE),
        postgresql_where=text(_KNOWN_TO_ACTIVE_PG),
    )
    op.create_index(
        "ix_orders_status_created_at",
        "orders",
        ["status", "created_at"],
        sqlite_where=text(_KNOWN_TO_ACTIVE_SQLITE),
        postgresql_where=text(_KNOWN_TO_ACTIVE_PG),
    )
    op.create_table(
        "executions",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("public_id", sa.String(36), nullable=False),
        sa.Column("order_public_id", sa.String(36), nullable=False),
        sa.Column("exec_id", sa.String(64), nullable=True),
        sa.Column("trade_id", sa.String(64), nullable=True),
        sa.Column("side", sa.String(4), nullable=False),
        sa.Column("status", sa.String(16), nullable=False),
        sa.Column("price", sa.Float(), nullable=False),
        sa.Column("size", sa.Float(), nullable=False),
        sa.Column("fee", sa.Float(), nullable=False),
        sa.Column("fee_asset", sa.String(16), nullable=False),
        sa.Column("executed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("wallet_public_id", sa.String(36), nullable=False),
        sa.Column("operator_public_id", sa.String(36), nullable=True),
        sa.Column("liquidity_role", sa.String(16), nullable=False, server_default="unknown"),
        sa.Column("session_id", sa.String(36), nullable=False),
        sa.Column("sequence_id", sa.Integer(), nullable=False),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("known_to", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.CheckConstraint(_CK_SESSION_ID, name="ck_executions_session_id"),
        sa.CheckConstraint(_CK_SEQUENCE_ID, name="ck_executions_sequence_id"),
    )
    op.create_index(
        "ix_executions_public_id",
        "executions",
        ["public_id"],
        unique=True,
        sqlite_where=text(_KNOWN_TO_ACTIVE_SQLITE),
        postgresql_where=text(_KNOWN_TO_ACTIVE_PG),
    )
    op.create_index("ix_executions_order_public_id", "executions", ["order_public_id"])
    op.create_index(
        "uq_executions_order_exec",
        "executions",
        ["order_public_id", "exec_id"],
        unique=True,
        sqlite_where=text("exec_id IS NOT NULL"),
    )
    op.create_index(
        "uq_executions_order_trade",
        "executions",
        ["order_public_id", "trade_id"],
        unique=True,
        sqlite_where=text("trade_id IS NOT NULL"),
    )
    op.create_table(
        "positions",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("public_id", sa.String(36), nullable=False),
        sa.Column("instrument_public_id", sa.String(36), nullable=False),
        sa.Column("mode", sa.String(8), server_default="live", nullable=False),
        sa.Column("quantity", sa.Float(), nullable=False),
        sa.Column("average_price", sa.Float(), nullable=False),
        sa.Column("unrealized_pnl", sa.Float(), nullable=False),
        sa.Column("realized_pnl", sa.Float(), nullable=False),
        sa.Column("wallet_public_id", sa.String(36), nullable=False),
        sa.Column("session_id", sa.String(36), nullable=False),
        sa.Column("sequence_id", sa.Integer(), nullable=False),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("known_to", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.CheckConstraint(_CK_SESSION_ID, name="ck_positions_session_id"),
        sa.CheckConstraint(_CK_SEQUENCE_ID, name="ck_positions_sequence_id"),
    )
    op.create_index(
        "ix_positions_public_id",
        "positions",
        ["public_id"],
        unique=True,
        sqlite_where=text(_KNOWN_TO_ACTIVE_SQLITE),
        postgresql_where=text(_KNOWN_TO_ACTIVE_PG),
    )
    op.create_index(
        "uq_positions_instrument_public_id",
        "positions",
        ["instrument_public_id", "mode", "wallet_public_id"],
        unique=True,
        sqlite_where=text(_KNOWN_TO_ACTIVE_SQLITE),
        postgresql_where=text(_KNOWN_TO_ACTIVE_PG),
    )
    op.create_index("ix_positions_instrument_public_id", "positions", ["instrument_public_id"])
    op.create_table(
        "signals",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("public_id", sa.String(36), nullable=False),
        sa.Column("instrument_public_id", sa.String(36), nullable=False),
        sa.Column("side", sa.String(4), nullable=False),
        sa.Column("strength", sa.Float(), nullable=False),
        sa.Column("reason", sa.String(256), nullable=False),
        sa.Column("strategy_name", sa.String(64), nullable=True),
        sa.Column("price", sa.Float(), nullable=True),
        sa.Column("fired_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("wallet_public_id", sa.String(36), nullable=False),
        sa.Column("operator_public_id", sa.String(36), nullable=True),
        sa.Column("session_id", sa.String(36), nullable=False),
        sa.Column("sequence_id", sa.Integer(), nullable=False),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("known_to", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.CheckConstraint(_CK_SESSION_ID, name="ck_signals_session_id"),
        sa.CheckConstraint(_CK_SEQUENCE_ID, name="ck_signals_sequence_id"),
    )
    op.create_index(
        "ix_signals_public_id",
        "signals",
        ["public_id"],
        unique=True,
        sqlite_where=text(_KNOWN_TO_ACTIVE_SQLITE),
        postgresql_where=text(_KNOWN_TO_ACTIVE_PG),
    )
    op.create_index("ix_signals_instrument_public_id", "signals", ["instrument_public_id"])
    op.create_index("ix_signals_fired_at", "signals", ["fired_at"])
    op.create_table(
        "users",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("public_id", sa.String(36), nullable=False),
        sa.Column("username", sa.String(64), nullable=False),
        sa.Column("email", sa.String(255), nullable=True),
        sa.Column("password_hash", sa.String(255), nullable=False),
        sa.Column("role", sa.String(32), nullable=False),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.text("true")),
        sa.Column("created_by_user_public_id", sa.String(36), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("session_id", sa.String(36), nullable=False),
        sa.Column("sequence_id", sa.Integer(), nullable=False),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("known_to", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.CheckConstraint(_CK_SESSION_ID, name="ck_users_session_id"),
        sa.CheckConstraint(_CK_SEQUENCE_ID, name="ck_users_sequence_id"),
    )
    op.create_index(
        "ix_users_public_id",
        "users",
        ["public_id"],
        unique=True,
        sqlite_where=text(_KNOWN_TO_ACTIVE_SQLITE),
        postgresql_where=text(_KNOWN_TO_ACTIVE_PG),
    )
    op.create_index(
        "ix_users_username",
        "users",
        ["username"],
        unique=True,
        sqlite_where=text(_KNOWN_TO_ACTIVE_SQLITE),
        postgresql_where=text(_KNOWN_TO_ACTIVE_PG),
    )
    op.create_index(
        "ix_users_created_by_user_public_id",
        "users",
        ["created_by_user_public_id"],
    )
    op.create_table(
        "user_trading_caps",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("public_id", sa.String(36), nullable=False),
        sa.Column("user_public_id", sa.String(36), nullable=False),
        sa.Column("max_order_quantity_per_instrument", sa.JSON(), nullable=True),
        sa.Column("max_open_orders", sa.Integer(), nullable=True),
        sa.Column("max_daily_notional_usd", sa.Numeric(18, 2), nullable=True),
        sa.Column("max_cancels_per_minute", sa.Integer(), nullable=True),
        sa.Column("session_id", sa.String(36), nullable=False),
        sa.Column("sequence_id", sa.Integer(), nullable=False),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("known_to", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.CheckConstraint(_CK_SESSION_ID_NONEMPTY, name="ck_user_trading_caps_session_id"),
        sa.CheckConstraint(_CK_SEQUENCE_ID_NONNEG, name="ck_user_trading_caps_sequence_id"),
    )
    op.create_index(
        "ix_user_trading_caps_public_id",
        "user_trading_caps",
        ["public_id"],
        unique=True,
        sqlite_where=text(_KNOWN_TO_ACTIVE_SQLITE),
        postgresql_where=text(_KNOWN_TO_ACTIVE_PG),
    )
    op.create_index(
        "ix_user_trading_caps_active",
        "user_trading_caps",
        ["user_public_id"],
        unique=True,
        sqlite_where=text(_KNOWN_TO_ACTIVE_SQLITE),
        postgresql_where=text(_KNOWN_TO_ACTIVE_PG),
    )
    op.create_table(
        "user_active_tokens",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("public_id", sa.String(36), nullable=False),
        sa.Column("user_public_id", sa.String(36), nullable=False),
        sa.Column("jti", sa.String(64), nullable=False),
        sa.Column("token_hash", sa.String(64), nullable=False),
        sa.Column("token_type", sa.String(10), nullable=False),
        sa.Column("issued_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("revoked_at", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("public_id", name="uq_user_active_tokens_public_id"),
        sa.UniqueConstraint("jti", name="uq_user_active_tokens_jti"),
        sa.UniqueConstraint("token_hash", name="uq_user_active_tokens_token_hash"),
    )
    op.create_index(
        "ix_user_active_tokens_user_public_id",
        "user_active_tokens",
        ["user_public_id"],
    )
    op.create_index(
        "ix_user_active_tokens_user_revoked",
        "user_active_tokens",
        ["user_public_id", "revoked_at"],
    )
    op.create_table(
        "user_login_events",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("public_id", sa.String(36), nullable=False),
        sa.Column("user_public_id", sa.String(36), nullable=False),
        sa.Column("logged_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("session_id", sa.String(36), nullable=False),
        sa.Column("sequence_id", sa.Integer(), nullable=False),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("known_to", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.CheckConstraint(_CK_SESSION_ID, name="ck_user_login_events_session_id"),
        sa.CheckConstraint(_CK_SEQUENCE_ID, name="ck_user_login_events_sequence_id"),
    )
    op.create_index("ix_user_login_events_user_public_id", "user_login_events", ["user_public_id"])
    op.create_index(
        "ix_user_login_events_public_id",
        "user_login_events",
        ["public_id"],
        unique=True,
        sqlite_where=text(_KNOWN_TO_ACTIVE_SQLITE),
        postgresql_where=text(_KNOWN_TO_ACTIVE_PG),
    )
    op.create_table(
        "settings",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("public_id", sa.String(36), nullable=False),
        sa.Column("key", sa.String(64), nullable=False),
        sa.Column("value", sa.String(1024), nullable=False),
        sa.Column("category", sa.String(32), nullable=False),
        sa.Column("description", sa.String(1024), nullable=True),
        sa.Column("is_encrypted", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("updated_by", sa.String(64), nullable=True),
        sa.Column("session_id", sa.String(36), nullable=False),
        sa.Column("sequence_id", sa.Integer(), nullable=False),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("known_to", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.CheckConstraint(_CK_SESSION_ID, name="ck_settings_session_id"),
        sa.CheckConstraint(_CK_SEQUENCE_ID, name="ck_settings_sequence_id"),
    )
    op.create_index(
        "ix_settings_public_id",
        "settings",
        ["public_id"],
        unique=True,
        sqlite_where=text(_KNOWN_TO_ACTIVE_SQLITE),
        postgresql_where=text(_KNOWN_TO_ACTIVE_PG),
    )
    op.create_index(
        "uq_settings_key",
        "settings",
        ["key"],
        unique=True,
        sqlite_where=text(_KNOWN_TO_ACTIVE_SQLITE),
        postgresql_where=text(_KNOWN_TO_ACTIVE_PG),
    )
    op.create_table(
        "process_runs",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("public_id", sa.String(36), nullable=False),
        sa.Column("process_name", sa.String(64), nullable=False),
        sa.Column("role", sa.String(16), nullable=False),
        sa.Column("lifecycle", sa.String(16), nullable=False),
        sa.Column("status", sa.String(16), nullable=False),
        sa.Column("parameters", sa.JSON(), nullable=True),
        sa.Column("result", sa.JSON(), nullable=True),
        sa.Column("error", sa.String(1024), nullable=True),
        sa.Column("tags", sa.JSON(), nullable=True),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("wallet_public_id", sa.String(36), nullable=True),
        sa.Column("session_id", sa.String(36), nullable=False),
        sa.Column("sequence_id", sa.Integer(), nullable=False),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("known_to", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.CheckConstraint(_CK_SESSION_ID, name="ck_process_runs_session_id"),
        sa.CheckConstraint(_CK_SEQUENCE_ID, name="ck_process_runs_sequence_id"),
    )
    op.create_index(
        "ix_process_runs_public_id",
        "process_runs",
        ["public_id"],
        unique=True,
        sqlite_where=text(_KNOWN_TO_ACTIVE_SQLITE),
        postgresql_where=text(_KNOWN_TO_ACTIVE_PG),
    )
    op.create_index("ix_process_runs_process_name", "process_runs", ["process_name"])
    op.create_index("ix_process_runs_status", "process_runs", ["status"])
    op.create_index("ix_process_runs_started_at", "process_runs", ["started_at"])
    op.create_table(
        "backtest_runs",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("public_id", sa.String(36), nullable=False),
        sa.Column("session_id", sa.String(36), nullable=False),
        sa.Column("sequence_id", sa.Integer(), nullable=False),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("known_to", sa.DateTime(timezone=True), nullable=False),
        sa.Column("wallet_public_id", sa.String(36), nullable=False),
        sa.Column("operator_public_id", sa.String(36), nullable=True),
        sa.Column("strategy_name", sa.String(128), nullable=False),
        sa.Column("strategy_params", sa.JSON(), nullable=False),
        sa.Column("instrument_public_id", sa.String(36), nullable=False),
        sa.Column("exchange", sa.String(32), nullable=False),
        sa.Column("mode", sa.String(8), nullable=False, server_default="paper"),
        sa.Column("timeframe", sa.String(16), nullable=False),
        sa.Column("start_date", sa.DateTime(timezone=True), nullable=False),
        sa.Column("end_date", sa.DateTime(timezone=True), nullable=False),
        sa.Column("initial_cash", sa.Float(), nullable=False, server_default="10000.0"),
        sa.Column("status", sa.String(24), nullable=False, server_default="pending"),
        sa.Column("created_by_user_id", sa.String(64), nullable=True),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("error", sa.Text(), nullable=True),
        sa.Column("process_name", sa.String(128), nullable=True),
        sa.Column(
            "execution_mode",
            sa.String(16),
            nullable=False,
            server_default="direct_db",
        ),
        sa.Column(
            "fill_model",
            sa.String(32),
            nullable=False,
            server_default="market",
        ),
        sa.Column(
            "slippage_bps",
            sa.Float(),
            nullable=False,
            server_default="0",
        ),
        sa.Column(
            "commission_bps",
            sa.Float(),
            nullable=False,
            server_default="0",
        ),
        sa.Column("config_hash", sa.String(64), nullable=True),
        sa.Column("target_execution_exchange", sa.String(32), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        sa.CheckConstraint(
            "status IN ('pending', 'running', 'completed', 'failed', "
            "'cancel_requested', 'cancelled')",
            name="ck_br_status",
        ),
        sa.CheckConstraint(_CK_SESSION_ID, name="ck_backtest_runs_session_id"),
        sa.CheckConstraint(_CK_SEQUENCE_ID, name="ck_backtest_runs_sequence_id"),
        sa.CheckConstraint(
            "execution_mode IN ('direct_db', 'zmq_replay')",
            name="ck_br_execution_mode",
        ),
        sa.CheckConstraint(
            "fill_model IN ('market')",
            name="ck_br_fill_model",
        ),
        sa.CheckConstraint(
            "slippage_bps >= 0 AND slippage_bps <= 500",
            name="ck_br_slippage_bounds",
        ),
        sa.CheckConstraint(
            "commission_bps >= 0 AND commission_bps <= 500",
            name="ck_br_commission_bounds",
        ),
        sa.CheckConstraint(
            "target_execution_exchange IS NULL OR target_execution_exchange IN "
            "('paper', 'kraken', 'kraken_futures', 'walutomat')",
            name="ck_br_target_execution_exchange",
        ),
    )
    op.create_index(
        "ix_backtest_runs_wallet_status",
        "backtest_runs",
        ["wallet_public_id", "status"],
    )
    op.create_index(
        "ix_backtest_runs_config_hash",
        "backtest_runs",
        ["wallet_public_id", "config_hash", "timestamp"],
    )
    op.create_index(
        "ix_backtest_runs_public_id",
        "backtest_runs",
        ["public_id"],
        unique=True,
        sqlite_where=text(_KNOWN_TO_ACTIVE_SQLITE),
        postgresql_where=text(_KNOWN_TO_ACTIVE_PG),
    )
    op.create_index(
        "uq_bt_single_running",
        "backtest_runs",
        ["status"],
        unique=True,
        sqlite_where=text("status = 'running' AND " + _KNOWN_TO_ACTIVE_SQLITE),
        postgresql_where=text("status = 'running' AND " + _KNOWN_TO_ACTIVE_PG),
    )
    op.create_table(
        "backtest_events",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("public_id", sa.String(36), nullable=False),
        sa.Column("session_id", sa.String(36), nullable=False),
        sa.Column("sequence_id", sa.Integer(), nullable=False),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("known_to", sa.DateTime(timezone=True), nullable=False),
        sa.Column("run_public_id", sa.String(36), nullable=False),
        sa.Column("event_type", sa.String(64), nullable=False),
        sa.Column("detail", sa.JSON(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.CheckConstraint(_CK_SESSION_ID, name="ck_backtest_events_session_id"),
        sa.CheckConstraint(_CK_SEQUENCE_ID, name="ck_backtest_events_sequence_id"),
    )
    op.create_index("ix_be_run_ts", "backtest_events", ["run_public_id", "timestamp"])
    op.create_index("ix_backtest_events_run_public_id", "backtest_events", ["run_public_id"])
    op.create_index(
        "ix_be_public_id",
        "backtest_events",
        ["public_id"],
        unique=True,
        sqlite_where=text(_KNOWN_TO_ACTIVE_SQLITE),
        postgresql_where=text(_KNOWN_TO_ACTIVE_PG),
    )
    op.create_table(
        "backtest_results",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("public_id", sa.String(36), nullable=False),
        sa.Column("session_id", sa.String(36), nullable=False),
        sa.Column("sequence_id", sa.Integer(), nullable=False),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("known_to", sa.DateTime(timezone=True), nullable=False),
        sa.Column("run_public_id", sa.String(36), nullable=False),
        sa.Column("total_trades", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("winning_trades", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("losing_trades", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("total_pnl", sa.Float(), nullable=False, server_default="0"),
        sa.Column("max_drawdown", sa.Float(), nullable=False, server_default="0"),
        sa.Column("sharpe_ratio", sa.Float(), nullable=True),
        sa.Column("win_rate", sa.Float(), nullable=True),
        sa.Column("profit_factor", sa.Float(), nullable=True),
        sa.Column("final_equity", sa.Float(), nullable=False, server_default="0"),
        sa.Column("max_equity", sa.Float(), nullable=False, server_default="0"),
        sa.Column("extra_metrics", sa.JSON(), nullable=False),
        sa.Column("sortino_ratio", sa.Float(), nullable=True),
        sa.Column("cagr", sa.Float(), nullable=True),
        sa.Column("calmar_ratio", sa.Float(), nullable=True),
        sa.Column("expectancy", sa.Float(), nullable=True),
        sa.Column("avg_trade_pnl", sa.Float(), nullable=True),
        sa.Column("max_drawdown_duration_seconds", sa.Float(), nullable=True),
        sa.Column("exposure_ratio", sa.Float(), nullable=True),
        sa.Column("turnover_ratio", sa.Float(), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        sa.CheckConstraint(_CK_SESSION_ID, name="ck_backtest_results_session_id"),
        sa.CheckConstraint(_CK_SEQUENCE_ID, name="ck_backtest_results_sequence_id"),
    )
    op.create_index("ix_backtest_results_run_public_id", "backtest_results", ["run_public_id"])
    op.create_index(
        "uq_br_run_active",
        "backtest_results",
        ["run_public_id"],
        unique=True,
        sqlite_where=text(_KNOWN_TO_ACTIVE_SQLITE),
        postgresql_where=text(_KNOWN_TO_ACTIVE_PG),
    )
    op.create_index(
        "ix_bres_public_id",
        "backtest_results",
        ["public_id"],
        unique=True,
        sqlite_where=text(_KNOWN_TO_ACTIVE_SQLITE),
        postgresql_where=text(_KNOWN_TO_ACTIVE_PG),
    )
    op.create_table(
        "backtest_signals",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("public_id", sa.String(36), nullable=False),
        sa.Column("session_id", sa.String(36), nullable=False),
        sa.Column("sequence_id", sa.Integer(), nullable=False),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("known_to", sa.DateTime(timezone=True), nullable=False),
        sa.Column("run_public_id", sa.String(36), nullable=False),
        sa.Column("signal_time", sa.DateTime(timezone=True), nullable=False),
        sa.Column("signal_type", sa.String(32), nullable=False),
        sa.Column("instrument", sa.String(64), nullable=False),
        sa.Column("price", sa.Float(), nullable=False),
        sa.Column("indicators", sa.JSON(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.CheckConstraint(_CK_SESSION_ID, name="ck_backtest_signals_session_id"),
        sa.CheckConstraint(_CK_SEQUENCE_ID, name="ck_backtest_signals_sequence_id"),
    )
    op.create_index("ix_bs_run_ts", "backtest_signals", ["run_public_id", "signal_time"])
    op.create_index("ix_backtest_signals_run_public_id", "backtest_signals", ["run_public_id"])
    op.create_index(
        "ix_bs_public_id",
        "backtest_signals",
        ["public_id"],
        unique=True,
        sqlite_where=text(_KNOWN_TO_ACTIVE_SQLITE),
        postgresql_where=text(_KNOWN_TO_ACTIVE_PG),
    )
    op.create_table(
        "backtest_trades",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("public_id", sa.String(36), nullable=False),
        sa.Column("session_id", sa.String(36), nullable=False),
        sa.Column("sequence_id", sa.Integer(), nullable=False),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("known_to", sa.DateTime(timezone=True), nullable=False),
        sa.Column("run_public_id", sa.String(36), nullable=False),
        sa.Column("executed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("instrument", sa.String(64), nullable=False),
        sa.Column("side", sa.String(8), nullable=False),
        sa.Column("quantity", sa.Float(), nullable=False),
        sa.Column("price", sa.Float(), nullable=False),
        sa.Column("fee", sa.Float(), nullable=False, server_default="0"),
        sa.Column("pnl", sa.Float(), nullable=True),
        sa.Column("position_after", sa.Float(), nullable=False, server_default="0"),
        sa.Column("signal_public_id", sa.String(36), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        sa.CheckConstraint(_CK_SESSION_ID, name="ck_backtest_trades_session_id"),
        sa.CheckConstraint(_CK_SEQUENCE_ID, name="ck_backtest_trades_sequence_id"),
    )
    op.create_index("ix_bt_run_ts", "backtest_trades", ["run_public_id", "executed_at"])
    op.create_index("ix_backtest_trades_run_public_id", "backtest_trades", ["run_public_id"])
    op.create_index(
        "ix_bt_public_id",
        "backtest_trades",
        ["public_id"],
        unique=True,
        sqlite_where=text(_KNOWN_TO_ACTIVE_SQLITE),
        postgresql_where=text(_KNOWN_TO_ACTIVE_PG),
    )
    op.create_table(
        "backtest_equity_points",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("public_id", sa.String(36), nullable=False),
        sa.Column("session_id", sa.String(36), nullable=False),
        sa.Column("sequence_id", sa.Integer(), nullable=False),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("known_to", sa.DateTime(timezone=True), nullable=False),
        sa.Column("run_public_id", sa.String(36), nullable=False),
        sa.Column("point_time", sa.DateTime(timezone=True), nullable=False),
        sa.Column("equity", sa.Float(), nullable=False),
        sa.Column("cash", sa.Float(), nullable=False),
        sa.Column("position_value", sa.Float(), nullable=False, server_default="0"),
        sa.Column("drawdown", sa.Float(), nullable=False, server_default="0"),
        sa.PrimaryKeyConstraint("id"),
        sa.CheckConstraint(_CK_SESSION_ID, name="ck_backtest_equity_points_session_id"),
        sa.CheckConstraint(_CK_SEQUENCE_ID, name="ck_backtest_equity_points_sequence_id"),
    )
    op.create_index("ix_bep_run_ts", "backtest_equity_points", ["run_public_id", "point_time"])
    op.create_index(
        "ix_backtest_equity_points_run_public_id",
        "backtest_equity_points",
        ["run_public_id"],
    )
    op.create_index(
        "ix_bep_public_id",
        "backtest_equity_points",
        ["public_id"],
        unique=True,
        sqlite_where=text(_KNOWN_TO_ACTIVE_SQLITE),
        postgresql_where=text(_KNOWN_TO_ACTIVE_PG),
    )
    op.create_table(
        "backtest_comparisons",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("public_id", sa.String(36), nullable=False),
        sa.Column("session_id", sa.String(36), nullable=False),
        sa.Column("sequence_id", sa.Integer(), nullable=False),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("known_to", sa.DateTime(timezone=True), nullable=False),
        sa.Column("wallet_public_id", sa.String(36), nullable=False),
        sa.Column("operator_public_id", sa.String(36), nullable=True),
        sa.Column("created_by_user_id", sa.String(64), nullable=True),
        sa.Column("run_a_public_id", sa.String(36), nullable=False),
        sa.Column("run_b_public_id", sa.String(36), nullable=False),
        sa.Column("config_hash", sa.String(64), nullable=True),
        sa.Column("pairing_mode", sa.String(16), nullable=False),
        sa.Column("anchor_run_public_id", sa.String(36), nullable=True),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_bc_wallet_hash_time",
        "backtest_comparisons",
        ["wallet_public_id", "config_hash", "timestamp"],
    )
    op.create_index(
        "ix_bc_public_id",
        "backtest_comparisons",
        ["public_id"],
    )
    op.create_index(
        "uq_bc_active_pair_per_wallet",
        "backtest_comparisons",
        ["wallet_public_id", "run_a_public_id", "run_b_public_id"],
        unique=True,
        sqlite_where=text(_KNOWN_TO_ACTIVE_SQLITE),
        postgresql_where=text(_KNOWN_TO_ACTIVE_PG),
    )
    op.create_table(
        "instrument_specs",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("public_id", sa.String(36), nullable=False),
        sa.Column("instrument_public_id", sa.String(36), nullable=False),
        sa.Column("tick_size", sa.Float(), nullable=True),
        sa.Column("lot_size", sa.Float(), nullable=True),
        sa.Column("min_order_size", sa.Float(), nullable=True),
        sa.Column("max_order_size", sa.Float(), nullable=True),
        sa.Column("cost_decimals", sa.Integer(), nullable=True),
        sa.Column("qty_decimals", sa.Integer(), nullable=True),
        sa.Column("margin_initial", sa.Float(), nullable=True),
        sa.Column("position_limit_long", sa.Integer(), nullable=True),
        sa.Column("position_limit_short", sa.Integer(), nullable=True),
        sa.Column("status", sa.String(20), nullable=True),
        sa.Column("expiry_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("instrument_kind", sa.String(16), nullable=True),
        sa.Column("funding_type", sa.String(32), nullable=True),
        sa.Column("funding_frequency_hours", sa.Integer(), nullable=True),
        sa.Column("rollover_rate_long", sa.Float(), nullable=True),
        sa.Column("rollover_rate_short", sa.Float(), nullable=True),
        sa.Column("max_funding_rate", sa.Float(), nullable=True),
        sa.Column("session_id", sa.String(36), nullable=False),
        sa.Column("sequence_id", sa.Integer(), nullable=False),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("known_to", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.CheckConstraint(_CK_SESSION_ID, name="ck_instrument_specs_session_id"),
        sa.CheckConstraint(_CK_SEQUENCE_ID, name="ck_instrument_specs_sequence_id"),
        sa.CheckConstraint(
            "instrument_kind IN ('spot', 'perpetual', 'future', 'etf', 'option') "
            "OR instrument_kind IS NULL",
            name="ck_instrument_specs_kind",
        ),
        sa.CheckConstraint(
            "funding_type IS NULL OR funding_type IN "
            "('spot_margin_rollover', 'perpetual_funding')",
            name="ck_instrument_specs_funding_type",
        ),
    )
    op.create_index(
        "ix_instrument_specs_public_id",
        "instrument_specs",
        ["public_id"],
        unique=True,
        sqlite_where=text(_KNOWN_TO_ACTIVE_SQLITE),
        postgresql_where=text(_KNOWN_TO_ACTIVE_PG),
    )
    op.create_index(
        "uq_instrument_spec_instrument",
        "instrument_specs",
        ["instrument_public_id"],
        unique=True,
        sqlite_where=text(_KNOWN_TO_ACTIVE_SQLITE),
        postgresql_where=text(_KNOWN_TO_ACTIVE_PG),
    )
    op.create_index(
        "ix_instrument_specs_instrument_public_id", "instrument_specs", ["instrument_public_id"]
    )
    op.create_index(
        "ix_instrument_specs_expiry",
        "instrument_specs",
        ["expiry_at"],
        sqlite_where=text(_KNOWN_TO_ACTIVE_SQLITE),
        postgresql_where=text(_KNOWN_TO_ACTIVE_PG),
    )
    op.create_table(
        "market_snapshots",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("public_id", sa.String(36), nullable=False),
        sa.Column("instrument_public_id", sa.String(36), nullable=False),
        sa.Column("bid", sa.Float(), nullable=True),
        sa.Column("bid_volume", sa.Float(), nullable=True),
        sa.Column("ask", sa.Float(), nullable=True),
        sa.Column("ask_volume", sa.Float(), nullable=True),
        sa.Column("last_price", sa.Float(), nullable=True),
        sa.Column("volume_24h", sa.Float(), nullable=True),
        sa.Column("vwap_24h", sa.Float(), nullable=True),
        sa.Column("low_24h", sa.Float(), nullable=True),
        sa.Column("high_24h", sa.Float(), nullable=True),
        sa.Column("change_24h", sa.Float(), nullable=True),
        sa.Column("spread", sa.Float(), nullable=True),
        sa.Column("spread_pct", sa.Float(), nullable=True),
        sa.Column("session_id", sa.String(36), nullable=False),
        sa.Column("sequence_id", sa.Integer(), nullable=False),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("known_to", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.CheckConstraint(_CK_SESSION_ID, name="ck_market_snapshots_session_id"),
        sa.CheckConstraint(_CK_SEQUENCE_ID, name="ck_market_snapshots_sequence_id"),
    )
    op.create_index(
        "ix_market_snapshots_public_id",
        "market_snapshots",
        ["public_id"],
        unique=True,
        sqlite_where=text(_KNOWN_TO_ACTIVE_SQLITE),
        postgresql_where=text(_KNOWN_TO_ACTIVE_PG),
    )
    op.create_index(
        "uq_market_snapshot_instrument",
        "market_snapshots",
        ["instrument_public_id"],
        unique=True,
        sqlite_where=text(_KNOWN_TO_ACTIVE_SQLITE),
        postgresql_where=text(_KNOWN_TO_ACTIVE_PG),
    )
    op.create_index(
        "ix_market_snapshots_instrument_public_id",
        "market_snapshots",
        ["instrument_public_id"],
    )
    op.create_index(
        "ix_market_snapshots_instrument_ts",
        "market_snapshots",
        ["instrument_public_id", "timestamp"],
    )
    op.create_table(
        "control",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("public_id", sa.String(36), nullable=False),
        sa.Column("transport", sa.String(10), nullable=False),
        sa.Column("direction", sa.String(10), nullable=False),
        sa.Column("message_type", sa.String(128), nullable=False),
        sa.Column("outcome", sa.String(16), nullable=False),
        sa.Column("detail", sa.Text(), nullable=True),
        sa.Column("payload", sa.Text(), nullable=True),
        sa.Column("client_session_id", sa.String(36), nullable=True),
        sa.Column("client_public_id", sa.String(36), nullable=True),
        sa.Column("session_id", sa.String(36), nullable=False),
        sa.Column("sequence_id", sa.Integer(), nullable=False),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("known_to", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.CheckConstraint(_CK_SESSION_ID, name="ck_control_session_id"),
        sa.CheckConstraint(_CK_SEQUENCE_ID, name="ck_control_sequence_id"),
    )
    op.create_index(
        "ix_control_public_id",
        "control",
        ["public_id"],
        unique=True,
        sqlite_where=text(_KNOWN_TO_ACTIVE_SQLITE),
        postgresql_where=text(_KNOWN_TO_ACTIVE_PG),
    )
    op.create_table(
        "telemetry",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("public_id", sa.String(36), nullable=False),
        sa.Column("transport", sa.String(10), nullable=False),
        sa.Column("direction", sa.String(10), nullable=False),
        sa.Column("message_type", sa.String(128), nullable=False),
        sa.Column("payload", sa.Text(), nullable=True),
        sa.Column("session_id", sa.String(36), nullable=False),
        sa.Column("sequence_id", sa.Integer(), nullable=False),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("known_to", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.CheckConstraint(_CK_SESSION_ID, name="ck_telemetry_session_id"),
        sa.CheckConstraint(_CK_SEQUENCE_ID, name="ck_telemetry_sequence_id"),
    )
    op.create_index(
        "ix_telemetry_public_id",
        "telemetry",
        ["public_id"],
        unique=True,
        sqlite_where=text(_KNOWN_TO_ACTIVE_SQLITE),
        postgresql_where=text(_KNOWN_TO_ACTIVE_PG),
    )
    op.create_index("ix_telemetry_timestamp", "telemetry", ["timestamp"])
    op.create_table(
        "trade_commands",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("public_id", sa.String(36), nullable=False),
        sa.Column("session_id", sa.String(36), nullable=False),
        sa.Column("sequence_id", sa.Integer, nullable=False),
        sa.Column("timestamp", sa.DateTime, nullable=False),
        sa.Column("known_to", sa.DateTime, nullable=False),
        sa.Column("command_type", sa.String(16), nullable=False),
        sa.Column("shard_key", sa.String(256), nullable=False),
        sa.Column("exchange", sa.String(32), nullable=False),
        sa.Column("instrument", sa.String(64), nullable=False),
        sa.Column("mode", sa.String(8), nullable=False),
        sa.Column("strategy_id", sa.String(64), nullable=False),
        sa.Column("client_order_id", sa.String(36), nullable=False),
        sa.Column("venue_client_id", sa.String(64), nullable=False),
        sa.Column("idempotency_key", sa.String(128), nullable=True),
        sa.Column("side", sa.String(4), nullable=False),
        sa.Column("order_type", sa.String(16), nullable=False),
        sa.Column("quantity", sa.Float, nullable=False),
        sa.Column("price", sa.Float, nullable=True),
        sa.Column("status", sa.String(32), nullable=False),
        sa.Column("attempt_count", sa.Integer, server_default="0", nullable=False),
        sa.Column("last_error", sa.String(512), nullable=True),
        sa.Column("created_at", sa.DateTime, nullable=False),
        sa.Column("dispatched_at", sa.DateTime, nullable=True),
        sa.Column("acked_at", sa.DateTime, nullable=True),
        sa.Column("terminal_at", sa.DateTime, nullable=True),
        sa.Column("exchange_order_id", sa.String(64), nullable=True),
        sa.Column("supersedes_command_id", sa.String(36), nullable=True),
        sa.Column("correlation_id", sa.String(36), nullable=False),
        sa.Column("leverage", sa.Integer(), nullable=True),
        sa.Column("reduce_only", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("source_surface", sa.String(20), nullable=False, server_default="rest"),
        sa.Column("wallet_public_id", sa.String(36), nullable=False),
        sa.Column("operator_public_id", sa.String(36), nullable=True),
        sa.Column("user_public_id", sa.String(36), nullable=True),
        sa.Column("plan_public_id", sa.String(36), nullable=True),
        sa.CheckConstraint(_CK_SESSION_ID, name="ck_trade_commands_session"),
        sa.CheckConstraint(_CK_SEQUENCE_ID, name="ck_trade_commands_sequence"),
    )
    op.create_index("ix_trade_commands_status", "trade_commands", ["status"])
    op.create_index("ix_trade_commands_shard_key", "trade_commands", ["shard_key"])
    op.create_index(
        "uq_trade_commands_idempotency",
        "trade_commands",
        ["idempotency_key"],
        unique=True,
        sqlite_where=text("idempotency_key IS NOT NULL AND " + _KNOWN_TO_ACTIVE_SQLITE),
        postgresql_where=text("idempotency_key IS NOT NULL AND " + _KNOWN_TO_ACTIVE_PG),
    )
    op.create_index(
        "ix_trade_commands_public_id",
        "trade_commands",
        ["public_id"],
        unique=True,
        sqlite_where=text(_KNOWN_TO_ACTIVE_SQLITE),
        postgresql_where=text(_KNOWN_TO_ACTIVE_PG),
    )
    op.create_index(
        "ix_trade_commands_outbox_pagination",
        "trade_commands",
        ["status", "created_at", "id"],
    )
    op.create_index("ix_trade_commands_plan_public_id", "trade_commands", ["plan_public_id"])
    op.create_table(
        "venue_events",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("public_id", sa.String(36), nullable=False),
        sa.Column("session_id", sa.String(36), nullable=False),
        sa.Column("sequence_id", sa.Integer, nullable=False),
        sa.Column("timestamp", sa.DateTime, nullable=False),
        sa.Column("known_to", sa.DateTime, nullable=False),
        sa.Column("event_type", sa.String(32), nullable=False),
        sa.Column("shard_key", sa.String(256), nullable=False),
        sa.Column("command_public_id", sa.String(36), nullable=True),
        sa.Column("exchange", sa.String(32), nullable=False),
        sa.Column("instrument", sa.String(64), nullable=False),
        sa.Column("mode", sa.String(8), nullable=False),
        sa.Column("exchange_order_id", sa.String(64), nullable=True),
        sa.Column("client_order_id", sa.String(64), nullable=True),
        sa.Column("venue_client_id", sa.String(64), nullable=True),
        sa.Column("side", sa.String(4), nullable=True),
        sa.Column("status", sa.String(32), nullable=True),
        sa.Column("fill_price", sa.Float, nullable=True),
        sa.Column("fill_size", sa.Float, nullable=True),
        sa.Column("cum_fill_size", sa.Float, nullable=True),
        sa.Column("fee", sa.Float, nullable=True),
        sa.Column("fee_asset", sa.String(16), nullable=True),
        sa.Column("exec_id", sa.String(64), nullable=True),
        sa.Column("trade_id", sa.String(64), nullable=True),
        sa.Column("error", sa.String(512), nullable=True),
        sa.Column("venue_timestamp", sa.DateTime, nullable=True),
        sa.Column("received_at", sa.DateTime, nullable=False),
        sa.Column("payload_json", sa.Text, nullable=True),
        sa.Column("wallet_public_id", sa.String(36), nullable=False),
        sa.Column("liquidity_role", sa.String(16), nullable=False, server_default="unknown"),
        sa.CheckConstraint(_CK_SESSION_ID, name="ck_venue_events_session"),
        sa.CheckConstraint(_CK_SEQUENCE_ID, name="ck_venue_events_sequence"),
    )
    op.create_index("ix_venue_events_shard_id", "venue_events", ["shard_key", "id"])
    op.create_index(
        "ix_venue_events_command_public_id",
        "venue_events",
        ["command_public_id"],
    )
    op.create_index(
        "ix_venue_events_public_id",
        "venue_events",
        ["public_id"],
        unique=True,
        sqlite_where=text(_KNOWN_TO_ACTIVE_SQLITE),
        postgresql_where=text(_KNOWN_TO_ACTIVE_PG),
    )
    op.create_table(
        "trade_projection_checkpoints",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("public_id", sa.String(36), nullable=False),
        sa.Column("session_id", sa.String(36), nullable=False),
        sa.Column("sequence_id", sa.Integer, nullable=False),
        sa.Column("timestamp", sa.DateTime, nullable=False),
        sa.Column("known_to", sa.DateTime, nullable=False),
        sa.Column("shard_key", sa.String(256), nullable=False),
        sa.Column("position_qty", sa.Float, server_default="0", nullable=False),
        sa.Column("entry_price", sa.Float, nullable=True),
        sa.Column("cash", sa.Float, nullable=False),
        sa.Column("peak_equity", sa.Float, nullable=False),
        sa.Column("realized_pnl", sa.Float, server_default="0", nullable=False),
        sa.Column("turnover", sa.Float, server_default="0", nullable=False),
        sa.Column("last_venue_event_id", sa.Integer, nullable=True),
        sa.Column("last_venue_event_at", sa.DateTime, nullable=True),
        sa.Column("open_command_ids", sa.Text, nullable=True),
        sa.Column("checkpoint_at", sa.DateTime, nullable=False),
        sa.Column("seen_exec_ids", sa.Text(), server_default="[]", nullable=False),
        sa.Column("position_opened_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("wallet_public_id", sa.String(36), nullable=False),
        sa.Column("operator_public_id", sa.String(36), nullable=True),
        sa.CheckConstraint(_CK_SESSION_ID, name="ck_trade_checkpoints_session"),
        sa.CheckConstraint(_CK_SEQUENCE_ID, name="ck_trade_checkpoints_sequence"),
    )
    op.create_index(
        "uq_trade_checkpoints_shard_key",
        "trade_projection_checkpoints",
        ["shard_key"],
        unique=True,
        sqlite_where=text(_KNOWN_TO_ACTIVE_SQLITE),
        postgresql_where=text(_KNOWN_TO_ACTIVE_PG),
    )
    op.create_index(
        "ix_trade_checkpoints_public_id",
        "trade_projection_checkpoints",
        ["public_id"],
        unique=True,
        sqlite_where=text(_KNOWN_TO_ACTIVE_SQLITE),
        postgresql_where=text(_KNOWN_TO_ACTIVE_PG),
    )
    op.create_table(
        "underlying_assets",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("public_id", sa.String(36), nullable=False),
        sa.Column("name", sa.String(64), nullable=False),
        sa.Column("ticker", sa.String(16), nullable=False),
        sa.Column("asset_class", sa.String(16), nullable=False),
        sa.Column("sector", sa.String(32), nullable=True),
        sa.Column("description", sa.String(256), nullable=True),
        sa.Column("session_id", sa.String(36), nullable=False),
        sa.Column("sequence_id", sa.Integer(), nullable=False),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("known_to", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.CheckConstraint(
            "asset_class IN ('crypto', 'forex', 'equity', 'index', 'commodity', 'yield')",
            name="ck_underlying_asset_class",
        ),
        sa.CheckConstraint(_CK_SESSION_ID, name="ck_underlying_assets_session_id"),
        sa.CheckConstraint(_CK_SEQUENCE_ID, name="ck_underlying_assets_sequence_id"),
    )
    op.create_index(
        "uq_underlying_assets_active_ticker",
        "underlying_assets",
        ["ticker"],
        unique=True,
        sqlite_where=text(_KNOWN_TO_ACTIVE_SQLITE),
        postgresql_where=text(_KNOWN_TO_ACTIVE_PG),
    )
    op.create_index(
        "uq_underlying_assets_active_name",
        "underlying_assets",
        ["name"],
        unique=True,
        sqlite_where=text(_KNOWN_TO_ACTIVE_SQLITE),
        postgresql_where=text(_KNOWN_TO_ACTIVE_PG),
    )
    op.create_index(
        "ix_underlying_assets_public_id",
        "underlying_assets",
        ["public_id"],
        unique=True,
        sqlite_where=text(_KNOWN_TO_ACTIVE_SQLITE),
        postgresql_where=text(_KNOWN_TO_ACTIVE_PG),
    )
    op.create_table(
        "instrument_underlying_mappings",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("public_id", sa.String(36), nullable=False),
        sa.Column("instrument_public_id", sa.String(36), nullable=False),
        sa.Column("underlying_public_id", sa.String(36), nullable=False),
        sa.Column("relationship_type", sa.String(16), nullable=False),
        sa.Column("contract_family", sa.String(16), nullable=True),
        sa.Column("session_id", sa.String(36), nullable=False),
        sa.Column("sequence_id", sa.Integer(), nullable=False),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("known_to", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.CheckConstraint(
            "relationship_type IN ('exact', 'derivative', 'proxy')",
            name="ck_ium_relationship_type",
        ),
        sa.CheckConstraint(_CK_SESSION_ID, name="ck_ium_session_id"),
        sa.CheckConstraint(_CK_SEQUENCE_ID, name="ck_ium_sequence_id"),
    )
    op.create_index(
        "uq_ium_active_instrument",
        "instrument_underlying_mappings",
        ["instrument_public_id"],
        unique=True,
        sqlite_where=text(_KNOWN_TO_ACTIVE_SQLITE),
        postgresql_where=text(_KNOWN_TO_ACTIVE_PG),
    )
    op.create_index(
        "ix_ium_underlying",
        "instrument_underlying_mappings",
        ["underlying_public_id"],
        sqlite_where=text(_KNOWN_TO_ACTIVE_SQLITE),
        postgresql_where=text(_KNOWN_TO_ACTIVE_PG),
    )
    op.create_index(
        "ix_ium_family",
        "instrument_underlying_mappings",
        ["underlying_public_id", "contract_family"],
        sqlite_where=text(_KNOWN_TO_ACTIVE_SQLITE),
        postgresql_where=text(_KNOWN_TO_ACTIVE_PG),
    )
    op.create_table(
        "continuous_contract_configs",
        sa.Column("id", sa.Integer(), autoincrement=True, primary_key=True),
        sa.Column("public_id", sa.String(36), nullable=False),
        sa.Column("session_id", sa.String(36), nullable=False),
        sa.Column("sequence_id", sa.Integer(), nullable=False),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "known_to",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default="9999-12-31 23:59:59.000000",
        ),
        sa.Column("underlying_public_id", sa.String(36), nullable=False),
        sa.Column("exchange", sa.String(20), nullable=False),
        sa.Column("contract_family", sa.String(16), nullable=False),
        sa.Column("method", sa.String(16), nullable=False),
        sa.Column("rollover_days_before", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("label", sa.String(64), nullable=True),
        sa.CheckConstraint(
            "method IN ('unadjusted', 'ratio', 'panama')",
            name="ck_ccc_method",
        ),
    )
    op.create_index(
        "uq_ccc_active_key",
        "continuous_contract_configs",
        [
            "underlying_public_id",
            "exchange",
            "contract_family",
            "method",
            "rollover_days_before",
        ],
        unique=True,
        sqlite_where=text(_KNOWN_TO_ACTIVE_SQLITE),
        postgresql_where=text(_KNOWN_TO_ACTIVE_PG),
    )
    op.create_index(
        "ix_ccc_public_id",
        "continuous_contract_configs",
        ["public_id"],
        unique=True,
        sqlite_where=text(_KNOWN_TO_ACTIVE_SQLITE),
        postgresql_where=text(_KNOWN_TO_ACTIVE_PG),
    )
    op.create_table(
        "funding_rates",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("public_id", sa.String(36), nullable=False),
        sa.Column("instrument_public_id", sa.String(36), nullable=False),
        sa.Column("exchange", sa.String(32), nullable=False),
        sa.Column("rate_type", sa.String(32), nullable=False),
        sa.Column("direction", sa.String(8), nullable=False),
        sa.Column("rate", sa.Float(), nullable=False),
        sa.Column("notional_asset", sa.String(16), nullable=False),
        sa.Column("effective_from", sa.DateTime(timezone=True), nullable=False),
        sa.Column("source", sa.String(32), nullable=False),
        sa.Column("session_id", sa.String(36), nullable=False),
        sa.Column("sequence_id", sa.Integer(), nullable=False),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("known_to", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.CheckConstraint(_CK_EXCHANGE_LOWER, name="ck_funding_rates_exchange_lower"),
        sa.CheckConstraint(
            "rate_type IN ('spot_margin_rollover', 'perpetual_funding')",
            name="ck_funding_rates_rate_type",
        ),
        sa.CheckConstraint(
            "direction IN ('long', 'short', 'both')",
            name="ck_funding_rates_direction",
        ),
        sa.CheckConstraint(
            "source IN ('exchange_api', 'exchange_docs', 'manual', 'derived')",
            name="ck_funding_rates_source",
        ),
        sa.CheckConstraint(_CK_SESSION_ID, name="ck_funding_rates_session_id"),
        sa.CheckConstraint(_CK_SEQUENCE_ID, name="ck_funding_rates_sequence_id"),
    )
    op.create_index(
        "ix_funding_rates_unique_active",
        "funding_rates",
        [
            "instrument_public_id",
            "exchange",
            "rate_type",
            "direction",
            "effective_from",
        ],
        unique=True,
        sqlite_where=text(_KNOWN_TO_ACTIVE_SQLITE),
        postgresql_where=text(_KNOWN_TO_ACTIVE_PG),
    )
    op.create_index(
        "ix_funding_rates_public_id",
        "funding_rates",
        ["public_id"],
        unique=True,
        sqlite_where=text(_KNOWN_TO_ACTIVE_SQLITE),
        postgresql_where=text(_KNOWN_TO_ACTIVE_PG),
    )
    op.create_index(
        "ix_funding_rates_instrument_public_id",
        "funding_rates",
        ["instrument_public_id"],
    )
    op.create_table(
        "accrual_ledger",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("public_id", sa.String(36), nullable=False),
        sa.Column("instrument_public_id", sa.String(36), nullable=False),
        sa.Column("mode", sa.String(8), nullable=False),
        sa.Column("accrual_type", sa.String(16), nullable=False),
        sa.Column("accrued_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("amount", sa.Float(), nullable=False),
        sa.Column("amount_asset", sa.String(16), nullable=False),
        sa.Column("rate", sa.Float(), nullable=False),
        sa.Column("notional", sa.Float(), nullable=False),
        sa.Column("position_quantity_at_accrual", sa.Float(), nullable=False),
        sa.Column("exchange", sa.String(32), nullable=False),
        sa.Column("wallet_public_id", sa.String(36), nullable=False),
        sa.Column("operator_public_id", sa.String(36), nullable=True),
        sa.Column("session_id", sa.String(36), nullable=False),
        sa.Column("sequence_id", sa.Integer(), nullable=False),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("known_to", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.CheckConstraint(_CK_EXCHANGE_LOWER, name="ck_accrual_ledger_exchange_lower"),
        sa.CheckConstraint(
            "mode IN ('live', 'paper', 'backtest')",
            name="ck_accrual_ledger_mode",
        ),
        sa.CheckConstraint(
            "accrual_type IN ('funding', 'rollover', 'borrow')",
            name="ck_accrual_ledger_type",
        ),
        sa.CheckConstraint(_CK_SESSION_ID, name="ck_accrual_ledger_session_id"),
        sa.CheckConstraint(_CK_SEQUENCE_ID, name="ck_accrual_ledger_sequence_id"),
    )
    op.create_index(
        "ix_accrual_ledger_unique_active",
        "accrual_ledger",
        [
            "wallet_public_id",
            "instrument_public_id",
            "mode",
            "accrual_type",
            "accrued_at",
        ],
        unique=True,
        sqlite_where=text(_KNOWN_TO_ACTIVE_SQLITE),
        postgresql_where=text(_KNOWN_TO_ACTIVE_PG),
    )
    op.create_index(
        "ix_accrual_ledger_public_id",
        "accrual_ledger",
        ["public_id"],
        unique=True,
        sqlite_where=text(_KNOWN_TO_ACTIVE_SQLITE),
        postgresql_where=text(_KNOWN_TO_ACTIVE_PG),
    )
    op.create_index(
        "ix_accrual_ledger_recovery",
        "accrual_ledger",
        [
            "wallet_public_id",
            "instrument_public_id",
            "mode",
            "accrued_at",
        ],
    )
    op.create_index(
        "ix_accrual_ledger_accrued_at",
        "accrual_ledger",
        ["accrued_at"],
    )
    op.create_index(
        "ix_accrual_ledger_instrument_public_id",
        "accrual_ledger",
        ["instrument_public_id"],
    )
    op.create_table(
        "wallets",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("public_id", sa.String(36), nullable=False),
        sa.Column("session_id", sa.String(36), nullable=False),
        sa.Column("sequence_id", sa.Integer(), nullable=False),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("known_to", sa.DateTime(timezone=True), nullable=False),
        sa.Column("label", sa.String(128), nullable=False),
        sa.Column("description", sa.String(512), nullable=True),
        sa.Column(
            "is_paper",
            sa.Boolean(),
            nullable=False,
            server_default=sa.text("false"),
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.CheckConstraint(_CK_SESSION_ID, name="ck_wallets_session_id"),
        sa.CheckConstraint(_CK_SEQUENCE_ID, name="ck_wallets_sequence_id"),
    )
    op.create_index(
        "ix_wallets_public_id",
        "wallets",
        ["public_id"],
        unique=True,
        sqlite_where=text(_KNOWN_TO_ACTIVE_SQLITE),
        postgresql_where=text(_KNOWN_TO_ACTIVE_PG),
    )
    op.create_index(
        "ix_wallets_label_is_paper_active",
        "wallets",
        ["label", "is_paper"],
        unique=True,
        sqlite_where=text(_KNOWN_TO_ACTIVE_SQLITE),
        postgresql_where=text(_KNOWN_TO_ACTIVE_PG),
    )
    op.create_table(
        "wallet_credentials",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("public_id", sa.String(36), nullable=False),
        sa.Column("session_id", sa.String(36), nullable=False),
        sa.Column("sequence_id", sa.Integer(), nullable=False),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("known_to", sa.DateTime(timezone=True), nullable=False),
        sa.Column("wallet_public_id", sa.String(36), nullable=False),
        sa.Column("exchange", sa.String(20), nullable=False),
        sa.Column("credential_type", sa.String(32), nullable=False),
        sa.Column("encrypted_payload", sa.Text(), nullable=False),
        sa.Column("label", sa.String(128), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        sa.CheckConstraint(_CK_EXCHANGE_LOWER, name="ck_wallet_credentials_exchange_lower"),
        sa.CheckConstraint(
            "credential_type IN ('api_key_secret', 'rsa_pem', 'oauth', 'paper')",
            name="ck_wallet_credentials_type",
        ),
        sa.CheckConstraint(_CK_SESSION_ID, name="ck_wallet_credentials_session_id"),
        sa.CheckConstraint(_CK_SEQUENCE_ID, name="ck_wallet_credentials_sequence_id"),
    )
    op.create_index(
        "ix_wallet_credentials_public_id",
        "wallet_credentials",
        ["public_id"],
        unique=True,
        sqlite_where=text(_KNOWN_TO_ACTIVE_SQLITE),
        postgresql_where=text(_KNOWN_TO_ACTIVE_PG),
    )
    op.create_index(
        "ix_wallet_credentials_wallet_exchange_active",
        "wallet_credentials",
        ["wallet_public_id", "exchange"],
        unique=True,
        sqlite_where=text(_KNOWN_TO_ACTIVE_SQLITE),
        postgresql_where=text(_KNOWN_TO_ACTIVE_PG),
    )
    op.create_index(
        "ix_wallet_credentials_exchange",
        "wallet_credentials",
        ["exchange"],
    )
    op.create_table(
        "operators",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("public_id", sa.String(36), nullable=False),
        sa.Column("session_id", sa.String(36), nullable=False),
        sa.Column("sequence_id", sa.Integer(), nullable=False),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("known_to", sa.DateTime(timezone=True), nullable=False),
        sa.Column("label", sa.String(128), nullable=False),
        sa.Column("description", sa.String(512), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        sa.CheckConstraint(_CK_SESSION_ID, name="ck_operators_session_id"),
        sa.CheckConstraint(_CK_SEQUENCE_ID, name="ck_operators_sequence_id"),
    )
    op.create_index(
        "ix_operators_public_id",
        "operators",
        ["public_id"],
        unique=True,
        sqlite_where=text(_KNOWN_TO_ACTIVE_SQLITE),
        postgresql_where=text(_KNOWN_TO_ACTIVE_PG),
    )
    op.create_index(
        "ix_operators_label_active",
        "operators",
        ["label"],
        unique=True,
        sqlite_where=text(_KNOWN_TO_ACTIVE_SQLITE),
        postgresql_where=text(_KNOWN_TO_ACTIVE_PG),
    )
    op.create_table(
        "user_operator_memberships",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("public_id", sa.String(36), nullable=False),
        sa.Column("session_id", sa.String(36), nullable=False),
        sa.Column("sequence_id", sa.Integer(), nullable=False),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("known_to", sa.DateTime(timezone=True), nullable=False),
        sa.Column("user_public_id", sa.String(36), nullable=False),
        sa.Column("operator_public_id", sa.String(36), nullable=False),
        sa.Column(
            "is_primary",
            sa.Boolean(),
            nullable=False,
            server_default=sa.text("false"),
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.CheckConstraint(_CK_SESSION_ID, name="ck_user_operator_memberships_session_id"),
        sa.CheckConstraint(_CK_SEQUENCE_ID, name="ck_user_operator_memberships_sequence_id"),
    )
    op.create_index(
        "ix_user_operator_memberships_public_id",
        "user_operator_memberships",
        ["public_id"],
        unique=True,
        sqlite_where=text(_KNOWN_TO_ACTIVE_SQLITE),
        postgresql_where=text(_KNOWN_TO_ACTIVE_PG),
    )
    op.create_index(
        "ix_user_operator_memberships_unique_active",
        "user_operator_memberships",
        ["user_public_id", "operator_public_id"],
        unique=True,
        sqlite_where=text(_KNOWN_TO_ACTIVE_SQLITE),
        postgresql_where=text(_KNOWN_TO_ACTIVE_PG),
    )
    op.create_index(
        "ix_user_operator_memberships_primary_unique_active",
        "user_operator_memberships",
        ["user_public_id"],
        unique=True,
        sqlite_where=text("is_primary = 1 AND known_to = '9999-12-31 23:59:59.000000'"),
        postgresql_where=text("is_primary = TRUE AND known_to = '9999-12-31T23:59:59+00:00'"),
    )
    op.create_index(
        "ix_user_operator_memberships_user",
        "user_operator_memberships",
        ["user_public_id"],
    )
    op.create_index(
        "ix_user_operator_memberships_operator",
        "user_operator_memberships",
        ["operator_public_id"],
    )
    op.create_table(
        "wallet_operator_scope_grants",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("public_id", sa.String(36), nullable=False),
        sa.Column("session_id", sa.String(36), nullable=False),
        sa.Column("sequence_id", sa.Integer(), nullable=False),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("known_to", sa.DateTime(timezone=True), nullable=False),
        sa.Column("operator_public_id", sa.String(36), nullable=False),
        sa.Column("wallet_public_id", sa.String(36), nullable=False),
        sa.Column("granted_by_user_public_id", sa.String(36), nullable=False),
        sa.Column("scope_kind", sa.String(16), nullable=False),
        sa.Column("underlying_public_id", sa.String(36), nullable=True),
        sa.Column("instrument_public_id", sa.String(36), nullable=True),
        sa.Column("note", sa.String(512), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        sa.CheckConstraint(
            "scope_kind IN ('underlying', 'instrument')",
            name="ck_scope_grants_scope_kind",
        ),
        sa.CheckConstraint(
            "(scope_kind = 'underlying' AND underlying_public_id IS NOT NULL "
            "AND instrument_public_id IS NULL) "
            "OR (scope_kind = 'instrument' AND instrument_public_id IS NOT NULL "
            "AND underlying_public_id IS NULL)",
            name="ck_scope_grants_scope_kind_xor",
        ),
        sa.CheckConstraint(_CK_SESSION_ID, name="ck_scope_grants_session_id"),
        sa.CheckConstraint(_CK_SEQUENCE_ID, name="ck_scope_grants_sequence_id"),
    )
    op.create_index(
        "ix_scope_grants_public_id",
        "wallet_operator_scope_grants",
        ["public_id"],
        unique=True,
        sqlite_where=text(_KNOWN_TO_ACTIVE_SQLITE),
        postgresql_where=text(_KNOWN_TO_ACTIVE_PG),
    )
    op.create_index(
        "ix_scope_grants_instrument_exclusive_active",
        "wallet_operator_scope_grants",
        ["wallet_public_id", "instrument_public_id"],
        unique=True,
        sqlite_where=text(
            "instrument_public_id IS NOT NULL AND known_to = '9999-12-31 23:59:59.000000'"
        ),
        postgresql_where=text(
            "instrument_public_id IS NOT NULL AND known_to = '9999-12-31T23:59:59+00:00'"
        ),
    )
    op.create_index(
        "ix_scope_grants_underlying_exclusive_active",
        "wallet_operator_scope_grants",
        ["wallet_public_id", "underlying_public_id"],
        unique=True,
        sqlite_where=text(
            "underlying_public_id IS NOT NULL AND known_to = '9999-12-31 23:59:59.000000'"
        ),
        postgresql_where=text(
            "underlying_public_id IS NOT NULL AND known_to = '9999-12-31T23:59:59+00:00'"
        ),
    )
    op.create_index(
        "ix_scope_grants_operator",
        "wallet_operator_scope_grants",
        ["operator_public_id"],
    )
    op.create_index(
        "ix_scope_grants_wallet",
        "wallet_operator_scope_grants",
        ["wallet_public_id"],
    )
    op.create_table(
        "instrument_order_capabilities",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("public_id", sa.String(36), nullable=False),
        sa.Column("instrument_public_id", sa.String(36), nullable=False),
        sa.Column("exchange", sa.String(32), nullable=False),
        sa.Column("supported_order_types", sa.JSON(), nullable=False),
        sa.Column(
            "supports_post_only", sa.Boolean(), nullable=False, server_default=sa.text("false")
        ),
        sa.Column(
            "supports_reduce_only", sa.Boolean(), nullable=False, server_default=sa.text("false")
        ),
        sa.Column(
            "supports_amend_in_place", sa.Boolean(), nullable=False, server_default=sa.text("false")
        ),
        sa.Column(
            "supports_native_stop_loss",
            sa.Boolean(),
            nullable=False,
            server_default=sa.text("false"),
        ),
        sa.Column(
            "supports_native_take_profit",
            sa.Boolean(),
            nullable=False,
            server_default=sa.text("false"),
        ),
        sa.Column(
            "supports_trailing_stop_client_side",
            sa.Boolean(),
            nullable=False,
            server_default="1",
        ),
        sa.Column(
            "supports_market_making", sa.Boolean(), nullable=False, server_default=sa.text("false")
        ),
        sa.Column(
            "supports_short_selling", sa.Boolean(), nullable=False, server_default=sa.text("false")
        ),
        sa.Column(
            "supports_leverage", sa.Boolean(), nullable=False, server_default=sa.text("false")
        ),
        sa.Column("max_leverage_long", sa.Float(), nullable=False, server_default="1.0"),
        sa.Column("max_leverage_short", sa.Float(), nullable=False, server_default="0.0"),
        sa.Column("min_notional", sa.Float(), nullable=True),
        sa.Column("max_order_size", sa.Float(), nullable=True),
        sa.Column("top_of_book_quality", sa.String(16), nullable=False, server_default="unknown"),
        sa.Column("session_id", sa.String(36), nullable=False),
        sa.Column("sequence_id", sa.Integer(), nullable=False),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("known_to", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.CheckConstraint(_CK_EXCHANGE_LOWER, name="ck_ioc_exchange_lower"),
        sa.CheckConstraint(
            "top_of_book_quality IN ('realtime', 'polled', 'thin', 'unknown')",
            name="ck_ioc_tob_quality",
        ),
    )
    op.create_index(
        "ix_ioc_instrument_exchange_active",
        "instrument_order_capabilities",
        ["instrument_public_id", "exchange"],
        unique=True,
        sqlite_where=text(_KNOWN_TO_ACTIVE_SQLITE),
        postgresql_where=text(_KNOWN_TO_ACTIVE_PG),
    )
    op.create_index(
        "ix_ioc_public_id",
        "instrument_order_capabilities",
        ["public_id"],
        unique=True,
        sqlite_where=text(_KNOWN_TO_ACTIVE_SQLITE),
        postgresql_where=text(_KNOWN_TO_ACTIVE_PG),
    )
    op.create_index(
        "ix_ioc_instrument_public_id",
        "instrument_order_capabilities",
        ["instrument_public_id"],
    )
    op.create_index(
        "ix_ioc_exchange",
        "instrument_order_capabilities",
        ["exchange"],
    )
    op.create_table(
        "venue_fee_schedules",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("public_id", sa.String(36), nullable=False),
        sa.Column("exchange", sa.String(32), nullable=False),
        sa.Column("instrument_public_id", sa.String(36), nullable=True),
        sa.Column("fee_tier", sa.String(32), nullable=False),
        sa.Column("maker_bps", sa.Float(), nullable=False),
        sa.Column("taker_bps", sa.Float(), nullable=False),
        sa.Column("min_volume_30d", sa.Float(), nullable=True),
        sa.Column("currency", sa.String(8), nullable=False),
        sa.Column("session_id", sa.String(36), nullable=False),
        sa.Column("sequence_id", sa.Integer(), nullable=False),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("known_to", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.CheckConstraint(_CK_EXCHANGE_LOWER, name="ck_vfs_exchange_lower"),
    )
    op.create_index(
        "ix_vfs_exchange_tier_active",
        "venue_fee_schedules",
        ["exchange", "fee_tier"],
        unique=True,
        sqlite_where=text(_KNOWN_TO_ACTIVE_SQLITE),
        postgresql_where=text(_KNOWN_TO_ACTIVE_PG),
    )
    op.create_index(
        "ix_vfs_public_id",
        "venue_fee_schedules",
        ["public_id"],
        unique=True,
        sqlite_where=text(_KNOWN_TO_ACTIVE_SQLITE),
        postgresql_where=text(_KNOWN_TO_ACTIVE_PG),
    )
    op.create_index(
        "ix_vfs_exchange",
        "venue_fee_schedules",
        ["exchange"],
    )
    op.create_table(
        "execution_plans",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("public_id", sa.String(36), nullable=False),
        sa.Column("plan_type", sa.String(32), nullable=False),
        sa.Column("created_by_user_id", sa.String(36), nullable=True),
        sa.Column("created_by_strategy", sa.String(128), nullable=True),
        sa.Column("created_via", sa.String(16), nullable=False),
        sa.Column("instrument_public_id", sa.String(36), nullable=False),
        sa.Column("exchange", sa.String(32), nullable=False),
        sa.Column("mode", sa.String(8), nullable=False),
        sa.Column("shard_key", sa.String(128), nullable=False),
        sa.Column("wallet_public_id", sa.String(36), nullable=False),
        sa.Column("operator_public_id", sa.String(36), nullable=True),
        sa.Column("total_quantity", sa.Float(), nullable=False),
        sa.Column("filled_quantity", sa.Float(), nullable=False, server_default="0"),
        sa.Column("side", sa.String(8), nullable=False),
        sa.Column("parent_plan_public_id", sa.String(36), nullable=True),
        sa.Column("position_cycle_public_id", sa.String(36), nullable=True),
        sa.Column("params", sa.JSON(), nullable=False),
        sa.Column("status", sa.String(20), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("cancel_requested_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_evaluated_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_error", sa.String(1024), nullable=True),
        sa.Column("idempotency_key", sa.String(64), nullable=True),
        sa.Column("cancel_idempotency_key", sa.String(64), nullable=True),
        sa.Column("session_id", sa.String(36), nullable=False),
        sa.Column("sequence_id", sa.Integer(), nullable=False),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("known_to", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.CheckConstraint(_CK_EXCHANGE_LOWER, name="ck_ep_exchange_lower"),
        sa.CheckConstraint(
            "plan_type IN ('manual_once', 'bracket', 'trailing_stop', "
            "'passive_mm', 'peg', 'scheduler')",
            name="ck_ep_plan_type",
        ),
        sa.CheckConstraint(
            "status IN ('pending', 'armed', 'active', 'paused', 'completed', "
            "'cancel_requested', 'cancelled', 'failed', 'expired')",
            name="ck_ep_status",
        ),
        sa.CheckConstraint("side IN ('buy', 'sell')", name="ck_ep_side"),
        sa.CheckConstraint("mode IN ('live', 'paper')", name="ck_ep_mode"),
        sa.CheckConstraint(
            "created_via IN ('ui', 'api', 'cli', 'strategy')",
            name="ck_ep_created_via",
        ),
    )
    op.create_index("ix_ep_plan_type", "execution_plans", ["plan_type"])
    op.create_index("ix_ep_created_by_user_id", "execution_plans", ["created_by_user_id"])
    op.create_index("ix_ep_instrument_public_id", "execution_plans", ["instrument_public_id"])
    op.create_index("ix_ep_exchange", "execution_plans", ["exchange"])
    op.create_index("ix_ep_shard_key", "execution_plans", ["shard_key"])
    op.create_index("ix_ep_status", "execution_plans", ["status"])
    op.create_index("ix_ep_status_exchange_mode", "execution_plans", ["status", "exchange", "mode"])
    op.create_index(
        "ix_ep_instrument_status", "execution_plans", ["instrument_public_id", "status"]
    )
    op.create_index("ix_ep_shard_status", "execution_plans", ["shard_key", "status"])

    dialect = op.get_bind().dialect.name
    active_filter = _KNOWN_TO_ACTIVE_PG if dialect == "postgresql" else _KNOWN_TO_ACTIVE_SQLITE
    _terminal_statuses = "('completed', 'cancelled', 'failed', 'expired')"
    with op.get_context().autocommit_block():
        op.execute(
            text(
                "CREATE UNIQUE INDEX IF NOT EXISTS uq_ep_idempotency_key "
                "ON execution_plans (idempotency_key) "
                f"WHERE idempotency_key IS NOT NULL AND {active_filter}"
            )
        )
        op.execute(
            text(
                "CREATE UNIQUE INDEX IF NOT EXISTS ix_ep_public_id "
                f"ON execution_plans (public_id) WHERE {active_filter}"
            )
        )
        op.execute(
            text(
                "CREATE UNIQUE INDEX IF NOT EXISTS uq_ep_active_bracket_per_cycle "
                "ON execution_plans (position_cycle_public_id) "
                "WHERE position_cycle_public_id IS NOT NULL "
                f"AND plan_type = 'bracket' "
                f"AND status NOT IN {_terminal_statuses} "
                f"AND {active_filter}"
            )
        )
        op.execute(
            text(
                "CREATE UNIQUE INDEX IF NOT EXISTS uq_ep_active_trailing_stop_per_cycle "
                "ON execution_plans (position_cycle_public_id) "
                "WHERE position_cycle_public_id IS NOT NULL "
                f"AND plan_type = 'trailing_stop' "
                f"AND status NOT IN {_terminal_statuses} "
                f"AND {active_filter}"
            )
        )

    op.create_table(
        "execution_plan_checkpoints",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("public_id", sa.String(36), nullable=False),
        sa.Column("plan_public_id", sa.String(36), nullable=False),
        sa.Column("state", sa.JSON(), nullable=False),
        sa.Column("last_venue_event_id", sa.Integer(), nullable=False),
        sa.Column("last_tick_timestamp", sa.DateTime(timezone=True), nullable=True),
        sa.Column("checkpoint_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("session_id", sa.String(36), nullable=False),
        sa.Column("sequence_id", sa.Integer(), nullable=False),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("known_to", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_epc_plan_public_id", "execution_plan_checkpoints", ["plan_public_id"])

    with op.get_context().autocommit_block():
        op.execute(
            text(
                "CREATE UNIQUE INDEX IF NOT EXISTS ix_epc_public_id "
                f"ON execution_plan_checkpoints (public_id) WHERE {active_filter}"
            )
        )

    op.create_table(
        "execution_plan_decisions",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("public_id", sa.String(36), nullable=False),
        sa.Column("plan_public_id", sa.String(36), nullable=False),
        sa.Column("decision_type", sa.String(32), nullable=False),
        sa.Column("decided_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("trigger_type", sa.String(16), nullable=False),
        sa.Column("evidence", sa.JSON(), nullable=False),
        sa.Column("emitted_command_public_id", sa.String(36), nullable=True),
        sa.Column("new_status", sa.String(20), nullable=True),
        sa.Column("reason", sa.String(512), nullable=False),
        sa.Column("decision_importance", sa.String(16), nullable=False),
        sa.Column("source_surface", sa.String(20), nullable=False, server_default="strategy"),
        sa.Column("session_id", sa.String(36), nullable=False),
        sa.Column("sequence_id", sa.Integer(), nullable=False),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("known_to", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.CheckConstraint(
            "decision_importance IN ('action', 'transition', 'routine')",
            name="ck_epd_importance",
        ),
    )
    op.create_index("ix_epd_plan_public_id", "execution_plan_decisions", ["plan_public_id"])
    op.create_index("ix_epd_decided_at", "execution_plan_decisions", ["decided_at"])

    with op.get_context().autocommit_block():
        op.execute(
            text(
                "CREATE UNIQUE INDEX IF NOT EXISTS ix_epd_public_id "
                f"ON execution_plan_decisions (public_id) WHERE {active_filter}"
            )
        )

    op.create_table(
        "position_cycles",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("public_id", sa.String(36), nullable=False),
        sa.Column("instrument_public_id", sa.String(36), nullable=False),
        sa.Column("exchange", sa.String(32), nullable=False),
        sa.Column("mode", sa.String(8), nullable=False),
        sa.Column("shard_key", sa.String(128), nullable=False),
        sa.Column("wallet_public_id", sa.String(36), nullable=False),
        sa.Column("operator_public_id", sa.String(36), nullable=True),
        sa.Column("direction", sa.String(8), nullable=False),
        sa.Column("max_qty", sa.Float(), nullable=False),
        sa.Column("status", sa.String(16), nullable=False),
        sa.Column("opened_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("closed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("opening_command_public_id", sa.String(36), nullable=True),
        sa.Column("closing_command_public_id", sa.String(36), nullable=True),
        sa.Column("session_id", sa.String(36), nullable=False),
        sa.Column("sequence_id", sa.Integer(), nullable=False),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("known_to", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.CheckConstraint(_CK_EXCHANGE_LOWER, name="ck_pc_exchange_lower"),
        sa.CheckConstraint("mode IN ('live', 'paper')", name="ck_pc_mode"),
        sa.CheckConstraint(
            "direction IN ('long', 'short')",
            name="ck_pc_direction",
        ),
        sa.CheckConstraint(
            "status IN ('open', 'closed', 'liquidated')",
            name="ck_pc_status",
        ),
        sa.CheckConstraint("max_qty >= 0", name="ck_pc_max_qty_nonneg"),
    )
    op.create_index("ix_pc_instrument_public_id", "position_cycles", ["instrument_public_id"])
    op.create_index("ix_pc_exchange", "position_cycles", ["exchange"])
    op.create_index("ix_pc_shard_key", "position_cycles", ["shard_key"])
    op.create_index("ix_pc_wallet_public_id", "position_cycles", ["wallet_public_id"])
    op.create_index("ix_pc_status", "position_cycles", ["status"])
    op.create_index("ix_pc_shard_status", "position_cycles", ["shard_key", "status"])
    op.create_index(
        "ix_pc_instrument_status", "position_cycles", ["instrument_public_id", "status"]
    )
    op.create_index("ix_pc_wallet_status", "position_cycles", ["wallet_public_id", "status"])

    with op.get_context().autocommit_block():
        op.execute(
            text(
                "CREATE UNIQUE INDEX IF NOT EXISTS ix_pc_public_id "
                f"ON position_cycles (public_id) WHERE {active_filter}"
            )
        )
        op.execute(
            text(
                "CREATE UNIQUE INDEX IF NOT EXISTS uq_pc_shard_open_active "
                "ON position_cycles (shard_key) "
                f"WHERE status = 'open' AND {active_filter}"
            )
        )

    conn = op.get_bind()
    now = datetime.now(tz=UTC)
    dialect_name = conn.dialect.name
    bind_now = _bind_datetime_literal(now, dialect_name)
    bind_known_to = _bind_datetime_literal(_KNOWN_TO_MAX, dialect_name)
    seed_session_id = str(uuid7())
    seed_seq = 0
    symbol_public_ids: dict[str, str] = {}
    for entry in SYMBOL_CATALOG:
        spid = str(uuid7())
        symbol_public_ids[entry[0]] = spid
        seed_seq += 1
        conn.execute(
            text("""
                INSERT INTO symbols
                (public_id, native_symbol, base, quote, asset_type, created_at,
                 session_id, sequence_id, timestamp, known_to)
                VALUES (:public_id, :native_symbol, :base, :quote, :asset_type,
                        :created_at, :session_id, :sequence_id, :timestamp, :known_to)
                """),
            {
                "public_id": spid,
                "native_symbol": entry[0],
                "base": entry[1],
                "quote": entry[2],
                "asset_type": entry[3],
                "created_at": bind_now,
                "session_id": seed_session_id,
                "sequence_id": seed_seq,
                "timestamp": bind_now,
                "known_to": bind_known_to,
            },
        )
    for alias in SYMBOL_ALIASES:
        seed_seq += 1
        conn.execute(
            text("""
                INSERT INTO symbol_aliases
                (public_id, symbol_public_id, exchange, channel, exchange_symbol,
                 created_at, session_id, sequence_id, timestamp, known_to)
                VALUES (:public_id, :symbol_public_id, :exchange, :channel, :exchange_symbol,
                        :created_at, :session_id, :sequence_id, :timestamp, :known_to)
                """),
            {
                "public_id": str(uuid7()),
                "symbol_public_id": symbol_public_ids[alias[0]],
                "exchange": alias[1],
                "channel": alias[2],
                "exchange_symbol": alias[3],
                "created_at": bind_now,
                "session_id": seed_session_id,
                "sequence_id": seed_seq,
                "timestamp": bind_now,
                "known_to": bind_known_to,
            },
        )
    for cap in SYMBOL_CAPABILITIES:
        seed_seq += 1
        conn.execute(
            text("""
                INSERT INTO symbol_exchange_capabilities
                (public_id, symbol_public_id, exchange, can_market_data, can_trade,
                 source, created_at, session_id, sequence_id, timestamp, known_to)
                VALUES (:public_id, :symbol_public_id, :exchange, :can_market_data, :can_trade,
                        :source, :created_at, :session_id, :sequence_id, :timestamp, :known_to)
                """),
            {
                "public_id": str(uuid7()),
                "symbol_public_id": symbol_public_ids[cap[0]],
                "exchange": cap[1],
                "can_market_data": cap[2],
                "can_trade": cap[3],
                "source": "seed",
                "created_at": bind_now,
                "session_id": seed_session_id,
                "sequence_id": seed_seq,
                "timestamp": bind_now,
                "known_to": bind_known_to,
            },
        )

    op.create_table(
        "notification_devices",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("public_id", sa.String(36), nullable=False),
        sa.Column("session_id", sa.String(36), nullable=False),
        sa.Column("sequence_id", sa.Integer(), nullable=False),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("known_to", sa.DateTime(timezone=True), nullable=False),
        sa.Column("user_public_id", sa.String(36), nullable=False),
        sa.Column("device_token", sa.String(200), nullable=False),
        sa.Column("device_id", sa.String(64), nullable=False),
        sa.Column("platform", sa.String(10), server_default="ios", nullable=False),
        sa.Column("env", sa.String(10), nullable=False),
        sa.Column("app_version", sa.String(20), nullable=True),
        sa.Column("previews_mode", sa.String(10), server_default="private", nullable=False),
        sa.Column("registered_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_seen_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("token_status", sa.String(20), server_default="active", nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.CheckConstraint(_CK_SESSION_ID, name="ck_notification_devices_session_id"),
        sa.CheckConstraint(_CK_SEQUENCE_ID, name="ck_notification_devices_sequence_id"),
        sa.CheckConstraint(
            _CK_NOTIFICATION_DEVICE_TOKEN_STATUS,
            name="ck_notification_devices_token_status",
        ),
    )
    op.create_index(
        "ix_notification_devices_public_id",
        "notification_devices",
        ["public_id"],
        unique=True,
        sqlite_where=text(_NOTIFICATION_DEVICE_ACTIVE_SQLITE),
        postgresql_where=text(_NOTIFICATION_DEVICE_ACTIVE_PG),
    )
    op.create_index(
        "uq_notification_devices_token_active",
        "notification_devices",
        ["device_token"],
        unique=True,
        sqlite_where=text(_NOTIFICATION_DEVICE_ACTIVE_SQLITE),
        postgresql_where=text(_NOTIFICATION_DEVICE_ACTIVE_PG),
    )
    op.create_index(
        "ix_notification_devices_user_active",
        "notification_devices",
        ["user_public_id"],
        sqlite_where=text(_NOTIFICATION_DEVICE_ACTIVE_SQLITE),
        postgresql_where=text(_NOTIFICATION_DEVICE_ACTIVE_PG),
    )
    op.create_table(
        "device_alert_prefs",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("public_id", sa.String(36), nullable=False),
        sa.Column("session_id", sa.String(36), nullable=False),
        sa.Column("sequence_id", sa.Integer(), nullable=False),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("known_to", sa.DateTime(timezone=True), nullable=False),
        sa.Column("device_public_id", sa.String(36), nullable=False),
        sa.Column("alert_type", sa.String(50), nullable=False),
        sa.Column("operator_public_id", sa.String(36), nullable=True),
        sa.Column("wallet_public_id", sa.String(36), nullable=True),
        sa.Column("enabled", sa.Boolean(), server_default=sa.text("true"), nullable=False),
        sa.Column("min_priority", sa.String(10), server_default="medium", nullable=False),
        sa.Column("quiet_hours_start_min", sa.Integer(), nullable=True),
        sa.Column("quiet_hours_end_min", sa.Integer(), nullable=True),
        sa.Column("mute_until", sa.DateTime(timezone=True), nullable=True),
        sa.Column("timezone", sa.String(64), server_default="UTC", nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.CheckConstraint(
            "NOT (wallet_public_id IS NOT NULL AND operator_public_id IS NULL)",
            name="ck_device_alert_valid_scope",
        ),
        sa.CheckConstraint(_CK_SESSION_ID, name="ck_device_alert_prefs_session_id"),
        sa.CheckConstraint(_CK_SEQUENCE_ID, name="ck_device_alert_prefs_sequence_id"),
    )
    op.create_index(
        "ix_device_alert_prefs_public_id",
        "device_alert_prefs",
        ["public_id"],
        unique=True,
        sqlite_where=text(_KNOWN_TO_ACTIVE_SQLITE),
        postgresql_where=text(_KNOWN_TO_ACTIVE_PG),
    )
    op.create_index(
        "ix_device_alert_prefs_lookup",
        "device_alert_prefs",
        ["device_public_id", "alert_type"],
        sqlite_where=text(_KNOWN_TO_ACTIVE_SQLITE),
        postgresql_where=text(_KNOWN_TO_ACTIVE_PG),
    )
    op.create_index(
        "uq_device_alert_wallet_scope",
        "device_alert_prefs",
        ["device_public_id", "alert_type", "operator_public_id", "wallet_public_id"],
        unique=True,
        sqlite_where=text(
            f"{_KNOWN_TO_ACTIVE_SQLITE} AND "
            "operator_public_id IS NOT NULL AND wallet_public_id IS NOT NULL"
        ),
        postgresql_where=text(
            f"{_KNOWN_TO_ACTIVE_PG} AND "
            "operator_public_id IS NOT NULL AND wallet_public_id IS NOT NULL"
        ),
    )
    op.create_index(
        "uq_device_alert_operator_scope",
        "device_alert_prefs",
        ["device_public_id", "alert_type", "operator_public_id"],
        unique=True,
        sqlite_where=text(
            f"{_KNOWN_TO_ACTIVE_SQLITE} AND "
            "operator_public_id IS NOT NULL AND wallet_public_id IS NULL"
        ),
        postgresql_where=text(
            f"{_KNOWN_TO_ACTIVE_PG} AND "
            "operator_public_id IS NOT NULL AND wallet_public_id IS NULL"
        ),
    )
    op.create_index(
        "uq_device_alert_device_scope",
        "device_alert_prefs",
        ["device_public_id", "alert_type"],
        unique=True,
        sqlite_where=text(
            f"{_KNOWN_TO_ACTIVE_SQLITE} AND "
            "operator_public_id IS NULL AND wallet_public_id IS NULL"
        ),
        postgresql_where=text(
            f"{_KNOWN_TO_ACTIVE_PG} AND operator_public_id IS NULL AND wallet_public_id IS NULL"
        ),
    )
    op.create_table(
        "user_alert_defaults",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("public_id", sa.String(36), nullable=False),
        sa.Column("session_id", sa.String(36), nullable=False),
        sa.Column("sequence_id", sa.Integer(), nullable=False),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("known_to", sa.DateTime(timezone=True), nullable=False),
        sa.Column("user_public_id", sa.String(36), nullable=False),
        sa.Column("alert_type", sa.String(50), nullable=False),
        sa.Column("enabled", sa.Boolean(), server_default=sa.text("true"), nullable=False),
        sa.Column("min_priority", sa.String(10), server_default="medium", nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.CheckConstraint(_CK_SESSION_ID, name="ck_user_alert_defaults_session_id"),
        sa.CheckConstraint(_CK_SEQUENCE_ID, name="ck_user_alert_defaults_sequence_id"),
    )
    op.create_index(
        "ix_user_alert_defaults_public_id",
        "user_alert_defaults",
        ["public_id"],
        unique=True,
        sqlite_where=text(_KNOWN_TO_ACTIVE_SQLITE),
        postgresql_where=text(_KNOWN_TO_ACTIVE_PG),
    )
    op.create_index(
        "uq_user_alert_default_active",
        "user_alert_defaults",
        ["user_public_id", "alert_type"],
        unique=True,
        sqlite_where=text(_KNOWN_TO_ACTIVE_SQLITE),
        postgresql_where=text(_KNOWN_TO_ACTIVE_PG),
    )
    op.create_table(
        "alert_events",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("public_id", sa.String(36), nullable=False),
        sa.Column("session_id", sa.String(36), nullable=False),
        sa.Column("sequence_id", sa.Integer(), nullable=False),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("known_to", sa.DateTime(timezone=True), nullable=False),
        sa.Column("user_public_id", sa.String(36), nullable=False),
        sa.Column("operator_public_id", sa.String(36), nullable=True),
        sa.Column("wallet_public_id", sa.String(36), nullable=True),
        sa.Column("alert_type", sa.String(50), nullable=False),
        sa.Column("priority", sa.String(10), nullable=False),
        sa.Column(
            "is_safety_critical", sa.Boolean(), server_default=sa.text("false"), nullable=False
        ),
        sa.Column("title", sa.String(200), nullable=False),
        sa.Column("body", sa.String(500), nullable=False),
        sa.Column("payload", sa.JSON(), nullable=True),
        sa.Column("dedup_key", sa.String(200), nullable=True),
        sa.Column("thread_key", sa.String(100), nullable=True),
        sa.Column("source_topic", sa.String(200), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        sa.CheckConstraint(_CK_SESSION_ID, name="ck_alert_events_session_id"),
        sa.CheckConstraint(_CK_SEQUENCE_ID, name="ck_alert_events_sequence_id"),
    )
    op.create_index(
        "ix_alert_events_public_id_active",
        "alert_events",
        ["public_id"],
        unique=True,
        sqlite_where=text(_KNOWN_TO_ACTIVE_SQLITE),
        postgresql_where=text(_KNOWN_TO_ACTIVE_PG),
    )
    op.create_index(
        "ix_alert_events_user_type_time",
        "alert_events",
        ["user_public_id", "alert_type", "timestamp"],
        sqlite_where=text(_KNOWN_TO_ACTIVE_SQLITE),
        postgresql_where=text(_KNOWN_TO_ACTIVE_PG),
    )
    op.create_index(
        "ix_alert_events_dedup",
        "alert_events",
        ["user_public_id", "dedup_key", "timestamp"],
        sqlite_where=text(_KNOWN_TO_ACTIVE_SQLITE),
        postgresql_where=text(_KNOWN_TO_ACTIVE_PG),
    )
    op.create_table(
        "alert_deliveries",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("public_id", sa.String(36), nullable=False),
        sa.Column("session_id", sa.String(36), nullable=False),
        sa.Column("sequence_id", sa.Integer(), nullable=False),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("known_to", sa.DateTime(timezone=True), nullable=False),
        sa.Column("alert_event_public_id", sa.String(36), nullable=False),
        sa.Column("device_public_id", sa.String(36), nullable=False),
        sa.Column("user_public_id", sa.String(36), nullable=False),
        sa.Column("operator_public_id", sa.String(36), nullable=True),
        sa.Column("wallet_public_id", sa.String(36), nullable=True),
        sa.Column("status", sa.String(20), nullable=False),
        sa.Column("attempt_count", sa.Integer(), server_default="0", nullable=False),
        sa.Column("last_attempt_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("next_attempt_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("apns_id", sa.String(64), nullable=True),
        sa.Column("error_reason", sa.String(200), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.CheckConstraint(
            "status IN ('queued', 'sent', 'failed', 'unregistered', 'cancelled_scope')",
            name="ck_alert_delivery_status",
        ),
        sa.CheckConstraint(_CK_SESSION_ID, name="ck_alert_deliveries_session_id"),
        sa.CheckConstraint(_CK_SEQUENCE_ID, name="ck_alert_deliveries_sequence_id"),
    )
    op.create_index(
        "ix_alert_deliveries_public_id",
        "alert_deliveries",
        ["public_id"],
        unique=True,
        sqlite_where=text(_KNOWN_TO_ACTIVE_SQLITE),
        postgresql_where=text(_KNOWN_TO_ACTIVE_PG),
    )
    op.create_index(
        "ix_alert_deliveries_alert_event_public_id",
        "alert_deliveries",
        ["alert_event_public_id"],
        sqlite_where=text(_KNOWN_TO_ACTIVE_SQLITE),
        postgresql_where=text(_KNOWN_TO_ACTIVE_PG),
    )
    op.create_index(
        "ix_alert_deliveries_device_public_id",
        "alert_deliveries",
        ["device_public_id"],
        sqlite_where=text(_KNOWN_TO_ACTIVE_SQLITE),
        postgresql_where=text(_KNOWN_TO_ACTIVE_PG),
    )
    op.create_index(
        "ix_alert_deliveries_status_queued",
        "alert_deliveries",
        ["created_at"],
        sqlite_where=text(f"{_KNOWN_TO_ACTIVE_SQLITE} AND status = 'queued'"),
        postgresql_where=text(f"{_KNOWN_TO_ACTIVE_PG} AND status = 'queued'"),
    )
    op.create_index(
        "ix_alert_deliveries_next_attempt",
        "alert_deliveries",
        ["next_attempt_at"],
        sqlite_where=text(f"{_KNOWN_TO_ACTIVE_SQLITE} AND status = 'queued'"),
        postgresql_where=text(f"{_KNOWN_TO_ACTIVE_PG} AND status = 'queued'"),
    )
    op.create_index(
        "ix_alert_deliveries_scope_status",
        "alert_deliveries",
        ["user_public_id", "operator_public_id", "wallet_public_id", "status"],
        sqlite_where=text(_KNOWN_TO_ACTIVE_SQLITE),
        postgresql_where=text(_KNOWN_TO_ACTIVE_PG),
    )

    op.create_table(
        "ai_delegates",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("public_id", sa.String(36), nullable=False),
        sa.Column("user_public_id", sa.String(36), nullable=False),
        sa.Column("last_seen_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "active_reviews_count",
            sa.Integer(),
            nullable=False,
            server_default="0",
        ),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("public_id", name="uq_ai_delegates_public_id"),
        sa.UniqueConstraint("user_public_id", name="uq_ai_delegates_user_public_id"),
        sa.CheckConstraint(
            "active_reviews_count >= 0",
            name="ck_ai_delegates_active_reviews_nonneg",
        ),
    )
    op.create_index("ix_ai_delegates_last_seen_at", "ai_delegates", ["last_seen_at"])

    op.create_table(
        "ai_reviews",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("public_id", sa.String(36), nullable=False),
        sa.Column("session_id", sa.String(36), nullable=False),
        sa.Column("sequence_id", sa.Integer(), nullable=False),
        sa.Column("user_public_id", sa.String(36), nullable=False),
        sa.Column("operator_public_id", sa.String(36), nullable=False),
        sa.Column("wallet_public_id", sa.String(36), nullable=False),
        sa.Column("instrument_public_id", sa.String(36), nullable=False),
        sa.Column("strategy_public_id", sa.String(36), nullable=False),
        sa.Column("selected_delegate_public_id", sa.String(36), nullable=False),
        sa.Column("responding_delegate_public_id", sa.String(36), nullable=True),
        sa.Column("resolution_mode", sa.String(32), nullable=True),
        sa.Column("status", sa.String(24), nullable=False, server_default="pending"),
        sa.Column("signal_envelope", sa.JSON(), nullable=False),
        sa.Column("signal_snapshot_hash", sa.String(64), nullable=False),
        sa.Column("instrument_metadata", sa.JSON(), nullable=False),
        sa.Column("deadline", sa.DateTime(timezone=True), nullable=False),
        sa.Column("fanout_after", sa.DateTime(timezone=True), nullable=False),
        sa.Column("decision", sa.String(8), nullable=True),
        sa.Column("rationale", sa.String(4096), nullable=True),
        sa.Column(
            "dispatch_version",
            sa.Integer(),
            nullable=False,
            server_default="0",
        ),
        sa.Column("counter_decremented_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("resolved_at", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("public_id", name="uq_ai_reviews_public_id"),
        sa.CheckConstraint(
            "(status IN ('pending', 'fanout_dispatched') AND decision IS NULL "
            " AND responding_delegate_public_id IS NULL AND resolution_mode IS NULL "
            " AND resolved_at IS NULL)"
            " OR "
            "(status IN ('resolved_approved', 'resolved_rejected') AND decision IS NOT NULL "
            " AND responding_delegate_public_id IS NOT NULL AND resolution_mode IS NOT NULL "
            " AND resolved_at IS NOT NULL)"
            " OR "
            "(status = 'timeout' AND decision IS NULL "
            " AND resolution_mode = 'timeout_no_response' AND resolved_at IS NOT NULL)"
            " OR "
            "(status = 'superseded' AND decision IS NULL "
            " AND resolution_mode = 'superseded_by_strategy' AND resolved_at IS NOT NULL)",
            name="ck_ai_reviews_status_consistency",
        ),
        sa.CheckConstraint("deadline > created_at", name="ck_ai_reviews_deadline_future"),
        sa.CheckConstraint("dispatch_version >= 0", name="ck_ai_reviews_dispatch_version_nonneg"),
        sa.CheckConstraint(
            "status IN ('pending', 'fanout_dispatched', 'resolved_approved', "
            "'resolved_rejected', 'timeout', 'superseded')",
            name="ck_ai_reviews_status_enum",
        ),
        sa.CheckConstraint(
            "decision IS NULL OR decision IN ('approve', 'reject')",
            name="ck_ai_reviews_decision_enum",
        ),
        sa.CheckConstraint(
            "resolution_mode IS NULL OR resolution_mode IN ("
            "'pick_one_primary', 'secondary_after_fanout', 'fanout_first_responder', "
            "'timeout_no_response', 'superseded_by_strategy')",
            name="ck_ai_reviews_resolution_mode_enum",
        ),
    )
    op.create_index("ix_ai_reviews_public_id_lookup", "ai_reviews", ["public_id"])
    op.create_index(
        "ix_ai_reviews_pending_per_delegate",
        "ai_reviews",
        ["selected_delegate_public_id", "status"],
    )
    op.create_index("ix_ai_reviews_deadline_pending", "ai_reviews", ["deadline", "status"])
    op.create_index(
        "ix_ai_reviews_strategy_pending", "ai_reviews", ["strategy_public_id", "status"]
    )
    op.create_index("ix_ai_reviews_user_public_id", "ai_reviews", ["user_public_id"])
    op.create_index("ix_ai_reviews_operator_public_id", "ai_reviews", ["operator_public_id"])
    op.create_index("ix_ai_reviews_wallet_public_id", "ai_reviews", ["wallet_public_id"])
    op.create_index("ix_ai_reviews_instrument_public_id", "ai_reviews", ["instrument_public_id"])

    op.create_table(
        "ai_review_events",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("public_id", sa.String(36), nullable=False),
        sa.Column("review_public_id", sa.String(36), nullable=False),
        sa.Column("event_type", sa.String(32), nullable=False),
        sa.Column("actor_delegate_public_id", sa.String(36), nullable=True),
        sa.Column("previous_status", sa.String(24), nullable=True),
        sa.Column("new_status", sa.String(24), nullable=False),
        sa.Column("payload", sa.JSON(), nullable=False, server_default="{}"),
        sa.Column("occurred_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("public_id", name="uq_ai_review_events_public_id"),
        sa.CheckConstraint(
            "event_type IN ('created', 'fanout_dispatched', 'decision_recorded', "
            "'timeout_marked', 'superseded', 'counter_decremented', 'counter_adjusted')",
            name="ck_ai_review_events_type_enum",
        ),
    )
    op.create_index(
        "ix_ai_review_events_review_chrono",
        "ai_review_events",
        ["review_public_id", "occurred_at"],
    )
    op.create_index(
        "ix_ai_review_events_type_chrono",
        "ai_review_events",
        ["event_type", "occurred_at"],
    )

    with op.get_context().autocommit_block():
        op.execute(
            text(
                "CREATE UNIQUE INDEX IF NOT EXISTS uq_ep_active_cancel_idempotency_key "
                "ON execution_plans (operator_public_id, cancel_idempotency_key) "
                f"WHERE cancel_idempotency_key IS NOT NULL AND {active_filter}"
            )
        )


def downgrade() -> None:
    """Drop all tables in reverse order of creation."""
    with op.get_context().autocommit_block():
        op.execute(text("DROP INDEX IF EXISTS uq_ep_active_cancel_idempotency_key"))
        op.execute(text("DROP INDEX IF EXISTS uq_pc_shard_open_active"))
        op.execute(text("DROP INDEX IF EXISTS ix_pc_public_id"))
        op.execute(text("DROP INDEX IF EXISTS ix_epd_public_id"))
        op.execute(text("DROP INDEX IF EXISTS ix_epc_public_id"))
        op.execute(text("DROP INDEX IF EXISTS uq_ep_active_trailing_stop_per_cycle"))
        op.execute(text("DROP INDEX IF EXISTS uq_ep_active_bracket_per_cycle"))
        op.execute(text("DROP INDEX IF EXISTS ix_ep_public_id"))
        op.execute(text("DROP INDEX IF EXISTS uq_ep_idempotency_key"))
        op.execute(text("DROP INDEX IF EXISTS ix_alert_events_public_id_active"))
        op.execute(text("DROP INDEX IF EXISTS ix_alert_events_user_type_time"))
        op.execute(text("DROP INDEX IF EXISTS ix_alert_events_dedup"))
        op.execute(text("DROP INDEX IF EXISTS ix_alert_deliveries_next_attempt"))
        op.execute(text("DROP INDEX IF EXISTS uq_device_alert_wallet_scope"))
        op.execute(text("DROP INDEX IF EXISTS uq_device_alert_operator_scope"))
        op.execute(text("DROP INDEX IF EXISTS uq_device_alert_device_scope"))
        op.execute(text("DROP INDEX IF EXISTS ix_telemetry_timestamp"))
    op.drop_table("ai_review_events")
    op.drop_table("ai_reviews")
    op.drop_table("ai_delegates")
    op.drop_table("alert_deliveries")
    op.drop_table("alert_events")
    op.drop_table("user_alert_defaults")
    op.drop_table("device_alert_prefs")
    op.drop_table("notification_devices")
    op.drop_table("position_cycles")
    op.drop_table("execution_plan_decisions")
    op.drop_table("execution_plan_checkpoints")
    op.drop_table("execution_plans")
    op.drop_table("venue_fee_schedules")
    op.drop_table("instrument_order_capabilities")
    op.drop_table("wallet_operator_scope_grants")
    op.drop_table("user_operator_memberships")
    op.drop_table("operators")
    op.drop_table("wallet_credentials")
    op.drop_table("wallets")
    op.drop_table("accrual_ledger")
    op.drop_table("funding_rates")
    op.drop_table("continuous_contract_configs")
    op.drop_table("instrument_underlying_mappings")
    op.drop_table("underlying_assets")
    op.drop_table("trade_projection_checkpoints")
    op.drop_table("venue_events")
    op.drop_table("trade_commands")
    op.drop_table("telemetry")
    op.drop_table("control")
    op.drop_table("market_snapshots")
    op.drop_table("instrument_specs")
    op.drop_table("backtest_comparisons")
    op.drop_table("backtest_equity_points")
    op.drop_table("backtest_trades")
    op.drop_table("backtest_signals")
    op.drop_table("backtest_results")
    op.drop_table("backtest_events")
    op.drop_table("backtest_runs")
    op.drop_table("process_runs")
    op.drop_table("settings")
    op.drop_table("user_login_events")
    op.drop_table("user_active_tokens")
    op.drop_table("user_trading_caps")
    op.drop_table("users")
    op.drop_table("signals")
    op.drop_table("positions")
    op.drop_table("executions")
    op.drop_table("orders")
    op.drop_table("ticks")
    op.drop_table("trades")
    op.drop_table("candles")
    op.drop_table("instruments")
    op.drop_table("symbol_exchange_capabilities")
    op.drop_table("symbol_aliases")
    op.drop_table("symbols")
