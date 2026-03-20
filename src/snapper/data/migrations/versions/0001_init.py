"""Initial database schema migration.

Creates all core tables for the Snapper trading system including
instruments, candles, trades, orders, users, and market snapshots.
Seeds symbols, aliases, and exchange capabilities.
"""

from collections.abc import Sequence
from datetime import UTC
from datetime import datetime
from uuid import uuid7

import sqlalchemy as sa
from alembic import op
from sqlalchemy import text

_INSTRUMENT_FK = "instruments.id"
_CK_EXCHANGE_LOWER = "exchange = LOWER(exchange)"
_KNOWN_TO_MAX = datetime(9999, 12, 31, 23, 59, 59, tzinfo=UTC)
_KNOWN_TO_ACTIVE = "known_to = '9999-12-31T23:59:59+00:00'"
_CK_SESSION_ID = "session_id != ''"
_CK_SEQUENCE_ID = "sequence_id > 0"

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
    ("BTC-USD", "zonda", "ws", "BTC-USD"),
    ("BTC-USD", "polygon", "rest", "X:BTCUSD"),
    ("BTC-EUR", "kraken", "ws", "BTC/EUR"),
    ("BTC-EUR", "kraken", "rest", "XXBTZEUR"),
    ("BTC-EUR", "kraken", "ccxt", "BTC/EUR"),
    ("BTC-EUR", "zonda", "ws", "BTC-EUR"),
    ("BTC-EUR", "polygon", "rest", "X:BTCEUR"),
    ("ETH-USD", "kraken", "ws", "ETH/USD"),
    ("ETH-USD", "kraken", "rest", "XETHZUSD"),
    ("ETH-USD", "kraken", "ccxt", "ETH/USD"),
    ("ETH-USD", "zonda", "ws", "ETH-USD"),
    ("ETH-USD", "polygon", "rest", "X:ETHUSD"),
    ("ETH-EUR", "kraken", "ws", "ETH/EUR"),
    ("ETH-EUR", "kraken", "rest", "XETHZEUR"),
    ("ETH-EUR", "kraken", "ccxt", "ETH/EUR"),
    ("ETH-EUR", "zonda", "ws", "ETH-EUR"),
    ("ETH-BTC", "kraken", "ws", "ETH/BTC"),
    ("ETH-BTC", "kraken", "rest", "XETHXXBT"),
    ("ETH-BTC", "kraken", "ccxt", "ETH/BTC"),
    ("ETH-BTC", "zonda", "ws", "ETH-BTC"),
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
    "zonda": (True, True),
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
    """Create initial database schema and seed reference data.

    Creates all tables for instruments, candles, ticks, trades, orders,
    executions, positions, signals, users, settings, symbols, symbol aliases,
    process runs, instrument specs, market snapshots, control, and telemetry.
    Seeds symbols, aliases, and exchange capabilities.
    """
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
            "asset_type IN ('crypto', 'forex', 'equity', 'index')",
            name="ck_symbol_asset_type",
        ),
        sa.CheckConstraint(
            "asset_type IN ('equity', 'index') OR quote IS NOT NULL",
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
        sqlite_where=text(_KNOWN_TO_ACTIVE),
        postgresql_where=text(_KNOWN_TO_ACTIVE),
    )
    op.create_index(
        "ix_symbols_public_id",
        "symbols",
        ["public_id"],
        unique=True,
        sqlite_where=text(_KNOWN_TO_ACTIVE),
        postgresql_where=text(_KNOWN_TO_ACTIVE),
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
        sqlite_where=text(_KNOWN_TO_ACTIVE),
        postgresql_where=text(_KNOWN_TO_ACTIVE),
    )
    op.create_index(
        "uq_alias_spid_exchange_channel",
        "symbol_aliases",
        ["symbol_public_id", "exchange", "channel"],
        unique=True,
        sqlite_where=text(_KNOWN_TO_ACTIVE),
        postgresql_where=text(_KNOWN_TO_ACTIVE),
    )
    op.create_index(
        "uq_alias_exchange_channel_symbol",
        "symbol_aliases",
        ["exchange", "channel", "exchange_symbol"],
        unique=True,
        sqlite_where=text(_KNOWN_TO_ACTIVE),
        postgresql_where=text(_KNOWN_TO_ACTIVE),
    )
    op.create_index("ix_symbol_aliases_symbol_public_id", "symbol_aliases", ["symbol_public_id"])
    op.create_table(
        "symbol_exchange_capabilities",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("public_id", sa.String(36), nullable=False),
        sa.Column("symbol_public_id", sa.String(36), nullable=False),
        sa.Column("exchange", sa.String(20), nullable=False),
        sa.Column("can_market_data", sa.Boolean(), nullable=False, server_default="0"),
        sa.Column("can_trade", sa.Boolean(), nullable=False, server_default="0"),
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
        sqlite_where=text(_KNOWN_TO_ACTIVE),
        postgresql_where=text(_KNOWN_TO_ACTIVE),
    )
    op.create_index(
        "uq_sec_symbol_exchange",
        "symbol_exchange_capabilities",
        ["symbol_public_id", "exchange"],
        unique=True,
        sqlite_where=text(_KNOWN_TO_ACTIVE),
        postgresql_where=text(_KNOWN_TO_ACTIVE),
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
        sa.Column("symbol", sa.String(32), nullable=False),
        sa.Column("exchange", sa.String(20), nullable=False),
        sa.Column("base", sa.String(16), nullable=False),
        sa.Column("quote", sa.String(16), nullable=False),
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
        sqlite_where=text(_KNOWN_TO_ACTIVE),
        postgresql_where=text(_KNOWN_TO_ACTIVE),
    )
    op.create_index(
        "uq_instrument_spid_exchange",
        "instruments",
        ["symbol_public_id", "exchange"],
        unique=True,
        sqlite_where=text(_KNOWN_TO_ACTIVE),
        postgresql_where=text(_KNOWN_TO_ACTIVE),
    )
    op.create_index("ix_instruments_symbol_public_id", "instruments", ["symbol_public_id"])
    op.create_index("ix_instruments_symbol", "instruments", ["symbol"])
    op.create_index("ix_instruments_exchange", "instruments", ["exchange"])
    op.create_table(
        "candles",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("public_id", sa.String(36), nullable=False),
        sa.Column("instrument_id", sa.Integer(), nullable=False),
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
        sa.ForeignKeyConstraint(["instrument_id"], [_INSTRUMENT_FK]),
        sa.PrimaryKeyConstraint("id"),
        sa.CheckConstraint(_CK_SESSION_ID, name="ck_candles_session_id"),
        sa.CheckConstraint(_CK_SEQUENCE_ID, name="ck_candles_sequence_id"),
    )
    op.create_index(
        "ix_candles_public_id",
        "candles",
        ["public_id"],
        unique=True,
        sqlite_where=text(_KNOWN_TO_ACTIVE),
        postgresql_where=text(_KNOWN_TO_ACTIVE),
    )
    op.create_index(
        "uq_candle_itf_open",
        "candles",
        ["instrument_id", "timeframe", "open_at"],
        unique=True,
        sqlite_where=text(_KNOWN_TO_ACTIVE),
        postgresql_where=text(_KNOWN_TO_ACTIVE),
    )
    op.create_index("ix_candles_instrument_id", "candles", ["instrument_id"])
    op.create_index("ix_candle_instrument_open", "candles", ["instrument_id", "open_at"])
    op.create_table(
        "trades",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("public_id", sa.String(36), nullable=False),
        sa.Column("instrument_id", sa.Integer(), nullable=False),
        sa.Column("trade_id", sa.String(64), nullable=False),
        sa.Column("price", sa.Float(), nullable=False),
        sa.Column("size", sa.Float(), nullable=False),
        sa.Column("side", sa.String(4), nullable=False),
        sa.Column("session_id", sa.String(36), nullable=False),
        sa.Column("sequence_id", sa.Integer(), nullable=False),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("known_to", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["instrument_id"], [_INSTRUMENT_FK]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("trade_id", name="uq_trade_trade_id"),
        sa.CheckConstraint(_CK_SESSION_ID, name="ck_trades_session_id"),
        sa.CheckConstraint(_CK_SEQUENCE_ID, name="ck_trades_sequence_id"),
    )
    op.create_index(
        "ix_trades_public_id",
        "trades",
        ["public_id"],
        unique=True,
        sqlite_where=text(_KNOWN_TO_ACTIVE),
        postgresql_where=text(_KNOWN_TO_ACTIVE),
    )
    op.create_index("ix_trades_instrument_id", "trades", ["instrument_id"])
    op.create_index("ix_trades_timestamp", "trades", ["timestamp"])
    op.create_index("ix_trade_instrument_ts", "trades", ["instrument_id", "timestamp"])
    op.create_table(
        "ticks",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("public_id", sa.String(36), nullable=False),
        sa.Column("instrument_id", sa.Integer(), nullable=False),
        sa.Column("bid", sa.Float(), nullable=True),
        sa.Column("ask", sa.Float(), nullable=True),
        sa.Column("last", sa.Float(), nullable=True),
        sa.Column("volume", sa.Float(), nullable=False),
        sa.Column("session_id", sa.String(36), nullable=False),
        sa.Column("sequence_id", sa.Integer(), nullable=False),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("known_to", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["instrument_id"], [_INSTRUMENT_FK]),
        sa.PrimaryKeyConstraint("id"),
        sa.CheckConstraint(_CK_SESSION_ID, name="ck_ticks_session_id"),
        sa.CheckConstraint(_CK_SEQUENCE_ID, name="ck_ticks_sequence_id"),
    )
    op.create_index(
        "ix_ticks_public_id",
        "ticks",
        ["public_id"],
        unique=True,
        sqlite_where=text(_KNOWN_TO_ACTIVE),
        postgresql_where=text(_KNOWN_TO_ACTIVE),
    )
    op.create_index("ix_ticks_instrument_id", "ticks", ["instrument_id"])
    op.create_index("ix_tick_instrument_ts", "ticks", ["instrument_id", "timestamp"])
    op.create_table(
        "orders",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("public_id", sa.String(36), nullable=False),
        sa.Column("instrument_id", sa.Integer(), nullable=False),
        sa.Column("client_order_id", sa.String(64), nullable=True),
        sa.Column("exchange_order_id", sa.String(64), nullable=True),
        sa.Column("side", sa.String(4), nullable=False),
        sa.Column("order_type", sa.String(16), nullable=False),
        sa.Column("price", sa.Float(), nullable=True),
        sa.Column("size", sa.Float(), nullable=False),
        sa.Column("filled_size", sa.Float(), nullable=False, server_default="0"),
        sa.Column("average_price", sa.Float(), nullable=True),
        sa.Column("status", sa.String(16), nullable=False),
        sa.Column("time_in_force", sa.String(16), nullable=True),
        sa.Column("error", sa.String(512), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("session_id", sa.String(36), nullable=False),
        sa.Column("sequence_id", sa.Integer(), nullable=False),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("known_to", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["instrument_id"], [_INSTRUMENT_FK]),
        sa.PrimaryKeyConstraint("id"),
        sa.CheckConstraint(_CK_SESSION_ID, name="ck_orders_session_id"),
        sa.CheckConstraint(_CK_SEQUENCE_ID, name="ck_orders_sequence_id"),
    )
    op.create_index(
        "ix_orders_public_id",
        "orders",
        ["public_id"],
        unique=True,
        sqlite_where=text(_KNOWN_TO_ACTIVE),
        postgresql_where=text(_KNOWN_TO_ACTIVE),
    )
    op.create_index("ix_orders_instrument_id", "orders", ["instrument_id"])
    op.create_index("ix_orders_client_order_id", "orders", ["client_order_id"])
    op.create_index("ix_orders_exchange_order_id", "orders", ["exchange_order_id"])
    op.create_index(
        "uq_orders_client_oid",
        "orders",
        ["instrument_id", "client_order_id"],
        unique=True,
        sqlite_where=text("client_order_id IS NOT NULL AND " + _KNOWN_TO_ACTIVE),
        postgresql_where=text("client_order_id IS NOT NULL AND " + _KNOWN_TO_ACTIVE),
    )
    op.create_index(
        "uq_orders_exchange_oid",
        "orders",
        ["instrument_id", "exchange_order_id"],
        unique=True,
        sqlite_where=text("exchange_order_id IS NOT NULL AND " + _KNOWN_TO_ACTIVE),
        postgresql_where=text("exchange_order_id IS NOT NULL AND " + _KNOWN_TO_ACTIVE),
    )
    op.create_table(
        "executions",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("public_id", sa.String(36), nullable=False),
        sa.Column("order_id", sa.Integer(), nullable=False),
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
        sa.Column("session_id", sa.String(36), nullable=False),
        sa.Column("sequence_id", sa.Integer(), nullable=False),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("known_to", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["order_id"], ["orders.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.CheckConstraint(_CK_SESSION_ID, name="ck_executions_session_id"),
        sa.CheckConstraint(_CK_SEQUENCE_ID, name="ck_executions_sequence_id"),
    )
    op.create_index(
        "ix_executions_public_id",
        "executions",
        ["public_id"],
        unique=True,
        sqlite_where=text(_KNOWN_TO_ACTIVE),
        postgresql_where=text(_KNOWN_TO_ACTIVE),
    )
    op.create_index("ix_executions_order_id", "executions", ["order_id"])
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
        sa.Column("instrument_id", sa.Integer(), nullable=False),
        sa.Column("quantity", sa.Float(), nullable=False),
        sa.Column("average_price", sa.Float(), nullable=False),
        sa.Column("unrealized_pnl", sa.Float(), nullable=False),
        sa.Column("realized_pnl", sa.Float(), nullable=False),
        sa.Column("session_id", sa.String(36), nullable=False),
        sa.Column("sequence_id", sa.Integer(), nullable=False),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("known_to", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["instrument_id"], [_INSTRUMENT_FK]),
        sa.PrimaryKeyConstraint("id"),
        sa.CheckConstraint(_CK_SESSION_ID, name="ck_positions_session_id"),
        sa.CheckConstraint(_CK_SEQUENCE_ID, name="ck_positions_sequence_id"),
    )
    op.create_index(
        "ix_positions_public_id",
        "positions",
        ["public_id"],
        unique=True,
        sqlite_where=text(_KNOWN_TO_ACTIVE),
        postgresql_where=text(_KNOWN_TO_ACTIVE),
    )
    op.create_index(
        "uq_positions_instrument_id",
        "positions",
        ["instrument_id"],
        unique=True,
        sqlite_where=text(_KNOWN_TO_ACTIVE),
        postgresql_where=text(_KNOWN_TO_ACTIVE),
    )
    op.create_index("ix_positions_instrument_id", "positions", ["instrument_id"])
    op.create_table(
        "signals",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("public_id", sa.String(36), nullable=False),
        sa.Column("instrument_id", sa.Integer(), nullable=False),
        sa.Column("side", sa.String(4), nullable=False),
        sa.Column("strength", sa.Float(), nullable=False),
        sa.Column("reason", sa.String(256), nullable=False),
        sa.Column("strategy_name", sa.String(64), nullable=True),
        sa.Column("price", sa.Float(), nullable=True),
        sa.Column("fired_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("session_id", sa.String(36), nullable=False),
        sa.Column("sequence_id", sa.Integer(), nullable=False),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("known_to", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["instrument_id"], [_INSTRUMENT_FK]),
        sa.PrimaryKeyConstraint("id"),
        sa.CheckConstraint(_CK_SESSION_ID, name="ck_signals_session_id"),
        sa.CheckConstraint(_CK_SEQUENCE_ID, name="ck_signals_sequence_id"),
    )
    op.create_index(
        "ix_signals_public_id",
        "signals",
        ["public_id"],
        unique=True,
        sqlite_where=text(_KNOWN_TO_ACTIVE),
        postgresql_where=text(_KNOWN_TO_ACTIVE),
    )
    op.create_index("ix_signals_instrument_id", "signals", ["instrument_id"])
    op.create_index("ix_signals_fired_at", "signals", ["fired_at"])
    op.create_table(
        "users",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("public_id", sa.String(36), nullable=False),
        sa.Column("username", sa.String(64), nullable=False),
        sa.Column("email", sa.String(255), nullable=True),
        sa.Column("password_hash", sa.String(255), nullable=False),
        sa.Column("role", sa.String(32), nullable=False),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default="1"),
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
        sqlite_where=text(_KNOWN_TO_ACTIVE),
        postgresql_where=text(_KNOWN_TO_ACTIVE),
    )
    op.create_index(
        "ix_users_username",
        "users",
        ["username"],
        unique=True,
        sqlite_where=text(_KNOWN_TO_ACTIVE),
        postgresql_where=text(_KNOWN_TO_ACTIVE),
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
        sqlite_where=text(_KNOWN_TO_ACTIVE),
        postgresql_where=text(_KNOWN_TO_ACTIVE),
    )
    op.create_table(
        "settings",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("public_id", sa.String(36), nullable=False),
        sa.Column("key", sa.String(64), nullable=False),
        sa.Column("value", sa.String(1024), nullable=False),
        sa.Column("category", sa.String(32), nullable=False),
        sa.Column("description", sa.String(256), nullable=True),
        sa.Column("is_encrypted", sa.Boolean(), nullable=False, server_default="0"),
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
        sqlite_where=text(_KNOWN_TO_ACTIVE),
        postgresql_where=text(_KNOWN_TO_ACTIVE),
    )
    op.create_index(
        "uq_settings_key",
        "settings",
        ["key"],
        unique=True,
        sqlite_where=text(_KNOWN_TO_ACTIVE),
        postgresql_where=text(_KNOWN_TO_ACTIVE),
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
        sqlite_where=text(_KNOWN_TO_ACTIVE),
        postgresql_where=text(_KNOWN_TO_ACTIVE),
    )
    op.create_index("ix_process_runs_process_name", "process_runs", ["process_name"])
    op.create_index("ix_process_runs_status", "process_runs", ["status"])
    op.create_index("ix_process_runs_started_at", "process_runs", ["started_at"])
    op.create_table(
        "instrument_specs",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("public_id", sa.String(36), nullable=False),
        sa.Column("instrument_id", sa.Integer(), nullable=False),
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
        sa.Column("session_id", sa.String(36), nullable=False),
        sa.Column("sequence_id", sa.Integer(), nullable=False),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("known_to", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["instrument_id"], [_INSTRUMENT_FK]),
        sa.PrimaryKeyConstraint("id"),
        sa.CheckConstraint(_CK_SESSION_ID, name="ck_instrument_specs_session_id"),
        sa.CheckConstraint(_CK_SEQUENCE_ID, name="ck_instrument_specs_sequence_id"),
    )
    op.create_index(
        "ix_instrument_specs_public_id",
        "instrument_specs",
        ["public_id"],
        unique=True,
        sqlite_where=text(_KNOWN_TO_ACTIVE),
        postgresql_where=text(_KNOWN_TO_ACTIVE),
    )
    op.create_index(
        "uq_instrument_spec_instrument",
        "instrument_specs",
        ["instrument_id"],
        unique=True,
        sqlite_where=text(_KNOWN_TO_ACTIVE),
        postgresql_where=text(_KNOWN_TO_ACTIVE),
    )
    op.create_index("ix_instrument_specs_instrument_id", "instrument_specs", ["instrument_id"])
    op.create_table(
        "market_snapshots",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("public_id", sa.String(36), nullable=False),
        sa.Column("exchange", sa.String(20), server_default="kraken", nullable=False),
        sa.Column("symbol", sa.String(20), nullable=False),
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
        sqlite_where=text(_KNOWN_TO_ACTIVE),
        postgresql_where=text(_KNOWN_TO_ACTIVE),
    )
    op.create_index("ix_market_snapshots_symbol", "market_snapshots", ["symbol"])
    op.create_index("ix_market_snapshots_timestamp", "market_snapshots", ["timestamp"])
    op.create_index("ix_market_snapshots_symbol_ts", "market_snapshots", ["symbol", "timestamp"])
    op.create_index(
        "ix_market_snapshots_exchange_symbol_ts",
        "market_snapshots",
        ["exchange", "symbol", "timestamp"],
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
        sqlite_where=text(_KNOWN_TO_ACTIVE),
        postgresql_where=text(_KNOWN_TO_ACTIVE),
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
        sqlite_where=text(_KNOWN_TO_ACTIVE),
        postgresql_where=text(_KNOWN_TO_ACTIVE),
    )
    conn = op.get_bind()
    now = datetime.now(tz=UTC)
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
                "created_at": now,
                "session_id": seed_session_id,
                "sequence_id": seed_seq,
                "timestamp": now,
                "known_to": _KNOWN_TO_MAX,
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
                "created_at": now,
                "session_id": seed_session_id,
                "sequence_id": seed_seq,
                "timestamp": now,
                "known_to": _KNOWN_TO_MAX,
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
                "created_at": now,
                "session_id": seed_session_id,
                "sequence_id": seed_seq,
                "timestamp": now,
                "known_to": _KNOWN_TO_MAX,
            },
        )


def downgrade() -> None:
    """Drop all tables in reverse order of creation.

    Removes all tables created by the upgrade function, respecting
    foreign key constraints by dropping in reverse dependency order.
    """
    op.drop_table("telemetry")
    op.drop_table("control")
    op.drop_table("market_snapshots")
    op.drop_table("instrument_specs")
    op.drop_table("process_runs")
    op.drop_table("settings")
    op.drop_table("user_login_events")
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
