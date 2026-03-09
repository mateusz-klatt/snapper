"""Initial database schema migration.

Creates all core tables for the Snapper trading system including
instruments, candles, trades, orders, users, and market snapshots.
Seeds symbol catalog with aliases and exchange capabilities.
"""

from collections.abc import Sequence
from datetime import UTC
from datetime import datetime

import sqlalchemy as sa
from alembic import op
from sqlalchemy import text

_INSTRUMENT_FK = "instruments.id"
_FK_SYMBOL_CATALOG = "symbol_catalog.native_symbol"
_CK_EXCHANGE_LOWER = "exchange = LOWER(exchange)"

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

    Creates all tables for instruments, candles, trades, orders, executions,
    positions, signal events, users, settings, symbol catalog, symbol aliases,
    process runs, instrument specs, and market snapshots.
    Seeds symbol catalog entries, aliases, and exchange capabilities.
    """
    op.create_table(
        "symbol_catalog",
        sa.Column("native_symbol", sa.String(32), nullable=False),
        sa.Column("base", sa.String(16), nullable=False),
        sa.Column("quote", sa.String(16), nullable=True),
        sa.Column("asset_type", sa.String(16), server_default="crypto", nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("native_symbol"),
        sa.CheckConstraint(
            "asset_type IN ('crypto', 'forex', 'equity', 'index')",
            name="ck_symbol_catalog_asset_type",
        ),
        sa.CheckConstraint(
            "asset_type IN ('equity', 'index') OR quote IS NOT NULL",
            name="ck_symbol_catalog_quote_required_for_pairs",
        ),
    )
    op.create_index("ix_sc_base_quote", "symbol_catalog", ["base", "quote"])
    op.create_table(
        "symbol_aliases",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("native_symbol", sa.String(32), nullable=False),
        sa.Column("exchange", sa.String(20), nullable=False),
        sa.Column("channel", sa.String(10), nullable=False),
        sa.Column("exchange_symbol", sa.String(40), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["native_symbol"], [_FK_SYMBOL_CATALOG]),
        sa.PrimaryKeyConstraint("id"),
        sa.CheckConstraint(
            _CK_EXCHANGE_LOWER,
            name="ck_symbol_alias_exchange_lower",
        ),
        sa.CheckConstraint(
            "channel IN ('ws', 'rest', 'ccxt')",
            name="ck_symbol_alias_channel",
        ),
        sa.UniqueConstraint(
            "native_symbol",
            "exchange",
            "channel",
            name="uq_alias_native_exchange_channel",
        ),
        sa.UniqueConstraint(
            "exchange",
            "channel",
            "exchange_symbol",
            name="uq_alias_exchange_channel_symbol",
        ),
    )
    op.create_index("ix_symbol_aliases_native_symbol", "symbol_aliases", ["native_symbol"])
    op.create_table(
        "symbol_exchange_capabilities",
        sa.Column("native_symbol", sa.String(32), nullable=False),
        sa.Column("exchange", sa.String(20), nullable=False),
        sa.Column("can_market_data", sa.Boolean(), nullable=False, server_default="0"),
        sa.Column("can_trade", sa.Boolean(), nullable=False, server_default="0"),
        sa.Column("source", sa.String(50), nullable=True),
        sa.Column("reason", sa.String(1024), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["native_symbol"], [_FK_SYMBOL_CATALOG]),
        sa.PrimaryKeyConstraint("native_symbol", "exchange"),
        sa.CheckConstraint(
            _CK_EXCHANGE_LOWER,
            name="ck_sec_exchange_lower",
        ),
    )
    op.create_index("ix_sec_exchange", "symbol_exchange_capabilities", ["exchange"])
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
        sa.Column("symbol", sa.String(32), nullable=False),
        sa.Column("exchange", sa.String(20), nullable=False),
        sa.Column("base", sa.String(16), nullable=False),
        sa.Column("quote", sa.String(16), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        sa.ForeignKeyConstraint(["symbol"], [_FK_SYMBOL_CATALOG]),
        sa.UniqueConstraint("symbol", "exchange", name="uq_instrument_symbol_exchange"),
        sa.CheckConstraint(_CK_EXCHANGE_LOWER, name="ck_instrument_exchange_lower"),
    )
    op.create_index("ix_instruments_symbol", "instruments", ["symbol"])
    op.create_index("ix_instruments_exchange", "instruments", ["exchange"])
    op.create_table(
        "candles",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("instrument_id", sa.Integer(), nullable=False),
        sa.Column("open_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("timeframe", sa.String(8), nullable=False),
        sa.Column("open", sa.Float(), nullable=False),
        sa.Column("high", sa.Float(), nullable=False),
        sa.Column("low", sa.Float(), nullable=False),
        sa.Column("close", sa.Float(), nullable=False),
        sa.Column("volume", sa.Float(), nullable=False),
        sa.Column("vwap", sa.Float(), nullable=True),
        sa.Column("trades", sa.Integer(), nullable=True),
        sa.ForeignKeyConstraint(["instrument_id"], [_INSTRUMENT_FK]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("instrument_id", "timeframe", "open_at", name="uq_candle_itf_open"),
    )
    op.create_index("ix_candles_instrument_id", "candles", ["instrument_id"])
    op.create_index("ix_candle_instrument_open", "candles", ["instrument_id", "open_at"])
    op.create_table(
        "trades",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("instrument_id", sa.Integer(), nullable=False),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("price", sa.Float(), nullable=False),
        sa.Column("size", sa.Float(), nullable=False),
        sa.Column("side", sa.String(4), nullable=False),
        sa.Column("trade_id", sa.String(64), nullable=False),
        sa.ForeignKeyConstraint(["instrument_id"], [_INSTRUMENT_FK]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("trade_id", name="uq_trade_trade_id"),
    )
    op.create_index("ix_trades_instrument_id", "trades", ["instrument_id"])
    op.create_index("ix_trades_timestamp", "trades", ["timestamp"])
    op.create_index("ix_trade_instrument_ts", "trades", ["instrument_id", "timestamp"])
    op.create_table(
        "orders",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("instrument_id", sa.Integer(), nullable=False),
        sa.Column("client_order_id", sa.String(64), nullable=True),
        sa.Column("exchange_order_id", sa.String(64), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("side", sa.String(4), nullable=False),
        sa.Column("type", sa.String(16), nullable=False),
        sa.Column("price", sa.Float(), nullable=True),
        sa.Column("size", sa.Float(), nullable=False),
        sa.Column("filled_size", sa.Float(), nullable=False, server_default="0"),
        sa.Column("average_price", sa.Float(), nullable=True),
        sa.Column("status", sa.String(16), nullable=False),
        sa.Column("time_in_force", sa.String(16), nullable=True),
        sa.Column("error", sa.String(512), nullable=True),
        sa.ForeignKeyConstraint(["instrument_id"], [_INSTRUMENT_FK]),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_orders_instrument_id", "orders", ["instrument_id"])
    op.create_index("ix_orders_client_order_id", "orders", ["client_order_id"])
    op.create_index("ix_orders_exchange_order_id", "orders", ["exchange_order_id"])
    op.create_index(
        "uq_orders_client_oid",
        "orders",
        ["instrument_id", "client_order_id"],
        unique=True,
        sqlite_where=text("client_order_id IS NOT NULL"),
    )
    op.create_index(
        "uq_orders_exchange_oid",
        "orders",
        ["instrument_id", "exchange_order_id"],
        unique=True,
        sqlite_where=text("exchange_order_id IS NOT NULL"),
    )
    op.create_table(
        "executions",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("order_id", sa.Integer(), nullable=False),
        sa.Column("exec_id", sa.String(64), nullable=True),
        sa.Column("trade_id", sa.String(64), nullable=True),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("price", sa.Float(), nullable=False),
        sa.Column("size", sa.Float(), nullable=False),
        sa.Column("fee", sa.Float(), nullable=False),
        sa.Column("fee_asset", sa.String(16), nullable=False),
        sa.Column("executed_at", sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(["order_id"], ["orders.id"]),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_executions_order_id", "executions", ["order_id"])
    op.create_index(
        "uq_executions_order_exec",
        "executions",
        ["order_id", "exec_id"],
        unique=True,
        sqlite_where=text("exec_id IS NOT NULL"),
    )
    op.create_index(
        "uq_executions_order_trade",
        "executions",
        ["order_id", "trade_id"],
        unique=True,
        sqlite_where=text("trade_id IS NOT NULL"),
    )
    op.create_table(
        "positions",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("instrument_id", sa.Integer(), nullable=False),
        sa.Column("quantity", sa.Float(), nullable=False),
        sa.Column("average_price", sa.Float(), nullable=False),
        sa.Column("unrealized_pnl", sa.Float(), nullable=False),
        sa.Column("realized_pnl", sa.Float(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["instrument_id"], [_INSTRUMENT_FK]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("instrument_id", name="uq_positions_instrument_id"),
    )
    op.create_index("ix_positions_instrument_id", "positions", ["instrument_id"])
    op.create_table(
        "signal_events",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("instrument_id", sa.Integer(), nullable=False),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("side", sa.String(4), nullable=False),
        sa.Column("strength", sa.Float(), nullable=False),
        sa.Column("reason", sa.String(256), nullable=False),
        sa.Column("strategy_name", sa.String(64), nullable=True),
        sa.Column("price", sa.Float(), nullable=True),
        sa.ForeignKeyConstraint(["instrument_id"], [_INSTRUMENT_FK]),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_signal_events_instrument_id", "signal_events", ["instrument_id"])
    op.create_index("ix_signal_events_timestamp", "signal_events", ["timestamp"])
    op.create_table(
        "users",
        sa.Column("id", sa.String(255), nullable=False),
        sa.Column("username", sa.String(64), nullable=False),
        sa.Column("email", sa.String(255), nullable=True),
        sa.Column("password_hash", sa.String(255), nullable=False),
        sa.Column("role", sa.String(32), nullable=False),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default="1"),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_login", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_users_username", "users", ["username"], unique=True)
    op.create_table(
        "settings",
        sa.Column("key", sa.String(64), nullable=False),
        sa.Column("value", sa.String(1024), nullable=False),
        sa.Column("category", sa.String(32), nullable=False),
        sa.Column("description", sa.String(256), nullable=True),
        sa.Column("is_encrypted", sa.Boolean(), nullable=False, server_default="0"),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_by", sa.String(64), nullable=True),
        sa.PrimaryKeyConstraint("key"),
    )
    op.create_table(
        "process_runs",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("run_id", sa.String(36), nullable=False),
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
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_process_runs_run_id", "process_runs", ["run_id"], unique=True)
    op.create_index("ix_process_runs_process_name", "process_runs", ["process_name"])
    op.create_index("ix_process_runs_status", "process_runs", ["status"])
    op.create_index("ix_process_runs_started_at", "process_runs", ["started_at"])
    op.create_table(
        "instrument_specs",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
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
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["instrument_id"], [_INSTRUMENT_FK]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("instrument_id", name="uq_instrument_spec_instrument"),
    )
    op.create_index("ix_instrument_specs_instrument_id", "instrument_specs", ["instrument_id"])
    op.create_table(
        "market_snapshots",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
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
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_market_snapshots_symbol", "market_snapshots", ["symbol"])
    op.create_index("ix_market_snapshots_updated_at", "market_snapshots", ["updated_at"])
    op.create_index(
        "ix_market_snapshots_symbol_updated", "market_snapshots", ["symbol", "updated_at"]
    )
    op.create_index(
        "ix_market_snapshots_exchange_symbol_updated",
        "market_snapshots",
        ["exchange", "symbol", "updated_at"],
    )
    conn = op.get_bind()
    now = datetime.now(tz=UTC)
    for entry in SYMBOL_CATALOG:
        conn.execute(
            text("""
                INSERT INTO symbol_catalog (native_symbol, base, quote, asset_type, created_at, updated_at)
                VALUES (:native_symbol, :base, :quote, :asset_type, :created_at, :updated_at)
                """),
            {
                "native_symbol": entry[0],
                "base": entry[1],
                "quote": entry[2],
                "asset_type": entry[3],
                "created_at": now,
                "updated_at": now,
            },
        )
    for alias in SYMBOL_ALIASES:
        conn.execute(
            text("""
                INSERT INTO symbol_aliases (native_symbol, exchange, channel, exchange_symbol, created_at, updated_at)
                VALUES (:native_symbol, :exchange, :channel, :exchange_symbol, :created_at, :updated_at)
                """),
            {
                "native_symbol": alias[0],
                "exchange": alias[1],
                "channel": alias[2],
                "exchange_symbol": alias[3],
                "created_at": now,
                "updated_at": now,
            },
        )
    for cap in SYMBOL_CAPABILITIES:
        conn.execute(
            text("""
                INSERT INTO symbol_exchange_capabilities
                (native_symbol, exchange, can_market_data, can_trade, source, created_at, updated_at)
                VALUES (:native_symbol, :exchange, :can_market_data, :can_trade, :source, :created_at, :updated_at)
                """),
            {
                "native_symbol": cap[0],
                "exchange": cap[1],
                "can_market_data": cap[2],
                "can_trade": cap[3],
                "source": "seed",
                "created_at": now,
                "updated_at": now,
            },
        )


def downgrade() -> None:
    """Drop all tables in reverse order of creation.

    Removes all tables created by the upgrade function, respecting
    foreign key constraints by dropping in reverse dependency order.
    """
    op.drop_table("market_snapshots")
    op.drop_table("instrument_specs")
    op.drop_table("process_runs")
    op.drop_table("settings")
    op.drop_table("users")
    op.drop_table("signal_events")
    op.drop_table("positions")
    op.drop_table("executions")
    op.drop_table("orders")
    op.drop_table("trades")
    op.drop_table("candles")
    op.drop_table("instruments")
    op.drop_table("symbol_exchange_capabilities")
    op.drop_table("symbol_aliases")
    op.drop_table("symbol_catalog")
