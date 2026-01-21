"""Initial database schema migration.

Creates all core tables for the Snapper trading system including
instruments, candles, trades, orders, users, and market snapshots.
Also seeds demo users and symbol mappings.
"""

import hashlib
from collections.abc import Sequence
from datetime import UTC
from datetime import datetime

import sqlalchemy as sa
from alembic import op
from sqlalchemy import text

revision: str = "0001"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None
DEMO_USERS = [
    {
        "id": "admin",
        "username": "admin",
        "email": "admin@snapper.local",
        "password": "AdminSnapper2026!",
        "salt": "11111111111111111111111111111111",
        "role": "admin",
    },
    {
        "id": "operator",
        "username": "operator",
        "email": "operator@snapper.local",
        "password": "OpSnapper2026!",
        "salt": "22222222222222222222222222222222",
        "role": "operator",
    },
    {
        "id": "viewer",
        "username": "viewer",
        "email": "viewer@snapper.local",
        "password": "ViewSnapper2026!",
        "salt": "33333333333333333333333333333333",
        "role": "viewer",
    },
]
SYMBOL_MAPPINGS = [
    ("BTC-USD", "BTC/USD", "XXBTZUSD", "BTC/USD", "BTC-USD", "X:BTCUSD", None, None, "BTC", "USD"),
    (
        "EUR-USD",
        "EUR/USD",
        "ZEURZUSD",
        "EUR/USD",
        None,
        "C:EURUSD",
        "EUR_USD",
        "EURUSD",
        "EUR",
        "USD",
    ),
    ("BTC-EUR", "BTC/EUR", "XXBTZEUR", "BTC/EUR", "BTC-EUR", "X:BTCEUR", None, None, "BTC", "EUR"),
    ("ETH-USD", "ETH/USD", "XETHZUSD", "ETH/USD", "ETH-USD", "X:ETHUSD", None, None, "ETH", "USD"),
    ("ETH-EUR", "ETH/EUR", "XETHZEUR", "ETH/EUR", "ETH-EUR", None, None, None, "ETH", "EUR"),
    ("ETH-BTC", "ETH/BTC", "XETHXXBT", "ETH/BTC", "ETH-BTC", "X:ETHBTC", None, None, "ETH", "BTC"),
    ("USD-PLN", None, None, None, None, "C:USDPLN", "USD_PLN", "USDPLN", "USD", "PLN"),
    ("EUR-PLN", None, None, None, None, "C:EURPLN", "EUR_PLN", "EURPLN", "EUR", "PLN"),
    ("GBP-PLN", None, None, None, None, "C:GBPPLN", "GBP_PLN", "GBPPLN", "GBP", "PLN"),
    ("GBP-USD", None, None, None, None, "C:GBPUSD", None, None, "GBP", "USD"),
    ("EUR-GBP", None, None, None, None, "C:EURGBP", None, None, "EUR", "GBP"),
]


def _hash_password(password: str, salt: str) -> str:
    return hashlib.sha256((password + salt).encode()).hexdigest()


def upgrade() -> None:
    """Create initial database schema and seed data.

    Creates all tables for instruments, candles, trades, orders, executions,
    positions, strategy runs, signal events, users, settings, symbol mappings,
    process runs, instrument specs, market snapshots, and polygon import log.
    Seeds demo users and default symbol mappings.
    """
    op.create_table(
        "instruments",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("symbol", sa.String(32), nullable=False),
        sa.Column("base", sa.String(16), nullable=False),
        sa.Column("quote", sa.String(16), nullable=False),
        sa.Column("tick_size", sa.Float(), nullable=False),
        sa.Column("lot_size", sa.Float(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_instruments_symbol", "instruments", ["symbol"], unique=True)
    op.create_table(
        "candles",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("instrument_id", sa.Integer(), nullable=False),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("timeframe", sa.String(8), nullable=False),
        sa.Column("open", sa.Float(), nullable=False),
        sa.Column("high", sa.Float(), nullable=False),
        sa.Column("low", sa.Float(), nullable=False),
        sa.Column("close", sa.Float(), nullable=False),
        sa.Column("volume", sa.Float(), nullable=False),
        sa.Column("vwap", sa.Float(), nullable=True),
        sa.Column("trades", sa.Integer(), nullable=True),
        sa.ForeignKeyConstraint(["instrument_id"], ["instruments.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("instrument_id", "timeframe", "timestamp", name="uq_candle_itf_ts"),
    )
    op.create_index("ix_candles_instrument_id", "candles", ["instrument_id"])
    op.create_index("ix_candle_instrument_ts", "candles", ["instrument_id", "timestamp"])
    op.create_table(
        "trades",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("instrument_id", sa.Integer(), nullable=False),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("price", sa.Float(), nullable=False),
        sa.Column("size", sa.Float(), nullable=False),
        sa.Column("side", sa.String(4), nullable=False),
        sa.Column("trade_id", sa.String(64), nullable=False),
        sa.ForeignKeyConstraint(["instrument_id"], ["instruments.id"]),
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
        sa.Column("exchange", sa.String(32), server_default="", nullable=False),
        sa.Column("client_order_id", sa.String(64), nullable=True),
        sa.Column("exchange_order_id", sa.String(64), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("side", sa.String(4), nullable=False),
        sa.Column("type", sa.String(16), nullable=False),
        sa.Column("price", sa.Float(), nullable=True),
        sa.Column("size", sa.Float(), nullable=False),
        sa.Column("status", sa.String(16), nullable=False),
        sa.Column("time_in_force", sa.String(16), nullable=True),
        sa.Column("error", sa.String(256), nullable=True),
        sa.ForeignKeyConstraint(["instrument_id"], ["instruments.id"]),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_orders_instrument_id", "orders", ["instrument_id"])
    op.create_index("ix_orders_client_order_id", "orders", ["client_order_id"])
    op.create_index("ix_orders_exchange_order_id", "orders", ["exchange_order_id"])
    op.create_table(
        "executions",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("order_id", sa.Integer(), nullable=False),
        sa.Column("exchange", sa.String(32), server_default="", nullable=False),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("price", sa.Float(), nullable=False),
        sa.Column("size", sa.Float(), nullable=False),
        sa.Column("fee", sa.Float(), nullable=False),
        sa.Column("fee_asset", sa.String(16), nullable=False),
        sa.ForeignKeyConstraint(["order_id"], ["orders.id"]),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_executions_order_id", "executions", ["order_id"])
    op.create_table(
        "positions",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("instrument_id", sa.Integer(), nullable=False),
        sa.Column("exchange", sa.String(32), server_default="", nullable=False),
        sa.Column("quantity", sa.Float(), nullable=False),
        sa.Column("average_price", sa.Float(), nullable=False),
        sa.Column("unrealized_pnl", sa.Float(), nullable=False),
        sa.Column("realized_pnl", sa.Float(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["instrument_id"], ["instruments.id"]),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_positions_instrument_id", "positions", ["instrument_id"])
    op.create_table(
        "strategy_runs",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("name", sa.String(64), nullable=False),
        sa.Column("params", sa.JSON(), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("ended_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("metrics", sa.JSON(), nullable=True),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_strategy_runs_name", "strategy_runs", ["name"])
    op.create_table(
        "signal_events",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("instrument_id", sa.Integer(), nullable=False),
        sa.Column("exchange", sa.String(32), server_default="", nullable=False),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=False),
        sa.Column("side", sa.String(4), nullable=False),
        sa.Column("strength", sa.Float(), nullable=False),
        sa.Column("reason", sa.String(256), nullable=False),
        sa.Column("strategy_name", sa.String(64), nullable=True),
        sa.Column("price", sa.Float(), nullable=True),
        sa.ForeignKeyConstraint(["instrument_id"], ["instruments.id"]),
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
        sa.Column("salt", sa.String(32), nullable=False),
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
        "symbol_mappings",
        sa.Column("native_symbol", sa.String(20), nullable=False),
        sa.Column("kraken_websocket_symbol", sa.String(20), nullable=True),
        sa.Column("kraken_rest_symbol", sa.String(20), nullable=True),
        sa.Column("ccxt_symbol", sa.String(30), nullable=True),
        sa.Column("zonda_symbol", sa.String(20), nullable=True),
        sa.Column("polygon_symbol", sa.String(30), nullable=True),
        sa.Column("walutomat_symbol", sa.String(10), nullable=True),
        sa.Column("walutomat_rest_symbol", sa.String(10), nullable=True),
        sa.Column("base_currency", sa.String(10), nullable=False),
        sa.Column("quote_currency", sa.String(10), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("native_symbol"),
    )
    op.create_index(
        "ix_symbol_mappings_kraken_websocket", "symbol_mappings", ["kraken_websocket_symbol"]
    )
    op.create_index("ix_symbol_mappings_kraken_rest", "symbol_mappings", ["kraken_rest_symbol"])
    op.create_index(
        "ix_symbol_mappings_base_quote", "symbol_mappings", ["base_currency", "quote_currency"]
    )
    op.create_index("ix_symbol_mappings_zonda", "symbol_mappings", ["zonda_symbol"])
    op.create_index("ix_symbol_mappings_polygon", "symbol_mappings", ["polygon_symbol"])
    op.create_index("ix_symbol_mappings_walutomat", "symbol_mappings", ["walutomat_symbol"])
    op.create_index(
        "ix_symbol_mappings_walutomat_rest", "symbol_mappings", ["walutomat_rest_symbol"]
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
        sa.Column("symbol", sa.String(20), nullable=False),
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
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_instrument_specs_symbol", "instrument_specs", ["symbol"], unique=True)
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
    op.create_table(
        "polygon_import_log",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("s3_key", sa.String(255), nullable=False),
        sa.Column("file_date", sa.DateTime(timezone=True), nullable=False),
        sa.Column("data_type", sa.String(50), nullable=False),
        sa.Column("prefix", sa.String(50), nullable=False),
        sa.Column("records_imported", sa.Integer(), nullable=False),
        sa.Column("imported_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_polygon_import_log_s3_key", "polygon_import_log", ["s3_key"], unique=True)
    op.create_index("ix_polygon_import_log_imported_at", "polygon_import_log", ["imported_at"])
    conn = op.get_bind()
    demo_created_at = datetime(2026, 9, 19, 12, 0, 0, tzinfo=UTC)
    for user in DEMO_USERS:
        password_hash = _hash_password(user["password"], user["salt"])
        conn.execute(
            text("""
                INSERT INTO users (id, username, email, password_hash, salt, role, is_active, created_at)
                VALUES (:id, :username, :email, :password_hash, :salt, :role, 1, :created_at)
                """),
            {
                "id": user["id"],
                "username": user["username"],
                "email": user["email"],
                "password_hash": password_hash,
                "salt": user["salt"],
                "role": user["role"],
                "created_at": demo_created_at,
            },
        )
    now = datetime.now(tz=UTC)
    for mapping in SYMBOL_MAPPINGS:
        conn.execute(
            text("""
                INSERT INTO symbol_mappings (
                    native_symbol, kraken_websocket_symbol, kraken_rest_symbol, ccxt_symbol,
                    zonda_symbol, polygon_symbol, walutomat_symbol, walutomat_rest_symbol,
                    base_currency, quote_currency, created_at, updated_at
                ) VALUES (
                    :native, :kraken_ws, :kraken_rest, :ccxt,
                    :zonda, :polygon, :walutomat, :walutomat_rest,
                    :base, :quote, :created_at, :updated_at
                )
                """),
            {
                "native": mapping[0],
                "kraken_ws": mapping[1],
                "kraken_rest": mapping[2],
                "ccxt": mapping[3],
                "zonda": mapping[4],
                "polygon": mapping[5],
                "walutomat": mapping[6],
                "walutomat_rest": mapping[7],
                "base": mapping[8],
                "quote": mapping[9],
                "created_at": now,
                "updated_at": now,
            },
        )


def downgrade() -> None:
    """Drop all tables in reverse order of creation.

    Removes all tables created by the upgrade function, respecting
    foreign key constraints by dropping in reverse dependency order.
    """
    op.drop_table("polygon_import_log")
    op.drop_table("market_snapshots")
    op.drop_table("instrument_specs")
    op.drop_table("process_runs")
    op.drop_table("symbol_mappings")
    op.drop_table("settings")
    op.drop_table("users")
    op.drop_table("signal_events")
    op.drop_table("strategy_runs")
    op.drop_table("positions")
    op.drop_table("executions")
    op.drop_table("orders")
    op.drop_table("trades")
    op.drop_table("candles")
    op.drop_table("instruments")
