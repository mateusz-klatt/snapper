"""SQLAlchemy ORM models for Snapper persistence."""

from datetime import UTC
from datetime import datetime
from typing import Any
from uuid import UUID
from uuid import uuid7

from sqlalchemy import JSON
from sqlalchemy import Boolean
from sqlalchemy import CheckConstraint
from sqlalchemy import DateTime
from sqlalchemy import Float
from sqlalchemy import Index
from sqlalchemy import Integer
from sqlalchemy import String
from sqlalchemy import Text
from sqlalchemy import UniqueConstraint
from sqlalchemy import text
from sqlalchemy import types
from sqlalchemy.dialects.postgresql import UUID as PG_UUID
from sqlalchemy.engine.interfaces import Dialect
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy.types import TypeDecorator

from snapper.core.json_types import JsonObject
from snapper.core.types import AliasChannelEnum
from snapper.core.types import AssetTypeEnum

KNOWN_TO_MAX = datetime(9999, 12, 31, 23, 59, 59, tzinfo=UTC)


def _public_id() -> str:
    """Generate a new UUID7 string for use as a public identifier."""
    return str(uuid7())


class TZDateTime(TypeDecorator[datetime]):
    impl = DateTime
    cache_ok = True

    def process_bind_param(self, value: datetime | None, dialect: Dialect) -> datetime | None:
        if value is None:
            return None
        if value.tzinfo is None:
            raise ValueError(
                f"Cannot save naive datetime {value} to database. "
                "All datetime values must have timezone info."
            )
        return value.astimezone(UTC)

    def process_result_value(self, value: datetime | None, dialect: Dialect) -> datetime | None:
        if value is not None and value.tzinfo is None:
            value = value.replace(tzinfo=UTC)
        return value


class UUIDColumn(TypeDecorator[str]):
    """UUID storage: native UUID on PostgreSQL, String(36) on SQLite."""

    impl = String(36)
    cache_ok = True

    def load_dialect_impl(self, dialect: Dialect) -> types.TypeEngine[Any]:
        if dialect.name == "postgresql":

            return dialect.type_descriptor(PG_UUID(as_uuid=False))
        return dialect.type_descriptor(String(36))

    def process_bind_param(self, value: str | UUID | None, dialect: Dialect) -> str | None:
        if value is None:
            return None
        return str(value)

    def process_result_value(self, value: str | None, dialect: Dialect) -> str | None:
        return value


__all__ = [
    "KNOWN_TO_MAX",
    "Base",
    "TemporalMixin",
    "Instrument",
    "Candle",
    "Tick",
    "Trade",
    "Order",
    "Execution",
    "Position",
    "Signal",
    "User",
    "UserLoginEvent",
    "Setting",
    "Symbol",
    "SymbolAlias",
    "SymbolExchangeCapability",
    "ProcessRun",
    "InstrumentSpec",
    "MarketSnapshot",
    "Control",
    "Telemetry",
    "TradeCommand",
    "VenueEvent",
    "TradeProjectionCheckpoint",
]


_CK_EXCHANGE_LOWER = "exchange = LOWER(exchange)"
_KNOWN_TO_ACTIVE_PG = text("known_to = '9999-12-31T23:59:59+00:00'")
_KNOWN_TO_ACTIVE_SQLITE = text("known_to = '9999-12-31 23:59:59.000000'")


class Base(DeclarativeBase):
    """Base class for all SQLAlchemy ORM models."""


class TemporalMixin:
    """Mixin providing standard temporal columns for all entity tables.

    Every entity table inherits: autoincrement integer id, UUID7 public_id,
    provenance fields (session_id, sequence_id), bus-time timestamp,
    and SCD2 known_to with KNOWN_TO_MAX default.
    """

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    public_id: Mapped[str] = mapped_column(UUIDColumn(), default=_public_id)
    session_id: Mapped[str] = mapped_column(String(36))
    sequence_id: Mapped[int] = mapped_column(Integer)
    timestamp: Mapped[datetime] = mapped_column(TZDateTime())
    known_to: Mapped[datetime] = mapped_column(TZDateTime(), default=KNOWN_TO_MAX)


class Instrument(TemporalMixin, Base):
    """SQLAlchemy model for tradeable financial instruments.

    Logical identity key is (symbol_public_id, exchange) -- stable across
    symbol renames.  Symbol name, base, and quote are derived from the
    Symbol table via symbol_public_id temporal join.
    """

    __tablename__ = "instruments"
    __table_args__ = (
        Index(
            "uq_instrument_spid_exchange",
            "symbol_public_id",
            "exchange",
            unique=True,
            sqlite_where=_KNOWN_TO_ACTIVE_SQLITE,
            postgresql_where=_KNOWN_TO_ACTIVE_PG,
        ),
        Index(
            "ix_instruments_public_id",
            "public_id",
            unique=True,
            sqlite_where=_KNOWN_TO_ACTIVE_SQLITE,
            postgresql_where=_KNOWN_TO_ACTIVE_PG,
        ),
        CheckConstraint(_CK_EXCHANGE_LOWER, name="ck_instrument_exchange_lower"),
        Index("ix_instruments_exchange", "exchange"),
    )
    symbol_public_id: Mapped[str] = mapped_column(UUIDColumn(), index=True)
    exchange: Mapped[str] = mapped_column(String(20))


class Candle(TemporalMixin, Base):
    """SQLAlchemy model for OHLCV candlestick data."""

    __tablename__ = "candles"
    __table_args__ = (
        Index(
            "uq_candle_itf_open",
            "instrument_public_id",
            "timeframe",
            "open_at",
            unique=True,
            sqlite_where=_KNOWN_TO_ACTIVE_SQLITE,
            postgresql_where=_KNOWN_TO_ACTIVE_PG,
        ),
        Index("ix_candle_instrument_open", "instrument_public_id", "open_at"),
        Index(
            "ix_candles_public_id",
            "public_id",
            unique=True,
            sqlite_where=_KNOWN_TO_ACTIVE_SQLITE,
            postgresql_where=_KNOWN_TO_ACTIVE_PG,
        ),
    )
    instrument_public_id: Mapped[str] = mapped_column(UUIDColumn(), index=True)
    open_at: Mapped[datetime] = mapped_column(TZDateTime())
    timeframe: Mapped[str] = mapped_column(String(8))
    open: Mapped[float] = mapped_column(Float)
    high: Mapped[float] = mapped_column(Float)
    low: Mapped[float] = mapped_column(Float)
    close: Mapped[float] = mapped_column(Float)
    volume: Mapped[float] = mapped_column(Float)
    vwap: Mapped[float | None] = mapped_column(Float)
    trades: Mapped[int | None] = mapped_column(Integer)


class Tick(TemporalMixin, Base):
    """SQLAlchemy model for real-time price tick snapshots."""

    __tablename__ = "ticks"
    __table_args__ = (
        Index("ix_tick_instrument_ts", "instrument_public_id", "timestamp"),
        Index(
            "ix_ticks_public_id",
            "public_id",
            unique=True,
            sqlite_where=_KNOWN_TO_ACTIVE_SQLITE,
            postgresql_where=_KNOWN_TO_ACTIVE_PG,
        ),
    )
    instrument_public_id: Mapped[str] = mapped_column(UUIDColumn(), index=True)
    bid: Mapped[float | None] = mapped_column(Float)
    ask: Mapped[float | None] = mapped_column(Float)
    last: Mapped[float | None] = mapped_column(Float)
    volume: Mapped[float] = mapped_column(Float)


class Trade(TemporalMixin, Base):
    """SQLAlchemy model for individual market trades.

    timestamp (from TemporalMixin) is bus-time when the trade was received.
    executed_at is domain-time when the trade actually occurred on the exchange.
    """

    __tablename__ = "trades"
    __table_args__ = (
        UniqueConstraint("instrument_public_id", "trade_id", name="uq_trade_instrument_trade_id"),
        Index("ix_trade_instrument_ts", "instrument_public_id", "timestamp"),
        Index(
            "ix_trades_public_id",
            "public_id",
            unique=True,
            sqlite_where=_KNOWN_TO_ACTIVE_SQLITE,
            postgresql_where=_KNOWN_TO_ACTIVE_PG,
        ),
    )
    instrument_public_id: Mapped[str] = mapped_column(UUIDColumn(), index=True)
    price: Mapped[float] = mapped_column(Float)
    size: Mapped[float] = mapped_column(Float)
    side: Mapped[str] = mapped_column(String(4))
    trade_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    executed_at: Mapped[datetime | None] = mapped_column(TZDateTime(), nullable=True)


class Order(TemporalMixin, Base):
    """SQLAlchemy model for trading order records."""

    __tablename__ = "orders"
    __table_args__ = (
        Index(
            "uq_orders_client_oid",
            "instrument_public_id",
            "mode",
            "client_order_id",
            unique=True,
            sqlite_where=text(
                "client_order_id IS NOT NULL AND known_to = '9999-12-31 23:59:59.000000'"
            ),
            postgresql_where=text(
                "client_order_id IS NOT NULL AND known_to = '9999-12-31 23:59:59+00'"
            ),
        ),
        Index(
            "uq_orders_exchange_oid",
            "instrument_public_id",
            "mode",
            "exchange_order_id",
            unique=True,
            sqlite_where=text(
                "exchange_order_id IS NOT NULL AND known_to = '9999-12-31 23:59:59.000000'"
            ),
            postgresql_where=text(
                "exchange_order_id IS NOT NULL AND known_to = '9999-12-31 23:59:59+00'"
            ),
        ),
        Index(
            "ix_orders_public_id",
            "public_id",
            unique=True,
            sqlite_where=_KNOWN_TO_ACTIVE_SQLITE,
            postgresql_where=_KNOWN_TO_ACTIVE_PG,
        ),
    )
    instrument_public_id: Mapped[str] = mapped_column(UUIDColumn(), index=True)
    mode: Mapped[str] = mapped_column(String(8), default="live", server_default="live")
    client_order_id: Mapped[str | None] = mapped_column(String(64), index=True)
    exchange_order_id: Mapped[str | None] = mapped_column(String(64), index=True)
    created_at: Mapped[datetime] = mapped_column(TZDateTime())
    updated_at: Mapped[datetime | None] = mapped_column(TZDateTime())
    side: Mapped[str] = mapped_column(String(4))
    order_type: Mapped[str] = mapped_column(String(16))
    price: Mapped[float | None] = mapped_column(Float)
    size: Mapped[float] = mapped_column(Float)
    status: Mapped[str] = mapped_column(String(16))
    time_in_force: Mapped[str | None] = mapped_column(String(16))
    filled_size: Mapped[float] = mapped_column(Float, default=0.0, server_default="0")
    average_price: Mapped[float | None] = mapped_column(Float)
    error: Mapped[str | None] = mapped_column(String(512))


class Execution(TemporalMixin, Base):
    """SQLAlchemy model for order execution fills."""

    __tablename__ = "executions"
    __table_args__ = (
        Index(
            "uq_executions_order_exec",
            "order_public_id",
            "exec_id",
            unique=True,
            sqlite_where=text("exec_id IS NOT NULL"),
        ),
        Index(
            "uq_executions_order_trade",
            "order_public_id",
            "trade_id",
            unique=True,
            sqlite_where=text("trade_id IS NOT NULL"),
        ),
        Index(
            "ix_executions_public_id",
            "public_id",
            unique=True,
            sqlite_where=_KNOWN_TO_ACTIVE_SQLITE,
            postgresql_where=_KNOWN_TO_ACTIVE_PG,
        ),
    )
    order_public_id: Mapped[str] = mapped_column(UUIDColumn(), index=True)
    exec_id: Mapped[str | None] = mapped_column(String(64))
    trade_id: Mapped[str | None] = mapped_column(String(64))
    side: Mapped[str] = mapped_column(String(4))
    status: Mapped[str] = mapped_column(String(16))
    price: Mapped[float] = mapped_column(Float)
    size: Mapped[float] = mapped_column(Float)
    fee: Mapped[float] = mapped_column(Float)
    fee_asset: Mapped[str] = mapped_column(String(16))
    executed_at: Mapped[datetime | None] = mapped_column(TZDateTime())


class Position(TemporalMixin, Base):
    """SQLAlchemy model for open trading positions."""

    __tablename__ = "positions"
    __table_args__ = (
        Index(
            "uq_positions_instrument_public_id",
            "instrument_public_id",
            "mode",
            unique=True,
            sqlite_where=_KNOWN_TO_ACTIVE_SQLITE,
            postgresql_where=_KNOWN_TO_ACTIVE_PG,
        ),
        Index(
            "ix_positions_public_id",
            "public_id",
            unique=True,
            sqlite_where=_KNOWN_TO_ACTIVE_SQLITE,
            postgresql_where=_KNOWN_TO_ACTIVE_PG,
        ),
    )
    instrument_public_id: Mapped[str] = mapped_column(UUIDColumn(), index=True)
    mode: Mapped[str] = mapped_column(String(8), default="live", server_default="live")
    quantity: Mapped[float] = mapped_column(Float)
    average_price: Mapped[float] = mapped_column(Float)
    unrealized_pnl: Mapped[float] = mapped_column(Float)
    realized_pnl: Mapped[float] = mapped_column(Float)


class Signal(TemporalMixin, Base):
    """SQLAlchemy model for trading signal events."""

    __tablename__ = "signals"
    __table_args__ = (
        Index(
            "ix_signals_public_id",
            "public_id",
            unique=True,
            sqlite_where=_KNOWN_TO_ACTIVE_SQLITE,
            postgresql_where=_KNOWN_TO_ACTIVE_PG,
        ),
    )
    instrument_public_id: Mapped[str] = mapped_column(UUIDColumn(), index=True)
    fired_at: Mapped[datetime] = mapped_column(TZDateTime(), index=True)
    side: Mapped[str] = mapped_column(String(4))
    strength: Mapped[float] = mapped_column(Float)
    reason: Mapped[str] = mapped_column(String(256))
    strategy_name: Mapped[str | None] = mapped_column(String(64))
    price: Mapped[float | None] = mapped_column(Float)


class User(TemporalMixin, Base):
    """SQLAlchemy model for user accounts and authentication."""

    __tablename__ = "users"
    __table_args__ = (
        Index(
            "ix_users_public_id",
            "public_id",
            unique=True,
            sqlite_where=_KNOWN_TO_ACTIVE_SQLITE,
            postgresql_where=_KNOWN_TO_ACTIVE_PG,
        ),
        Index(
            "uq_users_username",
            "username",
            unique=True,
            sqlite_where=_KNOWN_TO_ACTIVE_SQLITE,
            postgresql_where=_KNOWN_TO_ACTIVE_PG,
        ),
    )
    username: Mapped[str] = mapped_column(String(64))
    email: Mapped[str | None] = mapped_column(String(255))
    password_hash: Mapped[str] = mapped_column(String(255))
    role: Mapped[str] = mapped_column(String(32))
    is_active: Mapped[bool] = mapped_column(default=True)
    created_at: Mapped[datetime] = mapped_column(TZDateTime())


class UserLoginEvent(TemporalMixin, Base):
    """Temporal log of user login events.

    Each login creates a new event. Events can be closed (known_to < MAX)
    to hide them from current queries while preserving audit history.
    Point-in-time queries via as_of show the login state at any moment.
    """

    __tablename__ = "user_login_events"
    __table_args__ = (
        Index(
            "ix_user_login_events_public_id",
            "public_id",
            unique=True,
            sqlite_where=_KNOWN_TO_ACTIVE_SQLITE,
            postgresql_where=_KNOWN_TO_ACTIVE_PG,
        ),
    )
    user_public_id: Mapped[str] = mapped_column(UUIDColumn(), index=True)
    logged_at: Mapped[datetime] = mapped_column(TZDateTime())


class Setting(TemporalMixin, Base):
    """SQLAlchemy model for application configuration settings."""

    __tablename__ = "settings"
    __table_args__ = (
        Index(
            "ix_settings_public_id",
            "public_id",
            unique=True,
            sqlite_where=_KNOWN_TO_ACTIVE_SQLITE,
            postgresql_where=_KNOWN_TO_ACTIVE_PG,
        ),
        Index(
            "uq_settings_key",
            "key",
            unique=True,
            sqlite_where=_KNOWN_TO_ACTIVE_SQLITE,
            postgresql_where=_KNOWN_TO_ACTIVE_PG,
        ),
    )
    key: Mapped[str] = mapped_column(String(64))
    value: Mapped[str] = mapped_column(String(1024))
    category: Mapped[str] = mapped_column(String(32))
    description: Mapped[str | None] = mapped_column(String(256))
    is_encrypted: Mapped[bool] = mapped_column(Boolean, default=False)
    updated_by: Mapped[str | None] = mapped_column(String(64))


class Symbol(TemporalMixin, Base):
    """Temporal symbol table with versioned attributes (SCD Type 2).

    Merges the former Symbol identity table and SymbolVersion versioned
    attributes into a single temporal table.  Each row carries the full
    payload (native_symbol, base, quote, asset_type) and participates in
    the standard close-and-insert lifecycle via TemporalMixin.

    The partial unique index on native_symbol ensures only one active row
    per native symbol at any point in time.
    """

    __tablename__ = "symbols"
    __table_args__ = (
        Index(
            "uq_symbols_active_native",
            "native_symbol",
            unique=True,
            sqlite_where=_KNOWN_TO_ACTIVE_SQLITE,
            postgresql_where=_KNOWN_TO_ACTIVE_PG,
        ),
        Index(
            "ix_symbols_public_id",
            "public_id",
            unique=True,
            sqlite_where=_KNOWN_TO_ACTIVE_SQLITE,
            postgresql_where=_KNOWN_TO_ACTIVE_PG,
        ),
        CheckConstraint(
            f"asset_type IN ({', '.join(repr(v.value) for v in AssetTypeEnum)})",
            name="ck_symbol_asset_type",
        ),
        CheckConstraint(
            f"asset_type IN ({', '.join(repr(v.value) for v in AssetTypeEnum if v in (AssetTypeEnum.EQUITY, AssetTypeEnum.INDEX))}) OR quote IS NOT NULL",
            name="ck_symbol_quote_required",
        ),
    )
    native_symbol: Mapped[str] = mapped_column(String(32))
    base: Mapped[str] = mapped_column(String(16))
    quote: Mapped[str | None] = mapped_column(String(16))
    asset_type: Mapped[str] = mapped_column(String(16), server_default=AssetTypeEnum.CRYPTO)
    created_at: Mapped[datetime] = mapped_column(TZDateTime())


class SymbolAlias(TemporalMixin, Base):
    """SQLAlchemy model for exchange-specific symbol aliases.

    Normalized: one row per (symbol_public_id, exchange, channel) instead
    of one column per exchange.  References Symbol via logical public_id
    (no hard FK because Symbol is temporal with multiple rows per public_id).
    """

    __tablename__ = "symbol_aliases"
    __table_args__ = (
        CheckConstraint(
            _CK_EXCHANGE_LOWER,
            name="ck_symbol_alias_exchange_lower",
        ),
        CheckConstraint(
            f"channel IN ({', '.join(repr(v.value) for v in AliasChannelEnum)})",
            name="ck_symbol_alias_channel",
        ),
        Index(
            "uq_alias_spid_exchange_channel",
            "symbol_public_id",
            "exchange",
            "channel",
            unique=True,
            sqlite_where=_KNOWN_TO_ACTIVE_SQLITE,
            postgresql_where=_KNOWN_TO_ACTIVE_PG,
        ),
        Index(
            "uq_alias_exchange_channel_symbol",
            "exchange",
            "channel",
            "exchange_symbol",
            unique=True,
            sqlite_where=_KNOWN_TO_ACTIVE_SQLITE,
            postgresql_where=_KNOWN_TO_ACTIVE_PG,
        ),
        Index(
            "ix_symbol_aliases_public_id",
            "public_id",
            unique=True,
            sqlite_where=_KNOWN_TO_ACTIVE_SQLITE,
            postgresql_where=_KNOWN_TO_ACTIVE_PG,
        ),
    )
    symbol_public_id: Mapped[str] = mapped_column(UUIDColumn(), index=True)
    exchange: Mapped[str] = mapped_column(String(20))
    channel: Mapped[str] = mapped_column(String(10))
    exchange_symbol: Mapped[str] = mapped_column(String(40))
    created_at: Mapped[datetime] = mapped_column(TZDateTime())


class SymbolExchangeCapability(TemporalMixin, Base):
    """Exchange-specific symbol capabilities.

    Separates symbol translation (what format?) from capabilities (what can I
    do?). Each row declares whether a given symbol is tradeable and/or has
    market data on a specific exchange.  References Symbol via logical
    public_id (no hard FK because Symbol is temporal).

    Attributes:
        symbol_public_id: Logical key referencing Symbol.public_id.
        exchange: Exchange identifier (lowercase).
        can_market_data: Whether exchange provides market data for this symbol.
        can_trade: Whether exchange supports trading this symbol.
        source: Origin of the capability information (e.g., updater name).
        reason: Human-readable explanation for the capability values.
        created_at: Row creation timestamp (UTC).
    """

    __tablename__ = "symbol_exchange_capabilities"
    __table_args__ = (
        Index(
            "uq_sec_symbol_exchange",
            "symbol_public_id",
            "exchange",
            unique=True,
            sqlite_where=_KNOWN_TO_ACTIVE_SQLITE,
            postgresql_where=_KNOWN_TO_ACTIVE_PG,
        ),
        CheckConstraint(
            _CK_EXCHANGE_LOWER,
            name="ck_sec_exchange_lower",
        ),
        Index("ix_sec_exchange", "exchange"),
        Index(
            "ix_sec_exchange_trade",
            "exchange",
            "can_trade",
            sqlite_where=text("can_trade = 1"),
        ),
        Index(
            "ix_sec_exchange_md",
            "exchange",
            "can_market_data",
            sqlite_where=text("can_market_data = 1"),
        ),
        Index(
            "ix_symbol_exchange_capabilities_public_id",
            "public_id",
            unique=True,
            sqlite_where=_KNOWN_TO_ACTIVE_SQLITE,
            postgresql_where=_KNOWN_TO_ACTIVE_PG,
        ),
    )
    symbol_public_id: Mapped[str] = mapped_column(UUIDColumn(), index=True)
    exchange: Mapped[str] = mapped_column(String(20))
    can_market_data: Mapped[bool] = mapped_column(Boolean, default=False)
    can_trade: Mapped[bool] = mapped_column(Boolean, default=False)
    source: Mapped[str | None] = mapped_column(String(50))
    reason: Mapped[str | None] = mapped_column(String(1024))
    created_at: Mapped[datetime] = mapped_column(TZDateTime())


class ProcessRun(TemporalMixin, Base):
    """SQLAlchemy model for background process execution records."""

    __tablename__ = "process_runs"
    __table_args__ = (
        Index(
            "ix_process_runs_public_id",
            "public_id",
            unique=True,
            sqlite_where=_KNOWN_TO_ACTIVE_SQLITE,
            postgresql_where=_KNOWN_TO_ACTIVE_PG,
        ),
    )
    process_name: Mapped[str] = mapped_column(String(64), index=True)
    role: Mapped[str] = mapped_column(String(16))
    lifecycle: Mapped[str] = mapped_column(String(16))
    status: Mapped[str] = mapped_column(String(16), index=True)
    parameters: Mapped[JsonObject | None] = mapped_column(JSON)
    result: Mapped[JsonObject | None] = mapped_column(JSON)
    error: Mapped[str | None] = mapped_column(String(1024))
    tags: Mapped[list[str] | None] = mapped_column(JSON)
    started_at: Mapped[datetime] = mapped_column(TZDateTime(), index=True)
    completed_at: Mapped[datetime | None] = mapped_column(TZDateTime())


class InstrumentSpec(TemporalMixin, Base):
    """SQLAlchemy model for instrument trading specifications."""

    __tablename__ = "instrument_specs"
    __table_args__ = (
        Index(
            "uq_instrument_spec_instrument",
            "instrument_public_id",
            unique=True,
            sqlite_where=_KNOWN_TO_ACTIVE_SQLITE,
            postgresql_where=_KNOWN_TO_ACTIVE_PG,
        ),
        Index(
            "ix_instrument_specs_public_id",
            "public_id",
            unique=True,
            sqlite_where=_KNOWN_TO_ACTIVE_SQLITE,
            postgresql_where=_KNOWN_TO_ACTIVE_PG,
        ),
    )
    instrument_public_id: Mapped[str] = mapped_column(UUIDColumn(), index=True)
    tick_size: Mapped[float | None] = mapped_column(Float, comment="Minimum price increment")
    lot_size: Mapped[float | None] = mapped_column(Float, comment="Minimum order size increment")
    min_order_size: Mapped[float | None] = mapped_column(Float, comment="Minimum order size")
    max_order_size: Mapped[float | None] = mapped_column(Float, comment="Maximum order size")
    cost_decimals: Mapped[int | None] = mapped_column(Integer, comment="Decimal precision for cost")
    qty_decimals: Mapped[int | None] = mapped_column(
        Integer, comment="Decimal precision for quantity"
    )
    margin_initial: Mapped[float | None] = mapped_column(Float, comment="Initial margin percentage")
    position_limit_long: Mapped[int | None] = mapped_column(Integer, comment="Long position limit")
    position_limit_short: Mapped[int | None] = mapped_column(
        Integer, comment="Short position limit"
    )
    status: Mapped[str | None] = mapped_column(
        String(20), comment="Trading status (e.g., online, offline)"
    )


class MarketSnapshot(TemporalMixin, Base):
    """SQLAlchemy model for real-time market data snapshots.

    One active row per instrument (SCD2 close+insert on each update).
    Symbol and exchange are derived via instrument_public_id temporal
    join to Instrument and Symbol tables.
    """

    __tablename__ = "market_snapshots"
    __table_args__ = (
        Index(
            "uq_market_snapshot_instrument",
            "instrument_public_id",
            unique=True,
            sqlite_where=_KNOWN_TO_ACTIVE_SQLITE,
            postgresql_where=_KNOWN_TO_ACTIVE_PG,
        ),
        Index("ix_market_snapshots_instrument_ts", "instrument_public_id", "timestamp"),
        Index(
            "ix_market_snapshots_public_id",
            "public_id",
            unique=True,
            sqlite_where=_KNOWN_TO_ACTIVE_SQLITE,
            postgresql_where=_KNOWN_TO_ACTIVE_PG,
        ),
    )
    instrument_public_id: Mapped[str] = mapped_column(UUIDColumn(), index=True)
    bid: Mapped[float | None] = mapped_column(Float, comment="Best bid price")
    bid_volume: Mapped[float | None] = mapped_column(Float, comment="Volume at best bid")
    ask: Mapped[float | None] = mapped_column(Float, comment="Best ask price")
    ask_volume: Mapped[float | None] = mapped_column(Float, comment="Volume at best ask")
    last_price: Mapped[float | None] = mapped_column(Float, comment="Last trade price")
    volume_24h: Mapped[float | None] = mapped_column(Float, comment="24-hour trading volume")
    vwap_24h: Mapped[float | None] = mapped_column(
        Float, comment="24-hour volume-weighted average price"
    )
    low_24h: Mapped[float | None] = mapped_column(Float, comment="24-hour low price")
    high_24h: Mapped[float | None] = mapped_column(Float, comment="24-hour high price")
    change_24h: Mapped[float | None] = mapped_column(
        Float, comment="24-hour price change percentage"
    )
    spread: Mapped[float | None] = mapped_column(Float, comment="Current spread (ask - bid)")
    spread_pct: Mapped[float | None] = mapped_column(
        Float, comment="Spread as percentage of mid price"
    )


class Control(TemporalMixin, Base):
    """Temporal log of control-plane messages (commands, responses, errors).

    Each row captures a single inbound or outbound control message with its
    transport, discriminator, outcome, and optional redacted payload.
    """

    __tablename__ = "control"
    __table_args__ = (
        Index(
            "ix_control_public_id",
            "public_id",
            unique=True,
            sqlite_where=_KNOWN_TO_ACTIVE_SQLITE,
            postgresql_where=_KNOWN_TO_ACTIVE_PG,
        ),
    )
    transport: Mapped[str] = mapped_column(String(10))
    direction: Mapped[str] = mapped_column(String(10))
    message_type: Mapped[str] = mapped_column(String(128))
    outcome: Mapped[str] = mapped_column(String(16))
    detail: Mapped[str | None] = mapped_column(Text)
    payload: Mapped[str | None] = mapped_column(Text)
    client_session_id: Mapped[str | None] = mapped_column(String(36))
    client_public_id: Mapped[str | None] = mapped_column(String(36))


class Telemetry(TemporalMixin, Base):
    """Temporal log of data-plane messages (market data, order updates).

    Each row captures a single inbound or outbound data message with its
    transport, discriminator, and optional payload snapshot.
    """

    __tablename__ = "telemetry"
    __table_args__ = (
        Index(
            "ix_telemetry_public_id",
            "public_id",
            unique=True,
            sqlite_where=_KNOWN_TO_ACTIVE_SQLITE,
            postgresql_where=_KNOWN_TO_ACTIVE_PG,
        ),
    )
    transport: Mapped[str] = mapped_column(String(10))
    direction: Mapped[str] = mapped_column(String(10))
    message_type: Mapped[str] = mapped_column(String(128))
    payload: Mapped[str | None] = mapped_column(Text)


class TradeCommand(TemporalMixin, Base):
    """Durable intent log for order commands.

    Written by TradingEngineService BEFORE ZMQ publish. Outbox dispatcher
    picks up rows with status='created' and publishes to ZMQ. TradeService
    updates status as venue events arrive. Status transitions use SCD2
    versioning (close old row, insert new version) like Order.
    """

    __tablename__ = "trade_commands"
    __table_args__ = (
        Index("ix_trade_commands_status", "status"),
        Index("ix_trade_commands_shard_key", "shard_key"),
        Index(
            "uq_trade_commands_idempotency",
            "idempotency_key",
            unique=True,
            sqlite_where=text(
                "idempotency_key IS NOT NULL AND known_to = '9999-12-31 23:59:59.000000'"
            ),
            postgresql_where=text(
                "idempotency_key IS NOT NULL AND known_to = '9999-12-31T23:59:59+00:00'"
            ),
        ),
        Index(
            "ix_trade_commands_public_id",
            "public_id",
            unique=True,
            sqlite_where=_KNOWN_TO_ACTIVE_SQLITE,
            postgresql_where=_KNOWN_TO_ACTIVE_PG,
        ),
    )
    command_type: Mapped[str] = mapped_column(String(16))
    shard_key: Mapped[str] = mapped_column(String(64))
    exchange: Mapped[str] = mapped_column(String(32))
    instrument: Mapped[str] = mapped_column(String(64))
    mode: Mapped[str] = mapped_column(String(8))
    strategy_id: Mapped[str] = mapped_column(String(64))
    client_order_id: Mapped[str] = mapped_column(UUIDColumn())
    venue_client_id: Mapped[str] = mapped_column(String(64))
    idempotency_key: Mapped[str | None] = mapped_column(String(128))
    side: Mapped[str] = mapped_column(String(4))
    order_type: Mapped[str] = mapped_column(String(16))
    quantity: Mapped[float] = mapped_column(Float)
    price: Mapped[float | None] = mapped_column(Float)
    status: Mapped[str] = mapped_column(String(32))
    attempt_count: Mapped[int] = mapped_column(Integer, default=0, server_default="0")
    last_error: Mapped[str | None] = mapped_column(String(512))
    created_at: Mapped[datetime] = mapped_column(TZDateTime())
    dispatched_at: Mapped[datetime | None] = mapped_column(TZDateTime())
    acked_at: Mapped[datetime | None] = mapped_column(TZDateTime())
    terminal_at: Mapped[datetime | None] = mapped_column(TZDateTime())
    exchange_order_id: Mapped[str | None] = mapped_column(String(64))
    supersedes_command_id: Mapped[str | None] = mapped_column(UUIDColumn())
    correlation_id: Mapped[str] = mapped_column(UUIDColumn(), default=_public_id)


class VenueEvent(TemporalMixin, Base):
    """Append-only log of raw venue observations.

    Created by ExchangeExecutorService when venue state changes are
    detected (WS stream for Kraken/Zonda, HTTP polling for Walutomat,
    in-process for Paper). All exchanges produce the same schema.

    The TemporalMixin id (auto-increment PK) serves as the monotonic
    watermark for checkpoint recovery. Append-only rows use
    known_to=KNOWN_TO_MAX and are never closed.
    """

    __tablename__ = "venue_events"
    __table_args__ = (
        Index("ix_venue_events_shard_id", "shard_key", "id"),
        Index("ix_venue_events_command_public_id", "command_public_id"),
        Index(
            "ix_venue_events_public_id",
            "public_id",
            unique=True,
            sqlite_where=_KNOWN_TO_ACTIVE_SQLITE,
            postgresql_where=_KNOWN_TO_ACTIVE_PG,
        ),
    )
    event_type: Mapped[str] = mapped_column(String(32))
    shard_key: Mapped[str] = mapped_column(String(64))
    command_public_id: Mapped[str | None] = mapped_column(UUIDColumn())
    exchange: Mapped[str] = mapped_column(String(32))
    instrument: Mapped[str] = mapped_column(String(64))
    mode: Mapped[str] = mapped_column(String(8))
    exchange_order_id: Mapped[str | None] = mapped_column(String(64))
    client_order_id: Mapped[str | None] = mapped_column(String(64))
    venue_client_id: Mapped[str | None] = mapped_column(String(64))
    side: Mapped[str | None] = mapped_column(String(4))
    status: Mapped[str | None] = mapped_column(String(32))
    fill_price: Mapped[float | None] = mapped_column(Float)
    fill_size: Mapped[float | None] = mapped_column(Float)
    cum_fill_size: Mapped[float | None] = mapped_column(Float)
    fee: Mapped[float | None] = mapped_column(Float)
    fee_asset: Mapped[str | None] = mapped_column(String(16))
    exec_id: Mapped[str | None] = mapped_column(String(64))
    trade_id: Mapped[str | None] = mapped_column(String(64))
    error: Mapped[str | None] = mapped_column(String(512))
    venue_timestamp: Mapped[datetime | None] = mapped_column(TZDateTime())
    received_at: Mapped[datetime] = mapped_column(TZDateTime())
    payload_json: Mapped[str | None] = mapped_column(Text)


class TradeProjectionCheckpoint(TemporalMixin, Base):
    """Materialized projection of trading state per shard.

    Written by TradeService after every confirmed fill. Recovery reads
    the checkpoint, then replays VenueEvents after the watermark to
    rebuild current state. One row per shard_key (upsert via SCD2:
    close old version, insert new).
    """

    __tablename__ = "trade_projection_checkpoints"
    __table_args__ = (
        Index(
            "uq_trade_checkpoints_shard_key",
            "shard_key",
            unique=True,
            sqlite_where=_KNOWN_TO_ACTIVE_SQLITE,
            postgresql_where=_KNOWN_TO_ACTIVE_PG,
        ),
        Index(
            "ix_trade_checkpoints_public_id",
            "public_id",
            unique=True,
            sqlite_where=_KNOWN_TO_ACTIVE_SQLITE,
            postgresql_where=_KNOWN_TO_ACTIVE_PG,
        ),
    )
    shard_key: Mapped[str] = mapped_column(String(64))
    position_qty: Mapped[float] = mapped_column(Float, default=0.0, server_default="0")
    entry_price: Mapped[float | None] = mapped_column(Float)
    cash: Mapped[float] = mapped_column(Float)
    peak_equity: Mapped[float] = mapped_column(Float)
    realized_pnl: Mapped[float] = mapped_column(Float, default=0.0, server_default="0")
    turnover: Mapped[float] = mapped_column(Float, default=0.0, server_default="0")
    last_venue_event_id: Mapped[int | None] = mapped_column(Integer)
    last_venue_event_at: Mapped[datetime | None] = mapped_column(TZDateTime())
    open_command_ids: Mapped[str | None] = mapped_column(Text)
    checkpoint_at: Mapped[datetime] = mapped_column(TZDateTime())
