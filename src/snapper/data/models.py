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
from sqlalchemy import ForeignKey
from sqlalchemy import Index
from sqlalchemy import Integer
from sqlalchemy import String
from sqlalchemy import UniqueConstraint
from sqlalchemy import text
from sqlalchemy import types
from sqlalchemy.dialects.postgresql import UUID as PG_UUID
from sqlalchemy.engine.interfaces import Dialect
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy.orm import relationship
from sqlalchemy.types import TypeDecorator

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
    "Instrument",
    "Candle",
    "Trade",
    "Order",
    "Execution",
    "Position",
    "Signal",
    "User",
    "UserLoginEvent",
    "Setting",
    "Symbol",
    "SymbolVersion",
    "SymbolAlias",
    "SymbolExchangeCapability",
    "ProcessRun",
    "InstrumentSpec",
    "MarketSnapshot",
]


_INSTRUMENT_FK = "instruments.id"
_FK_SYMBOLS = "symbols.native_symbol"
_CK_EXCHANGE_LOWER = "exchange = LOWER(exchange)"
_KNOWN_TO_ACTIVE = text("known_to = '9999-12-31T23:59:59+00:00'")


class Base(DeclarativeBase):
    """Base class for all SQLAlchemy ORM models."""


class Instrument(Base):
    """SQLAlchemy model for tradeable financial instruments."""

    __tablename__ = "instruments"
    __table_args__ = (
        Index(
            "uq_instrument_symbol_exchange",
            "symbol",
            "exchange",
            unique=True,
            sqlite_where=_KNOWN_TO_ACTIVE,
            postgresql_where=_KNOWN_TO_ACTIVE,
        ),
        Index(
            "ix_instruments_public_id",
            "public_id",
            unique=True,
            sqlite_where=_KNOWN_TO_ACTIVE,
            postgresql_where=_KNOWN_TO_ACTIVE,
        ),
        CheckConstraint(_CK_EXCHANGE_LOWER, name="ck_instrument_exchange_lower"),
        Index("ix_instruments_exchange", "exchange"),
    )
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    public_id: Mapped[str] = mapped_column(UUIDColumn(), default=_public_id)
    symbol: Mapped[str] = mapped_column(String(32), ForeignKey(_FK_SYMBOLS), index=True)
    exchange: Mapped[str] = mapped_column(String(20))
    base: Mapped[str] = mapped_column(String(16))
    quote: Mapped[str] = mapped_column(String(16))
    timestamp: Mapped[datetime | None] = mapped_column(TZDateTime(), nullable=True)
    known_to: Mapped[datetime] = mapped_column(TZDateTime(), default=KNOWN_TO_MAX)
    candles: Mapped[list[Candle]] = relationship(back_populates="instrument")
    trades: Mapped[list[Trade]] = relationship(back_populates="instrument")


class Candle(Base):
    """SQLAlchemy model for OHLCV candlestick data."""

    __tablename__ = "candles"
    __table_args__ = (
        Index(
            "uq_candle_itf_open",
            "instrument_id",
            "timeframe",
            "open_at",
            unique=True,
            sqlite_where=_KNOWN_TO_ACTIVE,
            postgresql_where=_KNOWN_TO_ACTIVE,
        ),
        Index("ix_candle_instrument_open", "instrument_id", "open_at"),
        Index(
            "ix_candles_public_id",
            "public_id",
            unique=True,
            sqlite_where=_KNOWN_TO_ACTIVE,
            postgresql_where=_KNOWN_TO_ACTIVE,
        ),
    )
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    public_id: Mapped[str] = mapped_column(UUIDColumn(), default=_public_id)
    instrument_id: Mapped[int] = mapped_column(ForeignKey(_INSTRUMENT_FK), index=True)
    open_at: Mapped[datetime] = mapped_column(TZDateTime())
    timestamp: Mapped[datetime] = mapped_column(TZDateTime())
    timeframe: Mapped[str] = mapped_column(String(8))
    open: Mapped[float] = mapped_column(Float)
    high: Mapped[float] = mapped_column(Float)
    low: Mapped[float] = mapped_column(Float)
    close: Mapped[float] = mapped_column(Float)
    volume: Mapped[float] = mapped_column(Float)
    vwap: Mapped[float | None] = mapped_column(Float, nullable=True)
    trades: Mapped[int | None] = mapped_column(Integer, nullable=True)
    known_to: Mapped[datetime] = mapped_column(TZDateTime(), default=KNOWN_TO_MAX)
    instrument: Mapped[Instrument] = relationship(back_populates="candles")


class Trade(Base):
    """SQLAlchemy model for individual market trades."""

    __tablename__ = "trades"
    __table_args__ = (
        UniqueConstraint("trade_id", name="uq_trade_trade_id"),
        Index("ix_trade_instrument_ts", "instrument_id", "timestamp"),
        Index(
            "ix_trades_public_id",
            "public_id",
            unique=True,
            sqlite_where=_KNOWN_TO_ACTIVE,
            postgresql_where=_KNOWN_TO_ACTIVE,
        ),
    )
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    public_id: Mapped[str] = mapped_column(UUIDColumn(), default=_public_id)
    instrument_id: Mapped[int] = mapped_column(ForeignKey(_INSTRUMENT_FK), index=True)
    timestamp: Mapped[datetime] = mapped_column(TZDateTime(), index=True)
    price: Mapped[float] = mapped_column(Float)
    size: Mapped[float] = mapped_column(Float)
    side: Mapped[str] = mapped_column(String(4))
    trade_id: Mapped[str] = mapped_column(String(64))
    known_to: Mapped[datetime] = mapped_column(TZDateTime(), default=KNOWN_TO_MAX)
    instrument: Mapped[Instrument] = relationship(back_populates="trades")


class Order(Base):
    """SQLAlchemy model for trading order records."""

    __tablename__ = "orders"
    __table_args__ = (
        Index(
            "uq_orders_client_oid",
            "instrument_id",
            "client_order_id",
            unique=True,
            sqlite_where=text(
                "client_order_id IS NOT NULL AND known_to = '9999-12-31T23:59:59+00:00'"
            ),
            postgresql_where=text(
                "client_order_id IS NOT NULL AND known_to = '9999-12-31 23:59:59+00'"
            ),
        ),
        Index(
            "uq_orders_exchange_oid",
            "instrument_id",
            "exchange_order_id",
            unique=True,
            sqlite_where=text(
                "exchange_order_id IS NOT NULL AND known_to = '9999-12-31T23:59:59+00:00'"
            ),
            postgresql_where=text(
                "exchange_order_id IS NOT NULL AND known_to = '9999-12-31 23:59:59+00'"
            ),
        ),
        Index(
            "ix_orders_public_id",
            "public_id",
            unique=True,
            sqlite_where=_KNOWN_TO_ACTIVE,
            postgresql_where=_KNOWN_TO_ACTIVE,
        ),
    )
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    public_id: Mapped[str] = mapped_column(UUIDColumn(), default=_public_id)
    instrument_id: Mapped[int] = mapped_column(ForeignKey(_INSTRUMENT_FK), index=True)
    client_order_id: Mapped[str | None] = mapped_column(String(64), index=True)
    exchange_order_id: Mapped[str | None] = mapped_column(String(64), index=True)
    created_at: Mapped[datetime] = mapped_column(TZDateTime())
    updated_at: Mapped[datetime | None] = mapped_column(TZDateTime())
    timestamp: Mapped[datetime] = mapped_column(TZDateTime())
    side: Mapped[str] = mapped_column(String(4))
    order_type: Mapped[str] = mapped_column(String(16))
    price: Mapped[float | None] = mapped_column(Float)
    size: Mapped[float] = mapped_column(Float)
    status: Mapped[str] = mapped_column(String(16))
    time_in_force: Mapped[str | None] = mapped_column(String(16))
    filled_size: Mapped[float] = mapped_column(Float, default=0.0, server_default="0")
    average_price: Mapped[float | None] = mapped_column(Float, nullable=True)
    error: Mapped[str | None] = mapped_column(String(512))
    known_to: Mapped[datetime] = mapped_column(TZDateTime(), default=KNOWN_TO_MAX)


class Execution(Base):
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
            sqlite_where=_KNOWN_TO_ACTIVE,
            postgresql_where=_KNOWN_TO_ACTIVE,
        ),
    )
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    public_id: Mapped[str] = mapped_column(UUIDColumn(), default=_public_id)
    order_id: Mapped[int] = mapped_column(ForeignKey("orders.id"), index=True)
    order_public_id: Mapped[str] = mapped_column(UUIDColumn(), index=True)
    exec_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    trade_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    timestamp: Mapped[datetime] = mapped_column(TZDateTime())
    side: Mapped[str] = mapped_column(String(4))
    status: Mapped[str] = mapped_column(String(16))
    price: Mapped[float] = mapped_column(Float)
    size: Mapped[float] = mapped_column(Float)
    fee: Mapped[float] = mapped_column(Float)
    fee_asset: Mapped[str] = mapped_column(String(16))
    executed_at: Mapped[datetime | None] = mapped_column(TZDateTime(), nullable=True)
    known_to: Mapped[datetime] = mapped_column(TZDateTime(), default=KNOWN_TO_MAX)
    order: Mapped[Order] = relationship()


class Position(Base):
    """SQLAlchemy model for open trading positions."""

    __tablename__ = "positions"
    __table_args__ = (
        Index(
            "uq_positions_instrument_id",
            "instrument_id",
            unique=True,
            sqlite_where=_KNOWN_TO_ACTIVE,
            postgresql_where=_KNOWN_TO_ACTIVE,
        ),
        Index(
            "ix_positions_public_id",
            "public_id",
            unique=True,
            sqlite_where=_KNOWN_TO_ACTIVE,
            postgresql_where=_KNOWN_TO_ACTIVE,
        ),
    )
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    public_id: Mapped[str] = mapped_column(UUIDColumn(), default=_public_id)
    instrument_id: Mapped[int] = mapped_column(ForeignKey(_INSTRUMENT_FK), index=True)
    quantity: Mapped[float] = mapped_column(Float)
    average_price: Mapped[float] = mapped_column(Float)
    unrealized_pnl: Mapped[float] = mapped_column(Float)
    realized_pnl: Mapped[float] = mapped_column(Float)
    timestamp: Mapped[datetime] = mapped_column(TZDateTime())
    known_to: Mapped[datetime] = mapped_column(TZDateTime(), default=KNOWN_TO_MAX)


class Signal(Base):
    """SQLAlchemy model for trading signal events."""

    __tablename__ = "signals"
    __table_args__ = (
        Index(
            "ix_signals_public_id",
            "public_id",
            unique=True,
            sqlite_where=_KNOWN_TO_ACTIVE,
            postgresql_where=_KNOWN_TO_ACTIVE,
        ),
    )
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    public_id: Mapped[str] = mapped_column(UUIDColumn(), default=_public_id)
    instrument_id: Mapped[int] = mapped_column(ForeignKey(_INSTRUMENT_FK), index=True)
    timestamp: Mapped[datetime] = mapped_column(TZDateTime())
    fired_at: Mapped[datetime] = mapped_column(TZDateTime(), index=True)
    side: Mapped[str] = mapped_column(String(4))
    strength: Mapped[float] = mapped_column(Float)
    reason: Mapped[str] = mapped_column(String(256))
    strategy_name: Mapped[str | None] = mapped_column(String(64), nullable=True)
    price: Mapped[float | None] = mapped_column(Float, nullable=True)
    known_to: Mapped[datetime] = mapped_column(TZDateTime(), default=KNOWN_TO_MAX)
    instrument: Mapped[Instrument] = relationship()


class User(Base):
    """SQLAlchemy model for user accounts and authentication."""

    __tablename__ = "users"
    __table_args__ = (
        Index(
            "ix_users_public_id",
            "public_id",
            unique=True,
            sqlite_where=_KNOWN_TO_ACTIVE,
            postgresql_where=_KNOWN_TO_ACTIVE,
        ),
        Index(
            "uq_users_username",
            "username",
            unique=True,
            sqlite_where=_KNOWN_TO_ACTIVE,
            postgresql_where=_KNOWN_TO_ACTIVE,
        ),
    )
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    public_id: Mapped[str] = mapped_column(UUIDColumn(), default=_public_id)
    username: Mapped[str] = mapped_column(String(64))
    email: Mapped[str | None] = mapped_column(String(255), nullable=True)
    password_hash: Mapped[str] = mapped_column(String(255))
    role: Mapped[str] = mapped_column(String(32))
    is_active: Mapped[bool] = mapped_column(default=True)
    created_at: Mapped[datetime] = mapped_column(TZDateTime())
    timestamp: Mapped[datetime] = mapped_column(TZDateTime())
    known_to: Mapped[datetime] = mapped_column(TZDateTime(), default=KNOWN_TO_MAX)


class UserLoginEvent(Base):
    """Append-only log of user login events."""

    __tablename__ = "user_login_events"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    user_public_id: Mapped[str] = mapped_column(UUIDColumn(), index=True)
    logged_at: Mapped[datetime] = mapped_column(TZDateTime())


class Setting(Base):
    """SQLAlchemy model for application configuration settings."""

    __tablename__ = "settings"
    __table_args__ = (
        Index(
            "ix_settings_public_id",
            "public_id",
            unique=True,
            sqlite_where=_KNOWN_TO_ACTIVE,
            postgresql_where=_KNOWN_TO_ACTIVE,
        ),
        Index(
            "uq_settings_key",
            "key",
            unique=True,
            sqlite_where=_KNOWN_TO_ACTIVE,
            postgresql_where=_KNOWN_TO_ACTIVE,
        ),
    )
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    public_id: Mapped[str] = mapped_column(UUIDColumn(), default=_public_id)
    key: Mapped[str] = mapped_column(String(64))
    value: Mapped[str] = mapped_column(String(1024))
    category: Mapped[str] = mapped_column(String(32))
    description: Mapped[str | None] = mapped_column(String(256), nullable=True)
    is_encrypted: Mapped[bool] = mapped_column(Boolean, default=False)
    timestamp: Mapped[datetime] = mapped_column(TZDateTime())
    updated_by: Mapped[str | None] = mapped_column(String(64), nullable=True)
    known_to: Mapped[datetime] = mapped_column(TZDateTime(), default=KNOWN_TO_MAX)


class Symbol(Base):
    """Stable identity table for native symbols. No versioning."""

    __tablename__ = "symbols"
    native_symbol: Mapped[str] = mapped_column(String(32), primary_key=True)
    created_at: Mapped[datetime] = mapped_column(TZDateTime())
    aliases: Mapped[list[SymbolAlias]] = relationship(back_populates="symbol")
    versions: Mapped[list[SymbolVersion]] = relationship(back_populates="symbol")


class SymbolVersion(Base):
    """Versioned attributes for a native symbol (SCD Type 2)."""

    __tablename__ = "symbol_versions"
    __table_args__ = (
        CheckConstraint(
            "asset_type IN ('crypto', 'forex', 'equity', 'index')",
            name="ck_symbol_version_asset_type",
        ),
        CheckConstraint(
            "asset_type IN ('equity', 'index') OR quote IS NOT NULL",
            name="ck_symbol_version_quote_required_for_pairs",
        ),
        Index("ix_sv_base_quote", "base", "quote"),
        Index(
            "uq_symbol_version_active",
            "native_symbol",
            unique=True,
            sqlite_where=_KNOWN_TO_ACTIVE,
            postgresql_where=_KNOWN_TO_ACTIVE,
        ),
        Index(
            "ix_symbol_versions_public_id",
            "public_id",
            unique=True,
            sqlite_where=_KNOWN_TO_ACTIVE,
            postgresql_where=_KNOWN_TO_ACTIVE,
        ),
    )
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    public_id: Mapped[str] = mapped_column(UUIDColumn(), default=_public_id)
    native_symbol: Mapped[str] = mapped_column(String(32), ForeignKey(_FK_SYMBOLS), index=True)
    base: Mapped[str] = mapped_column(String(16), nullable=False)
    quote: Mapped[str | None] = mapped_column(String(16), nullable=True)
    asset_type: Mapped[str] = mapped_column(String(16), nullable=False, server_default="crypto")
    timestamp: Mapped[datetime] = mapped_column(TZDateTime())
    known_to: Mapped[datetime] = mapped_column(TZDateTime(), default=KNOWN_TO_MAX)
    symbol: Mapped[Symbol] = relationship(back_populates="versions")


class SymbolAlias(Base):
    """SQLAlchemy model for exchange-specific symbol aliases.

    Normalized: one row per (native_symbol, exchange, channel) instead
    of one column per exchange. Replaces the old SymbolMapping table.
    """

    __tablename__ = "symbol_aliases"
    __table_args__ = (
        CheckConstraint(
            _CK_EXCHANGE_LOWER,
            name="ck_symbol_alias_exchange_lower",
        ),
        CheckConstraint(
            "channel IN ('ws', 'rest', 'ccxt')",
            name="ck_symbol_alias_channel",
        ),
        Index(
            "uq_alias_native_exchange_channel",
            "native_symbol",
            "exchange",
            "channel",
            unique=True,
            sqlite_where=_KNOWN_TO_ACTIVE,
            postgresql_where=_KNOWN_TO_ACTIVE,
        ),
        Index(
            "uq_alias_exchange_channel_symbol",
            "exchange",
            "channel",
            "exchange_symbol",
            unique=True,
            sqlite_where=_KNOWN_TO_ACTIVE,
            postgresql_where=_KNOWN_TO_ACTIVE,
        ),
        Index(
            "ix_symbol_aliases_public_id",
            "public_id",
            unique=True,
            sqlite_where=_KNOWN_TO_ACTIVE,
            postgresql_where=_KNOWN_TO_ACTIVE,
        ),
    )
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    public_id: Mapped[str] = mapped_column(UUIDColumn(), default=_public_id)
    native_symbol: Mapped[str] = mapped_column(
        String(32),
        ForeignKey(_FK_SYMBOLS),
        nullable=False,
        index=True,
    )
    exchange: Mapped[str] = mapped_column(String(20), nullable=False)
    channel: Mapped[str] = mapped_column(String(10), nullable=False)
    exchange_symbol: Mapped[str] = mapped_column(String(40), nullable=False)
    created_at: Mapped[datetime] = mapped_column(TZDateTime())
    timestamp: Mapped[datetime] = mapped_column(TZDateTime())
    known_to: Mapped[datetime] = mapped_column(TZDateTime(), default=KNOWN_TO_MAX)
    symbol: Mapped[Symbol] = relationship(back_populates="aliases")


class SymbolExchangeCapability(Base):
    """Exchange-specific symbol capabilities.

    Separates symbol translation (what format?) from capabilities (what can I
    do?). Each row declares whether a given native_symbol is tradeable and/or
    has market data on a specific exchange. Paper exchange is handled as a
    special case in code and has no rows in this table.

    Attributes:
        native_symbol: FK to symbols. Part of composite PK.
        exchange: Exchange identifier (lowercase). Part of composite PK.
        can_market_data: Whether exchange provides market data for this symbol.
        can_trade: Whether exchange supports trading this symbol.
        source: Origin of the capability information (e.g., updater name).
        reason: Human-readable explanation for the capability values.
        created_at: Row creation timestamp (UTC).
        timestamp: Last modification timestamp (UTC).
        symbol: Relationship to Symbol.
    """

    __tablename__ = "symbol_exchange_capabilities"
    __table_args__ = (
        Index(
            "uq_sec_symbol_exchange",
            "native_symbol",
            "exchange",
            unique=True,
            sqlite_where=_KNOWN_TO_ACTIVE,
            postgresql_where=_KNOWN_TO_ACTIVE,
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
            sqlite_where=_KNOWN_TO_ACTIVE,
            postgresql_where=_KNOWN_TO_ACTIVE,
        ),
    )
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    public_id: Mapped[str] = mapped_column(UUIDColumn(), default=_public_id)
    native_symbol: Mapped[str] = mapped_column(
        String(32),
        ForeignKey(_FK_SYMBOLS),
        nullable=False,
        index=True,
    )
    exchange: Mapped[str] = mapped_column(String(20), nullable=False)
    can_market_data: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    can_trade: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    source: Mapped[str | None] = mapped_column(String(50), nullable=True)
    reason: Mapped[str | None] = mapped_column(String(1024), nullable=True)
    created_at: Mapped[datetime] = mapped_column(TZDateTime())
    timestamp: Mapped[datetime] = mapped_column(TZDateTime())
    known_to: Mapped[datetime] = mapped_column(TZDateTime(), default=KNOWN_TO_MAX)
    symbol: Mapped[Symbol] = relationship()


class ProcessRun(Base):
    """SQLAlchemy model for background process execution records."""

    __tablename__ = "process_runs"
    __table_args__ = (
        Index(
            "ix_process_runs_public_id",
            "public_id",
            unique=True,
            sqlite_where=_KNOWN_TO_ACTIVE,
            postgresql_where=_KNOWN_TO_ACTIVE,
        ),
    )
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    public_id: Mapped[str] = mapped_column(UUIDColumn(), default=_public_id)
    process_name: Mapped[str] = mapped_column(String(64), index=True)
    role: Mapped[str] = mapped_column(String(16))
    lifecycle: Mapped[str] = mapped_column(String(16))
    status: Mapped[str] = mapped_column(String(16), index=True)
    parameters: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    result: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    error: Mapped[str | None] = mapped_column(String(1024), nullable=True)
    tags: Mapped[list[str] | None] = mapped_column(JSON, nullable=True)
    started_at: Mapped[datetime] = mapped_column(TZDateTime(), index=True)
    completed_at: Mapped[datetime | None] = mapped_column(TZDateTime())
    timestamp: Mapped[datetime] = mapped_column(TZDateTime())
    known_to: Mapped[datetime] = mapped_column(TZDateTime(), default=KNOWN_TO_MAX)


class InstrumentSpec(Base):
    """SQLAlchemy model for instrument trading specifications."""

    __tablename__ = "instrument_specs"
    __table_args__ = (
        Index(
            "uq_instrument_spec_instrument",
            "instrument_id",
            unique=True,
            sqlite_where=_KNOWN_TO_ACTIVE,
            postgresql_where=_KNOWN_TO_ACTIVE,
        ),
        Index(
            "ix_instrument_specs_public_id",
            "public_id",
            unique=True,
            sqlite_where=_KNOWN_TO_ACTIVE,
            postgresql_where=_KNOWN_TO_ACTIVE,
        ),
    )
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    public_id: Mapped[str] = mapped_column(UUIDColumn(), default=_public_id)
    instrument_id: Mapped[int] = mapped_column(
        ForeignKey(_INSTRUMENT_FK), nullable=False, index=True
    )
    tick_size: Mapped[float | None] = mapped_column(
        Float, nullable=True, comment="Minimum price increment"
    )
    lot_size: Mapped[float | None] = mapped_column(
        Float, nullable=True, comment="Minimum order size increment"
    )
    min_order_size: Mapped[float | None] = mapped_column(
        Float, nullable=True, comment="Minimum order size"
    )
    max_order_size: Mapped[float | None] = mapped_column(
        Float, nullable=True, comment="Maximum order size"
    )
    cost_decimals: Mapped[int | None] = mapped_column(
        Integer, nullable=True, comment="Decimal precision for cost"
    )
    qty_decimals: Mapped[int | None] = mapped_column(
        Integer, nullable=True, comment="Decimal precision for quantity"
    )
    margin_initial: Mapped[float | None] = mapped_column(
        Float, nullable=True, comment="Initial margin percentage"
    )
    position_limit_long: Mapped[int | None] = mapped_column(
        Integer, nullable=True, comment="Long position limit"
    )
    position_limit_short: Mapped[int | None] = mapped_column(
        Integer, nullable=True, comment="Short position limit"
    )
    status: Mapped[str | None] = mapped_column(
        String(20), nullable=True, comment="Trading status (e.g., online, offline)"
    )
    timestamp: Mapped[datetime] = mapped_column(TZDateTime())
    known_to: Mapped[datetime] = mapped_column(TZDateTime(), default=KNOWN_TO_MAX)


class MarketSnapshot(Base):
    """SQLAlchemy model for real-time market data snapshots."""

    __tablename__ = "market_snapshots"
    __table_args__ = (
        Index("ix_market_snapshots_symbol_ts", "symbol", "timestamp"),
        Index("ix_market_snapshots_exchange_symbol_ts", "exchange", "symbol", "timestamp"),
        Index(
            "ix_market_snapshots_public_id",
            "public_id",
            unique=True,
            sqlite_where=_KNOWN_TO_ACTIVE,
            postgresql_where=_KNOWN_TO_ACTIVE,
        ),
    )
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    public_id: Mapped[str] = mapped_column(UUIDColumn(), default=_public_id)
    exchange: Mapped[str] = mapped_column(
        String(20),
        nullable=False,
        server_default="kraken",
        comment="Exchange name (kraken, zonda, walutomat)",
    )
    symbol: Mapped[str] = mapped_column(
        String(20), index=True, comment="Trading pair symbol (e.g., BTC-USD)"
    )
    bid: Mapped[float | None] = mapped_column(Float, nullable=True, comment="Best bid price")
    bid_volume: Mapped[float | None] = mapped_column(
        Float, nullable=True, comment="Volume at best bid"
    )
    ask: Mapped[float | None] = mapped_column(Float, nullable=True, comment="Best ask price")
    ask_volume: Mapped[float | None] = mapped_column(
        Float, nullable=True, comment="Volume at best ask"
    )
    last_price: Mapped[float | None] = mapped_column(
        Float, nullable=True, comment="Last trade price"
    )
    volume_24h: Mapped[float | None] = mapped_column(
        Float, nullable=True, comment="24-hour trading volume"
    )
    vwap_24h: Mapped[float | None] = mapped_column(
        Float, nullable=True, comment="24-hour volume-weighted average price"
    )
    low_24h: Mapped[float | None] = mapped_column(Float, nullable=True, comment="24-hour low price")
    high_24h: Mapped[float | None] = mapped_column(
        Float, nullable=True, comment="24-hour high price"
    )
    change_24h: Mapped[float | None] = mapped_column(
        Float, nullable=True, comment="24-hour price change percentage"
    )
    spread: Mapped[float | None] = mapped_column(
        Float, nullable=True, comment="Current spread (ask - bid)"
    )
    spread_pct: Mapped[float | None] = mapped_column(
        Float, nullable=True, comment="Spread as percentage of mid price"
    )
    timestamp: Mapped[datetime] = mapped_column(
        TZDateTime(), index=True, comment="Snapshot timestamp"
    )
    known_to: Mapped[datetime] = mapped_column(TZDateTime(), default=KNOWN_TO_MAX)
