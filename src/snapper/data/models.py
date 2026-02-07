"""SQLAlchemy ORM models for Snapper persistence."""

from datetime import UTC
from datetime import datetime
from typing import Any

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
from sqlalchemy.engine.interfaces import Dialect
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy.orm import relationship
from sqlalchemy.types import TypeDecorator


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


__all__ = [
    "Base",
    "Instrument",
    "Candle",
    "Trade",
    "OrderRecord",
    "Execution",
    "Position",
    "SignalEvent",
    "User",
    "Setting",
    "SymbolCatalog",
    "SymbolAlias",
    "ProcessRun",
    "InstrumentSpec",
    "MarketSnapshot",
]


_INSTRUMENT_FK = "instruments.id"


class Base(DeclarativeBase):
    """Base class for all SQLAlchemy ORM models."""


class Instrument(Base):
    """SQLAlchemy model for tradeable financial instruments."""

    __tablename__ = "instruments"
    __table_args__ = (
        UniqueConstraint("symbol", "exchange", name="uq_instrument_symbol_exchange"),
        CheckConstraint("exchange = LOWER(exchange)", name="ck_instrument_exchange_lower"),
        Index("ix_instruments_exchange", "exchange"),
    )
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    symbol: Mapped[str] = mapped_column(
        String(32), ForeignKey("symbol_catalog.native_symbol"), index=True
    )
    exchange: Mapped[str] = mapped_column(String(20))
    base: Mapped[str] = mapped_column(String(16))
    quote: Mapped[str] = mapped_column(String(16))
    updated_at: Mapped[datetime | None] = mapped_column(TZDateTime(), nullable=True)
    candles: Mapped[list["Candle"]] = relationship(back_populates="instrument")
    trades: Mapped[list["Trade"]] = relationship(back_populates="instrument")


class Candle(Base):
    """SQLAlchemy model for OHLCV candlestick data."""

    __tablename__ = "candles"
    __table_args__ = (
        UniqueConstraint("instrument_id", "timeframe", "timestamp", name="uq_candle_itf_ts"),
        Index("ix_candle_instrument_ts", "instrument_id", "timestamp"),
    )
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    instrument_id: Mapped[int] = mapped_column(ForeignKey(_INSTRUMENT_FK), index=True)
    timestamp: Mapped[datetime] = mapped_column(TZDateTime())
    timeframe: Mapped[str] = mapped_column(String(8))
    open: Mapped[float] = mapped_column(Float)
    high: Mapped[float] = mapped_column(Float)
    low: Mapped[float] = mapped_column(Float)
    close: Mapped[float] = mapped_column(Float)
    volume: Mapped[float] = mapped_column(Float)
    vwap: Mapped[float | None] = mapped_column(Float, nullable=True)
    trades: Mapped[int | None] = mapped_column(Integer, nullable=True)
    instrument: Mapped["Instrument"] = relationship(back_populates="candles")


class Trade(Base):
    """SQLAlchemy model for individual market trades."""

    __tablename__ = "trades"
    __table_args__ = (
        UniqueConstraint("trade_id", name="uq_trade_trade_id"),
        Index("ix_trade_instrument_ts", "instrument_id", "timestamp"),
    )
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    instrument_id: Mapped[int] = mapped_column(ForeignKey(_INSTRUMENT_FK), index=True)
    timestamp: Mapped[datetime] = mapped_column(TZDateTime(), index=True)
    price: Mapped[float] = mapped_column(Float)
    size: Mapped[float] = mapped_column(Float)
    side: Mapped[str] = mapped_column(String(4))
    trade_id: Mapped[str] = mapped_column(String(64))
    instrument: Mapped["Instrument"] = relationship(back_populates="trades")


class OrderRecord(Base):
    """SQLAlchemy model for trading order records."""

    __tablename__ = "orders"
    __table_args__ = (
        Index(
            "uq_orders_client_oid",
            "instrument_id",
            "client_order_id",
            unique=True,
            sqlite_where=text("client_order_id IS NOT NULL"),
        ),
        Index(
            "uq_orders_exchange_oid",
            "instrument_id",
            "exchange_order_id",
            unique=True,
            sqlite_where=text("exchange_order_id IS NOT NULL"),
        ),
    )
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    instrument_id: Mapped[int] = mapped_column(ForeignKey(_INSTRUMENT_FK), index=True)
    client_order_id: Mapped[str | None] = mapped_column(String(64), index=True)
    exchange_order_id: Mapped[str | None] = mapped_column(String(64), index=True)
    created_at: Mapped[datetime] = mapped_column(TZDateTime())
    updated_at: Mapped[datetime | None] = mapped_column(TZDateTime())
    side: Mapped[str] = mapped_column(String(4))
    type: Mapped[str] = mapped_column(String(16))
    price: Mapped[float | None] = mapped_column(Float)
    size: Mapped[float] = mapped_column(Float)
    status: Mapped[str] = mapped_column(String(16))
    time_in_force: Mapped[str | None] = mapped_column(String(16))
    error: Mapped[str | None] = mapped_column(String(512))


class Execution(Base):
    """SQLAlchemy model for order execution fills."""

    __tablename__ = "executions"
    __table_args__ = (
        Index(
            "uq_executions_order_exec",
            "order_id",
            "exec_id",
            unique=True,
            sqlite_where=text("exec_id IS NOT NULL"),
        ),
        Index(
            "uq_executions_order_trade",
            "order_id",
            "trade_id",
            unique=True,
            sqlite_where=text("trade_id IS NOT NULL"),
        ),
    )
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    order_id: Mapped[int] = mapped_column(ForeignKey("orders.id"), index=True)
    exec_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    trade_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    timestamp: Mapped[datetime] = mapped_column(TZDateTime())
    price: Mapped[float] = mapped_column(Float)
    size: Mapped[float] = mapped_column(Float)
    fee: Mapped[float] = mapped_column(Float)
    fee_asset: Mapped[str] = mapped_column(String(16))
    order: Mapped["OrderRecord"] = relationship()


class Position(Base):
    """SQLAlchemy model for open trading positions."""

    __tablename__ = "positions"
    __table_args__ = (UniqueConstraint("instrument_id", name="uq_positions_instrument_id"),)
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    instrument_id: Mapped[int] = mapped_column(ForeignKey(_INSTRUMENT_FK), index=True)
    quantity: Mapped[float] = mapped_column(Float)
    average_price: Mapped[float] = mapped_column(Float)
    unrealized_pnl: Mapped[float] = mapped_column(Float)
    realized_pnl: Mapped[float] = mapped_column(Float)
    updated_at: Mapped[datetime] = mapped_column(TZDateTime())


class SignalEvent(Base):
    """SQLAlchemy model for trading signal events."""

    __tablename__ = "signal_events"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    instrument_id: Mapped[int] = mapped_column(ForeignKey(_INSTRUMENT_FK), index=True)
    timestamp: Mapped[datetime] = mapped_column(TZDateTime(), index=True)
    side: Mapped[str] = mapped_column(String(4))
    strength: Mapped[float] = mapped_column(Float)
    reason: Mapped[str] = mapped_column(String(256))
    strategy_name: Mapped[str | None] = mapped_column(String(64), nullable=True)
    price: Mapped[float | None] = mapped_column(Float, nullable=True)
    instrument: Mapped["Instrument"] = relationship()


class User(Base):
    """SQLAlchemy model for user accounts and authentication."""

    __tablename__ = "users"
    id: Mapped[str] = mapped_column(String(255), primary_key=True)
    username: Mapped[str] = mapped_column(String(64), unique=True, index=True)
    email: Mapped[str | None] = mapped_column(String(255), nullable=True)
    password_hash: Mapped[str] = mapped_column(String(255))
    salt: Mapped[str] = mapped_column(String(32))
    role: Mapped[str] = mapped_column(String(32))
    is_active: Mapped[bool] = mapped_column(default=True)
    created_at: Mapped[datetime] = mapped_column(TZDateTime())
    last_login: Mapped[datetime | None] = mapped_column(TZDateTime(), nullable=True)


class Setting(Base):
    """SQLAlchemy model for application configuration settings."""

    __tablename__ = "settings"
    key: Mapped[str] = mapped_column(String(64), primary_key=True)
    value: Mapped[str] = mapped_column(String(1024))
    category: Mapped[str] = mapped_column(String(32))
    description: Mapped[str | None] = mapped_column(String(256), nullable=True)
    is_encrypted: Mapped[bool] = mapped_column(Boolean, default=False)
    updated_at: Mapped[datetime] = mapped_column(TZDateTime())
    updated_by: Mapped[str | None] = mapped_column(String(64), nullable=True)


class SymbolCatalog(Base):
    """SQLAlchemy model for the native symbol registry.

    Authoritative source for native_symbol, base, and asset_type.
    Quote is authoritative for pairs (crypto/forex) where it is encoded
    in the native symbol; informational for single-asset (equity/index)
    where the exchange determines the denomination.
    """

    __tablename__ = "symbol_catalog"
    __table_args__ = (
        CheckConstraint(
            "asset_type IN ('crypto', 'forex', 'equity', 'index')",
            name="ck_symbol_catalog_asset_type",
        ),
        CheckConstraint(
            "asset_type IN ('equity', 'index') OR quote IS NOT NULL",
            name="ck_symbol_catalog_quote_required_for_pairs",
        ),
        Index("ix_sc_base_quote", "base", "quote"),
    )
    native_symbol: Mapped[str] = mapped_column(String(32), primary_key=True)
    base: Mapped[str] = mapped_column(String(16), nullable=False)
    quote: Mapped[str | None] = mapped_column(String(16), nullable=True)
    asset_type: Mapped[str] = mapped_column(String(16), nullable=False, server_default="crypto")
    created_at: Mapped[datetime] = mapped_column(TZDateTime())
    updated_at: Mapped[datetime] = mapped_column(TZDateTime())
    aliases: Mapped[list["SymbolAlias"]] = relationship(back_populates="catalog")


class SymbolAlias(Base):
    """SQLAlchemy model for exchange-specific symbol aliases.

    Normalized: one row per (native_symbol, exchange, channel) instead
    of one column per exchange. Replaces the old SymbolMapping table.
    """

    __tablename__ = "symbol_aliases"
    __table_args__ = (
        CheckConstraint(
            "exchange = LOWER(exchange)",
            name="ck_symbol_alias_exchange_lower",
        ),
        CheckConstraint(
            "channel IN ('ws', 'rest', 'ccxt')",
            name="ck_symbol_alias_channel",
        ),
        UniqueConstraint(
            "native_symbol",
            "exchange",
            "channel",
            name="uq_alias_native_exchange_channel",
        ),
        UniqueConstraint(
            "exchange",
            "channel",
            "exchange_symbol",
            name="uq_alias_exchange_channel_symbol",
        ),
    )
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    native_symbol: Mapped[str] = mapped_column(
        String(32),
        ForeignKey("symbol_catalog.native_symbol"),
        nullable=False,
        index=True,
    )
    exchange: Mapped[str] = mapped_column(String(20), nullable=False)
    channel: Mapped[str] = mapped_column(String(10), nullable=False)
    exchange_symbol: Mapped[str] = mapped_column(String(40), nullable=False)
    created_at: Mapped[datetime] = mapped_column(TZDateTime())
    updated_at: Mapped[datetime] = mapped_column(TZDateTime())
    catalog: Mapped["SymbolCatalog"] = relationship(back_populates="aliases")


class ProcessRun(Base):
    """SQLAlchemy model for background process execution records."""

    __tablename__ = "process_runs"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    run_id: Mapped[str] = mapped_column(String(36), unique=True, index=True)
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


class InstrumentSpec(Base):
    """SQLAlchemy model for instrument trading specifications."""

    __tablename__ = "instrument_specs"
    __table_args__ = (UniqueConstraint("instrument_id", name="uq_instrument_spec_instrument"),)
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
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
    updated_at: Mapped[datetime] = mapped_column(TZDateTime())


class MarketSnapshot(Base):
    """SQLAlchemy model for real-time market data snapshots."""

    __tablename__ = "market_snapshots"
    __table_args__ = (
        Index("ix_market_snapshots_symbol_updated", "symbol", "updated_at"),
        Index("ix_market_snapshots_exchange_symbol_updated", "exchange", "symbol", "updated_at"),
    )
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
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
    updated_at: Mapped[datetime] = mapped_column(
        TZDateTime(), index=True, comment="Snapshot timestamp"
    )
