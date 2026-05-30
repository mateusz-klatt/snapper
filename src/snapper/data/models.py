"""SQLAlchemy ORM models for Snapper persistence."""

from datetime import UTC
from datetime import datetime
from typing import Any
from uuid import UUID
from uuid import uuid7

from sqlalchemy import JSON
from sqlalchemy import BigInteger
from sqlalchemy import Boolean
from sqlalchemy import CheckConstraint
from sqlalchemy import DateTime
from sqlalchemy import Float
from sqlalchemy import Index
from sqlalchemy import Integer
from sqlalchemy import Numeric
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
from snapper.core.json_types import JsonValue
from snapper.core.types import AliasChannelEnum
from snapper.core.types import AssetTypeEnum
from snapper.core.types import RelationshipTypeEnum

KNOWN_TO_MAX = datetime(9999, 12, 31, 23, 59, 59, tzinfo=UTC)


def _public_id() -> str:
    """Generate a new UUID7 string for use as a public identifier."""
    return str(uuid7())


class TZDateTime(TypeDecorator[datetime]):
    """Timezone-aware datetime column, dialect-native at rest.

    Postgres (production): the column is ``TIMESTAMP WITH TIME ZONE``
    (``TIMESTAMPTZ``) — PG stores the instant natively and asyncpg
    accepts/returns tz-aware ``datetime`` values directly. No
    application-level workaround.

    SQLite (tests + dev): the column is ``DateTime`` (no timezone).
    SQLite has no native tz type, so the decorator strips ``tzinfo``
    on write (after converting to UTC) and re-attaches UTC on read.
    Storage contract: every SQLite row is logically UTC by
    construction.

    Either way the application sees a tz-aware ``datetime`` on both
    sides of the roundtrip. The dialect split keeps Postgres on its
    native fast path instead of forcing the SQLite-style strip on
    production — earlier revisions did the strip everywhere, which
    landed a ``TIMESTAMP WITHOUT TIME ZONE`` schema in PG and lost
    the timezone information the production backend was designed to
    preserve (operator decision 2026-05-16: "Postgres = production,
    natywne typy gdzie się da, SQLite konwertuje").
    """

    impl = DateTime(timezone=True)
    cache_ok = True

    def load_dialect_impl(self, dialect: Dialect) -> types.TypeEngine[Any]:
        if dialect.name == "postgresql":
            return dialect.type_descriptor(DateTime(timezone=True))
        return dialect.type_descriptor(DateTime(timezone=False))

    def process_bind_param(self, value: datetime | None, dialect: Dialect) -> datetime | None:
        if value is None:
            return None
        if value.tzinfo is None:
            raise ValueError(
                f"Cannot save naive datetime {value} to database. "
                "All datetime values must have timezone info."
            )
        utc_value = value.astimezone(UTC)
        if dialect.name == "postgresql":
            return utc_value
        return utc_value.replace(tzinfo=None)

    def process_result_value(self, value: datetime | None, dialect: Dialect) -> datetime | None:
        if value is not None and value.tzinfo is None:
            value = value.replace(tzinfo=UTC)
        return value


class UUIDColumn(TypeDecorator[str]):
    """UUID storage: native ``UUID`` on PostgreSQL, ``String(36)`` on SQLite.

    Production runs on Postgres where the native ``UUID`` type is
    8 bytes vs the 36 bytes a ``VARCHAR`` representation costs, plus
    PG indexes UUIDs more efficiently. SQLite has no native UUID
    type, so the decorator falls back to ``String(36)``; values
    round-trip as ``str`` on both backends so application code is
    dialect-agnostic.

    The migration file 0001_init.py declares UUID columns with the
    same ``with_variant(postgresql.UUID, "postgresql")`` mapping so
    the schema agrees with the decorator on both backends — earlier
    revisions had the columns as plain ``String(36)`` which produced
    ``operator does not exist: character varying = uuid`` at query
    compile time.
    """

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
    "UnderlyingAsset",
    "InstrumentUnderlyingMapping",
    "MarketSnapshot",
    "Control",
    "Telemetry",
    "TradeCommand",
    "VenueEvent",
    "TradeProjectionCheckpoint",
    "FundingRate",
    "AccrualLedger",
    "Wallet",
    "WalletCredential",
    "Operator",
    "UserOperatorMembership",
    "WalletOperatorScopeGrant",
    "InstrumentOrderCapability",
    "VenueFeeSchedule",
    "ExecutionPlan",
    "ExecutionPlanCheckpoint",
    "ExecutionPlanDecision",
    "PositionCycle",
    "UserTradingCaps",
    "UserActiveToken",
    "NotificationDevice",
    "DeviceAlertPref",
    "UserAlertDefault",
    "AlertEvent",
    "AlertDelivery",
]


_CK_EXCHANGE_LOWER = "exchange = LOWER(exchange)"
_KNOWN_TO_ACTIVE_PG = text("known_to = '9999-12-31T23:59:59+00:00'")
_KNOWN_TO_ACTIVE_SQLITE = text("known_to = '9999-12-31 23:59:59.000000'")
_NOTIFICATION_DEVICE_ACTIVE_PG = text(
    "known_to = '9999-12-31T23:59:59+00:00' AND token_status = 'active'"
)
_NOTIFICATION_DEVICE_ACTIVE_SQLITE = text(
    "known_to = '9999-12-31 23:59:59.000000' AND token_status = 'active'"
)
_CK_NOTIFICATION_DEVICE_TOKEN_STATUS = (
    "token_status IN ('active', 'unregistered', 'user_unregistered')"
)


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
    session_id: Mapped[str] = mapped_column(UUIDColumn())
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
    exchange: Mapped[str] = mapped_column(String(32))
    requires_ai_review: Mapped[bool] = mapped_column(
        Boolean, nullable=False, server_default=text("false")
    )
    """Instrument-level default for AI review opt-in.

    Strategies with ``ai_review_policy="instrument_default"`` consult
    AI delegate before submitting trades on instruments where this
    flag is TRUE. Set via curation for less-liquid instruments (low
    volume, wide spread) where AI judgment justifies +5-30s latency.
    Default FALSE — opt-in only.
    """


class Candle(TemporalMixin, Base):
    """SQLAlchemy model for OHLCV candlestick data.

    PK ``id`` is overridden to ``BigInteger`` on PostgreSQL and kept as
    ``Integer`` on SQLite (where INTEGER PRIMARY KEY is already 64-bit
    rowid). High-write tables would otherwise overflow the INT4
    sequence at production write rates — see
    ``proprietary/plans/plan_2026_05_19_bigint_pk_high_write_tables.md``.
    """

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
    id: Mapped[int] = mapped_column(
        BigInteger().with_variant(Integer, "sqlite"),
        primary_key=True,
        autoincrement=True,
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
    """SQLAlchemy model for real-time price tick snapshots.

    PK ``id`` is overridden to ``BigInteger`` on PostgreSQL. This is the
    most volume-exposed table — production write rate is observed at
    sustained 1500/s burst, with the INT4 sequence at 18% headroom
    (2026-05-19). Without this override the sequence overflows in ~13 days.
    """

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
    id: Mapped[int] = mapped_column(
        BigInteger().with_variant(Integer, "sqlite"),
        primary_key=True,
        autoincrement=True,
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
        Index("ix_trades_timestamp", "timestamp"),
        Index(
            "ix_trades_public_id",
            "public_id",
            unique=True,
            sqlite_where=_KNOWN_TO_ACTIVE_SQLITE,
            postgresql_where=_KNOWN_TO_ACTIVE_PG,
        ),
    )
    id: Mapped[int] = mapped_column(
        BigInteger().with_variant(Integer, "sqlite"),
        primary_key=True,
        autoincrement=True,
    )
    instrument_public_id: Mapped[str] = mapped_column(UUIDColumn())
    price: Mapped[float] = mapped_column(Float)
    size: Mapped[float] = mapped_column(Float)
    side: Mapped[str] = mapped_column(String(4))
    trade_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    executed_at: Mapped[datetime | None] = mapped_column(TZDateTime(), nullable=True)


class Order(TemporalMixin, Base):
    """SQLAlchemy model for trading order records.

    PK ``id`` is overridden to ``BigInteger`` on PostgreSQL for
    consistency with the other high-write tables — see
    ``proprietary/plans/plan_2026_05_19_bigint_pk_high_write_tables.md``.
    """

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
        Index(
            "ix_orders_wallet_public_id_created_at",
            "wallet_public_id",
            "created_at",
            sqlite_where=_KNOWN_TO_ACTIVE_SQLITE,
            postgresql_where=_KNOWN_TO_ACTIVE_PG,
        ),
        Index(
            "ix_orders_status_created_at",
            "status",
            "created_at",
            sqlite_where=_KNOWN_TO_ACTIVE_SQLITE,
            postgresql_where=_KNOWN_TO_ACTIVE_PG,
        ),
    )
    id: Mapped[int] = mapped_column(
        BigInteger().with_variant(Integer, "sqlite"),
        primary_key=True,
        autoincrement=True,
    )
    instrument_public_id: Mapped[str] = mapped_column(UUIDColumn(), index=True)
    mode: Mapped[str] = mapped_column(String(8), default="live", server_default="live")
    wallet_public_id: Mapped[str] = mapped_column(UUIDColumn(), nullable=False)
    operator_public_id: Mapped[str | None] = mapped_column(UUIDColumn(), nullable=True)
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
    leverage: Mapped[int | None] = mapped_column(Integer, nullable=True)
    reduce_only: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=False, server_default=text("false")
    )
    plan_public_id: Mapped[str | None] = mapped_column(UUIDColumn(), nullable=True, index=True)


class Execution(TemporalMixin, Base):
    """SQLAlchemy model for order execution fills.

    PK ``id`` is overridden to ``BigInteger`` on PostgreSQL for
    consistency with the other high-write tables — see
    ``proprietary/plans/plan_2026_05_19_bigint_pk_high_write_tables.md``.
    """

    __tablename__ = "executions"
    __table_args__ = (
        Index(
            "uq_executions_order_exec",
            "order_public_id",
            "exec_id",
            unique=True,
            sqlite_where=text("exec_id IS NOT NULL"),
            postgresql_where=text("exec_id IS NOT NULL"),
        ),
        Index(
            "uq_executions_order_trade",
            "order_public_id",
            "trade_id",
            unique=True,
            sqlite_where=text("trade_id IS NOT NULL"),
            postgresql_where=text("trade_id IS NOT NULL"),
        ),
        Index(
            "ix_executions_public_id",
            "public_id",
            unique=True,
            sqlite_where=_KNOWN_TO_ACTIVE_SQLITE,
            postgresql_where=_KNOWN_TO_ACTIVE_PG,
        ),
    )
    id: Mapped[int] = mapped_column(
        BigInteger().with_variant(Integer, "sqlite"),
        primary_key=True,
        autoincrement=True,
    )
    order_public_id: Mapped[str] = mapped_column(UUIDColumn(), index=True)
    wallet_public_id: Mapped[str] = mapped_column(UUIDColumn(), nullable=False)
    operator_public_id: Mapped[str | None] = mapped_column(UUIDColumn(), nullable=True)
    exec_id: Mapped[str | None] = mapped_column(String(64))
    trade_id: Mapped[str | None] = mapped_column(String(64))
    side: Mapped[str] = mapped_column(String(4))
    status: Mapped[str] = mapped_column(String(16))
    price: Mapped[float] = mapped_column(Float)
    size: Mapped[float] = mapped_column(Float)
    fee: Mapped[float] = mapped_column(Float)
    fee_asset: Mapped[str] = mapped_column(String(16))
    executed_at: Mapped[datetime | None] = mapped_column(TZDateTime())
    liquidity_role: Mapped[str] = mapped_column(String(16), default="unknown")


class Position(TemporalMixin, Base):
    """SQLAlchemy model for open trading positions."""

    __tablename__ = "positions"
    __table_args__ = (
        Index(
            "uq_positions_instrument_public_id",
            "instrument_public_id",
            "mode",
            "wallet_public_id",
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
    wallet_public_id: Mapped[str] = mapped_column(UUIDColumn(), nullable=False)
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
    wallet_public_id: Mapped[str] = mapped_column(UUIDColumn(), nullable=False)
    operator_public_id: Mapped[str | None] = mapped_column(UUIDColumn(), nullable=True)
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
    default_language: Mapped[str | None] = mapped_column(String(20), nullable=True)
    created_at: Mapped[datetime] = mapped_column(TZDateTime())
    created_by_user_public_id: Mapped[str | None] = mapped_column(
        UUIDColumn(), nullable=True, index=True
    )


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
    value: Mapped[str] = mapped_column(Text)
    category: Mapped[str] = mapped_column(String(32))
    description: Mapped[str | None] = mapped_column(String(1024))
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
            f"asset_type IN ({', '.join(repr(v.value) for v in AssetTypeEnum if v in (AssetTypeEnum.EQUITY, AssetTypeEnum.INDEX, AssetTypeEnum.COMMODITY, AssetTypeEnum.YIELD))}) OR quote IS NOT NULL",
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
    exchange: Mapped[str] = mapped_column(String(32))
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
            postgresql_where=text("can_trade = true"),
        ),
        Index(
            "ix_sec_exchange_md",
            "exchange",
            "can_market_data",
            sqlite_where=text("can_market_data = 1"),
            postgresql_where=text("can_market_data = true"),
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
    exchange: Mapped[str] = mapped_column(String(32))
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
    wallet_public_id: Mapped[str | None] = mapped_column(UUIDColumn(), nullable=True)
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
        Index(
            "ix_instrument_specs_expiry",
            "expiry_at",
            sqlite_where=_KNOWN_TO_ACTIVE_SQLITE,
            postgresql_where=_KNOWN_TO_ACTIVE_PG,
        ),
        CheckConstraint(
            "instrument_kind IN ('spot', 'perpetual', 'future', 'etf', 'option') "
            "OR instrument_kind IS NULL",
            name="ck_instrument_specs_kind",
        ),
        CheckConstraint(
            "funding_type IS NULL OR funding_type IN "
            "('spot_margin_rollover', 'perpetual_funding')",
            name="ck_instrument_specs_funding_type",
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
    expiry_at: Mapped[datetime | None] = mapped_column(
        TZDateTime(), comment="Contract expiry timestamp (UTC); NULL for spots/perpetuals"
    )
    instrument_kind: Mapped[str | None] = mapped_column(
        String(16), comment="Product type: spot, perpetual, future, etf, option"
    )
    funding_type: Mapped[str | None] = mapped_column(
        String(32),
        comment="Funding model: spot_margin_rollover, perpetual_funding, or NULL",
    )
    funding_frequency_hours: Mapped[int | None] = mapped_column(
        Integer, comment="Hours between funding/rollover boundaries"
    )
    rollover_rate_long: Mapped[float | None] = mapped_column(
        Float, comment="Spot margin rollover fee per boundary for longs"
    )
    rollover_rate_short: Mapped[float | None] = mapped_column(
        Float, comment="Spot margin rollover fee per boundary for shorts"
    )
    max_funding_rate: Mapped[float | None] = mapped_column(
        Float, comment="Per-boundary cap on perpetual funding rate magnitude"
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
    id: Mapped[int] = mapped_column(
        BigInteger().with_variant(Integer, "sqlite"),
        primary_key=True,
        autoincrement=True,
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
    client_session_id: Mapped[str | None] = mapped_column(UUIDColumn())
    client_public_id: Mapped[str | None] = mapped_column(UUIDColumn())


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
        Index("ix_telemetry_timestamp", "timestamp"),
    )
    id: Mapped[int] = mapped_column(
        BigInteger().with_variant(Integer, "sqlite"),
        primary_key=True,
        autoincrement=True,
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
        Index(
            "ix_trade_commands_outbox_pagination",
            "status",
            "created_at",
            "id",
        ),
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
    shard_key: Mapped[str] = mapped_column(String(256))
    wallet_public_id: Mapped[str] = mapped_column(UUIDColumn(), nullable=False)
    operator_public_id: Mapped[str | None] = mapped_column(UUIDColumn(), nullable=True)
    user_public_id: Mapped[str | None] = mapped_column(UUIDColumn(), nullable=True)
    exchange: Mapped[str] = mapped_column(String(32))
    instrument: Mapped[str] = mapped_column(String(64))
    mode: Mapped[str] = mapped_column(String(8))
    strategy_id: Mapped[str] = mapped_column(String(64))
    client_order_id: Mapped[str] = mapped_column(String(64))
    venue_client_id: Mapped[str] = mapped_column(String(64))
    idempotency_key: Mapped[str | None] = mapped_column(String(128))
    side: Mapped[str] = mapped_column(String(4))
    order_type: Mapped[str] = mapped_column(String(16))
    quantity: Mapped[float] = mapped_column(Float)
    price: Mapped[float | None] = mapped_column(Float)
    leverage: Mapped[int | None] = mapped_column(Integer)
    reduce_only: Mapped[bool] = mapped_column(Boolean, default=False, server_default=text("false"))
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
    plan_public_id: Mapped[str | None] = mapped_column(UUIDColumn(), nullable=True, index=True)
    source_surface: Mapped[str] = mapped_column(String(20), nullable=False, server_default="rest")


class VenueEvent(TemporalMixin, Base):
    """Append-only log of raw venue observations.

    Created by ExchangeExecutorService when venue state changes are
    detected (WS stream for Kraken, HTTP polling for Walutomat,
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
    shard_key: Mapped[str] = mapped_column(String(256))
    wallet_public_id: Mapped[str] = mapped_column(UUIDColumn(), nullable=False)
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
    liquidity_role: Mapped[str] = mapped_column(String(16), default="unknown")


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
    shard_key: Mapped[str] = mapped_column(String(256))
    wallet_public_id: Mapped[str] = mapped_column(UUIDColumn(), nullable=False)
    operator_public_id: Mapped[str | None] = mapped_column(UUIDColumn(), nullable=True)
    position_qty: Mapped[float] = mapped_column(Float, default=0.0, server_default="0")
    entry_price: Mapped[float | None] = mapped_column(Float)
    position_opened_at: Mapped[datetime | None] = mapped_column(TZDateTime())
    cash: Mapped[float] = mapped_column(Float)
    peak_equity: Mapped[float] = mapped_column(Float)
    realized_pnl: Mapped[float] = mapped_column(Float, default=0.0, server_default="0")
    turnover: Mapped[float] = mapped_column(Float, default=0.0, server_default="0")
    last_venue_event_id: Mapped[int | None] = mapped_column(Integer)
    last_venue_event_at: Mapped[datetime | None] = mapped_column(TZDateTime())
    open_command_ids: Mapped[str | None] = mapped_column(Text)
    seen_exec_ids: Mapped[str] = mapped_column(Text, default="[]", server_default="[]")
    checkpoint_at: Mapped[datetime] = mapped_column(TZDateTime())


class UnderlyingAsset(TemporalMixin, Base):
    """Canonical underlying asset linking related instruments across exchanges.

    Examples: S&P 500 (SPX), Gold (GOLD), Bitcoin (BTC). Each underlying
    can have multiple instruments on different venues (ETFs, futures,
    perpetuals, tokenized stocks) linked via InstrumentUnderlyingMapping.
    """

    __tablename__ = "underlying_assets"
    __table_args__ = (
        Index(
            "uq_underlying_assets_active_ticker",
            "ticker",
            unique=True,
            sqlite_where=_KNOWN_TO_ACTIVE_SQLITE,
            postgresql_where=_KNOWN_TO_ACTIVE_PG,
        ),
        Index(
            "ix_underlying_assets_public_id",
            "public_id",
            unique=True,
            sqlite_where=_KNOWN_TO_ACTIVE_SQLITE,
            postgresql_where=_KNOWN_TO_ACTIVE_PG,
        ),
        CheckConstraint(
            f"asset_class IN ({', '.join(repr(v.value) for v in AssetTypeEnum)})",
            name="ck_underlying_asset_class",
        ),
    )
    name: Mapped[dict[str, JsonValue]] = mapped_column(JSON, nullable=False)
    ticker: Mapped[str] = mapped_column(String(16))
    asset_class: Mapped[str] = mapped_column(String(16))
    sector: Mapped[str | None] = mapped_column(String(32))
    description: Mapped[dict[str, JsonValue] | None] = mapped_column(JSON, nullable=True)


class InstrumentUnderlyingMapping(TemporalMixin, Base):
    """Temporal mapping between an instrument and its underlying asset.

    Separate table (not a column on Instrument) so lifespans are decoupled:
    Instrument can be SCD2-revised without affecting the mapping, because
    the mapping references instrument_public_id (stable across revisions).

    Relationship types:
        exact — direct price feed for the underlying
        derivative — futures, options (price derived from underlying)
        proxy — ETFs, tokenized stocks (tracks underlying with basis/fees)
    """

    __tablename__ = "instrument_underlying_mappings"
    __table_args__ = (
        Index(
            "uq_ium_active_instrument",
            "instrument_public_id",
            unique=True,
            sqlite_where=_KNOWN_TO_ACTIVE_SQLITE,
            postgresql_where=_KNOWN_TO_ACTIVE_PG,
        ),
        Index(
            "ix_ium_underlying",
            "underlying_public_id",
            sqlite_where=_KNOWN_TO_ACTIVE_SQLITE,
            postgresql_where=_KNOWN_TO_ACTIVE_PG,
        ),
        Index(
            "ix_ium_family",
            "underlying_public_id",
            "contract_family",
            sqlite_where=_KNOWN_TO_ACTIVE_SQLITE,
            postgresql_where=_KNOWN_TO_ACTIVE_PG,
        ),
        CheckConstraint(
            f"relationship_type IN ({', '.join(repr(v.value) for v in RelationshipTypeEnum)})",
            name="ck_ium_relationship_type",
        ),
    )
    instrument_public_id: Mapped[str] = mapped_column(UUIDColumn())
    underlying_public_id: Mapped[str] = mapped_column(UUIDColumn())
    relationship_type: Mapped[str] = mapped_column(String(16))
    contract_family: Mapped[str | None] = mapped_column(String(16))


class ContinuousContractConfig(TemporalMixin, Base):
    """Saved configuration preset for continuous contract series.

    Stores parameters for on-demand continuous contract computation.
    The series data itself is NOT persisted — it is computed per request
    by ContinuousContractBuilder. CRUD endpoints deferred to a future
    phase; ships schema/migration only.
    """

    __tablename__ = "continuous_contract_configs"
    __table_args__ = (
        Index(
            "uq_ccc_active_key",
            "underlying_public_id",
            "exchange",
            "contract_family",
            "method",
            "rollover_days_before",
            unique=True,
            sqlite_where=_KNOWN_TO_ACTIVE_SQLITE,
            postgresql_where=_KNOWN_TO_ACTIVE_PG,
        ),
        Index(
            "ix_ccc_public_id",
            "public_id",
            unique=True,
            sqlite_where=_KNOWN_TO_ACTIVE_SQLITE,
            postgresql_where=_KNOWN_TO_ACTIVE_PG,
        ),
        CheckConstraint(
            "method IN ('unadjusted', 'ratio', 'panama')",
            name="ck_ccc_method",
        ),
    )
    underlying_public_id: Mapped[str] = mapped_column(UUIDColumn())
    exchange: Mapped[str] = mapped_column(String(32))
    contract_family: Mapped[str] = mapped_column(String(16))
    method: Mapped[str] = mapped_column(String(16))
    rollover_days_before: Mapped[int] = mapped_column(Integer, default=0, server_default="0")
    label: Mapped[str | None] = mapped_column(String(64))


class FundingRate(TemporalMixin, Base):
    """Exchange-published funding or rollover rate at a point in time.

    One active row per ``(instrument_public_id, exchange, rate_type,
    direction, effective_from)`` tuple. Bitemporal: ``timestamp`` is bus
    time when the row was inserted, ``effective_from`` is the
    exchange-side time at which the rate became active. Funding accrual
    queries use ``as_of`` to look up the rate locked at position-open
    time (spot margin) or at the boundary (perpetual funding).
    """

    __tablename__ = "funding_rates"
    __table_args__ = (
        Index(
            "ix_funding_rates_unique_active",
            "instrument_public_id",
            "exchange",
            "rate_type",
            "direction",
            "effective_from",
            unique=True,
            sqlite_where=_KNOWN_TO_ACTIVE_SQLITE,
            postgresql_where=_KNOWN_TO_ACTIVE_PG,
        ),
        Index(
            "ix_funding_rates_public_id",
            "public_id",
            unique=True,
            sqlite_where=_KNOWN_TO_ACTIVE_SQLITE,
            postgresql_where=_KNOWN_TO_ACTIVE_PG,
        ),
        CheckConstraint(_CK_EXCHANGE_LOWER, name="ck_funding_rates_exchange_lower"),
        CheckConstraint(
            "rate_type IN ('spot_margin_rollover', 'perpetual_funding')",
            name="ck_funding_rates_rate_type",
        ),
        CheckConstraint(
            "direction IN ('long', 'short', 'both')",
            name="ck_funding_rates_direction",
        ),
        CheckConstraint(
            "source IN ('exchange_api', 'exchange_docs', 'manual', 'derived')",
            name="ck_funding_rates_source",
        ),
    )
    instrument_public_id: Mapped[str] = mapped_column(UUIDColumn(), index=True)
    exchange: Mapped[str] = mapped_column(String(32))
    rate_type: Mapped[str] = mapped_column(String(32))
    direction: Mapped[str] = mapped_column(String(8))
    rate: Mapped[float] = mapped_column(Float)
    notional_asset: Mapped[str] = mapped_column(String(16))
    effective_from: Mapped[datetime] = mapped_column(TZDateTime())
    source: Mapped[str] = mapped_column(String(32))


class AccrualLedger(TemporalMixin, Base):
    """Periodic funding/rollover/borrow charge applied to an open position.

    Append-only ledger of accruals materialized by the funding accrual
    coroutine. The unique key over
    ``(instrument_public_id, mode, accrual_type, accrued_at)`` provides
    idempotency on retry: a duplicate insert hits the partial unique
    index and is swallowed by the caller. Bitemporal so a corrected
    accrual can be issued without losing the prior history.
    """

    __tablename__ = "accrual_ledger"
    __table_args__ = (
        Index(
            "ix_accrual_ledger_unique_active",
            "wallet_public_id",
            "instrument_public_id",
            "mode",
            "accrual_type",
            "accrued_at",
            unique=True,
            sqlite_where=_KNOWN_TO_ACTIVE_SQLITE,
            postgresql_where=_KNOWN_TO_ACTIVE_PG,
        ),
        Index(
            "ix_accrual_ledger_public_id",
            "public_id",
            unique=True,
            sqlite_where=_KNOWN_TO_ACTIVE_SQLITE,
            postgresql_where=_KNOWN_TO_ACTIVE_PG,
        ),
        Index(
            "ix_accrual_ledger_recovery",
            "wallet_public_id",
            "instrument_public_id",
            "mode",
            "accrued_at",
        ),
        CheckConstraint(_CK_EXCHANGE_LOWER, name="ck_accrual_ledger_exchange_lower"),
        CheckConstraint(
            "mode IN ('live', 'paper', 'backtest')",
            name="ck_accrual_ledger_mode",
        ),
        CheckConstraint(
            "accrual_type IN ('funding', 'rollover', 'borrow')",
            name="ck_accrual_ledger_type",
        ),
    )
    instrument_public_id: Mapped[str] = mapped_column(UUIDColumn(), index=True)
    wallet_public_id: Mapped[str] = mapped_column(UUIDColumn(), nullable=False)
    operator_public_id: Mapped[str | None] = mapped_column(UUIDColumn(), nullable=True)
    mode: Mapped[str] = mapped_column(String(8))
    accrual_type: Mapped[str] = mapped_column(String(16))
    accrued_at: Mapped[datetime] = mapped_column(TZDateTime(), index=True)
    amount: Mapped[float] = mapped_column(Float)
    amount_asset: Mapped[str] = mapped_column(String(16))
    rate: Mapped[float] = mapped_column(Float)
    notional: Mapped[float] = mapped_column(Float)
    position_quantity_at_accrual: Mapped[float] = mapped_column(Float)
    exchange: Mapped[str] = mapped_column(String(32))


class Wallet(TemporalMixin, Base):
    """Logical container for credentials and positions on one or more exchanges.

    A wallet is a "bag of money" with its own set of API keys and its own position
    state. Examples: alice's personal Kraken wallet, the firm's shared futures
    wallet, a paper-mode sandbox. One wallet may have credentials on multiple
    exchanges (e.g., the same Kraken login covers both Kraken Spot and Kraken
    Futures via two WalletCredential rows).

    Multiple operators may share one wallet via WalletOperatorScopeGrant rows
    (trading desk pattern). One operator may hold grants on multiple wallets
    (own personal + delegated firm wallet). The Wallet entity is distinct from
    the Operator entity (multi-tenant foundation).
    """

    __tablename__ = "wallets"
    __table_args__ = (
        Index(
            "ix_wallets_public_id",
            "public_id",
            unique=True,
            sqlite_where=_KNOWN_TO_ACTIVE_SQLITE,
            postgresql_where=_KNOWN_TO_ACTIVE_PG,
        ),
        Index(
            "ix_wallets_label_is_paper_active",
            "label",
            "is_paper",
            unique=True,
            sqlite_where=_KNOWN_TO_ACTIVE_SQLITE,
            postgresql_where=_KNOWN_TO_ACTIVE_PG,
        ),
    )
    label: Mapped[str] = mapped_column(String(128))
    description: Mapped[str | None] = mapped_column(String(512), nullable=True)
    is_paper: Mapped[bool] = mapped_column(Boolean, default=False, server_default=text("false"))


class WalletCredential(TemporalMixin, Base):
    """Per-exchange encrypted credential for a wallet.

    The encrypted_payload stores a JSON envelope whose contents depend on
    credential_type:
        api_key_secret  -> {"api_key": "...", "api_secret": "..."}
        rsa_pem         -> {"api_key": "...", "private_key_pem": "..."}
        oauth           -> {"client_id": "...", "client_secret": "...", "refresh_token": "..."}
        paper           -> {"initial_balance": 10000.0}

    The payload is encrypted with the same master-password-derived
    Fernet key used by ``SettingsEncryptionService`` for sensitive
    settings. Rotation semantics: rotate the master password →
    re-encrypt every ``wallet_credentials`` row in lockstep →
    restart. There is no per-row ``encryption_key_id``: the master
    password is the single source of truth. A multi-key overlap
    window (envelope encryption with KMS-style data-encryption keys)
    is intentionally deferred until a real use case appears.

    Encryption reuses the existing Setting encryption infrastructure (master
    key from env var, encrypted at rest). Credentials are pull-on-startup only
    and MUST NOT be broadcast on the system.settings ZMQ topic. Rotation
    requires a process restart of the affected executor instance.
    """

    __tablename__ = "wallet_credentials"
    __table_args__ = (
        Index(
            "ix_wallet_credentials_public_id",
            "public_id",
            unique=True,
            sqlite_where=_KNOWN_TO_ACTIVE_SQLITE,
            postgresql_where=_KNOWN_TO_ACTIVE_PG,
        ),
        Index(
            "ix_wallet_credentials_wallet_exchange_active",
            "wallet_public_id",
            "exchange",
            unique=True,
            sqlite_where=_KNOWN_TO_ACTIVE_SQLITE,
            postgresql_where=_KNOWN_TO_ACTIVE_PG,
        ),
        Index("ix_wallet_credentials_exchange", "exchange"),
        CheckConstraint(_CK_EXCHANGE_LOWER, name="ck_wallet_credentials_exchange_lower"),
        CheckConstraint(
            "credential_type IN ('api_key_secret', 'rsa_pem', 'oauth', 'paper')",
            name="ck_wallet_credentials_type",
        ),
    )
    wallet_public_id: Mapped[str] = mapped_column(UUIDColumn())
    exchange: Mapped[str] = mapped_column(String(32))
    credential_type: Mapped[str] = mapped_column(String(32))
    encrypted_payload: Mapped[str] = mapped_column(Text)
    label: Mapped[str | None] = mapped_column(String(128), nullable=True)


class Operator(TemporalMixin, Base):
    """A trading identity, distinct from User (login identity).

    A User logs in; an Operator places trades. Many users may act as the same
    operator (delegate access during vacation cover) and one user may act as
    many operators (personal seat + firm seat). The user-to-operator mapping
    is M:N via UserOperatorMembership.

    The Operator is distinct from Wallet: an operator does not
    own a wallet, it is granted scope on one or more wallets via
    WalletOperatorScopeGrant. Multiple operators may share one wallet
    (trading desk pattern).
    """

    __tablename__ = "operators"
    __table_args__ = (
        Index(
            "ix_operators_public_id",
            "public_id",
            unique=True,
            sqlite_where=_KNOWN_TO_ACTIVE_SQLITE,
            postgresql_where=_KNOWN_TO_ACTIVE_PG,
        ),
        Index(
            "ix_operators_label_active",
            "label",
            unique=True,
            sqlite_where=_KNOWN_TO_ACTIVE_SQLITE,
            postgresql_where=_KNOWN_TO_ACTIVE_PG,
        ),
    )
    label: Mapped[str] = mapped_column(String(128))
    description: Mapped[str | None] = mapped_column(String(512), nullable=True)


class UserOperatorMembership(TemporalMixin, Base):
    """Which users may act AS which operators.

    M:N relationship between User and Operator. The is_primary flag identifies
    the user's default operator (the one selected when the user logs in
    without explicitly choosing). At most one primary operator per user.
    """

    __tablename__ = "user_operator_memberships"
    __table_args__ = (
        Index(
            "ix_user_operator_memberships_public_id",
            "public_id",
            unique=True,
            sqlite_where=_KNOWN_TO_ACTIVE_SQLITE,
            postgresql_where=_KNOWN_TO_ACTIVE_PG,
        ),
        Index(
            "ix_user_operator_memberships_unique_active",
            "user_public_id",
            "operator_public_id",
            unique=True,
            sqlite_where=_KNOWN_TO_ACTIVE_SQLITE,
            postgresql_where=_KNOWN_TO_ACTIVE_PG,
        ),
        Index(
            "ix_user_operator_memberships_primary_unique_active",
            "user_public_id",
            unique=True,
            sqlite_where=text("is_primary = 1 AND known_to = '9999-12-31 23:59:59.000000'"),
            postgresql_where=text("is_primary = TRUE AND known_to = '9999-12-31T23:59:59+00:00'"),
        ),
        Index("ix_user_operator_memberships_user", "user_public_id"),
        Index("ix_user_operator_memberships_operator", "operator_public_id"),
    )
    user_public_id: Mapped[str] = mapped_column(UUIDColumn())
    operator_public_id: Mapped[str] = mapped_column(UUIDColumn())
    is_primary: Mapped[bool] = mapped_column(Boolean, default=False, server_default=text("false"))


class WalletOperatorScopeGrant(TemporalMixin, Base):
    """Grant: operator X may trade scope Y on wallet Z.

    All grants are instrument-exclusive: at most ONE operator
    may hold an active grant on any (wallet, instrument) tuple at any time.
    There is no lock_mode column. Cooperative grants (multiple operators
    sharing the same instrument on the same wallet) are deferred to a
    future plan.

    The CHECK constraint enforces scope_kind XOR: exactly one of
    underlying_public_id / instrument_public_id is non-NULL. The two partial
    unique indexes catch same-scope duplicates as defense in depth, while the
    repository layer uses an advisory lock + cross-scope overlap check
    (instrument_underlying_mappings expansion) to prevent cross-scope races
    that the partial indexes cannot detect.
    """

    __tablename__ = "wallet_operator_scope_grants"
    __table_args__ = (
        Index(
            "ix_scope_grants_public_id",
            "public_id",
            unique=True,
            sqlite_where=_KNOWN_TO_ACTIVE_SQLITE,
            postgresql_where=_KNOWN_TO_ACTIVE_PG,
        ),
        Index(
            "ix_scope_grants_instrument_exclusive_active",
            "wallet_public_id",
            "instrument_public_id",
            unique=True,
            sqlite_where=text(
                "instrument_public_id IS NOT NULL AND known_to = '9999-12-31 23:59:59.000000'"
            ),
            postgresql_where=text(
                "instrument_public_id IS NOT NULL AND known_to = '9999-12-31T23:59:59+00:00'"
            ),
        ),
        Index(
            "ix_scope_grants_underlying_exclusive_active",
            "wallet_public_id",
            "underlying_public_id",
            unique=True,
            sqlite_where=text(
                "underlying_public_id IS NOT NULL AND known_to = '9999-12-31 23:59:59.000000'"
            ),
            postgresql_where=text(
                "underlying_public_id IS NOT NULL AND known_to = '9999-12-31T23:59:59+00:00'"
            ),
        ),
        Index("ix_scope_grants_operator", "operator_public_id"),
        Index("ix_scope_grants_wallet", "wallet_public_id"),
        CheckConstraint(
            "scope_kind IN ('underlying', 'instrument')",
            name="ck_scope_grants_scope_kind",
        ),
        CheckConstraint(
            "(scope_kind = 'underlying' AND underlying_public_id IS NOT NULL "
            "AND instrument_public_id IS NULL) "
            "OR (scope_kind = 'instrument' AND instrument_public_id IS NOT NULL "
            "AND underlying_public_id IS NULL)",
            name="ck_scope_grants_scope_kind_xor",
        ),
    )
    operator_public_id: Mapped[str] = mapped_column(UUIDColumn())
    wallet_public_id: Mapped[str] = mapped_column(UUIDColumn())
    granted_by_user_public_id: Mapped[str] = mapped_column(UUIDColumn())
    scope_kind: Mapped[str] = mapped_column(String(16))
    underlying_public_id: Mapped[str | None] = mapped_column(UUIDColumn(), nullable=True)
    instrument_public_id: Mapped[str | None] = mapped_column(UUIDColumn(), nullable=True)
    note: Mapped[str | None] = mapped_column(String(512), nullable=True)


class InstrumentOrderCapability(TemporalMixin, Base):
    """Per-instrument order capability matrix for execution plan evaluators.

    Describes which order types, features, and limits are available
    for a given instrument on a given exchange. Seeded by symbol
    updaters and reference data; consumed by PlanExecutorService to
    gate plan creation and evaluator behavior.
    """

    __tablename__ = "instrument_order_capabilities"
    __table_args__ = (
        Index(
            "ix_ioc_instrument_exchange_active",
            "instrument_public_id",
            "exchange",
            unique=True,
            sqlite_where=_KNOWN_TO_ACTIVE_SQLITE,
            postgresql_where=_KNOWN_TO_ACTIVE_PG,
        ),
        Index(
            "ix_ioc_public_id",
            "public_id",
            unique=True,
            sqlite_where=_KNOWN_TO_ACTIVE_SQLITE,
            postgresql_where=_KNOWN_TO_ACTIVE_PG,
        ),
        CheckConstraint(_CK_EXCHANGE_LOWER, name="ck_ioc_exchange_lower"),
        CheckConstraint(
            "top_of_book_quality IN ('realtime', 'polled', 'thin', 'unknown')",
            name="ck_ioc_tob_quality",
        ),
    )
    instrument_public_id: Mapped[str] = mapped_column(UUIDColumn(), index=True)
    exchange: Mapped[str] = mapped_column(String(32), index=True)
    supported_order_types: Mapped[list[str]] = mapped_column(JSON)
    supports_post_only: Mapped[bool] = mapped_column(Boolean, default=False)
    supports_reduce_only: Mapped[bool] = mapped_column(Boolean, default=False)
    supports_amend_in_place: Mapped[bool] = mapped_column(Boolean, default=False)
    supports_native_stop_loss: Mapped[bool] = mapped_column(Boolean, default=False)
    supports_native_take_profit: Mapped[bool] = mapped_column(Boolean, default=False)
    supports_trailing_stop_client_side: Mapped[bool] = mapped_column(Boolean, default=True)
    supports_market_making: Mapped[bool] = mapped_column(Boolean, default=False)
    supports_short_selling: Mapped[bool] = mapped_column(Boolean, default=False)
    supports_leverage: Mapped[bool] = mapped_column(Boolean, default=False)
    max_leverage_long: Mapped[float] = mapped_column(Float, default=1.0)
    max_leverage_short: Mapped[float] = mapped_column(Float, default=0.0)
    min_notional: Mapped[float | None] = mapped_column(Float, nullable=True)
    max_order_size: Mapped[float | None] = mapped_column(Float, nullable=True)
    top_of_book_quality: Mapped[str] = mapped_column(String(16), default="unknown")


class VenueFeeSchedule(TemporalMixin, Base):
    """Exchange fee schedule for maker/taker cost estimation.

    Used by market-making and peg evaluators to estimate profitability
    before placing orders. Tiers are seeded from public exchange fee
    pages; user-specific tier overrides are applied via admin settings.
    """

    __tablename__ = "venue_fee_schedules"
    __table_args__ = (
        Index(
            "ix_vfs_exchange_tier_active",
            "exchange",
            "fee_tier",
            unique=True,
            sqlite_where=_KNOWN_TO_ACTIVE_SQLITE,
            postgresql_where=_KNOWN_TO_ACTIVE_PG,
        ),
        Index(
            "ix_vfs_public_id",
            "public_id",
            unique=True,
            sqlite_where=_KNOWN_TO_ACTIVE_SQLITE,
            postgresql_where=_KNOWN_TO_ACTIVE_PG,
        ),
        CheckConstraint(_CK_EXCHANGE_LOWER, name="ck_vfs_exchange_lower"),
    )
    exchange: Mapped[str] = mapped_column(String(32), index=True)
    instrument_public_id: Mapped[str | None] = mapped_column(UUIDColumn(), nullable=True)
    fee_tier: Mapped[str] = mapped_column(String(32))
    maker_bps: Mapped[float] = mapped_column(Float)
    taker_bps: Mapped[float] = mapped_column(Float)
    min_volume_30d: Mapped[float | None] = mapped_column(Float, nullable=True)
    currency: Mapped[str] = mapped_column(String(8))


class ExecutionPlan(TemporalMixin, Base):
    """Unified execution plan controlling all manual and algorithmic orders.

    Every user trading action (manual orders, SL/TP brackets, trailing stops,
    market making, scheduled orders) becomes an ExecutionPlan instance
    evaluated by a pluggable PlanEvaluator. Child TradeCommand rows flow
    through the existing executor pipeline.
    """

    __tablename__ = "execution_plans"
    __table_args__ = (
        Index("ix_ep_status_exchange_mode", "status", "exchange", "mode"),
        Index("ix_ep_instrument_status", "instrument_public_id", "status"),
        Index("ix_ep_shard_status", "shard_key", "status"),
        Index(
            "uq_ep_idempotency_key",
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
            "uq_ep_active_cancel_idempotency_key",
            "operator_public_id",
            "cancel_idempotency_key",
            unique=True,
            sqlite_where=text(
                "cancel_idempotency_key IS NOT NULL AND known_to = '9999-12-31 23:59:59.000000'"
            ),
            postgresql_where=text(
                "cancel_idempotency_key IS NOT NULL AND known_to = '9999-12-31T23:59:59+00:00'"
            ),
        ),
        Index(
            "ix_ep_public_id",
            "public_id",
            unique=True,
            sqlite_where=_KNOWN_TO_ACTIVE_SQLITE,
            postgresql_where=_KNOWN_TO_ACTIVE_PG,
        ),
        Index(
            "uq_ep_active_bracket_per_cycle",
            "position_cycle_public_id",
            unique=True,
            sqlite_where=text(
                "position_cycle_public_id IS NOT NULL "
                "AND plan_type = 'bracket' "
                "AND status NOT IN ('completed', 'cancelled', 'failed', 'expired') "
                "AND known_to = '9999-12-31 23:59:59.000000'"
            ),
            postgresql_where=text(
                "position_cycle_public_id IS NOT NULL "
                "AND plan_type = 'bracket' "
                "AND status NOT IN ('completed', 'cancelled', 'failed', 'expired') "
                "AND known_to = '9999-12-31T23:59:59+00:00'"
            ),
        ),
        Index(
            "uq_ep_active_trailing_stop_per_cycle",
            "position_cycle_public_id",
            unique=True,
            sqlite_where=text(
                "position_cycle_public_id IS NOT NULL "
                "AND plan_type = 'trailing_stop' "
                "AND status NOT IN ('completed', 'cancelled', 'failed', 'expired') "
                "AND known_to = '9999-12-31 23:59:59.000000'"
            ),
            postgresql_where=text(
                "position_cycle_public_id IS NOT NULL "
                "AND plan_type = 'trailing_stop' "
                "AND status NOT IN ('completed', 'cancelled', 'failed', 'expired') "
                "AND known_to = '9999-12-31T23:59:59+00:00'"
            ),
        ),
        CheckConstraint(_CK_EXCHANGE_LOWER, name="ck_ep_exchange_lower"),
        CheckConstraint(
            "plan_type IN ('manual_once', 'bracket', 'trailing_stop', "
            "'passive_mm', 'peg', 'scheduler')",
            name="ck_ep_plan_type",
        ),
        CheckConstraint(
            "status IN ('pending', 'armed', 'active', 'paused', 'completed', "
            "'cancel_requested', 'cancelled', 'failed', 'expired')",
            name="ck_ep_status",
        ),
        CheckConstraint(
            "side IN ('buy', 'sell')",
            name="ck_ep_side",
        ),
        CheckConstraint(
            "mode IN ('live', 'paper')",
            name="ck_ep_mode",
        ),
        CheckConstraint(
            "created_via IN ('ui', 'api', 'cli', 'strategy')",
            name="ck_ep_created_via",
        ),
    )
    plan_type: Mapped[str] = mapped_column(String(32), index=True)
    created_by_user_id: Mapped[str | None] = mapped_column(UUIDColumn(), nullable=True, index=True)
    created_by_strategy: Mapped[str | None] = mapped_column(String(128), nullable=True)
    created_via: Mapped[str] = mapped_column(String(16))
    instrument_public_id: Mapped[str] = mapped_column(UUIDColumn(), index=True)
    exchange: Mapped[str] = mapped_column(String(32), index=True)
    mode: Mapped[str] = mapped_column(String(8))
    shard_key: Mapped[str] = mapped_column(String(128), index=True)
    wallet_public_id: Mapped[str] = mapped_column(UUIDColumn(), nullable=False)
    operator_public_id: Mapped[str | None] = mapped_column(UUIDColumn(), nullable=True)
    total_quantity: Mapped[float] = mapped_column(Float)
    filled_quantity: Mapped[float] = mapped_column(Float, default=0.0, server_default="0")
    side: Mapped[str] = mapped_column(String(8))
    parent_plan_public_id: Mapped[str | None] = mapped_column(UUIDColumn(), nullable=True)
    position_cycle_public_id: Mapped[str | None] = mapped_column(UUIDColumn(), nullable=True)
    params: Mapped[JsonObject] = mapped_column(JSON)
    status: Mapped[str] = mapped_column(String(20), index=True)
    created_at: Mapped[datetime] = mapped_column(TZDateTime())
    started_at: Mapped[datetime | None] = mapped_column(TZDateTime(), nullable=True)
    completed_at: Mapped[datetime | None] = mapped_column(TZDateTime(), nullable=True)
    expires_at: Mapped[datetime | None] = mapped_column(TZDateTime(), nullable=True)
    cancel_requested_at: Mapped[datetime | None] = mapped_column(TZDateTime(), nullable=True)
    last_evaluated_at: Mapped[datetime | None] = mapped_column(TZDateTime(), nullable=True)
    last_error: Mapped[str | None] = mapped_column(String(1024), nullable=True)
    idempotency_key: Mapped[str | None] = mapped_column(String(64), nullable=True)
    cancel_idempotency_key: Mapped[str | None] = mapped_column(String(64), nullable=True)
    """Cancel idempotency.

    Caller-supplied dedup key for ``PlansCancelService.cancel_by_plan_public_id``
    (extracts existing ``_cancel_plan`` helper from
    ``order_routes.py:707`` for MCP-side reuse without HTTP/CSRF
    coupling). Same key on second call returns terminal state without
    re-executing the cancel — idempotent replay. Different key for
    same plan -> ``idempotency_key_conflict`` error_code via partial
    unique index ``uq_ep_active_cancel_idempotency_key`` on
    ``(operator_public_id, cancel_idempotency_key)`` WHERE
    ``cancel_idempotency_key IS NOT NULL AND known_to=KNOWN_TO_MAX``.
    """


class ExecutionPlanCheckpoint(TemporalMixin, Base):
    """High-churn evaluator state snapshot, separate from the plan row.

    Plan params change rarely, but evaluator state (trailing peak, current
    quote, next wake time) changes on every tick. Separating high-churn state
    avoids SCD2 row explosion on the main plan table.
    """

    __tablename__ = "execution_plan_checkpoints"
    __table_args__ = (
        Index(
            "ix_epc_plan_public_id",
            "plan_public_id",
        ),
        Index(
            "ix_epc_public_id",
            "public_id",
            unique=True,
            sqlite_where=_KNOWN_TO_ACTIVE_SQLITE,
            postgresql_where=_KNOWN_TO_ACTIVE_PG,
        ),
    )
    plan_public_id: Mapped[str] = mapped_column(UUIDColumn())
    state: Mapped[JsonObject] = mapped_column(JSON)
    last_venue_event_id: Mapped[int] = mapped_column(Integer)
    last_tick_timestamp: Mapped[datetime | None] = mapped_column(TZDateTime(), nullable=True)
    checkpoint_at: Mapped[datetime] = mapped_column(TZDateTime())


class ExecutionPlanDecision(TemporalMixin, Base):
    """Decision log for time-travel debugging of plan evaluator behavior.

    Records why a plan did or did not emit a command at a given moment.
    Uses tiered importance for volume control: action (always logged),
    transition (state changes), routine (sampled every 60th tick-skip).
    """

    __tablename__ = "execution_plan_decisions"
    __table_args__ = (
        Index("ix_epd_plan_public_id", "plan_public_id"),
        Index("ix_epd_decided_at", "decided_at"),
        Index(
            "ix_epd_public_id",
            "public_id",
            unique=True,
            sqlite_where=_KNOWN_TO_ACTIVE_SQLITE,
            postgresql_where=_KNOWN_TO_ACTIVE_PG,
        ),
        CheckConstraint(
            "decision_importance IN ('action', 'transition', 'routine')",
            name="ck_epd_importance",
        ),
    )
    plan_public_id: Mapped[str] = mapped_column(UUIDColumn())
    decision_type: Mapped[str] = mapped_column(String(32))
    decided_at: Mapped[datetime] = mapped_column(TZDateTime())
    trigger_type: Mapped[str] = mapped_column(String(16))
    evidence: Mapped[JsonObject] = mapped_column(JSON)
    emitted_command_public_id: Mapped[str | None] = mapped_column(UUIDColumn(), nullable=True)
    new_status: Mapped[str | None] = mapped_column(String(20), nullable=True)
    reason: Mapped[str] = mapped_column(String(512))
    decision_importance: Mapped[str] = mapped_column(String(16))
    source_surface: Mapped[str] = mapped_column(
        String(20), nullable=False, server_default="strategy"
    )


class PositionCycle(TemporalMixin, Base):
    """A single open->close lifetime of a position on one shard.

    Brackets (SL/TP) attach to a cycle, not to an order: if a position closes
    and the user reopens, the new trades belong to a new cycle even though
    instrument/wallet/mode are identical. Created when a shard's position
    goes flat -> non-flat, closed when it returns to zero, flipped (close +
    open) atomically when the sign reverses in a single fill.

    Attributes:
        max_qty: Per-cycle peak absolute quantity (NOT lifetime). Resets to
            abs(opening_qty) on each new cycle. See column docstring for the
            worked example + UI scope rule.
    """

    __tablename__ = "position_cycles"
    __table_args__ = (
        Index("ix_pc_shard_status", "shard_key", "status"),
        Index("ix_pc_instrument_status", "instrument_public_id", "status"),
        Index("ix_pc_wallet_status", "wallet_public_id", "status"),
        Index(
            "ix_pc_public_id",
            "public_id",
            unique=True,
            sqlite_where=_KNOWN_TO_ACTIVE_SQLITE,
            postgresql_where=_KNOWN_TO_ACTIVE_PG,
        ),
        Index(
            "uq_pc_shard_open_active",
            "shard_key",
            unique=True,
            sqlite_where=text("status = 'open' AND known_to = '9999-12-31 23:59:59.000000'"),
            postgresql_where=text("status = 'open' AND known_to = '9999-12-31T23:59:59+00:00'"),
        ),
        CheckConstraint(_CK_EXCHANGE_LOWER, name="ck_pc_exchange_lower"),
        CheckConstraint("mode IN ('live', 'paper')", name="ck_pc_mode"),
        CheckConstraint(
            "direction IN ('long', 'short')",
            name="ck_pc_direction",
        ),
        CheckConstraint(
            "status IN ('open', 'closed', 'liquidated')",
            name="ck_pc_status",
        ),
        CheckConstraint("max_qty >= 0", name="ck_pc_max_qty_nonneg"),
    )
    instrument_public_id: Mapped[str] = mapped_column(UUIDColumn(), index=True)
    exchange: Mapped[str] = mapped_column(String(32), index=True)
    mode: Mapped[str] = mapped_column(String(8))
    shard_key: Mapped[str] = mapped_column(String(128), index=True)
    wallet_public_id: Mapped[str] = mapped_column(UUIDColumn(), nullable=False, index=True)
    operator_public_id: Mapped[str | None] = mapped_column(UUIDColumn(), nullable=True)
    direction: Mapped[str] = mapped_column(String(8))
    max_qty: Mapped[float] = mapped_column(
        Float,
        doc=(
            "Per-cycle peak absolute quantity (NOT lifetime). Accumulates "
            "max(abs(position_qty)) across the fills of a SINGLE open->close "
            "cycle. Resets to abs(opening_qty) on each new cycle on the same "
            "shard. Example: cycle C1 opens with 1 unit, scales to 3 units, "
            "closes -> max_qty=3. A subsequent cycle C2 on the same shard "
            "opening with 2 units starts a fresh max_qty=2, NOT max(3, 2). "
            "UI rendering MUST scope to the cycle's opened_at/closed_at "
            "window; never sum or max across cycles. Updated monotonically "
            "via update_position_cycle_max_qty - silent no-op if the new "
            "value is not strictly greater than the current."
        ),
    )
    status: Mapped[str] = mapped_column(String(16), index=True)
    opened_at: Mapped[datetime] = mapped_column(TZDateTime())
    closed_at: Mapped[datetime | None] = mapped_column(TZDateTime(), nullable=True)
    opening_command_public_id: Mapped[str | None] = mapped_column(UUIDColumn(), nullable=True)
    closing_command_public_id: Mapped[str | None] = mapped_column(UUIDColumn(), nullable=True)


class BacktestRun(TemporalMixin, Base):
    """A single backtest run from creation to completion.

    Status transitions use SCD2 close-and-insert: pending -> running ->
    completed/failed/cancelled. Multi-tenant via wallet_public_id.
    """

    __tablename__ = "backtest_runs"
    __table_args__ = (
        Index("ix_backtest_runs_wallet_status", "wallet_public_id", "status"),
        Index(
            "ix_backtest_runs_public_id",
            "public_id",
            unique=True,
            sqlite_where=_KNOWN_TO_ACTIVE_SQLITE,
            postgresql_where=_KNOWN_TO_ACTIVE_PG,
        ),
        CheckConstraint(
            "status IN ('pending', 'running', 'completed', 'failed', "
            "'cancel_requested', 'cancelled')",
            name="ck_br_status",
        ),
        CheckConstraint(
            "execution_mode IN ('direct_db', 'zmq_replay')",
            name="ck_br_execution_mode",
        ),
        CheckConstraint("fill_model IN ('market')", name="ck_br_fill_model"),
        CheckConstraint(
            "slippage_bps >= 0 AND slippage_bps <= 500",
            name="ck_br_slippage_bounds",
        ),
        CheckConstraint(
            "commission_bps >= 0 AND commission_bps <= 500",
            name="ck_br_commission_bounds",
        ),
        CheckConstraint(
            "target_execution_exchange IS NULL OR target_execution_exchange IN "
            "('paper', 'kraken', 'kraken_futures', 'walutomat')",
            name="ck_br_target_execution_exchange",
        ),
    )
    wallet_public_id: Mapped[str] = mapped_column(UUIDColumn(), nullable=False)
    operator_public_id: Mapped[str | None] = mapped_column(UUIDColumn(), nullable=True)
    strategy_name: Mapped[str] = mapped_column(String(128))
    strategy_params: Mapped[JsonObject] = mapped_column(JSON, default=dict)
    instrument_public_id: Mapped[str] = mapped_column(UUIDColumn())
    exchange: Mapped[str] = mapped_column(String(32))
    mode: Mapped[str] = mapped_column(String(8), default="paper")
    timeframe: Mapped[str] = mapped_column(String(16))
    start_date: Mapped[datetime] = mapped_column(TZDateTime())
    end_date: Mapped[datetime] = mapped_column(TZDateTime())
    initial_cash: Mapped[float] = mapped_column(Float, default=10000.0)
    status: Mapped[str] = mapped_column(String(24), default="pending")
    execution_mode: Mapped[str] = mapped_column(String(16), default="direct_db")
    fill_model: Mapped[str] = mapped_column(String(32), default="market")
    slippage_bps: Mapped[float] = mapped_column(Float, default=0.0)
    commission_bps: Mapped[float] = mapped_column(Float, default=0.0)
    config_hash: Mapped[str | None] = mapped_column(String(64), nullable=True)
    target_execution_exchange: Mapped[str | None] = mapped_column(String(32), nullable=True)
    created_by_user_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    started_at: Mapped[datetime | None] = mapped_column(TZDateTime(), nullable=True)
    completed_at: Mapped[datetime | None] = mapped_column(TZDateTime(), nullable=True)
    error: Mapped[str | None] = mapped_column(Text, nullable=True)
    process_name: Mapped[str | None] = mapped_column(String(128), nullable=True)


class BacktestEvent(TemporalMixin, Base):
    """Append-only event log for a backtest run.

    Records lifecycle events (started, candle_processed, signal_generated,
    trade_executed, completed, failed, cancelled). known_to stays MAX.
    """

    __tablename__ = "backtest_events"
    __table_args__ = (
        Index("ix_be_run_ts", "run_public_id", "timestamp"),
        Index(
            "ix_be_public_id",
            "public_id",
            unique=True,
            sqlite_where=_KNOWN_TO_ACTIVE_SQLITE,
            postgresql_where=_KNOWN_TO_ACTIVE_PG,
        ),
    )
    run_public_id: Mapped[str] = mapped_column(UUIDColumn(), index=True)
    event_type: Mapped[str] = mapped_column(String(64))
    detail: Mapped[JsonObject] = mapped_column(JSON, default=dict)


class BacktestResult(TemporalMixin, Base):
    """Aggregate metrics for a completed backtest run.

    One-to-one with completed run. On recalculation (bug fix), the old
    row is SCD2-closed and a new corrected version is inserted.
    """

    __tablename__ = "backtest_results"
    __table_args__ = (
        Index(
            "uq_br_run_active",
            "run_public_id",
            unique=True,
            sqlite_where=_KNOWN_TO_ACTIVE_SQLITE,
            postgresql_where=_KNOWN_TO_ACTIVE_PG,
        ),
        Index(
            "ix_bres_public_id",
            "public_id",
            unique=True,
            sqlite_where=_KNOWN_TO_ACTIVE_SQLITE,
            postgresql_where=_KNOWN_TO_ACTIVE_PG,
        ),
    )
    run_public_id: Mapped[str] = mapped_column(UUIDColumn(), index=True)
    total_trades: Mapped[int] = mapped_column(Integer, default=0)
    winning_trades: Mapped[int] = mapped_column(Integer, default=0)
    losing_trades: Mapped[int] = mapped_column(Integer, default=0)
    total_pnl: Mapped[float] = mapped_column(Float, default=0.0)
    max_drawdown: Mapped[float] = mapped_column(Float, default=0.0)
    sharpe_ratio: Mapped[float | None] = mapped_column(Float, nullable=True)
    win_rate: Mapped[float | None] = mapped_column(Float, nullable=True)
    profit_factor: Mapped[float | None] = mapped_column(Float, nullable=True)
    final_equity: Mapped[float] = mapped_column(Float, default=0.0)
    max_equity: Mapped[float] = mapped_column(Float, default=0.0)
    sortino_ratio: Mapped[float | None] = mapped_column(Float, nullable=True)
    cagr: Mapped[float | None] = mapped_column(Float, nullable=True)
    calmar_ratio: Mapped[float | None] = mapped_column(Float, nullable=True)
    expectancy: Mapped[float | None] = mapped_column(Float, nullable=True)
    avg_trade_pnl: Mapped[float | None] = mapped_column(Float, nullable=True)
    max_drawdown_duration_seconds: Mapped[float | None] = mapped_column(Float, nullable=True)
    exposure_ratio: Mapped[float | None] = mapped_column(Float, nullable=True)
    turnover_ratio: Mapped[float | None] = mapped_column(Float, nullable=True)
    extra_metrics: Mapped[JsonObject] = mapped_column(JSON, default=dict)


class BacktestSignal(TemporalMixin, Base):
    """Immutable signal generated during a backtest.

    Records each strategy signal with its context (candle close price,
    indicator values). Append-only, known_to stays MAX.
    """

    __tablename__ = "backtest_signals"
    __table_args__ = (
        Index("ix_bs_run_ts", "run_public_id", "signal_time"),
        Index(
            "ix_bs_public_id",
            "public_id",
            unique=True,
            sqlite_where=_KNOWN_TO_ACTIVE_SQLITE,
            postgresql_where=_KNOWN_TO_ACTIVE_PG,
        ),
    )
    run_public_id: Mapped[str] = mapped_column(UUIDColumn(), index=True)
    signal_time: Mapped[datetime] = mapped_column(TZDateTime())
    signal_type: Mapped[str] = mapped_column(String(32))
    instrument: Mapped[str] = mapped_column(String(64))
    price: Mapped[float] = mapped_column(Float)
    indicators: Mapped[JsonObject] = mapped_column(JSON, default=dict)


class BacktestTrade(TemporalMixin, Base):
    """Immutable simulated trade fill from a backtest.

    Records entry/exit fills with price, size, fees, and PnL.
    Append-only, known_to stays MAX.
    """

    __tablename__ = "backtest_trades"
    __table_args__ = (
        Index("ix_bt_run_ts", "run_public_id", "executed_at"),
        Index(
            "ix_bt_public_id",
            "public_id",
            unique=True,
            sqlite_where=_KNOWN_TO_ACTIVE_SQLITE,
            postgresql_where=_KNOWN_TO_ACTIVE_PG,
        ),
    )
    run_public_id: Mapped[str] = mapped_column(UUIDColumn(), index=True)
    executed_at: Mapped[datetime] = mapped_column(TZDateTime())
    instrument: Mapped[str] = mapped_column(String(64))
    side: Mapped[str] = mapped_column(String(8))
    quantity: Mapped[float] = mapped_column(Float)
    price: Mapped[float] = mapped_column(Float)
    fee: Mapped[float] = mapped_column(Float, default=0.0)
    pnl: Mapped[float | None] = mapped_column(Float, nullable=True)
    position_after: Mapped[float] = mapped_column(Float, default=0.0)
    signal_public_id: Mapped[str | None] = mapped_column(UUIDColumn(), nullable=True)


class BacktestEquityPoint(TemporalMixin, Base):
    """Immutable equity curve data point from a backtest.

    Records portfolio value at each candle close. Used for equity
    chart visualization and drawdown calculation.
    Append-only, known_to stays MAX.
    """

    __tablename__ = "backtest_equity_points"
    __table_args__ = (
        Index("ix_bep_run_ts", "run_public_id", "point_time"),
        Index(
            "ix_bep_public_id",
            "public_id",
            unique=True,
            sqlite_where=_KNOWN_TO_ACTIVE_SQLITE,
            postgresql_where=_KNOWN_TO_ACTIVE_PG,
        ),
    )
    run_public_id: Mapped[str] = mapped_column(UUIDColumn(), index=True)
    point_time: Mapped[datetime] = mapped_column(TZDateTime())
    equity: Mapped[float] = mapped_column(Float)
    cash: Mapped[float] = mapped_column(Float)
    position_value: Mapped[float] = mapped_column(Float, default=0.0)
    drawdown: Mapped[float] = mapped_column(Float, default=0.0)


class BacktestComparison(TemporalMixin, Base):
    """Persisted comparison request pairing two terminal backtest runs.

    The diff payload is recomputed on GET from current artifact rows,
    so this row stores only the request metadata: normalised (A, B)
    pair + pairing_mode + optional anchor. Pair is normalised to
    (min, max) by lexical public_id so (A,B) and (B,A) collapse.
    """

    __tablename__ = "backtest_comparisons"
    __table_args__ = (
        Index(
            "ix_bc_wallet_hash_time",
            "wallet_public_id",
            "config_hash",
            "timestamp",
        ),
        Index(
            "ix_bc_public_id",
            "public_id",
            unique=True,
            sqlite_where=_KNOWN_TO_ACTIVE_SQLITE,
            postgresql_where=_KNOWN_TO_ACTIVE_PG,
        ),
        Index(
            "uq_bc_active_pair_per_wallet",
            "wallet_public_id",
            "run_a_public_id",
            "run_b_public_id",
            unique=True,
            sqlite_where=_KNOWN_TO_ACTIVE_SQLITE,
            postgresql_where=_KNOWN_TO_ACTIVE_PG,
        ),
        CheckConstraint(
            "run_a_public_id <> run_b_public_id",
            name="ck_bc_runs_distinct",
        ),
    )
    wallet_public_id: Mapped[str] = mapped_column(UUIDColumn(), index=True)
    operator_public_id: Mapped[str | None] = mapped_column(UUIDColumn(), nullable=True)
    created_by_user_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    run_a_public_id: Mapped[str] = mapped_column(UUIDColumn())
    run_b_public_id: Mapped[str] = mapped_column(UUIDColumn())
    config_hash: Mapped[str | None] = mapped_column(String(64), nullable=True)
    pairing_mode: Mapped[str] = mapped_column(String(16))
    anchor_run_public_id: Mapped[str | None] = mapped_column(UUIDColumn(), nullable=True)


class UserTradingCaps(TemporalMixin, Base):
    """Per-user trading safety caps enforced by ``TradingCapsEnforcer``.

    Temporal (SCD2) — the active row for a user is the one with
    ``known_to = KNOWN_TO_MAX``. Updates close + insert a new version
    per the standard TemporalMixin lifecycle so cap history is
    auditable.
    All cap columns are nullable; a NULL cap means "unbounded" for
    that axis. Default enforcement policy
        ``max_order_quantity_per_instrument``: JSON dict
          ``{instrument_public_id: Decimal}`` OR a scalar Decimal
          (applied to every instrument when scalar).
        ``max_open_orders``: all-time count of user's in-flight
          commands (status IN created/dispatched/acked/accepted/
          partially_filled). No time window — this is an in-flight
          exposure cap, not a rate cap.
        ``max_daily_notional_usd``: rolling 24h sum of
          ``submit_quantity * submit_price_usd`` over non-rejected
          commands. Submit-time commitment basis; partial fills do
          not change accounting.
        ``max_cancels_per_minute``: sliding 60-second count of
          the user's cancel commands.
    """

    __tablename__ = "user_trading_caps"
    __table_args__ = (
        Index(
            "ix_user_trading_caps_public_id",
            "public_id",
            unique=True,
            sqlite_where=_KNOWN_TO_ACTIVE_SQLITE,
            postgresql_where=_KNOWN_TO_ACTIVE_PG,
        ),
        Index(
            "ix_user_trading_caps_active",
            "user_public_id",
            unique=True,
            sqlite_where=_KNOWN_TO_ACTIVE_SQLITE,
            postgresql_where=_KNOWN_TO_ACTIVE_PG,
        ),
    )
    user_public_id: Mapped[str] = mapped_column(UUIDColumn(), nullable=False)
    max_order_quantity_per_instrument: Mapped[JsonObject | None] = mapped_column(
        JSON, nullable=True
    )
    max_open_orders: Mapped[int | None] = mapped_column(Integer, nullable=True)
    max_daily_notional_usd: Mapped[float | None] = mapped_column(Numeric(18, 2), nullable=True)
    max_cancels_per_minute: Mapped[int | None] = mapped_column(Integer, nullable=True)


class UserActiveToken(Base):
    """Non-temporal token inventory for kill-switch + fast-path blacklist.

    Rows carry explicit lifecycle (``issued_at``, ``expires_at``
    ``revoked_at``) — NOT SCD2 versioned — so deactivation flips
    ``revoked_at`` in place rather than closing + inserting a new
    row. Cleaned by ``token_cleanup_loop``
    (``snapper.application.admin.token_cleanup``) on a daily cycle.
    The ``jti`` column enables ``TokenManager.revoke_user_sessions``
    to push every active token's JWT ID into the in-memory
    ``_blacklisted_tokens`` fast-path cache — since SHA-256 is not
    reversible to the JTI but the JWT payload carries the JTI for
    fast lookup in ``verify_token()``.
    ``token_hash`` is SHA-256 HEX of the full token so the DB-backed
    inventory can be checked per-request in ``verify_token()``
    without holding the raw JWT plaintext in storage.
    """

    __tablename__ = "user_active_tokens"
    __table_args__ = (Index("ix_user_active_tokens_user_revoked", "user_public_id", "revoked_at"),)
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    public_id: Mapped[str] = mapped_column(UUIDColumn(), nullable=False, unique=True)
    user_public_id: Mapped[str] = mapped_column(UUIDColumn(), nullable=False, index=True)
    jti: Mapped[str] = mapped_column(String(64), nullable=False, unique=True)
    token_hash: Mapped[str] = mapped_column(String(64), nullable=False, unique=True)
    token_type: Mapped[str] = mapped_column(String(10), nullable=False)
    issued_at: Mapped[datetime] = mapped_column(TZDateTime(), nullable=False)
    expires_at: Mapped[datetime] = mapped_column(TZDateTime(), nullable=False)
    revoked_at: Mapped[datetime | None] = mapped_column(TZDateTime(), nullable=True)


class NotificationDevice(TemporalMixin, Base):
    """Temporal (SCD2) inventory of iOS devices registered for APNs push.

    Per project invariant (``feedback_bitemporal_all_tables``): every
    table must carry ``TemporalMixin`` (``timestamp`` + ``known_to``).
    Device lifecycle is versioned via SCD2: an active row is
    ``known_to = KNOWN_TO_MAX AND token_status = 'active'``. Close +
    insert successor is used for every transition — token refresh
    (next active row), APNs 410 (successor with
    ``token_status = 'unregistered'``), and explicit user unregister
    from the app (successor with ``token_status = 'user_unregistered'``).
    The tombstone successor keeps ``known_to = KNOWN_TO_MAX`` so an
    ``as_of`` query returns a visible inactive row instead of a gap.
    Partial indexes add ``token_status = 'active'`` so multiple historical
    tombstones per token do not collide with the active-row uniqueness
    constraint, and re-registration of the same token after an
    unregister is permitted.

    ``device_token`` is the APNs binary token hex-encoded; ``env``
    tracks sandbox vs production APNs scope (one token is valid for
    exactly one env per Apple); ``previews_mode`` gates iOS
    lock-screen payload visibility (``private`` by default).
    """

    __tablename__ = "notification_devices"
    __table_args__ = (
        CheckConstraint(
            _CK_NOTIFICATION_DEVICE_TOKEN_STATUS,
            name="ck_notification_devices_token_status",
        ),
        Index(
            "ix_notification_devices_public_id",
            "public_id",
            unique=True,
            sqlite_where=_NOTIFICATION_DEVICE_ACTIVE_SQLITE,
            postgresql_where=_NOTIFICATION_DEVICE_ACTIVE_PG,
        ),
        Index(
            "uq_notification_devices_token_active",
            "device_token",
            unique=True,
            sqlite_where=_NOTIFICATION_DEVICE_ACTIVE_SQLITE,
            postgresql_where=_NOTIFICATION_DEVICE_ACTIVE_PG,
        ),
        Index(
            "ix_notification_devices_user_active",
            "user_public_id",
            sqlite_where=_NOTIFICATION_DEVICE_ACTIVE_SQLITE,
            postgresql_where=_NOTIFICATION_DEVICE_ACTIVE_PG,
        ),
    )
    user_public_id: Mapped[str] = mapped_column(UUIDColumn(), nullable=False)
    device_token: Mapped[str] = mapped_column(String(200), nullable=False)
    device_id: Mapped[str] = mapped_column(String(64), nullable=False)
    platform: Mapped[str] = mapped_column(String(10), nullable=False, server_default="ios")
    env: Mapped[str] = mapped_column(String(10), nullable=False)
    app_version: Mapped[str | None] = mapped_column(String(20), nullable=True)
    previews_mode: Mapped[str] = mapped_column(String(10), nullable=False, server_default="private")
    registered_at: Mapped[datetime] = mapped_column(TZDateTime(), nullable=False)
    last_seen_at: Mapped[datetime | None] = mapped_column(TZDateTime(), nullable=True)
    token_status: Mapped[str] = mapped_column(String(20), nullable=False, server_default="active")


class DeviceAlertPref(TemporalMixin, Base):
    """Temporal (SCD2) per-(device, alert_type, scope) preferences.

    Scope layers (narrowest first, routing precedence):
    wallet (operator NOT NULL + wallet NOT NULL) →
    operator (operator NOT NULL + wallet NULL) →
    device-global (operator NULL + wallet NULL). Three partial unique
    indexes gated on ``known_to = KNOWN_TO_MAX`` encode uniqueness per
    scope depth among ACTIVE rows only (historical closed rows do not
    block re-insertion of a changed preference at the same scope).
    The CHECK constraint ``ck_device_alert_valid_scope`` rejects the
    nonsensical (operator NULL + wallet NOT NULL) combination.
    Preference updates close the active row and insert a new version
    (SCD2) so audit replay can reconstruct historical routing verdicts.
    """

    __tablename__ = "device_alert_prefs"
    __table_args__ = (
        Index(
            "ix_device_alert_prefs_public_id",
            "public_id",
            unique=True,
            sqlite_where=_KNOWN_TO_ACTIVE_SQLITE,
            postgresql_where=_KNOWN_TO_ACTIVE_PG,
        ),
        Index(
            "uq_device_alert_wallet_scope",
            "device_public_id",
            "alert_type",
            "operator_public_id",
            "wallet_public_id",
            unique=True,
            sqlite_where=text(
                "known_to = '9999-12-31 23:59:59.000000' AND "
                "operator_public_id IS NOT NULL AND wallet_public_id IS NOT NULL"
            ),
            postgresql_where=text(
                "known_to = '9999-12-31T23:59:59+00:00' AND "
                "operator_public_id IS NOT NULL AND wallet_public_id IS NOT NULL"
            ),
        ),
        Index(
            "uq_device_alert_operator_scope",
            "device_public_id",
            "alert_type",
            "operator_public_id",
            unique=True,
            sqlite_where=text(
                "known_to = '9999-12-31 23:59:59.000000' AND "
                "operator_public_id IS NOT NULL AND wallet_public_id IS NULL"
            ),
            postgresql_where=text(
                "known_to = '9999-12-31T23:59:59+00:00' AND "
                "operator_public_id IS NOT NULL AND wallet_public_id IS NULL"
            ),
        ),
        Index(
            "uq_device_alert_device_scope",
            "device_public_id",
            "alert_type",
            unique=True,
            sqlite_where=text(
                "known_to = '9999-12-31 23:59:59.000000' AND "
                "operator_public_id IS NULL AND wallet_public_id IS NULL"
            ),
            postgresql_where=text(
                "known_to = '9999-12-31T23:59:59+00:00' AND "
                "operator_public_id IS NULL AND wallet_public_id IS NULL"
            ),
        ),
        Index(
            "ix_device_alert_prefs_lookup",
            "device_public_id",
            "alert_type",
            sqlite_where=_KNOWN_TO_ACTIVE_SQLITE,
            postgresql_where=_KNOWN_TO_ACTIVE_PG,
        ),
        CheckConstraint(
            "NOT (wallet_public_id IS NOT NULL AND operator_public_id IS NULL)",
            name="ck_device_alert_valid_scope",
        ),
    )
    device_public_id: Mapped[str] = mapped_column(UUIDColumn(), nullable=False)
    alert_type: Mapped[str] = mapped_column(String(50), nullable=False)
    operator_public_id: Mapped[str | None] = mapped_column(UUIDColumn(), nullable=True)
    wallet_public_id: Mapped[str | None] = mapped_column(UUIDColumn(), nullable=True)
    enabled: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=True, server_default=text("true")
    )
    min_priority: Mapped[str] = mapped_column(String(10), nullable=False, server_default="medium")
    quiet_hours_start_min: Mapped[int | None] = mapped_column(Integer, nullable=True)
    quiet_hours_end_min: Mapped[int | None] = mapped_column(Integer, nullable=True)
    mute_until: Mapped[datetime | None] = mapped_column(TZDateTime(), nullable=True)
    timezone: Mapped[str] = mapped_column(String(64), nullable=False, server_default="UTC")


class UserAlertDefault(TemporalMixin, Base):
    """Temporal (SCD2) user-level fallback preference per alert type.

    Last step in the routing precedence chain: when no
    device-scoped (wallet / operator / device-global) row matches
    for a given (device, alert_type) combo, routing falls back to
    the per-user default. Unique among ACTIVE rows per
    (user_public_id, alert_type) — enforced via a partial unique
    index gated on ``known_to = KNOWN_TO_MAX``.
    Updates close the active row and insert a new version.
    """

    __tablename__ = "user_alert_defaults"
    __table_args__ = (
        Index(
            "ix_user_alert_defaults_public_id",
            "public_id",
            unique=True,
            sqlite_where=_KNOWN_TO_ACTIVE_SQLITE,
            postgresql_where=_KNOWN_TO_ACTIVE_PG,
        ),
        Index(
            "uq_user_alert_default_active",
            "user_public_id",
            "alert_type",
            unique=True,
            sqlite_where=_KNOWN_TO_ACTIVE_SQLITE,
            postgresql_where=_KNOWN_TO_ACTIVE_PG,
        ),
    )
    user_public_id: Mapped[str] = mapped_column(UUIDColumn(), nullable=False)
    alert_type: Mapped[str] = mapped_column(String(50), nullable=False)
    enabled: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=True, server_default=text("true")
    )
    min_priority: Mapped[str] = mapped_column(String(10), nullable=False, server_default="medium")


class AlertEvent(TemporalMixin, Base):
    """Temporal (SCD2) authoritative log of alerts generated for a user.

    SCD2 via ``TemporalMixin`` (``timestamp`` + ``known_to``): correction
    semantics allow a rule to update its policy verdict for an already-
    fired alert without rewriting history (rare, but preserves audit
    trail). Primary-path inserts set ``known_to = KNOWN_TO_MAX`` and are
    served to the iOS alert history view via partial active indexes.
    ``dedup_key`` enables the rule engine to suppress repeat fires (e.g.
    same ``client_order_id`` rejected twice within a minute). ``payload``
    carries alert-type-specific context (order_id, position_id,
    instrument symbol, price, etc.) as JSON — keep it small,
    privacy-safe (no PII beyond what the alert body shows).
    """

    __tablename__ = "alert_events"
    __table_args__ = (
        Index(
            "ix_alert_events_public_id_active",
            "public_id",
            unique=True,
            sqlite_where=_KNOWN_TO_ACTIVE_SQLITE,
            postgresql_where=_KNOWN_TO_ACTIVE_PG,
        ),
        Index(
            "ix_alert_events_user_type_time",
            "user_public_id",
            "alert_type",
            "timestamp",
            sqlite_where=_KNOWN_TO_ACTIVE_SQLITE,
            postgresql_where=_KNOWN_TO_ACTIVE_PG,
        ),
        Index(
            "ix_alert_events_dedup",
            "user_public_id",
            "dedup_key",
            "timestamp",
            sqlite_where=_KNOWN_TO_ACTIVE_SQLITE,
            postgresql_where=_KNOWN_TO_ACTIVE_PG,
        ),
    )
    user_public_id: Mapped[str] = mapped_column(UUIDColumn(), nullable=False)
    operator_public_id: Mapped[str | None] = mapped_column(UUIDColumn(), nullable=True)
    wallet_public_id: Mapped[str | None] = mapped_column(UUIDColumn(), nullable=True)
    alert_type: Mapped[str] = mapped_column(String(50), nullable=False)
    priority: Mapped[str] = mapped_column(String(10), nullable=False)
    is_safety_critical: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=False, server_default=text("false")
    )
    title: Mapped[str] = mapped_column(String(200), nullable=False)
    body: Mapped[str] = mapped_column(String(500), nullable=False)
    payload: Mapped[JsonObject | None] = mapped_column(JSON, nullable=True)
    dedup_key: Mapped[str | None] = mapped_column(String(200), nullable=True)
    thread_key: Mapped[str | None] = mapped_column(String(100), nullable=True)
    source_topic: Mapped[str | None] = mapped_column(String(200), nullable=True)


class AlertDelivery(TemporalMixin, Base):
    """Temporal (SCD2) audit of APNs delivery attempts.

    Per project invariant, even append-only audit tables carry
    ``TemporalMixin`` — status transitions are SCD2 versioned (close
    old row + insert new version with updated ``status`` /
    ``attempt_count`` / ``apns_id`` / etc.) so the full lifecycle
    of every delivery is queryable with ``as_of``. Active-row lookups
    (queue drain, retry) use ``known_to == KNOWN_TO_MAX`` via the
    partial active indexes below. Crash-safety invariant:
    ``attempt_count`` increments BEFORE the APNs HTTP call,
    so on sidecar restart mid-flight a row with ``attempt_count=N``
    and ``status='queued'`` is retriable at most once redundantly
    (bounded ≤1 duplicate send per crash). Scope columns
    (``user_public_id`` / ``operator_public_id`` / ``wallet_public_id``)
    are denormalised from the source ``alert_event`` at queue time so
    scope-based cancel passes do NOT depend on the current active
    SCD2 version of the event.
    """

    __tablename__ = "alert_deliveries"
    __table_args__ = (
        Index(
            "ix_alert_deliveries_public_id",
            "public_id",
            unique=True,
            sqlite_where=_KNOWN_TO_ACTIVE_SQLITE,
            postgresql_where=_KNOWN_TO_ACTIVE_PG,
        ),
        Index(
            "ix_alert_deliveries_alert_event_public_id",
            "alert_event_public_id",
            sqlite_where=_KNOWN_TO_ACTIVE_SQLITE,
            postgresql_where=_KNOWN_TO_ACTIVE_PG,
        ),
        Index(
            "ix_alert_deliveries_device_public_id",
            "device_public_id",
            sqlite_where=_KNOWN_TO_ACTIVE_SQLITE,
            postgresql_where=_KNOWN_TO_ACTIVE_PG,
        ),
        Index(
            "ix_alert_deliveries_status_queued",
            "created_at",
            sqlite_where=text("known_to = '9999-12-31 23:59:59.000000' AND status = 'queued'"),
            postgresql_where=text("known_to = '9999-12-31T23:59:59+00:00' AND status = 'queued'"),
        ),
        Index(
            "ix_alert_deliveries_next_attempt",
            "next_attempt_at",
            sqlite_where=text("known_to = '9999-12-31 23:59:59.000000' AND status = 'queued'"),
            postgresql_where=text("known_to = '9999-12-31T23:59:59+00:00' AND status = 'queued'"),
        ),
        Index(
            "ix_alert_deliveries_scope_status",
            "user_public_id",
            "operator_public_id",
            "wallet_public_id",
            "status",
            sqlite_where=_KNOWN_TO_ACTIVE_SQLITE,
            postgresql_where=_KNOWN_TO_ACTIVE_PG,
        ),
        CheckConstraint(
            "status IN ('queued', 'sent', 'failed', 'unregistered', 'cancelled_scope')",
            name="ck_alert_delivery_status",
        ),
    )
    alert_event_public_id: Mapped[str] = mapped_column(UUIDColumn(), nullable=False)
    device_public_id: Mapped[str] = mapped_column(UUIDColumn(), nullable=False)
    user_public_id: Mapped[str] = mapped_column(UUIDColumn(), nullable=False)
    operator_public_id: Mapped[str | None] = mapped_column(UUIDColumn(), nullable=True)
    wallet_public_id: Mapped[str | None] = mapped_column(UUIDColumn(), nullable=True)
    status: Mapped[str] = mapped_column(String(20), nullable=False)
    attempt_count: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")
    last_attempt_at: Mapped[datetime | None] = mapped_column(TZDateTime(), nullable=True)
    next_attempt_at: Mapped[datetime | None] = mapped_column(TZDateTime(), nullable=True)
    apns_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    error_reason: Mapped[str | None] = mapped_column(String(200), nullable=True)
    created_at: Mapped[datetime] = mapped_column(TZDateTime(), nullable=False)


class AiDelegate(Base):
    """Runtime state for AI delegates.

    Logical 1-to-1 with ``users`` rows where ``role=AI_DELEGATE``.
    Created by ``UserService.create_ai_delegate`` AFTER the user
    row is committed. Stores fields that change frequently (liveness
    timestamp, in-flight review counter) and are NOT bitemporal —
    keeping them off the SCD2 ``users`` table avoids spamming
    history rows for routine WS heartbeats.

    The ``ai_reviews.selected_delegate_public_id`` /
    ``responding_delegate_public_id`` FKs reference
    ``ai_delegates.public_id`` (NOT ``users.public_id``). Service
    layer translates between the two as needed.

    Attributes:
        public_id: UUID7 — used as ``selected_delegate_public_id``
            on :class:`AiReview`.
        user_public_id: Logical FK to ``users.public_id`` (the user
            row with ``role=AI_DELEGATE``).
        last_seen_at: Most recent WS connection / heartbeat /
            authenticate frame. Updated by
            ``WebSocketAuthManager``. Used by the Layer 2 scanner +
            admission control.
        active_reviews_count: In-flight review counter — incremented
            exactly once per review at creation, decremented exactly
            once at terminal transition via
            ``ai_reviews.counter_decremented_at`` writable-CTE
            primitive.
        created_at: Row creation timestamp.
        updated_at: Last mutation timestamp.
    """

    __tablename__ = "ai_delegates"
    __table_args__ = (
        UniqueConstraint("public_id", name="uq_ai_delegates_public_id"),
        UniqueConstraint("user_public_id", name="uq_ai_delegates_user_public_id"),
        CheckConstraint(
            "active_reviews_count >= 0",
            name="ck_ai_delegates_active_reviews_nonneg",
        ),
        Index("ix_ai_delegates_last_seen_at", "last_seen_at"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    public_id: Mapped[str] = mapped_column(UUIDColumn(), nullable=False, default=_public_id)
    user_public_id: Mapped[str] = mapped_column(UUIDColumn(), nullable=False)
    last_seen_at: Mapped[datetime | None] = mapped_column(TZDateTime(), nullable=True)
    active_reviews_count: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")
    created_at: Mapped[datetime] = mapped_column(TZDateTime(), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(TZDateTime(), nullable=False)


class AiReview(Base):
    """A CONSULT review request issued by a strategy to an AI delegate.

    Mutable status row (no SCD2 for ``ai_reviews``); audit trail
    via append-only :class:`AiReviewEvent` rows. Status transitions
    via atomic UPDATE (writable-CTE counter primitive).

    Lifecycle: created with ``status=pending`` -> optional
    ``fanout_dispatched`` -> terminal ``resolved_approved`` /
    ``resolved_rejected`` / ``timeout`` / ``superseded``. All
    terminal states are FINAL.

    The state machine + bus pub/sub + WS fanout + reaper + offline
    scanner + admission control are owned by ``AiReviewService``.

    Attributes:
        public_id: UUID7 review correlation ID — used as
            ``review_id`` in MCP ``submit_ai_review_decision``.
        user_public_id: Owner of the strategy. DISTINCT from
            delegate users.
        operator_public_id: Operator scope.
        wallet_public_id: Wallet for caps + scope grants.
        instrument_public_id: Instrument for scope grants
            (grants are wallet+instrument).
        strategy_public_id: Origin strategy.
        selected_delegate_public_id: AI delegate originally chosen
            at creation. IMMUTABLE.
        responding_delegate_public_id: Delegate that submitted the
            resolving decision. NULL until terminal.
        resolution_mode: How resolution happened (see
            :class:`AiReviewResolutionModeEnum`).
        status: Current state (see
            :class:`AiReviewStatusEnum`).
        signal_envelope: Full signal payload (JSON). Persisted for
            restart-time reconstruction of pending review lists.
        signal_snapshot_hash: SHA-256 hex of canonical-encoded
            signal_envelope. Audit/forensics — NOT used for replay
            protection.
        instrument_metadata: JSON — spread, recent volume, last
            price — context for AI decision.
        deadline: Wall-clock deadline (TZ-aware UTC). Reaper
            transitions to ``timeout`` when ``deadline < NOW()``
            on rows still in ``pending``/``fanout_dispatched``.
        fanout_after: When fanout fires if selected delegate stays
            offline. Default = ``created_at + 30s``.
        decision: ``approve`` / ``reject`` / NULL. Set at terminal.
        rationale: Free text. Bounded 4096 chars at insert.
        dispatch_version: Monotonically incremented on every
            fanout-related UPDATE. Bridge dedupes by
            ``(public_id, dispatch_version)``. Initial value 0.
        counter_decremented_at: Set NON-NULL when
            ``ai_delegates.active_reviews_count`` has been
            decremented for this review. Idempotency primitive:
            writable-CTE `WHERE counter_decremented_at IS NULL`
            ensures exactly-once decrement under concurrent
            reaper/decision/supersede races.
        created_at: Row creation timestamp.
        updated_at: Last status mutation timestamp.
        resolved_at: Wall-clock when review reached terminal
            state. NULL while ``pending``/``fanout_dispatched``.
        session_id: Strategy's bus session UUID at creation —
            strategy passes via ``StrategyContext.create_ai_review``.
        sequence_id: Strategy's monotonic sequence at creation.
    """

    __tablename__ = "ai_reviews"
    __table_args__ = (
        UniqueConstraint("public_id", name="uq_ai_reviews_public_id"),
        Index("ix_ai_reviews_public_id_lookup", "public_id"),
        Index(
            "ix_ai_reviews_pending_per_delegate",
            "selected_delegate_public_id",
            "status",
        ),
        Index("ix_ai_reviews_deadline_pending", "deadline", "status"),
        Index(
            "ix_ai_reviews_strategy_pending",
            "strategy_public_id",
            "status",
        ),
        Index("ix_ai_reviews_user_public_id", "user_public_id"),
        Index("ix_ai_reviews_operator_public_id", "operator_public_id"),
        Index("ix_ai_reviews_wallet_public_id", "wallet_public_id"),
        Index("ix_ai_reviews_instrument_public_id", "instrument_public_id"),
        CheckConstraint(
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
        CheckConstraint("deadline > created_at", name="ck_ai_reviews_deadline_future"),
        CheckConstraint("dispatch_version >= 0", name="ck_ai_reviews_dispatch_version_nonneg"),
        CheckConstraint(
            "status IN ('pending', 'fanout_dispatched', 'resolved_approved', "
            "'resolved_rejected', 'timeout', 'superseded')",
            name="ck_ai_reviews_status_enum",
        ),
        CheckConstraint(
            "decision IS NULL OR decision IN ('approve', 'reject')",
            name="ck_ai_reviews_decision_enum",
        ),
        CheckConstraint(
            "resolution_mode IS NULL OR resolution_mode IN ("
            "'pick_one_primary', 'secondary_after_fanout', 'fanout_first_responder', "
            "'timeout_no_response', 'superseded_by_strategy')",
            name="ck_ai_reviews_resolution_mode_enum",
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    public_id: Mapped[str] = mapped_column(UUIDColumn(), nullable=False, default=_public_id)
    session_id: Mapped[str] = mapped_column(UUIDColumn(), nullable=False)
    sequence_id: Mapped[int] = mapped_column(Integer, nullable=False)
    user_public_id: Mapped[str] = mapped_column(UUIDColumn(), nullable=False)
    operator_public_id: Mapped[str] = mapped_column(UUIDColumn(), nullable=False)
    wallet_public_id: Mapped[str] = mapped_column(UUIDColumn(), nullable=False)
    instrument_public_id: Mapped[str] = mapped_column(UUIDColumn(), nullable=False)
    strategy_public_id: Mapped[str] = mapped_column(UUIDColumn(), nullable=False)
    selected_delegate_public_id: Mapped[str] = mapped_column(UUIDColumn(), nullable=False)
    responding_delegate_public_id: Mapped[str | None] = mapped_column(UUIDColumn(), nullable=True)
    resolution_mode: Mapped[str | None] = mapped_column(String(32), nullable=True)
    status: Mapped[str] = mapped_column(String(24), nullable=False, server_default="pending")
    signal_envelope: Mapped[JsonObject] = mapped_column(JSON(), nullable=False)
    signal_snapshot_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    instrument_metadata: Mapped[JsonObject] = mapped_column(JSON(), nullable=False)
    deadline: Mapped[datetime] = mapped_column(TZDateTime(), nullable=False)
    fanout_after: Mapped[datetime] = mapped_column(TZDateTime(), nullable=False)
    decision: Mapped[str | None] = mapped_column(String(8), nullable=True)
    rationale: Mapped[str | None] = mapped_column(String(4096), nullable=True)
    dispatch_version: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")
    counter_decremented_at: Mapped[datetime | None] = mapped_column(TZDateTime(), nullable=True)
    created_at: Mapped[datetime] = mapped_column(TZDateTime(), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(TZDateTime(), nullable=False)
    resolved_at: Mapped[datetime | None] = mapped_column(TZDateTime(), nullable=True)


class AiReviewEvent(Base):
    """Append-only audit log for every :class:`AiReview` transition.

    Replaces SCD2 history that ``TemporalMixin`` would have
    provided. Each transition (created,
    fanout_dispatched, decision_recorded, timeout_marked,
    superseded, counter_decremented, counter_adjusted) appends ONE
    row. Rows are immutable — no UPDATE allowed.

    Read patterns:
    - Audit trail for review X: ``SELECT * FROM ai_review_events
      WHERE review_public_id = :pid ORDER BY occurred_at``.
    - Operator dashboard: ``SELECT event_type, COUNT(*) FROM
      ai_review_events WHERE occurred_at > :since GROUP BY``.

    Attributes:
        public_id: UUID7 event ID.
        review_public_id: Logical FK to :class:`AiReview` `.public_id`.
        event_type: Transition name (see
            :class:`AiReviewEventTypeEnum`).
        actor_delegate_public_id: Which delegate triggered the
            event. NULL for reaper / strategy-supersede events
            (Sonnet m1 v1.3 — event_type fully discriminates).
        previous_status: ``ai_reviews.status`` before this event.
            NULL acceptable for reaper-driven transitions.
        new_status: ``ai_reviews.status`` after this event.
        payload: Event-specific JSON data.
        occurred_at: Wall-clock when event was appended.
    """

    __tablename__ = "ai_review_events"
    __table_args__ = (
        UniqueConstraint("public_id", name="uq_ai_review_events_public_id"),
        Index("ix_ai_review_events_review_chrono", "review_public_id", "occurred_at"),
        Index("ix_ai_review_events_type_chrono", "event_type", "occurred_at"),
        CheckConstraint(
            "event_type IN ('created', 'fanout_dispatched', 'decision_recorded', "
            "'timeout_marked', 'superseded', 'counter_decremented', 'counter_adjusted')",
            name="ck_ai_review_events_type_enum",
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    public_id: Mapped[str] = mapped_column(UUIDColumn(), nullable=False, default=_public_id)
    review_public_id: Mapped[str] = mapped_column(UUIDColumn(), nullable=False)
    event_type: Mapped[str] = mapped_column(String(32), nullable=False)
    actor_delegate_public_id: Mapped[str | None] = mapped_column(UUIDColumn(), nullable=True)
    previous_status: Mapped[str | None] = mapped_column(String(24), nullable=True)
    new_status: Mapped[str] = mapped_column(String(24), nullable=False)
    payload: Mapped[JsonObject] = mapped_column(JSON(), nullable=False, server_default="{}")
    occurred_at: Mapped[datetime] = mapped_column(TZDateTime(), nullable=False)
