"""SQLAlchemy ORM models for Snapper persistence."""

from collections.abc import Callable
from datetime import UTC
from datetime import datetime
from decimal import Decimal
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
    preserve (operator decision 2026-05-16: Postgres is production,
    native types where possible, SQLite converts).
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


class ExactDecimalNumeric(Numeric[Decimal]):
    """Native Numeric on PostgreSQL with exact Decimal text storage on SQLite.

    SQLite applies NUMERIC affinity by converting ordinary decimal text to an
    IEEE-754 value, which loses precision before SQLAlchemy can reconstruct a
    Decimal. A non-numeric suffix keeps the storage class as text while the
    declared column type remains NUMERIC(38,18). Result processing removes the
    suffix. PostgreSQL uses SQLAlchemy's native Numeric processors unchanged.
    """

    def bind_processor(self, dialect: Dialect) -> Callable[[Any], Any] | None:
        """Return the dialect-specific exact Decimal bind processor."""
        if dialect.name == "sqlite":
            return lambda value: None if value is None else f"{value}d"
        return self._native_bind

    def result_processor(
        self,
        dialect: Dialect,
        coltype: object,
    ) -> Callable[[Any], Any] | None:
        """Return the dialect-specific exact Decimal result processor."""
        if dialect.name == "sqlite":
            return self._sqlite_result
        return self._native_result

    @staticmethod
    def _native_bind(value: object) -> object:
        """Pass a Decimal directly to a native Numeric database driver."""
        return value

    @staticmethod
    def _native_result(value: object) -> Decimal | None:
        """Normalize a native Numeric result to Decimal."""
        if value is None:
            return None
        if isinstance(value, Decimal):
            return value
        return Decimal(str(value))

    @staticmethod
    def _sqlite_result(value: object) -> Decimal | None:
        """Decode one SQLite exact-text value or a legacy numeric value."""
        if value is None:
            return None
        raw = str(value)
        if raw.endswith("d"):
            raw = raw[:-1]
        return Decimal(raw)


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
    "SymbolMarketDataChannelCapability",
    "ProcessRun",
    "InstrumentSpec",
    "UnderlyingAsset",
    "InstrumentUnderlyingMapping",
    "MarketSnapshot",
    "Control",
    "Telemetry",
    "TradeCommand",
    "VenueEvent",
    "PairedExecutionGroup",
    "PairedExecutionLeg",
    "PairedExecutionHalt",
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
    "ExecutionPlanDecisionOutbox",
    "PositionCycle",
    "UserTradingCaps",
    "UserActiveToken",
    "NotificationDevice",
    "DeviceAlertPref",
    "UserAlertDefault",
    "AlertEvent",
    "AlertDelivery",
    "InstrumentFeedHealth",
    "PortfolioSpotReconciliationAnchor",
]


_CK_EXCHANGE_LOWER = "exchange = LOWER(exchange)"
_CK_MODE_LIVE_PAPER = "mode IN ('live', 'paper')"
_CK_SIDE_BUY_SELL = "side IN ('buy', 'sell')"
_CK_ORDER_TYPE_VALUES = (
    "order_type IN ('market', 'limit', 'stop', 'stop_limit', 'stop-loss', "
    "'stop-loss-limit', 'take-profit', 'trailing-stop', 'iceberg', 'settle-position')"
)
"""order_type vocabulary CHECK shared by orders + trade_commands.

Dual-era and varchar(16)-bounded: post-#156 writers emit CORE values
(market/limit/stop/stop_limit), pre-#156 persisted raw wire
ExchangeOrderTypeEnum spellings. The two over-length wire members
(``take-profit-limit``/``trailing-stop-limit``) cannot fit the
``String(16)`` column on PostgreSQL, so column width — not this CHECK —
excludes them; the CHECK lists only the ten that fit.
"""
_CK_ORDERS_STATUS_WIRE = (
    "status IN ('pending', 'open', 'closed', 'canceled', 'expired', "
    "'pending_new', 'new', 'partially_filled', 'filled')"
)
"""orders.status carries WIRE values (ExchangeOrderStatusEnum, American
spelling — ``canceled`` not ``cancelled``), persisted from
``order.status.value`` where order is an ExchangeOrderSnapshot."""
_CK_EXECUTIONS_STATUS = "status IN ('filled', 'partial')"
"""executions.status carries DOMAIN FillStatusEnum values."""
_CK_TRADE_COMMAND_TYPE = "command_type IN ('create', 'submit', 'cancel', 'replace')"
"""Intentional dual vocabulary: REST/MCP/plan write 'create', engine/guard
write 'submit', cancel paths 'cancel', OrderCommandEnum 'replace'."""
_CK_TRADE_COMMAND_STATUS = (
    "status IN ('created', 'dispatched', 'direct_dispatched', 'accepted', "
    "'filled', 'partially_filled', 'rejected', 'cancelled', 'expired', 'failed')"
)
"""trade_commands.status carries TradeCommandStatusEnum (DOMAIN spelling —
``cancelled``, distinct from the wire ``canceled`` in orders.status)."""
_CK_PAIRING_MODE = "pairing_mode IN ('auto', 'manual')"
_KNOWN_TO_ACTIVE_PG = text("known_to = '9999-12-31T23:59:59+00:00'")
_KNOWN_TO_ACTIVE_SQLITE = text("known_to = '9999-12-31 23:59:59.000000'")
_PAIRED_GROUP_ID_NOT_NULL = text("paired_group_id IS NOT NULL")
_NOTIFICATION_DEVICE_ACTIVE_PG = text(
    "known_to = '9999-12-31T23:59:59+00:00' AND token_status = 'active'"
)
_NOTIFICATION_DEVICE_ACTIVE_SQLITE = text(
    "known_to = '9999-12-31 23:59:59.000000' AND token_status = 'active'"
)
_CK_NOTIFICATION_DEVICE_TOKEN_STATUS = (
    "token_status IN ('active', 'unregistered', 'user_unregistered')"
)
_CK_CHANNEL_LOWER_NON_EMPTY = "channel = LOWER(channel) AND LENGTH(channel) > 0"
_CK_VENUE_ACCOUNT_ATTEMPT_STATUS = (
    "attempt_status IN ('observed', 'simulated', 'unsupported', 'error')"
)
"""Overall outcome of one venue account-observation attempt (PnL Phase 3).

``observed`` = a real venue read succeeded; ``simulated`` = paper (fiction,
never reconciled); ``unsupported`` = the venue is market-data-only and cannot
be account-tracked (structural NotImplementedError); ``error`` = the attempt
failed (auth/permission/timeout/transport/parse). The forbidden outcome is
labeling a simulated/unsupported/errored attempt ``observed``."""
_CK_VENUE_ACCOUNT_SYNC_STATUS = "sync_status IN ('observed', 'simulated', 'unsupported', 'error')"
"""Stored roll-up status of the current venue_account_states row.

Distinct from the EFFECTIVE read status, which additionally derives
``stale``/``unobserved``/``corrupt``/``clock_error`` at read time — a stored
``observed`` row past its ``authoritative_until`` is served as stale, never as
live truth."""
_CK_VENUE_ACCOUNT_BALANCE_STATUS = (
    "balance_status IN ('observed', 'simulated', 'unsupported', 'error')"
)
"""Per-component status for the balance read (independent of positions)."""
_CK_VENUE_ACCOUNT_POSITION_STATUS = (
    "position_status IN ('observed', 'unsupported', 'not_applicable', 'error')"
)
"""Per-component status for the open-positions read.

``not_applicable`` = the venue structurally has no positions concept
(spot/FX/paper) — a KNOWN, benign state, never conflated with ``unsupported``
(cannot determine) or ``error`` (read failed)."""
_CK_VENUE_ACCOUNT_VALUATION_STATUS = "valuation_status IN ('native_only')"
"""Phase 3 stores NATIVE balances/positions only — zero USD math (operator
directive: fail-closed on futures USD valuation until the Kraken
contract-multiplier/balanceValue semantics are verified). USD is Phase 5."""
_CK_VENUE_ACCOUNT_SIMULATED_PAPER = (
    "(sync_status != 'simulated' AND balance_status != 'simulated') OR mode = 'paper'"
)
"""A ``simulated`` status can only ride a paper row — a live account is never
fiction."""
_CK_VENUE_ACCOUNT_BALANCE_OBSERVED_AT = (
    "balance_status != 'observed' OR balance_observed_at IS NOT NULL"
)
"""An ``observed`` balance MUST carry the venue observation timestamp — a
timestamp-less ``observed`` would be indistinguishable from a fabrication."""
_CK_VENUE_ACCOUNT_OBSERVED_BALANCE = "sync_status != 'observed' OR balance_status = 'observed'"
"""An ``observed`` roll-up requires the balance component to be genuinely
observed — the roll-up can never claim truth a simulated/errored/unsupported
balance did not provide."""
_CK_VENUE_ACCOUNT_OBSERVED_POSITION = (
    "sync_status != 'observed' OR position_status IN ('observed', 'not_applicable')"
)
"""An ``observed`` roll-up requires positions to be observed or structurally
absent (``not_applicable``) — an errored/unsupported position read can never
ride an ``observed`` account."""
_CK_VENUE_ACCOUNT_OBSERVED_AUTHORITY = (
    "sync_status != 'observed' OR authoritative_until IS NOT NULL"
)
"""An ``observed`` row MUST carry an authority window — without one the read
layer cannot expire it and would serve it as live truth forever."""
_CK_VENUE_ACCOUNT_OBS_SIMULATED_PAPER = (
    "(attempt_status != 'simulated' AND balance_status != 'simulated') OR mode = 'paper'"
)
"""A ``simulated`` observation attempt can only ride a paper row — a live
account attempt is never fiction."""
_CK_VENUE_ACCOUNT_OBS_OBSERVED_BALANCE = (
    "attempt_status != 'observed' OR balance_status = 'observed'"
)
"""An ``observed`` attempt roll-up requires the balance component observed."""
_CK_VENUE_ACCOUNT_OBS_OBSERVED_POSITION = (
    "attempt_status != 'observed' OR position_status IN ('observed', 'not_applicable')"
)
"""An ``observed`` attempt roll-up requires positions observed or n/a."""
_CK_VENUE_ACCOUNT_BALANCE_JSON_PRESENT = (
    "balance_status NOT IN ('observed', 'simulated') OR balances_json IS NOT NULL"
)
"""An ``observed`` or ``simulated`` balance MUST carry its JSON payload — a
genuinely empty account is an empty array, never NULL. Without this a
materially empty snapshot could stand as authoritative ``observed`` truth."""
_CK_VENUE_ACCOUNT_POSITION_OBSERVED_PRESENT = (
    "position_status != 'observed' OR "
    "(open_positions_json IS NOT NULL AND position_observed_at IS NOT NULL)"
)
"""An ``observed`` positions component MUST carry both its JSON payload and its
observation timestamp — an empty derivatives book is an empty array with a
timestamp, never NULLs served as observed."""
_CK_VENUE_ACCOUNT_BALANCE_PAYLOAD_SOURCE = (
    "(balances_json IS NULL AND balance_payload_source_observation_id IS NULL) OR "
    "(balances_json IS NOT NULL AND balance_payload_source_observation_id IS NOT NULL)"
)
"""A displayed balance payload and its provenance are inseparable — a balance
JSON without a source observation id (or a source id pointing at no payload) is
forged provenance, so the two are both-null or both-non-null."""
_CK_VENUE_ACCOUNT_POSITION_PAYLOAD_SOURCE = (
    "(open_positions_json IS NULL AND position_payload_source_observation_id IS NULL) OR "
    "(open_positions_json IS NOT NULL AND position_payload_source_observation_id IS NOT NULL)"
)
"""A displayed positions payload and its provenance are inseparable (see the
balance rule)."""
_CK_VENUE_ACCOUNT_BALANCE_FRESH_SOURCE = (
    "balance_status NOT IN ('observed', 'simulated') OR "
    "balance_payload_source_observation_id = current_attempt_observation_id"
)
"""A freshly observed/simulated balance's payload source MUST be this very
attempt — a fresh read can never be attributed to an earlier observation."""
_CK_VENUE_ACCOUNT_POSITION_FRESH_SOURCE = (
    "position_status != 'observed' OR "
    "position_payload_source_observation_id = current_attempt_observation_id"
)
"""A freshly observed positions component's payload source MUST be this very
attempt."""
_CK_RECONCILIATION_METHOD = "method IN ('futures_position', 'spot_execution_replay')"
_CK_RECONCILIATION_EVALUATION_STATUS = (
    "evaluation_status IN ('matched', 'mismatched', 'incomplete', 'unsupported', 'error')"
)
_CK_RECONCILIATION_CURRENT_STATUS = (
    "current_evaluation_status IN ('matched', 'mismatched', 'incomplete', 'unsupported', 'error')"
)
_CK_RECONCILIATION_LAST_OUTCOME = (
    "last_full_outcome IS NULL OR last_full_outcome IN ('matched', 'mismatched')"
)
_CK_RECONCILIATION_WATERMARK_PAIR = (
    "(source_watermark IS NULL AND source_watermark_kind IS NULL) OR "
    "(source_watermark IS NOT NULL AND source_watermark_kind IS NOT NULL)"
)
_CK_RECONCILIATION_OBSERVATION_FULL_EVIDENCE = (
    "evaluation_status NOT IN ('matched', 'mismatched') OR "
    "(venue_account_state_public_id IS NOT NULL AND "
    "venue_account_observation_id IS NOT NULL AND "
    "account_authoritative_until IS NOT NULL AND source_watermark IS NOT NULL AND "
    "source_watermark_kind IS NOT NULL AND expected_json IS NOT NULL AND "
    "actual_json IS NOT NULL AND difference_json IS NOT NULL AND tolerance_json IS NOT NULL)"
)
_CK_RECONCILIATION_OBSERVATION_MATCHED = (
    "evaluation_status != 'matched' OR "
    "(resulting_full_mismatch_count = 0 AND drift_episode_public_id IS NULL AND error IS NULL)"
)
_CK_RECONCILIATION_OBSERVATION_MISMATCHED = (
    "evaluation_status != 'mismatched' OR resulting_full_mismatch_count >= 1"
)
_CK_RECONCILIATION_EPISODE_THRESHOLD = (
    "(resulting_full_mismatch_count < 3 AND drift_episode_public_id IS NULL) OR "
    "(resulting_full_mismatch_count >= 3 AND drift_episode_public_id IS NOT NULL)"
)
_CK_RECONCILIATION_SPOT_ANCHOR = (
    "method != 'spot_execution_replay' OR "
    "evaluation_status NOT IN ('matched', 'mismatched') OR anchor_public_id IS NOT NULL"
)
_CK_RECONCILIATION_ERROR_TEXT = (
    "evaluation_status != 'error' OR (error IS NOT NULL AND LENGTH(TRIM(error)) > 0)"
)
_CK_RECONCILIATION_STATE_ERROR_TEXT = (
    "current_evaluation_status != 'error' OR (error IS NOT NULL AND LENGTH(TRIM(error)) > 0)"
)
_CK_RECONCILIATION_ERROR_LENGTH = "error IS NULL OR LENGTH(error) <= 512"
_CK_RECONCILIATION_STATE_DETAIL = (
    "(detail_source_observation_id IS NULL AND last_full_observation_id IS NULL AND "
    "last_full_outcome IS NULL AND venue_account_state_public_id IS NULL AND "
    "venue_account_observation_id IS NULL AND anchor_public_id IS NULL AND "
    "source_watermark_kind IS NULL AND "
    "source_watermark IS NULL AND expected_json IS NULL AND actual_json IS NULL AND "
    "difference_json IS NULL AND tolerance_json IS NULL AND reconciled_at IS NULL AND "
    "authoritative_until IS NULL) OR "
    "(detail_source_observation_id IS NOT NULL AND "
    "last_full_observation_id IS NOT NULL AND "
    "detail_source_observation_id = last_full_observation_id AND "
    "last_full_outcome IS NOT NULL AND venue_account_state_public_id IS NOT NULL AND "
    "venue_account_observation_id IS NOT NULL AND source_watermark_kind IS NOT NULL AND "
    "source_watermark IS NOT NULL AND expected_json IS NOT NULL AND actual_json IS NOT NULL AND "
    "difference_json IS NOT NULL AND tolerance_json IS NOT NULL AND reconciled_at IS NOT NULL AND "
    "authoritative_until IS NOT NULL)"
)
_CK_RECONCILIATION_STATE_CURRENT_FULL = (
    "current_evaluation_status NOT IN ('matched', 'mismatched') OR "
    "(last_full_observation_id IS NOT NULL AND "
    "detail_source_observation_id IS NOT NULL AND "
    "current_observation_id = last_full_observation_id AND "
    "current_observation_id = detail_source_observation_id AND "
    "last_full_outcome IS NOT NULL AND current_evaluation_status = last_full_outcome AND "
    "venue_account_state_public_id IS NOT NULL AND "
    "venue_account_observation_id IS NOT NULL AND "
    "source_watermark_kind IS NOT NULL AND source_watermark IS NOT NULL AND "
    "expected_json IS NOT NULL AND actual_json IS NOT NULL AND "
    "difference_json IS NOT NULL AND tolerance_json IS NOT NULL)"
)
_CK_RECONCILIATION_STATE_MATCHED = (
    "(last_full_outcome IS NULL OR last_full_outcome != 'matched' OR "
    "(consecutive_full_mismatches = 0 AND open_drift_episode_public_id IS NULL)) AND "
    "(current_evaluation_status != 'matched' OR error IS NULL)"
)
_CK_RECONCILIATION_STATE_MISMATCHED = (
    "last_full_outcome IS NULL OR last_full_outcome != 'mismatched' OR "
    "consecutive_full_mismatches >= 1"
)
_CK_RECONCILIATION_STATE_NO_FULL = (
    "last_full_outcome IS NOT NULL OR "
    "(consecutive_full_mismatches = 0 AND open_drift_episode_public_id IS NULL)"
)
_CK_RECONCILIATION_STATE_EPISODE = (
    "(open_drift_episode_public_id IS NULL AND "
    "(last_full_outcome IS NULL OR last_full_outcome = 'matched' OR "
    "(last_full_outcome = 'mismatched' AND consecutive_full_mismatches < 3))) OR "
    "(open_drift_episode_public_id IS NOT NULL AND last_full_outcome IS NOT NULL AND "
    "last_full_outcome = 'mismatched' AND "
    "consecutive_full_mismatches >= 3)"
)
_CK_RECONCILIATION_STATE_SPOT_ANCHOR = (
    "method != 'spot_execution_replay' OR "
    "current_evaluation_status NOT IN ('matched', 'mismatched') OR anchor_public_id IS NOT NULL"
)
_CK_DRIFT_EPISODE_STATUS = "status IN ('open', 'resolved', 'rebased')"
_CK_DRIFT_EPISODE_OBSERVATION_ORDER = "last_observation_id >= trigger_observation_id"
_CK_DRIFT_EPISODE_DETAIL_ORDER = (
    "details_source_observation_id >= trigger_observation_id AND "
    "details_source_observation_id <= last_observation_id"
)
_CK_DRIFT_EPISODE_CLOSED_ORDER = "closed_at IS NULL OR closed_at >= opened_at"
_CK_DRIFT_EPISODE_MISMATCH_COUNT = "latest_full_mismatch_count >= 3"
_CK_DRIFT_EPISODE_OPEN = (
    "status != 'open' OR (closed_at IS NULL AND resolution_reason IS NULL AND "
    "closed_by_user_public_id IS NULL AND closed_by_operator_public_id IS NULL AND "
    "rebase_anchor_public_id IS NULL)"
)
_CK_DRIFT_EPISODE_RESOLVED = (
    "status != 'resolved' OR (closed_at IS NOT NULL AND resolution_reason IS NOT NULL AND "
    "resolution_reason = 'matched' AND "
    "closed_by_user_public_id IS NULL AND closed_by_operator_public_id IS NULL AND "
    "rebase_anchor_public_id IS NULL)"
)
_CK_DRIFT_EPISODE_REBASED = (
    "status != 'rebased' OR (closed_at IS NOT NULL AND "
    "resolution_reason IS NOT NULL AND resolution_reason = 'operator_rebase' AND "
    "((closed_by_user_public_id IS NOT NULL AND closed_by_operator_public_id IS NULL) OR "
    "(closed_by_user_public_id IS NULL AND closed_by_operator_public_id IS NOT NULL)) AND "
    "rebase_anchor_public_id IS NOT NULL)"
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
        CheckConstraint(
            "source_exchange IS NULL OR "
            "(source_exchange = LOWER(source_exchange) AND exchange = 'paper')",
            name="ck_instrument_source_exchange",
        ),
        Index("ix_instruments_exchange", "exchange"),
    )
    symbol_public_id: Mapped[str] = mapped_column(UUIDColumn(), index=True)
    exchange: Mapped[str] = mapped_column(String(32))
    source_exchange: Mapped[str | None] = mapped_column(String(32), nullable=True)
    """Source-venue identity a PAPER instrument replays/prices from.

    Authored by the source-bound paper publisher (it knows which real
    venue its frames come from — the trader's settings view may
    differ). NULL for every non-paper instrument (CHECK-enforced) and
    for paper instruments minted before the mapping existed. The
    canonical source→paper identity map for cap keys and USD valuation
    resolves through this column (PnL Phase 1)."""
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
    sequence at production write rates.
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
        CheckConstraint(
            "source IN ('native', 'calculated', 'synthesized')", name="ck_candle_source"
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
    source: Mapped[str] = mapped_column(String(16), server_default="native")
    complete: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default=text("true"))


class ShadowCandle(TemporalMixin, Base):
    """SQLAlchemy model for shadow OHLCV candlestick A/B rows."""

    __tablename__ = "shadow_candles"
    __table_args__ = (
        Index(
            "uq_shadow_candle_itf_open_source",
            "instrument_public_id",
            "timeframe",
            "open_at",
            "source",
            unique=True,
            sqlite_where=_KNOWN_TO_ACTIVE_SQLITE,
            postgresql_where=_KNOWN_TO_ACTIVE_PG,
        ),
        Index(
            "ix_shadow_candle_instrument_open_source", "instrument_public_id", "open_at", "source"
        ),
        Index(
            "ix_shadow_candles_public_id",
            "public_id",
            unique=True,
            sqlite_where=_KNOWN_TO_ACTIVE_SQLITE,
            postgresql_where=_KNOWN_TO_ACTIVE_PG,
        ),
        CheckConstraint(
            "source IN ('native', 'calculated', 'synthesized')",
            name="ck_shadow_candle_source",
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
    source: Mapped[str] = mapped_column(String(16), server_default="native")
    complete: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default=text("true"))


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
        Index("ix_trades_executed_at", "executed_at"),
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
    consistency with the other high-write tables.
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
        CheckConstraint(_CK_MODE_LIVE_PAPER, name="ck_orders_mode"),
        CheckConstraint(_CK_SIDE_BUY_SELL, name="ck_orders_side"),
        CheckConstraint(_CK_ORDER_TYPE_VALUES, name="ck_orders_order_type"),
        CheckConstraint(_CK_ORDERS_STATUS_WIRE, name="ck_orders_status"),
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
    consistency with the other high-write tables.
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
        Index(
            "ix_executions_wallet_ts",
            "wallet_public_id",
            "timestamp",
            sqlite_where=_KNOWN_TO_ACTIVE_SQLITE,
            postgresql_where=_KNOWN_TO_ACTIVE_PG,
        ),
        CheckConstraint(_CK_SIDE_BUY_SELL, name="ck_executions_side"),
        CheckConstraint(_CK_EXECUTIONS_STATUS, name="ck_executions_status"),
        CheckConstraint(
            "numeric_provenance IS NULL OR numeric_provenance IN ('venue_raw', 'legacy_float')",
            name="ck_executions_numeric_provenance",
        ),
        CheckConstraint(
            "(price_decimal IS NULL OR LENGTH(TRIM(price_decimal)) > 0) AND "
            "(size_decimal IS NULL OR LENGTH(TRIM(size_decimal)) > 0) AND "
            "(fee_decimal IS NULL OR LENGTH(TRIM(fee_decimal)) > 0) AND "
            "((price_decimal IS NULL AND size_decimal IS NULL AND fee_decimal IS NULL) OR "
            "numeric_provenance IS NOT NULL)",
            name="ck_executions_raw_decimals",
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
    price_decimal: Mapped[str | None] = mapped_column(Text, nullable=True)
    size_decimal: Mapped[str | None] = mapped_column(Text, nullable=True)
    fee_decimal: Mapped[str | None] = mapped_column(Text, nullable=True)
    numeric_provenance: Mapped[str | None] = mapped_column(String(16), nullable=True)
    executed_at: Mapped[datetime | None] = mapped_column(TZDateTime())
    liquidity_role: Mapped[str] = mapped_column(String(16), default="unknown")


class Position(TemporalMixin, Base):
    """Truthful position projection per (instrument, mode, wallet) identity.

    PnL Phase 2: written by the trader after every committed checkpoint
    (fill and funding paths) and rebuilt on recovery — before Phase 2 the
    table had NO production writer and the read surface served an empty
    projection. One SCD2 active row per identity; paper strategy-tag
    shards aggregate into the identity (quantity is the fsum of shard
    quantities, ``average_price`` is the absolute-quantity-weighted VWAP
    of same-direction shards and NULL when directions oppose or any
    non-flat shard lacks an entry).

    Provenance columns are stale-VISIBLE, never faked current:
    ``mark_price``/``marked_at`` echo the active market snapshot
    (``marked_at`` is the snapshot's own bus timestamp, no age gate) and
    all three of ``mark_price``/``marked_at``/``unrealized_pnl`` are NULL
    when no usable mark exists — a previous mark is never carried
    forward. ``source_venue_event_id`` is the maximum durable
    venue-event WATERMARK consumed into this state, not the exact causal
    fill: recovery may advance it over non-fill lifecycle events and
    funding changes ``realized_pnl`` without advancing it (funding
    provenance lives in the accrual ledger). ``realized_pnl`` is
    execution-fee-exclusive but funding-inclusive, mirroring
    TradeService semantics.
    """

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
    average_price: Mapped[float | None] = mapped_column(Float, nullable=True)
    unrealized_pnl: Mapped[float | None] = mapped_column(Float, nullable=True)
    realized_pnl: Mapped[float] = mapped_column(Float)
    mark_price: Mapped[float | None] = mapped_column(Float, nullable=True)
    marked_at: Mapped[datetime | None] = mapped_column(TZDateTime(), nullable=True)
    source_venue_event_id: Mapped[int | None] = mapped_column(Integer, nullable=True)


class VenueAccountObservation(TemporalMixin, Base):
    """Append-only log of every venue account-observation ATTEMPT (Phase 3).

    A dedicated truth plane, DISJOINT from the order-lifecycle
    ``venue_events`` table and the fill-derived ``positions`` projection:
    account snapshots are never folded into a shard, never touch
    TradeService, and never seed recovery. Each row records one poll
    attempt by the per-wallet account observer — including failures and
    unsupported venues — so authority can never be silently invented.
    Balance and open-position reads are SEPARATE venue calls (not an
    atomic snapshot), so their statuses and observation timestamps are
    tracked independently. ``balances_json``/``open_positions_json`` are
    NULL unless the corresponding component was actually ``observed`` or
    ``simulated``. Identity is the FULL ``wallet_public_id`` (UUID), never
    the 12-hex suffix.
    """

    __tablename__ = "venue_account_observations"
    __table_args__ = (
        Index(
            "ix_venue_account_observations_public_id",
            "public_id",
            unique=True,
            sqlite_where=_KNOWN_TO_ACTIVE_SQLITE,
            postgresql_where=_KNOWN_TO_ACTIVE_PG,
        ),
        Index(
            "ix_venue_account_observations_identity",
            "wallet_public_id",
            "exchange",
            "mode",
        ),
        CheckConstraint(_CK_EXCHANGE_LOWER, name="ck_venue_account_obs_exchange_lower"),
        CheckConstraint(_CK_MODE_LIVE_PAPER, name="ck_venue_account_obs_mode"),
        CheckConstraint(
            _CK_VENUE_ACCOUNT_ATTEMPT_STATUS, name="ck_venue_account_obs_attempt_status"
        ),
        CheckConstraint(
            _CK_VENUE_ACCOUNT_BALANCE_STATUS, name="ck_venue_account_obs_balance_status"
        ),
        CheckConstraint(
            _CK_VENUE_ACCOUNT_POSITION_STATUS, name="ck_venue_account_obs_position_status"
        ),
        CheckConstraint(
            _CK_VENUE_ACCOUNT_BALANCE_OBSERVED_AT,
            name="ck_venue_account_obs_balance_observed_at",
        ),
        CheckConstraint(
            _CK_VENUE_ACCOUNT_OBS_SIMULATED_PAPER,
            name="ck_venue_account_obs_simulated_paper",
        ),
        CheckConstraint(
            _CK_VENUE_ACCOUNT_OBS_OBSERVED_BALANCE,
            name="ck_venue_account_obs_observed_balance",
        ),
        CheckConstraint(
            _CK_VENUE_ACCOUNT_OBS_OBSERVED_POSITION,
            name="ck_venue_account_obs_observed_position",
        ),
        CheckConstraint(
            _CK_VENUE_ACCOUNT_BALANCE_JSON_PRESENT,
            name="ck_venue_account_obs_balance_json_present",
        ),
        CheckConstraint(
            _CK_VENUE_ACCOUNT_POSITION_OBSERVED_PRESENT,
            name="ck_venue_account_obs_position_observed_present",
        ),
    )
    wallet_public_id: Mapped[str] = mapped_column(UUIDColumn(), nullable=False)
    exchange: Mapped[str] = mapped_column(String(32))
    mode: Mapped[str] = mapped_column(String(8), default="live", server_default="live")
    attempt_status: Mapped[str] = mapped_column(String(16))
    balance_status: Mapped[str] = mapped_column(String(16))
    position_status: Mapped[str] = mapped_column(String(16))
    balances_json: Mapped[str | None] = mapped_column(Text, nullable=True)
    open_positions_json: Mapped[str | None] = mapped_column(Text, nullable=True)
    balance_observed_at: Mapped[datetime | None] = mapped_column(TZDateTime(), nullable=True)
    position_observed_at: Mapped[datetime | None] = mapped_column(TZDateTime(), nullable=True)
    error: Mapped[str | None] = mapped_column(String(512), nullable=True)


class VenueAccountState(TemporalMixin, Base):
    """Current venue account truth per (wallet, exchange, mode) — SCD2 (Phase 3).

    One active SCD2 row per identity, materialized ATOMICALLY with the
    ``VenueAccountObservation`` that produced it. Authority-driving fields
    are first-class CHECK-constrained columns (never buried in JSON): a
    ``simulated`` status can only ride a paper row, an ``observed`` balance
    must carry its observation timestamp, and ``valuation_status`` is
    ``native_only`` in Phase 3 (zero USD math). The stored ``sync_status``
    is the raw attempt outcome; the read layer derives the EFFECTIVE status
    (``stale`` past ``authoritative_until``, ``clock_error`` on future-dated
    clocks) so a stale row is never served as live truth.

    ``current_attempt_observation_id`` is the latest attempt (never NULL — every
    state comes from an attempt). Balance and positions are INDEPENDENT reads, so
    each carries its own retained-payload provenance:
    ``balance_payload_source_observation_id`` and
    ``position_payload_source_observation_id`` each point at the observation
    whose balance / positions JSON is currently displayed — the attempt's own on
    a fresh read, an earlier successful observation when that component was
    retained. Separate per-component provenance keeps a retained payload from
    masquerading as fresh and never conflates a fresh balance with stale
    positions. This plane NEVER overwrites the fill-derived ``positions``
    projection; reconciling the two is Phase 4.
    """

    __tablename__ = "venue_account_states"
    __table_args__ = (
        Index(
            "uq_venue_account_states_identity",
            "wallet_public_id",
            "exchange",
            "mode",
            unique=True,
            sqlite_where=_KNOWN_TO_ACTIVE_SQLITE,
            postgresql_where=_KNOWN_TO_ACTIVE_PG,
        ),
        Index(
            "ix_venue_account_states_public_id",
            "public_id",
            unique=True,
            sqlite_where=_KNOWN_TO_ACTIVE_SQLITE,
            postgresql_where=_KNOWN_TO_ACTIVE_PG,
        ),
        Index("ix_venue_account_states_wallet", "wallet_public_id"),
        CheckConstraint(_CK_EXCHANGE_LOWER, name="ck_venue_account_states_exchange_lower"),
        CheckConstraint(_CK_MODE_LIVE_PAPER, name="ck_venue_account_states_mode"),
        CheckConstraint(_CK_VENUE_ACCOUNT_SYNC_STATUS, name="ck_venue_account_states_sync_status"),
        CheckConstraint(
            _CK_VENUE_ACCOUNT_BALANCE_STATUS, name="ck_venue_account_states_balance_status"
        ),
        CheckConstraint(
            _CK_VENUE_ACCOUNT_POSITION_STATUS,
            name="ck_venue_account_states_position_status",
        ),
        CheckConstraint(
            _CK_VENUE_ACCOUNT_VALUATION_STATUS,
            name="ck_venue_account_states_valuation_status",
        ),
        CheckConstraint(
            _CK_VENUE_ACCOUNT_SIMULATED_PAPER,
            name="ck_venue_account_states_simulated_paper",
        ),
        CheckConstraint(
            _CK_VENUE_ACCOUNT_BALANCE_OBSERVED_AT,
            name="ck_venue_account_states_balance_observed_at",
        ),
        CheckConstraint(
            _CK_VENUE_ACCOUNT_OBSERVED_BALANCE,
            name="ck_venue_account_states_observed_balance",
        ),
        CheckConstraint(
            _CK_VENUE_ACCOUNT_OBSERVED_POSITION,
            name="ck_venue_account_states_observed_position",
        ),
        CheckConstraint(
            _CK_VENUE_ACCOUNT_OBSERVED_AUTHORITY,
            name="ck_venue_account_states_observed_authority",
        ),
        CheckConstraint(
            _CK_VENUE_ACCOUNT_BALANCE_JSON_PRESENT,
            name="ck_venue_account_states_balance_json_present",
        ),
        CheckConstraint(
            _CK_VENUE_ACCOUNT_POSITION_OBSERVED_PRESENT,
            name="ck_venue_account_states_position_observed_present",
        ),
        CheckConstraint(
            _CK_VENUE_ACCOUNT_BALANCE_PAYLOAD_SOURCE,
            name="ck_venue_account_states_balance_payload_source",
        ),
        CheckConstraint(
            _CK_VENUE_ACCOUNT_POSITION_PAYLOAD_SOURCE,
            name="ck_venue_account_states_position_payload_source",
        ),
        CheckConstraint(
            _CK_VENUE_ACCOUNT_BALANCE_FRESH_SOURCE,
            name="ck_venue_account_states_balance_fresh_source",
        ),
        CheckConstraint(
            _CK_VENUE_ACCOUNT_POSITION_FRESH_SOURCE,
            name="ck_venue_account_states_position_fresh_source",
        ),
    )
    wallet_public_id: Mapped[str] = mapped_column(UUIDColumn(), nullable=False)
    exchange: Mapped[str] = mapped_column(String(32))
    mode: Mapped[str] = mapped_column(String(8), default="live", server_default="live")
    sync_status: Mapped[str] = mapped_column(String(16))
    balance_status: Mapped[str] = mapped_column(String(16))
    position_status: Mapped[str] = mapped_column(String(16))
    valuation_status: Mapped[str] = mapped_column(
        String(16), default="native_only", server_default="native_only"
    )
    balances_json: Mapped[str | None] = mapped_column(Text, nullable=True)
    open_positions_json: Mapped[str | None] = mapped_column(Text, nullable=True)
    balance_observed_at: Mapped[datetime | None] = mapped_column(TZDateTime(), nullable=True)
    position_observed_at: Mapped[datetime | None] = mapped_column(TZDateTime(), nullable=True)
    current_attempt_observation_id: Mapped[int] = mapped_column(Integer, nullable=False)
    balance_payload_source_observation_id: Mapped[int | None] = mapped_column(
        Integer, nullable=True
    )
    position_payload_source_observation_id: Mapped[int | None] = mapped_column(
        Integer, nullable=True
    )
    authoritative_until: Mapped[datetime | None] = mapped_column(TZDateTime(), nullable=True)
    error: Mapped[str | None] = mapped_column(String(512), nullable=True)


class PortfolioSpotReconciliationAnchor(TemporalMixin, Base):
    """Immutable exact bootstrap inventory for live spot reconciliation."""

    __tablename__ = "portfolio_spot_reconciliation_anchors"
    __table_args__ = (
        Index(
            "uq_portfolio_spot_reconciliation_anchors_identity",
            "wallet_public_id",
            "exchange",
            "mode",
            unique=True,
            sqlite_where=_KNOWN_TO_ACTIVE_SQLITE,
            postgresql_where=_KNOWN_TO_ACTIVE_PG,
        ),
        Index(
            "ix_portfolio_spot_reconciliation_anchors_public_id",
            "public_id",
            unique=True,
            sqlite_where=_KNOWN_TO_ACTIVE_SQLITE,
            postgresql_where=_KNOWN_TO_ACTIVE_PG,
        ),
        CheckConstraint(_CK_EXCHANGE_LOWER, name="ck_portfolio_spot_anchor_exchange_lower"),
        CheckConstraint("mode = 'live'", name="ck_portfolio_spot_anchor_mode"),
        CheckConstraint(
            "source_watermark_kind = 'execution_id' AND source_watermark >= 0",
            name="ck_portfolio_spot_anchor_watermark",
        ),
        CheckConstraint(
            "LENGTH(TRIM(balances_json)) > 0 AND LENGTH(TRIM(provenance)) > 0",
            name="ck_portfolio_spot_anchor_evidence_text",
        ),
        CheckConstraint("balance_observation_id > 0", name="ck_portfolio_spot_anchor_observation"),
        CheckConstraint(
            "first_request_completed_at >= first_request_started_at AND "
            "second_request_started_at >= first_request_completed_at AND "
            "second_request_completed_at >= second_request_started_at AND "
            "timestamp >= second_request_completed_at",
            name="ck_portfolio_spot_anchor_timestamp_order",
        ),
        CheckConstraint(
            "boundary_status IN ('cursor_certified', 'double_read_equal', 'uncertified')",
            name="ck_portfolio_spot_anchor_boundary_status",
        ),
        CheckConstraint(
            "inventory_status IN ('certified_full', 'uncertified', 'suspect_partial')",
            name="ck_portfolio_spot_anchor_inventory_status",
        ),
        CheckConstraint(
            "margin_status IN ('cash', 'unsupported_margin', 'unknown')",
            name="ck_portfolio_spot_anchor_margin_status",
        ),
    )
    wallet_public_id: Mapped[str] = mapped_column(UUIDColumn(), nullable=False)
    exchange: Mapped[str] = mapped_column(String(32), nullable=False)
    mode: Mapped[str] = mapped_column(String(8), default="live", server_default="live")
    venue_account_state_public_id: Mapped[str] = mapped_column(UUIDColumn(), nullable=False)
    balance_observation_id: Mapped[int] = mapped_column(Integer, nullable=False)
    source_watermark_kind: Mapped[str] = mapped_column(String(32), nullable=False)
    source_watermark: Mapped[int] = mapped_column(
        BigInteger().with_variant(Integer, "sqlite"), nullable=False
    )
    balances_json: Mapped[str] = mapped_column(Text, nullable=False)
    first_request_started_at: Mapped[datetime] = mapped_column(TZDateTime(), nullable=False)
    first_request_completed_at: Mapped[datetime] = mapped_column(TZDateTime(), nullable=False)
    second_request_started_at: Mapped[datetime] = mapped_column(TZDateTime(), nullable=False)
    second_request_completed_at: Mapped[datetime] = mapped_column(TZDateTime(), nullable=False)
    boundary_status: Mapped[str] = mapped_column(String(24), nullable=False)
    inventory_status: Mapped[str] = mapped_column(String(24), nullable=False)
    margin_status: Mapped[str] = mapped_column(String(24), nullable=False)
    provenance: Mapped[str] = mapped_column(String(128), nullable=False)


class PortfolioReconciliationObservation(TemporalMixin, Base):
    """Append-only reconciliation evaluation evidence (PnL Phase 4)."""

    __tablename__ = "portfolio_reconciliation_observations"
    __table_args__ = (
        Index(
            "ix_portfolio_reconciliation_observations_public_id",
            "public_id",
            unique=True,
            sqlite_where=_KNOWN_TO_ACTIVE_SQLITE,
            postgresql_where=_KNOWN_TO_ACTIVE_PG,
        ),
        Index(
            "ix_portfolio_reconciliation_observations_identity",
            "wallet_public_id",
            "exchange",
            "mode",
        ),
        Index(
            "uq_portfolio_reconciliation_observations_evaluation",
            "wallet_public_id",
            "exchange",
            "mode",
            "session_id",
            "sequence_id",
            unique=True,
        ),
        CheckConstraint(_CK_EXCHANGE_LOWER, name="ck_portfolio_recon_obs_exchange_lower"),
        CheckConstraint("mode = 'live'", name="ck_portfolio_recon_obs_mode"),
        CheckConstraint(_CK_RECONCILIATION_METHOD, name="ck_portfolio_recon_obs_method"),
        CheckConstraint(
            _CK_RECONCILIATION_EVALUATION_STATUS,
            name="ck_portfolio_recon_obs_evaluation_status",
        ),
        CheckConstraint(
            "resulting_full_mismatch_count >= 0",
            name="ck_portfolio_recon_obs_mismatch_count",
        ),
        CheckConstraint(
            _CK_RECONCILIATION_OBSERVATION_FULL_EVIDENCE,
            name="ck_portfolio_recon_obs_full_evidence",
        ),
        CheckConstraint(
            _CK_RECONCILIATION_OBSERVATION_MATCHED,
            name="ck_portfolio_recon_obs_matched",
        ),
        CheckConstraint(
            _CK_RECONCILIATION_OBSERVATION_MISMATCHED,
            name="ck_portfolio_recon_obs_mismatched",
        ),
        CheckConstraint(
            _CK_RECONCILIATION_EPISODE_THRESHOLD,
            name="ck_portfolio_recon_obs_episode_threshold",
        ),
        CheckConstraint(
            _CK_RECONCILIATION_SPOT_ANCHOR,
            name="ck_portfolio_recon_obs_spot_anchor",
        ),
        CheckConstraint(
            _CK_RECONCILIATION_WATERMARK_PAIR,
            name="ck_portfolio_recon_obs_watermark_pair",
        ),
        CheckConstraint(
            _CK_RECONCILIATION_ERROR_TEXT,
            name="ck_portfolio_recon_obs_error_text",
        ),
        CheckConstraint(
            _CK_RECONCILIATION_ERROR_LENGTH,
            name="ck_portfolio_recon_obs_error_length",
        ),
    )
    wallet_public_id: Mapped[str] = mapped_column(UUIDColumn(), nullable=False)
    exchange: Mapped[str] = mapped_column(String(32), nullable=False)
    mode: Mapped[str] = mapped_column(String(8), default="live", server_default="live")
    method: Mapped[str] = mapped_column(String(32), nullable=False)
    evaluation_status: Mapped[str] = mapped_column(String(16), nullable=False)
    venue_account_state_public_id: Mapped[str | None] = mapped_column(UUIDColumn(), nullable=True)
    venue_account_observation_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    account_authoritative_until: Mapped[datetime | None] = mapped_column(
        TZDateTime(), nullable=True
    )
    source_watermark_kind: Mapped[str | None] = mapped_column(String(32), nullable=True)
    source_watermark: Mapped[int | None] = mapped_column(
        BigInteger().with_variant(Integer, "sqlite"), nullable=True
    )
    anchor_public_id: Mapped[str | None] = mapped_column(UUIDColumn(), nullable=True)
    expected_json: Mapped[str | None] = mapped_column(Text, nullable=True)
    actual_json: Mapped[str | None] = mapped_column(Text, nullable=True)
    difference_json: Mapped[str | None] = mapped_column(Text, nullable=True)
    tolerance_json: Mapped[str | None] = mapped_column(Text, nullable=True)
    resulting_full_mismatch_count: Mapped[int] = mapped_column(Integer, nullable=False)
    drift_episode_public_id: Mapped[str | None] = mapped_column(UUIDColumn(), nullable=True)
    error: Mapped[str | None] = mapped_column(String(512), nullable=True)


class PortfolioReconciliationState(TemporalMixin, Base):
    """Current reconciliation truth per live venue account as SCD2 state."""

    __tablename__ = "portfolio_reconciliation_states"
    __table_args__ = (
        Index(
            "uq_portfolio_reconciliation_states_identity",
            "wallet_public_id",
            "exchange",
            "mode",
            unique=True,
            sqlite_where=_KNOWN_TO_ACTIVE_SQLITE,
            postgresql_where=_KNOWN_TO_ACTIVE_PG,
        ),
        Index(
            "ix_portfolio_reconciliation_states_public_id",
            "public_id",
            unique=True,
            sqlite_where=_KNOWN_TO_ACTIVE_SQLITE,
            postgresql_where=_KNOWN_TO_ACTIVE_PG,
        ),
        Index("ix_portfolio_reconciliation_states_wallet", "wallet_public_id"),
        CheckConstraint(_CK_EXCHANGE_LOWER, name="ck_portfolio_recon_states_exchange_lower"),
        CheckConstraint("mode = 'live'", name="ck_portfolio_recon_states_mode"),
        CheckConstraint(_CK_RECONCILIATION_METHOD, name="ck_portfolio_recon_states_method"),
        CheckConstraint(
            _CK_RECONCILIATION_CURRENT_STATUS,
            name="ck_portfolio_recon_states_current_status",
        ),
        CheckConstraint(
            _CK_RECONCILIATION_LAST_OUTCOME,
            name="ck_portfolio_recon_states_last_outcome",
        ),
        CheckConstraint(
            "consecutive_full_mismatches >= 0",
            name="ck_portfolio_recon_states_mismatch_count",
        ),
        CheckConstraint(
            _CK_RECONCILIATION_STATE_DETAIL,
            name="ck_portfolio_recon_states_detail",
        ),
        CheckConstraint(
            _CK_RECONCILIATION_STATE_CURRENT_FULL,
            name="ck_portfolio_recon_states_current_full",
        ),
        CheckConstraint(
            _CK_RECONCILIATION_STATE_MATCHED,
            name="ck_portfolio_recon_states_matched",
        ),
        CheckConstraint(
            _CK_RECONCILIATION_STATE_MISMATCHED,
            name="ck_portfolio_recon_states_mismatched",
        ),
        CheckConstraint(
            _CK_RECONCILIATION_STATE_NO_FULL,
            name="ck_portfolio_recon_states_no_full",
        ),
        CheckConstraint(
            _CK_RECONCILIATION_STATE_EPISODE,
            name="ck_portfolio_recon_states_episode",
        ),
        CheckConstraint(
            _CK_RECONCILIATION_STATE_SPOT_ANCHOR,
            name="ck_portfolio_recon_states_spot_anchor",
        ),
        CheckConstraint(
            _CK_RECONCILIATION_STATE_ERROR_TEXT,
            name="ck_portfolio_recon_states_error_text",
        ),
        CheckConstraint(
            _CK_RECONCILIATION_ERROR_LENGTH,
            name="ck_portfolio_recon_states_error_length",
        ),
    )
    wallet_public_id: Mapped[str] = mapped_column(UUIDColumn(), nullable=False)
    exchange: Mapped[str] = mapped_column(String(32), nullable=False)
    mode: Mapped[str] = mapped_column(String(8), default="live", server_default="live")
    method: Mapped[str] = mapped_column(String(32), nullable=False)
    current_evaluation_status: Mapped[str] = mapped_column(String(16), nullable=False)
    current_observation_id: Mapped[int] = mapped_column(Integer, nullable=False)
    last_full_observation_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    last_full_outcome: Mapped[str | None] = mapped_column(String(16), nullable=True)
    detail_source_observation_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    consecutive_full_mismatches: Mapped[int] = mapped_column(Integer, nullable=False)
    open_drift_episode_public_id: Mapped[str | None] = mapped_column(UUIDColumn(), nullable=True)
    anchor_public_id: Mapped[str | None] = mapped_column(UUIDColumn(), nullable=True)
    venue_account_state_public_id: Mapped[str | None] = mapped_column(UUIDColumn(), nullable=True)
    venue_account_observation_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    source_watermark_kind: Mapped[str | None] = mapped_column(String(32), nullable=True)
    source_watermark: Mapped[int | None] = mapped_column(
        BigInteger().with_variant(Integer, "sqlite"), nullable=True
    )
    expected_json: Mapped[str | None] = mapped_column(Text, nullable=True)
    actual_json: Mapped[str | None] = mapped_column(Text, nullable=True)
    difference_json: Mapped[str | None] = mapped_column(Text, nullable=True)
    tolerance_json: Mapped[str | None] = mapped_column(Text, nullable=True)
    reconciled_at: Mapped[datetime | None] = mapped_column(TZDateTime(), nullable=True)
    authoritative_until: Mapped[datetime | None] = mapped_column(TZDateTime(), nullable=True)
    error: Mapped[str | None] = mapped_column(String(512), nullable=True)


class PortfolioDriftEpisode(TemporalMixin, Base):
    """SCD2 lifecycle evidence for a sustained reconciliation mismatch."""

    __tablename__ = "portfolio_drift_episodes"
    __table_args__ = (
        Index(
            "uq_portfolio_drift_episodes_open_identity",
            "wallet_public_id",
            "exchange",
            "mode",
            unique=True,
            sqlite_where=text("status = 'open' AND known_to = '9999-12-31 23:59:59.000000'"),
            postgresql_where=text("status = 'open' AND known_to = '9999-12-31T23:59:59+00:00'"),
        ),
        Index(
            "ix_portfolio_drift_episodes_public_id",
            "public_id",
            unique=True,
            sqlite_where=_KNOWN_TO_ACTIVE_SQLITE,
            postgresql_where=_KNOWN_TO_ACTIVE_PG,
        ),
        Index("ix_portfolio_drift_episodes_status_opened", "status", "opened_at"),
        CheckConstraint(_CK_EXCHANGE_LOWER, name="ck_portfolio_drift_exchange_lower"),
        CheckConstraint("mode = 'live'", name="ck_portfolio_drift_mode"),
        CheckConstraint(_CK_DRIFT_EPISODE_STATUS, name="ck_portfolio_drift_status"),
        CheckConstraint(
            _CK_DRIFT_EPISODE_OBSERVATION_ORDER,
            name="ck_portfolio_drift_observation_order",
        ),
        CheckConstraint(
            _CK_DRIFT_EPISODE_DETAIL_ORDER,
            name="ck_portfolio_drift_detail_order",
        ),
        CheckConstraint(
            _CK_DRIFT_EPISODE_CLOSED_ORDER,
            name="ck_portfolio_drift_closed_order",
        ),
        CheckConstraint(
            _CK_DRIFT_EPISODE_MISMATCH_COUNT,
            name="ck_portfolio_drift_mismatch_count",
        ),
        CheckConstraint(_CK_DRIFT_EPISODE_OPEN, name="ck_portfolio_drift_open"),
        CheckConstraint(_CK_DRIFT_EPISODE_RESOLVED, name="ck_portfolio_drift_resolved"),
        CheckConstraint(_CK_DRIFT_EPISODE_REBASED, name="ck_portfolio_drift_rebased"),
    )
    wallet_public_id: Mapped[str] = mapped_column(UUIDColumn(), nullable=False)
    exchange: Mapped[str] = mapped_column(String(32), nullable=False)
    mode: Mapped[str] = mapped_column(String(8), default="live", server_default="live")
    status: Mapped[str] = mapped_column(String(16), nullable=False)
    opened_at: Mapped[datetime] = mapped_column(TZDateTime(), nullable=False)
    closed_at: Mapped[datetime | None] = mapped_column(TZDateTime(), nullable=True)
    trigger_observation_id: Mapped[int] = mapped_column(Integer, nullable=False)
    last_observation_id: Mapped[int] = mapped_column(Integer, nullable=False)
    details_source_observation_id: Mapped[int] = mapped_column(Integer, nullable=False)
    latest_full_mismatch_count: Mapped[int] = mapped_column(Integer, nullable=False)
    resolution_reason: Mapped[str | None] = mapped_column(String(32), nullable=True)
    closed_by_user_public_id: Mapped[str | None] = mapped_column(UUIDColumn(), nullable=True)
    closed_by_operator_public_id: Mapped[str | None] = mapped_column(UUIDColumn(), nullable=True)
    rebase_anchor_public_id: Mapped[str | None] = mapped_column(UUIDColumn(), nullable=True)


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
        Index(
            "ix_signals_paired_group_id",
            "paired_group_id",
            sqlite_where=_PAIRED_GROUP_ID_NOT_NULL,
            postgresql_where=_PAIRED_GROUP_ID_NOT_NULL,
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
    paired_group_id: Mapped[str | None] = mapped_column(UUIDColumn(), nullable=True)


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


class SymbolMarketDataChannelCapability(TemporalMixin, Base):
    """Channel-specific market-data capability overlay.

    The symbol-level ``SymbolExchangeCapability.can_market_data`` row remains
    the coarse gate. Rows in this table only override individual market-data
    channels for symbols that are otherwise market-data capable. Absence of a
    current channel row means the channel inherits the symbol-level allowance.
    """

    __tablename__ = "symbol_market_data_channel_capabilities"
    __table_args__ = (
        Index(
            "uq_smdcc_symbol_exchange_channel",
            "symbol_public_id",
            "exchange",
            "channel",
            unique=True,
            sqlite_where=_KNOWN_TO_ACTIVE_SQLITE,
            postgresql_where=_KNOWN_TO_ACTIVE_PG,
        ),
        CheckConstraint(_CK_EXCHANGE_LOWER, name="ck_smdcc_exchange_lower"),
        CheckConstraint(_CK_CHANNEL_LOWER_NON_EMPTY, name="ck_smdcc_channel_lower_non_empty"),
        Index("ix_smdcc_exchange_channel", "exchange", "channel"),
        Index(
            "ix_symbol_market_data_channel_capabilities_public_id",
            "public_id",
            unique=True,
            sqlite_where=_KNOWN_TO_ACTIVE_SQLITE,
            postgresql_where=_KNOWN_TO_ACTIVE_PG,
        ),
    )
    symbol_public_id: Mapped[str] = mapped_column(UUIDColumn(), index=True)
    exchange: Mapped[str] = mapped_column(String(32))
    channel: Mapped[str] = mapped_column(String(64))
    can_market_data: Mapped[bool] = mapped_column(Boolean, default=False)
    source: Mapped[str | None] = mapped_column(String(64))
    reason: Mapped[str | None] = mapped_column(String(1024))
    created_at: Mapped[datetime] = mapped_column(TZDateTime())


class ProcessRun(TemporalMixin, Base):
    """SQLAlchemy model for background process execution records.

    ``parameters`` / ``result`` / ``tags`` use ``JSON`` (text-with-
    validation on PostgreSQL), NOT ``JSONB``. They are written and read
    as whole documents; no query path uses key-indexed access. If a
    future query needs containment / key operators (e.g. PostgreSQL
    ``tags @> '[...]'``), migrate the affected column to ``JSONB`` in a
    dedicated migration — a blanket JSONB switch is deferred.
    """

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
            "funding_type IS NULL OR funding_type IN ('spot_margin_rollover', 'perpetual_funding')",
            name="ck_instrument_specs_funding_type",
        ),
        CheckConstraint(
            "contract_size IS NULL OR CAST(contract_size AS NUMERIC) > 0",
            name="ck_instrument_specs_contract_size_positive",
        ),
        CheckConstraint(
            "quantity_unit IS NULL OR quantity_unit IN ('base_asset', 'contract_count')",
            name="ck_instrument_specs_quantity_unit",
        ),
        CheckConstraint(
            "(spec_source IS NULL AND spec_version IS NULL AND spec_observed_at IS NULL) OR "
            "(spec_source IS NOT NULL AND spec_version IS NOT NULL AND "
            "spec_observed_at IS NOT NULL)",
            name="ck_instrument_specs_provenance",
        ),
        CheckConstraint(
            "unit_certified = false OR (contract_size IS NOT NULL AND "
            "CAST(contract_size AS NUMERIC) > 0 AND "
            "quantity_unit IS NOT NULL AND quantity_unit = 'contract_count' AND "
            "spec_source IS NOT NULL AND "
            "spec_version IS NOT NULL AND spec_observed_at IS NOT NULL)",
            name="ck_instrument_specs_unit_certified",
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
    position_limit_long: Mapped[int | None] = mapped_column(
        BigInteger().with_variant(Integer, "sqlite"), comment="Long position limit"
    )
    position_limit_short: Mapped[int | None] = mapped_column(
        BigInteger().with_variant(Integer, "sqlite"), comment="Short position limit"
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
    contract_size: Mapped[Decimal | None] = mapped_column(
        ExactDecimalNumeric(38, 18), comment="Venue contract multiplier"
    )
    quantity_unit: Mapped[str | None] = mapped_column(
        String(32), comment="Canonical order and position quantity unit"
    )
    spec_source: Mapped[str | None] = mapped_column(
        String(64), comment="Authoritative instrument metadata source"
    )
    spec_version: Mapped[str | None] = mapped_column(
        String(96), comment="Instrument metadata ETL and content version"
    )
    spec_observed_at: Mapped[datetime | None] = mapped_column(
        TZDateTime(), comment="UTC time when the venue definition was observed"
    )
    unit_certified: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=False, server_default=text("false")
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
        Index("ix_trade_commands_client_order_id", "client_order_id"),
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
        CheckConstraint(_CK_TRADE_COMMAND_TYPE, name="ck_trade_commands_command_type"),
        CheckConstraint(_CK_MODE_LIVE_PAPER, name="ck_trade_commands_mode"),
        CheckConstraint(_CK_SIDE_BUY_SELL, name="ck_trade_commands_side"),
        CheckConstraint(_CK_ORDER_TYPE_VALUES, name="ck_trade_commands_order_type"),
        CheckConstraint(_CK_TRADE_COMMAND_STATUS, name="ck_trade_commands_status"),
        CheckConstraint(
            "origin IN ('live', 'replay')",
            name="ck_trade_commands_origin",
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
    stop_price: Mapped[float | None] = mapped_column(Float)
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
    signal_public_id: Mapped[str | None] = mapped_column(UUIDColumn(), nullable=True, index=True)
    ai_review_public_id: Mapped[str | None] = mapped_column(UUIDColumn(), nullable=True, index=True)
    submitted_notional_usd: Mapped[float | None] = mapped_column(Numeric(18, 2), nullable=True)
    origin: Mapped[str] = mapped_column(String(8), nullable=False, server_default="live")
    """Provenance of the triggering market frame (PnL Phase 1).

    ``live`` or ``replay`` — stamped at insert from the signal's
    frame provenance and carried immutably across SCD2 successors.
    Executors reject ``replay`` submits pre-venue; the outbox rebuild
    (``order_request_from_command``) projects it onto the dispatch
    payload so the guard re-fires deterministically on replay."""
    replay_window_start: Mapped[datetime | None] = mapped_column(TZDateTime(), nullable=True)
    replay_window_end: Mapped[datetime | None] = mapped_column(TZDateTime(), nullable=True)


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
        Index(
            "ix_venue_events_paired_group_id",
            "paired_group_id",
            sqlite_where=_PAIRED_GROUP_ID_NOT_NULL,
            postgresql_where=_PAIRED_GROUP_ID_NOT_NULL,
        ),
        Index("ix_venue_events_cid_event_type", "client_order_id", "event_type"),
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
    paired_group_id: Mapped[str | None] = mapped_column(UUIDColumn(), nullable=True)


class PairedExecutionGroup(TemporalMixin, Base):
    """SCD2 group record for the multi-leg paired-execution guard.

    One row per multi-leg signal group (``correlation_id`` of the
    legs' trade commands == this ``public_id``). Tracks the
    bounded-compensation FSM (see :class:`PairedExecutionGroupStatusEnum`)
    from ``assembling`` through arming, breakage, compensation and a
    terminal state. ``group_key`` is the canonical sorted set of
    per-leg ``exchange:instrument:mode`` tokens joined by ``|`` so a
    halt can recognise the same strategy pair. ``status`` is not
    constrained by a DB CHECK (the FSM can gain states without a
    schema migration); only the immutable ``policy`` is pinned.
    """

    __tablename__ = "paired_execution_groups"
    __table_args__ = (
        Index(
            "ix_peg_public_id",
            "public_id",
            unique=True,
            sqlite_where=_KNOWN_TO_ACTIVE_SQLITE,
            postgresql_where=_KNOWN_TO_ACTIVE_PG,
        ),
        Index("ix_peg_status", "status"),
        Index("ix_peg_status_timestamp", "status", "timestamp"),
        Index("ix_peg_group_key", "group_key"),
        CheckConstraint(
            "policy IN ('simultaneous', 'sequential_handoff')",
            name="ck_peg_policy",
        ),
    )
    wallet_public_id: Mapped[str] = mapped_column(UUIDColumn(), nullable=False)
    operator_public_id: Mapped[str | None] = mapped_column(UUIDColumn(), nullable=True)
    strategy_id: Mapped[str] = mapped_column(String(64))
    policy: Mapped[str] = mapped_column(String(24))
    expected_leg_count: Mapped[int] = mapped_column(Integer)
    group_key: Mapped[str] = mapped_column(String(512))
    status: Mapped[str] = mapped_column(String(32))
    assembly_deadline: Mapped[datetime] = mapped_column(TZDateTime())
    fill_deadline: Mapped[datetime] = mapped_column(TZDateTime())
    failure_reason: Mapped[str | None] = mapped_column(String(512))
    halted_at: Mapped[datetime | None] = mapped_column(TZDateTime())
    created_at: Mapped[datetime] = mapped_column(TZDateTime())


class PairedExecutionLeg(TemporalMixin, Base):
    """SCD2 authoritative per-leg record for the paired-execution guard.

    One logical leg per ``(group_public_id, leg_index)``; SCD2 versions
    advance its status and fill/compensation accounting. Carries the
    durable ``exchange`` / ``mode`` (never reconstructed by parsing
    ``shard_key``) plus the venue identifiers and signed quantities the
    compensator needs: ``open_group_qty = filled_signed_qty -
    compensated_signed_qty``. ``status`` is not constrained by a DB
    CHECK (the FSM can gain states without a schema migration); only
    the immutable ``side`` and ``mode`` are pinned.
    """

    __tablename__ = "paired_execution_legs"
    __table_args__ = (
        Index(
            "ix_pel_public_id",
            "public_id",
            unique=True,
            sqlite_where=_KNOWN_TO_ACTIVE_SQLITE,
            postgresql_where=_KNOWN_TO_ACTIVE_PG,
        ),
        Index(
            "uq_pel_group_leg",
            "group_public_id",
            "leg_index",
            unique=True,
            sqlite_where=_KNOWN_TO_ACTIVE_SQLITE,
            postgresql_where=_KNOWN_TO_ACTIVE_PG,
        ),
        Index(
            "uq_pel_command",
            "command_public_id",
            unique=True,
            sqlite_where=text(
                "command_public_id IS NOT NULL AND known_to = '9999-12-31 23:59:59.000000'"
            ),
            postgresql_where=text(
                "command_public_id IS NOT NULL AND known_to = '9999-12-31T23:59:59+00:00'"
            ),
        ),
        Index("ix_pel_group", "group_public_id"),
        Index("ix_pel_client_order_id", "client_order_id"),
        Index("ix_pel_exchange_order_id", "exchange_order_id"),
        Index("ix_pel_shard_status", "shard_key", "status"),
        CheckConstraint(_CK_SIDE_BUY_SELL, name="ck_pel_side"),
        CheckConstraint(_CK_MODE_LIVE_PAPER, name="ck_pel_mode"),
    )
    group_public_id: Mapped[str] = mapped_column(UUIDColumn(), nullable=False)
    leg_index: Mapped[int] = mapped_column(Integer)
    exchange: Mapped[str] = mapped_column(String(32))
    mode: Mapped[str] = mapped_column(String(8))
    instrument: Mapped[str] = mapped_column(String(64))
    shard_key: Mapped[str] = mapped_column(String(256))
    side: Mapped[str] = mapped_column(String(4))
    target_qty: Mapped[float] = mapped_column(Float)
    signal_public_id: Mapped[str] = mapped_column(UUIDColumn(), nullable=False)
    command_public_id: Mapped[str | None] = mapped_column(UUIDColumn(), nullable=True)
    client_order_id: Mapped[str | None] = mapped_column(String(64))
    exchange_order_id: Mapped[str | None] = mapped_column(String(64))
    status: Mapped[str] = mapped_column(String(32))
    filled_signed_qty: Mapped[float] = mapped_column(Float, default=0.0, server_default="0")
    compensated_signed_qty: Mapped[float] = mapped_column(Float, default=0.0, server_default="0")
    compensation_seq: Mapped[int] = mapped_column(Integer, default=0, server_default="0")
    last_venue_event_id: Mapped[int | None] = mapped_column(Integer)
    wallet_public_id: Mapped[str] = mapped_column(UUIDColumn(), nullable=False)
    operator_public_id: Mapped[str | None] = mapped_column(UUIDColumn(), nullable=True)
    created_at: Mapped[datetime] = mapped_column(TZDateTime())


class PairedExecutionHalt(TemporalMixin, Base):
    """SCD2 halt projection for the paired-execution guard.

    Prevents a strategy pair from opening a NEW group while a prior
    group is broken / compensating. Active-unique on
    ``(wallet_public_id, strategy_id, group_key)`` so the ``_on_signal``
    fast-reject can match a pending halt cheaply. The guard scanner
    writes halts when a haltable group breaks
    (``ensure_paired_execution_halt``) and clears them via its
    quiet-halt sweep (``_sweep_halt_clears``).
    """

    __tablename__ = "paired_execution_halts"
    __table_args__ = (
        Index(
            "ix_peh_public_id",
            "public_id",
            unique=True,
            sqlite_where=_KNOWN_TO_ACTIVE_SQLITE,
            postgresql_where=_KNOWN_TO_ACTIVE_PG,
        ),
        Index(
            "uq_peh_scope",
            "wallet_public_id",
            "strategy_id",
            "group_key",
            unique=True,
            sqlite_where=_KNOWN_TO_ACTIVE_SQLITE,
            postgresql_where=_KNOWN_TO_ACTIVE_PG,
        ),
        Index("ix_peh_group_key", "group_key"),
        CheckConstraint(_CK_MODE_LIVE_PAPER, name="ck_peh_mode"),
    )
    wallet_public_id: Mapped[str] = mapped_column(UUIDColumn(), nullable=False)
    operator_public_id: Mapped[str | None] = mapped_column(UUIDColumn(), nullable=True)
    strategy_id: Mapped[str] = mapped_column(String(64))
    mode: Mapped[str] = mapped_column(String(8))
    group_key: Mapped[str] = mapped_column(String(512))
    group_public_id: Mapped[str] = mapped_column(UUIDColumn(), nullable=False)
    reason: Mapped[str] = mapped_column(String(512))
    created_at: Mapped[datetime] = mapped_column(TZDateTime())


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
    by ContinuousContractBuilder. No CRUD endpoints exist for these
    presets; the continuous-series endpoint takes its parameters
    directly as query arguments.
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
    is intentionally not implemented; rotation is always the
    all-rows lockstep described above.

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
    sharing the same instrument on the same wallet) are not supported.

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
            _CK_SIDE_BUY_SELL,
            name="ck_ep_side",
        ),
        CheckConstraint(
            _CK_MODE_LIVE_PAPER,
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


class ExecutionPlanDecisionOutbox(TemporalMixin, Base):
    """Durable delivery state for ``plans.decisions.*`` event fanout.

    The decision audit row remains the source of truth, while this table
    tracks whether the corresponding ZMQ frame has reached the broker.
    Status transitions are SCD2-versioned so retry history stays
    inspectable and concurrent drainers can race on the active row
    without producing multiple active successors.
    """

    __tablename__ = "execution_plan_decision_outbox"
    __table_args__ = (
        Index(
            "ix_epd_outbox_public_id",
            "public_id",
            unique=True,
            sqlite_where=_KNOWN_TO_ACTIVE_SQLITE,
            postgresql_where=_KNOWN_TO_ACTIVE_PG,
        ),
        Index(
            "ix_epd_outbox_decision_public_id",
            "decision_public_id",
            unique=True,
            sqlite_where=_KNOWN_TO_ACTIVE_SQLITE,
            postgresql_where=_KNOWN_TO_ACTIVE_PG,
        ),
        Index(
            "ix_epd_outbox_ready",
            "next_attempt_at",
            "created_at",
            sqlite_where=text("known_to = '9999-12-31 23:59:59.000000' AND status = 'pending'"),
            postgresql_where=text("known_to = '9999-12-31T23:59:59+00:00' AND status = 'pending'"),
        ),
        Index("ix_epd_outbox_plan_public_id", "plan_public_id"),
        CheckConstraint(
            "status IN ('pending', 'sent', 'failed')",
            name="ck_epd_outbox_status",
        ),
    )
    decision_public_id: Mapped[str] = mapped_column(UUIDColumn(), nullable=False)
    plan_public_id: Mapped[str] = mapped_column(UUIDColumn(), nullable=False)
    topic: Mapped[str] = mapped_column(String(256), nullable=False)
    payload_json: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[str] = mapped_column(String(16), nullable=False)
    attempt_count: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")
    last_attempt_at: Mapped[datetime | None] = mapped_column(TZDateTime(), nullable=True)
    next_attempt_at: Mapped[datetime | None] = mapped_column(TZDateTime(), nullable=True)
    sent_at: Mapped[datetime | None] = mapped_column(TZDateTime(), nullable=True)
    error_reason: Mapped[str | None] = mapped_column(String(512), nullable=True)
    created_at: Mapped[datetime] = mapped_column(TZDateTime(), nullable=False)


class PositionCycle(TemporalMixin, Base):
    """A single open->close lifetime of a position on one shard.

    Brackets (SL/TP) attach to a cycle, not to an order: if a position closes
    and the user reopens, the new trades belong to a new cycle even though
    instrument/wallet/mode are identical. Created when a shard's position
    goes flat -> non-flat, closed when it returns to zero, flipped (close +
    open) atomically when the sign reverses in a single fill.

    Attributes:
        instrument_public_id: Instrument UUID7 for the position shard.
        exchange: Exchange name for the shard.
        mode: Trading mode, either ``live`` or ``paper``.
        shard_key: Stable instrument/exchange/mode/wallet shard key.
        wallet_public_id: Wallet UUID7 that owns the cycle.
        operator_public_id: Owning operator UUID7 when available.
        direction: Cycle direction, either ``long`` or ``short``.
        max_qty: Per-cycle peak absolute quantity (NOT lifetime). Resets to
            abs(opening_qty) on each new cycle. See column docstring for the
            worked example + UI scope rule.
        status: Cycle lifecycle status.
        opened_at: UTC timestamp when the cycle opened.
        closed_at: UTC timestamp when the cycle closed, if closed.
        opening_command_public_id: Trade command that opened the cycle,
            when available.
        closing_command_public_id: Trade command that closed the cycle,
            when available.
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
        CheckConstraint(_CK_MODE_LIVE_PAPER, name="ck_pc_mode"),
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
        CheckConstraint(_CK_PAIRING_MODE, name="ck_bc_pairing_mode"),
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
          LIVE commands (``mode='paper'`` history is excluded — paper
          commands carry a simulator reference price and simulated
          notional must not consume the live allowance; the current
          submission is evaluated regardless of mode). Submit-time
          commitment basis; partial fills do not change accounting.
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

    Per the bitemporal/SCD2 invariant, every
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
            (``event_type`` fully discriminates).
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


class InstrumentFeedHealth(Base):
    """Current-state per-symbol subscription / feed-health snapshot.

    Persists the in-memory
    :class:`snapper.infrastructure.exchanges._subscription_health.SubscriptionHealthTracker`
    state so operators can answer, AFTER the fact, which symbols are
    dark, when each last received data, and why. The tracker lives in
    each publisher subprocess and is lost on restart; a periodic flush
    in :class:`snapper.messaging.publishers.base.MarketDataPublisherService`
    writes the current snapshot here.

    This is a CURRENT-STATE table (last-write-wins per key), NOT
    bitemporal / SCD2: the periodic flush upserts on the natural key
    ``(coordinator, exchange, channel, symbol)`` so each row reflects
    only the latest observed state, with no version history.

    All monotonic-clock fields on the tracker
    (``requested_at`` / ``confirmed_at`` / ``last_seen_data_at``) are
    converted to wall-clock UTC by the publisher BEFORE upsert, so the
    timestamps stored here are real instants comparable across rows and
    restarts.

    Attributes:
        coordinator: ``coord-<id>`` slug of the coordinator instance
            that owns the publisher subprocess (the natural key's tenant
            dimension so two coordinators tracking the same symbol do
            not collide).
        exchange: Exchange identifier the publisher feeds (lowercase).
        channel: Tracker channel key, including parameters where needed
            (e.g. ``ohlc:1m``).
        symbol: Wire-format symbol or product id tracked.
        status: Subscription lifecycle state
            (``pending`` / ``confirmed`` / ``failed``).
        requested_at: Wall-clock when the current subscribe attempt was
            issued.
        confirmed_at: Wall-clock when ACK or data confirmed the
            subscription; NULL until confirmed.
        last_seen_data_at: Wall-clock when market data last arrived;
            NULL until the first datum.
        last_error: Last failure reason reported by the exchange or
            retry loop; NULL when healthy.
        retry_count: Retry attempts already consumed for this entry.
        snapshot_at: Wall-clock when this snapshot row was flushed.
    """

    __tablename__ = "instrument_feed_health"
    __table_args__ = (
        UniqueConstraint(
            "coordinator",
            "exchange",
            "channel",
            "symbol",
            name="uq_instrument_feed_health_key",
        ),
        CheckConstraint(
            "status IN ('pending', 'confirmed', 'failed')",
            name="ck_instrument_feed_health_status",
        ),
        CheckConstraint(
            "retry_count >= 0",
            name="ck_instrument_feed_health_retry_count_nonneg",
        ),
        CheckConstraint(_CK_EXCHANGE_LOWER, name="ck_instrument_feed_health_exchange_lower"),
        Index("ix_instrument_feed_health_exchange", "exchange"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    coordinator: Mapped[str] = mapped_column(String(32), nullable=False)
    exchange: Mapped[str] = mapped_column(String(32), nullable=False)
    channel: Mapped[str] = mapped_column(String(64), nullable=False)
    symbol: Mapped[str] = mapped_column(String(64), nullable=False)
    status: Mapped[str] = mapped_column(String(16), nullable=False)
    requested_at: Mapped[datetime] = mapped_column(TZDateTime(), nullable=False)
    confirmed_at: Mapped[datetime | None] = mapped_column(TZDateTime(), nullable=True)
    last_seen_data_at: Mapped[datetime | None] = mapped_column(TZDateTime(), nullable=True)
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    retry_count: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")
    snapshot_at: Mapped[datetime] = mapped_column(TZDateTime(), nullable=False)
