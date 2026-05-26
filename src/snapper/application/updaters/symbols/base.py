"""Base class for symbol updater services.

Provides common functionality for fetching and persisting exchange symbol
mappings to the database.
"""

from abc import ABC
from abc import abstractmethod
from collections.abc import Callable
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from typing import Any
from uuid import uuid7

import zmq
import zmq.asyncio
from loguru import logger
from sqlalchemy import or_
from sqlalchemy import select
from sqlalchemy import update

from snapper.application.process_manager.models import RegisterableProcess
from snapper.application.services.settings import get_settings_service
from snapper.config.settings import AppSettings
from snapper.config.settings import get_settings
from snapper.config.settings import get_settings_with_service
from snapper.core.types import AliasChannelEnum
from snapper.core.types import AssetTypeEnum
from snapper.core.types import UpsertResult
from snapper.data.models import Instrument
from snapper.data.models import InstrumentSpec
from snapper.data.models import Setting
from snapper.data.models import Symbol
from snapper.data.models import SymbolAlias
from snapper.data.models import SymbolExchangeCapability
from snapper.data.repository import DatabaseRepository
from snapper.data.repository import InstrumentSpecInput
from snapper.data.repository import close_and_insert
from snapper.data.repository import close_and_insert_sync
from snapper.data.repository import get_repository
from snapper.data.repository import where_active_now
from snapper.infrastructure.exchanges.base import ExchangeClientBase
from snapper.messaging.infrastructure.publisher import MessagePublisher
from snapper.messaging.infrastructure.publisher import SequenceTracker
from snapper.messaging.infrastructure.validated_socket import HWM_MARKET_DATA
from snapper.messaging.infrastructure.validated_socket import ValidatedPublisher
from snapper.messaging.infrastructure.validated_socket import apply_hwm
from snapper.messaging.schemas.data import SymbolAliasUpdateData
from snapper.messaging.topics.builders import system_topic
from snapper.utils.logging import set_log_context


class _Preserve:
    """Sentinel indicating a funding field should carry forward its existing value.

    Use the module-level ``PRESERVE_EXISTING`` instance as the default
    parameter value in ``_revise_instrument_spec``. Callers that omit a
    funding field get carry-forward (safe for unaware updaters). Callers
    that explicitly pass ``None`` clear the field to NULL.
    """

    def __repr__(self) -> str:
        """Return a human-readable representation for debugging."""
        return "PRESERVE_EXISTING"


PRESERVE_EXISTING = _Preserve()


class SymbolUpdaterService[T: ExchangeClientBase](RegisterableProcess, ABC):
    """Base service for updating exchange symbol mappings in the database."""

    def __init__(self, update_threshold_hours: int, force: bool = False) -> None:
        """Initialize the instance.

        Args:
            update_threshold_hours: Minimum hours between automatic updates.
            force: If True, bypass the update threshold check.
        """
        self.settings: AppSettings = get_settings()
        self.repository: DatabaseRepository | None = None
        self.context: zmq.asyncio.Context | None = None
        self._tracker: SequenceTracker = SequenceTracker()
        self.msg_publisher: MessagePublisher | None = None
        self.update_threshold_hours = update_threshold_hours
        self.force = force

    @abstractmethod
    def _create_exchange_client(self) -> T:
        """Create and return the exchange client instance.

        Returns:
            Configured exchange client for fetching symbol data.
        """
        ...

    @abstractmethod
    def _get_setting_key(self) -> str:
        """Return the database setting key for storing last update timestamp.

        Returns:
            Setting key string unique to this exchange updater.
        """
        ...

    @abstractmethod
    async def _update_database(self, symbols: list[dict[str, Any]]) -> None:
        """Persist fetched symbol mappings to the database.

        Args:
            symbols: List of symbol dictionaries from the exchange.
        """
        ...

    def _setup_zmq(self) -> None:
        """Initialize ZMQ context and publisher socket for cache invalidation."""
        if self.context is not None:
            return
        self.context = zmq.asyncio.Context()
        raw_pub_socket = self.context.socket(zmq.PUB)
        apply_hwm(raw_pub_socket, sndhwm=HWM_MARKET_DATA)
        raw_pub_socket.connect(self.settings.zmq_broker_xsub)
        self.msg_publisher = MessagePublisher(ValidatedPublisher(raw_pub_socket), self._tracker)
        logger.info(f"SymbolUpdater: Connected to broker {self.settings.zmq_broker_xsub}")

    def _cleanup_zmq(self) -> None:
        """Close ZMQ publisher socket and terminate context."""
        if self.msg_publisher:
            self.msg_publisher.setsockopt(zmq.LINGER, 0)
            self.msg_publisher.close()
            self.msg_publisher = None
        if self.context:
            self.context.term()
            self.context = None
        logger.info("SymbolUpdater: ZMQ resources cleaned up")

    async def broadcast_cache_invalidation(self) -> None:
        """Broadcast cache invalidation message via ZMQ to all subscribers."""
        if not self.msg_publisher:
            self._setup_zmq()
        if self.msg_publisher:
            topic = system_topic("symbol_aliases")
            envelope = SymbolAliasUpdateData(
                public_id=str(uuid7()),
                timestamp=datetime.now(UTC),
                session_id=self.msg_publisher.tracker.session_id,
                sequence_id=self.msg_publisher.tracker.next_sequence(topic),
            )
            await self.msg_publisher.send(topic, envelope)
            logger.info("Broadcasted cache invalidation: system.symbol_aliases")
        else:
            logger.warning("ZMQ publisher not available, skipping cache invalidation broadcast")

    @staticmethod
    def _upsert_symbol(
        session: Any,
        native_symbol: str,
        base: str,
        quote: str | None,
        asset_type: AssetTypeEnum,
        now: datetime,
        session_id: str,
        sequence_id: int,
    ) -> str:
        """Upsert a Symbol row using SCD Type 2 close+insert.

        Finds the active Symbol by native_symbol. If identical, returns
        the existing public_id. If changed, closes the old row and inserts
        a new one with the same public_id. If not found, inserts a new row.

        Args:
            session: SQLAlchemy session.
            native_symbol: Native symbol string.
            base: Base currency code.
            quote: Quote currency code, or None for equity/index.
            asset_type: One of crypto, forex, equity, index.
            now: Current UTC timestamp.
            session_id: Producer session identifier for provenance.
            sequence_id: Per-topic monotonic counter for provenance.

        Returns:
            The public_id of the active Symbol row.
        """
        existing = session.execute(
            select(Symbol).where(
                Symbol.native_symbol == native_symbol,
                Symbol.timestamp <= now,
                Symbol.known_to > now,
            )
        ).scalar_one_or_none()
        if existing is None:
            sym = Symbol(
                native_symbol=native_symbol,
                base=base,
                quote=quote,
                asset_type=asset_type,
                created_at=now,
                timestamp=now,
                session_id=session_id,
                sequence_id=sequence_id,
            )
            session.add(sym)
            session.flush()
            return sym.public_id

        changed = (
            existing.base != base or existing.quote != quote or existing.asset_type != asset_type
        )
        if changed:
            close_and_insert_sync(
                session=session,
                model=Symbol,
                match_filters=[Symbol.native_symbol == native_symbol],
                new_values={
                    "native_symbol": native_symbol,
                    "base": base,
                    "quote": quote,
                    "asset_type": asset_type,
                    "created_at": existing.created_at,
                    "session_id": session_id,
                    "sequence_id": sequence_id,
                },
                bus_time=now,
            )
        return str(existing.public_id)

    @staticmethod
    def _upsert_alias(
        session: Any,
        symbol_public_id: str,
        exchange: str,
        channel: AliasChannelEnum,
        exchange_symbol: str,
        now: datetime,
        session_id: str,
        sequence_id: int,
    ) -> UpsertResult:
        """Upsert a SymbolAlias row using SCD Type 2 close+insert.

        Args:
            session: SQLAlchemy session.
            symbol_public_id: Public ID of the owning Symbol.
            exchange: Exchange name (lowercase).
            channel: Channel type (ws, rest, or ccxt).
            exchange_symbol: Exchange-specific symbol string.
            now: Current UTC timestamp.
            session_id: Producer session identifier for provenance.
            sequence_id: Per-topic monotonic counter for provenance.

        Returns:
            One of ``created``, ``updated``, or ``unchanged``.
        """
        existing = session.execute(
            select(SymbolAlias).where(
                SymbolAlias.symbol_public_id == symbol_public_id,
                SymbolAlias.exchange == exchange,
                SymbolAlias.channel == channel,
                SymbolAlias.timestamp <= now,
                SymbolAlias.known_to > now,
            )
        ).scalar_one_or_none()
        if existing is None:
            session.add(
                SymbolAlias(
                    symbol_public_id=symbol_public_id,
                    exchange=exchange,
                    channel=channel,
                    exchange_symbol=exchange_symbol,
                    created_at=now,
                    timestamp=now,
                    session_id=session_id,
                    sequence_id=sequence_id,
                )
            )
            return "created"
        if existing.exchange_symbol != exchange_symbol:
            close_and_insert_sync(
                session=session,
                model=SymbolAlias,
                match_filters=[
                    SymbolAlias.symbol_public_id == symbol_public_id,
                    SymbolAlias.exchange == exchange,
                    SymbolAlias.channel == channel,
                ],
                new_values={
                    "symbol_public_id": symbol_public_id,
                    "exchange": exchange,
                    "channel": channel,
                    "exchange_symbol": exchange_symbol,
                    "created_at": existing.created_at,
                    "session_id": session_id,
                    "sequence_id": sequence_id,
                },
                bus_time=now,
            )
            return "updated"
        return "unchanged"

    @staticmethod
    def _upsert_capability(
        session: Any,
        symbol_public_id: str,
        exchange: str,
        can_market_data: bool,
        can_trade: bool,
        source: str | None,
        reason: str | None,
        now: datetime,
        session_id: str,
        sequence_id: int,
    ) -> UpsertResult:
        """Upsert a SymbolExchangeCapability row using SCD Type 2 close+insert.

        Args:
            session: SQLAlchemy session.
            symbol_public_id: Public ID of the owning Symbol.
            exchange: Exchange name (lowercase).
            can_market_data: Whether exchange provides market data for this symbol.
            can_trade: Whether exchange supports trading this symbol.
            source: Origin of the capability information (e.g., updater name).
            reason: Human-readable explanation for the capability values.
            now: Current UTC timestamp.
            session_id: Producer session identifier for provenance.
            sequence_id: Per-topic monotonic counter for provenance.

        Returns:
            One of ``created``, ``updated``, or ``unchanged``.
        """
        existing = session.execute(
            select(SymbolExchangeCapability).where(
                SymbolExchangeCapability.symbol_public_id == symbol_public_id,
                SymbolExchangeCapability.exchange == exchange,
                SymbolExchangeCapability.timestamp <= now,
                SymbolExchangeCapability.known_to > now,
            )
        ).scalar_one_or_none()
        if existing is None:
            session.add(
                SymbolExchangeCapability(
                    symbol_public_id=symbol_public_id,
                    exchange=exchange,
                    can_market_data=can_market_data,
                    can_trade=can_trade,
                    source=source,
                    reason=reason,
                    created_at=now,
                    timestamp=now,
                    session_id=session_id,
                    sequence_id=sequence_id,
                )
            )
            return "created"
        changed = (
            existing.can_market_data != can_market_data
            or existing.can_trade != can_trade
            or existing.source != source
            or existing.reason != reason
        )
        if changed:
            close_and_insert_sync(
                session=session,
                model=SymbolExchangeCapability,
                match_filters=[
                    SymbolExchangeCapability.symbol_public_id == symbol_public_id,
                    SymbolExchangeCapability.exchange == exchange,
                ],
                new_values={
                    "symbol_public_id": symbol_public_id,
                    "exchange": exchange,
                    "can_market_data": can_market_data,
                    "can_trade": can_trade,
                    "source": source,
                    "reason": reason,
                    "created_at": existing.created_at,
                    "session_id": session_id,
                    "sequence_id": sequence_id,
                },
                bus_time=now,
            )
            return "updated"
        return "unchanged"

    @staticmethod
    def _ensure_instrument_identity(
        session: Any,
        symbol_public_id: str,
        exchange: str,
        now: datetime,
        session_id: str,
        sequence_id: int,
    ) -> str:
        """Ensure an Instrument row exists for the given symbol+exchange pair.

        Idempotent sync variant of Repository.ensure_instrument. Looks up
        the active Instrument by business key; inserts a new row if none
        exists. Never closes an existing version.

        Args:
            session: SQLAlchemy sync session.
            symbol_public_id: Public ID of the owning Symbol.
            exchange: Exchange name (lowercase).
            now: Current UTC timestamp for temporal fields.
            session_id: Producer session identifier.
            sequence_id: Per-topic monotonic counter.

        Returns:
            The instrument public_id (stable across SCD2 versions).
        """
        existing = session.execute(
            select(Instrument).where(
                Instrument.symbol_public_id == symbol_public_id,
                Instrument.exchange == exchange,
                Instrument.timestamp <= now,
                Instrument.known_to > now,
            )
        ).scalar_one_or_none()
        if existing is not None:
            return str(existing.public_id)
        inst = Instrument(
            symbol_public_id=symbol_public_id,
            exchange=exchange,
            timestamp=now,
            session_id=session_id,
            sequence_id=sequence_id,
        )
        session.add(inst)
        session.flush()
        return str(inst.public_id)

    @staticmethod
    def _resolve_one_funding_field[V](
        value: V | _Preserve,
        existing_value: V | None,
    ) -> V | None:
        """Resolve a single funding field using the sentinel protocol.

        ``PRESERVE_EXISTING`` → carry forward the existing value.
        ``None`` → write NULL (clear the field).
        Any other value → write that value.

        When there is no existing row, ``PRESERVE_EXISTING`` resolves
        to ``None`` (no value to carry forward).
        """
        if isinstance(value, _Preserve):
            return existing_value
        return value

    @staticmethod
    def _resolve_funding_fields(
        existing: InstrumentSpec | None,
        funding_type: str | None | _Preserve,
        funding_frequency_hours: int | None | _Preserve,
        rollover_rate_long: float | None | _Preserve,
        rollover_rate_short: float | None | _Preserve,
        max_funding_rate: float | None | _Preserve,
    ) -> tuple[str | None, int | None, float | None, float | None, float | None]:
        """Resolve funding metadata fields using the sentinel protocol.

        Three-way semantics per field:

        - ``PRESERVE_EXISTING`` (the default for callers that omit the
          argument): carry forward the existing row's value. Safe for
          unaware updaters that should not clobber data seeded by
          another updater.
        - ``None``: write NULL — explicitly clears the field. Used
          when the owning updater knows the instrument no longer has
          this funding characteristic.
        - Any concrete value: write that value.

        Returns a 5-tuple in the same order as the parameters.
        """
        resolve = SymbolUpdaterService._resolve_one_funding_field
        ex_funding_type = existing.funding_type if existing else None
        ex_freq = existing.funding_frequency_hours if existing else None
        ex_long = existing.rollover_rate_long if existing else None
        ex_short = existing.rollover_rate_short if existing else None
        ex_max = existing.max_funding_rate if existing else None
        return (
            resolve(funding_type, ex_funding_type),
            resolve(funding_frequency_hours, ex_freq),
            resolve(rollover_rate_long, ex_long),
            resolve(rollover_rate_short, ex_short),
            resolve(max_funding_rate, ex_max),
        )

    @staticmethod
    def _revise_instrument_spec(
        session: Any,
        instrument_public_id: str,
        now: datetime,
        session_id: str,
        sequence_id: int,
        expiry_at: datetime | None = None,
        instrument_kind: str | None = None,
        funding_type: str | None | _Preserve = PRESERVE_EXISTING,
        funding_frequency_hours: int | None | _Preserve = PRESERVE_EXISTING,
        rollover_rate_long: float | None | _Preserve = PRESERVE_EXISTING,
        rollover_rate_short: float | None | _Preserve = PRESERVE_EXISTING,
        max_funding_rate: float | None | _Preserve = PRESERVE_EXISTING,
    ) -> None:
        """Merge spec fields into InstrumentSpec (sync).

        Reads the existing spec (if any) and carries forward fields the
        caller did not provide. Three patterns coexist in the merged
        payload:

        - **Carry-forward** (``tick_size``, ``lot_size``, etc.): always
          taken from the existing row, since these helpers do not accept
          them as parameters.
        - **Direct assignment** (``expiry_at``, ``instrument_kind``):
          always overwritten with the parameter value, even when None.
        - **Sentinel carry-forward** (the funding fields): resolved
          via :meth:`_resolve_funding_fields`. ``PRESERVE_EXISTING``
          (the default) carries forward the existing value;
          ``None`` explicitly clears the field to NULL; any concrete
          value overwrites.

        Args:
            session: SQLAlchemy sync session.
            instrument_public_id: Public ID of the instrument.
            now: Current UTC timestamp.
            session_id: Producer session identifier.
            sequence_id: Per-topic monotonic counter.
            expiry_at: Contract expiry timestamp (UTC), or None.
            instrument_kind: Product type string, or None.
            funding_type: Funding model identifier, ``None`` to clear,
                or ``PRESERVE_EXISTING`` to carry forward.
            funding_frequency_hours: Hours between accrual boundaries,
                ``None`` to clear, or ``PRESERVE_EXISTING`` to carry
                forward.
            rollover_rate_long: Spot margin rollover rate per boundary
                for long positions, ``None`` to clear, or
                ``PRESERVE_EXISTING`` to carry forward.
            rollover_rate_short: Spot margin rollover rate per boundary
                for short positions, ``None`` to clear, or
                ``PRESERVE_EXISTING`` to carry forward.
            max_funding_rate: Per-boundary cap on perpetual funding
                rate magnitude, ``None`` to clear, or
                ``PRESERVE_EXISTING`` to carry forward.
        """
        existing = DatabaseRepository.get_instrument_spec_sync(
            session,
            instrument_public_id,
            now,
        )
        (
            resolved_funding_type,
            resolved_funding_frequency_hours,
            resolved_rollover_rate_long,
            resolved_rollover_rate_short,
            resolved_max_funding_rate,
        ) = SymbolUpdaterService._resolve_funding_fields(
            existing,
            funding_type,
            funding_frequency_hours,
            rollover_rate_long,
            rollover_rate_short,
            max_funding_rate,
        )
        spec = InstrumentSpecInput(
            tick_size=existing.tick_size if existing else None,
            lot_size=existing.lot_size if existing else None,
            min_order_size=existing.min_order_size if existing else None,
            max_order_size=existing.max_order_size if existing else None,
            cost_decimals=existing.cost_decimals if existing else None,
            qty_decimals=existing.qty_decimals if existing else None,
            margin_initial=existing.margin_initial if existing else None,
            position_limit_long=existing.position_limit_long if existing else None,
            position_limit_short=existing.position_limit_short if existing else None,
            status=existing.status if existing else None,
            expiry_at=expiry_at,
            instrument_kind=instrument_kind,
            funding_type=resolved_funding_type,
            funding_frequency_hours=resolved_funding_frequency_hours,
            rollover_rate_long=resolved_rollover_rate_long,
            rollover_rate_short=resolved_rollover_rate_short,
            max_funding_rate=resolved_max_funding_rate,
        )
        DatabaseRepository.revise_instrument_spec_sync(
            session=session,
            instrument_public_id=instrument_public_id,
            session_id=session_id,
            sequence_id=sequence_id,
            timestamp=now,
            spec=spec,
        )

    @staticmethod
    def _deactivate_stale_capabilities(
        session: Any,
        exchange: str,
        active_symbol_public_ids: set[str],
        source: str,
        now: datetime,
        session_id: str,
        next_sequence_fn: Callable[[], int],
    ) -> int:
        """Deactivate capabilities for symbols no longer seen on the exchange.

        Uses SCD Type 2 close+insert to set ``can_trade=False`` and
        ``can_market_data=False`` for capability rows belonging to
        ``exchange`` whose ``symbol_public_id`` is not in
        ``active_symbol_public_ids``.

        Args:
            session: SQLAlchemy session.
            exchange: Exchange name (lowercase).
            active_symbol_public_ids: Set of symbol public IDs still active.
            source: Updater source tag for the deactivation record.
            now: Current UTC timestamp.
            session_id: Producer session identifier for provenance.
            next_sequence_fn: Callable returning the next sequence_id for each
                row.

        Returns:
            Number of deactivated capability rows.
        """
        stmt = select(SymbolExchangeCapability).where(
            SymbolExchangeCapability.exchange == exchange,
            SymbolExchangeCapability.timestamp <= now,
            SymbolExchangeCapability.known_to > now,
            or_(
                SymbolExchangeCapability.can_trade.is_(True),
                SymbolExchangeCapability.can_market_data.is_(True),
            ),
        )
        active_caps = session.execute(stmt).scalars().all()
        deactivated = 0
        for cap in active_caps:
            if cap.symbol_public_id not in active_symbol_public_ids:
                close_and_insert_sync(
                    session=session,
                    model=SymbolExchangeCapability,
                    match_filters=[
                        SymbolExchangeCapability.symbol_public_id == cap.symbol_public_id,
                        SymbolExchangeCapability.exchange == exchange,
                    ],
                    new_values={
                        "symbol_public_id": cap.symbol_public_id,
                        "exchange": exchange,
                        "can_market_data": False,
                        "can_trade": False,
                        "source": source,
                        "reason": "Delisted: not seen in updater run",
                        "created_at": cap.created_at,
                        "session_id": session_id,
                        "sequence_id": next_sequence_fn(),
                    },
                    bus_time=now,
                )
                deactivated += 1
        return deactivated

    @staticmethod
    def _reconcile_capabilities(
        session: Any,
        exchange: str,
        active_symbol_public_ids: set[str],
        source: str,
        now: datetime,
        session_id: str,
        next_sequence_fn: Callable[[], int],
        min_active_ratio: float = 0.5,
    ) -> int:
        """Reconcile capabilities: deactivate stale rows with safety threshold.

        Skips deactivation when the ratio of active symbols to previously
        known active capabilities is below ``min_active_ratio``, guarding
        against mass deactivation caused by partial API responses.

        Args:
            session: SQLAlchemy session.
            exchange: Exchange name (lowercase).
            active_symbol_public_ids: Set of symbol public IDs seen in the current run.
            source: Updater source tag.
            now: Current UTC timestamp.
            min_active_ratio: Minimum ratio of active / existing to proceed.
            session_id: Producer session identifier for provenance.
            next_sequence_fn: Callable returning the next sequence_id for each
                deactivated row. Threaded through to ``_deactivate_stale_capabilities``.

        Returns:
            Number of deactivated rows (0 if skipped due to safety threshold).
        """
        existing_count_stmt = select(SymbolExchangeCapability).where(
            SymbolExchangeCapability.exchange == exchange,
            SymbolExchangeCapability.timestamp <= now,
            SymbolExchangeCapability.known_to > now,
            or_(
                SymbolExchangeCapability.can_trade.is_(True),
                SymbolExchangeCapability.can_market_data.is_(True),
            ),
        )
        existing_count = len(session.execute(existing_count_stmt).scalars().all())
        if existing_count > 0:
            ratio = len(active_symbol_public_ids) / existing_count
            if ratio < min_active_ratio:
                logger.warning(
                    f"Skipping capability reconciliation for {exchange}: "
                    f"active/existing ratio {ratio:.1%} < threshold {min_active_ratio:.0%} "
                    f"({len(active_symbol_public_ids)} active vs {existing_count} existing)"
                )
                return 0
        deactivated = SymbolUpdaterService._deactivate_stale_capabilities(
            session,
            exchange,
            active_symbol_public_ids,
            source,
            now,
            session_id=session_id,
            next_sequence_fn=next_sequence_fn,
        )
        if deactivated > 0:
            logger.info(
                f"Reconciled {exchange} capabilities: deactivated {deactivated} stale symbols"
            )
        return deactivated

    @staticmethod
    def _reconcile_aliases(session: Any, exchange: str, now: datetime) -> int:
        """Close SCD2 aliases for symbols whose current capability is deactivated.

        Sets ``known_to = now`` on every ``symbol_aliases`` row that is
        currently active (``known_to > now``) and whose owning
        ``symbol_public_id`` has a current ``symbol_exchange_capabilities``
        row reporting BOTH ``can_market_data = False`` AND
        ``can_trade = False``. Close-only — does not insert a successor
        row; the absence of an active alias is itself the signal that
        the alias is retired.

        This is the canonical cleanup for the architectural rule that
        the operational gate for "can we use this symbol now" is the
        capability row, not the alias row (see
        ``proprietary/memory/feedback_capability_gate_on_symbol_aliases.md``).
        Without this reconcile step, ``symbol_aliases`` rows for
        delisted / expired / retired contracts accumulate forever with
        ``known_to = KNOWN_TO_MAX``, polluting
        ``mapper.native_to_<exchange>_<channel>`` reverse dicts and
        forcing every alias-consuming code path to remember the
        capability filter (the 2026-05-25 incident root cause).

        Idempotent: a second invocation with no new capability changes
        closes zero rows because every targeted alias is already
        closed. Safe to run in the same DB transaction as
        :func:`_reconcile_capabilities` (both writes commit together
        or roll back together; a crash midway leaves stores
        consistent).

        Live data consumers (publishers + WS message adapters) only
        ever resolve currently-subscribed symbols, and the publishers
        already exclude delisted symbols via ``can_market_data`` (see
        commit ``c940e791``); they tolerate alias closure without
        change. Historical backfill helpers
        (``application/updaters/historical/*``) expand only ACTIVE
        symbols via ``get_available_*`` and additionally use null-safe
        ``.get()`` for reverse lookups, so they degrade gracefully on
        a missing alias. A bitemporal AS-OF lookup helper is NOT
        shipped today — no current caller needs one. If a future
        replay path needs to resolve a delisted exchange symbol AT
        the historical time of delivery, build the helper then; the
        closed alias row's slice still covers the historical period
        (``timestamp <= as_of < known_to``), the in-memory mapper
        dict is just current-only.

        Args:
            session: SQLAlchemy session (shared with caller's
                transaction).
            exchange: Exchange name (lowercase), matches the
                ``symbol_aliases.exchange`` column.
            now: Current UTC timestamp; used both as the close-time
                cutoff and the new ``known_to`` value.

        Returns:
            Number of alias rows closed by this invocation. Returns 0
            when there is nothing to do.
        """
        disabled_capability_ids = select(SymbolExchangeCapability.symbol_public_id).where(
            SymbolExchangeCapability.exchange == exchange,
            SymbolExchangeCapability.timestamp <= now,
            SymbolExchangeCapability.known_to > now,
            SymbolExchangeCapability.can_market_data.is_(False),
            SymbolExchangeCapability.can_trade.is_(False),
        )
        result = session.execute(
            update(SymbolAlias)
            .where(
                SymbolAlias.exchange == exchange,
                SymbolAlias.known_to > now,
                SymbolAlias.symbol_public_id.in_(disabled_capability_ids),
            )
            .values(known_to=now)
        )
        rowcount = getattr(result, "rowcount", None)
        closed = rowcount if isinstance(rowcount, int) and rowcount > 0 else 0
        if closed > 0:
            logger.info(f"Reconciled {exchange} aliases: closed {closed} retired alias rows")
        return closed

    def _get_last_update_timestamp(self) -> datetime | None:
        """Retrieve the timestamp of the last symbol mapping update from database.

        Returns:
            Datetime of last update, or None if no previous update exists.
        """
        try:
            assert self.repository is not None, "Repository not initialized"
            with self.repository.get_session() as session:
                stmt = select(Setting).where(
                    Setting.key == self._get_setting_key(), *where_active_now(Setting)
                )
                setting = session.execute(stmt).scalar_one_or_none()
                if setting is None or setting.value == "null" or not setting.value:
                    return None
                return datetime.fromisoformat(setting.value.replace("Z", "+00:00"))
        except Exception as e:
            logger.warning(f"Error getting last update timestamp: {e}")
            return None

    async def _set_last_update_timestamp(self, timestamp: datetime) -> None:
        """Store the timestamp of the current symbol mapping update to database.

        Args:
            timestamp: Datetime to record as the last update time.
        """
        try:
            repository = get_repository(self.settings.db_url)
            async with repository.session() as session:
                iso_timestamp = timestamp.isoformat()
                await close_and_insert(
                    session=session,
                    model=Setting,
                    match_filters=[Setting.key == self._get_setting_key()],
                    new_values={
                        "key": self._get_setting_key(),
                        "value": iso_timestamp,
                        "category": "system",
                        "description": f"Timestamp of last {self._get_setting_key()} update",
                        "session_id": self._tracker.session_id,
                        "sequence_id": self._tracker.next_sequence("settings"),
                    },
                    bus_time=timestamp,
                )
                await session.commit()
                logger.info(f"Updated timestamp: {self._get_setting_key()} = {iso_timestamp}")
        except Exception as e:
            logger.error(f"Error setting last update timestamp: {e}")
            raise

    def should_update(self) -> bool:
        """Check if symbol mappings should be updated based on threshold and force flag.

        Returns:
            True if update should proceed, False otherwise.
        """
        if self.force:
            logger.info("Force mode enabled - update will proceed")
            return True
        last_update = self._get_last_update_timestamp()
        if last_update is None:
            logger.info("No previous update found - update will proceed")
            return True
        time_since_update = datetime.now(UTC) - last_update
        threshold = timedelta(hours=self.update_threshold_hours)
        if time_since_update < threshold:
            remaining = threshold - time_since_update
            hours_ago = time_since_update.total_seconds() / 3600
            hours_remaining = remaining.total_seconds() / 3600
            logger.info(
                f"Update not needed - last update {hours_ago:.1f}h ago "
                f"(threshold: {self.update_threshold_hours}h, remaining: {hours_remaining:.1f}h)"
            )
            return False
        logger.info(
            f"Update needed - last update {time_since_update.total_seconds() / 3600:.1f}h ago "
            f"(threshold: {self.update_threshold_hours}h)"
        )
        return True

    async def _fetch_symbols(self, client: T) -> list[dict[str, Any]]:
        """Fetch symbol data from the exchange client.

        Args:
            client: Exchange client instance to fetch symbols from.

        Returns:
            List of symbol dictionaries containing exchange instrument data.
        """
        logger.info("Fetching symbols via subscribe_instruments()...")
        symbols: list[dict[str, Any]] = []
        async for symbol_data in client.subscribe_instruments():
            symbols.append(symbol_data)
        logger.info(f"Fetched {len(symbols)} symbols from exchange")
        return symbols

    async def start(self) -> None:
        """Execute the symbol mapping update process."""
        setting_key = self._get_setting_key()
        exchange = setting_key.split("_")[0]
        set_log_context(f"sym:{exchange}")
        try:
            self.repository = DatabaseRepository(self.settings.db_url)
            logger.info("Repository initialized")
            settings_service = await get_settings_service(
                self.settings.db_url,
                self.settings.zmq_broker_xsub,
            )
            self.settings = get_settings_with_service(settings_service)
            logger.info("AppSettings service initialized with database access")
            if not self.should_update():
                logger.info("Skipping update")
                return
            self._setup_zmq()
            client = self._create_exchange_client()
            await client.connect()
            logger.info(f"Exchange client connected: {type(client).__name__}")
            try:
                symbols = await self._fetch_symbols(client)
                await self._update_database(symbols)
                await self.broadcast_cache_invalidation()
                await self._set_last_update_timestamp(datetime.now(UTC))
                logger.info("Symbol mapping update completed successfully")
            finally:
                await client.disconnect()
                logger.info("Exchange client disconnected")
        except Exception as e:
            logger.error(f"Symbol mapping update failed: {e}")
            raise
        finally:
            self._cleanup_zmq()
            self.repository = None
