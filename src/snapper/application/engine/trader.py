"""Trader coordinator module.

This module provides the central trading coordination through TraderCoordinator.
It is the single point of entry for all trading signals in the system - there
should be exactly ONE TraderCoordinator running per deployment.

The coordinator:
- Subscribes to ZMQ signal topics from strategy publishers
- Creates and manages TradingEngineService instances per instrument
- Handles settings updates and symbol mapping changes
- Monitors signal health and execution fills
"""

import asyncio
import contextlib
import json
import time
from collections import OrderedDict
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from typing import Any
from typing import cast
from typing import get_args
from uuid import uuid7

import zmq
import zmq.asyncio
from loguru import logger
from sqlalchemy.exc import IntegrityError

from snapper.application.engine.config import EngineConfigModel
from snapper.application.engine.guard_scanner import PairedExecutionGuardScanner
from snapper.application.engine.service import InstrumentSpec
from snapper.application.engine.service import TradingEngineService
from snapper.application.engine.service import compute_shard_key
from snapper.application.portfolio.models import PositionStateModel
from snapper.application.pricing.usd_converter import USDConverter
from snapper.application.process_manager.models import RegisterableProcess
from snapper.application.process_manager.process_parameters import TraderParameters
from snapper.application.process_manager.registry import register_process
from snapper.application.risk.models import RiskConfigModel
from snapper.application.risk.models import RiskEvaluator
from snapper.application.services.settings import SettingsService
from snapper.application.trade.balance_service import BalanceService
from snapper.application.trade.caps_enforcer import TradingCapsEnforcer
from snapper.application.trade.command_request import order_request_from_command
from snapper.application.trade.command_request import parse_shard_key
from snapper.application.trade.outbox import OutboxDispatcher
from snapper.application.trade.reconciler import ReconciliationLoop
from snapper.application.trade.trade_service import ShardState
from snapper.application.trade.trade_service import TradeService
from snapper.config.settings import AppSettings
from snapper.config.settings import get_bootstrap_settings
from snapper.config.settings import get_settings
from snapper.config.settings import get_settings_service
from snapper.config.settings import get_settings_with_service
from snapper.core.paired_execution import paired_halt_reason
from snapper.core.partitioning import ShardOwnership
from snapper.core.types import ORDER_STATUS_REASON_ADOPTED
from snapper.core.types import ExchangeEnum
from snapper.core.types import ExecutionModeEnum
from snapper.core.types import FillStatusEnum
from snapper.core.types import OrderCommandEnum
from snapper.core.types import OrderEventEnum
from snapper.core.types import OrderExchange
from snapper.core.types import PairedExecutionGroupStatusEnum
from snapper.core.types import PairedExecutionLegStatusEnum
from snapper.core.types import PairedFillProjection
from snapper.core.types import ProcessModeEnum
from snapper.core.types import ProcessRoleEnum
from snapper.core.types import TradeSideEnum
from snapper.core.wallet_short import compute_legacy_wallet_short
from snapper.core.wallet_short import compute_wallet_short
from snapper.data.repository import SQLAlchemyRepository
from snapper.data.repository import get_repository
from snapper.data.repository_types import AccrualLedgerInsertRow
from snapper.data.repository_types import ExecutionRow
from snapper.data.repository_types import OrderRow
from snapper.data.repository_types import PairedExecutionGroupInsertRow
from snapper.data.repository_types import PairedExecutionGroupRow
from snapper.data.repository_types import PairedExecutionLegInsertRow
from snapper.data.repository_types import PairedExecutionLegRow
from snapper.data.repository_types import PositionCycleInsertRow
from snapper.data.repository_types import PositionCycleRow
from snapper.data.repository_types import TradeCommandRow
from snapper.data.repository_types import TradeProjectionCheckpointRow
from snapper.data.repository_types import VenueEventRow
from snapper.infrastructure.symbols.functions import is_tradeable
from snapper.infrastructure.symbols.functions import resolve_symbol_public_id
from snapper.infrastructure.symbols.mapper import SymbolMapperService
from snapper.messaging.infrastructure.gap_detector import GapDetector
from snapper.messaging.infrastructure.publisher import MessagePublisher
from snapper.messaging.infrastructure.publisher import SequenceTracker
from snapper.messaging.infrastructure.validated_socket import HWM_ORDER_FLOW
from snapper.messaging.infrastructure.validated_socket import ValidatedPublisher
from snapper.messaging.infrastructure.validated_socket import ValidatedSubscriber
from snapper.messaging.infrastructure.validated_socket import apply_hwm
from snapper.messaging.schemas.data import ExecutionData
from snapper.messaging.schemas.data import FundingAccrualData
from snapper.messaging.schemas.data import HeartbeatData
from snapper.messaging.schemas.data import OrderCancelData
from snapper.messaging.schemas.data import OrderData
from snapper.messaging.schemas.data import OrderEventData
from snapper.messaging.schemas.data import SettingChangedData
from snapper.messaging.schemas.data import SignalData
from snapper.messaging.schemas.messages import MessageParseError
from snapper.messaging.schemas.messages import parse_message
from snapper.messaging.topics.builders import ParsedOrderTopic
from snapper.messaging.topics.builders import accrual_topic
from snapper.messaging.topics.builders import order_command_topic
from snapper.messaging.topics.builders import order_event_topic
from snapper.messaging.topics.builders import parse_order_event_topic
from snapper.messaging.topics.builders import parse_signal_topic

_bootstrap_settings = get_bootstrap_settings()

_REARM_RETIRED_CIDS_MAX = 10_000
"""Bound of the retired-cid LRU guarding late adopted re-arm frames (#155).

Mirrors the engine exec-id dedupe bound; at one order per second this
covers hours of lookback while capping memory."""

_COMPLETED_GROUP_REPLAY_WINDOW = timedelta(days=7)
"""How far back startup recovery replays fills into COMPLETED paired groups.

A late original fill that landed while the coordinator was DOWN must reopen
its completed group (the live reopen trigger never fires for events already
persisted), but listing ALL completed groups is unbounded over a deployment's
lifetime, so the replay is bounded to groups completed within this window.
Venue late fills realistically trail by minutes; a week is a generous margin
for an extended outage. A fill older than the window is reconciliation /
operator territory.
"""


def _compute_pending_boundaries(
    frequency_hours: int,
    position_opened_at: datetime,
    last_accrued_at: datetime | None,
    now: datetime,
) -> list[datetime]:
    """Compute accrual boundary timestamps that are due but not yet applied.

    Boundaries are absolute UTC times aligned to ``frequency_hours``
    epochs (e.g., every 4 hours from midnight: 00:00, 04:00, 08:00,
    ...). The list is clamped so that the earliest boundary is after
    ``position_opened_at`` (a reopened position must not retro-charge
    boundaries from a previous open cycle) and after
    ``last_accrued_at`` (boundaries already persisted are not
    replayed).

    Args:
        frequency_hours: Hours between accrual boundaries.
        position_opened_at: Timestamp when the current position was
            opened. Boundaries before this time are excluded.
        last_accrued_at: Most recent accrued boundary (from the
            ledger), or None if no accrual has ever been applied.
        now: Current UTC time. Boundaries strictly before ``now`` are
            included; the boundary AT ``now`` is excluded (it will be
            applied on the next iteration).

    Returns:
        Sorted list of boundary datetimes to be applied.
    """
    interval = timedelta(hours=frequency_hours)
    interval_seconds = interval.total_seconds()
    start_after = max(
        position_opened_at,
        last_accrued_at if last_accrued_at is not None else position_opened_at,
    )
    epoch = datetime(2020, 1, 1, tzinfo=UTC)
    start_elapsed = (start_after - epoch).total_seconds()
    first_n = int(start_elapsed // interval_seconds) + 1
    boundaries: list[datetime] = []
    n = first_n
    while len(boundaries) <= 100:
        boundary = epoch + timedelta(seconds=n * interval_seconds)
        if boundary >= now:
            break
        boundaries.append(boundary)
        n += 1
    return boundaries


_ACCRUAL_POLL_SECONDS: float = 60.0

_FUNDING_TYPE_TO_ACCRUAL_TYPE: dict[str, str] = {
    "spot_margin_rollover": "rollover",
    "perpetual_funding": "funding",
}


@dataclass(frozen=True)
class SignalRoutingContext:
    """Parsed routing data for one signal delivery."""

    exchange: OrderExchange
    mode: str
    strategy_tag: str | None
    wallet_public_id: str
    operator_public_id: str
    engine_key: str
    shard_key: str


class _EngineRegistry(dict[str, "TradingEngineService"]):
    """``self.engines`` subclass that auto-indexes engines on insert.

    Wrapping the engines map ensures every ``self.engines[key] = engine``
    site — including the direct insertions test helpers reach for —
    fans out into the fill-dispatch lookup indices the
    :py:class:`TraderCoordinator` maintains for O(1) coid/scope match.
    The auto-register callback is owned by the coordinator so it sees
    the full ``self``-bound state when the engine arrives.
    """

    def __init__(
        self,
        register_callback: Callable[[TradingEngineService], None],
    ) -> None:
        """Bind the auto-registration callback the trader uses to keep indices fresh."""
        super().__init__()
        self._on_register = register_callback

    def __setitem__(self, key: str, value: TradingEngineService) -> None:
        """Insert and fan out the new engine to the trader's lookup indices."""
        super().__setitem__(key, value)
        self._on_register(value)


@register_process(
    "trader_coordinator",
    description="Trading coordinator",
    priority=40,
    role=ProcessRoleEnum.CORE,
    tags=("trading", "signals", "risk"),
    parameters_model=TraderParameters,
    enabled=True,
    mode=ProcessModeEnum.THREAD,
)
class TraderCoordinator(RegisterableProcess):
    """Central trading coordinator - ONE instance per system.

    The TraderCoordinator is the heart of the trading system. It:
    - Subscribes to strategy signal topics via ZMQ
    - Dynamically creates TradingEngineService for each instrument/exchange
    - Routes signals to appropriate engines
    - Handles system events (settings changes, symbol mapping updates)
    - Monitors execution fills and order status updates

    Only ONE TraderCoordinator should run per deployment to ensure
    consistent position tracking and risk management.

    Attributes:
        settings: Application settings.
        signal_topics: List of ZMQ topics to subscribe for signals.
        repository: Database repository for instrument persistence.
        engines: Dict mapping engine_key to TradingEngineService instances.
        zmq_context: ZMQ context for signal subscription.
        signal_subscriber: ZMQ subscriber socket.
        last_signal_time: Dict tracking last signal time per engine.
        execution_publisher: ZMQ publisher for order requests.
    """

    def __init__(
        self,
        signal_topics: list[str] | None = None,
        *,
        settings: AppSettings | None = None,
    ):
        """Initialize the trader coordinator.

        Args:
            signal_topics: List of ZMQ topic prefixes to subscribe to.
                Defaults to ["signals."] to receive all strategy signals.
            settings: Optional pre-built :class:`AppSettings` for tests
                that need to inject per-coordinator
                ``coordinator_instance_id`` / ``coordinator_instance_count``
                without going through the DB-backed settings service.
                When ``None`` (production), :meth:`_initialize_settings`
                upgrades ``self.settings`` via
                :func:`get_settings_with_service` exactly as before.
        """
        self.settings = get_settings()
        self._injected_settings: AppSettings | None = settings
        self.signal_topics = signal_topics or ["signals."]
        self.repository = get_repository(self.settings.db_url)
        self._engines_by_pending_coid: dict[str, TradingEngineService] = {}
        self._engines_by_scope: dict[tuple[str, str, str], TradingEngineService] = {}
        self._engines_by_scope_legacy: dict[tuple[str, str], TradingEngineService] = {}
        self.engines: _EngineRegistry = _EngineRegistry(self._register_engine_for_lookup)
        self.zmq_context: zmq.asyncio.Context | None = None
        self.signal_subscriber: ValidatedSubscriber | None = None
        self.last_signal_time: dict[str, float] = {}
        self.execution_context: zmq.Context[Any] | None = None
        self.execution_publisher: ValidatedPublisher | None = None
        self.msg_publisher: MessagePublisher | None = None
        self._tracker: SequenceTracker = SequenceTracker()
        self._gap_detector: GapDetector = GapDetector("trader")
        self._current_topic: str = ""
        self.trade_service: TradeService = TradeService()
        self.balance_service: BalanceService = BalanceService()
        self.outbox: OutboxDispatcher | None = None
        self.guard_scanner: PairedExecutionGuardScanner | None = None
        self._order_shard_keys: dict[str, str] = {}
        self._rearm_retired_cids: OrderedDict[str, None] = OrderedDict()
        self._wallet_short_to_id: dict[str, str] = {}
        self._unhealthy_executor_scopes: set[str] = set()
        self._ownership: ShardOwnership | None = None
        self._caps_enforcer: TradingCapsEnforcer | None = None

    @staticmethod
    def get_default_parameters(settings: AppSettings) -> dict[str, Any]:
        """Get default parameters from settings.

        Args:
            settings: Application settings instance.

        Returns:
            Dict with default parameters for TraderCoordinator.
        """
        return {
            "signal_topics": ["signals."],
        }

    def __repr__(self) -> str:
        """Return string representation."""
        return f"TraderCoordinator(signal_topics={self.signal_topics})"

    @staticmethod
    def _build_engine_key(
        instrument: str,
        exchange: str,
        mode_or_tag: str,
        wallet_public_id: str,
    ) -> str:
        """Build the in-memory engine_key string.

        Wallet-aware sharding: when ``wallet_public_id`` is
        non-empty, the key gets a ``-w{wallet_short}`` suffix where
        ``wallet_short`` is the last 12 hex characters of the wallet
        UUID7 with dashes stripped (matching the spawner naming used
        by ``ProcessLauncherService.spawn_per_wallet_executors`` and
        the ``TradingEngineService._shard_key`` segment). Empty
        ``wallet_public_id`` keeps the flat format
        (``{instrument}@{exchange}-{mode_or_tag}``) for backwards
        compatibility with the single-wallet template path.

        The wallet-aware key is what unblocks the
        ``_on_signal`` fail-closed guard:
        the recovery sites at :meth:`_recover_from_checkpoints`,
        :meth:`_recover_from_executions`, and
        :meth:`_recover_active_orders` all parse the persisted
        ``shard_key`` segment back into ``wallet_public_id`` so
        live and recovery engines key the same way.
        """
        base = f"{instrument}@{exchange}-{mode_or_tag}"
        if wallet_public_id:
            wallet_short = compute_wallet_short(wallet_public_id)
            return f"{base}-w{wallet_short}"
        return base

    @staticmethod
    def _parse_shard_key(shard_key: str) -> tuple[str, str, str, str, str | None] | None:
        """Parse a persisted ``shard_key`` into its components.

        Thin delegate to the shared
        :func:`snapper.application.trade.command_request.parse_shard_key`
        (the executor-side request reconstruction needs the identical
        parse), kept as a method for the existing call sites and tests.

        Returns:
            Tuple of ``(exchange, instrument, mode, wallet_short,
            strategy_tag)`` where ``wallet_short`` is empty for
            legacy keys and ``strategy_tag`` is None when absent.
            Returns ``None`` if the key has fewer than 3 segments.
        """
        return parse_shard_key(shard_key)

    async def start(self) -> None:
        """Start the trader coordinator.

        Sets up ZMQ connections, initializes trading components
        and enters the main trading loop.
        ``self._ownership`` is built AFTER settings resolve
        (``_initialize_settings``) and BEFORE the trade service / outbox
        / reconciliation loops are constructed.
        """
        logger.info("Starting ZMQ Signal TraderCoordinator")
        logger.info(f"Signal Topics: {self.signal_topics}")
        await self._initialize_settings()
        self._ownership = self._build_ownership()
        logger.info(
            "TraderCoordinator instance {}/{} owns ~{:.1f}% of shards",
            self._ownership.instance_id,
            self._ownership.instance_count,
            100.0 / self._ownership.instance_count,
        )
        self._caps_enforcer = self._build_caps_enforcer()
        self._setup_external_execution()
        self._setup_trading_components()
        self._setup_signal_subscriber()
        self._setup_trade_services()
        await self._recover_engine_state()
        await self._recover_paired_execution_leg_fills()
        await self._recover_paired_execution_guard_state()
        await self._run_trading_loop()

    def _build_ownership(self) -> ShardOwnership:
        """Construct :class:`ShardOwnership` from current ``self.settings``.

        Called from :meth:`start` after ``_initialize_settings`` has
        upgraded (or test-injected) ``self.settings``. Validation is
        fail-fast: ``instance_count < 1``, ``instance_id`` outside
        ``[0, instance_count)``, or ``instance_count > 1`` on a SQLite
        backend raises :class:`ValueError` with a message naming the
        invalid value, which surfaces the operator misconfiguration
        before any signal is dispatched. SQLite compiles
        ``SELECT ... FOR UPDATE`` (and ``NOWAIT``) to a plain SELECT, so
        multiple coordinators cannot serialize their row claims — the
        money-path CAS/claim DALs would race instead of queueing, which
        only PostgreSQL prevents. A single-instance SQLite coordinator is
        allowed but logged: concurrent WRITER processes against the same
        file (e.g. a full local ZMQ stack) carry the same caveat, per the
        repository module's documented locking contract.

        Returns:
            A validated :class:`ShardOwnership` for this coordinator.

        Raises:
            ValueError: If ``coordinator_instance_count < 1``,
                ``coordinator_instance_id`` is outside the half-open
                range ``[0, instance_count)``, or ``instance_count > 1``
                with a SQLite repository backend.
        """
        instance_id = self.settings.coordinator_instance_id
        instance_count = self.settings.coordinator_instance_count
        if instance_count < 1:
            raise ValueError(f"coordinator_instance_count must be >= 1, got {instance_count}")
        if not 0 <= instance_id < instance_count:
            raise ValueError(
                f"coordinator_instance_id {instance_id} out of range [0, {instance_count})"
            )
        if self.repository.dialect_name == "sqlite":
            if instance_count > 1:
                raise ValueError(
                    "coordinator_instance_count > 1 is unsupported on a SQLite "
                    "backend: SELECT ... FOR UPDATE is a no-op on sqlite, so "
                    "cross-instance row claims cannot serialize; use PostgreSQL "
                    "for multi-instance deployments"
                )
            logger.warning(
                "ZMQTrader: SQLite backend — SELECT ... FOR UPDATE is a no-op; "
                "running additional writer processes against this database file "
                "is unsupported (see the repository module locking contract)"
            )
        return ShardOwnership(
            instance_id=instance_id,
            instance_count=instance_count,
        )

    def _build_caps_enforcer(self) -> TradingCapsEnforcer | None:
        """Construct the process-singleton :class:`TradingCapsEnforcer`.

        Wires ``USDConverter(repository)`` as the pricing oracle and
        hands both to the enforcer. Called once at
        :meth:`start` after settings resolve + ownership is built so
        the enforcer is ready before any child engine is spawned.
        Returns ``None`` when the repository is not a
        :class:`SQLAlchemyRepository` — test fixtures that inject a
        MagicMock repo fall through the enforcer entirely, which is
        the same behavior the engine had before so pre-existing
        coordinator tests stay byte-identical.
        """
        if not isinstance(self.repository, SQLAlchemyRepository):
            return None
        pricing = USDConverter(repository=self.repository)
        return TradingCapsEnforcer(repository=self.repository, pricing=pricing)

    async def _initialize_settings(self) -> None:
        """Upgrade settings to DB-backed instance for runtime access.

        Without this, accessing DB settings like risk_r_per_trade would
        raise RuntimeError because the bootstrap-only AppSettings does
        not have a SettingsService.
        When ``__init__`` received a
        pre-built :class:`AppSettings` via the ``settings`` kwarg, the
        DB-service upgrade is skipped and the injected instance becomes
        ``self.settings``. This lets integration tests provide
        per-coordinator ``coordinator_instance_id`` without spinning up
        a DB-backed settings service per instance.
        """
        if self._injected_settings is not None:
            self.settings = self._injected_settings
            self.repository = get_repository(self.settings.db_url)
            await self._build_wallet_short_cache()
            logger.info("ZMQTrader: using injected settings (test path)")
            return

        settings_service = await get_settings_service(
            self.settings.db_url,
            self.settings.zmq_broker_xsub,
        )
        self.settings = get_settings_with_service(settings_service)
        self.repository = get_repository(self.settings.db_url)
        await self._build_wallet_short_cache()
        logger.info("ZMQTrader: Settings service initialized with database access")

    async def _build_wallet_short_cache(self) -> None:
        """Populate the wallet_short -> wallet_public_id cache.

        Reads every active row from ``wallet_credentials`` and indexes
        the wallet by its 12-hex-char prefix. The cache lets the
        signal-routing path resolve a wallet UUID7 from a parsed
        ``shard_key`` segment without an extra DB roundtrip per
        message. Boot-time only — rotation requires a coordinator
        restart, matching the executor credential pull contract.
        """
        if not isinstance(self.repository, SQLAlchemyRepository):
            return
        try:
            credentials = await self.repository.list_active_wallet_credentials(
                as_of=datetime.now(UTC)
            )
        except Exception as exc:
            logger.warning(f"ZMQTrader: failed to load wallet_credentials cache: {exc}")
            return
        cache: dict[str, str] = {}
        for row in credentials:
            wallet_public_id = row["wallet_public_id"]
            new_short = compute_wallet_short(wallet_public_id)
            self._install_canonical_short(cache, new_short, wallet_public_id)
        for row in credentials:
            wallet_public_id = row["wallet_public_id"]
            new_short = compute_wallet_short(wallet_public_id)
            legacy_short = compute_legacy_wallet_short(wallet_public_id)
            if legacy_short == new_short:
                continue
            self._install_legacy_alias(cache, legacy_short, wallet_public_id)
        self._wallet_short_to_id = cache
        logger.info(f"ZMQTrader: wallet_short cache populated with {len(cache)} entries")

    @staticmethod
    def _install_canonical_short(
        cache: dict[str, str],
        wallet_short: str,
        wallet_public_id: str,
    ) -> None:
        """Insert a canonical (last-12) ``wallet_short → wallet_public_id`` entry.

        Two different wallets sharing the canonical short means a real
        collision in the random portion of their UUID7s — birthday-bound
        to ~1 in 2^48 per same-exchange wallet pair. Log error and skip
        the second insertion so signal routing for the first wallet
        keeps working; the operator regenerates the second wallet.
        """
        existing = cache.get(wallet_short)
        if existing and existing != wallet_public_id:
            logger.error(
                f"ZMQTrader: canonical wallet_short collision on '{wallet_short}' "
                f"between {existing} and {wallet_public_id}; signal routing "
                f"to the second wallet will fail"
            )
            return
        cache[wallet_short] = wallet_public_id

    @staticmethod
    def _install_legacy_alias(
        cache: dict[str, str],
        legacy_short: str,
        wallet_public_id: str,
    ) -> None:
        """Install a legacy (first-12) alias only when the slot is empty.

        The legacy alias keeps recovery from old persisted ``shard_key``
        strings working after the algorithm change from first-12 to
        last-12. The canonical pass MUST run first so canonical keys
        own their slots; this pass refuses to overwrite a canonical
        owner — that would silently re-route live signals away from
        the legitimate wallet to one whose timestamp prefix happened
        to match. When the legacy alias would shadow another wallet's
        canonical key, log a warning so the operator notices that
        old persisted shard_keys for ``wallet_public_id`` will now
        recover to the canonical owner instead.
        """
        existing = cache.get(legacy_short)
        if existing is None:
            cache[legacy_short] = wallet_public_id
            return
        if existing == wallet_public_id:
            return
        logger.warning(
            f"ZMQTrader: legacy wallet_short alias '{legacy_short}' for "
            f"{wallet_public_id} collides with canonical owner {existing}; "
            f"persisted shard_keys with first-12={legacy_short} will resolve "
            f"to {existing} — verify recovery for {wallet_public_id} if it "
            f"has older shard_keys in DB"
        )

    async def _recover_engine_state(self) -> None:
        """Rebuild engine confirmed state from checkpoints, executions, and active orders.

        Step 0: Read checkpoints, restore TradeService/BalanceService,
            replay delta VenueEvents, create engines with restored state.
        Step 1: Full-replay for shards without checkpoints (legacy path).
        Step 2: Query active orders across ALL exchanges, create engines
            for orders that have no executions yet, set order_in_flight.
        Step 3: Detect fill gaps (DB filled_size vs order filled_size)
            and enter degraded read-only mode if cost basis is unrecoverable.
        Step 4: Reconcile position_cycles against recovered engine state
            so non-flat shards always have an active cycle row and the
            ShardState cycle cache is populated before live fills arrive.
        """
        now = datetime.now(UTC)
        recovered_shards = await self._recover_from_checkpoints(now)
        executions = await self._recover_from_executions(now, recovered_shards)
        await self._recover_active_orders(now, executions)
        await self._reconcile_position_cycles()
        logger.info(
            f"ZMQTrader: Engine recovery complete: "
            f"{len(self.engines)} engines, "
            f"{sum(1 for e in self.engines.values() if e.order_in_flight)} in-flight, "
            f"{sum(1 for e in self.engines.values() if e.read_only)} degraded"
        )

    async def _recover_paired_execution_leg_fills(self) -> None:
        """Re-project paired-execution leg fills from authoritative venue_events on startup.

        The live fill projection runs only on the live fill path, so a grouped
        leg whose order filled while the coordinator was DOWN would recover with
        ``filled_signed_qty == 0`` and stay invisible to the guard scanner's halt
        projection and the startup halt-mirror recovery. This pass re-reads the
        AUTHORITATIVE durable ``venue_events``
        (the executor writes them fail-closed with the cumulative ``cum_fill_size``
        BEFORE publishing) and re-projects each owned active leg's MAX cumulative
        ORIGINAL fill via the monotonic projection DAL — a leg already projected
        live is a no-op; a downtime fill is restored. When the guard is enabled it
        ALSO re-derives each leg's ``compensated_signed_qty`` from its reduce-only
        flatten orders (mirroring the live compensation projection), so a flatten
        that filled during downtime is restored too. Covers ``armed`` / ``broken`` /
        ``compensating`` / ``manual_intervention`` groups — a leg can be in any of
        these while a sibling's flatten is mid-flight — plus groups COMPLETED
        within :data:`_COMPLETED_GROUP_REPLAY_WINDOW`: a late
        original fill that landed during the downtime re-projects through the
        fill DAL, whose completed-group path reopens the group to
        ``compensating`` so the scanner re-halts and re-flattens the residual.
        Runs AFTER ``_recover_engine_state``
        and BEFORE the trading loop, so the post-recovery guard scanner sees the
        restored exposure. Gated on a SQL repository + shard ownership; per-leg
        fail-soft so one bad leg (e.g. the data-integrity duplicate-client_order_id
        guard) never blocks startup.
        """
        if not isinstance(self.repository, SQLAlchemyRepository):
            return
        if self._ownership is None:
            return
        now = datetime.now(UTC)
        try:
            groups = await self.repository.list_current_paired_execution_groups(
                [
                    PairedExecutionGroupStatusEnum.ARMED.value,
                    PairedExecutionGroupStatusEnum.BROKEN.value,
                    PairedExecutionGroupStatusEnum.COMPENSATING.value,
                    PairedExecutionGroupStatusEnum.MANUAL_INTERVENTION.value,
                ]
            )
            groups = list(
                groups
            ) + await self.repository.list_recent_completed_paired_execution_groups(
                now - _COMPLETED_GROUP_REPLAY_WINDOW
            )
        except Exception as exc:
            logger.error(
                f"ZMQTrader: paired-execution leg fill recovery group list failed, skipping: {exc}"
            )
            return
        for group in groups:
            try:
                await self._reproject_group_leg_fills(self.repository, self._ownership, group, now)
            except Exception as exc:
                logger.error(
                    f"ZMQTrader: paired-execution leg fill recovery for group "
                    f"{group['public_id']} failed, continuing: {exc}"
                )

    async def _reproject_group_leg_fills(
        self,
        repository: SQLAlchemyRepository,
        ownership: ShardOwnership,
        group: PairedExecutionGroupRow,
        now: datetime,
    ) -> None:
        """Re-project the max cumulative fill of each owned dispatched leg of one group.

        Reads the group's CURRENT active legs (clock-skew-safe, pairing with the
        current-active group list) and re-projects each owned leg bound to a
        client order. A per-leg failure (e.g. the data-integrity
        duplicate-client_order_id guard) is logged at HIGH severity and skipped so
        one bad leg never blocks the rest; the leg-read failure for THIS group is
        caught by the caller so one bad group never blocks the others.
        """
        legs = await repository.get_current_paired_execution_legs(group["public_id"])
        for leg in legs:
            if not ownership.owns(leg["shard_key"]):
                continue
            client_order_id = leg["client_order_id"]
            if client_order_id is None:
                continue
            try:
                await self._reproject_leg_fill(repository, leg, client_order_id, now)
            except Exception as exc:
                logger.error(
                    f"ZMQTrader: paired-execution leg fill recovery failed for leg "
                    f"{leg['public_id']} (client_order_id {client_order_id}), continuing: {exc}"
                )

    async def _reproject_leg_fill(
        self,
        repository: SQLAlchemyRepository,
        leg: PairedExecutionLegRow,
        client_order_id: str,
        now: datetime,
    ) -> None:
        """Project one owned leg's max cumulative venue fill, if any, onto the leg.

        Re-derives the ORIGINAL fill (``filled_signed_qty``) from the leg's
        ``client_order_id`` and then, when the guard is enabled, re-derives the
        COMPENSATION fill (``compensated_signed_qty``) from the leg's reduce-only
        flatten orders (recovery parity with the live compensation projection)
        so a flatten that filled while the coordinator was down is restored.
        Both projections read the
        authoritative ``venue_events`` and are idempotent no-ops when the live
        path already applied them.
        """
        event = await repository.get_max_cumulative_fill_venue_event(client_order_id)
        if event is not None:
            cum_fill_size = event["cum_fill_size"] or 0.0
            signed_qty = cum_fill_size if leg["side"] == TradeSideEnum.BUY else -cum_fill_size
            new_status = (
                PairedExecutionLegStatusEnum.FILLED.value
                if event["status"] == FillStatusEnum.FILLED.value
                else PairedExecutionLegStatusEnum.PARTIALLY_FILLED.value
            )
            await repository.project_paired_execution_leg_fill(
                client_order_id,
                signed_qty,
                new_status,
                now,
                self._tracker.session_id,
                self._tracker.next_sequence(f"paired.fill.recover.{client_order_id}"),
                exchange_order_id=event["exchange_order_id"],
                last_venue_event_id=event["id"],
            )
        if _bootstrap_settings.paired_execution_guard_enabled:
            await repository.reproject_paired_execution_leg_compensation(
                leg["public_id"],
                now,
                self._tracker.session_id,
                self._tracker.next_sequence(f"paired.comp.recover.{leg['public_id']}"),
            )

    async def _recover_paired_execution_guard_state(self) -> None:
        """Rebuild paired-execution guard in-memory state from the DB on startup.

        Runs AFTER :meth:`_recover_engine_state` and BEFORE the signal listener
        starts (in :meth:`_run_trading_loop`), so a NEW grouped signal is never
        accepted before the in-memory shard-halt mirror is restored. Both effects
        are read-only on the DB (the guard scanner re-projects any missing durable
        halt within one cycle):

        - Halt mirror (halt-driven): for every CURRENT active durable halt,
          re-halt this coordinator's owned leg shards in ``trade_service``. The
          durable halt is the authoritative record, so a halt closed by
          ``clear_paired_execution_halt`` is excluded — a deliberately-cleared
          pair is never re-halted on restart (a group-driven restore from
          still-``broken`` groups would wrongly re-wedge it).
        - Outbox wake: if any active ``armed`` group has an owned leg, wake the
          outbox once so its held commands re-dispatch immediately rather than
          waiting for the first poll.

        Gated on a SQL repository + shard ownership (skipped in tests). The halt
        mirror and the outbox wake are INDEPENDENT best-effort steps (each wrapped
        in its own fail-closed handler, mirroring ``_recover_engine_state``), so a
        DB error restoring the safety-critical halt mirror never suppresses the
        outbox wake, and neither ever blocks startup.
        """
        if not isinstance(self.repository, SQLAlchemyRepository):
            return
        if self._ownership is None:
            return
        now = datetime.now(UTC)
        try:
            await self._restore_paired_execution_halt_mirror(self.repository, self._ownership)
        except Exception as exc:
            logger.warning(
                f"ZMQTrader: paired-execution halt mirror recovery failed, continuing: {exc}"
            )
        try:
            await self._wake_outbox_for_owned_armed_groups(self.repository, self._ownership, now)
        except Exception as exc:
            logger.warning(
                f"ZMQTrader: paired-execution outbox wake recovery failed, continuing: {exc}"
            )

    async def _restore_paired_execution_halt_mirror(
        self,
        repository: SQLAlchemyRepository,
        ownership: ShardOwnership,
    ) -> None:
        """Re-halt this coordinator's owned leg shards from every active halt.

        Reads CURRENT active legs (``get_current_paired_execution_legs``), not a
        temporal ``as_of`` view, so a leg stamped with a slightly future
        ``timestamp`` under clock skew is still mirrored — a temporal read would
        skip it and leave the owned shard un-halted until the scanner's next
        cycle, reopening the very startup window this recovery closes.

        Each halt is mirrored INDEPENDENTLY: a per-halt leg-read error logs and
        skips only that halt, so one transient failure never drops the mirror for
        the OTHER halted pairs. The scanner re-projects any skipped halt within a
        cycle.

        The mirror reason is the canonical per-scope :func:`paired_halt_reason`
        key (NOT the durable row's human-readable reason): the scanner's halt
        mirror and the quiet-halt sweep's reason-scoped release use the same
        key, so a halt restored here is released the moment its scope quiets —
        a mismatched string would strand the mirror until the next restart.
        """
        halts = await repository.list_active_paired_execution_halts()
        for halt in halts:
            try:
                legs = await repository.get_current_paired_execution_legs(halt["group_public_id"])
            except Exception as exc:
                logger.warning(
                    f"ZMQTrader: paired-execution halt {halt['public_id']} leg restore failed, "
                    f"skipping: {exc}"
                )
                continue
            mirror_reason = paired_halt_reason(
                halt["wallet_public_id"], halt["strategy_id"], halt["group_key"]
            )
            for leg in legs:
                if ownership.owns(leg["shard_key"]):
                    self.trade_service.halt_shard(leg["shard_key"], mirror_reason)

    async def _wake_outbox_for_owned_armed_groups(
        self,
        repository: SQLAlchemyRepository,
        ownership: ShardOwnership,
        now: datetime,
    ) -> None:
        """Wake the outbox once if this coordinator owns a leg of an armed group."""
        if self.outbox is None:
            return
        armed = await repository.list_active_paired_execution_groups(
            [PairedExecutionGroupStatusEnum.ARMED.value], now
        )
        for group in armed:
            legs = await repository.get_paired_execution_legs(group["public_id"], now)
            if any(ownership.owns(leg["shard_key"]) for leg in legs):
                self.outbox.notify()
                return

    async def _recover_from_checkpoints(self, now: datetime) -> set[str]:
        """Restore shards from persisted checkpoints + delta replay.

        Returns:
            Set of engine_keys that were fully recovered from checkpoints.
        """
        if not isinstance(self.repository, SQLAlchemyRepository):
            return set()
        try:
            checkpoints = await self.repository.get_all_checkpoints(as_of=now)
        except Exception as e:
            logger.error(f"ZMQTrader: Failed to query checkpoints for recovery: {e}")
            return set()
        if not checkpoints:
            logger.info("ZMQTrader: No checkpoints found, using full replay")
            return set()

        recovered: set[str] = set()
        for checkpoint in checkpoints:
            engine_key = await self._recover_checkpoint_row(checkpoint, now)
            if engine_key is not None:
                recovered.add(engine_key)
        return recovered

    async def _recover_checkpoint_row(
        self,
        checkpoint: TradeProjectionCheckpointRow,
        now: datetime,
    ) -> str | None:
        """Recover one checkpoint-backed shard, or return ``None`` to full-replay it."""
        shard_key = checkpoint["shard_key"]
        if self._ownership is not None and not self._ownership.owns(shard_key):
            logger.debug(
                "ZMQTrader: skipping checkpoint for foreign shard {} (owner {}/{})",
                shard_key,
                self._ownership.instance_id,
                self._ownership.instance_count,
            )
            return None
        parsed_shard = self._parse_shard_key(shard_key)
        if parsed_shard is None:
            logger.warning(f"ZMQTrader: Invalid shard_key format: {shard_key}, skipping")
            return None
        exchange_str, instrument, mode_str, wallet_short, strategy_tag = parsed_shard
        if exchange_str not in get_args(OrderExchange):
            logger.warning(f"ZMQTrader: Checkpoint exchange {exchange_str} not valid, skipping")
            return None
        wallet_public_id = await self._resolve_checkpoint_wallet_public_id(
            shard_key,
            wallet_short,
            checkpoint["checkpoint_at"] or now,
        )
        delta_events = await self._load_checkpoint_delta_events(checkpoint, shard_key)
        if delta_events is None:
            return None
        self._restore_trade_service_from_checkpoint(checkpoint, shard_key, delta_events)
        await self._register_checkpoint_open_orders(checkpoint, shard_key, now)
        await self._replay_checkpoint_accruals(
            checkpoint=checkpoint,
            now=now,
            instrument=instrument,
            exchange_str=exchange_str,
            mode_str=mode_str,
            wallet_public_id=wallet_public_id,
            shard_key=shard_key,
        )
        self._restore_balance_service_from_shard(shard_key)
        engine = await self._create_engine_for_recovery(
            instrument,
            exchange_str,
            strategy_tag=strategy_tag,
            wallet_public_id=wallet_public_id,
            operator_public_id=checkpoint.get("operator_public_id") or "",
        )
        if engine is None:
            return None
        self._restore_engine_from_shard(engine, shard_key, instrument)
        engine_key = self._build_engine_key(
            instrument,
            exchange_str,
            strategy_tag if strategy_tag else mode_str,
            wallet_public_id,
        )
        self._register_recovered_engine(engine_key, engine)
        logger.info(
            f"ZMQTrader: Recovered {engine_key} from checkpoint: "
            f"pos={engine.position_qty:.6f}, "
            f"entry={engine.entry_price}, "
            f"cash={engine.portfolio.cash:.2f}"
        )
        return engine_key

    async def _register_checkpoint_open_orders(
        self,
        checkpoint: TradeProjectionCheckpointRow,
        shard_key: str,
        now: datetime,
    ) -> None:
        """Register ``cid -> shard_key`` for each open command in a recovered checkpoint.

        Closes the gap where a still-open command is durable in the checkpoint /
        ``trade_commands`` table but its active ``Order`` row is unrecoverable, so
        the active-order recovery pass never registers it and the N>1 CID filter
        would hard-drop the order's fills. Each command is registered ONLY when
        its own persisted ``shard_key`` equals this checkpoint's shard, so a
        stale or foreign command can never claim a foreign shard.

        Args:
            checkpoint: The owned checkpoint row being recovered.
            shard_key: This checkpoint's shard key (the owning engine's key).
            now: Bitemporal ``as_of`` anchor for the command lookups.
        """
        raw_command_ids: list[object] = json.loads(checkpoint["open_command_ids"] or "[]")
        for command_public_id in raw_command_ids:
            if not isinstance(command_public_id, str):
                continue
            command = await self.repository.get_trade_command_by_public_id(
                command_public_id, as_of=now
            )
            if command is None:
                continue
            client_order_id = command["client_order_id"]
            if client_order_id and command["shard_key"] == shard_key:
                self._register_order_shard_key(client_order_id, shard_key)

    async def _resolve_checkpoint_wallet_public_id(
        self,
        shard_key: str,
        wallet_short: str,
        as_of: datetime,
    ) -> str:
        """Resolve checkpoint wallet attribution from temporal DB state."""
        if not wallet_short:
            return ""
        temporal_wallet_public_id = await self._lookup_checkpoint_wallet_public_id(
            wallet_short,
            as_of,
        )
        if temporal_wallet_public_id:
            return temporal_wallet_public_id
        wallet_public_id = self._wallet_short_to_id.get(wallet_short, "")
        if wallet_public_id:
            return wallet_public_id
        logger.warning(
            f"ZMQTrader: Checkpoint shard_key {shard_key} carries unknown "
            f"wallet_short '{wallet_short}' (wallet credential rotated, "
            f"deactivated, or wallet_credentials cache stale). Recovering "
            f"with empty wallet attribution; consider clearing this stale "
            f"checkpoint via the recovery tooling once the wallet status "
            f"is confirmed."
        )
        return ""

    async def _lookup_checkpoint_wallet_public_id(
        self,
        wallet_short: str,
        as_of: datetime,
    ) -> str:
        """Resolve a checkpoint wallet short through the repository at ``as_of``."""
        if not isinstance(self.repository, SQLAlchemyRepository):
            return ""
        try:
            return (
                await self.repository.resolve_wallet_public_id_by_short(wallet_short, as_of) or ""
            )
        except Exception as exc:
            logger.warning(
                f"ZMQTrader: failed temporal wallet_short lookup for "
                f"'{wallet_short}' at {as_of.isoformat()}: {exc}"
            )
            return ""

    async def _load_checkpoint_delta_events(
        self,
        checkpoint: TradeProjectionCheckpointRow,
        shard_key: str,
    ) -> list[VenueEventRow] | None:
        """Load post-checkpoint venue events, or ``None`` if full replay should take over."""
        watermark = checkpoint["last_venue_event_id"]
        if watermark is None:
            logger.info(
                f"ZMQTrader: Checkpoint for {shard_key} has no watermark, "
                f"falling back to full replay"
            )
            return None
        if not isinstance(self.repository, SQLAlchemyRepository):
            return None
        try:
            return await self.repository.get_venue_events_after(
                shard_key=shard_key,
                after_id=watermark,
            )
        except Exception as e:
            logger.error(
                f"ZMQTrader: Failed delta replay for {shard_key}: {e}, "
                f"will fall back to full replay"
            )
            return None

    def _restore_trade_service_from_checkpoint(
        self,
        checkpoint: TradeProjectionCheckpointRow,
        shard_key: str,
        delta_events: list[VenueEventRow],
    ) -> None:
        """Restore TradeService state from one checkpoint plus delta venue events."""
        seen_exec_ids_raw: list[object] = json.loads(checkpoint["seen_exec_ids"] or "[]")
        open_command_ids_raw: list[object] = json.loads(checkpoint["open_command_ids"] or "[]")
        seen_ids: OrderedDict[str, None] = OrderedDict.fromkeys(
            exec_id for exec_id in seen_exec_ids_raw if isinstance(exec_id, str)
        )
        open_command_ids = [
            command_public_id
            for command_public_id in open_command_ids_raw
            if isinstance(command_public_id, str)
        ]
        self.trade_service.restore_from_checkpoint(
            shard_key=shard_key,
            position_qty=checkpoint["position_qty"],
            entry_price=checkpoint["entry_price"],
            cash=checkpoint["cash"],
            peak_equity=checkpoint["peak_equity"],
            realized_pnl=checkpoint["realized_pnl"],
            turnover=checkpoint["turnover"],
            last_venue_event_id=checkpoint["last_venue_event_id"] or 0,
            open_command_ids=open_command_ids,
            seen_exec_ids=seen_ids,
            position_opened_at=checkpoint.get("position_opened_at"),
        )
        for event in delta_events:
            self.trade_service.apply_venue_event(event)
        if delta_events:
            logger.info(f"ZMQTrader: Replayed {len(delta_events)} delta events for {shard_key}")

    async def _replay_checkpoint_accruals(
        self,
        *,
        checkpoint: TradeProjectionCheckpointRow,
        now: datetime,
        instrument: str,
        exchange_str: str,
        mode_str: str,
        wallet_public_id: str,
        shard_key: str,
    ) -> None:
        """Replay persisted accruals between ``checkpoint_at`` and ``now``."""
        checkpoint_at = checkpoint.get("checkpoint_at")
        if checkpoint_at is None:
            return
        try:
            instrument_public_id = await self.repository.get_instrument_public_id_by_symbol(
                native_symbol=instrument,
                exchange=exchange_str,
                as_of=now,
            )
            if instrument_public_id is None:
                return
            pending_accruals = await self.repository.get_accruals(
                instrument_public_id=instrument_public_id,
                mode=mode_str,
                range_start=checkpoint_at,
                range_end=now,
                wallet_public_id=wallet_public_id,
            )
            if not pending_accruals:
                return
            self.trade_service.replay_funding_accruals(shard_key, pending_accruals)
            logger.info(
                "ZMQTrader: Replayed {} accruals for {}",
                len(pending_accruals),
                shard_key,
            )
        except Exception:
            logger.opt(exception=True).warning("ZMQTrader: Accrual replay failed for {}", shard_key)

    def _restore_balance_service_from_shard(self, shard_key: str) -> None:
        """Mirror the recovered TradeService shard into BalanceService."""
        shard = self.trade_service._shards[shard_key]
        self.balance_service.restore_from_checkpoint(
            shard_key=shard_key,
            cash=shard.cash,
            position_qty=shard.position.position_qty,
            entry_price=shard.position.entry_price,
            peak_equity=shard.peak_equity,
            realized_pnl=shard.position.realized_pnl,
        )

    def _register_recovered_engine(
        self,
        engine_key: str,
        engine: TradingEngineService,
    ) -> None:
        """Register a recovered engine and refresh its liveness timestamp."""
        self.engines[engine_key] = engine
        self.last_signal_time[engine_key] = time.time()

    def _register_engine_for_lookup(self, engine: TradingEngineService) -> None:
        """Index an engine for O(1) fill dispatch.

        Adds the engine to:
        * ``_engines_by_scope`` keyed by
          ``(exchange, instrument, wallet_public_id)`` when the engine
          carries a wallet_public_id;
        * ``_engines_by_scope_legacy`` keyed by ``(exchange, instrument)``
          unconditionally so wallet-less fills still resolve.

        Registers ``_on_engine_pending_coid_change`` as the engine's
        ``pending_coid_listener`` so subsequent property assignments
        on ``engine.pending_client_order_id`` keep
        ``_engines_by_pending_coid`` consistent without scanning all
        engines on every fill.

        Defensive against partial-stub engines used in unit tests
        (e.g. StubEngine without ``exchange``/``wallet_public_id``): a
        missing attribute simply skips the corresponding scope index
        entry; the engine still wires into ``self.engines`` and the
        coid listener if those attributes exist.
        """
        exchange = getattr(engine, "exchange", None)
        instrument = getattr(engine, "instrument", None)
        wallet_public_id = getattr(engine, "wallet_public_id", "")
        if exchange is not None and instrument is not None:
            scope_legacy_key = (exchange, instrument)
            self._engines_by_scope_legacy.setdefault(scope_legacy_key, engine)
            if wallet_public_id:
                scope_key = (exchange, instrument, wallet_public_id)
                self._engines_by_scope.setdefault(scope_key, engine)
        engine.pending_coid_listener = lambda old, new: self._on_engine_pending_coid_change(
            engine, old, new
        )
        current_pending = getattr(engine, "pending_client_order_id", None)
        if current_pending is not None:
            self._engines_by_pending_coid.setdefault(current_pending, engine)

    def _on_engine_pending_coid_change(
        self,
        engine: TradingEngineService,
        old: str | None,
        new: str | None,
    ) -> None:
        """Listener invoked by ``TradingEngineService.pending_client_order_id`` setter."""
        if old is not None and self._engines_by_pending_coid.get(old) is engine:
            del self._engines_by_pending_coid[old]
        if new is not None:
            self._engines_by_pending_coid[new] = engine

    def _restore_engine_from_shard(
        self,
        engine: TradingEngineService,
        shard_key: str,
        instrument: str,
    ) -> None:
        """Restore TradingEngineService from the post-delta TradeService shard.

        Reads the current in-memory shard state (which already reflects
        checkpoint + delta replay) and copies it into the engine. This
        ensures engine, portfolio, and TradeService are all in sync.

        Args:
            engine: Freshly created engine to restore.
            shard_key: Shard key to read from TradeService.
            instrument: Instrument symbol for portfolio position key.
        """
        shard = self.trade_service._shards[shard_key]
        engine.position_qty = shard.position.position_qty
        engine.entry_price = shard.position.entry_price
        engine.peak_equity = shard.peak_equity
        engine.seen_exec_ids = OrderedDict.fromkeys(shard.seen_exec_ids)
        engine.portfolio.cash = shard.cash
        engine.portfolio.turnover = shard.turnover
        if shard.position.position_qty != 0 and shard.position.entry_price is not None:
            engine.portfolio.positions[instrument] = PositionStateModel(
                quantity=shard.position.position_qty,
                average_price=shard.position.entry_price,
                realized_pnl=shard.position.realized_pnl,
            )

    async def _recover_from_executions(
        self, now: datetime, checkpoint_recovered: set[str] | None = None
    ) -> list[ExecutionRow]:
        """Shadow-replay DB executions through TradeService to rebuild state.

        Mirrors the checkpoint path's delta-replay contract: each execution
        row is wrapped in a synthetic VenueEventRow and dispatched via
        ``self.trade_service.apply_venue_event``. After the loop the engine
        is slaved to the rebuilt TradeService shard via
        ``self._restore_engine_from_shard`` so engine, portfolio, and
        TradeService stay in sync — previously this path bypassed
        TradeService and left its projection flat, creating a latent
        ``old_qty == 0`` hazard in the first live fill on a non-flat shard.
        Skips engine_keys already recovered from checkpoints.

        Args:
            now: Current timestamp for DB queries.
            checkpoint_recovered: Engine keys already restored from checkpoints.

        Returns:
            All recovered execution rows.
        """
        skip_keys = checkpoint_recovered or set()
        try:
            executions = await self.repository.get_executions_for_recovery(as_of=now)
        except Exception as e:
            logger.error(f"ZMQTrader: Failed to query executions for recovery: {e}")
            return []
        if not executions:
            logger.info("ZMQTrader: No executions to recover")
            return []
        fills_by_key, wallet_for_key, operator_for_key = self._group_execution_recovery_rows(
            executions
        )
        for engine_key, fills in fills_by_key.items():
            if engine_key in skip_keys:
                logger.debug(f"ZMQTrader: Skipping full replay for {engine_key} (checkpoint)")
                continue
            await self._recover_execution_group(
                engine_key=engine_key,
                fills=fills,
                wallet_public_id=wallet_for_key.get(engine_key, ""),
                operator_public_id=operator_for_key.get(engine_key, ""),
            )
        return executions

    def _group_execution_recovery_rows(
        self,
        executions: list[ExecutionRow],
    ) -> tuple[dict[str, list[ExecutionRow]], dict[str, str], dict[str, str]]:
        """Group execution rows into engine recovery buckets."""
        fills_by_key: dict[str, list[ExecutionRow]] = {}
        wallet_for_key: dict[str, str] = {}
        operator_for_key: dict[str, str] = {}
        for execution in executions:
            execution_group = self._classify_execution_recovery_row(execution)
            if execution_group is None:
                continue
            engine_key, wallet_public_id, operator_public_id = execution_group
            fills_by_key.setdefault(engine_key, []).append(execution)
            wallet_for_key.setdefault(engine_key, wallet_public_id)
            if operator_public_id and engine_key not in operator_for_key:
                operator_for_key[engine_key] = operator_public_id
        return fills_by_key, wallet_for_key, operator_for_key

    def _classify_execution_recovery_row(
        self,
        execution: ExecutionRow,
    ) -> tuple[str, str, str] | None:
        """Return ``(engine_key, wallet_public_id, operator_public_id)`` for recovery."""
        partitioned = self._ownership is not None and self._ownership.instance_count > 1
        if partitioned and execution["exchange"] == ExchangeEnum.PAPER:
            logger.debug(
                "ZMQTrader: skipping paper execution recovery under N>1 "
                "(strategy_tag unavailable): instrument={}",
                execution["instrument"],
            )
            return None
        wallet_public_id = execution.get("wallet_public_id") or ""
        recovery_shard_key = compute_shard_key(
            instrument=execution["instrument"],
            exchange=cast(OrderExchange, execution["exchange"]),
            mode=ExecutionModeEnum.LIVE,
            wallet_public_id=wallet_public_id,
            strategy_tag=None,
        )
        if self._ownership is not None and not self._ownership.owns(recovery_shard_key):
            logger.debug(
                "ZMQTrader: skipping execution for foreign shard {} (owner {}/{})",
                recovery_shard_key,
                self._ownership.instance_id,
                self._ownership.instance_count,
            )
            return None
        engine_key = self._build_engine_key(
            execution["instrument"],
            execution["exchange"],
            "live",
            wallet_public_id,
        )
        operator_public_id = execution.get("operator_public_id") or ""
        return engine_key, wallet_public_id, operator_public_id

    async def _recover_execution_group(
        self,
        *,
        engine_key: str,
        fills: list[ExecutionRow],
        wallet_public_id: str,
        operator_public_id: str,
    ) -> None:
        """Replay one grouped execution bucket into TradeService and engine state."""
        engine = await self._create_engine_for_recovery(
            fills[0]["instrument"],
            fills[0]["exchange"],
            wallet_public_id=wallet_public_id,
            operator_public_id=operator_public_id,
        )
        if engine is None:
            return
        shard_key = engine._shard_key
        shard = self.trade_service._get_or_create_shard(shard_key)
        start_id = shard.last_venue_event_id + 1
        for offset, fill_row in enumerate(fills):
            event = self._build_replay_venue_event(
                engine,
                fill_row,
                synthetic_id=start_id + offset,
            )
            self.trade_service.apply_venue_event(event)
        self._restore_engine_from_shard(engine, shard_key, engine.instrument)
        self._register_recovered_engine(engine_key, engine)
        logger.info(
            f"ZMQTrader: Recovered {engine_key} via full-replay shadow: "
            f"pos={engine.position_qty:.6f}, "
            f"entry={engine.entry_price}, "
            f"fills={len(fills)}"
        )

    def _build_replay_venue_event(
        self,
        engine: TradingEngineService,
        fill_row: ExecutionRow,
        *,
        synthetic_id: int,
    ) -> VenueEventRow:
        """Build a synthetic VenueEventRow for a single execution row.

        Used by ``_recover_from_executions`` to dispatch historical
        executions through ``TradeService.apply_venue_event``. The
        ``synthetic_id`` must be strictly monotonic within the replay
        loop and bootstrapped from ``shard.last_venue_event_id + 1`` so
        the stored watermark never collides with real ``venue_events.id``
        writes that land after recovery.

        Dedup fidelity is preserved via ``trade_id`` (``ExecutionRow``
        has no ``exec_id`` field, so the synthetic event passes
        ``exec_id=None`` and ``_dedup_fill`` keys on ``trade_id`` alone —
        matches the existing checkpoint-replay dedup behaviour).

        Args:
            engine: Engine whose shard_key owns this event.
            fill_row: Execution row loaded from the DB.
            synthetic_id: Per-shard monotonic counter assigned by the
                caller.

        Returns:
            A VenueEventRow suitable for ``apply_venue_event``.
        """
        return {
            "id": synthetic_id,
            "public_id": "",
            "timestamp": fill_row["timestamp"],
            "session_id": self._tracker.session_id,
            "sequence_id": self._tracker.next_sequence("full_replay"),
            "event_type": "fill_observed",
            "shard_key": engine._shard_key,
            "command_public_id": None,
            "exchange": fill_row["exchange"],
            "instrument": fill_row["instrument"],
            "mode": "live",
            "exchange_order_id": fill_row["exchange_order_id"],
            "client_order_id": fill_row["client_order_id"],
            "venue_client_id": None,
            "side": fill_row["side"],
            "status": fill_row["status"],
            "fill_price": fill_row["price"],
            "fill_size": fill_row["size"],
            "cum_fill_size": None,
            "fee": fill_row["fee"],
            "fee_asset": fill_row["fee_asset"],
            "exec_id": None,
            "trade_id": fill_row["trade_id"],
            "error": None,
            "venue_timestamp": fill_row["executed_at"],
            "received_at": fill_row["timestamp"],
        }

    async def _recover_active_orders(self, now: datetime, executions: list[ExecutionRow]) -> None:
        """Process active orders across all exchanges."""
        active_orders = await self._load_active_orders_for_recovery(now)
        execution_fill_sizes = self._build_execution_fill_sizes(executions)
        for db_order in active_orders:
            await self._recover_active_order_row(db_order, execution_fill_sizes)

    async def _load_active_orders_for_recovery(self, now: datetime) -> list[OrderRow]:
        """Load active orders across all supported exchanges.

        Issues a single ``get_active_orders_for_recovery`` call with
        ``exchange=None``; the repository then returns rows for every
        exchange in one round-trip. An earlier implementation
        looped over each :class:`OrderExchange` value and paid one
        request per exchange.
        """
        try:
            return await self.repository.get_active_orders_for_recovery(
                exchange=None,
                as_of=now,
            )
        except Exception as e:
            logger.error(f"ZMQTrader: Failed to query active orders: {e}")
            return []

    def _build_execution_fill_sizes(self, executions: list[ExecutionRow]) -> dict[str, float]:
        """Sum replayed execution sizes by ``client_order_id``."""
        fill_sizes: dict[str, float] = {}
        for execution in executions:
            client_order_id = execution["client_order_id"]
            fill_sizes[client_order_id] = fill_sizes.get(client_order_id, 0.0) + execution["size"]
        return fill_sizes

    async def _recover_active_order_row(
        self,
        db_order: OrderRow,
        execution_fill_sizes: dict[str, float],
    ) -> None:
        """Recover one active order row into engine state."""
        active_order_group = self._classify_active_order_recovery_row(db_order)
        if active_order_group is None:
            return
        engine_key, wallet_public_id, operator_public_id = active_order_group
        engine = await self._get_or_create_active_order_engine(
            engine_key=engine_key,
            db_order=db_order,
            wallet_public_id=wallet_public_id,
            operator_public_id=operator_public_id,
        )
        if engine is None:
            return
        self._sync_active_order_operator(engine_key, engine, db_order, operator_public_id)
        client_order_id = db_order["client_order_id"]
        self._mark_order_in_flight(engine, client_order_id)
        db_filled = float(db_order.get("filled_size", 0.0))
        exec_filled = execution_fill_sizes.get(client_order_id, 0.0)
        if db_filled > 0 and abs(db_filled - exec_filled) > 1e-9:
            engine.read_only = True
            logger.warning(
                f"ZMQTrader: DEGRADED MODE for {engine_key} - fill gap detected: "
                f"order filled_size={db_filled}, replayed executions={exec_filled}. "
                f"Cost basis cannot be reconstructed. Manual resolution required."
            )
            return
        logger.info(
            f"ZMQTrader: Recovered in-flight order "
            f"{client_order_id} for {engine_key} "
            f"with fresh timeout window"
        )

    def _classify_active_order_recovery_row(
        self,
        db_order: OrderRow,
    ) -> tuple[str, str, str] | None:
        """Return ``(engine_key, wallet_public_id, operator_public_id)`` for recovery."""
        partitioned = self._ownership is not None and self._ownership.instance_count > 1
        instrument = db_order["instrument"]
        exchange_str = db_order["exchange"]
        if partitioned and exchange_str == ExchangeEnum.PAPER:
            logger.debug(
                "ZMQTrader: skipping paper active-order recovery under N>1 "
                "(strategy_tag unavailable): instrument={}, order_public_id={}",
                instrument,
                db_order.get("order_public_id") or "<unknown>",
            )
            return None
        wallet_public_id = db_order.get("wallet_public_id") or ""
        recovery_shard_key = compute_shard_key(
            instrument=instrument,
            exchange=cast(OrderExchange, exchange_str),
            mode=ExecutionModeEnum.LIVE,
            wallet_public_id=wallet_public_id,
            strategy_tag=None,
        )
        if self._ownership is not None and not self._ownership.owns(recovery_shard_key):
            logger.debug(
                "ZMQTrader: skipping active order for foreign shard {} (owner {}/{})",
                recovery_shard_key,
                self._ownership.instance_id,
                self._ownership.instance_count,
            )
            return None
        operator_public_id = db_order.get("operator_public_id") or ""
        engine_key = self._build_engine_key(instrument, exchange_str, "live", wallet_public_id)
        return engine_key, wallet_public_id, operator_public_id

    async def _get_or_create_active_order_engine(
        self,
        *,
        engine_key: str,
        db_order: OrderRow,
        wallet_public_id: str,
        operator_public_id: str,
    ) -> TradingEngineService | None:
        """Reuse or create the engine that owns an active order row."""
        engine = self.engines.get(engine_key)
        if engine is not None:
            return engine
        engine = await self._create_engine_for_recovery(
            db_order["instrument"],
            db_order["exchange"],
            wallet_public_id=wallet_public_id,
            operator_public_id=operator_public_id,
        )
        if engine is None:
            return None
        self._register_recovered_engine(engine_key, engine)
        return engine

    def _sync_active_order_operator(
        self,
        engine_key: str,
        engine: TradingEngineService,
        db_order: OrderRow,
        operator_public_id: str,
    ) -> None:
        """Backfill or validate operator attribution for a recovered active order."""
        if not engine.operator_public_id and operator_public_id:
            engine.operator_public_id = operator_public_id
            return
        if (
            engine.operator_public_id
            and operator_public_id
            and engine.operator_public_id != operator_public_id
        ):
            logger.warning(
                f"ZMQTrader: operator conflict on {engine_key}: engine has "
                f"{engine.operator_public_id}, active order "
                f"{db_order['client_order_id']} has "
                f"{operator_public_id}; keeping existing engine "
                f"operator attribution. Investigate whether the two "
                f"orders belong to the same (wallet, instrument) but "
                f"different operators — this indicates a grant overlap "
                f"or a stale recovery row."
            )

    def _register_order_shard_key(self, client_order_id: str, shard_key: str) -> None:
        """Register ``client_order_id -> full shard_key`` for the N>1 CID filter.

        Single registration point so the N>1 venue-event admission filter can
        route ACK/fills for an order to the owning coordinator. Only the FULL
        persisted shard key is ever stored — never a wallet-less or
        strategy-tag-less reconstruction, which would hash to a different owner.

        Idempotent on an identical mapping. A CONFLICT (same client_order_id,
        different shard_key) is data corruption: since we cannot know which key
        is correct, the mapping is DROPPED entirely and logged loudly, so the
        order's venue events hard-drop as unknown rather than risk routing a fill
        to the wrong wallet/engine. A lost fill is recoverable by reconciliation;
        a mis-applied fill corrupts a position.

        Args:
            client_order_id: The venue client order id (unique per order).
            shard_key: The order's full persisted shard key.
        """
        existing = self._order_shard_keys.get(client_order_id)
        if existing is not None and existing != shard_key:
            logger.error(
                "ZMQTrader: conflicting shard_key for client_order_id {}: "
                "existing={} incoming={}; dropping the mapping so its venue events "
                "hard-drop as unknown rather than risk a mis-routed fill "
                "(investigate shard-key corruption)",
                client_order_id,
                existing,
                shard_key,
            )
            self._order_shard_keys.pop(client_order_id, None)
            return
        self._order_shard_keys[client_order_id] = shard_key

    def _mark_order_in_flight(
        self,
        engine: TradingEngineService,
        client_order_id: str,
    ) -> None:
        """Hydrate in-flight order state on a recovered engine."""
        engine.order_in_flight = True
        engine.pending_client_order_id = client_order_id
        engine._in_flight_since = time.monotonic()
        self._register_order_shard_key(client_order_id, engine._shard_key)

    def _build_position_cycle_insert_row(
        self,
        *,
        engine: TradingEngineService,
        instrument_public_id: str,
        direction: str,
        max_qty: float,
        opened_at: datetime,
        timestamp: datetime,
        session_id: str,
        sequence_id: int,
    ) -> PositionCycleInsertRow:
        """Build a position-cycle insert payload from engine identity and fill state."""
        return {
            "instrument_public_id": instrument_public_id,
            "exchange": str(engine.exchange),
            "mode": str(engine.mode),
            "shard_key": engine._shard_key,
            "wallet_public_id": engine.wallet_public_id,
            "operator_public_id": engine.operator_public_id or None,
            "direction": direction,
            "max_qty": max_qty,
            "status": "open",
            "opened_at": opened_at,
            "opening_command_public_id": None,
            "session_id": session_id,
            "sequence_id": sequence_id,
            "timestamp": timestamp,
        }

    @staticmethod
    def _describe_position_cycle(position_qty: float) -> tuple[str, float]:
        """Return the current direction label and absolute size."""
        return ("long" if position_qty > 0 else "short", abs(position_qty))

    async def _reconcile_position_cycles(self) -> None:
        """Reconcile position_cycles rows against recovered engine state.

        Iterates ``self.engines`` (engines are the recovery source of
        truth — full replay rebuilds ``engine.position_qty`` but not
        ``TradeService``, so keying this off TradeService would miss
        exactly the shards it is meant to repair). For each engine
        with a valid wallet identity, four cases are handled:

        1. **Recovered flat + stale open row**: a cycle was open when
           the coordinator went down and the position returned to flat
           during the downtime. Close the stale row so the next live
           fill does not reuse it. Cache stays empty.
        2. **Recovered non-flat + matching open row**: the common warm
           restart case. Hydrate ``ShardState.active_cycle_public_id``
           / ``active_cycle_max_qty`` from the DB row. Additionally,
           if the recovered ``abs(position_qty)`` exceeds the stored
           ``max_qty`` (position scaled up during downtime beyond the
           last checkpointed peak), bump the peak so the next
           :meth:`_sync_position_cycle_on_fill` scale_up guard is
           monotonic.
        3. **Recovered non-flat + opposite-direction open row**: the
           position flipped during downtime. Use
           :meth:`Repository.flip_position_cycle` to atomically close
           the stale cycle and open a new one matching the recovered
           direction, then hydrate the cache to the new row. If the
           instrument cannot be resolved, degrade to close-only:
           shut the stale cycle, leave the new leg uncovered, and
           clear the cache — the same trade-off applied by the live
           fill path.
        4. **Recovered non-flat + no open row**: bootstrap a synthetic
           cycle so the live fill path has a target on the next
           scale_up / close / flip.

        Engines with degraded wallet attribution (``wallet_public_id``
        falsy, e.g. a checkpoint whose ``wallet_short`` no longer
        resolves on this node) are skipped fail-closed; any orphan
        cycles left behind are closed by an operator via the
        position-cycle orphan admin endpoints. Engines whose
        ``native_symbol`` cannot be resolved to an
        ``instrument_public_id`` at bootstrap time are logged and
        skipped; a later live fill via
        :meth:`_sync_position_cycle_on_fill` will retry the resolution.
        """
        if not self.engines:
            return
        now = datetime.now(UTC)
        eligible_shards = [
            engine._shard_key for engine in self.engines.values() if engine.wallet_public_id
        ]
        open_cycles: dict[str, PositionCycleRow] = (
            await self.repository.get_open_position_cycles_for_shards(eligible_shards, as_of=now)
            if eligible_shards
            else {}
        )
        for engine_key, engine in self.engines.items():
            existing = open_cycles.get(engine._shard_key)
            await self._reconcile_position_cycle_for_engine(engine_key, engine, existing, now)

    async def _reconcile_position_cycle_for_engine(
        self,
        engine_key: str,
        engine: TradingEngineService,
        existing: PositionCycleRow | None,
        now: datetime,
    ) -> None:
        """Reconcile the persisted cycle row for one recovered engine.

        ``existing`` and ``now`` are supplied by the parent
        :meth:`_reconcile_position_cycles` so the entire engine-set
        shares one batched ``get_open_position_cycles_for_shards``
        result and one bus timestamp.
        """
        if not engine.wallet_public_id:
            logger.warning(
                "ZMQTrader: position_cycle reconcile skipped "
                "(degraded identity) engine={} shard={}",
                engine_key,
                engine._shard_key,
            )
            return
        shard_key = engine._shard_key
        position_qty = engine.position_qty
        shard = self.trade_service._get_or_create_shard(shard_key)
        if abs(position_qty) < 1e-12:
            await self._reconcile_flat_position_cycle(
                engine_key=engine_key,
                shard_key=shard_key,
                existing=existing,
                shard=shard,
                now=now,
            )
            return
        current_direction, current_abs = self._describe_position_cycle(position_qty)
        if existing is not None:
            await self._reconcile_existing_position_cycle(
                engine_key=engine_key,
                engine=engine,
                shard_key=shard_key,
                existing=existing,
                shard=shard,
                current_direction=current_direction,
                current_abs=current_abs,
                now=now,
            )
            return
        await self._bootstrap_reconciled_position_cycle(
            engine_key=engine_key,
            engine=engine,
            shard_key=shard_key,
            shard=shard,
            current_direction=current_direction,
            current_abs=current_abs,
            now=now,
        )

    async def _reconcile_flat_position_cycle(
        self,
        *,
        engine_key: str,
        shard_key: str,
        existing: PositionCycleRow | None,
        shard: ShardState,
        now: datetime,
    ) -> None:
        """Close any stale open cycle when recovery finds the shard flat."""
        if existing is None:
            return
        await self.repository.close_position_cycle(
            cycle_public_id=existing["public_id"],
            closed_at=now,
            closing_command_public_id=None,
            bus_time=now,
            session_id=self._tracker.session_id,
            sequence_id=self._tracker.next_sequence(f"reconcile.{shard_key}"),
        )
        logger.info(
            "ZMQTrader: position_cycle reconcile closed stale cycle "
            "(recovered flat) engine={} shard={} cycle={}",
            engine_key,
            shard_key,
            existing["public_id"],
        )
        shard.active_cycle_public_id = None
        shard.active_cycle_max_qty = 0.0

    async def _reconcile_existing_position_cycle(
        self,
        *,
        engine_key: str,
        engine: TradingEngineService,
        shard_key: str,
        existing: PositionCycleRow,
        shard: ShardState,
        current_direction: str,
        current_abs: float,
        now: datetime,
    ) -> None:
        """Handle warm-restart hydration or direction-mismatch recovery."""
        if existing["direction"] == current_direction:
            await self._hydrate_reconciled_position_cycle(
                engine_key=engine_key,
                shard_key=shard_key,
                existing=existing,
                shard=shard,
                current_abs=current_abs,
                now=now,
            )
            return
        await self._reconcile_direction_mismatch_cycle(
            engine_key=engine_key,
            engine=engine,
            shard_key=shard_key,
            existing=existing,
            shard=shard,
            current_direction=current_direction,
            current_abs=current_abs,
            now=now,
        )

    async def _hydrate_reconciled_position_cycle(
        self,
        *,
        engine_key: str,
        shard_key: str,
        existing: PositionCycleRow,
        shard: ShardState,
        current_abs: float,
        now: datetime,
    ) -> None:
        """Hydrate the shard cache from the existing open cycle row."""
        shard.active_cycle_public_id = existing["public_id"]
        shard.active_cycle_max_qty = existing["max_qty"]
        if current_abs <= existing["max_qty"]:
            logger.info(
                "ZMQTrader: position_cycle reconcile hydrated cache engine={} shard={} cycle={}",
                engine_key,
                shard_key,
                existing["public_id"],
            )
            return
        await self.repository.update_position_cycle_max_qty(
            cycle_public_id=existing["public_id"],
            new_max_qty=current_abs,
            bus_time=now,
            session_id=self._tracker.session_id,
            sequence_id=self._tracker.next_sequence(f"reconcile.{shard_key}"),
        )
        shard.active_cycle_max_qty = current_abs
        logger.info(
            "ZMQTrader: position_cycle reconcile bumped max_qty "
            "engine={} shard={} cycle={} old_max={} new_max={}",
            engine_key,
            shard_key,
            existing["public_id"],
            existing["max_qty"],
            current_abs,
        )

    async def _reconcile_direction_mismatch_cycle(
        self,
        *,
        engine_key: str,
        engine: TradingEngineService,
        shard_key: str,
        existing: PositionCycleRow,
        shard: ShardState,
        current_direction: str,
        current_abs: float,
        now: datetime,
    ) -> None:
        """Flip the stale cycle, or degrade to close-only if identity cannot be resolved."""
        instrument_public_id = await self.repository.get_instrument_public_id_by_symbol(
            native_symbol=engine.instrument,
            exchange=str(engine.exchange),
            as_of=now,
        )
        if instrument_public_id is None:
            await self.repository.close_position_cycle(
                cycle_public_id=existing["public_id"],
                closed_at=now,
                closing_command_public_id=None,
                bus_time=now,
                session_id=self._tracker.session_id,
                sequence_id=self._tracker.next_sequence(f"reconcile.{shard_key}"),
            )
            shard.active_cycle_public_id = None
            shard.active_cycle_max_qty = 0.0
            logger.warning(
                "ZMQTrader: position_cycle reconcile degraded flip to close-only "
                "(direction mismatch + unresolved instrument) "
                "engine={} shard={} db_direction={} current_direction={}",
                engine_key,
                shard_key,
                existing["direction"],
                current_direction,
            )
            return
        flip_row = self._build_position_cycle_insert_row(
            engine=engine,
            instrument_public_id=instrument_public_id,
            direction=current_direction,
            max_qty=current_abs,
            opened_at=shard.position.position_opened_at or now,
            timestamp=now,
            session_id=self._tracker.session_id,
            sequence_id=self._tracker.next_sequence(f"reconcile.{shard_key}"),
        )
        _id, new_pid = await self.repository.flip_position_cycle(
            close_cycle_public_id=existing["public_id"],
            new_open_row=flip_row,
            bus_time=now,
            session_id=self._tracker.session_id,
            sequence_id=self._tracker.next_sequence(f"reconcile.{shard_key}"),
        )
        shard.active_cycle_public_id = new_pid
        shard.active_cycle_max_qty = current_abs
        logger.info(
            "ZMQTrader: position_cycle reconcile flipped stale cycle "
            "engine={} shard={} old={} new={} db_direction={} current_direction={}",
            engine_key,
            shard_key,
            existing["public_id"],
            new_pid,
            existing["direction"],
            current_direction,
        )

    async def _bootstrap_reconciled_position_cycle(
        self,
        *,
        engine_key: str,
        engine: TradingEngineService,
        shard_key: str,
        shard: ShardState,
        current_direction: str,
        current_abs: float,
        now: datetime,
    ) -> None:
        """Insert a synthetic open cycle for a recovered non-flat shard."""
        instrument_public_id = await self.repository.get_instrument_public_id_by_symbol(
            native_symbol=engine.instrument,
            exchange=str(engine.exchange),
            as_of=now,
        )
        if instrument_public_id is None:
            logger.warning(
                "ZMQTrader: position_cycle reconcile skipped "
                "(unresolved instrument) engine={} shard={} symbol={}",
                engine_key,
                shard_key,
                engine.instrument,
            )
            return
        bootstrap_row = self._build_position_cycle_insert_row(
            engine=engine,
            instrument_public_id=instrument_public_id,
            direction=current_direction,
            max_qty=current_abs,
            opened_at=shard.position.position_opened_at or now,
            timestamp=now,
            session_id=self._tracker.session_id,
            sequence_id=self._tracker.next_sequence(f"reconcile.{shard_key}"),
        )
        _id, new_pid = await self.repository.insert_position_cycle(bootstrap_row)
        shard.active_cycle_public_id = new_pid
        shard.active_cycle_max_qty = current_abs
        logger.info(
            "ZMQTrader: position_cycle reconcile bootstrapped "
            "engine={} shard={} cycle={} direction={} qty={}",
            engine_key,
            shard_key,
            new_pid,
            current_direction,
            current_abs,
        )

    async def _resolve_instrument_specs(
        self, instrument: str, exchange: str
    ) -> dict[str, InstrumentSpec]:
        """Resolve tick_size, lot_size, and public_id from InstrumentSpec repository.

        Falls back to conservative defaults (without ``public_id``) when the
        lookup fails or the instrument has no spec row yet. The
        ``public_id`` key is populated only when the upstream lookup
        succeeds — the strategy hot-path's AI-attribution gate
        reads it for fail-closed cap evaluation
        on AI-attributed emits, while non-AI emits remain tolerant of
        an absent ``public_id``.
        """
        fallback: dict[str, InstrumentSpec] = {
            instrument: InstrumentSpec(tick_size=0.01, lot_size=0.0001)
        }
        try:
            now = datetime.now(UTC)
            inst_pid = await self.repository.get_instrument_public_id_by_symbol(
                native_symbol=instrument, exchange=exchange, as_of=now
            )
            if inst_pid is None:
                return fallback
            spec = await self.repository.get_instrument_spec(inst_pid, as_of=now)
            if spec is None:
                return fallback
            tick = spec["tick_size"] if spec["tick_size"] is not None else 0.01
            lot = spec["lot_size"] if spec["lot_size"] is not None else 0.0001
            return {instrument: InstrumentSpec(public_id=inst_pid, tick_size=tick, lot_size=lot)}
        except Exception:
            logger.opt(exception=True).debug(
                "InstrumentSpec lookup failed for {}/{}, using defaults", instrument, exchange
            )
            return fallback

    async def _create_engine_for_recovery(
        self,
        instrument: str,
        exchange_str: str,
        strategy_tag: str | None = None,
        wallet_public_id: str = "",
        operator_public_id: str = "",
    ) -> TradingEngineService | None:
        """Create a TradingEngineService for recovery if exchange is valid."""
        valid_exchanges = get_args(OrderExchange)
        if exchange_str not in valid_exchanges:
            return None
        exchange = cast(OrderExchange, exchange_str)
        assert self.msg_publisher is not None
        await self._ensure_instrument(instrument, exchange=exchange)
        risk = RiskEvaluator(
            RiskConfigModel(
                r_per_trade=self.settings.risk_r_per_trade,
                max_leverage=self.settings.risk_max_leverage,
                max_drawdown=self.settings.risk_max_drawdown,
            )
        )
        specs_map = await self._resolve_instrument_specs(instrument, exchange)
        repo_for_engine = (
            self.repository if isinstance(self.repository, SQLAlchemyRepository) else None
        )
        return TradingEngineService(
            instrument,
            execution_socket=self.msg_publisher,
            risk=risk,
            cfg=EngineConfigModel(),
            instrument_specs=specs_map,
            exchange=exchange,
            repository=repo_for_engine,
            outbox=self.outbox,
            strategy_tag=strategy_tag,
            wallet_public_id=wallet_public_id,
            operator_public_id=operator_public_id,
            ownership=self._ownership,
            caps_enforcer=self._caps_enforcer,
        )

    def _handle_settings_update(self, payload: bytes) -> None:
        """Handle settings change event from ZMQ.

        Updates local settings cache when a setting is changed elsewhere
        in the system.

        Args:
            payload: JSON-encoded settings change data.
        """
        try:
            message = SettingChangedData.from_json(payload.decode("utf-8"))
            settings_service = SettingsService.get_instance()
            if settings_service:
                parsed_value = settings_service._parse_value(message.value)
                settings_service._cache[message.key] = parsed_value
                logger.info(f"ZMQTrader: Setting {message.key} updated via ZMQ event")
        except Exception as e:
            logger.error(f"ZMQTrader: Error handling settings update: {e}")

    async def _dispatch_order_event(self, topic: str, payload: bytes) -> None:
        """Dispatch order event to appropriate handler based on message type.

        Uses parse_message() to determine message type and routes accordingly
        ExecutionData -> _handle_execution_fill
        OrderData -> _handle_order_status
        OrderEventData -> _handle_order_event

        Under multi-instance partitioning
        (``instance_count > 1``) the shared ZMQ broker delivers every
        venue event to every coordinator. This method drops events that
        don't belong to this coordinator BEFORE the handlers would
        otherwise fall back to the flat ``{exchange}.{instrument}``
        shard key and mutate local :class:`TradeService` state for
        foreign-instance orders. Three gate states
        1. Empty ``client_order_id`` under N>1 — impossible to route
           deterministically; drop. (N=1 preserves existing
           fallthrough for legacy payload shapes.)
        2. Unknown ``client_order_id`` under N>1 — belongs to another
           coordinator (or is a late-arrival after this instance's
           state was rebuilt without it); drop.
        3. Known ``client_order_id`` with a shard NOT owned by this
           instance — defensive check against inconsistency between
           ``_order_shard_keys`` population and ownership; drop.

        Args:
            topic: ZMQ topic (e.g., "orders.events.kraken.BTC-USD.executed").
            payload: JSON-encoded message data.
        """
        try:
            msg = parse_message(payload.decode("utf-8"))
        except MessageParseError as e:
            logger.error(f"ZMQTrader: Invalid orders.events payload on {topic}: {e}")
            return
        if not isinstance(msg, ExecutionData | OrderData | OrderEventData):
            logger.debug(f"ZMQTrader: Ignoring orders.events message type={msg.type} on {topic}")
            return
        client_order_id = msg.client_order_id
        if self._ownership is not None and self._ownership.instance_count > 1:
            if not client_order_id:
                logger.debug(
                    "ZMQTrader: dropping venue event with empty client_order_id "
                    "under N>1 partitioning: topic={}",
                    topic,
                )
                return
            if client_order_id not in self._order_shard_keys:
                logger.debug(
                    "ZMQTrader: dropping venue event with unknown client_order_id "
                    "under N>1 partitioning: cid={}, topic={}",
                    client_order_id,
                    topic,
                )
                return
        if (
            self._ownership is not None
            and client_order_id
            and client_order_id in self._order_shard_keys
        ):
            owned_shard_key = self._order_shard_keys[client_order_id]
            if not self._ownership.owns(owned_shard_key):
                logger.debug(
                    "ZMQTrader: dropping venue event for foreign shard: cid={}, shard={}",
                    client_order_id,
                    owned_shard_key,
                )
                return
        if isinstance(msg, ExecutionData):
            await self._handle_execution_fill(topic, msg)
        elif isinstance(msg, OrderData):
            await self._handle_order_status(topic, msg)
        else:
            await self._handle_order_event(topic, msg)

    def _find_engine_for_fill(self, fill: ExecutionData) -> TradingEngineService | None:
        """Find engine matching an execution fill by client_order_id or instrument.

        Searches engines in three passes:
        1. Exact match on pending_client_order_id (current in-flight order).
        2. Wallet-scoped fallback: instrument + exchange + wallet_public_id.
        3. Legacy fallback: instrument + exchange only (when fill has no wallet).

        Args:
            fill: Execution fill to match.

        Returns:
            Matching engine or None if no engine found.
        """
        exact_match = self._find_engine_by_pending_client_order_id(fill.client_order_id)
        if exact_match is not None:
            return exact_match
        return self._find_engine_by_fill_scope(fill)

    def _find_engine_by_pending_client_order_id(
        self,
        client_order_id: str,
    ) -> TradingEngineService | None:
        """Find the engine owning the in-flight client_order_id.

        O(1) lookup against an index maintained by
        :py:meth:`_on_engine_pending_coid_change` (registered as the
        engine's ``pending_coid_listener`` at creation/recovery time).
        Falls back to a linear scan only when ``self.engines`` is no
        longer the auto-indexing :py:class:`_EngineRegistry` instance
        (test fixtures that replace the dict wholesale).
        """
        if isinstance(self.engines, _EngineRegistry):
            return self._engines_by_pending_coid.get(client_order_id)
        for engine in self.engines.values():
            if getattr(engine, "pending_client_order_id", None) == client_order_id:
                return engine
        return None

    def _find_engine_by_fill_scope(self, fill: ExecutionData) -> TradingEngineService | None:
        """Match a fill by wallet-aware or legacy instrument scope.

        Delegates to :py:meth:`_find_engine_by_scope_values` — one source
        of truth for the walleted-frames-never-fall-back-to-legacy rule.
        """
        return self._find_engine_by_scope_values(
            fill.exchange, fill.instrument, fill.wallet_public_id
        )

    def _find_engine_by_scope_values(
        self,
        exchange: str,
        instrument: str,
        wallet_public_id: str | None,
    ) -> TradingEngineService | None:
        """Match an engine by wallet-aware or legacy instrument scope.

        O(1) lookup against scope indices populated when an engine is
        registered via :py:meth:`_register_engine_for_lookup`. The
        wallet-aware path requires a non-empty ``wallet_public_id`` on
        both the frame and the engine — a WALLET-SCOPED frame never
        falls back to the legacy (exchange, instrument) lookup (the
        legacy index is populated unconditionally, so a fallback could
        match wallet A's engine for wallet B's order), matching the
        prior linear-scan semantics. Falls back to a linear scan only
        when ``self.engines`` is no longer the auto-indexing
        :py:class:`_EngineRegistry` instance.
        """
        if isinstance(self.engines, _EngineRegistry):
            if wallet_public_id:
                return self._engines_by_scope.get((exchange, instrument, wallet_public_id))
            return self._engines_by_scope_legacy.get((exchange, instrument))
        if wallet_public_id:
            for engine in self.engines.values():
                if (
                    getattr(engine, "instrument", None) == instrument
                    and getattr(engine, "exchange", None) == exchange
                    and getattr(engine, "wallet_public_id", None) == wallet_public_id
                ):
                    return engine
            return None
        for engine in self.engines.values():
            if (
                getattr(engine, "instrument", None) == instrument
                and getattr(engine, "exchange", None) == exchange
            ):
                return engine
        return None

    async def _handle_execution_fill(self, topic: str, fill: ExecutionData) -> None:
        """Handle execution fill event from ZMQ.

        Finds the matching engine and applies the fill using delta semantics.
        Duplicate fills are silently dropped via idempotency guard.

        Args:
            topic: ZMQ topic (e.g., "orders.events.kraken.BTC-USD.executed").
            fill: Parsed execution fill data.
        """
        parsed = parse_order_event_topic(topic)
        if parsed is None:
            logger.debug(f"ZMQTrader: Ignoring malformed fill topic: {topic}")
            return
        if fill.exchange != parsed.exchange or fill.instrument != parsed.instrument:
            logger.warning(
                f"ZMQTrader: Invariant violation - topic '{parsed.exchange}/{parsed.instrument}' "
                f"!= payload '{fill.exchange}/{fill.instrument}'"
            )
            return
        engine = self._find_engine_for_fill(fill)
        if engine is None:
            logger.info(
                f"ZMQTrader: No engine found for fill {fill.client_order_id} "
                f"{fill.instrument} on {fill.exchange}"
            )
            return
        if fill.status == FillStatusEnum.FILLED:
            self._retire_rearmable_cid(fill.client_order_id)
        applied = engine.apply_fill(fill)
        if applied:
            logger.info(
                f"ZMQTrader: Fill applied - {fill.client_order_id} "
                f"{fill.side} {fill.last_size}@{fill.last_price} "
                f"{fill.instrument} on {parsed.exchange} "
                f"(pos={engine.position_qty:.6f}, status={fill.status})"
            )
            await self._sync_fill_to_trade_service(fill, engine)
        else:
            logger.info(
                f"ZMQTrader: Duplicate fill ignored - {fill.client_order_id} "
                f"trade_id={fill.trade_id} {fill.instrument} on {parsed.exchange}"
            )

    def _parse_valid_order_status_topic(
        self, topic: str, order_status: OrderData
    ) -> ParsedOrderTopic | None:
        """Parse and validate an order-status topic against its payload."""
        parsed = parse_order_event_topic(topic)
        if parsed is None:
            logger.debug(f"ZMQTrader: Ignoring malformed order status topic: {topic}")
            return None
        if order_status.exchange != parsed.exchange or order_status.instrument != parsed.instrument:
            logger.warning(
                f"ZMQTrader: Invariant violation - topic '{parsed.exchange}/{parsed.instrument}' "
                f"!= payload '{order_status.exchange}/{order_status.instrument}'"
            )
            return None
        if order_status.status != parsed.suffix:
            logger.warning(
                f"ZMQTrader: Invariant violation - topic suffix '{parsed.suffix}' "
                f"!= payload status '{order_status.status}', dropping message"
            )
            return None
        return parsed

    def _clear_rejected_order_intent(
        self, parsed: ParsedOrderTopic, order_status: OrderData
    ) -> None:
        """Clear in-flight intent for a rejected submit status."""
        for engine in self.engines.values():
            if engine.clear_pending_intent(order_status.client_order_id):
                logger.info(
                    f"ZMQTrader: Cleared in-flight for rejected order "
                    f"{order_status.client_order_id} on {parsed.exchange}"
                )
                break

    def _mark_unknown_order_intent(self, parsed: ParsedOrderTopic, order_status: OrderData) -> None:
        """Mark in-flight intent as unknown after an ambiguous submit."""
        for engine in self.engines.values():
            if engine.mark_pending_unknown(order_status.client_order_id):
                logger.warning(
                    f"ZMQTrader: Order {order_status.client_order_id} submit state "
                    f"UNKNOWN on {parsed.exchange} — engine guard held, no "
                    f"re-emission until venue verification resolves"
                )
                break

    def _clear_unknown_order_intent(
        self, parsed: ParsedOrderTopic, order_status: OrderData
    ) -> None:
        """Clear UNKNOWN state after the order resolves to accepted."""
        for engine in self.engines.values():
            if engine.clear_pending_unknown(order_status.client_order_id):
                logger.info(
                    f"ZMQTrader: Order {order_status.client_order_id} resolved from "
                    f"UNKNOWN to accepted on {parsed.exchange}"
                )
                break

    def _rearm_adopted_order_intent(
        self, parsed: ParsedOrderTopic, order_status: OrderData
    ) -> bool:
        """Re-arm a released engine's guard for a venue-live adopted order (#155).

        The executor's adoption paths (ghost adoption, ambiguous-submit
        verification, false-reject heal, startup recovery of a
        venue-verified-open row) publish ACCEPTED with
        ``reason="adopted"``. A RUNNING engine that already released its
        in-flight intent (it consumed the false REJECTED, or the lazy
        timeout valve cleared the guard) would otherwise emit a NEW
        order on the next signal while the adopted one still works the
        book — double exposure.

        Routing mirrors fill routing exactly: exact pending-coid index
        first (engine still tracks the cid), then the wallet-aware scope
        index — a WALLETED frame never falls back to the legacy
        (exchange, instrument) index. Dispositions:

        - no engine in scope: WARN + drop (foreign wallet, or not this
          coordinator's scope; under N>1 the admission gate in
          ``_dispatch_order_event`` already dropped unregistered cids —
          re-arm targets engines that ONCE HELD intent, and those
          coordinators registered the cid at dispatch and never popped
          it on the false reject).
        - registered shard key mismatching the routed engine's shard:
          WARN + skip BEFORE any engine mutation.
        - engine in flight for a DIFFERENT order: WARN + skip (never
          clobber the newer live intent — the inverse failure of the
          bug; documented #155 residual).
        - engine in flight for the SAME order: refresh the in-flight
          window (a live venue observation must not let a near-expired
          valve clear right after adoption).
        - engine released: re-arm with a fresh window and re-register
          the cid->shard mapping via the conflict-dropping helper.

        Returns True only when the frame was honored (re-arm or
        refresh) — the caller SUPPRESSES the TradeService shadow-write
        for every refused/stale adopted frame, because the shard
        command projection tracks the CURRENT command and a stale
        adopted ACCEPTED would overwrite a newer command's identity or
        regress a FILLED projection back to ACCEPTED.

        A cid retired by an honest terminal (FILLED fill,
        cancelled/expired confirm) is refused outright: a late
        duplicate adopted frame (parked retry, recovery republish)
        re-arming a DEAD order's guard would block honest emission
        until the lazy valve clears it.
        """
        cid = order_status.client_order_id
        if cid in self._rearm_retired_cids:
            logger.info(
                f"ZMQTrader: adopted frame for {cid} arrived after its honest "
                f"terminal — stale duplicate dropped (no re-arm, no shadow-write)"
            )
            return False
        engine = self._find_engine_by_pending_client_order_id(cid)
        if engine is None:
            engine = self._find_engine_by_scope_values(
                order_status.exchange, order_status.instrument, order_status.wallet_public_id
            )
        if engine is None:
            logger.warning(
                f"ZMQTrader: adopted order {cid} on {parsed.exchange}/"
                f"{order_status.instrument} (wallet={order_status.wallet_public_id!r}) "
                f"has no engine in scope — re-arm dropped"
            )
            return False
        known_shard = self._order_shard_keys.get(cid)
        if known_shard is not None and known_shard != engine._shard_key:
            logger.warning(
                f"ZMQTrader: adopted order {cid} maps to shard {known_shard} but the "
                f"scope-routed engine owns {engine._shard_key} — re-arm skipped before "
                f"any engine mutation"
            )
            return False
        was_in_flight = engine.order_in_flight
        if not engine.rearm_pending_intent(cid):
            logger.warning(
                f"ZMQTrader: adopted order {cid} is live at the venue but engine "
                f"{engine._shard_key} already holds intent for "
                f"{engine.pending_client_order_id} — NOT clobbering the newer order "
                f"(#155 residual: both orders are live; fills of the adopted one "
                f"still book position)"
            )
            return False
        if was_in_flight:
            logger.info(
                f"ZMQTrader: adopted order {cid} confirmed live — refreshed the "
                f"in-flight window on {engine._shard_key}"
            )
            return True
        self._register_order_shard_key(cid, engine._shard_key)
        logger.warning(
            f"ZMQTrader: RE-ARMED in-flight intent for adopted order {cid} on "
            f"{engine._shard_key} — the engine had released it (false reject or "
            f"timeout valve) while the order stayed live at the venue"
        )
        return True

    def _retire_rearmable_cid(self, client_order_id: str) -> None:
        """Record an honest terminal so late adopted frames cannot re-arm it.

        Fed by FILLED fills and cancelled/expired confirms — NOT by
        rejections (the false-reject heal is exactly the case the
        re-arm exists for). LRU-bounded like the engine's exec-id
        dedupe; cids are uuid7-unique so retirement never needs
        clearing.
        """
        if not client_order_id:
            return
        self._rearm_retired_cids[client_order_id] = None
        while len(self._rearm_retired_cids) > _REARM_RETIRED_CIDS_MAX:
            self._rearm_retired_cids.popitem(last=False)

    @staticmethod
    def _log_order_status(parsed: ParsedOrderTopic, order_status: OrderData) -> None:
        """Log an accepted, rejected, or ordinary order status."""
        if parsed.suffix == "unknown":
            return
        if parsed.suffix == "rejected":
            logger.info(
                f"ZMQTrader: Order status [OrderData] - {order_status.client_order_id} "
                f"{parsed.suffix} (submit rejection) {order_status.instrument} "
                f"on {parsed.exchange}"
            )
            return
        logger.info(
            f"ZMQTrader: Order status - {order_status.client_order_id} {parsed.suffix} "
            f"{order_status.instrument} on {parsed.exchange}"
        )

    async def _handle_order_status(self, topic: str, order_status: OrderData) -> None:
        """Handle order status event from ZMQ.

        Logs order status changes (submitted, accepted, rejected, etc.).
        Performs invariant checks:
        - topic exchange/instrument must match payload
        - topic suffix must match payload status

        For 'rejected' status, the log includes message type to disambiguate
        submit rejection (OrderData) vs cancel/replace rejection
        (OrderEventData). A submit rejection also projects the terminal state
        onto its paired-execution leg so the guard scanner can break
        an armed group whose sibling rejected before filling.

        Args:
            topic: ZMQ topic (e.g., "orders.events.kraken.BTC-USD.accepted").
            order_status: Parsed order status data.
        """
        parsed = self._parse_valid_order_status_topic(topic, order_status)
        if parsed is None:
            return
        suppress_shadow_write = False
        if parsed.suffix == "rejected":
            self._clear_rejected_order_intent(parsed, order_status)
        elif parsed.suffix == "unknown":
            self._mark_unknown_order_intent(parsed, order_status)
        elif parsed.suffix == "accepted":
            self._clear_unknown_order_intent(parsed, order_status)
            if order_status.reason == ORDER_STATUS_REASON_ADOPTED:
                suppress_shadow_write = not self._rearm_adopted_order_intent(parsed, order_status)
        self._log_order_status(parsed, order_status)
        if not suppress_shadow_write:
            self._sync_status_to_trade_service(order_status, parsed)
        if parsed.suffix == "rejected":
            await self._project_paired_execution_leg_terminal(
                order_status.client_order_id,
                PairedExecutionLegStatusEnum.REJECTED.value,
                order_status.exchange_order_id,
            )

    async def _handle_order_event(self, topic: str, order_event: OrderEventData) -> None:
        """Handle lightweight order event from ZMQ (cancel/replace confirmations).

        Logs cancel/replace event confirmations (cancelled, replaced, rejected).
        Performs invariant checks (all are hard drops on violation):
        - topic exchange/instrument must match payload
        - topic suffix must match payload event

        For 'rejected' event, the log includes message type to disambiguate
        cancel/replace rejection (OrderEventData) vs submit rejection
        (OrderData). A 'cancelled' / 'expired' event projects the terminal
        state onto its paired-execution leg; a 'rejected' event does
        NOT — a rejected cancel/replace leaves the original order live, so the
        leg is not terminal.

        Args:
            topic: ZMQ topic (e.g., "orders.events.kraken.BTC-USD.cancelled").
            order_event: Parsed order event data.
        """
        parsed = parse_order_event_topic(topic)
        if parsed is None:
            logger.debug(f"ZMQTrader: Ignoring malformed order event topic: {topic}")
            return
        if order_event.exchange != parsed.exchange or order_event.instrument != parsed.instrument:
            logger.warning(
                f"ZMQTrader: Invariant violation - topic '{parsed.exchange}/{parsed.instrument}' "
                f"!= payload '{order_event.exchange}/{order_event.instrument}'"
            )
            return
        if order_event.event != parsed.suffix:
            logger.warning(
                f"ZMQTrader: Invariant violation - topic suffix '{parsed.suffix}' "
                f"!= payload event '{order_event.event}', dropping message"
            )
            return
        if parsed.suffix in ("cancelled", "expired"):
            self._retire_rearmable_cid(order_event.client_order_id)
            for engine in self.engines.values():
                if engine.clear_pending_intent(order_event.client_order_id):
                    logger.info(
                        f"ZMQTrader: Cleared in-flight for {parsed.suffix} order "
                        f"{order_event.client_order_id} on {parsed.exchange}"
                    )
                    break
        if parsed.suffix == "rejected":
            logger.info(
                f"ZMQTrader: Order event [OrderEventData] - {order_event.client_order_id} "
                f"{parsed.suffix} (cancel/replace rejection) {order_event.instrument} "
                f"on {parsed.exchange}"
            )
        else:
            logger.info(
                f"ZMQTrader: Order event - {order_event.client_order_id} {parsed.suffix} "
                f"{order_event.instrument} on {parsed.exchange}"
            )
        self._sync_order_event_to_trade_service(order_event, parsed)
        if parsed.suffix in ("cancelled", "expired"):
            leg_status = (
                PairedExecutionLegStatusEnum.CANCELLED.value
                if parsed.suffix == "cancelled"
                else PairedExecutionLegStatusEnum.EXPIRED.value
            )
            await self._project_paired_execution_leg_terminal(
                order_event.client_order_id,
                leg_status,
                order_event.exchange_order_id,
            )

    async def _sync_fill_to_trade_service(
        self, fill: ExecutionData, engine: TradingEngineService
    ) -> None:
        """Shadow-write fill to TradeService and persist checkpoint.

        Constructs a VenueEventRow-like dict from the ZMQ ExecutionData,
        applies it to TradeService, updates BalanceService, syncs the
        position cycle lifecycle to DB, and writes a checkpoint for
        durable recovery. ``old_qty`` is sampled BEFORE
        :meth:`TradeService.apply_venue_event` and ``new_qty`` AFTER so
        that :meth:`_sync_position_cycle_on_fill` can classify the
        transition via :meth:`TradeService._detect_cycle_transition`.

        Args:
            fill: Execution fill data from ZMQ.
            engine: Engine that processed the fill (for shard_key).
        """
        shard_key = engine._shard_key
        old_qty = self.trade_service.get_position(shard_key).position_qty
        venue_event: VenueEventRow = {
            "id": int(time.monotonic_ns()),
            "public_id": "",
            "timestamp": datetime.now(UTC),
            "session_id": fill.session_id,
            "sequence_id": fill.sequence_id,
            "event_type": "fill_observed",
            "shard_key": shard_key,
            "command_public_id": None,
            "exchange": fill.exchange,
            "instrument": fill.instrument,
            "mode": engine.mode,
            "exchange_order_id": fill.exchange_order_id,
            "client_order_id": fill.client_order_id,
            "venue_client_id": None,
            "side": fill.side,
            "status": fill.status,
            "fill_price": fill.last_price,
            "fill_size": fill.last_size,
            "cum_fill_size": fill.size,
            "fee": fill.fee,
            "fee_asset": fill.fee_asset,
            "exec_id": None,
            "trade_id": fill.trade_id,
            "error": None,
            "venue_timestamp": fill.executed_at,
            "received_at": datetime.now(UTC),
        }
        self.trade_service.apply_venue_event(venue_event)
        await self._project_paired_execution_leg_fill(fill, venue_event)
        pos = self.trade_service.get_position(shard_key)
        new_qty = pos.position_qty
        self.balance_service.on_position_changed(
            shard_key=shard_key,
            position_qty=pos.position_qty,
            entry_price=pos.entry_price,
            cash=self.trade_service.get_equity(shard_key),
            peak_equity=self.trade_service.get_peak_equity(shard_key),
            realized_pnl=pos.realized_pnl,
        )
        await self._sync_position_cycle_on_fill(engine, old_qty, new_qty, fill)
        await self._persist_checkpoint(shard_key)

    async def _project_paired_execution_leg_fill(
        self, fill: ExecutionData, venue_event: VenueEventRow
    ) -> None:
        """Project a live venue fill onto its paired-execution leg, if grouped.

        A fill on the original order of a paired-execution leg sets the leg's
        signed cumulative ``filled_signed_qty`` so the guard scanner sees real
        exposure (activating the durable halt projection + recovery mirror). The
        leg is resolved by ``client_order_id``; a non-grouped fill matches no
        leg and is a no-op. Sign follows the fill side (buy +, sell −), matching
        the leg's own side for an original order. Skipped without a SQL
        repository (the paired-execution tables do not exist there). This is the
        LIVE path only; startup recovery handles fills observed while the
        coordinator was down.

        When the fill matched no leg directly (``NO_MATCH``) AND the guard is
        enabled, the fill is re-routed to the compensation projection:
        a reduce-only FLATTEN order carries a fresh ``client_order_id`` whose
        command supersedes a leg's original, so its fill lands on that leg's
        ``compensated_signed_qty`` instead. The compensation lookup is gated on
        the feature flag so a non-paired deployment never pays the second query.
        """
        if not isinstance(self.repository, SQLAlchemyRepository):
            return
        signed_qty = fill.size if fill.side == TradeSideEnum.BUY else -fill.size
        new_status = (
            PairedExecutionLegStatusEnum.FILLED.value
            if fill.status == FillStatusEnum.FILLED
            else PairedExecutionLegStatusEnum.PARTIALLY_FILLED.value
        )
        result = await self.repository.project_paired_execution_leg_fill(
            fill.client_order_id,
            signed_qty,
            new_status,
            datetime.now(UTC),
            self._tracker.session_id,
            self._tracker.next_sequence(f"paired.fill.{fill.client_order_id}"),
            exchange_order_id=fill.exchange_order_id,
            last_venue_event_id=venue_event["id"],
        )
        if (
            result == PairedFillProjection.NO_MATCH
            and _bootstrap_settings.paired_execution_guard_enabled
        ):
            await self.repository.project_paired_execution_compensation_fill(
                fill.client_order_id,
                datetime.now(UTC),
                self._tracker.session_id,
                self._tracker.next_sequence(f"paired.comp.{fill.client_order_id}"),
            )

    async def _project_paired_execution_leg_terminal(
        self, client_order_id: str, leg_status: str, exchange_order_id: str | None
    ) -> None:
        """Project a venue terminal (reject / cancel / expire) onto its leg, if grouped.

        A submit rejection, cancel or expiry of the original order of a
        paired-execution leg records the terminal ``leg_status`` so the guard
        scanner's ``_sweep_armed`` can break an armed group whose sibling went
        terminal before fully filling. The leg is resolved by
        ``client_order_id``; a non-grouped or already-terminal / already-FILLED
        leg is a no-op. ``filled_signed_qty`` is preserved, so a partially-filled
        leg that then cancels keeps its exposure for compensation.
        Skipped without a SQL repository (the paired-execution tables do not
        exist there). The dispatcher already drops foreign-shard venue events,
        so this is not ownership-gated here (matching the live fill projection).

        When the guard is enabled the terminal is ALSO routed to the compensation
        projection: a FLATTEN order's own cancel / expire / reject
        carries the flatten's fresh ``client_order_id`` (matching no leg as an
        original), so it lands via ``supersedes_command_id`` on the leg it was
        flattening — settling that leg to ``flattened`` (residual gone) or back to
        ``filled`` (residual remains because a late original fill grew exposure) so
        the sweep re-flattens. A non-flatten terminal is a harmless ``NO_MATCH``.
        """
        if not isinstance(self.repository, SQLAlchemyRepository):
            return
        await self.repository.project_paired_execution_leg_terminal(
            client_order_id,
            leg_status,
            datetime.now(UTC),
            self._tracker.session_id,
            self._tracker.next_sequence(f"paired.terminal.{client_order_id}"),
            exchange_order_id=exchange_order_id,
        )
        if _bootstrap_settings.paired_execution_guard_enabled:
            await self.repository.project_paired_execution_compensation_fill(
                client_order_id,
                datetime.now(UTC),
                self._tracker.session_id,
                self._tracker.next_sequence(f"paired.comp.terminal.{client_order_id}"),
                flatten_terminal=True,
            )

    async def _sync_position_cycle_on_fill(
        self,
        engine: TradingEngineService,
        old_qty: float,
        new_qty: float,
        fill: ExecutionData,
    ) -> None:
        """Sync the position_cycles DB row for a single shadow-written fill.

        Classifies the transition via
        :meth:`TradeService._detect_cycle_transition` and issues the
        matching repository call, hydrating
        :attr:`ShardState.active_cycle_public_id` and
        :attr:`ShardState.active_cycle_max_qty` on success. TradeService
        itself stays pure — it does not know about position_cycles;
        cache hydration lives on the trader side.

        **Degraded identity**: if ``engine.wallet_public_id`` is falsy
        (recovery path where ``wallet_short`` could not be resolved in
        ``_recover_from_checkpoints``), all cycle writes are skipped
        fail-closed and a warning is logged. Pre-existing orphan open
        cycles may be left unclosed until an operator closes them via
        the position-cycle orphan admin endpoints.

        **Symmetric DB fallback (close/flip/scale_up)**: if the shard
        cache is empty on a non-open transition, the trader re-queries
        ``get_open_position_cycle`` to avoid reusing a stale open row
        from a prior life of the shard. Without this, a degraded
        reconciliation leaves ``active_cycle_public_id=None``; a later
        close is silently skipped; a later open would then hydrate the
        cache to the still-open DB row, mixing two logical cycles into
        one ``public_id``.

        **Idempotency (open)**: ``insert_position_cycle`` is preceded
        by a ``get_open_position_cycle`` check so a restart that
        missed the flat->non-flat transition in memory (but not in
        DB) becomes a cache-hydration no-op instead of a unique index
        violation.

        Args:
            engine: Engine owning the shard; source of wallet/operator
                and shard_key identity.
            old_qty: Signed position quantity before the fill.
            new_qty: Signed position quantity after the fill.
            fill: Execution payload providing timestamps and session
                provenance.
        """
        shard_key = engine._shard_key
        if not engine.wallet_public_id:
            logger.warning(
                "ZMQTrader: position_cycle write skipped (degraded identity) shard={}",
                shard_key,
            )
            return
        transition = TradeService._detect_cycle_transition(old_qty, new_qty)
        if transition is None:
            return
        shard = self.trade_service._get_or_create_shard(shard_key)
        now = datetime.now(UTC)
        if transition == "open":
            await self._sync_open_position_cycle(
                engine=engine,
                shard=shard,
                new_qty=new_qty,
                fill=fill,
                now=now,
            )
            return
        if transition == "close":
            await self._sync_close_position_cycle(
                shard_key=shard_key,
                shard=shard,
                fill=fill,
                now=now,
            )
            return
        if transition == "flip":
            await self._sync_flip_position_cycle(
                engine=engine,
                shard_key=shard_key,
                shard=shard,
                new_qty=new_qty,
                fill=fill,
                now=now,
            )
            return
        await self._sync_scale_up_position_cycle(
            shard_key=shard_key,
            shard=shard,
            new_qty=new_qty,
            fill=fill,
            now=now,
        )

    async def _sync_open_position_cycle(
        self,
        *,
        engine: TradingEngineService,
        shard: ShardState,
        new_qty: float,
        fill: ExecutionData,
        now: datetime,
    ) -> None:
        """Insert or hydrate the open cycle after a flat-to-position transition."""
        shard_key = engine._shard_key
        existing = await self.repository.get_open_position_cycle(shard_key, as_of=now)
        if existing is not None:
            shard.active_cycle_public_id = existing["public_id"]
            shard.active_cycle_max_qty = existing["max_qty"]
            return
        instrument_public_id = await self._lookup_position_cycle_instrument_public_id(
            engine=engine,
            now=now,
        )
        if instrument_public_id is None:
            logger.warning(
                "ZMQTrader: position_cycle open skipped "
                "(unresolved instrument) shard={} symbol={}",
                shard_key,
                engine.instrument,
            )
            return
        direction, max_qty = self._describe_position_cycle(new_qty)
        open_row = self._build_position_cycle_insert_row(
            engine=engine,
            instrument_public_id=instrument_public_id,
            direction=direction,
            max_qty=max_qty,
            opened_at=fill.executed_at,
            timestamp=now,
            session_id=fill.session_id,
            sequence_id=fill.sequence_id,
        )
        _id, new_pid = await self.repository.insert_position_cycle(open_row)
        shard.active_cycle_public_id = new_pid
        shard.active_cycle_max_qty = max_qty

    async def _sync_close_position_cycle(
        self,
        *,
        shard_key: str,
        shard: ShardState,
        fill: ExecutionData,
        now: datetime,
    ) -> None:
        """Close the active cycle and clear the in-memory cache."""
        cycle_id = await self._resolve_active_cycle_public_id(
            shard_key=shard_key,
            shard=shard,
            now=now,
            transition="close",
        )
        if cycle_id is None:
            return
        await self._close_position_cycle_and_clear_cache(
            shard=shard,
            cycle_public_id=cycle_id,
            fill=fill,
            now=now,
        )

    async def _sync_flip_position_cycle(
        self,
        *,
        engine: TradingEngineService,
        shard_key: str,
        shard: ShardState,
        new_qty: float,
        fill: ExecutionData,
        now: datetime,
    ) -> None:
        """Flip the active cycle or degrade to close-only when identity is unresolved."""
        cycle_id = await self._resolve_active_cycle_public_id(
            shard_key=shard_key,
            shard=shard,
            now=now,
            transition="flip",
        )
        if cycle_id is None:
            return
        instrument_public_id = await self._lookup_position_cycle_instrument_public_id(
            engine=engine,
            now=now,
        )
        if instrument_public_id is None:
            logger.warning(
                "ZMQTrader: position_cycle flip degraded to close-only "
                "(unresolved instrument) shard={} symbol={} closing cycle={}",
                shard_key,
                engine.instrument,
                cycle_id,
            )
            await self._close_position_cycle_and_clear_cache(
                shard=shard,
                cycle_public_id=cycle_id,
                fill=fill,
                now=now,
            )
            return
        direction, max_qty = self._describe_position_cycle(new_qty)
        new_open_row = self._build_position_cycle_insert_row(
            engine=engine,
            instrument_public_id=instrument_public_id,
            direction=direction,
            max_qty=max_qty,
            opened_at=fill.executed_at,
            timestamp=now,
            session_id=fill.session_id,
            sequence_id=fill.sequence_id,
        )
        _id, new_pid = await self.repository.flip_position_cycle(
            close_cycle_public_id=cycle_id,
            new_open_row=new_open_row,
            bus_time=now,
            session_id=fill.session_id,
            sequence_id=fill.sequence_id,
        )
        shard.active_cycle_public_id = new_pid
        shard.active_cycle_max_qty = max_qty

    async def _sync_scale_up_position_cycle(
        self,
        *,
        shard_key: str,
        shard: ShardState,
        new_qty: float,
        fill: ExecutionData,
        now: datetime,
    ) -> None:
        """Persist max_qty growth for an already-open cycle."""
        cycle_id = await self._resolve_active_cycle_public_id(
            shard_key=shard_key,
            shard=shard,
            now=now,
            transition="scale_up",
            hydrate_cache=True,
        )
        if cycle_id is None:
            return
        _direction, new_max = self._describe_position_cycle(new_qty)
        if new_max <= shard.active_cycle_max_qty:
            return
        await self.repository.update_position_cycle_max_qty(
            cycle_public_id=cycle_id,
            new_max_qty=new_max,
            bus_time=now,
            session_id=fill.session_id,
            sequence_id=fill.sequence_id,
        )
        shard.active_cycle_max_qty = new_max

    async def _lookup_position_cycle_instrument_public_id(
        self,
        *,
        engine: TradingEngineService,
        now: datetime,
    ) -> str | None:
        """Resolve the instrument_public_id used by position_cycle writes."""
        return await self.repository.get_instrument_public_id_by_symbol(
            native_symbol=engine.instrument,
            exchange=str(engine.exchange),
            as_of=now,
        )

    async def _resolve_active_cycle_public_id(
        self,
        *,
        shard_key: str,
        shard: ShardState,
        now: datetime,
        transition: str,
        hydrate_cache: bool = False,
    ) -> str | None:
        """Resolve the active cycle id from cache first, then from the DB."""
        cycle_id = shard.active_cycle_public_id
        if cycle_id is not None:
            return cycle_id
        fallback = await self.repository.get_open_position_cycle(shard_key, as_of=now)
        if fallback is None:
            logger.warning(
                "ZMQTrader: position_cycle {} skipped (no open cycle in cache or DB) shard={}",
                transition,
                shard_key,
            )
            return None
        cycle_id = fallback["public_id"]
        if hydrate_cache:
            shard.active_cycle_public_id = cycle_id
            shard.active_cycle_max_qty = fallback["max_qty"]
        logger.warning(
            "ZMQTrader: position_cycle {} recovered via DB fallback "
            "(cache miss) shard={} cycle={}",
            transition,
            shard_key,
            cycle_id,
        )
        return cycle_id

    async def _close_position_cycle_and_clear_cache(
        self,
        *,
        shard: ShardState,
        cycle_public_id: str,
        fill: ExecutionData,
        now: datetime,
    ) -> None:
        """Close the active cycle row and clear the shard cache."""
        await self.repository.close_position_cycle(
            cycle_public_id=cycle_public_id,
            closed_at=fill.executed_at,
            closing_command_public_id=None,
            bus_time=now,
            session_id=fill.session_id,
            sequence_id=fill.sequence_id,
        )
        shard.active_cycle_public_id = None
        shard.active_cycle_max_qty = 0.0

    def _sync_status_to_trade_service(self, order_status: OrderData, parsed: Any) -> None:
        """Shadow-write order status to TradeService.

        Maps ZMQ OrderData status events (submitted, accepted, rejected,
        unknown) to synthetic VenueEventRow and applies to TradeService.
        The map is exhaustive on purpose: an unmapped suffix is skipped
        with a warning rather than defaulted — the previous default of
        ``order_accepted`` would have silently marked a command ACCEPTED
        for any new event type.

        Args:
            order_status: Order status data from ZMQ.
            parsed: Parsed topic with exchange, instrument, suffix.
        """
        mode = (
            ExecutionModeEnum.PAPER
            if parsed.exchange == ExchangeEnum.PAPER
            else ExecutionModeEnum.LIVE
        )
        flat_key = compute_shard_key(
            instrument=parsed.instrument,
            exchange=cast(OrderExchange, parsed.exchange),
            mode=mode,
            wallet_public_id="",
            strategy_tag=None,
        )
        shard_key = self._order_shard_keys.get(order_status.client_order_id, flat_key)
        event_type_map = {
            "accepted": "order_accepted",
            "rejected": "order_rejected",
            "submitted": "order_accepted",
            "unknown": "order_submit_unknown",
        }
        event_type = event_type_map.get(parsed.suffix)
        if event_type is None:
            logger.warning(
                f"ZMQTrader: no venue-event mapping for order status suffix "
                f"'{parsed.suffix}' ({order_status.client_order_id}), skipping shadow-write"
            )
            return
        venue_event: VenueEventRow = {
            "id": int(time.monotonic_ns()),
            "public_id": "",
            "timestamp": datetime.now(UTC),
            "session_id": order_status.session_id,
            "sequence_id": order_status.sequence_id,
            "event_type": event_type,
            "shard_key": shard_key,
            "command_public_id": None,
            "exchange": order_status.exchange,
            "instrument": order_status.instrument,
            "mode": mode,
            "exchange_order_id": order_status.exchange_order_id,
            "client_order_id": order_status.client_order_id,
            "venue_client_id": None,
            "side": order_status.side,
            "status": parsed.suffix,
            "fill_price": None,
            "fill_size": None,
            "cum_fill_size": None,
            "fee": None,
            "fee_asset": None,
            "exec_id": None,
            "trade_id": None,
            "error": order_status.error,
            "venue_timestamp": None,
            "received_at": datetime.now(UTC),
        }
        self.trade_service.apply_venue_event(venue_event)

    def _sync_order_event_to_trade_service(self, order_event: OrderEventData, parsed: Any) -> None:
        """Shadow-write order event (cancel/expire) to TradeService.

        Maps ZMQ OrderEventData to synthetic VenueEventRow and applies
        to TradeService for terminal state tracking.

        Args:
            order_event: Order event data from ZMQ.
            parsed: Parsed topic with exchange, instrument, suffix.
        """
        if parsed.suffix not in ("cancelled", "expired"):
            return
        mode = (
            ExecutionModeEnum.PAPER
            if parsed.exchange == ExchangeEnum.PAPER
            else ExecutionModeEnum.LIVE
        )
        flat_key = compute_shard_key(
            instrument=parsed.instrument,
            exchange=cast(OrderExchange, parsed.exchange),
            mode=mode,
            wallet_public_id="",
            strategy_tag=None,
        )
        shard_key = self._order_shard_keys.get(order_event.client_order_id, flat_key)
        venue_event: VenueEventRow = {
            "id": int(time.monotonic_ns()),
            "public_id": "",
            "timestamp": datetime.now(UTC),
            "session_id": order_event.session_id,
            "sequence_id": order_event.sequence_id,
            "event_type": "order_terminal",
            "shard_key": shard_key,
            "command_public_id": None,
            "exchange": order_event.exchange,
            "instrument": order_event.instrument,
            "mode": mode,
            "exchange_order_id": order_event.exchange_order_id,
            "client_order_id": order_event.client_order_id,
            "venue_client_id": None,
            "side": None,
            "status": parsed.suffix,
            "fill_price": None,
            "fill_size": None,
            "cum_fill_size": None,
            "fee": None,
            "fee_asset": None,
            "exec_id": None,
            "trade_id": None,
            "error": None,
            "venue_timestamp": None,
            "received_at": datetime.now(UTC),
        }
        self.trade_service.apply_venue_event(venue_event)
        self._order_shard_keys.pop(order_event.client_order_id, None)

    async def _persist_checkpoint(self, shard_key: str) -> None:
        """Write trade projection checkpoint to DB.

        Persists the current shard state for durable recovery. Silent
        no-op if repository is not SQLAlchemyRepository.

        Args:
            shard_key: Shard to checkpoint.
        """
        if not isinstance(self.repository, SQLAlchemyRepository):
            return
        snap = self.trade_service.snapshot_for_checkpoint(shard_key)
        now = datetime.now(UTC)
        ep = snap["entry_price"]
        oci = snap["open_command_ids"]
        real_watermark = await self.repository.get_latest_venue_event_id(shard_key)
        parsed_shard = self._parse_shard_key(shard_key)
        wallet_short = parsed_shard[3] if parsed_shard else ""
        wallet_public_id = self._wallet_short_to_id.get(wallet_short, "") if wallet_short else ""
        operator_public_id: str | None = None
        for engine in self.engines.values():
            if engine._shard_key == shard_key:
                operator_public_id = engine.operator_public_id or None
                break
        try:
            opened_at = snap.get("position_opened_at")
            await self.repository.upsert_checkpoint(
                {
                    "shard_key": shard_key,
                    "wallet_public_id": wallet_public_id,
                    "operator_public_id": operator_public_id,
                    "position_qty": cast(float, snap["position_qty"]),
                    "entry_price": cast(float, ep) if ep is not None else None,
                    "position_opened_at": (
                        cast(datetime, opened_at) if opened_at is not None else None
                    ),
                    "cash": cast(float, snap["cash"]),
                    "peak_equity": cast(float, snap["peak_equity"]),
                    "realized_pnl": cast(float, snap["realized_pnl"]),
                    "turnover": cast(float, snap["turnover"]),
                    "last_venue_event_id": real_watermark,
                    "last_venue_event_at": now if real_watermark is not None else None,
                    "open_command_ids": cast(str, oci) if oci is not None else None,
                    "seen_exec_ids": cast(str, snap["seen_exec_ids"]),
                    "checkpoint_at": now,
                    "session_id": self._tracker.session_id,
                    "sequence_id": self._tracker.next_sequence(f"checkpoint.{shard_key}"),
                    "bus_time": now,
                }
            )
        except Exception:
            logger.exception(f"TraderCoordinator: Failed to persist checkpoint for {shard_key}")

    async def stop(self) -> None:
        """Stop the trader coordinator and cleanup resources.

        Closes all ZMQ sockets and terminates contexts.
        """
        logger.info("Stopping ZMQ Signal TraderCoordinator")
        if self.outbox is not None:
            self.outbox.stop()
        if self.guard_scanner is not None:
            self.guard_scanner.stop()
        if self.signal_subscriber:
            self.signal_subscriber.setsockopt(zmq.LINGER, 0)
            self.signal_subscriber.close()
            self.signal_subscriber = None
        if self.zmq_context:
            self.zmq_context.term()
            self.zmq_context = None
        if self.execution_publisher:
            self.execution_publisher.setsockopt(zmq.LINGER, 0)
            self.execution_publisher.close()
        if self.execution_context:
            self.execution_context.term()

    def _setup_trading_components(self) -> None:
        """Initialize trading components.

        Validates that execution publisher is ready. Engines are created
        dynamically when signals arrive.
        """
        if not self.execution_publisher:
            raise RuntimeError(
                "execution_publisher not initialized - call _setup_external_execution first"
            )
        logger.info("ZMQTrader: Engines will be created dynamically from incoming signals")

    async def _ensure_instrument(self, instrument: str, exchange: str) -> None:
        """Ensure instrument exists in database.

        Creates or updates the instrument record with base/quote currencies.
        Resolves Symbol.public_id so the Instrument carries the stable
        symbol identity key.

        Args:
            instrument: Symbol string (e.g., "BTC-USD" or "BTC/USD").
            exchange: Exchange name (lowercase).
        """
        now = datetime.now(UTC)
        symbol_pid = await resolve_symbol_public_id(self.repository, instrument, as_of=now)
        if symbol_pid is None:
            logger.warning(
                f"ZMQTrader: No active Symbol row for {instrument}, skipping instrument upsert"
            )
            return
        await self.repository.ensure_instrument(
            symbol_public_id=symbol_pid,
            exchange=exchange,
            session_id=self._tracker.session_id,
            sequence_id=self._tracker.next_sequence("instruments"),
            timestamp=now,
        )

    def _setup_external_execution(self) -> None:
        """Set up ZMQ publisher for order execution.

        Connects to the broker XSUB endpoint for publishing order requests.
        """
        self.execution_context = zmq.asyncio.Context()
        raw_pub_socket = self.execution_context.socket(zmq.PUB)
        apply_hwm(raw_pub_socket, sndhwm=HWM_ORDER_FLOW)
        raw_pub_socket.connect(self.settings.zmq_broker_xsub)
        self.execution_publisher = ValidatedPublisher(raw_pub_socket)
        self.msg_publisher = MessagePublisher(self.execution_publisher, self._tracker)
        logger.info(
            f"ZMQTrader: Connected to broker for order publishing: {self.settings.zmq_broker_xsub}"
        )

    def _setup_signal_subscriber(self) -> None:
        """Set up ZMQ subscriber for signals and system events.

        Subscribes to:
        - Signal topics (configurable)
        - system.symbol_aliases (cache invalidation)
        - system.settings (settings updates)
        - orders.events.* (fill notifications and order status updates)
        """
        self.zmq_context = zmq.asyncio.Context()
        raw_sub_socket = self.zmq_context.socket(zmq.SUB)
        apply_hwm(raw_sub_socket, rcvhwm=HWM_ORDER_FLOW)
        broker_addr = _bootstrap_settings.zmq_broker_xpub
        logger.info(f"ZMQTrader: Connecting signal subscriber to broker {broker_addr}")
        raw_sub_socket.connect(broker_addr)
        self.signal_subscriber = ValidatedSubscriber(raw_sub_socket)
        for topic in self.signal_topics:
            logger.info(f"ZMQTrader: Subscribing to {topic}")
            self.signal_subscriber.subscribe(topic)
        logger.info("ZMQTrader: Subscribing to system.symbol_aliases")
        self.signal_subscriber.subscribe("system.symbol_aliases")
        logger.info("ZMQTrader: Subscribing to system.settings")
        self.signal_subscriber.subscribe("system.settings")
        logger.info("ZMQTrader: Subscribing to orders.events. (fills and status updates)")
        self.signal_subscriber.subscribe("orders.events.")
        logger.info("ZMQTrader: Subscribing to system.heartbeats.executor.")
        self.signal_subscriber.subscribe("system.heartbeats.executor.")

    def _setup_trade_services(self) -> None:
        """Initialize trade domain services + outbox dispatcher.

        TradeService and BalanceService are initialized in __init__.
        The durable outbox dispatcher always owns the dispatch path
        when a real SQLAlchemyRepository is wired; the engine writes
        TradeCommand rows and notifies the outbox, which in turn
        publishes on ZMQ. Tests running without a SQL-backed repo
        (e.g. MagicMock) skip outbox construction and the engine's
        in-process ZMQ send path is exercised directly.
        """
        if isinstance(self.repository, SQLAlchemyRepository):
            try:
                configured_ttl = float(self.settings.trade_command_dispatch_ttl_s)
            except (AttributeError, TypeError, ValueError):
                configured_ttl = 0.0
            self.outbox = OutboxDispatcher(
                repository=self.repository,
                publish_fn=self._outbox_publish,
                poll_interval=0.05,
                ownership=self._ownership,
                max_scan_rows=self.settings.coordinator_outbox_max_scan_rows,
                dispatch_ttl_s=configured_ttl if configured_ttl > 0 else None,
                expire_fn=self._on_command_expired,
            )
            logger.info("TraderCoordinator: durable command mode (outbox active)")
        else:
            logger.info("TraderCoordinator: no SQL repo, outbox disabled for tests only")

    def _create_guard_scanner_task(self) -> asyncio.Task[None] | None:
        """Create the paired-execution guard scanner task.

        Returns the started scan-loop task (and stores the scanner on
        ``self.guard_scanner`` so ``stop()`` can halt it), or ``None`` when no
        SQL repository or shard ownership is wired (tests). The interval is
        half the assembly timeout so a stalled group is broken within roughly
        one timeout of its deadline.
        """
        if not isinstance(self.repository, SQLAlchemyRepository) or self._ownership is None:
            return None
        self.guard_scanner = PairedExecutionGuardScanner(
            repository=self.repository,
            ownership=self._ownership,
            trade_service=self.trade_service,
            interval_seconds=max(1.0, _bootstrap_settings.paired_execution_assembly_timeout_s / 2),
            outbox=self.outbox,
        )
        return asyncio.create_task(self.guard_scanner.run())

    def _create_reconciliation_tasks(self) -> list[asyncio.Task[None]]:
        """Create per-exchange reconciliation background tasks.

        Reconciliation loops (one per configured exchange, querying
        ``TradeCommand.exchange``) share the ``TradeService`` for
        circuit-breaker state. Skipped when no SQL-backed repository
        is wired (tests running with MagicMock repos).

        Returns:
            List of asyncio tasks, one per supported exchange; empty
            list when the repository is not a ``SQLAlchemyRepository``.
        """
        tasks: list[asyncio.Task[None]] = []
        if not isinstance(self.repository, SQLAlchemyRepository):
            return tasks
        exchanges: list[str] = list(get_args(OrderExchange))
        for exchange_name in exchanges:
            recon = ReconciliationLoop(
                exchange_name=exchange_name,
                repository=self.repository,
                trade_service=self.trade_service,
                interval_seconds=60.0,
                ownership=self._ownership,
            )
            tasks.append(asyncio.create_task(recon.run()))
        logger.info(f"TraderCoordinator: reconciliation loops enabled for {exchanges}")
        return tasks

    async def _on_command_expired(self, cmd: TradeCommandRow) -> None:
        """Release engine intent for a command the outbox expired.

        The command was never published, so no executor event will ever
        arrive for it — without this release the engine's in-flight
        guard (armed at command INSERT) would wedge for its full 60s
        valve per expiry. The outbox runs in-process, so instead of
        fabricating bus traffic this routes a synthetic
        ``OrderEventData(event="expired")`` through
        ``_handle_order_event`` — the exact pipeline a venue expiry
        uses: clears ``order_in_flight`` via ``clear_pending_intent``,
        shadow-writes ``order_terminal`` to TradeService, and projects
        the paired-execution leg EXPIRED.

        Args:
            cmd: The expired TradeCommandRow.
        """
        self._register_order_shard_key(cmd["client_order_id"], cmd["shard_key"])
        exchange = cast(OrderExchange, cmd["exchange"])
        topic = order_event_topic(exchange, cmd["instrument"], OrderEventEnum.EXPIRED)
        event = OrderEventData(
            public_id=cmd["client_order_id"],
            timestamp=datetime.now(UTC),
            session_id=cmd["session_id"],
            sequence_id=cmd["sequence_id"],
            exchange_order_id="",
            client_order_id=cmd["client_order_id"],
            exchange=exchange,
            instrument=cmd["instrument"],
            event=OrderEventEnum.EXPIRED,
            reason="expired by outbox dispatch TTL",
            wallet_public_id=cmd.get("wallet_public_id") or "",
            operator_public_id=cmd.get("operator_public_id"),
            user_public_id=cmd.get("user_public_id"),
        )
        await self._handle_order_event(topic, event)

    async def _outbox_publish(self, cmd: TradeCommandRow) -> None:
        """Publish a trade command from outbox to ZMQ.

        Branches on ``command_type``:

        - ``create`` / ``submit``: publishes an ``OrderRequestData`` to
          the ``.submit`` topic for the exchange executor to place.
        - ``cancel``: publishes an ``OrderCancelData`` to the ``.cancel``
          topic, carrying the original order's ``client_order_id`` and
          ``exchange_order_id``. The venue id is re-hydrated from the
          active ``orders`` row at dispatch time so a late venue ACK
          landing after the REST route snapshotted the command row
          still results in a cancel that targets the correct exchange
          order id.

        Args:
            cmd: TradeCommandRow dict from the outbox dispatcher.
        """
        assert self.msg_publisher is not None
        exchange = cast(OrderExchange, cmd["exchange"])
        command_type = cmd["command_type"]
        if command_type == OrderCommandEnum.CANCEL.value:
            topic = order_command_topic(exchange, cmd["instrument"], OrderCommandEnum.CANCEL)
            resolved_venue_id = cmd.get("exchange_order_id") or ""
            if not resolved_venue_id:
                fresh = await self.repository.get_exchange_order_id_for_client_order_id(
                    cmd["client_order_id"], as_of=datetime.now(UTC)
                )
                if fresh:
                    resolved_venue_id = fresh
            cancel = OrderCancelData(
                public_id=cmd["client_order_id"],
                timestamp=datetime.now(UTC),
                session_id=cmd["session_id"],
                sequence_id=cmd["sequence_id"],
                exchange=exchange,
                instrument=cmd["instrument"],
                exchange_order_id=resolved_venue_id,
                client_order_id=cmd["client_order_id"],
                wallet_public_id=cmd.get("wallet_public_id") or "",
                operator_public_id=cmd.get("operator_public_id"),
                user_public_id=cmd.get("user_public_id"),
            )
            await self.msg_publisher.send(topic, cancel)
            return
        topic = order_command_topic(exchange, cmd["instrument"], OrderCommandEnum.SUBMIT)
        order = order_request_from_command(cmd)
        if command_type in ("create", OrderCommandEnum.SUBMIT.value):
            self._register_order_shard_key(cmd["client_order_id"], cmd["shard_key"])
        await self.msg_publisher.send(topic, order)

    async def _funding_accrual_loop(self) -> None:
        """Periodically charge funding/rollover fees to open positions.

        Runs as a coroutine inside the trading loop task list. On each
        iteration: for every engine with a funding-enabled instrument,
        computes missed accrual boundaries since the last applied
        charge, inserts an AccrualLedger row, mutates in-memory state,
        persists a checkpoint, and publishes a ZMQ event.
        """
        while True:
            await asyncio.sleep(_ACCRUAL_POLL_SECONDS)
            try:
                await self._accrue_all_due_boundaries()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.opt(exception=True).warning("Accrual loop iteration failed")

    async def _accrue_all_due_boundaries(self) -> None:
        """Scan all engines and apply any missed accrual boundaries."""
        now = datetime.now(UTC)
        for engine_key, engine in tuple(self.engines.items()):
            try:
                await self._accrue_engine(engine_key, engine, now)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.opt(exception=True).debug("Accrual check failed for {}", engine_key)

    async def _accrue_engine(
        self, _engine_key: str, engine: TradingEngineService, now: datetime
    ) -> None:
        """Apply pending accrual boundaries for a single engine."""
        instrument_public_id = await self.repository.get_instrument_public_id_by_symbol(
            native_symbol=engine.instrument,
            exchange=engine.exchange,
            as_of=now,
        )
        if instrument_public_id is None:
            return
        spec = await self.repository.get_instrument_spec(instrument_public_id, as_of=now)
        if spec is None or spec["funding_type"] is None:
            return
        accrual_type = _FUNDING_TYPE_TO_ACCRUAL_TYPE.get(spec["funding_type"])
        if accrual_type is None:
            return
        position = self.trade_service.get_position(engine._shard_key)
        if position.position_qty == 0 or position.position_opened_at is None:
            return
        frequency_hours = spec["funding_frequency_hours"] or 4
        last_accrual = await self.repository.get_last_accrual(
            instrument_public_id,
            engine.mode,
            accrual_type,
            wallet_public_id=engine.wallet_public_id,
        )
        last_boundary = last_accrual["accrued_at"] if last_accrual else None
        boundaries = _compute_pending_boundaries(
            frequency_hours=frequency_hours,
            position_opened_at=position.position_opened_at,
            last_accrued_at=last_boundary,
            now=now,
        )
        for boundary in boundaries:
            await self._accrue_one_boundary(
                engine=engine,
                instrument_public_id=instrument_public_id,
                spec=spec,
                accrual_type=accrual_type,
                boundary=boundary,
                position=position,
                now=now,
            )

    async def _accrue_one_boundary(
        self,
        engine: TradingEngineService,
        instrument_public_id: str,
        spec: Any,
        accrual_type: str,
        boundary: datetime,
        position: Any,
        now: datetime,
    ) -> None:
        """Compute and apply a single accrual boundary charge."""
        direction = "long" if position.position_qty > 0 else "short"
        rate_direction = "both" if spec["funding_type"] == "perpetual_funding" else direction
        rate_rows = await self.repository.get_funding_rates(
            instrument_public_id=instrument_public_id,
            exchange=engine.exchange,
            rate_type=spec["funding_type"],
            direction=rate_direction,
            as_of=(
                boundary
                if spec["funding_type"] == "perpetual_funding"
                else position.position_opened_at
            ),
            range_end=boundary,
        )
        if not rate_rows:
            return
        rate_row = rate_rows[-1]
        mark_price = position.entry_price or 0.0
        notional = abs(position.position_qty) * mark_price
        if spec["funding_type"] == "perpetual_funding":
            signed_charge = notional * rate_row["rate"] * (1 if position.position_qty > 0 else -1)
        else:
            signed_charge = notional * rate_row["rate"]
        row: AccrualLedgerInsertRow = {
            "instrument_public_id": instrument_public_id,
            "mode": engine.mode,
            "accrual_type": accrual_type,
            "accrued_at": boundary,
            "amount": signed_charge,
            "amount_asset": rate_row["notional_asset"],
            "rate": rate_row["rate"],
            "notional": notional,
            "position_quantity_at_accrual": position.position_qty,
            "exchange": engine.exchange,
            "session_id": self._tracker.session_id,
            "sequence_id": self._tracker.next_sequence("accruals"),
            "timestamp": now,
            "wallet_public_id": engine.wallet_public_id,
            "operator_public_id": engine.operator_public_id or None,
        }
        try:
            await self.repository.insert_accrual(row)
        except IntegrityError:
            return
        self.trade_service.add_funding_accrual(engine._shard_key, signed_charge)
        engine.portfolio.accrue_funding(engine.instrument, signed_charge)
        await self._persist_checkpoint(engine._shard_key)
        if self.msg_publisher:
            topic = accrual_topic(engine.exchange, engine.instrument, accrual_type)
            envelope = FundingAccrualData(
                public_id=str(uuid7()),
                timestamp=now,
                session_id=self._tracker.session_id,
                sequence_id=self._tracker.next_sequence("accruals"),
                instrument=engine.instrument,
                exchange=engine.exchange,
                mode=engine.mode,
                accrual_type=cast(Any, accrual_type),
                accrued_at=boundary,
                amount=signed_charge,
                amount_asset=rate_row["notional_asset"],
                rate=rate_row["rate"],
                notional=notional,
                position_quantity=position.position_qty,
            )
            await self.msg_publisher.send(topic, envelope)

    async def _run_trading_loop(self) -> None:
        """Run the main trading loop.

        Spawns tasks for signal listening and health monitoring.
        Runs until cancelled.
        """
        tasks = [
            asyncio.create_task(self._listen_signals()),
            asyncio.create_task(self._signal_health_monitor()),
            asyncio.create_task(self._funding_accrual_loop()),
        ]
        if self.outbox is not None:
            tasks.append(asyncio.create_task(self.outbox.run()))
        scanner_task = self._create_guard_scanner_task()
        if scanner_task is not None:
            tasks.append(scanner_task)
        tasks.extend(self._create_reconciliation_tasks())
        try:
            await asyncio.gather(*tasks)
        except asyncio.CancelledError:
            logger.info("Trading loop cancelled")
            raise
        finally:
            for task in tasks:
                if not task.done():
                    task.cancel()
                    with contextlib.suppress(asyncio.CancelledError):
                        await task

    async def _listen_signals(self) -> None:
        """Listen for incoming signals and system events.

        Main message processing loop that routes messages based on topic:
        - Signals → _on_signal()
        - Settings → _handle_settings_update()
        - Order events → _handle_execution_fill() or _handle_order_status()
        """
        if not self.signal_subscriber:
            logger.error("ZMQTrader: No signal subscriber in listen loop")
            return
        logger.info("ZMQTrader: Starting signal listen loop")
        try:
            while True:
                topic_str, msg_bytes = await self.signal_subscriber.recv_multipart()
                if topic_str == "system.symbol_aliases":
                    logger.info("ZMQTrader: Received symbol_aliases update, refreshing cache")
                    SymbolMapperService.get_instance().trigger_cache_invalidation(fail_fast=False)
                    continue
                if topic_str == "system.settings":
                    self._handle_settings_update(msg_bytes)
                    continue
                if topic_str.startswith("orders.events."):
                    await self._dispatch_order_event(topic_str, msg_bytes)
                    continue
                if topic_str.startswith("system.heartbeats.executor."):
                    self._handle_executor_heartbeat(topic_str, msg_bytes)
                    continue
                signal = SignalData.from_json(msg_bytes.decode())
                self._gap_detector.check(
                    topic_str,
                    signal.session_id,
                    signal.sequence_id,
                    wallet_public_id=signal.wallet_public_id or "",
                )
                logger.info(f"ZMQTrader: Received signal from {topic_str}: {signal}")
                self._current_topic = topic_str
                await self._on_signal(signal)
        except asyncio.CancelledError:
            logger.info("ZMQTrader: Signal listen loop cancelled")
            raise
        except Exception as e:
            logger.error(f"ZMQTrader: Error in signal listen loop: {e}", exc_info=True)

    async def _on_signal(self, signal: SignalData) -> None:
        """Process incoming trading signal.

        Extracts exchange and mode from topic, creates engine if needed,
        and executes the signal through the appropriate engine.

        Args:
            signal: Validated signal envelope with instrument, side,
                strength, and price information.
        """
        context = self._build_signal_routing_context(signal)
        if context is None:
            return
        if self._should_drop_signal_for_foreign_shard(context.shard_key):
            return
        if self._should_drop_signal_for_unhealthy_executor(context):
            return
        halt_key = self._resolve_signal_halt_key(context)
        if self.trade_service.is_halted(halt_key):
            logger.warning(f"ZMQTrader: shard {halt_key} is halted, dropping signal")
            return
        if await self._grouped_signal_blocked_by_halt(signal):
            return
        assert (
            self.execution_publisher is not None
        ), "execution_publisher not initialized - _setup_external_execution must be called first"
        engine = await self._get_or_create_signal_engine(signal, context)
        self.last_signal_time[context.engine_key] = time.time()
        price = cast(float, signal.price)
        desired_units = signal.strength if signal.side == TradeSideEnum.BUY else -signal.strength
        strategy_name = signal.strategy_name or "unknown"
        logger.info(
            f"ZMQTrader: Processing signal from {strategy_name} - "
            f"{context.engine_key} {signal.side} "
            f"(strength={signal.strength:.2f}, price={price:.2f}, "
            f"desired_units={desired_units:.4f})"
        )
        signaled_at = signal.fired_at.timestamp()
        prev_oid = engine.pending_client_order_id
        group_public_id = await self._ensure_paired_execution_group(signal)
        command_public_id = await engine.execute_desired_units(
            desired_units,
            price,
            signaled_at=signaled_at,
            ai_review_public_id=signal.ai_review_public_id,
            ai_review_dispatch_version=signal.ai_review_dispatch_version,
            grouped_correlation_id=group_public_id,
        )
        new_oid = engine.pending_client_order_id
        if new_oid and new_oid != prev_oid:
            self._register_order_shard_key(new_oid, engine._shard_key)
            if group_public_id is not None and command_public_id is not None:
                await self._register_and_arm_paired_leg(
                    signal,
                    engine,
                    group_public_id=group_public_id,
                    command_public_id=command_public_id,
                    client_order_id=new_oid,
                )

    async def _grouped_signal_blocked_by_halt(self, signal: SignalData) -> bool:
        """Fast-reject a NEW grouped signal whose pair scope is durably halted.

        Only grouped signals (``paired_group_id`` set) on a SQL repository are
        checked: a durable ``paired_execution_halts`` row on the
        ``(wallet, strategy, group_key)`` scope means a prior group on this pair
        broke with exposure (compensation pending), so opening a new group would
        stack a second one-sided position. Identity is normalized exactly as
        :meth:`_ensure_paired_execution_group` stores it (wallet ``or ""``,
        strategy ``or "unknown"``) so the query matches the projected halt.
        Fails CLOSED: any DB error drops the grouped signal rather than risk a
        naked group. Non-grouped signals keep the cheap in-memory shard-halt gate
        and never reach the DB here; the no-SQL-repository test path is unguarded
        because the guard tables do not exist there.
        """
        group_id = signal.paired_group_id
        if group_id is None:
            return False
        group_key = signal.paired_group_key
        if group_key is None:
            return False
        if not isinstance(self.repository, SQLAlchemyRepository):
            return False
        wallet_public_id = signal.wallet_public_id or ""
        strategy_id = signal.strategy_name or "unknown"
        try:
            halt = await self.repository.get_active_paired_execution_halt(
                wallet_public_id, strategy_id, group_key
            )
        except Exception as exc:
            logger.warning(
                f"ZMQTrader: paired-execution halt check failed for group {group_id}, "
                f"dropping grouped signal (fail-closed): {exc}"
            )
            return True
        if halt is None:
            return False
        logger.warning(
            f"ZMQTrader: pair scope halted ({strategy_id}/{group_key}), "
            f"dropping grouped signal {group_id}"
        )
        return True

    async def _ensure_paired_execution_group(self, signal: SignalData) -> str | None:
        """Ensure a paired-execution group row exists for a grouped signal.

        Returns the group ``public_id`` (== ``signal.paired_group_id``) so the
        engine stamps it as the command ``correlation_id`` — held by the outbox
        arming gate until the group arms — or ``None`` for a standalone signal
        or a non-SQL repository (tests). ``ensure_paired_execution_group`` is
        idempotent, so sibling coordinators that each own a leg of the same
        group race safely on the active-unique ``public_id``: the first creates
        the assembling group, the rest no-op. The group must be committed
        BEFORE the leg's command is inserted so the gate classifies the command
        as grouped (held) rather than non-grouped (dispatchable).
        """
        group_id = signal.paired_group_id
        if group_id is None:
            return None
        if not isinstance(self.repository, SQLAlchemyRepository):
            return None
        size = signal.paired_group_size
        policy = signal.paired_group_policy
        group_key = signal.paired_group_key
        if size is None or policy is None or group_key is None:
            return None
        now = datetime.now(UTC)
        row: PairedExecutionGroupInsertRow = {
            "public_id": group_id,
            "wallet_public_id": signal.wallet_public_id or "",
            "operator_public_id": signal.operator_public_id or None,
            "strategy_id": signal.strategy_name or "unknown",
            "policy": policy,
            "expected_leg_count": size,
            "group_key": group_key,
            "status": PairedExecutionGroupStatusEnum.ASSEMBLING.value,
            "assembly_deadline": now
            + timedelta(seconds=_bootstrap_settings.paired_execution_assembly_timeout_s),
            "fill_deadline": now
            + timedelta(seconds=_bootstrap_settings.paired_execution_fill_timeout_s),
            "created_at": now,
            "session_id": self._tracker.session_id,
            "sequence_id": self._tracker.next_sequence(f"paired.group.{group_id}"),
            "timestamp": now,
        }
        await self.repository.ensure_paired_execution_group(row)
        return group_id

    async def _register_and_arm_paired_leg(
        self,
        signal: SignalData,
        engine: TradingEngineService,
        *,
        group_public_id: str,
        command_public_id: str,
        client_order_id: str,
    ) -> None:
        """Register this coordinator's leg, then arm the group when complete.

        Inserts the ``paired_execution_leg`` bound to the just-inserted command
        (so the outbox gate can match the leg to its command), then attempts the
        validated ``assembling -> armed`` CAS. On a successful arm the outbox is
        notified so the now-armed group's held commands dispatch together.
        Cross-coordinator, the last leg to register sees the full set and wins
        the arm; the others' CAS no-ops. If a sibling never registers, the group
        never arms and no command dispatches — safe (the guard scanner breaks
        the stalled group).
        """
        if not isinstance(self.repository, SQLAlchemyRepository):
            return
        leg_index = signal.paired_group_index
        if leg_index is None:
            return
        now = datetime.now(UTC)
        leg_row: PairedExecutionLegInsertRow = {
            "public_id": str(uuid7()),
            "group_public_id": group_public_id,
            "leg_index": leg_index,
            "exchange": engine.exchange,
            "mode": engine.mode,
            "instrument": signal.instrument,
            "shard_key": engine._shard_key,
            "side": signal.side,
            "target_qty": abs(signal.strength),
            "signal_public_id": signal.public_id,
            "command_public_id": command_public_id,
            "client_order_id": client_order_id,
            "status": PairedExecutionLegStatusEnum.PENDING.value,
            "wallet_public_id": signal.wallet_public_id or "",
            "operator_public_id": signal.operator_public_id or None,
            "created_at": now,
            "session_id": self._tracker.session_id,
            "sequence_id": self._tracker.next_sequence(f"paired.leg.{group_public_id}"),
            "timestamp": now,
        }
        await self.repository.insert_paired_execution_leg(leg_row)
        armed = await self.repository.try_arm_paired_execution_group_if_complete(
            group_public_id,
            now,
            self._tracker.session_id,
            self._tracker.next_sequence(f"paired.arm.{group_public_id}"),
        )
        if armed and self.outbox is not None:
            self.outbox.notify()
            logger.info(f"Paired-execution group armed, outbox notified: {group_public_id}")

    def _build_signal_routing_context(self, signal: SignalData) -> SignalRoutingContext | None:
        """Parse and validate the routing identity for one signal."""
        parsed = parse_signal_topic(self._current_topic)
        if parsed is None:
            logger.warning(f"ZMQTrader: Invalid signal topic format: {self._current_topic}")
            return None
        exchange = self._validate_signal_exchange(parsed.exchange)
        if exchange is None:
            return None
        if not self._validate_signal_payload(signal, exchange):
            return None
        wallet_public_id = signal.wallet_public_id or ""
        strategy_tag = parsed.signal_type if exchange == ExchangeEnum.PAPER else None
        execution_mode = (
            ExecutionModeEnum.PAPER if exchange == ExchangeEnum.PAPER else ExecutionModeEnum.LIVE
        )
        if (
            exchange == ExchangeEnum.PAPER
            and self._ownership is not None
            and self._ownership.instance_count > 1
        ):
            logger.error(
                "ZMQTrader: refusing paper signal for {} under N>1 partitioning "
                "(instance {}/{}). Paper shard keys embed the strategy tag, which the "
                "Order table cannot persist, so an in-flight paper order cannot be "
                "recovered after a crash and its fills would be silently lost. Run paper "
                "strategies on a single coordinator instance until durable paper shard "
                "identity lands.",
                signal.instrument,
                self._ownership.instance_id,
                self._ownership.instance_count,
            )
            return None
        return SignalRoutingContext(
            exchange=exchange,
            mode=parsed.signal_type,
            strategy_tag=strategy_tag,
            wallet_public_id=wallet_public_id,
            operator_public_id=signal.operator_public_id or "",
            engine_key=self._build_engine_key(
                signal.instrument,
                exchange,
                parsed.signal_type,
                wallet_public_id,
            ),
            shard_key=compute_shard_key(
                instrument=signal.instrument,
                exchange=exchange,
                mode=execution_mode,
                wallet_public_id=wallet_public_id,
                strategy_tag=strategy_tag,
            ),
        )

    def _validate_signal_exchange(self, exchange_str: str) -> OrderExchange | None:
        """Validate the exchange segment from the current signal topic."""
        if exchange_str in get_args(OrderExchange):
            return cast(OrderExchange, exchange_str)
        logger.warning(f"ZMQTrader: Unknown exchange '{exchange_str}' in topic")
        return None

    def _validate_signal_payload(self, signal: SignalData, exchange: OrderExchange) -> bool:
        """Validate tradeability and price before routing a signal."""
        instrument = signal.instrument
        if not is_tradeable(instrument, exchange):
            logger.warning(
                f"ZMQTrader: instrument {instrument} not tradeable on {exchange}, "
                f"dropping signal"
            )
            return False
        if not signal.price or signal.price <= 0:
            logger.warning(f"ZMQTrader: Invalid signal (missing or invalid price): {signal}")
            return False
        return True

    def _should_drop_signal_for_foreign_shard(self, shard_key: str) -> bool:
        """Return whether the current coordinator does not own the routed shard."""
        if self._ownership is None or self._ownership.owns(shard_key):
            return False
        logger.debug(
            "ZMQTrader: dropping signal for foreign shard {} (owner {}/{})",
            shard_key,
            self._ownership.instance_id,
            self._ownership.instance_count,
        )
        return True

    def _executor_health_scope(self, exchange: OrderExchange, wallet_short: str) -> str:
        """Return the stable executor venue-health scope key."""
        if wallet_short:
            return f"{exchange}:{wallet_short}"
        return f"{exchange}:legacy"

    def _signal_executor_health_scope(self, context: SignalRoutingContext) -> str:
        """Return the executor venue-health scope for a routed signal."""
        wallet_short = (
            compute_wallet_short(context.wallet_public_id) if context.wallet_public_id else ""
        )
        return self._executor_health_scope(context.exchange, wallet_short)

    def _should_drop_signal_for_unhealthy_executor(self, context: SignalRoutingContext) -> bool:
        """Return whether executor health has fail-closed this exchange wallet."""
        scope = self._signal_executor_health_scope(context)
        if scope not in self._unhealthy_executor_scopes:
            return False
        self.trade_service.halt_shard(context.shard_key, f"venue-health:{scope}")
        logger.warning(
            f"ZMQTrader: executor venue health halted {scope}, dropping signal "
            f"for shard {context.shard_key}"
        )
        return True

    def _handle_executor_heartbeat(self, topic: str, payload: bytes) -> None:
        """Apply executor venue-health heartbeat metadata to real shard halts."""
        parts = topic.split(".")
        if len(parts) not in (4, 5):
            logger.warning(f"ZMQTrader: invalid executor heartbeat topic {topic}")
            return
        exchange = self._validate_signal_exchange(parts[3])
        if exchange is None:
            return
        wallet_short = parts[4] if len(parts) == 5 else ""
        try:
            heartbeat = HeartbeatData.from_json(payload.decode())
        except Exception as exc:
            logger.warning(f"ZMQTrader: invalid executor heartbeat payload on {topic}: {exc}")
            return
        scope = self._executor_health_scope(exchange, wallet_short)
        if heartbeat.meta.get("venue_health_halt_recommended") is True:
            self._unhealthy_executor_scopes.add(scope)
            self._halt_known_executor_scope_shards(exchange, wallet_short, f"venue-health:{scope}")
            return
        if heartbeat.meta.get("venue_rest_reachable") is True:
            self._unhealthy_executor_scopes.discard(scope)

    def _halt_known_executor_scope_shards(
        self,
        exchange: OrderExchange,
        wallet_short: str,
        reason: str,
    ) -> None:
        """Halt known real shards matching an executor exchange wallet scope."""
        for shard_key in sorted(self.trade_service.known_shard_keys()):
            parsed = parse_shard_key(shard_key)
            if parsed is None:
                continue
            shard_exchange, _instrument, _mode, shard_wallet_short, _strategy_tag = parsed
            if shard_exchange == exchange and shard_wallet_short == wallet_short:
                self.trade_service.halt_shard(shard_key, reason)

    def _resolve_signal_halt_key(self, context: SignalRoutingContext) -> str:
        """Resolve the shard key used by the halt guard."""
        if context.engine_key in self.engines:
            return self.engines[context.engine_key]._shard_key
        return context.shard_key

    async def _get_or_create_signal_engine(
        self,
        signal: SignalData,
        context: SignalRoutingContext,
    ) -> TradingEngineService:
        """Fetch the routed engine or create it with the current settings."""
        existing = self.engines.get(context.engine_key)
        if existing is not None:
            return existing
        logger.info(f"ZMQTrader: Creating new engine for {context.engine_key}")
        await self._ensure_instrument(signal.instrument, exchange=context.exchange)
        risk = RiskEvaluator(
            RiskConfigModel(
                r_per_trade=self.settings.risk_r_per_trade,
                max_leverage=self.settings.risk_max_leverage,
                max_drawdown=self.settings.risk_max_drawdown,
            )
        )
        specs_map = await self._resolve_instrument_specs(signal.instrument, context.exchange)
        assert self.msg_publisher is not None, "MessagePublisher not initialized"
        repo_for_engine = (
            self.repository if isinstance(self.repository, SQLAlchemyRepository) else None
        )
        engine = TradingEngineService(
            signal.instrument,
            execution_socket=self.msg_publisher,
            risk=risk,
            cfg=EngineConfigModel(),
            instrument_specs=specs_map,
            exchange=context.exchange,
            repository=repo_for_engine,
            outbox=self.outbox,
            strategy_tag=context.strategy_tag,
            wallet_public_id=context.wallet_public_id,
            operator_public_id=context.operator_public_id,
            ownership=self._ownership,
            caps_enforcer=self._caps_enforcer,
        )
        self.engines[context.engine_key] = engine
        self.last_signal_time[context.engine_key] = 0.0
        return engine

    async def _signal_health_monitor(self) -> None:
        """Monitor signal health and warn on signal gaps.

        Periodically checks when last signal was received for each engine
        and logs warnings if no signals received within timeout.
        """
        signal_timeout = 60.0
        while True:
            await asyncio.sleep(5.0)
            current_time = time.time()
            for engine_key in self.engines:
                last_signal = self.last_signal_time.get(engine_key, 0)
                if current_time - last_signal > signal_timeout:
                    logger.debug(f"ZMQTrader: No signals for {engine_key} in {signal_timeout}s")


async def run_zmq_trader(
    signal_topics: list[str] | None = None,
) -> None:
    """Run the ZMQ trader coordinator.

    Convenience function to create and run a TraderCoordinator.

    Args:
        signal_topics: List of ZMQ topic prefixes to subscribe to.
    """
    trader = TraderCoordinator(signal_topics=signal_topics)
    try:
        await trader.start()
    except KeyboardInterrupt:
        logger.info("TraderCoordinator stopped by user")
    finally:
        await trader.stop()
