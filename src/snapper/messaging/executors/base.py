"""Base class for order execution services.

Provides common functionality for ZeroMQ-based execution services
that handle order placement and fill reporting.
"""

import asyncio
import json
import math
import random
import time
from abc import ABC
from abc import abstractmethod
from collections import OrderedDict
from collections.abc import AsyncGenerator
from collections.abc import Awaitable
from collections.abc import Callable
from dataclasses import dataclass
from dataclasses import field
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from typing import Any
from typing import Literal
from typing import cast
from uuid import uuid7

import httpx
import zmq
import zmq.asyncio
from loguru import logger

from snapper.application.engine.service import compute_shard_key
from snapper.application.portfolio.account_view import build_portfolio_account_state
from snapper.application.portfolio.reconciliation_dispatch import dispatch_portfolio_reconciliation
from snapper.application.process_manager.models import RegisterableProcess
from snapper.application.services.settings import SettingsService
from snapper.application.trade.command_request import order_request_from_command
from snapper.application.trade.trade_service import TradeService
from snapper.config.credentials import CredentialResolver
from snapper.config.settings import AppSettings
from snapper.config.settings import get_settings
from snapper.config.settings import get_settings_service
from snapper.config.settings import get_settings_with_service
from snapper.core.json_types import JsonValue
from snapper.core.types import ORDER_STATUS_REASON_ADOPTED
from snapper.core.types import CancelEventType
from snapper.core.types import ExchangeEnum
from snapper.core.types import ExecutionModeEnum
from snapper.core.types import FillStatus
from snapper.core.types import FillStatusEnum
from snapper.core.types import HealthStatusEnum
from snapper.core.types import OrderCommandEnum
from snapper.core.types import OrderEventEnum
from snapper.core.types import OrderEventType
from snapper.core.types import OrderExchange
from snapper.core.types import ReplaceEventType
from snapper.core.types import StreamTerminalEventType
from snapper.core.types import TradeCommandStatusEnum
from snapper.core.types import TradeSideEnum
from snapper.core.wallet_short import compute_wallet_short
from snapper.data.repository import Repository
from snapper.data.repository import SQLAlchemyRepository
from snapper.data.repository import get_repository
from snapper.data.repository_types import ExecutionRow
from snapper.data.repository_types import OrderRow
from snapper.data.repository_types import PortfolioReconciliationEvaluationRow
from snapper.data.repository_types import RecordVenueEventParams
from snapper.data.repository_types import TradeCommandRow
from snapper.data.repository_types import VenueAccountAttemptRow
from snapper.data.repository_types import VenueEventRow
from snapper.infrastructure.exchanges.base import ExchangeClientBase
from snapper.infrastructure.exchanges.contracts import CORE_TO_EXCHANGE_ORDER_TYPE
from snapper.infrastructure.exchanges.contracts import CapabilityStatus
from snapper.infrastructure.exchanges.contracts import ExchangeOrderRequest
from snapper.infrastructure.exchanges.contracts import ExchangeOrderSnapshot
from snapper.infrastructure.exchanges.contracts import ExchangeOrderStatusEnum
from snapper.infrastructure.exchanges.contracts import ExchangeOrderTypeEnum
from snapper.infrastructure.exchanges.contracts import ExecType
from snapper.infrastructure.exchanges.contracts import ExecutionFeeBreakdown
from snapper.infrastructure.exchanges.contracts import ExecutionUpdate
from snapper.infrastructure.exchanges.contracts import NativeBalanceEntry
from snapper.infrastructure.exchanges.contracts import OpenPositionSnapshot
from snapper.infrastructure.exchanges.contracts import OrderFillSummary
from snapper.infrastructure.exchanges.contracts import OrderSideEnum
from snapper.infrastructure.exchanges.contracts import to_fill_status
from snapper.infrastructure.exchanges.errors import AmbiguousOrderSubmitError
from snapper.infrastructure.exchanges.errors import CircuitBreakerOpenError
from snapper.infrastructure.network.egress_context import egress_identity
from snapper.infrastructure.symbols.functions import is_tradeable
from snapper.infrastructure.symbols.mapper import SymbolMapperService
from snapper.messaging.infrastructure.gap_detector import GapDetector
from snapper.messaging.infrastructure.publisher import MessagePublisher
from snapper.messaging.infrastructure.publisher import SequenceTracker
from snapper.messaging.infrastructure.validated_socket import HWM_ORDER_FLOW
from snapper.messaging.infrastructure.validated_socket import ValidatedPublisher
from snapper.messaging.infrastructure.validated_socket import ValidatedSubscriber
from snapper.messaging.infrastructure.validated_socket import apply_hwm
from snapper.messaging.schemas.data import ExecutionData
from snapper.messaging.schemas.data import HeartbeatData
from snapper.messaging.schemas.data import OrderCancelData
from snapper.messaging.schemas.data import OrderData
from snapper.messaging.schemas.data import OrderEventData
from snapper.messaging.schemas.data import OrderReplaceData
from snapper.messaging.schemas.data import OrderRequestData
from snapper.messaging.schemas.data import PortfolioDriftEpisodeEventData
from snapper.messaging.schemas.data import SettingChangedData
from snapper.messaging.schemas.data import SymbolAliasUpdateData
from snapper.messaging.schemas.messages import MessageParseError
from snapper.messaging.schemas.messages import parse_message
from snapper.messaging.topics.builders import heartbeat_topic
from snapper.messaging.topics.builders import order_commands_prefix
from snapper.messaging.topics.builders import order_event_topic
from snapper.messaging.topics.builders import parse_order_command_topic
from snapper.utils.logging import set_log_context

_EXCHANGE_NOT_INIT_MSG = "Exchange client not initialized"

_PORTFOLIO_DRIFT_EPISODE_TOPIC = "bus.portfolio_drift_episode"
"""Internal notify-only topic for committed drift lifecycle transitions."""

_AMBIGUOUS_VERIFY_TIMEOUT_S = 15.0
"""Bound on a single venue lookup during ambiguous-submit verification.

The lookup runs while the engine's in-flight guard is held; an
unbounded venue call here would silently extend the UNKNOWN window. A
timed-out attempt counts as could-not-verify, never as absence.
"""

_EXEC_STREAM_BACKOFF_INITIAL_S = 1.0
"""Supervisor backoff after the first execution-stream death."""

_EXEC_STREAM_BACKOFF_CAP_S = 60.0
"""Backoff ceiling between execution-stream reconnect attempts.

Kept at the recon-loop period: even while the stream is down, every
fill is at most one recon cycle behind, so retrying the (cheap)
resubscribe more than once a minute buys nothing during a long outage.
"""

_EXEC_STREAM_JITTER_FRACTION = 0.2
"""Relative jitter applied to each supervisor backoff sleep.

Decorrelates reconnect attempts across executor instances after a
shared outage so the venue does not see synchronized resubscribe
bursts on recovery.
"""

_EXEC_STREAM_HEALTHY_RUNTIME_S = 300.0
"""Stream runtime above which the next death restarts backoff from initial.

Without the reset, a stream that lived for hours would inherit the
60s ceiling from an incident long resolved and sit dark a full minute
on its next blip.
"""

_HB_STREAK_ERROR_S = 600.0
"""Active death streak age that flips the heartbeat to ERROR.

Pages ~15 minutes before the 1500s ExecutorTaskDeadError ceiling would
crash the service anyway — the operator hears about persistent dying
while in-process respawn is still trying."""

_HB_RECON_WARN_S = 300.0
"""Reconciliation progress age that flips the heartbeat to WARNING.

Recon swallows its per-cycle failures by design (durable-state poison
must never crash-loop the supervisor), so it can fail forever WITHOUT
dying — death signals cannot see it. The progress clock can: ~4 failed
60s cycles or one wedged cycle."""

_HB_RECON_ERROR_S = 900.0
"""Reconciliation progress age that flips the heartbeat to ERROR.

Three full cycle budgets without one successful pass: the heal engine
(gap correctives, parked-UNKNOWN verification, accept-event retries) is
effectively down."""

_HB_INFLIGHT_WARN_S = 120.0
"""Order-command in-flight age that flips the heartbeat to WARNING.

A command wedged INSIDE its venue call never dies (the supervisor sees
no termination) and never stamps a progress clock — the only visible
signal is how long the current command has been in flight. Venue calls
are individually bounded well below this, so a two-minute in-flight age
means something upstream of those bounds is stuck."""

_HB_INFLIGHT_ERROR_S = 600.0
"""Order-command in-flight age that flips the heartbeat to ERROR."""

_HB_DEATHS_WARN = 2
"""Deaths within ONE active streak that flip the heartbeat to WARNING.

A single death that respawns clean stays HEALTHY — that is the P1-3
self-heal working as designed, not a page. Dying twice in the same
streak is a condition the operator should see building."""

_VENUE_RECON_FAILURE_HALT_THRESHOLD = 3
"""Consecutive reconciliation failures that recommend venue-scope halting."""

_AMBIGUOUS_VERIFY_PER_CYCLE_MAX = 3
"""Fairness cap on parked-ambiguous verifications per recon cycle.

One verification can legitimately spend ~62s (retry sleeps plus three
bounded venue lookups), so an uncapped pass over many parked entries
would exceed the cycle timeout at the SAME prefix every cycle and starve
the tail forever. Three worst-case verifications (~186s) leave the 300s
cycle budget room for the rest of the pass; the index-based rotation
(``_ambiguous_rotation_offset``) guarantees forward progress through the
parked set regardless of entries getting popped mid-rotation — a
popped-identity resume pointer would silently fall back to prefix order
and re-starve the tail."""

_GHOST_ADOPT_PER_CYCLE_MAX = 5
"""Cap on ghost-order adoptions per recon cycle.

Each adoption finalizes acceptance (durable write + publish) and may
route a terminal snapshot through the disappeared-order reconciler;
five per cycle bounds the added cycle time while the set shrinks as
adoptions land — no rotation needed."""

_DISPATCHED_VERIFY_PER_CYCLE_MAX = 3
"""Fairness cap on dispatched-command venue verifications per cycle.

Same budget rationale as ``_AMBIGUOUS_VERIFY_PER_CYCLE_MAX``: each
verification is one bounded venue lookup, and index rotation
(``_dispatched_rotation_offset``) guarantees tail progress."""

_DISPATCHED_VERIFY_MIN_AGE_S = 120.0
"""Minimum command age before the dispatched-verification sweep acts.

Two recon intervals: gives the lifecycle fold and the ghost-adoption
sweep a chance to resolve the command from durable evidence or the
open-orders snapshot first, and comfortably exceeds the dispatch TTL
so no in-flight frame is still legitimately pending."""

_ABSENCE_REJECT_MAX_AGE_S = 3600.0
"""Command age beyond which venue absence stops being authoritative.

Venue closed-order endpoints have bounded lookback, so an old order
can be reported absent while it actually existed (and filled). Older
commands get WARN-only escalation, never an auto-REJECT."""

_GHOST_FOREIGN_WARNED_MAX = 512
"""LRU bound on the warned-foreign-order cid set (process-memory cap)."""

_GAP_FEE_DEFERRAL_MAX = 5
"""Recon cycles a fill-gap corrective may defer on a missing fee source.

Transient failures (transport, fills-page lag) heal within a cycle or
two; a fills page whose entries AGED OUT never heals — an unbounded
deferral would hold the terminal projection and the engine's intent
forever. Past the cap the corrective emits fee-less with a CRITICAL
manual-reconcile signal."""

_GapResult = Literal["emitted", "no_gap", "deferred", "skipped"]
"""Outcome of one fill-gap reconciliation attempt.

``deferred`` means a TRANSIENT fee-source failure blocked an emission
whose stable exec id would otherwise freeze fee-less — terminal paths
must NOT pop the pending entry on it, or the retry never happens.
``skipped`` is the documented permanent no-price skip (terminal still
emits); ``emitted`` is asserted from the COMMITTED watermark (advanced
only on publish success), so a swallowed corrective publish failure
also reports ``deferred``; ``no_gap`` is the ordinary clean outcome."""

_RecoveryWatermarks = tuple[float, float, list[VenueEventRow], dict[str, float]]
_PortfolioReconciliationKey = tuple[str, str, str, str, int]


@dataclass(frozen=True)
class _PortfolioReconciliationWork:
    """Immutable identity and capability captured from one account snapshot."""

    state_id: int
    identity: _PortfolioReconciliationKey
    position_capability: CapabilityStatus


@dataclass(frozen=True)
class _FillSummaryResolution:
    """Resolved fill-summary state for a corrective fill decision."""

    fill_price: float | None
    summary: OrderFillSummary | None
    summary_unusable: bool


@dataclass(frozen=True)
class _FillAccounting:
    """Durable and cumulative-fee accounting derived from one fill frame."""

    is_fill_frame: bool
    durable_size: float
    durable_fee: float
    frame_cum_fee: float | None
    frame_cum_fee_asset: str


_COMMAND_TERMINAL_STATUSES = frozenset(
    {
        TradeCommandStatusEnum.FILLED.value,
        TradeCommandStatusEnum.CANCELLED.value,
        TradeCommandStatusEnum.EXPIRED.value,
        TradeCommandStatusEnum.REJECTED.value,
        TradeCommandStatusEnum.FAILED.value,
    }
)

_LIVE_TRADING_MODE_KEY = "live_trading_mode"
_LIVE_TRADING_HALTED = "halted"
_LIVE_TRADING_REDUCE_ONLY = "reduce_only"
_LIVE_TRADING_ENABLED = "enabled"
_LIVE_TRADING_UNAVAILABLE = "__unavailable__"
"""Sentinel returned when the mode cannot be read authoritatively.

Distinct from a genuine ``halted`` value so the interlock can surface
``live_trading_mode_unavailable`` (an infrastructure incident: no
settings service, a timed-out or errored read, a missing row, or an
unrecognized value) separately from a deliberate operator ``halted``.
Both block — the sentinel is never a permitted mode — but the reason
tells the two apart. Never stored; not one of the three valid modes.
"""
_LIVE_TRADING_MODE_READ_TIMEOUT_S = 2.0
"""Bound on the per-submit fresh read of ``live_trading_mode``.

The interlock reads the setting straight from the database on every
non-paper submit (the ZMQ-refreshed cache is best-effort — a lost
``system.settings`` broadcast would leave a kill-switch stale). A
wedged database must not starve the serialized per-wallet handler and
its queued cancels, so the read is time-boxed and a timeout fails
closed to blocked.
"""
_INTERLOCK_REASON_HALTED = "live_trading_halted"
_INTERLOCK_REASON_REDUCE_ONLY = "live_trading_reduce_only_unavailable"
_INTERLOCK_REASON_MODE_UNAVAILABLE = "live_trading_mode_unavailable"
_INTERLOCK_REASON_BY_MODE = {
    _LIVE_TRADING_HALTED: _INTERLOCK_REASON_HALTED,
    _LIVE_TRADING_REDUCE_ONLY: _INTERLOCK_REASON_REDUCE_ONLY,
    _LIVE_TRADING_UNAVAILABLE: _INTERLOCK_REASON_MODE_UNAVAILABLE,
}
_INTERLOCK_BLOCKED_EVENT_TYPE = "order_interlock_blocked"

_RECON_CYCLE_TIMEOUT_S = 300.0
"""Bound on one full reconciliation cycle INCLUDING lock acquisition.

A hung venue call inside the cycle used to hold ``_recon_lock`` forever,
wedging both the 60s periodic loop and the stream supervisor's
post-reconnect heal. Five times the cycle period: a legitimate cycle is
bounded far lower by the venue clients' own timeouts, and a cancelled
cycle is safely recomputed next period (correctives carry stable
synthetic ids)."""

_ACCOUNT_OBSERVE_INTERVAL_S = 240.0
"""Cadence of the venue account-truth observer (PnL Phase 3).

Account state changes slowly; ~4 min keeps every state comfortably inside its
``_ACCOUNT_FRESHNESS_CEILING_S`` window while staying well under Kraken's REST
pressure. Independent of the 60s order-reconciliation cycle."""
_ACCOUNT_FRESHNESS_CEILING_S = 300.0
"""Authority window stamped on a freshly observed account balance. Past this
the read layer demotes an ``observed`` row to ``stale`` rather than serving it
as live truth."""
_ACCOUNT_FETCH_TIMEOUT_S = 15.0
"""Per-call bound on a native balance/position read. A wedged venue call is
recorded as an ``error`` observation (last-good retained, stale-visible) and
never blocks the observer loop or the order-reconciliation cycle."""
_PORTFOLIO_RECONCILIATION_TIMEOUT_S = 60.0
"""Bound on the complete observer-side portfolio reconciliation branch."""
_ACCOUNT_UNEXPECTED_BALANCE_CAPABILITY_MSG = (
    "balance reader returned data under a non-observable capability"
)
_ACCOUNT_UNEXPECTED_POSITION_CAPABILITY_MSG = (
    "position reader returned data under a non-observable capability"
)

_TASK_DEATH_ESCALATION_CEILING_S = 1500.0
"""Death-streak ceiling after which a supervised loop escalates.

When a task keeps dying for this long without a healthy run, in-process
respawn has proven insufficient and the supervisor raises
``ExecutorTaskDeadError`` out of ``start()`` so the launcher rebuilds a
FRESH service instance (safe since recovery-time corrective fills).
INVARIANT: strictly greater than the launcher's ``_TOTAL_RESET_UPTIME_S``
(1200s) — a ceiling-driven death therefore always presents as a
long-healthy run to the launcher, resetting its restart budget, so this
path yields an unbounded slow restart-and-retry cadence that self-heals
when the cause clears, never a permanent park (publisher dark-feed
precedent)."""

_ADOPTED_REARM_REASON = ORDER_STATUS_REASON_ADOPTED
"""Local alias for the shared #155 re-arm reason (see core.types)."""

_SEEN_EXEC_IDS_MAX = 10_000
"""Bound of the executor-level seen-exec-id LRU (mirrors the engine's
apply_fill LRU). At one fill per second this covers ~3 hours of
lookback — far beyond any venue replay window — while capping memory.
"""


_STOP_TYPED_EXCHANGE_ORDER_TYPES = frozenset(
    {ExchangeOrderTypeEnum.STOP_LOSS, ExchangeOrderTypeEnum.STOP_LOSS_LIMIT}
)
"""Wire order types that REQUIRE a trigger price on submit (#156)."""


def _exchange_order_request_from_core(
    order: OrderRequestData, wallet_public_id: str
) -> ExchangeOrderRequest:
    """Translate a CORE-vocabulary order request into the venue wire contract.

    The durable plane (trade_commands, ``OrderRequestData``) speaks CORE
    (``market``/``limit``/``stop``/``stop_limit``); venue SDKs speak
    ``ExchangeOrderTypeEnum`` wire values (``stop-loss``/...). The bare
    ``ExchangeOrderTypeEnum(value)`` cast this replaces only worked
    because the two vocabularies coincide on market/limit — for stop
    types it raised an opaque ValueError (#156). Raising HERE, before
    any network send, is provably-not-placed, so the caller's generic
    definitive-reject branch (publish REJECTED + durable
    ``order_rejected`` event) is the correct disposition.

    Raises:
        ValueError: When the core order type has no wire mapping, or a
            stop-typed order carries no ``stop_price`` (redispatched
            legacy frames must not reach the venue half-formed).
    """
    wire_type = CORE_TO_EXCHANGE_ORDER_TYPE.get(order.order_type)
    if wire_type is None:
        raise ValueError(
            f"order {order.client_order_id}: core order type {order.order_type!r} "
            f"has no exchange wire mapping — vocabulary error, definitive reject"
        )
    if wire_type in _STOP_TYPED_EXCHANGE_ORDER_TYPES and order.stop_price is None:
        raise ValueError(
            f"order {order.client_order_id}: stop-typed order ({order.order_type}) "
            f"without stop_price — refusing half-formed venue submit, definitive reject"
        )
    return ExchangeOrderRequest(
        symbol=order.instrument,
        side=OrderSideEnum(order.side),
        type=wire_type,
        amount=float(order.quantity),
        price=float(order.price) if order.price else None,
        stop_price=float(order.stop_price) if order.stop_price else None,
        client_order_id=order.client_order_id,
        signaled_at=order.signaled_at,
        leverage=order.leverage,
        reduce_only=order.reduce_only,
        wallet_public_id=wallet_public_id,
        operator_public_id=order.operator_public_id,
    )


class ExecutorTaskDeadError(RuntimeError):
    """A supervised executor loop kept dying past the escalation ceiling.

    Raised out of the loop supervisor so it propagates through
    ``start()``'s gather: siblings get cancelled, the exchange client
    disconnects, the crash-path ``stop()`` closes ZMQ, and the launcher's
    task-completion handler resolves FAILED and schedules a fresh-instance
    restart through the watchdog.
    """


@dataclass
class PendingOrderState:
    """Per-order executor state for tracking orders through their lifecycle.

    Consolidates request data, DB identifiers, and cumulative fill tracking
    into a single model. Used by the executor base for persistence and
    delta fill computation.

    Attributes:
        request: Original order request data.
        db_order_id: Database row ID from _log_order_to_db (for status updates).
        order_public_id: Logical order identity (for execution inserts).
        exchange_order_id: Exchange-assigned order ID (set after ACK).
        last_seen_cum_qty: COMMITTED cumulative — what has actually been
            published to the engine; advanced only on publish success.
        last_recorded_cum_qty: DURABLE cumulative — the highest
            cum_fill_size successfully written to venue_events for this
            order; advanced on record success (even when the subsequent
            publish fails). The durable fill_size of each new row is the
            gap from THIS watermark, so additive checkpoint replay always
            sums to venue truth regardless of which prior step failed.
        submit_ambiguous: True when the submit failed ambiguously (the
            venue MAY have the order); the entry is parked pending
            venue verification instead of being rejected.
        unknown_published: True once the single UNKNOWN order event has
            been published for this entry (guards duplicate publishes
            across recon touches).
        accept_event_pending: True when the order was accepted by the
            venue but the durable order_accepted venue event failed to
            persist; the recon loop retries the write until it sticks.
        last_recorded_fee: DURABLE fee watermark PER CURRENCY: the fee
            attributed into venue_events rows so far, keyed by fee
            asset (scalar watermarks would subtract USD-equivalent
            per-fill fees from an EUR cumulative commission). Each
            cumulative-frame row's fee is the SIGNED delta from this
            currency's entry (negative = maker rebate; a nonnegative
            clamp would silently drop rebates); advanced with the
            durable write like ``last_recorded_cum_qty``. Attaching the
            full snapshot commission to every poll delta used to
            multi-charge partial fills (#145 P2-5).
        last_published_fee: PUBLISHED cumulative fee watermark — what
            the engine has actually been charged; advanced only on
            publish success, mirroring ``last_seen_cum_qty``. The
            published fee delta anchors HERE: when a publish fails
            after the durable write, the next frame's published
            quantity absorbs the unpublished span (cum-anchored), so
            its fee must absorb the unpublished fee slice too — a
            durable-anchored published fee would silently drop it.
            BOTH fee watermarks track per-currency
            fee-attributed-so-far across BOTH attribution channels:
            cumulative frames (``cum_fee``) SET the currency's entry to
            the venue cumulative (signed — rebates can shrink it; stale
            replays are dropped by the exec-id/cum guards, not by a
            clamp) and per-fill frames ADD onto their asset's entry — a
            cumulative corrective landing after per-fill live fees must
            charge only the same-currency remainder, never re-charge
            what the live frames already attributed.
        breaker_open_pending: True when a breaker-open submit's durable
            disposition (order_breaker_open event + command FAILED CAS +
            REJECTED publish) could not complete; the recon loop reruns
            the sequence until it sticks — intent must never release
            before the durable terminal (#145 P2-5 §2d).
        adopted_accept_publish_pending: True when an adoption-shaped
            ACCEPTED publish (``reason="adopted"``, the running
            engine's only re-arm signal, #155) failed; the recon loop
            retries the publish and clears the flag on success —
            without the clear, periodic republish would keep
            refreshing the engine's in-flight window and starve the
            timeout valve.
        fill_lock: Serializes fill booking for this order across the live
            stream task and the recon task — dedupe gate, delta build,
            durable write, publish, and committed-cumulative advance form
            one atomic section (see ``_book_correlated_fill``). Lifecycle
            is tied to the entry itself, so the lock vanishes with the
            order and cannot leak.
    """

    request: OrderRequestData
    db_order_id: int | None = field(default=None)
    order_public_id: str | None = field(default=None)
    exchange_order_id: str | None = field(default=None)
    last_seen_cum_qty: float = field(default=0.0)
    last_recorded_cum_qty: float = field(default=0.0)
    last_recorded_fee: dict[str, float] = field(default_factory=dict)
    last_published_fee: dict[str, float] = field(default_factory=dict)
    submit_ambiguous: bool = field(default=False)
    unknown_published: bool = field(default=False)
    accept_event_pending: bool = field(default=False)
    breaker_open_pending: bool = field(default=False)
    interlock_blocked_pending: bool = field(default=False)
    interlock_blocked_reason: str = field(default=_INTERLOCK_REASON_MODE_UNAVAILABLE)
    adopted_accept_publish_pending: bool = field(default=False)
    fill_lock: asyncio.Lock = field(default_factory=asyncio.Lock)


class ExchangeExecutorService[T: ExchangeClientBase](RegisterableProcess, ABC):
    """Base service for executing orders on exchanges via ZMQ messaging."""

    @staticmethod
    def get_default_parameters(settings: AppSettings) -> dict[str, Any]:
        """Return default parameters for the executor service.

        Args:
            settings: Application settings instance.

        Returns:
            Dictionary of default parameters for this executor.
        """
        return {"wallet_public_id": ""}

    def __init__(self, wallet_public_id: str = "") -> None:
        """Initialize the instance.

        Args:
            wallet_public_id: Multi-tenant routing key. When
                non-empty, the executor loads credentials from
                ``wallet_credentials`` via ``CredentialResolver`` at
                startup and drops incoming command messages whose
                ``wallet_public_id`` does not match. When empty (the
                legacy default preserved for tests that instantiate
                concrete executors directly), ``_resolve_credentials``
                is a no-op and ``self._credentials`` stays ``None``;
                those tests must inject ``self._credentials`` directly
                before calling ``start()``, otherwise the concrete
                ``_create_exchange_client`` raises ``RuntimeError``.
        """
        self.settings = get_settings()
        self.wallet_public_id: str = wallet_public_id
        self._credentials: dict[str, str] | None = None
        self.context: zmq.asyncio.Context | None = None
        self.subscriber: ValidatedSubscriber | None = None
        self.publisher: ValidatedPublisher | None = None
        self.msg_publisher: MessagePublisher | None = None
        self._tracker: SequenceTracker = SequenceTracker()
        self._gap_detector: GapDetector = GapDetector()
        self.running = False
        self.heartbeat_seq = 0
        self.exchange_client: T | None = None
        self._client_context_active: bool = False
        self.repository: Repository | None = None
        self._settings_service: SettingsService | None = None
        self.pending_orders: dict[str, PendingOrderState] = {}
        self.client_by_exchange: dict[str, str] = {}
        self.orphaned_executions: dict[str, tuple[ExecutionUpdate, float]] = {}
        self.orphan_ttl_seconds: float = 5.0
        self.orphan_drop_count: int = 0
        self._unhealed_accept_events: dict[str, RecordVenueEventParams] = {}
        self._seen_exec_ids: OrderedDict[str, None] = OrderedDict()
        self._exec_stream_restarts: int = 0
        self._task_restarts: dict[str, int] = {}
        self._task_last_pass: dict[str, float] = {}
        self._task_last_death: dict[str, float] = {}
        self._task_streak_started: dict[str, float] = {}
        self._task_deaths_in_streak: dict[str, int] = {}
        self._venue_recon_failure_count = 0
        self._last_venue_recon_error = ""
        self._account_observer_failure_count = 0
        self._order_inflight_started: float | None = None
        self._ambiguous_rotation_offset: int = 0
        self._dispatched_rotation_offset: int = 0
        self._dispatched_absence_counts: dict[str, int] = {}
        self._ghost_foreign_warned: OrderedDict[str, None] = OrderedDict()
        self._pending_rejected_restores: dict[str, TradeCommandRow] = {}
        self._gap_fee_deferrals: dict[str, int] = {}
        self._verify_unsupported_logged: bool = False
        self._recon_lock = asyncio.Lock()
        self._background_tasks: set[asyncio.Task[None]] = set()
        self._portfolio_reconciliation_tasks: dict[
            _PortfolioReconciliationKey, asyncio.Task[None]
        ] = {}
        self._portfolio_drift_notification_tasks: set[asyncio.Task[None]] = set()
        self._portfolio_reconciliation_dispatch_open = False
        self._portfolio_reconciliation_failure_count = 0
        self._last_portfolio_reconciliation_error = ""

    def _require_context(self) -> zmq.asyncio.Context:
        """Return initialized ZMQ context or raise an explicit runtime error.

        Returns:
            Initialized ZMQ context.

        Raises:
            RuntimeError: If socket setup has not initialized the context.
        """
        context = self.context
        if context is None:
            raise RuntimeError("ZMQ context must exist before subscriber setup")
        return context

    def _require_exchange_client(self) -> T:
        """Return initialized exchange client or raise an explicit runtime error.

        Returns:
            Initialized exchange client.

        Raises:
            RuntimeError: If exchange-client setup has not completed.
        """
        exchange_client = self.exchange_client
        if exchange_client is None:
            raise RuntimeError(_EXCHANGE_NOT_INIT_MSG)
        return exchange_client

    def _require_repository(self) -> Repository:
        """Return initialized repository or raise an explicit runtime error.

        Returns:
            Initialized repository.

        Raises:
            RuntimeError: If repository setup has not completed.
        """
        repository = self.repository
        if repository is None:
            raise RuntimeError("Repository not initialized")
        return repository

    def _require_sqlalchemy_repository(self) -> SQLAlchemyRepository:
        """Return initialized SQLAlchemy repository or raise explicit type error.

        Returns:
            Initialized SQLAlchemy repository.

        Raises:
            RuntimeError: If repository setup has not completed.
            TypeError: If the configured repository is not SQLAlchemy-backed.
        """
        repository = self._require_repository()
        if not isinstance(repository, SQLAlchemyRepository):
            raise TypeError("SQLAlchemyRepository required")
        return repository

    @abstractmethod
    def _create_exchange_client(self) -> T:
        """Create and return the exchange client instance.

        Returns:
            Exchange client instance for this executor.
        """
        ...

    @abstractmethod
    def _get_exchange_name(self) -> OrderExchange:
        """Return the trading exchange identifier.

        Returns:
            Trading exchange enum value for this executor.
        """
        ...

    async def _initialize_settings(self) -> None:
        """Initialize settings service with database access."""
        self.repository = get_repository(self.settings.db_url)
        settings_service = await get_settings_service(
            self.settings.db_url,
            self.settings.zmq_broker_xsub,
        )
        self.settings = get_settings_with_service(settings_service)
        self._settings_service = settings_service
        logger.info("AppSettings service initialized with database access")

    async def _resolve_credentials(self, exchange_name: OrderExchange) -> None:
        """Load wallet-scoped credentials when wallet_public_id is populated.

        Per-wallet executor instances resolve their credentials from
        ``wallet_credentials`` via ``CredentialResolver`` exactly once
        during startup. When ``self.wallet_public_id`` is empty
        (legacy template path used by tests that instantiate concrete
        executors directly), no lookup happens and ``self._credentials``
        stays ``None``; those tests must inject ``self._credentials``
        directly before ``start()`` or the concrete
        ``_create_exchange_client`` will raise ``RuntimeError``.

        Args:
            exchange_name: Exchange identifier used as the credential
                lookup key (normalized to lowercase by the resolver).

        Raises:
            RuntimeError: When ``self.wallet_public_id`` is set but the
                repository has not yet been initialized.
            CredentialNotFoundError: When no active credential row
                exists for the wallet/exchange pair. Propagated so the
                executor process fails fast at startup.
        """
        if not self.wallet_public_id:
            return
        if self.repository is None:
            raise RuntimeError(
                "ExchangeExecutorService._resolve_credentials requires "
                "repository initialization before credential lookup."
            )
        resolver = CredentialResolver(self.repository)
        self._credentials = await resolver.get_credentials(
            exchange=exchange_name,
            wallet_public_id=self.wallet_public_id,
        )
        logger.info(
            f"ExchangeExecutorService[{exchange_name}]: Resolved credentials "
            f"for wallet={self.wallet_public_id}"
        )

    def _is_for_my_wallet(self, msg: Any) -> bool:
        """Filter guard: is this command targeted at my wallet instance?

        Per-wallet executor instances all subscribe to the same
        exchange-prefix topic. Each instance drops commands whose
        ``wallet_public_id`` does not match its own. When
        ``self.wallet_public_id`` is empty (legacy template path),
        every message is accepted — this branch only fires for
        single-wallet template instances and tests that instantiate
        executors directly without populating a wallet.

        Args:
            msg: Parsed command message (OrderRequestData,
                OrderCancelData, or OrderReplaceData). The filter
                reads the optional ``wallet_public_id`` attribute.

        Returns:
            True if the message should be processed by this executor
            instance, False if it must be silently dropped.
        """
        if not self.wallet_public_id:
            return True
        msg_wallet = getattr(msg, "wallet_public_id", "") or ""
        return msg_wallet == self.wallet_public_id

    def _connect_order_subscriber(self, exchange_name: OrderExchange) -> None:
        """Build, connect, and subscribe the order-commands SUB socket.

        Extracted from :meth:`_setup_zmq_sockets` so the order-handler
        supervisor can rebuild ONLY its own subscriber before a respawn —
        the dominant order-handler fault is a poisoned or closed SUB
        socket, and re-entering the loop on the same dead socket would
        just die again.

        Args:
            exchange_name: Exchange name for topic prefix construction.
        """
        context = self._require_context()
        raw_sub_socket = context.socket(zmq.SUB)
        apply_hwm(raw_sub_socket, rcvhwm=HWM_ORDER_FLOW)
        raw_sub_socket.connect(self.settings.zmq_broker_xpub)
        self.subscriber = ValidatedSubscriber(raw_sub_socket)
        cmd_prefix = order_commands_prefix(exchange_name)
        self.subscriber.subscribe(cmd_prefix)
        self.subscriber.subscribe("system.symbol_aliases")
        self.subscriber.subscribe("system.settings")
        logger.info(
            f"ExchangeExecutorService[{exchange_name}]: Subscribed to {cmd_prefix}, "
            f"system.symbol_aliases, system.settings from {self.settings.zmq_broker_xpub}"
        )

    def _rebuild_order_subscriber(self) -> None:
        """Replace the order-commands subscriber with a fresh socket.

        Runs as the order-handler supervisor's pre-respawn hook: closes
        the old SUB (LINGER 0 — never block a respawn on unsent acks) and
        connects a new one with identical subscriptions. Local-safe: the
        subscriber is consumed only by the order handler.
        """
        old = self.subscriber
        if old is not None:
            try:
                old.setsockopt(zmq.LINGER, 0)
                old.close()
            except Exception as exc:
                logger.warning(f"Closing poisoned order subscriber failed: {exc!r}")
        self._connect_order_subscriber(self._get_exchange_name())

    def _setup_zmq_sockets(self, exchange_name: OrderExchange) -> None:
        """Create and connect ZMQ subscriber and publisher sockets.

        Args:
            exchange_name: Exchange name for topic prefix construction.
        """
        self.context = zmq.asyncio.Context()
        self._connect_order_subscriber(exchange_name)
        raw_pub_socket = self.context.socket(zmq.PUB)
        apply_hwm(raw_pub_socket, sndhwm=HWM_ORDER_FLOW)
        raw_pub_socket.connect(self.settings.zmq_broker_xsub)
        self.publisher = ValidatedPublisher(raw_pub_socket)
        self.msg_publisher = MessagePublisher(self.publisher, self._tracker)
        logger.info(
            f"ExchangeExecutorService[{exchange_name}]: "
            f"Publishing to broker {self.settings.zmq_broker_xsub}"
        )

    async def _recover_pending_orders(self, exchange_name: OrderExchange) -> None:
        """Rebuild pending order state from exchange and database on startup.

        Runs before the order handler loop to ensure in-flight orders from
        a previous session are tracked. This prevents orphaned execution
        events from being dropped.

        Steps:
        1. Query exchange for open orders via get_orders(status=OPEN).
        2. Query DB for active orders via get_active_orders_for_recovery.
        3. Correlate: rebuild PendingOrderState for orders found on exchange.
        4. For DB-active orders missing on exchange: verify via get_order()
           and mark terminal if genuinely absent.

        Args:
            exchange_name: Exchange name for logging and DB queries.
        """
        exchange_client = self._require_exchange_client()
        if self.repository is None:
            logger.warning(f"[{exchange_name}] No repository, skipping recovery")
            return
        try:
            exchange_open = await exchange_client.get_orders(status=ExchangeOrderStatusEnum.OPEN)
        except Exception as e:
            logger.error(f"[{exchange_name}] Failed to query exchange open orders: {e}")
            exchange_open = []
        exchange_by_id: dict[str, Any] = {o.id: o for o in exchange_open}
        now = datetime.now(UTC)
        try:
            db_active = await self.repository.get_active_orders_for_recovery(
                exchange=exchange_name,
                as_of=now,
                wallet_public_id=self.wallet_public_id,
            )
        except Exception as e:
            logger.error(f"[{exchange_name}] Failed to query DB active orders: {e}")
            db_active = []
        recovered = 0
        for db_order in db_active:
            try:
                result = await self._recover_single_order(db_order, exchange_by_id, exchange_name)
            except Exception:
                logger.exception(
                    f"[{exchange_name}] Recovery failed for order "
                    f"{db_order.get('client_order_id')} - continuing sweep"
                )
                continue
            if result:
                recovered += 1
        logger.info(
            f"[{exchange_name}] Recovery complete: {recovered} pending orders restored "
            f"from {len(db_active)} DB active / {len(exchange_open)} exchange open"
        )

    async def _recover_single_order(
        self,
        db_order: OrderRow,
        exchange_by_id: dict[str, Any],
        exchange_name: OrderExchange,
    ) -> bool:
        """Recover one DB-active order: honest seeds, healing, tracking.

        Replaces the silent re-baseline that seeded both watermarks from
        the venue's CURRENT cumulative — which made every downtime fill
        invisible forever (the next recon cycle computed
        ``gap = filled - last_seen = 0`` by construction). Sequence:

        1. Read the durable plane (``venue_events`` cumulative fill rows)
           and the published plane (``executions`` sum — rows are inserted
           only after a successful publish) and seed the dual watermarks
           from what each plane actually proves.
        2. Resolve the venue snapshot and classify open / terminal /
           unverifiable. Unverifiable orders are PARKED with DB seeds and
           no emission — the recon loop retries them every cycle instead
           of the order silently vanishing from tracking.
        3. Register the pending entry FIRST (real ``db_order_id`` PK, so
           post-recovery status updates hit the actual row) and pre-warm
           the exec-id LRU with already-published row ids.
        4. Republish recorded-but-unpublished ID-BEARING rows (the
           ``durable_max - exec_sum`` tail) through the normal pipeline
           under their ORIGINAL exec ids — every consumer (engine
           ``apply_fill``, checkpoint replay, executions insert's partial
           unique index) dedupes by exec id, so a consumer that already
           saw a row ignores it and one that missed it applies it once.
           ID-LESS rows (Walutomat) are NOT republished: after cumulative
           absorption no identity-shaped key can correlate the
           republished partition with what a live engine already applied
           (the venue partition and the published partition legitimately
           differ), so any republish risks double-application. Their
           cumulative counts toward the committed seed as shown; the
           durable rows themselves heal an engine that missed them at
           its next checkpoint replay, and that residual is logged
           loudly.
        5. Emit the remaining venue-ahead-of-durable gap as a recon
           corrective (stable synthetic id), then for terminal orders the
           terminal execution — both through the same pipeline recon uses
           at steady state, so recovery and recon converge on identical
           emissions instead of recovery hiding state.

        Args:
            db_order: Order row from DB recovery query.
            exchange_by_id: Map of exchange_order_id to ExchangeOrderSnapshot.
            exchange_name: Exchange name for logging.

        Returns:
            True if order was recovered into (or parked in) pending state;
            False for skipped rows and orders that went terminal.
        """
        self._require_exchange_client()
        exchange_order_id = db_order.get("exchange_order_id")
        client_order_id = db_order.get("client_order_id", "")
        if not exchange_order_id or not client_order_id:
            return False
        seeds = await self._read_recovery_watermarks(
            client_order_id, db_order["public_id"], exchange_name
        )
        snapshot, classification = await self._resolve_recovery_snapshot(
            exchange_order_id, db_order, exchange_by_id, exchange_name
        )
        pending, fill_rows = self._register_recovered_pending(
            db_order,
            client_order_id,
            exchange_order_id,
            snapshot,
            seeds,
            exchange_name,
        )
        last_seen = self._absorb_idless_recovery_fills(
            client_order_id, pending, fill_rows, exchange_name
        )
        await self._republish_recovery_fill_rows(
            fill_rows, last_seen, exchange_order_id, db_order, exchange_name
        )
        return await self._finish_recovered_order(
            client_order_id,
            exchange_order_id,
            snapshot,
            classification,
            exchange_name,
        )

    def _register_recovered_pending(
        self,
        db_order: OrderRow,
        client_order_id: str,
        exchange_order_id: str,
        snapshot: ExchangeOrderSnapshot | None,
        seeds: _RecoveryWatermarks | None,
        exchange_name: OrderExchange,
    ) -> tuple[PendingOrderState, list[VenueEventRow]]:
        """Create and register the pending state for one recovery row."""
        fill_rows: list[VenueEventRow]
        if seeds is None:
            venue_filled = float(snapshot.filled or 0.0) if snapshot is not None else 0.0
            last_seen, durable_max, fill_rows = venue_filled, venue_filled, []
            published_fee_seed: dict[str, float] = {}
            logger.error(
                f"[{exchange_name}] Recovery: durable fill history unreadable for "
                f"{client_order_id} - falling back to venue-truth seeding "
                f"(downtime gap for this order will NOT be healed)"
            )
        else:
            last_seen, durable_max, fill_rows, published_fee_seed = seeds
        pending = PendingOrderState(
            request=self._build_recovered_request(db_order, client_order_id, exchange_name),
            db_order_id=db_order.get("id"),
            order_public_id=db_order["public_id"],
            exchange_order_id=exchange_order_id,
            last_seen_cum_qty=last_seen,
            last_recorded_cum_qty=durable_max,
            last_recorded_fee=self._sum_row_fees(fill_rows),
            last_published_fee=published_fee_seed,
        )
        self.pending_orders[client_order_id] = pending
        self.client_by_exchange[exchange_order_id] = client_order_id
        return pending, fill_rows

    def _absorb_idless_recovery_fills(
        self,
        client_order_id: str,
        pending: PendingOrderState,
        fill_rows: list[VenueEventRow],
        exchange_name: OrderExchange,
    ) -> float:
        """Count id-less recovered fills as shown before id-bearing republish."""
        last_seen = pending.last_seen_cum_qty
        idless_cum_max = 0.0
        for row in fill_rows:
            row_cum = row["cum_fill_size"]
            if row_cum is not None and not row["exec_id"] and row_cum > last_seen + 1e-12:
                idless_cum_max = max(idless_cum_max, row_cum)
        if idless_cum_max > last_seen + 1e-12:
            last_seen = idless_cum_max
            pending.last_seen_cum_qty = max(pending.last_seen_cum_qty, idless_cum_max)
            logger.warning(
                f"[{exchange_name}] Recovery: {client_order_id} has id-less "
                f"recorded fills up to cum={idless_cum_max} that cannot be "
                f"safely republished (no venue exec id; after cumulative "
                f"absorption no identity key can correlate what a live "
                f"engine already applied) - counted as shown BEFORE the "
                f"id-bearing republish so a later row's cum-anchored delta "
                f"cannot re-absorb them; a coordinator that missed their "
                f"publish heals at its next restart via checkpoint replay"
            )
        return last_seen

    async def _republish_recovery_fill_rows(
        self,
        fill_rows: list[VenueEventRow],
        last_seen: float,
        exchange_order_id: str,
        db_order: OrderRow,
        exchange_name: OrderExchange,
    ) -> None:
        """Republish id-bearing recovered rows beyond the shown cumulative."""
        for row in fill_rows:
            row_cum = row["cum_fill_size"]
            if row_cum is None:
                continue
            if row_cum <= last_seen + 1e-12:
                self._register_seen_exec_id(row["exec_id"])
            else:
                await self._republish_recorded_fill(row, exchange_order_id, db_order, exchange_name)

    async def _finish_recovered_order(
        self,
        client_order_id: str,
        exchange_order_id: str,
        snapshot: ExchangeOrderSnapshot | None,
        classification: str,
        exchange_name: OrderExchange,
    ) -> bool:
        """Project recovered terminal state or leave the order tracked.

        Venue-verified-OPEN recovered orders additionally republish the
        adoption-shaped ACCEPTED (``reason="adopted"``, #155) BEFORE any
        fill-gap emission: startup recovery re-inserts DB-active orders
        into ``pending_orders`` ahead of the ghost sweep (which then
        skips them), so without this publish an executor crash would
        permanently strand the running engine's re-arm signal. The
        publish precedes the fill-gap corrective so accepted/fill
        ordering stays coherent and the entry cannot have been popped by
        a completing gap emission. Duplicates are absorbed engine-side
        (exact-match refresh / in-flight skip), and the one-shot-per-
        restart cadence cannot starve the timeout valve. UNVERIFIABLE
        rows stay silent — re-arming a possibly-dead order's guard would
        block honest emission; they remain on the ambiguous-verify
        track.
        """
        if classification == "unverifiable":
            logger.warning(
                f"[{exchange_name}] Recovery: order {client_order_id} parked with DB "
                f"seeds - venue unverifiable, recon will retry"
            )
            return True
        if snapshot is None:
            raise RuntimeError("Recovery classification requires a venue snapshot")
        live_pending = self.pending_orders.get(client_order_id)
        if classification == "open" and live_pending is not None:
            republished = await self._publish_order_status(
                live_pending.request,
                OrderEventEnum.ACCEPTED,
                exchange_order_id,
                reason=_ADOPTED_REARM_REASON,
            )
            if not republished:
                live_pending.adopted_accept_publish_pending = True
                logger.warning(
                    f"[{exchange_name}] Recovery: adoption ACCEPTED republish failed "
                    f"for {client_order_id} — parked for the recon loop"
                )
        if (
            live_pending is not None
            and float(snapshot.filled or 0.0) > live_pending.last_seen_cum_qty
        ):
            gap_result = await self._reconcile_fill_gap(
                exchange_name, exchange_order_id, live_pending, snapshot
            )
            if gap_result == "deferred":
                logger.warning(
                    f"[{exchange_name}] Recovery: terminal for {exchange_order_id} HELD "
                    f"— the fill-gap corrective deferred on a transient fee-source "
                    f"failure; the recon loop retries and projects the terminal after"
                )
                return True
        if classification == "terminal":
            terminal_pending = self.pending_orders.get(client_order_id)
            if terminal_pending is not None:
                await self._emit_disappeared_terminal(
                    exchange_name, exchange_order_id, terminal_pending, snapshot
                )
            logger.info(
                f"[{exchange_name}] Recovery: order {client_order_id} went "
                f"{snapshot.status.value} during downtime - healed and projected"
            )
            return False
        return True

    @staticmethod
    def _sum_execution_fees(execs: list[ExecutionRow]) -> dict[str, float]:
        """Sum the published executions plane's fees per currency.

        Args:
            execs: Execution rows for one order.

        Returns:
            Signed fee totals keyed by fee asset.
        """
        totals: dict[str, float] = {}
        for execution_row in execs:
            fee = execution_row.get("fee") or 0.0
            if not fee:
                continue
            asset = execution_row.get("fee_asset") or ""
            totals[asset] = totals.get(asset, 0.0) + fee
        return totals

    @staticmethod
    def _sum_row_fees(fill_rows: list[VenueEventRow]) -> dict[str, float]:
        """Sum durable fill-row fees PER CURRENCY with exec-id dedupe.

        Watermark seeding input: venue_events is append-only with no
        exec-id uniqueness — a redelivered fill re-writes its row under
        the SAME exec id and replay dedupes by that id, so a raw sum
        would inflate the fee watermark and suppress future
        cumulative-fee deltas. Id-less rows each count: replay applies
        them via distinct fallback keys. Keyed by fee asset because the
        watermarks are per-currency (#145 P2-5: scalar watermarks
        subtracted USD-equivalent fees from EUR commissions).

        Args:
            fill_rows: The order's durable fill rows.

        Returns:
            Deduplicated signed fee totals keyed by fee asset.
        """
        seen: set[str] = set()
        totals: dict[str, float] = {}
        for row in fill_rows:
            exec_id = row["exec_id"]
            if exec_id:
                if exec_id in seen:
                    continue
                seen.add(exec_id)
            fee = row["fee"] or 0.0
            if not fee:
                continue
            asset = row["fee_asset"] or ""
            totals[asset] = totals.get(asset, 0.0) + fee
        return totals

    async def _read_recovery_watermarks(
        self,
        client_order_id: str,
        order_public_id: str,
        exchange_name: OrderExchange,
    ) -> tuple[float, float, list[VenueEventRow], dict[str, float]] | None:
        """Read both truth planes and derive honest watermark seeds.

        Returns:
            ``(last_seen_seed, durable_max, fill_rows, published_fee_seed)``
            where ``published_fee_seed`` is the EXECUTIONS plane's
            per-asset signed fee totals VERBATIM — executions rows are
            written only after a successful publish, so their sum IS
            what the engine has been charged. No durable capping or
            netting: net per-asset comparisons lose signed components
            (a published ``+0.10`` charge plus an unpublished ``-0.15``
            rebate net to opposite-sign totals, and any netting rule
            then mis-anchors later cumulative correctives): a row recorded before the crash but never
            published carries fee the engine was never charged, so
            seeding the published fee watermark from ALL durable rows
            would make the republished tail (and any cumulative-fee
            corrective) silently fee-less. The republish path advances
            the watermark per successful publish through the normal
            booking pipeline, and
            ``last_seen_seed = min(executions_sum, durable_max)`` (the
            engine was never told more than what was durably recorded —
            the clamp guards pre-dual-watermark history), or None when
            the durable plane is unreadable (caller falls back to legacy
            venue-truth seeding for this order only). An unreadable
            published plane seeds the committed watermark CONSERVATIVELY
            to 0.0 and republishes the entire ID-BEARING durable tail —
            seeding it to the durable max would permanently mark a
            recorded-but-unpublished fill as shown (the engine never saw
            it, the venue matches the durable plane, so no later recon
            gap could ever surface it); republishing is idempotent
            because every consumer dedupes by exec id. ID-LESS rows are
            never republished — they count as shown per the recovery
            policy (see :meth:`_recover_single_order`).
            Non-SQLAlchemy repositories have no durable plane: both seeds
            come from the executions sum and no rows exist to republish.
        """
        repository = self._require_repository()
        now = datetime.now(UTC)
        if not isinstance(repository, SQLAlchemyRepository):
            try:
                execs = await repository.get_executions_for_order(order_public_id, now)
                exec_sum = sum(e["size"] for e in execs)
                exec_fees = self._sum_execution_fees(execs)
            except Exception as e:
                logger.warning(
                    f"[{exchange_name}] Recovery: executions unreadable for {client_order_id}: {e}"
                )
                exec_sum = 0.0
                exec_fees = {}
            return exec_sum, exec_sum, [], exec_fees
        try:
            fill_rows = await repository.get_fill_venue_events_for_order(client_order_id)
        except Exception as e:
            logger.error(
                f"[{exchange_name}] Recovery: venue_events unreadable for {client_order_id}: {e}"
            )
            return None
        durable_max = 0.0
        for row in fill_rows:
            row_cum = row["cum_fill_size"]
            if row_cum is not None and row_cum > durable_max:
                durable_max = row_cum
        try:
            execs = await repository.get_executions_for_order(order_public_id, now)
            exec_sum = sum(e["size"] for e in execs)
            exec_fees = self._sum_execution_fees(execs)
        except Exception as e:
            logger.warning(
                f"[{exchange_name}] Recovery: executions unreadable for "
                f"{client_order_id} - republishing the full durable tail "
                f"(idempotent via exec-id/fallback dedupe): {e}"
            )
            return 0.0, durable_max, fill_rows, {}
        return min(exec_sum, durable_max), durable_max, fill_rows, exec_fees

    async def _resolve_recovery_snapshot(
        self,
        exchange_order_id: str,
        db_order: OrderRow,
        exchange_by_id: dict[str, Any],
        exchange_name: OrderExchange,
    ) -> tuple[ExchangeOrderSnapshot | None, str]:
        """Resolve the venue snapshot for a recovering order and classify it.

        Returns:
            ``(snapshot, classification)`` with classification one of
            ``"open"`` / ``"terminal"`` / ``"unverifiable"`` (snapshot is
            None only for unverifiable).
        """
        exchange_client = self._require_exchange_client()
        if exchange_order_id in exchange_by_id:
            return exchange_by_id[exchange_order_id], "open"
        try:
            snap = await exchange_client.get_order(exchange_order_id, symbol=db_order["instrument"])
        except Exception as e:
            logger.warning(
                f"[{exchange_name}] Recovery: cannot verify order "
                f"{exchange_order_id} on exchange: {e}"
            )
            return None, "unverifiable"
        if snap.status in (
            ExchangeOrderStatusEnum.CLOSED,
            ExchangeOrderStatusEnum.CANCELED,
            ExchangeOrderStatusEnum.EXPIRED,
        ):
            return snap, "terminal"
        return snap, "open"

    def _build_recovered_request(
        self,
        db_order: OrderRow,
        client_order_id: str,
        exchange_name: OrderExchange,
    ) -> OrderRequestData:
        """Reconstruct the order request facade for a recovered order.

        Args:
            db_order: Order row from DB recovery query.
            client_order_id: Client order id of the recovering order.
            exchange_name: Exchange identifier.

        Returns:
            OrderRequestData carrying the recovered order's identity.
        """
        return OrderRequestData(
            public_id=db_order["public_id"],
            timestamp=db_order["timestamp"],
            session_id=db_order["session_id"],
            sequence_id=db_order["sequence_id"],
            strategy_id="recovered",
            instrument=db_order["instrument"],
            mode=(
                ExecutionModeEnum.LIVE
                if exchange_name != ExchangeEnum.PAPER
                else ExecutionModeEnum.PAPER
            ),
            side=cast(Any, db_order["side"]),
            order_type=cast(Any, db_order["order_type"]),
            quantity=db_order["size"],
            price=db_order.get("price"),
            client_order_id=client_order_id,
            exchange=exchange_name,
            wallet_public_id=db_order.get("wallet_public_id") or self.wallet_public_id,
            operator_public_id=db_order.get("operator_public_id"),
        )

    async def _republish_recorded_fill(
        self,
        row: VenueEventRow,
        exchange_order_id: str,
        db_order: OrderRow,
        exchange_name: OrderExchange,
    ) -> None:
        """Republish one recorded-but-unpublished fill row at recovery.

        The frame carries the row's ORIGINAL exec id, cumulative, size,
        price, and fee, and routes through :meth:`_process_execution` —
        the engine and checkpoint replay dedupe by exec id, the
        executions insert dedupes via its partial unique index, and the
        durable write produces a zero-gap row under the same id (replay-
        deduped). A consumer that already saw the fill ignores it; one
        that missed it applies it exactly once.

        Args:
            row: The recorded fill_observed venue event.
            exchange_order_id: Venue order id of the recovering order.
            db_order: DB order row for instrument/side context.
            exchange_name: Exchange identifier (logging).
        """
        fees = None
        fee = row["fee"]
        if fee is not None and abs(fee) > 1e-12:
            fees = [ExecutionFeeBreakdown(asset=row["fee_asset"] or "", quantity=fee)]
        update = ExecutionUpdate(
            order_id=exchange_order_id,
            exec_type="trade",
            exec_id=row["exec_id"],
            symbol=db_order["instrument"],
            side=OrderSideEnum(db_order["side"]),
            order_type=ExchangeOrderTypeEnum.MARKET,
            order_status=ExchangeOrderStatusEnum.OPEN,
            timestamp=row["venue_timestamp"] or row["timestamp"],
            cum_qty=row["cum_fill_size"],
            last_qty=row["fill_size"],
            last_price=row["fill_price"],
            fees=fees,
        )
        logger.info(
            f"[{exchange_name}] Recovery: republishing recorded-but-unpublished fill "
            f"{row['exec_id']} (cum={row['cum_fill_size']})"
        )
        await self._process_execution(update)

    async def start(self) -> None:
        """Start the execution service and subscribe to order topics.

        ``running`` is set BEFORE recovery on purpose: recovery republishes
        recorded-but-unpublished fills and emits gap correctives through
        the normal pipeline, and ``_publish_execution`` refuses to send
        while ``running`` is False. No task observes the flag early —
        every loop task spawns only after recovery returns.
        """
        exchange_name = self._get_exchange_name()
        set_log_context(f"exec:{exchange_name}")
        if self.running:
            logger.warning(f"{exchange_name} execution service already running")
            return
        self._portfolio_reconciliation_dispatch_open = False
        await self._initialize_settings()
        await self._resolve_credentials(exchange_name)
        self._client_context_active = False
        self.exchange_client = self._create_exchange_client()
        self.exchange_client.set_tracker(self._tracker)
        self._setup_zmq_sockets(exchange_name)
        supports_ws = self.exchange_client.supports_websocket_executions
        self._client_context_active = True
        try:
            with egress_identity(
                exchange=exchange_name,
                traffic_class="private",
                owner="executor",
                operation="client_lifecycle",
            ):
                async with self.exchange_client:
                    logger.info(
                        f"ExchangeExecutorService[{exchange_name}]: "
                        f"Exchange client initialized with WebSocket"
                    )
                    self.running = True
                    self._task_last_pass["reconciliation"] = time.monotonic()
                    await self._recover_pending_orders(exchange_name)
                    try:
                        tasks = [
                            asyncio.create_task(
                                self._supervise_loop(
                                    "order_handler",
                                    self._order_handler,
                                    pre_respawn=self._rebuild_order_subscriber,
                                )
                            ),
                            asyncio.create_task(
                                self._supervise_loop("heartbeat", self._heartbeat_loop)
                            ),
                        ]
                        if supports_ws:
                            tasks.append(asyncio.create_task(self._supervise_execution_stream()))
                        tasks.append(
                            asyncio.create_task(
                                self._supervise_loop("reconciliation", self._reconciliation_handler)
                            )
                        )
                        if isinstance(self.exchange_client, ExchangeClientBase) and (
                            self.exchange_client.balance_capability
                            is not CapabilityStatus.UNSUPPORTED
                            or self.exchange_client.position_capability
                            is CapabilityStatus.SUPPORTED
                        ):
                            self._portfolio_reconciliation_dispatch_open = True
                            tasks.append(
                                asyncio.create_task(
                                    self._supervise_loop(
                                        "account_observer", self._account_observer_handler
                                    )
                                )
                            )
                        try:
                            await asyncio.gather(*tasks)
                        except asyncio.CancelledError:
                            logger.info(f"ExchangeExecutorService[{exchange_name}] tasks cancelled")
                            raise
                        finally:
                            self._portfolio_reconciliation_dispatch_open = False
                            for task in tasks:
                                task.cancel()
                            await asyncio.gather(*tasks, return_exceptions=True)
                    except asyncio.CancelledError:
                        raise
                    except Exception:
                        await self.stop()
                        raise
        finally:
            try:
                await self._close_and_drain_portfolio_reconciliation()
            finally:
                self._client_context_active = False

    async def stop(self) -> None:
        """Stop the execution service and close ZMQ connections.

        Observer-side portfolio dispatch closes and drains first, before the
        running flag can stop the account observer. ZMQ teardown then unwinds
        blocked consumers, and the exchange client is disconnected as an
        idempotent FALLBACK — but
        ONLY when ``start()`` does not currently own the client
        (``_client_context_active`` False). ``start()`` claims ownership
        BEFORE entering ``async with`` — i.e. before ``connect()`` even
        begins — and releases it in a ``finally`` after the context has
        fully unwound. Disconnecting here while ownership is held would
        either close the REST pool under an in-flight ``connect()``
        (whose success would then leave the service running with a
        permanently closed pool) or tear the pool and ccxt session
        under an order handler or reconciliation cycle mid-call — a
        transport-class error on a live submit must classify as
        ambiguous (parked UNKNOWN), never be provoked by our own
        teardown. The fallback exists because without it a crash
        between client creation and ownership claim, or a start that
        never ran, would leak the client's ccxt session, WebSocket
        client, and bounded REST thread pool on every launcher
        fresh-instance restart (P1-3 multiplier). It runs even when
        ``running`` is already False, provided a client object exists;
        ``disconnect()`` is idempotent on both venues. Accepted
        residual: a wedged unwind that never reaches ``__aexit__`` is
        not healed here — loop supervision and bounded cycles own that.
        """
        await self._close_and_drain_portfolio_reconciliation()
        if self.running:
            self.running = False
            if self.subscriber:
                self.subscriber.setsockopt(zmq.LINGER, 0)
                self.subscriber.close()
            if self.publisher:
                self.publisher.setsockopt(zmq.LINGER, 0)
                self.publisher.close()
            if self.context:
                self.context.term()
            exchange_name = self._get_exchange_name()
            logger.info(f"ExchangeExecutorService[{exchange_name}] stopped")
        client = self.exchange_client
        if client is not None and not self._client_context_active:
            try:
                await client.disconnect()
            except Exception as exc:
                logger.warning(
                    f"ExchangeExecutorService[{self._get_exchange_name()}]: "
                    f"fallback exchange-client disconnect failed: {exc!r}"
                )

    async def _dispatch_command(
        self, parsed_suffix: str, payload_str: str, exchange_name: OrderExchange, instrument: str
    ) -> None:
        """Dispatch a parsed order command to its handler.

        Args:
            parsed_suffix: Command suffix (submit, cancel, replace).
            payload_str: JSON payload string.
            exchange_name: Exchange name from topic.
            instrument: Instrument from topic.
        """
        handler_map = {
            OrderCommandEnum.SUBMIT.value: self._handle_submit_command,
            OrderCommandEnum.CANCEL.value: self._handle_cancel_command,
            OrderCommandEnum.REPLACE.value: self._handle_replace_command,
        }
        handler = handler_map.get(parsed_suffix)
        if handler:
            await handler(payload_str, exchange_name, instrument)
        else:
            logger.debug(f"Ignoring unknown command suffix: {parsed_suffix}")

    async def _route_message(
        self, topic_str: str, payload_str: str, exchange_name: OrderExchange, commands_prefix: str
    ) -> None:
        """Route a received ZMQ message to the appropriate handler.

        Args:
            topic_str: ZMQ topic string.
            payload_str: Decoded payload string.
            exchange_name: Exchange name for this executor.
            commands_prefix: Expected command topic prefix.
        """
        if topic_str.startswith(commands_prefix):
            parsed = parse_order_command_topic(topic_str)
            if parsed is None:
                logger.warning(f"Malformed command topic: {topic_str}")
                return
            await self._dispatch_command(
                parsed.suffix, payload_str, exchange_name, parsed.instrument
            )
        elif topic_str == "system.symbol_aliases":
            self._handle_symbol_alias_update(payload_str)
        elif topic_str == "system.settings":
            self._handle_settings_update(payload_str)
        else:
            logger.warning(f"Received message on unexpected topic: {topic_str}")

    async def _order_handler(self) -> None:
        """Process incoming order commands from ZMQ subscription.

        Dispatches to appropriate handler based on command suffix:
        - .submit -> _process_order (OrderRequestData)
        - .cancel -> _process_cancel (OrderCancelData)
        - .replace -> _process_replace (OrderReplaceData)

        Topic invariant enforced: topic parts must match payload fields.
        """
        exchange_name = self._get_exchange_name()
        commands_prefix = order_commands_prefix(exchange_name)
        while self.running:
            if not self.subscriber:
                await asyncio.sleep(0.1)
                continue
            try:
                topic_str, payload_bytes = await self.subscriber.recv_multipart()
                payload_str = payload_bytes.decode("utf-8")
            except ValueError as e:
                logger.error(f"Poison order frame dropped (already consumed): {e}")
                continue
            self._order_inflight_started = time.monotonic()
            try:
                try:
                    parsed_msg = parse_message(payload_str)
                    parsed_wallet = getattr(parsed_msg, "wallet_public_id", "") or ""
                    self._gap_detector.check(
                        topic_str,
                        parsed_msg.session_id,
                        parsed_msg.sequence_id,
                        wallet_public_id=parsed_wallet,
                    )
                except MessageParseError:
                    pass
                await self._route_message(topic_str, payload_str, exchange_name, commands_prefix)
            except Exception as e:
                if self.running:
                    logger.error(f"Error handling order: {e}")
            finally:
                self._order_inflight_started = None

    @staticmethod
    def _validate_command_invariants(
        msg: Any, exchange_name: OrderExchange, topic_instrument: str
    ) -> bool:
        """Validate topic/payload invariants for a command message.

        Args:
            msg: Parsed command message with exchange and instrument fields.
            exchange_name: Expected exchange from topic.
            topic_instrument: Expected instrument from topic.

        Returns:
            True if invariants hold, False otherwise.
        """
        if msg.exchange != exchange_name:
            logger.warning(
                f"Invariant violation: payload exchange '{msg.exchange}' "
                f"!= topic exchange '{exchange_name}'"
            )
            return False
        if msg.instrument != topic_instrument:
            logger.warning(
                f"Invariant violation: payload instrument '{msg.instrument}' "
                f"!= topic instrument '{topic_instrument}'"
            )
            return False
        return True

    async def _handle_submit_command(
        self, payload_str: str, exchange_name: OrderExchange, topic_instrument: str
    ) -> None:
        """Handle submit command from orders.commands.*.*.submit topic.

        Args:
            payload_str: JSON payload string.
            exchange_name: Expected exchange name from topic.
            topic_instrument: Instrument from topic for invariant check.
        """
        try:
            order_msg = parse_message(payload_str)
        except MessageParseError as e:
            logger.warning(f"[{exchange_name}] Invalid submit command payload: {e}")
            return
        if not isinstance(order_msg, OrderRequestData):
            logger.warning(f"Received non-order message on submit topic: {order_msg.type}")
            return
        if not self._is_for_my_wallet(order_msg):
            return
        if self._validate_command_invariants(order_msg, exchange_name, topic_instrument):
            await self._process_order(order_msg)

    async def _handle_cancel_command(
        self, payload_str: str, exchange_name: OrderExchange, topic_instrument: str
    ) -> None:
        """Handle cancel command from orders.commands.*.*.cancel topic.

        Args:
            payload_str: JSON payload string.
            exchange_name: Expected exchange name from topic.
            topic_instrument: Instrument from topic for invariant check.
        """
        try:
            cancel_msg = parse_message(payload_str)
        except MessageParseError as e:
            logger.warning(f"[{exchange_name}] Invalid cancel command payload: {e}")
            return
        if not isinstance(cancel_msg, OrderCancelData):
            logger.warning(f"Received non-cancel message on cancel topic: {cancel_msg.type}")
            return
        if not self._is_for_my_wallet(cancel_msg):
            return
        if self._validate_command_invariants(cancel_msg, exchange_name, topic_instrument):
            await self._process_cancel(cancel_msg)

    async def _handle_replace_command(
        self, payload_str: str, exchange_name: OrderExchange, topic_instrument: str
    ) -> None:
        """Handle replace command from orders.commands.*.*.replace topic.

        Args:
            payload_str: JSON payload string.
            exchange_name: Expected exchange name from topic.
            topic_instrument: Instrument from topic for invariant check.
        """
        try:
            replace_msg = parse_message(payload_str)
        except MessageParseError as e:
            logger.warning(f"[{exchange_name}] Invalid replace command payload: {e}")
            return
        if not isinstance(replace_msg, OrderReplaceData):
            logger.warning(f"Received non-replace message on replace topic: {replace_msg.type}")
            return
        if not self._is_for_my_wallet(replace_msg):
            return
        if self._validate_command_invariants(replace_msg, exchange_name, topic_instrument):
            await self._process_replace(replace_msg)

    async def _is_duplicate_submit(self, order: OrderRequestData) -> bool:
        """Detect a replayed submit command before it can touch the venue.

        The outbox can publish the same client_order_id twice: a
        coordinator crash between the ZMQ publish and the bulk
        CREATED→DISPATCHED commit replays the row on restart, and a
        failed bulk write leaves it re-fetchable on the very next 50ms
        tick. Venue-side cl_ord_id dedupe covers only OPEN orders while
        strategy orders are MARKET, so an unguarded replay double-places
        a real position.

        Checks, cheapest first:

        1. in-memory pending entry — same-process replays, including a
           parked-UNKNOWN entry whose ambiguous/fill-tracking state the
           submit path's unconditional overwrite would destroy;
        2. the unhealed-accept queue — accepted order whose pending
           entry a terminal fill already popped;
        3. durable venue-event evidence (``has_order_submit_evidence``)
           — fresh-process crash-replay where memory is empty but an
           accepted/fill/terminal/unknown row exists. ``order_rejected``
           is excluded there so the outbox's legitimate
           retry-after-definitive-reject path still flows. A replay
           whose evidence includes ``order_breaker_open`` does NOT drop
           silently: the breaker disposition is rerun idempotently —
           an executor crash between the evidence write and the
           REJECTED publish left the intent release incomplete, and the
           in-memory retry queue died with the process (#145 P2-5 §2d).

        A durable-check failure is FAIL-CLOSED: the command is dropped
        as if duplicate. Proceeding on a true duplicate irreversibly
        doubles a MARKET position; dropping a fresh command self-heals
        via the engine's in-flight timeout valve with a NEW id.

        Dropped duplicates publish NOTHING: the outbox transitions the
        row from its own publish success, never from executor events,
        and a synthetic REJECTED here would fabricate terminal state
        for a possibly-live order.

        Args:
            order: The incoming order request.

        Returns:
            True when the command must be dropped as a duplicate.
        """
        exchange_name = self._get_exchange_name()
        cid = order.client_order_id
        if cid in self.pending_orders:
            logger.warning(
                f"[{exchange_name}] Duplicate submit {cid} dropped: already pending "
                f"in this executor (replayed dispatch)"
            )
            return True
        if cid in self._unhealed_accept_events:
            logger.warning(
                f"[{exchange_name}] Duplicate submit {cid} dropped: accepted order "
                f"awaiting durable-event heal"
            )
            return True
        if isinstance(self.repository, SQLAlchemyRepository):
            try:
                if await self.repository.has_order_submit_evidence(cid):
                    if await self.repository.has_venue_event(cid, "order_breaker_open"):
                        logger.warning(
                            f"[{exchange_name}] Replayed submit {cid} carries "
                            f"breaker-open evidence — rerunning the terminal "
                            f"disposition instead of a silent drop (an executor "
                            f"crash mid-disposition would otherwise strand the "
                            f"engine's intent until its timeout valve)"
                        )
                        await self._handle_breaker_open_submit(order)
                        return True
                    if await self.repository.has_venue_event(cid, _INTERLOCK_BLOCKED_EVENT_TYPE):
                        logger.warning(
                            f"[{exchange_name}] Replayed submit {cid} carries "
                            f"interlock-blocked evidence — rerunning the terminal "
                            f"disposition instead of a silent drop (an executor "
                            f"crash mid-disposition would otherwise strand the "
                            f"engine's intent; the original block reason is not "
                            f"reconstructed post-release)"
                        )
                        await self._handle_interlock_blocked_submit(
                            order, _INTERLOCK_REASON_MODE_UNAVAILABLE
                        )
                        return True
                    logger.warning(
                        f"[{exchange_name}] Duplicate submit {cid} dropped: durable "
                        f"venue-event evidence exists (crash-replayed dispatch)"
                    )
                    return True
            except Exception as e:
                logger.error(
                    f"[{exchange_name}] Duplicate-evidence check failed for {cid}: {e} "
                    f"— FAIL-CLOSED, dropping the command (a true duplicate would "
                    f"double a MARKET position; a fresh command re-emits via the "
                    f"engine timeout valve)"
                )
                return True
        return False

    async def _reject_if_stale(self, order: OrderRequestData) -> bool:
        """Reject a command older than the dispatch TTL before any venue call.

        The executor's half of the staleness control: the order-flow
        socket buffers an outage backlog unbounded (HWM=0),
        so frames can arrive long after the outbox published them. The
        age anchor is ``signaled_at`` — the command row's creation time
        forwarded by the outbox publish — because the frame
        ``timestamp`` is re-stamped at every publish. Runs AFTER the
        duplicate guard on purpose: a stale REPLAY of an accepted order
        must drop silently as a duplicate, never reject — a synthetic
        REJECTED for a live order is exactly the false-reject
        fabrication the UNKNOWN state exists to prevent. Frames
        without ``signaled_at`` (legacy/direct paths) and a disabled
        TTL skip the gate.

        The rejection is VENUE-TRUTH-BACKED: before rejecting, a single
        bounded client-id lookup runs. A found order is ADOPTED (it is
        a replay of a submit that actually placed — e.g. the
        crash-window replay whose ``order_submit_unknown`` durable
        write also failed, leaving no evidence for the duplicate
        guard); a venue that cannot answer (lookup unsupported,
        unreachable, paper) makes the frame DROP silently — rejecting
        on no evidence could fabricate a terminal state for a live
        order, while dropping leaves the row DISPATCHED for the
        reconciler's stale WARN and the engine's timeout valve. Only
        an AUTHORITATIVE absence rejects.

        REJECTED (not the expired suffix) is deliberate: the trader's
        OrderData handler clears engine intent only on ``rejected``;
        the ``expired`` suffix rides the cancel/replace OrderEventData
        channel. The durable ``order_rejected`` row never blocks a
        later legitimate retry because the duplicate guard's evidence
        set excludes rejections.

        Args:
            order: The incoming order request.

        Returns:
            True when the command was consumed here (rejected, adopted,
            or dropped); False when it should proceed to submit.
        """
        ttl = self._resolve_dispatch_ttl()
        if ttl <= 0 or order.signaled_at is None:
            return False
        age_s = (datetime.now(UTC) - order.signaled_at).total_seconds()
        if age_s <= ttl:
            return False
        exchange_name = self._get_exchange_name()
        if self.exchange_client is None:
            logger.warning(
                f"[{exchange_name}] STALE command {order.client_order_id} dropped "
                f"(age {age_s:.1f}s > TTL {ttl:.1f}s; no venue client to verify absence)"
            )
            return True
        try:
            async with asyncio.timeout(_AMBIGUOUS_VERIFY_TIMEOUT_S):
                snapshot = await self.exchange_client.find_order_by_client_id(
                    order.client_order_id, order.instrument
                )
        except Exception as e:
            logger.warning(
                f"[{exchange_name}] STALE command {order.client_order_id} dropped "
                f"(age {age_s:.1f}s > TTL {ttl:.1f}s; venue absence unverifiable: {e}) "
                f"— rejecting without venue truth could fabricate a terminal state"
            )
            return True
        if snapshot is not None:
            logger.warning(
                f"[{exchange_name}] STALE frame {order.client_order_id} is a replay of "
                f"a PLACED order ({snapshot.id}, status={snapshot.status}) — adopting "
                f"instead of rejecting"
            )
            pending = PendingOrderState(request=order)
            self.pending_orders[order.client_order_id] = pending
            await self._adopt_found_order(order, pending, snapshot)
            return True
        logger.warning(
            f"[{exchange_name}] Rejecting STALE command {order.client_order_id}: "
            f"age {age_s:.1f}s exceeds dispatch TTL {ttl:.1f}s and the venue verified "
            f"absence — an outage-backlog MARKET order must not fire into a moved market"
        )
        if not await self._publish_order_status(order, OrderEventEnum.REJECTED):
            logger.warning(
                f"[{exchange_name}] REJECTED publish failed for STALE "
                f"{order.client_order_id} — NOT recording order_rejected (a terminal "
                f"row before a confirmed publish would exempt the command from the "
                f"dispatched-verification sweep while the engine guard stays held); "
                f"the frame is consumed and the sweep retries the release durably"
            )
            return True
        await self._record_venue_event(
            {
                "event_type": "order_rejected",
                "exchange_name": exchange_name,
                "instrument": order.instrument,
                "client_order_id": order.client_order_id,
                "side": order.side,
                "error": f"stale command: age {age_s:.1f}s exceeds dispatch TTL {ttl:.1f}s",
                "strategy_tag": order.strategy_tag,
            }
        )
        return True

    def _resolve_dispatch_ttl(self) -> float:
        """Return the dispatch TTL from settings, tolerating test doubles.

        Test fixtures replace ``self.settings`` with plain mocks whose
        attributes are not floats; treating anything unparseable as
        disabled keeps the gate strictly opt-in.

        Returns:
            TTL seconds, or 0.0 when unset/unparseable (gate disabled).
        """
        try:
            return float(self.settings.trade_command_dispatch_ttl_s)
        except AttributeError, TypeError, ValueError:
            return 0.0

    async def _reject_if_replay_origin(self, order: OrderRequestData) -> bool:
        """Reject a replay-origin market command before any venue call.

        PnL Phase 1 S4 (incident 2026-07-10 #3): a strategy fed
        REPLAYED historical frames drives the live engine into real
        market commands — without this gate they fill a live (paper)
        account at historical prices. The frame's ``origin`` is
        stamped by the replaying publisher, carried immutably through
        SignalData → the durable command row → the outbox rebuild, so
        the gate re-fires DETERMINISTICALLY on every replay of the
        same command (no interlock-style disposition machinery
        needed). Runs AFTER the duplicate guard (a replayed frame of
        an already-accepted order must drop as a duplicate, never
        fabricate a REJECTED) and BEFORE the stale gate (replay origin
        is a harder verdict than age).

        Publish-first like the stale reject: a durable
        ``order_rejected`` row before a confirmed publish would exempt
        the command from the dispatched-verification sweep while the
        engine guard stays held.

        Args:
            order: The incoming order request.

        Returns:
            True when the command was consumed here (rejected);
            False when it should proceed.
        """
        if order.origin != "replay":
            return False
        exchange_name = self._get_exchange_name()
        window = f"window=[{order.replay_window_start} .. {order.replay_window_end}]"
        logger.warning(
            f"[{exchange_name}] Rejecting REPLAY-ORIGIN command "
            f"{order.client_order_id} ({window}) — replayed historical frames "
            f"must never trade against a live account"
        )
        if not await self._publish_order_status(
            order, OrderEventEnum.REJECTED, reason="replay_origin"
        ):
            logger.warning(
                f"[{exchange_name}] REJECTED publish failed for REPLAY-ORIGIN "
                f"{order.client_order_id} — NOT recording order_rejected; the "
                f"frame is consumed and the sweep retries the release durably"
            )
            return True
        await self._record_venue_event(
            {
                "event_type": "order_rejected",
                "exchange_name": exchange_name,
                "instrument": order.instrument,
                "client_order_id": order.client_order_id,
                "side": order.side,
                "error": f"replay-origin command rejected pre-venue ({window})",
                "strategy_tag": order.strategy_tag,
            }
        )
        return True

    async def _process_order(self, order: OrderRequestData) -> None:
        """Submit an order to the exchange and handle the response.

        Order correlation uses two-level mapping:
        - pending_orders: keyed by client_order_id (always present, unique)
        - client_by_exchange: maps exchange_order_id -> client_order_id (after ACK)

        Publishes order events to orders.events.{exchange}.{instrument}.{event}:
        - submitted: Executor accepted command, sending to exchange
        - accepted: Exchange ACK returned order_id (sync REST response)
        - rejected: Exchange DEFINITIVELY rejected the order or
          validation failed before any send
        - unknown: Submit outcome ambiguous (order may exist on the
          venue) — pending entry parked, engine holds in-flight
        - executed: Order executed (see _execution_handler for WebSocket fills)

        Note: 'accepted' is published when the exchange REST API returns an order_id,
        confirming the order was received and queued. This is a synchronous response.
        Actual fills come asynchronously via WebSocket execution updates.
        Acceptance finalization runs outside the submit try/except in
        ``_finalize_accepted_submit`` so a DB blip on a live order can
        never publish REJECTED.

        Args:
            order: Order request data containing order details.
        """
        exchange_name = self._get_exchange_name()
        if await self._is_duplicate_submit(order):
            return
        if await self._reject_if_replay_origin(order):
            return
        if await self._reject_if_stale(order):
            return
        if not is_tradeable(order.instrument, exchange_name):
            logger.warning(
                f"[{exchange_name}] Rejecting order {order.client_order_id}: "
                f"instrument {order.instrument} not tradeable"
            )
            self.pending_orders.pop(order.client_order_id, None)
            await self._publish_order_status(order, OrderEventEnum.REJECTED)
            return
        if await self._is_live_trading_interlocked(order, exchange_name):
            return
        try:
            self.pending_orders[order.client_order_id] = PendingOrderState(request=order)
            await self._publish_order_status(order, OrderEventEnum.SUBMITTED)
            exchange_order_id = await self._execute_live_order(order)
        except AmbiguousOrderSubmitError as e:
            await self._handle_ambiguous_submit(order, e)
            return
        except CircuitBreakerOpenError:
            await self._handle_breaker_open_submit(order)
            return
        except Exception as e:
            logger.error(f"[{exchange_name}] Error processing order {order.client_order_id}: {e}")
            self.pending_orders.pop(order.client_order_id, None)
            await self._publish_order_status(order, OrderEventEnum.REJECTED)
            await self._record_venue_event(
                {
                    "event_type": "order_rejected",
                    "exchange_name": exchange_name,
                    "instrument": order.instrument,
                    "client_order_id": order.client_order_id,
                    "side": order.side,
                    "error": str(e),
                    "strategy_tag": order.strategy_tag,
                }
            )
            return
        if exchange_order_id:
            await self._finalize_accepted_submit(order, exchange_order_id)
        else:
            logger.warning(f"[{exchange_name}] Order {order.client_order_id} rejected by exchange")
            self.pending_orders.pop(order.client_order_id, None)
            await self._publish_order_status(order, OrderEventEnum.REJECTED)
            await self._record_venue_event(
                {
                    "event_type": "order_rejected",
                    "exchange_name": exchange_name,
                    "instrument": order.instrument,
                    "client_order_id": order.client_order_id,
                    "side": order.side,
                    "error": "rejected by exchange",
                    "strategy_tag": order.strategy_tag,
                }
            )

    async def _handle_breaker_open_submit(self, order: OrderRequestData) -> None:
        """Give a breaker-open submit its distinct, redispatch-safe disposition.

        Breaker-open is authoritative not-submitted, but NOT a venue
        rejection: an ``order_rejected`` row is deliberately excluded
        from duplicate-submit evidence so the outbox may retry — a
        breaker-open command must NOT blind-retry (the breaker may have
        closed; a late redispatched frame would place an order the
        engine no longer tracks). The sequence is therefore (#145 §2d):
        durable ``order_breaker_open`` event (IS submit evidence — the
        dup guard drops any in-flight redispatch), CAS the command row
        to FAILED (a terminal row can never be re-fetched by the
        outbox), and ONLY THEN publish REJECTED with the
        ``circuit_breaker_open`` reason so the engine releases intent.
        Any step failing parks the entry with ``breaker_open_pending``
        — intent stays held and the recon loop reruns the sequence.

        Args:
            order: The order request refused by the open breaker.
        """
        exchange_name = self._get_exchange_name()
        logger.warning(
            f"[{exchange_name}] Order {order.client_order_id} refused locally: venue "
            f"circuit breaker OPEN — distinct breaker disposition (no venue rejection "
            f"fabricated)"
        )
        if await self._complete_breaker_open_disposition(order):
            return
        pending = self.pending_orders.get(order.client_order_id)
        if pending is None:
            pending = PendingOrderState(request=order)
            self.pending_orders[order.client_order_id] = pending
        pending.breaker_open_pending = True
        logger.warning(
            f"[{exchange_name}] breaker-open disposition incomplete for "
            f"{order.client_order_id} — entry parked, recon retries (engine intent "
            f"stays held until the durable terminal lands)"
        )

    async def _complete_breaker_open_disposition(self, order: OrderRequestData) -> bool:
        """Run the breaker-open sequence: record, CAS FAILED, publish REJECTED.

        Idempotent for retries: the durable event write is probe-guarded,
        the lifecycle CAS tolerates an already-FAILED row, and the
        REJECTED publish is engine-idempotent. The pending entry is
        popped only on full success.

        Args:
            order: The breaker-refused order request.

        Returns:
            True when the full sequence completed.
        """
        exchange_name = self._get_exchange_name()
        cid = order.client_order_id
        try:
            already_recorded = isinstance(
                self.repository, SQLAlchemyRepository
            ) and await self.repository.has_venue_event(cid, "order_breaker_open")
            if not already_recorded:
                await self._record_venue_event(
                    {
                        "event_type": "order_breaker_open",
                        "exchange_name": exchange_name,
                        "instrument": order.instrument,
                        "client_order_id": cid,
                        "side": order.side,
                        "status": TradeCommandStatusEnum.FAILED.value,
                        "error": "circuit_breaker_open",
                        "strategy_tag": order.strategy_tag,
                    }
                )
        except Exception:
            logger.warning(
                f"[{exchange_name}] order_breaker_open event write failed for {cid} — "
                f"retrying via recon"
            )
            return False
        if not await self._fail_command_for_breaker(order):
            return False
        if not await self._publish_order_status(
            order, OrderEventEnum.REJECTED, reason="circuit_breaker_open"
        ):
            logger.warning(
                f"[{exchange_name}] REJECTED publish failed for breaker-open {cid} — "
                f"retrying via recon (intent must not silently stay held)"
            )
            return False
        self.pending_orders.pop(cid, None)
        return True

    async def _fail_command_for_breaker(self, order: OrderRequestData) -> bool:
        """CAS the breaker-refused command row to FAILED.

        Resolves the durable row through the STRICT cid lookup first —
        ``TradeCommand.public_id`` is generated independently of the
        client order id, so a cid-keyed CAS would silently miss. Then
        tries the statuses a breaker-refused command can legally be in
        (``dispatched``, ``direct_dispatched``, ``created``); a row that
        is ALREADY terminal counts as done (an earlier attempt or the
        lifecycle fold landed it), and NO row at all means there is no
        durable command to terminalize (manual/paper flows). A DB error
        fails the sequence —
        publishing REJECTED before the durable terminal would let a
        still-CREATED row redispatch after the engine released intent.

        Args:
            order: The breaker-refused order request.

        Returns:
            True when the row is verifiably terminal (or no SQL
            repository is wired — paper/test mode has no outbox).
        """
        if not isinstance(self.repository, SQLAlchemyRepository):
            return True
        now = datetime.now(UTC)
        try:
            cmd = await self.repository.get_active_create_command_by_client_order_id(
                order.client_order_id, self._get_exchange_name()
            )
            if cmd is None:
                return True
            if cmd["status"] in _COMMAND_TERMINAL_STATUSES:
                return True
            for expected in (
                TradeCommandStatusEnum.DISPATCHED.value,
                TradeCommandStatusEnum.DIRECT_DISPATCHED.value,
                TradeCommandStatusEnum.CREATED.value,
            ):
                if await self.repository.advance_trade_command_lifecycle(
                    public_id=cmd["public_id"],
                    expected_status=expected,
                    new_status=TradeCommandStatusEnum.FAILED.value,
                    bus_time=now,
                    session_id=cmd["session_id"],
                    sequence_id=cmd["sequence_id"],
                    terminal_at=now,
                    last_error="circuit_breaker_open",
                ):
                    return True
            current = await self.repository.get_current_trade_command_status(cmd["public_id"])
        except Exception as e:
            logger.warning(
                f"[{self._get_exchange_name()}] breaker-open FAILED CAS errored for "
                f"{order.client_order_id}: {e}"
            )
            return False
        if current is None or current in _COMMAND_TERMINAL_STATUSES:
            return True
        logger.warning(
            f"[{self._get_exchange_name()}] breaker-open CAS lost for "
            f"{order.client_order_id} (row at {current}) — retrying via recon"
        )
        return False

    async def _retry_breaker_open(self, client_order_id: str) -> None:
        """Rerun one parked breaker-open disposition from the recon loop.

        Args:
            client_order_id: Key into ``pending_orders``.
        """
        pending = self.pending_orders.get(client_order_id)
        if pending is None or not pending.breaker_open_pending:
            return
        if await self._complete_breaker_open_disposition(pending.request):
            logger.info(
                f"[{self._get_exchange_name()}] Recon healed the breaker-open "
                f"disposition for {client_order_id}"
            )

    async def _read_live_trading_mode(self) -> str:
        """Fresh-read ``live_trading_mode`` for the interlock, fail-closed.

        Reads the setting directly from the database (never the
        ZMQ-refreshed cache — a lost broadcast would leave a kill-switch
        stale) under a hard timeout so a wedged database cannot starve
        the serialized handler. A genuine operator value passes through
        as-is. Every FAILURE mode — no settings service, timeout,
        query/decrypt error, a duplicate active row, a missing row, or a
        value that is not exactly one of the three accepted strings —
        collapses to :data:`_LIVE_TRADING_UNAVAILABLE`, a blocking
        sentinel distinct from a deliberate ``halted`` so the caller can
        report the incident honestly. The result is one of the three
        valid modes or the sentinel; never an arbitrary stored string.

        Returns:
            ``halted``, ``reduce_only``, ``enabled``, or
            :data:`_LIVE_TRADING_UNAVAILABLE`.
        """
        if self._settings_service is None:
            logger.error(
                f"[{self._get_exchange_name()}] live_trading_mode unreadable: no "
                f"settings service wired — failing closed to blocked"
            )
            return _LIVE_TRADING_UNAVAILABLE
        try:
            async with asyncio.timeout(_LIVE_TRADING_MODE_READ_TIMEOUT_S):
                raw = await self._settings_service.get_setting_fresh(_LIVE_TRADING_MODE_KEY)
        except Exception as exc:
            logger.error(
                f"[{self._get_exchange_name()}] live_trading_mode fresh read failed "
                f"({type(exc).__name__}) — failing closed to blocked"
            )
            return _LIVE_TRADING_UNAVAILABLE
        if raw in (_LIVE_TRADING_HALTED, _LIVE_TRADING_REDUCE_ONLY, _LIVE_TRADING_ENABLED):
            return raw
        logger.error(
            f"[{self._get_exchange_name()}] live_trading_mode is missing or invalid "
            f"({raw!r}) — failing closed to blocked"
        )
        return _LIVE_TRADING_UNAVAILABLE

    async def _is_live_trading_interlocked(
        self, order: OrderRequestData, exchange_name: str
    ) -> bool:
        """Gate a submit on the durable live-trading interlock.

        Paper venues are simulated and ALWAYS pass — the discriminator
        is the executor's venue (never the caller-supplied ``order.mode``,
        which a client could forge); halting paper would kill the
        heartbeat-consult validation loop. For every other venue the
        mode is read fresh per submit: only ``enabled`` proceeds. In
        Phase 0 ``reduce_only`` blocks like ``halted`` (a caller's
        reduce assertion is not proof, and authoritative position truth
        does not exist yet), differing only in the observability reason.
        A block runs the distinct interlock-blocked disposition (a
        durable ``order_interlock_blocked`` terminal, then REJECTED) so a
        replayed frame can never execute after the mode later flips to
        ``enabled``.

        Args:
            order: The incoming submit.
            exchange_name: This executor's venue.

        Returns:
            True when the submit was blocked and disposed of.
        """
        if exchange_name == ExchangeEnum.PAPER:
            return False
        mode = await self._read_live_trading_mode()
        if mode == _LIVE_TRADING_ENABLED:
            return False
        reason = _INTERLOCK_REASON_BY_MODE.get(mode, _INTERLOCK_REASON_MODE_UNAVAILABLE)
        logger.warning(
            f"[{exchange_name}] Order {order.client_order_id} blocked by live-trading "
            f"interlock (mode={mode}, reason={reason}) — distinct interlock disposition"
        )
        await self._handle_interlock_blocked_submit(order, reason)
        return True

    async def _handle_interlock_blocked_submit(self, order: OrderRequestData, reason: str) -> None:
        """Give an interlock-blocked submit its redispatch-safe disposition.

        Mirrors :meth:`_handle_breaker_open_submit`: the interlock is
        authoritative not-submitted but NOT a venue rejection, so the
        command must not blind-retry after the mode flips to ``enabled``.
        The sequence records a durable ``order_interlock_blocked`` event
        (IS submit evidence — the dup guard drops any redispatch), CASes
        the command to FAILED, and ONLY THEN publishes REJECTED so the
        engine releases intent. Any step failing parks the entry with
        ``interlock_blocked_pending`` for the recon loop to rerun.

        Args:
            order: The submit refused by the interlock.
            reason: Stable machine-readable reason for the wire REJECTED.
        """
        if await self._complete_interlock_blocked_disposition(order, reason):
            return
        pending = self.pending_orders.get(order.client_order_id)
        if pending is None:
            pending = PendingOrderState(request=order)
            self.pending_orders[order.client_order_id] = pending
        pending.interlock_blocked_pending = True
        pending.interlock_blocked_reason = reason
        logger.warning(
            f"[{self._get_exchange_name()}] interlock disposition incomplete for "
            f"{order.client_order_id} — entry parked, recon retries (engine intent "
            f"stays held until the durable terminal lands)"
        )

    async def _complete_interlock_blocked_disposition(
        self, order: OrderRequestData, reason: str
    ) -> bool:
        """Run the interlock sequence: record, CAS FAILED, publish REJECTED.

        Idempotent for retries: the durable event write is probe-guarded,
        the lifecycle CAS tolerates an already-FAILED row, and the
        REJECTED publish is engine-idempotent. The pending entry is
        popped only on full success. The durable event and command carry
        the canonical ``order_interlock_blocked`` error; only the wire
        REJECTED reason varies for observability.

        Args:
            order: The interlock-refused order request.
            reason: Stable machine-readable reason for the wire REJECTED.

        Returns:
            True when the full sequence completed.
        """
        exchange_name = self._get_exchange_name()
        cid = order.client_order_id
        try:
            already_recorded = isinstance(
                self.repository, SQLAlchemyRepository
            ) and await self.repository.has_venue_event(cid, _INTERLOCK_BLOCKED_EVENT_TYPE)
            if not already_recorded:
                await self._record_venue_event(
                    {
                        "event_type": _INTERLOCK_BLOCKED_EVENT_TYPE,
                        "exchange_name": exchange_name,
                        "instrument": order.instrument,
                        "client_order_id": cid,
                        "side": order.side,
                        "status": TradeCommandStatusEnum.FAILED.value,
                        "error": _INTERLOCK_BLOCKED_EVENT_TYPE,
                        "strategy_tag": order.strategy_tag,
                    }
                )
        except Exception:
            logger.warning(
                f"[{exchange_name}] {_INTERLOCK_BLOCKED_EVENT_TYPE} event write failed "
                f"for {cid} — retrying via recon"
            )
            return False
        if not await self._fail_command_for_interlock(order):
            return False
        if not await self._publish_order_status(order, OrderEventEnum.REJECTED, reason=reason):
            logger.warning(
                f"[{exchange_name}] REJECTED publish failed for interlock-blocked {cid} "
                f"— retrying via recon (intent must not silently stay held)"
            )
            return False
        self.pending_orders.pop(cid, None)
        return True

    async def _fail_command_for_interlock(self, order: OrderRequestData) -> bool:
        """CAS the interlock-refused command row to FAILED.

        Byte-for-byte the breaker CAS (:meth:`_fail_command_for_breaker`)
        with an ``order_interlock_blocked`` ``last_error``: STRICT cid
        lookup, already-terminal counts as done, otherwise CAS-advance
        through ``(dispatched, direct_dispatched, created) -> failed``.
        Publishing REJECTED before the durable terminal would let a
        still-CREATED row redispatch after the engine released intent, so
        a DB error fails the sequence.

        Args:
            order: The interlock-refused order request.

        Returns:
            True when the row is verifiably terminal (or no SQL
            repository is wired — paper/test mode has no outbox).
        """
        if not isinstance(self.repository, SQLAlchemyRepository):
            return True
        now = datetime.now(UTC)
        try:
            cmd = await self.repository.get_active_create_command_by_client_order_id(
                order.client_order_id, self._get_exchange_name()
            )
            if cmd is None:
                return True
            if cmd["status"] in _COMMAND_TERMINAL_STATUSES:
                return True
            for expected in (
                TradeCommandStatusEnum.DISPATCHED.value,
                TradeCommandStatusEnum.DIRECT_DISPATCHED.value,
                TradeCommandStatusEnum.CREATED.value,
            ):
                if await self.repository.advance_trade_command_lifecycle(
                    public_id=cmd["public_id"],
                    expected_status=expected,
                    new_status=TradeCommandStatusEnum.FAILED.value,
                    bus_time=now,
                    session_id=cmd["session_id"],
                    sequence_id=cmd["sequence_id"],
                    terminal_at=now,
                    last_error=_INTERLOCK_BLOCKED_EVENT_TYPE,
                ):
                    return True
            current = await self.repository.get_current_trade_command_status(cmd["public_id"])
        except Exception as e:
            logger.warning(
                f"[{self._get_exchange_name()}] interlock FAILED CAS errored for "
                f"{order.client_order_id}: {e}"
            )
            return False
        if current is None or current in _COMMAND_TERMINAL_STATUSES:
            return True
        logger.warning(
            f"[{self._get_exchange_name()}] interlock CAS lost for "
            f"{order.client_order_id} (row at {current}) — retrying via recon"
        )
        return False

    async def _retry_interlock_blocked(self, client_order_id: str) -> None:
        """Rerun one parked interlock disposition from the recon loop.

        Args:
            client_order_id: Key into ``pending_orders``.
        """
        pending = self.pending_orders.get(client_order_id)
        if pending is None or not pending.interlock_blocked_pending:
            return
        if await self._complete_interlock_blocked_disposition(
            pending.request, pending.interlock_blocked_reason
        ):
            logger.info(
                f"[{self._get_exchange_name()}] Recon healed the interlock "
                f"disposition for {client_order_id}"
            )

    async def _finalize_accepted_submit(
        self,
        order: OrderRequestData,
        exchange_order_id: str,
        *,
        flush_orphans: bool = True,
        adopted: bool = False,
    ) -> None:
        """Record and publish acceptance of a venue-confirmed live order.

        Runs OUTSIDE the submit try/except on purpose: once the venue
        returned an order id the order IS live, so nothing in
        here may publish REJECTED or pop the pending entry. A failed
        durable ``order_accepted`` write is logged CRITICAL and marked
        on the pending entry (``accept_event_pending``) for the recon
        loop to retry — live correctness (engine learns the order is
        accepted, fills correlate via client_by_exchange) takes priority
        over the durable row, which recon heals.

        Orphaned-execution flushing happens AFTER the durable-write
        attempt: a buffered fill can complete the order and pop its
        pending entry, which would make the ``accept_event_pending``
        flag unsettable if the write failed. Callers that immediately
        project the order from a REST snapshot (the terminal-verified
        ambiguous path) pass ``flush_orphans=False`` to skip the
        BACKGROUND flush here and instead drain the orphan buffer
        inline via ``_flush_orphaned_inline`` before reconciling — full
        WS fill fidelity, no interleaving with the synthetic terminal.

        Args:
            order: The original order request.
            exchange_order_id: Venue-assigned order id from the submit.
            flush_orphans: When False, skip orphaned-execution flushing
                for this order (caller projects fills from REST).
            adopted: When True the ACCEPTED publish carries
                ``reason="adopted"`` — the running engine's re-arm
                signal (#155) — and a FAILED publish parks
                ``adopted_accept_publish_pending`` for the recon loop
                to retry (an ordinary acceptance keeps the historical
                fire-and-forget publish: the engine already holds the
                intent it armed at submit time).
        """
        exchange_name = self._get_exchange_name()
        self.client_by_exchange[exchange_order_id] = order.client_order_id
        accept_event: RecordVenueEventParams = {
            "event_type": "order_accepted",
            "exchange_name": exchange_name,
            "instrument": order.instrument,
            "exchange_order_id": exchange_order_id,
            "client_order_id": order.client_order_id,
            "side": order.side,
            "strategy_tag": order.strategy_tag,
        }
        try:
            await self._record_venue_event(accept_event)
        except Exception:
            logger.critical(
                f"[{exchange_name}] Order {order.client_order_id} accepted as "
                f"{exchange_order_id} but the order_accepted venue event failed to "
                f"persist — order is LIVE; recon will retry the durable write"
            )
            self._unhealed_accept_events[order.client_order_id] = accept_event
            pending = self.pending_orders.get(order.client_order_id)
            if pending is not None:
                pending.accept_event_pending = True
        if flush_orphans:
            self._try_process_orphaned(exchange_order_id, order.client_order_id)
        published = await self._publish_order_status(
            order,
            OrderEventEnum.ACCEPTED,
            exchange_order_id,
            reason=_ADOPTED_REARM_REASON if adopted else None,
        )
        if adopted and not published:
            adopted_pending = self.pending_orders.get(order.client_order_id)
            if adopted_pending is not None:
                adopted_pending.adopted_accept_publish_pending = True
                logger.warning(
                    f"[{exchange_name}] adoption ACCEPTED publish failed for "
                    f"{order.client_order_id} — the running engine's re-arm signal "
                    f"is parked; recon retries until it lands"
                )
        logger.info(
            f"[{exchange_name}] Order {order.client_order_id} "
            f"accepted as {exchange_order_id}, waiting for execution"
        )

    async def _verify_ambiguous_submit(
        self, order: OrderRequestData, pending: PendingOrderState
    ) -> bool:
        """Resolve an ambiguous submit against venue truth by client id.

        Up to three lookups (after 2s/5s/10s — the venue needs a moment
        to materialize an order whose response was lost), each bounded
        to 15s. Outcomes:

        - FOUND: the order is live or was — finalize as accepted; an
          already-terminal snapshot is additionally routed through
          ``_reconcile_disappeared_order`` so its fills and terminal
          event project normally.
        - TWO consecutive authoritative not-found answers: the venue
          never saw the id — now a SAFE definitive rejection. One
          answer is not enough: closed-order listings can lag the
          submit by seconds.
        - Anything else (lookup unsupported, venue unreachable, mixed
          answers): unresolved — the caller parks the order UNKNOWN.

        Blocking is a known, accepted tradeoff: the sequential order
        handler can spend up to ~62s here (3 sleeps + 3 bounded
        lookups) before parking, delaying queued commands for this
        executor. During the outages that produce ambiguity those
        commands would fail venue-side anyway, and resolving the
        current order's truth first is worth more than dispatching the
        next one into the same outage.

        Args:
            order: The original order request.
            pending: The parked pending entry for the order.

        Returns:
            True when the order was resolved (accepted or rejected);
            False when verification could not produce an answer.
        """
        exchange_name = self._get_exchange_name()
        if self.exchange_client is None:
            return False
        not_found_streak = 0
        for delay_s in (2.0, 5.0, 10.0):
            await asyncio.sleep(delay_s)
            try:
                async with asyncio.timeout(_AMBIGUOUS_VERIFY_TIMEOUT_S):
                    snapshot = await self.exchange_client.find_order_by_client_id(
                        order.client_order_id, order.instrument
                    )
            except NotImplementedError:
                logger.warning(
                    f"[{exchange_name}] venue cannot verify orders by client id — "
                    f"parking {order.client_order_id} as UNKNOWN"
                )
                return False
            except Exception as e:
                logger.warning(
                    f"[{exchange_name}] ambiguous-submit verification attempt failed "
                    f"for {order.client_order_id}: {e}"
                )
                not_found_streak = 0
                continue
            if snapshot is not None:
                logger.warning(
                    f"[{exchange_name}] Order {order.client_order_id} VERIFIED on venue "
                    f"as {snapshot.id} (status={snapshot.status}) after ambiguous submit"
                )
                await self._adopt_found_order(order, pending, snapshot)
                return True
            not_found_streak += 1
            if not_found_streak >= 2:
                logger.warning(
                    f"[{exchange_name}] Order {order.client_order_id} verified ABSENT "
                    f"on venue (2 consecutive authoritative answers) — safe to reject"
                )
                if not await self._publish_order_status(order, OrderEventEnum.REJECTED):
                    logger.warning(
                        f"[{exchange_name}] REJECTED publish for "
                        f"{order.client_order_id} failed - entry stays parked for "
                        f"the next cycle (the engine holds its UNKNOWN guard until "
                        f"it actually receives the rejection; popping now would "
                        f"strand that guard forever)"
                    )
                    return False
                await self._record_venue_event(
                    {
                        "event_type": "order_rejected",
                        "exchange_name": exchange_name,
                        "instrument": order.instrument,
                        "client_order_id": order.client_order_id,
                        "side": order.side,
                        "error": "ambiguous submit; venue verified order absent",
                        "strategy_tag": order.strategy_tag,
                    }
                )
                self.pending_orders.pop(order.client_order_id, None)
                return True
        return False

    async def _adopt_found_order(
        self,
        order: OrderRequestData,
        pending: PendingOrderState,
        snapshot: ExchangeOrderSnapshot,
    ) -> None:
        """Adopt an order the venue confirmed exists under our client id.

        Shared by the ambiguous-submit verifier and the stale-frame
        gate: records the venue id on the pending entry, finalizes
        acceptance, and — for an already-terminal snapshot — drains any
        buffered orphan fill inline before projecting through the
        disappeared-order reconciler (full WS fidelity, no background
        interleaving).

        LIVE snapshots finalize with ``adopted=True`` so the ACCEPTED
        publish carries the running-engine re-arm marker (#155);
        TERMINAL snapshots keep the plain reason-less ACCEPTED — marking
        them would re-arm an engine for a cancelled/expired order with
        no clearing publish behind it.

        Args:
            order: The original order request.
            pending: The pending entry tracking the order.
            snapshot: The venue's order snapshot for the client id.
        """
        exchange_name = self._get_exchange_name()
        pending.submit_ambiguous = False
        pending.exchange_order_id = snapshot.id
        is_live = snapshot.status in (
            ExchangeOrderStatusEnum.PENDING,
            ExchangeOrderStatusEnum.PENDING_NEW,
            ExchangeOrderStatusEnum.NEW,
            ExchangeOrderStatusEnum.OPEN,
            ExchangeOrderStatusEnum.PARTIALLY_FILLED,
        )
        await self._finalize_accepted_submit(
            order, snapshot.id, flush_orphans=is_live, adopted=is_live
        )
        if not is_live:
            await self._flush_orphaned_inline(snapshot.id)
            await self._reconcile_disappeared_order(exchange_name, snapshot.id, pending)

    async def _handle_ambiguous_submit(
        self, order: OrderRequestData, error: AmbiguousOrderSubmitError
    ) -> None:
        """Park an ambiguously-failed submit in the UNKNOWN state.

        The venue call failed in a way where the order MAY exist on the
        venue. Publishing REJECTED here would fabricate
        state: the engine would clear its in-flight intent and could
        re-emit a replacement order while the original is live, doubling
        exposure. Instead the pending entry is kept and marked
        ``submit_ambiguous`` and venue verification runs inline: a
        bounded lookup by client id that resolves the order to accepted
        (found — including already-terminal, routed through the
        disappeared-order reconciler) or rejected (two consecutive
        authoritative not-found answers). Only when verification cannot
        resolve — venue unreachable, lookup unsupported — does the
        entry park: a non-terminal ``order_submit_unknown`` venue event
        is recorded and a single UNKNOWN order event is published — the
        engine holds its guard on it and the operator is alerted.

        The durable venue-event write is best-effort here: if it fails
        (likely the same outage), holding the engine guard via the
        UNKNOWN publish matters more than the durable row. The UNKNOWN
        publish itself is retried with short backoff and
        ``unknown_published`` is set ONLY on a confirmed send — a
        swallowed publish failure would leave the engine's in-flight
        timeout free to clear the guard and re-emit while the original
        order may be live. A total publish failure is CRITICAL; the
        recon loop (``_resolve_ambiguous_pending``) re-attempts the
        publish each cycle until a send confirms.

        Args:
            order: The original order request.
            error: The ambiguous failure raised by the venue client.
        """
        exchange_name = self._get_exchange_name()
        pending = self._ensure_pending_order(order)
        pending.submit_ambiguous = True
        logger.error(
            f"[{exchange_name}] Order {order.client_order_id} submit AMBIGUOUS "
            f"(order may exist on venue): {error} (cause: {error.__cause__!r})"
        )
        if await self._verify_ambiguous_submit(order, pending):
            return
        await self._record_submit_unknown(order, error, exchange_name)
        await self._publish_unknown_until_confirmed(order, pending, exchange_name)

    def _ensure_pending_order(self, order: OrderRequestData) -> PendingOrderState:
        """Return the existing pending state for an order or create it."""
        pending = self.pending_orders.get(order.client_order_id)
        if pending is None:
            pending = PendingOrderState(request=order)
            self.pending_orders[order.client_order_id] = pending
        return pending

    async def _record_submit_unknown(
        self,
        order: OrderRequestData,
        error: AmbiguousOrderSubmitError,
        exchange_name: OrderExchange,
    ) -> None:
        """Persist best-effort venue evidence for an ambiguous submit."""
        try:
            await self._record_venue_event(
                {
                    "event_type": "order_submit_unknown",
                    "exchange_name": exchange_name,
                    "instrument": order.instrument,
                    "client_order_id": order.client_order_id,
                    "side": order.side,
                    "error": str(error),
                    "strategy_tag": order.strategy_tag,
                }
            )
        except Exception:
            logger.critical(
                f"[{exchange_name}] order_submit_unknown venue event failed to persist "
                f"for {order.client_order_id} — still publishing UNKNOWN to hold the "
                f"engine guard"
            )

    async def _publish_unknown_until_confirmed(
        self,
        order: OrderRequestData,
        pending: PendingOrderState,
        exchange_name: OrderExchange,
    ) -> None:
        """Publish UNKNOWN status with short retries until one send confirms."""
        if pending.unknown_published:
            return
        for delay_s in (0.0, 0.5, 2.0):
            if delay_s:
                await asyncio.sleep(delay_s)
            if await self._publish_order_status(order, OrderEventEnum.UNKNOWN):
                pending.unknown_published = True
                return
        logger.critical(
            f"[{exchange_name}] UNKNOWN order event for {order.client_order_id} "
            f"could not be published after retries — engine guard may clear on "
            f"timeout; operator must verify this order on the venue"
        )

    async def _process_cancel(self, cancel: OrderCancelData) -> None:
        """Cancel an existing order on the exchange.

        Cleans up pending_orders and client_by_exchange on success. The
        lifecycle pop runs under the order's ``fill_lock``: an
        unserialized pop could yank the entry out from under an in-flight
        fill booking, stranding that fill (uncorrelatable, orphan-TTL
        dropped, invisible to recon once untracked) — the same interleave
        class the stream-side cancellation path serializes against. A
        trailing fill arriving AFTER a completed cancel pop remains
        recon/P0-3 territory: the order is legitimately gone.

        Args:
            cancel: Cancel request data containing order ID to cancel.
        """
        exchange_name = self._get_exchange_name()
        try:
            exchange_client = self._require_exchange_client()
            result = await exchange_client.cancel_order(cancel.exchange_order_id, cancel.instrument)
            await self._handle_cancel_result(cancel, result, exchange_name)
        except Exception as e:
            logger.error(
                f"[{exchange_name}] Error cancelling order {cancel.exchange_order_id}: {e}"
            )
            await self._publish_cancel_event(cancel, OrderEventEnum.REJECTED)

    async def _handle_cancel_result(
        self,
        cancel: OrderCancelData,
        result: ExchangeOrderSnapshot,
        exchange_name: OrderExchange,
    ) -> None:
        """Publish and persist the result of a venue cancel attempt."""
        if result and result.status == ExchangeOrderStatusEnum.CANCELED:
            await self._finalize_successful_cancel(cancel)
            await self._publish_cancel_event(cancel, OrderEventEnum.CANCELLED)
            logger.info(
                f"[{exchange_name}] Order {cancel.exchange_order_id} cancelled successfully"
            )
            return
        await self._publish_cancel_event(cancel, OrderEventEnum.REJECTED)
        logger.warning(f"[{exchange_name}] Cancel request for {cancel.exchange_order_id} failed")

    async def _finalize_successful_cancel(self, cancel: OrderCancelData) -> None:
        """Clean tracked order state and persist the terminal cancel status."""
        exchange_client = self._require_exchange_client()
        client_id = self.client_by_exchange.get(cancel.exchange_order_id)
        holder = self.pending_orders.get(client_id) if client_id else None
        if holder is None:
            self.client_by_exchange.pop(cancel.exchange_order_id, None)
            return
        async with holder.fill_lock:
            self.client_by_exchange.pop(cancel.exchange_order_id, None)
            pending = self.pending_orders.pop(client_id, None) if client_id else None
            if pending and pending.db_order_id is not None:
                await exchange_client._log_order_update_to_db(
                    db_order_id=pending.db_order_id,
                    status=ExchangeOrderStatusEnum.CANCELED,
                )

    async def _process_replace(self, replace: OrderReplaceData) -> None:
        """Reject an order-replace request (replace is not implemented).

        No exchange client implements atomic replace here; the request
        is logged and answered with a REJECTED replace event so the
        caller can fall back to an explicit cancel + new order flow.

        Args:
            replace: Replace request data containing new order parameters.
        """
        exchange_name = self._get_exchange_name()
        logger.warning(
            f"[{exchange_name}] Order replace not yet implemented for {replace.exchange_order_id}. "
            f"Consider cancel + new order workflow."
        )
        await self._publish_replace_event(replace, OrderEventEnum.REJECTED)

    async def _publish_cancel_event(self, cancel: OrderCancelData, event: CancelEventType) -> None:
        """Publish cancel event to orders.events.*.*.cancelled or rejected.

        Uses lightweight OrderEventData since cancel commands do not carry
        full order details (side/order_type are not needed).

        Args:
            cancel: Original cancel request.
            event: Event type (must be 'cancelled' or 'rejected').
        """
        if not self.msg_publisher or not self.running:
            return
        exchange_name = self._get_exchange_name()
        now = datetime.now(UTC)
        try:
            topic = order_event_topic(exchange_name, cancel.instrument, event)
            order_event = OrderEventData(
                public_id=str(uuid7()),
                timestamp=now,
                session_id=self._tracker.session_id,
                sequence_id=self._tracker.next_sequence(topic),
                exchange_order_id=cancel.exchange_order_id,
                client_order_id=cancel.client_order_id,
                exchange=exchange_name,
                instrument=cancel.instrument,
                event=event,
            )
            await self.msg_publisher.send(topic, order_event)
            logger.info(
                f"[{exchange_name}] Published cancel event: {cancel.exchange_order_id} - {event}"
            )
        except Exception as e:
            logger.error(f"[{exchange_name}] Error publishing cancel event: {e}")

    async def _publish_stream_terminal_event(
        self,
        *,
        event: StreamTerminalEventType,
        exchange_order_id: str,
        client_order_id: str,
        instrument: str,
        exchange_name: OrderExchange,
        pending: PendingOrderState | None,
    ) -> None:
        """Publish a bus terminal for a venue-stream cancel/expiry.

        Historically a terminal arriving on the EXECUTION STREAM
        (venue-initiated cancel, TIF expiry, the paper simulator's
        unpriceable-market cancel) updated the order row and dropped
        correlation WITHOUT any bus event — the engine's in-flight
        guard then starved until the lazy 60s valve, dropping every
        interim signal for the instrument (incident 2026-07-10 #2).
        Publishing an :class:`OrderEventData` with the ``cancelled`` /
        ``expired`` suffix feeds the trader's existing
        ``_handle_order_event`` release (intent clear + paired-leg
        projection) with zero consumer change.

        Best-effort by design: the durable ``order_terminal`` venue
        event is already written before this publish, and a publish
        failure merely degrades to the pre-fix status quo (the 60s
        valve backstop) — liveness, not safety — so failures log a
        WARNING and never block the correlation cleanup.

        Args:
            event: ``cancelled`` or ``expired`` (wire vocabulary).
            exchange_order_id: Venue-assigned order id.
            client_order_id: Command/order client id (the engine's
                intent key).
            instrument: Native venue symbol for the topic.
            exchange_name: Executor's venue discriminator.
            pending: Tracked order state, when still correlated —
                supplies wallet/operator/user attribution copied onto
                the event (recon-synthetic terminals may lack it).
        """
        if not self.msg_publisher or not self.running:
            logger.warning(
                f"[{exchange_name}] Stream terminal {event} for {client_order_id} "
                "NOT published (publisher unavailable); engine guard falls back "
                "to the in-flight timeout valve"
            )
            return
        now = datetime.now(UTC)
        request = pending.request if pending is not None else None
        try:
            topic = order_event_topic(exchange_name, instrument, event)
            payload = OrderEventData(
                public_id=str(uuid7()),
                timestamp=now,
                session_id=self._tracker.session_id,
                sequence_id=self._tracker.next_sequence(topic),
                exchange_order_id=exchange_order_id,
                client_order_id=client_order_id,
                exchange=exchange_name,
                instrument=instrument,
                event=event,
                wallet_public_id=request.wallet_public_id if request is not None else "",
                operator_public_id=request.operator_public_id if request is not None else None,
                user_public_id=request.user_public_id if request is not None else None,
            )
            await self.msg_publisher.send(topic, payload)
            logger.info(
                f"[{exchange_name}] Published stream terminal event: {client_order_id} - {event}"
            )
        except Exception as e:
            logger.warning(
                f"[{exchange_name}] Failed to publish stream terminal {event} for "
                f"{client_order_id}: {e}; engine guard falls back to the "
                "in-flight timeout valve"
            )

    async def _publish_replace_event(
        self, replace: OrderReplaceData, event: ReplaceEventType
    ) -> None:
        """Publish replace event to orders.events.*.*.replaced or rejected.

        Uses lightweight OrderEventData since replace commands do not carry
        full order details (only identifiers and new values).

        Args:
            replace: Original replace request.
            event: Event type (must be 'replaced' or 'rejected').
        """
        if not self.msg_publisher or not self.running:
            return
        exchange_name = self._get_exchange_name()
        now = datetime.now(UTC)
        try:
            topic = order_event_topic(exchange_name, replace.instrument, event)
            order_event = OrderEventData(
                public_id=str(uuid7()),
                timestamp=now,
                session_id=self._tracker.session_id,
                sequence_id=self._tracker.next_sequence(topic),
                exchange_order_id=replace.exchange_order_id,
                client_order_id=replace.client_order_id,
                exchange=exchange_name,
                instrument=replace.instrument,
                event=event,
            )
            await self.msg_publisher.send(topic, order_event)
            logger.info(
                f"[{exchange_name}] Published replace event: {replace.exchange_order_id} - {event}"
            )
        except Exception as e:
            logger.error(f"[{exchange_name}] Error publishing replace event: {e}")

    async def _execute_live_order(self, order: OrderRequestData) -> str | None:
        """Execute an order on the exchange and return the exchange order ID.

        Exceptions PROPAGATE to the caller: the previous
        blanket except-return-None coerced every failure — including
        ambiguous network failures where the order may have executed —
        into the definitive-reject path. ``_process_order`` now
        distinguishes ``AmbiguousOrderSubmitError`` (UNKNOWN/verify
        path) from genuine errors (reject path); a ``None``/empty-id
        return remains the definitive venue-side rejection signal.

        Args:
            order: Order request data containing order details.

        Returns:
            Exchange order ID if accepted, None when the venue
            definitively rejected the submit.

        Raises:
            AmbiguousOrderSubmitError: If the venue call failed in a way
                where the order MAY exist on the venue.
            Exception: Any other submit failure (definitive reject).
        """
        exchange_name = self._get_exchange_name()
        order_request = _exchange_order_request_from_core(order, self.wallet_public_id)
        exchange_client = self._require_exchange_client()
        result = await exchange_client.create_order(order_request)
        exchange_order_id = result.id if result else None
        if exchange_order_id:
            pending = self.pending_orders.get(order.client_order_id)
            if pending is not None:
                pending.exchange_order_id = exchange_order_id
                pending.db_order_id = result.db_order_id
                pending.order_public_id = result.db_order_public_id
            logger.info(
                f"[{exchange_name}] ExchangeOrderSnapshot submitted: {order.client_order_id} -> "
                f"{exchange_order_id}, waiting for execution via WebSocket"
            )
        return exchange_order_id

    async def _publish_execution(self, topic: str, fill: ExecutionData) -> bool:
        """Send a complete fill notification to ZMQ.

        Args:
            topic: ZMQ topic for routing.
            fill: Complete ExecutionData with provenance set.

        Returns:
            True when the fill was handed to the publisher without error;
            False when the publisher is unavailable or the send raised.
            The caller's dedupe registration MUST check this — a swallowed
            send failure that still registered the exec id / advanced the
            seen cumulative would make the venue's redelivery look like a
            replay and drop the fill for good.
        """
        if not self.msg_publisher or not self.running:
            return False
        exchange_name = self._get_exchange_name()
        try:
            await self.msg_publisher.send(topic, fill)
            logger.info(
                f"[{exchange_name}] Published fill: {fill.client_order_id} - "
                f"{fill.size}@{fill.price}"
            )
        except Exception as e:
            logger.error(f"[{exchange_name}] Error publishing fill: {e}")
            return False
        return True

    async def _record_venue_event(self, params: RecordVenueEventParams) -> None:
        """Write a VenueEvent row to DB if repository supports it.

        Silent no-op if repository is not an SQLAlchemyRepository
        or is not set. Write failures are FAIL-CLOSED: the error is
        logged and RE-RAISED — durable mode requires every venue event
        persisted before the corresponding ZMQ publish, so callers must
        decide per call site whether a failed write may abort the flow
        (it must NOT for an already-accepted live order).

        Args:
            params: Event data including event_type, exchange_name, instrument,
                and optional fill/error fields.
        """
        if not isinstance(self.repository, SQLAlchemyRepository):
            return
        event_type = params.get("event_type")
        exchange_name = params.get("exchange_name")
        instrument = params.get("instrument")
        if event_type is None or exchange_name is None or instrument is None:
            logger.error("Venue event params missing required identifiers: {}", params)
            return
        mode = (
            ExecutionModeEnum.PAPER
            if exchange_name == ExchangeEnum.PAPER
            else ExecutionModeEnum.LIVE
        )
        strategy_tag = params.get("strategy_tag")
        shard_key = compute_shard_key(
            instrument=instrument,
            exchange=cast(OrderExchange, exchange_name),
            mode=mode,
            wallet_public_id=self.wallet_public_id,
            strategy_tag=strategy_tag,
        )
        now = datetime.now(UTC)
        try:
            await self.repository.insert_venue_event(
                {
                    "event_type": event_type,
                    "shard_key": shard_key,
                    "wallet_public_id": self.wallet_public_id,
                    "exchange": exchange_name,
                    "instrument": instrument,
                    "mode": mode,
                    "received_at": now,
                    "session_id": self._tracker.session_id,
                    "sequence_id": self._tracker.next_sequence(f"venue.{shard_key}"),
                    "timestamp": now,
                    "exchange_order_id": params.get("exchange_order_id"),
                    "client_order_id": params.get("client_order_id"),
                    "side": params.get("side"),
                    "status": params.get("status"),
                    "fill_price": params.get("fill_price"),
                    "fill_size": params.get("fill_size"),
                    "cum_fill_size": params.get("cum_fill_size"),
                    "fee": params.get("fee"),
                    "fee_asset": params.get("fee_asset"),
                    "exec_id": params.get("exec_id"),
                    "trade_id": params.get("trade_id"),
                    "error": params.get("error"),
                    "venue_timestamp": params.get("venue_timestamp"),
                    "liquidity_role": params.get("liquidity_role") or "unknown",
                }
            )
        except Exception:
            logger.error(
                f"[{exchange_name}] FAIL-CLOSED: venue event {event_type} write failed "
                f"for {instrument}. Durable mode requires all events persisted."
            )
            raise

    async def _reconciliation_handler(self) -> None:
        """Periodic exchange API reconciliation.

        Compares exchange order state with local pending orders.
        Detects fill gaps and disappeared orders. Logs balance mismatches.
        Uses existing execution flow for corrective fills.
        """
        interval = 60.0
        exchange_name = self._get_exchange_name()
        while self.running:
            await asyncio.sleep(interval)
            try:
                await self._reconcile_with_exchange()
                self._venue_recon_failure_count = 0
                self._last_venue_recon_error = ""
                self._task_last_pass["reconciliation"] = time.monotonic()
            except TimeoutError as exc:
                self._record_venue_recon_failure(exc)
                logger.error(
                    f"[{exchange_name}] Reconciliation cycle exceeded "
                    f"{_RECON_CYCLE_TIMEOUT_S:.0f}s and was cancelled (wedged venue "
                    f"call?) - lock released, retrying next cycle"
                )
            except httpx.HTTPError as exc:
                self._record_venue_recon_failure(exc)
                logger.warning(
                    "[{}] Reconciliation cycle transient HTTP error — will retry on next cycle: {}",
                    exchange_name,
                    exc,
                )
            except Exception as exc:
                self._record_venue_recon_failure(exc)
                logger.exception(f"[{exchange_name}] Reconciliation cycle failed")

    def _record_venue_recon_failure(self, exc: BaseException) -> None:
        """Record a venue REST reconciliation failure for heartbeat consumers."""
        self._venue_recon_failure_count += 1
        self._last_venue_recon_error = str(exc) or exc.__class__.__name__

    def _record_portfolio_reconciliation_failure(self, exc: BaseException) -> None:
        """Record an observer-side portfolio failure without affecting order healing."""
        self._portfolio_reconciliation_failure_count += 1
        self._last_portfolio_reconciliation_error = str(exc) or exc.__class__.__name__

    async def _close_and_drain_portfolio_reconciliation(self) -> None:
        """Close observer dispatch and drain reconciliation plus notify work."""
        self._portfolio_reconciliation_dispatch_open = False
        reconciliation_tasks = tuple(self._portfolio_reconciliation_tasks.values())
        notification_tasks = tuple(self._portfolio_drift_notification_tasks)
        tasks = reconciliation_tasks + notification_tasks
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self._portfolio_reconciliation_tasks.clear()
        self._portfolio_drift_notification_tasks.clear()

    def _portfolio_reconciliation_task_done(
        self,
        key: _PortfolioReconciliationKey,
        task: asyncio.Task[None],
    ) -> None:
        """Retrieve one task outcome and remove only its exact tracked instance."""
        if self._portfolio_reconciliation_tasks.get(key) is task:
            self._portfolio_reconciliation_tasks.pop(key, None)
        if task.cancelled():
            return
        error = task.exception()
        if error is not None:
            self._record_portfolio_reconciliation_failure(error)
            logger.error(
                "[{}] portfolio reconciliation task escaped containment: {}",
                key[1],
                error,
            )

    def _schedule_portfolio_reconciliation(
        self,
        *,
        state_id: int,
        attempt: VenueAccountAttemptRow,
        position_capability: CapabilityStatus,
    ) -> None:
        """Schedule one live snapshot evaluation without delaying its observer."""
        try:
            if attempt["mode"] != "live" or not self._portfolio_reconciliation_dispatch_open:
                return
            key: _PortfolioReconciliationKey = (
                attempt["wallet_public_id"],
                attempt["exchange"],
                attempt["mode"],
                attempt["session_id"],
                attempt["sequence_id"],
            )
            existing = self._portfolio_reconciliation_tasks.get(key)
            if existing is not None and not existing.done():
                return
            work = _PortfolioReconciliationWork(
                state_id=state_id,
                identity=key,
                position_capability=position_capability,
            )
            runner = self._run_portfolio_reconciliation(work)
            try:
                task = asyncio.create_task(
                    runner,
                    name=(f"portfolio-reconciliation:{key[0]}:{key[1]}:{key[2]}:{key[3]}:{key[4]}"),
                )
            except Exception:
                runner.close()
                raise
            self._portfolio_reconciliation_tasks[key] = task
            task.add_done_callback(
                lambda completed: self._portfolio_reconciliation_task_done(key, completed)
            )
        except Exception as exc:
            self._record_portfolio_reconciliation_failure(exc)
            logger.exception(
                f"[{attempt.get('exchange', 'unknown')}] "
                f"portfolio reconciliation scheduling failed: {exc}"
            )

    def _schedule_portfolio_drift_notification(
        self,
        evaluation: PortfolioReconciliationEvaluationRow,
    ) -> None:
        """Start isolated post-commit drift lifecycle publication.

        Args:
            evaluation: The exact evaluation whose reconciliation transaction
                has already committed.
        """
        runner = self._publish_portfolio_drift_notification(evaluation)
        try:
            task = asyncio.create_task(
                runner,
                name=(
                    "portfolio-drift-notify:"
                    f"{evaluation['wallet_public_id']}:{evaluation['exchange']}:"
                    f"{evaluation['session_id']}:{evaluation['sequence_id']}"
                ),
            )
        except Exception as exc:
            runner.close()
            logger.warning(
                "portfolio drift notification scheduling failed: {}",
                exc,
            )
            return
        self._portfolio_drift_notification_tasks.add(task)
        task.add_done_callback(self._portfolio_drift_notification_tasks.discard)

    async def _publish_portfolio_drift_notification(
        self,
        evaluation: PortfolioReconciliationEvaluationRow,
    ) -> None:
        """Publish one committed open or resolved transition without blocking.

        The exact evaluation tuple is looked up only after the reconciliation
        transaction commits. Missing, continued-open, and non-drift results are
        silent. Every lookup, schema, and non-blocking ZMQ failure is contained
        here so notification delivery can never alter reconciliation, order,
        position, or trading state.

        Args:
            evaluation: The committed reconciliation evaluation provenance.
        """
        try:
            repository = self._require_sqlalchemy_repository()
            transition = await repository.get_portfolio_drift_episode_transition(
                evaluation["wallet_public_id"],
                evaluation["exchange"],
                evaluation["mode"],
                evaluation["session_id"],
                evaluation["sequence_id"],
            )
            if transition is None or transition["mode"] != "live":
                return
            lifecycle: Literal["opened", "resolved"]
            closed_at: datetime | None
            resolution_reason: Literal["matched"] | None
            if transition["status"] == "open":
                if (
                    transition["latest_full_mismatch_count"] != 3
                    or transition["trigger_observation_id"] != transition["last_observation_id"]
                ):
                    return
                lifecycle = "opened"
                closed_at = None
                resolution_reason = None
            elif transition["status"] == "resolved":
                if transition["closed_at"] is None or transition["resolution_reason"] != "matched":
                    return
                lifecycle = "resolved"
                closed_at = transition["closed_at"]
                resolution_reason = "matched"
            else:
                return
            publisher = self.msg_publisher
            if publisher is None:
                return
            now = datetime.now(UTC)
            tracker = publisher.tracker
            event = PortfolioDriftEpisodeEventData(
                sequence_id=tracker.next_sequence(_PORTFOLIO_DRIFT_EPISODE_TOPIC),
                public_id=str(uuid7()),
                timestamp=now,
                session_id=tracker.session_id,
                wallet_public_id=transition["wallet_public_id"],
                exchange=transition["exchange"],
                mode="live",
                episode_public_id=transition["public_id"],
                lifecycle=lifecycle,
                opened_at=transition["opened_at"],
                closed_at=closed_at,
                mismatch_count=transition["latest_full_mismatch_count"],
                resolution_reason=resolution_reason,
            )
            await publisher.send(
                _PORTFOLIO_DRIFT_EPISODE_TOPIC,
                event,
                flags=zmq.NOBLOCK,
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.warning(
                "portfolio drift notification failed after reconciliation commit: {}",
                exc,
            )

    async def _run_portfolio_reconciliation(self, work: _PortfolioReconciliationWork) -> None:
        """Evaluate and persist one exact account-state version within a hard bound."""
        repository = self._require_sqlalchemy_repository()
        wallet_public_id, exchange, mode, session_id, sequence_id = work.identity
        try:
            async with asyncio.timeout(_PORTFOLIO_RECONCILIATION_TIMEOUT_S):
                if await repository.has_portfolio_reconciliation_evaluation(
                    wallet_public_id,
                    exchange,
                    mode,
                    session_id,
                    sequence_id,
                ):
                    return
                evaluated_at = datetime.now(UTC)
                state_row = await repository.get_venue_account_state_version(work.state_id)
                if state_row is None:
                    raise RuntimeError("venue account state version is unavailable")
                state_identity: _PortfolioReconciliationKey = (
                    state_row["wallet_public_id"],
                    state_row["exchange"],
                    state_row["mode"],
                    state_row["session_id"],
                    state_row["sequence_id"],
                )
                if state_identity != work.identity:
                    raise ValueError("venue account state version identity mismatch")
                account = build_portfolio_account_state(state_row, evaluated_at)
                method_config = await repository.get_active_portfolio_reconciliation_method_config(
                    wallet_public_id,
                    exchange,
                    mode,
                )
                evaluation = await dispatch_portfolio_reconciliation(
                    repository=repository,
                    account=account,
                    method_config=method_config,
                    position_capability=work.position_capability,
                    evaluated_at=evaluated_at,
                )
                await repository.record_portfolio_reconciliation(evaluation)
                self._schedule_portfolio_drift_notification(evaluation)
        except asyncio.CancelledError:
            raise
        except TimeoutError as exc:
            self._record_portfolio_reconciliation_failure(exc)
            logger.error(
                f"[{exchange}] portfolio reconciliation exceeded "
                f"{_PORTFOLIO_RECONCILIATION_TIMEOUT_S:.0f}s"
            )
        except Exception as exc:
            self._record_portfolio_reconciliation_failure(exc)
            logger.exception(f"[{exchange}] portfolio reconciliation failed: {exc}")

    async def _account_observer_handler(self) -> None:
        """Supervised loop that observes and persists venue account truth (Phase 3).

        Fully INDEPENDENT of order reconciliation: it runs on its own cadence,
        its per-call fetches are separately bounded, and its failures increment
        only ``_account_observer_failure_count`` — never the order-recon
        counter and never the order-healing path. Observes once immediately on
        start, then every ``_ACCOUNT_OBSERVE_INTERVAL_S``. Each cycle records a
        snapshot regardless of outcome (a failed read is persisted as an
        ``error`` observation so the last-good state stays visibly stale rather
        than vanishing).
        """
        exchange_name = self._get_exchange_name()
        while self.running:
            try:
                await self._observe_account_once()
                self._task_last_pass["account_observer"] = time.monotonic()
            except Exception as exc:
                self._account_observer_failure_count += 1
                logger.exception(f"[{exchange_name}] account observation cycle failed: {exc}")
            await asyncio.sleep(_ACCOUNT_OBSERVE_INTERVAL_S)

    async def _observe_account_once(self) -> None:
        """Observe balances + positions once and persist the account snapshot.

        Balance and positions are SEPARATE venue reads with independent
        statuses and timestamps (not an atomic snapshot). The stored roll-up,
        retention, and provenance are all owned by the repository; this method
        only supplies the raw attempt. Persists nothing when no durable
        repository is attached.
        """
        client = self.exchange_client
        repository = self.repository
        if client is None or not isinstance(repository, SQLAlchemyRepository):
            return
        now = datetime.now(UTC)
        (
            balance_status,
            balances_json,
            balance_observed_at,
            balance_error,
        ) = await self._read_account_balances(client, now)
        (
            position_status,
            open_positions_json,
            position_observed_at,
            position_error,
        ) = await self._read_account_positions(client, now)
        authoritative_until: datetime | None = None
        if balance_status in ("observed", "simulated") and balance_observed_at is not None:
            authoritative_until = balance_observed_at + timedelta(
                seconds=_ACCOUNT_FRESHNESS_CEILING_S
            )
        exchange_name = self._get_exchange_name()
        attempt: VenueAccountAttemptRow = {
            "wallet_public_id": self.wallet_public_id,
            "exchange": exchange_name,
            "mode": self._account_mode(),
            "balance_status": balance_status,
            "position_status": position_status,
            "valuation_status": "native_only",
            "balances_json": balances_json,
            "open_positions_json": open_positions_json,
            "balance_observed_at": balance_observed_at,
            "position_observed_at": position_observed_at,
            "authoritative_until": authoritative_until,
            "error": balance_error or position_error,
            "session_id": self._tracker.session_id,
            "sequence_id": self._tracker.next_sequence(
                f"account.{exchange_name}.{self.wallet_public_id}"
            ),
            "bus_time": now,
        }
        state_id = await repository.record_venue_account_snapshot(attempt)
        self._schedule_portfolio_reconciliation(
            state_id=state_id,
            attempt=attempt,
            position_capability=client.position_capability,
        )

    def _account_mode(self) -> str:
        """Return the account-truth mode for this executor (paper venue → paper)."""
        if self._get_exchange_name() == ExchangeEnum.PAPER:
            return "paper"
        return "live"

    async def _read_account_balances(
        self, client: ExchangeClientBase, now: datetime
    ) -> tuple[str, str | None, datetime | None, str | None]:
        """Read native balances, mapping capability/outcome to a fail-closed status.

        Returns ``(balance_status, balances_json, balance_observed_at, error)``.
        An ``unsupported`` venue never calls the reader; a structural
        ``NotImplementedError`` is ``unsupported``; any other failure (incl.
        timeout) is ``error`` with the payload/timestamp left NULL so the
        repository retains the last-good balance stale-visible. A successful
        read is ``observed`` (or ``simulated`` for paper) with the serialized
        native balances (an empty account serializes to ``[]``, never NULL).

        Args:
            client: The executor's authenticated venue client.
            now: The observation instant.

        Returns:
            The balance status tuple.
        """
        capability = client.balance_capability
        if capability is CapabilityStatus.UNSUPPORTED:
            return "unsupported", None, None, None
        try:
            async with asyncio.timeout(_ACCOUNT_FETCH_TIMEOUT_S):
                entries = await client.read_native_balances()
        except NotImplementedError:
            return "unsupported", None, None, None
        except Exception as exc:
            logger.warning(f"[{self._get_exchange_name()}] native balance read failed: {exc}")
            return "error", None, None, (str(exc) or exc.__class__.__name__)[:512]
        if capability is CapabilityStatus.SUPPORTED:
            return "observed", self._serialize_native_balances(entries), now, None
        if capability is CapabilityStatus.SIMULATED:
            return "simulated", self._serialize_native_balances(entries), now, None
        return "error", None, None, _ACCOUNT_UNEXPECTED_BALANCE_CAPABILITY_MSG

    async def _read_account_positions(
        self, client: ExchangeClientBase, now: datetime
    ) -> tuple[str, str | None, datetime | None, str | None]:
        """Read native positions, mapping capability/outcome to a fail-closed status.

        Returns ``(position_status, open_positions_json, position_observed_at,
        error)``. ``not_applicable`` (venue has no positions) and
        ``unsupported`` never call the reader and clear the component; a
        structural ``NotImplementedError`` is ``unsupported``; any other
        failure is ``error``. A successful read is ``observed`` with the
        serialized positions (empty book → ``[]``, never NULL).

        Args:
            client: The executor's authenticated venue client.
            now: The observation instant.

        Returns:
            The position status tuple.
        """
        capability = client.position_capability
        if capability is CapabilityStatus.NOT_APPLICABLE:
            return "not_applicable", None, None, None
        if capability is CapabilityStatus.UNSUPPORTED:
            return "unsupported", None, None, None
        try:
            async with asyncio.timeout(_ACCOUNT_FETCH_TIMEOUT_S):
                positions = await client.read_native_positions()
        except NotImplementedError:
            return "unsupported", None, None, None
        except Exception as exc:
            logger.warning(f"[{self._get_exchange_name()}] native position read failed: {exc}")
            return "error", None, None, (str(exc) or exc.__class__.__name__)[:512]
        if capability is CapabilityStatus.SUPPORTED:
            return "observed", self._serialize_open_positions(positions), now, None
        return "error", None, None, _ACCOUNT_UNEXPECTED_POSITION_CAPABILITY_MSG

    @staticmethod
    def _serialize_native_balances(entries: list[NativeBalanceEntry]) -> str:
        """Serialize native balance entries to a stable JSON array (Phase 3)."""
        payload: list[dict[str, str | float | None]] = []
        for entry in entries:
            item: dict[str, str | float | None] = {
                "currency": entry.currency,
                "total": entry.total,
                "free": entry.free,
                "used": entry.used,
            }
            if (
                entry.total_decimal is not None
                or entry.free_decimal is not None
                or entry.used_decimal is not None
            ):
                item["total_decimal"] = entry.total_decimal
                item["free_decimal"] = entry.free_decimal
                item["used_decimal"] = entry.used_decimal
                item["numeric_provenance"] = entry.numeric_provenance
            payload.append(item)
        return json.dumps(payload)

    @staticmethod
    def _serialize_open_positions(positions: list[OpenPositionSnapshot]) -> str:
        """Serialize open positions to a stable JSON array (Phase 3)."""
        return json.dumps(
            [
                {
                    "symbol": p.symbol,
                    "side": p.side.value,
                    "size": p.size,
                    "entry_price": p.entry_price,
                    "mark_price": p.mark_price,
                    "unrealized_pnl": p.unrealized_pnl,
                    "unrealized_funding": p.unrealized_funding,
                    "timestamp": p.timestamp.isoformat(),
                }
                for p in positions
            ]
        )

    async def _reconcile_with_exchange(self) -> None:
        """Run one reconciliation cycle, serialized on ``_recon_lock``.

        Two callers exist: the periodic 60s handler and the stream
        supervisor's post-reconnect heal. Unserialized, two concurrent
        cycles could both observe the same fill gap and double-emit the
        corrective execution — the lock became necessary exactly when the
        second caller appeared.

        The whole cycle INCLUDING lock acquisition is bounded by
        ``_RECON_CYCLE_TIMEOUT_S``: a hung venue call used to hold the
        lock forever, wedging both callers permanently. Cancellation via
        the timeout releases the lock (async-with), and the next periodic
        cycle recomputes any cancelled corrective idempotently (stable
        synthetic exec ids).

        Raises:
            TimeoutError: When the cycle exceeded the bound; callers log
                and retry on their own schedule.
        """
        async with asyncio.timeout(_RECON_CYCLE_TIMEOUT_S):
            async with self._recon_lock:
                await self._reconcile_with_exchange_unlocked()

    async def _post_reconnect_reconcile(self) -> None:
        """Best-effort recon pass before re-entering a respawned stream.

        Heals the dark-window fill/terminal gap promptly (instead of
        waiting up to a full periodic cycle) and orders "recon heals
        first, then resubscribe with no snapshot" — closing the window
        where a corrective fill and a venue replay could interleave.
        Failures are logged and never block the resubscribe: the periodic
        cycle remains the steady-state backstop.
        """
        try:
            await self._reconcile_with_exchange()
        except Exception:
            logger.exception(
                f"[{self._get_exchange_name()}] Post-reconnect reconciliation "
                f"failed - resubscribing anyway"
            )

    async def _reconcile_with_exchange_unlocked(self) -> None:
        """Run one reconciliation cycle against the exchange API.

        Queries exchange for current order state and balances,
        compares with local pending orders, and processes corrective
        fills for detected gaps. Uses get_order() to verify the actual
        terminal status of disappeared orders (not just absent from
        open set). Parked ambiguous-submit entries (no exchange id yet,
        ``submit_ambiguous`` set) get a fresh venue-verification round
        each cycle, and entries whose durable ``order_accepted`` write
        failed (``accept_event_pending``) get the write retried until
        it sticks — the 60s loop is the steady-state resolver for
        everything the inline paths could not settle.
        """
        if self.exchange_client is None:
            return
        exchange_name = self._get_exchange_name()

        await self._retry_pending_reconciliation_work()

        pending_at_snapshot = set(self.pending_orders)
        exchange_orders = await self.exchange_client.get_orders(status=ExchangeOrderStatusEnum.OPEN)
        exchange_by_id = {o.id: o for o in exchange_orders}

        await self._resolve_ambiguous_reconciliation_batch(exchange_name)
        await self._reconcile_tracked_orders(exchange_name, exchange_by_id)

        adopted_this_cycle: set[str] = set()
        await self._adopt_ghost_orders(exchange_orders, adopted_this_cycle, pending_at_snapshot)
        await self._verify_unresolved_dispatched(adopted_this_cycle)

        await self._warn_on_balance_mismatches(exchange_name)

    async def _retry_pending_reconciliation_work(self) -> None:
        """Retry durable work that previous order/recon paths left pending."""
        for client_order_id in tuple(self._unhealed_accept_events):
            await self._retry_accept_event(client_order_id)

        for restore_public_id in tuple(self._pending_rejected_restores):
            await self._retry_rejected_restore(restore_public_id)

        for breaker_cid, breaker_entry in tuple(self.pending_orders.items()):
            if breaker_entry.breaker_open_pending:
                await self._retry_breaker_open(breaker_cid)

        for interlock_cid, interlock_entry in tuple(self.pending_orders.items()):
            if interlock_entry.interlock_blocked_pending:
                await self._retry_interlock_blocked(interlock_cid)

        for adopted_cid, adopted_entry in tuple(self.pending_orders.items()):
            if adopted_entry.adopted_accept_publish_pending:
                await self._retry_adopted_accept_publish(adopted_cid)

    async def _retry_adopted_accept_publish(self, client_order_id: str) -> None:
        """Retry a parked adoption-shaped ACCEPTED publish (#155).

        The adopted ACCEPTED frame is the running engine's ONLY re-arm
        signal, so a failed publish may not be dropped while this
        executor lives (an executor restart self-heals differently: the
        startup recovery republishes for every venue-verified-open
        recovered order). Success CLEARS the flag — periodic republish
        would keep refreshing the engine's in-flight window and starve
        the lazy timeout valve.
        """
        pending = self.pending_orders.get(client_order_id)
        if pending is None or not pending.adopted_accept_publish_pending:
            return
        published = await self._publish_order_status(
            pending.request,
            OrderEventEnum.ACCEPTED,
            pending.exchange_order_id,
            reason=_ADOPTED_REARM_REASON,
        )
        if published:
            pending.adopted_accept_publish_pending = False
            logger.info(
                f"[{self._get_exchange_name()}] parked adoption ACCEPTED publish "
                f"landed for {client_order_id} — engine re-arm signal delivered"
            )

    async def _resolve_ambiguous_reconciliation_batch(self, exchange_name: OrderExchange) -> None:
        """Resolve a fairness-capped slice of parked ambiguous orders."""
        ambiguous_budget = _AMBIGUOUS_VERIFY_PER_CYCLE_MAX
        ambiguous_cids = [
            cid
            for cid, entry in self.pending_orders.items()
            if not entry.exchange_order_id and entry.submit_ambiguous
        ]
        if ambiguous_cids:
            start = self._ambiguous_rotation_offset % len(ambiguous_cids)
            ambiguous_cids = ambiguous_cids[start:] + ambiguous_cids[:start]
            self._ambiguous_rotation_offset = start + min(ambiguous_budget, len(ambiguous_cids))
        for cid in ambiguous_cids[:ambiguous_budget]:
            entry = self.pending_orders.get(cid)
            if entry is None or entry.exchange_order_id:
                continue
            await self._resolve_ambiguous_pending(entry)
        if len(ambiguous_cids) > ambiguous_budget:
            logger.info(
                f"[{exchange_name}] Recon: {len(ambiguous_cids) - ambiguous_budget} "
                f"parked ambiguous orders deferred to later cycles (fairness cap "
                f"{ambiguous_budget}/cycle)"
            )

    async def _reconcile_tracked_orders(
        self,
        exchange_name: OrderExchange,
        exchange_by_id: dict[str, ExchangeOrderSnapshot],
    ) -> None:
        """Compare tracked orders against the exchange open-order snapshot."""
        for _eid, pending in tuple(self.pending_orders.items()):
            exchange_oid = pending.exchange_order_id
            if not exchange_oid:
                continue

            if exchange_oid not in exchange_by_id:
                await self._reconcile_disappeared_order(exchange_name, exchange_oid, pending)
            else:
                exchange_order = exchange_by_id[exchange_oid]
                await self._reconcile_fill_gap(exchange_name, exchange_oid, pending, exchange_order)

    async def _warn_on_balance_mismatches(self, exchange_name: OrderExchange) -> None:
        """Log reconciliation balance mismatches beyond the configured threshold."""
        exchange_client = self._require_exchange_client()
        balances = await exchange_client.get_balance()
        threshold = self.settings.recon_balance_threshold
        for currency, bal in balances.items():
            if abs(bal.free + bal.used - bal.total) > threshold:
                logger.warning(
                    f"[{exchange_name}] Recon: balance mismatch for {currency}: "
                    f"free={bal.free} used={bal.used} total={bal.total} "
                    f"(threshold={threshold})"
                )

    async def _resolve_ambiguous_pending(self, pending: PendingOrderState) -> None:
        """Run one recon-cycle verification round for a parked entry.

        Reuses the full bounded verification routine
        (``_verify_ambiguous_submit``) — the two-consecutive-absence
        rule needs multiple lookups anyway, and parked entries are rare
        enough that the extra seconds inside the 60s loop are
        acceptable. When the entry stays unresolved AND its UNKNOWN
        publish never confirmed (the park-time publish retries all
        failed), one publish retry per cycle keeps working toward
        holding the engine guard.

        Args:
            pending: The parked ambiguous pending entry.
        """
        order = pending.request
        exchange_name = self._get_exchange_name()
        if await self._verify_ambiguous_submit(order, pending):
            logger.info(
                f"[{exchange_name}] Recon resolved parked ambiguous order {order.client_order_id}"
            )
            return
        if not pending.unknown_published:
            if await self._publish_order_status(order, OrderEventEnum.UNKNOWN):
                pending.unknown_published = True
        logger.warning(
            f"[{exchange_name}] Recon: order {order.client_order_id} still UNKNOWN "
            f"(venue verification pending) — retrying next cycle"
        )

    async def _adopt_ghost_orders(
        self,
        exchange_orders: list[ExchangeOrderSnapshot],
        adopted: set[str],
        pending_at_snapshot: set[str],
    ) -> None:
        """Reverse sweep: adopt open venue orders missing from pending_orders.

        The forward pass only checks pending entries against the venue;
        an open venue order with NO in-memory entry (executor restart,
        crash after send, redispatched frame processed by a previous
        incarnation) was previously ignored forever. For each such
        order: a strict command lookup attributes it — our wallet's
        create/submit command rebuilds the ORIGINAL request from the
        command row (snapshot-built requests would lose strategy_tag
        and corrupt shard attribution) and adopts via
        ``_adopt_found_order`` with watermarks seeded from durable fill
        evidence; another wallet's command is the owning executor's
        job; no command row at all means a foreign/manual order that
        must never be touched (one warning per cid, LRU-bounded). A
        durably REJECTED command found OPEN is the false-absence-reject
        healing valve: adopt AND restore the durable row to ACCEPTED;
        other terminal statuses refuse loudly. Cids that were pending
        when the venue snapshot was taken are skipped for the whole
        cycle — the snapshot is stale for them (a live fill may have
        popped the entry mid-cycle) and adopting from it would reset
        the fill watermark to zero and double-emit correctives.
        Adoptions are capped per cycle; the set shrinks as they land.

        Args:
            exchange_orders: The open-orders snapshot already fetched
                by the cycle.
            adopted: Cycle-shared set of adopted cids (the
                dispatched-verification sweep must not double-adopt).
            pending_at_snapshot: Cids pending when the snapshot was
                fetched.
        """
        if not isinstance(self.repository, SQLAlchemyRepository):
            return
        exchange_name = self._get_exchange_name()
        budget = _GHOST_ADOPT_PER_CYCLE_MAX
        for snapshot in exchange_orders:
            cid = snapshot.client_order_id
            if self._should_skip_ghost_snapshot(cid, adopted, pending_at_snapshot):
                continue
            if cid is None:
                raise RuntimeError("Ghost-order adoption requires client_order_id")
            if budget <= 0:
                logger.info(
                    f"[{exchange_name}] Recon: ghost-order adoptions deferred to later "
                    f"cycles (cap {_GHOST_ADOPT_PER_CYCLE_MAX}/cycle)"
                )
                return
            if await self._try_adopt_ghost_order(snapshot, cid, adopted, exchange_name):
                budget -= 1

    def _should_skip_ghost_snapshot(
        self,
        cid: str | None,
        adopted: set[str],
        pending_at_snapshot: set[str],
    ) -> bool:
        """Return whether an open venue snapshot is not a ghost adoption candidate."""
        if not cid or cid in self.pending_orders or cid in adopted:
            return True
        return cid in pending_at_snapshot or cid in self._ghost_foreign_warned

    async def _lookup_ghost_command(
        self,
        snapshot: ExchangeOrderSnapshot,
        cid: str,
        exchange_name: OrderExchange,
    ) -> tuple[TradeCommandRow | None, bool]:
        """Lookup the command row for a ghost snapshot, returning retryable failure state."""
        repository = self._require_sqlalchemy_repository()
        try:
            return (
                await repository.get_active_create_command_by_client_order_id(cid, exchange_name),
                False,
            )
        except Exception as e:
            logger.warning(
                f"[{exchange_name}] Recon: command lookup failed for ghost order "
                f"{snapshot.id} (cid={cid}): {e} — retrying next cycle"
            )
            return None, True

    def _resolve_ghost_heal_mode(
        self,
        snapshot: ExchangeOrderSnapshot,
        cmd: TradeCommandRow,
        cid: str,
        exchange_name: OrderExchange,
    ) -> bool | None:
        """Return rejected-heal mode when a command row may be adopted."""
        if cmd["wallet_public_id"] != self.wallet_public_id:
            return None
        heal_rejected = cmd["status"] == TradeCommandStatusEnum.REJECTED.value
        if cmd["status"] in _COMMAND_TERMINAL_STATUSES and not heal_rejected:
            logger.warning(
                f"[{exchange_name}] Recon: venue order {snapshot.id} (cid={cid}) is "
                f"OPEN but its command {cmd['public_id']} is durably "
                f"{cmd['status']} — refusing to adopt a terminally-accounted "
                f"command; operator attention required"
            )
            return None
        return heal_rejected

    async def _try_adopt_ghost_order(
        self,
        snapshot: ExchangeOrderSnapshot,
        cid: str,
        adopted: set[str],
        exchange_name: OrderExchange,
    ) -> bool:
        """Attempt one ghost-order adoption and return whether the cycle budget was used."""
        cmd, retryable_lookup_failure = await self._lookup_ghost_command(
            snapshot, cid, exchange_name
        )
        if retryable_lookup_failure:
            return False
        if cmd is None:
            self._mark_unknown_ghost_order(snapshot, cid, exchange_name)
            return False
        heal_rejected = self._resolve_ghost_heal_mode(snapshot, cmd, cid, exchange_name)
        if heal_rejected is None:
            return False
        await self._consume_ghost_adoption_budget(
            snapshot, cid, cmd, heal_rejected, adopted, exchange_name
        )
        return True

    def _mark_unknown_ghost_order(
        self,
        snapshot: ExchangeOrderSnapshot,
        cid: str,
        exchange_name: OrderExchange,
    ) -> None:
        """Remember and log a venue order that has no command row."""
        self._mark_foreign_order_warned(cid)
        logger.warning(
            f"[{exchange_name}] Recon: open venue order {snapshot.id} "
            f"(cid={cid}) has NO command row — foreign/manual order, leaving "
            f"untouched"
        )

    async def _consume_ghost_adoption_budget(
        self,
        snapshot: ExchangeOrderSnapshot,
        cid: str,
        cmd: TradeCommandRow,
        heal_rejected: bool,
        adopted: set[str],
        exchange_name: OrderExchange,
    ) -> None:
        """Run adoption work after a command has consumed the cycle budget."""
        if cid in self.pending_orders:
            return
        order = self._reconstruct_ghost_order(cmd, cid, exchange_name)
        if order is None:
            return
        pending = PendingOrderState(request=order)
        await self._repair_adopted_order_row(order, snapshot, pending)
        if not await self._seed_adoption_watermarks(pending, cid):
            return
        self.pending_orders[cid] = pending
        logger.warning(
            f"[{exchange_name}] Recon: ADOPTING ghost venue order {snapshot.id} "
            f"(cid={cid}, command {cmd['public_id']}, status={snapshot.status}) — "
            f"open at the venue with no in-memory entry"
        )
        await self._adopt_found_order(order, pending, snapshot)
        adopted.add(cid)
        if heal_rejected:
            await self._queue_rejected_restore(cmd, snapshot.id)

    def _reconstruct_ghost_order(
        self,
        cmd: TradeCommandRow,
        cid: str,
        exchange_name: OrderExchange,
    ) -> OrderRequestData | None:
        """Rebuild the original dispatch payload for a ghost adoption."""
        try:
            return order_request_from_command(cmd)
        except Exception as e:
            logger.error(
                f"[{exchange_name}] Recon: command {cmd['public_id']} cannot be "
                f"reconstructed into a dispatch payload ({e}) — skipping adoption "
                f"of {cid}; operator attention required (vocabulary mismatch, see "
                f"the stop-order pipeline follow-up)"
            )
            return None

    async def _queue_rejected_restore(self, cmd: TradeCommandRow, exchange_order_id: str) -> None:
        """Queue and immediately retry a REJECTED command restore."""
        restore_cmd = dict(cmd)
        restore_cmd["exchange_order_id"] = exchange_order_id
        self._pending_rejected_restores[cmd["public_id"]] = cast(TradeCommandRow, restore_cmd)
        await self._retry_rejected_restore(cmd["public_id"])

    async def _repair_adopted_order_row(
        self,
        order: OrderRequestData,
        snapshot: ExchangeOrderSnapshot,
        pending: PendingOrderState,
    ) -> None:
        """Ensure an adopted order has an active durable ``orders`` row.

        Coordinator restart recovery re-arms engine in-flight intent
        from ACTIVE order rows; the original post-accept row write is
        best-effort and can have failed (that failure is one of the
        ways an order becomes a ghost in the first place). Repairing
        the row at adoption time restores that recovery path and gives
        the pending entry its ``db_order_id``/``order_public_id`` so
        terminal status updates persist and the dual-plane watermark
        seeding (which needs the logical order identity for the
        executions read) can run. Best-effort like the original write:
        never raises, a miss is logged — the seeding step then defers
        the adoption rather than guessing watermarks.

        Args:
            order: The reconstructed original request.
            snapshot: The venue snapshot for the adopted order.
            pending: The pending entry being adopted.
        """
        if self.exchange_client is None or not isinstance(self.repository, SQLAlchemyRepository):
            return
        try:
            existing = await self.repository.get_order_identity_for_client_order_id(
                order.client_order_id, as_of=datetime.now(UTC)
            )
            if existing is not None:
                pending.db_order_id, pending.order_public_id, _venue_id = existing
                return
            request = _exchange_order_request_from_core(order, self.wallet_public_id)
            logged = await self.exchange_client._log_order_to_db(request, snapshot)
            if logged is not None:
                pending.db_order_id, pending.order_public_id = logged
        except Exception as e:
            logger.warning(
                f"[{self._get_exchange_name()}] Recon: adopted-order row repair failed "
                f"for {order.client_order_id}: {e} — adoption proceeds; the durable "
                f"row stays missing until the next adoption-path retry"
            )

    def _mark_foreign_order_warned(self, cid: str) -> None:
        """Record a warned foreign cid with an LRU bound on the set."""
        self._ghost_foreign_warned[cid] = None
        while len(self._ghost_foreign_warned) > _GHOST_FOREIGN_WARNED_MAX:
            self._ghost_foreign_warned.popitem(last=False)

    async def _seed_adoption_watermarks(self, pending: PendingOrderState, cid: str) -> bool:
        """Seed an adopted entry's watermarks with full dual-plane recovery rules.

        A fresh ``PendingOrderState`` starts every watermark at zero; if
        the order has durable fill history (a previous incarnation
        recorded fills before losing the entry), a zero watermark would
        make the next fill-gap pass re-record and re-emit the FULL venue
        cumulative — and the engine books corrective deltas, so an
        already-published fill re-emitted under a fresh recon exec id
        would DOUBLE exposure. Seeding from durable rows ALONE is the
        opposite failure: it marks recorded-but-unpublished fills (and
        their fees) as shown, silencing the fill-gap corrective that
        would deliver them. So adoption reuses the startup-recovery
        seeding verbatim (``_read_recovery_watermarks``): committed
        quantity = min(executions, durable), durable quantity = durable
        max, published fees = the executions plane verbatim, recorded
        fees = deduped durable rows — the next recon cycle's fill-gap
        corrective then delivers any venue-ahead remainder WITH its fee.
        Requires the repaired ``order_public_id`` (the executions plane
        is keyed by it); FAIL-CLOSED without it or on a failed read —
        adopting at untrusted watermarks is the double-booking path.

        Args:
            pending: The freshly-built pending entry being adopted
                (row repair must have run first).
            cid: The order's client id.

        Returns:
            True when the watermarks are trustworthy; False to defer
            the adoption to the next cycle.
        """
        if not isinstance(self.repository, SQLAlchemyRepository):
            return True
        exchange_name = self._get_exchange_name()
        if pending.order_public_id is None:
            logger.warning(
                f"[{exchange_name}] Recon: adoption of {cid} deferred — no orders-row "
                f"identity available for dual-plane watermark seeding (row repair "
                f"failed; retrying next cycle)"
            )
            return False
        seeds = await self._read_recovery_watermarks(cid, pending.order_public_id, exchange_name)
        if seeds is None:
            logger.warning(
                f"[{exchange_name}] Recon: durable watermark seed failed for {cid} — "
                f"deferring adoption to the next cycle (FAIL-CLOSED: a zero watermark "
                f"could double-book already-published fills)"
            )
            return False
        last_seen, durable_max, fill_rows, exec_fees = seeds
        pending.last_seen_cum_qty = last_seen
        pending.last_recorded_cum_qty = durable_max
        pending.last_recorded_fee = self._sum_row_fees(fill_rows)
        pending.last_published_fee = exec_fees
        return True
        try:
            fill_rows = await self.repository.get_fill_venue_events_for_order(cid)
        except Exception as e:
            logger.warning(
                f"[{self._get_exchange_name()}] Recon: durable watermark seed failed "
                f"for {cid}: {e} — deferring adoption to the next cycle (FAIL-CLOSED: "
                f"a zero watermark could double-book already-published fills)"
            )
            return False
        durable_max = 0.0
        for row in fill_rows:
            row_cum = row["cum_fill_size"]
            if row_cum is not None and row_cum > durable_max:
                durable_max = row_cum
        if durable_max > 0.0:
            pending.last_seen_cum_qty = durable_max
            pending.last_recorded_cum_qty = durable_max
        pending.last_recorded_fee = self._sum_row_fees(fill_rows)
        pending.last_published_fee = dict(pending.last_recorded_fee)
        return True

    async def _retry_rejected_restore(self, public_id: str) -> None:
        """Drive one queued REJECTED->ACCEPTED restore to completion.

        The false-absence-rejection heal must be retryable: the adopted
        order's cid is in ``pending_orders`` (so the ghost sweep will
        never revisit it) while a stale durably-REJECTED row would keep
        the lifecycle fold blind to it AND let the paired-leg backstop
        project a false terminal. The queue entry survives until the
        row's CURRENT status is verifiably no longer ``rejected``: a
        lost CAS alone is dropped only after a re-read confirms another
        writer moved the row; errors keep the entry queued for the next
        recon cycle (this sweep is DB-only and runs before venue calls).

        Args:
            public_id: Key into ``_pending_rejected_restores``.
        """
        cmd = self._pending_rejected_restores.get(public_id)
        if cmd is None or not isinstance(self.repository, SQLAlchemyRepository):
            return
        exchange_name = self._get_exchange_name()
        now = datetime.now(UTC)
        try:
            restored = await self.repository.advance_trade_command_lifecycle(
                public_id=public_id,
                expected_status=TradeCommandStatusEnum.REJECTED.value,
                new_status=TradeCommandStatusEnum.ACCEPTED.value,
                bus_time=now,
                session_id=cmd["session_id"],
                sequence_id=cmd["sequence_id"],
                acked_at=now,
                exchange_order_id=cmd["exchange_order_id"],
                last_error="restored from venue truth after false absence rejection",
                clear_terminal_at=True,
            )
            if not restored:
                current = await self.repository.get_current_trade_command_status(public_id)
                if current == TradeCommandStatusEnum.REJECTED.value:
                    logger.warning(
                        f"[{exchange_name}] Recon: restore CAS for {public_id} lost but "
                        f"the row is STILL rejected — retrying next cycle"
                    )
                    return
        except Exception as e:
            logger.warning(
                f"[{exchange_name}] Recon: REJECTED->ACCEPTED restore failed for "
                f"{public_id}: {e} — retrying next cycle"
            )
            return
        self._pending_rejected_restores.pop(public_id, None)
        logger.warning(
            f"[{exchange_name}] Recon: command {public_id} was durably REJECTED but "
            f"the venue shows its order OPEN — durable status restored to ACCEPTED"
        )

    async def _verify_unresolved_dispatched(self, adopted: set[str]) -> None:
        """Venue-verify stale dispatched commands with no resolving evidence.

        The durable plane's work queue: active dispatched/
        direct_dispatched create/submit commands past
        ``_DISPATCHED_VERIFY_MIN_AGE_S`` whose cid has no resolving
        venue event (a lone ``order_submit_unknown`` does not resolve —
        restart-lost parked entries land exactly here, closing P0-1
        slice 7). Each gets a bounded ``find_order_by_client_id`` round,
        fairness-capped with index rotation like the parked-ambiguous
        pass. The per-cid absence counters of cids that left the
        candidate set are dropped (resolved elsewhere).

        Args:
            adopted: Cids adopted earlier this cycle (skip — their
                evidence row may not be visible to the query snapshot).
        """
        if not isinstance(self.repository, SQLAlchemyRepository) or self.exchange_client is None:
            return
        exchange_name = self._get_exchange_name()
        ttl = self._resolve_dispatch_ttl()
        allow_reject = ttl > 0
        min_age_s = max(_DISPATCHED_VERIFY_MIN_AGE_S, ttl * 2)
        cutoff = datetime.now(UTC) - timedelta(seconds=min_age_s)
        try:
            commands = await self.repository.get_unresolved_dispatched_commands(
                exchange_name, self.wallet_public_id, cutoff
            )
        except Exception as e:
            logger.warning(
                f"[{exchange_name}] Recon: unresolved-dispatched query failed: {e} — "
                f"retrying next cycle"
            )
            return
        candidates = [
            cmd
            for cmd in commands
            if cmd["client_order_id"] not in self.pending_orders
            and cmd["client_order_id"] not in adopted
        ]
        candidate_cids = {cmd["client_order_id"] for cmd in candidates}
        for stale_cid in tuple(self._dispatched_absence_counts):
            if stale_cid not in candidate_cids:
                self._dispatched_absence_counts.pop(stale_cid, None)
        if not candidates:
            return
        budget = _DISPATCHED_VERIFY_PER_CYCLE_MAX
        start = self._dispatched_rotation_offset % len(candidates)
        rotated = candidates[start:] + candidates[:start]
        self._dispatched_rotation_offset = start + min(budget, len(rotated))
        for cmd in rotated[:budget]:
            await self._verify_one_dispatched_command(cmd, allow_reject=allow_reject)
        if len(rotated) > budget:
            logger.info(
                f"[{exchange_name}] Recon: {len(rotated) - budget} unresolved dispatched "
                f"commands deferred to later cycles (fairness cap {budget}/cycle)"
            )

    async def _verify_one_dispatched_command(
        self, cmd: TradeCommandRow, *, allow_reject: bool = True
    ) -> None:
        """Resolve one evidence-less dispatched command against venue truth.

        FOUND: rebuild the original request from the command row and
        adopt (covers executor-restart-lost parked UNKNOWN entries and
        crash-during-ambiguity — the DISPATCHED row is the durable
        intent, closing P0-1 open question 4). Authoritative absence
        twice in a row AND command younger than
        ``_ABSENCE_REJECT_MAX_AGE_S``: REJECT with publish-success-
        before-terminal-event semantics — the ``order_rejected`` row is
        written ONLY after a confirmed REJECTED publish, because a
        premature terminal row would exempt the command from this sweep
        (any-evidence rule) while the engine guard stays held forever.
        Older commands get WARN-only escalation: venue closed-order
        lookback makes absence non-authoritative with age. Lookup
        errors leave the absence counter untouched.

        Args:
            cmd: The unresolved dispatched command row.
            allow_reject: False when the dispatch TTL is disabled — a
                frame can then be legitimately in flight at ANY age
                (nothing expires it), so absence may never auto-REJECT:
                a late frame arriving after the engine released intent
                would place an untracked order. The caller derives the
                sweep cutoff from the TTL for the same reason.
        """
        if self.exchange_client is None:
            return
        exchange_name = self._get_exchange_name()
        cid = cmd["client_order_id"]
        snapshot, lookup_finished = await self._lookup_dispatched_snapshot(cmd, cid, exchange_name)
        if lookup_finished:
            return
        order = self._reconstruct_dispatched_order(cmd, cid, exchange_name)
        if order is None:
            return
        if snapshot is not None:
            await self._adopt_dispatched_snapshot(cmd, order, snapshot, cid, exchange_name)
            return
        await self._handle_absent_dispatched_command(
            cmd, order, cid, allow_reject=allow_reject, exchange_name=exchange_name
        )

    async def _lookup_dispatched_snapshot(
        self,
        cmd: TradeCommandRow,
        cid: str,
        exchange_name: OrderExchange,
    ) -> tuple[ExchangeOrderSnapshot | None, bool]:
        """Lookup a dispatched command by client id and classify terminal lookup states."""
        exchange_client = self._require_exchange_client()
        try:
            async with asyncio.timeout(_AMBIGUOUS_VERIFY_TIMEOUT_S):
                snapshot = await exchange_client.find_order_by_client_id(cid, cmd["instrument"])
        except NotImplementedError:
            if not self._verify_unsupported_logged:
                self._verify_unsupported_logged = True
                logger.info(
                    f"[{exchange_name}] venue cannot verify orders by client id — "
                    f"dispatched-command verification skipped (park-only default)"
                )
            return None, True
        except Exception as e:
            logger.warning(
                f"[{exchange_name}] Recon: dispatched-command verification failed for "
                f"{cid}: {e} — retrying next cycle"
            )
            return None, True
        return snapshot, False

    def _reconstruct_dispatched_order(
        self,
        cmd: TradeCommandRow,
        cid: str,
        exchange_name: OrderExchange,
    ) -> OrderRequestData | None:
        """Rebuild the original dispatch payload for a dispatched-command verification."""
        try:
            return order_request_from_command(cmd)
        except Exception as e:
            logger.error(
                f"[{exchange_name}] Recon: command {cmd['public_id']} cannot be "
                f"reconstructed into a dispatch payload ({e}) — skipping verification "
                f"actions for {cid}; operator attention required (vocabulary mismatch, "
                f"see the stop-order pipeline follow-up)"
            )
            return None

    async def _adopt_dispatched_snapshot(
        self,
        cmd: TradeCommandRow,
        order: OrderRequestData,
        snapshot: ExchangeOrderSnapshot,
        cid: str,
        exchange_name: OrderExchange,
    ) -> None:
        """Adopt an unresolved dispatched command that venue lookup found."""
        self._dispatched_absence_counts.pop(cid, None)
        if cid in self.pending_orders:
            return
        pending = PendingOrderState(request=order)
        await self._repair_adopted_order_row(order, snapshot, pending)
        if not await self._seed_adoption_watermarks(pending, cid):
            return
        self.pending_orders[cid] = pending
        logger.warning(
            f"[{exchange_name}] Recon: unresolved DISPATCHED command "
            f"{cmd['public_id']} FOUND on venue as {snapshot.id} "
            f"(status={snapshot.status}) — adopting"
        )
        await self._adopt_found_order(order, pending, snapshot)

    async def _handle_absent_dispatched_command(
        self,
        cmd: TradeCommandRow,
        order: OrderRequestData,
        cid: str,
        *,
        allow_reject: bool,
        exchange_name: OrderExchange,
    ) -> None:
        """Advance absence evidence and optionally reject an unresolved command."""
        count = self._dispatched_absence_counts.get(cid, 0) + 1
        self._dispatched_absence_counts[cid] = count
        if count < 2:
            return
        if not allow_reject:
            logger.warning(
                f"[{exchange_name}] Recon: command {cmd['public_id']} verified absent "
                f"x{count} but the dispatch TTL is disabled — a frame may still be in "
                f"flight at any age, NOT auto-rejecting; operator attention required"
            )
            return
        age_s = (datetime.now(UTC) - cmd["created_at"]).total_seconds()
        if age_s >= _ABSENCE_REJECT_MAX_AGE_S:
            logger.warning(
                f"[{exchange_name}] Recon: command {cmd['public_id']} verified absent "
                f"x{count} but is {age_s:.0f}s old — venue closed-order lookback makes "
                f"absence non-authoritative; NOT auto-rejecting, operator attention "
                f"required"
            )
            return
        await self._reject_absent_dispatched_command(cmd, order, cid, exchange_name, count)

    async def _reject_absent_dispatched_command(
        self,
        cmd: TradeCommandRow,
        order: OrderRequestData,
        cid: str,
        exchange_name: OrderExchange,
        count: int,
    ) -> None:
        """Publish and persist a verified-absent dispatched command rejection."""
        if not await self._publish_order_status(order, OrderEventEnum.REJECTED):
            logger.warning(
                f"[{exchange_name}] Recon: REJECTED publish failed for {cid} — keeping "
                f"absence state and retrying next cycle (a terminal event before a "
                f"confirmed publish would strand the engine guard)"
            )
            return
        try:
            await self._record_venue_event(
                {
                    "event_type": "order_rejected",
                    "exchange_name": exchange_name,
                    "instrument": cmd["instrument"],
                    "client_order_id": cid,
                    "side": cmd["side"],
                    "error": "dispatched command verified absent on venue (2 consecutive)",
                    "strategy_tag": order.strategy_tag,
                }
            )
        except Exception:
            logger.warning(
                f"[{exchange_name}] Recon: order_rejected venue event failed for {cid} "
                f"after a confirmed REJECTED publish — the sweep re-verifies next cycle "
                f"and re-records (duplicate REJECTED publishes are engine-idempotent)"
            )
            return
        self._dispatched_absence_counts.pop(cid, None)
        logger.warning(
            f"[{exchange_name}] Recon: REJECTED dispatched command {cmd['public_id']} "
            f"({cid}) — venue verified absent twice, engine intent released"
        )

    async def _retry_accept_event(self, client_order_id: str) -> None:
        """Retry the durable order_accepted write for an accepted order.

        Heals the row whose write failed during acceptance
        finalization; the order is live (or by now terminal) and was
        already published ACCEPTED, so only the durable side needs
        repair. The retry state lives in ``_unhealed_accept_events`` on
        the executor — NOT on the pending entry — so a terminal fill or
        cancel popping the entry before the write sticks cannot lose
        the retry. Before re-inserting, a durable probe checks whether
        the supposedly-failed write actually committed (a
        timeout-after-commit race used to produce known-benign
        duplicate accept rows): an existing row heals without writing.
        Failure of the probe or the write keeps the event queued for
        the next cycle.

        Args:
            client_order_id: Key into ``_unhealed_accept_events``.
        """
        exchange_name = self._get_exchange_name()
        accept_event = self._unhealed_accept_events.get(client_order_id)
        if accept_event is None:
            return
        try:
            already_durable = isinstance(
                self.repository, SQLAlchemyRepository
            ) and await self.repository.has_venue_event(client_order_id, "order_accepted")
            if not already_durable:
                await self._record_venue_event(accept_event)
        except Exception:
            logger.warning(
                f"[{exchange_name}] Recon: order_accepted venue event still failing "
                f"for {client_order_id} — retrying next cycle"
            )
            return
        self._unhealed_accept_events.pop(client_order_id, None)
        pending = self.pending_orders.get(client_order_id)
        if pending is not None:
            pending.accept_event_pending = False
        if already_durable:
            logger.info(
                f"[{exchange_name}] Recon: order_accepted for {client_order_id} was "
                f"already durable (write committed despite the reported failure) — "
                f"healed without inserting a duplicate"
            )
        else:
            logger.info(
                f"[{exchange_name}] Recon healed the durable order_accepted event for "
                f"{client_order_id}"
            )

    async def _reconcile_disappeared_order(
        self,
        exchange_name: str,
        exchange_oid: str,
        pending: PendingOrderState,
    ) -> None:
        """Handle an order absent from the open-orders snapshot.

        Calls get_order() to determine actual status. Emits any
        remaining fill gap before the terminal event.
        """
        exchange_client = self._require_exchange_client()
        try:
            snapshot = await exchange_client.get_order(exchange_oid, pending.request.instrument)
        except Exception:
            logger.warning(
                f"[{exchange_name}] Recon: get_order failed for {exchange_oid}, skipping this cycle"
            )
            return

        if snapshot.status not in (
            ExchangeOrderStatusEnum.CLOSED,
            ExchangeOrderStatusEnum.CANCELED,
            ExchangeOrderStatusEnum.EXPIRED,
        ):
            logger.info(
                f"[{exchange_name}] Recon: order {exchange_oid} not in open list "
                f"but status={snapshot.status}, deferring"
            )
            return

        if snapshot.filled > pending.last_seen_cum_qty:
            gap_result = await self._reconcile_fill_gap(
                exchange_name, exchange_oid, pending, snapshot
            )
            if gap_result == "deferred":
                logger.warning(
                    f"[{exchange_name}] Recon: terminal for {exchange_oid} HELD — the "
                    f"fill-gap corrective deferred on a transient fee-source failure "
                    f"and the terminal would pop the entry before the retry"
                )
                return

        await self._emit_disappeared_terminal(exchange_name, exchange_oid, pending, snapshot)

    async def _emit_disappeared_terminal(
        self,
        exchange_name: str,
        exchange_oid: str,
        pending: PendingOrderState,
        snapshot: ExchangeOrderSnapshot,
    ) -> None:
        """Emit the synthetic terminal execution for a venue-terminal order.

        Maps the venue's terminal status to the matching ExecType and
        routes a synthetic terminal ExecutionUpdate through the normal
        execution pipeline (status projection, lifecycle pop). Extracted
        from :meth:`_reconcile_disappeared_order` so recovery-time
        terminal handling can reuse the exact same emission path.

        Args:
            exchange_name: Exchange identifier for logging.
            exchange_oid: Venue-assigned order id.
            pending: Tracked state of the disappeared order.
            snapshot: Venue order snapshot with a terminal status.
        """
        terminal_type: ExecType
        terminal_status: ExchangeOrderStatusEnum
        if snapshot.status == ExchangeOrderStatusEnum.CLOSED:
            terminal_type = "filled"
            terminal_status = ExchangeOrderStatusEnum.CLOSED
        elif snapshot.status == ExchangeOrderStatusEnum.EXPIRED:
            terminal_type = "expired"
            terminal_status = ExchangeOrderStatusEnum.EXPIRED
        else:
            terminal_type = "canceled"
            terminal_status = ExchangeOrderStatusEnum.CANCELED

        logger.warning(
            f"[{exchange_name}] Recon: order {exchange_oid} "
            f"disappeared (status={snapshot.status}), "
            f"emitting terminal={terminal_type}"
        )
        corrective = ExecutionUpdate(
            order_id=exchange_oid,
            exec_type=terminal_type,
            symbol=pending.request.instrument,
            side=OrderSideEnum(pending.request.side),
            order_type=ExchangeOrderTypeEnum.MARKET,
            order_status=terminal_status,
            timestamp=datetime.now(UTC),
        )
        await self._process_execution(corrective)

    @staticmethod
    def _snapshot_has_fee(exchange_order: ExchangeOrderSnapshot) -> bool:
        """Return whether an exchange snapshot carries usable cumulative fee fields."""
        return bool(
            getattr(exchange_order, "fee", None) and getattr(exchange_order, "fee_currency", None)
        )

    @staticmethod
    def _needs_fill_summary(fill_price: float | None, snapshot_has_fee: bool) -> bool:
        """Return whether a venue fill-summary lookup can improve the corrective."""
        return fill_price is None or not snapshot_has_fee

    def _fill_summary_source_is_implemented(self) -> bool:
        """Return whether the exchange client advertises a real fills source."""
        return self.exchange_client is not None and bool(
            getattr(self.exchange_client, "supports_fill_summary", False)
        )

    async def _fetch_order_fill_summary(
        self, exchange_name: str, exchange_oid: str
    ) -> tuple[OrderFillSummary | None, bool]:
        """Fetch a per-order fill summary and classify lookup failures."""
        exchange_client = self._require_exchange_client()
        try:
            return await exchange_client.get_order_fill_summary(exchange_oid), False
        except Exception as exc:
            logger.warning(
                f"[{exchange_name}] Recon: fill-summary lookup failed for "
                f"{exchange_oid}, treating as unresolved: {exc}"
            )
            return None, True

    def _resolve_summary_coverage(
        self,
        exchange_name: str,
        exchange_oid: str,
        exchange_order: ExchangeOrderSnapshot,
        fill_price: float | None,
        summary: OrderFillSummary,
    ) -> _FillSummaryResolution:
        """Accept only whole-order fill-summary coverage."""
        if summary.covered_qty >= exchange_order.filled * (1.0 - 1e-6):
            resolved_price = summary.vwap if fill_price is None else fill_price
            return _FillSummaryResolution(
                fill_price=resolved_price,
                summary=summary,
                summary_unusable=False,
            )
        logger.warning(
            f"[{exchange_name}] Recon: fills page for {exchange_oid} "
            f"covers only {summary.covered_qty} of "
            f"{exchange_order.filled}, refusing a partial-page aggregate"
        )
        return _FillSummaryResolution(
            fill_price=fill_price,
            summary=None,
            summary_unusable=True,
        )

    async def _resolve_fill_summary(
        self,
        exchange_name: str,
        exchange_oid: str,
        exchange_order: ExchangeOrderSnapshot,
        fill_price: float | None,
        snapshot_has_fee: bool,
    ) -> _FillSummaryResolution:
        """Resolve optional fill-summary data for a corrective fill."""
        if not self._needs_fill_summary(fill_price, snapshot_has_fee):
            return _FillSummaryResolution(fill_price, None, False)
        if self.exchange_client is None:
            return _FillSummaryResolution(fill_price, None, False)
        summary, lookup_failed = await self._fetch_order_fill_summary(exchange_name, exchange_oid)
        if lookup_failed:
            return _FillSummaryResolution(fill_price, None, True)
        if summary is None:
            return _FillSummaryResolution(
                fill_price,
                None,
                self._fill_summary_source_is_implemented(),
            )
        return self._resolve_summary_coverage(
            exchange_name,
            exchange_oid,
            exchange_order,
            fill_price,
            summary,
        )

    def _maybe_defer_fee_source(
        self,
        exchange_name: str,
        exchange_oid: str,
        snapshot_has_fee: bool,
        summary_unusable: bool,
    ) -> _GapResult | None:
        """Return a deferral result when fee evidence is temporarily unusable."""
        if snapshot_has_fee or not summary_unusable:
            return None
        deferral_count = self._gap_fee_deferrals.get(exchange_oid, 0) + 1
        if deferral_count <= _GAP_FEE_DEFERRAL_MAX:
            self._gap_fee_deferrals[exchange_oid] = deferral_count
            logger.warning(
                f"[{exchange_name}] Recon: deferring the corrective for "
                f"{exchange_oid} ({deferral_count}/{_GAP_FEE_DEFERRAL_MAX}) — the "
                f"fee source is unavailable (failed lookup, partial fills page, "
                f"or an implemented source with no usable rows) and the "
                f"corrective's stable exec id would freeze a fee-less emission; "
                f"retrying next cycle"
            )
            return "deferred"
        self._gap_fee_deferrals.pop(exchange_oid, None)
        logger.critical(
            f"[{exchange_name}] Recon: fee source for {exchange_oid} stayed "
            f"unavailable for {_GAP_FEE_DEFERRAL_MAX} cycles (fills page likely "
            f"aged out) — emitting the corrective FEE-LESS so the terminal can "
            f"project; reconcile the fee manually against the venue's fill "
            f"history"
        )
        return None

    @staticmethod
    def _corrective_fill_timestamp(exchange_order: ExchangeOrderSnapshot) -> datetime:
        """Return the timestamp to stamp on a corrective execution."""
        snapshot_ts = getattr(exchange_order, "timestamp", None)
        if snapshot_ts:
            return datetime.fromtimestamp(snapshot_ts, tz=UTC)
        return datetime.now(UTC)

    def _build_corrective_fill(
        self,
        exchange_oid: str,
        pending: PendingOrderState,
        exchange_order: ExchangeOrderSnapshot,
        gap: float,
        fill_price: float,
        summary: OrderFillSummary | None,
    ) -> ExecutionUpdate:
        """Build the corrective fill update after price and fee decisions."""
        cum_fee, cum_fee_currency = self._corrective_fees(exchange_order, summary)
        return ExecutionUpdate(
            order_id=exchange_oid,
            exec_type="trade",
            symbol=pending.request.instrument,
            side=OrderSideEnum(pending.request.side),
            order_type=ExchangeOrderTypeEnum.LIMIT,
            order_status=ExchangeOrderStatusEnum.OPEN,
            timestamp=self._corrective_fill_timestamp(exchange_order),
            cum_qty=exchange_order.filled,
            last_qty=gap,
            last_price=fill_price,
            exec_id=f"recon-{exchange_oid}-c{exchange_order.filled!r}",
            cum_fee=cum_fee,
            cum_fee_currency=cum_fee_currency,
        )

    @staticmethod
    def _corrective_committed(
        pending: PendingOrderState,
        exchange_order: ExchangeOrderSnapshot,
    ) -> bool:
        """Return whether the corrective advanced the committed fill watermark."""
        return exchange_order.filled <= pending.last_seen_cum_qty + 1e-12

    async def _reconcile_fill_gap(
        self,
        exchange_name: str,
        exchange_oid: str,
        pending: PendingOrderState,
        exchange_order: ExchangeOrderSnapshot,
    ) -> _GapResult:
        """Emit a corrective fill if exchange shows more fills than local.

        Uses the exchange order's reported price as the approximate fill
        price for the corrective ExecutionUpdate.

        Known limitation — market orders without price:
            If the exchange reports a market order whose snapshot has
            ``price=None``, this method first asks the venue for the
            order's OWN fills via
            :meth:`ExchangeClientBase.get_order_fill_vwap` (venue-true
            quantity-weighted average; implemented for Kraken Futures,
            whose snapshots carry only ``limitPrice``). The VWAP is
            trusted ONLY when the returned covered quantity spans the
            order's whole filled quantity within a RELATIVE 1e-6
            tolerance (venue-side decimal rounding and float summation
            can legitimately leave a fully-covered page a few ulps
            short) — a partial fills page (older fills aged out) must
            never price the entire gap. Only when
            that also yields nothing — no usable fills lookup, partial
            coverage, or a failed lookup — does it log an ERROR
            (``"no price on market order, skipping corrective fill"``)
            and return without emitting a corrective fill.

            The CCXT snapshot builder backfills ``price`` from the
            order's executed ``average`` (the venue's VWAP) when the limit
            price is absent and the order has filled, so this skip now
            only fires when the venue reports neither a price, nor an
            executed average, nor per-order fills.

            Rationale: the executor does not subscribe to ticks and thus
            cannot approximate the fill price locally. Cross-process RPC
            against the publisher adds latency + rate-limit cost and still
            drifts relative to the true VWAP. Forcing an approximate price
            would degrade position-projection accuracy silently — so when
            even the venue's own average is missing, the skip is correct.

            Impact: the local position projection lags the exchange by the
            gap quantity. Whether the gap recovers on a later iteration
            depends on the order's lifecycle — if the order remains open
            and a later snapshot populates ``price``, the next loop will
            emit the corrective fill; if the order disappears from the
            open-orders snapshot before that, ``_reconcile_disappeared_order``
            calls this method once more before emitting a terminal event,
            after which the order is no longer tracked and the skipped gap
            persists until manually reconciled against the exchange's own
            fill history.

            Operational guidance: the ERROR log is the observability hook.
            If the same ``(exchange, exchange_oid)`` pair appears across
            multiple iterations without clearing, or if the order has
            already reached a terminal state, reconcile the position
            manually against the exchange's fill history. Do NOT
            approximate inside Snapper — the skip is the correct behaviour.
        """
        if exchange_order.filled <= pending.last_seen_cum_qty:
            self._gap_fee_deferrals.pop(exchange_oid, None)
            return "no_gap"
        gap = exchange_order.filled - pending.last_seen_cum_qty
        snapshot_has_fee = self._snapshot_has_fee(exchange_order)
        resolution = await self._resolve_fill_summary(
            exchange_name,
            exchange_oid,
            exchange_order,
            exchange_order.price,
            snapshot_has_fee,
        )
        fill_price = resolution.fill_price
        if fill_price is None:
            logger.error(
                f"[{exchange_name}] Recon: fill gap for {exchange_oid} "
                f"but no price on market order, skipping corrective fill"
            )
            return "skipped"
        fee_deferral = self._maybe_defer_fee_source(
            exchange_name, exchange_oid, snapshot_has_fee, resolution.summary_unusable
        )
        if fee_deferral is not None:
            return fee_deferral
        logger.warning(
            f"[{exchange_name}] Recon: fill gap for {exchange_oid}: "
            f"exchange={exchange_order.filled} "
            f"local={pending.last_seen_cum_qty}, "
            f"corrective delta={gap} at price~{fill_price}"
        )
        corrective = self._build_corrective_fill(
            exchange_oid,
            pending,
            exchange_order,
            gap,
            fill_price,
            resolution.summary,
        )
        await self._process_execution(corrective)
        self._gap_fee_deferrals.pop(exchange_oid, None)
        if not self._corrective_committed(pending, exchange_order):
            logger.warning(
                f"[{exchange_name}] Recon: corrective for {exchange_oid} did not "
                f"COMMIT (publish failed or the fill was orphan-buffered) — treating "
                f"as deferred so terminal paths keep the entry for the retry"
            )
            return "deferred"
        return "emitted"

    @staticmethod
    def _corrective_fees(
        exchange_order: ExchangeOrderSnapshot,
        summary: OrderFillSummary | None,
    ) -> tuple[float | None, str | None]:
        """Derive venue-true CUMULATIVE fee fields for a corrective fill.

        Correctives used to fabricate fee=0 permanently (their stable
        exec id makes re-emission a dedup no-op, so fidelity attaches at
        first emission or never). Two honest sources, in order — BOTH
        cumulative over the order, BOTH passed through as ``cum_fee``:

        - The snapshot's order-level running commission
          (spot / walutomat).
        - The fills-summary total (futures, per-fill fees summed over
          the order's WHOLE filled quantity — the caller refuses
          partial pages).

        The executor's dual fee watermark turns the cumulative into the
        exactly-not-yet-attributed remainder: a per-corrective pro-rata
        fraction would overlap across successive gap corrections (each
        recomputed against the then-current filled total) and a
        watermark-blind passthrough would re-charge fees that live
        per-fill frames already attributed. Neither source available →
        both None (the corrective stays fee-less, exactly as honest as
        before).

        Args:
            exchange_order: The venue order snapshot driving the gap.
            summary: Fills aggregate when fetched and coverage-complete.

        Returns:
            ``(cum_fee, cum_fee_currency)`` for the corrective.
        """
        snapshot_fee = getattr(exchange_order, "fee", None)
        snapshot_fee_currency = getattr(exchange_order, "fee_currency", None)
        if snapshot_fee and snapshot_fee_currency:
            return snapshot_fee, snapshot_fee_currency
        if summary is not None and summary.fee_total is not None and summary.fee_currency:
            return summary.fee_total, summary.fee_currency
        return None, None

    async def _sleep_with_jitter(self, delay_s: float) -> None:
        """Sleep ``delay_s`` scaled by ±``_EXEC_STREAM_JITTER_FRACTION``.

        Args:
            delay_s: Base backoff delay in seconds.
        """
        jitter = 1.0 + _EXEC_STREAM_JITTER_FRACTION * (2.0 * random.random() - 1.0)
        await asyncio.sleep(delay_s * jitter)

    async def _supervise_loop(
        self,
        task_label: str,
        attempt: Callable[[], Awaitable[None]],
        pre_respawn: Callable[[], None] | None = None,
    ) -> None:
        """Respawn a core executor loop until shutdown or escalation.

        Direct generalization of :meth:`_supervise_execution_stream` for
        the order, reconciliation, and heartbeat loops, which previously
        either swallowed every fault (hot-spinning on a poisoned socket)
        or — if they did die — orphaned their siblings via the bare
        gather. Death is termination itself: a clean return while
        ``running`` and any ``Exception`` both respawn after the shared
        capped jittered backoff; ``CancelledError`` re-raises (shutdown);
        a return after ``running`` cleared exits silently (a stop closing
        the sockets makes the loops raise — that is not a death).

        A death streak older than ``_TASK_DEATH_ESCALATION_CEILING_S``
        (without a healthy run of ``_EXEC_STREAM_HEALTHY_RUNTIME_S`` to
        clear it) raises :class:`ExecutorTaskDeadError`: in-process
        respawn has proven insufficient, so the whole service crashes out
        of ``start()`` and the launcher watchdog rebuilds a fresh
        instance.

        Args:
            task_label: Stable label for logs and the restart counter.
            attempt: One full pass of the supervised loop.
            pre_respawn: Optional hook run before every re-entry after a
                death (e.g. rebuilding the order SUB socket).
        """
        exchange_name = self._get_exchange_name()
        backoff = _EXEC_STREAM_BACKOFF_INITIAL_S
        streak_started: float | None = None
        while self.running:
            if self._task_restarts.get(task_label, 0) > 0 and pre_respawn is not None:
                pre_respawn()
            started = time.monotonic()
            should_respawn = await self._run_supervised_attempt(
                task_label, attempt, backoff, exchange_name
            )
            if not should_respawn:
                return
            streak_started, backoff = self._record_supervised_task_death(
                task_label, started, streak_started, backoff, exchange_name
            )
            await self._sleep_with_jitter(backoff)
            backoff = min(backoff * 2.0, _EXEC_STREAM_BACKOFF_CAP_S)

    async def _run_supervised_attempt(
        self,
        task_label: str,
        attempt: Callable[[], Awaitable[None]],
        backoff: float,
        exchange_name: OrderExchange,
    ) -> bool:
        """Run one supervised attempt and log why it needs respawn."""
        try:
            await attempt()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            if not self.running:
                return False
            logger.warning(
                f"[{exchange_name}] {task_label} died ({exc!r}) - "
                f"respawn #{self._task_restarts.get(task_label, 0) + 1} "
                f"after {backoff:.0f}s backoff"
            )
            return True
        if not self.running:
            return False
        logger.warning(
            f"[{exchange_name}] {task_label} returned unexpectedly - "
            f"respawn #{self._task_restarts.get(task_label, 0) + 1} "
            f"after {backoff:.0f}s backoff"
        )
        return True

    def _record_supervised_task_death(
        self,
        task_label: str,
        started: float,
        streak_started: float | None,
        backoff: float,
        exchange_name: OrderExchange,
    ) -> tuple[float, float]:
        """Update restart streak state after a supervised attempt dies."""
        now = time.monotonic()
        self._task_last_death[task_label] = now
        if now - started >= _EXEC_STREAM_HEALTHY_RUNTIME_S:
            backoff = _EXEC_STREAM_BACKOFF_INITIAL_S
            streak_started = None
            self._task_streak_started.pop(task_label, None)
            self._task_deaths_in_streak.pop(task_label, None)
        if streak_started is None:
            streak_started = now
            self._task_streak_started[task_label] = now
            self._task_deaths_in_streak[task_label] = 1
        elif now - streak_started >= _TASK_DEATH_ESCALATION_CEILING_S:
            logger.critical(
                f"[{exchange_name}] {task_label} death streak exceeded "
                f"{_TASK_DEATH_ESCALATION_CEILING_S:.0f}s - escalating to "
                f"a full service restart"
            )
            raise ExecutorTaskDeadError(task_label)
        else:
            self._task_deaths_in_streak[task_label] = (
                self._task_deaths_in_streak.get(task_label, 0) + 1
            )
        self._task_restarts[task_label] = self._task_restarts.get(task_label, 0) + 1
        return streak_started, backoff

    async def _supervise_execution_stream(self) -> None:
        """Respawn the private execution stream until shutdown.

        Replaces the previous spawn-once model in which ANY termination of
        :meth:`_execution_handler` — SDK reconnect-budget exhaustion after
        ~2 min (futures) / ~5 min (spot) of outage, a raised error, even a
        clean generator return — left fills permanently dark while the
        process kept reporting RUNNING, until an operator restarted it.

        Death signal is termination itself: a clean return is treated
        exactly like an exception (the spot generator can still end
        cleanly via its inner receive-error break), mirroring the #144
        publisher supervisor. Backoff doubles from
        ``_EXEC_STREAM_BACKOFF_INITIAL_S`` to ``_EXEC_STREAM_BACKOFF_CAP_S``
        with ±``_EXEC_STREAM_JITTER_FRACTION`` jitter and never abandons —
        each attempt is internally bounded by the venue clients' connect
        and send timeouts, so a wedged attempt cannot stall the loop. A
        stream that survived ``_EXEC_STREAM_HEALTHY_RUNTIME_S`` resets the
        backoff so an old incident's ceiling is not inherited by the next
        blip. Every attempt after a death first runs a best-effort
        :meth:`_post_reconnect_reconcile` so the dark-window gap is healed
        BEFORE the snapshot-free resubscribe. Client rebuild and
        resubscribe happen inside the venue generator on re-entry (locked
        ensure-connected, poisoned-slot rebuild); this loop deliberately
        touches no executor maps —
        ``pending_orders``/``client_by_exchange`` survive respawns, which
        is what keeps fill correlation working across the gap.

        Terminal exits: ``running`` cleared (shutdown), ``CancelledError``
        (propagates to ``start()``'s gather), ``NotImplementedError`` (the
        venue cannot stream executions — permanent, the recon loop is the
        only fill source), or a venue client that does not exist /
        advertises no WebSocket executions support.

        ``_exec_stream_restarts`` counts respawns as an observability seam
        for the future honest-heartbeat work (P2-1).
        """
        exchange_name = self._get_exchange_name()
        backoff = _EXEC_STREAM_BACKOFF_INITIAL_S
        while self.running:
            if not self._execution_stream_is_available(exchange_name):
                return
            if self._exec_stream_restarts:
                await self._post_reconnect_reconcile()
            started = time.monotonic()
            should_respawn = await self._run_execution_stream_attempt(backoff, exchange_name)
            if not should_respawn:
                return
            backoff = self._record_execution_stream_death(started, backoff)
            await self._sleep_with_jitter(backoff)
            backoff = min(backoff * 2.0, _EXEC_STREAM_BACKOFF_CAP_S)

    def _execution_stream_is_available(self, exchange_name: OrderExchange) -> bool:
        """Return whether this venue can run the private execution stream."""
        client = self.exchange_client
        if client is None or not client.supports_websocket_executions:
            logger.warning(
                f"[{exchange_name}] Execution stream unavailable on this venue - supervisor exiting"
            )
            return False
        return True

    async def _run_execution_stream_attempt(
        self,
        backoff: float,
        exchange_name: OrderExchange,
    ) -> bool:
        """Run one execution-stream attempt and log its respawn reason."""
        try:
            await self._execution_handler()
        except asyncio.CancelledError:
            raise
        except NotImplementedError:
            logger.info(
                f"[{exchange_name}] Exchange client does not support execution "
                f"streaming - supervisor exiting"
            )
            return False
        except Exception as exc:
            logger.warning(
                f"[{exchange_name}] Execution stream died ({exc!r}) - "
                f"respawn #{self._exec_stream_restarts + 1} after {backoff:.0f}s backoff"
            )
            return True
        if not self.running:
            return False
        logger.warning(
            f"[{exchange_name}] Execution stream returned cleanly - "
            f"respawn #{self._exec_stream_restarts + 1} after {backoff:.0f}s backoff"
        )
        return True

    def _record_execution_stream_death(self, started: float, backoff: float) -> float:
        """Update execution-stream restart streak state and return next sleep base."""
        self._exec_stream_restarts += 1
        now = time.monotonic()
        self._task_last_death["execution_stream"] = now
        if now - started >= _EXEC_STREAM_HEALTHY_RUNTIME_S:
            backoff = _EXEC_STREAM_BACKOFF_INITIAL_S
            self._task_streak_started.pop("execution_stream", None)
            self._task_deaths_in_streak.pop("execution_stream", None)
        if "execution_stream" not in self._task_streak_started:
            self._task_streak_started["execution_stream"] = now
            self._task_deaths_in_streak["execution_stream"] = 1
        else:
            self._task_deaths_in_streak["execution_stream"] = (
                self._task_deaths_in_streak.get("execution_stream", 0) + 1
            )
        return backoff

    async def _execution_handler(self) -> None:
        """Consume one execution-stream pass from the exchange WebSocket.

        Single pass by design: any termination — clean generator end or a
        raised error — returns or propagates to
        :meth:`_supervise_execution_stream`, which owns logging, backoff,
        and respawn (the broad swallow that used to live here hid death
        from any caller). The ``finally`` aclose runs the generator's
        finalizers (venue-side unsubscribe, poisoned-slot
        compare-and-clear) deterministically BEFORE the supervisor's next
        attempt, so a stale generator's deferred cleanup can never
        unsubscribe the fills feed out from under a freshly subscribed
        stream. The contract type is ``AsyncIterator`` (not every test
        double is a generator), hence the runtime ``AsyncGenerator``
        check instead of ``contextlib.aclosing``.
        """
        if self.exchange_client is None:
            logger.warning(f"ExchangeExecutorService: {_EXCHANGE_NOT_INIT_MSG}")
            return
        if not self.exchange_client.supports_websocket_executions:
            logger.warning(
                "ExchangeExecutorService: WebSocket executions unsupported; skipping handler"
            )
            return
        exchange_name = self._get_exchange_name()
        with egress_identity(
            exchange=exchange_name,
            traffic_class="private",
            owner="executor",
            operation="order_ws",
        ):
            stream = self.exchange_client.subscribe_executions()
            try:
                async for message in stream:
                    if not self.running:
                        break
                    await self._process_execution(message)
            finally:
                if isinstance(stream, AsyncGenerator):
                    await stream.aclose()

    def _cleanup_expired_orphans(self) -> None:
        """Remove orphaned executions that exceeded TTL.

        Called from two sites:

        - The scheduled 60s cleanup task — handles steady-state cases
          where no new orphans arrive to trigger lazy eviction.
        - The inline insert path in :meth:`_resolve_execution_order` —
          drops stale entries BEFORE adding a new one so the dict
          stays bounded by the 5s TTL window rather than the 60s
          cleanup-task period. Without this lazy eviction a sustained
          stream of unmapped executions plus a hung cleanup task would
          let ``orphaned_executions`` grow unbounded.
        """
        now = time.monotonic()
        expired_keys = [
            key
            for key, (_, timestamp) in self.orphaned_executions.items()
            if now - timestamp > self.orphan_ttl_seconds
        ]
        for key in expired_keys:
            self.orphaned_executions.pop(key, None)
            self.orphan_drop_count += 1
        if expired_keys:
            exchange_name = self._get_exchange_name()
            logger.info(
                f"[{exchange_name}] Dropped {len(expired_keys)} orphaned executions after TTL "
                f"(total drops: {self.orphan_drop_count})"
            )

    async def _flush_orphaned_inline(self, exchange_order_id: str) -> None:
        """Process a buffered orphan execution synchronously, in caller order.

        The terminal-verified ambiguous path awaits the
        buffered WS fill BEFORE REST reconciliation: the fill keeps its
        full fidelity (exec id, fees, exact prices) and advances
        ``last_seen_cum_qty``, so the reconciler then emits only the
        remaining gap — no interleaving with the synthetic terminal is
        possible, unlike the background-task flush used on the normal
        acceptance path.

        Args:
            exchange_order_id: Exchange-assigned order ID to flush.
        """
        orphan = self.orphaned_executions.pop(exchange_order_id, None)
        if orphan is not None:
            execution, _ = orphan
            exchange_name = self._get_exchange_name()
            logger.info(
                f"[{exchange_name}] Processing buffered orphan execution inline "
                f"for {exchange_order_id} before REST reconciliation"
            )
            await self._process_execution(execution)

    def _try_process_orphaned(self, exchange_order_id: str, client_order_id: str) -> None:
        """Try to process any orphaned execution for a newly mapped order.

        Called after ACK when client_by_exchange mapping is established.

        Args:
            exchange_order_id: Exchange-assigned order ID.
            client_order_id: Client-assigned order ID.
        """
        orphan = self.orphaned_executions.pop(exchange_order_id, None)
        if orphan is not None:
            execution, _ = orphan
            exchange_name = self._get_exchange_name()
            logger.info(
                f"[{exchange_name}] Processing buffered orphan execution for {exchange_order_id}"
            )
            task = asyncio.create_task(self._process_execution(execution))
            self._background_tasks.add(task)
            task.add_done_callback(self._background_tasks.discard)

    def _resolve_execution_order(
        self, execution: ExecutionUpdate, exchange_name: OrderExchange
    ) -> tuple[str, str, OrderRequestData] | None:
        """Resolve execution to its order using two-level correlation.

        Args:
            execution: Execution update from exchange.
            exchange_name: Exchange name for logging.

        Returns:
            Tuple of (exchange_order_id, client_order_id, original_order)
            or None if correlation fails or execution should be skipped.
        """
        exchange_order_id = execution.order_id
        if exchange_order_id is None:
            logger.warning(
                f"[{exchange_name}] Received execution with None order_id, cannot correlate"
            )
            return None
        client_order_id = self.client_by_exchange.get(exchange_order_id)
        if client_order_id is None:
            self._cleanup_expired_orphans()
            already_buffered = exchange_order_id in self.orphaned_executions
            self.orphaned_executions[exchange_order_id] = (execution, time.monotonic())
            if not already_buffered:
                logger.info(
                    f"[{exchange_name}] Buffered orphan execution for {exchange_order_id} "
                    f"(status={execution.exec_type}, awaiting ACK or TTL expiry)"
                )
            return None
        pending = self.pending_orders.get(client_order_id)
        if pending is None:
            logger.warning(
                f"[{exchange_name}] Received execution for unknown client order: "
                f"{client_order_id} (exchange: {exchange_order_id})"
            )
            return None
        return exchange_order_id, client_order_id, pending.request

    async def _handle_cancellation(
        self,
        execution: ExecutionUpdate,
        exchange_order_id: str,
        client_order_id: str,
        exchange_name: OrderExchange,
    ) -> bool:
        """Handle cancelled or expired execution by cleaning up maps.

        Writes the durable ``order_terminal`` venue event FIRST (the
        replay/fencing side), then publishes the stream-terminal bus
        event (:meth:`_publish_stream_terminal_event`) so the engine's
        in-flight guard releases promptly, then drops correlation.
        Both the live venue stream and the reconciliation
        disappeared-order path converge here, so one publish covers
        both sinks.

        Args:
            execution: Execution update.
            exchange_order_id: Exchange-assigned order ID.
            client_order_id: Client-assigned order ID.
            exchange_name: Exchange name for logging.

        Returns:
            True if the execution was a cancellation and was handled.
        """
        if execution.exec_type not in ("canceled", "expired"):
            return False
        pending = self.pending_orders.get(client_order_id)
        if pending and pending.db_order_id is not None and self.exchange_client is not None:
            status = (
                ExchangeOrderStatusEnum.CANCELED
                if execution.exec_type == "canceled"
                else ExchangeOrderStatusEnum.EXPIRED
            )
            await self.exchange_client._log_order_update_to_db(
                db_order_id=pending.db_order_id, status=status
            )
        instrument = getattr(execution, "symbol", "") or ""
        tag = None
        if pending and pending.request:
            instrument = pending.request.instrument
            tag = pending.request.strategy_tag
        await self._record_venue_event(
            {
                "event_type": "order_terminal",
                "exchange_name": exchange_name,
                "instrument": instrument,
                "exchange_order_id": exchange_order_id,
                "client_order_id": client_order_id,
                "status": execution.exec_type or "cancelled",
                "strategy_tag": tag,
            }
        )
        terminal_event: StreamTerminalEventType = (
            OrderEventEnum.CANCELLED
            if execution.exec_type == "canceled"
            else OrderEventEnum.EXPIRED
        )
        await self._publish_stream_terminal_event(
            event=terminal_event,
            exchange_order_id=exchange_order_id,
            client_order_id=client_order_id,
            instrument=instrument,
            exchange_name=exchange_name,
            pending=pending,
        )
        self.pending_orders.pop(client_order_id, None)
        self.client_by_exchange.pop(exchange_order_id, None)
        logger.info(
            f"[{exchange_name}] Order {client_order_id} {execution.exec_type}, "
            f"cleaned up maps (stream terminal published)"
        )
        return True

    @staticmethod
    def _resolve_fill_quantities(
        execution: ExecutionUpdate,
        prev_cum: float,
        *,
        tracked: bool = False,
    ) -> tuple[float, float, float, float]:
        """Derive cumulative and delta quantities from an execution update.

        For a TRACKED order whose frame carries both the absolute
        cumulative and a venue delta, the published delta is anchored to
        the COMMITTED cumulative (``cum_qty - prev_cum``) rather than the
        venue's ``last_qty``: the engine applies deltas, so every
        published delta must close the distance from what was actually
        published before. When a prior fill's publish failed, the next
        cum-carrying frame thereby absorbs the unpublished gap instead of
        leaving the engine permanently behind venue truth (a divergence
        the recon loop cannot see, because the executor-side cumulative
        matches the venue). The two values coincide in normal operation;
        a divergence is logged. Untracked orders keep the venue delta —
        with no committed cumulative to anchor to, cum-diff deltas would
        repeat-count across frames.

        Status-only frames (neither ``cum_qty`` nor ``last_qty``, e.g.
        synthetic terminals) resolve to the COMMITTED cumulative with a
        zero delta: resolving them to 0.0 turned every terminal for a
        partially-filled order into a NEGATIVE delta that a
        delta-applying engine would book as a position reversal.

        Args:
            execution: Execution update from exchange.
            prev_cum: Previously seen cumulative quantity for this order.
            tracked: True when the order has a pending entry whose
                committed cumulative advances on successful publish.

        Returns:
            Tuple of (cum_qty, delta_size, delta_price, avg_price).
        """
        if execution.cum_qty is not None:
            cum_qty = execution.cum_qty
        elif execution.last_qty is not None:
            cum_qty = prev_cum + execution.last_qty
        else:
            cum_qty = prev_cum
        if (
            tracked
            and execution.cum_qty is not None
            and execution.last_qty is not None
            and execution.last_price is not None
        ):
            delta_size = cum_qty - prev_cum
            delta_price = execution.last_price
            if abs(delta_size - execution.last_qty) > max(1e-12, abs(execution.last_qty) * 1e-6):
                logger.warning(
                    f"Cum-anchored delta {delta_size} diverges from venue last_qty "
                    f"{execution.last_qty} (cum {cum_qty}, committed {prev_cum}) - "
                    f"absorbing unpublished gap"
                )
        elif execution.last_qty is not None and execution.last_price is not None:
            delta_size = execution.last_qty
            delta_price = execution.last_price
        else:
            delta_size = cum_qty - prev_cum
            delta_price = execution.average_price or 0.0
        avg_price = execution.average_price or delta_price
        return cum_qty, delta_size, delta_price, avg_price

    @staticmethod
    def _determine_fill_status(
        execution: ExecutionUpdate,
        cum_qty: float,
        expected_qty: float,
    ) -> FillStatus:
        """Determine whether execution represents a full or partial fill.

        Args:
            execution: Execution update from exchange.
            cum_qty: Resolved cumulative filled quantity.
            expected_qty: Absolute expected order quantity.

        Returns:
            Fill status literal.
        """
        if execution.cum_qty is None and execution.last_qty is None:
            return to_fill_status(execution)
        tolerance = max(1e-12, expected_qty * 1e-6)
        qty_complete = cum_qty >= expected_qty - tolerance
        exchange_terminal = execution.cum_qty is not None and execution.order_status in (
            ExchangeOrderStatusEnum.CLOSED,
            ExchangeOrderStatusEnum.CANCELED,
        )
        return (
            FillStatusEnum.FILLED if qty_complete or exchange_terminal else FillStatusEnum.PARTIAL
        )

    @staticmethod
    def _resolve_fee(execution: ExecutionUpdate) -> tuple[float, str]:
        """Resolve scalar fee amount and asset from execution data.

        Priority: fee_usd_equiv (USD, incl. negative rebates) then
        non-zero entries from fees breakdown (native currency).

        Args:
            execution: Execution update with optional fee fields.

        Returns:
            Tuple of (fee_amount, fee_asset). Zero fee returns (0.0, "").
        """
        if execution.fee_usd_equiv is not None and abs(execution.fee_usd_equiv) > 1e-12:
            return execution.fee_usd_equiv, "USD"
        if execution.fees:
            nonzero = [e for e in execution.fees if abs(e.quantity) > 1e-12]
            if not nonzero:
                return 0.0, ""
            if len(nonzero) > 1:
                logger.warning(
                    "Multi-asset fees not supported, using first entry: {}",
                    execution.fees,
                )
            entry = nonzero[0]
            return entry.quantity, entry.asset
        return 0.0, ""

    def _build_execution_data(
        self,
        execution: ExecutionUpdate,
        exchange_order_id: str,
        original_order: OrderRequestData,
        exchange_name: OrderExchange,
    ) -> tuple[str, ExecutionData]:
        """Build an ExecutionData from execution and order data.

        Args:
            execution: Execution update from exchange.
            exchange_order_id: Exchange-assigned order ID.
            original_order: Original order request data.
            exchange_name: Exchange name.

        ``last_seen_cum_qty`` is deliberately NOT advanced here: it is
        COMMITTED state, advanced by :meth:`_book_correlated_fill` only
        after the publish succeeded, under the order's ``fill_lock``. A
        builder-side advance would leak tentative state to concurrent
        readers and, after a failed publish, make the venue's redelivery
        look non-advancing and drop the fill for good.

        Returns:
            Tuple of (stream_key, ExecutionData) ready for publishing.
        """
        now = datetime.now(UTC)
        topic = order_event_topic(exchange_name, original_order.instrument, OrderEventEnum.EXECUTED)
        client_id = original_order.client_order_id
        pending = self.pending_orders.get(client_id)
        cum_fee = getattr(execution, "cum_fee", None)
        if cum_fee is not None:
            fee_asset = getattr(execution, "cum_fee_currency", None) or ""
            anchor = pending.last_published_fee.get(fee_asset, 0.0) if pending is not None else 0.0
            fee_amount = cum_fee - anchor
        else:
            fee_amount, fee_asset = self._resolve_fee(execution)
        prev_cum = pending.last_seen_cum_qty if pending else 0.0
        cum_qty, delta_size, delta_price, avg_price = self._resolve_fill_quantities(
            execution, prev_cum, tracked=pending is not None
        )
        status = self._determine_fill_status(
            execution, cum_qty, abs(float(original_order.quantity))
        )
        return topic, ExecutionData(
            public_id=str(uuid7()),
            timestamp=now,
            session_id=self._tracker.session_id,
            sequence_id=self._tracker.next_sequence(topic),
            trade_id=execution.exec_id,
            exchange_order_id=exchange_order_id,
            client_order_id=client_id,
            instrument=original_order.instrument,
            exchange=exchange_name,
            side=original_order.side,
            size=cum_qty,
            price=avg_price,
            last_size=delta_size,
            last_price=delta_price,
            fee=fee_amount,
            fee_asset=fee_asset,
            status=status,
            executed_at=execution.timestamp,
            wallet_public_id=original_order.wallet_public_id or self.wallet_public_id,
            operator_public_id=original_order.operator_public_id,
            liquidity_role={"m": "maker", "t": "taker"}.get(
                getattr(execution, "liquidity_ind", None) or "", "unknown"
            ),
        )

    def _is_duplicate_fill(self, execution: ExecutionUpdate, client_order_id: str) -> bool:
        """Decide whether a fill frame was already booked and must be dropped.

        Two complementary keys, checked BEFORE :meth:`_build_execution_data`
        mutates ``last_seen_cum_qty``:

        - ``exec_id`` LRU: catches venue redelivery of the same execution
          (at-least-once delivery, replays across resubscribes). Futures
          fills carry ``fill_id`` as exec id, spot carries ``exec_id``;
          recon correctives use STABLE synthetic ids
          (``recon-{oid}-c{filled}``) so re-emitting the same gap — after
          a failed publish, across restarts — dedupes here and at every
          downstream consumer instead of double-applying; a gap whose
          venue cumulative advanced gets a new id with disjoint
          durable-anchored quantities.
        - Cumulative-monotonic guard: a fill carrying absolute ``cum_qty``
          that does not ADVANCE the order's seen cumulative is a replay or
          a stale corrective. This closes the cross-key hole exec ids
          cannot: a recon corrective healed the gap under a synthetic id,
          then the real fill (a NEVER-seen exec id) arrives — and equally
          the reverse race, a stale corrective (recon always stamps
          ``cum_qty = exchange.filled``) landing after a live fill already
          advanced the cumulative. Futures live fills carry no ``cum_qty``
          so this guard never misfires there; their replay exposure is
          closed by snapshot suppression at the source plus the exec-id LRU.

        Status-only frames (no ``last_qty`` AND no ``cum_qty``) always
        pass — they carry no quantity to double-book. CUM-ONLY frames
        (walutomat polling: ``cum_qty`` without ``last_qty``) get the
        exec-id LRU check ONLY: their deterministic ``wal-`` ids make
        redelivery droppable, while an equal-cum frame under a DISTINCT
        id (the disappeared-order terminal upgrade, suffixed ``-t``)
        must pass — its cum-anchored delta is zero, so it double-books
        nothing and carries the terminal status the engine needs.

        Args:
            execution: Execution update from the exchange WebSocket.
            client_order_id: Correlated client order id.

        Returns:
            True when the frame must be dropped without publishing.
        """
        if execution.last_qty is None and execution.cum_qty is None:
            return False
        exec_id = getattr(execution, "exec_id", None)
        if exec_id and exec_id in self._seen_exec_ids:
            logger.debug(f"Skipping duplicate fill {exec_id} for {client_order_id}")
            return True
        if execution.last_qty is None:
            return False
        pending = self.pending_orders.get(client_order_id)
        if (
            pending is not None
            and execution.cum_qty is not None
            and execution.cum_qty <= pending.last_seen_cum_qty + 1e-12
        ):
            logger.debug(
                f"Skipping non-advancing fill for {client_order_id}: "
                f"cum {execution.cum_qty} <= seen {pending.last_seen_cum_qty}"
            )
            return True
        return False

    def _register_seen_exec_id(self, exec_id: str | None) -> None:
        """Record a PUBLISHED fill's exec id in the bounded LRU.

        Registration happens only after :meth:`_publish_execution`
        succeeded: a fill whose durable venue-event write or publish
        failed must NOT be marked seen, or its redelivery would be dropped
        and the fill lost for good.

        Args:
            exec_id: Venue execution id; falsy values are ignored.
        """
        if not exec_id:
            return
        self._seen_exec_ids[exec_id] = None
        self._seen_exec_ids.move_to_end(exec_id)
        while len(self._seen_exec_ids) > _SEEN_EXEC_IDS_MAX:
            self._seen_exec_ids.popitem(last=False)

    async def _process_execution(self, execution: ExecutionUpdate) -> None:
        """Process an execution update and publish fill notification.

        Uses two-level correlation:
        1. Resolve exchange_order_id -> client_order_id via client_by_exchange
        2. Lookup order by client_order_id in pending_orders

        Cancelled/expired executions clean up maps but do not publish execution data.
        Unknown executions are buffered for TTL in case ACK arrives later (race).

        Fill booking AND cancellation handling for a tracked order run
        under the order's ``fill_lock``: the live stream task, the recon
        task, and the orphan-flush task can all deliver executions for
        the same order. An unserialized interleave lets a later fill read
        tentative (not yet published) cumulative state, and an
        unserialized cancellation can pop the pending entry out from
        under an in-flight booking — the gate, the delta computation, the
        durable write, the publish, the committed-cumulative advance, and
        any lifecycle pop must be one atomic section per order.

        Args:
            execution: Execution update from the exchange WebSocket.
        """
        exchange_name = self._get_exchange_name()
        try:
            self._cleanup_expired_orphans()
            resolved = self._resolve_execution_order(execution, exchange_name)
            if resolved is None:
                return
            exchange_order_id, client_order_id, original_order = resolved
            holder = self.pending_orders.get(client_order_id)
            if holder is None:
                return
            async with holder.fill_lock:
                if await self._handle_cancellation(
                    execution, exchange_order_id, client_order_id, exchange_name
                ):
                    return
                await self._book_correlated_fill(
                    execution, exchange_order_id, client_order_id, original_order, exchange_name
                )
        except Exception as e:
            logger.error(f"[{exchange_name}] Error processing execution: {e}")

    async def _book_correlated_fill(
        self,
        execution: ExecutionUpdate,
        exchange_order_id: str,
        client_order_id: str,
        original_order: OrderRequestData,
        exchange_name: OrderExchange,
    ) -> None:
        """Book one correlated execution: dedupe, persist, publish, commit.

        Runs under the order's ``fill_lock`` when the order is tracked.
        ``last_seen_cum_qty`` is COMMITTED state — advanced only after the
        publish succeeded, so it always equals the cumulative the engine
        has actually been told about. A fill whose publish failed commits
        nothing: its redelivery passes the gate, and if a LATER fill for
        the same order arrives first, that fill's cum-anchored delta (see
        :meth:`_resolve_fill_quantities`) absorbs the unpublished gap —
        either way the published deltas sum to the committed cumulative.

        The durable venue-event row's ``fill_size`` anchoring depends on
        what the frame carries. Replay (``TradeService.apply_venue_event``)
        dedupes rows by exec id and sums ``fill_size`` additively across
        DISTINCT fills, so:

        - CUM-CARRYING frames anchor to the DURABLE watermark
          (``last_recorded_cum_qty``): the gap since the last persisted
          row. The venue's raw ``last_qty`` would underbook when the
          predecessor's durable write failed (its quantity exists in no
          row), and the publish-anchored absorbed delta would overbook
          when the predecessor's row DID persist and only its publish
          failed. The watermark advances right after the record succeeds
          — before the publish — because it tracks DB truth, not engine
          truth.
        - DELTA-ONLY frames (futures: no ``cum_qty``) write the venue's
          own ``last_qty``: their ``fill.size`` is fabricated from the
          COMMITTED cumulative, which lags venue truth after a publish
          failure, and a watermark gap computed from a fabricated
          cumulative would write a zero row for a real, distinct fill —
          underbooking replay. Redelivered duplicates of the SAME fill
          re-write rows under the same exec id, which replay dedupes.

        Args:
            execution: Execution update from the exchange WebSocket.
            exchange_order_id: Venue-assigned order id.
            client_order_id: Correlated client order id.
            original_order: Original order request data.
            exchange_name: Exchange identifier.
        """
        if self._is_duplicate_fill(execution, client_order_id):
            return
        topic, fill = self._build_execution_data(
            execution, exchange_order_id, original_order, exchange_name
        )
        durable_holder = self.pending_orders.get(client_order_id)
        accounting = self._resolve_fill_accounting(execution, fill, durable_holder)
        if accounting.is_fill_frame and self._published_fill_harmful(fill):
            logger.error(
                f"ExchangeExecutorService: refusing to book a malformed fill frame for "
                f"{client_order_id} (published last_size={fill.last_size}, "
                f"price={fill.last_price}, side={fill.side}) — dropping the fill "
                f"entirely rather than writing poison to the durable plane or "
                f"publishing a corrupt economics delta to the engine; reconciliation "
                f"recovers a lost fill, a mis-booked one corrupts a position"
            )
            return
        await self._record_correlated_fill_event(
            execution, fill, accounting, exchange_order_id, original_order, exchange_name
        )
        self._advance_durable_fill_watermark(durable_holder, fill, accounting)
        if not await self._publish_execution(topic, fill):
            return
        self._advance_committed_fill_watermark(client_order_id, fill, accounting)
        self._register_fill_exec_id(execution, accounting)
        await self._persist_correlated_fill(execution, fill, client_order_id, self.wallet_public_id)
        self._remove_filled_order(fill, client_order_id, exchange_order_id, exchange_name)

    @staticmethod
    def _fill_economics_sound(size: float, price: float, side: str) -> bool:
        """Return True when a POSITIVE-quantity fill's economics are sound.

        Defense-in-depth bulwark mirroring the recovery-side verifier
        (``TraderCoordinator._fill_events_sound``): the durable
        ``fill_observed`` plane and the engine's live fill path both
        trust these three values, so a positive fill must carry a finite
        positive price and a ``buy``/``sell`` side. Size is required
        positive here; the zero and negative cases are decided by the
        callers (``_published_fill_harmful`` for the publish side,
        ``_record_correlated_fill_event`` for the durable write), because
        a zero-quantity frame is a legitimate no-op terminal-status
        publish while a negative quantity is always corruption.
        """
        return (
            isinstance(size, int | float)
            and isinstance(price, int | float)
            and math.isfinite(size)
            and math.isfinite(price)
            and size > 0
            and price > 0
            and str(side or "").lower() in (TradeSideEnum.BUY, TradeSideEnum.SELL)
        )

    def _published_fill_harmful(self, fill: ExecutionData) -> bool:
        """Return True when publishing this fill would corrupt the engine.

        The engine's live fill path (``apply_fill``) folds the PUBLISHED
        delta (``fill.last_size`` at ``fill.last_price``) with no
        economics guard of its own, so a corrupt published delta poisons
        the live position, cash, turnover, and the persisted checkpoint
        for the process lifetime (the next-restart digest then
        fail-closed quarantines it, but the intra-process corruption is
        real). A published delta is harmful when its size is non-finite
        or negative (a spurious reduction, e.g. a futures bust or a
        cumulative regressing after a NaN poll), or when a POSITIVE size
        carries an unsound price or side. A published size of exactly
        zero is a legitimate no-op: terminal-status frames (and the
        walutomat ``-t`` zero-delta upgrade) publish a zero delta to
        carry FILLED status without moving the position, so they must
        NOT be aborted regardless of the (unused) price.
        """
        size = fill.last_size
        if not isinstance(size, int | float) or not math.isfinite(size) or size < 0:
            return True
        if size == 0:
            return False
        return not self._fill_economics_sound(size, fill.last_price, fill.side)

    def _resolve_fill_accounting(
        self,
        execution: ExecutionUpdate,
        fill: ExecutionData,
        durable_holder: PendingOrderState | None,
    ) -> _FillAccounting:
        """Resolve durable quantity and fee deltas for one correlated fill."""
        is_fill_frame = execution.last_qty is not None or execution.cum_qty is not None
        if durable_holder is not None and execution.cum_qty is not None:
            durable_size = max(0.0, fill.size - durable_holder.last_recorded_cum_qty)
        elif execution.last_qty is not None:
            durable_size = execution.last_qty
        else:
            durable_size = fill.last_size
        frame_cum_fee = getattr(execution, "cum_fee", None)
        frame_cum_fee_asset = getattr(execution, "cum_fee_currency", None) or ""
        if durable_holder is not None and frame_cum_fee is not None:
            durable_fee = frame_cum_fee - durable_holder.last_recorded_fee.get(
                frame_cum_fee_asset, 0.0
            )
        else:
            durable_fee = fill.fee
        return _FillAccounting(
            is_fill_frame=is_fill_frame,
            durable_size=durable_size,
            durable_fee=durable_fee,
            frame_cum_fee=frame_cum_fee,
            frame_cum_fee_asset=frame_cum_fee_asset,
        )

    async def _record_correlated_fill_event(
        self,
        execution: ExecutionUpdate,
        fill: ExecutionData,
        accounting: _FillAccounting,
        exchange_order_id: str,
        original_order: OrderRequestData,
        exchange_name: OrderExchange,
    ) -> None:
        """Persist the venue-event row for one correlated fill.

        A zero (or non-finite) durable size carries no fill accounting —
        status-only frames and the zero-delta terminal upgrade
        (walutomat ``-t``) reach here to let the publish carry terminal
        STATUS to the engine, but must NOT write a zero-size
        ``fill_observed`` row: the recovery-side verifier rejects a
        zero-size fill as malformed, so persisting one would quarantine
        the whole shard on the next restart. The publish still runs in
        the caller; only the durable fill row is suppressed. A malformed
        REAL fill (positive size, bad price/side) never reaches here —
        :meth:`_book_correlated_fill` aborts the whole booking first.
        """
        if not (
            isinstance(accounting.durable_size, int | float)
            and math.isfinite(accounting.durable_size)
            and accounting.durable_size > 0
        ):
            return
        raw_tid = getattr(execution, "trade_id", None)
        await self._record_venue_event(
            {
                "event_type": "fill_observed",
                "exchange_name": exchange_name,
                "instrument": fill.instrument,
                "exchange_order_id": exchange_order_id,
                "client_order_id": fill.client_order_id,
                "side": fill.side,
                "status": fill.status,
                "fill_price": fill.last_price,
                "fill_size": accounting.durable_size,
                "cum_fill_size": fill.size,
                "fee": accounting.durable_fee,
                "fee_asset": fill.fee_asset,
                "exec_id": getattr(execution, "exec_id", None),
                "trade_id": str(raw_tid) if raw_tid else None,
                "venue_timestamp": getattr(execution, "timestamp", None),
                "strategy_tag": original_order.strategy_tag,
                "liquidity_role": fill.liquidity_role,
            }
        )

    def _advance_durable_fill_watermark(
        self,
        durable_holder: PendingOrderState | None,
        fill: ExecutionData,
        accounting: _FillAccounting,
    ) -> None:
        """Advance durable fill and fee watermarks after the venue-event write."""
        if durable_holder is None or not accounting.is_fill_frame:
            return
        durable_holder.last_recorded_cum_qty = max(durable_holder.last_recorded_cum_qty, fill.size)
        if accounting.frame_cum_fee is not None:
            durable_holder.last_recorded_fee[accounting.frame_cum_fee_asset] = (
                accounting.frame_cum_fee
            )
        elif accounting.durable_fee and accounting.durable_size > 0:
            fee_key = fill.fee_asset or ""
            durable_holder.last_recorded_fee[fee_key] = (
                durable_holder.last_recorded_fee.get(fee_key, 0.0) + accounting.durable_fee
            )

    def _advance_committed_fill_watermark(
        self,
        client_order_id: str,
        fill: ExecutionData,
        accounting: _FillAccounting,
    ) -> None:
        """Advance published fill and fee watermarks after a successful publish."""
        committed = self.pending_orders.get(client_order_id)
        if committed is None or not accounting.is_fill_frame:
            return
        committed.last_seen_cum_qty = max(committed.last_seen_cum_qty, fill.size)
        if accounting.frame_cum_fee is not None:
            committed.last_published_fee[accounting.frame_cum_fee_asset] = accounting.frame_cum_fee
        elif fill.fee:
            fee_key = fill.fee_asset or ""
            committed.last_published_fee[fee_key] = (
                committed.last_published_fee.get(fee_key, 0.0) + fill.fee
            )

    def _register_fill_exec_id(
        self,
        execution: ExecutionUpdate,
        accounting: _FillAccounting,
    ) -> None:
        """Register the execution id once a real fill was published."""
        if accounting.is_fill_frame:
            self._register_seen_exec_id(getattr(execution, "exec_id", None))

    async def _persist_correlated_fill(
        self,
        execution: ExecutionUpdate,
        fill: ExecutionData,
        client_order_id: str,
        wallet_public_id: str,
    ) -> None:
        """Persist execution and order status rows after a successful publish.

        Fill truth (PnL Phase 1): the order-row update forwards the
        venue-true CUMULATIVE (:meth:`_resolve_persisted_cumulative`)
        and the RAW venue ``average_price`` (``fill.price`` may be a
        delta-price fallback and must not be persisted as an average),
        and the returned successor id re-points
        ``pending.db_order_id`` — a multi-partial order is not popped
        from tracking, so a stale id would make the second partial's
        SCD2 update collide with the active-unique index and silently
        fail.
        """
        pending = self.pending_orders.get(client_order_id)
        if pending is None or self.exchange_client is None:
            return
        if pending.order_public_id is not None:
            await self.exchange_client._log_execution_to_db(
                order_public_id=pending.order_public_id,
                execution=execution,
                wallet_public_id=wallet_public_id,
                operator_public_id=pending.request.operator_public_id,
                delta_size=fill.last_size,
                delta_price=fill.last_price,
                fee=fill.fee,
                fee_asset=fill.fee_asset,
                status=fill.status,
            )
        if pending.db_order_id is not None:
            db_status = (
                ExchangeOrderStatusEnum.CLOSED
                if fill.status == FillStatusEnum.FILLED
                else ExchangeOrderStatusEnum.OPEN
            )
            persisted_cum = await self._resolve_persisted_cumulative(
                execution, fill, client_order_id, pending
            )
            new_db_order_id = await self.exchange_client._log_order_update_to_db(
                db_order_id=pending.db_order_id,
                status=db_status,
                filled_size=persisted_cum,
                average_price=execution.average_price,
            )
            if new_db_order_id is not None:
                pending.db_order_id = new_db_order_id

    async def _resolve_persisted_cumulative(
        self,
        execution: ExecutionUpdate,
        fill: ExecutionData,
        client_order_id: str,
        pending: PendingOrderState,
    ) -> float:
        """Resolve the venue-true cumulative to persist on the order row.

        Cum-carrying frames persist the venue's own cumulative
        (``fill.size``). DELTA-ONLY frames (futures: no ``cum_qty``)
        fabricate ``fill.size`` from the PUBLISHED watermark, which
        lags venue truth after a publish failure — two distinct 0.5
        fills straddling a failed publish would both read as 0.5. For
        those frames the durable additive truth is recomputed from the
        stable-identity ``fill_observed`` rows deduplicated by the
        CANONICAL replay rule (:meth:`TradeService.dedup_fill_events`
        — exec_id OR trade_id, id-less fallback key, first row wins),
        falling back to the fabricated value when the durable plane is
        unreadable.

        Args:
            execution: The raw venue frame.
            fill: The built execution payload.
            client_order_id: Order correlation id.
            pending: Tracked order state (stable-identity scope source).

        Returns:
            The cumulative filled size to persist.
        """
        if execution.cum_qty is not None:
            return fill.size
        if self.repository is None:
            return fill.size
        try:
            rows = await self.repository.get_fill_venue_events_for_order_identity(
                client_order_id,
                self.wallet_public_id,
                pending.request.mode,
                self._get_exchange_name(),
                pending.exchange_order_id,
            )
        except Exception as e:
            logger.warning(
                f"Durable fill rows unreadable for {client_order_id}: {e}; "
                "persisting the published-anchored cumulative"
            )
            return fill.size
        deduped = TradeService.dedup_fill_events(rows)
        additive = sum(row["fill_size"] or 0.0 for row in deduped)
        return max(fill.size, additive)

    def _remove_filled_order(
        self,
        fill: ExecutionData,
        client_order_id: str,
        exchange_order_id: str,
        exchange_name: OrderExchange,
    ) -> None:
        """Drop tracking maps after a filled status has been published and persisted."""
        if fill.status == FillStatusEnum.FILLED:
            self.pending_orders.pop(client_order_id, None)
            self.client_by_exchange.pop(exchange_order_id, None)
            logger.info(f"[{exchange_name}] Order {client_order_id} filled, removed from pending")

    async def _publish_order_status(
        self,
        order: OrderRequestData,
        status: OrderEventType,
        exchange_order_id: str | None = None,
        *,
        reason: str | None = None,
    ) -> bool:
        """Publish order status event to the ZMQ topic.

        The status value is used both as the topic suffix and the payload
        status field, ensuring consistency between routing and content.

        Args:
            order: Order request data containing order details.
            status: Event type for topic suffix and payload status field.
            exchange_order_id: Exchange-assigned order ID (if known).
            reason: Optional machine-readable disposition detail carried
                in ``OrderData.reason`` (the schema field exists and was
                never populated; e.g. ``circuit_breaker_open`` lets
                consumers distinguish an infra refusal from a venue
                rejection without a wire-contract change).

        Returns:
            True when the event was handed to the publisher without
            error; False when the publisher is unavailable or the send
            raised. Callers of safety-critical statuses (UNKNOWN) must
            check this — a swallowed publish failure would
            leave the engine unaware that its in-flight guard must hold.
        """
        if not self.msg_publisher or not self.running:
            return False
        exchange_name = self._get_exchange_name()
        now = datetime.now(UTC)
        try:
            topic = order_event_topic(exchange_name, order.instrument, status)
            order_status = OrderData(
                public_id=str(uuid7()),
                timestamp=now,
                session_id=self._tracker.session_id,
                sequence_id=self._tracker.next_sequence(topic),
                exchange_order_id=exchange_order_id,
                client_order_id=order.client_order_id,
                instrument=order.instrument,
                exchange=exchange_name,
                side=order.side,
                status=status,
                reason=reason,
                order_type=order.order_type,
                size=order.quantity,
                filled_size=0.0,
                price=order.price,
                created_at=order.timestamp,
                leverage=order.leverage,
                reduce_only=order.reduce_only,
                wallet_public_id=order.wallet_public_id,
                operator_public_id=order.operator_public_id,
                user_public_id=order.user_public_id,
            )
            await self.msg_publisher.send(topic, order_status)
            logger.info(
                f"[{exchange_name}] Published order event: {order.client_order_id} - {status}"
            )
            return True
        except Exception as e:
            logger.error(f"[{exchange_name}] Error publishing order event: {e}")
            return False

    def _compute_heartbeat_status(self) -> tuple[HealthStatusEnum, int, list[str]]:
        """Derive honest heartbeat status from the supervised-loop seams.

        Replaces the hardcoded HEALTHY that kept reporting green while
        sibling loops were dead or dying. Status comes from CURRENT,
        self-clearing conditions only — lifetime counters
        (``_task_restarts``) go to meta forensics, never into status,
        because they cannot distinguish "dying now" from "died once
        yesterday". Evaluated ERROR-first:

        A streak counts as ACTIVE only while its last death is younger
        than ``_EXEC_STREAM_HEALTHY_RUNTIME_S`` — the supervisors clear
        their streak dicts only at the NEXT death, so without this
        freshness filter a loop that died once and then self-healed
        would silently age into a false ERROR page at the streak
        threshold.

        - ERROR: any active death streak older than ``_HB_STREAK_ERROR_S``
          (pages well before the 1500s escalation ceiling), or the recon
          progress clock older than ``_HB_RECON_ERROR_S`` — recon swallows
          its per-cycle failures by design, so it can fail forever without
          dying; only the progress clock sees that.
        The order handler additionally exposes an IN-FLIGHT clock: a
        command wedged inside its processing never dies and never
        progresses — only the age of the current command shows it
        (WARNING at ``_HB_INFLIGHT_WARN_S``, ERROR at
        ``_HB_INFLIGHT_ERROR_S``).

        - WARNING: >= ``_HB_DEATHS_WARN`` deaths within one active streak
          (a single clean respawn stays HEALTHY — that is the supervisor
          working, not a page), recon progress older than
          ``_HB_RECON_WARN_S``, a non-empty unhealed accept-event backlog,
          or parked ambiguous submits awaiting venue verification.

        ``lag_ms`` is the recon progress age — honest data-staleness
        semantics mirroring publisher heartbeats.

        Returns:
            Tuple of (status, lag_ms, human-readable reasons).
        """
        now = time.monotonic()
        recon_age = now - self._task_last_pass.get("reconciliation", now)
        inflight_started = self._order_inflight_started
        inflight_age = now - inflight_started if inflight_started is not None else 0.0
        active_streaks = self._active_heartbeat_streaks(now)
        reasons = self._heartbeat_error_reasons(now, recon_age, inflight_age, active_streaks)
        status = HealthStatusEnum.ERROR if reasons else HealthStatusEnum.HEALTHY
        if status is not HealthStatusEnum.ERROR:
            reasons = self._heartbeat_warning_reasons(recon_age, inflight_age, active_streaks)
            if reasons:
                status = HealthStatusEnum.WARNING
        return status, int(recon_age * 1000), reasons

    def _active_heartbeat_streaks(self, now: float) -> dict[str, float]:
        """Return death streaks still fresh enough to affect heartbeat status."""
        return {
            label: streak_start
            for label, streak_start in self._task_streak_started.items()
            if now - self._task_last_death.get(label, now) < _EXEC_STREAM_HEALTHY_RUNTIME_S
        }

    def _heartbeat_error_reasons(
        self,
        now: float,
        recon_age: float,
        inflight_age: float,
        active_streaks: dict[str, float],
    ) -> list[str]:
        """Build heartbeat ERROR reasons in priority order."""
        reasons: list[str] = []
        for label, streak_start in active_streaks.items():
            streak_age = now - streak_start
            if streak_age >= _HB_STREAK_ERROR_S:
                reasons.append(f"{label}: death streak {streak_age:.0f}s")
        if recon_age >= _HB_RECON_ERROR_S:
            reasons.append(f"reconciliation: no successful pass for {recon_age:.0f}s")
        if inflight_age >= _HB_INFLIGHT_ERROR_S:
            reasons.append(f"order command in flight for {inflight_age:.0f}s")
        return reasons

    def _heartbeat_warning_reasons(
        self,
        recon_age: float,
        inflight_age: float,
        active_streaks: dict[str, float],
    ) -> list[str]:
        """Build heartbeat WARNING reasons once no ERROR condition is active."""
        reasons: list[str] = []
        for label, deaths in self._task_deaths_in_streak.items():
            if label in active_streaks and deaths >= _HB_DEATHS_WARN:
                reasons.append(f"{label}: {deaths} deaths in active streak")
        if _HB_RECON_WARN_S <= recon_age < _HB_RECON_ERROR_S:
            reasons.append(f"reconciliation: pass age {recon_age:.0f}s")
        if _HB_INFLIGHT_WARN_S <= inflight_age < _HB_INFLIGHT_ERROR_S:
            reasons.append(f"order command in flight for {inflight_age:.0f}s")
        if self._unhealed_accept_events:
            reasons.append(f"unhealed accept events: {len(self._unhealed_accept_events)}")
        parked = self._parked_ambiguous_order_count()
        if parked:
            reasons.append(f"parked ambiguous orders: {parked}")
        return reasons

    def _parked_ambiguous_order_count(self) -> int:
        """Count parked ambiguous submits awaiting venue verification."""
        return sum(
            1
            for entry in self.pending_orders.values()
            if not entry.exchange_order_id and entry.submit_ambiguous
        )

    async def _heartbeat_loop(self) -> None:
        """Periodically publish heartbeat messages and cleanup orphans.

        When ``self.wallet_public_id`` is
        populated, the heartbeat topic gains a 5th ``{wallet_short}``
        segment so per-wallet executor instances publish on distinct
        topics (``system.heartbeats.executor.{exchange}.{wallet_short}``).
        The legacy 4-segment format still fires for template-mode
        executors with empty ``wallet_public_id`` (test fixtures that
        have not migrated to the per-wallet path).

        Status, lag and forensics come from
        :meth:`_compute_heartbeat_status`, wrapped so a crashing
        computation can NEVER kill the loop (that would be heartbeat
        absence) and never reports false HEALTHY — it degrades to WARNING
        with the failure in ``status_reasons``.
        """
        exchange_name = self._get_exchange_name()
        wallet_short = compute_wallet_short(self.wallet_public_id) if self.wallet_public_id else ""
        component = (
            f"executor.{exchange_name}.{wallet_short}"
            if wallet_short
            else f"executor.{exchange_name}"
        )
        while self.running:
            await asyncio.sleep(self.settings.zmq_heartbeat_interval_ms / 1000.0)
            if not self.running:
                break
            self._cleanup_expired_orphans()
            self.heartbeat_seq += 1
            try:
                status, lag_ms, reasons = self._compute_heartbeat_status()
            except Exception as exc:
                status = HealthStatusEnum.WARNING
                lag_ms = 0
                reasons = [f"status_computation_failed: {exc!r}"]
                logger.error(f"[{exchange_name}] Heartbeat status computation failed: {exc!r}")
            hb_topic = heartbeat_topic("executor", exchange_name, wallet_short=wallet_short)
            hb_msg = HeartbeatData(
                public_id=str(uuid7()),
                timestamp=datetime.now(UTC),
                session_id=self._tracker.session_id,
                sequence_id=self._tracker.next_sequence(hb_topic),
                component=component,
                sequence=self.heartbeat_seq,
                status=status,
                lag_ms=lag_ms,
                meta={
                    "running": self.running,
                    "exchange": exchange_name,
                    "wallet_public_id": self.wallet_public_id,
                    "broker_xsub": self.settings.zmq_broker_xsub,
                    "broker_xpub": self.settings.zmq_broker_xpub,
                    "status_reasons": cast("list[JsonValue]", list(reasons)),
                    "venue_recon_failure_count": self._venue_recon_failure_count,
                    "venue_rest_reachable": self._venue_recon_failure_count == 0,
                    "venue_health_halt_recommended": (
                        self._venue_recon_failure_count >= _VENUE_RECON_FAILURE_HALT_THRESHOLD
                    ),
                    "last_venue_recon_error": self._last_venue_recon_error,
                    "task_restarts": cast("dict[str, JsonValue]", dict(self._task_restarts)),
                    "exec_stream_restarts": self._exec_stream_restarts,
                    "unhealed_accept_events": len(self._unhealed_accept_events),
                    "parked_unknown": sum(
                        1
                        for entry in self.pending_orders.values()
                        if not entry.exchange_order_id and entry.submit_ambiguous
                    ),
                    "pending_orders": len(self.pending_orders),
                    "orphaned_executions": len(self.orphaned_executions),
                },
            )
            await self._publish_heartbeat(hb_topic, hb_msg)

    async def _publish_heartbeat(self, topic: str, message: HeartbeatData) -> None:
        """Send a complete heartbeat message to ZMQ.

        Args:
            topic: Heartbeat ZMQ topic string.
            message: Complete HeartbeatData with provenance set.
        """
        if not self.msg_publisher or not self.running:
            return
        try:
            await self.msg_publisher.send(topic, message)
        except Exception as e:
            logger.error(f"Error publishing heartbeat: {e}")

    def _handle_symbol_alias_update(self, payload: str) -> None:
        """Handle symbol alias cache invalidation message.

        Args:
            payload: JSON payload string from ZMQ message.
        """
        exchange_name = self._get_exchange_name()
        try:
            SymbolAliasUpdateData.from_json(payload)
            logger.info(f"[{exchange_name}] Received symbol alias cache invalidation")
            SymbolMapperService.get_instance().trigger_cache_invalidation(fail_fast=False)
            logger.debug(f"[{exchange_name}] Symbol alias cache invalidated successfully")
        except Exception as e:
            logger.error(f"[{exchange_name}] Error handling symbol alias update: {e}")

    def _handle_settings_update(self, payload: str) -> None:
        """Handle settings update message and refresh cached settings.

        Args:
            payload: JSON payload string from ZMQ message.
        """
        exchange_name = self._get_exchange_name()
        try:
            envelope = SettingChangedData.from_json(payload)
            settings_service = SettingsService.get_instance()
            if settings_service:
                parsed_value = settings_service._parse_value(envelope.value)
                settings_service._cache[envelope.key] = parsed_value
                logger.info(f"[{exchange_name}] Setting {envelope.key} updated via ZMQ event")
        except Exception as e:
            logger.error(f"[{exchange_name}] Error handling settings update: {e}")

    def get_status(self) -> dict[str, Any]:
        """Return the current status of the executor service.

        Returns:
            Dictionary containing running state and connection info.
        """
        return {
            "running": self.running,
            "exchange": self._get_exchange_name(),
            "broker_xsub": self.settings.zmq_broker_xsub,
            "broker_xpub": self.settings.zmq_broker_xpub,
            "heartbeat_seq": self.heartbeat_seq,
        }
