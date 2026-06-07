"""Topic string builders and parsers for ZMQ pub/sub messaging.

Provides centralized functions for constructing and parsing valid topic strings.
Using these builders/parsers eliminates hardcoded topic strings scattered throughout
the codebase and ensures consistency with the topic hierarchy contract.

Topic Hierarchy Contract
------------------------
- market.{exchange}.{instrument}.{type}[.{timeframe}]
- orders.commands.{exchange}.{instrument}.{command}
- orders.events.{exchange}.{instrument}.{event}
- signals.{exchange}.{instrument}.{signal_type}
- system.heartbeats.{component}.{name}
- system.{type}
- admin.{resource}
- accruals.{exchange}.{instrument}.{accrual_type}

Builder Functions:
    market_topic: Build market data topic string.
    order_command_topic: Build order command topic string.
    order_event_topic: Build order event topic string.
    signal_topic: Build signal topic string.
    heartbeat_topic: Build heartbeat topic string.
    system_topic: Build system topic string.
    admin_topic: Build admin topic string.
    accrual_topic: Build accrual ledger topic string.

Parser Functions:
    parse_market_topic: Parse market.* topic into components.
    parse_order_command_topic: Parse orders.commands.* topic into components.
    parse_order_event_topic: Parse orders.events.* topic into components.
    parse_signal_topic: Parse signals.* topic into components.
"""

from dataclasses import dataclass
from typing import Any
from typing import cast

from snapper.api.schemas.base import StrictDataSchema
from snapper.core.types import AllExchange
from snapper.core.types import ExchangeEnum
from snapper.core.types import MarketDataExchange
from snapper.core.types import MarketDataType
from snapper.core.types import MarketDataTypeEnum
from snapper.core.types import OrderCommand
from snapper.core.types import OrderCommandEnum
from snapper.core.types import OrderEvent
from snapper.core.types import OrderEventEnum
from snapper.core.types import OrderExchange


def market_topic(
    exchange: AllExchange,
    instrument: str,
    data_type: MarketDataType,
    timeframe: str | None = None,
    source_exchange: MarketDataExchange | None = None,
) -> str:
    """Build a market data topic string.

    Args:
        exchange: Exchange name from AllExchange.
            This builder accepts all known exchange identifiers, including
            venues that may be enabled for live publishing in the future.
            Runtime support for live feed topics is enforced separately by
            topic validation.
        instrument: Trading instrument symbol (e.g., 'BTC-USD').
        data_type: Type of market data ('ticks', 'trades', 'candles').
        timeframe: Candle timeframe (required when data_type is 'candles').
        source_exchange: Source exchange for paper replay topics.

    Returns:
        Formatted topic string. For paper replay with source exchange,
        returns 'market.paper.{source_exchange}.{instrument}.{type}[.{timeframe}]'.

    Raises:
        ValueError: If timeframe is missing for candles data type.

    Examples:
        >>> market_topic("kraken", "BTC-USD", "ticks")
        'market.kraken.BTC-USD.ticks'
        >>> market_topic("kraken", "BTC-USD", "candles", "1m")
        'market.kraken.BTC-USD.candles.1m'
        >>> market_topic("paper", "BTC-USD", "ticks", source_exchange="kraken")
        'market.paper.kraken.BTC-USD.ticks'
    """
    exchange_str = exchange
    if exchange_str == ExchangeEnum.PAPER:
        if not source_exchange:
            raise ValueError("source_exchange is required for paper market topics")
        if data_type == MarketDataTypeEnum.CANDLES:
            if not timeframe:
                raise ValueError("timeframe is required for candles data type")
            return f"market.paper.{source_exchange}.{instrument}.candles.{timeframe}"
        return f"market.paper.{source_exchange}.{instrument}.{data_type}"
    if data_type == MarketDataTypeEnum.CANDLES:
        if not timeframe:
            raise ValueError("timeframe is required for candles data type")
        return f"market.{exchange_str}.{instrument}.candles.{timeframe}"
    return f"market.{exchange_str}.{instrument}.{data_type}"


def order_command_topic(
    exchange: OrderExchange,
    instrument: str,
    command: OrderCommand,
) -> str:
    """Build an order command topic string.

    Order commands flow from trader/strategy to executor.

    Args:
        exchange: Exchange name (OrderExchange literal).
        instrument: Trading instrument symbol (e.g., 'BTC-USD').
        command: Command type ('submit', 'cancel', 'replace').

    Returns:
        Formatted topic string like 'orders.commands.kraken.BTC-USD.submit'.

    Examples:
        >>> order_command_topic("kraken", "BTC-USD", "submit")
        'orders.commands.kraken.BTC-USD.submit'
        >>> order_command_topic("paper", "ETH-USD", "cancel")
        'orders.commands.paper.ETH-USD.cancel'
    """
    exchange_str = exchange
    return f"orders.commands.{exchange_str}.{instrument}.{command}"


def order_event_topic(
    exchange: OrderExchange,
    instrument: str,
    event: OrderEvent,
) -> str:
    """Build an order event topic string.

    Order events flow from executor to trader/UI.

    Args:
        exchange: Exchange name (OrderExchange literal).
        instrument: Trading instrument symbol (e.g., 'BTC-USD').
        event: Event type ('submitted', 'accepted', 'rejected', 'executed', etc.).

    Returns:
        Formatted topic string like 'orders.events.kraken.BTC-USD.executed'.

    Examples:
        >>> order_event_topic("kraken", "BTC-USD", "submitted")
        'orders.events.kraken.BTC-USD.submitted'
        >>> order_event_topic("kraken", "BTC-USD", "executed")
        'orders.events.kraken.BTC-USD.executed'
    """
    exchange_str = exchange
    return f"orders.events.{exchange_str}.{instrument}.{event}"


def signal_topic(
    exchange: OrderExchange,
    instrument: str,
    signal_type: str = "live",
) -> str:
    """Build a signal topic string.

    Args:
        exchange: Exchange name (OrderExchange literal).
        instrument: Trading instrument symbol (e.g., 'BTC-USD').
        signal_type: Signal type identifier (default 'live', or strategy name).

    Returns:
        Formatted topic string like 'signals.kraken.BTC-USD.live'.

    Examples:
        >>> signal_topic("kraken", "BTC-USD")
        'signals.kraken.BTC-USD.live'
        >>> signal_topic("paper", "ETH-USD", "momentum_strategy")
        'signals.paper.ETH-USD.momentum_strategy'
    """
    exchange_str = exchange
    return f"signals.{exchange_str}.{instrument}.{signal_type}"


def heartbeat_topic(component: str, name: str, wallet_short: str = "") -> str:
    """Build a heartbeat topic string.

    Args:
        component: Component category (e.g., 'feed', 'executor', 'strategy').
        name: Specific component name (e.g., 'kraken', 'paper').
        wallet_short: Optional 12-hex-char wallet prefix.
            When supplied for an executor component, the topic gains a
            5th segment ``.{wallet_short}`` so per-wallet executor
            instances publish on distinct heartbeat topics. Empty
            string (the default) preserves the legacy 4-segment layout
            used by template executors and non-executor components.

    Returns:
        Formatted topic string like 'system.heartbeats.executor.kraken'
        or 'system.heartbeats.executor.kraken.019d6ca45f2e' for a
        per-wallet instance.

    Examples:
        >>> heartbeat_topic("executor", "kraken")
        'system.heartbeats.executor.kraken'
        >>> heartbeat_topic("executor", "kraken", wallet_short="019d6ca45f2e")
        'system.heartbeats.executor.kraken.019d6ca45f2e'
        >>> heartbeat_topic("feed", "polygon")
        'system.heartbeats.feed.polygon'
    """
    base = f"system.heartbeats.{component}.{name}"
    if wallet_short:
        return f"{base}.{wallet_short}"
    return base


def system_topic(topic_type: str) -> str:
    """Build a system topic string.

    Args:
        topic_type: System topic type (e.g., 'symbol_aliases', 'settings').

    Returns:
        Formatted topic string like 'system.symbol_aliases'.

    Examples:
        >>> system_topic("symbol_aliases")
        'system.symbol_aliases'
        >>> system_topic("settings")
        'system.settings'
    """
    return f"system.{topic_type}"


def admin_topic(resource: str) -> str:
    """Build an admin topic string.

    Args:
        resource: Admin resource type (e.g., 'command', 'users').

    Returns:
        Formatted topic string like 'admin.command'.

    Examples:
        >>> admin_topic("command")
        'admin.command'
    """
    return f"admin.{resource}"


def accrual_topic(
    exchange: OrderExchange,
    instrument: str,
    accrual_type: str,
) -> str:
    """Build an accrual ledger topic string.

    Args:
        exchange: Exchange name (OrderExchange literal).
        instrument: Trading instrument symbol (e.g., 'BTC-USD').
        accrual_type: One of 'funding', 'rollover', or 'borrow'.

    Returns:
        Formatted topic string like 'accruals.kraken.BTC-USD.rollover'.

    Examples:
        >>> accrual_topic("kraken", "BTC-USD", "rollover")
        'accruals.kraken.BTC-USD.rollover'
        >>> accrual_topic("kraken_futures", "PF_XBTUSD", "funding")
        'accruals.kraken_futures.PF_XBTUSD.funding'
    """
    exchange_str = exchange
    return f"accruals.{exchange_str}.{instrument}.{accrual_type}"


def alerts_topic(user_public_id: str, alert_type: str) -> str:
    """Build an iOS push-notification alert topic string.

    Args:
        user_public_id: Recipient user UUID7.
        alert_type: One of the enumerated ``AlertType`` values (see
            ``snapper.messaging.schemas.data.AlertType``).

    Returns:
        Formatted topic string
        ``alerts.{user_public_id}.{alert_type}`` — validated by
        ``_validate_alerts_topic`` before publish.

    Examples:
        >>> alerts_topic("019dbb34-f439-77bd-afa8-ee5321d60307", "order_fill_full")
        'alerts.019dbb34-f439-77bd-afa8-ee5321d60307.order_fill_full'
    """
    return f"alerts.{user_public_id}.{alert_type}"


def plans_decisions_topic(plan_public_id: str) -> str:
    """Build a ``plans.decisions.{plan_public_id}`` topic string.

    Published by ``PlanExecutorService`` immediately after each
    ``ExecutionPlanDecision`` row commits. Subscribed by the notify
    sidecar's stop-loss rule.

    Args:
        plan_public_id: UUID7 of the parent ``ExecutionPlan`` row.

    Returns:
        Formatted topic string ``plans.decisions.{plan_public_id}``
        validated by ``_validate_plans_decisions_topic`` before publish.

    Examples:
        >>> plans_decisions_topic("019dbb34-f439-77bd-afa8-ee5321d60307")
        'plans.decisions.019dbb34-f439-77bd-afa8-ee5321d60307'
    """
    return f"plans.decisions.{plan_public_id}"


def order_commands_prefix(exchange: OrderExchange) -> str:
    """Build subscription prefix for all order commands for an exchange.

    Used by executors to subscribe to all commands for their exchange.

    Args:
        exchange: Exchange name (OrderExchange literal).

    Returns:
        Prefix string like 'orders.commands.kraken.'.

    Examples:
        >>> order_commands_prefix("kraken")
        'orders.commands.kraken.'
    """
    exchange_str = exchange
    return f"orders.commands.{exchange_str}."


def order_events_prefix(exchange: OrderExchange | None = None) -> str:
    """Build subscription prefix for order events.

    Args:
        exchange: Optional exchange to filter by. If None, subscribes to all.

    Returns:
        Prefix string like 'orders.events.' or 'orders.events.kraken.'.

    Examples:
        >>> order_events_prefix()
        'orders.events.'
        >>> order_events_prefix("kraken")
        'orders.events.kraken.'
    """
    if exchange is None:
        return "orders.events."
    exchange_str = exchange
    return f"orders.events.{exchange_str}."


@dataclass(frozen=True, slots=True)
class ParsedOrderTopic:
    """Parsed components of an order command or event topic.

    Attributes:
        exchange: Exchange name from topic segment.
        instrument: Instrument symbol from topic segment.
        suffix: Command (submit/cancel/replace) or event (execution/accepted/etc.).
    """

    exchange: str
    instrument: str
    suffix: str


@dataclass(frozen=True, slots=True)
class ParsedMarketTopic:
    """Parsed components of a market data topic.

    Attributes:
        exchange: Main exchange segment (or 'paper' for replay topics).
        instrument: Instrument symbol from topic.
        data_type: Market data type (candles/ticks/trades).
        timeframe: Optional timeframe for candles topics.
        source_exchange: Source exchange for paper replay topics.
    """

    exchange: str
    instrument: str
    data_type: MarketDataType
    timeframe: str | None
    source_exchange: str | None = None


@dataclass(frozen=True, slots=True)
class ParsedSignalTopic:
    """Parsed components of a signal topic."""

    exchange: str
    instrument: str
    signal_type: str


_MARKET_DATA_TYPES: set[str] = set(MarketDataTypeEnum)


def _build_market_topic_result(
    exchange: str,
    instrument: str,
    data_type: str,
    timeframe: str | None,
    source_exchange: str | None,
) -> ParsedMarketTopic | None:
    """Validate parsed market topic fields and build dataclass result.

    ``parse_market_topic`` already validates ``data_type`` membership
    in ``_MARKET_DATA_TYPES`` before calling this helper, so we only
    re-check the empty-string / candles-timeframe invariants here.
    """
    if not exchange or not instrument or not data_type:
        return None
    if data_type == MarketDataTypeEnum.CANDLES and not timeframe:
        return None
    if data_type != MarketDataTypeEnum.CANDLES and timeframe is not None:
        return None
    return ParsedMarketTopic(
        exchange=exchange,
        instrument=instrument,
        data_type=cast(MarketDataType, data_type),
        timeframe=timeframe,
        source_exchange=source_exchange,
    )


def parse_market_topic(topic: str) -> ParsedMarketTopic | None:
    """Parse a market topic into its components.

    Supports two market topic variants:
    - Live: ``market.{exchange}.{instrument}.{type}[.{timeframe}]``
    - Paper replay:
      ``market.paper.{source_exchange}.{instrument}.{type}[.{timeframe}]``

    Right-anchored parsing: ``{instrument}`` may contain dots (e.g.
    ``BRK.B`` for Berkshire Hathaway Class B). The parser anchors on
    the well-known prefix and suffix segments (``market`` / exchange /
    optional paper source / data_type / optional candles timeframe)
    and reassembles every leftover middle segment as the instrument.
    A left-anchored ``str.split('.')`` indexer would mis-parse
    ``market.kraken.BRK.B.ticks`` into ``instrument=BRK`` +
    ``data_type=B``; the right-anchored variant correctly recovers
    ``instrument=BRK.B`` + ``data_type=ticks``.

    Args:
        topic: Full topic string to parse.

    Returns:
        ParsedMarketTopic on success, None for malformed topics.
    """
    parts = topic.split(".")
    if len(parts) < 4 or parts[0] != "market":
        return None
    candidate_timeframe = parts[-1]
    timeframe: str | None = None
    if _is_valid_market_timeframe(candidate_timeframe):
        timeframe = candidate_timeframe
        data_type = parts[-2]
        body = parts[1:-2]
    else:
        data_type = parts[-1]
        body = parts[1:-1]
    if data_type not in _MARKET_DATA_TYPES:
        return None
    if body[0] == ExchangeEnum.PAPER:
        if len(body) < 3:
            return None
        return _build_market_topic_result(
            exchange=ExchangeEnum.PAPER,
            instrument=".".join(body[2:]),
            data_type=data_type,
            timeframe=timeframe,
            source_exchange=body[1],
        )
    if len(body) < 2:
        return None
    return _build_market_topic_result(
        exchange=body[0],
        instrument=".".join(body[1:]),
        data_type=data_type,
        timeframe=timeframe,
        source_exchange=None,
    )


def _is_valid_market_timeframe(token: str) -> bool:
    """Return True when ``token`` matches the candles-timeframe pattern.

    Candles topics carry a trailing timeframe segment such as ``1m`` or
    ``4h``; ticks/trades topics never do. The right-anchored parser
    uses this gate to decide whether to peel a timeframe segment off
    the right before reassembling the instrument from the middle
    tokens.

    Args:
        token: Topic segment to test.

    Returns:
        True if the token is a valid candles timeframe.
    """
    if not token or len(token) < 2:
        return False
    if not token[:-1].isdigit():
        return False
    return token[-1] in {"s", "m", "h", "d", "w", "M"}


def parse_order_command_topic(topic: str) -> ParsedOrderTopic | None:
    """Parse an order command topic into its components.

    Validates topic structure: orders.commands.{exchange}.{instrument}.{cmd}

    Args:
        topic: Full topic string to parse.

    Returns:
        ParsedOrderTopic with exchange, instrument, and command suffix,
        or None if topic is malformed.

    Examples:
        >>> parse_order_command_topic("orders.commands.kraken.BTC-USD.submit")
        ParsedOrderTopic(exchange='kraken', instrument='BTC-USD', suffix='submit')
        >>> parse_order_command_topic("market.kraken.ticks")
        None
    """
    parts = topic.split(".")
    if len(parts) != 5:
        return None
    if parts[0] != "orders" or parts[1] != "commands":
        return None
    return ParsedOrderTopic(exchange=parts[2], instrument=parts[3], suffix=parts[4])


def parse_order_event_topic(topic: str) -> ParsedOrderTopic | None:
    """Parse an order event topic into its components.

    Validates topic structure: orders.events.{exchange}.{instrument}.{event}

    Args:
        topic: Full topic string to parse.

    Returns:
        ParsedOrderTopic with exchange, instrument, and event suffix,
        or None if topic is malformed.

    Examples:
        >>> parse_order_event_topic("orders.events.kraken.BTC-USD.executed")
        ParsedOrderTopic(exchange='kraken', instrument='BTC-USD', suffix='executed')
        >>> parse_order_event_topic("orders.commands.kraken.BTC-USD.submit")
        None
    """
    parts = topic.split(".")
    if len(parts) != 5:
        return None
    if parts[0] != "orders" or parts[1] != "events":
        return None
    return ParsedOrderTopic(exchange=parts[2], instrument=parts[3], suffix=parts[4])


def parse_signal_topic(topic: str) -> ParsedSignalTopic | None:
    """Parse a signal topic into its components.

    Valid format: signals.{exchange}.{instrument}.{signal_type}

    Args:
        topic: Full topic string to parse.

    Returns:
        ParsedSignalTopic on success, None for malformed topics.
    """
    parts = topic.split(".")
    if len(parts) != 4:
        return None
    if parts[0] != "signals":
        return None
    if not parts[1] or not parts[2] or not parts[3]:
        return None
    return ParsedSignalTopic(exchange=parts[1], instrument=parts[2], signal_type=parts[3])


def is_order_topic(topic: str) -> bool:
    """Check if topic is an order topic (command or event).

    Uses 2-level prefix matching (orders.commands or orders.events).

    Args:
        topic: Topic string to check.

    Returns:
        True if topic starts with orders.commands or orders.events.

    Examples:
        >>> is_order_topic("orders.events.kraken.BTC-USD.executed")
        True
        >>> is_order_topic("market.kraken.BTC-USD.ticks")
        False
    """
    parts = topic.split(".")
    if len(parts) < 2:
        return False
    two_level = f"{parts[0]}.{parts[1]}"
    return two_level in ("orders.commands", "orders.events")


def heartbeat_topic_from_component(component: str) -> str:
    """Derive heartbeat topic from a dotted component name.

    The component name is expected to use dots as separators
    (e.g. 'executor.kraken', 'feed.kraken', 'strategy.momentum').

    Args:
        component: Dotted component identifier.

    Returns:
        Full heartbeat topic string.

    Examples:
        >>> heartbeat_topic_from_component("executor.kraken")
        'system.heartbeats.executor.kraken'
        >>> heartbeat_topic_from_component("feed.paper.kraken")
        'system.heartbeats.feed.paper.kraken'
    """
    return f"system.heartbeats.{component}"


def topic_for_message(data: StrictDataSchema[Any]) -> str:
    """Derive ZMQ topic from a Data schema instance.

    Uses isinstance dispatch to call the appropriate builder function
    for each message type. Raises ValueError for types that cannot
    derive a topic from their payload fields alone.

    Args:
        data: Any StrictDataSchema subclass instance.

    Returns:
        Fully-qualified ZMQ topic string.

    Raises:
        ValueError: If topic cannot be derived for the given type,
            or if required fields are missing (e.g. paper signal
            without strategy_name).
    """
    from snapper.messaging.schemas.data import AlertEventData
    from snapper.messaging.schemas.data import CandleData
    from snapper.messaging.schemas.data import ExecutionData
    from snapper.messaging.schemas.data import HeartbeatData
    from snapper.messaging.schemas.data import OrderCancelData
    from snapper.messaging.schemas.data import OrderData
    from snapper.messaging.schemas.data import OrderEventData
    from snapper.messaging.schemas.data import OrderReplaceData
    from snapper.messaging.schemas.data import OrderRequestData
    from snapper.messaging.schemas.data import ReplayEndData
    from snapper.messaging.schemas.data import ReplayStartData
    from snapper.messaging.schemas.data import SettingChangedData
    from snapper.messaging.schemas.data import SignalData
    from snapper.messaging.schemas.data import SymbolAliasUpdateData
    from snapper.messaging.schemas.data import TickData
    from snapper.messaging.schemas.data import TradeData

    match data:
        case TickData():
            return market_topic(data.exchange, data.instrument, MarketDataTypeEnum.TICKS)
        case CandleData():
            return market_topic(
                data.exchange, data.instrument, MarketDataTypeEnum.CANDLES, data.timeframe
            )
        case TradeData():
            return market_topic(data.exchange, data.instrument, MarketDataTypeEnum.TRADES)
        case OrderRequestData():
            return order_command_topic(data.exchange, data.instrument, OrderCommandEnum.SUBMIT)
        case OrderCancelData():
            return order_command_topic(data.exchange, data.instrument, OrderCommandEnum.CANCEL)
        case OrderReplaceData():
            return order_command_topic(data.exchange, data.instrument, OrderCommandEnum.REPLACE)
        case OrderData():
            return order_event_topic(data.exchange, data.instrument, cast(OrderEvent, data.status))
        case OrderEventData():
            return order_event_topic(data.exchange, data.instrument, data.event)
        case ExecutionData():
            return order_event_topic(data.exchange, data.instrument, OrderEventEnum.EXECUTED)
        case SignalData():
            if data.exchange == ExchangeEnum.PAPER:
                if not data.strategy_name:
                    raise ValueError("Paper signal requires strategy_name for topic derivation")
                return signal_topic(data.exchange, data.instrument, data.strategy_name)
            return signal_topic(data.exchange, data.instrument, "live")
        case HeartbeatData():
            return heartbeat_topic_from_component(data.component)
        case SettingChangedData():
            return system_topic("settings")
        case SymbolAliasUpdateData():
            return system_topic("symbol_aliases")
        case ReplayStartData():
            return "system.replay.start"
        case ReplayEndData():
            return "system.replay.end"
        case AlertEventData():
            return alerts_topic(data.user_public_id, data.alert_type)
        case _:
            raise ValueError(f"No topic derivation for {type(data).__name__}")
