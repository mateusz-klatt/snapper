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

Builder Functions:
    market_topic: Build market data topic string.
    order_command_topic: Build order command topic string.
    order_event_topic: Build order event topic string.
    signal_topic: Build signal topic string.
    heartbeat_topic: Build heartbeat topic string.
    system_topic: Build system topic string.
    admin_topic: Build admin topic string.

Parser Functions:
    parse_market_topic: Parse market.* topic into components.
    parse_order_command_topic: Parse orders.commands.* topic into components.
    parse_order_event_topic: Parse orders.events.* topic into components.
    parse_signal_topic: Parse signals.* topic into components.
"""

from dataclasses import dataclass
from typing import Literal

OrderCommand = Literal["submit", "cancel", "replace"]
"""Valid order command types for orders.commands.* topics."""

OrderEvent = Literal[
    "submitted", "accepted", "rejected", "fill", "cancelled", "expired", "replaced"
]
"""Valid order event types for orders.events.* topics."""

MarketDataType = Literal["tick", "ticks", "trades", "book", "candles"]
"""Valid market data types for market.* topics."""


def market_topic(
    exchange: str,
    instrument: str,
    data_type: MarketDataType,
    timeframe: str | None = None,
    source_exchange: str | None = None,
) -> str:
    """Build a market data topic string.

    Args:
        exchange: Exchange name or TradingExchange enum.
        instrument: Trading instrument symbol (e.g., 'BTC-USD').
        data_type: Type of market data ('tick', 'trades', 'book', 'candles').
        timeframe: Candle timeframe (required when data_type is 'candles').
        source_exchange: Source exchange for paper replay topics.

    Returns:
        Formatted topic string. For paper replay with source exchange,
        returns 'market.paper.{source_exchange}.{instrument}.{type}[.{timeframe}]'.

    Raises:
        ValueError: If timeframe is missing for candles data type.

    Examples:
        >>> market_topic("kraken", "BTC-USD", "tick")
        'market.kraken.BTC-USD.tick'
        >>> market_topic("kraken", "BTC-USD", "candles", "1m")
        'market.kraken.BTC-USD.candles.1m'
        >>> market_topic("paper", "BTC-USD", "ticks", source_exchange="kraken")
        'market.paper.kraken.BTC-USD.ticks'
    """
    exchange_str = exchange
    if exchange_str == "paper":
        if not source_exchange:
            raise ValueError("source_exchange is required for paper market topics")
        if data_type == "candles":
            if not timeframe:
                raise ValueError("timeframe is required for candles data type")
            return f"market.paper.{source_exchange}.{instrument}.candles.{timeframe}"
        return f"market.paper.{source_exchange}.{instrument}.{data_type}"
    if data_type == "candles":
        if not timeframe:
            raise ValueError("timeframe is required for candles data type")
        return f"market.{exchange_str}.{instrument}.candles.{timeframe}"
    return f"market.{exchange_str}.{instrument}.{data_type}"


def order_command_topic(
    exchange: str,
    instrument: str,
    command: OrderCommand,
) -> str:
    """Build an order command topic string.

    Order commands flow from trader/strategy to executor.

    Args:
        exchange: Exchange name or TradingExchange enum.
        instrument: Trading instrument symbol (e.g., 'BTC-USD').
        command: Command type ('submit', 'cancel', 'replace').

    Returns:
        Formatted topic string like 'orders.commands.kraken.BTC-USD.submit'.

    Examples:
        >>> order_command_topic("kraken", "BTC-USD", "submit")
        'orders.commands.kraken.BTC-USD.submit'
        >>> order_command_topic(TradingExchange.PAPER, "ETH-USD", "cancel")
        'orders.commands.paper.ETH-USD.cancel'
    """
    exchange_str = exchange
    return f"orders.commands.{exchange_str}.{instrument}.{command}"


def order_event_topic(
    exchange: str,
    instrument: str,
    event: OrderEvent,
) -> str:
    """Build an order event topic string.

    Order events flow from executor to trader/UI.

    Args:
        exchange: Exchange name or TradingExchange enum.
        instrument: Trading instrument symbol (e.g., 'BTC-USD').
        event: Event type ('submitted', 'accepted', 'rejected', 'fill', etc.).

    Returns:
        Formatted topic string like 'orders.events.kraken.BTC-USD.fill'.

    Examples:
        >>> order_event_topic("kraken", "BTC-USD", "submitted")
        'orders.events.kraken.BTC-USD.submitted'
        >>> order_event_topic("kraken", "BTC-USD", "fill")
        'orders.events.kraken.BTC-USD.fill'
    """
    exchange_str = exchange
    return f"orders.events.{exchange_str}.{instrument}.{event}"


def signal_topic(
    exchange: str,
    instrument: str,
    signal_type: str = "live",
) -> str:
    """Build a signal topic string.

    Args:
        exchange: Exchange name or TradingExchange enum.
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


def heartbeat_topic(component: str, name: str) -> str:
    """Build a heartbeat topic string.

    Args:
        component: Component category (e.g., 'feed', 'executor', 'strategy').
        name: Specific component name (e.g., 'kraken', 'paper').

    Returns:
        Formatted topic string like 'system.heartbeats.executor.kraken'.

    Examples:
        >>> heartbeat_topic("executor", "kraken")
        'system.heartbeats.executor.kraken'
        >>> heartbeat_topic("feed", "polygon")
        'system.heartbeats.feed.polygon'
    """
    return f"system.heartbeats.{component}.{name}"


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


def order_commands_prefix(exchange: str) -> str:
    """Build subscription prefix for all order commands for an exchange.

    Used by executors to subscribe to all commands for their exchange.

    Args:
        exchange: Exchange name or TradingExchange enum.

    Returns:
        Prefix string like 'orders.commands.kraken.'.

    Examples:
        >>> order_commands_prefix("kraken")
        'orders.commands.kraken.'
    """
    exchange_str = exchange
    return f"orders.commands.{exchange_str}."


def order_events_prefix(exchange: str | None = None) -> str:
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
        suffix: Command (submit/cancel/replace) or event (fill/accepted/etc.).
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
        data_type: Market data type (candles/ticks/trades/book/tick).
        timeframe: Optional timeframe for candles topics.
        source_exchange: Source exchange for paper replay topics.
    """

    exchange: str
    instrument: str
    data_type: str
    timeframe: str | None
    source_exchange: str | None = None


@dataclass(frozen=True, slots=True)
class ParsedSignalTopic:
    """Parsed components of a signal topic."""

    exchange: str
    instrument: str
    signal_type: str


_MARKET_DATA_TYPES: set[str] = {"tick", "ticks", "trades", "book", "candles"}


def _build_market_topic_result(
    exchange: str,
    instrument: str,
    data_type: str,
    timeframe: str | None,
    source_exchange: str | None,
) -> ParsedMarketTopic | None:
    """Validate parsed market topic fields and build dataclass result."""
    if not exchange or not instrument or not data_type:
        return None
    if data_type not in _MARKET_DATA_TYPES:
        return None
    if data_type == "candles" and not timeframe:
        return None
    if data_type != "candles" and timeframe is not None:
        return None
    return ParsedMarketTopic(
        exchange=exchange,
        instrument=instrument,
        data_type=data_type,
        timeframe=timeframe,
        source_exchange=source_exchange,
    )


def parse_market_topic(topic: str) -> ParsedMarketTopic | None:
    """Parse a market topic into its components.

    Supports two market topic variants:
    - Live: market.{exchange}.{instrument}.{type}[.{timeframe}]
    - Paper replay: market.paper.{source_exchange}.{instrument}.{type}[.{timeframe}]

    Args:
        topic: Full topic string to parse.

    Returns:
        ParsedMarketTopic on success, None for malformed topics.
    """
    parts = topic.split(".")
    if len(parts) < 4 or parts[0] != "market":
        return None
    if parts[1] != "paper":
        if len(parts) not in (4, 5):
            return None
        timeframe = parts[4] if len(parts) == 5 else None
        return _build_market_topic_result(
            exchange=parts[1],
            instrument=parts[2],
            data_type=parts[3],
            timeframe=timeframe,
            source_exchange=None,
        )
    if len(parts) not in (5, 6):
        return None
    timeframe = parts[5] if len(parts) == 6 else None
    return _build_market_topic_result(
        exchange="paper",
        instrument=parts[3],
        data_type=parts[4],
        timeframe=timeframe,
        source_exchange=parts[2],
    )


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
        >>> parse_order_command_topic("market.kraken.tick")
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
        >>> parse_order_event_topic("orders.events.kraken.BTC-USD.fill")
        ParsedOrderTopic(exchange='kraken', instrument='BTC-USD', suffix='fill')
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
        >>> is_order_topic("orders.events.kraken.BTC-USD.fill")
        True
        >>> is_order_topic("market.kraken.BTC-USD.tick")
        False
    """
    parts = topic.split(".")
    if len(parts) < 2:
        return False
    two_level = f"{parts[0]}.{parts[1]}"
    return two_level in ("orders.commands", "orders.events")
