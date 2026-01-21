"""Topic validation for ZMQ pub/sub messaging.

This module provides validation functions for topic strings used in the
ZMQ messaging system. Topics must follow a structured hierarchy that
ensures messages are routed correctly and subscriptions are meaningful.

The validation enforces:
- Correct topic category prefixes
- Valid exchange names from database
- Valid instrument symbols from database
- Proper data type suffixes
- Correct segment counts for each category

Functions
---------
validate_topic
    Validate a complete topic string.
validate_subscription_pattern
    Validate a subscription pattern (topic or prefix).

Exceptions
----------
TopicValidationError
    Raised when publishing/subscribing to invalid topics.

Topic Categories
----------------
market
    Real-time market data (ticks, candles, trades, book).
orders
    Order lifecycle messages (requests, status).
executions
    Trade execution notifications (fills).
signals
    Trading signals from strategies.
system
    System-level messages (heartbeats, settings, mappings).
admin
    Administrative commands and responses.

Example:
-------
Validate before publishing::

    is_valid, error = validate_topic("market.kraken.BTC-USD.ticks")
    if not is_valid:
        raise TopicValidationError(error)

Validate subscription pattern::

    is_valid, error = validate_subscription_pattern("market.kraken.")
    if is_valid:
        subscriber.subscribe(pattern)
"""

import logging
import re

from snapper.infrastructure.symbols.functions import get_available_exchanges
from snapper.infrastructure.symbols.functions import get_available_symbols

__all__ = ["validate_topic", "validate_subscription_pattern", "TopicValidationError"]
logger = logging.getLogger(__name__)


class TopicValidationError(ValueError):
    """Exception raised when a topic string fails validation.

    Inherits from ValueError for compatibility with general validation
    error handling patterns.
    """

    pass


def validate_topic(topic: str) -> tuple[bool, str]:
    """Validate a complete topic string.

    Dispatches to category-specific validators based on topic prefix.

    Args:
        topic: Topic string to validate (e.g., "market.kraken.BTC-USD.ticks").

    Returns:
        Tuple of (is_valid, error_message). If valid, error_message is empty.

    Example:
        ::

            valid, err = validate_topic("market.kraken.BTC-USD.ticks")
            # valid=True, err=""

            valid, err = validate_topic("invalid")
            # valid=False, err="Unknown topic category: invalid"
    """
    if not topic:
        return False, "Topic cannot be empty"
    if topic.startswith("market."):
        return _validate_market_topic(topic)
    elif topic.startswith("orders."):
        return _validate_orders_topic(topic)
    elif topic.startswith("executions."):
        return _validate_executions_topic(topic)
    elif topic.startswith("signals."):
        return _validate_signal_topic(topic)
    elif topic.startswith("system."):
        return _validate_system_topic(topic)
    elif topic.startswith("admin."):
        return _validate_admin_topic(topic)
    else:
        return False, f"Unknown topic category: {topic.split('.', maxsplit=1)[0]}"


def validate_subscription_pattern(pattern: str) -> tuple[bool, str]:
    """Validate a subscription pattern.

    Patterns can be:
    - Complete topics: Validated as topics
    - Prefix patterns: End with '.', validated for structure

    ZMQ SUB uses prefix matching, so "market.kraken." matches all
    topics starting with that prefix.

    Args:
        pattern: Topic or prefix pattern to validate.

    Returns:
        Tuple of (is_valid, error_message). If valid, error_message is empty.

    Example:
        ::

            # Full topic subscription
            valid, err = validate_subscription_pattern("market.kraken.BTC-USD.ticks")

            # Prefix subscription
            valid, err = validate_subscription_pattern("market.kraken.")
    """
    if not pattern:
        return False, "Pattern cannot be empty"
    if "*" in pattern:
        return False, "Wildcards (*) not supported in ZMQ prefix matching"
    if pattern.endswith("."):
        return _validate_prefix_pattern(pattern)
    return validate_topic(pattern)


def _validate_market_topic(topic: str) -> tuple[bool, str]:
    """Validate market data topic structure.

    Expected format: market.{exchange}.{instrument}.{type}[.{timeframe}]

    Args:
        topic: Topic string starting with "market.".

    Returns:
        Tuple of (is_valid, error_message).
    """
    segments = topic.split(".")
    if topic.endswith("."):
        return (
            False,
            "Market topic must have 4-5 segments: market.{exchange}.{instrument}.{data_type}[.{timeframe}]",
        )
    if len(segments) not in (4, 5):
        return (
            False,
            "Market topic must have 4-5 segments: market.{exchange}.{instrument}.{data_type}[.{timeframe}]",
        )
    category = segments[0]
    exchange = segments[1]
    instrument = segments[2]
    data_type = segments[3]
    if category != "market":
        return False, f"Expected 'market' category, got '{category}'"
    valid_inst, err_inst = _validate_instrument(instrument)
    if not valid_inst:
        return False, err_inst
    valid_exch, err_exch = _validate_exchange(exchange)
    if not valid_exch:
        return False, err_exch
    if data_type not in {"candles", "ticks", "trades"}:
        return (
            False,
            f"Invalid market data type '{data_type}'. Must be: candles, ticks, trades",
        )
    if data_type == "candles":
        if len(segments) != 5:
            return (
                False,
                f"Candles topic must include timeframe: market.{exchange}.{instrument}.candles.<timeframe>",
            )
        candle_timeframe = segments[4]
        if not _is_valid_timeframe(candle_timeframe):
            return (
                False,
                f"Invalid timeframe '{candle_timeframe}'. Must match pattern like: 1m, 5m, 15m, 1h, 4h, 1d",
            )
    if data_type != "candles" and len(segments) == 5:
        return (
            False,
            f"Only candles topics support timeframe. Remove '.{segments[4]}' from {data_type} topic",
        )
    return True, ""


def _validate_orders_topic(topic: str) -> tuple[bool, str]:
    """Validate order topic structure.

    Expected format: orders.{exchange}.{instrument}.{type}
    where type is 'new' or 'status'.

    Args:
        topic: Topic string starting with "orders.".

    Returns:
        Tuple of (is_valid, error_message).
    """
    segments = topic.split(".")
    if topic.endswith("."):
        return (
            False,
            "Orders topic must have 4 segments: orders.{exchange}.{instrument}.{type}",
        )
    if len(segments) != 4:
        return (
            False,
            "Orders topic must have 4 segments: orders.{exchange}.{instrument}.{type}",
        )
    category, exchange, instrument, order_type = segments
    if category != "orders":
        return False, f"Expected 'orders' category, got '{category}'"
    valid_exch, err_exch = _validate_exchange(exchange)
    if not valid_exch:
        return False, err_exch
    valid_inst, err_inst = _validate_instrument(instrument)
    if not valid_inst:
        return False, err_inst
    if order_type not in {"new", "status"}:
        return False, f"Invalid order type '{order_type}'. Must be: new, status"
    return True, ""


def _validate_executions_topic(topic: str) -> tuple[bool, str]:
    """Validate execution topic structure.

    Expected format: executions.{exchange}.{instrument}.{type}
    where type is typically 'fill'.

    Args:
        topic: Topic string starting with "executions.".

    Returns:
        Tuple of (is_valid, error_message).
    """
    segments = topic.split(".")
    if topic.endswith("."):
        return (
            False,
            "Executions topic must have 4 segments: executions.{exchange}.{instrument}.{type}",
        )
    if len(segments) != 4:
        return (
            False,
            "Executions topic must have 4 segments: executions.{exchange}.{instrument}.{type}",
        )
    category, exchange, instrument, execution_type = segments
    if category != "executions":
        return False, f"Expected 'executions' category, got '{category}'"
    valid_exch, err_exch = _validate_exchange(exchange)
    if not valid_exch:
        return False, err_exch
    valid_inst, err_inst = _validate_instrument(instrument)
    if not valid_inst:
        return False, err_inst
    if execution_type not in {"fill"}:
        return False, f"Invalid execution type '{execution_type}'. Must be: fill"
    return True, ""


def _validate_signal_topic(topic: str) -> tuple[bool, str]:
    """Validate trading signal topic structure.

    Expected format:
    - Live: signals.{exchange}.{instrument}.live
    - Paper: signals.paper.{instrument}.{strategy_id}

    Args:
        topic: Topic string starting with "signals.".

    Returns:
        Tuple of (is_valid, error_message).
    """
    segments = topic.split(".")
    if topic.endswith("."):
        return (
            False,
            (
                "Signal topic must have 4 segments: signals.{exchange}.{instrument}.live (LIVE) "
                "or signals.paper.{instrument}.{strategy_id} (PAPER)"
            ),
        )
    if len(segments) != 4:
        return (
            False,
            (
                "Signal topic must have 4 segments: signals.{exchange}.{instrument}.live (LIVE) "
                "or signals.paper.{instrument}.{strategy_id} (PAPER)"
            ),
        )
    if segments[0] != "signals":
        return False, f"Expected 'signals' category, got '{segments[0]}'"
    exchange = segments[1]
    instrument = segments[2]
    type_or_strategy = segments[3]
    valid, msg = _validate_signal_exchange(exchange)
    if not valid:
        return False, msg
    valid, msg = _validate_instrument(instrument)
    if not valid:
        return False, msg
    if exchange != "paper":
        if type_or_strategy != "live":
            return (
                False,
                f"LIVE signal topics must use '.live' as 4th segment, got '.{type_or_strategy}'",
            )
        return True, ""
    return True, ""


def _validate_signal_exchange(exchange: str) -> tuple[bool, str]:
    """Validate exchange name for signal topics.

    Args:
        exchange: Exchange name to validate.

    Returns:
        Tuple of (is_valid, error_message).
    """
    valid_exchanges = set(get_available_exchanges())
    if exchange not in valid_exchanges:
        return (
            False,
            f"Invalid exchange '{exchange}'. Must be: {', '.join(sorted(valid_exchanges))}",
        )
    return True, ""


def _validate_system_topic(topic: str) -> tuple[bool, str]:
    """Validate system topic structure.

    Expected formats:
    - system.heartbeats[.{component_type}[.{component_name}]]
    - system.symbol_mappings
    - system.settings

    Args:
        topic: Topic string starting with "system.".

    Returns:
        Tuple of (is_valid, error_message).
    """
    segments = topic.split(".")
    if topic == "system.heartbeats.":
        return False, "System topic cannot end with '.'"
    if topic.endswith(".") and len(segments) == 2:
        return False, "System topic cannot end with '.'"
    if len(segments) < 2:
        return False, "System topic must have at least 2 segments: system.{type}"
    category, system_type = segments[0], segments[1]
    if category != "system":
        return False, f"Expected 'system' category, got '{category}'"
    if system_type == "heartbeats":
        if len(segments) == 2:
            return True, ""
        component_type = segments[2]
        if component_type in {"strategy", "executor"}:
            if len(segments) >= 4:
                return True, ""
            return False, f"system.heartbeats.{component_type} requires component name"
        elif component_type == "feed":
            if len(segments) == 4:
                return True, ""
            return False, "system.heartbeats.feed requires exactly exchange (4 segments)"
        else:
            return False, f"Invalid heartbeat component type '{component_type}'"
    elif system_type in {"symbol_mappings", "settings"}:
        if len(segments) != 2:
            return False, f"system.{system_type} must have exactly 2 segments"
        return True, ""
    else:
        return (
            False,
            f"Invalid system type '{system_type}'. Must be: heartbeats, settings, symbol_mappings",
        )


def _validate_admin_topic(topic: str) -> tuple[bool, str]:
    """Validate admin topic structure.

    Expected format: admin.{resource}

    Args:
        topic: Topic string starting with "admin.".

    Returns:
        Tuple of (is_valid, error_message).
    """
    segments = topic.split(".")
    if topic.endswith("."):
        return False, "Admin topic must have 2 segments: admin.{resource}"
    if len(segments) != 2:
        return False, "Admin topic must have 2 segments: admin.{resource}"
    category, resource = segments
    if category != "admin":
        return False, f"Expected 'admin' category, got '{category}'"
    return True, ""


def _validate_prefix_pattern(pattern: str) -> tuple[bool, str]:
    """Validate a subscription prefix pattern.

    Prefix patterns end with '.' and match all topics with that prefix.
    Validates that the prefix follows valid topic hierarchy.

    Args:
        pattern: Subscription pattern ending with '.'.

    Returns:
        Tuple of (is_valid, error_message).
    """
    if not pattern.endswith("."):
        return False, "Prefix must end with dot"
    segments = pattern[:-1].split(".")
    for segment in segments:
        if not segment:
            return False, "Prefix segments cannot be empty"
    category = segments[0]
    valid_categories = {"market", "orders", "executions", "signals", "strategy", "system", "admin"}
    if category not in valid_categories:
        return False, f"Unknown topic category: {category}"
    if (
        category == "market"
        and len(segments) >= 2
        or category in ("orders", "executions")
        and len(segments) >= 2
    ):
        exchange = segments[1]
        valid, err = _validate_exchange(exchange)
        if not valid:
            return False, err
        if len(segments) >= 3:
            instrument = segments[2]
            valid, err = _validate_instrument(instrument)
            if not valid:
                return False, err
    elif category == "signals" and len(segments) >= 2:
        exchange = segments[1]
        valid, err = _validate_signal_exchange(exchange)
        if not valid:
            return False, err
        if len(segments) >= 3:
            instrument = segments[2]
            valid, err = _validate_instrument(instrument)
            if not valid:
                return False, err
    return True, ""


def _validate_instrument(instrument: str) -> tuple[bool, str]:
    """Validate instrument symbol exists in database.

    Args:
        instrument: Instrument symbol (e.g., "BTC-USD").

    Returns:
        Tuple of (is_valid, error_message).
    """
    if not instrument:
        return False, "Instrument cannot be empty"
    all_instruments = get_available_symbols()
    if instrument not in all_instruments:
        return (
            False,
            f"Unknown instrument '{instrument}'. Must be in database instruments table.",
        )
    return True, ""


def _validate_exchange(exchange: str) -> tuple[bool, str]:
    """Validate exchange name exists in supported exchanges.

    Args:
        exchange: Exchange name (e.g., "kraken").

    Returns:
        Tuple of (is_valid, error_message).
    """
    if not exchange:
        return False, "Exchange cannot be empty"
    supported_exchanges = set(get_available_exchanges())
    if exchange not in supported_exchanges:
        return (
            False,
            f"Unknown exchange '{exchange}'. Must be one of: {', '.join(sorted(supported_exchanges))}",
        )
    return True, ""


def _is_valid_timeframe(timeframe: str) -> bool:
    """Check if timeframe string matches valid pattern.

    Valid timeframes: digit(s) + unit (m=minute, h=hour, d=day, w=week, M=month).
    Examples: 1m, 5m, 15m, 1h, 4h, 1d, 1w, 1M.

    Args:
        timeframe: Timeframe string to validate.

    Returns:
        True if timeframe matches valid pattern.
    """
    pattern = r"^\d+[mhdwM]$"
    return bool(re.match(pattern, timeframe))
