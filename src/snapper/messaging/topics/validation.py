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
    Real-time market data (ticks, candles, trades).
orders.commands
    Order command messages (submit, cancel, replace).
orders.events
    Order event notifications (submitted, accepted, rejected, fill, cancelled, expired).
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

    is_valid, error = validate_subscription_pattern("orders.commands.kraken.")
    if is_valid:
        subscriber.subscribe(pattern)
"""

import logging
import re
from collections.abc import Callable

from snapper.core.types import MarketDataTypeEnum
from snapper.core.types import OrderCommandEnum
from snapper.infrastructure.symbols.functions import get_available_exchanges
from snapper.infrastructure.symbols.functions import get_available_symbols
from snapper.infrastructure.symbols.functions import get_market_data_exchanges
from snapper.infrastructure.symbols.functions import get_market_subscribe_exchanges

__all__ = ["validate_topic", "validate_subscription_pattern", "TopicValidationError"]
logger = logging.getLogger(__name__)


class TopicValidationError(ValueError):
    """Exception raised when a topic string fails validation.

    Inherits from ValueError for compatibility with general validation
    error handling patterns.
    """

    pass


def _get_topic_prefix_validators() -> list[tuple[str, Callable[[str], tuple[bool, str]]]]:
    """Return the ordered list of (prefix, validator) pairs for topic dispatch.

    Returns:
        List of tuples mapping topic prefixes to their validator functions.
    """
    return [
        ("market.", _validate_market_topic),
        ("orders.commands.", _validate_orders_commands_topic),
        ("orders.events.", _validate_orders_events_topic),
        ("signals.", _validate_signal_topic),
        ("system.", _validate_system_topic),
        ("admin.", _validate_admin_topic),
        ("accruals.", _validate_accruals_topic),
    ]


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
    if topic.startswith("orders.") and not topic.startswith(("orders.commands.", "orders.events.")):
        return False, "Orders topics must use 'orders.commands.' or 'orders.events.' prefix"
    for prefix, validator in _get_topic_prefix_validators():
        if topic.startswith(prefix):
            return validator(topic)
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


_MARKET_TOPIC_FMT = (
    "Market topic must have 4-6 segments: "
    "market.{exchange}.{instrument}.{data_type}[.{timeframe}] "
    "or market.paper.{source_exchange}.{instrument}.{data_type}[.{timeframe}]"
)


def _validate_candle_timeframe(
    segments: list[str], exchange: str, instrument: str
) -> tuple[bool, str]:
    """Validate the timeframe segment of a candles market topic.

    Args:
        segments: Split topic segments.
        exchange: Exchange name.
        instrument: Instrument symbol.

    Returns:
        Tuple of (is_valid, error_message).
    """
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
    return True, ""


def _validate_market_data_type(
    data_type: str,
    segment_count: int,
    timeframe: str | None,
    exchange_name: str,
    instrument_name: str,
) -> tuple[bool, str]:
    """Validate market data type and optional timeframe.

    Args:
        data_type: The market data type (candles, ticks, trades).
        segment_count: Total number of segments in the topic.
        timeframe: Timeframe string for candles topics, or None.
        exchange_name: Exchange name for error messages.
        instrument_name: Instrument name for error messages.

    Returns:
        Tuple of (is_valid, error_message).
    """
    valid_types = set(MarketDataTypeEnum)
    if data_type not in valid_types:
        return (
            False,
            f"Invalid market data type '{data_type}'. Must be: {', '.join(sorted(valid_types))}",
        )
    if data_type == MarketDataTypeEnum.CANDLES:
        if timeframe is None:
            return (
                False,
                f"Candles topic must include timeframe: "
                f"market.{exchange_name}.{instrument_name}.candles.<timeframe>",
            )
        if not _is_valid_timeframe(timeframe):
            return (
                False,
                f"Invalid timeframe '{timeframe}'. Must match pattern like: "
                "1m, 5m, 15m, 1h, 4h, 1d",
            )
        return True, ""
    if segment_count > 0 and timeframe is not None:
        return (
            False,
            f"Only candles topics support timeframe. Remove '.{timeframe}' from {data_type} topic",
        )
    return True, ""


def _validate_standard_market_topic(segments: list[str]) -> tuple[bool, str]:
    """Validate a non-paper market topic.

    Expected format segments: [market, exchange, instrument, data_type, ?timeframe]

    Args:
        segments: Split topic segments (4 or 5 elements).

    Returns:
        Tuple of (is_valid, error_message).
    """
    if len(segments) not in (4, 5):
        return False, _MARKET_TOPIC_FMT
    exchange = segments[1]
    instrument = segments[2]
    data_type = segments[3]
    timeframe = segments[4] if len(segments) == 5 else None
    valid_inst, err_inst = _validate_instrument(instrument)
    if not valid_inst:
        return False, err_inst
    valid_exch, err_exch = _validate_market_source(exchange)
    if not valid_exch:
        return False, err_exch
    return _validate_market_data_type(data_type, len(segments), timeframe, exchange, instrument)


def _validate_paper_market_topic(segments: list[str]) -> tuple[bool, str]:
    """Validate a paper market topic.

    Expected format segments: [market, paper, source_exchange, instrument, data_type, ?timeframe]

    Args:
        segments: Split topic segments (5 or 6 elements).

    Returns:
        Tuple of (is_valid, error_message).
    """
    if len(segments) not in (5, 6):
        return False, _MARKET_TOPIC_FMT
    source_exchange = segments[2]
    instrument = segments[3]
    data_type = segments[4]
    timeframe = segments[5] if len(segments) == 6 else None
    valid_inst, err_inst = _validate_instrument(instrument)
    if not valid_inst:
        return False, err_inst
    valid_exch, err_exch = _validate_replay_source(source_exchange)
    if not valid_exch:
        return False, err_exch
    return _validate_market_data_type(
        data_type,
        len(segments),
        timeframe,
        source_exchange,
        instrument,
    )


def _validate_market_topic(topic: str) -> tuple[bool, str]:
    """Validate market data topic structure.

    Expected format: market.{exchange}.{instrument}.{type}[.{timeframe}]
    Paper format: market.paper.{source_exchange}.{instrument}.{type}[.{timeframe}]

    Args:
        topic: Topic string starting with "market.".

    Returns:
        Tuple of (is_valid, error_message).
    """
    segments = topic.split(".")
    if topic.endswith(".") or len(segments) not in (4, 5, 6):
        return False, _MARKET_TOPIC_FMT
    category = segments[0]
    if category != "market":
        return False, f"Expected 'market' category, got '{category}'"
    if segments[1] != "paper":
        return _validate_standard_market_topic(segments)
    return _validate_paper_market_topic(segments)


def _validate_orders_topic_base(
    topic: str,
    expected_subcategory: str,
    valid_suffixes: set[str],
    suffix_label: str,
) -> tuple[bool, str]:
    """Validate an orders topic (commands or events).

    Args:
        topic: Topic string to validate.
        expected_subcategory: Expected second segment ("commands" or "events").
        valid_suffixes: Set of valid 5th-segment values.
        suffix_label: Human-readable label for the suffix (e.g., "command" or "event").

    Returns:
        Tuple of (is_valid, error_message).
    """
    fmt_msg = (
        f"Orders {suffix_label} topic must have 5 segments: "
        f"orders.{expected_subcategory}.{{exchange}}.{{instrument}}.{{{suffix_label}}}"
    )
    segments = topic.split(".")
    if topic.endswith(".") or len(segments) != 5:
        return False, fmt_msg
    category, subcategory, exchange, instrument, suffix = segments
    if category != "orders" or subcategory != expected_subcategory:
        return (
            False,
            f"Expected 'orders.{expected_subcategory}' prefix, got '{category}.{subcategory}'",
        )
    valid_exch, err_exch = _validate_exchange(exchange)
    if not valid_exch:
        return False, err_exch
    valid_inst, err_inst = _validate_instrument(instrument)
    if not valid_inst:
        return False, err_inst
    if suffix not in valid_suffixes:
        return (
            False,
            f"Invalid order {suffix_label} '{suffix}'. Must be: {', '.join(sorted(valid_suffixes))}",
        )
    return True, ""


_ORDER_COMMANDS: set[str] = {
    OrderCommandEnum.SUBMIT,
    OrderCommandEnum.CANCEL,
    OrderCommandEnum.REPLACE,
}
_ORDER_EVENTS: set[str] = {
    "submitted",
    "accepted",
    "rejected",
    "executed",
    "cancelled",
    "expired",
    "replaced",
}


def _validate_orders_commands_topic(topic: str) -> tuple[bool, str]:
    """Validate order command topic structure.

    Expected format: orders.commands.{exchange}.{instrument}.{command}
    where command is 'submit', 'cancel', or 'replace'.

    Args:
        topic: Topic string starting with "orders.commands.".

    Returns:
        Tuple of (is_valid, error_message).
    """
    return _validate_orders_topic_base(topic, "commands", _ORDER_COMMANDS, "command")


def _validate_orders_events_topic(topic: str) -> tuple[bool, str]:
    """Validate order event topic structure.

    Expected format: orders.events.{exchange}.{instrument}.{event}
    where event is 'submitted', 'accepted', 'rejected', 'executed', 'cancelled', etc.

    Args:
        topic: Topic string starting with "orders.events.".

    Returns:
        Tuple of (is_valid, error_message).
    """
    return _validate_orders_topic_base(topic, "events", _ORDER_EVENTS, "event")


_SIGNAL_TOPIC_FORMAT_MSG = (
    "Signal topic must have 4 segments: signals.{exchange}.{instrument}.live (LIVE) "
    "or signals.paper.{instrument}.{strategy_id} (PAPER)"
)


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
    if topic.endswith(".") or len(segments) != 4:
        return False, _SIGNAL_TOPIC_FORMAT_MSG
    if segments[0] != "signals":
        return False, f"Expected 'signals' category, got '{segments[0]}'"
    exchange, instrument, type_or_strategy = segments[1], segments[2], segments[3]
    valid, msg = _validate_signal_exchange(exchange)
    if not valid:
        return False, msg
    valid, msg = _validate_instrument(instrument)
    if not valid:
        return False, msg
    if exchange != "paper" and type_or_strategy != "live":
        return (
            False,
            f"LIVE signal topics must use '.live' as 4th segment, got '.{type_or_strategy}'",
        )
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


def _validate_feed_heartbeat(segments: list[str]) -> tuple[bool, str]:
    """Validate feed heartbeat topic structure.

    Expected formats:
        system.heartbeats.feed.{exchange} — live feed (4 segments)
        system.heartbeats.feed.paper.{source} — paper replay (5 segments)

    Live feed exchange must be in MarketSubscribeExchange (kraken/zonda/walutomat).
    Paper source must be in MarketDataExchange (kraken/zonda/walutomat/polygon).

    Args:
        segments: Split topic segments starting with system.heartbeats.feed.

    Returns:
        Tuple of (is_valid, error_message).
    """
    if len(segments) == 4:
        if segments[3] == "paper":
            return False, "system.heartbeats.feed.paper requires source_exchange (5 segments)"
        return _validate_market_source(segments[3])
    if len(segments) == 5 and segments[3] == "paper":
        if segments[4] == "paper":
            return False, "system.heartbeats.feed.paper.{source}: source cannot be 'paper'"
        return _validate_replay_source(segments[4])
    return (
        False,
        "system.heartbeats.feed requires exchange (4 seg) or feed.paper.{source} (5 seg)",
    )


_HEX_CHARSET = frozenset("0123456789abcdef")
_WALLET_SHORT_LENGTH = 12


def _is_valid_wallet_short(segment: str) -> bool:
    """Return True when ``segment`` matches the wallet_short shape.

    A wallet_short segment is exactly 12 lowercase hex characters — the
    same derivation used by ``TradingEngineService._shard_key``,
    ``ProcessLauncherService.spawn_per_wallet_executors``, and
    ``TraderCoordinator._build_engine_key``. This helper keeps the
    heartbeat topic validator in sync with those producers so a
    typo'd segment is rejected as early as the first publish.
    """
    if len(segment) != _WALLET_SHORT_LENGTH:
        return False
    return all(c in _HEX_CHARSET for c in segment)


def _validate_heartbeat_topic(segments: list[str]) -> tuple[bool, str]:
    """Validate heartbeat topic structure.

    Expected layouts:

    - ``system.heartbeats`` (2 seg) — global heartbeat
    - ``system.heartbeats.strategy.{name}`` (4 seg)
    - ``system.heartbeats.executor.{exchange}`` (4 seg) — single-wallet
      template
    - ``system.heartbeats.executor.{exchange}.{wallet_short}`` (5 seg) —
      per-wallet executor instance. The 5th segment must be
      exactly 12 lowercase hex characters.
    - ``system.heartbeats.feed.{exchange}`` or
      ``system.heartbeats.feed.paper.{source}`` — delegated to
      :func:`_validate_feed_heartbeat`.

    Args:
        segments: Split topic segments (first two are 'system.heartbeats').

    Returns:
        Tuple of (is_valid, error_message).
    """
    if len(segments) == 2:
        return True, ""
    component_type = segments[2]
    if component_type == "strategy":
        if len(segments) == 4:
            return True, ""
        return (
            False,
            "system.heartbeats.strategy requires exactly 4 segments: "
            "system.heartbeats.strategy.{name}",
        )
    if component_type == "executor":
        if len(segments) == 4:
            return True, ""
        if len(segments) == 5:
            wallet_short = segments[4]
            if _is_valid_wallet_short(wallet_short):
                return True, ""
            return (
                False,
                "system.heartbeats.executor.{exchange}.{wallet_short}: "
                "wallet_short must be 12 lowercase hex characters",
            )
        return (
            False,
            "system.heartbeats.executor requires 4 segments (template) "
            "or 5 segments (per-wallet instance with wallet_short)",
        )
    if component_type == "feed":
        return _validate_feed_heartbeat(segments)
    return False, f"Invalid heartbeat component type '{component_type}'"


def _validate_system_topic(topic: str) -> tuple[bool, str]:
    """Validate system topic structure.

    Expected formats:
    - system.heartbeats[.{component_type}[.{component_name}]]
    - system.symbol_aliases
    - system.settings

    Args:
        topic: Topic string starting with "system.".

    Returns:
        Tuple of (is_valid, error_message).
    """
    segments = topic.split(".")
    if topic.endswith("."):
        return False, "System topic cannot end with '.'; check segments"
    if len(segments) < 2:
        return False, "System topic must have at least 2 segments: system.{type}"
    category, system_type = segments[0], segments[1]
    if category != "system":
        return False, f"Expected 'system' category, got '{category}'"
    if system_type == "heartbeats":
        return _validate_heartbeat_topic(segments)
    if system_type in {"symbol_aliases", "settings"}:
        if len(segments) != 2:
            return False, f"system.{system_type} must have exactly 2 segments"
        return True, ""
    return (
        False,
        f"Invalid system type '{system_type}'. Must be: heartbeats, settings, symbol_aliases",
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
    category, _resource = segments
    if category != "admin":
        return False, f"Expected 'admin' category, got '{category}'"
    return True, ""


_ACCRUAL_TYPES: frozenset[str] = frozenset({"funding", "rollover", "borrow"})

_ACCRUAL_TOPIC_FORMAT_MSG = (
    "Accrual topics must have 4 segments: accruals.{exchange}.{instrument}.{accrual_type}"
)


def _validate_accruals_topic(topic: str) -> tuple[bool, str]:
    """Validate accrual ledger topic structure.

    Expected format: accruals.{exchange}.{instrument}.{accrual_type}
    where accrual_type is one of: funding, rollover, borrow.

    Args:
        topic: Topic string starting with "accruals.".

    Returns:
        Tuple of (is_valid, error_message).
    """
    segments = topic.split(".")
    if topic.endswith(".") or len(segments) != 4:
        return False, _ACCRUAL_TOPIC_FORMAT_MSG
    if segments[0] != "accruals":
        return False, f"Expected 'accruals' category, got '{segments[0]}'"
    exchange, instrument, accrual_type = segments[1], segments[2], segments[3]
    valid, msg = _validate_exchange(exchange)
    if not valid:
        return False, msg
    valid, msg = _validate_instrument(instrument)
    if not valid:
        return False, msg
    if accrual_type not in _ACCRUAL_TYPES:
        return (
            False,
            f"Invalid accrual_type '{accrual_type}'. Must be one of: "
            f"{', '.join(sorted(_ACCRUAL_TYPES))}",
        )
    return True, ""


_ExchangeValidatorType = Callable[[str], tuple[bool, str]]


def _validate_exchange_instrument_segments(
    segments: list[str],
    exchange_offset: int,
    exchange_validator: _ExchangeValidatorType,
) -> tuple[bool, str]:
    """Validate optional exchange and instrument segments in a prefix.

    Args:
        segments: Prefix segments (without trailing dot).
        exchange_offset: Index of the exchange segment.
        exchange_validator: Callable that validates an exchange name.

    Returns:
        Tuple of (is_valid, error_message).
    """
    validator = exchange_validator
    if len(segments) > exchange_offset:
        valid, err = validator(segments[exchange_offset])
        if not valid:
            return False, err
    instrument_offset = exchange_offset + 1
    if len(segments) > instrument_offset:
        valid, err = _validate_instrument(segments[instrument_offset])
        if not valid:
            return False, err
    return True, ""


def _validate_orders_prefix(segments: list[str]) -> tuple[bool, str]:
    """Validate orders prefix pattern segments.

    Args:
        segments: Prefix segments (without trailing dot).

    Returns:
        Tuple of (is_valid, error_message).
    """
    if len(segments) < 2:
        return (
            False,
            "Orders prefix requires subcategory: use 'orders.commands.' or 'orders.events.'",
        )
    subcategory = segments[1]
    if subcategory not in {"commands", "events"}:
        return (
            False,
            f"Invalid orders subcategory '{subcategory}'. Must be: commands, events",
        )
    return _validate_exchange_instrument_segments(segments, 2, _validate_exchange)


def _validate_market_prefix(segments: list[str]) -> tuple[bool, str]:
    """Validate market prefix pattern segments.

    Handles both standard (market.exchange.instrument.) and paper
    (market.paper.source.instrument.) prefix patterns.

    Args:
        segments: Prefix segments (without trailing dot).

    Returns:
        Tuple of (is_valid, error_message).
    """
    if len(segments) == 1:
        return True, ""
    if segments[1] != "paper":
        return _validate_exchange_instrument_segments(segments, 1, _validate_market_source)
    if len(segments) == 2:
        return True, ""
    source_exchange = segments[2]
    valid_source, source_err = _validate_replay_source(source_exchange)
    if not valid_source:
        return False, source_err
    if len(segments) >= 4:
        valid_inst, inst_err = _validate_instrument(segments[3])
        if not valid_inst:
            return False, inst_err
    return True, ""


def _validate_prefix_pattern(pattern: str) -> tuple[bool, str]:
    """Validate a subscription prefix pattern.

    Prefix patterns end with '.' and match all topics with that prefix.
    Validates that the prefix follows valid topic hierarchy.

    Note: 'orders.' alone is NOT a valid subscription pattern.
    Use 'orders.commands.' or 'orders.events.' for order-related subscriptions.

    Args:
        pattern: Subscription pattern ending with '.'.

    Returns:
        Tuple of (is_valid, error_message).
    """
    if not pattern.endswith("."):
        return False, "Prefix must end with dot"
    segments = pattern[:-1].split(".")
    if any(not segment for segment in segments):
        return False, "Prefix segments cannot be empty"
    category = segments[0]
    valid_categories = {"market", "orders", "signals", "strategy", "system", "admin", "accruals"}
    if category not in valid_categories:
        return False, f"Unknown topic category: {category}"
    if category == "orders":
        return _validate_orders_prefix(segments)
    if category == "market":
        return _validate_market_prefix(segments)
    if category == "signals" and len(segments) >= 2:
        return _validate_exchange_instrument_segments(segments, 1, _validate_signal_exchange)
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
    """Validate exchange for order-capable domain (paper + live venues).

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


def _validate_market_source(exchange: str) -> tuple[bool, str]:
    """Validate exchange for live market data topics (kraken/zonda/walutomat).

    Args:
        exchange: Exchange name from market topic segment.

    Returns:
        Tuple of (is_valid, error_message).
    """
    if not exchange:
        return False, "Exchange cannot be empty"
    valid = set(get_market_subscribe_exchanges())
    if exchange not in valid:
        return (
            False,
            f"Unknown market feed exchange '{exchange}'. "
            f"Must be one of: {', '.join(sorted(valid))}",
        )
    return True, ""


def _validate_replay_source(exchange: str) -> tuple[bool, str]:
    """Validate source exchange for paper replay topics.

    Valid sources: kraken, zonda, walutomat, polygon.
    Paper is excluded — it is the consumer, not a data source.

    Args:
        exchange: Source exchange name from paper market topic.

    Returns:
        Tuple of (is_valid, error_message).
    """
    if not exchange:
        return False, "Source exchange cannot be empty"
    valid = set(get_market_data_exchanges())
    if exchange not in valid:
        return (
            False,
            f"Unknown replay source '{exchange}'. Must be one of: {', '.join(sorted(valid))}",
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
