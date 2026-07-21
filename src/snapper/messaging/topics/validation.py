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
import typing
from collections.abc import Callable

from snapper.core.ids import is_uuid7
from snapper.core.types import MarketDataTypeEnum
from snapper.core.types import OrderCommandEnum
from snapper.infrastructure.symbols.functions import get_available_exchanges
from snapper.infrastructure.symbols.functions import get_available_symbols_set
from snapper.infrastructure.symbols.functions import get_market_data_exchanges
from snapper.infrastructure.symbols.functions import get_market_subscribe_exchanges
from snapper.messaging.schemas.data import AlertType
from snapper.messaging.schemas.data import BacktestProgressEvent

__all__ = [
    "validate_topic",
    "validate_subscription_pattern",
    "TopicValidationError",
    "BACKTEST_EVENTS",
    "_validate_backtest_prefix",
    "_validate_backtest_topic",
    "_validate_alerts_topic",
    "_validate_portfolio_accounts_topic",
]


BACKTEST_EVENTS: frozenset[str] = frozenset(typing.get_args(BacktestProgressEvent))
"""Canonical backtest event names derived from ``BacktestProgressEvent``.

Single source of truth: the Literal type in
``snapper.messaging.schemas.data.BacktestProgressEvent`` is mirrored
here via ``typing.get_args`` so the topic validator, the emitter
payload schema, and every test read from the same tuple.
"""
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
        ("backtest.", _validate_backtest_topic),
        ("alerts.", _validate_alerts_topic),
        ("portfolio.accounts.", _validate_portfolio_accounts_topic),
        ("plans.decisions.", _validate_plans_decisions_topic),
        ("ai_reviews.", _validate_ai_reviews_topic),
        ("bus.", _validate_bus_topic),
        ("processes.events.summary.", _validate_processes_summary_topic),
        ("processes.events.configured.", _validate_processes_configured_topic),
        ("processes.events.runs.", _validate_processes_runs_topic),
        ("processes.events.command_ack.", _validate_processes_command_ack_topic),
        ("processes.commands.", _validate_processes_commands_topic),
        ("strategies.events.list.", _validate_strategies_list_topic),
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
        return True, ""
    if segment_count > 0 and timeframe is not None:
        return (
            False,
            f"Only candles topics support timeframe. Remove '.{timeframe}' from {data_type} topic",
        )
    return True, ""


def _validate_standard_market_topic(
    exchange: str, instrument: str, data_type: str, timeframe: str | None
) -> tuple[bool, str]:
    """Validate a non-paper market topic from already-parsed components.

    Args:
        exchange: Exchange segment.
        instrument: Instrument segment (may contain dots — e.g. BRK.B).
        data_type: Data-type segment (ticks/trades/candles).
        timeframe: Optional candle timeframe segment.

    Returns:
        Tuple of (is_valid, error_message).
    """
    valid_inst, err_inst = _validate_instrument(instrument)
    if not valid_inst:
        return False, err_inst
    valid_exch, err_exch = _validate_market_source(exchange)
    if not valid_exch:
        return False, err_exch
    segment_count = 5 if timeframe else 4
    return _validate_market_data_type(data_type, segment_count, timeframe, exchange, instrument)


def _validate_paper_market_topic(
    source_exchange: str, instrument: str, data_type: str, timeframe: str | None
) -> tuple[bool, str]:
    """Validate a paper market topic from already-parsed components.

    Args:
        source_exchange: Source-exchange segment carried after ``paper``.
        instrument: Instrument segment (may contain dots).
        data_type: Data-type segment (ticks/trades/candles).
        timeframe: Optional candle timeframe segment.

    Returns:
        Tuple of (is_valid, error_message).
    """
    valid_inst, err_inst = _validate_instrument(instrument)
    if not valid_inst:
        return False, err_inst
    valid_exch, err_exch = _validate_replay_source(source_exchange)
    if not valid_exch:
        return False, err_exch
    segment_count = 6 if timeframe else 5
    return _validate_market_data_type(
        data_type,
        segment_count,
        timeframe,
        source_exchange,
        instrument,
    )


def _validate_market_topic(topic: str) -> tuple[bool, str]:
    """Validate market data topic structure with right-anchored parsing.

    Expected format: ``market.{exchange}.{instrument}.{type}[.{timeframe}]``
    Paper format: ``market.paper.{source_exchange}.{instrument}.{type}[.{timeframe}]``

    The instrument segment may contain dots (e.g. ``BRK.B`` for
    Berkshire Hathaway Class B). The validator anchors on the
    well-known prefix (``market`` / exchange / optional paper source)
    and suffix (data_type / optional candles timeframe) and treats
    every middle segment as part of the instrument. Without this, a
    naive left-anchored split would mis-parse ``market.kraken.BRK.B.ticks``
    as ``instrument=BRK`` + ``data_type=B`` and reject the topic even
    though the underlying symbol is valid.

    Args:
        topic: Topic string starting with ``"market."``.

    Returns:
        Tuple of (is_valid, error_message).
    """
    if topic.endswith("."):
        return False, _MARKET_TOPIC_FMT
    segments = topic.split(".")
    if len(segments) < 4 or segments[0] != "market":
        return False, _MARKET_TOPIC_FMT
    candidate_timeframe = segments[-1]
    timeframe: str | None = None
    if _is_valid_timeframe(candidate_timeframe):
        timeframe = candidate_timeframe
        data_type = segments[-2]
        body = segments[1:-2]
    else:
        data_type = segments[-1]
        body = segments[1:-1]
    if body[0] == "paper":
        if len(body) < 3:
            return False, _MARKET_TOPIC_FMT
        return _validate_paper_market_topic(
            source_exchange=body[1],
            instrument=".".join(body[2:]),
            data_type=data_type,
            timeframe=timeframe,
        )
    if len(body) < 2:
        return False, _MARKET_TOPIC_FMT
    return _validate_standard_market_topic(
        exchange=body[0],
        instrument=".".join(body[1:]),
        data_type=data_type,
        timeframe=timeframe,
    )


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
    "unknown",
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

    Live feed exchange must be in MarketSubscribeExchange (kraken/walutomat).
    Paper source must be in MarketDataExchange (kraken/walutomat/polygon).

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


def _validate_strategy_heartbeat(segments: list[str]) -> tuple[bool, str]:
    """Validate strategy heartbeat topic structure."""
    if len(segments) == 4:
        return True, ""
    return (
        False,
        "system.heartbeats.strategy requires exactly 4 segments: system.heartbeats.strategy.{name}",
    )


def _validate_executor_heartbeat(segments: list[str]) -> tuple[bool, str]:
    """Validate executor heartbeat topic structure."""
    if len(segments) == 4:
        return True, ""
    if len(segments) != 5:
        return (
            False,
            "system.heartbeats.executor requires 4 segments (template) "
            "or 5 segments (per-wallet instance with wallet_short)",
        )
    wallet_short = segments[4]
    if _is_valid_wallet_short(wallet_short):
        return True, ""
    return (
        False,
        "system.heartbeats.executor.{exchange}.{wallet_short}: "
        "wallet_short must be 12 lowercase hex characters",
    )


def _validate_host_heartbeat(segments: list[str]) -> tuple[bool, str]:
    """Validate host heartbeat topic structure."""
    if len(segments) != 4:
        return (
            False,
            "system.heartbeats.host requires exactly 4 segments: system.heartbeats.host.disk",
        )
    if segments[3] != "disk":
        return False, "system.heartbeats.host supports only system.heartbeats.host.disk"
    return True, ""


def _validate_marketdata_heartbeat(segments: list[str]) -> tuple[bool, str]:
    """Validate market-data watchdog heartbeat topic structure.

    ``system.heartbeats.marketdata.{exchange}`` carries the API-side
    market-data watchdog's synthetic exchange-silence heartbeats. It is
    a distinct component from ``feed`` on purpose: the live feed
    publishes HEALTHY frames on ``system.heartbeats.feed.{exchange}``
    every second, so a watchdog interleaving WARNING frames there could
    never satisfy the critical-system-error rule's 3-consecutive gate.

    Args:
        segments: Split topic segments starting with
            ``system.heartbeats.marketdata``.

    Returns:
        Tuple of (is_valid, error_message).
    """
    if len(segments) != 4:
        return (
            False,
            "system.heartbeats.marketdata requires exactly 4 segments: "
            "system.heartbeats.marketdata.{exchange}",
        )
    return _validate_market_source(segments[3])


def _validate_ai_delegate_heartbeat(segments: list[str]) -> tuple[bool, str]:
    """Validate the global AI-delegate watchdog heartbeat topic.

    Args:
        segments: Split topic segments starting with
            ``system.heartbeats.ai_delegate``.

    Returns:
        Tuple of validation success and an explanatory error string.
    """
    if len(segments) != 4:
        return (
            False,
            "system.heartbeats.ai_delegate requires exactly 4 segments: "
            "system.heartbeats.ai_delegate.global",
        )
    if segments[3] != "global":
        return False, "system.heartbeats.ai_delegate supports only the global scope"
    return True, ""


_HEARTBEAT_COMPONENT_VALIDATORS: dict[str, Callable[[list[str]], tuple[bool, str]]] = {
    "strategy": _validate_strategy_heartbeat,
    "executor": _validate_executor_heartbeat,
    "host": _validate_host_heartbeat,
    "feed": _validate_feed_heartbeat,
    "marketdata": _validate_marketdata_heartbeat,
    "ai_delegate": _validate_ai_delegate_heartbeat,
}


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
    - ``system.heartbeats.host.disk`` (4 seg) — API host disk-pressure
      heartbeat emitted by the system metrics snapshotter.
    - ``system.heartbeats.feed.{exchange}`` or
      ``system.heartbeats.feed.paper.{source}`` — delegated to
      :func:`_validate_feed_heartbeat`.
    - ``system.heartbeats.marketdata.{exchange}`` (4 seg) — synthetic
      exchange-silence heartbeats from the API market-data watchdog.
    - ``system.heartbeats.ai_delegate.global`` (4 seg) — synthetic
      liveness and response heartbeats from the API AI-delegate watchdog.

    Args:
        segments: Split topic segments (first two are 'system.heartbeats').

    Returns:
        Tuple of (is_valid, error_message).
    """
    if len(segments) == 2:
        return True, ""
    component_type = segments[2]
    validator = _HEARTBEAT_COMPONENT_VALIDATORS.get(component_type)
    if validator is None:
        return False, f"Invalid heartbeat component type '{component_type}'"
    return validator(segments)


def _validate_system_topic(topic: str) -> tuple[bool, str]:
    """Validate system topic structure.

    Expected formats:
    - system.heartbeats[.{component_type}[.{component_name}]]
    - system.egress.snapshot
    - system.egress.transfer
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
    if system_type == "egress":
        if len(segments) == 3 and segments[2] in {"snapshot", "transfer"}:
            return True, ""
        return False, "system.egress must be system.egress.snapshot or system.egress.transfer"
    if system_type in {"symbol_aliases", "settings"}:
        if len(segments) != 2:
            return False, f"system.{system_type} must have exactly 2 segments"
        return True, ""
    return (
        False,
        "Invalid system type "
        f"'{system_type}'. Must be: egress, heartbeats, settings, symbol_aliases",
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


_ALERT_TYPES: frozenset[str] = frozenset(typing.get_args(AlertType))
"""Canonical alert_type names — derived from ``AlertType`` in
``snapper.messaging.schemas.data`` via ``typing.get_args`` so the
validator and the wire schema share a single source of truth
(eliminates drift risk). Any addition to
``AlertType`` flows here automatically; the matching entries in
``DeviceAlertPrefBody.alert_type`` and ``UserAlertDefaultBody.alert_type``
(``src/snapper/api/schemas/devices.py``) are still separate Literals —
the parity is asserted in the test suite rather than at import time."""


_ALERT_TOPIC_FORMAT_MSG = "Alert topics must have 3 segments: alerts.{user_public_id}.{alert_type}"


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


def _validate_alerts_topic(topic: str) -> tuple[bool, str]:
    """Validate an iOS push-notification alert topic.

    Expected shape: ``alerts.{user_public_id}.{alert_type}`` where
    ``user_public_id`` is a UUID7 string and ``alert_type`` is one of
    the enumerated alert types (mirrored in ``DeviceAlertPrefBody``
    and the ``AlertEventData`` dataclass).

    Args:
        topic: Topic string starting with ``alerts.``.

    Returns:
        Tuple of (is_valid, error_message). Empty error_message when
        valid; dense diagnostic string when not.
    """
    segments = topic.split(".")
    if topic.endswith(".") or len(segments) != 3:
        return False, _ALERT_TOPIC_FORMAT_MSG
    if segments[0] != "alerts":
        return False, f"Expected 'alerts' category, got '{segments[0]}'"
    _, user_public_id, alert_type = segments
    if not is_uuid7(user_public_id):
        return False, f"alerts.* segment 2 must be UUID7, got '{user_public_id}'"
    if alert_type not in _ALERT_TYPES:
        return (
            False,
            f"Invalid alert_type '{alert_type}'. Must be one of: {', '.join(sorted(_ALERT_TYPES))}",
        )
    return True, ""


def _validate_portfolio_accounts_topic(topic: str) -> tuple[bool, str]:
    """Validate a wallet-scoped account-state invalidation topic.

    Expected shape: ``portfolio.accounts.{wallet_public_id}``, where
    ``wallet_public_id`` is a canonical UUID7. The payload repeats the wallet
    id so the WebSocket bridge can enforce per-frame wallet scope.

    Args:
        topic: Topic string starting with ``portfolio.accounts.``.

    Returns:
        Tuple of validity and a diagnostic message.
    """
    segments = topic.split(".")
    if topic.endswith(".") or len(segments) != 3:
        return (
            False,
            "portfolio.accounts.* requires 3 segments (portfolio.accounts.<wallet_public_id>)",
        )
    if segments[0] != "portfolio" or segments[1] != "accounts":
        return False, f"Expected 'portfolio.accounts' prefix, got '{segments[0]}.{segments[1]}'"
    wallet_public_id = segments[2]
    if not is_uuid7(wallet_public_id):
        return False, f"portfolio.accounts.* segment 3 must be UUID7, got '{wallet_public_id}'"
    return True, ""


_AI_REVIEW_FRAME_SUFFIXES: frozenset[str] = frozenset({"request", "decision_ack", "caps_violation"})
"""Fixed external WS frame suffixes for the
``ai_reviews.{user}.{strategy}.*`` topic family. The bridge per-frame
scope filter routes by user/strategy + the JS dispatcher's
``switch (frame.type)`` covers exactly these three branches; any new
suffix would need a paired JS handler so we fail closed on unknown
ones."""


_AI_REVIEW_TOPIC_FORMAT_MSG = (
    "ai_reviews.* requires 4 segments "
    "(ai_reviews.<user_public_id>.<strategy_public_id>.<request|decision_ack|caps_violation>)"
)


def _validate_ai_reviews_topic(topic: str) -> tuple[bool, str]:
    """Validate an external WS ``ai_reviews.*`` fanout topic.

    Outbound topic family for delegate consultations. Shape is
    ``ai_reviews.{user_public_id}.{strategy_public_id}.{suffix}`` where
    user / strategy ids are UUID7 and the suffix is one of the three
    frame discriminators (``request`` / ``decision_ack`` /
    ``caps_violation``). The matching ``ai_reviews.`` entry already
    exists in :data:`TOPIC_REGISTRY`; this validator is the publish-side
    counterpart so the shared ZMQ PUB socket accepts the topic before
    handing it to the bridge per-frame scope filter.

    Args:
        topic: Topic string starting with ``ai_reviews.``.

    Returns:
        Tuple of (is_valid, error_message). Empty error_message when
        valid; dense diagnostic string when not.
    """
    segments = topic.split(".")
    if topic.endswith(".") or len(segments) != 4:
        return False, f"{_AI_REVIEW_TOPIC_FORMAT_MSG}, got '{topic}'"
    if segments[0] != "ai_reviews":
        return False, f"Expected 'ai_reviews' category, got '{segments[0]}'"
    user_public_id = segments[1]
    strategy_public_id = segments[2]
    suffix = segments[3]
    if not is_uuid7(user_public_id):
        return False, f"ai_reviews.* segment 2 must be UUID7, got '{user_public_id}'"
    if not is_uuid7(strategy_public_id):
        return False, f"ai_reviews.* segment 3 must be UUID7, got '{strategy_public_id}'"
    if suffix not in _AI_REVIEW_FRAME_SUFFIXES:
        return False, (
            f"ai_reviews.* segment 4 must be one of "
            f"{', '.join(sorted(_AI_REVIEW_FRAME_SUFFIXES))}, got '{suffix}'"
        )
    return True, ""


_BUS_TOPIC_NAME_PATTERN = re.compile(r"^[a-z][a-z0-9_]*$")
"""Snake-case bus topic name shape — lowercase, digits and underscores
allowed after the leading letter. Mirrors the bus topic naming
convention so a typo (``bus.Delegate-Offline``) fails fast at the
publisher boundary."""


def _validate_bus_topic(topic: str) -> tuple[bool, str]:
    """Validate an internal ``bus.*`` cross-service event topic.

    Internal-only event bus used by services that fan out across
    the same ZMQ broker (e.g.
    ``bus.delegate_offline``, ``bus.ai_review_request``,
    ``bus.caps_violation_after_ai_approve``). Shape is ``bus.{name}``
    with a snake-case suffix; the discriminator on the wire payload
    (``StrictDataSchema.type``) is the authoritative routing hint, so
    the validator only enforces the topic's structural shape and lets
    the schema layer reject unknown payload types.

    Args:
        topic: Topic string starting with ``bus.``.

    Returns:
        Tuple of (is_valid, error_message). Empty error_message when
        valid; dense diagnostic string when not.
    """
    segments = topic.split(".")
    if topic.endswith(".") or len(segments) != 2:
        return False, f"bus.* requires 2 segments (bus.<name>), got '{topic}'"
    if segments[0] != "bus":
        return False, f"Expected 'bus' category, got '{segments[0]}'"
    name = segments[1]
    if not _BUS_TOPIC_NAME_PATTERN.fullmatch(name):
        return False, (
            f"bus.* segment 2 must be snake_case (lowercase + digits + underscores, "
            f"leading letter), got '{name}'"
        )
    return True, ""


_PROCESSES_TOPIC_NAME_PATTERN = re.compile(r"^[A-Za-z][A-Za-z0-9_-]*$")
"""Process / strategy snapshot topic suffix shape — alphanumeric +
underscore/hyphen, leading letter. Matches the launcher's
``coordinator_instance_id`` (UUID7 or short slug) and process names
that ``ProcessLauncherService`` produces. The discriminator on the
wire payload (``type`` field on ``StrictDataSchema``) is the
authoritative routing hint so the validator only enforces structural
shape on the trailing segment."""


def _validate_processes_snapshot_topic(
    topic: str,
    *,
    prefix: str,
    suffix_label: str,
) -> tuple[bool, str]:
    """Shared validator for the three ``processes.events.*`` snapshot topics.

    Each topic is exactly 4 segments: ``processes.events.{kind}.{tail}``
    where ``kind`` is ``summary`` / ``configured`` / ``runs`` and
    ``tail`` is either the coordinator instance id (for fanout topics)
    or the process name (for the per-process runs topic). The tail
    must match :data:`_PROCESSES_TOPIC_NAME_PATTERN`.

    Args:
        topic: Topic string starting with ``prefix``.
        prefix: The 3-segment ``processes.events.{kind}.`` prefix.
        suffix_label: Human-readable name of the tail segment for
            diagnostic messages.

    Returns:
        Tuple of (is_valid, error_message).
    """
    segments = topic.split(".")
    if topic.endswith(".") or len(segments) != 4:
        return False, f"{prefix}* requires 4 segments ({prefix}<{suffix_label}>), got '{topic}'"
    if segments[0] != "processes" or segments[1] != "events":
        return (
            False,
            f"Expected '{prefix.rstrip('.')}' prefix, got '{segments[0]}.{segments[1]}'",
        )
    tail = segments[3]
    if not _PROCESSES_TOPIC_NAME_PATTERN.fullmatch(tail):
        return False, (f"{prefix}* {suffix_label} must match [A-Za-z][A-Za-z0-9_-]*, got '{tail}'")
    return True, ""


def _validate_processes_summary_topic(topic: str) -> tuple[bool, str]:
    """Validate ``processes.events.summary.{instance_id}`` topics."""
    return _validate_processes_snapshot_topic(
        topic, prefix="processes.events.summary.", suffix_label="instance_id"
    )


def _validate_processes_configured_topic(topic: str) -> tuple[bool, str]:
    """Validate ``processes.events.configured.{instance_id}`` topics."""
    return _validate_processes_snapshot_topic(
        topic, prefix="processes.events.configured.", suffix_label="instance_id"
    )


def _validate_processes_runs_topic(topic: str) -> tuple[bool, str]:
    """Validate ``processes.events.runs.{process_name}`` topics."""
    return _validate_processes_snapshot_topic(
        topic, prefix="processes.events.runs.", suffix_label="process_name"
    )


def _validate_processes_commands_topic(topic: str) -> tuple[bool, str]:
    """Validate a ``processes.commands.{coordinator}`` control-plane nudge topic.

    Published by the API coordinator after a desired-state PATCH so the owning
    coordinator reconciles now instead of waiting for its periodic tick. Three
    segments; the trailing coordinator slug must match
    :data:`_PROCESSES_TOPIC_NAME_PATTERN`. This topic is deliberately NOT
    exposed through WS RBAC — the payload HMAC signature, not a topic ACL, is
    the trust boundary (the receiver also enforces slug and freshness).

    Args:
        topic: Topic string starting with ``processes.commands.``.

    Returns:
        Tuple of (is_valid, error_message).
    """
    segments = topic.split(".")
    if topic.endswith(".") or len(segments) != 3:
        return False, (
            f"processes.commands.* requires 3 segments (processes.commands.<coordinator>),"
            f" got '{topic}'"
        )
    if segments[0] != "processes" or segments[1] != "commands":
        return False, (f"Expected 'processes.commands' prefix, got '{segments[0]}.{segments[1]}'")
    tail = segments[2]
    if not _PROCESSES_TOPIC_NAME_PATTERN.fullmatch(tail):
        return False, (
            f"processes.commands.* coordinator must match [A-Za-z][A-Za-z0-9_-]*, got '{tail}'"
        )
    return True, ""


def _validate_processes_command_ack_topic(topic: str) -> tuple[bool, str]:
    """Validate a ``processes.events.command_ack.{coordinator}`` ack topic.

    Published by the owning coordinator after handling a command nudge; the
    API's blocking PATCH matches the ack (verifying its signature first) to
    resolve the pending request. Four segments; the trailing coordinator slug
    must match :data:`_PROCESSES_TOPIC_NAME_PATTERN`.

    Args:
        topic: Topic string starting with ``processes.events.command_ack.``.

    Returns:
        Tuple of (is_valid, error_message).
    """
    return _validate_processes_snapshot_topic(
        topic, prefix="processes.events.command_ack.", suffix_label="coordinator"
    )


def _validate_strategies_list_topic(topic: str) -> tuple[bool, str]:
    """Validate ``strategies.events.list.{instance_id}`` topics.

    Strategy fanout topics share the launcher coordinator id with the
    processes.* topics so the regex constraint matches.
    """
    segments = topic.split(".")
    if topic.endswith(".") or len(segments) != 4:
        return False, (
            f"strategies.events.list.* requires 4 segments "
            f"(strategies.events.list.<instance_id>), got '{topic}'"
        )
    if segments[0] != "strategies" or segments[1] != "events" or segments[2] != "list":
        return False, (
            f"Expected 'strategies.events.list' prefix, "
            f"got '{segments[0]}.{segments[1]}.{segments[2]}'"
        )
    tail = segments[3]
    if not _PROCESSES_TOPIC_NAME_PATTERN.fullmatch(tail):
        return False, (
            f"strategies.events.list.* instance_id must match [A-Za-z][A-Za-z0-9_-]*, got '{tail}'"
        )
    return True, ""


def _validate_plans_decisions_topic(topic: str) -> tuple[bool, str]:
    """Validate a ``plans.decisions.{plan_public_id}`` topic.

    Published by ``PlanExecutorService`` immediately after every
    ``ExecutionPlanDecision`` row insert. Subscribed by the notify
    sidecar's stop-loss rule to turn bracket / trailing-stop fires
    into iOS push notifications. Best-effort delivery with
    fail-closed semantics.

    Args:
        topic: Topic string starting with ``plans.decisions.``.

    Returns:
        Tuple of (is_valid, error_message). Empty error_message when
        valid; dense diagnostic string when not.
    """
    segments = topic.split(".")
    if topic.endswith(".") or len(segments) != 3:
        return False, (
            f"plans.decisions.* requires 3 segments (plans.decisions.<plan_public_id>),"
            f" got '{topic}'"
        )
    if segments[0] != "plans" or segments[1] != "decisions":
        return False, f"Expected 'plans.decisions' prefix, got '{segments[0]}.{segments[1]}'"
    plan_public_id = segments[2]
    if not is_uuid7(plan_public_id):
        return False, f"plans.decisions.* segment 3 must be UUID7, got '{plan_public_id}'"
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
    valid_categories = {
        "market",
        "orders",
        "signals",
        "strategy",
        "system",
        "admin",
        "accruals",
        "backtest",
        "alerts",
        "portfolio",
        "plans",
        "ai_reviews",
        "bus",
        "processes",
        "strategies",
    }
    if category not in valid_categories:
        return False, f"Unknown topic category: {category}"
    if category == "orders":
        return _validate_orders_prefix(segments)
    if category == "market":
        return _validate_market_prefix(segments)
    if category == "signals" and len(segments) >= 2:
        return _validate_exchange_instrument_segments(segments, 1, _validate_signal_exchange)
    if category == "backtest":
        return _validate_backtest_prefix_segments(segments)
    return True, ""


def _validate_instrument(instrument: str) -> tuple[bool, str]:
    """Validate instrument symbol exists in database.

    Reads the cached frozenset from
    :func:`snapper.infrastructure.symbols.functions.get_available_symbols_set`
    so the publish-time hot path (called once per tick at peak rates of
    several thousand per second) does an O(1) ``in`` check instead of
    rebuilding the 5-exchange union and doing an O(N) list scan.

    Args:
        instrument: Instrument symbol (e.g., "BTC-USD").

    Returns:
        Tuple of (is_valid, error_message).
    """
    if not instrument:
        return False, "Instrument cannot be empty"
    if instrument not in get_available_symbols_set():
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
    """Validate exchange for live market data topics (kraken/walutomat).

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

    Valid sources: kraken, walutomat, polygon.
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

    Valid timeframes: digit(s) + unit.
    Examples: 1m, 5m, 15m, 1h, 4h, 1d, 1w, 1M.

    Args:
        timeframe: Timeframe string to validate.

    Returns:
        True if timeframe matches valid pattern.
    """
    pattern = r"^\d+[mhdwM]$"
    return bool(re.match(pattern, timeframe))


_BACKTEST_TOPIC_FORMAT_MSG = (
    "Backtest topic must have 4 segments: "
    "backtest.{wallet_public_id}.{run_public_id}.{event} — "
    f"event must be one of: {', '.join(sorted(BACKTEST_EVENTS))}"
)


def _validate_backtest_topic(topic: str) -> tuple[bool, str]:
    """Validate a fully-qualified backtest progress topic.

    Expected shape: ``backtest.{wallet_public_id}.{run_public_id}.{event}``
    with both UUID segments in canonical UUID7 format and the event
    name drawn from the ``BacktestProgressEvent`` Literal (the
    canonical single source of truth at
    ``snapper.messaging.schemas.data``).

    Args:
        topic: Topic string starting with ``backtest.``.

    Returns:
        Tuple of (is_valid, error_message).
    """
    segments = topic.split(".")
    if topic.endswith(".") or len(segments) != 4:
        return False, _BACKTEST_TOPIC_FORMAT_MSG
    if segments[0] != "backtest":
        return False, f"Expected 'backtest' category, got '{segments[0]}'"
    wallet_public_id, run_public_id, event = segments[1], segments[2], segments[3]
    if not is_uuid7(wallet_public_id):
        return (
            False,
            f"Backtest wallet segment '{wallet_public_id}' is not a valid UUID7",
        )
    if not is_uuid7(run_public_id):
        return (
            False,
            f"Backtest run segment '{run_public_id}' is not a valid UUID7",
        )
    if event not in BACKTEST_EVENTS:
        return (
            False,
            f"Invalid backtest event '{event}'. Must be: {', '.join(sorted(BACKTEST_EVENTS))}",
        )
    return True, ""


def _validate_backtest_prefix_segments(segments: list[str]) -> tuple[bool, str]:
    """Validate a backtest subscription prefix.

    Three shapes are accepted (all end with ``.`` — the caller has
    already stripped the trailing dot):

    - ``backtest`` (1 seg) — root, admin-only at the handler level.
    - ``backtest.{wallet_public_id}`` (2 seg) — wallet-scoped.
    - ``backtest.{wallet_public_id}.{run_public_id}`` (3 seg) —
      run-scoped.

    UUID7 format is enforced for wallet + run segments so a malformed
    prefix is rejected before any RBAC decision.

    Args:
        segments: Prefix segments (trailing dot already stripped by
            ``_validate_prefix_pattern``).

    Returns:
        Tuple of (is_valid, error_message).
    """
    if len(segments) == 1:
        return True, ""
    if len(segments) >= 2 and not is_uuid7(segments[1]):
        return False, f"Backtest wallet segment '{segments[1]}' is not a valid UUID7"
    if len(segments) == 2:
        return True, ""
    if len(segments) == 3 and not is_uuid7(segments[2]):
        return False, f"Backtest run segment '{segments[2]}' is not a valid UUID7"
    if len(segments) > 3:
        return False, "Backtest prefix has too many segments (max 3 before the event suffix)"
    return True, ""


def _validate_backtest_prefix(pattern: str) -> tuple[bool, str]:
    """Validate a backtest subscription prefix string (with trailing dot).

    Thin wrapper over :func:`_validate_backtest_prefix_segments` for
    consumers (``subscribe.py`` + ``bridge.py``) that pass the raw
    prefix string rather than pre-split segments.

    Args:
        pattern: Subscription prefix ending with ``.``.

    Returns:
        Tuple of (is_valid, error_message).
    """
    if not pattern.endswith("."):
        return False, "Backtest prefix must end with dot"
    segments = pattern[:-1].split(".")
    if any(not segment for segment in segments):
        return False, "Backtest prefix segments cannot be empty"
    if segments[0] != "backtest":
        return False, f"Expected 'backtest' prefix, got '{segments[0]}'"
    return _validate_backtest_prefix_segments(segments)
