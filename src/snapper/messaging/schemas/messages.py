"""ZMQ message routing and parsing for inter-process communication.

This module provides the message type registry and polymorphic deserialization
for typed messages on the ZMQ bus. All message types are Data classes defined
in snapper.messaging.schemas.data.

Functions:
    parse_message: Deserialize JSON string to typed Data instance.
"""

import json
from typing import Any

from snapper.api.schemas.base import PartialBody
from snapper.api.schemas.base import StrictDataSchema
from snapper.messaging.schemas.data import AlertEventData
from snapper.messaging.schemas.data import CandleData
from snapper.messaging.schemas.data import ExecutionData
from snapper.messaging.schemas.data import ExecutionPlanDecisionEventData
from snapper.messaging.schemas.data import HeartbeatData
from snapper.messaging.schemas.data import OrderCancelData
from snapper.messaging.schemas.data import OrderData
from snapper.messaging.schemas.data import OrderEventData
from snapper.messaging.schemas.data import OrderReplaceData
from snapper.messaging.schemas.data import OrderRequestData
from snapper.messaging.schemas.data import ProcessConfiguredEventData
from snapper.messaging.schemas.data import ProcessRunEventData
from snapper.messaging.schemas.data import ProcessSummaryEventData
from snapper.messaging.schemas.data import ReplayEndData
from snapper.messaging.schemas.data import ReplayStartData
from snapper.messaging.schemas.data import SettingChangedData
from snapper.messaging.schemas.data import SignalData
from snapper.messaging.schemas.data import StrategyListEventData
from snapper.messaging.schemas.data import SymbolAliasUpdateData
from snapper.messaging.schemas.data import TickData
from snapper.messaging.schemas.data import TradeData

MarketDataMessage = TickData | CandleData | TradeData
"""Union type for all market data message types."""


class GapEnvelope(PartialBody):
    """Minimal typed envelope for gap detection on the bridge receive path.

    Extracts only the provenance fields needed for gap detection from any
    ZMQ payload. All other fields are silently ignored, so this model
    accepts every well-formed JSON object on the bus regardless of type.

    Attributes:
        session_id: Producer session identifier (empty string when absent).
        sequence_id: Per-table monotonic counter (zero when absent).
        type: Payload item type discriminator (empty string when absent).
        wallet_public_id: Owning wallet for per-wallet stream
            partitioning. Empty string when absent — legacy
            producers simply fall back to topic-only GapDetector keying.
    """

    session_id: str = ""
    sequence_id: int = 0
    type: str = ""
    wallet_public_id: str = ""


class MessageParseError(Exception):
    """Raised when message deserialization fails.

    Indicates malformed JSON, missing type field, unknown message type,
    or validation errors during parsing.
    """

    pass


MESSAGE_TYPE_MAP: dict[str, type[StrictDataSchema[Any]]] = {
    "tick": TickData,
    "candle": CandleData,
    "trade": TradeData,
    "signal": SignalData,
    "order_request": OrderRequestData,
    "order_cancel": OrderCancelData,
    "order_replace": OrderReplaceData,
    "order_event": OrderEventData,
    "execution": ExecutionData,
    "heartbeat": HeartbeatData,
    "setting_changed": SettingChangedData,
    "order": OrderData,
    "symbol_alias_update": SymbolAliasUpdateData,
    "replay_start": ReplayStartData,
    "replay_end": ReplayEndData,
    "alert_event": AlertEventData,
    "execution_plan_decision_event": ExecutionPlanDecisionEventData,
    "process_summary_event": ProcessSummaryEventData,
    "process_configured_event": ProcessConfiguredEventData,
    "process_run_event": ProcessRunEventData,
    "strategy_list_event": StrategyListEventData,
}
"""Mapping from message type string to Data class for deserialization."""


def parse_message(data: str) -> StrictDataSchema[Any]:
    """Parse JSON string into typed Data instance.

    Deserializes a JSON message string and returns the appropriate
    typed instance based on the 'type' field discriminator.

    Args:
        data: JSON string containing the serialized message.

    Returns:
        Typed Data instance (TickData, CandleData, etc.).

    Raises:
        MessageParseError: If JSON is invalid, type field is missing,
            message type is unknown, or validation fails.
    """
    try:
        raw_data = json.loads(data)
    except json.JSONDecodeError as e:
        raise MessageParseError(f"Invalid JSON: {e}") from e
    msg_type = raw_data.get("type")
    if not msg_type:
        raise MessageParseError("Message missing 'type' field")
    message_class = MESSAGE_TYPE_MAP.get(msg_type)
    if not message_class:
        raise MessageParseError(f"Unknown message type: {msg_type}")
    try:
        return message_class.model_validate_json(data)
    except Exception as e:
        raise MessageParseError(f"Failed to validate {msg_type} message: {e}") from e
