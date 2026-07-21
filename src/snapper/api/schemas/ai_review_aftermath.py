"""Pydantic contract shared by AI-review aftermath REST and MCP reads."""

from datetime import datetime

from snapper.api.schemas.base import StrictBody
from snapper.core.json_types import JsonObject


class AiReviewAftermathReview(StrictBody):
    """Complete persisted terminal AI-review row."""

    public_id: str
    session_id: str
    sequence_id: int
    user_public_id: str
    operator_public_id: str
    wallet_public_id: str
    instrument_public_id: str
    strategy_public_id: str
    selected_delegate_public_id: str
    responding_delegate_public_id: str | None
    resolution_mode: str | None
    status: str
    signal_envelope: JsonObject
    signal_snapshot_hash: str
    instrument_metadata: JsonObject
    deadline: datetime
    fanout_after: datetime
    decision: str | None
    rationale: str | None
    dispatch_version: int
    counter_decremented_at: datetime | None
    created_at: datetime
    updated_at: datetime
    resolved_at: datetime | None


class AiReviewAftermathOrder(StrictBody):
    """Order version active at the aftermath temporal anchor."""

    public_id: str
    timestamp: datetime
    session_id: str
    sequence_id: int
    instrument: str
    exchange: str
    mode: str
    client_order_id: str
    exchange_order_id: str | None
    created_at: datetime
    updated_at: datetime | None
    side: str
    order_type: str
    price: float | None
    size: float
    filled_size: float
    average_price: float | None
    status: str
    time_in_force: str | None
    error: str | None
    leverage: int | None
    reduce_only: bool
    wallet_public_id: str | None
    operator_public_id: str | None
    plan_public_id: str | None


class AiReviewAftermathExecution(StrictBody):
    """Execution or fill version active at the aftermath temporal anchor."""

    public_id: str
    timestamp: datetime
    session_id: str
    sequence_id: int
    trade_id: str | None
    exec_id: str | None
    exchange_order_id: str | None
    client_order_id: str
    instrument: str
    exchange: str
    side: str
    size: float
    price: float
    fee: float
    fee_asset: str
    status: str
    executed_at: datetime
    wallet_public_id: str | None
    operator_public_id: str | None
    liquidity_role: str
    price_decimal: str | None
    size_decimal: str | None
    fee_decimal: str | None
    counter_amount_decimal: str | None
    numeric_provenance: str | None


class AiReviewAftermathPositionCycleTransition(StrictBody):
    """One open, close, or liquidation transition in the review window."""

    cycle_public_id: str
    transition: str
    occurred_at: datetime
    instrument_public_id: str
    exchange: str
    mode: str
    shard_key: str
    wallet_public_id: str
    operator_public_id: str | None
    direction: str
    max_qty: float
    status_at_as_of: str
    opening_command_public_id: str | None
    closing_command_public_id: str | None


class AiReviewAftermathPosition(StrictBody):
    """Current position version for one execution mode at ``as_of``."""

    public_id: str
    timestamp: datetime
    session_id: str
    sequence_id: int
    instrument: str
    instrument_public_id: str
    exchange: str
    mode: str
    quantity: float
    average_price: float | None
    unrealized_pnl: float | None
    realized_pnl: float | None
    mark_price: float | None
    marked_at: datetime | None
    source_venue_event_id: int | None
    position_cycle_public_id: str | None
    wallet_public_id: str


class AiReviewAftermathResponse(StrictBody):
    """Read-only terminal review plus exact-scope activity through ``as_of``."""

    review: AiReviewAftermathReview
    window_started_at: datetime
    as_of: datetime
    orders: list[AiReviewAftermathOrder]
    executions: list[AiReviewAftermathExecution]
    position_cycle_transitions: list[AiReviewAftermathPositionCycleTransition]
    current_positions: list[AiReviewAftermathPosition]
