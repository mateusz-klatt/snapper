"""Reconstruction of dispatch-time payloads from durable TradeCommand rows.

The outbox publishes an ``OrderRequestData`` built from the command row;
the executor's ghost-adoption and dispatched-verification sweeps (#145
Phase E) must rebuild the SAME request when re-attaching to an order the
venue confirmed — building it from a venue snapshot instead would lose
``strategy_tag`` and corrupt shard attribution for every subsequent
venue event (the shard key is computed from the request's tag). Keeping
the reconstruction here, shared by both sides, makes drift impossible.
"""

from datetime import UTC
from datetime import datetime
from typing import cast

from snapper.core.types import ExecutionMode
from snapper.core.types import FrameOrigin
from snapper.core.types import OrderExchange
from snapper.core.types import OrderType
from snapper.core.types import TradeSide
from snapper.data.repository_types import TradeCommandRow
from snapper.messaging.schemas.data import OrderRequestData

_WALLET_SHORT_SEGMENT_LEN = 13


def parse_shard_key(shard_key: str) -> tuple[str, str, str, str, str | None] | None:
    """Parse a persisted ``shard_key`` into its components.

    An optional ``w{wallet_short}`` segment sits between ``mode``
    and the optional paper-mode strategy_tag.
    The parser handles both legacy (3- or 4-segment) and
    wallet-aware (4- or 5-segment) formats.

    Args:
        shard_key: The persisted shard key string.

    Returns:
        Tuple of ``(exchange, instrument, mode, wallet_short,
        strategy_tag)`` where ``wallet_short`` is empty for
        legacy keys and ``strategy_tag`` is None when absent.
        Returns ``None`` if the key has fewer than 3 segments.
    """
    parts = shard_key.split(".")
    if len(parts) < 3:
        return None
    exchange_str, instrument, mode_str = parts[0], parts[1], parts[2]
    wallet_short = ""
    strategy_tag: str | None = None
    remaining = parts[3:]
    if (
        remaining
        and remaining[0].startswith("w")
        and len(remaining[0]) == _WALLET_SHORT_SEGMENT_LEN
        and all(c in "0123456789abcdef" for c in remaining[0][1:])
    ):
        wallet_short = remaining[0][1:]
        remaining = remaining[1:]
    if remaining:
        strategy_tag = remaining[0]
    return exchange_str, instrument, mode_str, wallet_short, strategy_tag


def order_request_from_command(cmd: TradeCommandRow) -> OrderRequestData:
    """Build the ``OrderRequestData`` a dispatch of this command publishes.

    Field-for-field the outbox's create/submit publish payload: identity
    and sizing from the command row, ``strategy_tag`` parsed back out of
    the persisted ``shard_key`` (the row stores no tag column), and
    ``signaled_at=created_at`` so the executor's staleness gate sees the
    command's TRUE age on adoption paths exactly as it does on dispatch.

    Args:
        cmd: The durable trade command row (create/submit type).

    Returns:
        The reconstructed order request.
    """
    parsed_shard = parse_shard_key(cmd["shard_key"])
    tag = parsed_shard[4] if parsed_shard else None
    return OrderRequestData(
        public_id=cmd["client_order_id"],
        timestamp=datetime.now(UTC),
        session_id=cmd["session_id"],
        sequence_id=cmd["sequence_id"],
        strategy_id=cmd["strategy_id"],
        instrument=cmd["instrument"],
        mode=cast(ExecutionMode, cmd["mode"]),
        side=cast(TradeSide, cmd["side"]),
        order_type=cast(OrderType, cmd["order_type"]),
        quantity=cmd["quantity"],
        price=cmd["price"],
        stop_price=cmd.get("stop_price"),
        client_order_id=cmd["client_order_id"],
        exchange=cast(OrderExchange, cmd["exchange"]),
        strategy_tag=tag,
        leverage=cmd["leverage"],
        reduce_only=cmd["reduce_only"],
        wallet_public_id=cmd.get("wallet_public_id") or "",
        operator_public_id=cmd.get("operator_public_id"),
        user_public_id=cmd.get("user_public_id"),
        signaled_at=cmd["created_at"],
        origin=cast(FrameOrigin, cmd.get("origin", "live")),
        replay_window_start=cmd.get("replay_window_start"),
        replay_window_end=cmd.get("replay_window_end"),
    )
