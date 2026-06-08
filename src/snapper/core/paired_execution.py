"""Canonical key helpers for the paired-execution guard.

The paired-execution arming barrier validates a group's full leg set from
any single leg by comparing the sorted per-leg ``{exchange}:{instrument}:{mode}``
tokens against the ``group_key`` stamped by the originating strategy. The
formula MUST be byte-identical between the strategy that emits the key
(``BaseStrategy``) and the coordinator that validates it
(``try_arm_paired_execution_group_if_complete``), exactly like
``compute_shard_key``. Keeping it in one leaf module prevents drift that would
either fail every arming (legs never match the key) or, worse, let a wrong leg
set arm.
"""

from snapper.core.types import ExecutionMode
from snapper.core.types import OrderExchange


def paired_group_leg_token(exchange: OrderExchange, instrument: str, mode: ExecutionMode) -> str:
    """Return the canonical single-leg token ``{exchange}:{instrument}:{mode}``.

    Args:
        exchange: Order-capable exchange identifier for the leg.
        instrument: The traded symbol (e.g. ``"BTC-USD"``).
        mode: Execution mode — ``"live"`` or ``"paper"``.

    Returns:
        The canonical leg token used to build a ``group_key``.
    """
    return f"{exchange}:{instrument}:{mode}"


def compute_paired_group_key(legs: list[tuple[OrderExchange, str, ExecutionMode]]) -> str:
    """Return the canonical ``group_key`` for a paired-execution leg set.

    The key is the sorted per-leg tokens joined with ``"|"``. Sorting makes
    the key independent of leg emission/arrival order so any coordinator can
    reconstruct and compare it.

    Args:
        legs: One ``(exchange, instrument, mode)`` tuple per group leg.

    Returns:
        The canonical, order-independent group key.
    """
    tokens = sorted(
        paired_group_leg_token(exchange, instrument, mode) for exchange, instrument, mode in legs
    )
    return "|".join(tokens)
