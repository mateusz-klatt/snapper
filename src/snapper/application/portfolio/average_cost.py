"""Pure volume-weighted average-cost position accounting kernel.

Single source of truth for the average-cost position transition shared by the
live ``TradeService`` projection (the authoritative fill-derived position feed)
and the P&L timeline reconstruction (``pnl_timeline``). Routing both through this
kernel is what makes the timeline's realized/unrealized decomposition provably
consistent with the ``positions`` surface, since that surface is written from the
same ``TradeService`` projection. ``PortfolioTracker`` keeps a legacy in-memory
copy of the same VWAP / reduce / flip math for the engine's local cash view; it
is not the durable positions source and is left on its own copy for now.

The kernel is fee-exclusive and funding-exclusive: :attr:`PoolFillOutcome.
realized_delta` is price-realized profit and loss only. Execution fees and
funding accruals are separate P&L components tracked by the callers, matching the
``Position.realized_pnl`` contract (execution-fee-exclusive, funding-inclusive)
where funding is folded in downstream rather than inside the price math.

The flat threshold is ``1e-12`` to match the zero-snap the legacy call sites
used, so quantities below it are indistinguishable from flat.
"""

from dataclasses import dataclass
from typing import Final
from typing import Literal

FLAT_EPSILON: Final[float] = 1e-12
"""Absolute quantity at or below which a position is treated as flat."""

CycleTransition = Literal["open", "close", "flip", "scale_up"]
"""Lifecycle transition a single fill induces on the open cycle."""


@dataclass(frozen=True)
class PoolFillOutcome:
    """Result of applying one fill to an average-cost pool.

    Attributes:
        position_qty: Signed position quantity after the fill, snapped to
            exactly ``0.0`` when within :data:`FLAT_EPSILON` of flat.
        entry_price: Volume-weighted average entry price after the fill, or
            ``None`` when the resulting position is flat.
        realized_delta: Price-realized profit and loss produced by this fill
            alone (fee-exclusive, funding-exclusive). Non-zero only for the
            portion of the fill that closed pre-fill inventory.
        closed_qty: Quantity of the pre-fill pool this fill closed. Allocated
            pro-rata against the pre-fill origin/strategy weights by attribution
            consumers. Zero for a pure position-increasing fill.
        added_qty: Quantity this fill added to a same-direction side, or the
            overshoot quantity that opened the new side on a flip. Assigned to
            the incoming fill's origin bucket by attribution consumers.
        transition: Cycle lifecycle classification, or ``None`` when the fill
            required no cycle boundary change (flat-to-flat or scale-down/hold).
        opened_new_side: ``True`` when the fill established a fresh open side —
            opening from flat or the overshoot side of a flip — which the live
            projection uses to stamp the open-cycle timestamp.
    """

    position_qty: float
    entry_price: float | None
    realized_delta: float
    closed_qty: float
    added_qty: float
    transition: CycleTransition | None
    opened_new_side: bool


def classify_transition(old_qty: float, new_qty: float) -> CycleTransition | None:
    """Classify a signed position change into a cycle lifecycle transition.

    Pure helper with no side effects. ``open``: flat became non-flat. ``close``:
    non-flat returned to flat. ``flip``: direction reversed in a single fill.
    ``scale_up``: same direction with strictly larger absolute quantity.
    ``None``: flat-to-flat, or a scale-down / hold where the absolute quantity
    did not grow. The epsilon matches :data:`FLAT_EPSILON`.

    Args:
        old_qty: Signed position quantity before the fill.
        new_qty: Signed position quantity after the fill.

    Returns:
        The transition label, or ``None`` when no cycle boundary changed.
    """
    old_flat = abs(old_qty) < FLAT_EPSILON
    new_flat = abs(new_qty) < FLAT_EPSILON
    if old_flat and new_flat:
        return None
    if old_flat:
        return "open"
    if new_flat:
        return "close"
    same_sign = (old_qty > 0.0) == (new_qty > 0.0)
    if not same_sign:
        return "flip"
    if abs(new_qty) > abs(old_qty):
        return "scale_up"
    return None


def apply_fill(
    position_qty: float,
    entry_price: float | None,
    signed_qty: float,
    fill_size: float,
    fill_price: float,
) -> PoolFillOutcome:
    """Apply one fill to an average-cost pool and return the transition outcome.

    Reproduces the legacy ``TradeService._update_position`` /
    ``PortfolioTracker.update_fill`` position math exactly: same-direction fills
    recompute the weighted-average entry over absolute quantity; opposite-
    direction fills realize price P&L on the closed portion (sign-aware for
    shorts) and reset the entry to the fill price only on overshoot; the
    resulting quantity is snapped to flat within :data:`FLAT_EPSILON`.

    Args:
        position_qty: Signed position quantity before the fill.
        entry_price: Weighted-average entry price before the fill, or ``None``
            when the pre-fill position is flat.
        signed_qty: Fill quantity with sign — positive for BUY, negative for
            SELL.
        fill_size: Unsigned fill quantity (``abs(signed_qty)``).
        fill_price: Execution price of the fill.

    Returns:
        The :class:`PoolFillOutcome` describing the post-fill pool and the
        per-fill realized/closed/added decomposition.
    """
    is_increasing = (position_qty >= 0.0 and signed_qty > 0.0) or (
        position_qty <= 0.0 and signed_qty < 0.0
    )
    realized_delta = 0.0
    closed_qty = 0.0
    added_qty = 0.0
    opened_new_side = False
    if is_increasing:
        old_qty = abs(position_qty)
        new_abs = old_qty + fill_size
        if entry_price is not None and old_qty > 0.0 and new_abs > 0.0:
            new_entry: float | None = (old_qty * entry_price + fill_size * fill_price) / new_abs
        else:
            new_entry = fill_price
            opened_new_side = True
        added_qty = fill_size
    else:
        close_qty = min(fill_size, abs(position_qty))
        overshoot = fill_size - close_qty
        if entry_price is not None and close_qty > 0.0:
            pnl_per_unit = fill_price - entry_price
            if position_qty < 0.0:
                pnl_per_unit = entry_price - fill_price
            realized_delta = close_qty * pnl_per_unit
        closed_qty = close_qty
        added_qty = overshoot
        if overshoot > FLAT_EPSILON:
            new_entry = fill_price
            opened_new_side = True
        else:
            new_entry = entry_price

    new_qty = position_qty + signed_qty
    if abs(new_qty) < FLAT_EPSILON:
        new_qty = 0.0
        new_entry = None
    transition = classify_transition(position_qty, new_qty)
    return PoolFillOutcome(
        position_qty=new_qty,
        entry_price=new_entry,
        realized_delta=realized_delta,
        closed_qty=closed_qty,
        added_qty=added_qty,
        transition=transition,
        opened_new_side=opened_new_side,
    )
