"""Shared readers for persisted execution-plan params.

Centralizes the order-type extraction that cancel/flatten paths
previously open-coded six times against the legacy
``venue_order_type`` param. Since #156 the route and MCP writers
persist the CORE vocabulary under ``order_type`` and stopped writing
``venue_order_type``; plans created before the fix still carry the
wire-vocabulary value, so the fallback normalizes it through
:data:`~snapper.infrastructure.exchanges.contracts.EXCHANGE_TO_CORE_ORDER_TYPE`
to keep pre-fix plans cancellable. One helper, one behaviour — drift
between the four reader files is impossible.
"""

from snapper.core.json_types import JsonObject
from snapper.infrastructure.exchanges.contracts import EXCHANGE_TO_CORE_ORDER_TYPE


def core_order_type_from_plan_params(params: JsonObject) -> str:
    """Return the CORE order type recorded in plan params.

    Prefers the ``order_type`` param (CORE vocabulary, written by every
    plan creator). Falls back to the legacy ``venue_order_type`` param
    (wire vocabulary, written before #156) normalized to CORE; unmapped
    wire values pass through unchanged rather than being guessed.
    Defaults to ``"market"`` when neither param is present — the
    pre-existing behaviour of every replaced reader.

    Args:
        params: Persisted execution-plan params.

    Returns:
        CORE order type string for re-emitted cancel/flatten commands.
    """
    order_type = params.get("order_type")
    if isinstance(order_type, str) and order_type:
        return order_type
    legacy = params.get("venue_order_type")
    if isinstance(legacy, str) and legacy:
        return EXCHANGE_TO_CORE_ORDER_TYPE.get(legacy, legacy)
    return "market"
