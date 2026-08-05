"""Which execution modifiers each venue's client actually sends, and what to refuse.

This lives beside :mod:`execution_venue` rather than in a transport because the
question it answers is a property of the exchange CLIENT, not of MCP or REST.
Both surfaces resolve the execution venue and then need the same answer, and the
answer must not differ between them: an order refused for an agent and accepted
for a human would be the worst possible split.

The decision is returned as :class:`ModifierRefusal`, not as an HTTP response or
an MCP envelope. Each transport formats it in its own shape — REST as a 400 with
a structured ``detail``, MCP as the canonical failure envelope — while sharing
the truth about what the venue will do.
"""

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Final

from snapper.core.json_types import JsonObject
from snapper.core.json_types import JsonValue
from snapper.core.types import ExchangeEnum

LEVERAGE: Final[str] = "leverage"
POST_ONLY: Final[str] = "post_only"
REDUCE_ONLY: Final[str] = "reduce_only"

HONOURED_MANUAL_ORDER_FLAGS: Final[Mapping[str, frozenset[str]]] = {
    ExchangeEnum.KRAKEN: frozenset({LEVERAGE, POST_ONLY, REDUCE_ONLY}),
    ExchangeEnum.KRAKEN_FUTURES: frozenset({POST_ONLY, REDUCE_ONLY}),
    ExchangeEnum.KRAKEN_EQUITIES: frozenset(),
    ExchangeEnum.WALUTOMAT: frozenset(),
    ExchangeEnum.PAPER: frozenset(),
}
"""Execution modifiers each venue's client actually puts on the wire.

Read this as a property of OUR CLIENT, never of the exchange. The distinction is
the whole point: Kraken spot's API accepts all three — ``kraken.spot`` exposes
``leverage``, ``oflags`` and ``reduce_only`` on ``create_order`` — so a refusal
worded "the venue does not support this" would simply be false. What varies is
which of them we forward.

A venue missing from this mapping honours nothing, because
:func:`honoured_flags` defaults to the empty set. That default is load-bearing
rather than tidy: a venue added later cannot silently inherit permission to
accept a flag its client drops on the floor.
"""

LIMIT_POST_ONLY_TYPES: Final[Mapping[str, frozenset[str]]] = {
    ExchangeEnum.KRAKEN: frozenset({"limit", "stop_limit"}),
    ExchangeEnum.KRAKEN_FUTURES: frozenset({"limit"}),
}
"""Order types that actually carry ``post_only`` through, per venue.

Maker-only is meaningless for an order that must take liquidity, and each venue
drops it differently, so the venue set alone is not a fine enough gate.

On Kraken spot a ``stop_limit`` reaches ccxt as type ``limit`` and comes out with
``oflags=post`` on the wire, so it belongs here; ``market`` and ``stop`` both
reach ccxt as ``market``, where ccxt itself raises rather than submit. On Kraken
futures only the plain ``lmt`` is rewritten to ``post`` — ``stop_limit`` maps to
``stp`` and the flag vanishes with no error at all, which is the silent drop this
gate exists to catch.
"""

DISCLOSING_VENUES: Final[frozenset[str]] = frozenset({ExchangeEnum.PAPER})
"""Venues where a dropped modifier is disclosed rather than refused.

Paper is where an agent rehearses, and refusing there would block the default
practice venue for a workflow that is legitimate the moment it moves to Kraken.
Nothing can be overshot into: there is no real position and no real money, so the
only cost of the ignored flag is fidelity.

That cost is real, so it is stated rather than swallowed — the caller is not
stopped, and is not left believing a constraint was applied.
"""


@dataclass(frozen=True)
class ModifierRefusal:
    """A refusal decision, formatted by whichever transport asked for it."""

    error_code: str
    message: str
    details: JsonObject


def honoured_flags(execution_exchange: str) -> frozenset[str]:
    """Return the modifiers the resolved venue's client puts on the wire.

    Args:
        execution_exchange: The resolved execution venue.

    Returns:
        The honoured flag names, empty for any venue not enumerated.
    """
    return HONOURED_MANUAL_ORDER_FLAGS.get(execution_exchange, frozenset())


def requested_flags(*, leverage: int | None, post_only: bool, reduce_only: bool) -> tuple[str, ...]:
    """Name the execution modifiers a submit actually asked for.

    ``leverage`` counts only when set, since ``None`` is the absence of a request
    rather than a request for no leverage.

    Args:
        leverage: Requested margin leverage, or ``None``.
        post_only: Whether maker-only placement was requested.
        reduce_only: Whether the reduce-only clamp was requested.

    Returns:
        Requested flag names, in a stable order.
    """
    requested: list[str] = []
    if leverage is not None:
        requested.append(LEVERAGE)
    if post_only:
        requested.append(POST_ONLY)
    if reduce_only:
        requested.append(REDUCE_ONLY)
    return tuple(requested)


def inert_flags(
    execution_exchange: str, *, leverage: int | None, post_only: bool, reduce_only: bool
) -> tuple[str, ...]:
    """Name every requested flag the resolved venue will not act on.

    Args:
        execution_exchange: The resolved execution venue.
        leverage: Requested margin leverage, or ``None``.
        post_only: Whether maker-only placement was requested.
        reduce_only: Whether the reduce-only clamp was requested.

    Returns:
        Requested-but-dropped flag names, empty when all are honoured.
    """
    honoured = honoured_flags(execution_exchange)
    requested = requested_flags(leverage=leverage, post_only=post_only, reduce_only=reduce_only)
    return tuple(flag for flag in requested if flag not in honoured)


def venues_honouring(flag: str) -> list[str]:
    """List the venues whose client forwards a given modifier.

    Args:
        flag: One of ``leverage``, ``post_only``, ``reduce_only``.

    Returns:
        Sorted venue names, for a refusal's ``details``.
    """
    return sorted(venue for venue, flags in HONOURED_MANUAL_ORDER_FLAGS.items() if flag in flags)


def _flags_unsupported(exchange: str, flags: tuple[str, ...]) -> ModifierRefusal:
    """Refuse modifiers this venue's client drops.

    The wording says SNAPPER does not send the flag, never that the exchange
    cannot do it. On Kraken spot in particular the venue accepts all three, so a
    "venue does not support it" refusal would be a false statement handed to a
    caller that acts on it.

    Args:
        exchange: The resolved execution venue.
        flags: The requested-but-dropped flag names.

    Returns:
        The refusal decision.
    """
    named = ", ".join(flags)
    supported: dict[str, JsonValue] = {}
    for flag in flags:
        venues: list[JsonValue] = [*venues_honouring(flag)]
        supported[flag] = venues
    return ModifierRefusal(
        error_code="order_flags_unsupported",
        message=(
            f"Snapper does not send {named} to {exchange}, so the order would be "
            "submitted without it. Resubmit without the flag, or use a venue whose "
            "client forwards it (details.supported_exchanges)."
        ),
        details={
            "exchange": exchange,
            "flags": list(flags),
            "supported_exchanges": supported,
        },
    )


def _futures_leverage(exchange: str) -> ModifierRefusal:
    """Refuse per-order ``leverage`` on a futures venue.

    Kraken Futures DOES trade on leverage; it simply is not an order parameter
    there. ``sendorder`` has no such field — leverage is a per-symbol account
    preference reached through ``set_leverage_preference``. A refusal that let a
    caller conclude "this venue cannot do leverage" would be false and would push
    it toward the wrong remedy, so the message names the real mechanism.

    Snapper deliberately does not set that preference on the caller's behalf: it
    is account-wide for the symbol and outlives the order, so one manual submit
    would silently re-lever every other position and every later order on that
    symbol. A refusal is recoverable; a silent re-lever is not.

    Args:
        exchange: The resolved execution venue.

    Returns:
        The refusal decision.
    """
    return ModifierRefusal(
        error_code="leverage_not_an_order_parameter",
        message=(
            f"{exchange} trades on leverage, but not as an order parameter: it is a "
            "per-symbol account preference, set separately and outliving the order. "
            "Resubmit without leverage; the position will use whatever leverage "
            "preference the symbol already carries."
        ),
        details={"exchange": exchange, "flag": LEVERAGE},
    )


def _post_only_order_type(exchange: str, order_type: str) -> ModifierRefusal:
    """Refuse ``post_only`` on a taker order type.

    Args:
        exchange: The resolved execution venue.
        order_type: The submitted order type.

    Returns:
        The refusal decision.
    """
    resting: list[JsonValue] = [*sorted(LIMIT_POST_ONLY_TYPES.get(exchange, frozenset()))]
    return ModifierRefusal(
        error_code="post_only_order_type_unsupported",
        message=(
            f"post_only does not reach {exchange} on a {order_type} order. Maker-only "
            "constrains an order that rests on the book, so it is meaningless for one "
            "that must take liquidity. Resubmit as a resting order type "
            "(details.supported_order_types), or without post_only."
        ),
        details={
            "exchange": exchange,
            "order_type": order_type,
            "supported_order_types": resting,
        },
    )


def _reduce_only_needs_margin(exchange: str) -> ModifierRefusal:
    """Refuse spot ``reduce_only`` without leverage.

    Kraken spot binds the reduce-only clamp to a MARGIN position, so on a cash
    order there is nothing for it to reduce and the venue would reject or ignore
    it. This gate is venue-scoped on purpose: Kraken Futures carries
    ``reduceOnly`` with no leverage precondition at all, and applying the same
    rule there would make the flag unreachable in both directions — refused
    without leverage, and refused WITH leverage because futures takes no
    per-order leverage.

    Deliberately NOT enforced in the exchange client. The client serves every
    producer of the flag, and the strategy engine's close, the paired-execution
    guard scanner's protective flatten, and the bracket and trailing-stop exits
    all emit ``reduce_only`` with no leverage. A client-side version of this rule
    would refuse every one of them, leaving spot positions that can be opened and
    never closed.

    Args:
        exchange: The resolved execution venue.

    Returns:
        The refusal decision.
    """
    return ModifierRefusal(
        error_code="reduce_only_requires_margin",
        message=(
            f"reduce_only on {exchange} applies to margin orders only — the clamp "
            "binds to a leveraged position, and a cash order has none for it to bind "
            "to. Resubmit with leverage, or size the order so it cannot exceed the "
            "position you hold."
        ),
        details={"exchange": exchange, "flag": REDUCE_ONLY},
    )


def modifier_refusal(
    execution_exchange: str,
    *,
    order_type: str,
    leverage: int | None,
    post_only: bool,
    reduce_only: bool,
) -> ModifierRefusal | None:
    """Refuse a submit whose execution modifiers would not reach the venue.

    Fail-closed: a modifier the caller asked for either reaches the exchange or
    the submit is refused. The one exception is :data:`DISCLOSING_VENUES`, where
    the flag is named on the accepted response instead.

    Args:
        execution_exchange: The resolved execution venue.
        order_type: The submitted order type.
        leverage: Requested margin leverage, or ``None``.
        post_only: Whether maker-only placement was requested.
        reduce_only: Whether the reduce-only clamp was requested.

    Returns:
        The refusal decision, or ``None`` when every modifier is honoured.
    """
    if execution_exchange in DISCLOSING_VENUES:
        return None
    dropped = inert_flags(
        execution_exchange,
        leverage=leverage,
        post_only=post_only,
        reduce_only=reduce_only,
    )
    if dropped == (LEVERAGE,) and execution_exchange == ExchangeEnum.KRAKEN_FUTURES:
        return _futures_leverage(execution_exchange)
    if dropped:
        return _flags_unsupported(execution_exchange, dropped)
    if post_only and order_type not in LIMIT_POST_ONLY_TYPES.get(execution_exchange, frozenset()):
        return _post_only_order_type(execution_exchange, order_type)
    if reduce_only and leverage is None and execution_exchange == ExchangeEnum.KRAKEN:
        return _reduce_only_needs_margin(execution_exchange)
    return None
