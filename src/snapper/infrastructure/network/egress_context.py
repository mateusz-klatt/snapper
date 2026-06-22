"""Context-local egress identity for routing public and private traffic."""

from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from typing import Literal
from typing import Protocol

TrafficClass = Literal["public", "private"]
"""Allowed egress traffic classes."""


@dataclass(frozen=True)
class EgressIdentity:
    """Explicit identity for one egress-routing scope.

    Attributes:
        exchange: Venue name used by the egress pool.
        traffic_class: ``"public"`` for market-data style traffic or
            ``"private"`` for authenticated executor traffic.
        owner: Component that owns the scoped traffic.
        operation: Operation name inside the owner.
    """

    exchange: str
    traffic_class: TrafficClass
    owner: str
    operation: str


class _PublisherContext(Protocol):
    """Publisher protocol used by the legacy public fallback."""

    def _get_exchange_name(self) -> str:
        """Return the exchange name used to tag public publisher traffic."""
        ...


_EGRESS_IDENTITY: ContextVar[EgressIdentity | None] = ContextVar(
    "snapper_egress_identity", default=None
)

_CURRENT_PUBLISHER: ContextVar[_PublisherContext | None] = ContextVar(
    "_kraken_current_publisher", default=None
)
"""ContextVar carrying the owning publisher instance for legacy public traffic."""


@contextmanager
def egress_identity(
    *,
    exchange: str,
    traffic_class: TrafficClass,
    owner: str,
    operation: str,
) -> Iterator[None]:
    """Set an explicit egress identity for the current context.

    Args:
        exchange: Venue name used by the egress pool.
        traffic_class: Explicit public/private traffic class.
        owner: Component that owns the scoped traffic.
        operation: Operation name inside the owner.

    Yields:
        None while the identity is active.
    """
    token = _EGRESS_IDENTITY.set(
        EgressIdentity(
            exchange=exchange,
            traffic_class=traffic_class,
            owner=owner,
            operation=operation,
        )
    )
    try:
        yield
    finally:
        _EGRESS_IDENTITY.reset(token)


def current_egress_identity() -> EgressIdentity | None:
    """Return the explicit egress identity for the current context.

    Returns:
        The active ``EgressIdentity`` or ``None`` when traffic is untagged.
    """
    return _EGRESS_IDENTITY.get()


def resolve_egress_traffic(default_exchange: str = "kraken") -> tuple[str, str]:
    """Resolve the current exchange and traffic class.

    Explicit identities always win. Untagged traffic falls back to the
    existing publisher ContextVar as public traffic, then finally to the
    supplied default exchange as public traffic. This function never returns
    ``"private"`` unless an explicit private ``EgressIdentity`` is active.

    Args:
        default_exchange: Exchange name used when no explicit identity or
            publisher fallback exists.

    Returns:
        ``(exchange, traffic_class)`` for the current egress scope.
    """
    identity = _EGRESS_IDENTITY.get()
    if identity is not None:
        return identity.exchange, identity.traffic_class

    publisher = _CURRENT_PUBLISHER.get()
    if publisher is not None:
        return publisher._get_exchange_name(), "public"
    return default_exchange, "public"
