"""Per-frame scope filters for WebSocket-broadcast topic families.

Lives in its own module (rather than alongside the subscribe-time
RBAC handlers) so the bridge can import it WITHOUT pulling in the
broader handlers package — that path crosses
``connection_manager`` -> ``bridge`` and would close the import loop.

Filters have no transport coupling: each takes already-resolved data
(topic + principal + parsed payload) and returns a boolean. The bridge
calls them inside ``_forward_to_clients`` per-subscription so each
client sees only the events they have scope for.

Three filters live here today:

- :func:`enforce_ai_review_scope` — gates ``ai_reviews.*`` per-AI-delegate
  by ``(wallet, instrument)`` grant.
- :func:`enforce_orders_events_scope` — gates ``orders.events.*`` per
  principal by accessible-wallet set, mirroring the REST
  ``/api/orders`` wallet-scope filter (the v0.7.0 RBAC symmetry fix).
- :func:`enforce_alerts_scope` — gates ``alerts.*`` per principal by
  exact ``user_public_id`` match (ADMIN bypass). Powers the Phase E
  live-refresh path so a web user only sees their own alert frames.
"""

from collections.abc import Mapping
from collections.abc import MutableMapping
from datetime import UTC
from datetime import datetime
from typing import Any

from snapper.auth.domain.roles import UserRole
from snapper.auth.schemas.principal import AuthPrincipal
from snapper.auth.scope_grant_service import ScopeGrantService

__all__ = [
    "AI_REVIEWS_TOPIC_PREFIX",
    "ALERTS_TOPIC_PREFIX",
    "ORDERS_EVENTS_TOPIC_PREFIX",
    "OrdersEventsAccessCache",
    "orders_events_access_cache_key",
    "enforce_ai_review_scope",
    "enforce_alerts_scope",
    "enforce_orders_events_scope",
]


AI_REVIEWS_TOPIC_PREFIX = "ai_reviews."
ALERTS_TOPIC_PREFIX = "alerts."
ORDERS_EVENTS_TOPIC_PREFIX = "orders.events."
OrdersEventsAccessCacheKey = tuple[str, str, tuple[str, ...], str, str | None, str | None]
OrdersEventsAccessCache = MutableMapping[OrdersEventsAccessCacheKey, frozenset[str]]
"""Topic-family prefix the orders.events. per-frame filter gates.

Frames whose topic does NOT start with this prefix bypass the filter
entirely (other RBAC paths own them). Frames inside the family go
through :func:`enforce_orders_events_scope` per-subscription so a
VIEWER / OPERATOR / AI_DELEGATE only sees ``orders.events.*`` frames
for wallets covered by their accessible wallet set.
"""
"""Topic-family prefix the per-frame filter gates.

Frames whose topic does NOT start with this prefix bypass the filter
entirely (registry-root subscribed roles already passed the
subscribe-time RBAC check). Frames inside the family go through
:func:`enforce_ai_review_scope` per-subscription so AI delegates only
see CONSULT events for ``(wallet, instrument)`` tuples their scope
grant covers.
"""


def orders_events_access_cache_key(principal: AuthPrincipal) -> OrdersEventsAccessCacheKey:
    """Return the authorization identity key for frame-local wallet access caching.

    Args:
        principal: Authenticated principal whose wallet-scope query
            result is being cached.

    Returns:
        Immutable identity tuple suitable for a single-frame fan-out
        cache key.
    """
    return (
        principal.role.value,
        principal.user_public_id,
        tuple(principal.operator_public_ids),
        principal.primary_operator_public_id,
        principal.active_wallet_public_id,
        principal.delegate_public_id,
    )


async def enforce_ai_review_scope(
    *,
    topic: str,
    connection_principal: AuthPrincipal | None,
    payload: Mapping[str, Any],
    scope_grant_service: ScopeGrantService,
    as_of: datetime | None = None,
) -> bool:
    """Return True iff the principal may receive the frame.

    Bridge calls this per-subscription before sending an
    ``ai_reviews.*`` frame so a delegate can subscribe to the
    registry-root prefix without seeing CONSULT events for wallets /
    instruments outside its scope grant.

    Pass-through rules:

    - Non-``ai_reviews.*`` topics return ``True`` immediately
      (every other category has its own RBAC at subscribe-time +
      any wallet narrowing already applied; this filter only owns
      the ai_reviews family).
    - Missing principal (e.g. WS pre-auth or auth dropped) returns
      ``False`` — no frame leaks to an un-authenticated socket.
    - Missing ``delegate_public_id`` (non-AI_DELEGATE principal that
      somehow subscribed) returns ``False`` because the scope-grant
      check is delegate-keyed; a non-delegate has no row to evaluate.
    - Malformed payload (missing / non-string ``wallet_public_id`` or
      ``instrument_public_id``) returns ``False``: the SCD2 query
      requires string keys + the upstream serialisation would have
      crashed downstream anyway, so dropping here costs nothing.

    Args:
        topic: The full ZMQ topic string (e.g.
            ``ai_reviews.user-1.strat-2.request``).
        connection_principal: The authenticated principal for the
            destination socket; ``None`` when the WS hasn't yet
            authenticated.
        payload: Already-parsed JSON envelope. The filter consults
            ``wallet_public_id`` and ``instrument_public_id``.
        scope_grant_service: Singleton owner of the scope-grant
            check; bridge passes the lifespan-attached instance.
        as_of: Wall-clock for SCD2-active filtering on memberships
            and grants. Defaults to ``datetime.now(UTC)``.

    Returns:
        ``True`` to forward the frame, ``False`` to drop it.
    """
    if not topic.startswith(AI_REVIEWS_TOPIC_PREFIX):
        return True
    if connection_principal is None:
        return False
    delegate_public_id = connection_principal.delegate_public_id
    if delegate_public_id is None:
        return False
    wallet_public_id = payload.get("wallet_public_id")
    instrument_public_id = payload.get("instrument_public_id")
    if not isinstance(wallet_public_id, str) or not isinstance(instrument_public_id, str):
        return False
    wall_clock = as_of if as_of is not None else datetime.now(UTC)
    return await scope_grant_service.has_grant_for_delegate(
        delegate_public_id=delegate_public_id,
        wallet_public_id=wallet_public_id,
        instrument_public_id=instrument_public_id,
        as_of=wall_clock,
    )


async def enforce_orders_events_scope(
    *,
    topic: str,
    connection_principal: AuthPrincipal | None,
    payload: Mapping[str, Any],
    scope_grant_service: ScopeGrantService,
    as_of: datetime | None = None,
    accessible_wallets_cache: OrdersEventsAccessCache | None = None,
) -> bool:
    """Return ``True`` iff principal may receive this ``orders.events.*`` frame.

    Mirrors the REST ``/api/orders`` wallet-scope filter so a VIEWER /
    OPERATOR / AI_DELEGATE on the WebSocket sees the same wallet set
    via live deltas that they would see via the REST snapshot — closing
    the v0.7.0 RBAC asymmetry.

    Pass-through rules:

    - Non-``orders.events.*`` topics return ``True`` immediately
      (every other category has its own RBAC at subscribe-time + any
      wallet narrowing already applied; this filter only owns the
      ``orders.events.*`` family).
    - Missing principal (e.g. WS pre-auth or auth dropped) returns
      ``False`` — no frame leaks to an un-authenticated socket.
    - ADMIN role bypass — returns ``True`` without a service call;
      ADMIN sees every wallet by contract (mirrors REST ADMIN bypass).
    - Missing or non-string ``wallet_public_id`` in the payload
      returns ``False``: the bridge's fail-closed parsing layer should
      already drop these before reaching the filter, but this is the
      belt-and-braces guard so a stale call site cannot leak.

    Args:
        topic: The full ZMQ topic string (e.g.
            ``orders.events.kraken.BTC-USD.executed``).
        connection_principal: The authenticated principal for the
            destination socket; ``None`` when the WS hasn't yet
            authenticated.
        payload: Already-parsed JSON envelope. The filter consults
            ``wallet_public_id``.
        scope_grant_service: Singleton owner of the scope-grant
            check; bridge passes the lifespan-attached instance.
        as_of: Wall-clock for SCD2-active filtering on memberships
            and grants. Defaults to ``datetime.now(UTC)``.
        accessible_wallets_cache: Optional frame-local cache keyed by
            principal authorization identity. Bridge fan-out passes one
            cache per ZMQ frame so repeated subscriptions for the same
            principal reuse a single wallet-scope query without carrying
            access state across frames.

    Returns:
        ``True`` to forward the frame, ``False`` to drop it.
    """
    if not topic.startswith(ORDERS_EVENTS_TOPIC_PREFIX):
        return True
    if connection_principal is None:
        return False
    if connection_principal.role == UserRole.ADMIN:
        return True
    wallet_public_id = payload.get("wallet_public_id")
    if not isinstance(wallet_public_id, str):
        return False
    wall_clock = as_of if as_of is not None else datetime.now(UTC)
    accessible: set[str] | frozenset[str]
    if accessible_wallets_cache is None:
        accessible = await scope_grant_service.list_accessible_wallet_public_ids(
            principal=connection_principal,
            as_of=wall_clock,
        )
    else:
        cache_key = orders_events_access_cache_key(connection_principal)
        cached_accessible = accessible_wallets_cache.get(cache_key)
        if cached_accessible is None:
            cached_accessible = frozenset(
                await scope_grant_service.list_accessible_wallet_public_ids(
                    principal=connection_principal,
                    as_of=wall_clock,
                )
            )
            accessible_wallets_cache[cache_key] = cached_accessible
        accessible = cached_accessible
    return wallet_public_id in accessible


async def enforce_alerts_scope(
    *,
    topic: str,
    connection_principal: AuthPrincipal | None,
    payload: Mapping[str, Any],
) -> bool:
    """Return ``True`` iff principal may receive this ``alerts.*`` frame.

    The alerts topic family is sharded by ``user_public_id``: each
    frame's topic encodes the recipient
    (``alerts.{user_public_id}.{alert_type}``) and the JSON payload
    repeats it on the envelope. The filter forwards iff the
    destination socket's authenticated user matches that
    ``user_public_id`` — exactly the REST scoping behaviour for
    ``/api/alerts/history`` and ``/api/alerts/{public_id}`` (each
    user reads their own alerts).

    Pass-through rules:

    - Non-``alerts.*`` topics return ``True`` immediately (every other
      category has its own RBAC at subscribe-time + per-frame scope
      filter where needed; this filter only owns the ``alerts.*``
      family).
    - Missing principal (e.g. WS pre-auth or auth dropped) returns
      ``False`` — no frame leaks to an un-authenticated socket.
    - ADMIN role bypass — returns ``True`` without inspecting the
      payload; ADMIN sees every user's alerts by contract (mirrors
      REST ADMIN bypass).
    - Missing or non-string ``user_public_id`` in the payload returns
      ``False``: the bridge's fail-closed parsing layer should already
      drop these before reaching the filter, but this is the
      belt-and-braces guard so a stale call site cannot leak.

    Args:
        topic: The full ZMQ topic string (e.g.
            ``alerts.user-1.order_fill_full``).
        connection_principal: The authenticated principal for the
            destination socket; ``None`` when the WS hasn't yet
            authenticated.
        payload: Already-parsed JSON envelope. The filter consults
            ``user_public_id``.

    Returns:
        ``True`` to forward the frame, ``False`` to drop it.
    """
    if not topic.startswith(ALERTS_TOPIC_PREFIX):
        return True
    if connection_principal is None:
        return False
    if connection_principal.role == UserRole.ADMIN:
        return True
    payload_user_pid = payload.get("user_public_id")
    if not isinstance(payload_user_pid, str):
        return False
    return payload_user_pid == connection_principal.user_public_id
