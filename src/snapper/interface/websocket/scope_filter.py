"""Per-frame scope filters for WebSocket-broadcast topic families.

Lives in its own module (rather than alongside the subscribe-time
RBAC handlers) so the bridge can import it WITHOUT pulling in the
broader handlers package — that path crosses
``connection_manager`` -> ``bridge`` and would close the import loop.

Filters have no transport coupling: each takes already-resolved data
(topic + principal + parsed payload) and returns a boolean. The bridge
calls them inside ``_forward_to_clients`` per-subscription so each
client sees only the events they have scope for.

Four filters live here today:

- :func:`enforce_ai_review_scope` — gates ``ai_reviews.*`` per-AI-delegate
  by ``(wallet, instrument)`` grant.
- :func:`enforce_orders_events_scope` — gates ``orders.events.*`` per
  principal by accessible-wallet set, mirroring the REST
  ``/api/orders`` wallet-scope filter (the v0.7.0 RBAC symmetry fix).
- :func:`enforce_account_state_scope` — gates ``portfolio.accounts.*``
  invalidations by the same accessible-wallet set as the REST account page.
- :func:`enforce_alerts_scope` — gates ``alerts.*`` per principal by
  exact ``user_public_id`` match (global-scope permission bypass). Powers the web
  (WebSocket) live-refresh path so a web user only sees their own
  alert frames.
"""

import json
from collections.abc import Mapping
from collections.abc import MutableMapping
from datetime import UTC
from datetime import datetime
from typing import Any

from snapper.auth.domain.permissions import Permission
from snapper.auth.domain.permissions import has_effective_permission
from snapper.auth.schemas.principal import AuthPrincipal
from snapper.auth.scope_grant_service import ScopeGrantService
from snapper.core.json_types import JsonValue
from snapper.messaging.schemas.data import AccountStateChangedEventData
from snapper.messaging.topics.validation import _validate_portfolio_accounts_topic

__all__ = [
    "AI_REVIEWS_TOPIC_PREFIX",
    "ALERTS_TOPIC_PREFIX",
    "ORDERS_EVENTS_TOPIC_PREFIX",
    "PORTFOLIO_ACCOUNTS_TOPIC_PREFIX",
    "WalletAccessCache",
    "wallet_access_cache_key",
    "enforce_account_state_scope",
    "enforce_ai_review_scope",
    "enforce_alerts_scope",
    "enforce_orders_events_scope",
    "validate_account_state_event",
]


AI_REVIEWS_TOPIC_PREFIX = "ai_reviews."
ALERTS_TOPIC_PREFIX = "alerts."
ORDERS_EVENTS_TOPIC_PREFIX = "orders.events."
PORTFOLIO_ACCOUNTS_TOPIC_PREFIX = "portfolio.accounts."
WalletAccessCacheKey = tuple[bool, str, tuple[str, ...], str, str | None, str | None]
WalletAccessCache = MutableMapping[WalletAccessCacheKey, frozenset[str]]
"""Frame-local accessible-wallet cache keyed by authorization identity.

Both wallet-scoped topic filters reuse this shape. A fresh mapping is created
for every received ZMQ frame so access changes take effect on the next frame.
"""


def wallet_access_cache_key(principal: AuthPrincipal) -> WalletAccessCacheKey:
    """Return the authorization identity key for frame-local wallet access caching.

    Args:
        principal: Authenticated principal whose wallet-scope query
            result is being cached.

    Returns:
        Immutable identity tuple suitable for a single-frame fan-out
        cache key.
    """
    return (
        has_effective_permission(
            principal.role,
            principal.permissions,
            principal.permission_scope_version,
            Permission.IMPERSONATE_OPERATOR,
        ),
        principal.user_public_id,
        tuple(principal.operator_public_ids),
        principal.primary_operator_public_id,
        principal.active_wallet_public_id,
        principal.delegate_public_id,
    )


def validate_account_state_event(
    *,
    topic: str,
    payload: str | Mapping[str, JsonValue] | AccountStateChangedEventData,
) -> AccountStateChangedEventData | None:
    """Return a fully validated account invalidation or fail closed.

    The discriminator is checked before Pydantic validation because the model
    supplies its literal as a construction default. The topic must then satisfy
    the canonical wallet-topic contract and repeat the payload wallet exactly.

    Args:
        topic: Full received ``portfolio.accounts.{wallet_public_id}`` topic.
        payload: Raw JSON, JSON-shaped mapping, or an already typed event.

    Returns:
        The strict typed event when every frame invariant holds, otherwise
        ``None``.
    """
    raw_json: str
    if isinstance(payload, AccountStateChangedEventData):
        raw_json = payload.model_dump_json()
    elif isinstance(payload, str):
        try:
            decoded: JsonValue = json.loads(payload)
        except json.JSONDecodeError:
            return None
        if not isinstance(decoded, dict) or decoded.get("type") != "account_state_changed_event":
            return None
        raw_json = payload
    else:
        if payload.get("type") != "account_state_changed_event":
            return None
        try:
            raw_json = json.dumps(dict(payload))
        except (TypeError, ValueError):
            return None
    try:
        event = AccountStateChangedEventData.model_validate_json(raw_json)
    except ValueError:
        return None
    topic_valid, _ = _validate_portfolio_accounts_topic(topic)
    if not topic_valid:
        return None
    if topic != f"{PORTFOLIO_ACCOUNTS_TOPIC_PREFIX}{event.wallet_public_id}":
        return None
    return event


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
    payload: Mapping[str, JsonValue],
    scope_grant_service: ScopeGrantService,
    as_of: datetime | None = None,
    accessible_wallets_cache: WalletAccessCache | None = None,
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
    - ``IMPERSONATE_OPERATOR`` bypass — returns ``True`` without a service call;
      the permission grants every-wallet visibility, matching REST.
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
    return await _enforce_wallet_scope(
        topic=topic,
        topic_prefix=ORDERS_EVENTS_TOPIC_PREFIX,
        connection_principal=connection_principal,
        payload=payload,
        scope_grant_service=scope_grant_service,
        as_of=as_of,
        accessible_wallets_cache=accessible_wallets_cache,
    )


async def enforce_account_state_scope(
    *,
    topic: str,
    connection_principal: AuthPrincipal | None,
    payload: Mapping[str, JsonValue] | AccountStateChangedEventData,
    scope_grant_service: ScopeGrantService,
    as_of: datetime | None = None,
    accessible_wallets_cache: WalletAccessCache | None = None,
) -> bool:
    """Return whether a principal may receive an account invalidation frame.

    Mirrors the REST account page's accessible-wallet scope. Every account
    frame is first revalidated against the exact event schema, UUID7 topic,
    and topic-payload wallet invariant. ``IMPERSONATE_OPERATOR`` then bypasses
    only the accessible wallet lookup. The optional cache is frame-local so one ZMQ frame performs
    at most one wallet query per authorization identity while scope changes
    take effect on the next frame.

    Args:
        topic: Full ``portfolio.accounts.{wallet_public_id}`` topic.
        connection_principal: Authenticated destination principal.
        payload: Typed or JSON-shaped account invalidation envelope.
        scope_grant_service: Owner of accessible-wallet resolution.
        as_of: Optional wall-clock shared across one frame's fan-out.
        accessible_wallets_cache: Optional per-frame wallet-access cache.

    Returns:
        ``True`` to forward the invalidation, otherwise ``False``.
    """
    if not topic.startswith(PORTFOLIO_ACCOUNTS_TOPIC_PREFIX):
        return True
    event = validate_account_state_event(topic=topic, payload=payload)
    if event is None:
        return False
    return await _enforce_validated_account_state_scope(
        topic=topic,
        connection_principal=connection_principal,
        event=event,
        scope_grant_service=scope_grant_service,
        as_of=as_of,
        accessible_wallets_cache=accessible_wallets_cache,
    )


async def _enforce_validated_account_state_scope(
    *,
    topic: str,
    connection_principal: AuthPrincipal | None,
    event: AccountStateChangedEventData,
    scope_grant_service: ScopeGrantService,
    as_of: datetime | None = None,
    accessible_wallets_cache: WalletAccessCache | None = None,
) -> bool:
    """Authorize an account event whose schema and topic were already validated.

    The bridge calls this only after :func:`validate_account_state_event` has
    accepted the frame, allowing every subscriber to reuse the same strict
    typed event. Raw or otherwise untrusted callers must use
    :func:`enforce_account_state_scope`, which performs validation before this
    authorization-only step and therefore before the global wallet bypass.

    Args:
        topic: Full validated ``portfolio.accounts.{wallet_public_id}`` topic.
        connection_principal: Authenticated destination principal.
        event: Strict event returned by :func:`validate_account_state_event`.
        scope_grant_service: Owner of accessible-wallet resolution.
        as_of: Optional wall-clock shared across one frame's fan-out.
        accessible_wallets_cache: Optional per-frame wallet-access cache.

    Returns:
        ``True`` when the validated event may reach the destination.
    """
    return await _enforce_wallet_scope(
        topic=topic,
        topic_prefix=PORTFOLIO_ACCOUNTS_TOPIC_PREFIX,
        connection_principal=connection_principal,
        payload={"wallet_public_id": event.wallet_public_id},
        scope_grant_service=scope_grant_service,
        as_of=as_of,
        accessible_wallets_cache=accessible_wallets_cache,
    )


async def _enforce_wallet_scope(
    *,
    topic: str,
    topic_prefix: str,
    connection_principal: AuthPrincipal | None,
    payload: Mapping[str, JsonValue],
    scope_grant_service: ScopeGrantService,
    as_of: datetime | None,
    accessible_wallets_cache: WalletAccessCache | None,
) -> bool:
    """Apply the shared accessible-wallet gate for one topic family.

    Args:
        topic: Full received topic.
        topic_prefix: Topic family owned by the calling public filter.
        connection_principal: Authenticated destination principal.
        payload: Parsed event envelope containing ``wallet_public_id``.
        scope_grant_service: Owner of accessible-wallet resolution.
        as_of: Optional wall-clock shared across one frame's fan-out.
        accessible_wallets_cache: Optional per-frame wallet-access cache.

    Returns:
        ``True`` when the topic is unrelated or the wallet is accessible.
    """
    if not topic.startswith(topic_prefix):
        return True
    if connection_principal is None:
        return False
    if has_effective_permission(
        connection_principal.role,
        connection_principal.permissions,
        connection_principal.permission_scope_version,
        Permission.IMPERSONATE_OPERATOR,
    ):
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
        cache_key = wallet_access_cache_key(connection_principal)
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


def enforce_alerts_scope(
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
    - ``IMPERSONATE_OPERATOR`` bypass — returns ``True`` without inspecting the
      payload and matches the REST global-scope contract.
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
    if has_effective_permission(
        connection_principal.role,
        connection_principal.permissions,
        connection_principal.permission_scope_version,
        Permission.IMPERSONATE_OPERATOR,
    ):
        return True
    payload_user_pid = payload.get("user_public_id")
    if not isinstance(payload_user_pid, str):
        return False
    return payload_user_pid == connection_principal.user_public_id
