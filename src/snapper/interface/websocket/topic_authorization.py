"""Shared WebSocket topic-authorization policy.

This module owns the single answer to "may this principal HOLD this topic".
It exists because that decision has three consumers with different lifecycles:

- subscribe time (:func:`handlers.subscribe.handle_subscribe`),
- admin-bus reconciliation after an authority change
  (:meth:`auth.websocket_auth.WebSocketAuthManager._drop_unscoped_subscriptions`),
- in-place principal replacement at re-authentication.

Before this module those consumers disagreed. The reconciliation path recognised
only the three instrument-pair families that
:func:`helpers.parse_wallet_scoped_topic` decomposes, so it structurally could not
drop a ``backtest.*`` subscription — the family gated by
``principal.active_wallet_public_id``. Subscribe time enforced that gate and the
role/permission categories as well. A connection could therefore keep a topic that
the same principal would be refused if it asked for it again.

The decision here is deliberately SIDE-EFFECT FREE: it partitions topics and
returns. Registration, bridge attachment, error framing and response shaping stay
with the caller, because those differ per consumer.

Frame-level authorization is NOT part of this module. The bridge decides per
payload (order, account, alert and AI-review frames) and its operator-plane
filters must stay on the operator plane; personal read grants must not widen them.
"""

from collections.abc import Iterable
from datetime import datetime

from snapper.auth.domain.permissions import Permission
from snapper.auth.domain.permissions import has_effective_permission
from snapper.auth.schemas.principal import AuthPrincipal
from snapper.data.repository import Repository
from snapper.interface.websocket.helpers import filter_topics
from snapper.interface.websocket.helpers import get_allowed_topics_for_role
from snapper.interface.websocket.helpers import parse_wallet_scoped_topic
from snapper.interface.websocket.helpers import role_allowed_categories

__all__ = [
    "BACKTEST_PREFIX",
    "partition_authorized_topics",
]

BACKTEST_PREFIX = "backtest."


def _extract_backtest_wallet_public_id(topic: str) -> str | None:
    """Extract the wallet public id from a backtest topic.

    Returns ``None`` for the bare ``backtest.`` root or malformed
    walletless bodies.
    """
    body = topic[len(BACKTEST_PREFIX) :].removesuffix(".")
    if not body:
        return None
    wallet_public_id, _, _remainder = body.partition(".")
    return wallet_public_id or None


def _is_backtest_topic_allowed(topic: str, principal: AuthPrincipal) -> bool:
    """Return whether the principal may subscribe to a backtest topic."""
    if has_effective_permission(
        principal.role,
        principal.permissions,
        principal.permission_scope_version,
        Permission.IMPERSONATE_OPERATOR,
    ):
        return True
    active_wallet = principal.active_wallet_public_id
    if active_wallet is None:
        return False
    return _extract_backtest_wallet_public_id(topic) == active_wallet


def _enforce_backtest_wallet_scope(
    topics: list[str], principal: AuthPrincipal
) -> tuple[list[str], list[str]]:
    """Split ``backtest.*`` topics into (allowed, denied) by wallet RBAC.

    A caller granted ``IMPERSONATE_OPERATOR`` may subscribe to any
    backtest topic. Other callers may subscribe only to the topic for
    their active wallet and cannot subscribe to the bare root.
    Wallet segment is extracted from the second dotted segment (the
    topic validator has already proven it is a UUID7). Non-backtest
    topics pass through unchanged on the allowed side.

    Args:
        topics: Already-validated topics (shape-correct but scope
            unchecked).
        principal: Authenticated caller. Permissions determine global
            scope; ``principal.active_wallet_public_id`` is the only
            wallet a caller without global scope may subscribe to.

    Returns:
        Tuple of (wallet_allowed, wallet_denied) in original order.
    """
    wallet_allowed: list[str] = []
    wallet_denied: list[str] = []
    for topic in topics:
        if not topic.startswith(BACKTEST_PREFIX) or _is_backtest_topic_allowed(topic, principal):
            wallet_allowed.append(topic)
            continue
        wallet_denied.append(topic)
    return wallet_allowed, wallet_denied


async def _enforce_ai_delegate_wallet_scope(
    topics: list[str],
    principal: AuthPrincipal,
    repository: Repository | None,
    as_of: datetime,
) -> tuple[list[str], list[str]]:
    """Split wallet-scoped topics for an AI review principal.

    Fast-paths any principal without a delegate identity. For a principal
    backed by an ``ai_delegates`` row the filter:

    1. Computes the delegate's allowed ``(exchange, native_symbol)``
       pairs via ``repository.list_scope_grant_instrument_pairs`` (one
       read per call — no caching so scope changes take effect
       immediately).
    2. Decomposes every topic with :func:`parse_wallet_scoped_topic`.
    3. Passes non-wallet-scoped topics through unchanged (market,
       system, backtest, accruals, admin, and the ``signals.paper.*``
       sandbox).
    4. Allows wallet-scoped topics whose pair is in the delegate's
       set; denies everything else so they surface as
       ``topic_outside_scope`` in the response envelope.

    Raising on missing ``repository`` is intentional for the
    AI review-principal path: a principal reaching this filter
    without a live repository reference indicates a runtime wiring
    bug, and silently passing topics through would leak wallet scope.

    Args:
        topics: Already shape-validated topic list.
        principal: Authenticated caller; delegate state gates the whole filter.
        repository: Repository for the scope-grant pair projection.
            Ignored when ``delegate_public_id`` is absent and required
            when it is populated.
        as_of: Bus time for the temporal scope read.

    Returns:
        Tuple of (allowed, denied) in original input order.
    """
    if principal.delegate_public_id is None:
        return topics, []
    if repository is None:
        raise RuntimeError(
            "AI review principal reached the wallet-scope filter without a "
            "repository reference; dispatch table wiring is broken"
        )
    allowed_pairs = await repository.list_scope_grant_instrument_pairs(
        principal.operator_public_ids, as_of
    )
    allowed: list[str] = []
    denied: list[str] = []
    for topic in topics:
        pair = parse_wallet_scoped_topic(topic)
        if pair is None:
            allowed.append(topic)
            continue
        if pair in allowed_pairs:
            allowed.append(topic)
        else:
            denied.append(topic)
    return allowed, denied


async def partition_authorized_topics(
    *,
    topics: Iterable[str],
    principal: AuthPrincipal,
    repository: Repository | None,
    as_of: datetime,
) -> tuple[list[str], list[str]]:
    """Partition already shape-valid topics into (allowed, denied).

    The single authority on topic-holding eligibility. Applies, in order:

    1. ``backtest.*`` wallet scope against ``active_wallet_public_id``.
    2. AI-delegate instrument-pair scope from the operator's active grants.
    3. Role and token-permission category authorization.

    Input order is preserved within each returned list. The denied list is
    ordered category-denied first, then backtest-denied, then delegate-denied,
    matching the envelope order clients have always received from subscribe.

    Shape validation is NOT performed here — callers pass topics that already
    satisfy :func:`handlers.subscribe._validate_ws_topics`, because an invalid
    shape is a client protocol error at subscribe time and simply cannot occur
    for a topic already held in the connection registry.

    ``repository`` stays optional to match the subscribe path, where it is only
    required once the principal carries a delegate identity; the reconciliation
    callers always have one.

    Args:
        topics: Shape-valid topic strings.
        principal: The principal whose CURRENT authority decides.
        repository: Repository for the delegate scope projection.
        as_of: Bus time for the temporal scope read.

    Returns:
        Tuple of (allowed, denied).
    """
    survivors, backtest_denied = _enforce_backtest_wallet_scope(list(topics), principal)
    survivors, delegate_denied = await _enforce_ai_delegate_wallet_scope(
        survivors, principal, repository, as_of
    )
    allowed_set = set(
        get_allowed_topics_for_role(
            principal.role,
            principal.permissions,
            principal.permission_scope_version,
        )
    )
    allowed_categories = role_allowed_categories(
        principal.role,
        principal.permissions,
        principal.permission_scope_version,
    )
    allowed, category_denied = filter_topics(survivors, allowed_set, allowed_categories)
    return allowed, [*category_denied, *backtest_denied, *delegate_denied]
