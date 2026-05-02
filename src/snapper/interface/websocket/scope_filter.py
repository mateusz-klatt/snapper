"""Per-frame scope filter for ``ai_reviews.*`` topics.

Lives in its own module (rather than alongside the subscribe-time
RBAC handlers) so the bridge can import it WITHOUT pulling in the
broader handlers package — that path crosses
``connection_manager`` -> ``bridge`` and would close the import loop.

The filter has no transport coupling: it takes already-resolved data
(topic + principal + parsed payload) and returns a boolean. The bridge
calls it inside ``_forward_to_clients`` per-subscription so each
delegate sees only the CONSULT events they have scope for.
"""

from collections.abc import Mapping
from datetime import UTC
from datetime import datetime
from typing import Any

from snapper.auth.schemas.principal import AuthPrincipal
from snapper.auth.scope_grant_service import ScopeGrantService

__all__ = ["AI_REVIEWS_TOPIC_PREFIX", "enforce_ai_review_scope"]


AI_REVIEWS_TOPIC_PREFIX = "ai_reviews."
"""Topic-family prefix the per-frame filter gates.

Frames whose topic does NOT start with this prefix bypass the filter
entirely (registry-root subscribed roles already passed the
subscribe-time RBAC check). Frames inside the family go through
:func:`enforce_ai_review_scope` per-subscription so AI delegates only
see CONSULT events for ``(wallet, instrument)`` tuples their scope
grant covers.
"""


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
