"""Multi-tenant wallet scoping for REST endpoints.

Exposes TWO named primitives — never one function with an ``intent``
argument — so the read plane and the trade plane can never be widened by
the same edit:

``resolve_readable_wallets``
    What the caller may SEE. Resolves through
    ``Repository.list_readable_wallets_for_user``, the UNION of the
    wallets the principal's operators hold active scope grants on and the
    wallets the USER personally holds an active
    ``wallet_user_read_grants`` row on. A principal with ZERO operator
    memberships and one read grant therefore still sees that one wallet.

``resolve_tradable_wallets``
    What the caller may ACT on. Resolves through
    ``Repository.list_accessible_wallets_for_operators`` — the operator
    scope-grant plane alone. Read grants never reach it, so a read grant
    can never authorize an order, a cancel, or a plan action.

Keeping the two apart by NAME is the whole point of the split. A single
entry point taking a boolean or an enum would mean that the day a viewer
cannot open a read page, the cheapest fix flips a flag at the call site
and silently widens every mutation route that shares the function.

Authorization rules (identical on both planes except for the lookup)
Principals granted ``IMPERSONATE_OPERATOR``: when neither query param
  is set, returns ``None`` (no filter — see all). When a param is set,
  narrows accordingly, always through the operator plane: an admin
  narrowing to one operator asks about THAT operator's grants, not about
  the admin's own personal read grants.
Other principals: scoped to their plane's wallet set. An explicit
  ``operator_public_id`` narrows the operator half of that set to a single
  operator; an explicit ``wallet_public_id`` narrows to a single wallet.
  403 is raised when the caller asks about an operator or wallet outside
  their set.

What ``operator_public_id`` narrows differs by plane, and the difference is
deliberate. On the trade plane it narrows the whole answer, because the whole
answer is operator grants. On the read plane it narrows only the
operator-covered half of the union: ``list_readable_wallets_for_user`` always
contributes the caller's personal ``wallet_user_read_grants`` rows, which
belong to the USER and hang off no operator, so there is nothing for an
operator filter to match them against. Consequences, both intended:
``?operator_public_id=X`` still returns the caller's read-granted wallets even
when operator X holds no grant on them, and
``?operator_public_id=X&wallet_public_id=W`` answers 200 for a read-granted W
outside X where the pre-split operator-only resolver answered 403. Refusing a
wallet the caller may demonstrably read is the exact failure mode this plane
exists to remove, so the union wins over the narrowing. Pinned by
``TestReadPlaneOperatorNarrowing`` in ``tests/server/test_scoping.py``.

``operator_public_id`` is a NARROWING parameter, and on the read plane it
is only ever supplied by endpoints that expose it as a query parameter.
Read routes that already know the record's wallet pass ``wallet_public_id``
alone: the wallet is the authoritative read boundary, and gating on the
record's owning operator would 403 exactly the read-granted principal the
read plane exists to serve.

Not every read surface resolves here. The WebSocket per-frame filters
(``orders.events.*``, ``portfolio.accounts.*``) still resolve through
``ScopeGrantService.list_accessible_wallet_public_ids``, which is the
OPERATOR plane alone — see that method's docstring for why the read grants
were deliberately not wired into it.

Two further exports gate the ``active_wallet_public_id`` JWT claim at
consumption time, one per plane. The claim is minted once by
``POST /auth/refresh`` and then travels on every later request, so a
surface that reads it raw is authorizing from a snapshot — against the
wrong plane, and at the wrong time.

``require_tradable_active_wallet``
    Re-resolves the claim through ``resolve_tradable_wallets``, so a
    wallet a caller may only SEE never becomes the wallet a mutation
    writes to. Declared as a dependency, never called, so deleting it
    leaves the handler with an undefined name.

``resolve_readable_active_wallet``
    Re-resolves the claim through ``resolve_readable_wallets``, so a
    revoked read grant stops the NEXT request rather than the next
    login. Called from the handler body, not declared: the read plane's
    established convention is a call (every other read surface calls
    ``resolve_readable_wallets`` inline), and the wallet-scoped backtest
    readers already sit at their pinned ``PLR0913`` argument budget, so
    adding a parameter would demand a complexity-baseline bump for a
    gate the baseline is meant to stop growing. Its regression cover is
    a test per read route, not a signature.

Both docstrings record the escalation they close.
"""

from datetime import UTC
from datetime import datetime
from typing import Annotated
from typing import Final

from fastapi import Depends
from fastapi import HTTPException
from fastapi import status

from snapper.auth.dependencies import require_authentication
from snapper.auth.domain.permissions import Permission
from snapper.auth.domain.permissions import has_effective_permission
from snapper.auth.schemas.principal import AuthPrincipal
from snapper.data.repository import Repository
from snapper.server.dependencies import get_repository_dependency

ACTIVE_WALLET_REQUIRED_DETAIL: Final[str] = "no active wallet selected"
"""Uniform 400 detail for a wallet-scoped surface reached with no claim."""


def _has_global_scope(principal: AuthPrincipal) -> bool:
    """Return whether the principal sees every operator's wallets."""
    return has_effective_permission(
        principal.role,
        principal.permissions,
        principal.permission_scope_version,
        Permission.IMPERSONATE_OPERATOR,
    )


def _resolve_operator_filter(
    principal: AuthPrincipal,
    has_global_scope: bool,
    operator_public_id: str | None,
) -> list[str] | None:
    """Return the operator ids to resolve through, or ``None`` when unscoped.

    ``None`` means the caller holds global scope and named no operator, so
    no wallet lookup is needed at all.
    """
    if operator_public_id is not None:
        if not has_global_scope and operator_public_id not in principal.operator_public_ids:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Operator not in accessible set",
            )
        return [operator_public_id]
    if has_global_scope:
        return None
    return principal.operator_public_ids


def _narrow_to_requested_wallet(accessible_ids: list[str], wallet_public_id: str) -> list[str]:
    """Return the requested wallet as a singleton, or 403 when out of scope."""
    if wallet_public_id not in accessible_ids:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Wallet not in accessible set",
        )
    return [wallet_public_id]


def _unscoped_result(wallet_public_id: str | None) -> list[str] | None:
    """Return the global-scope answer when no operator narrowing applies."""
    if wallet_public_id is not None:
        return [wallet_public_id]
    return None


async def resolve_readable_wallets(
    principal: AuthPrincipal,
    repo: Repository,
    operator_public_id: str | None = None,
    wallet_public_id: str | None = None,
) -> list[str] | None:
    """Derive the ``wallet_public_ids`` filter for a READ endpoint.

    The read plane: operator scope grants UNION the user's personal
    ``wallet_user_read_grants``. Deliberately does NOT short-circuit on an
    empty operator list — a user with no memberships and one read grant
    must still see that wallet on every read surface.

    Args:
        principal: Authenticated caller whose permissions, operator set and
            ``user_public_id`` determine the read scope.
        repo: Repository for the readable-wallets lookup.
        operator_public_id: Optional operator scope. 403 if the caller asks
            about an operator outside their membership set. It narrows the
            operator-covered half of the union ONLY: personal read grants
            are not attached to any operator and always participate, so a
            read-granted wallet stays visible under this filter (module
            docstring records why).
        wallet_public_id: Optional wallet scope (narrows to a single
            wallet). 403 if the wallet is not readable.

    Returns:
        ``None`` when no wallet scoping should be applied (a global-scope
        caller with no explicit params). A ``list[str]`` of wallet IDs
        otherwise, which may be empty when the caller can read no wallets
        (resulting in an empty query result).

    Raises:
        HTTPException: 403 when the caller requests an operator or wallet
            outside their readable set.
    """
    now = datetime.now(UTC)
    has_global_scope = _has_global_scope(principal)
    op_ids = _resolve_operator_filter(principal, has_global_scope, operator_public_id)
    if op_ids is None:
        return _unscoped_result(wallet_public_id)

    if has_global_scope:
        readable = await repo.list_accessible_wallets_for_operators(op_ids, now)
    else:
        readable = await repo.list_readable_wallets_for_user(
            principal.user_public_id,
            op_ids,
            now,
        )
    readable_ids = [row["public_id"] for row in readable]

    if wallet_public_id is not None:
        return _narrow_to_requested_wallet(readable_ids, wallet_public_id)
    return readable_ids


async def resolve_tradable_wallets(
    principal: AuthPrincipal,
    repo: Repository,
    operator_public_id: str | None = None,
    wallet_public_id: str | None = None,
) -> list[str] | None:
    """Derive the ``wallet_public_ids`` filter for a TRADE endpoint.

    The trade plane: operator scope grants ONLY. Personal read grants are
    never consulted here, so widening what a user may see can never widen
    what they may do.

    Args:
        principal: Authenticated caller whose permissions and operator set
            determine the trading scope.
        repo: Repository for the accessible-wallets lookup.
        operator_public_id: Optional operator scope (narrows to a single
            operator's grants). 403 if the caller asks about an operator
            outside their membership set.
        wallet_public_id: Optional wallet scope (narrows to a single
            wallet). 403 if the wallet is not in the accessible set.

    Returns:
        ``None`` when no wallet scoping should be applied (a global-scope
        caller with no explicit params). A ``list[str]`` of wallet IDs
        otherwise, which may be empty when the caller has no accessible
        wallets (resulting in an empty query result).

    Raises:
        HTTPException: 403 when the caller requests an operator or wallet
            outside their accessible set.
    """
    now = datetime.now(UTC)
    has_global_scope = _has_global_scope(principal)
    op_ids = _resolve_operator_filter(principal, has_global_scope, operator_public_id)
    if op_ids is None:
        return _unscoped_result(wallet_public_id)

    accessible = await repo.list_accessible_wallets_for_operators(op_ids, now)
    accessible_ids = [row["public_id"] for row in accessible]

    if wallet_public_id is not None:
        return _narrow_to_requested_wallet(accessible_ids, wallet_public_id)
    return accessible_ids


async def require_tradable_active_wallet(
    principal: Annotated[AuthPrincipal, Depends(require_authentication)],
    repo: Annotated[Repository, Depends(get_repository_dependency)],
) -> str:
    """Re-resolve the active-wallet claim through the TRADE plane.

    ``active_wallet_public_id`` is minted by ``POST /auth/refresh``
    (:func:`snapper.auth.routes._apply_wallet_hint`) after validation
    against the READ plane, and that is deliberate: a user holding only a
    personal ``wallet_user_read_grants`` row on wallet W must be able to
    pin the hint to W and receive W's read surfaces and ``backtest.*``
    frames. The claim therefore certifies VISIBILITY and nothing else.

    Every surface that derives a wallet to WRITE against from that claim
    must re-resolve it here first. The escalation this closes was
    reachable, not theoretical: ``AI_REVIEWER`` holds
    ``CREATE_BACKTEST_COMPARISONS`` and is absent from the seed loader's
    read-grant deny list, which refuses read grants only to permission
    sets holding ``CREATE_ORDERS`` or ``IMPERSONATE_OPERATOR``. A
    read-granted reviewer with zero operator memberships could therefore
    pin W and persist a ``backtest_comparisons`` row against it.

    The gate cannot move up into ``_apply_wallet_hint``: narrowing the
    mint to the trade plane deletes the read feature the split exists to
    deliver. It cannot move into an AST guard either — the mint and the
    consumption are two different requests, so no call-graph edge joins
    them, which ``scripts/check_read_visibility_boundary.py`` now records
    as an explicit non-claim. Consumption time is the one place both
    planes are in scope at once.

    Declared as a dependency that RETURNS the wallet rather than one
    listed in ``dependencies=[...]``, so a handler cannot keep reading
    the raw claim after someone deletes the gate: the name would simply
    be undefined.

    Args:
        principal: Authenticated caller carrying the wallet claim.
        repo: Repository used for the trade-plane wallet lookup.

    Returns:
        The active wallet public id, proven tradable by this principal.

    Raises:
        HTTPException: 400 when no wallet is selected, and 403 (from
            :func:`resolve_tradable_wallets`) when the selected wallet is
            outside the caller's operator scope grants.
    """
    wallet_public_id = principal.active_wallet_public_id
    if wallet_public_id is None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=ACTIVE_WALLET_REQUIRED_DETAIL,
        )
    await resolve_tradable_wallets(principal, repo, wallet_public_id=wallet_public_id)
    return wallet_public_id


async def resolve_readable_active_wallet(
    principal: AuthPrincipal,
    repo: Repository,
) -> str:
    """Re-resolve the active-wallet claim through the READ plane.

    The claim certifies visibility, so consuming it on a read surface is
    the RIGHT plane — but consuming it *as minted* is the wrong time.
    ``POST /auth/refresh`` validates the claim once, at mint; every later
    request then reads a snapshot. Refresh carries the claim forward, and
    the zero-body callers refresh forever, so without a re-resolution here
    an admin revoking a personal ``wallet_user_read_grants`` row never
    takes effect: the holder keeps listing that wallet's runs,
    comparisons, trades, signals, events and equity indefinitely.

    Every other read surface in the tree already resolves
    :func:`resolve_readable_wallets` per request and is therefore
    revocation-immediate. The wallet-scoped backtest readers were the one
    family authorizing from the claim alone, and this restores the
    property they were missing rather than adding a new one.

    Re-validating the carried claim at refresh (which
    :func:`snapper.auth.routes._apply_wallet_hint` now also does) bounds
    the staleness to one access-token lifetime; it cannot replace this
    call, because a grant revoked mid-token would still be honoured until
    the client next refreshes.

    Raising 403 rather than 404 matches what ``resolve_readable_wallets``
    already answers for an out-of-scope ``?wallet_public_id=`` on the
    other read routes. The wallet is the caller's OWN prior selection, so
    there is no cross-tenant existence to leak by naming the refusal.

    Args:
        principal: Authenticated caller carrying the wallet claim.
        repo: Repository used for the read-plane wallet lookup.

    Returns:
        The active wallet public id, proven still readable by this
        principal at this instant.

    Raises:
        HTTPException: 400 when no wallet is selected, and 403 (from
            :func:`resolve_readable_wallets`) when the selected wallet is
            no longer inside the caller's readable set.
    """
    wallet_public_id = principal.active_wallet_public_id
    if wallet_public_id is None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=ACTIVE_WALLET_REQUIRED_DETAIL,
        )
    await resolve_readable_wallets(principal, repo, wallet_public_id=wallet_public_id)
    return wallet_public_id
