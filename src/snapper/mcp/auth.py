"""MCP wallet-scope re-validation helper.

The bearer-auth middleware verifies that an MCP caller holds
a valid JWT, and the DB-backed ``verify_token_with_db`` path
guarantees the token has not been revoked or the user deactivated.
Neither check, however, covers the narrower question a write tool
must answer on every call
    *Does this caller still have an active wallet scope grant
    covering the wallet they just referenced in this tool
    invocation?*
Token claims are snapshotted at login time; the enclosing scope
grant row can be revoked at any later moment by an admin without
invalidating the token itself. The plan's WS design addresses
this for subscriptions via the ``admin.scope_revoked`` subscriber
's synchronous MCP tool dispatch needs an equivalent
per-call gate.
func:`validate_user_wallet_scope` is that gate. It is a thin
adapter over
meth:`snapper.data.repository.Repository.list_accessible_wallets_for_operators`
so cross-surface policy stays in one place (the REST list
endpoints go through :func:`snapper.server.scoping.resolve_target_wallets`
which consults the same primitive). Keeping the MCP helper
*separate* from the REST scoping function is intentional: the REST
variant raises :class:`HTTPException` whose semantics are
REST-specific (status codes, detail strings, OpenAPI responses)
the MCP variant raises :class:`PermissionError` which FastMCP
surfaces as a structured tool error.
"""

from datetime import UTC
from datetime import datetime

from snapper.auth.domain.roles import UserRole
from snapper.auth.schemas.tokens import TokenClaims
from snapper.data.repository import Repository

WALLET_SCOPE_ERROR_CODE: str = "wallet_out_of_scope"
"""Error code returned when the caller cannot act on the wallet.

Mirrors the REST 403 ``detail`` phrasing but surfaces as an
``error_code`` on the MCP tool error envelope so clients can
distinguish scope rejections from caps rejections, token issues, or
feature-flag toggles. Enumerated in ``docs/ai-integration.md``.
"""

OPERATOR_SCOPE_ERROR_CODE: str = "operator_out_of_scope"
"""Error code returned when the caller-supplied operator is not in their claims.

MCP write tools accept an optional ``operator_public_id`` so a
caller who belongs to multiple operators can pick which one to act
AS. The selection MUST be inside the authenticated operator set;
otherwise the write would be attributed to an operator the caller
cannot act for. Enumerated in ``docs/ai-integration.md``.
"""


def ensure_operator_in_claims(
    claims: TokenClaims,
    operator_public_id: str | None,
) -> None:
    """Reject when the caller picks an operator outside their authenticated set.

    ADMIN bypass mirrors the wallet gate: a role that implicitly
    covers every operator does not need this check. All other roles
    must select one of the operators listed on their JWT claims. A
    ``None`` selection is admitted because the caller defers to
    :attr:`TokenClaims.primary_operator_public_id`, which the tool
    plugs in downstream; the claim set is the source of truth for
    that fallback so no extra validation is required.

    Wiring pairs with :func:`validate_user_wallet_scope` so the two
    checks together cover the `(operator, wallet)` tuple any MCP
    write tool stamps onto the persisted row: the wallet gate
    proves the wallet sits inside *some* operator the caller holds,
    and this helper proves the caller actually chose *that* same
    operator instead of spoofing a peer.

    Args:
        claims: Verified :class:`TokenClaims` for the current call.
        operator_public_id: Caller-supplied operator selection.
            ``None`` means "fall back to the primary operator" and
            is always admitted.

    Raises:
        PermissionError: when a non-ADMIN caller picks an operator
            outside :attr:`TokenClaims.operator_public_ids`. The
            message starts with :data:`OPERATOR_SCOPE_ERROR_CODE`
            so FastMCP tool errors carry a stable classifier.
    """
    if operator_public_id is None:
        return
    if claims.role == UserRole.ADMIN:
        return
    if operator_public_id in claims.operator_public_ids:
        return
    raise PermissionError(
        f"{OPERATOR_SCOPE_ERROR_CODE}: operator '{operator_public_id}' is not "
        f"in the caller's authenticated operator set."
    )


async def validate_user_wallet_scope(
    claims: TokenClaims,
    wallet_public_id: str,
    repository: Repository,
    *,
    as_of: datetime | None = None,
) -> None:
    """Reject the current MCP call when the caller cannot act on the wallet.

    Wallet access is derived — not stored on :class:`TokenClaims` —
    so a grant revoked after token issue immediately blocks the next
    tool call even with a still-valid JWT. The derivation walks the
    caller's :attr:`TokenClaims.operator_public_ids` set and asks
    the repository for the union of wallets covered by any of those
    operators at ``as_of``. A wallet outside that union is
    rejected.

    ADMIN bypass: a caller whose role is :attr:`UserRole.ADMIN`
    implicitly covers every wallet, matching the REST
    ``resolve_target_wallets`` behaviour. All other roles
    (AI_DELEGATE included) go through the repository lookup.

    A caller with *no* operator memberships (``operator_public_ids``
    is empty) is rejected without hitting the repository — the
    repository primitive short-circuits on an empty list, but
    pre-checking here keeps the error message specific.

    Args:
        claims: Verified :class:`TokenClaims` for the current call.
        wallet_public_id: UUID7 of the wallet the tool is about to
            touch. Non-empty strings only; callers normalize empty
            strings to a wallet-less code path before getting here.
        repository: Shared :class:`Repository` singleton resolved by
            the tool via its ``repository_getter``.
        as_of: Optional bus time for the temporal lookup. Defaults
            to ``datetime.now(UTC)``. Tests override this to pin a
            deterministic instant.

    Raises:
        PermissionError: when the caller is not ADMIN and the
            wallet is not in the operator-accessible set. The
            message includes :data:`WALLET_SCOPE_ERROR_CODE` so
            FastMCP-surfaced errors carry a stable classifier.
    """
    if claims.role == UserRole.ADMIN:
        return
    if not claims.operator_public_ids:
        raise PermissionError(
            f"{WALLET_SCOPE_ERROR_CODE}: caller has no operator memberships; "
            f"wallet '{wallet_public_id}' is out of scope."
        )
    lookup_time = as_of or datetime.now(UTC)
    accessible_rows = await repository.list_accessible_wallets_for_operators(
        list(claims.operator_public_ids),
        lookup_time,
    )
    accessible_ids = {row["public_id"] for row in accessible_rows}
    if wallet_public_id not in accessible_ids:
        raise PermissionError(
            f"{WALLET_SCOPE_ERROR_CODE}: wallet '{wallet_public_id}' is not in "
            f"the caller's accessible scope set."
        )
