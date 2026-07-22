"""Multi-tenant wallet scoping for list endpoints.

Provides a single ``resolve_target_wallets`` function that every
scoped list endpoint (orders, executions, positions, signals) calls
to derive the ``wallet_public_ids`` filter from the authenticated
principal and optional ``operator_public_id`` / ``wallet_public_id``
query parameters.
Authorization rules
Principals granted ``IMPERSONATE_OPERATOR``: when neither query param
  is set, returns ``None`` (no filter — see all). When a param is set,
  narrows accordingly.
Other principals: always scoped to the wallets their operator set
  covers via ``list_accessible_wallets_for_operators``. An explicit
  ``operator_public_id`` narrows to a single operator; an explicit
  ``wallet_public_id`` narrows to a single wallet. 403 is raised
  when the caller asks about an operator or wallet outside their set.
"""

from datetime import UTC
from datetime import datetime

from fastapi import HTTPException
from fastapi import status

from snapper.auth.domain.permissions import Permission
from snapper.auth.domain.permissions import has_effective_permission
from snapper.auth.schemas.principal import AuthPrincipal
from snapper.data.repository import Repository


async def resolve_target_wallets(
    principal: AuthPrincipal,
    repo: Repository,
    operator_public_id: str | None = None,
    wallet_public_id: str | None = None,
) -> list[str] | None:
    """Derive the ``wallet_public_ids`` filter for a scoped list endpoint.

    Args:
        principal: Authenticated caller whose permissions and operator
            set determine the visibility scope.
        repo: Repository for the accessible-wallets lookup.
        operator_public_id: Optional operator scope (narrows to a
            single operator's grants). 403 if the caller asks about
            an operator outside their membership set.
        wallet_public_id: Optional wallet scope (narrows to a single
            wallet). 403 if the wallet is not in the accessible set.

    Returns:
        ``None`` when no wallet scoping should be applied (a global-scope
        caller with no explicit params). A ``list[str]`` of wallet IDs otherwise,
        which may be empty when the caller has no accessible wallets
        (resulting in an empty query result).

    Raises:
        HTTPException: 403 when the caller requests an operator or
            wallet outside their accessible set.
    """
    now = datetime.now(UTC)
    has_global_scope = has_effective_permission(
        principal.role,
        principal.permissions,
        principal.permission_scope_version,
        Permission.IMPERSONATE_OPERATOR,
    )

    if operator_public_id is not None:
        if not has_global_scope and operator_public_id not in principal.operator_public_ids:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Operator not in accessible set",
            )
        op_ids = [operator_public_id]
    elif has_global_scope:
        if wallet_public_id is not None:
            return [wallet_public_id]
        return None
    else:
        op_ids = principal.operator_public_ids

    accessible = await repo.list_accessible_wallets_for_operators(op_ids, now)
    accessible_ids = [row["public_id"] for row in accessible]

    if wallet_public_id is not None:
        if wallet_public_id not in accessible_ids:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Wallet not in accessible set",
            )
        return [wallet_public_id]

    return accessible_ids
