"""Current owner and operator authority for caller-owned OAuth transactions."""

from datetime import UTC
from datetime import datetime

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from snapper.auth.domain.permissions import Permission
from snapper.auth.domain.permissions import get_effective_permissions
from snapper.auth.domain.roles import UserRole
from snapper.auth.schemas.principal import AuthPrincipal
from snapper.auth.tokens import PERMISSION_SCOPE_VERSION
from snapper.data.models import User
from snapper.data.models import UserOperatorMembership
from snapper.data.repository import where_active_now


async def lock_oauth_owner(
    session: AsyncSession,
    owner: AuthPrincipal,
    required: frozenset[Permission],
) -> AuthPrincipal:
    """Lock current authority and recheck its temporal interval after any lock wait.

    Args:
        session: Caller-owned transaction with its isolation already selected.
        owner: Authenticated caller whose existing token can only narrow authority.
        required: Scope permissions that this owner is being asked to delegate.

    Returns:
        Fresh owner identity with token and current-role permission intersection.

    Raises:
        PermissionError: If identity, activity, or current authority is insufficient.
    """
    result = await session.execute(
        select(User)
        .where(
            User.public_id == owner.user_public_id,
            *where_active_now(User),
        )
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    row = result.scalar_one_or_none()
    if row is None or row.known_to <= datetime.now(UTC) or not row.is_active or not owner.is_active:
        raise PermissionError("OAuth owner must be a current active user")
    try:
        live_role = UserRole(row.role)
    except ValueError as error:
        raise PermissionError("OAuth owner role is not recognized") from error
    permissions = get_effective_permissions(
        owner.role, owner.permissions, owner.permission_scope_version
    ) & get_effective_permissions(live_role)
    needed = required | {Permission.MANAGE_AI_INTEGRATION}
    if not needed <= permissions:
        raise PermissionError("OAuth owner lacks current delegation authority")
    return owner.model_copy(
        update={
            "username": row.username,
            "role": live_role,
            "permissions": sorted(permission.value for permission in permissions),
            "permission_scope_version": PERMISSION_SCOPE_VERSION,
        }
    )


async def lock_oauth_operator(
    session: AsyncSession,
    owner: AuthPrincipal,
    explicit_operator: str | None,
) -> str:
    """Require live membership or the current named impersonation exception.

    Args:
        session: Same transaction holding the current owner row lock.
        owner: Owner already clamped to authenticated and live permissions.
        explicit_operator: Optional explicitly requested operator identity.

    Returns:
        The exact operator identity this owner currently has authority to bind.

    Raises:
        PermissionError: If the selected operator lacks current authority.
    """
    operator = explicit_operator or owner.primary_operator_public_id
    if not operator:
        raise PermissionError("OAuth requires an explicit or primary operator")
    permissions = get_effective_permissions(
        owner.role, owner.permissions, owner.permission_scope_version
    )
    if explicit_operator and Permission.IMPERSONATE_OPERATOR in permissions:
        return operator
    result = await session.execute(
        select(UserOperatorMembership)
        .where(
            UserOperatorMembership.user_public_id == owner.user_public_id,
            UserOperatorMembership.operator_public_id == operator,
            *where_active_now(UserOperatorMembership),
        )
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    row = result.scalar_one_or_none()
    token_membership = owner.operator_membership_public_ids.get(operator)
    if (
        row is None
        or row.known_to <= datetime.now(UTC)
        or operator not in owner.operator_public_ids
        or (token_membership is not None and token_membership != row.public_id)
    ):
        raise PermissionError("OAuth owner lacks the current operator membership")
    return operator
