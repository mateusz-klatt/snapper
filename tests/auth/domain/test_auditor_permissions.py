"""Authorization contracts for global read-only administrative sessions."""

import pytest

from snapper.auth.domain.permissions import RESOURCE_PERMISSIONS
from snapper.auth.domain.permissions import ROLE_PERMISSIONS
from snapper.auth.domain.permissions import Permission
from snapper.auth.domain.permissions import get_effective_permissions
from snapper.auth.domain.permissions import get_role_allowed_categories
from snapper.auth.domain.permissions import has_global_read_permission
from snapper.auth.domain.roles import UserRole


def test_auditor_has_every_admin_read_and_no_mutation_permission() -> None:
    """A future permission cannot silently give the auditor write authority."""
    admin_reads = {
        permission
        for permission in ROLE_PERMISSIONS[UserRole.ADMIN]
        if permission.value.startswith("read:")
    }
    assert ROLE_PERMISSIONS[UserRole.AUDITOR] == admin_reads
    assert {
        Permission.MANAGE_SELF_ACCOUNT,
        Permission.IMPERSONATE_OPERATOR,
        Permission.INITIALIZE_PNL_ANCHORS,
        Permission.CREATE_ORDERS,
        Permission.CONFIGURE_SYSTEM,
        Permission.MANAGE_USERS,
    }.isdisjoint(ROLE_PERMISSIONS[UserRole.AUDITOR])


@pytest.mark.parametrize("resource", sorted(RESOURCE_PERMISSIONS))
def test_auditor_can_open_every_authenticated_resource(resource: str) -> None:
    """All application screens remain accessible with only read capabilities."""
    required = RESOURCE_PERMISSIONS[resource]
    assert not required or required & ROLE_PERMISSIONS[UserRole.AUDITOR]


@pytest.mark.parametrize("scope_version", [None, 1, 2, 3, 4])
def test_auditor_cannot_claim_writes_under_any_scope_version(
    scope_version: int | None,
) -> None:
    """Neither forged permission strings nor legacy migration cross the ceiling."""
    effective = get_effective_permissions(
        UserRole.AUDITOR,
        [permission.value for permission in Permission],
        scope_version,
    )
    assert effective == ROLE_PERMISSIONS[UserRole.AUDITOR]


@pytest.mark.parametrize("scope_version", [None, 1, 2, 3, 4])
def test_auditor_global_scope_is_removable_even_in_legacy_shaped_tokens(
    scope_version: int | None,
) -> None:
    """Observation scope is explicitly downscopable and never a structural grant."""
    assert not has_global_read_permission(UserRole.AUDITOR, [], scope_version)
    assert has_global_read_permission(
        UserRole.AUDITOR, [Permission.READ_ALL_SCOPES.value], scope_version
    )
    assert get_effective_permissions(UserRole.AUDITOR, [], scope_version) == set()


def test_claimless_auditor_gets_global_read_without_impersonation() -> None:
    """A normal newly created auditor observes all wallets without trade bypass."""
    assert has_global_read_permission(UserRole.AUDITOR)
    assert Permission.IMPERSONATE_OPERATOR not in get_effective_permissions(UserRole.AUDITOR)


@pytest.mark.parametrize("role", [UserRole.VIEWER, UserRole.OPERATOR, UserRole.AI_DELEGATE])
def test_global_read_claim_is_bounded_by_role(role: UserRole) -> None:
    """Adding a raw global permission string cannot expand scoped sessions."""
    assert not has_global_read_permission(role, [Permission.READ_ALL_SCOPES.value], 4)


def test_administrator_keeps_historical_structural_read_scope() -> None:
    """An existing narrowed administrator retains its established global scope."""
    assert has_global_read_permission(UserRole.ADMIN, [], 4)


def test_auditor_receives_live_read_events_without_command_categories() -> None:
    """Read subscription alternatives expose administrative events only."""
    categories = get_role_allowed_categories(UserRole.AUDITOR)
    assert {"admin", "processes_admin", "ai_reviews", "trade_events", "account_state"} <= categories
    assert {"trade", "strategy", "ai_research"}.isdisjoint(categories)
    assert get_role_allowed_categories(UserRole.AUDITOR, [], 4) == set()


@pytest.mark.parametrize(
    ("category", "permission"),
    [
        ("admin", Permission.READ_USERS),
        ("admin", Permission.READ_DESK_MEMBERSHIPS),
        ("admin", Permission.READ_SCOPE_GRANTS),
        ("admin", Permission.READ_ALL_SCOPES),
        ("processes_admin", Permission.READ_PROCESSES),
        ("processes_admin", Permission.READ_ALL_SCOPES),
        ("ai_reviews", Permission.READ_AI_REVIEWS),
        ("ai_reviews", Permission.READ_ALL_SCOPES),
    ],
)
def test_global_read_events_require_every_content_and_scope_capability(
    category: str, permission: Permission
) -> None:
    """Removing one mixed-stream content or scope grant must block subscription."""
    permissions = [item.value for item in ROLE_PERMISSIONS[UserRole.AUDITOR] if item != permission]
    assert category not in get_role_allowed_categories(UserRole.AUDITOR, permissions, 4)


@pytest.mark.parametrize("scope_version", [None, 1, 2, 3])
@pytest.mark.parametrize("role", [role for role in UserRole if role is not UserRole.AUDITOR])
def test_old_sessions_preserve_previously_authenticated_self_service(
    role: UserRole, scope_version: int | None
) -> None:
    """New self-service gates must not disable previously valid old sessions."""
    effective = get_effective_permissions(role, [Permission.READ_MARKET_DATA.value], scope_version)
    assert Permission.MANAGE_SELF_ACCOUNT in effective
    assert Permission.INITIALIZE_PNL_ANCHORS not in effective


@pytest.mark.parametrize("scope_version", [None, 1, 2, 3])
@pytest.mark.parametrize("role", [role for role in UserRole if role is not UserRole.AUDITOR])
def test_old_sessions_preserve_authenticated_scope_grant_reads(
    role: UserRole, scope_version: int | None
) -> None:
    """Previously authenticated-only reads survive an explicit old narrow token."""
    effective = get_effective_permissions(role, [], scope_version)
    assert Permission.READ_SCOPE_GRANTS in effective


@pytest.mark.parametrize("scope_version", [None, 1, 2, 3, 4])
def test_auditor_global_scope_does_not_imply_scope_grant_content(
    scope_version: int | None,
) -> None:
    """The new role has no historical content grant, regardless of token shape."""
    effective = get_effective_permissions(
        UserRole.AUDITOR, [Permission.READ_ALL_SCOPES.value], scope_version
    )
    assert effective == {Permission.READ_ALL_SCOPES}


@pytest.mark.parametrize("role", list(UserRole))
def test_current_scope_grant_reads_are_downscopable(role: UserRole) -> None:
    """Every role can omit scope-grant content from a current explicit token."""
    assert Permission.READ_SCOPE_GRANTS in ROLE_PERMISSIONS[role]
    assert Permission.READ_SCOPE_GRANTS not in get_effective_permissions(role, [], 4)


@pytest.mark.parametrize(
    "role",
    [UserRole.VIEWER, UserRole.AI_RESEARCHER, UserRole.AI_REVIEWER, UserRole.AI_DELEGATE],
)
def test_scope_grant_read_does_not_expose_admin_navigation(role: UserRole) -> None:
    """Preserving the old API read does not add administrative screens to a role."""
    assert RESOURCE_PERMISSIONS["admin"].isdisjoint(ROLE_PERMISSIONS[role])


def test_legacy_scope_grant_projection_obeys_the_role_ceiling(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Compatibility cannot restore a capability removed from the canonical role."""
    monkeypatch.setitem(
        ROLE_PERMISSIONS,
        UserRole.VIEWER,
        ROLE_PERMISSIONS[UserRole.VIEWER] - {Permission.READ_SCOPE_GRANTS},
    )
    assert Permission.READ_SCOPE_GRANTS not in get_effective_permissions(UserRole.VIEWER, [], 3)


@pytest.mark.parametrize("scope_version", [None, 1, 2, 3])
@pytest.mark.parametrize(
    "role",
    [
        UserRole.VIEWER,
        UserRole.OPERATOR,
        UserRole.ADMIN,
        UserRole.AI_REVIEWER,
        UserRole.AI_DELEGATE,
    ],
)
def test_old_position_readers_keep_anchor_initialization(
    role: UserRole, scope_version: int | None
) -> None:
    """The new anchor capability preserves old readers' existing behavior."""
    effective = get_effective_permissions(role, [Permission.READ_POSITIONS.value], scope_version)
    assert Permission.INITIALIZE_PNL_ANCHORS in effective


@pytest.mark.parametrize("role", list(UserRole))
def test_current_narrow_tokens_do_not_inherit_new_write_capabilities(role: UserRole) -> None:
    """Explicit version-four scopes can remove self-service and anchor writes."""
    effective = get_effective_permissions(role, [Permission.READ_POSITIONS.value], 4)
    assert {
        Permission.MANAGE_SELF_ACCOUNT,
        Permission.INITIALIZE_PNL_ANCHORS,
    }.isdisjoint(effective)


def test_version_two_shape_does_not_upgrade_the_new_auditor_role() -> None:
    """A role introduced in version four has no legacy full-role expansion."""
    assert get_effective_permissions(UserRole.AUDITOR, [Permission.READ_POSITIONS.value], 2) == {
        Permission.READ_POSITIONS
    }
