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
    """A future permission cannot silently give the auditor write authority.

    Given: The canonical administrator and auditor permission sets.
    When: The administrator's read capabilities are selected.
    Then: The auditor has exactly those reads and no mutation capabilities.
    """
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
    """All application screens remain accessible with only read capabilities.

    Given: An authenticated resource and the canonical auditor permissions.
    When: Its permission requirements are compared with the auditor grant.
    Then: The resource is public to authenticated users or permits an auditor read.
    """
    required = RESOURCE_PERMISSIONS[resource]
    assert not required or required & ROLE_PERMISSIONS[UserRole.AUDITOR]


@pytest.mark.parametrize("scope_version", [None, 1, 2, 3, 4])
def test_auditor_cannot_claim_writes_under_any_scope_version(
    scope_version: int | None,
) -> None:
    """Neither forged permission strings nor legacy migration cross the ceiling.

    Given: An auditor token claiming every permission under a supported version.
    When: Its effective permissions are resolved against the canonical role.
    Then: Only the auditor's read capabilities remain effective.
    """
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
    """Observation scope is explicitly downscopable and never a structural grant.

    Given: Auditor tokens with empty or explicitly global permission claims.
    When: Their global scope is resolved under each supported scope version.
    Then: Only the explicit global claim grants observation across all scopes.
    """
    assert not has_global_read_permission(UserRole.AUDITOR, [], scope_version)
    assert has_global_read_permission(
        UserRole.AUDITOR, [Permission.READ_ALL_SCOPES.value], scope_version
    )
    assert get_effective_permissions(UserRole.AUDITOR, [], scope_version) == set()


def test_claimless_auditor_gets_global_read_without_impersonation() -> None:
    """A normal newly created auditor observes all wallets without trade bypass.

    Given: An auditor whose token has no explicit permission claim.
    When: The canonical permissions and global read authority are resolved.
    Then: Global observation is granted without operator impersonation.
    """
    assert has_global_read_permission(UserRole.AUDITOR)
    assert Permission.IMPERSONATE_OPERATOR not in get_effective_permissions(UserRole.AUDITOR)


@pytest.mark.parametrize("role", [UserRole.VIEWER, UserRole.OPERATOR, UserRole.AI_DELEGATE])
def test_global_read_claim_is_bounded_by_role(role: UserRole) -> None:
    """Adding a raw global permission string cannot expand scoped sessions.

    Given: A scoped role with a token claiming global read access.
    When: The effective global read authority is checked.
    Then: The canonical role ceiling rejects the additional scope capability.
    """
    assert not has_global_read_permission(role, [Permission.READ_ALL_SCOPES.value], 4)


def test_administrator_keeps_historical_structural_read_scope() -> None:
    """An existing narrowed administrator retains its established global scope.

    Given: An administrator token with an empty current permission claim.
    When: Its effective global read authority is checked.
    Then: The historical structural operator capability preserves global reads.
    """
    assert has_global_read_permission(UserRole.ADMIN, [], 4)


def test_auditor_receives_live_read_events_without_command_categories() -> None:
    """Read subscription alternatives expose administrative events only.

    Given: A full auditor grant and an explicitly empty auditor grant.
    When: Their allowed WebSocket categories are resolved.
    Then: The full grant receives read events without command categories.
    """
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
    """Removing one mixed-stream content or scope grant must block subscription.

    Given: An auditor grant missing one required content or scope permission.
    When: Its allowed WebSocket categories are resolved.
    Then: The affected administrative event category is denied.
    """
    permissions = [item.value for item in ROLE_PERMISSIONS[UserRole.AUDITOR] if item != permission]
    assert category not in get_role_allowed_categories(UserRole.AUDITOR, permissions, 4)


@pytest.mark.parametrize("scope_version", [None, 1, 2, 3])
@pytest.mark.parametrize("role", [role for role in UserRole if role is not UserRole.AUDITOR])
def test_old_sessions_preserve_previously_authenticated_self_service(
    role: UserRole, scope_version: int | None
) -> None:
    """New self-service gates must not disable previously valid old sessions.

    Given: A historical role with a narrowed pre-version-four market-read token.
    When: Its effective permissions are projected into the current catalog.
    Then: Self-service survives without adding position-anchor initialization.
    """
    effective = get_effective_permissions(role, [Permission.READ_MARKET_DATA.value], scope_version)
    assert Permission.MANAGE_SELF_ACCOUNT in effective
    assert Permission.INITIALIZE_PNL_ANCHORS not in effective


@pytest.mark.parametrize("scope_version", [None, 1, 2, 3])
@pytest.mark.parametrize("role", [role for role in UserRole if role is not UserRole.AUDITOR])
def test_old_sessions_preserve_authenticated_scope_grant_reads(
    role: UserRole, scope_version: int | None
) -> None:
    """Previously authenticated-only reads survive an explicit old narrow token.

    Given: A historical role with an empty pre-version-four permission claim.
    When: Its effective permissions are projected into the current catalog.
    Then: The previously authenticated scope-grant read remains available.
    """
    effective = get_effective_permissions(role, [], scope_version)
    assert Permission.READ_SCOPE_GRANTS in effective


@pytest.mark.parametrize("scope_version", [None, 1, 2, 3, 4])
def test_auditor_global_scope_does_not_imply_scope_grant_content(
    scope_version: int | None,
) -> None:
    """The new role has no historical content grant, regardless of token shape.

    Given: An auditor token carrying global scope without content permissions.
    When: Its effective permissions are resolved under a supported version.
    Then: No scope-grant content permission is synthesized.
    """
    effective = get_effective_permissions(
        UserRole.AUDITOR, [Permission.READ_ALL_SCOPES.value], scope_version
    )
    assert effective == {Permission.READ_ALL_SCOPES}


@pytest.mark.parametrize("role", list(UserRole))
def test_current_scope_grant_reads_are_downscopable(role: UserRole) -> None:
    """Every role can omit scope-grant content from a current explicit token.

    Given: A role whose canonical set includes scope-grant reads.
    When: Its current token carries an explicitly empty permission claim.
    Then: Scope-grant content remains outside the effective permissions.
    """
    assert Permission.READ_SCOPE_GRANTS in ROLE_PERMISSIONS[role]
    assert Permission.READ_SCOPE_GRANTS not in get_effective_permissions(role, [], 4)


@pytest.mark.parametrize(
    "role",
    [UserRole.VIEWER, UserRole.AI_RESEARCHER, UserRole.AI_REVIEWER, UserRole.AI_DELEGATE],
)
def test_scope_grant_read_does_not_expose_admin_navigation(role: UserRole) -> None:
    """Preserving the old API read does not add administrative screens to a role.

    Given: A scoped read-only or AI role retaining historical scope-grant reads.
    When: Its canonical permissions are compared with admin navigation requirements.
    Then: None of those requirements expose the administrative screens.
    """
    assert RESOURCE_PERMISSIONS["admin"].isdisjoint(ROLE_PERMISSIONS[role])


def test_legacy_scope_grant_projection_obeys_the_role_ceiling(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Compatibility cannot restore a capability removed from the canonical role.

    Given: A viewer role with scope-grant reads removed from its canonical set.
    When: An empty legacy token is projected into the current catalog.
    Then: Compatibility does not restore the removed read capability.
    """
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
    """The new anchor capability preserves old readers' existing behavior.

    Given: A historical role holding position reads in a pre-version-four token.
    When: Its effective permissions are projected into the current catalog.
    Then: The existing ability to initialize position anchors is retained.
    """
    effective = get_effective_permissions(role, [Permission.READ_POSITIONS.value], scope_version)
    assert Permission.INITIALIZE_PNL_ANCHORS in effective


@pytest.mark.parametrize("role", list(UserRole))
def test_current_narrow_tokens_do_not_inherit_new_write_capabilities(role: UserRole) -> None:
    """Explicit version-four scopes can remove self-service and anchor writes.

    Given: A current token claiming position reads alone.
    When: Its effective permissions are resolved against the canonical role.
    Then: Neither self-service nor anchor mutation authority is inferred.
    """
    effective = get_effective_permissions(role, [Permission.READ_POSITIONS.value], 4)
    assert {
        Permission.MANAGE_SELF_ACCOUNT,
        Permission.INITIALIZE_PNL_ANCHORS,
    }.isdisjoint(effective)


def test_version_two_shape_does_not_upgrade_the_new_auditor_role() -> None:
    """A role introduced in version four has no legacy full-role expansion.

    Given: An auditor token with version-two shape claiming position reads alone.
    When: Its effective permissions are projected into the current catalog.
    Then: No historical full-role expansion broadens its explicit grant.
    """
    assert get_effective_permissions(UserRole.AUDITOR, [Permission.READ_POSITIONS.value], 2) == {
        Permission.READ_POSITIONS
    }
