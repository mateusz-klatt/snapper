"""Contract tests for MCP OAuth scope policy and first-release ceiling."""

from collections.abc import Callable
from collections.abc import Iterable

import pytest

import snapper.mcp.oauth.scopes as oauth_scopes
from snapper.auth.domain.permissions import Permission
from snapper.mcp.oauth.scopes import OFFLINE_ACCESS
from snapper.mcp.oauth.scopes import SNAPPER_ACCOUNT_READ
from snapper.mcp.oauth.scopes import SNAPPER_READ
from snapper.mcp.oauth.scopes import SNAPPER_RESEARCH_WRITE
from snapper.mcp.oauth.scopes import SNAPPER_REVIEW_WRITE
from snapper.mcp.oauth.scopes import SNAPPER_TRADE
from snapper.mcp.oauth.scopes import TOOL_OAUTH_SCOPES
from snapper.mcp.oauth.scopes import VALID_OAUTH_SCOPES
from snapper.mcp.oauth.scopes import permissions_for_oauth_scopes
from snapper.mcp.tool_catalog import MCP_TOOL_VISIBILITY_POLICY

type _ScopeValidator = Callable[..., tuple[str, ...]]


def _require_validator() -> _ScopeValidator:
    """Resolve the first-release OAuth scope ceiling validator."""
    validator: _ScopeValidator | None = getattr(
        oauth_scopes,
        "validate_initial_oauth_scopes",
        None,
    )
    assert validator is not None, "validate_initial_oauth_scopes missing in oauth.scopes"
    return validator


def test_oauth_scope_policy_covers_catalog_with_ten_base_read_tools() -> None:
    """Verify every MCP tool has one explicit OAuth scope contract.

    Given the authoritative internal tool visibility catalog,
    When OAuth scope mappings are compared and read-only entries counted,
    Then all 15 tools are covered and exactly ten require only snapper.read.
    """
    assert set(TOOL_OAUTH_SCOPES) == set(MCP_TOOL_VISIBILITY_POLICY)
    read_tools = {
        tool_name
        for tool_name, required_scopes in TOOL_OAUTH_SCOPES.items()
        if required_scopes == {SNAPPER_READ}
    }
    assert len(read_tools) == 10
    assert {
        "submit_manual_order",
        "cancel_order",
        "submit_ai_review_decision",
        "submit_market_view",
    }.isdisjoint(read_tools)


def test_snapper_read_projects_complete_internal_read_ceiling() -> None:
    """Verify OAuth read scope contains every internal read permission.

    Given a grant carrying snapper.read and offline_access,
    When it is projected into existing Snapper permissions,
    Then five base read permissions and no account or write permission are present.
    """
    permissions = permissions_for_oauth_scopes([SNAPPER_READ, OFFLINE_ACCESS])
    assert permissions == {
        Permission.READ_MARKET_DATA,
        Permission.READ_MARKET_VIEWS,
        Permission.READ_ORDERS,
        Permission.READ_POSITIONS,
        Permission.READ_SIGNALS,
    }
    assert Permission.READ_ACCOUNT_STATE not in permissions


def test_account_state_requires_explicit_additional_scope() -> None:
    """Verify sensitive account state is not bundled into base read scope.

    Given a grant carrying both base read and account-read scopes,
    When internal permission projection runs,
    Then account-state authority appears only with the additional scope.
    """
    permissions = permissions_for_oauth_scopes([SNAPPER_READ, SNAPPER_ACCOUNT_READ])
    assert Permission.READ_ACCOUNT_STATE in permissions


def test_unknown_oauth_scope_fails_closed() -> None:
    """Verify unknown scope names never silently map to authority.

    Given a token carrying a scope outside the stable OAuth catalog,
    When internal permission projection runs,
    Then it raises instead of dropping or granting the unknown scope.
    """
    with pytest.raises(ValueError, match="Unknown MCP OAuth scope"):
        permissions_for_oauth_scopes(["snapper.admin"])


def test_initial_oauth_ceiling_preserves_global_catalog() -> None:
    """Verify first-release ceiling is separate from the preserved global catalog.

    Given the global OAuth scope catalog and the first-release ceiling exports,
    When both sets are inspected,
    Then global scopes remain intact while first release allows only read + offline_access.
    """
    assert (
        frozenset(
            {
                SNAPPER_READ,
                SNAPPER_ACCOUNT_READ,
                SNAPPER_TRADE,
                SNAPPER_REVIEW_WRITE,
                SNAPPER_RESEARCH_WRITE,
                OFFLINE_ACCESS,
            }
        )
        == VALID_OAUTH_SCOPES
    )
    ceiling = getattr(oauth_scopes, "INITIAL_OAUTH_SCOPE_CEILING", None)
    resource_scopes = getattr(oauth_scopes, "INITIAL_OAUTH_RESOURCE_SCOPES", None)
    assert ceiling == frozenset({SNAPPER_READ, OFFLINE_ACCESS})
    assert resource_scopes == frozenset({SNAPPER_READ})


@pytest.mark.parametrize(
    "invalid_scopes",
    [
        (),
        ("",),
        (" snapper.read ",),
        ("snapper.unknown",),
        (OFFLINE_ACCESS,),
        (SNAPPER_READ, SNAPPER_ACCOUNT_READ),
        (SNAPPER_READ, SNAPPER_TRADE),
        (SNAPPER_READ, SNAPPER_REVIEW_WRITE),
        (SNAPPER_READ, SNAPPER_RESEARCH_WRITE),
    ],
)
def test_validate_initial_oauth_scopes_rejects_deferred_or_resource_less_inputs(
    invalid_scopes: Iterable[str],
) -> None:
    """Verify first-release validator refuses deferred, empty, and offline-only scopes.

    Given deferred scopes or a request without the read resource scope.
    When the first-release ceiling validates it.
    Then the request is rejected.
    """
    validator = _require_validator()
    with pytest.raises(ValueError):
        validator(invalid_scopes)


def test_validate_initial_oauth_scopes_deduplicates_and_enforces_client_ceiling() -> None:
    """Verify first-release validator preserves order and enforces client scope ceilings.

    Given repeated initial scopes and a narrower registered client.
    When scopes are validated.
    Then order is preserved while duplicates and unauthorized expansion are handled.
    """
    validator = _require_validator()
    normalized = validator(
        [SNAPPER_READ, OFFLINE_ACCESS, SNAPPER_READ],
        allowed_client_scopes=[SNAPPER_READ, OFFLINE_ACCESS],
    )
    assert normalized == (SNAPPER_READ, OFFLINE_ACCESS)
    with pytest.raises(ValueError, match="client"):
        validator(
            [SNAPPER_READ, OFFLINE_ACCESS],
            allowed_client_scopes=[SNAPPER_READ],
        )
