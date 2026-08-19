"""Contract tests for MCP OAuth scope policy."""

import pytest

from snapper.auth.domain.permissions import Permission
from snapper.mcp.oauth.scopes import OFFLINE_ACCESS
from snapper.mcp.oauth.scopes import SNAPPER_ACCOUNT_READ
from snapper.mcp.oauth.scopes import SNAPPER_READ
from snapper.mcp.oauth.scopes import TOOL_OAUTH_SCOPES
from snapper.mcp.oauth.scopes import permissions_for_oauth_scopes
from snapper.mcp.tool_catalog import MCP_TOOL_VISIBILITY_POLICY


def test_oauth_scope_policy_covers_catalog_with_nine_base_read_tools() -> None:
    """Verify every MCP tool has one explicit OAuth scope contract.

    Given the authoritative internal tool visibility catalog,
    When OAuth scope mappings are compared and read-only entries counted,
    Then all 14 tools are covered and exactly nine require only snapper.read.
    """
    assert set(TOOL_OAUTH_SCOPES) == set(MCP_TOOL_VISIBILITY_POLICY)
    read_tools = {
        tool_name
        for tool_name, required_scopes in TOOL_OAUTH_SCOPES.items()
        if required_scopes == {SNAPPER_READ}
    }
    assert len(read_tools) == 9
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
