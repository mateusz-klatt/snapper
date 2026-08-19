"""OAuth scope policy for the Snapper MCP tool catalog."""

from collections.abc import Iterable

from snapper.auth.domain.permissions import Permission

SNAPPER_READ = "snapper.read"
SNAPPER_ACCOUNT_READ = "snapper.account.read"
SNAPPER_TRADE = "snapper.trade"
SNAPPER_REVIEW_WRITE = "snapper.review.write"
SNAPPER_RESEARCH_WRITE = "snapper.research.write"
OFFLINE_ACCESS = "offline_access"

VALID_OAUTH_SCOPES = frozenset(
    {
        SNAPPER_READ,
        SNAPPER_ACCOUNT_READ,
        SNAPPER_TRADE,
        SNAPPER_REVIEW_WRITE,
        SNAPPER_RESEARCH_WRITE,
        OFFLINE_ACCESS,
    }
)

TOOL_OAUTH_SCOPES: dict[str, frozenset[str]] = {
    "list_instruments": frozenset({SNAPPER_READ}),
    "get_latest_research": frozenset({SNAPPER_READ}),
    "list_orders": frozenset({SNAPPER_READ}),
    "get_order_status": frozenset({SNAPPER_READ}),
    "list_positions": frozenset({SNAPPER_READ}),
    "list_venue_account_states": frozenset({SNAPPER_READ, SNAPPER_ACCOUNT_READ}),
    "get_position_cycle": frozenset({SNAPPER_READ}),
    "get_ai_review_aftermath": frozenset({SNAPPER_READ}),
    "get_ohlcv": frozenset({SNAPPER_READ}),
    "list_recent_signals": frozenset({SNAPPER_READ}),
    "submit_manual_order": frozenset({SNAPPER_READ, SNAPPER_TRADE}),
    "cancel_order": frozenset({SNAPPER_READ, SNAPPER_TRADE}),
    "submit_ai_review_decision": frozenset({SNAPPER_READ, SNAPPER_REVIEW_WRITE}),
    "submit_market_view": frozenset({SNAPPER_READ, SNAPPER_RESEARCH_WRITE}),
}

_SCOPE_PERMISSIONS: dict[str, frozenset[Permission]] = {
    SNAPPER_READ: frozenset(
        {
            Permission.READ_MARKET_DATA,
            Permission.READ_MARKET_VIEWS,
            Permission.READ_ORDERS,
            Permission.READ_POSITIONS,
            Permission.READ_SIGNALS,
        }
    ),
    SNAPPER_ACCOUNT_READ: frozenset({Permission.READ_ACCOUNT_STATE}),
    SNAPPER_TRADE: frozenset({Permission.CREATE_ORDERS, Permission.CANCEL_ORDERS}),
    SNAPPER_REVIEW_WRITE: frozenset({Permission.SUBMIT_AI_REVIEW_DECISION}),
    SNAPPER_RESEARCH_WRITE: frozenset({Permission.SUBMIT_MARKET_VIEW}),
    OFFLINE_ACCESS: frozenset(),
}


def permissions_for_oauth_scopes(scopes: Iterable[str]) -> frozenset[Permission]:
    """Project OAuth scopes into the existing Snapper permission ceiling.

    Args:
        scopes: OAuth scopes granted to one access token.

    Returns:
        Union of internal permissions represented by those scopes.

    Raises:
        ValueError: If any scope is outside the stable OAuth catalog.
    """
    permissions: set[Permission] = set()
    for scope in scopes:
        mapped = _SCOPE_PERMISSIONS.get(scope)
        if mapped is None:
            raise ValueError(f"Unknown MCP OAuth scope: {scope}")
        permissions.update(mapped)
    return frozenset(permissions)
