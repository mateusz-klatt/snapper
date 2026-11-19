"""Project verified Snapper OAuth authority onto the MCP SDK transport."""

from collections.abc import Callable
from dataclasses import dataclass
from urllib.parse import urlsplit

from mcp.server.auth.middleware.bearer_auth import AuthenticatedUser
from mcp.server.auth.provider import AccessToken
from mcp.types import CallToolResult
from mcp.types import TextContent
from mcp.types import Tool
from mcp.types import ToolAnnotations

from snapper.auth.schemas.tokens import TokenClaims
from snapper.data.repository import Repository
from snapper.mcp.oauth.contracts import MCPOAuthError
from snapper.mcp.oauth.scopes import TOOL_OAUTH_SCOPES
from snapper.mcp.oauth.service import MCPOAuthService


def protected_resource_metadata_url(resource: str) -> str:
    """Return the canonical RFC 9728 discovery URL for a protected resource.

    Args:
        resource: Validated public MCP resource identifier.

    Returns:
        The origin's protected-resource discovery URL for that resource path.
    """
    parts = urlsplit(resource)
    return f"{parts.scheme}://{parts.netloc}/.well-known/oauth-protected-resource{parts.path}"


@dataclass(frozen=True, slots=True)
class MCPOAuthTransport:
    """Configuration for OAuth on the bearer-protected MCP resource."""

    resource: str
    service_factory: Callable[[Repository], MCPOAuthService]

    async def verify(
        self, token: str, repository: Repository
    ) -> tuple[TokenClaims, AuthenticatedUser] | None:
        """Require live OAuth authority before projecting claims and SDK identity.

        Args:
            token: Raw bearer credential supplied on this MCP request.
            repository: Repository used for the current authority checks.

        Returns:
            Current Snapper claims and SDK identity, or no identity on refusal.
        """
        try:
            verified = await self.service_factory(repository).verify_access_token(token)
        except MCPOAuthError:
            return None
        signed = verified.claims
        principal = verified.principal
        claims = signed.model_copy(
            update={
                "username": principal.username,
                "permissions": principal.permissions,
                "permission_scope_version": principal.permission_scope_version,
            }
        )
        access = AccessToken(
            token=token,
            client_id=verified.client_id,
            scopes=list(verified.scopes),
            expires_at=signed.exp,
            resource=self.resource,
            subject=principal.user_public_id,
            claims={"iss": signed.iss},
        )
        return claims, AuthenticatedUser(access)


def oauth_tool_list(tools: list[Tool], access: AccessToken) -> list[Tool]:
    """Filter and describe protected tools for the initial read-only OAuth catalog.

    Args:
        tools: Tool descriptors from the SDK registration.
        access: Verified OAuth context carrying the current scopes.

    Returns:
        Scope-eligible descriptors with read annotations and security schemes.
    """
    visible: list[Tool] = []
    granted = set(access.scopes)
    for tool in tools:
        required = TOOL_OAUTH_SCOPES.get(tool.name)
        if required is None or not required <= granted:
            continue
        annotation = ToolAnnotations(
            read_only_hint=True,
            destructive_hint=False,
            idempotent_hint=True,
            open_world_hint=False,
        )
        metadata = {
            **(tool.meta or {}),
            "securitySchemes": [{"type": "oauth2", "scopes": sorted(required)}],
        }
        visible.append(tool.model_copy(update={"annotations": annotation, "meta": metadata}))
    return visible


def oauth_tool_refusal(name: str, access: AccessToken) -> CallToolResult | None:
    """Refuse direct calls outside the grant before resolving arguments or tools.

    Args:
        name: Requested MCP tool name.
        access: Verified OAuth context carrying the current scopes.

    Returns:
        A scoped denial result, or no refusal when the tool's scopes are granted.
    """
    required = TOOL_OAUTH_SCOPES.get(name)
    if required is not None and required <= set(access.scopes):
        return None
    parts = ['Bearer error="insufficient_scope"']
    if access.resource is not None:
        metadata_url = protected_resource_metadata_url(access.resource)
        parts.append(f'resource_metadata="{metadata_url}"')
    if required is not None:
        parts.append(f'scope="{" ".join(sorted(required))}"')
    return CallToolResult(
        meta={"mcp/www_authenticate": [", ".join(parts)]},
        content=[TextContent(type="text", text="The OAuth grant does not allow this tool.")],
        is_error=True,
    )
