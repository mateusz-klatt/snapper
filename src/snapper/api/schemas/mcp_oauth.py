"""Internal consent API envelopes for the MCP OAuth authorization flow.

These schemas belong to the authenticated Snapper consent page, not to the
OAuth protocol endpoints. ``/api/mcp/token`` and ``/api/mcp/revoke`` keep the
RFC form encodings and error bodies and never use these envelopes.
"""

from typing import Literal

from pydantic import Field

from snapper.api.schemas.base import PayloadRequest
from snapper.api.schemas.base import PayloadResponse
from snapper.api.schemas.base import StrictBody


class ConsentOperatorOption(StrictBody):
    """One operator the authenticated owner may bind the connector to.

    Attributes:
        public_id: Operator public identifier.
        name: Operator display label.
    """

    public_id: str
    name: str


class ConsentRequestDetail(StrictBody):
    """What the owner is asked to approve; never state, redirect or secrets.

    Attributes:
        client_name: Registered client display name.
        scopes: Requested OAuth scopes in request order.
        resource: Exact protected resource the grant is bound to.
        operators: Operators the owner may select for the binding.
        read_only: Always true for the first-release scope ceiling.
        expires_in: Seconds until the pending request expires.
    """

    client_name: str
    scopes: list[str]
    resource: str
    operators: list[ConsentOperatorOption]
    read_only: Literal[True] = True
    expires_in: int = Field(..., ge=1)


class ConsentRequestResponse(
    PayloadResponse[Literal["mcp_oauth_consent_request"], ConsentRequestDetail]
):
    """Pending consent request envelope."""

    type: Literal["mcp_oauth_consent_request"] = "mcp_oauth_consent_request"


class ConsentApproveBody(StrictBody):
    """Owner approval of one pending request.

    Attributes:
        oauth_request: Opaque pending request credential; redacted before persistence.
        operator_public_id: Selected operator, or None for the primary operator.
        replace_existing_grant: Explicit confirmation to replace an incompatible grant.
    """

    oauth_request: str = Field(..., min_length=1, max_length=512)
    operator_public_id: str | None = None
    replace_existing_grant: bool = False


class ConsentApproveRequest(
    PayloadRequest[Literal["mcp_oauth_consent_approve_request"], ConsentApproveBody]
):
    """Consent approval request envelope."""

    type: Literal["mcp_oauth_consent_approve_request"] = "mcp_oauth_consent_approve_request"


class ConsentDenyBody(StrictBody):
    """Owner denial of one pending request.

    Attributes:
        oauth_request: Opaque pending request credential; redacted before persistence.
        operator_public_id: Selected operator, or None for the primary operator.
    """

    oauth_request: str = Field(..., min_length=1, max_length=512)
    operator_public_id: str | None = None


class ConsentDenyRequest(
    PayloadRequest[Literal["mcp_oauth_consent_deny_request"], ConsentDenyBody]
):
    """Consent denial request envelope."""

    type: Literal["mcp_oauth_consent_deny_request"] = "mcp_oauth_consent_deny_request"


class ConsentRedirect(StrictBody):
    """Backend-validated navigation target after a consent decision.

    Attributes:
        redirect_to: Exact registered client callback carrying code or error and state.
    """

    redirect_to: str


class ConsentRedirectResponse(
    PayloadResponse[Literal["mcp_oauth_consent_redirect"], ConsentRedirect]
):
    """Consent decision response envelope."""

    type: Literal["mcp_oauth_consent_redirect"] = "mcp_oauth_consent_redirect"


class ProtectedResourceMetadata(StrictBody):
    """RFC 9728 protected-resource metadata with exact canonical identities.

    Attributes:
        resource: Exact protected resource, without trailing slash.
        authorization_servers: Issuers that protect this resource.
        scopes_supported: Scopes this resource understands.
        bearer_methods_supported: Accepted bearer token transports.
    """

    resource: str
    authorization_servers: list[str]
    scopes_supported: list[str]
    bearer_methods_supported: list[str]


class AuthorizationServerMetadata(StrictBody):
    """RFC 8414 authorization-server metadata; it never names a registration endpoint.

    Attributes:
        issuer: Exact issuer identity.
        authorization_endpoint: Browser authorization endpoint.
        token_endpoint: Code and refresh exchange endpoint.
        revocation_endpoint: RFC 7009 revocation endpoint.
        scopes_supported: Scopes the server issues.
        response_types_supported: Only the authorization code flow.
        grant_types_supported: Authorization code and refresh token.
        code_challenge_methods_supported: Only S256.
        token_endpoint_auth_methods_supported: Confidential client methods.
        revocation_endpoint_auth_methods_supported: Confidential client methods.
        authorization_response_iss_parameter_supported: RFC 9207 ``iss`` is returned.
    """

    issuer: str
    authorization_endpoint: str
    token_endpoint: str
    revocation_endpoint: str
    scopes_supported: list[str]
    response_types_supported: list[str]
    grant_types_supported: list[str]
    code_challenge_methods_supported: list[str]
    token_endpoint_auth_methods_supported: list[str]
    revocation_endpoint_auth_methods_supported: list[str]
    authorization_response_iss_parameter_supported: bool


class TokenResponse(StrictBody):
    """RFC 6749 successful token response; ``refresh_token`` only when issued.

    Attributes:
        access_token: Short-lived audience-bound access JWT.
        token_type: Always ``Bearer``.
        expires_in: Access token lifetime in seconds.
        scope: Space-separated granted scopes.
        refresh_token: Rotated opaque refresh token, or None when not issued.
    """

    access_token: str
    token_type: str
    expires_in: int
    scope: str
    refresh_token: str | None = None


class RevocationResponse(StrictBody):
    """RFC 7009 success; the body carries nothing and clients ignore it."""
