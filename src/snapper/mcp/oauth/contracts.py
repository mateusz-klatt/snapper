"""Typed, secret-safe inputs and outputs of the dormant MCP OAuth service."""

from dataclasses import dataclass
from dataclasses import field

from snapper.auth.domain.permissions import Permission
from snapper.auth.schemas.principal import AuthPrincipal
from snapper.auth.schemas.tokens import MCPOAuthAccessTokenClaims


class MCPOAuthError(ValueError):
    """Refuse an OAuth operation without returning credential or database details."""


class OAuthRefreshReuseDetectedError(MCPOAuthError):
    """Report confirmed replay only after grant-wide revocation commits."""


@dataclass(frozen=True, slots=True)
class MCPOAuthServiceConfig:
    """Exact issuer/resource identities and positive protocol artifact lifetimes."""

    issuer: str
    resource: str
    access_token_ttl_seconds: int = 900
    code_ttl_seconds: int = 120
    request_ttl_seconds: int = 300
    refresh_token_ttl_days: int = 90

    def __post_init__(self) -> None:
        """Reject unusable identities and nonpositive lifetimes at construction."""
        if not self.issuer or not self.resource:
            raise ValueError("OAuth issuer and resource must be non-empty")
        lifetimes = (
            self.access_token_ttl_seconds,
            self.code_ttl_seconds,
            self.request_ttl_seconds,
            self.refresh_token_ttl_days,
        )
        if any(value <= 0 for value in lifetimes):
            raise ValueError("OAuth artifact lifetimes must be positive")


@dataclass(frozen=True, slots=True)
class OAuthAuthorizationRequestCreate:
    """Untrusted authorization parameters that must be checked against a live client."""

    client_id: str
    redirect_uri: str | None
    scopes: tuple[str, ...]
    code_challenge: str
    code_challenge_method: str
    resource: str
    redirect_uri_provided_explicitly: bool = True
    state: str | None = field(default=None, repr=False)


@dataclass(frozen=True, slots=True)
class OAuthConsentApprovalCommand:
    """Authenticated consent to an exact pending request and operator binding."""

    raw_request_id: str = field(repr=False)
    owner: AuthPrincipal
    operator_public_id: str | None = None
    label: str = "chatgpt"
    replace_existing_grant: bool = False


@dataclass(frozen=True, slots=True)
class OAuthConsentDenialCommand:
    """Authenticated denial with the same current owner/operator authority checks."""

    raw_request_id: str = field(repr=False)
    owner: AuthPrincipal
    operator_public_id: str | None = None


@dataclass(frozen=True, slots=True)
class OAuthCodeExchangeCommand:
    """Client-authenticated, redirect-bound PKCE exchange of one opaque code."""

    client_id: str
    client_secret: str = field(repr=False)
    auth_method: str
    raw_code: str = field(repr=False)
    code_verifier: str = field(repr=False)
    redirect_uri: str | None
    resource: str


@dataclass(frozen=True, slots=True)
class OAuthRefreshExchangeCommand:
    """Client-authenticated refresh with an optional strictly narrowing scope set."""

    client_id: str
    client_secret: str = field(repr=False)
    auth_method: str
    raw_refresh_token: str = field(repr=False)
    resource: str
    scopes: tuple[str, ...] | None = None


@dataclass(frozen=True, slots=True)
class OAuthRevocationCommand:
    """Client-authenticated RFC 7009 revocation of one access or refresh credential."""

    client_id: str
    client_secret: str = field(repr=False)
    auth_method: str
    raw_token: str = field(repr=False)


@dataclass(frozen=True, slots=True)
class OAuthPendingDescription:
    """Consent-page view of one pending request, without state, redirect, or secrets."""

    client_name: str
    scopes: tuple[str, ...]
    resource: str
    expires_in: int


@dataclass(frozen=True, slots=True)
class OAuthPendingAuthorization:
    """Opaque request credential returned only after the request is durable."""

    raw_request_id: str = field(repr=False)
    expires_in: int


@dataclass(frozen=True, slots=True)
class OAuthApprovedAuthorization:
    """One-time code and exact callback metadata returned after the consent commits."""

    raw_code: str = field(repr=False)
    grant_public_id: str
    redirect_uri: str
    state: str | None = field(repr=False)


@dataclass(frozen=True, slots=True)
class OAuthDeniedAuthorization:
    """Exact registered callback and state for an error redirect after a committed denial."""

    redirect_uri: str
    state: str | None = field(repr=False)


@dataclass(frozen=True, slots=True)
class OAuthTokenExchangeResult:
    """Credentials constructed privately and returned only after one successful commit."""

    access_token: str = field(repr=False)
    refresh_token: str | None = field(repr=False)
    expires_in: int
    scopes: tuple[str, ...]
    token_type: str = "Bearer"


@dataclass(frozen=True, slots=True)
class OAuthVerifiedAccess:
    """Uncached live authority after JWT, client, grant, and current-role intersection."""

    principal: AuthPrincipal
    permissions: frozenset[Permission]
    grant_public_id: str
    client_id: str
    scopes: tuple[str, ...]
    claims: MCPOAuthAccessTokenClaims = field(repr=False)
