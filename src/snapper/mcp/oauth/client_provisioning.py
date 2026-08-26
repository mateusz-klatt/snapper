"""Operator-controlled provisioning for confidential MCP OAuth clients."""

import asyncio
from collections.abc import Sequence
from dataclasses import dataclass
from dataclasses import field
from datetime import UTC
from datetime import datetime
from enum import StrEnum
from secrets import token_urlsafe
from typing import Annotated
from typing import Final
from typing import Protocol
from uuid import uuid7

import bcrypt
from pydantic import AnyUrl
from pydantic import TypeAdapter
from pydantic import UrlConstraints
from pydantic import ValidationError
from sqlalchemy.exc import IntegrityError

from snapper.data.models import OAuthClient
from snapper.mcp.oauth.scopes import OFFLINE_ACCESS
from snapper.mcp.oauth.scopes import SNAPPER_READ
from snapper.mcp.oauth.scopes import VALID_OAUTH_SCOPES

_CLIENT_ID_ENTROPY_BYTES: Final[int] = 32
_CLIENT_SECRET_ENTROPY_BYTES: Final[int] = 48
_CLIENT_ID_PREFIX: Final[str] = "snapper-mcp-"
_MAX_CLIENT_NAME_LENGTH: Final[int] = 200
_LOOPBACK_REDIRECT_HOSTS: Final[frozenset[str]] = frozenset(
    {"localhost", "127.0.0.1", "::1", "[::1]"}
)

DEFAULT_OAUTH_CLIENT_SCOPES: Final[tuple[str, ...]] = (SNAPPER_READ, OFFLINE_ACCESS)
_INITIAL_OAUTH_CLIENT_SCOPE_CEILING: Final[frozenset[str]] = frozenset(DEFAULT_OAUTH_CLIENT_SCOPES)
_CLIENT_ID_UNIQUE_CONSTRAINT: Final[str] = "uq_oauth_clients_client_id"
_SQLITE_CLIENT_ID_UNIQUE_MESSAGE: Final[str] = "UNIQUE constraint failed: oauth_clients.client_id"
_SQLITE_CONSTRAINT_UNIQUE_EXTCODE: Final[int] = 2067

OAuthRedirectUri = Annotated[
    AnyUrl,
    UrlConstraints(
        allowed_schemes=["http", "https"],
        host_required=True,
        preserve_empty_path=True,
    ),
]
_REDIRECT_URI_ADAPTER = TypeAdapter(OAuthRedirectUri)


class OAuthTokenEndpointAuthMethod(StrEnum):
    """Confidential client authentication methods accepted by Snapper."""

    CLIENT_SECRET_BASIC = "client_secret_basic"
    CLIENT_SECRET_POST = "client_secret_post"


class OAuthClientProvisioningError(ValueError):
    """Raised when an OAuth client cannot be provisioned safely."""


class OAuthClientAlreadyExistsError(OAuthClientProvisioningError):
    """Raised when a generated client identifier collides durably."""


class OAuthClientStore(Protocol):
    """Minimal persistence boundary required by client provisioning."""

    async def add_client(self, client: OAuthClient) -> None:
        """Persist one validated client row.

        Args:
            client: Hash-only OAuth client row to commit.
        """


@dataclass(frozen=True, slots=True)
class OAuthClientProvisioningRequest:
    """Validated metadata for one confidential OAuth client."""

    client_name: str
    redirect_uris: tuple[str, ...]
    token_endpoint_auth_method: OAuthTokenEndpointAuthMethod
    allowed_scopes: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class ProvisionedOAuthClient:
    """One-time client credentials returned only after durable persistence."""

    client_id: str
    client_secret: str = field(repr=False)
    client_name: str
    redirect_uris: tuple[str, ...]
    token_endpoint_auth_method: str
    allowed_scopes: tuple[str, ...]


def _validate_client_name(client_name: str) -> str:
    """Validate and preserve one operator-facing client name."""
    if not client_name.strip():
        raise OAuthClientProvisioningError("client name must not be empty")
    if client_name != client_name.strip():
        raise OAuthClientProvisioningError("client name must not contain surrounding whitespace")
    if len(client_name) > _MAX_CLIENT_NAME_LENGTH:
        raise OAuthClientProvisioningError(
            f"client name must not exceed {_MAX_CLIENT_NAME_LENGTH} characters"
        )
    return client_name


def _validate_redirect_uri(value: str) -> str:
    """Validate one exact web redirect URI without canonicalizing it."""
    if value != value.strip():
        raise OAuthClientProvisioningError("redirect URI must not contain whitespace")
    if any(character.isspace() for character in value):
        raise OAuthClientProvisioningError("redirect URI must not contain whitespace")
    try:
        parsed = _REDIRECT_URI_ADAPTER.validate_python(value)
    except ValidationError as error:
        raise OAuthClientProvisioningError(f"invalid redirect URI: {value}") from error
    if str(parsed) != value:
        raise OAuthClientProvisioningError(
            f"redirect URI must already be in its exact canonical form: {value}"
        )
    if parsed.fragment is not None:
        raise OAuthClientProvisioningError("redirect URI must not contain a fragment")
    if (parsed.username, parsed.password) != (None, None):
        raise OAuthClientProvisioningError("redirect URI must not contain user information")
    host = str(parsed.host).lower()
    if parsed.scheme != "https" and host not in _LOOPBACK_REDIRECT_HOSTS:
        raise OAuthClientProvisioningError("redirect URI must use HTTPS or loopback HTTP")
    return value


def _validate_redirect_uris(redirect_uris: Sequence[str]) -> tuple[str, ...]:
    """Validate a non-empty, duplicate-free exact redirect URI set."""
    if not redirect_uris:
        raise OAuthClientProvisioningError("at least one redirect URI is required")
    validated = tuple(_validate_redirect_uri(value) for value in redirect_uris)
    if len(set(validated)) != len(validated):
        raise OAuthClientProvisioningError("duplicate redirect URI is not allowed")
    return validated


def _validate_auth_method(value: str) -> OAuthTokenEndpointAuthMethod:
    """Resolve one closed-set confidential client authentication method."""
    try:
        return OAuthTokenEndpointAuthMethod(value)
    except ValueError as error:
        supported = ", ".join(method.value for method in OAuthTokenEndpointAuthMethod)
        raise OAuthClientProvisioningError(
            f"Unsupported token endpoint auth method: {value}; expected one of {supported}"
        ) from error


def _validate_scopes(allowed_scopes: Sequence[str] | None) -> tuple[str, ...]:
    """Validate an ordered scope set within the initial read-only ceiling."""
    if allowed_scopes is None:
        return DEFAULT_OAUTH_CLIENT_SCOPES
    scopes = tuple(allowed_scopes)
    if not scopes:
        raise OAuthClientProvisioningError("at least one OAuth scope is required")
    if len(set(scopes)) != len(scopes):
        raise OAuthClientProvisioningError("duplicate OAuth scope is not allowed")
    for scope in scopes:
        if scope not in VALID_OAUTH_SCOPES:
            raise OAuthClientProvisioningError(f"Unknown OAuth scope: {scope}")
        if scope not in _INITIAL_OAUTH_CLIENT_SCOPE_CEILING:
            raise OAuthClientProvisioningError(
                f"OAuth scope is deferred from the initial read-only connector: {scope}"
            )
    return scopes


def build_oauth_client_provisioning_request(
    *,
    client_name: str,
    redirect_uris: Sequence[str],
    token_endpoint_auth_method: str = OAuthTokenEndpointAuthMethod.CLIENT_SECRET_BASIC,
    allowed_scopes: Sequence[str] | None = None,
) -> OAuthClientProvisioningRequest:
    """Validate operator input before opening a database connection.

    Args:
        client_name: Exact display name shown during future consent.
        redirect_uris: Exact callback URI values accepted for this client.
        token_endpoint_auth_method: Confidential client authentication method.
        allowed_scopes: Ordered scope ceiling, or the safe read-only default.

    Returns:
        Immutable validated provisioning request.

    Raises:
        OAuthClientProvisioningError: If any metadata is unsafe or ambiguous.
    """
    return OAuthClientProvisioningRequest(
        client_name=_validate_client_name(client_name),
        redirect_uris=_validate_redirect_uris(redirect_uris),
        token_endpoint_auth_method=_validate_auth_method(token_endpoint_auth_method),
        allowed_scopes=_validate_scopes(allowed_scopes),
    )


def _hash_client_secret(client_secret: str) -> str:
    """Hash one generated client secret with a fresh bcrypt salt."""
    return bcrypt.hashpw(client_secret.encode(), bcrypt.gensalt()).decode()


def _is_client_id_conflict(error: IntegrityError) -> bool:
    """Identify only the client-ID unique constraint on supported databases.

    PostgreSQL drivers expose the exact constraint name either directly or on
    their diagnostic payload. SQLAlchemy's asyncpg adapter retains that driver
    error as its immediate cause, so both structured layers are inspected.
    SQLite exposes extended error code 2067 for a unique violation and names
    the offending columns instead of the index, so both the structured code and
    its exact canonical message must match.
    """
    original = error.orig
    wrapped_cause = getattr(original, "__cause__", None)
    database_errors = (
        (original, wrapped_cause) if isinstance(wrapped_cause, BaseException) else (original,)
    )
    for database_error in database_errors:
        if getattr(database_error, "constraint_name", None) == _CLIENT_ID_UNIQUE_CONSTRAINT:
            return True
        diagnostic = getattr(database_error, "diag", None)
        if (
            diagnostic is not None
            and getattr(diagnostic, "constraint_name", None) == _CLIENT_ID_UNIQUE_CONSTRAINT
        ):
            return True
    return (
        getattr(original, "sqlite_errorcode", None) == _SQLITE_CONSTRAINT_UNIQUE_EXTCODE
        and str(original) == _SQLITE_CLIENT_ID_UNIQUE_MESSAGE
    )


async def provision_oauth_client_request(
    store: OAuthClientStore,
    request: OAuthClientProvisioningRequest,
) -> ProvisionedOAuthClient:
    """Generate, hash, and durably persist one validated OAuth client.

    Args:
        store: OAuth client persistence boundary.
        request: Fully validated client metadata.

    Returns:
        One-time raw credentials after the hash-only row commits.

    Raises:
        OAuthClientAlreadyExistsError: If the generated identifier collides.
    """
    client_id = f"{_CLIENT_ID_PREFIX}{token_urlsafe(_CLIENT_ID_ENTROPY_BYTES)}"
    client_secret = token_urlsafe(_CLIENT_SECRET_ENTROPY_BYTES)
    client_secret_hash = await asyncio.to_thread(_hash_client_secret, client_secret)
    client = OAuthClient(
        public_id=str(uuid7()),
        client_id=client_id,
        client_secret_hash=client_secret_hash,
        client_name=request.client_name,
        redirect_uris=list(request.redirect_uris),
        token_endpoint_auth_method=request.token_endpoint_auth_method.value,
        allowed_scopes=list(request.allowed_scopes),
        is_active=True,
        created_at=datetime.now(UTC),
    )
    try:
        await store.add_client(client)
    except IntegrityError as error:
        if not _is_client_id_conflict(error):
            raise
        raise OAuthClientAlreadyExistsError(
            f"OAuth client ID already exists: {client_id}"
        ) from error
    return ProvisionedOAuthClient(
        client_id=client_id,
        client_secret=client_secret,
        client_name=request.client_name,
        redirect_uris=request.redirect_uris,
        token_endpoint_auth_method=request.token_endpoint_auth_method.value,
        allowed_scopes=request.allowed_scopes,
    )


async def provision_oauth_client(
    store: OAuthClientStore,
    *,
    client_name: str,
    redirect_uris: Sequence[str],
    token_endpoint_auth_method: str = OAuthTokenEndpointAuthMethod.CLIENT_SECRET_BASIC,
    allowed_scopes: Sequence[str] | None = None,
) -> ProvisionedOAuthClient:
    """Validate metadata and provision one confidential OAuth client.

    Args:
        store: OAuth client persistence boundary.
        client_name: Exact display name shown during future consent.
        redirect_uris: Exact callback URI values accepted for this client.
        token_endpoint_auth_method: Confidential client authentication method.
        allowed_scopes: Ordered scope ceiling, or the safe read-only default.

    Returns:
        One-time raw credentials after the hash-only row commits.

    Raises:
        OAuthClientProvisioningError: If validation or unique persistence fails.
    """
    request = build_oauth_client_provisioning_request(
        client_name=client_name,
        redirect_uris=redirect_uris,
        token_endpoint_auth_method=token_endpoint_auth_method,
        allowed_scopes=allowed_scopes,
    )
    return await provision_oauth_client_request(store, request)
