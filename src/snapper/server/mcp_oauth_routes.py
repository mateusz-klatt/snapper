"""HTTP adapters for the MCP OAuth authorization server and its consent API.

Every route answers 404 unless ``MCP_OAUTH_ENABLED`` is true, so a default
deployment publishes no metadata and no endpoint. With the bootstrap flag on
and the database ``ai_integration_enabled`` flag off, metadata stays stable,
authorize/consent/token issuance are refused by live checks, and revocation
keeps working.

Client authentication on ``/token`` and ``/revoke`` is done here and verified
by the service against the stored bcrypt hash. The MCP SDK route builder and
its plaintext-comparing ``ClientAuthenticator`` are deliberately not used.

The authorize endpoint validates the client and its exact registered redirect
before it ever redirects. An unknown client or untrusted redirect is answered
directly with 400 and no ``Location``.

The pending request identifier is a bearer capability until consent: requests
are created by the client redirect before any Snapper login, so they have no
owner. Approval and denial re-check the authenticated owner's live authority.
"""

import base64
import binascii
from datetime import UTC
from datetime import datetime
from typing import Annotated
from urllib.parse import parse_qsl
from urllib.parse import urlencode
from urllib.parse import urlsplit
from urllib.parse import urlunsplit
from uuid import uuid7

from fastapi import APIRouter
from fastapi import Depends
from fastapi import HTTPException
from fastapi import Request
from fastapi import status
from fastapi.responses import JSONResponse
from fastapi.responses import RedirectResponse
from fastapi.responses import Response

from snapper.api.schemas.mcp_oauth import AuthorizationServerMetadata
from snapper.api.schemas.mcp_oauth import ConsentApproveRequest
from snapper.api.schemas.mcp_oauth import ConsentDenyRequest
from snapper.api.schemas.mcp_oauth import ConsentOperatorOption
from snapper.api.schemas.mcp_oauth import ConsentRedirect
from snapper.api.schemas.mcp_oauth import ConsentRedirectResponse
from snapper.api.schemas.mcp_oauth import ConsentRequestDetail
from snapper.api.schemas.mcp_oauth import ConsentRequestResponse
from snapper.api.schemas.mcp_oauth import ProtectedResourceMetadata
from snapper.api.schemas.mcp_oauth import RevocationResponse
from snapper.api.schemas.mcp_oauth import TokenResponse
from snapper.application.ai_delegates.service import DelegateOperatorBindingError
from snapper.application.ai_delegates.service import DelegateProliferationError
from snapper.application.ai_delegates.service import DelegateService
from snapper.application.ai_delegates.service import OAuthGrantReplacementRequiredError
from snapper.auth.dependencies import require_permission
from snapper.auth.dependencies import validate_csrf_token
from snapper.auth.domain.permissions import Permission
from snapper.auth.schemas.principal import AuthPrincipal
from snapper.auth.tokens import get_token_manager
from snapper.config.app import AppSettings
from snapper.config.settings import get_settings
from snapper.data.repository import Repository
from snapper.mcp.oauth.contracts import MCPOAuthError
from snapper.mcp.oauth.contracts import MCPOAuthServiceConfig
from snapper.mcp.oauth.contracts import OAuthAuthorizationRequestCreate
from snapper.mcp.oauth.contracts import OAuthCodeExchangeCommand
from snapper.mcp.oauth.contracts import OAuthConsentApprovalCommand
from snapper.mcp.oauth.contracts import OAuthConsentDenialCommand
from snapper.mcp.oauth.contracts import OAuthRefreshExchangeCommand
from snapper.mcp.oauth.contracts import OAuthRevocationCommand
from snapper.mcp.oauth.contracts import OAuthTokenExchangeResult
from snapper.mcp.oauth.scopes import OFFLINE_ACCESS
from snapper.mcp.oauth.scopes import SNAPPER_READ
from snapper.mcp.oauth.service import MCPOAuthService
from snapper.messaging.infrastructure.publisher import SequenceTracker
from snapper.server.ai_delegate_routes import require_ai_integration_enabled
from snapper.server.dependencies import get_repository_dependency
from snapper.server.json_body import json_body
from snapper.server.json_body import openapi_schema

CONSENT_PAGE_PATH = "/oauth/authorize"
"""Frontend route that renders the authenticated consent page.

The pending request travels in the URL fragment, which browsers never send to a
server, so neither the reverse proxy nor the access log records it.
"""

CONSENT_REQUEST_HEADER = "X-MCP-OAuth-Request"
"""Header carrying the opaque pending request to the consent read; never a query."""

_REST_STREAM = "rest.control"
_AI_INTEGRATION_FLAG_KEY = "ai_integration_enabled"
_NO_STORE = {"Cache-Control": "no-store", "Pragma": "no-cache", "Referrer-Policy": "no-referrer"}
_SUPPORTED_SCOPES = [SNAPPER_READ, OFFLINE_ACCESS]
_AUTH_METHODS = ["client_secret_basic", "client_secret_post"]
_AUTHORIZE_ERRORS = {
    "invalid_scope": "invalid_scope",
    "invalid_pkce": "invalid_request",
    "invalid_resource": "invalid_target",
}
_TOKEN_ERRORS = {
    "invalid_client": "invalid_client",
    "invalid_scope": "invalid_scope",
    "invalid_resource": "invalid_target",
    "invalid_request": "invalid_request",
}
_CONSENT_ERRORS = {
    "invalid_scope": (status.HTTP_400_BAD_REQUEST, "consent_scope_invalid"),
    "access_denied": (status.HTTP_403_FORBIDDEN, "consent_access_denied"),
}


class OAuthProtocolError(Exception):
    """An RFC 6749/7009 error body, rendered by :func:`oauth_protocol_error_handler`."""

    def __init__(
        self, error: str, status_code: int = 400, headers: dict[str, str] | None = None
    ) -> None:
        """Hold the RFC error code, HTTP status and extra response headers."""
        super().__init__(error)
        self.error = error
        self.status_code = status_code
        self.headers = {**_NO_STORE, **(headers or {})}


def oauth_protocol_error_handler(_request: Request, exc: Exception) -> JSONResponse:
    """Render an OAuth protocol error as ``{"error": ...}`` with no internal detail.

    Args:
        _request: The failed request.
        exc: The :class:`OAuthProtocolError` raised by a protocol route.

    Returns:
        The uncacheable RFC error response.
    """
    error = exc if isinstance(exc, OAuthProtocolError) else OAuthProtocolError("server_error", 500)
    return JSONResponse(
        {"error": error.error}, status_code=error.status_code, headers=error.headers
    )


def oauth_settings() -> AppSettings:
    """Return bootstrap settings; overridable in tests.

    Returns:
        The cached bootstrap-only application settings.
    """
    return get_settings()


def require_mcp_oauth_enabled(
    settings: Annotated[AppSettings, Depends(oauth_settings)],
) -> AppSettings:
    """Hide every OAuth surface unless the bootstrap flag publishes it.

    Args:
        settings: Bootstrap settings.

    Returns:
        The same settings, for handlers that need issuer and lifetimes.

    Raises:
        HTTPException: 404 when ``MCP_OAUTH_ENABLED`` is false.
    """
    if not settings.mcp_oauth_enabled:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Not Found")
    return settings


router = APIRouter(tags=["mcp-oauth"], dependencies=[Depends(require_mcp_oauth_enabled)])
"""Every route is hidden behind the bootstrap flag before any body is parsed."""

registration_router = APIRouter(tags=["mcp-oauth"])
"""Always-present 404 for dynamic client registration, independent of the flag."""


def build_oauth_service(repository: Repository, settings: AppSettings) -> MCPOAuthService:
    """Build the OAuth service from exact bootstrap identities and lifetimes.

    Args:
        repository: Request-scoped repository.
        settings: Bootstrap settings.

    Returns:
        A service bound to the configured issuer and protected resource.
    """
    manager = get_token_manager()
    return MCPOAuthService(
        repository=repository,
        token_manager=manager,
        config=MCPOAuthServiceConfig(
            issuer=settings.mcp_oauth_issuer,
            resource=settings.mcp_public_resource_url,
            access_token_ttl_seconds=settings.mcp_oauth_access_token_ttl_seconds,
            code_ttl_seconds=settings.mcp_oauth_code_ttl_seconds,
            request_ttl_seconds=settings.mcp_oauth_request_ttl_seconds,
            refresh_token_ttl_days=settings.mcp_oauth_refresh_token_ttl_days,
        ),
        delegate_service=DelegateService(repository=repository, token_manager=manager),
    )


def oauth_service(
    repo: Annotated[Repository, Depends(get_repository_dependency)],
    settings: Annotated[AppSettings, Depends(require_mcp_oauth_enabled)],
) -> MCPOAuthService:
    """Request-scoped OAuth service; 404 unless OAuth is enabled.

    Args:
        repo: Request-scoped repository.
        settings: Bootstrap settings.

    Returns:
        The service bound to this request's repository and exact identities.
    """
    return build_oauth_service(repo, settings)


_SERVICE = Depends(oauth_service)


def _integration_enabled(request: Request) -> bool:
    """Read the live database AI-integration flag without raising."""
    settings_service = getattr(request.app.state, "settings_service", None)
    if settings_service is None:
        return True
    return bool(settings_service.get_setting(_AI_INTEGRATION_FLAG_KEY, default=True))


@router.get("/.well-known/oauth-protected-resource/api/mcp")
@router.get("/api/mcp/.well-known/oauth-protected-resource")
async def protected_resource_metadata(
    settings: Annotated[AppSettings, Depends(require_mcp_oauth_enabled)],
) -> ProtectedResourceMetadata:
    """Publish where the MCP resource's authorization server lives.

    Args:
        settings: Bootstrap settings; 404 unless OAuth is enabled.

    Returns:
        The protected-resource metadata document.
    """
    return ProtectedResourceMetadata(
        resource=settings.mcp_public_resource_url,
        authorization_servers=[settings.mcp_oauth_issuer],
        scopes_supported=list(_SUPPORTED_SCOPES),
        bearer_methods_supported=["header"],
    )


@router.get("/.well-known/oauth-authorization-server/api/mcp")
@router.get("/api/mcp/.well-known/oauth-authorization-server")
async def authorization_server_metadata(
    settings: Annotated[AppSettings, Depends(require_mcp_oauth_enabled)],
) -> AuthorizationServerMetadata:
    """Publish the authorization-server endpoints and supported methods.

    Args:
        settings: Bootstrap settings; 404 unless OAuth is enabled.

    Returns:
        The authorization-server metadata document.
    """
    issuer = settings.mcp_oauth_issuer
    return AuthorizationServerMetadata(
        issuer=issuer,
        authorization_endpoint=f"{issuer}/authorize",
        token_endpoint=f"{issuer}/token",
        revocation_endpoint=f"{issuer}/revoke",
        scopes_supported=list(_SUPPORTED_SCOPES),
        response_types_supported=["code"],
        grant_types_supported=["authorization_code", "refresh_token"],
        code_challenge_methods_supported=["S256"],
        token_endpoint_auth_methods_supported=list(_AUTH_METHODS),
        revocation_endpoint_auth_methods_supported=list(_AUTH_METHODS),
        authorization_response_iss_parameter_supported=True,
    )


@registration_router.api_route(
    "/api/mcp/register", methods=["GET", "POST", "PUT", "DELETE"], include_in_schema=False
)
async def dynamic_client_registration() -> Response:
    """Keep dynamic client registration absent instead of reaching the MCP bearer gate.

    Returns:
        Always 404; clients are provisioned with the operator CLI.
    """
    return Response(status_code=status.HTTP_404_NOT_FOUND)


def _with_query(uri: str, params: dict[str, str | None]) -> str:
    """Append non-empty parameters while keeping the registered query intact."""
    parts = urlsplit(uri)
    query = parse_qsl(parts.query, keep_blank_values=True)
    query.extend((key, value) for key, value in params.items() if value is not None)
    return urlunsplit(parts._replace(query=urlencode(query)))


def _redirect(location: str) -> RedirectResponse:
    """Return a non-cacheable 302 that sends no referrer."""
    return RedirectResponse(location, status_code=status.HTTP_302_FOUND, headers=_NO_STORE)


@router.get("/api/mcp/authorize")
async def authorize(
    request: Request,
    settings: Annotated[AppSettings, Depends(require_mcp_oauth_enabled)],
    repo: Annotated[Repository, Depends(get_repository_dependency)],
) -> RedirectResponse:
    """Validate an authorization request and send the browser to the consent page.

    Args:
        request: Incoming request; parameters are read from the query string.
        settings: Bootstrap settings; 404 unless OAuth is enabled.
        repo: Request-scoped repository.

    Returns:
        A 302 either to the consent page or back to the registered callback with an error.

    Raises:
        OAuthProtocolError: 400 without any redirect for an untrusted client or callback.
    """
    params = request.query_params
    service = build_oauth_service(repo, settings)
    state = params.get("state")
    try:
        callback = await service.trusted_redirect(
            params.get("client_id", ""),
            params.get("redirect_uri"),
            provided_explicitly="redirect_uri" in params,
        )
    except MCPOAuthError as untrusted:
        code = "invalid_client" if str(untrusted) == "invalid_client" else "invalid_request"
        raise OAuthProtocolError(code) from untrusted
    issuer = settings.mcp_oauth_issuer
    refusal = _authorize_precondition(request)
    if refusal is None:
        try:
            pending = await service.create_authorization_request(_authorization_command(request))
        except MCPOAuthError as refused:
            refusal = _AUTHORIZE_ERRORS.get(str(refused), "invalid_request")
        else:
            return _redirect(
                f"{CONSENT_PAGE_PATH}#{urlencode({'request': pending.raw_request_id})}"
            )
    return _redirect(_with_query(callback, {"error": refusal, "state": state, "iss": issuer}))


def _authorize_precondition(request: Request) -> str | None:
    """Return the RFC 6749 error for a request that must not create a pending row."""
    params = request.query_params
    if params.get("response_type") != "code":
        return "unsupported_response_type"
    if "scope" not in params:
        return "invalid_scope"
    if not _integration_enabled(request):
        return "temporarily_unavailable"
    return None


def _authorization_command(request: Request) -> OAuthAuthorizationRequestCreate:
    """Translate exact query parameters into the service command; nothing is defaulted."""
    params = request.query_params
    return OAuthAuthorizationRequestCreate(
        client_id=params.get("client_id", ""),
        redirect_uri=params.get("redirect_uri"),
        redirect_uri_provided_explicitly="redirect_uri" in params,
        scopes=tuple(params.get("scope", "").split()),
        code_challenge=params.get("code_challenge", ""),
        code_challenge_method=params.get("code_challenge_method", ""),
        resource=params.get("resource", ""),
        state=params.get("state"),
    )


class _ClientCredentials:
    """Client identity and secret taken from exactly one RFC 6749 method."""

    __slots__ = ("client_id", "method", "secret")

    def __init__(self, client_id: str, secret: str, method: str) -> None:
        """Hold one authenticated-method triple without logging the secret."""
        self.client_id = client_id
        self.secret = secret
        self.method = method


def _client_credentials(request: Request, form: dict[str, str]) -> _ClientCredentials:
    """Read Basic or POST client credentials; mixing both is refused.

    Raises:
        MCPOAuthError: ``invalid_client`` when none, both or malformed credentials are given.
    """
    header = request.headers.get("authorization", "")
    posted = "client_secret" in form
    if header[:6].lower() == "basic ":
        if posted:
            raise MCPOAuthError("invalid_client")
        try:
            decoded = base64.b64decode(header[6:], validate=True).decode("utf-8")
        except (binascii.Error, UnicodeDecodeError) as error:
            raise MCPOAuthError("invalid_client") from error
        client_id, separator, secret = decoded.partition(":")
        if not separator:
            raise MCPOAuthError("invalid_client")
        return _ClientCredentials(client_id, secret, "client_secret_basic")
    if posted and form.get("client_id"):
        return _ClientCredentials(form["client_id"], form["client_secret"], "client_secret_post")
    raise MCPOAuthError("invalid_client")


async def _form(request: Request) -> dict[str, str]:
    """Read a form body, rejecting repeated parameters as RFC 6749 requires.

    Raises:
        MCPOAuthError: ``invalid_request`` for a non-form or repeated-parameter body.
    """
    if not request.headers.get("content-type", "").startswith("application/x-www-form-urlencoded"):
        raise MCPOAuthError("invalid_request")
    pairs = parse_qsl((await request.body()).decode("utf-8", errors="replace"))
    form = dict(pairs)
    if len(form) != len(pairs):
        raise MCPOAuthError("invalid_request")
    return form


def _protocol_error(code: str, method: str | None) -> OAuthProtocolError:
    """Map a service refusal onto an RFC 6749 token-endpoint error."""
    if code == "unsupported_grant_type":
        return OAuthProtocolError(code)
    error = _TOKEN_ERRORS.get(code, "invalid_grant")
    if error != "invalid_client":
        return OAuthProtocolError(error)
    challenge = (
        {"WWW-Authenticate": 'Basic realm="snapper-mcp"'}
        if method == "client_secret_basic"
        else None
    )
    return OAuthProtocolError(error, status.HTTP_401_UNAUTHORIZED, challenge)


async def _exchange(
    service: MCPOAuthService, form: dict[str, str], credentials: _ClientCredentials
) -> OAuthTokenExchangeResult:
    """Dispatch one grant type with the exact resource required on both exchanges.

    Raises:
        MCPOAuthError: Any protocol refusal, including an unsupported grant type.
    """
    grant_type = form.get("grant_type")
    resource = form.get("resource", "")
    if grant_type == "authorization_code":
        return await service.exchange_authorization_code(
            OAuthCodeExchangeCommand(
                client_id=credentials.client_id,
                client_secret=credentials.secret,
                auth_method=credentials.method,
                raw_code=form.get("code", ""),
                code_verifier=form.get("code_verifier", ""),
                redirect_uri=form.get("redirect_uri"),
                resource=resource,
            )
        )
    if grant_type == "refresh_token":
        scope = form.get("scope")
        return await service.exchange_refresh_token(
            OAuthRefreshExchangeCommand(
                client_id=credentials.client_id,
                client_secret=credentials.secret,
                auth_method=credentials.method,
                raw_refresh_token=form.get("refresh_token", ""),
                resource=resource,
                scopes=tuple(scope.split()) if scope is not None else None,
            )
        )
    raise MCPOAuthError("unsupported_grant_type")


@router.post("/api/mcp/token", response_model_exclude_none=True)
async def token(
    request: Request,
    response: Response,
    settings: Annotated[AppSettings, Depends(require_mcp_oauth_enabled)],
    repo: Annotated[Repository, Depends(get_repository_dependency)],
) -> TokenResponse:
    """Exchange an authorization code or rotate a refresh token.

    Args:
        request: Form-encoded RFC 6749 token request.
        response: Outgoing response, marked uncacheable.
        settings: Bootstrap settings; 404 unless OAuth is enabled.
        repo: Request-scoped repository.

    Returns:
        The token response; ``refresh_token`` is omitted when none is issued.

    Raises:
        OAuthProtocolError: Any RFC 6749 refusal, or 503 while the integration is off.
    """
    response.headers.update(_NO_STORE)
    method: str | None = None
    try:
        form = await _form(request)
        credentials = _client_credentials(request, form)
        method = credentials.method
        if not _integration_enabled(request):
            raise OAuthProtocolError("temporarily_unavailable", status.HTTP_503_SERVICE_UNAVAILABLE)
        result = await _exchange(build_oauth_service(repo, settings), form, credentials)
    except MCPOAuthError as error:
        raise _protocol_error(str(error), method) from error
    return TokenResponse(
        access_token=result.access_token,
        token_type=result.token_type,
        expires_in=result.expires_in,
        scope=" ".join(result.scopes),
        refresh_token=result.refresh_token,
    )


@router.post("/api/mcp/revoke")
async def revoke(
    request: Request,
    response: Response,
    settings: Annotated[AppSettings, Depends(require_mcp_oauth_enabled)],
    repo: Annotated[Repository, Depends(get_repository_dependency)],
) -> RevocationResponse:
    """Revoke the whole grant behind one credential; always available when published.

    Args:
        request: Form-encoded RFC 7009 revocation request.
        response: Outgoing response, marked uncacheable.
        settings: Bootstrap settings; 404 unless OAuth is enabled.
        repo: Request-scoped repository.

    Returns:
        An empty object on success, including for unknown tokens.

    Raises:
        OAuthProtocolError: RFC errors for bad requests or client authentication.
    """
    response.headers.update(_NO_STORE)
    method: str | None = None
    try:
        form = await _form(request)
        credentials = _client_credentials(request, form)
        method = credentials.method
        if "token" not in form:
            raise MCPOAuthError("invalid_request")
        await build_oauth_service(repo, settings).revoke_token(
            OAuthRevocationCommand(
                credentials.client_id, credentials.secret, credentials.method, form["token"]
            )
        )
    except MCPOAuthError as error:
        raise _protocol_error(str(error), method) from error
    return RevocationResponse()


def _consent_error(error: Exception) -> HTTPException:
    """Map service refusals onto the stable consent-API error codes."""
    if isinstance(error, OAuthGrantReplacementRequiredError):
        code: tuple[int, str] = (status.HTTP_409_CONFLICT, "grant_replacement_required")
    elif isinstance(error, DelegateProliferationError):
        code = (status.HTTP_409_CONFLICT, "consent_delegate_limit")
    elif isinstance(error, DelegateOperatorBindingError):
        code = (status.HTTP_403_FORBIDDEN, "consent_access_denied")
    else:
        code = _CONSENT_ERRORS.get(
            str(error), (status.HTTP_400_BAD_REQUEST, "consent_request_invalid")
        )
    return HTTPException(status_code=code[0], detail={"error_code": code[1]}, headers=_NO_STORE)


def _envelope(request: Request) -> tuple[str, int, str, datetime]:
    """Stamp REST provenance for one consent response."""
    tracker: SequenceTracker = request.app.state.rest_tracker
    return (
        tracker.session_id,
        tracker.next_sequence(_REST_STREAM),
        str(uuid7()),
        datetime.now(UTC),
    )


async def _operator_options(repo: Repository, owner: AuthPrincipal) -> list[ConsentOperatorOption]:
    """List only the owner's own active operators for the binding picker."""
    allowed = set(owner.operator_public_ids)
    rows = await repo.list_active_operators(datetime.now(UTC))
    return [
        ConsentOperatorOption(public_id=row["public_id"], name=row["label"])
        for row in rows
        if row["public_id"] in allowed
    ]


_MANAGE = Depends(require_permission(Permission.MANAGE_AI_INTEGRATION))


@router.get("/api/mcp-oauth/consent")
async def consent_request(
    request: Request,
    response: Response,
    owner: Annotated[AuthPrincipal, _MANAGE],
    repo: Annotated[Repository, Depends(get_repository_dependency)],
    settings: Annotated[AppSettings, Depends(require_mcp_oauth_enabled)],
    _flag: Annotated[None, Depends(require_ai_integration_enabled)] = None,
) -> ConsentRequestResponse:
    """Describe one pending request to its prospective authenticated owner.

    Args:
        request: Incoming request carrying the opaque request in ``X-MCP-OAuth-Request``.
        response: Outgoing response, marked uncacheable.
        owner: Authenticated caller with AI-integration management.
        repo: Request-scoped repository.
        settings: Bootstrap settings; 404 unless OAuth is enabled.
        _flag: Live AI-integration gate.

    Returns:
        The consent detail envelope, never state, redirect or secrets.

    Raises:
        HTTPException: 400 ``consent_request_invalid`` for any unavailable request.
    """
    response.headers.update(_NO_STORE)
    raw = request.headers.get(CONSENT_REQUEST_HEADER, "")
    try:
        detail = await build_oauth_service(repo, settings).describe_authorization_request(raw)
    except MCPOAuthError as error:
        raise _consent_error(MCPOAuthError("invalid_request")) from error
    session_id, sequence_id, public_id, timestamp = _envelope(request)
    return ConsentRequestResponse(
        session_id=session_id,
        sequence_id=sequence_id,
        public_id=public_id,
        timestamp=timestamp,
        payload=ConsentRequestDetail(
            client_name=detail.client_name,
            scopes=list(detail.scopes),
            resource=detail.resource,
            operators=await _operator_options(repo, owner),
            expires_in=detail.expires_in,
        ),
    )


def _redirect_response(
    request: Request, response: Response, redirect_to: str
) -> ConsentRedirectResponse:
    """Wrap a backend-validated callback in the uncacheable consent redirect envelope."""
    response.headers.update(_NO_STORE)
    session_id, sequence_id, public_id, timestamp = _envelope(request)
    return ConsentRedirectResponse(
        session_id=session_id,
        sequence_id=sequence_id,
        public_id=public_id,
        timestamp=timestamp,
        payload=ConsentRedirect(redirect_to=redirect_to),
    )


@router.post("/api/mcp-oauth/consent/approve", openapi_extra=openapi_schema(ConsentApproveRequest))
async def approve_consent(
    request: Request,
    response: Response,
    body: Annotated[ConsentApproveRequest, Depends(json_body(ConsentApproveRequest))],
    owner: Annotated[AuthPrincipal, _MANAGE],
    service: Annotated[MCPOAuthService, _SERVICE],
    _csrf: Annotated[None, Depends(validate_csrf_token)] = None,
    _flag: Annotated[None, Depends(require_ai_integration_enabled)] = None,
) -> ConsentRedirectResponse:
    """Approve one pending request and return the exact callback with code and state.

    Args:
        request: Incoming request, for REST provenance.
        response: Outgoing response, marked uncacheable.
        body: Approval envelope.
        owner: Authenticated caller with AI-integration management.
        service: Request-scoped OAuth service; 404 unless OAuth is enabled.
        _csrf: CSRF validation.
        _flag: Live AI-integration gate.

    Returns:
        The consent redirect envelope.

    Raises:
        HTTPException: A structured consent error.
    """
    payload = body.payload
    try:
        approved = await service.approve_authorization_request(
            OAuthConsentApprovalCommand(
                raw_request_id=payload.oauth_request,
                owner=owner,
                operator_public_id=payload.operator_public_id,
                replace_existing_grant=payload.replace_existing_grant,
            )
        )
    except (
        MCPOAuthError,
        OAuthGrantReplacementRequiredError,
        DelegateProliferationError,
        DelegateOperatorBindingError,
    ) as error:
        raise _consent_error(error) from error
    callback = _with_query(
        approved.redirect_uri,
        {"code": approved.raw_code, "state": approved.state, "iss": service.config.issuer},
    )
    return _redirect_response(request, response, callback)


@router.post("/api/mcp-oauth/consent/deny", openapi_extra=openapi_schema(ConsentDenyRequest))
async def deny_consent(
    request: Request,
    response: Response,
    body: Annotated[ConsentDenyRequest, Depends(json_body(ConsentDenyRequest))],
    owner: Annotated[AuthPrincipal, _MANAGE],
    service: Annotated[MCPOAuthService, _SERVICE],
    _csrf: Annotated[None, Depends(validate_csrf_token)] = None,
    _flag: Annotated[None, Depends(require_ai_integration_enabled)] = None,
) -> ConsentRedirectResponse:
    """Deny one pending request and return the callback with ``access_denied``.

    Args:
        request: Incoming request, for REST provenance.
        response: Outgoing response, marked uncacheable.
        body: Denial envelope.
        owner: Authenticated caller with AI-integration management.
        service: Request-scoped OAuth service; 404 unless OAuth is enabled.
        _csrf: CSRF validation.
        _flag: Live AI-integration gate.

    Returns:
        The consent redirect envelope.

    Raises:
        HTTPException: A structured consent error.
    """
    try:
        denied = await service.deny_authorization_request(
            OAuthConsentDenialCommand(
                raw_request_id=body.payload.oauth_request,
                owner=owner,
                operator_public_id=body.payload.operator_public_id,
            )
        )
    except MCPOAuthError as error:
        raise _consent_error(error) from error
    callback = _with_query(
        denied.redirect_uri,
        {"error": "access_denied", "state": denied.state, "iss": service.config.issuer},
    )
    return _redirect_response(request, response, callback)
