"""Authentication routes module.

This module provides FastAPI routes for user authentication
including login, logout, token refresh, and user management.
"""

from dataclasses import dataclass
from datetime import UTC
from datetime import datetime
from typing import Annotated
from typing import Literal
from uuid import uuid7

from fastapi import APIRouter
from fastapi import Depends
from fastapi import HTTPException
from fastapi import Request
from fastapi import Response
from fastapi import status
from loguru import logger

from snapper.api.auth.services.ws_token_service import get_ws_token_service
from snapper.api.schemas.base import MessageResponse
from snapper.auth.dependencies import get_csrf_manager
from snapper.auth.dependencies import require_authentication
from snapper.auth.dependencies import require_permission
from snapper.auth.dependencies import validate_csrf_token
from snapper.auth.domain.permissions import Permission
from snapper.auth.domain.permissions import get_effective_permissions
from snapper.auth.domain.permissions import has_effective_permission
from snapper.auth.schemas.principal import AuthPrincipal
from snapper.auth.schemas.requests import AdminResetPasswordRequest
from snapper.auth.schemas.requests import ChangePasswordRequest
from snapper.auth.schemas.requests import CreateUserRequest
from snapper.auth.schemas.requests import DeactivateUserRequest
from snapper.auth.schemas.requests import LoginRequest
from snapper.auth.schemas.requests import RefreshTokenPayload
from snapper.auth.schemas.requests import RefreshTokenRequest
from snapper.auth.schemas.requests import UpdateAuthMeRequest
from snapper.auth.schemas.requests import UpdateUserRequest
from snapper.auth.schemas.responses import LoginData
from snapper.auth.schemas.responses import LoginResponse
from snapper.auth.schemas.responses import RefreshData
from snapper.auth.schemas.responses import RefreshResponse
from snapper.auth.schemas.responses import UserListResponse
from snapper.auth.schemas.responses import UserResponse
from snapper.auth.schemas.responses import WsTokenData
from snapper.auth.schemas.responses import WsTokenResponse
from snapper.auth.schemas.tokens import TokenClaims
from snapper.auth.schemas.tokens import TokenPair
from snapper.auth.schemas.user import UserProfile
from snapper.auth.tokens import PERMISSION_SCOPE_VERSION
from snapper.auth.tokens import PermissionScopeError
from snapper.auth.tokens import TokenManager
from snapper.auth.tokens import get_token_manager
from snapper.auth.user_service import get_user_service
from snapper.data.repository import Repository
from snapper.messaging.infrastructure.publisher import SequenceTracker
from snapper.server.dependencies import get_repository_dependency
from snapper.server.json_body import json_body
from snapper.server.json_body import openapi_schema
from snapper.server.json_body import optional_json_body
from snapper.server.rate_limiting import ACCOUNT_CHANGE_RATE_LIMIT
from snapper.server.rate_limiting import ACCOUNT_RESET_RATE_LIMIT
from snapper.server.rate_limiting import WS_TOKEN_RATE_LIMIT
from snapper.server.rate_limiting import clear_failed_login_attempts
from snapper.server.rate_limiting import enforce_failed_login_rate_limit
from snapper.server.rate_limiting import limiter
from snapper.server.rate_limiting import register_failed_login_attempt

_AUTH_API_PATH = "/api/auth"
_REST_STREAM = "rest.control"
_USER_NOT_FOUND = "User not found"


@dataclass(frozen=True)
class _RefreshIdentity:
    """Verified refresh claims and their current account identity."""

    token_manager: TokenManager
    claims: TokenClaims
    user: UserProfile
    principal: AuthPrincipal


@dataclass(frozen=True)
class _RefreshRotation:
    """Winning token rotation and the capabilities it carries."""

    token_pair: TokenPair
    principal: AuthPrincipal
    permission_values: list[str] | None
    permission_scope_version: int | None


def _mint_provenance(request: Request) -> tuple[str, int, str, datetime]:
    """Extract one sid/seq/pid/ts quad from the REST tracker.

    Called once per handler. All nested minted DTOs in the response
    tree share the same provenance quad.

    Args:
        request: FastAPI request with app.state.rest_tracker.

    Returns:
        Tuple of (session_id, sequence_id, public_id, timestamp).
    """
    tracker: SequenceTracker = request.app.state.rest_tracker
    return tracker.session_id, tracker.next_sequence(_REST_STREAM), str(uuid7()), datetime.now(UTC)


def _should_return_tokens(request: Request) -> bool:
    """Return ``True`` when the caller opted into body-embedded tokens.

    MCP / CLI clients have no cookie jar; they pass
    ``?return_tokens=true`` on ``/api/auth/login`` or
    ``/api/auth/refresh`` to receive the access + refresh JWTs in the
    response body for subsequent ``Authorization: Bearer`` calls. The
    browser flow omits the query param and gets cookie-only behavior
    (unchanged).

    Args:
        request: FastAPI request whose ``query_params`` are consulted.

    Returns:
        ``True`` if ``?return_tokens=true`` (case-insensitive)
        ``False`` otherwise — including the canonical absent-param
        default, which preserves the cookie-only browser flow.
    """
    value = request.query_params.get("return_tokens", "").strip().lower()
    return value == "true"


def _authenticated_session_profile(
    user: UserProfile,
    principal: AuthPrincipal,
    token_permissions: list[str] | None,
    permission_scope_version: int | None,
) -> UserProfile:
    """Project current-session capabilities and state onto a user profile.

    Args:
        user: Account profile being returned.
        principal: Authenticated principal supplying wallet and delegate state.
        token_permissions: Permission strings carried by the current token.
        permission_scope_version: Version governing token-scope compatibility.

    Returns:
        Profile enriched with current token capabilities and session state.
    """
    effective_permissions = sorted(
        get_effective_permissions(
            principal.role,
            token_permissions,
            permission_scope_version,
        ),
        key=lambda permission: permission.value,
    )
    return user.model_copy(
        update={
            "active_wallet_public_id": principal.active_wallet_public_id,
            "effective_permissions": effective_permissions,
            "delegate_public_id": principal.delegate_public_id,
        }
    )


def _extract_refresh_bearer_token(request: Request) -> str | None:
    """Pull a refresh JWT from the ``Authorization: Bearer`` header.

    ``POST /api/auth/refresh`` reads the bearer header
    FIRST and falls back to the ``refresh_token`` cookie. MCP clients
    without cookie jars exclusively use the header path; browser
    clients continue to hit the cookie path untouched.

    Args:
        request: FastAPI request whose ``Authorization`` header is
            inspected.

    Returns:
        The token string if a ``Bearer`` header is present, else
        ``None``. The returned value is NOT verified — callers still
        run it through :meth:`TokenManager.verify_token` per the
        existing refresh flow.
    """
    auth_header = request.headers.get("authorization")
    if not auth_header:
        return None
    parts = auth_header.split(None, 1)
    if len(parts) != 2 or parts[0].lower() != "bearer":
        return None
    token = parts[1].strip()
    return token or None


def _message_response(request: Request, message: str) -> MessageResponse:
    """Create a stamped MessageResponse with provenance from the REST tracker.

    Args:
        request: FastAPI request (provides access to app.state.rest_tracker).
        message: Response message string.

    Returns:
        MessageResponse with session_id and sequence_id from the REST tracker.
    """
    sid, seq, pid, ts = _mint_provenance(request)
    return MessageResponse(
        payload=message,
        session_id=sid,
        sequence_id=seq,
        public_id=pid,
        timestamp=ts,
    )


router = APIRouter(prefix="/auth", tags=["authentication"])


@router.post("/login", openapi_extra=openapi_schema(LoginRequest))
async def login(
    request: Request,
    response: Response,
    login_data: Annotated[LoginRequest, Depends(json_body(LoginRequest))],
    repo: Annotated[Repository, Depends(get_repository_dependency)],
) -> LoginResponse:
    """Authenticate user and create session.

    Sets access_token, refresh_token, and csrf_token cookies.
    When the caller passes ``?return_tokens=true``
    the access / refresh JWTs are ALSO embedded in the response body
    so MCP / CLI clients with no cookie jar can store them for
    subsequent ``Authorization: Bearer`` calls. Cookies are still
    set unconditionally for the browser flow.

    Args:
        request: FastAPI request.
        response: FastAPI response for setting cookies.
        login_data: Login credentials.
        repo: Repository used to persist the freshly-minted token
            pair in ``user_active_tokens`` so the
             DB-backed ``verify_token`` and the kill switch
            can see the rows on the next request.

    Returns:
        LoginResponse with user profile.

    Raises:
        HTTPException: 401 if credentials invalid.
    """
    enforce_failed_login_rate_limit(request, login_data.payload.username)
    user_service = get_user_service()
    settings = request.app.state.settings
    user = await user_service.authenticate_user(
        login_data.payload.username, login_data.payload.password
    )
    if not user:
        register_failed_login_attempt(request, login_data.payload.username)
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid username or password",
        )
    clear_failed_login_attempts(request, login_data.payload.username)
    token_manager = get_token_manager()
    principal = await user_service.build_auth_principal(user)
    try:
        token_pair = token_manager.create_tokens(
            principal,
            permissions=login_data.payload.permissions,
        )
    except PermissionScopeError as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail=str(exc),
        ) from exc
    await token_manager.persist_tokens(token_pair, principal.user_public_id, repo)
    csrf_manager = get_csrf_manager()
    csrf_token = csrf_manager.generate_token()
    cookie_secure = settings.session_secure
    cookie_samesite: Literal["lax", "strict"] = (
        "lax" if settings.session_same_site == "lax" else "strict"
    )
    response.set_cookie(
        key="refresh_token",
        value=token_pair.refresh_token,
        httponly=True,
        secure=cookie_secure,
        samesite=cookie_samesite,
        path=_AUTH_API_PATH,
        max_age=7 * 24 * 60 * 60,
    )
    response.set_cookie(
        key="access_token",
        value=token_pair.access_token,
        httponly=True,
        secure=cookie_secure,
        samesite=cookie_samesite,
    )
    response.set_cookie(
        key="csrf_token",
        value=csrf_token,
        httponly=False,
        secure=cookie_secure,
        samesite=cookie_samesite,
        path="/",
    )
    sid, seq, _pid, ts = _mint_provenance(request)
    requested_permissions = login_data.payload.permissions
    token_permissions = (
        None
        if requested_permissions is None
        else [permission.value for permission in requested_permissions]
    )
    user = _authenticated_session_profile(
        user,
        principal,
        token_permissions,
        PERMISSION_SCOPE_VERSION,
    )
    return_tokens = _should_return_tokens(request)
    login_payload = LoginData(
        session_id=sid,
        sequence_id=seq,
        public_id=str(uuid7()),
        timestamp=ts,
        message="Login successful",
        expires_in=15 * 60,
        user=user,
        access_token=token_pair.access_token if return_tokens else None,
        refresh_token=token_pair.refresh_token if return_tokens else None,
    )
    return LoginResponse(
        payload=login_payload,
        session_id=sid,
        sequence_id=seq,
        public_id=str(uuid7()),
        timestamp=ts,
    )


async def _apply_wallet_hint(
    payload: RefreshTokenPayload,
    principal: AuthPrincipal,
    repo: Repository,
) -> AuthPrincipal:
    """Apply an optional wallet hint to the authenticated principal.

    Permission-derived membership validation: a named set carrying
    ``IMPERSONATE_OPERATOR`` sees every active wallet via
    ``list_active_wallets``; other sets see only wallets their operator
    memberships grant access to via
    ``list_accessible_wallets_for_operators``. A hint that doesn't
    match the caller's visibility returns 404 with a uniform message
    so cross-tenant existence is not leaked.
    """
    if payload.active_wallet_public_id is not None:
        now = datetime.now(UTC)
        if has_effective_permission(
            principal.role,
            principal.permissions,
            principal.permission_scope_version,
            Permission.IMPERSONATE_OPERATOR,
        ):
            rows = await repo.list_active_wallets(now)
        else:
            rows = await repo.list_accessible_wallets_for_operators(
                principal.operator_public_ids, now
            )
        if payload.active_wallet_public_id not in {w["public_id"] for w in rows}:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="wallet not found",
            )
        return principal.model_copy(
            update={"active_wallet_public_id": payload.active_wallet_public_id}
        )
    if payload.clear_active_wallet:
        return principal.model_copy(update={"active_wallet_public_id": None})
    return principal


async def _load_refresh_identity(
    request: Request,
    repo: Repository,
) -> _RefreshIdentity:
    """Verify one refresh token and resolve its current account principal."""
    refresh_token_value = _extract_refresh_bearer_token(request) or request.cookies.get(
        "refresh_token"
    )
    if not refresh_token_value:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Refresh token not found",
        )
    token_manager = get_token_manager()
    token_data = await token_manager.verify_token_with_db(refresh_token_value, repo)
    if not token_data or not token_data.jti.startswith("refresh_"):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid refresh token",
        )
    user_service = get_user_service()
    user = await user_service.get_user_by_id(token_data.sub)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=_USER_NOT_FOUND,
        )
    principal = await user_service.build_auth_principal(user)
    principal = principal.model_copy(
        update={"active_wallet_public_id": token_data.active_wallet_public_id}
    )
    return _RefreshIdentity(token_manager, token_data, user, principal)


def _refresh_permissions(
    principal: AuthPrincipal,
    token_data: TokenClaims,
) -> set[Permission] | None:
    """Resolve refresh-token permissions under the carried scope version."""
    if token_data.permission_scope_version is None:
        return None
    return get_effective_permissions(
        principal.role,
        token_data.permissions or [],
        token_data.permission_scope_version,
    )


async def _rotate_refresh_tokens(
    identity: _RefreshIdentity,
    principal: AuthPrincipal,
    permissions: set[Permission] | None,
    repo: Repository,
) -> _RefreshRotation:
    """Rotate once or adopt the idempotent winner of a concurrent rotation."""
    new_token_pair = identity.token_manager.create_tokens(
        principal,
        session_id=identity.claims.sid,
        permissions=permissions,
    )
    rotated_pair = await identity.token_manager.rotate_tokens(
        new_token_pair,
        principal.user_public_id,
        identity.claims.jti,
        repo,
    )
    if rotated_pair is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Refresh token already redeemed",
        )
    permission_values = (
        None if permissions is None else [permission.value for permission in permissions]
    )
    permission_scope_version: int | None = PERMISSION_SCOPE_VERSION
    if rotated_pair is new_token_pair:
        identity.token_manager.blacklist_token(identity.claims.jti)
    else:
        winner_claims = identity.token_manager.verify_token(rotated_pair.access_token)
        if winner_claims is not None:
            principal = principal.model_copy(
                update={"active_wallet_public_id": winner_claims.active_wallet_public_id}
            )
            permission_values = winner_claims.permissions
            permission_scope_version = winner_claims.permission_scope_version
    return _RefreshRotation(
        rotated_pair,
        principal,
        permission_values,
        permission_scope_version,
    )


@router.post(
    "/refresh",
    openapi_extra=openapi_schema(RefreshTokenRequest, required=False),
)
async def refresh_token(
    request: Request,
    response: Response,
    body: Annotated[RefreshTokenRequest | None, Depends(optional_json_body(RefreshTokenRequest))],
    repo: Annotated[Repository, Depends(get_repository_dependency)],
) -> RefreshResponse:
    """Refresh session tokens with optional wallet-scope change.

    Order (verify → parse → validate → rotate → blacklist)
    1. Verify refresh-token signature + blacklist status.
    2. Parse optional body (422 on malformed UUID7 or mutually
       exclusive fields — fires before any DB work via Pydantic
       field + model validators on ``RefreshTokenPayload``).
    3. Role-branched wallet-membership validation (404 when the
       hinted wallet is outside the caller's visibility).
    4. Mint new tokens from the post-validation principal, then
       call :meth:`TokenManager.rotate_tokens` which revokes the
       old refresh row AND inserts the new pair inside a SINGLE DB
       transaction. Outcome matrix
           Rowcount == 1 (atomic success): route continues.
           Rowcount == 0 (replay / unknown JTI): transaction
             rolls back and returns False → route raises 401.
           DB exception (connection reset, integrity error on
             the new-pair insert): the ``async with session()``
             scope rolls back; the exception propagates out of
             ``rotate_tokens`` and surfaces as a 5xx so the
             client can retry with the original refresh JWT
             no cookies / no successor tokens leaked.
    5. Seed the in-memory JTI blacklist AFTER the rotation commits
       so the grace-period window starts at the post-commit moment
       (cross-instance consistency).
    Response ``user.active_wallet_public_id`` is projected from
    ``principal.active_wallet_public_id`` (NOT ``token_data``) so
    a hinted refresh surfaces the NEW wallet, matching the
    freshly-minted token claims.
    Empty body preserved byte-identically for the three zero-body
    callers (``stores/auth.refreshToken``, WS ticket refresh
    ``apiClient.refreshAndRetry``): ``body is None`` → empty
    ``RefreshTokenPayload()`` → validation is a no-op.
    The refresh JWT is read from the
    ``Authorization: Bearer`` header FIRST and the ``refresh_token``
    cookie second. MCP / CLI clients without cookie jars use the
    header path exclusively. Passing ``?return_tokens=true``
    embeds the newly-minted access + refresh JWTs in the response
    body (in addition to the existing cookie set) so the same
    clients can rotate tokens without maintaining a cookie store.

    Args:
        request: FastAPI request with refresh_token cookie OR
            ``Authorization: Bearer`` header.
        response: FastAPI response for setting cookies.
        body: Optional refresh-token command envelope (``None`` on
            empty body).
        repo: Repository for wallet-membership lookups + atomic
            refresh rotation.

    Returns:
        RefreshResponse with new tokens, WS token, CSRF token, and
        user profile carrying ``active_wallet_public_id``.

    Raises:
        HTTPException: 401 if refresh token invalid / missing OR
            the refresh JTI has already been redeemed (replay).
            404 when the wallet hint is outside caller visibility.
        Exception: DB errors during ``rotate_tokens`` (integrity
            violation on the new pair, connection reset, etc.)
            propagate out — the atomic transaction has already
            rolled back so the old refresh JWT remains usable for
            retry. Surfaces to the client as a 5xx via FastAPI's
            default exception handler.
    """
    settings = request.app.state.settings
    identity = await _load_refresh_identity(request, repo)
    payload = RefreshTokenPayload() if body is None else body.payload
    principal = await _apply_wallet_hint(payload, identity.principal, repo)
    permissions = _refresh_permissions(principal, identity.claims)
    rotation = await _rotate_refresh_tokens(identity, principal, permissions, repo)
    rotated_pair = rotation.token_pair
    principal = rotation.principal
    csrf_manager = get_csrf_manager()
    csrf_token = csrf_manager.generate_token()
    cookie_secure = settings.session_secure
    cookie_samesite: Literal["lax", "strict"] = (
        "lax" if settings.session_same_site == "lax" else "strict"
    )
    response.set_cookie(
        key="refresh_token",
        value=rotated_pair.refresh_token,
        httponly=True,
        secure=cookie_secure,
        samesite=cookie_samesite,
        path=_AUTH_API_PATH,
        max_age=7 * 24 * 60 * 60,
    )
    response.set_cookie(
        key="access_token",
        value=rotated_pair.access_token,
        httponly=True,
        secure=cookie_secure,
        samesite=cookie_samesite,
    )
    response.set_cookie(
        key="csrf_token",
        value=csrf_token,
        httponly=False,
        secure=cookie_secure,
        samesite=cookie_samesite,
        path="/",
    )
    ws_token_service = get_ws_token_service()
    session_id = identity.claims.sid
    ws_token_result = ws_token_service.generate(
        user_id=identity.user.username, session_id=session_id
    )
    sid, seq, _pid, ts = _mint_provenance(request)
    user = _authenticated_session_profile(
        identity.user,
        principal,
        rotation.permission_values,
        rotation.permission_scope_version,
    )
    return_tokens = _should_return_tokens(request)
    refresh_data = RefreshData(
        session_id=sid,
        sequence_id=seq,
        public_id=str(uuid7()),
        timestamp=ts,
        message="session refreshed",
        ws_token=ws_token_result.token,
        ws_token_exp=ws_token_result.expires_at,
        csrf_token=csrf_token,
        user=user,
        access_token=rotated_pair.access_token if return_tokens else None,
        refresh_token=rotated_pair.refresh_token if return_tokens else None,
    )
    return RefreshResponse(
        payload=refresh_data,
        session_id=sid,
        sequence_id=seq,
        public_id=str(uuid7()),
        timestamp=ts,
    )


def _get_authenticated_token_claims(
    request: Request,
    _user: Annotated[AuthPrincipal, Depends(require_authentication)],
) -> TokenClaims:
    """Surface the access-token claims attached by ``get_current_user``.

    The :func:`get_current_user` dependency stores the verified
    :class:`TokenClaims` on ``request.state.token_data`` after a
    successful Bearer / cookie auth. This dependency lifts that
    state into a route-level parameter so handlers can pull the
    session-id (``sid``) without reaching into ``request.state``
    directly. Routes that need the session-id chain
    ``Depends(_get_authenticated_token_claims)`` after
    ``Depends(require_authentication)``; the latter is also passed
    here so test suites can override this dependency in isolation
    without short-circuiting the auth chain.

    Args:
        request: FastAPI request whose ``state.token_data`` is read.
        _user: Authenticated principal (forces the auth chain to run
            before we look at ``request.state``).

    Returns:
        The :class:`TokenClaims` attached during authentication.

    Raises:
        HTTPException: 500 if the auth chain succeeded but
            ``request.state.token_data`` is unset or of the wrong
            type — points at a regression in the auth chain itself.
    """
    claims = getattr(request.state, "token_data", None)
    if not isinstance(claims, TokenClaims):
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Authenticated request is missing token claims state",
        )
    return claims


@router.post("/ws_token")
@limiter.limit(WS_TOKEN_RATE_LIMIT)
async def issue_ws_token(
    request: Request,
    current_user: Annotated[AuthPrincipal, Depends(require_authentication)],
    token_claims: Annotated[TokenClaims, Depends(_get_authenticated_token_claims)],
) -> WsTokenResponse:
    """Mint a one-shot WebSocket token from the access JWT's session.

    Authenticates the caller via the access bearer (header or cookie)
    via :func:`require_authentication` and returns a fresh ws_token
    bound to the access JWT's session.

    Per-IP rate limited at ``WS_TOKEN_RATE_LIMIT`` to cap reconnect-
    storm minting from a single source. The TTL of the returned
    ws_token is dictated by ``AppSettings.ws_token_ttl_seconds`` and
    surfaces both as ``ws_token_exp`` (absolute) and ``expires_in``
    (relative seconds) for client convenience.

    Args:
        request: FastAPI request (provides REST tracker provenance).
        current_user: Authenticated principal from the access bearer.
        token_claims: Access-token claims surfaced by
            :func:`_get_authenticated_token_claims`; supplies the
            session-id used to bind the minted ws_token.

    Returns:
        WsTokenResponse wrapping a single :class:`WsTokenData` payload
        with the fresh token, absolute expiration, and seconds-to-expiry.

    Raises:
        HTTPException: 401 from :func:`require_authentication` when
            the access bearer is absent or invalid; 429 from the
            limiter when the per-IP minute budget is exhausted.
    """
    ws_token_service = get_ws_token_service()
    ws_token_result = ws_token_service.generate(
        user_id=current_user.username,
        session_id=token_claims.sid,
    )
    sid, seq, _pid, ts = _mint_provenance(request)
    expires_in = max(0, int((ws_token_result.expires_at - ts).total_seconds()))
    ws_token_data = WsTokenData(
        session_id=sid,
        sequence_id=seq,
        public_id=str(uuid7()),
        timestamp=ts,
        message="ws_token issued",
        ws_token=ws_token_result.token,
        ws_token_exp=ws_token_result.expires_at,
        expires_in=expires_in,
    )
    return WsTokenResponse(
        payload=ws_token_data,
        session_id=sid,
        sequence_id=seq,
        public_id=str(uuid7()),
        timestamp=ts,
    )


@router.get("/me")
async def get_current_user_profile(
    request: Request,
    current_user: Annotated[AuthPrincipal, Depends(require_authentication)],
) -> UserResponse:
    """Get current user's profile.

    Args:
        request: FastAPI request (provides REST tracker for provenance).
        current_user: Authenticated principal from dependency.

    Returns:
        UserResponse wrapping the current user's profile.

    Raises:
        HTTPException: 404 if user not found in database.
    """
    user_service = get_user_service()
    user = await user_service.get_user_with_operators(current_user.username)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=_USER_NOT_FOUND,
        )
    sid, seq, pid, ts = _mint_provenance(request)
    user = _authenticated_session_profile(
        user,
        current_user,
        current_user.permissions,
        current_user.permission_scope_version,
    )
    return UserResponse(
        session_id=sid,
        sequence_id=seq,
        public_id=pid,
        timestamp=ts,
        payload=user,
    )


@router.post("/me/update", openapi_extra=openapi_schema(UpdateAuthMeRequest))
async def update_current_user_preferences(
    request: Request,
    current_user: Annotated[AuthPrincipal, Depends(require_authentication)],
    _csrf: Annotated[None, Depends(validate_csrf_token)],
    body: Annotated[UpdateAuthMeRequest, Depends(json_body(UpdateAuthMeRequest))],
) -> UserResponse:
    """Update the caller's self-service preferences.

    Applies the caller's ``default_language`` preference. Mirrors the
    codebase's ``POST + verb`` admin endpoint shape
    (:func:`update_user` at ``POST /api/users/{user_id}/update``) so
    CORS stays untouched - ``POST`` is already in the allowlist and
    this route does not introduce REST ``PATCH`` semantics.

    Args:
        request: FastAPI request (provides REST tracker for provenance).
        current_user: Authenticated principal from dependency.
        _csrf: CSRF guard (cookie+header double-submit).
        body: Validated request envelope with ``default_language``.

    Returns:
        UserResponse wrapping the updated user profile.

    Raises:
        HTTPException: 404 if the caller's user row was not found
        (concurrent admin deactivation between auth-dep resolution
        and update is the only known way this surfaces).
    """
    user_service = get_user_service()
    updated_user = await user_service.update_self_preferences(
        user_id=current_user.username,
        default_language=body.payload.default_language,
    )
    if not updated_user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=_USER_NOT_FOUND,
        )
    sid, seq, pid, ts = _mint_provenance(request)
    updated_user = _authenticated_session_profile(
        updated_user,
        current_user,
        current_user.permissions,
        current_user.permission_scope_version,
    )
    return UserResponse(
        session_id=sid,
        sequence_id=seq,
        public_id=pid,
        timestamp=ts,
        payload=updated_user,
    )


@router.post("/logout")
async def logout(
    request: Request,
    response: Response,
) -> MessageResponse:
    """Logout and invalidate session.

    Clears all authentication cookies and invalidates tokens.

    Args:
        request: FastAPI request.
        response: FastAPI response for clearing cookies.

    Returns:
        MessageResponse confirming logout.
    """
    settings = request.app.state.settings
    refresh_token = request.cookies.get("refresh_token")
    access_token = request.cookies.get("access_token")
    csrf_token = request.cookies.get("csrf_token")
    token_manager = get_token_manager()
    csrf_manager = get_csrf_manager()
    if refresh_token:
        token_manager.invalidate_token(refresh_token)
    if access_token:
        token_manager.invalidate_token(access_token)
    if csrf_token:
        csrf_manager.invalidate_token(csrf_token)
    cookie_secure = settings.session_secure
    cookie_samesite: Literal["lax", "strict"] = (
        "lax" if settings.session_same_site == "lax" else "strict"
    )
    response.set_cookie(
        key="refresh_token",
        value="",
        httponly=True,
        secure=cookie_secure,
        samesite=cookie_samesite,
        path=_AUTH_API_PATH,
        max_age=0,
    )
    response.set_cookie(
        key="refresh_token",
        value="",
        httponly=True,
        secure=cookie_secure,
        samesite=cookie_samesite,
        path="/",
        max_age=0,
    )
    response.set_cookie(
        key="access_token",
        value="",
        httponly=True,
        secure=cookie_secure,
        samesite=cookie_samesite,
        max_age=0,
    )
    response.set_cookie(
        key="access_token",
        value="",
        httponly=True,
        secure=cookie_secure,
        samesite=cookie_samesite,
        path="/",
        max_age=0,
    )
    response.set_cookie(
        key="csrf_token",
        value="",
        httponly=False,
        secure=cookie_secure,
        samesite=cookie_samesite,
        path="/",
        max_age=0,
    )
    return _message_response(request, "Logged out successfully")


@router.get("/users")
async def get_users(
    request: Request,
    current_user: Annotated[AuthPrincipal, Depends(require_permission(Permission.MANAGE_USERS))],
    include_inactive: bool = False,
    as_of: datetime | None = None,
) -> UserListResponse:
    """List all users in the system.

    Args:
        request: FastAPI request (provides REST tracker for provenance).
        current_user: Authenticated user with MANAGE_USERS permission.
        include_inactive: Whether to include deactivated users.
        as_of: Optional point-in-time query timestamp (UTC).

    Returns:
        List of user profiles with total count.
    """
    user_service = get_user_service()
    users = await user_service.get_all_users(include_inactive=include_inactive, as_of=as_of)
    sid, seq, pid, ts = _mint_provenance(request)
    return UserListResponse(
        session_id=sid,
        sequence_id=seq,
        public_id=pid,
        timestamp=ts,
        payload=users,
        count=len(users),
    )


@router.post("/users", openapi_extra=openapi_schema(CreateUserRequest))
async def create_user(
    request: Request,
    current_user: Annotated[AuthPrincipal, Depends(require_permission(Permission.MANAGE_USERS))],
    _csrf: Annotated[None, Depends(validate_csrf_token)],
    user_data: Annotated[CreateUserRequest, Depends(json_body(CreateUserRequest))],
) -> UserResponse:
    """Create a new user account.

    Args:
        request: FastAPI request (provides REST tracker for provenance).
        user_data: User creation payload with username, password, etc.
        current_user: Authenticated user with MANAGE_USERS permission.

    Returns:
        UserResponse wrapping the created user profile.

    Raises:
        HTTPException: If user creation fails.
    """
    user_service = get_user_service()
    try:
        new_user = await user_service.create_user(
            username=user_data.payload.username,
            password=user_data.payload.password,
            email=user_data.payload.email,
            role=user_data.payload.role,
            is_active=user_data.payload.is_active,
        )
        sid, seq, pid, ts = _mint_provenance(request)
        return UserResponse(
            session_id=sid,
            sequence_id=seq,
            public_id=pid,
            timestamp=ts,
            payload=new_user,
        )
    except ValueError as e:
        logger.warning("User creation failed: {}", str(e))
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail="User creation failed"
        ) from e


@router.post("/users/{user_id}/update", openapi_extra=openapi_schema(UpdateUserRequest))
async def update_user(
    request: Request,
    user_id: str,
    current_user: Annotated[AuthPrincipal, Depends(require_permission(Permission.MANAGE_USERS))],
    _csrf: Annotated[None, Depends(validate_csrf_token)],
    user_data: Annotated[UpdateUserRequest, Depends(json_body(UpdateUserRequest))],
) -> UserResponse:
    """Update an existing user's profile.

    Args:
        request: FastAPI request (provides REST tracker for provenance).
        user_id: Target user ID.
        user_data: Update payload with optional email, role, is_active.
        current_user: Authenticated user with MANAGE_USERS permission.

    Returns:
        UserResponse wrapping the updated user profile.

    Raises:
        HTTPException: If user not found.
    """
    user_service = get_user_service()
    updated_user = await user_service.update_user(
        user_id=user_id,
        email=user_data.payload.email,
        role=user_data.payload.role,
        is_active=user_data.payload.is_active,
    )
    if not updated_user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail=f"User with ID '{user_id}' not found"
        )
    sid, seq, pid, ts = _mint_provenance(request)
    return UserResponse(
        session_id=sid,
        sequence_id=seq,
        public_id=pid,
        timestamp=ts,
        payload=updated_user,
    )


@router.post("/users/{user_id}/deactivate", openapi_extra=openapi_schema(DeactivateUserRequest))
async def deactivate_user(
    request: Request,
    user_id: str,
    current_user: Annotated[AuthPrincipal, Depends(require_permission(Permission.MANAGE_USERS))],
    _csrf: Annotated[None, Depends(validate_csrf_token)],
    body: Annotated[DeactivateUserRequest, Depends(json_body(DeactivateUserRequest))],
) -> MessageResponse:
    """Deactivate a user account through the canonical kill-switch flow.

    Resolves the username path segment to a `user_public_id` and
    delegates to `UserService.deactivate_user`, which is the SOLE
    publisher of `admin.user_deactivated`. The service
    layer also drives `TokenManager.revoke_user_sessions` synchronously
    so the local instance rejects subsequent requests immediately.

    Args:
        request: FastAPI request (provides REST tracker for provenance).
        user_id: Target user identified by username in the URL path.
        body: Request envelope; `body.payload.reason` is forwarded to
            the bus event for audit.
        current_user: Authenticated user with MANAGE_USERS permission.

    Returns:
        Success message.

    Raises:
        HTTPException: 400 when the caller targets their own account
            404 when no active user matches `user_id`.
    """
    user_service = get_user_service()
    if user_id == current_user.username:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail="Cannot deactivate your own account"
        )
    profile = await user_service.get_user_by_id(user_id)
    if profile is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail=f"User with ID '{user_id}' not found"
        )
    success = await user_service.deactivate_user(profile.public_id, body.payload.reason)
    if not success:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail=f"User with ID '{user_id}' not found"
        )
    return _message_response(request, f"User '{user_id}' has been deactivated")


@router.post(
    "/users/{user_id}/change-password", openapi_extra=openapi_schema(ChangePasswordRequest)
)
@limiter.limit(ACCOUNT_CHANGE_RATE_LIMIT)
async def change_user_password(
    request: Request,
    user_id: str,
    current_user: Annotated[AuthPrincipal, Depends(require_authentication)],
    _csrf: Annotated[None, Depends(validate_csrf_token)],
    password_data: Annotated[ChangePasswordRequest, Depends(json_body(ChangePasswordRequest))],
) -> MessageResponse:
    """Change a user's password.

    Users can change their own password. Admins can change any password.

    Args:
        request: FastAPI request (used by rate limiter).
        user_id: Target user ID.
        password_data: Current and new password.
        current_user: Authenticated user.

    Returns:
        Success message.

    Raises:
        HTTPException: If forbidden or invalid current password.
    """
    user_service = get_user_service()
    if user_id != current_user.username:
        user_permissions = get_effective_permissions(
            current_user.role,
            current_user.permissions,
            current_user.permission_scope_version,
        )
        if Permission.MANAGE_USERS not in user_permissions:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="You can only change your own password",
            )
    success = await user_service.change_password(
        user_id=user_id,
        old_password=password_data.payload.current_password,
        new_password=password_data.payload.new_password,
    )
    if not success:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid current password or user not found",
        )
    return _message_response(request, "Password changed successfully")


@router.post(
    "/users/{user_id}/admin-reset-password", openapi_extra=openapi_schema(AdminResetPasswordRequest)
)
@limiter.limit(ACCOUNT_RESET_RATE_LIMIT)
async def admin_reset_user_password(
    request: Request,
    user_id: str,
    current_user: Annotated[AuthPrincipal, Depends(require_permission(Permission.MANAGE_USERS))],
    _csrf: Annotated[None, Depends(validate_csrf_token)],
    password_data: Annotated[
        AdminResetPasswordRequest, Depends(json_body(AdminResetPasswordRequest))
    ],
) -> MessageResponse:
    """Admin endpoint to reset a user's password without current password.

    Args:
        request: FastAPI request (used by rate limiter).
        user_id: Target user ID.
        password_data: New password.
        current_user: Admin user with MANAGE_USERS permission.
        _csrf: CSRF token validation.

    Returns:
        Success message.

    Raises:
        HTTPException: If forbidden or user not found.
    """
    user_service = get_user_service()
    try:
        await user_service.admin_reset_password(user_id, password_data.payload.new_password)
        return _message_response(request, f"Password reset successfully for user {user_id}")
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=_USER_NOT_FOUND,
        ) from e
    except Exception as e:
        logger.error("Failed to reset password for user {}: {}", user_id, str(e))
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to reset password",
        ) from e
