"""Authentication routes module.

This module provides FastAPI routes for user authentication
including login, logout, token refresh, and user management.
"""

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
from snapper.auth.domain.permissions import ROLE_PERMISSIONS
from snapper.auth.domain.permissions import Permission
from snapper.auth.domain.roles import UserRole
from snapper.auth.schemas.principal import AuthPrincipal
from snapper.auth.schemas.requests import AdminResetPasswordRequest
from snapper.auth.schemas.requests import ChangePasswordRequest
from snapper.auth.schemas.requests import CreateUserRequest
from snapper.auth.schemas.requests import DeactivateUserRequest
from snapper.auth.schemas.requests import LoginRequest
from snapper.auth.schemas.requests import RefreshTokenPayload
from snapper.auth.schemas.requests import RefreshTokenRequest
from snapper.auth.schemas.requests import UpdateUserRequest
from snapper.auth.schemas.responses import LoginData
from snapper.auth.schemas.responses import LoginResponse
from snapper.auth.schemas.responses import RefreshData
from snapper.auth.schemas.responses import RefreshResponse
from snapper.auth.schemas.responses import UserListResponse
from snapper.auth.schemas.responses import UserResponse
from snapper.auth.tokens import get_token_manager
from snapper.auth.user_service import get_user_service
from snapper.data.repository import Repository
from snapper.messaging.infrastructure.publisher import SequenceTracker
from snapper.server.dependencies import get_repository_dependency
from snapper.server.json_body import json_body
from snapper.server.json_body import openapi_schema
from snapper.server.json_body import optional_json_body
from snapper.server.rate_limiting import PASSWORD_CHANGE_RATE_LIMIT
from snapper.server.rate_limiting import PASSWORD_RESET_RATE_LIMIT
from snapper.server.rate_limiting import clear_failed_login_attempts
from snapper.server.rate_limiting import enforce_failed_login_rate_limit
from snapper.server.rate_limiting import limiter
from snapper.server.rate_limiting import register_failed_login_attempt

_AUTH_API_PATH = "/api/auth"
_REST_STREAM = "rest.control"
_USER_NOT_FOUND = "User not found"


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
    (unchanged). Per plan §3.7.

    Args:
        request: FastAPI request whose ``query_params`` are consulted.

    Returns:
        ``True`` if ``?return_tokens=true`` (case-insensitive),
        ``False`` otherwise — including the canonical absent-param
        default, which preserves the cookie-only browser flow.
    """
    value = request.query_params.get("return_tokens", "").strip().lower()
    return value == "true"


def _extract_refresh_bearer_token(request: Request) -> str | None:
    """Pull a refresh JWT from the ``Authorization: Bearer`` header.

    Per plan §3.7: ``POST /api/auth/refresh`` reads the bearer header
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
) -> LoginResponse:
    """Authenticate user and create session.

    Sets access_token, refresh_token, and csrf_token cookies.

    Per plan §3.7: when the caller passes ``?return_tokens=true``,
    the access / refresh JWTs are ALSO embedded in the response body
    so MCP / CLI clients with no cookie jar can store them for
    subsequent ``Authorization: Bearer`` calls. Cookies are still
    set unconditionally for the browser flow.

    Args:
        request: FastAPI request.
        response: FastAPI response for setting cookies.
        login_data: Login credentials.

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
    token_pair = token_manager.create_tokens(principal)
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
    user = user.model_copy(update={"active_wallet_public_id": principal.active_wallet_public_id})
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

    Role-branched membership validation (plan §2.5, R12 triple-convergent
    security fix): ADMIN sees every active wallet via
    ``list_active_wallets``; non-admins see only wallets their
    operator memberships grant access to via
    ``list_accessible_wallets_for_operators``. A hint that doesn't
    match the caller's visibility returns 404 with a uniform message
    so cross-tenant existence is not leaked.
    """
    if payload.active_wallet_public_id is not None:
        now = datetime.now(UTC)
        if principal.role == UserRole.ADMIN:
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

    Order (plan §2.5 R14 gpt-5.4 fix #3 — verify → parse → validate →
    blacklist → mint):

    1. Verify refresh-token signature + blacklist status.
    2. Parse optional body (422 on malformed UUID7 or mutually
       exclusive fields — fires before any DB work via Pydantic
       field + model validators on ``RefreshTokenPayload``).
    3. Role-branched wallet-membership validation (404 when the
       hinted wallet is outside the caller's visibility).
    4. Blacklist the old refresh token ONLY after every validation
       passes — a rejected hint leaves the old token usable for a
       retry with a valid hint.
    5. Mint new tokens from the post-validation principal. Response
       ``user.active_wallet_public_id`` is projected from
       ``principal.active_wallet_public_id`` (NOT ``token_data``)
       so a hinted refresh surfaces the NEW wallet, matching the
       freshly-minted token claims.

    Empty body preserved byte-identically for the three zero-body
    callers (``stores/auth.refreshToken``, WS ticket refresh,
    ``apiClient.refreshAndRetry``): ``body is None`` → empty
    ``RefreshTokenPayload()`` → validation is a no-op.

    Per plan §3.7: the refresh JWT is read from the
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
        repo: Repository for wallet-membership lookups.

    Returns:
        RefreshResponse with new tokens, WS token, CSRF token, and
        user profile carrying ``active_wallet_public_id``.

    Raises:
        HTTPException: 401 if refresh token invalid / missing,
            404 when the wallet hint is outside caller visibility.
    """
    settings = request.app.state.settings
    refresh_token_value = _extract_refresh_bearer_token(request) or request.cookies.get(
        "refresh_token"
    )
    if not refresh_token_value:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Refresh token not found",
        )
    token_manager = get_token_manager()
    token_data = token_manager.verify_token(refresh_token_value)
    if not token_data:
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
    payload = RefreshTokenPayload() if body is None else body.payload
    principal = await _apply_wallet_hint(payload, principal, repo)
    token_manager.blacklist_token(token_data.jti)
    new_token_pair = token_manager.create_tokens(
        principal,
        session_id=token_data.sid,
    )
    csrf_manager = get_csrf_manager()
    csrf_token = csrf_manager.generate_token()
    cookie_secure = settings.session_secure
    cookie_samesite: Literal["lax", "strict"] = (
        "lax" if settings.session_same_site == "lax" else "strict"
    )
    response.set_cookie(
        key="refresh_token",
        value=new_token_pair.refresh_token,
        httponly=True,
        secure=cookie_secure,
        samesite=cookie_samesite,
        path=_AUTH_API_PATH,
        max_age=7 * 24 * 60 * 60,
    )
    response.set_cookie(
        key="access_token",
        value=new_token_pair.access_token,
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
    session_id = token_data.sid
    ws_token_result = ws_token_service.generate(user_id=user.username, session_id=session_id)
    sid, seq, _pid, ts = _mint_provenance(request)
    user = user.model_copy(update={"active_wallet_public_id": principal.active_wallet_public_id})
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
        access_token=new_token_pair.access_token if return_tokens else None,
        refresh_token=new_token_pair.refresh_token if return_tokens else None,
    )
    return RefreshResponse(
        payload=refresh_data,
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
    user = user.model_copy(update={"active_wallet_public_id": current_user.active_wallet_public_id})
    return UserResponse(
        session_id=sid,
        sequence_id=seq,
        public_id=pid,
        timestamp=ts,
        payload=user,
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
    _body: Annotated[DeactivateUserRequest, Depends(json_body(DeactivateUserRequest))],
) -> MessageResponse:
    """Deactivate a user account.

    Args:
        request: FastAPI request (provides REST tracker for provenance).
        user_id: Target user ID to deactivate.
        _body: Request envelope with provenance (payload is empty).
        current_user: Authenticated user with MANAGE_USERS permission.

    Returns:
        Success message.

    Raises:
        HTTPException: If user not found or trying to deactivate self.
    """
    user_service = get_user_service()
    if user_id == current_user.username:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail="Cannot deactivate your own account"
        )
    success = await user_service.delete_user(user_id)
    if not success:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail=f"User with ID '{user_id}' not found"
        )
    return _message_response(request, f"User '{user_id}' has been deactivated")


@router.post(
    "/users/{user_id}/change-password", openapi_extra=openapi_schema(ChangePasswordRequest)
)
@limiter.limit(PASSWORD_CHANGE_RATE_LIMIT)
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
        user_permissions = ROLE_PERMISSIONS.get(current_user.role, set())
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
@limiter.limit(PASSWORD_RESET_RATE_LIMIT)
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
