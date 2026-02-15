"""Authentication routes module.

This module provides FastAPI routes for user authentication
including login, logout, token refresh, and user management.
"""

from typing import Annotated
from typing import Literal

from fastapi import APIRouter
from fastapi import Depends
from fastapi import HTTPException
from fastapi import Request
from fastapi import Response
from fastapi import status
from loguru import logger
from sqlalchemy import select

from snapper.api.auth.services.ws_token_service import get_ws_token_service
from snapper.api.schemas.base import MessageResponse
from snapper.auth.dependencies import get_csrf_manager
from snapper.auth.dependencies import require_authentication
from snapper.auth.dependencies import require_permission
from snapper.auth.domain.permissions import ROLE_PERMISSIONS
from snapper.auth.domain.permissions import Permission
from snapper.auth.schemas.requests import AdminResetPasswordRequest
from snapper.auth.schemas.requests import ChangePasswordRequest
from snapper.auth.schemas.requests import CreateUserRequest
from snapper.auth.schemas.requests import LoginRequest
from snapper.auth.schemas.requests import UpdateUserRequest
from snapper.auth.schemas.responses import LoginResponse
from snapper.auth.schemas.responses import RefreshResponse
from snapper.auth.schemas.responses import UserListResponse
from snapper.auth.schemas.user import UserProfile
from snapper.auth.tokens import get_token_manager
from snapper.auth.user_service import get_user_service
from snapper.data.models import User
from snapper.server.rate_limiting import PASSWORD_CHANGE_RATE_LIMIT
from snapper.server.rate_limiting import PASSWORD_RESET_RATE_LIMIT
from snapper.server.rate_limiting import clear_failed_login_attempts
from snapper.server.rate_limiting import enforce_failed_login_rate_limit
from snapper.server.rate_limiting import limiter
from snapper.server.rate_limiting import register_failed_login_attempt

_AUTH_API_PATH = "/api/auth"

router = APIRouter(prefix="/auth", tags=["authentication"])


@router.post("/login")
async def login(
    request: Request,
    response: Response,
    login_data: LoginRequest,
) -> LoginResponse:
    """Authenticate user and create session.

    Sets access_token, refresh_token, and csrf_token cookies.

    Args:
        request: FastAPI request.
        response: FastAPI response for setting cookies.
        login_data: Login credentials.

    Returns:
        LoginResponse with user profile.

    Raises:
        HTTPException: 401 if credentials invalid.
    """
    enforce_failed_login_rate_limit(request, login_data.username)
    user_service = get_user_service()
    settings = request.app.state.settings
    user = await user_service.authenticate_user(login_data.username, login_data.password)
    if not user:
        register_failed_login_attempt(request, login_data.username)
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid username or password",
        )
    clear_failed_login_attempts(request, login_data.username)
    token_manager = get_token_manager()
    token_pair = token_manager.create_tokens(user)
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
    return LoginResponse(
        message="Login successful",
        expires_in=15 * 60,
        user=user,
    )


@router.post("/refresh")
async def refresh_token(
    request: Request,
    response: Response,
) -> RefreshResponse:
    """Refresh session tokens.

    Generates new access/refresh tokens and WebSocket token.

    Args:
        request: FastAPI request with refresh_token cookie.
        response: FastAPI response for setting cookies.

    Returns:
        RefreshResponse with new tokens and user profile.

    Raises:
        HTTPException: 401 if refresh token invalid.
    """
    settings = request.app.state.settings
    refresh_token = request.cookies.get("refresh_token")
    if not refresh_token:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Refresh token not found",
        )
    token_manager = get_token_manager()
    token_data = token_manager.verify_token(refresh_token)
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
            detail="User not found",
        )
    token_manager.blacklist_token(token_data.jti)
    new_token_pair = token_manager.create_tokens(user, session_id=token_data.sid)
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
    ws_token_result = ws_token_service.generate(user_id=user.id, session_id=session_id)
    return RefreshResponse(
        message="session refreshed",
        ws_token=ws_token_result.token,
        ws_token_exp=ws_token_result.expires_at,
        csrf_token=csrf_token,
        user=user,
    )


@router.get("/me")
async def get_current_user_profile(
    current_user: Annotated[UserProfile, Depends(require_authentication)],
) -> UserProfile:
    """Get current user's profile.

    Args:
        current_user: Authenticated user from dependency.

    Returns:
        Current user's UserProfile.
    """
    return current_user


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
    return MessageResponse(message="Logged out successfully")


@router.get("/me")
async def get_current_user_info(
    current_user: Annotated[UserProfile, Depends(require_authentication)],
) -> UserProfile:
    """Get profile information for the currently authenticated user.

    Args:
        current_user: Authenticated user from dependency.

    Returns:
        Current user's UserProfile.
    """
    return current_user


@router.get("/users")
async def get_users(
    current_user: Annotated[UserProfile, Depends(require_permission(Permission.MANAGE_USERS))],
    include_inactive: bool = False,
) -> UserListResponse:
    """List all users in the system.

    Args:
        current_user: Authenticated user with MANAGE_USERS permission.
        include_inactive: Whether to include deactivated users.

    Returns:
        List of user profiles with total count.
    """
    user_service = get_user_service()
    users = await user_service.get_all_users(include_inactive=include_inactive)
    return UserListResponse(users=users, total_count=len(users))


@router.post("/users")
async def create_user(
    user_data: CreateUserRequest,
    current_user: Annotated[UserProfile, Depends(require_permission(Permission.MANAGE_USERS))],
) -> UserProfile:
    """Create a new user account.

    Args:
        user_data: User creation payload with username, password, etc.
        current_user: Authenticated user with MANAGE_USERS permission.

    Returns:
        Created user profile.

    Raises:
        HTTPException: If user creation fails.
    """
    user_service = get_user_service()
    try:
        new_user = await user_service.create_user(
            username=user_data.username,
            password=user_data.password,
            email=user_data.email,
            role=user_data.role,
            is_active=user_data.is_active,
        )
        return new_user
    except ValueError as e:
        logger.warning("User creation failed: {}", str(e))
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail="User creation failed"
        ) from e


@router.put("/users/{user_id}")
async def update_user(
    user_id: str,
    user_data: UpdateUserRequest,
    current_user: Annotated[UserProfile, Depends(require_permission(Permission.MANAGE_USERS))],
) -> UserProfile:
    """Update an existing user's profile.

    Args:
        user_id: Target user ID.
        user_data: Update payload with optional email, role, is_active.
        current_user: Authenticated user with MANAGE_USERS permission.

    Returns:
        Updated user profile.

    Raises:
        HTTPException: If user not found.
    """
    user_service = get_user_service()
    updated_user = await user_service.update_user(
        user_id=user_id, email=user_data.email, role=user_data.role, is_active=user_data.is_active
    )
    if not updated_user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail=f"User with ID '{user_id}' not found"
        )
    return updated_user


@router.delete("/users/{user_id}")
async def delete_user(
    user_id: str,
    current_user: Annotated[UserProfile, Depends(require_permission(Permission.MANAGE_USERS))],
) -> MessageResponse:
    """Deactivate a user account.

    Args:
        user_id: Target user ID to deactivate.
        current_user: Authenticated user with MANAGE_USERS permission.

    Returns:
        Success message.

    Raises:
        HTTPException: If user not found or trying to delete self.
    """
    user_service = get_user_service()
    if user_id == current_user.id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail="Cannot delete your own account"
        )
    success = await user_service.delete_user(user_id)
    if not success:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail=f"User with ID '{user_id}' not found"
        )
    return MessageResponse(message=f"User '{user_id}' has been deactivated")


@router.post("/users/{user_id}/change-password")
@limiter.limit(PASSWORD_CHANGE_RATE_LIMIT)
async def change_user_password(
    request: Request,
    user_id: str,
    password_data: ChangePasswordRequest,
    current_user: Annotated[UserProfile, Depends(require_authentication)],
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
    if user_id != current_user.id:
        user_permissions = ROLE_PERMISSIONS.get(current_user.role, set())
        if Permission.MANAGE_USERS not in user_permissions:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="You can only change your own password",
            )
    success = await user_service.change_password(
        user_id=user_id,
        old_password=password_data.current_password,
        new_password=password_data.new_password,
    )
    if not success:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid current password or user not found",
        )
    return MessageResponse(message="Password changed successfully")


@router.post("/users/{user_id}/admin-reset-password")
@limiter.limit(PASSWORD_RESET_RATE_LIMIT)
async def admin_reset_user_password(
    request: Request,
    user_id: str,
    password_data: AdminResetPasswordRequest,
    current_user: Annotated[UserProfile, Depends(require_authentication)],
) -> MessageResponse:
    """Admin endpoint to reset a user's password without current password.

    Args:
        request: FastAPI request (used by rate limiter).
        user_id: Target user ID.
        password_data: New password.
        current_user: Admin user with MANAGE_USERS permission.

    Returns:
        Success message.

    Raises:
        HTTPException: If forbidden or user not found.
    """
    user_permissions = ROLE_PERMISSIONS.get(current_user.role, set())
    if Permission.MANAGE_USERS not in user_permissions:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Only administrators can reset user passwords",
        )
    user_service = get_user_service()
    try:
        async with user_service.repository.session() as session:
            stmt = select(User).where(User.id == user_id)
            result = await session.execute(stmt)
            db_user = result.scalar_one_or_none()
            if not db_user:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="User not found",
                )
            password_hash, salt = user_service.hash_password_with_salt(password_data.new_password)
            db_user.password_hash = password_hash
            db_user.salt = salt
            await session.commit()
            return MessageResponse(
                message=f"Password reset successfully for user {db_user.username}"
            )
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Failed to reset password for user {}: {}", user_id, str(e))
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to reset password",
        ) from e
