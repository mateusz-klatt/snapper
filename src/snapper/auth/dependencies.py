"""Authentication dependencies module.

This module provides FastAPI dependencies for authentication,
authorization, and CSRF protection.
"""

import hashlib
import hmac
import secrets
from datetime import UTC
from datetime import datetime
from typing import Annotated
from typing import Any

from fastapi import Depends
from fastapi import HTTPException
from fastapi import Request
from fastapi import status

from snapper.application.services.settings import SettingsService
from snapper.auth.domain.permissions import ROLE_PERMISSIONS
from snapper.auth.domain.permissions import Permission
from snapper.auth.domain.roles import UserRole
from snapper.auth.schemas.principal import AuthPrincipal
from snapper.auth.schemas.tokens import TokenClaims
from snapper.auth.tokens import get_token_manager
from snapper.config.settings import AppSettings
from snapper.config.settings import get_settings
from snapper.config.settings import get_settings_with_service
from snapper.data.repository import Repository
from snapper.interface.websocket.helpers import build_allowed_origins
from snapper.server.dependencies import get_repository_dependency


def _extract_bearer_token(request: Request) -> str | None:
    """Extract a Bearer access token from the ``Authorization`` header.

    Parses the HTTP ``Authorization: Bearer <jwt>`` header form. The
    comparison is case-insensitive on the scheme name (RFC 7235); the
    token payload itself is preserved verbatim. Returns ``None`` when
    the header is absent or not a Bearer grant, so the caller can fall
    back to cookie-based auth transparently.

    Args:
        request: FastAPI request whose ``headers`` are consulted.

    Returns:
        The stripped JWT string when a Bearer header is present,
        otherwise ``None``.
    """
    auth_header = request.headers.get("authorization")
    if not auth_header:
        return None
    parts = auth_header.split(None, 1)
    if len(parts) != 2 or parts[0].lower() != "bearer":
        return None
    token = parts[1].strip()
    return token or None


async def get_current_user(
    request: Request,
    repo: Annotated[Repository, Depends(get_repository_dependency)],
) -> AuthPrincipal | None:
    """Extract current auth principal via DB-backed verification.

    The ``Authorization: Bearer <jwt>`` header is
    consulted FIRST; the ``access_token`` cookie is the fallback. This
    lets MCP clients (which have no cookie jar) authenticate against
    the same `/api/*` surface as the browser UI while preserving the
    existing cookie flow for the frontend.
    Verification now calls
    meth:`TokenManager.verify_token_with_db` so each request
    checks the ``user_active_tokens`` inventory + ``users.is_active``
    via the 30-second LRU cache. Kill-switch propagation
        **Same-instance** — immediate via the JTI blacklist
          seeded by :meth:`TokenManager.revoke_user_sessions` on
          ``UserService.deactivate_user``. The blacklist is
          consulted inside the sync ``verify_token`` step of
          ``verify_token_with_db`` and short-circuits BEFORE the
          LRU lookup, so revoked tokens never serve from cache
          even if a stale positive verdict is still resident.
        **Cross-instance** — bounded by the 30-second LRU TTL
          until the admin-bus subscriber calls
          meth:`TokenManager.invalidate_user_cache` on
          ``admin.user_deactivated`` receipt, collapsing the
          latency to one bus-message round-trip.
    Either way, the effective ceiling drops from the 15-minute
    access-token TTL to 30 s.

    Args:
        request: FastAPI request object.
        repo: Repository dep for the DB-backed verify path.

    Returns:
        AuthPrincipal if authenticated, None otherwise.
    """
    access_token = _extract_bearer_token(request) or request.cookies.get("access_token")
    if not access_token:
        return None
    token_manager = get_token_manager()
    token_data: TokenClaims | None = await token_manager.verify_token_with_db(access_token, repo)
    if not token_data:
        return None
    delegate_public_id: str | None = None
    if token_data.role == UserRole.AI_DELEGATE:
        delegate_row = await repo.get_ai_delegate_by_user_public_id(token_data.user_public_id)
        if delegate_row is not None:
            delegate_public_id = delegate_row["public_id"]
    principal = AuthPrincipal(
        username=token_data.username,
        role=token_data.role,
        user_public_id=token_data.user_public_id,
        operator_public_ids=token_data.operator_public_ids,
        primary_operator_public_id=token_data.primary_operator_public_id,
        active_wallet_public_id=token_data.active_wallet_public_id,
        delegate_public_id=delegate_public_id,
    )
    request.state.user = principal
    request.state.token_data = token_data
    return principal


def require_authentication(
    current_user: Annotated[AuthPrincipal | None, Depends(get_current_user)],
) -> AuthPrincipal:
    """Require authenticated user dependency.

    Args:
        current_user: Current principal from get_current_user.

    Returns:
        AuthPrincipal if authenticated.

    Raises:
        HTTPException: 401 if not authenticated.
    """
    if not current_user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Authentication required",
            headers={"WWW-Authenticate": "Bearer"},
        )
    return current_user


def require_permission(permission: Permission) -> Any:
    """Create dependency that requires specific permission.

    Args:
        permission: Required permission.

    Returns:
        Dependency function that validates permission.
    """

    def permission_checker(
        current_user: Annotated[AuthPrincipal, Depends(require_authentication)],
    ) -> AuthPrincipal:
        user_permissions = ROLE_PERMISSIONS.get(current_user.role, set())
        if permission not in user_permissions:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Permission '{permission.value}' required",
            )
        return current_user

    return permission_checker


def require_role(role: UserRole) -> Any:
    """Create dependency that requires minimum role level.

    Args:
        role: Minimum required role.

    Returns:
        Dependency function that validates role hierarchy.
    """
    role_hierarchy = {
        UserRole.AI_DELEGATE: -1,
        UserRole.VIEWER: 0,
        UserRole.OPERATOR: 1,
        UserRole.ADMIN: 2,
    }

    def role_checker(
        current_user: Annotated[AuthPrincipal, Depends(require_authentication)],
    ) -> AuthPrincipal:
        if role_hierarchy[current_user.role] < role_hierarchy[role]:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Role '{role.value}' or higher required",
            )
        return current_user

    return role_checker


class CSRFManager:
    """CSRF token manager singleton.

    Generates and validates CSRF tokens using HMAC signatures
    with timestamp-based expiration.
    """

    _instance: CSRFManager | None = None
    _initialized: bool = False

    def __new__(cls) -> CSRFManager:
        """Create or return singleton CSRF manager instance.

        Returns:
            Singleton CSRFManager instance.
        """
        if cls._instance is None:
            instance = super().__new__(cls)
            cls._instance = instance
        return cls._instance

    def __init__(self) -> None:
        """Initialize the CSRF manager."""
        if self._initialized:
            return
        self._initialized = True
        self._settings: AppSettings | None = None

    def set_settings_service(self, settings_service: SettingsService) -> None:
        """Set settings service.

        Args:
            settings_service: Settings service instance.
        """
        self._settings = get_settings_with_service(settings_service)

    @property
    def settings(self) -> AppSettings:
        """Get application settings, lazy-loading if needed.

        Returns:
            Application settings object.
        """
        if self._settings is None:
            self._settings = get_settings()
        return self._settings

    def _create_hmac_signature(self, nonce: str, timestamp: str) -> str:
        """Create HMAC signature for token components.

        Args:
            nonce: Random nonce value.
            timestamp: Unix timestamp string.

        Returns:
            Hex-encoded HMAC signature.
        """
        message = f"{nonce}:{timestamp}"
        return hmac.new(
            self.settings.csrf_secret_key.encode(), message.encode(), hashlib.sha256
        ).hexdigest()

    def _verify_hmac_signature(self, nonce: str, timestamp: str, signature: str) -> bool:
        """Verify HMAC signature matches expected value.

        Args:
            nonce: Token nonce.
            timestamp: Token timestamp.
            signature: Signature to verify.

        Returns:
            True if signature is valid.
        """
        expected_signature = self._create_hmac_signature(nonce, timestamp)
        return hmac.compare_digest(expected_signature, signature)

    def _get_current_timestamp(self) -> str:
        """Get current Unix timestamp as string.

        Returns:
            Current timestamp string.
        """
        return str(int(datetime.now(UTC).timestamp()))

    def _is_timestamp_valid(self, timestamp: str) -> bool:
        """Check if timestamp is within valid age.

        Args:
            timestamp: Timestamp string to validate.

        Returns:
            True if timestamp is not expired.
        """
        try:
            token_time = int(timestamp)
            current_time = int(datetime.now(UTC).timestamp())
            max_age_seconds = int(self.settings.csrf_token_expire_minutes) * 60
            return (current_time - token_time) <= max_age_seconds
        except ValueError, TypeError:
            return False

    def generate_token(self) -> str:
        """Generate a new CSRF token.

        Token format: {nonce}.{timestamp}.{signature}

        Returns:
            CSRF token string.
        """
        nonce = secrets.token_urlsafe(24)
        timestamp = self._get_current_timestamp()
        signature = self._create_hmac_signature(nonce, timestamp)
        return f"{nonce}.{timestamp}.{signature}"

    def validate_token(self, token: str) -> bool:
        """Validate a CSRF token.

        Args:
            token: CSRF token to validate.

        Returns:
            True if token is valid and not expired.
        """
        try:
            parts = token.split(".")
            if len(parts) != 3:
                return False
            nonce, timestamp, signature = parts
            if not self._is_timestamp_valid(timestamp):
                return False
            return self._verify_hmac_signature(nonce, timestamp, signature)
        except ValueError, TypeError:
            return False

    def invalidate_token(self, token: str) -> None:
        """Invalidate a CSRF token (no-op for stateless tokens).

        Args:
            token: Token to invalidate.
        """
        pass

    def cleanup_expired_tokens(self) -> None:
        """Clean up expired tokens (no-op for stateless tokens)."""
        pass

    @classmethod
    def get_instance(cls) -> CSRFManager:
        """Get singleton instance.

        Returns:
            CSRFManager singleton.
        """
        if cls._instance is None:
            cls._instance = CSRFManager()
        return cls._instance

    @classmethod
    def clear_instance(cls) -> None:
        """Clear singleton for testing."""
        cls._instance = None


def get_csrf_manager() -> CSRFManager:
    """Get CSRFManager singleton.

    Returns:
        CSRFManager instance.
    """
    return CSRFManager.get_instance()


def get_csrf_token(request: Request) -> str | None:
    """Extract CSRF token from request.

    Checks header first, then cookie.

    Args:
        request: FastAPI request.

    Returns:
        CSRF token or None.
    """
    csrf_token = request.headers.get("X-CSRF-Token")
    if csrf_token:
        return csrf_token
    return request.cookies.get("csrf_token")


def _request_settings(request: Request) -> AppSettings:
    """Return DB-backed settings for the request, falling back to bootstrap.

    ``get_settings()`` yields a bootstrap-only :class:`AppSettings` whose
    database-backed properties (``ui_origin``, ``session_domain``) raise
    ``RuntimeError`` — :func:`build_allowed_origins` swallows those, so
    origin validation would silently collapse to the localhost defaults
    and reject every same-site browser mutation from the configured
    ``ui_origin`` (e.g. the deployment domain). The lifespan publishes the
    initialized ``SettingsService`` on ``app.state``; binding it here lets
    ``ui_origin`` resolve. Falls back to the bootstrap instance only when
    the service is absent (e.g. early startup), preserving the previous
    behaviour instead of erroring.

    Args:
        request: The incoming request whose ``app.state`` may carry the
            initialized settings service.

    Returns:
        A service-bound :class:`AppSettings` when available, else the
        bootstrap-only instance.
    """
    settings_service = getattr(request.app.state, "settings_service", None)
    if settings_service is None:
        return get_settings()
    return get_settings_with_service(settings_service)


def validate_csrf_token(
    request: Request,
    csrf_token: Annotated[str | None, Depends(get_csrf_token)] = None,
) -> None:
    """Validate CSRF token for state-changing requests.

    Validates origin, presence in both cookie and header
    and signature integrity.
    Requests carrying an ``Authorization: Bearer``
    header bypass CSRF validation — they are not subject to
    cookie-based request forgery because they present the access
    token explicitly in a header the browser cannot forge via
    cross-origin form submission. This preserves the cookie flow
    for the frontend while allowing MCP / CLI clients to call
    state-changing REST endpoints without minting CSRF tokens.

    Args:
        request: FastAPI request.
        csrf_token: Token from get_csrf_token dependency.

    Raises:
        HTTPException: 403 if CSRF validation fails.
    """
    if request.method in ("GET", "HEAD", "OPTIONS"):
        return
    if _extract_bearer_token(request) is not None:
        return
    origin = request.headers.get("origin") or ""
    referer = request.headers.get("referer") or ""
    settings = _request_settings(request)
    allowed_origins = build_allowed_origins(settings, settings.server_port)
    origin_valid = origin in allowed_origins or any(
        referer == allowed_origin or referer.startswith(allowed_origin + "/")
        for allowed_origin in allowed_origins
    )
    if not origin_valid and origin and referer:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"Invalid origin: {origin}",
        )
    if not csrf_token:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="CSRF token required",
        )
    csrf_cookie = request.cookies.get("csrf_token")
    csrf_header = request.headers.get("X-CSRF-Token")
    if not csrf_cookie or not csrf_header:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="CSRF token must be present in both cookie and header",
        )
    if csrf_cookie != csrf_header:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="CSRF token mismatch between cookie and header",
        )
    csrf_manager = get_csrf_manager()
    if not csrf_manager.validate_token(csrf_token):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Invalid or tampered CSRF token signature",
        )


AuthenticatedUser = Annotated[AuthPrincipal, Depends(require_authentication)]
OperatorUser = Annotated[AuthPrincipal, Depends(require_role(UserRole.OPERATOR))]
AdminUser = Annotated[AuthPrincipal, Depends(require_role(UserRole.ADMIN))]
ReadMarketDataUser = Annotated[
    AuthPrincipal, Depends(require_permission(Permission.READ_MARKET_DATA))
]
CreateOrdersUser = Annotated[AuthPrincipal, Depends(require_permission(Permission.CREATE_ORDERS))]
ManageProcessesUser = Annotated[
    AuthPrincipal, Depends(require_permission(Permission.MANAGE_PROCESSES))
]
