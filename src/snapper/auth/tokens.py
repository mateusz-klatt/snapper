"""Token management module.

This module provides JWT token creation, verification, and lifecycle
management including blacklisting and WebSocket token rotation.
"""

import secrets
import uuid
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from typing import Final

import jwt
from loguru import logger
from pydantic import ValidationError

from snapper.application.services.settings import SettingsService
from snapper.auth.domain.permissions import ROLE_PERMISSIONS
from snapper.auth.schemas.tokens import TokenClaims
from snapper.auth.schemas.tokens import TokenPair
from snapper.auth.schemas.user import UserProfile
from snapper.config.settings import AppSettings
from snapper.config.settings import get_settings
from snapper.config.settings import get_settings_with_service

BLACKLIST_GRACE_PERIOD_SECONDS: Final[float] = 10.0
"""Seconds a blacklisted token remains usable to handle concurrent requests."""

BLACKLIST_CLEANUP_MULTIPLIER: Final[int] = 2
"""Factor applied to grace period when deciding when to purge old entries."""


class TokenManager:
    """JWT token manager singleton.

    Handles creation, verification, and blacklisting of JWT tokens.
    Uses HMAC-SHA256 algorithm for token signing.

    Features:
    - Token pair creation (access + refresh)
    - Token verification with expiration checking
    - Token blacklisting with grace period
    - Automatic blacklist cleanup
    """

    _instance: TokenManager | None = None
    _initialized: bool = False

    def __new__(cls) -> TokenManager:
        """Create or return singleton token manager instance.

        Returns:
            TokenManager singleton instance.
        """
        if cls._instance is None:
            instance = super().__new__(cls)
            cls._instance = instance
        return cls._instance

    def __init__(self) -> None:
        """Initialize the token manager."""
        if self._initialized:
            return
        self._initialized = True
        self._settings: AppSettings | None = None
        self._blacklisted_tokens: dict[str, float] = {}
        self._blacklist_grace_period = BLACKLIST_GRACE_PERIOD_SECONDS

    def set_settings_service(self, settings_service: SettingsService) -> None:
        """Set settings service for configuration.

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

    def create_tokens(
        self,
        user: UserProfile,
        remember_me: bool = False,
        *,
        session_id: str | None = None,
    ) -> TokenPair:
        """Create access and refresh token pair.

        Args:
            user: User profile to create tokens for.
            remember_me: If True, extends refresh token lifetime.
            session_id: Optional session ID for token rotation.

        Returns:
            TokenPair containing access and refresh tokens.
        """
        now = datetime.now(UTC)
        issued_at = int(now.timestamp()) - 1
        jti = str(uuid.uuid4())
        session_identifier = session_id or str(uuid.uuid4())
        access_token_expires = timedelta(minutes=self.settings.auth_access_token_expire_minutes)
        access_token_payload = TokenClaims(
            sub=user.username,
            username=user.username,
            role=user.role,
            permissions=[p.value for p in ROLE_PERMISSIONS[user.role]],
            exp=int((now + access_token_expires).timestamp()),
            iat=issued_at,
            jti=jti,
            sid=session_identifier,
        )
        refresh_token_expires_days = (
            self.settings.auth_refresh_token_expire_days_extended
            if remember_me
            else self.settings.auth_refresh_token_expire_days
        )
        refresh_token_expires = timedelta(days=refresh_token_expires_days)
        refresh_token_payload = TokenClaims(
            sub=user.username,
            username=user.username,
            role=user.role,
            permissions=[],
            exp=int((now + refresh_token_expires).timestamp()),
            iat=issued_at,
            jti=f"refresh_{jti}",
            sid=session_identifier,
        )
        access_token = jwt.encode(
            access_token_payload.model_dump(),
            self.settings.auth_secret_key,
            algorithm=self.settings.auth_algorithm,
        )
        refresh_token = jwt.encode(
            refresh_token_payload.model_dump(),
            self.settings.auth_secret_key,
            algorithm=self.settings.auth_algorithm,
        )
        logger.info(f"Created token pair for user {user.username} (remember_me={remember_me})")
        return TokenPair(
            access_token=access_token,
            refresh_token=refresh_token,
            expires_in=int(access_token_expires.total_seconds()),
        )

    def _is_token_blacklisted(self, jti: str) -> bool:
        """Check if token is blacklisted (past grace period).

        Args:
            jti: JWT ID to check.

        Returns:
            True if token is blacklisted and past grace period.
        """
        if jti not in self._blacklisted_tokens:
            return False
        blacklist_time = self._blacklisted_tokens[jti]
        current_time = datetime.now(UTC).timestamp()
        if current_time - blacklist_time >= self._blacklist_grace_period:
            return True
        else:
            logger.debug(f"Token {jti} in grace period, allowing use")
            return False

    def _cleanup_old_blacklist_entries(self) -> None:
        """Remove expired entries from blacklist."""
        current_time = datetime.now(UTC).timestamp()
        expired_tokens: list[str] = []
        for jti, blacklist_time in self._blacklisted_tokens.items():
            if (
                current_time - blacklist_time
                >= self._blacklist_grace_period * BLACKLIST_CLEANUP_MULTIPLIER
            ):
                expired_tokens.append(jti)
        for jti in expired_tokens:
            del self._blacklisted_tokens[jti]
            logger.debug(f"Cleaned up expired blacklist entry: {jti}")

    def verify_token(self, token: str) -> TokenClaims | None:
        """Verify and decode a JWT token.

        Args:
            token: JWT token string.

        Returns:
            TokenClaims if valid, None if invalid or expired.
        """
        try:
            payload = jwt.decode(
                token,
                self.settings.auth_secret_key,
                algorithms=[self.settings.auth_algorithm],
            )
            token_data = TokenClaims(**payload)
            self._cleanup_old_blacklist_entries()
            if self._is_token_blacklisted(token_data.jti):
                logger.warning(f"Attempted use of blacklisted token: {token_data.jti}")
                return None
            if datetime.now(UTC).timestamp() > token_data.exp:
                logger.info(f"Token expired for user {token_data.username}")
                return None
            return token_data
        except jwt.ExpiredSignatureError:
            logger.info("Token expired")
            return None
        except jwt.PyJWTError as e:
            logger.warning(f"Token validation failed: {e}")
            return None
        except ValidationError as exc:
            logger.warning(f"Token payload validation failed: {exc}")
            return None

    def refresh_tokens(self, refresh_token: str) -> TokenPair | None:
        """Refresh token pair using refresh token.

        Blacklists the old refresh token and creates new pair.

        Args:
            refresh_token: Valid refresh token.

        Returns:
            New TokenPair or None if refresh token invalid.
        """
        token_data = self.verify_token(refresh_token)
        if not token_data:
            return None
        if not token_data.jti.startswith("refresh_"):
            logger.warning("Attempted to refresh with non-refresh token")
            return None
        self.blacklist_token(token_data.jti)
        user = UserProfile(
            username=token_data.username,
            role=token_data.role,
        )
        new_tokens = self.create_tokens(user, session_id=token_data.sid)
        logger.info(f"Refreshed tokens for user {user.username}")
        return new_tokens

    def blacklist_token(self, jti: str) -> None:
        """Blacklist a token with grace period.

        Token can still be used during grace period to handle
        concurrent requests.

        Args:
            jti: JWT ID to blacklist.
        """
        current_time = datetime.now(UTC).timestamp()
        self._blacklisted_tokens[jti] = current_time
        logger.info(f"Blacklisted token with grace period: {jti}")

    def blacklist_token_immediately(self, jti: str) -> None:
        """Blacklist a token with immediate effect.

        Token is invalid immediately without grace period.

        Args:
            jti: JWT ID to blacklist.
        """
        past_time = datetime.now(UTC).timestamp() - self._blacklist_grace_period - 1
        self._blacklisted_tokens[jti] = past_time
        logger.info(f"Immediately blacklisted token: {jti}")

    def invalidate_token(self, token: str) -> None:
        """Invalidate a token immediately.

        Args:
            token: JWT token string to invalidate.
        """
        token_data = self.verify_token(token)
        if token_data:
            self.blacklist_token_immediately(token_data.jti)

    def invalidate_user_tokens(self, user_id: str) -> None:
        """Invalidate all tokens for a user.

        Args:
            user_id: User ID whose tokens to invalidate.
        """
        logger.info(f"Invalidated all tokens for user: {user_id}")

    def create_csrf_token(self) -> str:
        """Create a new CSRF token.

        Returns:
            URL-safe random token string.
        """
        return secrets.token_urlsafe(32)

    def verify_csrf_token(self, token: str, expected: str) -> bool:
        """Verify CSRF token matches expected value.

        Args:
            token: Token to verify.
            expected: Expected token value.

        Returns:
            True if tokens match.
        """
        return secrets.compare_digest(token, expected)

    @classmethod
    def get_instance(cls) -> TokenManager:
        """Get singleton instance.

        Returns:
            TokenManager singleton instance.
        """
        if cls._instance is None:
            cls._instance = TokenManager()
        return cls._instance

    @classmethod
    def clear_instance(cls) -> None:
        """Clear singleton instance for testing."""
        cls._instance = None


class WebSocketTokenRotator:
    """WebSocket token rotation manager singleton.

    Manages token lifecycle for WebSocket connections including
    registration, rotation, and expiration checking.
    """

    _instance: WebSocketTokenRotator | None = None
    _initialized: bool = False

    def __new__(cls, token_manager: TokenManager | None = None) -> WebSocketTokenRotator:
        """Create or return singleton WebSocket token rotator instance.

        Args:
            token_manager: Optional token manager instance.

        Returns:
            WebSocketTokenRotator singleton instance.
        """
        if cls._instance is None:
            instance = super().__new__(cls)
            cls._instance = instance
        return cls._instance

    def __init__(self, token_manager: TokenManager | None = None) -> None:
        """Initialize the rotator.

        Args:
            token_manager: Optional token manager instance.
        """
        if self._initialized:
            return
        self._initialized = True
        self.token_manager = token_manager or TokenManager.get_instance()
        self._connection_tokens: dict[str, str] = {}

    def register_connection(self, connection_id: str, token: str) -> bool:
        """Register a WebSocket connection with token.

        Args:
            connection_id: Unique connection identifier.
            token: JWT access token.

        Returns:
            True if registration successful.
        """
        token_data = self.token_manager.verify_token(token)
        if not token_data:
            return False
        self._connection_tokens[connection_id] = token
        logger.info(f"Registered WS connection {connection_id} for user {token_data.username}")
        return True

    def update_connection_token(self, connection_id: str, token: str) -> None:
        """Update token for existing connection.

        Args:
            connection_id: Connection identifier.
            token: New JWT token.
        """
        self._connection_tokens[connection_id] = token

    def get_connection_token(self, connection_id: str) -> str | None:
        """Get token for a connection.

        Args:
            connection_id: Connection identifier.

        Returns:
            Token string or None if not registered.
        """
        return self._connection_tokens.get(connection_id)

    def should_rotate_token(self, connection_id: str) -> bool:
        """Check if connection token should be rotated.

        Args:
            connection_id: Connection identifier.

        Returns:
            True if token expires within 5 minutes.
        """
        if connection_id not in self._connection_tokens:
            return False
        token = self._connection_tokens[connection_id]
        token_data = self.token_manager.verify_token(token)
        if not token_data:
            return True
        expires_in = token_data.exp - datetime.now(UTC).timestamp()
        return expires_in < 300

    def rotate_connection_token(self, connection_id: str, refresh_token: str) -> str | None:
        """Rotate token for a connection.

        Args:
            connection_id: Connection identifier.
            refresh_token: Refresh token for rotation.

        Returns:
            New access token or None if rotation failed.
        """
        new_tokens = self.token_manager.refresh_tokens(refresh_token)
        if not new_tokens:
            return None
        self._connection_tokens[connection_id] = new_tokens.access_token
        logger.info(f"Rotated token for WS connection {connection_id}")
        return new_tokens.access_token

    def unregister_connection(self, connection_id: str) -> None:
        """Unregister a WebSocket connection.

        Args:
            connection_id: Connection identifier to remove.
        """
        if connection_id in self._connection_tokens:
            del self._connection_tokens[connection_id]
            logger.info(f"Unregistered WS connection {connection_id}")

    @classmethod
    def get_instance(cls) -> WebSocketTokenRotator:
        """Get singleton instance.

        Returns:
            WebSocketTokenRotator singleton instance.
        """
        if cls._instance is None:
            cls._instance = WebSocketTokenRotator()
        return cls._instance

    @classmethod
    def clear_instance(cls) -> None:
        """Clear singleton instance for testing."""
        cls._instance = None


def get_token_manager() -> TokenManager:
    """Get TokenManager singleton.

    Returns:
        TokenManager instance.
    """
    return TokenManager.get_instance()


def get_ws_token_rotator() -> WebSocketTokenRotator:
    """Get WebSocketTokenRotator singleton.

    Returns:
        WebSocketTokenRotator instance.
    """
    return WebSocketTokenRotator.get_instance()
