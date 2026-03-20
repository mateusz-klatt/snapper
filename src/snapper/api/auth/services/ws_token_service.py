"""WebSocket token service for secure connection establishment.

This module provides the WsTokenService for generating and verifying
single-use JWT tokens used to authenticate WebSocket connections.
The tokens are bound to a specific user and session, preventing
replay attacks and unauthorized access.

Token flow:
    1. Client obtains access token via /auth/login
    2. Client requests ws_token via authenticated endpoint
    3. Client connects to WebSocket with ws_token
    4. Server verifies and consumes the token (single-use)
"""

import hashlib
import uuid
from datetime import UTC
from datetime import datetime
from datetime import timedelta

import jwt
from loguru import logger
from pydantic import ValidationError

from snapper.api.auth.errors.ws_token import WsTokenAlreadyUsedError
from snapper.api.auth.errors.ws_token import WsTokenError
from snapper.api.auth.schemas.ws_token import WsTokenPayload
from snapper.api.auth.schemas.ws_token import WsTokenResult
from snapper.api.auth.services.ws_token_store import WsTokenStore
from snapper.application.services.settings import SettingsService
from snapper.config.settings import AppSettings
from snapper.config.settings import get_settings
from snapper.config.settings import get_settings_with_service

__all__ = ["WsTokenService", "compute_sid_hash", "get_ws_token_service"]
WS_TOKEN_PURPOSE = "ws_connect"


def compute_sid_hash(session_id: str) -> str:
    """Compute SHA-256 hash of a session ID.

    Used to bind WebSocket tokens to sessions without exposing
    the actual session ID in the token.

    Args:
        session_id: The session ID to hash.

    Returns:
        Hexadecimal SHA-256 hash of the session ID.
    """
    digest = hashlib.sha256()
    digest.update(session_id.encode("utf-8"))
    return digest.hexdigest()


class WsTokenService:
    """Service for generating and verifying WebSocket authentication tokens.

    Implements singleton pattern to share token state across the application.
    Tokens are short-lived, single-use JWTs bound to specific users and sessions.

    Token claims:
        - purpose: 'ws_connect' (ensures token is used for intended purpose)
        - sub: User ID
        - sid_hash: SHA-256 hash of session ID
        - iat: Issued at timestamp
        - exp: Expiration timestamp
        - jti: Unique token ID for replay prevention
    """

    _instance: WsTokenService | None = None
    _initialized: bool = False

    def __new__(cls) -> WsTokenService:
        """Create or return singleton instance.

        Returns:
            The singleton WsTokenService instance.
        """
        if cls._instance is None:
            instance = super().__new__(cls)
            cls._instance = instance
        return cls._instance

    def __init__(self) -> None:
        """Initialize the service with token store."""
        if self._initialized:
            return
        self._initialized = True
        self._settings: AppSettings | None = None
        self._store = WsTokenStore()

    def set_settings_service(self, settings_service: SettingsService) -> None:
        """Inject settings service for configuration.

        Args:
            settings_service: Settings service instance.
        """
        self._settings = get_settings_with_service(settings_service)

    @property
    def settings(self) -> AppSettings:
        """Get settings, lazily loading defaults if not injected.

        Returns:
            Application settings instance.
        """
        if self._settings is None:
            self._settings = get_settings()
        return self._settings

    def _now(self) -> datetime:
        """Get current UTC datetime for testing purposes.

        Returns:
            Current datetime in UTC timezone.
        """
        return datetime.now(UTC)

    def generate(self, *, user_id: str, session_id: str) -> WsTokenResult:
        """Generate a new WebSocket authentication token.

        Creates a short-lived, single-use JWT bound to the user and session.

        Args:
            user_id: The user ID to include in the token.
            session_id: The session ID to bind the token to.

        Returns:
            WsTokenResult containing the token, expiration, and payload.
        """
        now = self._now()
        ttl_seconds = self.settings.ws_token_ttl_seconds
        expires_at = now + timedelta(seconds=ttl_seconds)
        payload = WsTokenPayload(
            purpose=WS_TOKEN_PURPOSE,
            sub=user_id,
            sid_hash=compute_sid_hash(session_id),
            iat=int(now.timestamp()),
            exp=int(expires_at.timestamp()),
            jti=uuid.uuid4().hex,
        )
        token = jwt.encode(
            payload.model_dump(
                exclude={"public_id", "timestamp", "session_id", "sequence_id", "type"}
            ),
            self.settings.auth_secret_key,
            algorithm=self.settings.auth_algorithm,
        )
        logger.debug(
            "Issued ws_token for user '{}' expiring at {}",
            user_id,
            expires_at.isoformat(),
        )
        return WsTokenResult(token=token, expires_at=expires_at, payload=payload)

    def verify(self, token: str, *, expected_sub: str, expected_sid_hash: str) -> WsTokenPayload:
        """Verify a WebSocket token without consuming it.

        Validates signature, expiration, purpose, subject, and session binding.
        Does NOT mark the token as used (call mark_used separately).

        Args:
            token: The JWT token string.
            expected_sub: Expected user ID.
            expected_sid_hash: Expected session ID hash.

        Returns:
            Validated token payload.

        Raises:
            WsTokenError: If token is invalid, expired, or mismatched.
            WsTokenAlreadyUsedError: If token has already been consumed.
        """
        now_ts = int(self._now().timestamp())
        try:
            payload_dict = jwt.decode(
                token,
                self.settings.auth_secret_key,
                algorithms=[self.settings.auth_algorithm],
                options={"require": ["exp", "iat", "jti"]},
            )
            payload = WsTokenPayload.model_validate(payload_dict)
        except (jwt.PyJWTError, ValidationError) as exc:
            raise WsTokenError("invalid_ws_token") from exc
        if payload.exp <= now_ts:
            raise WsTokenError("ws_token_expired")
        if payload.purpose != WS_TOKEN_PURPOSE:
            raise WsTokenError("invalid_ws_token_purpose")
        if payload.sub != expected_sub:
            raise WsTokenError("ws_token_subject_mismatch")
        if payload.sid_hash != expected_sid_hash:
            raise WsTokenError("ws_token_session_mismatch")
        if self._store.is_used(payload.jti, now_ts):
            raise WsTokenAlreadyUsedError("ws_token_already_used")
        return payload

    def mark_used(self, payload: WsTokenPayload) -> None:
        """Mark a token as consumed to prevent replay.

        Args:
            payload: The validated token payload.
        """
        self._store.mark_used(payload.jti, payload.exp)

    @classmethod
    def get_instance(cls) -> WsTokenService:
        """Get or create the singleton instance.

        Returns:
            The WsTokenService singleton.
        """
        if cls._instance is None:
            cls._instance = WsTokenService()
        return cls._instance

    @classmethod
    def clear_instance(cls) -> None:
        """Clear the singleton instance (for testing)."""
        cls._instance = None


def get_ws_token_service() -> WsTokenService:
    """Get the WebSocket token service singleton.

    Returns:
        The WsTokenService instance.
    """
    return WsTokenService.get_instance()
