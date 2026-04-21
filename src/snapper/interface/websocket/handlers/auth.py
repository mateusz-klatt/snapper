"""WebSocket authentication handlers.

This module implements the WebSocket authentication flow including
initial authentication, re-authentication, and token expiration handling.
"""

import asyncio
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from uuid import uuid7

from fastapi import WebSocket
from loguru import logger
from pydantic import ValidationError

from snapper.api.auth.errors.ws_token import WsTokenAlreadyUsedError
from snapper.api.auth.errors.ws_token import WsTokenError
from snapper.api.auth.schemas.ws_token import WsTokenPayload
from snapper.api.auth.services.ws_token_service import WsTokenService
from snapper.api.auth.services.ws_token_service import compute_sid_hash
from snapper.auth.schemas.principal import AuthPrincipal
from snapper.auth.websocket_auth import WebSocketAuthManager
from snapper.data.repository import Repository
from snapper.interface.websocket.models import SERVER_CONTROL_SEQ
from snapper.interface.websocket.schemas import WSAuthenticateRequest
from snapper.interface.websocket.schemas import WSAuthExpiredResponse
from snapper.interface.websocket.schemas import WSAuthFailedResponse
from snapper.interface.websocket.schemas import WSAuthRequiredResponse
from snapper.interface.websocket.schemas import WSReauthOkResponse
from snapper.interface.websocket.schemas import WSReauthRequest
from snapper.interface.websocket.schemas import WSReauthRequiredResponse
from snapper.messaging.infrastructure.publisher import SequenceTracker

__all__ = [
    "AUTH_TIMEOUT_SECONDS",
    "REAUTH_WARN_OFFSET",
    "REAUTH_GRACE_PERIOD",
    "AuthResult",
    "authenticate_websocket",
    "handle_reauth",
    "create_deadline_tasks",
]

AUTH_TIMEOUT_SECONDS = 10

REAUTH_WARN_OFFSET = timedelta(seconds=60)

REAUTH_GRACE_PERIOD = timedelta(seconds=60)


class AuthResult:
    """Result of WebSocket authentication attempt.

    Encapsulates the outcome of authentication including user profile,
    token payload, and background deadline tasks.

    Attributes:
        success: Whether authentication was successful.
        user: Authenticated user profile, or None if failed.
        ws_payload: WebSocket token payload, or None if failed.
        warn_task: Background task for re-auth warning, or None.
        hard_task: Background task for token expiration, or None.
    """

    def __init__(
        self,
        success: bool,
        user: AuthPrincipal | None = None,
        ws_payload: WsTokenPayload | None = None,
        warn_task: asyncio.Task[None] | None = None,
        hard_task: asyncio.Task[None] | None = None,
    ) -> None:
        """Initialize authentication result.

        Args:
            success: Whether authentication was successful.
            user: Authenticated user profile.
            ws_payload: WebSocket token payload.
            warn_task: Background task for re-auth warning.
            hard_task: Background task for token expiration.
        """
        self.success = success
        self.user = user
        self.ws_payload = ws_payload
        self.warn_task = warn_task
        self.hard_task = hard_task


def create_deadline_tasks(
    websocket: WebSocket,
    exp_timestamp: int,
    tracker: SequenceTracker,
) -> tuple[asyncio.Task[None], asyncio.Task[None]]:
    """Create background tasks for token expiration handling.

    Creates two tasks: one for sending a re-authentication warning
    before expiration, and one for enforcing disconnect after grace period.

    Args:
        websocket: The WebSocket connection.
        exp_timestamp: Token expiration timestamp (Unix epoch seconds).
        tracker: Sequence tracker for stamping outbound messages.

    Returns:
        Tuple of (warning_task, expiration_task).
    """
    expiration = datetime.fromtimestamp(exp_timestamp, UTC)

    async def send_warning() -> None:
        try:
            delay = (expiration - datetime.now(UTC) - REAUTH_WARN_OFFSET).total_seconds()
            if delay > 0:
                await asyncio.sleep(delay)
            reauth_msg = WSReauthRequiredResponse(
                deadline=expiration,
                session_id=tracker.session_id,
                sequence_id=tracker.next_sequence(SERVER_CONTROL_SEQ),
                public_id=str(uuid7()),
                timestamp=datetime.now(UTC),
            )
            await websocket.send_text(reauth_msg.model_dump_json())
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.error("Failed to send reauth warning: {}", exc)

    async def enforce_expiration() -> None:
        try:
            delay = (expiration - datetime.now(UTC) + REAUTH_GRACE_PERIOD).total_seconds()
            if delay > 0:
                await asyncio.sleep(delay)
            expired_msg = WSAuthExpiredResponse(
                session_id=tracker.session_id,
                sequence_id=tracker.next_sequence(SERVER_CONTROL_SEQ),
                public_id=str(uuid7()),
                timestamp=datetime.now(UTC),
            )
            await websocket.send_text(expired_msg.model_dump_json())
            await asyncio.sleep(0.3)
            await websocket.close(code=4401, reason="Authorization expired")
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.error("Failed to enforce WebSocket expiration: {}", exc)

    return asyncio.create_task(send_warning()), asyncio.create_task(enforce_expiration())


async def authenticate_websocket(
    websocket: WebSocket,
    ws_auth_manager: WebSocketAuthManager,
    ws_token_service: WsTokenService,
    tracker: SequenceTracker,
    repository: Repository,
) -> AuthResult:
    """Authenticate a new WebSocket connection.

    Implements the full authentication flow
    1. Verify session cookie via DB-backed
       meth:`WebSocketAuthManager.verify_session_cookie`
       (— checks ``user_active_tokens`` +
       SCD2-active ``users.is_active``).
    2. Request ws_token from client.
    3. Verify ws_token against session.
    4. Create deadline tasks and register connection.

    Args:
        websocket: The WebSocket connection to authenticate.
        ws_auth_manager: Manager for WebSocket authentication state.
        ws_token_service: Service for verifying ws_tokens.
        tracker: Sequence tracker for stamping outbound messages.
        repository: Active :class:`Repository` threaded through to
            the DB-backed verify path.

    Returns:
        AuthResult with success status and authentication details.
    """
    session_result = await ws_auth_manager.verify_session_cookie(websocket, repository)
    if session_result is None:
        auth_failed = WSAuthFailedResponse(
            reason="missing_cookie",
            session_id=tracker.session_id,
            sequence_id=tracker.next_sequence(SERVER_CONTROL_SEQ),
            public_id=str(uuid7()),
            timestamp=datetime.now(UTC),
        )
        await websocket.send_text(auth_failed.model_dump_json())
        await websocket.close(code=4401, reason="Authentication cookie missing")
        return AuthResult(success=False)
    user, token_data = session_result
    expected_sid_hash = compute_sid_hash(token_data.sid)
    auth_required = WSAuthRequiredResponse(
        timeout=AUTH_TIMEOUT_SECONDS,
        session_id=tracker.session_id,
        sequence_id=tracker.next_sequence(SERVER_CONTROL_SEQ),
        public_id=str(uuid7()),
        timestamp=datetime.now(UTC),
    )
    await websocket.send_text(auth_required.model_dump_json())
    try:
        async with asyncio.timeout(AUTH_TIMEOUT_SECONDS):
            raw_message = await websocket.receive_text()
    except TimeoutError:
        auth_failed = WSAuthFailedResponse(
            reason="timeout",
            session_id=tracker.session_id,
            sequence_id=tracker.next_sequence(SERVER_CONTROL_SEQ),
            public_id=str(uuid7()),
            timestamp=datetime.now(UTC),
        )
        await websocket.send_text(auth_failed.model_dump_json())
        await websocket.close(code=4408, reason="Authentication timeout")
        return AuthResult(success=False)
    try:
        auth_message = WSAuthenticateRequest.model_validate_json(raw_message)
    except ValidationError:
        auth_failed = WSAuthFailedResponse(
            reason="invalid_json",
            session_id=tracker.session_id,
            sequence_id=tracker.next_sequence(SERVER_CONTROL_SEQ),
            public_id=str(uuid7()),
            timestamp=datetime.now(UTC),
        )
        await websocket.send_text(auth_failed.model_dump_json())
        await websocket.close(code=4401, reason="Invalid auth payload")
        return AuthResult(success=False)
    ws_token_value = auth_message.ws_token
    try:
        ws_payload = ws_token_service.verify(
            ws_token_value,
            expected_sub=user.username,
            expected_sid_hash=expected_sid_hash,
        )
    except WsTokenAlreadyUsedError:
        auth_failed = WSAuthFailedResponse(
            session_id=tracker.session_id,
            sequence_id=tracker.next_sequence(SERVER_CONTROL_SEQ),
            public_id=str(uuid7()),
            timestamp=datetime.now(UTC),
        )
        await websocket.send_text(auth_failed.model_dump_json())
        await websocket.close(code=4401, reason="ws_token replay")
        return AuthResult(success=False)
    except WsTokenError:
        auth_failed = WSAuthFailedResponse(
            session_id=tracker.session_id,
            sequence_id=tracker.next_sequence(SERVER_CONTROL_SEQ),
            public_id=str(uuid7()),
            timestamp=datetime.now(UTC),
        )
        await websocket.send_text(auth_failed.model_dump_json())
        await websocket.close(code=4401, reason="Invalid ws_token")
        return AuthResult(success=False)
    ws_token_service.mark_used(ws_payload)
    warn_task, hard_task = create_deadline_tasks(websocket, ws_payload.exp, tracker)
    ws_auth_manager.register_connection(
        websocket,
        user,
        token_data,
        ws_payload,
        warn_task=warn_task,
        hard_task=hard_task,
    )
    return AuthResult(
        success=True,
        user=user,
        ws_payload=ws_payload,
        warn_task=warn_task,
        hard_task=hard_task,
    )


async def handle_reauth(
    websocket: WebSocket,
    message: WSReauthRequest,
    user: AuthPrincipal,
    ws_auth_manager: WebSocketAuthManager,
    ws_token_service: WsTokenService,
    tracker: SequenceTracker,
) -> bool:
    """Handle re-authentication request for existing connection.

    Verifies the new ws_token, cancels old deadline tasks, and
    creates new ones with the updated expiration.

    Args:
        websocket: The WebSocket connection.
        message: Re-authentication request with new ws_token.
        user: Current authenticated user profile.
        ws_auth_manager: Manager for WebSocket authentication state.
        ws_token_service: Service for verifying ws_tokens.
        tracker: Sequence tracker for stamping outbound messages.

    Returns:
        True if re-authentication successful, False otherwise.
    """
    ws_token_candidate = message.ws_token
    state = ws_auth_manager.get_state(websocket)
    if state is None:
        auth_failed = WSAuthFailedResponse(
            session_id=tracker.session_id,
            sequence_id=tracker.next_sequence(SERVER_CONTROL_SEQ),
            public_id=str(uuid7()),
            timestamp=datetime.now(UTC),
        )
        await websocket.send_text(auth_failed.model_dump_json())
        await websocket.close(code=4401, reason="Missing session state")
        return False
    expected_hash = compute_sid_hash(state.session_id)
    try:
        new_payload = ws_token_service.verify(
            ws_token_candidate,
            expected_sub=user.username,
            expected_sid_hash=expected_hash,
        )
    except WsTokenAlreadyUsedError:
        auth_failed = WSAuthFailedResponse(
            session_id=tracker.session_id,
            sequence_id=tracker.next_sequence(SERVER_CONTROL_SEQ),
            public_id=str(uuid7()),
            timestamp=datetime.now(UTC),
        )
        await websocket.send_text(auth_failed.model_dump_json())
        await websocket.close(code=4401, reason="ws_token replay")
        return False
    except WsTokenError:
        auth_failed = WSAuthFailedResponse(
            session_id=tracker.session_id,
            sequence_id=tracker.next_sequence(SERVER_CONTROL_SEQ),
            public_id=str(uuid7()),
            timestamp=datetime.now(UTC),
        )
        await websocket.send_text(auth_failed.model_dump_json())
        await websocket.close(code=4401, reason="Invalid ws_token")
        return False
    ws_token_service.mark_used(new_payload)
    warn_task, hard_task = create_deadline_tasks(websocket, new_payload.exp, tracker)
    ws_auth_manager.update_ws_token_state(
        websocket,
        new_payload,
        warn_task=warn_task,
        hard_task=hard_task,
    )
    reauth_ok = WSReauthOkResponse(
        exp=datetime.fromtimestamp(new_payload.exp, UTC),
        session_id=tracker.session_id,
        sequence_id=tracker.next_sequence(SERVER_CONTROL_SEQ),
        public_id=str(uuid7()),
        timestamp=datetime.now(UTC),
    )
    await websocket.send_text(reauth_ok.model_dump_json())
    return True
