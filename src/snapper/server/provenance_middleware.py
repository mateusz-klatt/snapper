"""Server-side client provenance validation and control recording middleware.

Observability-first middleware that logs client provenance fields
(public_id, session_id, sequence_id) on mutation requests and detects
sequence gaps using a per-session GapDetector. Gaps are logged as
warnings but never reject requests.

Additionally records every mutation request to the ``control`` table for
audit purposes. The control write is non-blocking: any DB failure is
logged and swallowed so the response already sent to the client is never
invalidated.

The middleware inspects POST, PUT, DELETE, and PATCH requests that carry
a JSON body with provenance fields from StrictDataSchema.
"""

import json
from datetime import UTC
from datetime import datetime
from typing import Any

from loguru import logger
from starlette.types import ASGIApp
from starlette.types import Message
from starlette.types import Receive
from starlette.types import Scope
from starlette.types import Send

from snapper.core.redact import redact
from snapper.data.models import Control
from snapper.data.repository import get_repository
from snapper.messaging.infrastructure.gap_detector import GapDetector
from snapper.messaging.infrastructure.publisher import SequenceTracker

_MUTATION_METHODS = {b"POST", b"PUT", b"DELETE", b"PATCH"}


class ClientProvenanceMiddleware:
    """ASGI middleware that logs client provenance on mutation requests.

    For each mutation HTTP method (POST/PUT/DELETE/PATCH) with a JSON body,
    extracts provenance fields and:
    1. Emits a structured info log with client_session_id, client_sequence_id,
       client_public_id, and the request path.
    2. Runs a per-session_id GapDetector to warn on sequence gaps.
    3. Writes a row to the ``control`` table with redacted payload.

    Non-mutation requests and bodies without provenance fields pass through
    without any processing overhead beyond method check.

    Body chunks are collected transparently and replayed to downstream
    handlers so they can read the body normally.

    Attributes:
        app: The wrapped ASGI application.
        gap_detectors: Per-session GapDetector instances.
        tracker: Sequence tracker for control-table provenance.
        db_url: Database URL for control recording (may be None).
    """

    def __init__(self, app: ASGIApp, db_url: str | None = None) -> None:
        """Initialize middleware wrapping the given ASGI app.

        Args:
            app: The ASGI application to wrap.
            db_url: Optional database URL. When provided the middleware
                records mutation requests to the control table.
        """
        self.app = app
        self.gap_detectors: dict[str, GapDetector] = {}
        self.tracker = SequenceTracker()
        self.db_url = db_url

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        """Process an ASGI request, logging provenance for mutations.

        Args:
            scope: ASGI connection scope.
            receive: ASGI receive callable.
            send: ASGI send callable.
        """
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        method = scope.get("method", "")
        method_bytes = method.encode() if isinstance(method, str) else method

        if method_bytes not in _MUTATION_METHODS:
            await self.app(scope, receive, send)
            return

        body_chunks: list[bytes] = []
        status_code: int = 200
        outcome = "ok"
        detail: str | None = None

        async def wrapping_receive() -> Message:
            """Intercept body chunks for inspection, then replay them."""
            message = await receive()
            if message.get("type") == "http.request":
                body_chunks.append(message.get("body", b""))
            return message

        async def wrapping_send(message: Message) -> None:
            """Intercept response start to capture status code."""
            nonlocal status_code, outcome
            if message.get("type") == "http.response.start":
                status_code = message.get("status", 200)
                if status_code >= 400:
                    outcome = "error"
            await send(message)

        try:
            await self.app(scope, wrapping_receive, wrapping_send)
        except Exception as exc:
            outcome = "exception"
            detail = f"{type(exc).__name__}: {exc}"
            raise
        finally:
            full_body = b"".join(body_chunks)
            path: str = scope.get("path", "")
            payload_dict: dict[str, Any] | None = None
            if full_body:
                payload_dict = self._inspect_provenance(full_body, path)
            await self._record_control(
                path=path,
                method=method if isinstance(method, str) else method.decode(),
                outcome=outcome,
                detail=detail,
                payload_dict=payload_dict,
            )

    def _inspect_provenance(self, body: bytes, path: str) -> dict[str, Any] | None:
        """Extract and log provenance fields from a JSON request body.

        Args:
            body: Raw request body bytes.
            path: Request URL path for log context.

        Returns:
            Parsed payload dict if JSON, else None.
        """
        try:
            payload: Any = json.loads(body)
        except (json.JSONDecodeError, UnicodeDecodeError):
            return None

        if not isinstance(payload, dict):
            return None

        session_id: str = payload.get("session_id", "")
        sequence_id: int = payload.get("sequence_id", 0)
        public_id: str = payload.get("public_id", "")

        if not session_id and sequence_id == 0 and not public_id:
            return payload

        logger.info(
            "Client provenance on {path}: "
            "client_public_id={client_public_id}, "
            "client_session_id={client_session_id}, "
            "client_sequence_id={client_sequence_id}",
            path=path,
            client_public_id=public_id,
            client_session_id=session_id,
            client_sequence_id=sequence_id,
        )

        if session_id and sequence_id > 0:
            detector = self.gap_detectors.get(session_id)
            if detector is None:
                detector = GapDetector(name=f"rest-client:{session_id[:8]}")
                self.gap_detectors[session_id] = detector
            detector.check(path, session_id, sequence_id)

        return payload

    async def _record_control(
        self,
        path: str,
        method: str,
        outcome: str,
        detail: str | None,
        payload_dict: dict[str, Any] | None,
    ) -> None:
        """Persist a control row for this mutation request.

        Any failure is logged and swallowed so it never invalidates
        the response that was already sent to the client.

        Args:
            path: Request URL path.
            method: HTTP method string.
            outcome: ``ok``, ``error``, or ``exception``.
            detail: Optional error detail message.
            payload_dict: Parsed request body (will be redacted).
        """
        if self.db_url is None:
            return
        try:
            redacted = redact(payload_dict)
            client_session = (
                payload_dict.get("session_id") if isinstance(payload_dict, dict) else None
            )
            client_public = (
                payload_dict.get("public_id") if isinstance(payload_dict, dict) else None
            )
            repo = get_repository(self.db_url)
            now = datetime.now(UTC)
            row = Control(
                transport="rest",
                direction="inbound",
                message_type=f"{method} {path}",
                outcome=outcome,
                detail=detail,
                payload=redacted,
                client_session_id=client_session,
                client_public_id=client_public,
                session_id=self.tracker.session_id,
                sequence_id=self.tracker.next_sequence("control"),
                timestamp=now,
            )
            async with repo.session() as session:
                session.add(row)
                await session.commit()
        except Exception as exc:
            logger.warning("Control record write failed (non-blocking): {}", exc)
