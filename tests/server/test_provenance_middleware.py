"""Tests for ClientProvenanceMiddleware REST provenance validation."""

import json
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest
from loguru import logger
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Route
from starlette.routing import WebSocketRoute
from starlette.testclient import TestClient
from starlette.types import Message
from starlette.types import Receive
from starlette.types import Scope
from starlette.types import Send
from starlette.websockets import WebSocket

from snapper.server.provenance_middleware import ClientProvenanceMiddleware


async def _echo_handler(request: Request) -> JSONResponse:
    """Dummy handler that echoes request method."""
    body = await request.body()
    return JSONResponse({"method": request.method, "body_len": len(body)})


async def _ws_handler(websocket: WebSocket) -> None:
    """Dummy WebSocket handler for passthrough tests."""
    await websocket.accept()
    await websocket.close()


def _create_test_app() -> Starlette:
    """Build a minimal Starlette app with the provenance middleware."""
    app = Starlette(
        routes=[
            Route("/mutate", _echo_handler, methods=["POST", "PUT", "DELETE", "PATCH"]),
            Route("/read", _echo_handler, methods=["GET"]),
            WebSocketRoute("/ws", _ws_handler),
        ],
    )
    app.add_middleware(ClientProvenanceMiddleware)
    return app


class TestClientProvenanceMiddleware:
    """Tests for REST client provenance logging and gap detection."""

    def test_get_request_passes_through_without_logging(self) -> None:
        """GET requests bypass provenance inspection entirely.

        Given: A GET request,
        When: Processed by the middleware,
        Then: No provenance log is emitted and response is 200.
        """
        app = _create_test_app()
        client = TestClient(app)
        messages: list[str] = []
        sink_id = logger.add(lambda msg: messages.append(str(msg)), level="INFO")
        try:
            resp = client.get("/read")
        finally:
            logger.remove(sink_id)
        assert resp.status_code == 200
        assert not any("Client provenance" in m for m in messages)

    def test_post_with_provenance_logs_fields(self) -> None:
        """POST with provenance fields emits structured info log.

        Given: A POST request with session_id, sequence_id, public_id,
        When: Processed by the middleware,
        Then: Info log contains all three provenance fields.
        """
        app = _create_test_app()
        client = TestClient(app)
        messages: list[str] = []
        sink_id = logger.add(lambda msg: messages.append(str(msg)), level="INFO")
        body = {
            "session_id": "abc-session-123",
            "sequence_id": 1,
            "public_id": "pub-001",
            "type": "order_request",
        }
        try:
            resp = client.post("/mutate", content=json.dumps(body))
        finally:
            logger.remove(sink_id)
        assert resp.status_code == 200
        provenance_logs = [m for m in messages if "Client provenance" in m]
        assert len(provenance_logs) == 1
        assert "abc-session-123" in provenance_logs[0]
        assert "pub-001" in provenance_logs[0]

    def test_post_without_provenance_does_not_log(self) -> None:
        """POST without provenance fields does not emit provenance log.

        Given: A POST request with no session_id/sequence_id/public_id,
        When: Processed by the middleware,
        Then: No provenance log is emitted.
        """
        app = _create_test_app()
        client = TestClient(app)
        messages: list[str] = []
        sink_id = logger.add(lambda msg: messages.append(str(msg)), level="INFO")
        body = {"name": "test", "value": 42}
        try:
            resp = client.post("/mutate", content=json.dumps(body))
        finally:
            logger.remove(sink_id)
        assert resp.status_code == 200
        assert not any("Client provenance" in m for m in messages)

    def test_gap_detection_warns_on_sequence_gap(self) -> None:
        """Sequential gap in mutation requests triggers warning log.

        Given: Two POST requests from same session with gap (1 then 5),
        When: Processed by the middleware,
        Then: Gap warning is logged.
        """
        app = _create_test_app()
        client = TestClient(app)
        messages: list[str] = []
        sink_id = logger.add(lambda msg: messages.append(str(msg)), level="WARNING")
        session = "gap-session-aabbccdd"
        try:
            client.post(
                "/mutate",
                content=json.dumps({"session_id": session, "sequence_id": 1, "public_id": "p1"}),
            )
            client.post(
                "/mutate",
                content=json.dumps({"session_id": session, "sequence_id": 5, "public_id": "p5"}),
            )
        finally:
            logger.remove(sink_id)
        gap_warnings = [m for m in messages if "Gap" in m]
        assert len(gap_warnings) == 1

    def test_sequential_requests_no_gap_warning(self) -> None:
        """Sequential mutation requests (1, 2, 3) produce no gap warnings.

        Given: Three POST requests with sequential sequence_ids,
        When: Processed by the middleware,
        Then: No gap warnings are logged.
        """
        app = _create_test_app()
        client = TestClient(app)
        messages: list[str] = []
        sink_id = logger.add(lambda msg: messages.append(str(msg)), level="WARNING")
        session = "seq-session-aabbccdd"
        try:
            for seq in (1, 2, 3):
                client.post(
                    "/mutate",
                    content=json.dumps(
                        {"session_id": session, "sequence_id": seq, "public_id": f"p{seq}"}
                    ),
                )
        finally:
            logger.remove(sink_id)
        gap_warnings = [m for m in messages if "Gap" in m]
        assert len(gap_warnings) == 0

    def test_non_json_body_passes_through(self) -> None:
        """POST with non-JSON body passes through without error.

        Given: A POST request with plain text body,
        When: Processed by the middleware,
        Then: Response is 200 and no provenance log emitted.
        """
        app = _create_test_app()
        client = TestClient(app)
        messages: list[str] = []
        sink_id = logger.add(lambda msg: messages.append(str(msg)), level="INFO")
        try:
            resp = client.post("/mutate", content=b"not json at all")
        finally:
            logger.remove(sink_id)
        assert resp.status_code == 200
        assert not any("Client provenance" in m for m in messages)

    def test_put_and_patch_also_inspected(self) -> None:
        """PUT and PATCH mutation methods also trigger provenance logging.

        Given: PUT and PATCH requests with provenance fields,
        When: Processed by the middleware,
        Then: Provenance log is emitted for each.
        """
        app = _create_test_app()
        client = TestClient(app)
        messages: list[str] = []
        sink_id = logger.add(lambda msg: messages.append(str(msg)), level="INFO")
        body = json.dumps({"session_id": "put-patch-session", "sequence_id": 1, "public_id": "pp1"})
        try:
            client.put("/mutate", content=body)
            client.patch("/mutate", content=body)
        finally:
            logger.remove(sink_id)
        provenance_logs = [m for m in messages if "Client provenance" in m]
        assert len(provenance_logs) == 2

    def test_middleware_does_not_block_request(self) -> None:
        """Middleware never rejects requests even with bad provenance.

        Given: A POST request with empty session_id but nonzero sequence_id,
        When: Processed by the middleware,
        Then: Response is 200 (observability-first, no rejection).
        """
        app = _create_test_app()
        client = TestClient(app)
        body = json.dumps({"session_id": "", "sequence_id": 99, "public_id": "p99"})
        resp = client.post("/mutate", content=body)
        assert resp.status_code == 200

    def test_json_array_body_passes_through(self) -> None:
        """POST with JSON array body passes through without error.

        Given: A POST request with a JSON array body (not a dict),
        When: Processed by the middleware,
        Then: Response is 200 and no provenance log emitted.
        """
        app = _create_test_app()
        client = TestClient(app)
        messages: list[str] = []
        sink_id = logger.add(lambda msg: messages.append(str(msg)), level="INFO")
        try:
            resp = client.post("/mutate", content=json.dumps([1, 2, 3]))
        finally:
            logger.remove(sink_id)
        assert resp.status_code == 200
        assert not any("Client provenance" in m for m in messages)

    def test_websocket_scope_passes_through(self) -> None:
        """WebSocket connections bypass provenance middleware entirely.

        Given: A WebSocket connection,
        When: Processed by the middleware,
        Then: Connection succeeds without provenance inspection.
        """
        app = _create_test_app()
        client = TestClient(app)
        with client.websocket_connect("/ws") as ws:
            assert ws is not None

    @pytest.mark.asyncio
    async def test_non_body_receive_messages_pass_through(self) -> None:
        """Receive messages that are not http.request pass through unmodified.

        Given: A downstream ASGI app that calls receive() twice (body + disconnect),
        When: The middleware wraps receive,
        Then: The disconnect message passes through and body is still inspected.
        """
        received_types: list[str] = []

        async def greedy_app(scope: Scope, receive: Receive, send: Send) -> None:
            """App that reads body then reads disconnect."""
            msg1 = await receive()
            received_types.append(msg1.get("type", ""))
            msg2 = await receive()
            received_types.append(msg2.get("type", ""))
            await send({"type": "http.response.start", "status": 200, "headers": []})
            await send({"type": "http.response.body", "body": b"ok"})

        middleware = ClientProvenanceMiddleware(greedy_app)

        call_count = 0

        async def mock_receive() -> Message:
            """Provide body then disconnect messages."""
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return {"type": "http.request", "body": b'{"x": 1}', "more_body": False}
            return {"type": "http.disconnect"}

        sent: list[Message] = []

        async def mock_send(message: Message) -> None:
            """Collect sent messages."""
            sent.append(message)

        scope: Scope = {"type": "http", "method": "POST", "path": "/test"}
        await middleware(scope, mock_receive, mock_send)
        assert received_types == ["http.request", "http.disconnect"]

    def test_post_with_empty_body_passes_through(self) -> None:
        """POST with empty body passes through without provenance logging.

        Given: A POST request with no body content,
        When: Processed by the middleware,
        Then: Response is 200 and no provenance log emitted.
        """
        app = _create_test_app()
        client = TestClient(app)
        messages: list[str] = []
        sink_id = logger.add(lambda msg: messages.append(str(msg)), level="INFO")
        try:
            resp = client.post("/mutate", content=b"")
        finally:
            logger.remove(sink_id)
        assert resp.status_code == 200
        assert not any("Client provenance" in m for m in messages)

    @pytest.mark.parametrize(
        "credential_path",
        [
            "/api/mcp/token",
            "/api/mcp/token/",
            "/api/mcp/revoke",
            "/api/mcp/register",
            "/api/mcp/authorize",
            "/api/mcp-oauth/consent/approve",
            "/api/mcp-oauth/consent/deny",
        ],
    )
    def test_credential_path_skips_provenance_and_suppresses_control_payload(
        self, credential_path: str
    ) -> None:
        """Credential endpoints skip body inspection and store payload=None without query strings.

        Given: A POST request to an OAuth credential path with a sentinel query
            and a JSON body carrying code, code_verifier, client_secret, and provenance keys,
        When: Processed by ClientProvenanceMiddleware with a repository,
        Then: No provenance log is emitted, no gap detector is created, message_type
            contains the path without any query string, and Control.payload is None
            with no sentinel anywhere in the persisted Control row.

        Args:
            credential_path: OAuth credential endpoint path under test.
        """
        mock_session = AsyncMock(add=MagicMock())
        mock_ctx = AsyncMock()
        mock_ctx.__aenter__ = AsyncMock(return_value=mock_session)
        mock_ctx.__aexit__ = AsyncMock(return_value=False)
        mock_repo = MagicMock()
        mock_repo.session.return_value = mock_ctx
        detectors: dict[str, object] = {}

        async def _ok_handler(request: Request) -> JSONResponse:
            """Read request body and return 200."""
            await request.body()
            return JSONResponse({"ok": True})

        app = Starlette(
            routes=[
                Route(credential_path, _ok_handler, methods=["POST"]),
                Route("/api/mcp/tokenx", _ok_handler, methods=["POST"]),
            ],
        )
        app.add_middleware(
            ClientProvenanceMiddleware,
            db_url="sqlite+aiosqlite:///:memory:",
            gap_detectors=detectors,
        )
        client = TestClient(app)
        body = json.dumps(
            {
                "session_id": "019e873c-cf6c-72a4-88d6-07199e9cbd96",
                "sequence_id": 1,
                "public_id": "019fc3ba-79f6-72dd-8f24-09084ae1d7c5",
                "code": "SENTINEL_BODY_CODE",
                "code_verifier": "SENTINEL_VERIFIER",
                "client_secret": "SENTINEL_SECRET",
            }
        )
        messages: list[str] = []
        sink_id = logger.add(lambda msg: messages.append(str(msg)), level="INFO")
        try:
            with patch(
                "snapper.server.provenance_middleware.get_repository",
                return_value=mock_repo,
            ):
                resp = client.post(f"{credential_path}?code=SENTINEL", content=body)
        finally:
            logger.remove(sink_id)

        assert resp.status_code == 200
        assert not any("Client provenance" in m for m in messages)
        assert detectors == {}
        mock_session.add.assert_called_once()
        row = mock_session.add.call_args[0][0]
        assert row.message_type == f"POST {credential_path}"
        assert "SENTINEL" not in row.message_type
        assert "?" not in row.message_type
        assert row.payload is None
        assert row.outcome == "ok"
        assert row.detail is None
        assert row.client_session_id is None
        assert row.client_public_id is None
        for sentinel in (
            "SENTINEL",
            "SENTINEL_BODY_CODE",
            "SENTINEL_VERIFIER",
            "SENTINEL_SECRET",
        ):
            assert sentinel not in str(row.message_type)
            assert sentinel not in str(row.payload)
            assert sentinel not in str(row.detail)

    @pytest.mark.asyncio
    async def test_scope_path_query_is_stripped_on_mutation_and_telemetry(self) -> None:
        """Any accidental query suffix on ASGI scope path is stripped before recording.

        Given: Direct ASGI scopes whose ``path`` carries a ``?query`` suffix,
        When: Processed for a credential POST and a telemetry-enabled GET,
        Then: Both Control and Telemetry message_type omit the query string, and
            exception outcome/detail are preserved on the credential Control row.
        """
        mock_session = AsyncMock(add=MagicMock())
        mock_ctx = AsyncMock()
        mock_ctx.__aenter__ = AsyncMock(return_value=mock_session)
        mock_ctx.__aexit__ = AsyncMock(return_value=False)
        mock_repo = MagicMock()
        mock_repo.session.return_value = mock_ctx

        async def _raising_app(scope: Scope, receive: Receive, send: Send) -> None:
            """Consume body if POST, then raise for POST or reply 200 for GET."""
            if scope["method"] == "POST":
                await receive()
                raise RuntimeError("boom")
            await send({"type": "http.response.start", "status": 200, "headers": []})
            await send({"type": "http.response.body", "body": b"ok"})

        middleware = ClientProvenanceMiddleware(
            _raising_app,
            db_url="sqlite+aiosqlite:///:memory:",
            telemetry_enabled=True,
        )

        async def _post_receive() -> Message:
            """Return one token request body."""
            return {
                "type": "http.request",
                "body": b'{"code_verifier": "SENTINEL_VERIFIER"}',
                "more_body": False,
            }

        async def _noop_send(_message: Message) -> None:
            """Discard outgoing ASGI messages."""

        with patch(
            "snapper.server.provenance_middleware.get_repository",
            return_value=mock_repo,
        ):
            with pytest.raises(RuntimeError, match="boom"):
                await middleware(
                    {
                        "type": "http",
                        "method": "POST",
                        "path": "/api/mcp/token?code=SENTINEL",
                    },
                    _post_receive,
                    _noop_send,
                )
            await middleware(
                {
                    "type": "http",
                    "method": "GET",
                    "path": "/api/mcp/authorize?state=SENTINEL",
                },
                _post_receive,
                _noop_send,
            )

        assert mock_session.add.call_count == 2
        control_row = mock_session.add.call_args_list[0].args[0]
        telemetry_row = mock_session.add.call_args_list[1].args[0]
        assert control_row.message_type == "POST /api/mcp/token"
        assert control_row.payload is None
        assert control_row.outcome == "exception"
        assert control_row.detail == "RuntimeError: boom"
        assert telemetry_row.message_type == "GET /api/mcp/authorize"
        assert telemetry_row.payload is None

    def test_non_credential_boundary_sibling_keeps_control_payload(self) -> None:
        """A path starting with an OAuth prefix without a slash boundary is not suppressed.

        Given: A POST request to ``/api/mcp/tokenx`` with a JSON body,
        When: Processed by ClientProvenanceMiddleware,
        Then: The Control row records the JSON payload normally.
        """
        mock_session = AsyncMock(add=MagicMock())
        mock_ctx = AsyncMock()
        mock_ctx.__aenter__ = AsyncMock(return_value=mock_session)
        mock_ctx.__aexit__ = AsyncMock(return_value=False)
        mock_repo = MagicMock()
        mock_repo.session.return_value = mock_ctx

        app = Starlette(routes=[Route("/api/mcp/tokenx", _echo_handler, methods=["POST"])])
        app.add_middleware(ClientProvenanceMiddleware, db_url="sqlite+aiosqlite:///:memory:")
        client = TestClient(app)

        with patch(
            "snapper.server.provenance_middleware.get_repository",
            return_value=mock_repo,
        ):
            resp = client.post("/api/mcp/tokenx", content=json.dumps({"safe": "value"}))

        assert resp.status_code == 200
        row = mock_session.add.call_args[0][0]
        assert row.message_type == "POST /api/mcp/tokenx"
        assert row.payload == json.dumps({"safe": "value"})
