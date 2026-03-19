"""Tests for ClientProvenanceMiddleware REST provenance validation."""

import json

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
