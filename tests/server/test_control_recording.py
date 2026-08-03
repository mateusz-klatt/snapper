"""Tests for control and telemetry recording in ClientProvenanceMiddleware."""

import json
from typing import Any
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest
from loguru import logger
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Route
from starlette.testclient import TestClient

from snapper.messaging.infrastructure.gap_detector import GapDetector
from snapper.server.provenance_middleware import ClientProvenanceMiddleware

TEST_DB_URL = "sqlite+aiosqlite:///:memory:"
_CLIENT_SESSION_ID = "019e873c-cf6c-72a4-88d6-07199e9cbd96"
_CLIENT_PUBLIC_ID = "019fc3ba-79f6-72dd-8f24-09084ae1d7c5"


async def _echo_handler(request: Request) -> JSONResponse:
    """Dummy handler that echoes request method."""
    body = await request.body()
    return JSONResponse({"method": request.method, "body_len": len(body)})


async def _error_handler(request: Request) -> JSONResponse:
    """Handler that returns a 422 error."""
    await request.body()
    return JSONResponse({"error": "bad"}, status_code=422)


async def _exception_handler(request: Request) -> JSONResponse:
    """Handler that raises an exception."""
    raise RuntimeError("boom")


def _create_test_app(
    handler: object = _echo_handler,
    db_url: str | None = None,
    telemetry_enabled: bool = False,
    gap_detectors: dict[str, GapDetector] | None = None,
) -> Starlette:
    """Build a minimal Starlette app with the provenance middleware."""
    routes = [
        Route("/mutate", handler, methods=["POST", "PUT", "DELETE", "PATCH"]),
        Route("/error", _error_handler, methods=["POST"]),
        Route("/exception", _exception_handler, methods=["POST"]),
        Route("/read", _echo_handler, methods=["GET"]),
    ]
    app = Starlette(routes=routes)
    app.add_middleware(
        ClientProvenanceMiddleware,
        db_url=db_url,
        telemetry_enabled=telemetry_enabled,
        gap_detectors=gap_detectors,
    )
    return app


def _build_mock_repo() -> tuple[MagicMock, MagicMock]:
    """Build a mock repository with async session context.

    Returns:
        Tuple of (mock_repo, mock_session).
    """
    mock_session = AsyncMock(add=MagicMock())
    mock_ctx = AsyncMock()
    mock_ctx.__aenter__ = AsyncMock(return_value=mock_session)
    mock_ctx.__aexit__ = AsyncMock(return_value=False)
    mock_repo = MagicMock()
    mock_repo.session.return_value = mock_ctx
    return mock_repo, mock_session


class TestControlRecordingOk:
    """Control row is written for successful mutations."""

    @pytest.mark.asyncio
    async def test_control_row_written_on_success(self) -> None:
        """A successful POST mutation writes a control row with outcome=ok."""
        mock_repo, mock_session = _build_mock_repo()

        with patch(
            "snapper.server.provenance_middleware.get_repository",
            return_value=mock_repo,
        ):
            app = _create_test_app(db_url=TEST_DB_URL)
            client = TestClient(app)
            body = json.dumps(
                {
                    "session_id": _CLIENT_SESSION_ID,
                    "sequence_id": 1,
                    "public_id": _CLIENT_PUBLIC_ID,
                }
            )
            resp = client.post("/mutate", content=body)

        assert resp.status_code == 200
        mock_session.add.assert_called_once()
        row = mock_session.add.call_args[0][0]
        assert row.transport == "rest"
        assert row.direction == "inbound"
        assert row.outcome == "ok"
        assert row.detail is None
        assert row.client_session_id == _CLIENT_SESSION_ID
        assert row.client_public_id == _CLIENT_PUBLIC_ID
        assert "[REDACTED]" not in (row.payload or "")
        mock_session.commit.assert_awaited_once()

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "body",
        [
            {"sequence_id": 0},
            {"session_id": "", "sequence_id": 0, "public_id": ""},
        ],
        ids=["absent", "empty"],
    )
    async def test_missing_client_uuid_provenance_is_persisted_as_null(
        self, body: dict[str, object]
    ) -> None:
        """Missing client UUIDs remain in payload but bind as nullable SQL fields."""
        mock_repo, mock_session = _build_mock_repo()

        with patch(
            "snapper.server.provenance_middleware.get_repository",
            return_value=mock_repo,
        ):
            app = _create_test_app(db_url=TEST_DB_URL)
            client = TestClient(app)
            response = client.post("/mutate", content=json.dumps(body))

        assert response.status_code == 200
        row = mock_session.add.call_args[0][0]
        assert row.client_session_id is None
        assert row.client_public_id is None
        assert json.loads(row.payload) == body
        mock_session.commit.assert_awaited_once()

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "body",
        [
            {"session_id": "not-a-uuid", "public_id": "also-invalid"},
            {"session_id": " ", "public_id": "\t"},
            {"session_id": ["unexpected"], "public_id": 42},
        ],
        ids=["malformed", "whitespace", "wrong-types"],
    )
    async def test_invalid_client_uuid_provenance_is_persisted_as_null(
        self, body: dict[str, object]
    ) -> None:
        """Invalid client UUIDs cannot make PostgreSQL discard the audit row."""
        mock_repo, mock_session = _build_mock_repo()

        with patch(
            "snapper.server.provenance_middleware.get_repository",
            return_value=mock_repo,
        ):
            app = _create_test_app(db_url=TEST_DB_URL)
            client = TestClient(app)
            response = client.post("/mutate", content=json.dumps(body))

        assert response.status_code == 200
        row = mock_session.add.call_args[0][0]
        assert row.client_session_id is None
        assert row.client_public_id is None
        assert json.loads(row.payload) == body
        mock_session.commit.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_noncanonical_uuid_text_is_canonicalized_for_sql_columns(self) -> None:
        """Python-only UUID spellings cannot break PostgreSQL audit persistence.

        Given: Valid UUID values using URN and brace spellings,
        When: A mutation is recorded into native UUID columns,
        Then: The SQL fields are canonical while the audit payload remains original.
        """
        mock_repo, mock_session = _build_mock_repo()
        body = {
            "session_id": f"urn:uuid:{_CLIENT_SESSION_ID}",
            "public_id": "{" + _CLIENT_PUBLIC_ID.upper() + "}",
        }

        with patch(
            "snapper.server.provenance_middleware.get_repository",
            return_value=mock_repo,
        ):
            app = _create_test_app(db_url=TEST_DB_URL)
            client = TestClient(app)
            response = client.post("/mutate", content=json.dumps(body))

        assert response.status_code == 200
        row = mock_session.add.call_args[0][0]
        assert row.client_session_id == _CLIENT_SESSION_ID
        assert row.client_public_id == _CLIENT_PUBLIC_ID
        assert json.loads(row.payload) == body
        mock_session.commit.assert_awaited_once()

    @pytest.mark.asyncio
    @pytest.mark.parametrize("sequence_id", ["1", [1], {"value": 1}, True])
    async def test_invalid_sequence_type_does_not_break_request_or_recording(
        self, sequence_id: object
    ) -> None:
        """Malformed provenance remains non-blocking before endpoint validation."""
        mock_repo, mock_session = _build_mock_repo()
        body = {
            "session_id": _CLIENT_SESSION_ID,
            "sequence_id": sequence_id,
            "public_id": _CLIENT_PUBLIC_ID,
        }

        with patch(
            "snapper.server.provenance_middleware.get_repository",
            return_value=mock_repo,
        ):
            app = _create_test_app(db_url=TEST_DB_URL)
            client = TestClient(app)
            response = client.post("/mutate", content=json.dumps(body))

        assert response.status_code == 200
        assert mock_session.add.call_args[0][0].client_session_id == _CLIENT_SESSION_ID
        mock_session.commit.assert_awaited_once()


class TestControlRecordingError:
    """Control row records error outcome for HTTP 4xx/5xx."""

    @pytest.mark.asyncio
    async def test_control_row_outcome_error_on_4xx(self) -> None:
        """HTTP 422 response writes a control row with outcome=error."""
        mock_repo, mock_session = _build_mock_repo()

        with patch(
            "snapper.server.provenance_middleware.get_repository",
            return_value=mock_repo,
        ):
            app = _create_test_app(db_url=TEST_DB_URL)
            client = TestClient(app)
            resp = client.post("/error", content=b"{}")

        assert resp.status_code == 422
        mock_session.add.assert_called_once()
        row = mock_session.add.call_args[0][0]
        assert row.outcome == "error"


class TestControlRecordingException:
    """Control row records exception outcome when handler raises."""

    @pytest.mark.asyncio
    async def test_control_row_outcome_exception_on_raise(self) -> None:
        """Unhandled exception writes control row with outcome=exception."""
        mock_repo, mock_session = _build_mock_repo()

        with patch(
            "snapper.server.provenance_middleware.get_repository",
            return_value=mock_repo,
        ):
            app = _create_test_app(db_url=TEST_DB_URL)
            client = TestClient(app, raise_server_exceptions=False)
            resp = client.post("/exception", content=b"{}")

        assert resp.status_code == 500
        mock_session.add.assert_called_once()
        row = mock_session.add.call_args[0][0]
        assert row.outcome == "exception"
        assert "RuntimeError: boom" in (row.detail or "")


class TestControlRecordingNonBlocking:
    """DB failure in control write does not invalidate the response."""

    @pytest.mark.asyncio
    async def test_db_failure_does_not_break_response(self) -> None:
        """When the control write raises, the original response is still returned."""
        mock_repo, mock_session = _build_mock_repo()
        mock_session.commit = AsyncMock(side_effect=RuntimeError("db down"))

        with patch(
            "snapper.server.provenance_middleware.get_repository",
            return_value=mock_repo,
        ):
            app = _create_test_app(db_url=TEST_DB_URL)
            client = TestClient(app)
            resp = client.post("/mutate", content=b'{"x": 1}')

        assert resp.status_code == 200

    @pytest.mark.asyncio
    async def test_db_failure_logged_as_warning(self) -> None:
        """DB failure in control write emits a warning log."""
        mock_repo, mock_session = _build_mock_repo()
        mock_session.commit = AsyncMock(side_effect=RuntimeError("db down"))

        messages: list[str] = []
        sink_id = logger.add(lambda msg: messages.append(str(msg)), level="WARNING")
        try:
            with patch(
                "snapper.server.provenance_middleware.get_repository",
                return_value=mock_repo,
            ):
                app = _create_test_app(db_url=TEST_DB_URL)
                client = TestClient(app)
                client.post("/mutate", content=b'{"x": 1}')
        finally:
            logger.remove(sink_id)

        assert any("Control record write failed" in m for m in messages)


class TestControlRecordingNoDbUrl:
    """When db_url is None, no control recording occurs."""

    def test_no_recording_when_db_url_is_none(self) -> None:
        """Middleware with db_url=None does not attempt DB writes."""
        with patch(
            "snapper.server.provenance_middleware.get_repository",
        ) as mock_get_repo:
            app = _create_test_app(db_url=None)
            client = TestClient(app)
            resp = client.post("/mutate", content=b'{"x": 1}')

        assert resp.status_code == 200
        mock_get_repo.assert_not_called()


class TestControlRecordingRedaction:
    """Control row payload is redacted for sensitive keys."""

    @pytest.mark.asyncio
    async def test_sensitive_keys_redacted_in_control_payload(self) -> None:
        """Passwords and tokens are redacted before control persistence."""
        mock_repo, mock_session = _build_mock_repo()

        with patch(
            "snapper.server.provenance_middleware.get_repository",
            return_value=mock_repo,
        ):
            app = _create_test_app(db_url=TEST_DB_URL)
            client = TestClient(app)
            body = json.dumps({"password": "secret", "name": "test"})
            client.post("/mutate", content=body)

        row = mock_session.add.call_args[0][0]
        parsed = json.loads(row.payload)
        assert parsed["password"] == "[REDACTED]"
        assert parsed["name"] == "test"


class TestGetRequestNotRecorded:
    """GET requests are not recorded to control table when telemetry is off."""

    def test_get_request_skips_control_recording(self) -> None:
        """GET requests bypass the middleware when telemetry is disabled."""
        with patch(
            "snapper.server.provenance_middleware.get_repository",
        ) as mock_get_repo:
            app = _create_test_app(db_url=TEST_DB_URL)
            client = TestClient(app)
            resp = client.get("/read")

        assert resp.status_code == 200
        mock_get_repo.assert_not_called()


class TestTelemetryRecordingEnabled:
    """GET requests are recorded to telemetry table when enabled."""

    @pytest.mark.asyncio
    async def test_telemetry_row_written_on_get(self) -> None:
        """GET request writes a Telemetry row when telemetry is enabled."""
        mock_repo, mock_session = _build_mock_repo()

        with patch(
            "snapper.server.provenance_middleware.get_repository",
            return_value=mock_repo,
        ):
            app = _create_test_app(db_url=TEST_DB_URL, telemetry_enabled=True)
            client = TestClient(app)
            resp = client.get("/read")

        assert resp.status_code == 200
        mock_session.add.assert_called_once()
        row = mock_session.add.call_args[0][0]
        assert row.transport == "rest"
        assert row.direction == "inbound"
        assert row.message_type == "GET /read"
        assert row.payload is None
        mock_session.commit.assert_awaited_once()


class TestTelemetryRecordingDisabled:
    """GET requests skip telemetry when disabled."""

    def test_get_skips_telemetry_when_disabled(self) -> None:
        """GET does not write telemetry when telemetry_enabled is False."""
        with patch(
            "snapper.server.provenance_middleware.get_repository",
        ) as mock_get_repo:
            app = _create_test_app(db_url=TEST_DB_URL, telemetry_enabled=False)
            client = TestClient(app)
            resp = client.get("/read")

        assert resp.status_code == 200
        mock_get_repo.assert_not_called()

    def test_get_skips_telemetry_when_no_db_url(self) -> None:
        """GET does not write telemetry when db_url is None."""
        with patch(
            "snapper.server.provenance_middleware.get_repository",
        ) as mock_get_repo:
            app = _create_test_app(db_url=None, telemetry_enabled=True)
            client = TestClient(app)
            resp = client.get("/read")

        assert resp.status_code == 200
        mock_get_repo.assert_not_called()


class TestTelemetryNonBlocking:
    """Telemetry DB failure does not affect the response."""

    @pytest.mark.asyncio
    async def test_telemetry_db_failure_does_not_break_response(self) -> None:
        """When telemetry write raises, the response is still returned."""
        mock_repo, mock_session = _build_mock_repo()
        mock_session.commit = AsyncMock(side_effect=RuntimeError("db down"))

        with patch(
            "snapper.server.provenance_middleware.get_repository",
            return_value=mock_repo,
        ):
            app = _create_test_app(db_url=TEST_DB_URL, telemetry_enabled=True)
            client = TestClient(app)
            resp = client.get("/read")

        assert resp.status_code == 200

    @pytest.mark.asyncio
    async def test_telemetry_db_failure_logged_as_warning(self) -> None:
        """Telemetry DB failure emits a warning log."""
        mock_repo, mock_session = _build_mock_repo()
        mock_session.commit = AsyncMock(side_effect=RuntimeError("db down"))

        messages: list[str] = []
        sink_id = logger.add(lambda msg: messages.append(str(msg)), level="WARNING")
        try:
            with patch(
                "snapper.server.provenance_middleware.get_repository",
                return_value=mock_repo,
            ):
                app = _create_test_app(db_url=TEST_DB_URL, telemetry_enabled=True)
                client = TestClient(app)
                client.get("/read")
        finally:
            logger.remove(sink_id)

        assert any("Telemetry record write failed" in m for m in messages)


class TestTelemetryRecordingNoDbUrl:
    """Telemetry _record_telemetry short-circuits when db_url is None."""

    @pytest.mark.asyncio
    async def test_record_telemetry_noop_when_db_url_none(self) -> None:
        """Calling _record_telemetry with db_url=None does nothing."""
        middleware = ClientProvenanceMiddleware(MagicMock(), db_url=None)
        with patch(
            "snapper.server.provenance_middleware.get_repository",
        ) as mock_get_repo:
            await middleware._record_telemetry(path="/read")
        mock_get_repo.assert_not_called()


class TestSharedGapDetectors:
    """Middleware populates shared gap_detectors dict for health endpoint."""

    def test_gap_detectors_populated_on_provenance(self) -> None:
        """POST with provenance populates the shared gap_detectors dict."""
        shared_detectors: dict[str, Any] = {}
        mock_repo, _ = _build_mock_repo()

        with patch(
            "snapper.server.provenance_middleware.get_repository",
            return_value=mock_repo,
        ):
            app = _create_test_app(
                db_url=TEST_DB_URL,
                gap_detectors=shared_detectors,
            )
            client = TestClient(app)
            body = json.dumps(
                {
                    "session_id": "shared-sess-1234",
                    "sequence_id": 1,
                    "public_id": "p1",
                }
            )
            client.post("/mutate", content=body)

        assert "shared-sess-1234" in shared_detectors
        assert shared_detectors["shared-sess-1234"].stats.gaps_detected == 0
