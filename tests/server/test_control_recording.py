"""Tests for control recording in ClientProvenanceMiddleware."""

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
from starlette.testclient import TestClient

from snapper.server.provenance_middleware import ClientProvenanceMiddleware

TEST_DB_URL = "sqlite+aiosqlite:///:memory:"


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
) -> Starlette:
    """Build a minimal Starlette app with the provenance middleware."""
    routes = [
        Route("/mutate", handler, methods=["POST", "PUT", "DELETE", "PATCH"]),
        Route("/error", _error_handler, methods=["POST"]),
        Route("/exception", _exception_handler, methods=["POST"]),
        Route("/read", _echo_handler, methods=["GET"]),
    ]
    app = Starlette(routes=routes)
    app.add_middleware(ClientProvenanceMiddleware, db_url=db_url)
    return app


class TestControlRecordingOk:
    """Control row is written for successful mutations."""

    @pytest.mark.asyncio
    async def test_control_row_written_on_success(self) -> None:
        """A successful POST mutation writes a control row with outcome=ok."""
        mock_session = AsyncMock(add=MagicMock())
        mock_ctx = AsyncMock()
        mock_ctx.__aenter__ = AsyncMock(return_value=mock_session)
        mock_ctx.__aexit__ = AsyncMock(return_value=False)
        mock_repo = MagicMock()
        mock_repo.session.return_value = mock_ctx

        with patch(
            "snapper.server.provenance_middleware.get_repository",
            return_value=mock_repo,
        ):
            app = _create_test_app(db_url=TEST_DB_URL)
            client = TestClient(app)
            body = json.dumps({"session_id": "s1", "sequence_id": 1, "public_id": "p1"})
            resp = client.post("/mutate", content=body)

        assert resp.status_code == 200
        mock_session.add.assert_called_once()
        row = mock_session.add.call_args[0][0]
        assert row.transport == "rest"
        assert row.direction == "inbound"
        assert row.outcome == "ok"
        assert row.detail is None
        assert "[REDACTED]" not in (row.payload or "")
        mock_session.commit.assert_awaited_once()


class TestControlRecordingError:
    """Control row records error outcome for HTTP 4xx/5xx."""

    @pytest.mark.asyncio
    async def test_control_row_outcome_error_on_4xx(self) -> None:
        """HTTP 422 response writes a control row with outcome=error."""
        mock_session = AsyncMock(add=MagicMock())
        mock_ctx = AsyncMock()
        mock_ctx.__aenter__ = AsyncMock(return_value=mock_session)
        mock_ctx.__aexit__ = AsyncMock(return_value=False)
        mock_repo = MagicMock()
        mock_repo.session.return_value = mock_ctx

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
        mock_session = AsyncMock(add=MagicMock())
        mock_ctx = AsyncMock()
        mock_ctx.__aenter__ = AsyncMock(return_value=mock_session)
        mock_ctx.__aexit__ = AsyncMock(return_value=False)
        mock_repo = MagicMock()
        mock_repo.session.return_value = mock_ctx

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
        mock_session = AsyncMock(add=MagicMock())
        mock_session.commit = AsyncMock(side_effect=RuntimeError("db down"))
        mock_ctx = AsyncMock()
        mock_ctx.__aenter__ = AsyncMock(return_value=mock_session)
        mock_ctx.__aexit__ = AsyncMock(return_value=False)
        mock_repo = MagicMock()
        mock_repo.session.return_value = mock_ctx

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
        mock_session = AsyncMock(add=MagicMock())
        mock_session.commit = AsyncMock(side_effect=RuntimeError("db down"))
        mock_ctx = AsyncMock()
        mock_ctx.__aenter__ = AsyncMock(return_value=mock_session)
        mock_ctx.__aexit__ = AsyncMock(return_value=False)
        mock_repo = MagicMock()
        mock_repo.session.return_value = mock_ctx

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
        mock_session = AsyncMock(add=MagicMock())
        mock_ctx = AsyncMock()
        mock_ctx.__aenter__ = AsyncMock(return_value=mock_session)
        mock_ctx.__aexit__ = AsyncMock(return_value=False)
        mock_repo = MagicMock()
        mock_repo.session.return_value = mock_ctx

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
    """GET requests are not recorded to control table."""

    def test_get_request_skips_control_recording(self) -> None:
        """GET requests bypass the middleware entirely."""
        with patch(
            "snapper.server.provenance_middleware.get_repository",
        ) as mock_get_repo:
            app = _create_test_app(db_url=TEST_DB_URL)
            client = TestClient(app)
            resp = client.get("/read")

        assert resp.status_code == 200
        mock_get_repo.assert_not_called()
