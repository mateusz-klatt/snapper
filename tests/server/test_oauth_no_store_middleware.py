"""Tests for shared OAuth path matching and OAuthNoStoreMiddleware / OAuthNoStoreFastAPI."""

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from pathlib import Path
from uuid import uuid7

import pytest
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from fastapi.testclient import TestClient
from httpx import Response
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.routing import Route
from starlette.types import Message
from starlette.types import Receive
from starlette.types import Scope
from starlette.types import Send

from snapper.auth.dependencies import require_authentication
from snapper.auth.dependencies import validate_csrf_token
from snapper.auth.domain.roles import UserRole
from snapper.auth.schemas.principal import AuthPrincipal
from snapper.config.app import AppSettings
from snapper.config.bootstrap import BootstrapSettingsLoader
from snapper.messaging.infrastructure.publisher import SequenceTracker
from snapper.server import app as server_app
from snapper.server.app import create_app
from snapper.server.dependencies import get_repository_dependency
from snapper.server.mcp_oauth_routes import oauth_settings
from snapper.server.oauth_no_store_middleware import OAuthNoStoreMiddleware
from snapper.server.oauth_paths import is_oauth_credential_path
from snapper.server.oauth_paths import is_oauth_surface_path
from tests.mcp.test_oauth_service import _RESOURCE
from tests.mcp.test_oauth_service import _build_harness
from tests.mcp.test_oauth_service import _principal
from tests.mcp.test_oauth_service import _seed_user_with_membership


class _Flags:
    """In-memory live setting store for ``ai_integration_enabled``."""

    def __init__(self, *, ai_integration_enabled: bool = True) -> None:
        """Initialize the integration flag state.

        Args:
            ai_integration_enabled: Initial live feature flag value.
        """
        self.values: dict[str, bool] = {"ai_integration_enabled": ai_integration_enabled}

    def get_setting(self, key: str, default: bool) -> bool:
        """Return the configured boolean setting or ``default``.

        Args:
            key: Setting name to read.
            default: Value returned when ``key`` is absent.

        Returns:
            Boolean value for ``key``.
        """
        return self.values.get(key, default)


@asynccontextmanager
async def _noop_lifespan(_app: FastAPI) -> AsyncGenerator[None]:
    """Skip production startup/shutdown side effects in HTTP integration tests.

    Args:
        _app: FastAPI application instance.

    Yields:
        Control back to the test client without starting background services.
    """
    yield


def _oauth_settings(*, enabled: bool = True) -> AppSettings:
    """Construct deterministic bootstrap settings for OAuth edge tests.

    Args:
        enabled: Whether ``MCP_OAUTH_ENABLED`` is active.

    Returns:
        Configured :class:`AppSettings` instance.
    """
    return AppSettings(
        BootstrapSettingsLoader(
            _env_file=None,
            MCP_OAUTH_ENABLED=enabled,
            MCP_PUBLIC_RESOURCE_URL=_RESOURCE,
            MCP_OAUTH_ISSUER=_RESOURCE,
        )
    )


def _assert_no_store_headers(response: Response) -> None:
    """Assert every OAuth no-store and no-referrer response header is present.

    Args:
        response: HTTP response returned by TestClient.
    """
    assert response.headers["cache-control"] == "no-store"
    assert response.headers["pragma"] == "no-cache"
    assert response.headers["referrer-policy"] == "no-referrer"


def _assert_no_added_oauth_headers(response: Response) -> None:
    """Assert no OAuth cache/referrer headers were injected onto a non-OAuth response.

    Args:
        response: HTTP response returned by TestClient.
    """
    assert response.headers.get("cache-control") != "no-store"
    assert "pragma" not in response.headers
    assert "referrer-policy" not in response.headers


@pytest.fixture
async def real_app_case(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> AsyncGenerator[tuple[FastAPI, TestClient, _Flags, AuthPrincipal]]:
    """Build the real ``create_app()`` wired to an isolated SQLite harness.

    Args:
        tmp_path: Pytest temporary directory for SQLite storage.
        monkeypatch: Pytest monkeypatch fixture.

    Yields:
        Tuple of ``(app, client, flags, owner_principal)``.
    """
    harness = await _build_harness(tmp_path)
    owner_id = str(uuid7())
    membership = await _seed_user_with_membership(harness.repository, user_public_id=owner_id)
    owner = _principal(owner_id, membership_public_id=membership)
    monkeypatch.setattr(server_app, "get_settings", lambda: _oauth_settings(enabled=True))
    app = create_app()
    app.router.lifespan_context = _noop_lifespan
    app.state.rest_tracker = SequenceTracker()
    flags = _Flags(ai_integration_enabled=True)
    app.state.settings_service = flags
    app.dependency_overrides[oauth_settings] = lambda: _oauth_settings(enabled=True)
    app.dependency_overrides[get_repository_dependency] = lambda: harness.repository
    client = TestClient(app, follow_redirects=False)
    try:
        yield app, client, flags, owner
    finally:
        await harness.repository.engine.dispose()


@pytest.mark.parametrize(
    ("path", "expected"),
    [
        ("/api/mcp/token", True),
        ("/api/mcp/token/", True),
        ("/api/mcp/token?code=SENTINEL", True),
        ("/api/mcp/revoke", True),
        ("/api/mcp/register", True),
        ("/api/mcp/authorize", True),
        ("/api/mcp-oauth/consent/approve", True),
        ("/api/mcp-oauth/consent/deny", True),
        ("/api/mcp-oauth/consent/deny/", True),
        ("/api/mcp/token\n", True),
        ("/api/mcp/revoke\r\n", True),
        ("/api/mcp/register\n", True),
        ("/api/mcp/authorize\n", True),
        ("/api/mcp-oauth/consent/approve\n", True),
        ("/api/mcp-oauth/consent/approve\r\n", True),
        ("/api/mcp-oauth/consent/deny\n", True),
        ("/api/orders\x00", True),
        ("/api/orders\x7f", True),
        ("/api/mcp-oauth/consent", False),
        ("/api/mcp/tokenx", False),
        ("/api/mcp-oauth/consent/approvex", False),
        ("/api/mcp", False),
        ("/api/orders", False),
        ("", False),
        ("/", False),
    ],
)
def test_is_oauth_credential_path_segment_matching(path: str, expected: bool) -> None:
    """Verify segment-aware matching for OAuth credential endpoints.

    Given: Candidate request paths including trailing slashes and sibling prefixes,
    When: ``is_oauth_credential_path`` evaluates each path,
    Then: Exact credential routes, their slash-delimited subpaths, and any path
        carrying a control character (which Starlette's ``$`` still routes) match.

    Args:
        path: Candidate request path.
        expected: Expected boolean classification.
    """
    assert is_oauth_credential_path(path) is expected


@pytest.mark.parametrize(
    ("path", "expected"),
    [
        ("/api/mcp", True),
        ("/api/mcp/", True),
        ("/api/mcp/token", True),
        ("/api/mcp/authorize/", True),
        ("/api/mcp-oauth", True),
        ("/api/mcp-oauth/", True),
        ("/api/mcp-oauth/consent", True),
        ("/api/mcp-oauth/consent/approve", True),
        ("/.well-known/oauth-authorization-server", True),
        ("/.well-known/oauth-authorization-server/api/mcp", True),
        ("/.well-known/oauth-protected-resource", True),
        ("/.well-known/oauth-protected-resource/api/mcp", True),
        ("/.well-known/openid-configuration", True),
        ("/.well-known/openid-configuration/api/mcp", True),
        ("/api/mcpx", False),
        ("/api/mcp-oauthx", False),
        ("/.well-known/oauth-authorization-serverx", False),
        ("/.well-known/oauth-protected-resourcex", False),
        ("/.well-known/openid-configurationx", False),
        ("/.well-known/security.txt", False),
        ("/api/health", False),
        ("/api/mcp-oauth/consent\n", True),
        ("/api/health\t", True),
        ("", False),
        ("/", False),
    ],
)
def test_is_oauth_surface_path_segment_matching(path: str, expected: bool) -> None:
    """Verify segment-aware matching for the full OAuth HTTP surface.

    Given: Candidate request paths covering OAuth routes, discovery paths, and siblings,
    When: ``is_oauth_surface_path`` evaluates each path,
    Then: Exact OAuth surface prefixes, their slash-delimited subpaths, and any
        path carrying a control character match.

    Args:
        path: Candidate request path.
        expected: Expected boolean classification.
    """
    assert is_oauth_surface_path(path) is expected


@pytest.mark.asyncio
async def test_middleware_passes_non_http_scopes_through() -> None:
    """Verify non-HTTP ASGI scopes bypass header wrapping unchanged.

    Given: An ``OAuthNoStoreMiddleware`` wrapping a dummy ASGI app,
    When: A ``lifespan`` scope is dispatched,
    Then: It reaches the wrapped app directly without header manipulation.
    """
    seen_types: list[str] = []

    async def _app(scope: Scope, _receive: Receive, send: Send) -> None:
        """Record incoming scope type and emit a lifespan ack."""
        seen_types.append(scope["type"])
        await send({"type": "lifespan.startup.complete"})

    sent: list[Message] = []

    async def _receive() -> Message:
        """Provide an empty lifespan message."""
        return {"type": "lifespan.startup"}

    async def _send(message: Message) -> None:
        """Record outgoing ASGI events."""
        sent.append(message)

    middleware = OAuthNoStoreMiddleware(_app)
    await middleware({"type": "lifespan", "asgi": {"version": "3.0"}}, _receive, _send)

    assert seen_types == ["lifespan"]
    assert sent == [{"type": "lifespan.startup.complete"}]


def test_middleware_never_overrides_existing_cache_control() -> None:
    """Verify existing response headers are preserved while missing headers are added.

    Given: An OAuth surface handler that sets ``Cache-Control: private, max-age=60``,
    When: The request passes through ``OAuthNoStoreMiddleware``,
    Then: ``Cache-Control`` remains ``private, max-age=60`` while ``Pragma`` and
        ``Referrer-Policy`` are added, and a handler setting all three keeps all three.
    """

    async def _custom_cache(_request: Request) -> JSONResponse:
        """Return a custom Cache-Control header on an OAuth surface path."""
        return JSONResponse({"ok": True}, headers={"Cache-Control": "private, max-age=60"})

    async def _all_set(_request: Request) -> JSONResponse:
        """Return explicit values for all three headers."""
        return JSONResponse(
            {"ok": True},
            headers={
                "Cache-Control": "no-cache",
                "Pragma": "custom-pragma",
                "Referrer-Policy": "same-origin",
            },
        )

    async def _plain(_request: Request) -> JSONResponse:
        """Return a non-OAuth response without caching headers."""
        return JSONResponse({"ok": True})

    app = Starlette(
        routes=[
            Route("/api/mcp/custom", _custom_cache),
            Route("/api/mcp/all-set", _all_set),
            Route("/api/mcp-oauthx", _plain),
        ]
    )
    app.add_middleware(OAuthNoStoreMiddleware)
    client = TestClient(app)

    custom_resp = client.get("/api/mcp/custom")
    assert custom_resp.status_code == 200
    assert custom_resp.headers["cache-control"] == "private, max-age=60"
    assert custom_resp.headers["pragma"] == "no-cache"
    assert custom_resp.headers["referrer-policy"] == "no-referrer"

    all_set_resp = client.get("/api/mcp/all-set")
    assert all_set_resp.headers["cache-control"] == "no-cache"
    assert all_set_resp.headers["pragma"] == "custom-pragma"
    assert all_set_resp.headers["referrer-policy"] == "same-origin"

    plain_resp = client.get("/api/mcp-oauthx")
    assert plain_resp.status_code == 200
    _assert_no_added_oauth_headers(plain_resp)


@pytest.mark.asyncio
async def test_middleware_handles_response_start_without_headers_key() -> None:
    """Verify ``http.response.start`` without a ``headers`` key still receives headers.

    Given: A minimal downstream ASGI app emitting ``http.response.start`` with no ``headers``,
    When: Dispatched through ``OAuthNoStoreMiddleware`` on an OAuth surface path,
    Then: The outgoing ``http.response.start`` contains all three required headers.
    """

    async def _minimal_app(_scope: Scope, _receive: Receive, send: Send) -> None:
        """Send an ``http.response.start`` omitting ``headers``."""
        await send({"type": "http.response.start", "status": 204})
        await send({"type": "http.response.body", "body": b""})

    sent: list[Message] = []

    async def _receive() -> Message:
        """Return a minimal HTTP request event."""
        return {"type": "http.request", "body": b"", "more_body": False}

    async def _send(message: Message) -> None:
        """Capture sent ASGI messages."""
        sent.append(message)

    middleware = OAuthNoStoreMiddleware(_minimal_app)
    await middleware(
        {"type": "http", "method": "GET", "path": "/api/mcp/token"},
        _receive,
        _send,
    )

    assert len(sent) == 2
    assert dict(sent[0]["headers"]) == {
        b"cache-control": b"no-store",
        b"pragma": b"no-cache",
        b"referrer-policy": b"no-referrer",
    }


def test_real_app_dependency_errors_and_non_oauth_paths(
    real_app_case: tuple[FastAPI, TestClient, _Flags, AuthPrincipal],
) -> None:
    """Verify real FastAPI app enforces no-store/no-referrer on all OAuth errors only.

    Given: The real ``create_app()`` application with OAuth enabled,
    When: Requests trigger unauthenticated 401, permission 403, CSRF 403,
        validation 422, feature-flag 503, wrong content-type on token, discovery 200,
        and non-OAuth endpoints,
    Then: Every OAuth surface response carries ``Cache-Control: no-store``,
        ``Pragma: no-cache``, and ``Referrer-Policy: no-referrer``, and non-OAuth
        responses receive none of those added headers.

    Args:
        real_app_case: Fixture providing the real app, client, flags, and owner.
    """
    app, client, flags, owner = real_app_case

    resp_401 = client.get("/api/mcp-oauth/consent", headers={"X-MCP-OAuth-Request": "missing"})
    assert resp_401.status_code == 401
    _assert_no_store_headers(resp_401)

    app.dependency_overrides[require_authentication] = lambda: _principal(
        owner.user_public_id, role=UserRole.VIEWER
    )
    resp_perm_403 = client.get("/api/mcp-oauth/consent", headers={"X-MCP-OAuth-Request": "missing"})
    assert resp_perm_403.status_code == 403
    _assert_no_store_headers(resp_perm_403)

    app.dependency_overrides[require_authentication] = lambda: owner
    resp_csrf_403 = client.post(
        "/api/mcp-oauth/consent/approve",
        json={
            "type": "mcp_oauth_consent_approve_request",
            "public_id": str(uuid7()),
            "session_id": "csrf-test",
            "sequence_id": 1,
            "timestamp": "2026-11-20T12:00:00Z",
            "payload": {"oauth_request": "missing", "replace_existing_grant": False},
        },
    )
    assert resp_csrf_403.status_code == 403
    _assert_no_store_headers(resp_csrf_403)

    app.dependency_overrides[validate_csrf_token] = lambda: None
    resp_422 = client.post("/api/mcp-oauth/consent/approve", json={})
    assert resp_422.status_code == 422
    _assert_no_store_headers(resp_422)

    flags.values["ai_integration_enabled"] = False
    resp_503 = client.get("/api/mcp-oauth/consent", headers={"X-MCP-OAuth-Request": "missing"})
    assert resp_503.status_code == 503
    _assert_no_store_headers(resp_503)

    resp_token = client.post(
        "/api/mcp/token",
        headers={"content-type": "application/json"},
        content=b'{"grant_type": "refresh_token"}',
    )
    assert resp_token.status_code == 400
    _assert_no_store_headers(resp_token)

    resp_discovery = client.get("/.well-known/oauth-authorization-server/api/mcp")
    assert resp_discovery.status_code == 200
    _assert_no_store_headers(resp_discovery)

    resp_openid = client.get("/.well-known/openid-configuration")
    assert resp_openid.status_code == 404
    _assert_no_store_headers(resp_openid)

    resp_non_oauth_200 = client.get("/api/orders")
    assert resp_non_oauth_200.status_code == 200
    _assert_no_added_oauth_headers(resp_non_oauth_200)

    del app.dependency_overrides[require_authentication]
    resp_non_oauth_401 = client.get("/api/orders")
    assert resp_non_oauth_401.status_code == 401
    _assert_no_added_oauth_headers(resp_non_oauth_401)

    resp_non_oauth_404 = client.get("/api/mcp-oauthx")
    assert resp_non_oauth_404.status_code == 404
    _assert_no_added_oauth_headers(resp_non_oauth_404)


def test_real_app_unhandled_500_on_oauth_path_carries_no_store_headers(
    real_app_case: tuple[FastAPI, TestClient, _Flags, AuthPrincipal],
) -> None:
    """Verify unhandled 500s from Starlette ServerErrorMiddleware still get OAuth headers.

    Given: The real ``create_app()`` (``OAuthNoStoreFastAPI``) where an OAuth
        dependency and a non-OAuth route both raise unhandled ``RuntimeError``,
    When: Both are called with ``raise_server_exceptions=False``,
    Then: The OAuth 500 carries ``Cache-Control: no-store``, ``Pragma: no-cache``,
        and ``Referrer-Policy: no-referrer``, while the non-OAuth 500 carries none.

    The non-OAuth route is moved to the front of the router: when
    ``frontend/dist`` exists, ``create_app()`` mounts the SPA catch-all at
    ``/``, which would otherwise answer the late-registered route with a 404.

    Args:
        real_app_case: Fixture providing the real app, client, flags, and owner.
    """
    app, _client, _flags, _owner = real_app_case

    def _boom_settings() -> AppSettings:
        """Raise an unhandled exception inside an OAuth dependency."""
        raise RuntimeError("boom-oauth")

    @app.get("/api/non-oauth-boom")
    async def _non_oauth_boom() -> JSONResponse:
        """Raise an unhandled exception on a non-OAuth route."""
        raise RuntimeError("boom-plain")

    app.router.routes.insert(0, app.router.routes.pop())
    app.dependency_overrides[oauth_settings] = _boom_settings
    error_client = TestClient(app, raise_server_exceptions=False)

    oauth_500 = error_client.get("/api/mcp-oauth/consent")
    assert oauth_500.status_code == 500
    _assert_no_store_headers(oauth_500)

    plain_500 = error_client.get("/api/non-oauth-boom")
    assert plain_500.status_code == 500
    _assert_no_added_oauth_headers(plain_500)
