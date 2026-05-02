"""Tests for the MCP per-principal rate limiter.

Covers :class:`PrincipalRateLimitMiddleware` — guards against
regressions where ``build_mcp_app`` could drop the rate-limit
surface. The suite exercises the sliding-window behaviour
through a minimal Starlette app so the assertions do not depend
on the heavier FastMCP dispatcher.
"""

from datetime import UTC
from datetime import datetime
from typing import Any
from unittest.mock import MagicMock

import pytest
from fastapi.testclient import TestClient
from limits import parse
from starlette.applications import Starlette
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Route

from snapper.auth.domain.roles import UserRole
from snapper.auth.schemas.tokens import TokenClaims
from snapper.mcp.rate_limiting import MCP_RATE_LIMIT
from snapper.mcp.rate_limiting import PrincipalRateLimitMiddleware
from snapper.mcp.rate_limiting import _principal_key
from snapper.server.rate_limiting import limiter


@pytest.fixture
def enabled_limiter() -> Any:
    """Re-enable the slowapi limiter for tests that exercise throttling.

    The project-wide ``conftest.py`` auto-disables rate limiting so
    unrelated REST tests can run in tight loops without tripping
    429s. The limiter tests in this module need the real behaviour,
    so we flip it back on for the duration of the test and restore
    the disabled state on teardown.
    """
    previous = limiter.enabled
    limiter.enabled = True
    yield
    limiter.enabled = previous


def _claims(user_public_id: str = "user-ratelimit", sub: str = "user-ratelimit") -> TokenClaims:
    """Build a :class:`TokenClaims` for the test harness."""
    now = int(datetime.now(UTC).timestamp())
    return TokenClaims(
        sub=sub,
        username="delegate-rl",
        role=UserRole.AI_DELEGATE,
        permissions=[],
        exp=now + 3600,
        iat=now,
        jti="jti-rl",
        sid="sid-rl",
        user_public_id=user_public_id,
        primary_operator_public_id="op-rl",
    )


class _StubClaimsMiddleware(BaseHTTPMiddleware):
    """Stash a canned :class:`TokenClaims` on request.state for tests.

    Mirrors what :class:`BearerAuthMiddleware` does in production so
    the rate limiter sees the same state it expects at runtime
    without needing a real token or TokenManager singleton.
    """

    def __init__(self, app: Any, claims: TokenClaims | None) -> None:
        """Store the canned claims handed in by the test."""
        super().__init__(app)
        self._claims = claims

    async def dispatch(self, request: Request, call_next: Any) -> Any:
        """Attach claims and defer to downstream."""
        request.state.token_claims = self._claims
        return await call_next(request)


def _build_harness(claims: TokenClaims | None, rate_limit: str = "2/minute") -> Starlette:
    """Compose a small Starlette app with the limiter middleware."""

    def _echo(_request: Request) -> JSONResponse:
        return JSONResponse({"ok": True})

    app = Starlette(routes=[Route("/t", _echo, methods=["POST"])])
    app.add_middleware(PrincipalRateLimitMiddleware, rate_limit_item=parse(rate_limit))
    app.add_middleware(_StubClaimsMiddleware, claims=claims)
    return app


class TestPrincipalKey:
    """Unit-level coverage for the limiter key derivation helper."""

    def test_prefers_user_public_id_when_present(self) -> None:
        """Given claims with a user_public_id, Then key uses it.

        Given: a request whose ``token_claims`` carries a populated
            ``user_public_id``,
        When: :func:`_principal_key` runs,
        Then: the returned key embeds that id in the namespaced
            prefix — matches the per-principal contract.
        """
        request = MagicMock()
        request.state.token_claims = _claims(user_public_id="u-42")
        assert _principal_key(request) == "mcp:user:u-42"

    def test_falls_back_to_sub_when_user_public_id_blank(self) -> None:
        """Given claims without user_public_id, Then key uses sub.

        Given: a legacy claim shape with ``user_public_id=""``,
        When: :func:`_principal_key` runs,
        Then: the key falls back to the JWT ``sub`` claim — keeps
            some throttling rather than bucketing every legacy
            token under a single key.
        """
        request = MagicMock()
        claims = _claims(user_public_id="", sub="legacy-sub")
        request.state.token_claims = claims
        assert _principal_key(request) == "mcp:sub:legacy-sub"

    def test_falls_back_to_ip_when_no_claims(self) -> None:
        """Given no claims, Then key falls back to the client IP.

        Given: a request whose ``token_claims`` is ``None`` (auth
            middleware hasn't populated it),
        When: the key is derived,
        Then: the client host is used so there is still *some*
            throttling rather than none — fail-closed.
        """
        request = MagicMock()
        request.state = MagicMock(spec=["token_claims"])
        request.state.token_claims = None
        request.client.host = "10.0.0.7"
        assert _principal_key(request) == "mcp:ip:10.0.0.7"

    def test_anonymous_key_when_no_claims_and_no_client(self) -> None:
        """Given no claims and no client info, Then key is anonymous.

        Given: request with neither claims nor client host,
        When: the key is derived,
        Then: ``mcp:anonymous`` is returned so downstream storage
            does not receive an empty string.
        """
        request = MagicMock()
        request.state = MagicMock(spec=["token_claims"])
        request.state.token_claims = None
        request.client = None
        assert _principal_key(request) == "mcp:anonymous"


class TestRateLimitMiddleware:
    """End-to-end middleware behaviour via :class:`TestClient`.

    Each test uses a unique ``user_public_id`` so the in-memory
    slowapi storage does not leak state across tests. No reset
    primitive exists on the fixed-window strategy; per-key
    isolation is simpler and still deterministic.
    """

    def test_within_quota_requests_succeed(self, enabled_limiter: Any) -> None:
        """Requests under the cap pass through with 200.

        Given: a 2/minute quota and one authenticated principal,
        When: two sequential POSTs arrive,
        Then: both return 200 and downstream handler executes.
        """
        _ = enabled_limiter
        app = _build_harness(_claims(user_public_id="rl-under"))
        client = TestClient(app)
        r1 = client.post("/t", json={})
        r2 = client.post("/t", json={})
        assert r1.status_code == 200
        assert r2.status_code == 200

    def test_over_quota_returns_429_error_code(self, enabled_limiter: Any) -> None:
        """Third request within window → 429 with vendor-neutral envelope.

        Given: a 2/minute quota and three rapid requests,
        When: the third hits the middleware,
        Then: it responds 429 with
            ``error_code='rate_limit_exceeded'`` and a
            ``Retry-After`` header — pins the wire contract clients
            branch on.
        """
        _ = enabled_limiter
        app = _build_harness(_claims(user_public_id="rl-over"))
        client = TestClient(app)
        for _ in range(2):
            assert client.post("/t", json={}).status_code == 200
        rejected = client.post("/t", json={})
        assert rejected.status_code == 429
        body = rejected.json()
        assert body["error_code"] == "rate_limit_exceeded"
        assert rejected.headers.get("retry-after") == "60"

    def test_different_principals_get_separate_buckets(self, enabled_limiter: Any) -> None:
        """Two delegates with distinct ids do not starve each other.

        Given: a 2/minute quota and two principals each sending up
            to their per-principal cap,
        When: the fourth total request — which would exhaust a
            shared pool — arrives from the second principal,
        Then: it is admitted because the limiter key includes the
            principal id. Pins the per-principal contract.
        """
        _ = enabled_limiter
        app_a = _build_harness(_claims(user_public_id="rl-alpha"))
        app_b = _build_harness(_claims(user_public_id="rl-beta"))
        client_a = TestClient(app_a)
        client_b = TestClient(app_b)
        for _ in range(2):
            assert client_a.post("/t", json={}).status_code == 200
        for _ in range(2):
            assert client_b.post("/t", json={}).status_code == 200

    def test_limiter_disabled_passes_through(self) -> None:
        """Disabled global limiter short-circuits the middleware.

        Given: the global slowapi limiter has been disabled
            (deployment opt-out),
        When: a request arrives,
        Then: it is admitted without consuming a slot — useful for
            load-test and ops deployments that want to turn
            throttling off without editing code.
        """
        limiter.enabled = False
        try:
            app = _build_harness(_claims(user_public_id="no-limit"))
            client = TestClient(app)
            for _ in range(10):
                assert client.post("/t", json={}).status_code == 200
        finally:
            limiter.enabled = True

    def test_default_limit_constant_is_parseable(self) -> None:
        """:data:`MCP_RATE_LIMIT` parses to a valid window.

        Sanity check so a typo in the constant surfaces at import
        time rather than as a runtime throttle failure. Pinning the
        string shape ("N/unit") keeps future edits deliberate.
        """
        parsed = parse(MCP_RATE_LIMIT)
        assert parsed.amount > 0
        assert parsed.GRANULARITY.seconds > 0
