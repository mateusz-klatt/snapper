"""HTTP-surface tests for ``POST /api/auth/ws_token``.

Covers the ws_token-issuance route used by long-running WebSocket
clients to mint one-shot tokens. The route authenticates via the
access bearer (header or cookie), pulls the session-id from the
verified access-token claims, and delegates token minting to
:class:`WsTokenService`.

These tests exercise:

- Happy path: valid access bearer → ``200`` with payload carrying
  ``ws_token``, ``ws_token_exp``, ``expires_in`` and the canonical
  envelope provenance fields.
- Session-id binding: the minted ws_token's ``sid_hash`` matches
  ``compute_sid_hash`` of the access-token's ``sid`` claim.
- Auth chain: missing bearer → ``401`` from ``require_authentication``.
- State invariant: a regression in the auth chain that drops the
  token claims state surfaces as ``500``.
- Rate limiting: ``WS_TOKEN_RATE_LIMIT`` caps minting per source IP.
- ws_token fields are sane: ``expires_in`` matches
  ``ws_token_exp - now`` modulo provenance timestamping.
"""

from collections.abc import AsyncGenerator
from collections.abc import Generator
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from unittest.mock import Mock

import pytest
from fastapi import FastAPI
from fastapi import HTTPException
from fastapi.testclient import TestClient

from snapper.api.auth.services.ws_token_service import WsTokenService
from snapper.api.auth.services.ws_token_service import compute_sid_hash
from snapper.auth.dependencies import require_authentication
from snapper.auth.dependencies import validate_csrf_token
from snapper.auth.domain.roles import UserRole
from snapper.auth.routes import _get_authenticated_token_claims
from snapper.auth.schemas.principal import AuthPrincipal
from snapper.auth.schemas.tokens import TokenClaims
from snapper.messaging.infrastructure.publisher import SequenceTracker
from snapper.server.app import create_app
from snapper.server.rate_limiting import WS_TOKEN_RATE_LIMIT
from snapper.server.rate_limiting import limiter

_TEST_USERNAME = "watcher"
_TEST_USER_PUBLIC_ID = "user-pid-1"
_TEST_SID = "session-1"


async def _noop_lifespan(_app: FastAPI) -> AsyncGenerator[None]:
    """Disable the FastAPI lifespan so tests don't spin up ZMQ + DB."""
    yield


def _principal() -> AuthPrincipal:
    """Authenticated principal for the watch use case (OPERATOR)."""
    return AuthPrincipal(
        username=_TEST_USERNAME,
        role=UserRole.OPERATOR,
        user_public_id=_TEST_USER_PUBLIC_ID,
        operator_public_ids=["op-1"],
        primary_operator_public_id="op-1",
    )


def _claims() -> TokenClaims:
    """Access-token claims that would be attached by ``get_current_user``."""
    now = int(datetime.now(UTC).timestamp())
    return TokenClaims(
        sub=_TEST_USER_PUBLIC_ID,
        username=_TEST_USERNAME,
        role=UserRole.OPERATOR,
        permissions=["read:signals"],
        exp=now + 3600,
        iat=now,
        jti="jti-access-1",
        sid=_TEST_SID,
        user_public_id=_TEST_USER_PUBLIC_ID,
        operator_public_ids=["op-1"],
        primary_operator_public_id="op-1",
    )


def _create_client(
    *,
    principal: AuthPrincipal | None = None,
    claims: TokenClaims | None = None,
) -> TestClient:
    """Build a TestClient with auth + token-claims dependency overridden."""
    app = create_app()
    app.router.lifespan_context = _noop_lifespan
    app.state.rest_tracker = SequenceTracker()

    def _skip_csrf() -> None:
        return None

    if principal is None:
        app.dependency_overrides[validate_csrf_token] = _skip_csrf
        return TestClient(app)

    def _auth() -> AuthPrincipal:
        return principal

    def _token_claims() -> TokenClaims:
        if claims is None:
            raise AssertionError("claims must be supplied alongside principal")
        return claims

    app.dependency_overrides[validate_csrf_token] = _skip_csrf
    app.dependency_overrides[require_authentication] = _auth
    app.dependency_overrides[_get_authenticated_token_claims] = _token_claims
    return TestClient(app)


@pytest.fixture(autouse=True)
def _reset_ws_token_singleton() -> Generator[None]:
    """Clear the WsTokenService singleton between tests."""
    WsTokenService.clear_instance()
    yield
    WsTokenService.clear_instance()


class TestWsTokenRouteHappyPath:
    """``POST /api/auth/ws_token`` mints a token for an authenticated caller."""

    def test_returns_payload_with_ws_token_fields(self) -> None:
        """Valid access bearer → 200; payload carries ws_token + expiry fields.

        Asserts the canonical envelope shape: ``payload.type == "ws_token"``,
        ``payload.ws_token`` non-empty, ``payload.ws_token_exp`` parseable
        ISO 8601, and ``payload.expires_in`` close to the configured TTL.
        """
        client = _create_client(principal=_principal(), claims=_claims())
        response = client.post("/api/auth/ws_token")
        assert response.status_code == 200
        body = response.json()
        payload = body["payload"]
        assert payload["type"] == "ws_token"
        assert payload["message"] == "ws_token issued"
        assert isinstance(payload["ws_token"], str) and payload["ws_token"]
        ws_token_exp = datetime.fromisoformat(payload["ws_token_exp"])
        assert ws_token_exp > datetime.now(UTC) - timedelta(seconds=5)
        assert payload["expires_in"] >= 0
        assert body["session_id"] == client.app.state.rest_tracker.session_id
        assert body["sequence_id"] >= 1
        client.close()

    def test_minted_token_binds_to_access_session(self) -> None:
        """The minted ws_token's ``sid_hash`` matches the access-token sid.

        Closes the security-relevant invariant that the route does
        NOT pick a session-id from request body / query — it pulls
        from the verified access claims, so a caller cannot mint a
        token bound to a session they do not own.
        """
        claims = _claims()
        client = _create_client(principal=_principal(), claims=claims)
        response = client.post("/api/auth/ws_token")
        assert response.status_code == 200
        token = response.json()["payload"]["ws_token"]
        verified = WsTokenService.get_instance().verify(
            token,
            expected_sub=_TEST_USERNAME,
            expected_sid_hash=compute_sid_hash(claims.sid),
        )
        assert verified.sub == _TEST_USERNAME
        assert verified.sid_hash == compute_sid_hash(claims.sid)
        assert verified.purpose == "ws_connect"
        client.close()

    def test_ticket_carries_the_access_scope_not_the_principal(self) -> None:
        """The minted ticket's context comes from the verified access claims.

        Given: Access claims carrying ``["read:signals"]`` while the principal
            carries no permissions at all,
        When: A ws_token is minted,
        Then: The ticket carries the CLAIMS' scope.

        The two sources diverge on purpose. ``AuthPrincipal`` leaves
        ``permissions`` at ``None``, which downstream means the FULL role grant
        — so a route that reads the principal instead of the claims would widen
        a deliberately narrowed token, and would do it silently. Asserting the
        narrow value is what distinguishes the two code paths; asserting merely
        "some context is present" would pass either way.
        """
        claims = _claims()
        client = _create_client(principal=_principal(), claims=claims)
        response = client.post("/api/auth/ws_token")
        assert response.status_code == 200
        verified = WsTokenService.get_instance().verify(
            response.json()["payload"]["ws_token"],
            expected_sub=_TEST_USERNAME,
            expected_sid_hash=compute_sid_hash(claims.sid),
        )
        assert verified.authorization_context_version == 1
        assert verified.permissions == ["read:signals"]
        assert verified.permissions != _principal().permissions
        client.close()

    def test_ticket_carries_the_wallet_selection(self) -> None:
        """The client-selected wallet round-trips into the ticket.

        Given: Access claims carrying an active wallet,
        When: A ws_token is minted,
        Then: The ticket carries that wallet.

        This value has no other route into a rebuilt principal — the database
        deliberately does not store it — so if the ticket drops it, a
        reconnecting client silently loses its wallet selection.
        """
        claims = _claims().model_copy(
            update={"active_wallet_public_id": "019e873c-d062-720f-85df-fd4d7fce5bdf"}
        )
        client = _create_client(principal=_principal(), claims=claims)
        response = client.post("/api/auth/ws_token")
        assert response.status_code == 200
        verified = WsTokenService.get_instance().verify(
            response.json()["payload"]["ws_token"],
            expected_sub=_TEST_USERNAME,
            expected_sid_hash=compute_sid_hash(claims.sid),
        )
        assert verified.active_wallet_public_id == "019e873c-d062-720f-85df-fd4d7fce5bdf"
        client.close()

    def test_route_preserves_an_empty_permission_scope(self) -> None:
        """A zero-permission access token mints a zero-permission ticket.

        Given: Access claims whose permissions are an EMPTY list,
        When: A ws_token is minted through the route,
        Then: The ticket carries `[]`, not None.

        The service-level test of this invariant cannot protect the route: it
        never executes the route's assignment. With both other route fixtures
        carrying non-empty permissions, inserting `or None` HERE would widen a
        zero-scope session to the full role grant while leaving every other
        test green. That gap was real until this test existed.
        """
        claims = _claims().model_copy(update={"permissions": []})
        client = _create_client(principal=_principal(), claims=claims)
        response = client.post("/api/auth/ws_token")
        assert response.status_code == 200
        verified = WsTokenService.get_instance().verify(
            response.json()["payload"]["ws_token"],
            expected_sub=_TEST_USERNAME,
            expected_sid_hash=compute_sid_hash(claims.sid),
        )
        assert verified.permissions == []
        assert verified.permissions is not None

    def test_route_passes_through_the_scope_version(self) -> None:
        """The access token's scope version reaches the ticket unchanged.

        Given: Access claims carrying an explicit scope version,
        When: A ws_token is minted,
        Then: The ticket carries that same version.

        This path does not reproject permissions, so it must NOT stamp the
        current constant: doing so would reinterpret an older scope under
        today's compatibility rules. The other route fixtures leave the version
        unset, so without this assertion, replacing it with None or with the
        current constant would pass unnoticed.
        """
        claims = _claims().model_copy(update={"permission_scope_version": 2})
        client = _create_client(principal=_principal(), claims=claims)
        response = client.post("/api/auth/ws_token")
        assert response.status_code == 200
        verified = WsTokenService.get_instance().verify(
            response.json()["payload"]["ws_token"],
            expected_sub=_TEST_USERNAME,
            expected_sid_hash=compute_sid_hash(claims.sid),
        )
        assert verified.permission_scope_version == 2

    def test_route_preserves_a_full_role_scope_as_none(self) -> None:
        """A full-role access token mints a ticket carrying None, not `[]`.

        Given: Access claims whose permissions are None — the full role grant,
        When: A ws_token is minted through the route,
        Then: The ticket carries None.

        This is the MIRROR of the empty-scope test, and it needs saying because
        a distinction has two directions and defending one is not defending it.
        `permissions or []` here narrows a full-role session to zero scope. It
        is the less alarming direction — it denies rather than grants — but it
        would break every legacy and full-role session the moment a reader
        starts consuming the context, and nothing else in the suite catches it.
        """
        claims = _claims().model_copy(update={"permissions": None})
        client = _create_client(principal=_principal(), claims=claims)
        response = client.post("/api/auth/ws_token")
        assert response.status_code == 200
        verified = WsTokenService.get_instance().verify(
            response.json()["payload"]["ws_token"],
            expected_sub=_TEST_USERNAME,
            expected_sid_hash=compute_sid_hash(claims.sid),
        )
        assert verified.permissions is None
        assert verified.authorization_context_version == 1


class TestWsTokenRouteAuthChain:
    """Auth-chain failure surfaces from ``require_authentication``."""

    def test_missing_bearer_returns_401(self) -> None:
        """No access bearer → 401 from ``require_authentication``."""
        client = _create_client()
        response = client.post("/api/auth/ws_token")
        assert response.status_code == 401
        client.close()


class TestWsTokenRouteStateInvariant:
    """Regression: the state-fetch helper guards against missing claims."""

    def test_returns_500_when_token_claims_state_missing(self) -> None:
        """Override returning a non-TokenClaims surfaces 500.

        Models a regression in :func:`get_current_user` where the
        principal is set but ``request.state.token_data`` is left
        unset. The state-fetch helper raises 500 rather than letting
        the route mint a ws_token with an unverified session-id.
        """
        app = create_app()
        app.router.lifespan_context = _noop_lifespan
        app.state.rest_tracker = SequenceTracker()

        def _skip_csrf() -> None:
            return None

        def _auth() -> AuthPrincipal:
            return _principal()

        app.dependency_overrides[validate_csrf_token] = _skip_csrf
        app.dependency_overrides[require_authentication] = _auth
        client = TestClient(app, raise_server_exceptions=False)
        response = client.post("/api/auth/ws_token")
        assert response.status_code == 500
        client.close()


class TestWsTokenRouteRateLimit:
    """``WS_TOKEN_RATE_LIMIT`` caps per-IP minting."""

    def test_burst_above_limit_returns_429(self) -> None:
        """Hitting the route past the per-minute limit yields 429.

        ``WS_TOKEN_RATE_LIMIT`` is parsed at "10/minute" by default.
        We re-enable the slowapi limiter (the autouse
        ``disable_rate_limiting`` fixture turns it off globally so
        unrelated tests don't drift into 429 territory), hit the
        route ``budget+1`` times, and assert the final request
        crosses the limiter.
        """
        budget, unit = WS_TOKEN_RATE_LIMIT.split("/", 1)
        assert unit.startswith("minute")
        n = int(budget)
        was_enabled = limiter.enabled
        limiter.enabled = True
        limiter.reset()
        try:
            client = _create_client(principal=_principal(), claims=_claims())
            for _ in range(n):
                ok = client.post("/api/auth/ws_token")
                assert ok.status_code == 200
            blocked = client.post("/api/auth/ws_token")
            assert blocked.status_code == 429
            client.close()
        finally:
            limiter.reset()
            limiter.enabled = was_enabled


class TestWsTokenRouteExpiresIn:
    """Sanity: ``expires_in`` is a non-negative integer near the TTL."""

    def test_expires_in_matches_ttl(self) -> None:
        """``expires_in`` ≈ ``ws_token_ttl_seconds`` from settings.

        Allows ±5s slack to absorb the request round-trip + provenance
        timestamping inside the route. Pinning the lower bound at
        ``ttl - 5`` catches a regression where the route drops the
        seconds-to-expiry below the configured TTL.
        """
        client = _create_client(principal=_principal(), claims=_claims())
        response = client.post("/api/auth/ws_token")
        assert response.status_code == 200
        ttl = WsTokenService.get_instance().settings.ws_token_ttl_seconds
        expires_in: int = response.json()["payload"]["expires_in"]
        assert ttl - 5 <= expires_in <= ttl + 5
        client.close()


class TestGetAuthenticatedTokenClaimsHelper:
    """Direct unit coverage for the ``_get_authenticated_token_claims`` dep."""

    def test_returns_claims_when_state_populated(self) -> None:
        """Given ``request.state.token_data`` is a TokenClaims, Then return it."""
        request = Mock()
        claims = _claims()
        request.state.token_data = claims
        result = _get_authenticated_token_claims(request, _principal())
        assert result is claims

    def test_raises_500_when_state_missing(self) -> None:
        """Given ``request.state.token_data`` is unset, Then raise 500."""
        request = Mock()
        request.state = type("S", (), {})()
        with pytest.raises(HTTPException) as exc:
            _get_authenticated_token_claims(request, _principal())
        assert exc.value.status_code == 500

    def test_raises_500_when_state_wrong_type(self) -> None:
        """Given ``request.state.token_data`` is not a TokenClaims, Then 500."""
        request = Mock()
        request.state.token_data = {"sid": "spoofed"}
        with pytest.raises(HTTPException) as exc:
            _get_authenticated_token_claims(request, _principal())
        assert exc.value.status_code == 500
