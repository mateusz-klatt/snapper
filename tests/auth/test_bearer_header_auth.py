"""Bearer-header auth coverage for REST + WebSocket.

Exercises the auth extensions added for MCP / CLI clients that
have no cookie jar:

- ``get_current_user`` consults the ``Authorization: Bearer <jwt>``
  header FIRST and falls back to the ``access_token`` cookie.
- ``validate_csrf_token`` is skipped when a Bearer header is present
  on any state-changing request.
- ``POST /api/auth/login?return_tokens=true`` and
  ``POST /api/auth/refresh?return_tokens=true`` embed the minted
  tokens in the response body alongside the cookie set.
- ``POST /api/auth/refresh`` reads the refresh JWT from the Bearer
  header FIRST, cookie second.
- :meth:`WebSocketAuthManager.verify_session_cookie` reads the
  WebSocket upgrade ``Authorization`` header FIRST, cookie second.
"""

from datetime import UTC
from datetime import datetime
from unittest.mock import AsyncMock
from unittest.mock import Mock
from unittest.mock import patch

import pytest
from fastapi import HTTPException
from fastapi import Request

from snapper.auth.dependencies import _extract_bearer_token
from snapper.auth.dependencies import get_current_user
from snapper.auth.dependencies import validate_csrf_token
from snapper.auth.domain.roles import UserRole
from snapper.auth.routes import _extract_refresh_bearer_token
from snapper.auth.routes import _should_return_tokens
from snapper.auth.schemas.tokens import TokenClaims
from snapper.auth.websocket_auth import WebSocketAuthManager


def _make_request(
    *,
    headers: dict[str, str] | None = None,
    cookies: dict[str, str] | None = None,
    method: str = "POST",
    query_params: dict[str, str] | None = None,
) -> Request:
    """Build a Mock(spec=Request) with the given headers/cookies."""
    request = Mock(spec=Request)
    request.headers = headers or {}
    request.cookies = cookies or {}
    request.method = method
    request.query_params = query_params or {}
    request.state = Mock()
    return request


class TestExtractBearerToken:
    """Parser for ``Authorization: Bearer <jwt>`` REST headers."""

    def test_returns_token_when_bearer_present(self) -> None:
        """Given a ``Bearer <jwt>`` header, Then the JWT is returned verbatim."""
        request = _make_request(headers={"authorization": "Bearer abc.def.ghi"})
        assert _extract_bearer_token(request) == "abc.def.ghi"

    def test_returns_none_when_header_absent(self) -> None:
        """Given no Authorization header, Then ``None`` is returned."""
        request = _make_request()
        assert _extract_bearer_token(request) is None

    def test_returns_none_for_non_bearer_scheme(self) -> None:
        """Given a ``Basic ...`` header, Then ``None`` (not a Bearer grant)."""
        request = _make_request(headers={"authorization": "Basic dXNlcjpwYXNz"})
        assert _extract_bearer_token(request) is None

    def test_returns_none_for_malformed_single_token_header(self) -> None:
        """Given a header with no scheme prefix, Then ``None``.

        ``"abc.def.ghi"`` alone is malformed — two parts required.
        """
        request = _make_request(headers={"authorization": "abc.def.ghi"})
        assert _extract_bearer_token(request) is None

    def test_returns_none_for_empty_token_after_bearer(self) -> None:
        """Given ``Bearer  `` (empty payload), Then ``None``."""
        request = _make_request(headers={"authorization": "Bearer   "})
        assert _extract_bearer_token(request) is None

    def test_case_insensitive_scheme_match(self) -> None:
        """Given ``bearer`` lowercased, Then still recognized (RFC 7235)."""
        request = _make_request(headers={"authorization": "bearer tok.en"})
        assert _extract_bearer_token(request) == "tok.en"


class TestGetCurrentUserBearer:
    """Bearer header path for :func:`get_current_user`."""

    def _token_claims(self) -> TokenClaims:
        now = int(datetime.now(UTC).timestamp())
        return TokenClaims(
            sub="user-1",
            username="alice",
            role=UserRole.OPERATOR,
            permissions=["read:orders"],
            exp=now + 3600,
            iat=now,
            jti="jti-1",
            sid="sid-1",
        )

    @pytest.mark.asyncio
    async def test_bearer_header_path_authenticates(self) -> None:
        """Given a Bearer header, Then the principal is derived from the JWT.

        Cookie is absent; the header carries the token alone.
        """
        request = _make_request(headers={"authorization": "Bearer h.e.ader-jwt"})
        claims = self._token_claims()
        repo = Mock(get_ai_delegate_by_user_public_id=AsyncMock(return_value=None))
        with patch("snapper.auth.dependencies.get_token_manager") as mock_get:
            mgr = Mock()
            mgr.verify_token_with_db = AsyncMock(return_value=claims)
            mock_get.return_value = mgr
            principal = await get_current_user(request, repo)
        assert principal is not None
        assert principal.username == "alice"
        mgr.verify_token_with_db.assert_awaited_once_with("h.e.ader-jwt", repo)

    @pytest.mark.asyncio
    async def test_bearer_header_takes_precedence_over_cookie(self) -> None:
        """Given both Bearer header and cookie, Then the header wins.

        The header is the authoritative path for MCP clients; the
        cookie must not override it silently.
        """
        request = _make_request(
            headers={"authorization": "Bearer header.jwt"},
            cookies={"access_token": "cookie.jwt"},
        )
        claims = self._token_claims()
        repo = Mock(get_ai_delegate_by_user_public_id=AsyncMock(return_value=None))
        with patch("snapper.auth.dependencies.get_token_manager") as mock_get:
            mgr = Mock()
            mgr.verify_token_with_db = AsyncMock(return_value=claims)
            mock_get.return_value = mgr
            await get_current_user(request, repo)
        mgr.verify_token_with_db.assert_awaited_once_with("header.jwt", repo)


class TestValidateCsrfTokenBearerSkip:
    """Bearer-bearing requests bypass CSRF."""

    def test_bearer_header_skips_csrf_validation(self) -> None:
        """Given a Bearer header, Then no CSRF checks run.

        The handler returns without inspecting cookies / X-CSRF-Token
        / origin — which it otherwise would for a POST.
        """
        request = _make_request(
            method="POST",
            headers={"authorization": "Bearer some.jwt"},
            cookies={},
        )
        validate_csrf_token(request, csrf_token=None)


class TestReturnTokensFlag:
    """``?return_tokens=true`` opt-in for body-embedded JWTs."""

    def test_true_query_param_enables_return(self) -> None:
        """Given ``?return_tokens=true``, Then the helper returns True."""
        request = _make_request(query_params={"return_tokens": "true"})
        assert _should_return_tokens(request) is True

    def test_case_insensitive_true(self) -> None:
        """Given ``?return_tokens=TRUE`` (upper-case), Then still True."""
        request = _make_request(query_params={"return_tokens": "TRUE"})
        assert _should_return_tokens(request) is True

    def test_absent_query_param_returns_false(self) -> None:
        """Given no query param, Then False — cookie-only flow preserved."""
        request = _make_request()
        assert _should_return_tokens(request) is False

    def test_other_value_returns_false(self) -> None:
        """Given ``?return_tokens=1``, Then False — only literal "true" opts in."""
        request = _make_request(query_params={"return_tokens": "1"})
        assert _should_return_tokens(request) is False


class TestExtractRefreshBearer:
    """Bearer header path for the refresh endpoint."""

    def test_returns_token_for_bearer_refresh(self) -> None:
        """Given ``Authorization: Bearer rjwt``, Then ``rjwt`` is returned."""
        request = _make_request(headers={"authorization": "Bearer refresh.jwt"})
        assert _extract_refresh_bearer_token(request) == "refresh.jwt"

    def test_returns_none_without_header(self) -> None:
        """Given no Authorization header, Then ``None`` — cookie fallback."""
        request = _make_request()
        assert _extract_refresh_bearer_token(request) is None

    def test_returns_none_for_non_bearer_scheme(self) -> None:
        """Given ``Basic ...``, Then ``None``."""
        request = _make_request(headers={"authorization": "Basic creds"})
        assert _extract_refresh_bearer_token(request) is None

    def test_returns_none_for_empty_payload(self) -> None:
        """Given ``Bearer  `` (empty), Then ``None``."""
        request = _make_request(headers={"authorization": "Bearer "})
        assert _extract_refresh_bearer_token(request) is None


class TestWebSocketBearerAuth:
    """WebSocket handshake Bearer header path."""

    def _make_ws(
        self,
        *,
        headers: dict[str, str] | None = None,
        cookies: dict[str, str] | None = None,
    ) -> Mock:
        """Build a Mock WebSocket with header + cookie dicts."""
        ws = Mock()
        ws.headers = headers or {}
        ws.cookies = cookies or {}
        return ws

    @pytest.mark.asyncio
    async def test_bearer_header_path_authenticates(self) -> None:
        """Given a Bearer header on the WS upgrade, Then principal is built.

        Cookie is absent; the WebSocket manager resolves the token
        via the header-first path.
        """
        now = int(datetime.now(UTC).timestamp())
        claims = TokenClaims(
            sub="ai-user",
            username="ai-delegate-1",
            role=UserRole.AI_DELEGATE,
            permissions=["read:market_data"],
            exp=now + 3600,
            iat=now,
            jti="jti-ws",
            sid="sid-ws",
        )
        ws = self._make_ws(headers={"authorization": "Bearer ws.jwt"})

        manager = WebSocketAuthManager()
        WebSocketAuthManager._initialized = False
        manager.__init__()

        repo = Mock(get_ai_delegate_by_user_public_id=AsyncMock(return_value=None))
        with patch.object(manager, "token_manager") as token_manager_mock:
            token_manager_mock.verify_token_with_db = AsyncMock(return_value=claims)
            result = await manager.verify_session_cookie(ws, repo)

        assert result is not None
        principal, _claims = result
        assert principal.username == "ai-delegate-1"
        token_manager_mock.verify_token_with_db.assert_awaited_once_with("ws.jwt", repo)

    @pytest.mark.asyncio
    async def test_ai_delegate_principal_carries_delegate_public_id(self) -> None:
        """AI_DELEGATE WS upgrade -> ``AuthPrincipal.delegate_public_id`` populated.

        Plan D Q19 + Phase 1 #7 fix-up — the WS auth chain MUST
        mirror the REST chain's ``ai_delegates`` lookup so the
        per-frame scope filter (``enforce_ai_review_scope``) finds
        a non-None delegate id. Without this, every legitimate
        AI_DELEGATE WS subscriber gets dropped by the filter and
        silently receives no ``ai_reviews.*`` events.

        Given an AI_DELEGATE token + a repo whose
        ``get_ai_delegate_by_user_public_id`` returns a delegate row,
        When ``verify_session_cookie`` resolves the principal,
        Then ``delegate_public_id`` is populated from the row.
        """
        now = int(datetime.now(UTC).timestamp())
        claims = TokenClaims(
            sub="ai-user",
            username="ai-delegate-2",
            role=UserRole.AI_DELEGATE,
            permissions=["read:signals"],
            exp=now + 3600,
            iat=now,
            jti="jti-ws-2",
            sid="sid-ws-2",
            user_public_id="ai-user",
        )
        ws = self._make_ws(headers={"authorization": "Bearer ws.jwt"})
        manager = WebSocketAuthManager()
        WebSocketAuthManager._initialized = False
        manager.__init__()
        repo = Mock(
            get_ai_delegate_by_user_public_id=AsyncMock(
                return_value={
                    "public_id": "del-9",
                    "user_public_id": "ai-user",
                    "last_seen_at": None,
                    "active_reviews_count": 0,
                    "created_at": datetime.now(UTC),
                    "updated_at": datetime.now(UTC),
                }
            )
        )
        with patch.object(manager, "token_manager") as token_manager_mock:
            token_manager_mock.verify_token_with_db = AsyncMock(return_value=claims)
            result = await manager.verify_session_cookie(ws, repo)
        assert result is not None
        principal, _claims = result
        assert principal.delegate_public_id == "del-9"
        repo.get_ai_delegate_by_user_public_id.assert_awaited_once_with("ai-user")

    @pytest.mark.asyncio
    async def test_non_ai_delegate_role_skips_delegate_lookup(self) -> None:
        """OPERATOR / VIEWER WS upgrade -> no delegate lookup; field stays None.

        Given an OPERATOR token (not AI_DELEGATE),
        When ``verify_session_cookie`` resolves the principal,
        Then ``get_ai_delegate_by_user_public_id`` is never awaited
        and ``delegate_public_id`` stays None.
        """
        now = int(datetime.now(UTC).timestamp())
        claims = TokenClaims(
            sub="op-user",
            username="op",
            role=UserRole.OPERATOR,
            permissions=["create:orders"],
            exp=now + 3600,
            iat=now,
            jti="jti-ws-3",
            sid="sid-ws-3",
        )
        ws = self._make_ws(headers={"authorization": "Bearer ws.jwt"})
        manager = WebSocketAuthManager()
        WebSocketAuthManager._initialized = False
        manager.__init__()
        repo = Mock(get_ai_delegate_by_user_public_id=AsyncMock())
        with patch.object(manager, "token_manager") as token_manager_mock:
            token_manager_mock.verify_token_with_db = AsyncMock(return_value=claims)
            result = await manager.verify_session_cookie(ws, repo)
        assert result is not None
        principal, _claims = result
        assert principal.delegate_public_id is None
        repo.get_ai_delegate_by_user_public_id.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_bearer_header_takes_precedence_over_cookie(self) -> None:
        """Given both WS header and cookie, Then the header wins."""
        now = int(datetime.now(UTC).timestamp())
        claims = TokenClaims(
            sub="u",
            username="user",
            role=UserRole.OPERATOR,
            permissions=[],
            exp=now + 3600,
            iat=now,
            jti="j",
            sid="s",
        )
        ws = self._make_ws(
            headers={"authorization": "Bearer ws.header"},
            cookies={"access_token": "ws.cookie"},
        )

        manager = WebSocketAuthManager()
        WebSocketAuthManager._initialized = False
        manager.__init__()

        repo = Mock(get_ai_delegate_by_user_public_id=AsyncMock(return_value=None))
        with patch.object(manager, "token_manager") as token_manager_mock:
            token_manager_mock.verify_token_with_db = AsyncMock(return_value=claims)
            await manager.verify_session_cookie(ws, repo)

        token_manager_mock.verify_token_with_db.assert_awaited_once_with("ws.header", repo)

    @pytest.mark.asyncio
    async def test_both_missing_returns_none(self) -> None:
        """Given neither header nor cookie, Then ``None`` — 401 flow upstream."""
        ws = self._make_ws()
        manager = WebSocketAuthManager()
        WebSocketAuthManager._initialized = False
        manager.__init__()
        repo = Mock(get_ai_delegate_by_user_public_id=AsyncMock(return_value=None))
        assert await manager.verify_session_cookie(ws, repo) is None

    @pytest.mark.asyncio
    async def test_ws_bearer_non_bearer_scheme_falls_back_to_cookie(self) -> None:
        """Given a non-Bearer scheme on WS header and a valid cookie, Then cookie wins."""
        now = int(datetime.now(UTC).timestamp())
        claims = TokenClaims(
            sub="u",
            username="user",
            role=UserRole.VIEWER,
            permissions=[],
            exp=now + 3600,
            iat=now,
            jti="j",
            sid="s",
        )
        ws = self._make_ws(
            headers={"authorization": "Basic credz"},
            cookies={"access_token": "cookie.jwt"},
        )
        manager = WebSocketAuthManager()
        WebSocketAuthManager._initialized = False
        manager.__init__()
        repo = Mock(get_ai_delegate_by_user_public_id=AsyncMock(return_value=None))
        with patch.object(manager, "token_manager") as token_manager_mock:
            token_manager_mock.verify_token_with_db = AsyncMock(return_value=claims)
            await manager.verify_session_cookie(ws, repo)
        token_manager_mock.verify_token_with_db.assert_awaited_once_with("cookie.jwt", repo)


class TestCsrfStillRequiredWithoutBearer:
    """Regression: cookie-only state-changing requests still need CSRF.

    Without a Bearer header, ``validate_csrf_token`` must still raise
    on missing / mismatched CSRF tokens — bearer skip is narrow.
    """

    def test_cookie_only_post_without_csrf_raises_403(self) -> None:
        """Given POST with no bearer and no CSRF, Then 403 is raised."""
        request = _make_request(
            method="POST",
            headers={},
            cookies={},
        )
        with pytest.raises(HTTPException) as exc:
            validate_csrf_token(request, csrf_token=None)
        assert exc.value.status_code == 403
