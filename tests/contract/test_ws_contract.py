"""Vendor-neutral WebSocket contract tests.

Companion to ``tests/contract/test_mcp_contract.py``. The WS endpoint
must stay callable by any standards-compliant WebSocket client
(``websockets``, browser ``WebSocket``, ``wscat``, ...) using only:

    - the standard ``Authorization: Bearer <jwt>`` upgrade header
      (for MCP / CLI clients without cookie jars),
    - JSON frames over a plain WebSocket — no vendor envelope,
    - documented close codes (``4401`` auth, ``4003`` deactivation).

This suite also covers the subscribe-time AI_DELEGATE wallet-scope
filter — the wire contract is that an AI_DELEGATE subscribing to a mix
of in-scope and out-of-scope wallet-scoped topics gets a standard
``WSSubscriptionSuccessResponse`` with out-of-scope topics listed in
``denied_topics`` (no vendor envelope, no hidden error code).
Mid-session revalidation via ``admin.scope_revoked`` is out of scope
here. The suite pairs with ``make check-vendor-neutral`` so the wire
contract and the source-text contract both guard the seam.
"""

import json as _json
from datetime import UTC
from datetime import datetime
from typing import Any
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest

import snapper.auth.websocket_auth as ws_auth_mod
import snapper.interface.websocket.handlers.auth as auth_handler_mod
from snapper.auth.domain.roles import UserRole
from snapper.auth.schemas.principal import AuthPrincipal
from snapper.auth.schemas.tokens import TokenClaims
from snapper.auth.websocket_auth import WebSocketAuthManager
from snapper.core.types import SubscriptionStatusEnum
from snapper.interface.websocket.handlers.subscribe import handle_subscribe
from snapper.interface.websocket.schemas import WSSubscribeRequest
from snapper.messaging.infrastructure.publisher import SequenceTracker


def _token_claims(username: str = "delegate-ws-contract") -> TokenClaims:
    """Build a :class:`TokenClaims` pinned to AI_DELEGATE."""
    now = int(datetime.now(UTC).timestamp())
    return TokenClaims(
        sub="user-ws",
        username=username,
        role=UserRole.AI_DELEGATE,
        permissions=None,
        exp=now + 3600,
        iat=now,
        jti="jti-ws",
        sid="sid-ws",
        user_public_id="user-ws",
        operator_public_ids=["op-ws"],
        primary_operator_public_id="op-ws",
    )


def _fake_websocket(
    auth_header: str | None = None,
    cookie_token: str | None = None,
) -> MagicMock:
    """Return a mock :class:`WebSocket` with the requested transport artifacts.

    The mock mirrors the two surfaces the Starlette ``WebSocket``
    object exposes: ``headers.get("authorization")`` and
    ``cookies.get("access_token")``. Configure one or the other to
    simulate an MCP / CLI client (header-only) or a browser client
    (cookie-only).
    """
    ws = MagicMock()
    ws.headers = {"authorization": auth_header} if auth_header is not None else {}
    ws.cookies = {"access_token": cookie_token} if cookie_token is not None else {}
    return ws


class TestWsBearerTransportContract:
    """Bearer header MUST be honoured on the WS upgrade.

    The test suite boots a :class:`WebSocketAuthManager` singleton
    with a patched :class:`TokenManager` so the contract assertions
    focus on *transport surface* rather than cryptographic detail.
    """

    @pytest.mark.asyncio
    async def test_bearer_header_only_is_accepted(self) -> None:
        """Bearer-only client → auth succeeds with the JWT from the header.

        Given: a WebSocket upgrade carrying ``Authorization: Bearer …``
            and NO cookies (an MCP / CLI client has no cookie jar),
        When: the auth manager verifies the session,
        Then: the token manager is invoked with the header-extracted
            token and a :class:`AuthPrincipal` + :class:`TokenClaims`
            tuple is returned.
        """
        manager = WebSocketAuthManager()
        repo = MagicMock(get_ai_delegate_by_user_public_id=AsyncMock(return_value=None))
        ws = _fake_websocket(auth_header="Bearer header-only-token")
        claims = _token_claims()
        with patch.object(
            manager.token_manager,
            "verify_token_with_db",
            new=AsyncMock(return_value=claims),
        ) as mock_verify:
            result = await manager.verify_session_cookie(ws, repo)

        assert result is not None
        principal, returned_claims = result
        assert isinstance(principal, AuthPrincipal)
        assert returned_claims is claims
        mock_verify.assert_awaited_once_with("header-only-token", repo)

    @pytest.mark.asyncio
    async def test_cookie_fallback_is_accepted(self) -> None:
        """Cookie-only browser client → still works.

        Given: a WebSocket upgrade carrying ONLY the ``access_token``
            cookie (browser client, no ``Authorization`` header),
        When: the auth manager verifies the session,
        Then: the token manager receives the cookie token — keeps the
            browser flow working while extending the contract with
            a new transport option.
        """
        manager = WebSocketAuthManager()
        repo = MagicMock(get_ai_delegate_by_user_public_id=AsyncMock(return_value=None))
        ws = _fake_websocket(cookie_token="cookie-only-token")
        claims = _token_claims(username="browser-operator")
        with patch.object(
            manager.token_manager,
            "verify_token_with_db",
            new=AsyncMock(return_value=claims),
        ) as mock_verify:
            result = await manager.verify_session_cookie(ws, repo)

        assert result is not None
        mock_verify.assert_awaited_once_with("cookie-only-token", repo)

    @pytest.mark.asyncio
    async def test_bearer_preferred_over_cookie_when_both_present(self) -> None:
        """Header takes precedence over cookie.

        Given: both ``Authorization: Bearer …`` and ``access_token``
            cookie are present,
        When: the manager resolves the transport,
        Then: the Bearer token is used — a deliberate MCP override
            never competes with a leftover browser cookie.
        """
        manager = WebSocketAuthManager()
        repo = MagicMock(get_ai_delegate_by_user_public_id=AsyncMock(return_value=None))
        ws = _fake_websocket(
            auth_header="Bearer header-token",
            cookie_token="cookie-token",
        )
        claims = _token_claims()
        with patch.object(
            manager.token_manager,
            "verify_token_with_db",
            new=AsyncMock(return_value=claims),
        ) as mock_verify:
            await manager.verify_session_cookie(ws, repo)

        mock_verify.assert_awaited_once_with("header-token", repo)

    @pytest.mark.asyncio
    async def test_missing_header_and_cookie_returns_none(self) -> None:
        """No transport → return ``None`` so caller can emit ``auth_failed``.

        Given: the upgrade has neither header nor cookie,
        When: ``verify_session_cookie`` runs,
        Then: it returns ``None`` without invoking the token manager —
            the caller then sends the documented ``auth_failed`` frame
            and closes with code ``4401``.
        """
        manager = WebSocketAuthManager()
        repo = MagicMock(get_ai_delegate_by_user_public_id=AsyncMock(return_value=None))
        ws = _fake_websocket()
        with patch.object(
            manager.token_manager,
            "verify_token_with_db",
            new=AsyncMock(return_value=None),
        ) as mock_verify:
            result = await manager.verify_session_cookie(ws, repo)

        assert result is None
        mock_verify.assert_not_called()

    @pytest.mark.asyncio
    async def test_non_bearer_scheme_falls_through(self) -> None:
        """``Authorization: Basic …`` → treated as no header → cookie fallback.

        Given: a browser that also sends a :class:`Basic` header (for
            another upstream proxy) plus a real ``access_token``
            cookie,
        When: the manager resolves the transport,
        Then: the non-Bearer scheme is ignored and the cookie token
            is used — mirrors the REST behaviour and keeps the
            contract predictable.
        """
        manager = WebSocketAuthManager()
        repo = MagicMock(get_ai_delegate_by_user_public_id=AsyncMock(return_value=None))
        ws = _fake_websocket(
            auth_header="Basic dXNlcjpwYXNz",
            cookie_token="cookie-only-token",
        )
        with patch.object(
            manager.token_manager,
            "verify_token_with_db",
            new=AsyncMock(return_value=_token_claims()),
        ) as mock_verify:
            await manager.verify_session_cookie(ws, repo)

        mock_verify.assert_awaited_once_with("cookie-only-token", repo)


class TestWsBearerExtractionContract:
    """Shape of the Bearer-header extraction — the vendor-neutral primitive."""

    def test_empty_header_returns_none(self) -> None:
        """Empty ``Authorization`` → no token.

        Given: ``Authorization: ``,
        When: the helper runs,
        Then: returns ``None``. Guards against empty-string tokens
            being interpreted as valid.
        """
        manager = WebSocketAuthManager()
        ws = _fake_websocket(auth_header="")
        assert manager._extract_ws_bearer_token(ws) is None

    def test_bearer_prefix_extracted(self) -> None:
        """Standard ``Bearer <token>`` → returns the token.

        Given: ``Authorization: Bearer abc.def.ghi``,
        When: the helper runs,
        Then: returns ``"abc.def.ghi"``.
        """
        manager = WebSocketAuthManager()
        ws = _fake_websocket(auth_header="Bearer abc.def.ghi")
        assert manager._extract_ws_bearer_token(ws) == "abc.def.ghi"

    def test_lowercase_scheme_accepted(self) -> None:
        """Scheme match is case-insensitive per RFC 7235.

        Given: ``Authorization: bearer abc``,
        When: the helper runs,
        Then: returns ``"abc"``.
        """
        manager = WebSocketAuthManager()
        ws = _fake_websocket(auth_header="bearer abc")
        assert manager._extract_ws_bearer_token(ws) == "abc"

    def test_whitespace_only_token_returns_none(self) -> None:
        """``Bearer    `` (whitespace only) → ``None``.

        Given: scheme only, empty credential,
        When: the helper runs,
        Then: returns ``None`` rather than an empty string token.
        """
        manager = WebSocketAuthManager()
        ws = _fake_websocket(auth_header="Bearer    ")
        assert manager._extract_ws_bearer_token(ws) is None


class TestWsFrameContractConstants:
    """Documented close codes are part of the wire contract.

    Clients — including ``websockets`` CLI tooling used during
    incident triage — branch on the close code to distinguish
    "retry with fresh token" (``4401``) from "user deactivated"
    (``4003``). The assertions below pin BOTH codes so a rename on
    either side is caught by the contract test.
    """

    def test_auth_close_code_4401_present_in_handler(self) -> None:
        """The auth-handler source MUST reference close code ``4401``.

        Given: the WebSocket auth handler module,
        When: its source is inspected,
        Then: the literal ``4401`` appears — a regression to any
            other status code breaks client retry logic.
        """
        with open(auth_handler_mod.__file__, encoding="utf-8") as fh:
            body = fh.read()

        assert "4401" in body

    def test_deactivation_close_code_4003_present_in_ws_auth(self) -> None:
        """The WS auth manager source MUST reference close code ``4003``.

        Given: the ``WebSocketAuthManager`` admin-bus subscriber
            that closes matching sessions on ``admin.user_deactivated``,
        When: its source is inspected,
        Then: the literal ``4003`` appears — the documented code
            client UX uses to prompt a full re-login rather than a
            silent refresh.
        """
        with open(ws_auth_mod.__file__, encoding="utf-8") as fh:
            body = fh.read()

        assert "4003" in body


class TestWsSingletonStateIsolation:
    """The WebSocketAuthManager singleton MUST not leak state across sessions."""

    def test_singleton_reuse(self) -> None:
        """Calling ``WebSocketAuthManager()`` twice returns the same instance.

        Given: the singleton protocol documented on the class,
        When: two callers instantiate,
        Then: they receive the same object. A contract test because
            MCP and browser transports share the admin-bus listener
            owned by this singleton and every instance MUST see the
            same ``admin.user_deactivated`` events.
        """
        assert WebSocketAuthManager() is WebSocketAuthManager()

    @pytest.mark.asyncio
    async def test_authenticated_connections_registry_is_shared(self) -> None:
        """A connection registered on one reference is visible on the other.

        Given: the singleton contract,
        When: a connection is recorded via one reference,
        Then: another reference sees the same registry dict.
        """
        m1 = WebSocketAuthManager()
        m2 = WebSocketAuthManager()
        sentinel_ws: Any = MagicMock(name="ws-sentinel")
        principal = AuthPrincipal(username="probe", role=UserRole.VIEWER)
        m1.authenticated_connections[sentinel_ws] = principal
        try:
            assert m2.authenticated_connections[sentinel_ws] is principal
        finally:
            del m1.authenticated_connections[sentinel_ws]


class TestWsAiDelegateWalletScopeContract:
    """The subscribe-time wallet-scope filter wire contract.

    AI_DELEGATE subscribes to a mix of in-scope and out-of-scope
    wallet-scoped topics. The response MUST be a standard
    ``WSSubscriptionSuccessResponse`` with out-of-scope topics listed
    in ``denied_topics`` (no vendor envelope, no hidden error code).
    Non-wallet-scoped topics (market, system, paper-signals) pass
    through unchanged.
    """

    @pytest.mark.asyncio
    async def test_ai_delegate_mixed_subscribe_surfaces_denied_topics(self) -> None:
        """Wire contract: AI_DELEGATE mixed subscribe → standard envelope.

        Given: AI_DELEGATE principal with scope covering
            ``(kraken, BTC-USD)`` only,
        When: the client subscribes to the four-topic mix
            ``[signals.kraken.BTC-USD.live,
              signals.kraken.ETH-USD.live,
              market.kraken.BTC-USD.ticks,
              signals.paper.BTC-USD.my_strategy]``,
        Then: the single response frame is a ``subscription_success``
            envelope with ``status in {"denied", "partial"}``,
            ``denied_topics`` includes ``signals.kraken.ETH-USD.live``,
            and ``topics`` (accepted) includes the market +
            paper-signals pass-throughs. MCP / CLI clients rely on
            this being a plain-JSON frame on the standard WS endpoint
            — NO wallet-filter-specific error code, NO structured
            prefix in a separate error frame.
        """
        ws = AsyncMock()
        manager = MagicMock()
        manager.get_client_subscriptions = MagicMock(return_value=set())
        manager.subscribe_client = MagicMock()
        manager.zmq_bridge = MagicMock()
        manager.zmq_bridge.add_subscription = AsyncMock()
        manager.tracker = SequenceTracker()

        repo = AsyncMock()
        repo.list_scope_grant_instrument_pairs = AsyncMock(return_value={("kraken", "BTC-USD")})
        principal = AuthPrincipal(
            username="delegate",
            role=UserRole.AI_DELEGATE,
            user_public_id="user-contract",
            operator_public_ids=["op-contract"],
            delegate_public_id="delegate-contract",
        )
        message = WSSubscribeRequest(
            public_id="contract-pid",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            session_id="",
            sequence_id=0,
            topics=[
                "signals.kraken.BTC-USD.live",
                "signals.kraken.ETH-USD.live",
                "market.kraken.BTC-USD.ticks",
                "signals.paper.BTC-USD.my_strategy",
            ],
        )

        await handle_subscribe(ws, message, manager, principal, repo)

        frames = [call.args[0] for call in ws.send_text.call_args_list]
        assert len(frames) == 1, "AI_DELEGATE mixed subscribe must surface a SINGLE envelope"
        response: dict[str, Any] = _json.loads(frames[0])
        assert response["type"] == "subscription_success"
        assert response["status"] in {
            SubscriptionStatusEnum.DENIED.value,
            SubscriptionStatusEnum.PARTIAL.value,
        }
        assert "signals.kraken.ETH-USD.live" in response["denied_topics"]
        accepted = set(response.get("topics") or [])
        assert "signals.kraken.ETH-USD.live" not in accepted
        assert "market.kraken.BTC-USD.ticks" in accepted
        assert "signals.kraken.BTC-USD.live" in accepted, (
            "AI_DELEGATE holds READ_SIGNALS and the (kraken, BTC-USD) pair is in "
            "scope, so the subscribe-time wallet filter + category RBAC must "
            "BOTH accept this topic. Any regression here indicates the "
            "signals-category split (READ_SIGNALS vs START_STRATEGIES) was "
            "broken."
        )
