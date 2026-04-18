"""Day 2c tests for MCP tool handlers (plan §4 Day 2 item 7).

Exercises the two MVP tools exported by
:func:`snapper.mcp.tools.register_mcp_tools`:

    - ``list_instruments`` — read-only, READ_MARKET_DATA permission.
    - ``submit_manual_order`` — write, CREATE_ORDERS permission,
      writes ``TradeCommand`` with ``source_surface="mcp"`` via the
      shared :class:`TradingCapsEnforcer`.

Tests invoke tools directly through FastMCP's tool manager so the
same dispatch path a real MCP client exercises is under test.
"""

from contextlib import asynccontextmanager
from datetime import UTC
from datetime import datetime
from typing import Any
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import pytest
from mcp.server.fastmcp import FastMCP
from mcp.server.fastmcp.exceptions import ToolError

from snapper.application.trade.caps_enforcer import CapsViolationError
from snapper.auth.domain.permissions import ROLE_PERMISSIONS
from snapper.auth.domain.roles import UserRole
from snapper.auth.schemas.tokens import TokenClaims
from snapper.mcp.server import TOKEN_CLAIMS_CTX
from snapper.mcp.server import get_current_claims
from snapper.mcp.tools import register_mcp_tools


def _make_claims(
    role: UserRole = UserRole.AI_DELEGATE,
    user_public_id: str = "user-1",
    username: str = "delegate-1",
) -> TokenClaims:
    """Build a :class:`TokenClaims` for tool-permission tests."""
    now = int(datetime.now(UTC).timestamp())
    return TokenClaims(
        sub=user_public_id,
        username=username,
        role=role,
        permissions=[],
        exp=now + 3600,
        iat=now,
        jti="jti",
        sid="sid",
        user_public_id=user_public_id,
        primary_operator_public_id="op-1",
    )


def _build_server(
    repository: Any = None,
    caps_enforcer: Any = None,
    claims: TokenClaims | None = None,
) -> FastMCP:
    """Construct a FastMCP instance with tools registered + getters wired."""
    server = FastMCP("test")
    register_mcp_tools(
        server,
        repository_getter=lambda: repository,
        caps_enforcer_getter=lambda: caps_enforcer,
        claims_getter=lambda: claims or _make_claims(),
    )
    return server


class TestGetCurrentClaims:
    """Coverage for the ContextVar accessor."""

    def test_returns_value_when_set(self) -> None:
        """Given the ContextVar carries a claims, Then it is returned."""
        claims = _make_claims()
        token = TOKEN_CLAIMS_CTX.set(claims)
        try:
            assert get_current_claims() is claims
        finally:
            TOKEN_CLAIMS_CTX.reset(token)

    def test_raises_when_unset(self) -> None:
        """Unset ContextVar → RuntimeError signals middleware misconfiguration."""
        with pytest.raises(RuntimeError, match="misconfigured"):
            get_current_claims()


class TestListInstrumentsTool:
    """Coverage for the ``list_instruments`` MCP tool."""

    @pytest.mark.asyncio
    async def test_returns_sorted_instruments_for_role_with_permission(self) -> None:
        """Valid AI_DELEGATE call → sorted instrument list for the exchange.

        Given: a repository returning three native symbols in an
            unsorted order and a caller with AI_DELEGATE role (which
            includes READ_MARKET_DATA),
        When: the ``list_instruments`` tool is dispatched,
        Then: the result contains ``instruments`` sorted ascending.
        """
        repo = AsyncMock()
        repo.get_exchange_instruments = AsyncMock(return_value=["BTC-USD", "ADA-USD", "ETH-USD"])
        server = _build_server(repository=repo)
        result = await server._tool_manager.call_tool("list_instruments", {"exchange": "kraken"})
        assert result == {
            "exchange": "kraken",
            "instruments": ["ADA-USD", "BTC-USD", "ETH-USD"],
        }
        repo.get_exchange_instruments.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_raises_when_repository_is_none(self) -> None:
        """Pre-lifespan repository → RuntimeError, not an opaque crash.

        Given: the repository getter returns ``None`` (lifespan has
            not initialized the singleton),
        When: the tool is dispatched,
        Then: ToolError bubbles wrapping the underlying RuntimeError —
            FastMCP standardizes tool failures as :class:`ToolError`
            with the original exception chained on ``__cause__``.
        """
        server = _build_server(repository=None)
        with pytest.raises(ToolError) as exc:
            await server._tool_manager.call_tool("list_instruments", {"exchange": "kraken"})
        assert "Repository not yet initialized" in str(exc.value)

    @pytest.mark.asyncio
    async def test_denied_for_role_without_read_market_data(self) -> None:
        """Role missing READ_MARKET_DATA → PermissionError.

        Given: a contrived role with no permissions,
        When: the tool checks permissions,
        Then: PermissionError is raised with the permission name.
        """
        repo = AsyncMock()
        saved = ROLE_PERMISSIONS.get(UserRole.VIEWER)
        ROLE_PERMISSIONS[UserRole.VIEWER] = set()
        server = FastMCP("test")
        register_mcp_tools(
            server,
            repository_getter=lambda: repo,
            caps_enforcer_getter=lambda: None,
            claims_getter=lambda: _make_claims(role=UserRole.VIEWER),
        )
        try:
            with pytest.raises(ToolError) as exc:
                await server._tool_manager.call_tool("list_instruments", {"exchange": "kraken"})
            assert "read:market_data" in str(exc.value)
        finally:
            if saved is not None:
                ROLE_PERMISSIONS[UserRole.VIEWER] = saved


class TestSubmitManualOrderTool:
    """Coverage for the ``submit_manual_order`` MCP tool."""

    def _make_enforcer_admit(self) -> MagicMock:
        """Build an enforcer whose ``guard`` is a pass-through async ctx."""

        @asynccontextmanager
        async def _admit(submission: Any) -> Any:
            yield None

        enforcer = MagicMock()
        enforcer.guard = _admit
        return enforcer

    def _make_enforcer_reject(self) -> MagicMock:
        """Build an enforcer whose ``guard`` raises :class:`CapsViolationError`."""

        class _Ctx:
            async def __aenter__(self) -> None:
                raise CapsViolationError(
                    cap_type="max_open_orders",
                    attempted=6,
                    limit=5,
                    detail="too many open",
                )

            async def __aexit__(self, *_args: Any) -> None:
                """No-op — __aenter__ rejected before the body ran."""

        enforcer = MagicMock()
        enforcer.guard = MagicMock(return_value=_Ctx())
        return enforcer

    @pytest.mark.asyncio
    async def test_happy_path_writes_trade_command_with_mcp_source_surface(self) -> None:
        """Valid call → both plan + command inserted with source_surface='mcp'.

        Given: caller holds CREATE_ORDERS (AI_DELEGATE), a cap-admit
            enforcer, and a repo returning synthetic UUID7s,
        When: ``submit_manual_order`` is dispatched,
        Then: the returned payload carries the plan + command UUIDs
            AND the command insert received ``source_surface="mcp"``.
        """
        repo = AsyncMock()
        repo.insert_execution_plan = AsyncMock(return_value=(1, "plan-pid"))
        repo.insert_trade_command = AsyncMock(return_value=(2, "cmd-pid"))
        server = _build_server(
            repository=repo,
            caps_enforcer=self._make_enforcer_admit(),
        )
        result = await server._tool_manager.call_tool(
            "submit_manual_order",
            {
                "exchange": "kraken",
                "instrument": "BTC-USD",
                "instrument_public_id": "inst-1",
                "side": "buy",
                "order_type": "limit",
                "quantity": 0.5,
                "wallet_public_id": "wallet-1",
                "idempotency_key": "idem-1",
                "price": 50000.0,
            },
        )
        assert result["plan_public_id"] == "plan-pid"
        assert result["command_public_id"] == "cmd-pid"
        assert result["source_surface"] == "mcp"
        cmd_row = repo.insert_trade_command.await_args.args[0]
        assert cmd_row["source_surface"] == "mcp"

    @pytest.mark.asyncio
    async def test_caps_violation_propagates(self) -> None:
        """Enforcer rejection → CapsViolationError surfaces to MCP client.

        Given: the caps guard raises on entry,
        When: ``submit_manual_order`` runs,
        Then: CapsViolationError propagates and the repository insert
            is NEVER invoked — mirroring REST 422 behavior.
        """
        repo = AsyncMock()
        repo.insert_execution_plan = AsyncMock()
        repo.insert_trade_command = AsyncMock()
        server = _build_server(
            repository=repo,
            caps_enforcer=self._make_enforcer_reject(),
        )
        with pytest.raises(ToolError) as exc:
            await server._tool_manager.call_tool(
                "submit_manual_order",
                {
                    "exchange": "kraken",
                    "instrument": "BTC-USD",
                    "instrument_public_id": "inst-1",
                    "side": "buy",
                    "order_type": "market",
                    "quantity": 1.0,
                    "wallet_public_id": "wallet-1",
                    "idempotency_key": "idem-x",
                },
            )
        assert "max_open_orders" in str(exc.value)
        repo.insert_execution_plan.assert_not_called()
        repo.insert_trade_command.assert_not_called()

    @pytest.mark.asyncio
    async def test_denied_for_role_without_create_orders(self) -> None:
        """Caller missing CREATE_ORDERS → PermissionError, no DB touch."""
        repo = AsyncMock()
        saved = ROLE_PERMISSIONS.get(UserRole.VIEWER)
        ROLE_PERMISSIONS[UserRole.VIEWER] = set()
        try:
            server = FastMCP("test")
            register_mcp_tools(
                server,
                repository_getter=lambda: repo,
                caps_enforcer_getter=lambda: None,
                claims_getter=lambda: _make_claims(role=UserRole.VIEWER),
            )
            with pytest.raises(ToolError) as exc:
                await server._tool_manager.call_tool(
                    "submit_manual_order",
                    {
                        "exchange": "kraken",
                        "instrument": "BTC-USD",
                        "instrument_public_id": "inst-1",
                        "side": "buy",
                        "order_type": "market",
                        "quantity": 1.0,
                        "wallet_public_id": "wallet-1",
                        "idempotency_key": "idem-y",
                    },
                )
            assert "create:orders" in str(exc.value)
        finally:
            if saved is not None:
                ROLE_PERMISSIONS[UserRole.VIEWER] = saved

    @pytest.mark.asyncio
    async def test_raises_when_repository_or_enforcer_none(self) -> None:
        """Pre-lifespan repo / enforcer → RuntimeError.

        Given: neither the repository nor the caps enforcer is
            initialized yet,
        When: the write tool is dispatched,
        Then: RuntimeError is raised with the lifespan-startup message.
        """
        server = _build_server(repository=None, caps_enforcer=None)
        with pytest.raises(ToolError) as exc:
            await server._tool_manager.call_tool(
                "submit_manual_order",
                {
                    "exchange": "kraken",
                    "instrument": "BTC-USD",
                    "instrument_public_id": "inst-1",
                    "side": "buy",
                    "order_type": "market",
                    "quantity": 1.0,
                    "wallet_public_id": "wallet-1",
                    "idempotency_key": "idem-z",
                },
            )
        assert "lifespan startup" in str(exc.value)

    @pytest.mark.asyncio
    async def test_idempotency_conflict_maps_to_http_409(self) -> None:
        """Duplicate idempotency_key from repo → HTTP 409.

        Given: the repository's ``insert_execution_plan`` raises with
            a unique-constraint violation string,
        When: the write tool runs,
        Then: :class:`fastapi.HTTPException` with ``status_code=409``
            bubbles up so the MCP client can distinguish retry-safe
            from retry-unsafe errors.
        """
        from fastapi import HTTPException

        repo = AsyncMock()
        repo.insert_execution_plan = AsyncMock(
            side_effect=RuntimeError("UNIQUE constraint failed: execution_plans.idempotency_key")
        )
        repo.insert_trade_command = AsyncMock()
        server = _build_server(
            repository=repo,
            caps_enforcer=self._make_enforcer_admit(),
        )
        with pytest.raises(ToolError) as exc:
            await server._tool_manager.call_tool(
                "submit_manual_order",
                {
                    "exchange": "kraken",
                    "instrument": "BTC-USD",
                    "instrument_public_id": "inst-1",
                    "side": "buy",
                    "order_type": "market",
                    "quantity": 1.0,
                    "wallet_public_id": "wallet-1",
                    "idempotency_key": "idem-dup",
                },
            )
        cause = exc.value.__cause__
        assert isinstance(cause, HTTPException)
        assert cause.status_code == 409

    @pytest.mark.asyncio
    async def test_plan_insert_non_unique_error_reraises_verbatim(self) -> None:
        """Non-unique plan-insert failure → the original exception re-raises.

        Given: ``insert_execution_plan`` fails with a generic error
            not matching the uniqueness keywords,
        When: ``submit_manual_order`` runs,
        Then: the original exception propagates — so upstream
            operator alerting is not silently swallowed.
        """
        repo = AsyncMock()
        repo.insert_execution_plan = AsyncMock(side_effect=RuntimeError("db unavailable"))
        server = _build_server(
            repository=repo,
            caps_enforcer=self._make_enforcer_admit(),
        )
        with pytest.raises(ToolError) as exc:
            await server._tool_manager.call_tool(
                "submit_manual_order",
                {
                    "exchange": "kraken",
                    "instrument": "BTC-USD",
                    "instrument_public_id": "inst-1",
                    "side": "buy",
                    "order_type": "market",
                    "quantity": 1.0,
                    "wallet_public_id": "wallet-1",
                    "idempotency_key": "idem-broken",
                },
            )
        assert "db unavailable" in str(exc.value)
