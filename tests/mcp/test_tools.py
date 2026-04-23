"""tests for MCP tool handlers.

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
from fastapi import HTTPException
from mcp.server.fastmcp import FastMCP
from mcp.server.fastmcp.exceptions import ToolError
from sqlalchemy.exc import IntegrityError

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
        operator_public_ids=["op-1"],
        primary_operator_public_id="op-1",
    )


def _allow_wallet(repo: Any, wallet_public_id: str = "wallet-1") -> None:
    """Configure a mock repo so ``validate_user_wallet_scope`` admits the wallet.

    The helper sets ``list_accessible_wallets_for_operators`` on the
    AsyncMock to return the single row every ``submit_manual_order``
    test in this module uses. Call before dispatching the tool.
    """
    repo.list_accessible_wallets_for_operators = AsyncMock(
        return_value=[{"public_id": wallet_public_id}]
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
        _allow_wallet(repo)
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
        scope_args = repo.list_accessible_wallets_for_operators.call_args.args
        assert scope_args[0] == ["op-1"]
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
        _allow_wallet(repo)
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
    async def test_idempotency_conflict_postgres_sqlstate_maps_to_http_409(self) -> None:
        """PostgreSQL unique-violation SQLSTATE 23505 → HTTP 409.

        Given: ``insert_execution_plan`` raises :class:`IntegrityError`
            whose driver-level ``orig`` exception carries
            ``pgcode='23505'`` (PostgreSQL standard SQLSTATE for
            unique-constraint violation),
        When: the write tool runs,
        Then: :class:`fastapi.HTTPException` with ``status_code=409``
            bubbles up. Detection uses structured inspection of
            ``orig.pgcode``, NOT substring matching — a reformatted
            message from a psycopg version bump won't silently break
            this.
        """

        class _PgOrigError(Exception):
            pgcode = "23505"

        repo = AsyncMock()
        repo.insert_execution_plan = AsyncMock(
            side_effect=IntegrityError("INSERT failed", params=None, orig=_PgOrigError())
        )
        repo.insert_trade_command = AsyncMock()
        _allow_wallet(repo)
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
                    "idempotency_key": "idem-dup-pg",
                },
            )
        cause = exc.value.__cause__
        assert isinstance(cause, HTTPException)
        assert cause.status_code == 409

    @pytest.mark.asyncio
    async def test_idempotency_conflict_sqlite_extcode_maps_to_http_409(self) -> None:
        """SQLite extended result code 2067 → HTTP 409.

        Given: ``insert_execution_plan`` raises :class:`IntegrityError`
            whose driver ``orig`` exception carries
            ``sqlite_errorcode=2067``
            (``SQLITE_CONSTRAINT_UNIQUE``),
        When: the write tool runs,
        Then: HTTP 409 — parallel path to the PostgreSQL test using
            the SQLite-native code.
        """

        class _SqliteOrigError(Exception):
            sqlite_errorcode = 2067

        repo = AsyncMock()
        repo.insert_execution_plan = AsyncMock(
            side_effect=IntegrityError("INSERT failed", params=None, orig=_SqliteOrigError())
        )
        repo.insert_trade_command = AsyncMock()
        _allow_wallet(repo)
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
                    "idempotency_key": "idem-dup-sqlite",
                },
            )
        cause = exc.value.__cause__
        assert isinstance(cause, HTTPException)
        assert cause.status_code == 409

    @pytest.mark.asyncio
    async def test_plan_integrity_error_non_unique_reraises(self) -> None:
        """IntegrityError with non-unique-violation code → reraises verbatim.

        Given: ``insert_execution_plan`` raises :class:`IntegrityError`
            whose ``orig`` has NEITHER ``pgcode=23505`` NOR
            ``sqlite_errorcode=2067`` (e.g., a CHECK or FK
            violation — usually a bug, not a user conflict),
        When: the write tool runs,
        Then: the IntegrityError propagates untouched — we do not
            squash arbitrary constraint failures into a misleading
            409, preserving operator alerting signal.
        """

        class _CheckOrigError(Exception):
            pgcode = "23514"

        repo = AsyncMock()
        repo.insert_execution_plan = AsyncMock(
            side_effect=IntegrityError("INSERT failed", params=None, orig=_CheckOrigError())
        )
        _allow_wallet(repo)
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
        assert "IntegrityError" in str(exc.value) or "INSERT failed" in str(exc.value)

    @pytest.mark.asyncio
    async def test_integrity_error_with_none_orig_reraises(self) -> None:
        """IntegrityError without a driver exception reraises verbatim.

        Given: an IntegrityError where ``exc.orig`` is ``None``
            (rare — usually means SQLAlchemy synthesized the error
            itself rather than wrapping a driver-level one),
        When: the write tool runs,
        Then: reraise — the structured inspection has nothing to
            check, so we do not speculate about whether it was a
            unique violation.
        """
        repo = AsyncMock()
        repo.insert_execution_plan = AsyncMock(
            side_effect=IntegrityError("synthetic", params=None, orig=None)
        )
        _allow_wallet(repo)
        server = _build_server(
            repository=repo,
            caps_enforcer=self._make_enforcer_admit(),
        )
        with pytest.raises(ToolError):
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
                    "idempotency_key": "idem-none-orig",
                },
            )

    @pytest.mark.asyncio
    async def test_command_insert_failure_compensates_plan_to_failed(self) -> None:
        """Command-insert failure → plan is flipped to ``status='failed'``.

        Given: the plan insert succeeds but the trade-command insert
            raises,
        When: ``submit_manual_order`` runs,
        Then: the tool calls ``update_execution_plan_status`` with
            ``new_status='failed'`` before re-raising, preventing an
            orphaned pending plan — matches REST ``create_order``
            compensation in order_routes.py.
        """
        repo = AsyncMock()
        repo.insert_execution_plan = AsyncMock(return_value=(1, "plan-pid"))
        repo.insert_trade_command = AsyncMock(side_effect=RuntimeError("broker down"))
        repo.update_execution_plan_status = AsyncMock(return_value=2)
        _allow_wallet(repo)
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
                    "idempotency_key": "idem-cmd-fail",
                },
            )
        assert "broker down" in str(exc.value)
        repo.update_execution_plan_status.assert_awaited_once()
        compensation_kwargs = repo.update_execution_plan_status.await_args.kwargs
        assert compensation_kwargs["public_id"] == "plan-pid"
        assert compensation_kwargs["new_status"] == "failed"

    @pytest.mark.asyncio
    async def test_compensation_failure_still_reraises_original(self) -> None:
        """If plan-status compensation itself fails, original error still bubbles.

        Given: command insert fails AND the compensation call
            (``update_execution_plan_status``) also fails,
        When: the tool runs,
        Then: the ORIGINAL command-insert exception is what the
            caller sees — the compensation failure is logged but
            does not mask the real root cause.
        """
        repo = AsyncMock()
        repo.insert_execution_plan = AsyncMock(return_value=(1, "plan-pid"))
        repo.insert_trade_command = AsyncMock(side_effect=RuntimeError("broker down"))
        repo.update_execution_plan_status = AsyncMock(
            side_effect=RuntimeError("compensation blew up")
        )
        _allow_wallet(repo)
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
                    "idempotency_key": "idem-dbl-fail",
                },
            )
        assert "broker down" in str(exc.value)

    @pytest.mark.asyncio
    async def test_wallet_out_of_scope_rejects_before_any_write(self) -> None:
        """Wallet not in scope → tool rejects before plan + command inserts.

        Given: the repository's ``list_accessible_wallets_for_operators``
            returns a set that does NOT include the target wallet,
        When: ``submit_manual_order`` is dispatched,
        Then: a ``wallet_out_of_scope`` tool error surfaces AND neither
            ``insert_execution_plan`` nor ``insert_trade_command`` is
            invoked — the scope gate fails closed before any write.
            Guards against regressions where a wiring refactor might
            accidentally bypass :func:`validate_user_wallet_scope`.
        """
        repo = AsyncMock()
        repo.insert_execution_plan = AsyncMock()
        repo.insert_trade_command = AsyncMock()
        repo.list_accessible_wallets_for_operators = AsyncMock(
            return_value=[{"public_id": "wallet-other"}]
        )
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
                    "idempotency_key": "idem-scope-fail",
                },
            )
        assert "wallet_out_of_scope" in str(exc.value)
        repo.insert_execution_plan.assert_not_called()
        repo.insert_trade_command.assert_not_called()

    @pytest.mark.asyncio
    async def test_operator_out_of_scope_rejects_before_any_write(self) -> None:
        """Caller-supplied operator outside claims → reject before any write.

        Given: the caller picks an ``operator_public_id`` that is not
            in their authenticated ``operator_public_ids``,
        When: ``submit_manual_order`` is dispatched,
        Then: a ``operator_out_of_scope`` tool error surfaces AND no
            DB calls fire (not even the scope-gate wallet lookup) —
            the operator gate short-circuits first. Closes the R1
            cross-operator attribution bypass finding.
        """
        repo = AsyncMock()
        repo.insert_execution_plan = AsyncMock()
        repo.insert_trade_command = AsyncMock()
        repo.list_accessible_wallets_for_operators = AsyncMock()
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
                    "idempotency_key": "idem-op-fail",
                    "operator_public_id": "op-NOT-MINE",
                },
            )
        assert "operator_out_of_scope" in str(exc.value)
        repo.list_accessible_wallets_for_operators.assert_not_called()
        repo.insert_execution_plan.assert_not_called()
        repo.insert_trade_command.assert_not_called()
