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

import json
from contextlib import asynccontextmanager
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from typing import Any
from typing import cast
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import pytest
from fastapi import HTTPException
from mcp.server.fastmcp import FastMCP
from mcp.server.fastmcp.exceptions import ToolError
from sqlalchemy.exc import IntegrityError

from snapper.application.engine.service import compute_shard_key
from snapper.application.plans import cancel_service
from snapper.application.trade.caps_enforcer import CapsViolationError
from snapper.application.trade.caps_enforcer import Guard
from snapper.application.trade.caps_enforcer import TradingCapsEnforcer
from snapper.application.trade.submission import TradeCommandSubmission
from snapper.auth.domain.permissions import ROLE_PERMISSIONS
from snapper.auth.domain.roles import UserRole
from snapper.auth.schemas.tokens import TokenClaims
from snapper.core.ids import is_uuid7
from snapper.core.types import ExecutionMode
from snapper.core.types import OrderExchange
from snapper.data.models import Symbol
from snapper.data.models import Wallet
from snapper.data.models import WalletOperatorScopeGrant
from snapper.data.repository import SQLAlchemyRepository
from snapper.mcp.server import TOKEN_CLAIMS_CTX
from snapper.mcp.server import get_current_claims
from snapper.mcp.tools import _map_cancel_exception_to_envelope
from snapper.mcp.tools import _parse_iso8601_utc
from snapper.mcp.tools import register_mcp_tools


def _make_claims(
    role: UserRole = UserRole.AI_DELEGATE,
    user_public_id: str = "user-1",
    username: str = "delegate-1",
    operator_public_ids: list[str] | None = None,
) -> TokenClaims:
    """Build a :class:`TokenClaims` for tool-permission tests."""
    now = int(datetime.now(UTC).timestamp())
    resolved_operator_public_ids = (
        operator_public_ids if operator_public_ids is not None else ["op-1"]
    )
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
        operator_public_ids=resolved_operator_public_ids,
        primary_operator_public_id=(
            resolved_operator_public_ids[0] if resolved_operator_public_ids else ""
        ),
    )


def _wallet_row(public_id: str, *, is_paper: bool = False) -> dict[str, object]:
    """Build the wallet row shape consumed by the shared resolver."""
    return {"public_id": public_id, "is_paper": is_paper}


def _allow_wallet(repo: Any, wallet_public_id: str = "wallet-1") -> None:
    """Configure a mock repo so ``validate_user_wallet_scope`` admits the wallet.

    The helper sets ``list_accessible_wallets_for_operators`` on the
    AsyncMock to return the single row every ``submit_manual_order``
    test in this module uses. Call before dispatching the tool.
    """
    repo.list_accessible_wallets_for_operators = AsyncMock(
        return_value=[_wallet_row(wallet_public_id)]
    )
    repo.get_instrument_public_id_by_symbol = AsyncMock(return_value="inst-1")


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


class TestCancelEnvelopeMapping:
    """Coverage for the cancel-service exception-to-envelope mapper."""

    def test_reraises_unknown_exception(self) -> None:
        """Unknown exceptions are re-raised so real bugs do not get masked."""
        exc = RuntimeError("boom")
        with pytest.raises(RuntimeError, match="boom"):
            _map_cancel_exception_to_envelope(exc, "plan-1")


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


def _stub_mcp_guard() -> Guard:
    """Build the Guard payload an admitting MCP enforcer stub yields.

    Returns:
        A :class:`Guard` with a placeholder submission and a NULL
        admission notional, matching what the tool body reads.
    """
    return Guard(
        submission=TradeCommandSubmission(
            user_public_id="user-1",
            operator_public_id=None,
            wallet_public_id="wallet-1",
            instrument_public_id=None,
            command_type="create",
            side="buy",
            order_type="market",
            quantity=None,
            price=None,
            source_surface="mcp",
            idempotency_key=None,
        ),
        assigned_public_id="guard-pid",
        submitted_notional_usd=77.25,
    )


class TestSubmitManualOrderTool:
    """Coverage for the ``submit_manual_order`` MCP tool."""

    def _make_enforcer_admit(self) -> MagicMock:
        """Build an enforcer whose ``guard`` is a pass-through async ctx."""

        @asynccontextmanager
        async def _admit(submission: Any) -> Any:
            yield _stub_mcp_guard()

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
        expected_shard_key = compute_shard_key(
            instrument="BTC-USD",
            exchange=cast(OrderExchange, "kraken"),
            mode=cast(ExecutionMode, "live"),
            wallet_public_id="wallet-1",
            strategy_tag=None,
        )
        plan_row = repo.insert_execution_plan.await_args.args[0]
        cmd_row = repo.insert_trade_command.await_args.args[0]
        assert expected_shard_key != "kraken.BTC-USD.live"
        assert plan_row["mode"] == "live"
        assert cmd_row["mode"] == "live"
        assert plan_row["shard_key"] == expected_shard_key
        assert cmd_row["shard_key"] == expected_shard_key
        assert cmd_row["source_surface"] == "mcp"
        assert cmd_row["submitted_notional_usd"] == 77.25
        assert cmd_row["ai_review_public_id"] is None

    @pytest.mark.asyncio
    async def test_provenance_rows_carry_uuid7_session_id(self) -> None:
        """Plan and command rows stamp a canonical UUID7 session_id.

        Regression for the 2026-07-10 prod incident: the MCP path used
        to write the literal sequence-stream NAME ('rest.mcp') into the
        UUID-typed ``session_id`` columns — Postgres rejected the
        insert with an asyncpg DataError while SQLite-backed tests were
        blind to the type violation. This pin validates UUID7-ness
        directly so no dialect can mask it again.

        Given: a valid manual-order call against mocked persistence,
        When: ``submit_manual_order`` is dispatched,
        Then: BOTH the execution-plan row and the trade-command row
            carry a canonical UUID7 ``session_id`` (identical across
            the two rows — one tracker session) and their sequence_ids
            advance monotonically within the manual-order stream.
        """
        repo = AsyncMock()
        repo.insert_execution_plan = AsyncMock(return_value=(1, "plan-pid"))
        repo.insert_trade_command = AsyncMock(return_value=(2, "cmd-pid"))
        _allow_wallet(repo)
        server = _build_server(
            repository=repo,
            caps_enforcer=self._make_enforcer_admit(),
        )
        await server._tool_manager.call_tool(
            "submit_manual_order",
            {
                "exchange": "kraken",
                "instrument": "BTC-USD",
                "instrument_public_id": "inst-1",
                "side": "buy",
                "order_type": "limit",
                "quantity": 0.5,
                "wallet_public_id": "wallet-1",
                "idempotency_key": "idem-uuid7-pin",
                "price": 50000.0,
            },
        )
        plan_row = repo.insert_execution_plan.await_args.args[0]
        cmd_row = repo.insert_trade_command.await_args.args[0]
        assert is_uuid7(plan_row["session_id"])
        assert is_uuid7(cmd_row["session_id"])
        assert plan_row["session_id"] == cmd_row["session_id"]
        assert cmd_row["sequence_id"] == plan_row["sequence_id"] + 1

    @pytest.mark.asyncio
    async def test_spoofed_instrument_public_id_is_rejected(self) -> None:
        """A caller-cited PID mismatching the symbol/exchange rejects.

        Given: a BTC-USD/kraken order citing an UNRELATED instrument's
            public id (a delegate trying to price caps against a
            cheaper identity),
        When: ``submit_manual_order`` runs,
        Then: the tool rejects with ``instrument_identity_mismatch``
            and neither the plan nor the command inserts.
        """
        repo = AsyncMock()
        repo.insert_execution_plan = AsyncMock()
        repo.insert_trade_command = AsyncMock()
        _allow_wallet(repo)
        repo.get_instrument_public_id_by_symbol = AsyncMock(return_value="inst-real-btc")
        server = _build_server(
            repository=repo,
            caps_enforcer=self._make_enforcer_admit(),
        )
        result = await server._tool_manager.call_tool(
            "submit_manual_order",
            {
                "exchange": "kraken",
                "instrument": "BTC-USD",
                "instrument_public_id": "inst-cheap-shitcoin",
                "side": "buy",
                "order_type": "market",
                "quantity": 1.0,
                "wallet_public_id": "wallet-1",
                "idempotency_key": "idem-spoof",
            },
        )
        envelope = _decode_envelope(result)
        assert envelope["success"] is False
        assert envelope["error_code"] == "instrument_identity_mismatch"
        assert envelope["details"]["resolved_instrument_public_id"] == "inst-real-btc"
        repo.insert_execution_plan.assert_not_awaited()
        repo.insert_trade_command.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_unknown_instrument_identity_is_rejected(self) -> None:
        """A (symbol, exchange) pair with no active instrument rejects.

        Given: a submission whose (instrument, exchange) resolves to no
            active instrument row,
        When: ``submit_manual_order`` runs,
        Then: the tool rejects with ``instrument_identity_mismatch``.
        """
        repo = AsyncMock()
        repo.insert_execution_plan = AsyncMock()
        repo.insert_trade_command = AsyncMock()
        _allow_wallet(repo)
        repo.get_instrument_public_id_by_symbol = AsyncMock(return_value=None)
        server = _build_server(
            repository=repo,
            caps_enforcer=self._make_enforcer_admit(),
        )
        result = await server._tool_manager.call_tool(
            "submit_manual_order",
            {
                "exchange": "kraken",
                "instrument": "GHOST-USD",
                "instrument_public_id": "inst-ghost",
                "side": "buy",
                "order_type": "market",
                "quantity": 1.0,
                "wallet_public_id": "wallet-1",
                "idempotency_key": "idem-ghost",
            },
        )
        envelope = _decode_envelope(result)
        assert envelope["success"] is False
        assert envelope["error_code"] == "instrument_identity_mismatch"
        assert envelope["details"]["resolved_instrument_public_id"] is None
        repo.insert_trade_command.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_omitted_wallet_resolves_single_live_wallet(self) -> None:
        """Omitted wallet resolves to the caller's single accessible live wallet."""
        repo = AsyncMock()
        repo.insert_execution_plan = AsyncMock(return_value=(1, "plan-pid"))
        repo.insert_trade_command = AsyncMock(return_value=(2, "cmd-pid"))
        repo.list_accessible_wallets_for_operators = AsyncMock(
            return_value=[_wallet_row("wallet-live")]
        )
        repo.get_instrument_public_id_by_symbol = AsyncMock(return_value="inst-1")
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
                "order_type": "market",
                "quantity": 1.0,
                "idempotency_key": "idem-auto-one",
            },
        )
        assert result["plan_public_id"] == "plan-pid"
        plan_row = repo.insert_execution_plan.await_args.args[0]
        cmd_row = repo.insert_trade_command.await_args.args[0]
        assert plan_row["wallet_public_id"] == "wallet-live"
        assert cmd_row["wallet_public_id"] == "wallet-live"
        assert repo.list_accessible_wallets_for_operators.await_count == 2

    @pytest.mark.asyncio
    async def test_omitted_wallet_with_multiple_live_wallets_returns_structured_ambiguity(
        self,
    ) -> None:
        """Multiple live wallet candidates return ``wallet_ambiguous``."""
        repo = AsyncMock()
        repo.insert_execution_plan = AsyncMock()
        repo.insert_trade_command = AsyncMock()
        repo.list_accessible_wallets_for_operators = AsyncMock(
            return_value=[_wallet_row("wallet-a"), _wallet_row("wallet-b")]
        )
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
                "order_type": "market",
                "quantity": 1.0,
                "idempotency_key": "idem-auto-many",
            },
        )
        envelope = _decode_envelope(result)
        assert envelope["success"] is False
        assert envelope["error_code"] == "wallet_ambiguous"
        assert envelope["details"]["candidate_wallet_public_ids"] == [
            "wallet-a",
            "wallet-b",
        ]
        repo.insert_execution_plan.assert_not_awaited()
        repo.insert_trade_command.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_omitted_wallet_with_no_live_wallets_returns_structured_unresolved(
        self,
    ) -> None:
        """Zero wallet candidates return ``wallet_unresolved``."""
        repo = AsyncMock()
        repo.insert_execution_plan = AsyncMock()
        repo.insert_trade_command = AsyncMock()
        repo.list_accessible_wallets_for_operators = AsyncMock(return_value=[])
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
                "order_type": "market",
                "quantity": 1.0,
                "idempotency_key": "idem-auto-zero",
            },
        )
        envelope = _decode_envelope(result)
        assert envelope["success"] is False
        assert envelope["error_code"] == "wallet_unresolved"
        assert envelope["details"]["candidate_wallet_public_ids"] == []
        repo.insert_execution_plan.assert_not_awaited()
        repo.insert_trade_command.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_omitted_live_wallet_with_only_paper_wallet_returns_unresolved(
        self,
    ) -> None:
        """A live MCP order never binds the caller's single paper wallet."""
        repo = AsyncMock()
        repo.insert_execution_plan = AsyncMock()
        repo.insert_trade_command = AsyncMock()
        repo.list_accessible_wallets_for_operators = AsyncMock(
            return_value=[_wallet_row("wallet-paper", is_paper=True)]
        )
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
                "order_type": "market",
                "quantity": 1.0,
                "idempotency_key": "idem-auto-paper",
            },
        )
        envelope = _decode_envelope(result)
        assert envelope["success"] is False
        assert envelope["error_code"] == "wallet_unresolved"
        assert envelope["details"]["candidate_wallet_public_ids"] == []
        repo.insert_execution_plan.assert_not_awaited()
        repo.insert_trade_command.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_omitted_live_wallet_without_operator_context_returns_unresolved(
        self,
    ) -> None:
        """Live MCP autolookup fails closed without operator claims."""
        repo = AsyncMock()
        repo.insert_execution_plan = AsyncMock()
        repo.insert_trade_command = AsyncMock()
        repo.list_accessible_wallets_for_operators = AsyncMock()
        claims = _make_claims(operator_public_ids=[])
        server = _build_server(
            repository=repo,
            caps_enforcer=self._make_enforcer_admit(),
            claims=claims,
        )
        result = await server._tool_manager.call_tool(
            "submit_manual_order",
            {
                "exchange": "kraken",
                "instrument": "BTC-USD",
                "instrument_public_id": "inst-1",
                "side": "buy",
                "order_type": "market",
                "quantity": 1.0,
                "idempotency_key": "idem-auto-no-operator",
            },
        )
        envelope = _decode_envelope(result)
        assert envelope["success"] is False
        assert envelope["error_code"] == "wallet_unresolved"
        repo.list_accessible_wallets_for_operators.assert_not_called()
        repo.insert_execution_plan.assert_not_awaited()
        repo.insert_trade_command.assert_not_awaited()

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("wallet_public_id", "wallets"),
        [
            ("", [_wallet_row("wallet-live")]),
            ("   ", [_wallet_row("wallet-a"), _wallet_row("wallet-b")]),
        ],
    )
    async def test_blank_wallet_returns_invalid_argument_without_autolookup(
        self,
        wallet_public_id: str,
        wallets: list[dict[str, object]],
    ) -> None:
        """Blank MCP wallet IDs are invalid explicit values.

        Given: a blank explicit wallet ID and one or many accessible wallets,
        When: ``submit_manual_order`` prepares the manual order,
        Then: it returns a structured invalid-argument envelope without
            resolving or writing against any wallet.
        """
        repo = AsyncMock()
        repo.insert_execution_plan = AsyncMock()
        repo.insert_trade_command = AsyncMock()
        repo.list_accessible_wallets_for_operators = AsyncMock(return_value=wallets)
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
                "order_type": "market",
                "quantity": 1.0,
                "wallet_public_id": wallet_public_id,
                "idempotency_key": "idem-blank-wallet",
            },
        )
        envelope = _decode_envelope(result)
        assert envelope["success"] is False
        assert envelope["error_code"] == "invalid_argument"
        assert envelope["message"] == "wallet_public_id must not be blank."
        assert envelope["details"]["field"] == "wallet_public_id"
        repo.list_accessible_wallets_for_operators.assert_not_called()
        repo.insert_execution_plan.assert_not_awaited()
        repo.insert_trade_command.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_explicit_wallet_with_multiple_accessible_wallets_is_unchanged(
        self,
    ) -> None:
        """Explicit wallet bypasses autolookup and keeps scope validation."""
        repo = AsyncMock()
        repo.insert_execution_plan = AsyncMock(return_value=(1, "plan-pid"))
        repo.insert_trade_command = AsyncMock(return_value=(2, "cmd-pid"))
        repo.list_accessible_wallets_for_operators = AsyncMock(
            return_value=[_wallet_row("wallet-1"), _wallet_row("wallet-2")]
        )
        repo.get_instrument_public_id_by_symbol = AsyncMock(return_value="inst-1")
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
                "order_type": "market",
                "quantity": 1.0,
                "wallet_public_id": "wallet-1",
                "idempotency_key": "idem-explicit-many",
            },
        )
        assert result["command_public_id"] == "cmd-pid"
        plan_row = repo.insert_execution_plan.await_args.args[0]
        cmd_row = repo.insert_trade_command.await_args.args[0]
        assert plan_row["wallet_public_id"] == "wallet-1"
        assert cmd_row["wallet_public_id"] == "wallet-1"
        assert repo.list_accessible_wallets_for_operators.await_count == 1

    @pytest.mark.asyncio
    async def test_stop_order_without_trigger_is_rejected_before_any_write(self) -> None:
        """A stop order missing stop_price fails validation pre-persist (#156).

        Given: a stop submit with no stop_price,
        When: ``submit_manual_order`` runs,
        Then: the manual-order evaluator rule rejects it (same rule as
            REST 422) and neither the plan nor the command is written —
            MCP can no longer produce half-formed stop commands.
        """
        repo = AsyncMock()
        repo.insert_execution_plan = AsyncMock()
        repo.insert_trade_command = AsyncMock()
        _allow_wallet(repo)
        server = _build_server(
            repository=repo,
            caps_enforcer=self._make_enforcer_admit(),
        )
        with pytest.raises(ToolError, match="requires stop_price"):
            await server._tool_manager.call_tool(
                "submit_manual_order",
                {
                    "exchange": "kraken",
                    "instrument": "BTC-USD",
                    "instrument_public_id": "inst-1",
                    "side": "sell",
                    "order_type": "stop",
                    "quantity": 0.5,
                    "wallet_public_id": "wallet-1",
                    "idempotency_key": "idem-stop-1",
                },
            )
        repo.insert_execution_plan.assert_not_awaited()
        repo.insert_trade_command.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_stop_order_with_trigger_persists_core_type_and_stop_price(self) -> None:
        """A valid stop_limit submit persists the trigger durably (#156).

        Given: a stop_limit submit with price and stop_price,
        When: ``submit_manual_order`` runs,
        Then: the command row stores CORE order_type with stop_price,
            and the plan params carry stop_price WITHOUT the legacy
            venue_order_type key.
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
                "side": "sell",
                "order_type": "stop_limit",
                "quantity": 0.5,
                "wallet_public_id": "wallet-1",
                "idempotency_key": "idem-stop-2",
                "price": 47900.0,
                "stop_price": 48000.0,
            },
        )
        assert result["command_public_id"] == "cmd-pid"
        cmd_row = repo.insert_trade_command.await_args.args[0]
        assert cmd_row["order_type"] == "stop_limit"
        assert cmd_row["price"] == 47900.0
        assert cmd_row["stop_price"] == 48000.0
        plan_row = repo.insert_execution_plan.await_args.args[0]
        assert plan_row["params"]["stop_price"] == 48000.0
        assert "venue_order_type" not in plan_row["params"]

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
        plan_row = repo.insert_execution_plan.await_args.args[0]
        cmd_row = repo.insert_trade_command.await_args.args[0]
        assert is_uuid7(compensation_kwargs["session_id"])
        assert compensation_kwargs["session_id"] == plan_row["session_id"]
        assert compensation_kwargs["sequence_id"] == cmd_row["sequence_id"] + 1

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
            the operator gate short-circuits first. Closes the
            cross-operator attribution bypass.
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

    @pytest.mark.asyncio
    async def test_ai_review_citation_threads_through_to_caps_enforcer_guard(self) -> None:
        """Valid ai_review_public_id citation lands on the caps-guard submission.

        ``ai_review_public_id`` is a runtime-only signal on
        :class:`TradeCommandSubmission` consumed by
        :meth:`TradingCapsEnforcer.guard` to fire
        ``bus.caps_violation_after_ai_approve`` on cap reject; it is
        intentionally NOT persisted on the trade_command row. The test
        captures the submission via the enforcer to verify the field
        threaded all the way through ``_prepare_manual_order``.

        Given: a caller submits a manual order citing an
            ``ai_review_public_id`` whose row exists, is owned by the
            caller, matches the submission's wallet, and is in
            ``status='resolved_approved'``,
        When: ``submit_manual_order`` runs,
        Then: the citation passes validation AND the
            :class:`TradeCommandSubmission` handed to
            ``enforcer.guard`` carries ``ai_review_public_id``.
        """
        review_pid = "review-ok-1"
        captured: dict[str, Any] = {}

        class _Ctx:
            async def __aenter__(self) -> Guard:
                return _stub_mcp_guard()

            async def __aexit__(self, *_args: Any) -> None:
                return None

        def _guard_capturing(submission: Any) -> _Ctx:
            captured["submission"] = submission
            return _Ctx()

        enforcer = MagicMock()
        enforcer.guard = MagicMock(side_effect=_guard_capturing)
        repo = AsyncMock()
        repo.insert_execution_plan = AsyncMock(return_value=(1, "plan-pid"))
        repo.insert_trade_command = AsyncMock(return_value=(2, "cmd-pid"))
        repo.get_ai_review = AsyncMock(
            return_value={
                "public_id": review_pid,
                "user_public_id": "user-1",
                "wallet_public_id": "wallet-1",
                "status": "resolved_approved",
            }
        )
        _allow_wallet(repo)
        server = _build_server(repository=repo, caps_enforcer=enforcer)
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
                "idempotency_key": "idem-cit-ok",
                "ai_review_public_id": review_pid,
            },
        )
        repo.get_ai_review.assert_awaited_once_with(review_pid)
        assert captured["submission"].ai_review_public_id == review_pid

    @pytest.mark.asyncio
    async def test_ai_review_citation_owner_mismatch_rejects_before_any_write(self) -> None:
        """Citing another user's review is rejected before any write.

        Given: the caller cites an ``ai_review_public_id`` whose owner
            is a different user (the row's ``user_public_id`` does
            not match the caller's),
        When: ``submit_manual_order`` runs,
        Then: an :class:`AiReviewCitationError` surfaces (no plan
            insert, no command insert) so the caller cannot trigger
            ``bus.caps_violation_after_ai_approve`` fanout to an
            unrelated delegate's UI.
        """
        repo = AsyncMock()
        repo.insert_execution_plan = AsyncMock()
        repo.insert_trade_command = AsyncMock()
        repo.get_ai_review = AsyncMock(
            return_value={
                "public_id": "review-stranger",
                "user_public_id": "u-OTHER",
                "wallet_public_id": "wallet-1",
                "status": "resolved_approved",
            }
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
                    "idempotency_key": "idem-cit-stranger",
                    "ai_review_public_id": "review-stranger",
                },
            )
        assert "owner mismatch" in str(exc.value)
        repo.insert_execution_plan.assert_not_called()
        repo.insert_trade_command.assert_not_called()


class TestListOrdersTool:
    """Coverage for the ``list_orders`` MCP tool."""

    @staticmethod
    def _build_repo_with_orders(
        rows: list[dict[str, Any]],
        total: int,
        accessible_wallets: list[str] | None = None,
    ) -> Any:
        repo = AsyncMock()
        repo.get_orders = AsyncMock(return_value=rows)
        repo.get_orders_total_count = AsyncMock(return_value=total)
        if accessible_wallets is None:
            accessible_wallets = ["wallet-1"]
        repo.list_accessible_wallets_for_operators = AsyncMock(
            return_value=[{"public_id": w} for w in accessible_wallets]
        )
        return repo

    @pytest.mark.asyncio
    async def test_admit_returns_envelope_with_total_and_orders(self) -> None:
        """Happy path: AI_DELEGATE caller gets order list + total_count."""
        order_row = _ORDER_ROW_FIXTURE.copy()
        repo = self._build_repo_with_orders([order_row], total=42)
        server = _build_server(repository=repo)
        result = await server._tool_manager.call_tool("list_orders", {"limit": 10, "offset": 0})
        envelope = _decode_envelope(result)
        assert envelope["success"] is True
        assert envelope["details"]["total_count"] == 42
        assert envelope["details"]["orders"][0]["public_id"] == order_row["public_id"]

    @pytest.mark.asyncio
    async def test_invalid_status_returns_invalid_argument(self) -> None:
        """Invalid status enum → invalid_argument envelope, no repo call."""
        repo = self._build_repo_with_orders([], total=0)
        server = _build_server(repository=repo)
        result = await server._tool_manager.call_tool("list_orders", {"status": "bogus"})
        envelope = _decode_envelope(result)
        assert envelope["success"] is False
        assert envelope["error_code"] == "invalid_argument"
        repo.get_orders.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_negative_limit_returns_invalid_argument(self) -> None:
        """Negative limit/offset → invalid_argument."""
        repo = self._build_repo_with_orders([], total=0)
        server = _build_server(repository=repo)
        result = await server._tool_manager.call_tool("list_orders", {"limit": -1, "offset": 0})
        envelope = _decode_envelope(result)
        assert envelope["error_code"] == "invalid_argument"

    @pytest.mark.asyncio
    async def test_wallet_outside_scope_returns_anti_enumeration(self) -> None:
        """Inaccessible wallet → ``order_not_found`` (anti-enumeration)."""
        repo = self._build_repo_with_orders([], total=0, accessible_wallets=["wallet-1"])
        server = _build_server(repository=repo)
        result = await server._tool_manager.call_tool(
            "list_orders", {"wallet_public_id": "wallet-99"}
        )
        envelope = _decode_envelope(result)
        assert envelope["error_code"] == "order_not_found"
        repo.get_orders.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_role_without_read_orders_returns_permission_denied_envelope(self) -> None:
        """Permission failure surfaces as structured envelope.

        Given: a viewer-shaped role with READ_ORDERS removed from its
            permission set,
        When: the tool dispatches,
        Then: the envelope flag is ``permission_denied`` (NOT a raw
            FastMCP ``ToolError`` from a bare PermissionError).
        """
        repo = self._build_repo_with_orders([], total=0)
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
            result = await server._tool_manager.call_tool("list_orders", {})
            envelope = _decode_envelope(result)
            assert envelope["success"] is False
            assert envelope["error_code"] == "permission_denied"
        finally:
            if saved is not None:
                ROLE_PERMISSIONS[UserRole.VIEWER] = saved

    @pytest.mark.asyncio
    async def test_pre_lifespan_repository_returns_service_unavailable(self) -> None:
        """Pre-lifespan repository surfaces structured envelope."""
        server = _build_server(repository=None)
        result = await server._tool_manager.call_tool("list_orders", {})
        envelope = _decode_envelope(result)
        assert envelope["error_code"] == "service_unavailable"

    @pytest.mark.asyncio
    async def test_limit_clamps_to_cap(self) -> None:
        """Caller passing limit=10000 is clamped to the 200 ceiling."""
        repo = self._build_repo_with_orders([], total=0)
        server = _build_server(repository=repo)
        await server._tool_manager.call_tool("list_orders", {"limit": 10000})
        kwargs = repo.get_orders.await_args.kwargs
        assert kwargs["limit"] == 200

    @pytest.mark.asyncio
    async def test_admin_with_no_wallet_passes_none_filter(self) -> None:
        """ADMIN with no explicit wallet → repo gets ``wallet_public_ids=None``."""
        repo = AsyncMock()
        repo.get_orders = AsyncMock(return_value=[])
        repo.get_orders_total_count = AsyncMock(return_value=0)
        admin_claims = _make_claims(role=UserRole.ADMIN, user_public_id="admin-1")
        server = _build_server(repository=repo, claims=admin_claims)
        await server._tool_manager.call_tool("list_orders", {})
        kwargs = repo.get_orders.await_args.kwargs
        assert kwargs["wallet_public_ids"] is None
        repo.list_accessible_wallets_for_operators.assert_not_called()

    @pytest.mark.asyncio
    async def test_admin_with_explicit_wallet_passes_single_id(self) -> None:
        """ADMIN with explicit ``wallet_public_id`` skips the accessible lookup."""
        repo = AsyncMock()
        repo.get_orders = AsyncMock(return_value=[])
        repo.get_orders_total_count = AsyncMock(return_value=0)
        admin_claims = _make_claims(role=UserRole.ADMIN, user_public_id="admin-1")
        server = _build_server(repository=repo, claims=admin_claims)
        await server._tool_manager.call_tool(
            "list_orders", {"wallet_public_id": "wallet-admin-target"}
        )
        kwargs = repo.get_orders.await_args.kwargs
        assert kwargs["wallet_public_ids"] == ["wallet-admin-target"]
        repo.list_accessible_wallets_for_operators.assert_not_called()

    @pytest.mark.asyncio
    async def test_non_admin_with_in_scope_wallet_passes_single_id(self) -> None:
        """Non-admin caller with a wallet inside scope → repo gets ``[that wallet]``."""
        repo = self._build_repo_with_orders([], total=0, accessible_wallets=["wallet-1"])
        server = _build_server(repository=repo)
        await server._tool_manager.call_tool("list_orders", {"wallet_public_id": "wallet-1"})
        kwargs = repo.get_orders.await_args.kwargs
        assert kwargs["wallet_public_ids"] == ["wallet-1"]


class TestGetOrderStatusTool:
    """Coverage for the ``get_order_status`` MCP tool."""

    @pytest.mark.asyncio
    async def test_returns_full_envelope_with_executions(self) -> None:
        """Happy path: order found → envelope carries order + execution_history."""
        repo = AsyncMock()
        repo.list_accessible_wallets_for_operators = AsyncMock(
            return_value=[{"public_id": "wallet-1"}]
        )
        repo.get_trade_command_by_public_id = AsyncMock(
            return_value={
                "public_id": "cmd-1",
                "plan_public_id": "plan-1",
                "wallet_public_id": "wallet-1",
            }
        )
        order_row = _ORDER_ROW_FIXTURE.copy()
        repo.get_order_by_command_public_id = AsyncMock(return_value=order_row)
        repo.get_executions_for_order = AsyncMock(return_value=[_EXEC_ROW_FIXTURE.copy()])
        server = _build_server(repository=repo)
        result = await server._tool_manager.call_tool(
            "get_order_status", {"command_public_id": "cmd-1"}
        )
        envelope = _decode_envelope(result)
        assert envelope["success"] is True
        assert envelope["details"]["order"]["public_id"] == order_row["public_id"]
        assert len(envelope["details"]["execution_history"]) == 1

    @pytest.mark.asyncio
    async def test_unknown_command_returns_order_not_found(self) -> None:
        """Unknown command → ``order_not_found`` envelope (anti-enumeration)."""
        repo = AsyncMock()
        repo.list_accessible_wallets_for_operators = AsyncMock(
            return_value=[{"public_id": "wallet-1"}]
        )
        repo.get_trade_command_by_public_id = AsyncMock(return_value=None)
        server = _build_server(repository=repo)
        result = await server._tool_manager.call_tool(
            "get_order_status", {"command_public_id": "cmd-missing"}
        )
        envelope = _decode_envelope(result)
        assert envelope["error_code"] == "order_not_found"

    @pytest.mark.asyncio
    async def test_command_in_other_wallet_returns_order_not_found(self) -> None:
        """Command exists but its wallet is outside caller's scope."""
        repo = AsyncMock()
        repo.list_accessible_wallets_for_operators = AsyncMock(
            return_value=[{"public_id": "wallet-1"}]
        )
        repo.get_trade_command_by_public_id = AsyncMock(
            return_value={
                "public_id": "cmd-x",
                "plan_public_id": "plan-x",
                "wallet_public_id": "wallet-other",
            }
        )
        repo.get_order_by_command_public_id = AsyncMock(return_value=None)
        server = _build_server(repository=repo)
        result = await server._tool_manager.call_tool(
            "get_order_status", {"command_public_id": "cmd-x"}
        )
        envelope = _decode_envelope(result)
        assert envelope["error_code"] == "order_not_found"
        repo.get_order_by_command_public_id.assert_not_called()

    @pytest.mark.asyncio
    async def test_role_without_read_orders_returns_permission_denied_envelope(self) -> None:
        """get_order_status permission failure also envelope-wrapped."""
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
            result = await server._tool_manager.call_tool(
                "get_order_status", {"command_public_id": "cmd-x"}
            )
            envelope = _decode_envelope(result)
            assert envelope["error_code"] == "permission_denied"
        finally:
            if saved is not None:
                ROLE_PERMISSIONS[UserRole.VIEWER] = saved

    @pytest.mark.asyncio
    async def test_pre_lifespan_repository_returns_service_unavailable(self) -> None:
        """get_order_status pre-lifespan also envelope-wrapped."""
        server = _build_server(repository=None)
        result = await server._tool_manager.call_tool(
            "get_order_status", {"command_public_id": "cmd-x"}
        )
        envelope = _decode_envelope(result)
        assert envelope["error_code"] == "service_unavailable"

    @pytest.mark.asyncio
    async def test_pending_dispatch_returns_synthetic_envelope(self) -> None:
        """Command exists but order not ACK'd → ``status='pending_dispatch'``."""
        repo = AsyncMock()
        repo.list_accessible_wallets_for_operators = AsyncMock(
            return_value=[{"public_id": "wallet-1"}]
        )
        repo.get_trade_command_by_public_id = AsyncMock(
            return_value={
                "public_id": "cmd-pending",
                "plan_public_id": "plan-pending",
                "wallet_public_id": "wallet-1",
            }
        )
        repo.get_order_by_command_public_id = AsyncMock(return_value=None)
        server = _build_server(repository=repo)
        result = await server._tool_manager.call_tool(
            "get_order_status", {"command_public_id": "cmd-pending"}
        )
        envelope = _decode_envelope(result)
        assert envelope["success"] is True
        assert envelope["details"]["status"] == "pending_dispatch"
        assert envelope["details"]["plan_public_id"] == "plan-pending"
        assert envelope["details"]["execution_history"] == []
        repo.get_executions_for_order.assert_not_called()


def _decode_envelope(result: Any) -> dict[str, Any]:
    """Pull the JSON envelope out of a ``CallToolResult``."""
    if isinstance(result, tuple):
        result = result[1]
    text_blocks = [c.text for c in result.content if hasattr(c, "text")]
    payload = "\n".join(text_blocks)
    decoded: dict[str, Any] = json.loads(payload)
    return decoded


_ORDER_ROW_FIXTURE: dict[str, Any] = {
    "public_id": "order-1",
    "timestamp": datetime.now(UTC),
    "session_id": "s",
    "sequence_id": 1,
    "instrument": "BTC-USD",
    "exchange": "kraken",
    "mode": "live",
    "client_order_id": "cid",
    "exchange_order_id": "ex",
    "created_at": datetime.now(UTC),
    "updated_at": None,
    "side": "buy",
    "order_type": "limit",
    "price": 50000.0,
    "size": 1.0,
    "filled_size": 0.0,
    "average_price": None,
    "status": "open",
    "time_in_force": None,
    "error": None,
    "leverage": None,
    "reduce_only": False,
    "wallet_public_id": "wallet-1",
    "operator_public_id": "op-1",
    "plan_public_id": "plan-1",
}


_EXEC_ROW_FIXTURE: dict[str, Any] = {
    "public_id": "exe-1",
    "timestamp": datetime.now(UTC),
    "session_id": "s",
    "sequence_id": 2,
    "trade_id": "trade-1",
    "exchange_order_id": "ex",
    "client_order_id": "cid",
    "instrument": "BTC-USD",
    "exchange": "kraken",
    "side": "buy",
    "size": 0.5,
    "price": 50000.0,
    "fee": 0.0,
    "fee_asset": "USD",
    "status": "ok",
    "executed_at": datetime.now(UTC),
    "wallet_public_id": "wallet-1",
    "operator_public_id": "op-1",
    "liquidity_role": "taker",
}


_POSITION_ROW_FIXTURE: dict[str, Any] = {
    "public_id": "pos-1",
    "timestamp": datetime.now(UTC),
    "session_id": "s",
    "sequence_id": 3,
    "instrument": "BTC-USD",
    "instrument_public_id": "inst-1",
    "exchange": "kraken",
    "mode": "live",
    "quantity": 1.5,
    "average_price": 49500.0,
    "unrealized_pnl": 750.0,
    "realized_pnl": 100.0,
    "mark_price": 50000.0,
    "marked_at": datetime.now(UTC),
    "source_venue_event_id": 42,
    "position_cycle_public_id": "cycle-1",
    "wallet_public_id": "wallet-1",
}


def _venue_account_state_row(
    *,
    public_id: str = "acct-1",
    exchange: str = "kraken",
    wallet_public_id: str = "wallet-1",
) -> dict[str, Any]:
    """Build a fresh, authoritative ``VenueAccountStateRow`` fixture.

    Observation timestamps sit in the past and ``authoritative_until`` in
    the future so :func:`build_portfolio_account_state` derives
    ``effective_status="observed"`` (``is_authoritative=True``).
    """
    now = datetime.now(UTC)
    return {
        "wallet_public_id": wallet_public_id,
        "exchange": exchange,
        "mode": "live",
        "sync_status": "observed",
        "balance_status": "observed",
        "position_status": "observed",
        "valuation_status": "native_only",
        "balances_json": json.dumps(
            [{"currency": "USD", "total": 1000.0, "free": 900.0, "used": 100.0}]
        ),
        "open_positions_json": json.dumps([]),
        "balance_observed_at": now - timedelta(minutes=1),
        "position_observed_at": now - timedelta(minutes=1),
        "current_attempt_observation_id": 1,
        "balance_payload_source_observation_id": 1,
        "position_payload_source_observation_id": 1,
        "authoritative_until": now + timedelta(hours=1),
        "error": None,
        "public_id": public_id,
        "timestamp": now,
        "session_id": "s",
        "sequence_id": 5,
    }


_POSITION_CYCLE_ROW_FIXTURE: dict[str, Any] = {
    "public_id": "cycle-1",
    "timestamp": datetime.now(UTC),
    "session_id": "s",
    "sequence_id": 4,
    "instrument_public_id": "inst-1",
    "exchange": "kraken",
    "mode": "live",
    "shard_key": "kraken.BTC-USD.live",
    "wallet_public_id": "wallet-1",
    "operator_public_id": "op-1",
    "direction": "long",
    "max_qty": 2.0,
    "status": "open",
    "opened_at": datetime.now(UTC),
    "closed_at": None,
    "opening_command_public_id": "cmd-open",
    "closing_command_public_id": None,
}


class TestListPositionsTool:
    """Coverage for the ``list_positions`` MCP tool."""

    @staticmethod
    def _build_repo_with_positions(
        rows: list[dict[str, Any]],
        accessible_wallets: list[str] | None = None,
    ) -> Any:
        repo = AsyncMock()
        repo.get_positions = AsyncMock(return_value=rows)
        if accessible_wallets is None:
            accessible_wallets = ["wallet-1"]
        repo.list_accessible_wallets_for_operators = AsyncMock(
            return_value=[{"public_id": w} for w in accessible_wallets]
        )
        return repo

    @pytest.mark.asyncio
    async def test_happy_path_returns_positions_envelope(self) -> None:
        """Happy path: AI_DELEGATE caller gets a position list + count."""
        position_row = _POSITION_ROW_FIXTURE.copy()
        repo = self._build_repo_with_positions([position_row])
        server = _build_server(repository=repo)
        result = await server._tool_manager.call_tool("list_positions", {})
        envelope = _decode_envelope(result)
        assert envelope["success"] is True
        assert envelope["details"]["count"] == 1
        assert envelope["details"]["positions"][0]["public_id"] == position_row["public_id"]

    @pytest.mark.asyncio
    async def test_serialised_timestamp_is_iso_string(self) -> None:
        """JSON envelope cannot carry raw datetimes; ISO string."""
        position_row = _POSITION_ROW_FIXTURE.copy()
        repo = self._build_repo_with_positions([position_row])
        server = _build_server(repository=repo)
        result = await server._tool_manager.call_tool("list_positions", {})
        envelope = _decode_envelope(result)
        timestamp = envelope["details"]["positions"][0]["timestamp"]
        assert isinstance(timestamp, str)
        assert "T" in timestamp

    @pytest.mark.asyncio
    async def test_post_fetch_exchange_filter(self) -> None:
        """``exchange`` filter prunes rows post-fetch (repo has no native filter)."""
        kraken_row = _POSITION_ROW_FIXTURE.copy()
        kraken_futures_row = _POSITION_ROW_FIXTURE.copy()
        kraken_futures_row["public_id"] = "pos-2"
        kraken_futures_row["exchange"] = "kraken_futures"
        repo = self._build_repo_with_positions([kraken_row, kraken_futures_row])
        server = _build_server(repository=repo)
        result = await server._tool_manager.call_tool(
            "list_positions", {"exchange": "kraken_futures"}
        )
        envelope = _decode_envelope(result)
        assert envelope["details"]["count"] == 1
        assert envelope["details"]["positions"][0]["public_id"] == "pos-2"

    @pytest.mark.asyncio
    async def test_post_fetch_instrument_filter(self) -> None:
        """``instrument`` filter prunes rows post-fetch."""
        btc_row = _POSITION_ROW_FIXTURE.copy()
        eth_row = _POSITION_ROW_FIXTURE.copy()
        eth_row["public_id"] = "pos-eth"
        eth_row["instrument"] = "ETH-USD"
        repo = self._build_repo_with_positions([btc_row, eth_row])
        server = _build_server(repository=repo)
        result = await server._tool_manager.call_tool("list_positions", {"instrument": "ETH-USD"})
        envelope = _decode_envelope(result)
        assert envelope["details"]["count"] == 1
        assert envelope["details"]["positions"][0]["instrument"] == "ETH-USD"

    @pytest.mark.asyncio
    async def test_wallet_outside_scope_returns_anti_enumeration(self) -> None:
        """Inaccessible wallet → ``position_not_found`` (anti-enumeration)."""
        repo = self._build_repo_with_positions([], accessible_wallets=["wallet-1"])
        server = _build_server(repository=repo)
        result = await server._tool_manager.call_tool(
            "list_positions", {"wallet_public_id": "wallet-99"}
        )
        envelope = _decode_envelope(result)
        assert envelope["success"] is False
        assert envelope["error_code"] == "position_not_found"
        repo.get_positions.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_role_without_read_positions_returns_permission_denied_envelope(self) -> None:
        """Permission failure surfaces as structured envelope."""
        repo = self._build_repo_with_positions([])
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
            result = await server._tool_manager.call_tool("list_positions", {})
            envelope = _decode_envelope(result)
            assert envelope["success"] is False
            assert envelope["error_code"] == "permission_denied"
        finally:
            if saved is not None:
                ROLE_PERMISSIONS[UserRole.VIEWER] = saved

    @pytest.mark.asyncio
    async def test_pre_lifespan_repository_returns_service_unavailable(self) -> None:
        """Pre-lifespan repository surfaces structured envelope."""
        server = _build_server(repository=None)
        result = await server._tool_manager.call_tool("list_positions", {})
        envelope = _decode_envelope(result)
        assert envelope["error_code"] == "service_unavailable"

    @pytest.mark.asyncio
    async def test_admin_with_no_wallet_passes_none_filter(self) -> None:
        """ADMIN with no explicit wallet → repo gets ``wallet_public_ids=None``."""
        repo = AsyncMock()
        repo.get_positions = AsyncMock(return_value=[])
        admin_claims = _make_claims(role=UserRole.ADMIN, user_public_id="admin-1")
        server = _build_server(repository=repo, claims=admin_claims)
        await server._tool_manager.call_tool("list_positions", {})
        kwargs = repo.get_positions.await_args.kwargs
        assert kwargs["wallet_public_ids"] is None
        repo.list_accessible_wallets_for_operators.assert_not_called()

    @pytest.mark.asyncio
    async def test_non_admin_with_in_scope_wallet_passes_single_id(self) -> None:
        """Non-admin caller with a wallet inside scope → repo gets ``[that wallet]``."""
        repo = self._build_repo_with_positions([], accessible_wallets=["wallet-1"])
        server = _build_server(repository=repo)
        await server._tool_manager.call_tool("list_positions", {"wallet_public_id": "wallet-1"})
        kwargs = repo.get_positions.await_args.kwargs
        assert kwargs["wallet_public_ids"] == ["wallet-1"]


class TestListVenueAccountStatesTool:
    """Coverage for the ``list_venue_account_states`` MCP tool (PnL Phase 3)."""

    @staticmethod
    def _build_repo(
        rows: list[dict[str, Any]],
        accessible_wallets: list[str] | None = None,
    ) -> Any:
        """Build an AsyncMock repo wired for the venue-account-state read path."""
        repo = AsyncMock()
        repo.get_venue_account_states = AsyncMock(return_value=rows)
        if accessible_wallets is None:
            accessible_wallets = ["wallet-1"]
        repo.list_accessible_wallets_for_operators = AsyncMock(
            return_value=[{"public_id": w} for w in accessible_wallets]
        )
        return repo

    @pytest.mark.asyncio
    async def test_ai_delegate_without_permission_returns_permission_denied(self) -> None:
        """AI_DELEGATE lacks READ_ACCOUNT_STATE → permission_denied envelope."""
        repo = self._build_repo([_venue_account_state_row()])
        server = _build_server(repository=repo)
        result = await server._tool_manager.call_tool("list_venue_account_states", {})
        envelope = _decode_envelope(result)
        assert envelope["success"] is False
        assert envelope["error_code"] == "permission_denied"
        repo.get_venue_account_states.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_operator_happy_path_returns_mapped_states(self) -> None:
        """OPERATOR caller → success with fail-closed-mapped account states."""
        repo = self._build_repo([_venue_account_state_row()])
        server = _build_server(repository=repo, claims=_make_claims(role=UserRole.OPERATOR))
        result = await server._tool_manager.call_tool("list_venue_account_states", {})
        envelope = _decode_envelope(result)
        assert envelope["success"] is True
        assert envelope["details"]["count"] == 1
        state = envelope["details"]["account_states"][0]
        assert state["public_id"] == "acct-1"
        assert state["effective_status"] == "observed"
        assert state["is_authoritative"] is True
        assert state["sync_status"] == "observed"

    @pytest.mark.asyncio
    async def test_wallet_outside_scope_returns_account_state_not_found(self) -> None:
        """Inaccessible wallet → account_state_not_found (anti-enumeration)."""
        repo = self._build_repo([], accessible_wallets=["wallet-1"])
        server = _build_server(repository=repo, claims=_make_claims(role=UserRole.OPERATOR))
        result = await server._tool_manager.call_tool(
            "list_venue_account_states", {"wallet_public_id": "wallet-99"}
        )
        envelope = _decode_envelope(result)
        assert envelope["success"] is False
        assert envelope["error_code"] == "account_state_not_found"
        repo.get_venue_account_states.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_empty_result_returns_success_with_empty_list(self) -> None:
        """No rows → success with an empty account-state list."""
        repo = self._build_repo([], accessible_wallets=["wallet-1"])
        server = _build_server(repository=repo, claims=_make_claims(role=UserRole.OPERATOR))
        result = await server._tool_manager.call_tool("list_venue_account_states", {})
        envelope = _decode_envelope(result)
        assert envelope["success"] is True
        assert envelope["details"]["count"] == 0
        assert envelope["details"]["account_states"] == []

    @pytest.mark.asyncio
    async def test_post_fetch_exchange_filter(self) -> None:
        """``exchange`` filter prunes rows post-fetch (repo has no native filter)."""
        kraken = _venue_account_state_row(public_id="acct-kraken", exchange="kraken")
        futures = _venue_account_state_row(public_id="acct-futures", exchange="kraken_futures")
        repo = self._build_repo([kraken, futures], accessible_wallets=["wallet-1"])
        server = _build_server(repository=repo, claims=_make_claims(role=UserRole.OPERATOR))
        result = await server._tool_manager.call_tool(
            "list_venue_account_states", {"exchange": "kraken_futures"}
        )
        envelope = _decode_envelope(result)
        assert envelope["details"]["count"] == 1
        assert envelope["details"]["account_states"][0]["exchange"] == "kraken_futures"

    @pytest.mark.asyncio
    async def test_pre_lifespan_repository_returns_service_unavailable(self) -> None:
        """Pre-lifespan repository surfaces a structured envelope for an authorized role."""
        server = _build_server(repository=None, claims=_make_claims(role=UserRole.OPERATOR))
        result = await server._tool_manager.call_tool("list_venue_account_states", {})
        envelope = _decode_envelope(result)
        assert envelope["error_code"] == "service_unavailable"


class TestGetPositionCycleTool:
    """Coverage for the ``get_position_cycle`` MCP tool."""

    @pytest.mark.asyncio
    async def test_happy_path_returns_full_cycle(self) -> None:
        """Cycle found in caller's scope → envelope carries the full row."""
        repo = AsyncMock()
        repo.list_accessible_wallets_for_operators = AsyncMock(
            return_value=[{"public_id": "wallet-1"}]
        )
        cycle_row = _POSITION_CYCLE_ROW_FIXTURE.copy()
        repo.get_position_cycle_by_public_id = AsyncMock(return_value=cycle_row)
        server = _build_server(repository=repo)
        result = await server._tool_manager.call_tool(
            "get_position_cycle", {"cycle_public_id": "cycle-1"}
        )
        envelope = _decode_envelope(result)
        assert envelope["success"] is True
        assert envelope["details"]["position_cycle"]["public_id"] == cycle_row["public_id"]
        assert envelope["details"]["position_cycle"]["status"] == "open"

    @pytest.mark.asyncio
    async def test_serialised_datetime_fields_are_iso_strings(self) -> None:
        """``timestamp``, ``opened_at``, and ``closed_at`` ISO-stringified."""
        repo = AsyncMock()
        repo.list_accessible_wallets_for_operators = AsyncMock(
            return_value=[{"public_id": "wallet-1"}]
        )
        cycle_row = _POSITION_CYCLE_ROW_FIXTURE.copy()
        cycle_row["closed_at"] = datetime.now(UTC)
        repo.get_position_cycle_by_public_id = AsyncMock(return_value=cycle_row)
        server = _build_server(repository=repo)
        result = await server._tool_manager.call_tool(
            "get_position_cycle", {"cycle_public_id": "cycle-1"}
        )
        envelope = _decode_envelope(result)
        cycle = envelope["details"]["position_cycle"]
        assert isinstance(cycle["timestamp"], str) and "T" in cycle["timestamp"]
        assert isinstance(cycle["opened_at"], str) and "T" in cycle["opened_at"]
        assert isinstance(cycle["closed_at"], str) and "T" in cycle["closed_at"]

    @pytest.mark.asyncio
    async def test_open_cycle_returns_null_closed_at(self) -> None:
        """Open cycle (``closed_at=None``) emitted as JSON null."""
        repo = AsyncMock()
        repo.list_accessible_wallets_for_operators = AsyncMock(
            return_value=[{"public_id": "wallet-1"}]
        )
        cycle_row = _POSITION_CYCLE_ROW_FIXTURE.copy()
        cycle_row["closed_at"] = None
        repo.get_position_cycle_by_public_id = AsyncMock(return_value=cycle_row)
        server = _build_server(repository=repo)
        result = await server._tool_manager.call_tool(
            "get_position_cycle", {"cycle_public_id": "cycle-1"}
        )
        envelope = _decode_envelope(result)
        assert envelope["details"]["position_cycle"]["closed_at"] is None

    @pytest.mark.asyncio
    async def test_unknown_cycle_returns_position_cycle_not_found(self) -> None:
        """Cycle missing → ``position_cycle_not_found`` envelope (anti-enum)."""
        repo = AsyncMock()
        repo.list_accessible_wallets_for_operators = AsyncMock(
            return_value=[{"public_id": "wallet-1"}]
        )
        repo.get_position_cycle_by_public_id = AsyncMock(return_value=None)
        server = _build_server(repository=repo)
        result = await server._tool_manager.call_tool(
            "get_position_cycle", {"cycle_public_id": "cycle-missing"}
        )
        envelope = _decode_envelope(result)
        assert envelope["success"] is False
        assert envelope["error_code"] == "position_cycle_not_found"

    @pytest.mark.asyncio
    async def test_cycle_in_other_wallet_returns_position_cycle_not_found(self) -> None:
        """Cycle exists but its wallet outside caller's scope → not_found."""
        repo = AsyncMock()
        repo.list_accessible_wallets_for_operators = AsyncMock(
            return_value=[{"public_id": "wallet-1"}]
        )
        cycle_row = _POSITION_CYCLE_ROW_FIXTURE.copy()
        cycle_row["wallet_public_id"] = "wallet-other"
        repo.get_position_cycle_by_public_id = AsyncMock(return_value=cycle_row)
        server = _build_server(repository=repo)
        result = await server._tool_manager.call_tool(
            "get_position_cycle", {"cycle_public_id": "cycle-1"}
        )
        envelope = _decode_envelope(result)
        assert envelope["success"] is False
        assert envelope["error_code"] == "position_cycle_not_found"

    @pytest.mark.asyncio
    async def test_admin_sees_any_cycle(self) -> None:
        """ADMIN bypass: cycle in unfamiliar wallet still resolves."""
        repo = AsyncMock()
        cycle_row = _POSITION_CYCLE_ROW_FIXTURE.copy()
        cycle_row["wallet_public_id"] = "wallet-other"
        repo.get_position_cycle_by_public_id = AsyncMock(return_value=cycle_row)
        admin_claims = _make_claims(role=UserRole.ADMIN, user_public_id="admin-1")
        server = _build_server(repository=repo, claims=admin_claims)
        result = await server._tool_manager.call_tool(
            "get_position_cycle", {"cycle_public_id": "cycle-1"}
        )
        envelope = _decode_envelope(result)
        assert envelope["success"] is True
        assert envelope["details"]["position_cycle"]["wallet_public_id"] == "wallet-other"
        repo.list_accessible_wallets_for_operators.assert_not_called()

    @pytest.mark.asyncio
    async def test_role_without_read_positions_returns_permission_denied_envelope(self) -> None:
        """Permission failure surfaces as structured envelope."""
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
            result = await server._tool_manager.call_tool(
                "get_position_cycle", {"cycle_public_id": "cycle-1"}
            )
            envelope = _decode_envelope(result)
            assert envelope["error_code"] == "permission_denied"
        finally:
            if saved is not None:
                ROLE_PERMISSIONS[UserRole.VIEWER] = saved

    @pytest.mark.asyncio
    async def test_pre_lifespan_repository_returns_service_unavailable(self) -> None:
        """Pre-lifespan repository surfaces structured envelope."""
        server = _build_server(repository=None)
        result = await server._tool_manager.call_tool(
            "get_position_cycle", {"cycle_public_id": "cycle-1"}
        )
        envelope = _decode_envelope(result)
        assert envelope["error_code"] == "service_unavailable"


_PLAN_ROW_FIXTURE: dict[str, Any] = {
    "public_id": "plan-1",
    "timestamp": datetime.now(UTC),
    "session_id": "s",
    "sequence_id": 5,
    "plan_type": "manual_once",
    "created_by_user_id": "user-1",
    "created_by_strategy": None,
    "created_via": "api",
    "instrument_public_id": "inst-1",
    "exchange": "kraken",
    "mode": "live",
    "shard_key": "kraken.BTC-USD.live",
    "wallet_public_id": "wallet-1",
    "operator_public_id": "op-1",
    "side": "buy",
    "total_quantity": 1.0,
    "filled_quantity": 0.0,
    "parent_plan_public_id": None,
    "position_cycle_public_id": None,
    "params": {},
    "status": "cancel_requested",
    "created_at": datetime.now(UTC),
    "started_at": None,
    "completed_at": None,
    "expires_at": None,
    "cancel_requested_at": datetime.now(UTC),
    "last_evaluated_at": None,
    "last_error": None,
    "idempotency_key": "create-key",
    "cancel_idempotency_key": "cancel-key-1",
}


class TestCancelOrderTool:
    """Coverage for the ``cancel_order`` MCP write tool."""

    @staticmethod
    def _enforcer_admit() -> Any:

        @asynccontextmanager
        async def _admit(submission: Any) -> Any:
            del submission
            yield None

        enforcer = MagicMock(spec=TradingCapsEnforcer)
        enforcer.guard = _admit
        return enforcer

    @pytest.mark.asyncio
    async def test_happy_path_returns_updated_plan(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Cancel succeeds → envelope carries the updated plan row."""
        plan = _PLAN_ROW_FIXTURE.copy()

        async def _ok(**kwargs: Any) -> dict[str, Any]:
            del kwargs
            return plan

        repo = AsyncMock()
        server = _build_server(repository=repo, caps_enforcer=self._enforcer_admit())
        monkeypatch.setattr(cancel_service.PlansCancelService, "cancel_by_plan_public_id", _ok)
        result = await server._tool_manager.call_tool(
            "cancel_order",
            {"plan_public_id": "plan-1", "idempotency_key": "k-1"},
        )
        envelope = _decode_envelope(result)
        assert envelope["success"] is True
        assert envelope["details"]["plan"]["public_id"] == "plan-1"
        assert envelope["details"]["plan"]["status"] == "cancel_requested"
        assert envelope["details"]["plan"]["cancel_idempotency_key"] == "cancel-key-1"

    @pytest.mark.asyncio
    async def test_empty_idempotency_key_returns_invalid_argument(self) -> None:
        """Empty key → invalid_argument envelope, no service call."""
        repo = AsyncMock()
        server = _build_server(repository=repo, caps_enforcer=self._enforcer_admit())
        result = await server._tool_manager.call_tool(
            "cancel_order", {"plan_public_id": "plan-1", "idempotency_key": ""}
        )
        envelope = _decode_envelope(result)
        assert envelope["error_code"] == "invalid_argument"

    @pytest.mark.asyncio
    async def test_role_without_cancel_orders_returns_permission_denied(self) -> None:
        """Permission failure surfaces as structured envelope."""
        repo = AsyncMock()
        saved = ROLE_PERMISSIONS.get(UserRole.VIEWER)
        ROLE_PERMISSIONS[UserRole.VIEWER] = set()
        server = FastMCP("test")
        register_mcp_tools(
            server,
            repository_getter=lambda: repo,
            caps_enforcer_getter=lambda: self._enforcer_admit(),
            claims_getter=lambda: _make_claims(role=UserRole.VIEWER),
        )
        try:
            result = await server._tool_manager.call_tool(
                "cancel_order", {"plan_public_id": "plan-1", "idempotency_key": "k"}
            )
            envelope = _decode_envelope(result)
            assert envelope["error_code"] == "permission_denied"
        finally:
            if saved is not None:
                ROLE_PERMISSIONS[UserRole.VIEWER] = saved

    @pytest.mark.asyncio
    async def test_pre_lifespan_repository_returns_service_unavailable(self) -> None:
        """Pre-lifespan repository surfaces structured envelope."""
        server = _build_server(repository=None, caps_enforcer=self._enforcer_admit())
        result = await server._tool_manager.call_tool(
            "cancel_order", {"plan_public_id": "plan-1", "idempotency_key": "k"}
        )
        envelope = _decode_envelope(result)
        assert envelope["error_code"] == "service_unavailable"

    @pytest.mark.asyncio
    async def test_pre_lifespan_caps_enforcer_returns_service_unavailable(self) -> None:
        """Caps enforcer not yet wired → service_unavailable envelope."""
        repo = AsyncMock()
        server = _build_server(repository=repo, caps_enforcer=None)
        result = await server._tool_manager.call_tool(
            "cancel_order", {"plan_public_id": "plan-1", "idempotency_key": "k"}
        )
        envelope = _decode_envelope(result)
        assert envelope["error_code"] == "service_unavailable"

    @pytest.mark.asyncio
    async def test_plan_not_found_returns_order_not_found(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """:class:`PlanNotFoundError` → ``order_not_found`` envelope."""

        async def _miss(**kwargs: Any) -> dict[str, Any]:
            del kwargs
            raise cancel_service.PlanNotFoundError("plan-missing")

        repo = AsyncMock()
        server = _build_server(repository=repo, caps_enforcer=self._enforcer_admit())
        monkeypatch.setattr(cancel_service.PlansCancelService, "cancel_by_plan_public_id", _miss)
        result = await server._tool_manager.call_tool(
            "cancel_order", {"plan_public_id": "plan-missing", "idempotency_key": "k"}
        )
        envelope = _decode_envelope(result)
        assert envelope["error_code"] == "order_not_found"

    @pytest.mark.asyncio
    async def test_scope_error_collapses_to_order_not_found(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """:class:`PlanScopeError` collapses to ``order_not_found`` (anti-enum)."""

        async def _scope(**kwargs: Any) -> dict[str, Any]:
            del kwargs
            raise cancel_service.PlanScopeError("plan-other-tenant")

        repo = AsyncMock()
        server = _build_server(repository=repo, caps_enforcer=self._enforcer_admit())
        monkeypatch.setattr(cancel_service.PlansCancelService, "cancel_by_plan_public_id", _scope)
        result = await server._tool_manager.call_tool(
            "cancel_order", {"plan_public_id": "plan-other-tenant", "idempotency_key": "k"}
        )
        envelope = _decode_envelope(result)
        assert envelope["error_code"] == "order_not_found"

    @pytest.mark.asyncio
    async def test_terminal_plan_returns_already_terminal(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Terminal plan → ``already_terminal`` envelope with status detail."""

        async def _term(**kwargs: Any) -> dict[str, Any]:
            del kwargs
            raise cancel_service.PlanAlreadyTerminalError("plan-1", "cancelled")

        repo = AsyncMock()
        server = _build_server(repository=repo, caps_enforcer=self._enforcer_admit())
        monkeypatch.setattr(cancel_service.PlansCancelService, "cancel_by_plan_public_id", _term)
        result = await server._tool_manager.call_tool(
            "cancel_order", {"plan_public_id": "plan-1", "idempotency_key": "k"}
        )
        envelope = _decode_envelope(result)
        assert envelope["error_code"] == "already_terminal"
        assert envelope["details"]["status"] == "cancelled"

    @pytest.mark.asyncio
    async def test_cancel_in_progress_returns_specific_code(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """:class:`PlanCancelInProgressError` → ``cancel_in_progress`` envelope."""

        async def _inprog(**kwargs: Any) -> dict[str, Any]:
            del kwargs
            raise cancel_service.PlanCancelInProgressError("plan-1")

        repo = AsyncMock()
        server = _build_server(repository=repo, caps_enforcer=self._enforcer_admit())
        monkeypatch.setattr(cancel_service.PlansCancelService, "cancel_by_plan_public_id", _inprog)
        result = await server._tool_manager.call_tool(
            "cancel_order", {"plan_public_id": "plan-1", "idempotency_key": "k-new"}
        )
        envelope = _decode_envelope(result)
        assert envelope["error_code"] == "cancel_in_progress"

    @pytest.mark.asyncio
    async def test_idempotency_key_mismatch_returns_conflict(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Different key on a plan that has one → ``idempotency_key_conflict``."""

        async def _conflict(**kwargs: Any) -> dict[str, Any]:
            del kwargs
            raise cancel_service.PlanCancelIdempotencyKeyMismatchError("plan-1")

        repo = AsyncMock()
        server = _build_server(repository=repo, caps_enforcer=self._enforcer_admit())
        monkeypatch.setattr(
            cancel_service.PlansCancelService, "cancel_by_plan_public_id", _conflict
        )
        result = await server._tool_manager.call_tool(
            "cancel_order", {"plan_public_id": "plan-1", "idempotency_key": "k-B"}
        )
        envelope = _decode_envelope(result)
        assert envelope["error_code"] == "idempotency_key_conflict"

    @pytest.mark.asyncio
    async def test_concurrent_change_returns_service_unavailable(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """SCD2 race → ``service_unavailable`` (caller can retry to learn post-race state)."""

        async def _race(**kwargs: Any) -> dict[str, Any]:
            del kwargs
            raise cancel_service.PlanConcurrentChangeError("plan-1")

        repo = AsyncMock()
        server = _build_server(repository=repo, caps_enforcer=self._enforcer_admit())
        monkeypatch.setattr(cancel_service.PlansCancelService, "cancel_by_plan_public_id", _race)
        result = await server._tool_manager.call_tool(
            "cancel_order", {"plan_public_id": "plan-1", "idempotency_key": "k"}
        )
        envelope = _decode_envelope(result)
        assert envelope["error_code"] == "service_unavailable"

    @pytest.mark.asyncio
    async def test_oversize_idempotency_key_returns_invalid_argument(self) -> None:
        """idempotency_key > 64 chars rejected at MCP boundary as invalid_argument.

        ``execution_plans.cancel_idempotency_key`` is ``String(64)``.
        Without this guard PostgreSQL surfaces a raw DataError that
        escapes the structured envelope.
        """
        repo = AsyncMock()
        server = _build_server(repository=repo, caps_enforcer=self._enforcer_admit())
        result = await server._tool_manager.call_tool(
            "cancel_order",
            {"plan_public_id": "plan-1", "idempotency_key": "x" * 65},
        )
        envelope = _decode_envelope(result)
        assert envelope["error_code"] == "invalid_argument"
        assert envelope["details"]["max_length"] == 64

    @pytest.mark.asyncio
    async def test_caps_violation_returns_caps_violation(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """:class:`CapsViolationError` → ``caps_violation`` envelope with detail."""

        async def _caps(**kwargs: Any) -> dict[str, Any]:
            del kwargs
            raise CapsViolationError(
                cap_type="cancel_rate", attempted=11, limit=10, detail="too many"
            )

        repo = AsyncMock()
        server = _build_server(repository=repo, caps_enforcer=self._enforcer_admit())
        monkeypatch.setattr(cancel_service.PlansCancelService, "cancel_by_plan_public_id", _caps)
        result = await server._tool_manager.call_tool(
            "cancel_order", {"plan_public_id": "plan-1", "idempotency_key": "k"}
        )
        envelope = _decode_envelope(result)
        assert envelope["error_code"] == "caps_violation"
        assert envelope["details"]["cap_type"] == "cancel_rate"
        assert envelope["details"]["attempted"] == 11
        assert envelope["details"]["limit"] == 10

    @pytest.mark.asyncio
    async def test_emit_failure_collapses_to_service_unavailable(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """:class:`PlanCancelEmitError` → ``service_unavailable`` envelope."""

        async def _emit_fail(**kwargs: Any) -> dict[str, Any]:
            del kwargs
            raise cancel_service.PlanCancelEmitError("plan-1", RuntimeError("bus down"))

        repo = AsyncMock()
        server = _build_server(repository=repo, caps_enforcer=self._enforcer_admit())
        monkeypatch.setattr(
            cancel_service.PlansCancelService, "cancel_by_plan_public_id", _emit_fail
        )
        result = await server._tool_manager.call_tool(
            "cancel_order", {"plan_public_id": "plan-1", "idempotency_key": "k"}
        )
        envelope = _decode_envelope(result)
        assert envelope["error_code"] == "service_unavailable"

    @pytest.mark.asyncio
    async def test_serialised_plan_datetime_fields_are_iso_strings(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """JSON envelope cannot carry raw datetimes; ISO strings."""
        plan = _PLAN_ROW_FIXTURE.copy()

        async def _ok(**kwargs: Any) -> dict[str, Any]:
            del kwargs
            return plan

        repo = AsyncMock()
        server = _build_server(repository=repo, caps_enforcer=self._enforcer_admit())
        monkeypatch.setattr(cancel_service.PlansCancelService, "cancel_by_plan_public_id", _ok)
        result = await server._tool_manager.call_tool(
            "cancel_order", {"plan_public_id": "plan-1", "idempotency_key": "k"}
        )
        envelope = _decode_envelope(result)
        plan_payload = envelope["details"]["plan"]
        assert isinstance(plan_payload["timestamp"], str) and "T" in plan_payload["timestamp"]
        assert isinstance(plan_payload["created_at"], str)
        assert isinstance(plan_payload["cancel_requested_at"], str)


_SIGNAL_ROW_FIXTURE: dict[str, Any] = {
    "public_id": "sig-1",
    "timestamp": datetime(2026, 4, 28, 10, 0, 0, tzinfo=UTC),
    "session_id": "s",
    "sequence_id": 5,
    "instrument": "BTC-USD",
    "exchange": "kraken",
    "side": "buy",
    "strength": 0.75,
    "reason": "rsi_oversold",
    "strategy_name": "mean_reversion",
    "price": 50000.0,
    "fired_at": datetime(2026, 4, 28, 9, 59, 30, tzinfo=UTC),
    "wallet_public_id": "wallet-1",
    "operator_public_id": "op-1",
}


_CANDLE_ROW_FIXTURE: dict[str, Any] = {
    "open_at": datetime(2026, 4, 28, 9, 0, 0, tzinfo=UTC),
    "timeframe": "1h",
    "open": 49000.0,
    "high": 50500.0,
    "low": 48800.0,
    "close": 50000.0,
    "volume": 12.5,
    "vwap": 49600.0,
    "trades": 314,
    "public_id": "candle-1",
    "timestamp": datetime(2026, 4, 28, 10, 0, 0, tzinfo=UTC),
    "session_id": "s",
    "sequence_id": 6,
}


class TestGetOhlcvTool:
    """Coverage for the ``get_ohlcv`` MCP tool."""

    @staticmethod
    def _build_repo_with_candles(rows: list[dict[str, Any]]) -> Any:
        repo = AsyncMock()
        repo.get_candles = AsyncMock(return_value=rows)
        return repo

    @pytest.mark.asyncio
    async def test_latest_as_of_returns_descending_envelope(self) -> None:
        """Both since/until omitted → latest-as-of mode; envelope shape."""
        candle = _CANDLE_ROW_FIXTURE.copy()
        repo = self._build_repo_with_candles([candle])
        server = _build_server(repository=repo)
        result = await server._tool_manager.call_tool(
            "get_ohlcv",
            {"exchange": "kraken", "instrument": "BTC-USD", "timeframe": "1h"},
        )
        envelope = _decode_envelope(result)
        assert envelope["success"] is True
        assert envelope["details"]["mode"] == "latest_as_of"
        candles = envelope["details"]["candles"]
        assert len(candles) == 1
        row = candles[0]
        assert isinstance(row, list) and len(row) == 6
        assert row[0] == candle["open_at"].isoformat()
        repo.get_candles.assert_awaited_once()
        kwargs = repo.get_candles.await_args.kwargs
        assert kwargs["start"] is None and kwargs["end"] is None
        assert kwargs["order"] == "desc"
        assert kwargs["limit"] == 200

    @pytest.mark.asyncio
    async def test_range_mode_uses_ascending_order(self) -> None:
        """Both since/until supplied → range mode; ASC order, parsed bounds."""
        candle = _CANDLE_ROW_FIXTURE.copy()
        repo = self._build_repo_with_candles([candle])
        server = _build_server(repository=repo)
        result = await server._tool_manager.call_tool(
            "get_ohlcv",
            {
                "exchange": "kraken",
                "instrument": "BTC-USD",
                "timeframe": "1h",
                "since": "2026-04-28T08:00:00Z",
                "until": "2026-04-28T12:00:00Z",
            },
        )
        envelope = _decode_envelope(result)
        assert envelope["success"] is True
        assert envelope["details"]["mode"] == "range"
        kwargs = repo.get_candles.await_args.kwargs
        assert kwargs["start"] == datetime(2026, 4, 28, 8, 0, 0, tzinfo=UTC)
        assert kwargs["end"] == datetime(2026, 4, 28, 12, 0, 0, tzinfo=UTC)
        assert kwargs["order"] == "asc"

    @pytest.mark.asyncio
    async def test_invalid_timeframe_returns_invalid_argument(self) -> None:
        """Unknown timeframe → invalid_argument with allowed list in details."""
        repo = self._build_repo_with_candles([])
        server = _build_server(repository=repo)
        result = await server._tool_manager.call_tool(
            "get_ohlcv",
            {"exchange": "kraken", "instrument": "BTC-USD", "timeframe": "30s"},
        )
        envelope = _decode_envelope(result)
        assert envelope["success"] is False
        assert envelope["error_code"] == "invalid_argument"
        assert envelope["details"]["timeframe"] == "30s"
        assert "1m" in envelope["details"]["allowed_timeframes"]
        repo.get_candles.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_partial_range_returns_invalid_argument(self) -> None:
        """Only one of since/until set → invalid_argument."""
        repo = self._build_repo_with_candles([])
        server = _build_server(repository=repo)
        result = await server._tool_manager.call_tool(
            "get_ohlcv",
            {
                "exchange": "kraken",
                "instrument": "BTC-USD",
                "timeframe": "1h",
                "since": "2026-04-28T08:00:00Z",
            },
        )
        envelope = _decode_envelope(result)
        assert envelope["success"] is False
        assert envelope["error_code"] == "invalid_argument"
        repo.get_candles.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_unix_integer_string_since_rejected(self) -> None:
        """Bare Unix-integer since → invalid_argument (non-ISO 8601)."""
        repo = self._build_repo_with_candles([])
        server = _build_server(repository=repo)
        result = await server._tool_manager.call_tool(
            "get_ohlcv",
            {
                "exchange": "kraken",
                "instrument": "BTC-USD",
                "timeframe": "1h",
                "since": "1745000000",
                "until": "1745020000",
            },
        )
        envelope = _decode_envelope(result)
        assert envelope["success"] is False
        assert envelope["error_code"] == "invalid_argument"
        repo.get_candles.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_negative_limit_returns_invalid_argument(self) -> None:
        """Negative limit → invalid_argument."""
        repo = self._build_repo_with_candles([])
        server = _build_server(repository=repo)
        result = await server._tool_manager.call_tool(
            "get_ohlcv",
            {
                "exchange": "kraken",
                "instrument": "BTC-USD",
                "timeframe": "1h",
                "limit": -1,
            },
        )
        envelope = _decode_envelope(result)
        assert envelope["success"] is False
        assert envelope["error_code"] == "invalid_argument"
        repo.get_candles.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_limit_above_cap_is_clamped(self) -> None:
        """limit=5000 is clamped to _OHLCV_LIMIT_CAP=1000."""
        repo = self._build_repo_with_candles([])
        server = _build_server(repository=repo)
        await server._tool_manager.call_tool(
            "get_ohlcv",
            {
                "exchange": "kraken",
                "instrument": "BTC-USD",
                "timeframe": "1h",
                "limit": 5000,
            },
        )
        kwargs = repo.get_candles.await_args.kwargs
        assert kwargs["limit"] == 1000

    @pytest.mark.asyncio
    async def test_role_without_read_market_data_returns_permission_denied(self) -> None:
        """VIEWER without READ_MARKET_DATA → permission_denied envelope."""
        repo = self._build_repo_with_candles([])
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
            result = await server._tool_manager.call_tool(
                "get_ohlcv",
                {"exchange": "kraken", "instrument": "BTC-USD", "timeframe": "1h"},
            )
            envelope = _decode_envelope(result)
            assert envelope["success"] is False
            assert envelope["error_code"] == "permission_denied"
        finally:
            if saved is not None:
                ROLE_PERMISSIONS[UserRole.VIEWER] = saved

    @pytest.mark.asyncio
    async def test_pre_lifespan_repository_returns_service_unavailable(self) -> None:
        """Pre-lifespan repository → service_unavailable envelope."""
        server = _build_server(repository=None)
        result = await server._tool_manager.call_tool(
            "get_ohlcv",
            {"exchange": "kraken", "instrument": "BTC-USD", "timeframe": "1h"},
        )
        envelope = _decode_envelope(result)
        assert envelope["success"] is False
        assert envelope["error_code"] == "service_unavailable"


class TestListRecentSignalsTool:
    """Coverage for the ``list_recent_signals`` MCP tool."""

    @staticmethod
    def _build_repo_with_signals(
        rows: list[dict[str, Any]],
        accessible_wallets: list[str] | None = None,
    ) -> Any:
        repo = AsyncMock()
        repo.get_signals = AsyncMock(return_value=rows)
        if accessible_wallets is None:
            accessible_wallets = ["wallet-1"]
        repo.list_accessible_wallets_for_operators = AsyncMock(
            return_value=[{"public_id": w} for w in accessible_wallets]
        )
        return repo

    @pytest.mark.asyncio
    async def test_happy_path_returns_signals_envelope(self) -> None:
        """Valid AI_DELEGATE call → signals list + count."""
        signal = _SIGNAL_ROW_FIXTURE.copy()
        repo = self._build_repo_with_signals([signal])
        server = _build_server(repository=repo)
        result = await server._tool_manager.call_tool(
            "list_recent_signals", {"since": "2026-04-28T00:00:00Z"}
        )
        envelope = _decode_envelope(result)
        assert envelope["success"] is True
        assert envelope["details"]["count"] == 1
        assert envelope["details"]["signals"][0]["public_id"] == signal["public_id"]

    @pytest.mark.asyncio
    async def test_serialised_datetime_fields_are_iso_strings(self) -> None:
        """Datetimes surface as ISO-8601 strings."""
        signal = _SIGNAL_ROW_FIXTURE.copy()
        repo = self._build_repo_with_signals([signal])
        server = _build_server(repository=repo)
        result = await server._tool_manager.call_tool(
            "list_recent_signals", {"since": "2026-04-28T00:00:00Z"}
        )
        envelope = _decode_envelope(result)
        sig = envelope["details"]["signals"][0]
        assert isinstance(sig["fired_at"], str) and "T" in sig["fired_at"]
        assert isinstance(sig["timestamp"], str) and "T" in sig["timestamp"]

    @pytest.mark.asyncio
    async def test_missing_since_returns_invalid_argument(self) -> None:
        """Empty since → invalid_argument (since is REQUIRED)."""
        repo = self._build_repo_with_signals([])
        server = _build_server(repository=repo)
        result = await server._tool_manager.call_tool("list_recent_signals", {"since": ""})
        envelope = _decode_envelope(result)
        assert envelope["success"] is False
        assert envelope["error_code"] == "invalid_argument"
        repo.get_signals.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_non_iso_since_returns_invalid_argument(self) -> None:
        """Unparseable since → invalid_argument."""
        repo = self._build_repo_with_signals([])
        server = _build_server(repository=repo)
        result = await server._tool_manager.call_tool("list_recent_signals", {"since": "yesterday"})
        envelope = _decode_envelope(result)
        assert envelope["success"] is False
        assert envelope["error_code"] == "invalid_argument"
        repo.get_signals.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_negative_limit_returns_invalid_argument(self) -> None:
        """Negative limit → invalid_argument."""
        repo = self._build_repo_with_signals([])
        server = _build_server(repository=repo)
        result = await server._tool_manager.call_tool(
            "list_recent_signals",
            {"since": "2026-04-28T00:00:00Z", "limit": -5},
        )
        envelope = _decode_envelope(result)
        assert envelope["success"] is False
        assert envelope["error_code"] == "invalid_argument"
        repo.get_signals.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_wallet_outside_scope_returns_anti_enumeration(self) -> None:
        """Inaccessible wallet → signal_not_found (anti-enumeration)."""
        repo = self._build_repo_with_signals([], accessible_wallets=["wallet-1"])
        server = _build_server(repository=repo)
        result = await server._tool_manager.call_tool(
            "list_recent_signals",
            {"since": "2026-04-28T00:00:00Z", "wallet_public_id": "wallet-99"},
        )
        envelope = _decode_envelope(result)
        assert envelope["success"] is False
        assert envelope["error_code"] == "signal_not_found"
        repo.get_signals.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_role_without_read_signals_returns_permission_denied(self) -> None:
        """VIEWER without READ_SIGNALS → permission_denied envelope."""
        repo = self._build_repo_with_signals([])
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
            result = await server._tool_manager.call_tool(
                "list_recent_signals", {"since": "2026-04-28T00:00:00Z"}
            )
            envelope = _decode_envelope(result)
            assert envelope["success"] is False
            assert envelope["error_code"] == "permission_denied"
        finally:
            if saved is not None:
                ROLE_PERMISSIONS[UserRole.VIEWER] = saved

    @pytest.mark.asyncio
    async def test_pre_lifespan_repository_returns_service_unavailable(self) -> None:
        """Pre-lifespan repository → service_unavailable envelope."""
        server = _build_server(repository=None)
        result = await server._tool_manager.call_tool(
            "list_recent_signals", {"since": "2026-04-28T00:00:00Z"}
        )
        envelope = _decode_envelope(result)
        assert envelope["success"] is False
        assert envelope["error_code"] == "service_unavailable"

    @pytest.mark.asyncio
    async def test_admin_with_no_wallet_passes_none_filter(self) -> None:
        """ADMIN with no wallet hint → repo gets ``wallet_public_ids=None``."""
        repo = AsyncMock()
        repo.get_signals = AsyncMock(return_value=[])
        admin_claims = _make_claims(role=UserRole.ADMIN, user_public_id="admin-1")
        server = FastMCP("test")
        register_mcp_tools(
            server,
            repository_getter=lambda: repo,
            caps_enforcer_getter=lambda: None,
            claims_getter=lambda: admin_claims,
        )
        await server._tool_manager.call_tool(
            "list_recent_signals", {"since": "2026-04-28T00:00:00Z"}
        )
        kwargs = repo.get_signals.await_args.kwargs
        assert kwargs["wallet_public_ids"] is None
        assert kwargs["limit"] == 50

    @pytest.mark.asyncio
    async def test_limit_above_cap_is_clamped(self) -> None:
        """limit=500 is clamped to _LIST_SIGNALS_LIMIT_CAP=200."""
        repo = self._build_repo_with_signals([])
        server = _build_server(repository=repo)
        await server._tool_manager.call_tool(
            "list_recent_signals",
            {"since": "2026-04-28T00:00:00Z", "limit": 500},
        )
        kwargs = repo.get_signals.await_args.kwargs
        assert kwargs["limit"] == 200


class TestParseIso8601UtcHelper:
    """Direct unit coverage for the ``_parse_iso8601_utc`` helper."""

    def test_z_suffix_parses_as_utc(self) -> None:
        """``"...Z"`` is normalised to ``+00:00`` before parsing."""
        parsed = _parse_iso8601_utc("2026-04-28T10:00:00Z")
        assert parsed == datetime(2026, 4, 28, 10, 0, 0, tzinfo=UTC)

    def test_explicit_offset_preserved(self) -> None:
        """``"+00:00"`` round-trips losslessly."""
        parsed = _parse_iso8601_utc("2026-04-28T10:00:00+00:00")
        assert parsed == datetime(2026, 4, 28, 10, 0, 0, tzinfo=UTC)

    def test_garbage_raises_value_error(self) -> None:
        """Unparseable input raises ``ValueError``."""
        with pytest.raises(ValueError):
            _parse_iso8601_utc("yesterday")


class TestListPositionsDelegateScopeRealRepository:
    """M5 consensus test: real-SQLite delegate wallet scoping + provenance."""

    @pytest.mark.asyncio
    async def test_delegate_sees_only_granted_wallet_with_provenance(self, tmp_path: Any) -> None:
        """The delegate's grants gate real projection rows end to end.

        Given: a REAL repository where the trader's projection writer
            persisted rows for two wallets, and the AI_DELEGATE's
            operator holds a scope grant on only one of them,
        When: the list_positions MCP tool runs against that repository,
        Then: only the granted wallet's position is serialized, carrying
            the mark trio (ISO marked_at) and watermark, with the honest
            NULL unrealized value intact — and the other wallet is
            excluded.
        """
        repo = SQLAlchemyRepository(f"sqlite+aiosqlite:///{tmp_path / 'mcp_scope.db'}")
        await repo.create_all()
        now = datetime.now(UTC)
        async with repo.session() as s:
            sym = Symbol(
                native_symbol="BTC-USD",
                base="BTC",
                quote="USD",
                asset_type="crypto",
                created_at=now,
                timestamp=now,
                session_id="s1",
                sequence_id=1,
            )
            s.add(sym)
            await s.commit()
            await s.refresh(sym)
        _, inst_pid = await repo.ensure_instrument(
            symbol_public_id=sym.public_id,
            exchange="kraken",
            session_id="s1",
            sequence_id=2,
            timestamp=now,
        )
        granted_wallet = "00000000-0000-7000-8000-00000000aaa1"
        hidden_wallet = "00000000-0000-7000-8000-00000000aaa2"
        async with repo.session() as s:
            s.add(
                Wallet(
                    public_id=granted_wallet,
                    label="granted",
                    is_paper=True,
                    timestamp=now,
                    session_id="s1",
                    sequence_id=3,
                )
            )
            s.add(
                Wallet(
                    public_id=hidden_wallet,
                    label="hidden",
                    is_paper=True,
                    timestamp=now,
                    session_id="s1",
                    sequence_id=4,
                )
            )
            s.add(
                WalletOperatorScopeGrant(
                    operator_public_id="op-1",
                    wallet_public_id=granted_wallet,
                    granted_by_user_public_id="user-1",
                    scope_kind="instrument",
                    instrument_public_id=inst_pid,
                    timestamp=now,
                    session_id="s1",
                    sequence_id=5,
                )
            )
            await s.commit()
        for wallet, qty, unrealized in (
            (granted_wallet, 1.5, None),
            (hidden_wallet, 9.0, 250.0),
        ):
            await repo.upsert_position_projection(
                {
                    "instrument_public_id": inst_pid,
                    "mode": "paper",
                    "wallet_public_id": wallet,
                    "quantity": qty,
                    "average_price": 50000.0,
                    "unrealized_pnl": unrealized,
                    "realized_pnl": 10.0,
                    "mark_price": 50100.0,
                    "marked_at": now,
                    "source_venue_event_id": 42,
                    "session_id": "00000000-0000-7000-8000-0000000000aa",
                    "sequence_id": 6,
                    "bus_time": now,
                }
            )
        server = _build_server(repository=repo)
        result = await server._tool_manager.call_tool("list_positions", {})
        envelope = _decode_envelope(result)
        assert envelope["success"] is True
        assert envelope["details"]["count"] == 1
        position = envelope["details"]["positions"][0]
        assert position["wallet_public_id"] == granted_wallet
        assert position["quantity"] == 1.5
        assert position["unrealized_pnl"] is None
        assert position["mark_price"] == 50100.0
        assert position["marked_at"] == now.isoformat()
        assert position["source_venue_event_id"] == 42
