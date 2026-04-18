"""MCP tool registrations (plan §4 Day 2 item 7).

Thin wrappers over the existing :class:`Repository` and trade-command
insertion pipeline. Every tool:

    - Reads the authenticated :class:`TokenClaims` from
      :data:`snapper.mcp.server.TOKEN_CLAIMS_CTX` via a caller-supplied
      ``claims_getter``.
    - Enforces the plan's permission matrix: AI_DELEGATE (and every
      higher role) is admitted for read tools; write tools additionally
      require the caller to hold the corresponding Permission (e.g.
      ``CREATE_ORDERS`` for ``submit_manual_order``).
    - Write tools stamp ``source_surface="mcp"`` and wrap the insert
      with :meth:`TradingCapsEnforcer.guard` so per-user caps apply
      identically to REST-initiated writes (plan §3.5 / §3.9).

Day 2c MVP ships two tools covering the plan's acceptance criteria:

    - ``list_instruments(exchange)`` — read-only, returns native
      symbols available on the given exchange (read permission).
    - ``submit_manual_order(...)`` — write, inserts a ``manual_once``
      :class:`ExecutionPlan` + its initial ``TradeCommand`` with
      ``source_surface="mcp"`` and ``idempotency_key`` required. Caps
      evaluated via the shared enforcer before persistence.

Additional tools (cancel_order, list_positions, list_signals,
list_plans, get_status) are Day 2d / Day 3 scope.
"""

import datetime as dt
from collections.abc import Callable
from datetime import UTC
from datetime import datetime
from decimal import Decimal
from typing import Any
from uuid import uuid7

from fastapi import HTTPException
from mcp.server.fastmcp import FastMCP

from snapper.application.trade.caps_enforcer import CapsViolationError
from snapper.application.trade.caps_enforcer import TradingCapsEnforcer
from snapper.application.trade.submission import TradeCommandSubmission
from snapper.auth.domain.permissions import ROLE_PERMISSIONS
from snapper.auth.domain.permissions import Permission
from snapper.auth.schemas.tokens import TokenClaims
from snapper.core.types import TradeCommandStatusEnum
from snapper.data.repository import Repository
from snapper.data.repository_types import ExecutionPlanInsertRow
from snapper.data.repository_types import TradeCommandInsertRow

_MCP_SOURCE_SURFACE = "mcp"
_MCP_TOOL_STREAM = "rest.mcp"


def _require_permission(claims: TokenClaims, permission: Permission) -> None:
    """Raise :class:`PermissionError` if the caller lacks ``permission``.

    Tools call this before performing the action. The role→permission
    mapping is the same one used by :func:`require_permission` on REST
    routes, so MCP-initiated and REST-initiated paths apply the same
    permission matrix.

    Args:
        claims: Verified :class:`TokenClaims` of the MCP caller.
        permission: Required permission for the tool action.

    Raises:
        PermissionError: when the caller's role does not include the
            required permission. Caught by FastMCP and returned to the
            MCP client as a tool-error response.
    """
    role_permissions = ROLE_PERMISSIONS.get(claims.role, set())
    if permission not in role_permissions:
        raise PermissionError(
            f"Tool requires permission '{permission.value}' which is not "
            f"granted to role '{claims.role.value}'."
        )


def register_mcp_tools(
    mcp_server: FastMCP,
    *,
    repository_getter: Callable[[], Repository | None],
    caps_enforcer_getter: Callable[[], TradingCapsEnforcer | None],
    claims_getter: Callable[[], TokenClaims],
) -> None:
    """Register every MCP tool on the passed-in :class:`FastMCP` server.

    Tools are decorated via :meth:`FastMCP.tool` so the downstream
    Streamable HTTP app dispatches to them automatically. The getter
    pattern allows the sub-app to be constructed before the FastAPI
    lifespan has initialized the repository + enforcer singletons —
    the getters resolve lazily at each tool invocation.

    Args:
        mcp_server: The :class:`FastMCP` instance built in
            ``build_mcp_app()``.
        repository_getter: Zero-arg callable returning the
            :class:`Repository` singleton (or ``None`` pre-lifespan).
        caps_enforcer_getter: Zero-arg callable returning the
            :class:`TradingCapsEnforcer` singleton (or ``None``
            pre-lifespan).
        claims_getter: Zero-arg callable returning the authenticated
            :class:`TokenClaims` from the current MCP call's context.
    """

    @mcp_server.tool()
    async def list_instruments(exchange: str) -> dict[str, Any]:
        """List native instrument symbols for a venue.

        Args:
            exchange: Canonical exchange name (e.g. ``kraken``,
                ``kraken_futures``). Case-sensitive; matches the
                identifier used throughout the :class:`Repository`.

        Returns:
            Dict with ``exchange`` and a sorted ``instruments`` list
            of native symbol strings. Example::

                {"exchange": "kraken", "instruments": ["BTC-USD", "ETH-USD"]}

        Raises:
            PermissionError: if the caller lacks
                :data:`Permission.READ_MARKET_DATA`.
            RuntimeError: if the repository singleton is not yet
                initialized (lifespan-not-ready).
        """
        claims = claims_getter()
        _require_permission(claims, Permission.READ_MARKET_DATA)
        repo = repository_getter()
        if repo is None:
            raise RuntimeError(
                "Repository not yet initialized; MCP tool dispatched before lifespan startup."
            )
        rows = await repo.get_exchange_instruments(exchange, as_of=datetime.now(UTC))
        return {"exchange": exchange, "instruments": sorted(rows)}

    @mcp_server.tool()
    async def submit_manual_order(
        exchange: str,
        instrument: str,
        instrument_public_id: str,
        side: str,
        order_type: str,
        quantity: float,
        wallet_public_id: str,
        idempotency_key: str,
        price: float | None = None,
        operator_public_id: str | None = None,
    ) -> dict[str, Any]:
        """Submit a single manual order — wraps REST ``create_order`` via MCP.

        Caps are evaluated against the caller's
        :class:`UserTradingCaps` row before persistence. Writes are
        tagged ``source_surface="mcp"`` on both the plan decision and
        the trade command for audit parity with REST writes.

        Args:
            exchange: Canonical exchange name.
            instrument: Native venue symbol (e.g. ``BTC-USD``).
            instrument_public_id: UUID7 of the Snapper instrument row.
            side: ``buy`` or ``sell``.
            order_type: One of ``market``, ``limit``, ``stop``,
                ``stop_limit``.
            quantity: Order size in base asset units.
            wallet_public_id: UUID7 of the wallet the order attaches
                to. Caller must have scope access to this wallet.
            idempotency_key: Client-supplied uniqueness key; required
                (plan §4 Day 2 #7) because MCP clients are the most
                likely source of accidental retries.
            price: Limit / stop price. Required for non-market order
                types.
            operator_public_id: Optional operator scope. Omit to
                inherit the caller's primary operator.

        Returns:
            Dict with ``plan_public_id`` (UUID7), ``command_public_id``
            (UUID7), and ``source_surface`` (always ``"mcp"``).

        Raises:
            PermissionError: if the caller lacks
                :data:`Permission.CREATE_ORDERS`.
            RuntimeError: if repository / caps enforcer are not
                initialized yet.
            CapsViolationError: on a caps rejection — surfaced to the
                MCP client as a tool error. Same error_code /
                cap_type / attempted / limit shape as REST 422.
        """
        claims = claims_getter()
        _require_permission(claims, Permission.CREATE_ORDERS)
        repo = repository_getter()
        enforcer = caps_enforcer_getter()
        if repo is None or enforcer is None:
            raise RuntimeError(
                "Repository or caps enforcer not initialized; MCP tool dispatched "
                "before lifespan startup."
            )
        now = datetime.now(UTC)
        ts = dt.datetime.now(dt.UTC)
        shard_key = f"{exchange}.{instrument}.live"
        plan_public_id: str | None = None
        client_order_id = str(uuid7())
        submission = TradeCommandSubmission(
            user_public_id=claims.user_public_id,
            operator_public_id=operator_public_id or claims.primary_operator_public_id,
            wallet_public_id=wallet_public_id,
            instrument_public_id=instrument_public_id,
            command_type="create",
            side=side,
            order_type=order_type,
            quantity=Decimal(str(quantity)),
            price=Decimal(str(price)) if price is not None else None,
            source_surface=_MCP_SOURCE_SURFACE,
            idempotency_key=idempotency_key,
        )
        try:
            async with enforcer.guard(submission):
                plan_row: ExecutionPlanInsertRow = {
                    "plan_type": "manual_once",
                    "created_by_user_id": claims.user_public_id or claims.username,
                    "created_via": "api",
                    "instrument_public_id": instrument_public_id,
                    "exchange": exchange,
                    "mode": "live",
                    "shard_key": shard_key,
                    "wallet_public_id": wallet_public_id,
                    "operator_public_id": operator_public_id or claims.primary_operator_public_id,
                    "total_quantity": quantity,
                    "side": side,
                    "params": {
                        "order_type": order_type,
                        "side": side,
                        "child_client_order_id": client_order_id,
                        "native_instrument": instrument,
                        "venue_order_type": order_type,
                        **({"price": price} if price is not None else {}),
                    },
                    "status": "pending",
                    "created_at": now,
                    "idempotency_key": idempotency_key,
                    "session_id": _MCP_TOOL_STREAM,
                    "sequence_id": 1,
                    "timestamp": ts,
                }
                try:
                    _plan_id, plan_public_id = await repo.insert_execution_plan(plan_row)
                except Exception as exc:
                    err_str = str(exc).lower()
                    if "unique" in err_str or "duplicate" in err_str:
                        raise HTTPException(
                            status_code=409,
                            detail="Idempotency key already used",
                        ) from exc
                    raise
                cmd_row: TradeCommandInsertRow = {
                    "command_type": "create",
                    "shard_key": shard_key,
                    "exchange": exchange,
                    "instrument": instrument,
                    "mode": "live",
                    "strategy_id": "manual",
                    "client_order_id": client_order_id,
                    "venue_client_id": client_order_id,
                    "side": side,
                    "order_type": order_type,
                    "quantity": quantity,
                    "price": price,
                    "leverage": None,
                    "reduce_only": False,
                    "status": TradeCommandStatusEnum.CREATED,
                    "created_at": now,
                    "correlation_id": plan_public_id,
                    "session_id": _MCP_TOOL_STREAM,
                    "sequence_id": 2,
                    "timestamp": ts,
                    "wallet_public_id": wallet_public_id,
                    "operator_public_id": operator_public_id or claims.primary_operator_public_id,
                    "user_public_id": claims.user_public_id or claims.username,
                    "plan_public_id": plan_public_id,
                    "source_surface": _MCP_SOURCE_SURFACE,
                }
                _cmd_id, command_public_id = await repo.insert_trade_command(
                    cmd_row, ownership=None
                )
        except CapsViolationError:
            raise
        assert plan_public_id is not None
        assert command_public_id is not None
        return {
            "plan_public_id": plan_public_id,
            "command_public_id": command_public_id,
            "source_surface": _MCP_SOURCE_SURFACE,
        }
