"""MCP tool registrations.

Thin wrappers over the existing :class:`Repository` and trade-command
insertion pipeline. Every tool
    Reads the authenticated :class:`TokenClaims` from
      data:`snapper.mcp.server.TOKEN_CLAIMS_CTX` via a caller-supplied
      ``claims_getter``.
    Enforces the plan's permission matrix: AI_DELEGATE (and every
      higher role) is admitted for read tools; write tools additionally
      require the caller to hold the corresponding Permission (e.g.
      ``CREATE_ORDERS`` for ``submit_manual_order``).
    Write tools stamp ``source_surface="mcp"`` and wrap the insert
      with :meth:`TradingCapsEnforcer.guard` so per-user caps apply
      identically to REST-initiated writes.
    ``list_instruments(exchange)`` — read-only, returns native
      symbols available on the given exchange (read permission).
    ``submit_manual_order(...)`` — write, inserts a ``manual_once``
      class:`ExecutionPlan` + its initial ``TradeCommand`` with
      ``source_surface="mcp"`` and ``idempotency_key`` required. Caps
      evaluated via the shared enforcer before persistence.
Additional tools (cancel_order, list_positions, list_signals
list_plans, get_status) are / scope.
"""

import datetime as dt
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC
from datetime import datetime
from decimal import Decimal
from typing import Any
from uuid import uuid7

from fastapi import HTTPException
from loguru import logger
from mcp.server.fastmcp import FastMCP
from mcp.types import CallToolResult
from sqlalchemy.exc import IntegrityError

from snapper.application.ai_review.service import ERROR_DECISION_ALREADY_RECORDED
from snapper.application.ai_review.service import get_ai_review_service
from snapper.application.trade.caps_enforcer import TradingCapsEnforcer
from snapper.application.trade.submission import TradeCommandSubmission
from snapper.auth.domain.permissions import ROLE_PERMISSIONS
from snapper.auth.domain.permissions import Permission
from snapper.auth.schemas.tokens import TokenClaims
from snapper.auth.scope_grant_service import get_scope_grant_service
from snapper.core.types import AiReviewDecisionEnum
from snapper.core.types import TradeCommandStatusEnum
from snapper.data.repository import Repository
from snapper.data.repository_types import ExecutionPlanInsertRow
from snapper.data.repository_types import TradeCommandInsertRow
from snapper.mcp.auth import ensure_operator_in_claims
from snapper.mcp.auth import validate_user_wallet_scope
from snapper.mcp.error_envelope import to_call_tool_result
from snapper.mcp.output_sanitizer import sanitize_output

_MCP_SOURCE_SURFACE = "mcp"
_MCP_TOOL_STREAM = "rest.mcp"

_PG_UNIQUE_VIOLATION_SQLSTATE = "23505"
_SQLITE_CONSTRAINT_UNIQUE_EXTCODE = 2067


@dataclass(frozen=True)
class _ManualOrderInput:
    """Input bundle for :func:`_prepare_manual_order`.

    Plan D Phase 2 #10 — extracted to keep the helper signature under
    the project's 13-parameter cap after adding ``ai_review_public_id``
    for the AI-mediated manual-order flow. All fields mirror the
    matching MCP ``submit_manual_order`` tool parameters one-to-one.
    """

    exchange: str
    instrument: str
    instrument_public_id: str
    side: str
    order_type: str
    quantity: float
    wallet_public_id: str
    idempotency_key: str
    price: float | None
    operator_public_id: str | None
    ai_review_public_id: str | None


@dataclass(frozen=True)
class _PreparedManualOrder:
    """Precomputed context for the MCP manual-order tool."""

    repo: Repository
    enforcer: TradingCapsEnforcer
    submission: TradeCommandSubmission
    plan_row: ExecutionPlanInsertRow
    exchange: str
    instrument: str
    side: str
    order_type: str
    quantity: float
    price: float | None
    created_at: datetime
    bus_time: dt.datetime
    wallet_public_id: str
    operator_public_id: str | None
    user_public_id: str
    shard_key: str
    client_order_id: str


def _is_unique_constraint_violation(exc: IntegrityError) -> bool:
    """Return ``True`` when ``exc`` is a named unique-constraint violation.

    Structured inspection, driver-specific:

        - PostgreSQL (asyncpg / psycopg3): SQLSTATE ``23505``
          is the standard unique-violation code. Both drivers
          surface it on the underlying exception as ``pgcode``.
        - SQLite (aiosqlite): extended result code ``2067``
          (``SQLITE_CONSTRAINT_UNIQUE``) identifies a unique
          violation. Surfaced as ``sqlite_errorcode`` on the
          ``sqlite3.IntegrityError`` instance Python's sqlite3
          module raises.

    Fragile free-text substring matching is deliberately avoided:
    driver message formats change across versions, and unrelated
    constraint failures could contain words like "unique" by
    accident.

    Args:
        exc: The :class:`sqlalchemy.exc.IntegrityError` raised by
            an insert. ``exc.orig`` is the driver-specific
            exception carrying the structured error codes above.

    Returns:
        ``True`` only when the error codes match one of the
        recognized unique-violation codes. Any unknown driver +
        any non-unique integrity error (CHECK, FK, NOT NULL)
        returns ``False`` and the caller re-raises verbatim so
        upstream observability sees the real root cause.
    """
    orig = exc.orig
    if orig is None:
        return False
    pgcode = getattr(orig, "pgcode", None)
    if pgcode == _PG_UNIQUE_VIOLATION_SQLSTATE:
        return True
    sqlite_errorcode = getattr(orig, "sqlite_errorcode", None)
    return sqlite_errorcode == _SQLITE_CONSTRAINT_UNIQUE_EXTCODE


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


def _get_repository_or_raise(repository_getter: Callable[[], Repository | None]) -> Repository:
    """Return the initialized repository singleton or raise a stable error."""
    repo = repository_getter()
    if repo is None:
        raise RuntimeError(
            "Repository not yet initialized; MCP tool dispatched before lifespan startup."
        )
    return repo


def _get_write_dependencies(
    repository_getter: Callable[[], Repository | None],
    caps_enforcer_getter: Callable[[], TradingCapsEnforcer | None],
) -> tuple[Repository, TradingCapsEnforcer]:
    """Return initialized write-path dependencies or raise a stable error."""
    repo = repository_getter()
    enforcer = caps_enforcer_getter()
    if repo is None or enforcer is None:
        raise RuntimeError(
            "Repository or caps enforcer not initialized; MCP tool dispatched "
            "before lifespan startup."
        )
    return repo, enforcer


async def _prepare_manual_order(
    *,
    repository_getter: Callable[[], Repository | None],
    caps_enforcer_getter: Callable[[], TradingCapsEnforcer | None],
    claims_getter: Callable[[], TokenClaims],
    order: _ManualOrderInput,
) -> _PreparedManualOrder:
    """Validate access and precompute immutable rows for manual-order dispatch."""
    claims = claims_getter()
    _require_permission(claims, Permission.CREATE_ORDERS)
    repo, enforcer = _get_write_dependencies(repository_getter, caps_enforcer_getter)
    created_at = datetime.now(UTC)
    ensure_operator_in_claims(claims, order.operator_public_id)
    await validate_user_wallet_scope(claims, order.wallet_public_id, repo, as_of=created_at)
    bus_time = dt.datetime.now(dt.UTC)
    shard_key = f"{order.exchange}.{order.instrument}.live"
    client_order_id = str(uuid7())
    resolved_operator_public_id = order.operator_public_id or claims.primary_operator_public_id
    user_public_id = claims.user_public_id or claims.username
    submission = TradeCommandSubmission(
        user_public_id=claims.user_public_id,
        operator_public_id=resolved_operator_public_id,
        wallet_public_id=order.wallet_public_id,
        instrument_public_id=order.instrument_public_id,
        command_type="create",
        side=order.side,
        order_type=order.order_type,
        quantity=Decimal(str(order.quantity)),
        price=Decimal(str(order.price)) if order.price is not None else None,
        source_surface=_MCP_SOURCE_SURFACE,
        idempotency_key=order.idempotency_key,
        ai_review_public_id=order.ai_review_public_id,
    )
    plan_row: ExecutionPlanInsertRow = {
        "plan_type": "manual_once",
        "created_by_user_id": user_public_id,
        "created_via": "api",
        "instrument_public_id": order.instrument_public_id,
        "exchange": order.exchange,
        "mode": "live",
        "shard_key": shard_key,
        "wallet_public_id": order.wallet_public_id,
        "operator_public_id": resolved_operator_public_id,
        "total_quantity": order.quantity,
        "side": order.side,
        "params": {
            "order_type": order.order_type,
            "side": order.side,
            "child_client_order_id": client_order_id,
            "native_instrument": order.instrument,
            "venue_order_type": order.order_type,
            **({"price": order.price} if order.price is not None else {}),
        },
        "status": "pending",
        "created_at": created_at,
        "idempotency_key": order.idempotency_key,
        "session_id": _MCP_TOOL_STREAM,
        "sequence_id": 1,
        "timestamp": bus_time,
    }
    return _PreparedManualOrder(
        repo=repo,
        enforcer=enforcer,
        submission=submission,
        plan_row=plan_row,
        exchange=order.exchange,
        instrument=order.instrument,
        side=order.side,
        order_type=order.order_type,
        quantity=order.quantity,
        price=order.price,
        created_at=created_at,
        bus_time=bus_time,
        wallet_public_id=order.wallet_public_id,
        operator_public_id=resolved_operator_public_id,
        user_public_id=user_public_id,
        shard_key=shard_key,
        client_order_id=client_order_id,
    )


async def _insert_execution_plan_or_raise_conflict(
    repo: Repository,
    plan_row: ExecutionPlanInsertRow,
) -> str:
    """Insert a plan row and map idempotency conflicts to HTTP 409."""
    try:
        _plan_id, plan_public_id = await repo.insert_execution_plan(plan_row)
    except IntegrityError as exc:
        if _is_unique_constraint_violation(exc):
            raise HTTPException(status_code=409, detail="Idempotency key already used") from exc
        raise
    return plan_public_id


def _build_manual_order_command_row(
    prepared: _PreparedManualOrder,
    plan_public_id: str,
) -> TradeCommandInsertRow:
    """Build the trade-command row after the plan public ID is known."""
    return {
        "command_type": "create",
        "shard_key": prepared.shard_key,
        "exchange": prepared.exchange,
        "instrument": prepared.instrument,
        "mode": "live",
        "strategy_id": "manual",
        "client_order_id": prepared.client_order_id,
        "venue_client_id": prepared.client_order_id,
        "side": prepared.side,
        "order_type": prepared.order_type,
        "quantity": prepared.quantity,
        "price": prepared.price,
        "leverage": None,
        "reduce_only": False,
        "status": TradeCommandStatusEnum.CREATED,
        "created_at": prepared.created_at,
        "correlation_id": plan_public_id,
        "session_id": _MCP_TOOL_STREAM,
        "sequence_id": 2,
        "timestamp": prepared.bus_time,
        "wallet_public_id": prepared.wallet_public_id,
        "operator_public_id": prepared.operator_public_id,
        "user_public_id": prepared.user_public_id,
        "plan_public_id": plan_public_id,
        "source_surface": _MCP_SOURCE_SURFACE,
    }


async def _compensate_failed_plan_insert(
    repo: Repository,
    plan_public_id: str,
    bus_time: dt.datetime,
    exc: Exception,
) -> None:
    """Best-effort compensation when command persistence fails after plan insert."""
    logger.error(
        "MCP submit_manual_order command-insert failed for plan {}: {}",
        plan_public_id,
        exc,
    )
    try:
        await repo.update_execution_plan_status(
            public_id=plan_public_id,
            new_status="failed",
            bus_time=bus_time,
            session_id=_MCP_TOOL_STREAM,
            sequence_id=3,
            last_error=f"MCP TradeCommand insert failed: {exc}",
        )
    except Exception as comp_exc:
        logger.error(
            "MCP plan compensation to failed also failed for plan {}: {}",
            plan_public_id,
            comp_exc,
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
        repo = _get_repository_or_raise(repository_getter)
        rows = await repo.get_exchange_instruments(exchange, as_of=datetime.now(UTC))
        sanitized: dict[str, Any] = sanitize_output(
            {"exchange": exchange, "instruments": sorted(rows)}
        )
        return sanitized

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
        ai_review_public_id: str | None = None,
    ) -> dict[str, Any]:
        """Submit a single manual order — wraps REST ``create_order`` via MCP.

        Caps are evaluated against the caller's
        class:`UserTradingCaps` row before persistence. Writes are
        tagged ``source_surface="mcp"`` on both the plan decision and
        the trade command for audit parity with REST writes.

        Args:
            exchange: Canonical exchange name.
            instrument: Native venue symbol (e.g. ``BTC-USD``).
            instrument_public_id: UUID7 of the Snapper instrument row.
            side: ``buy`` or ``sell``.
            order_type: One of ``market``, ``limit``, ``stop``
                ``stop_limit``.
            quantity: Order size in base asset units.
            wallet_public_id: UUID7 of the wallet the order attaches
                to. Caller must have scope access to this wallet.
            idempotency_key: Client-supplied uniqueness key; required
        because MCP clients are the most
                likely source of accidental retries.
            price: Limit / stop price. Required for non-market order
                types.
            operator_public_id: Optional operator scope. Omit to
                inherit the caller's primary operator.
            ai_review_public_id: Plan D §3.6 / Plan D Phase 2 #10 —
                Optional UUID7 of the ``ai_reviews`` row that
                AI-approved this trade. When set, a
                :class:`CapsViolationError` raised inside
                :meth:`TradingCapsEnforcer.guard` triggers a
                ``bus.caps_violation_after_ai_approve`` publish so
                :class:`AiReviewService` can re-fanout the rejection
                to the delegate's UI.

        Returns:
            Dict with ``plan_public_id`` (UUID7), ``command_public_id``
            (UUID7), and ``source_surface`` (always ``"mcp"``).

        Raises:
            PermissionError: if the caller lacks
                data:`Permission.CREATE_ORDERS`.
            RuntimeError: if repository / caps enforcer are not
                initialized yet.
            CapsViolationError: on a caps rejection — surfaced to the
                MCP client as a tool error. Same error_code /
                cap_type / attempted / limit shape as REST 422.
        """
        order = _ManualOrderInput(
            exchange=exchange,
            instrument=instrument,
            instrument_public_id=instrument_public_id,
            side=side,
            order_type=order_type,
            quantity=quantity,
            wallet_public_id=wallet_public_id,
            idempotency_key=idempotency_key,
            price=price,
            operator_public_id=operator_public_id,
            ai_review_public_id=ai_review_public_id,
        )
        prepared = await _prepare_manual_order(
            repository_getter=repository_getter,
            caps_enforcer_getter=caps_enforcer_getter,
            claims_getter=claims_getter,
            order=order,
        )
        async with prepared.enforcer.guard(prepared.submission):
            plan_public_id = await _insert_execution_plan_or_raise_conflict(
                prepared.repo,
                prepared.plan_row,
            )
            cmd_row = _build_manual_order_command_row(prepared, plan_public_id)
            try:
                _cmd_id, command_public_id = await prepared.repo.insert_trade_command(
                    cmd_row, ownership=None
                )
            except Exception as exc:
                await _compensate_failed_plan_insert(
                    prepared.repo,
                    plan_public_id,
                    prepared.bus_time,
                    exc,
                )
                raise
        assert command_public_id is not None
        sanitized: dict[str, Any] = sanitize_output(
            {
                "plan_public_id": plan_public_id,
                "command_public_id": command_public_id,
                "source_surface": _MCP_SOURCE_SURFACE,
            }
        )
        return sanitized

    @mcp_server.tool()
    async def submit_ai_review_decision(
        review_id: str,
        decision: str,
        rationale: str | None = None,
    ) -> CallToolResult:
        """AI delegate decision endpoint for a CONSULT request (Plan D §6).

        Wraps :meth:`AiReviewService.submit_decision` for the MCP
        transport. Expects an AI_DELEGATE caller (Plan A Q14 narrows
        the permission set; ``CREATE_ORDERS`` is the explicit gate
        per Plan A v1.4 §6.3 since the decision affects the trade
        path the strategy is awaiting).

        Args:
            review_id: UUID7 of the ``ai_reviews`` row the delegate is
                deciding on.
            decision: ``"approve"`` or ``"reject"``. Validated server-
                side against :class:`AiReviewDecisionEnum`; an invalid
                value surfaces as ``error_code="invalid_decision"``.
            rationale: Optional free-text rationale (≤4096 chars per
                the column constraint). Persisted on the
                ``ai_reviews.rationale`` column AND on the audit-event
                ``decision_recorded`` payload.

        Returns:
            :class:`mcp.types.CallToolResult` carrying the Plan A Q14
            envelope (``success``, ``error_code``, ``message``,
            ``details``). Idempotent retries (same delegate + same
            decision after a successful resolve) surface as
            ``success=True, error_code="decision_already_recorded"``
            per cross-plan decision D3.

        Raises:
            PermissionError: if the caller lacks
                :data:`Permission.CREATE_ORDERS` (caught by FastMCP
                and surfaced to the client as a tool error). All
                other failure modes flow through the envelope —
                FastMCP NEVER sees an exception for a known
                :class:`AiReviewDecisionResult` outcome.
        """
        claims = claims_getter()
        _require_permission(claims, Permission.CREATE_ORDERS)
        repo = _get_repository_or_raise(repository_getter)
        try:
            decision_enum = AiReviewDecisionEnum(decision)
        except ValueError:
            sanitized_invalid: dict[str, Any] = sanitize_output({"decision": decision})
            return to_call_tool_result(
                success=False,
                error_code="invalid_decision",
                message=(
                    "decision must be 'approve' or 'reject'; got an "
                    "unrecognised value (see details.decision)."
                ),
                details=sanitized_invalid,
            )
        result = await get_ai_review_service().submit_decision(
            review_public_id=review_id,
            caller_user_public_id=claims.user_public_id,
            decision=decision_enum,
            rationale=rationale,
            repo=repo,
            scope_grant_service=get_scope_grant_service(),
        )
        details: dict[str, Any] = dict(result.details)
        if result.status is not None:
            details["status"] = result.status.value
        if result.resolution_mode is not None:
            details["resolution_mode"] = result.resolution_mode.value
        if result.dispatch_version is not None:
            details["dispatch_version"] = result.dispatch_version
        idempotent_retry = result.error_code == ERROR_DECISION_ALREADY_RECORDED
        success = result.error_code is None or idempotent_retry
        sanitized_details: dict[str, Any] = sanitize_output(details)
        return to_call_tool_result(
            success=success,
            error_code=result.error_code,
            message=result.message,
            details=sanitized_details,
        )
