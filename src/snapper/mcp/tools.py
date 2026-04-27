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

from snapper.application.ai_review.citation import validate_ai_review_citation
from snapper.application.ai_review.service import ERROR_DECISION_ALREADY_RECORDED
from snapper.application.ai_review.service import get_ai_review_service
from snapper.application.trade.caps_enforcer import TradingCapsEnforcer
from snapper.application.trade.submission import TradeCommandSubmission
from snapper.auth.domain.permissions import ROLE_PERMISSIONS
from snapper.auth.domain.permissions import Permission
from snapper.auth.domain.roles import UserRole
from snapper.auth.schemas.tokens import TokenClaims
from snapper.auth.scope_grant_service import get_scope_grant_service
from snapper.core.types import AiReviewDecisionEnum
from snapper.core.types import OrderStatusEnum
from snapper.core.types import TradeCommandStatusEnum
from snapper.data.repository import Repository
from snapper.data.repository_types import ExecutionPlanInsertRow
from snapper.data.repository_types import ExecutionRow
from snapper.data.repository_types import OrderRow
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


_LIST_ORDERS_LIMIT_CAP = 200


def _envelope_for_permission_check(
    claims: TokenClaims, permission: Permission
) -> CallToolResult | None:
    """Plan A Q14 envelope wrapper for the permission check.

    Returns the structured ``permission_denied`` envelope when the
    caller's role does not include ``permission``; ``None`` when the
    check passes (caller proceeds with the tool body). The legacy
    :func:`_require_permission` raises a :class:`PermissionError` —
    Plan B §5 requires Plan A Q14 envelopes from every tool, so the
    new tools wrap the check at their entry point.
    """
    role_permissions = ROLE_PERMISSIONS.get(claims.role, set())
    if permission not in role_permissions:
        return to_call_tool_result(
            success=False,
            error_code="permission_denied",
            message=(
                f"Tool requires permission '{permission.value}' which is not "
                f"granted to role '{claims.role.value}'."
            ),
            details={},
        )
    return None


def _envelope_for_repository(
    repository_getter: Callable[[], Repository | None],
) -> Repository | CallToolResult:
    """Plan A Q14 envelope wrapper for the repository getter.

    Returns the :class:`Repository` singleton on success, or a
    structured ``service_unavailable`` envelope when the lifespan has
    not yet initialised the repository — clients receive a
    well-formed Plan A Q14 envelope instead of a raw FastMCP tool
    error.
    """
    repo = repository_getter()
    if repo is None:
        return to_call_tool_result(
            success=False,
            error_code="service_unavailable",
            message=(
                "Repository not yet initialized; MCP tool dispatched before lifespan startup."
            ),
            details={},
        )
    return repo


def _serialize_order_row(row: OrderRow) -> dict[str, Any]:
    """JSON-serialise an :class:`OrderRow` for MCP envelope details.

    ``datetime`` columns are emitted as ISO-8601 strings so the JSON
    envelope stays stable across transports. The TypedDict carries
    ``timestamp`` / ``created_at`` / ``updated_at`` natively as
    :class:`datetime`; downstream :func:`json.dumps` would crash
    without this conversion.
    """
    return {
        "public_id": row["public_id"],
        "client_order_id": row["client_order_id"],
        "exchange_order_id": row["exchange_order_id"],
        "instrument": row["instrument"],
        "exchange": row["exchange"],
        "mode": row["mode"],
        "side": row["side"],
        "order_type": row["order_type"],
        "price": row["price"],
        "size": row["size"],
        "filled_size": row["filled_size"],
        "average_price": row["average_price"],
        "status": row["status"],
        "time_in_force": row["time_in_force"],
        "error": row["error"],
        "leverage": row["leverage"],
        "reduce_only": row["reduce_only"],
        "wallet_public_id": row["wallet_public_id"],
        "operator_public_id": row["operator_public_id"],
        "plan_public_id": row["plan_public_id"],
        "created_at": row["created_at"].isoformat(),
        "updated_at": row["updated_at"].isoformat() if row["updated_at"] is not None else None,
    }


def _serialize_execution_row(row: ExecutionRow) -> dict[str, Any]:
    """JSON-serialise an :class:`ExecutionRow` for MCP envelope details."""
    return {
        "public_id": row["public_id"],
        "trade_id": row["trade_id"],
        "exchange_order_id": row["exchange_order_id"],
        "client_order_id": row["client_order_id"],
        "instrument": row["instrument"],
        "exchange": row["exchange"],
        "side": row["side"],
        "size": row["size"],
        "price": row["price"],
        "fee": row["fee"],
        "fee_asset": row["fee_asset"],
        "status": row["status"],
        "executed_at": row["executed_at"].isoformat(),
        "wallet_public_id": row["wallet_public_id"],
        "operator_public_id": row["operator_public_id"],
        "liquidity_role": row["liquidity_role"],
    }


async def _resolve_target_wallets_for_mcp(
    *,
    claims: TokenClaims,
    repo: Repository,
    wallet_public_id: str | None,
    as_of: datetime,
) -> tuple[list[str] | None, bool]:
    """Mirror REST :func:`snapper.server.scoping.resolve_target_wallets`.

    Returns ``(wallet_public_ids, scope_violation)``. ``scope_violation``
    is ``True`` when the caller cannot legally see
    ``wallet_public_id`` — each tool maps the violation to its own
    entity-specific not-found error code (anti-enumeration: callers
    cannot tell whether the wallet exists or just isn't theirs).

    ADMIN with no explicit ``wallet_public_id`` returns
    ``(None, False)`` so :meth:`Repository.get_orders` skips wallet
    filtering entirely. Non-admin callers always go through
    :meth:`Repository.list_accessible_wallets_for_operators` keyed by
    every operator their token claims membership in.
    """
    if claims.role == UserRole.ADMIN:
        if wallet_public_id is not None:
            return [wallet_public_id], False
        return None, False
    accessible = await repo.list_accessible_wallets_for_operators(
        list(claims.operator_public_ids), as_of
    )
    accessible_ids = [row["public_id"] for row in accessible]
    if wallet_public_id is not None:
        if wallet_public_id not in accessible_ids:
            return [], True
        return [wallet_public_id], False
    return accessible_ids, False


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
    if order.ai_review_public_id is not None:
        await validate_ai_review_citation(
            repo,
            ai_review_public_id=order.ai_review_public_id,
            expected_user_public_id=claims.user_public_id or claims.username,
            expected_wallet_public_id=order.wallet_public_id,
        )
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

    @mcp_server.tool()
    async def list_orders(
        wallet_public_id: str | None = None,
        status: str | None = None,
        exchange: str | None = None,
        instrument: str | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> CallToolResult:
        """List orders with optional filters and pagination (Plan B §2.1).

        Args:
            wallet_public_id: Filter to one wallet. Caller must have
                scope; mismatch returns an empty list with
                ``error_code="order_not_found"`` (anti-enumeration: a
                caller cannot tell whether the wallet exists or simply
                isn't theirs). ``None`` returns orders across every wallet the caller
                can see (ADMIN: every wallet; non-admin: every wallet
                reachable through any operator the caller's claims
                hold membership in).
            status: Optional :class:`OrderStatusEnum` member; pushed
                INTO SQL by :meth:`Repository.get_orders`. Invalid
                values are surfaced as
                ``error_code="invalid_argument"``.
            exchange: Native exchange name filter.
            instrument: Native venue symbol filter.
            limit: Max rows. Default 50, clamped to 200. Negative
                values surface as ``invalid_argument``.
            offset: Pagination offset (0-based). Negative values
                surface as ``invalid_argument``.

        Returns:
            Plan A Q14 envelope. ``details`` carries ``orders``
            (list of ``OrderRow`` dicts) and ``total_count`` (int —
            cardinality of matching rows BEFORE limit/offset).
        """
        claims = claims_getter()
        permission_envelope = _envelope_for_permission_check(claims, Permission.READ_ORDERS)
        if permission_envelope is not None:
            return permission_envelope
        repo_or_envelope = _envelope_for_repository(repository_getter)
        if isinstance(repo_or_envelope, CallToolResult):
            return repo_or_envelope
        repo = repo_or_envelope
        if limit < 0 or offset < 0:
            return to_call_tool_result(
                success=False,
                error_code="invalid_argument",
                message="limit and offset must be non-negative integers.",
                details=sanitize_output({"limit": limit, "offset": offset}),
            )
        if status is not None and status not in OrderStatusEnum.__members__.values():
            return to_call_tool_result(
                success=False,
                error_code="invalid_argument",
                message=(
                    "status must be one of OrderStatusEnum members (see details.allowed_status)."
                ),
                details=sanitize_output(
                    {
                        "status": status,
                        "allowed_status": [s.value for s in OrderStatusEnum],
                    }
                ),
            )
        clamped_limit = min(limit, _LIST_ORDERS_LIMIT_CAP)
        now = datetime.now(UTC)
        wallet_ids, scope_violation = await _resolve_target_wallets_for_mcp(
            claims=claims,
            repo=repo,
            wallet_public_id=wallet_public_id,
            as_of=now,
        )
        if scope_violation:
            return to_call_tool_result(
                success=False,
                error_code="order_not_found",
                message="No orders found for the given filters.",
                details=sanitize_output({"wallet_public_id": wallet_public_id}),
            )
        rows = await repo.get_orders(
            limit=clamped_limit,
            offset=offset,
            as_of=now,
            symbol=instrument,
            exchange=exchange,
            status=status,
            wallet_public_ids=wallet_ids,
        )
        total = await repo.get_orders_total_count(
            as_of=now,
            symbol=instrument,
            exchange=exchange,
            status=status,
            wallet_public_ids=wallet_ids,
        )
        details = sanitize_output(
            {
                "orders": [_serialize_order_row(row) for row in rows],
                "total_count": total,
            }
        )
        return to_call_tool_result(
            success=True,
            error_code=None,
            message=f"Returned {len(rows)} of {total} matching orders.",
            details=details,
        )

    @mcp_server.tool()
    async def get_order_status(command_public_id: str) -> CallToolResult:
        """Fetch full state of a single order by command_public_id (Plan B §2.2).

        Args:
            command_public_id: UUID7 returned by ``submit_manual_order``
                (or any trade-command insert). The lookup is keyed on
                ``trade_commands.public_id``; the ORDER row is reached
                via ``trade_commands.plan_public_id == orders.plan_public_id``.

        Returns:
            Plan A Q14 envelope. On success ``details`` carries the
            full ``OrderRow`` plus ``execution_history`` (list of
            ``ExecutionRow`` dicts ordered oldest-first). When the
            command exists but the exchange has not ACK'd a row yet,
            the envelope returns ``status="pending_dispatch"`` with
            ``plan_public_id`` populated from the trade-command row
            (synthetic placeholder). When the command itself is
            unknown OR not in the caller's wallet scope, returns
            ``error_code="order_not_found"`` (anti-enumeration).
        """
        claims = claims_getter()
        permission_envelope = _envelope_for_permission_check(claims, Permission.READ_ORDERS)
        if permission_envelope is not None:
            return permission_envelope
        repo_or_envelope = _envelope_for_repository(repository_getter)
        if isinstance(repo_or_envelope, CallToolResult):
            return repo_or_envelope
        repo = repo_or_envelope
        now = datetime.now(UTC)
        accessible_wallets, _ = await _resolve_target_wallets_for_mcp(
            claims=claims,
            repo=repo,
            wallet_public_id=None,
            as_of=now,
        )
        command_row = await repo.get_trade_command_by_public_id(command_public_id, as_of=now)
        if command_row is None:
            return to_call_tool_result(
                success=False,
                error_code="order_not_found",
                message="No trade command found for the given command_public_id.",
                details=sanitize_output({"command_public_id": command_public_id}),
            )
        if (
            accessible_wallets is not None
            and command_row["wallet_public_id"] not in accessible_wallets
        ):
            return to_call_tool_result(
                success=False,
                error_code="order_not_found",
                message="No trade command found for the given command_public_id.",
                details=sanitize_output({"command_public_id": command_public_id}),
            )
        order_row = await repo.get_order_by_command_public_id(command_public_id, as_of=now)
        if order_row is None:
            details = sanitize_output(
                {
                    "command_public_id": command_public_id,
                    "plan_public_id": command_row["plan_public_id"],
                    "wallet_public_id": command_row["wallet_public_id"],
                    "status": "pending_dispatch",
                    "execution_history": [],
                }
            )
            return to_call_tool_result(
                success=True,
                error_code=None,
                message="Trade command exists but exchange has not acknowledged the order yet.",
                details=details,
            )
        executions = await repo.get_executions_for_order(
            order_public_id=order_row["public_id"], as_of=now
        )
        details = sanitize_output(
            {
                "order": _serialize_order_row(order_row),
                "execution_history": [_serialize_execution_row(e) for e in executions],
            }
        )
        return to_call_tool_result(
            success=True,
            error_code=None,
            message=f"Order found in status {order_row['status']!r}.",
            details=details,
        )
