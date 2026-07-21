"""MCP tool registrations.

Thin wrappers over :class:`Repository` and the trade-command insertion
pipeline. Each tool reads authenticated :class:`TokenClaims` from the
caller-supplied ``claims_getter`` and enforces the same
role-to-permission matrix as REST routes, using the permission that
matches the action:

- ``READ_MARKET_DATA`` for ``list_instruments`` and ``get_ohlcv``.
- ``READ_ORDERS`` for ``list_orders`` and ``get_order_status``.
- ``READ_POSITIONS`` for ``list_positions`` and ``get_position_cycle``.
- ``READ_ACCOUNT_STATE`` for ``list_venue_account_states``.
- ``READ_SIGNALS`` for ``list_recent_signals`` and
  ``get_ai_review_aftermath``.
- ``CREATE_ORDERS`` for ``submit_manual_order`` and
  ``submit_ai_review_decision``.
- ``CANCEL_ORDERS`` for ``cancel_order``.

Write tools that create trade commands stamp ``source_surface="mcp"``
and use :meth:`TradingCapsEnforcer.guard` so per-user caps apply
identically to REST-initiated writes. Tools that reference a wallet also
re-check wallet scope against active grants on every call because JWT
claims are only a login-time snapshot.
"""

import datetime as dt
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC
from datetime import datetime
from decimal import Decimal
from typing import Any
from typing import cast
from uuid import uuid7

from fastapi import HTTPException
from loguru import logger
from mcp.server.fastmcp import FastMCP
from mcp.types import CallToolResult
from sqlalchemy.exc import IntegrityError

from snapper.api.schemas.ai_review_aftermath import AiReviewAftermathResponse
from snapper.application.ai_review.citation import validate_ai_review_citation
from snapper.application.ai_review.service import ERROR_DECISION_ALREADY_RECORDED
from snapper.application.ai_review.service import ERROR_REVIEW_NOT_FOUND
from snapper.application.ai_review.service import get_ai_review_service
from snapper.application.engine.service import compute_shard_key
from snapper.application.plans.cancel_service import PlanAlreadyTerminalError
from snapper.application.plans.cancel_service import PlanCancelEmitError
from snapper.application.plans.cancel_service import PlanCancelIdempotencyKeyMismatchError
from snapper.application.plans.cancel_service import PlanCancelInProgressError
from snapper.application.plans.cancel_service import PlanConcurrentChangeError
from snapper.application.plans.cancel_service import PlanNotFoundError
from snapper.application.plans.cancel_service import PlansCancelService
from snapper.application.plans.cancel_service import PlanScopeError
from snapper.application.plans.manual_once import ManualOnceEvaluator
from snapper.application.portfolio.account_view import build_portfolio_account_state
from snapper.application.portfolio.reconciliation_view import build_portfolio_reconciliation_view
from snapper.application.trade.caps_enforcer import CapsViolationError
from snapper.application.trade.caps_enforcer import TradingCapsEnforcer
from snapper.application.trade.submission import TradeCommandSubmission
from snapper.auth.domain.permissions import ROLE_PERMISSIONS
from snapper.auth.domain.permissions import Permission
from snapper.auth.domain.roles import UserRole
from snapper.auth.schemas.principal import AuthPrincipal
from snapper.auth.schemas.tokens import TokenClaims
from snapper.auth.scope_grant_service import get_scope_grant_service
from snapper.core.types import AiReviewDecisionEnum
from snapper.core.types import AiReviewStatusEnum
from snapper.core.types import ExecutionModeEnum
from snapper.core.types import OrderExchange
from snapper.core.types import OrderStatusEnum
from snapper.core.types import TradeCommandStatusEnum
from snapper.core.wallet_resolution import WalletAmbiguousError
from snapper.core.wallet_resolution import WalletUnresolvedError
from snapper.core.wallet_resolution import resolve_wallet_or_default
from snapper.data.repository import Repository
from snapper.data.repository_types import CandleRow
from snapper.data.repository_types import ExecutionPlanInsertRow
from snapper.data.repository_types import ExecutionRow
from snapper.data.repository_types import OrderRow
from snapper.data.repository_types import PositionCycleRow
from snapper.data.repository_types import PositionRow
from snapper.data.repository_types import SignalRow
from snapper.data.repository_types import TradeCommandInsertRow
from snapper.mcp.auth import ensure_operator_in_claims
from snapper.mcp.auth import validate_user_wallet_scope
from snapper.mcp.error_envelope import to_call_tool_result
from snapper.mcp.output_sanitizer import sanitize_output
from snapper.messaging.infrastructure.publisher import SequenceTracker

_MCP_SOURCE_SURFACE = "mcp"
_MCP_TOOL_STREAM = "rest.mcp"
"""Named SEQUENCE STREAM for MCP manual-order provenance rows.

This is a sequence-stream NAME (like the strategies' consult stream),
never a ``session_id`` value: ``session_id`` columns are UUID-typed in
Postgres, so writing this literal there fails with an asyncpg
DataError (2026-07-10 prod incident — SQLite-backed tests were blind
to the type violation). Rows stamp ``tracker.session_id`` (UUID7) and
advance ``tracker.next_sequence(_MCP_TOOL_STREAM)``.
"""

_PG_UNIQUE_VIOLATION_SQLSTATE = "23505"
_SQLITE_CONSTRAINT_UNIQUE_EXTCODE = 2067
_ORDER_STATUS_VALUES: frozenset[str] = frozenset(member.value for member in OrderStatusEnum)
_MANUAL_ORDER_EVALUATOR = ManualOnceEvaluator()


@dataclass(frozen=True)
class _ManualOrderInput:
    """Input bundle for :func:`_prepare_manual_order`.

    Extracted to keep the helper signature under the project's
    13-parameter cap after adding ``ai_review_public_id`` for the
    AI-mediated manual-order flow. All fields mirror the matching
    MCP ``submit_manual_order`` tool parameters one-to-one.
    """

    exchange: str
    instrument: str
    instrument_public_id: str
    side: str
    order_type: str
    quantity: float
    wallet_public_id: str | None
    idempotency_key: str
    price: float | None
    stop_price: float | None
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
    mode: ExecutionModeEnum
    side: str
    order_type: str
    quantity: float
    price: float | None
    stop_price: float | None
    created_at: datetime
    bus_time: dt.datetime
    wallet_public_id: str
    operator_public_id: str | None
    user_public_id: str
    shard_key: str
    client_order_id: str
    tracker: SequenceTracker


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
_LIST_SIGNALS_LIMIT_CAP = 200
_OHLCV_LIMIT_CAP = 1000
_CANCEL_IDEMPOTENCY_KEY_MAX = 64
_VALID_OHLCV_TIMEFRAMES: frozenset[str] = frozenset({"1m", "5m", "15m", "1h", "4h", "1d"})
_TERMINAL_AI_REVIEW_STATUSES = frozenset(
    {
        AiReviewStatusEnum.RESOLVED_APPROVED.value,
        AiReviewStatusEnum.RESOLVED_REJECTED.value,
        AiReviewStatusEnum.TIMEOUT.value,
        AiReviewStatusEnum.SUPERSEDED.value,
    }
)


def _parse_iso8601_utc(value: str) -> datetime:
    """Parse an ISO-8601 UTC timestamp string into a timezone-aware datetime.

    Accepts canonical UTC suffixes (``"Z"`` or ``"+00:00"``) as the
    Snapper backend's existing public-OSS bridge emits. Bare Unix
    integers (e.g. ``"1745000000"``) are rejected — callers must
    supply ISO-8601 explicitly so the wire contract stays stable.

    Args:
        value: Caller-supplied timestamp string. The empty string is
            rejected to keep the malformed-input branch consistent
            with the broader envelope semantics.

    Returns:
        Timezone-aware :class:`datetime` parsed from ``value``.

    Raises:
        ValueError: if ``value`` cannot be parsed as ISO-8601 (caught
            by the calling tool wrapper and surfaced as
            ``invalid_argument``).
    """
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _envelope_for_permission_check(
    claims: TokenClaims, permission: Permission
) -> CallToolResult | None:
    """Envelope wrapper for the permission check.

    Returns the structured ``permission_denied`` envelope when the
    caller's role does not include ``permission``; ``None`` when the
    check passes and the caller proceeds with the tool body.
    :func:`_require_permission` is the raising-path helper used by the
    original tools (``list_instruments``, ``submit_manual_order``,
    ``submit_ai_review_decision``); envelope-first tools wrap the
    check here at their entry point so clients receive the canonical
    envelope.
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
    """Envelope wrapper for the repository getter.

    Returns the :class:`Repository` singleton on success, or a
    structured ``service_unavailable`` envelope when the lifespan has
    not yet initialised the repository — clients receive a
    well-formed envelope instead of a raw FastMCP tool error.
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


def _serialize_position_row(row: PositionRow) -> dict[str, Any]:
    """JSON-serialise a :class:`PositionRow` for MCP envelope details.

    ``timestamp`` is a :class:`datetime` natively; downstream
    :func:`json.dumps` (called by :func:`to_call_tool_result`) cannot
    encode it, so it is converted to ISO-8601 string.
    """
    return {
        "public_id": row["public_id"],
        "timestamp": row["timestamp"].isoformat(),
        "session_id": row["session_id"],
        "sequence_id": row["sequence_id"],
        "instrument": row["instrument"],
        "instrument_public_id": row["instrument_public_id"],
        "exchange": row["exchange"],
        "mode": row["mode"],
        "quantity": row["quantity"],
        "average_price": row["average_price"],
        "unrealized_pnl": row["unrealized_pnl"],
        "realized_pnl": row["realized_pnl"],
        "mark_price": row["mark_price"],
        "marked_at": row["marked_at"].isoformat() if row["marked_at"] else None,
        "source_venue_event_id": row["source_venue_event_id"],
        "position_cycle_public_id": row["position_cycle_public_id"],
        "wallet_public_id": row["wallet_public_id"],
    }


def _serialize_execution_plan_row(row: dict[str, Any]) -> dict[str, Any]:
    """JSON-serialise an :class:`ExecutionPlanRow` for MCP envelope details.

    The ``cancel_order`` tool returns the updated plan after
    the SCD2 transition. ``timestamp``, ``created_at``,
    ``cancel_requested_at`` and friends are :class:`datetime` natively;
    JSON encoding requires ISO-8601 strings.
    """

    def _iso(value: datetime | None) -> str | None:
        return value.isoformat() if value is not None else None

    return {
        "public_id": row["public_id"],
        "timestamp": _iso(row["timestamp"]),
        "session_id": row["session_id"],
        "sequence_id": row["sequence_id"],
        "plan_type": row["plan_type"],
        "status": row["status"],
        "instrument_public_id": row["instrument_public_id"],
        "exchange": row["exchange"],
        "mode": row["mode"],
        "shard_key": row["shard_key"],
        "wallet_public_id": row["wallet_public_id"],
        "operator_public_id": row["operator_public_id"],
        "side": row["side"],
        "total_quantity": row["total_quantity"],
        "filled_quantity": row["filled_quantity"],
        "created_at": _iso(row["created_at"]),
        "started_at": _iso(row["started_at"]),
        "completed_at": _iso(row["completed_at"]),
        "expires_at": _iso(row["expires_at"]),
        "cancel_requested_at": _iso(row["cancel_requested_at"]),
        "last_evaluated_at": _iso(row["last_evaluated_at"]),
        "last_error": row["last_error"],
        "idempotency_key": row["idempotency_key"],
        "cancel_idempotency_key": row.get("cancel_idempotency_key"),
        "parent_plan_public_id": row["parent_plan_public_id"],
        "position_cycle_public_id": row["position_cycle_public_id"],
    }


def _serialize_position_cycle_row(row: PositionCycleRow) -> dict[str, Any]:
    """JSON-serialise a :class:`PositionCycleRow` for MCP envelope details.

    All :class:`datetime` fields (``timestamp``, ``opened_at``, optional
    ``closed_at``) are converted to ISO-8601 strings so the envelope
    survives :func:`json.dumps` end-to-end.
    """
    return {
        "public_id": row["public_id"],
        "timestamp": row["timestamp"].isoformat(),
        "session_id": row["session_id"],
        "sequence_id": row["sequence_id"],
        "instrument_public_id": row["instrument_public_id"],
        "exchange": row["exchange"],
        "mode": row["mode"],
        "shard_key": row["shard_key"],
        "wallet_public_id": row["wallet_public_id"],
        "operator_public_id": row["operator_public_id"],
        "direction": row["direction"],
        "max_qty": row["max_qty"],
        "status": row["status"],
        "opened_at": row["opened_at"].isoformat(),
        "closed_at": row["closed_at"].isoformat() if row["closed_at"] is not None else None,
        "opening_command_public_id": row["opening_command_public_id"],
        "closing_command_public_id": row["closing_command_public_id"],
    }


def _serialize_signal_row(row: SignalRow) -> dict[str, Any]:
    """JSON-serialise a :class:`SignalRow` for MCP envelope details.

    ``timestamp`` and ``fired_at`` are :class:`datetime` natively;
    converted to ISO-8601 strings so :func:`json.dumps` can encode
    the envelope end-to-end.
    """
    return {
        "public_id": row["public_id"],
        "timestamp": row["timestamp"].isoformat(),
        "session_id": row["session_id"],
        "sequence_id": row["sequence_id"],
        "instrument": row["instrument"],
        "exchange": row["exchange"],
        "side": row["side"],
        "strength": row["strength"],
        "reason": row["reason"],
        "strategy_name": row["strategy_name"],
        "price": row["price"],
        "fired_at": row["fired_at"].isoformat(),
        "wallet_public_id": row["wallet_public_id"],
        "operator_public_id": row["operator_public_id"],
    }


def _serialize_candle_row(row: CandleRow) -> list[Any]:
    """JSON-serialise a :class:`CandleRow` as an OHLCV tuple.

    Emits a six-element list ``[open_at_iso, open, high, low, close,
    volume]``. The compact tuple shape — rather than a per-field
    dict — keeps the OHLCV envelope sized for typical 200-candle
    windows without inflating each row with redundant identity
    columns the caller does not need (``public_id`` /
    ``session_id`` / ``timeframe`` are constant across the
    requested window).
    """
    return [
        row["open_at"].isoformat(),
        row["open"],
        row["high"],
        row["low"],
        row["close"],
        row["volume"],
    ]


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


def _manual_order_wallet_resolution_result(
    exc: WalletAmbiguousError | WalletUnresolvedError,
) -> CallToolResult:
    """Return the structured MCP envelope for manual-order wallet autolookup."""
    if isinstance(exc, WalletAmbiguousError):
        return to_call_tool_result(
            success=False,
            error_code="wallet_ambiguous",
            message="Multiple accessible live wallets matched; specify wallet_public_id.",
            details=sanitize_output({"candidate_wallet_public_ids": exc.candidates}),
        )
    return to_call_tool_result(
        success=False,
        error_code="wallet_unresolved",
        message="No accessible live wallet matched; specify wallet_public_id.",
        details=sanitize_output({"candidate_wallet_public_ids": exc.candidates}),
    )


def _manual_order_wallet_unresolved_result() -> CallToolResult:
    """Return the structured no-wallet envelope for live manual orders."""
    return _manual_order_wallet_resolution_result(WalletUnresolvedError(candidates=[]))


def _manual_order_instrument_identity_result(
    order: _ManualOrderInput, resolved_instrument_public_id: str | None
) -> CallToolResult:
    """Build the error envelope for a spoofed/unknown instrument identity.

    The caller-supplied ``instrument_public_id`` must match the ACTIVE
    instrument for the submitted ``(instrument, exchange)`` pair — the
    caps enforcer and USD oracle key on that identity, so a mismatched
    citation would evaluate quantity and daily-notional caps against
    the wrong (possibly cheaper) instrument.

    Args:
        order: The rejected manual-order input.
        resolved_instrument_public_id: The server-resolved identity,
            or ``None`` when no active instrument exists for the pair.

    Returns:
        The canonical failure envelope.
    """
    return to_call_tool_result(
        success=False,
        error_code="instrument_identity_mismatch",
        message=(
            "instrument_public_id does not match the active instrument for the "
            "submitted (instrument, exchange) pair"
        ),
        details=sanitize_output(
            {
                "instrument": order.instrument,
                "exchange": order.exchange,
                "instrument_public_id": order.instrument_public_id,
                "resolved_instrument_public_id": resolved_instrument_public_id,
            }
        ),
    )


def _manual_order_wallet_blank_result() -> CallToolResult:
    """Return the structured envelope for blank explicit wallet IDs."""
    return to_call_tool_result(
        success=False,
        error_code="invalid_argument",
        message="wallet_public_id must not be blank.",
        details=sanitize_output({"field": "wallet_public_id"}),
    )


async def _prepare_manual_order(
    *,
    repository_getter: Callable[[], Repository | None],
    caps_enforcer_getter: Callable[[], TradingCapsEnforcer | None],
    claims_getter: Callable[[], TokenClaims],
    order: _ManualOrderInput,
    tracker: SequenceTracker,
) -> _PreparedManualOrder | CallToolResult:
    """Validate access and precompute immutable rows for manual-order dispatch.

    MCP manual orders are live-only because ``submit_manual_order``
    exposes no mode parameter. The local mode value feeds both
    persisted rows and the canonical shard-key helper.
    """
    claims = claims_getter()
    _require_permission(claims, Permission.CREATE_ORDERS)
    _MANUAL_ORDER_EVALUATOR.validate_params(
        {
            "order_type": order.order_type,
            "side": order.side,
            "price": order.price,
            "stop_price": order.stop_price,
        }
    )
    repo, enforcer = _get_write_dependencies(repository_getter, caps_enforcer_getter)
    created_at = datetime.now(UTC)
    manual_order_mode = ExecutionModeEnum.LIVE
    ensure_operator_in_claims(claims, order.operator_public_id)
    if order.wallet_public_id is not None and order.wallet_public_id.strip() == "":
        return _manual_order_wallet_blank_result()
    if order.wallet_public_id is None and not claims.operator_public_ids:
        return _manual_order_wallet_unresolved_result()
    try:
        wallet_public_id = await resolve_wallet_or_default(
            repo,
            explicit_wallet_public_id=order.wallet_public_id,
            operator_public_ids=list(claims.operator_public_ids),
            mode=manual_order_mode,
            as_of=created_at,
        )
    except (WalletAmbiguousError, WalletUnresolvedError) as exc:
        return _manual_order_wallet_resolution_result(exc)
    await validate_user_wallet_scope(claims, wallet_public_id, repo, as_of=created_at)
    resolved_instrument_public_id = await repo.get_instrument_public_id_by_symbol(
        order.instrument, order.exchange, created_at
    )
    if (
        resolved_instrument_public_id is None
        or order.instrument_public_id != resolved_instrument_public_id
    ):
        return _manual_order_instrument_identity_result(order, resolved_instrument_public_id)
    if order.ai_review_public_id is not None:
        await validate_ai_review_citation(
            repo,
            ai_review_public_id=order.ai_review_public_id,
            expected_user_public_id=claims.user_public_id or claims.username,
            expected_wallet_public_id=wallet_public_id,
        )
    bus_time = dt.datetime.now(dt.UTC)
    shard_key = compute_shard_key(
        instrument=order.instrument,
        exchange=cast(OrderExchange, order.exchange),
        mode=manual_order_mode,
        wallet_public_id=wallet_public_id,
        strategy_tag=None,
    )
    client_order_id = str(uuid7())
    resolved_operator_public_id = order.operator_public_id or claims.primary_operator_public_id
    user_public_id = claims.user_public_id or claims.username
    submission = TradeCommandSubmission(
        user_public_id=claims.user_public_id,
        operator_public_id=resolved_operator_public_id,
        wallet_public_id=wallet_public_id,
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
        "mode": manual_order_mode,
        "shard_key": shard_key,
        "wallet_public_id": wallet_public_id,
        "operator_public_id": resolved_operator_public_id,
        "total_quantity": order.quantity,
        "side": order.side,
        "params": {
            "order_type": order.order_type,
            "side": order.side,
            "child_client_order_id": client_order_id,
            "native_instrument": order.instrument,
            **({"price": order.price} if order.price is not None else {}),
            **({"stop_price": order.stop_price} if order.stop_price is not None else {}),
        },
        "status": "pending",
        "created_at": created_at,
        "idempotency_key": order.idempotency_key,
        "session_id": tracker.session_id,
        "sequence_id": tracker.next_sequence(_MCP_TOOL_STREAM),
        "timestamp": bus_time,
    }
    return _PreparedManualOrder(
        repo=repo,
        enforcer=enforcer,
        submission=submission,
        plan_row=plan_row,
        exchange=order.exchange,
        instrument=order.instrument,
        mode=manual_order_mode,
        side=order.side,
        order_type=order.order_type,
        quantity=order.quantity,
        price=order.price,
        stop_price=order.stop_price,
        created_at=created_at,
        bus_time=bus_time,
        wallet_public_id=wallet_public_id,
        operator_public_id=resolved_operator_public_id,
        user_public_id=user_public_id,
        shard_key=shard_key,
        client_order_id=client_order_id,
        tracker=tracker,
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
    submitted_notional_usd: float | None = None,
) -> TradeCommandInsertRow:
    """Build the trade-command row after the plan public ID is known.

    Args:
        prepared: The validated manual-order preparation bundle.
        plan_public_id: Public id of the just-inserted execution plan.
        submitted_notional_usd: Admission-time USD quote from the caps
            guard, persisted as decision/accounting lineage on the
            command row (PnL Phase 1).

    Returns:
        The :class:`TradeCommandInsertRow` for the SQL insert.
    """
    return {
        "command_type": "create",
        "shard_key": prepared.shard_key,
        "exchange": prepared.exchange,
        "instrument": prepared.instrument,
        "mode": prepared.mode,
        "strategy_id": "manual",
        "client_order_id": prepared.client_order_id,
        "venue_client_id": prepared.client_order_id,
        "side": prepared.side,
        "order_type": prepared.order_type,
        "quantity": prepared.quantity,
        "price": prepared.price,
        "stop_price": prepared.stop_price,
        "leverage": None,
        "reduce_only": False,
        "status": TradeCommandStatusEnum.CREATED,
        "created_at": prepared.created_at,
        "correlation_id": plan_public_id,
        "session_id": prepared.tracker.session_id,
        "sequence_id": prepared.tracker.next_sequence(_MCP_TOOL_STREAM),
        "timestamp": prepared.bus_time,
        "wallet_public_id": prepared.wallet_public_id,
        "operator_public_id": prepared.operator_public_id,
        "user_public_id": prepared.user_public_id,
        "plan_public_id": plan_public_id,
        "source_surface": _MCP_SOURCE_SURFACE,
        "ai_review_public_id": prepared.submission.ai_review_public_id,
        "submitted_notional_usd": submitted_notional_usd,
    }


async def _compensate_failed_plan_insert(
    repo: Repository,
    plan_public_id: str,
    bus_time: dt.datetime,
    exc: Exception,
    tracker: SequenceTracker,
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
            session_id=tracker.session_id,
            sequence_id=tracker.next_sequence(_MCP_TOOL_STREAM),
            last_error=f"MCP TradeCommand insert failed: {exc}",
        )
    except Exception as comp_exc:
        logger.error(
            "MCP plan compensation to failed also failed for plan {}: {}",
            plan_public_id,
            comp_exc,
        )


@dataclass(frozen=True)
class _ToolAccess:
    """Claims and repository resolved for one MCP tool invocation."""

    claims: TokenClaims
    repo: Repository


def _require_tool_access(
    *,
    claims_getter: Callable[[], TokenClaims],
    repository_getter: Callable[[], Repository | None],
    permission: Permission,
) -> _ToolAccess:
    """Resolve claims and repository, raising on permission or lifecycle errors."""
    claims = claims_getter()
    _require_permission(claims, permission)
    repo = _get_repository_or_raise(repository_getter)
    return _ToolAccess(claims=claims, repo=repo)


def _get_tool_access_or_envelope(
    *,
    claims_getter: Callable[[], TokenClaims],
    repository_getter: Callable[[], Repository | None],
    permission: Permission,
) -> _ToolAccess | CallToolResult:
    """Resolve claims and repository, returning an error envelope when unavailable."""
    claims = claims_getter()
    permission_envelope = _envelope_for_permission_check(claims, permission)
    if permission_envelope is not None:
        return permission_envelope
    repo_or_envelope = _envelope_for_repository(repository_getter)
    if isinstance(repo_or_envelope, CallToolResult):
        return repo_or_envelope
    return _ToolAccess(claims=claims, repo=repo_or_envelope)


def _invalid_argument_result(message: str, details: dict[str, Any]) -> CallToolResult:
    """Return the standard invalid-argument MCP envelope."""
    return to_call_tool_result(
        success=False,
        error_code="invalid_argument",
        message=message,
        details=sanitize_output(details),
    )


def _wallet_scope_not_found_result(
    *,
    error_code: str,
    message: str,
    wallet_public_id: str | None,
) -> CallToolResult:
    """Collapse wallet-scope mismatches into the entity-specific not-found envelope."""
    return to_call_tool_result(
        success=False,
        error_code=error_code,
        message=message,
        details=sanitize_output({"wallet_public_id": wallet_public_id}),
    )


async def _resolve_wallet_ids_or_envelope(
    *,
    claims: TokenClaims,
    repo: Repository,
    wallet_public_id: str | None,
    as_of: datetime,
    error_code: str,
    message: str,
) -> tuple[list[str] | None, CallToolResult | None]:
    """Resolve wallet scope or return the corresponding anti-enumeration envelope."""
    wallet_ids, scope_violation = await _resolve_target_wallets_for_mcp(
        claims=claims,
        repo=repo,
        wallet_public_id=wallet_public_id,
        as_of=as_of,
    )
    if not scope_violation:
        return wallet_ids, None
    return (
        None,
        _wallet_scope_not_found_result(
            error_code=error_code,
            message=message,
            wallet_public_id=wallet_public_id,
        ),
    )


def _build_cancel_principal(claims: TokenClaims) -> AuthPrincipal:
    """Project MCP token claims into the cancel-service principal payload."""
    return AuthPrincipal(
        username=claims.username,
        role=claims.role,
        user_public_id=claims.user_public_id or claims.username,
        operator_public_ids=list(claims.operator_public_ids),
        primary_operator_public_id=claims.primary_operator_public_id or "",
    )


def _map_cancel_exception_to_envelope(exc: Exception, plan_public_id: str) -> CallToolResult:
    """Map cancel-service domain exceptions into MCP error envelopes."""
    if isinstance(exc, (PlanNotFoundError, PlanScopeError)):
        return to_call_tool_result(
            success=False,
            error_code="order_not_found",
            message="No execution plan found for the given plan_public_id.",
            details=sanitize_output({"plan_public_id": plan_public_id}),
        )
    if isinstance(exc, PlanAlreadyTerminalError):
        return to_call_tool_result(
            success=False,
            error_code="already_terminal",
            message=f"Plan is already in terminal status {exc.status!r}.",
            details=sanitize_output({"plan_public_id": plan_public_id, "status": exc.status}),
        )
    if isinstance(exc, PlanCancelInProgressError):
        return to_call_tool_result(
            success=False,
            error_code="cancel_in_progress",
            message=(
                "Plan cancel already in progress with a different "
                "idempotency_key; retry with the original key or wait."
            ),
            details=sanitize_output({"plan_public_id": plan_public_id}),
        )
    if isinstance(exc, PlanCancelIdempotencyKeyMismatchError):
        return to_call_tool_result(
            success=False,
            error_code="idempotency_key_conflict",
            message=(
                "Plan already has a different cancel_idempotency_key; "
                "use the original key to obtain the cancel state."
            ),
            details=sanitize_output({"plan_public_id": plan_public_id}),
        )
    if isinstance(exc, PlanConcurrentChangeError):
        return to_call_tool_result(
            success=False,
            error_code="service_unavailable",
            message=(
                "Plan status changed concurrently before the cancel could be claimed; "
                "retry to surface the post-race state."
            ),
            details=sanitize_output({"plan_public_id": plan_public_id}),
        )
    if isinstance(exc, CapsViolationError):
        return to_call_tool_result(
            success=False,
            error_code="caps_violation",
            message="Caps enforcer rejected the cancel.",
            details=sanitize_output(
                {
                    "plan_public_id": plan_public_id,
                    "cap_type": exc.cap_type,
                    "attempted": exc.attempted,
                    "limit": exc.limit,
                }
            ),
        )
    if isinstance(exc, PlanCancelEmitError):
        return to_call_tool_result(
            success=False,
            error_code="service_unavailable",
            message=(
                "Failed to emit cancel command; plan compensated to "
                "failed. Executor recovery will re-emit on restart."
            ),
            details=sanitize_output({"plan_public_id": plan_public_id}),
        )
    raise exc


async def _list_instruments_tool(
    *,
    repository_getter: Callable[[], Repository | None],
    claims_getter: Callable[[], TokenClaims],
    exchange: str,
) -> dict[str, Any]:
    """Run the MCP instrument-listing read path."""
    access = _require_tool_access(
        claims_getter=claims_getter,
        repository_getter=repository_getter,
        permission=Permission.READ_MARKET_DATA,
    )
    rows = await access.repo.get_exchange_instruments(exchange, as_of=datetime.now(UTC))
    sanitized: dict[str, Any] = sanitize_output({"exchange": exchange, "instruments": sorted(rows)})
    return sanitized


async def _submit_ai_review_decision_tool(
    *,
    repository_getter: Callable[[], Repository | None],
    claims_getter: Callable[[], TokenClaims],
    review_id: str,
    decision: str,
    rationale: str | None,
) -> CallToolResult:
    """Run the AI-review decision MCP path."""
    access = _require_tool_access(
        claims_getter=claims_getter,
        repository_getter=repository_getter,
        permission=Permission.CREATE_ORDERS,
    )
    try:
        decision_enum = AiReviewDecisionEnum(decision)
    except ValueError:
        return to_call_tool_result(
            success=False,
            error_code="invalid_decision",
            message=(
                "decision must be 'approve' or 'reject'; got an "
                "unrecognised value (see details.decision)."
            ),
            details=sanitize_output({"decision": decision}),
        )
    result = await get_ai_review_service().submit_decision(
        review_public_id=review_id,
        caller_user_public_id=access.claims.user_public_id,
        decision=decision_enum,
        rationale=rationale,
        repo=access.repo,
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
    return to_call_tool_result(
        success=result.error_code is None or idempotent_retry,
        error_code=result.error_code,
        message=result.message,
        details=sanitize_output(details),
    )


async def _list_orders_tool(
    *,
    repository_getter: Callable[[], Repository | None],
    claims_getter: Callable[[], TokenClaims],
    wallet_public_id: str | None,
    status: str | None,
    exchange: str | None,
    instrument: str | None,
    limit: int,
    offset: int,
) -> CallToolResult:
    """Run the list-orders MCP read path."""
    access = _get_tool_access_or_envelope(
        claims_getter=claims_getter,
        repository_getter=repository_getter,
        permission=Permission.READ_ORDERS,
    )
    if isinstance(access, CallToolResult):
        return access
    if limit < 0 or offset < 0:
        return _invalid_argument_result(
            "limit and offset must be non-negative integers.",
            {"limit": limit, "offset": offset},
        )
    if status is not None and status not in _ORDER_STATUS_VALUES:
        return _invalid_argument_result(
            "status must be one of OrderStatusEnum members (see details.allowed_status).",
            {"status": status, "allowed_status": sorted(_ORDER_STATUS_VALUES)},
        )
    now = datetime.now(UTC)
    wallet_ids, scope_envelope = await _resolve_wallet_ids_or_envelope(
        claims=access.claims,
        repo=access.repo,
        wallet_public_id=wallet_public_id,
        as_of=now,
        error_code="order_not_found",
        message="No orders found for the given filters.",
    )
    if scope_envelope is not None:
        return scope_envelope
    clamped_limit = min(limit, _LIST_ORDERS_LIMIT_CAP)
    rows = await access.repo.get_orders(
        limit=clamped_limit,
        offset=offset,
        as_of=now,
        symbol=instrument,
        exchange=exchange,
        status=status,
        wallet_public_ids=wallet_ids,
    )
    total = await access.repo.get_orders_total_count(
        as_of=now,
        symbol=instrument,
        exchange=exchange,
        status=status,
        wallet_public_ids=wallet_ids,
    )
    return to_call_tool_result(
        success=True,
        error_code=None,
        message=f"Returned {len(rows)} of {total} matching orders.",
        details=sanitize_output(
            {
                "orders": [_serialize_order_row(row) for row in rows],
                "total_count": total,
            }
        ),
    )


async def _get_order_status_tool(
    *,
    repository_getter: Callable[[], Repository | None],
    claims_getter: Callable[[], TokenClaims],
    command_public_id: str,
) -> CallToolResult:
    """Run the get-order-status MCP read path."""
    access = _get_tool_access_or_envelope(
        claims_getter=claims_getter,
        repository_getter=repository_getter,
        permission=Permission.READ_ORDERS,
    )
    if isinstance(access, CallToolResult):
        return access
    now = datetime.now(UTC)
    accessible_wallets, _ = await _resolve_target_wallets_for_mcp(
        claims=access.claims,
        repo=access.repo,
        wallet_public_id=None,
        as_of=now,
    )
    command_row = await access.repo.get_trade_command_by_public_id(command_public_id, as_of=now)
    if command_row is None:
        return to_call_tool_result(
            success=False,
            error_code="order_not_found",
            message="No trade command found for the given command_public_id.",
            details=sanitize_output({"command_public_id": command_public_id}),
        )
    if accessible_wallets is not None and command_row["wallet_public_id"] not in accessible_wallets:
        return to_call_tool_result(
            success=False,
            error_code="order_not_found",
            message="No trade command found for the given command_public_id.",
            details=sanitize_output({"command_public_id": command_public_id}),
        )
    order_row = await access.repo.get_order_by_command_public_id(command_public_id, as_of=now)
    if order_row is None:
        return to_call_tool_result(
            success=True,
            error_code=None,
            message="Trade command exists but exchange has not acknowledged the order yet.",
            details=sanitize_output(
                {
                    "command_public_id": command_public_id,
                    "plan_public_id": command_row["plan_public_id"],
                    "wallet_public_id": command_row["wallet_public_id"],
                    "status": "pending_dispatch",
                    "execution_history": [],
                }
            ),
        )
    executions = await access.repo.get_executions_for_order(
        order_public_id=order_row["public_id"], as_of=now
    )
    return to_call_tool_result(
        success=True,
        error_code=None,
        message=f"Order found in status {order_row['status']!r}.",
        details=sanitize_output(
            {
                "order": _serialize_order_row(order_row),
                "execution_history": [
                    _serialize_execution_row(execution) for execution in executions
                ],
            }
        ),
    )


async def _list_positions_tool(
    *,
    repository_getter: Callable[[], Repository | None],
    claims_getter: Callable[[], TokenClaims],
    wallet_public_id: str | None,
    exchange: str | None,
    instrument: str | None,
) -> CallToolResult:
    """Run the list-positions MCP read path."""
    access = _get_tool_access_or_envelope(
        claims_getter=claims_getter,
        repository_getter=repository_getter,
        permission=Permission.READ_POSITIONS,
    )
    if isinstance(access, CallToolResult):
        return access
    now = datetime.now(UTC)
    wallet_ids, scope_envelope = await _resolve_wallet_ids_or_envelope(
        claims=access.claims,
        repo=access.repo,
        wallet_public_id=wallet_public_id,
        as_of=now,
        error_code="position_not_found",
        message="No positions found for the given filters.",
    )
    if scope_envelope is not None:
        return scope_envelope
    rows = await access.repo.get_positions(as_of=now, wallet_public_ids=wallet_ids)
    filtered = [
        row
        for row in rows
        if (exchange is None or row["exchange"] == exchange)
        and (instrument is None or row["instrument"] == instrument)
    ]
    return to_call_tool_result(
        success=True,
        error_code=None,
        message=f"Returned {len(filtered)} matching positions.",
        details=sanitize_output(
            {
                "positions": [_serialize_position_row(row) for row in filtered],
                "count": len(filtered),
            }
        ),
    )


async def _list_venue_account_states_tool(
    *,
    repository_getter: Callable[[], Repository | None],
    claims_getter: Callable[[], TokenClaims],
    wallet_public_id: str | None,
    exchange: str | None,
) -> CallToolResult:
    """Run the shared account and reconciliation truth read path."""
    access = _get_tool_access_or_envelope(
        claims_getter=claims_getter,
        repository_getter=repository_getter,
        permission=Permission.READ_ACCOUNT_STATE,
    )
    if isinstance(access, CallToolResult):
        return access
    now = datetime.now(UTC)
    wallet_ids, scope_envelope = await _resolve_wallet_ids_or_envelope(
        claims=access.claims,
        repo=access.repo,
        wallet_public_id=wallet_public_id,
        as_of=now,
        error_code="account_state_not_found",
        message="No venue account states found for the given filters.",
    )
    if scope_envelope is not None:
        return scope_envelope
    contexts = await access.repo.get_portfolio_reconciliation_read_contexts(wallet_ids)
    states = [
        build_portfolio_account_state(
            context["account_state"],
            now,
            build_portfolio_reconciliation_view(context, now),
            duplicate_active_rows=context["duplicate_active_rows"],
        )
        for context in contexts
        if exchange is None or context["account_state"]["exchange"] == exchange
    ]
    return to_call_tool_result(
        success=True,
        error_code=None,
        message=f"Returned {len(states)} venue account states.",
        details=sanitize_output(
            {
                "account_states": [state.model_dump(mode="json") for state in states],
                "count": len(states),
            }
        ),
    )


async def _get_position_cycle_tool(
    *,
    repository_getter: Callable[[], Repository | None],
    claims_getter: Callable[[], TokenClaims],
    cycle_public_id: str,
) -> CallToolResult:
    """Run the get-position-cycle MCP read path."""
    access = _get_tool_access_or_envelope(
        claims_getter=claims_getter,
        repository_getter=repository_getter,
        permission=Permission.READ_POSITIONS,
    )
    if isinstance(access, CallToolResult):
        return access
    now = datetime.now(UTC)
    accessible_wallets, _ = await _resolve_target_wallets_for_mcp(
        claims=access.claims,
        repo=access.repo,
        wallet_public_id=None,
        as_of=now,
    )
    cycle_row = await access.repo.get_position_cycle_by_public_id(cycle_public_id, as_of=now)
    if cycle_row is None or (
        accessible_wallets is not None and cycle_row["wallet_public_id"] not in accessible_wallets
    ):
        return to_call_tool_result(
            success=False,
            error_code="position_cycle_not_found",
            message="No position cycle found for the given cycle_public_id.",
            details=sanitize_output({"cycle_public_id": cycle_public_id}),
        )
    return to_call_tool_result(
        success=True,
        error_code=None,
        message=f"Position cycle found in status {cycle_row['status']!r}.",
        details=sanitize_output({"position_cycle": _serialize_position_cycle_row(cycle_row)}),
    )


async def _get_ai_review_aftermath_tool(
    *,
    repository_getter: Callable[[], Repository | None],
    claims_getter: Callable[[], TokenClaims],
    review_public_id: str,
) -> CallToolResult:
    """Run the scoped terminal AI-review aftermath MCP read path.

    Args:
        repository_getter: Deferred repository singleton accessor.
        claims_getter: Accessor for the authenticated caller's claims.
        review_public_id: Public identifier of the review to project.

    Returns:
        Canonical MCP envelope containing the aftermath or a stable read error.
    """
    access = _get_tool_access_or_envelope(
        claims_getter=claims_getter,
        repository_getter=repository_getter,
        permission=Permission.READ_SIGNALS,
    )
    if isinstance(access, CallToolResult):
        return access
    as_of = datetime.now(UTC)
    delegate = await access.repo.get_ai_delegate_by_user_public_id(access.claims.user_public_id)
    if delegate is None:
        return to_call_tool_result(
            success=False,
            error_code="not_a_delegate",
            message="AI review aftermath requires a registered AI delegate caller.",
            details={},
        )
    review = await access.repo.get_ai_review(review_public_id)
    if review is None:
        return to_call_tool_result(
            success=False,
            error_code=ERROR_REVIEW_NOT_FOUND,
            message="No terminal review with that id was found in the caller's scope.",
            details=sanitize_output({"review_public_id": review_public_id}),
        )
    scope_ok = await access.repo.has_grant_for_delegate(
        delegate_public_id=delegate["public_id"],
        wallet_public_id=review["wallet_public_id"],
        instrument_public_id=review["instrument_public_id"],
        as_of=as_of,
    )
    if not scope_ok:
        return to_call_tool_result(
            success=False,
            error_code=ERROR_REVIEW_NOT_FOUND,
            message="No terminal review with that id was found in the caller's scope.",
            details=sanitize_output({"review_public_id": review_public_id}),
        )
    if review["status"] not in _TERMINAL_AI_REVIEW_STATUSES:
        return to_call_tool_result(
            success=False,
            error_code="review_not_terminal",
            message="AI review aftermath is available only after terminal resolution.",
            details=sanitize_output(
                {
                    "review_public_id": review_public_id,
                    "status": review["status"],
                }
            ),
        )
    aftermath = await access.repo.get_ai_review_aftermath(review_public_id, as_of=as_of)
    if aftermath is None:
        return to_call_tool_result(
            success=False,
            error_code=ERROR_REVIEW_NOT_FOUND,
            message="No terminal review with that id was found in the caller's scope.",
            details=sanitize_output({"review_public_id": review_public_id}),
        )
    response = AiReviewAftermathResponse.model_validate(aftermath)
    return to_call_tool_result(
        success=True,
        error_code=None,
        message=(
            f"Returned aftermath with {len(response.orders)} orders, "
            f"{len(response.executions)} executions, and "
            f"{len(response.current_positions)} current positions."
        ),
        details=sanitize_output({"aftermath": response.model_dump(mode="json")}),
    )


async def _cancel_order_tool(
    *,
    repository_getter: Callable[[], Repository | None],
    caps_enforcer_getter: Callable[[], TradingCapsEnforcer | None],
    claims_getter: Callable[[], TokenClaims],
    tracker_getter: Callable[[], SequenceTracker | None],
    plan_public_id: str,
    idempotency_key: str,
) -> CallToolResult:
    """Run the cancel-order MCP write path."""
    access = _get_tool_access_or_envelope(
        claims_getter=claims_getter,
        repository_getter=repository_getter,
        permission=Permission.CANCEL_ORDERS,
    )
    if isinstance(access, CallToolResult):
        return access
    enforcer = caps_enforcer_getter()
    if enforcer is None:
        return to_call_tool_result(
            success=False,
            error_code="service_unavailable",
            message=(
                "Caps enforcer not yet initialized; MCP cancel "
                "dispatched before lifespan startup."
            ),
            details=sanitize_output({}),
        )
    if not idempotency_key or len(idempotency_key) > _CANCEL_IDEMPOTENCY_KEY_MAX:
        return _invalid_argument_result(
            (
                f"idempotency_key must be 1-{_CANCEL_IDEMPOTENCY_KEY_MAX} chars; "
                "matches the execution_plans.cancel_idempotency_key column width."
            ),
            {
                "idempotency_key_length": len(idempotency_key) if idempotency_key else 0,
                "max_length": _CANCEL_IDEMPOTENCY_KEY_MAX,
            },
        )
    tracker = tracker_getter() or SequenceTracker()
    try:
        updated = await PlansCancelService.cancel_by_plan_public_id(
            plan_public_id=plan_public_id,
            idempotency_key=idempotency_key,
            principal=_build_cancel_principal(access.claims),
            repo=access.repo,
            tracker=tracker,
            caps_enforcer=enforcer,
        )
    except (
        PlanAlreadyTerminalError,
        PlanCancelEmitError,
        PlanCancelIdempotencyKeyMismatchError,
        PlanCancelInProgressError,
        PlanConcurrentChangeError,
        PlanNotFoundError,
        PlanScopeError,
        CapsViolationError,
    ) as exc:
        return _map_cancel_exception_to_envelope(exc, plan_public_id)
    return to_call_tool_result(
        success=True,
        error_code=None,
        message=f"Cancel claimed; plan now in status {updated['status']!r}.",
        details=sanitize_output(
            {"plan": _serialize_execution_plan_row(cast(dict[str, Any], updated))}
        ),
    )


async def _get_ohlcv_tool(
    *,
    repository_getter: Callable[[], Repository | None],
    claims_getter: Callable[[], TokenClaims],
    exchange: str,
    instrument: str,
    timeframe: str,
    since: str | None,
    until: str | None,
    limit: int,
) -> CallToolResult:
    """Run the OHLCV MCP read path."""
    access = _get_tool_access_or_envelope(
        claims_getter=claims_getter,
        repository_getter=repository_getter,
        permission=Permission.READ_MARKET_DATA,
    )
    if isinstance(access, CallToolResult):
        return access
    if limit < 0:
        return _invalid_argument_result("limit must be a non-negative integer.", {"limit": limit})
    if timeframe not in _VALID_OHLCV_TIMEFRAMES:
        return _invalid_argument_result(
            "timeframe must be one of the supported intervals (see details.allowed_timeframes).",
            {"timeframe": timeframe, "allowed_timeframes": sorted(_VALID_OHLCV_TIMEFRAMES)},
        )
    if (since is None) != (until is None):
        return _invalid_argument_result(
            (
                "since and until must be supplied together for range mode; "
                "omit both for latest-as-of mode."
            ),
            {"since": since, "until": until},
        )
    try:
        parsed_since = _parse_iso8601_utc(since) if since is not None else None
        parsed_until = _parse_iso8601_utc(until) if until is not None else None
    except ValueError as exc:
        return _invalid_argument_result(
            "since and until must be ISO 8601 UTC strings (e.g. '2026-01-01T00:00:00Z').",
            {"since": since, "until": until, "error": str(exc)},
        )
    range_mode = parsed_since is not None
    rows = await access.repo.get_candles(
        instrument=instrument,
        timeframe=timeframe,
        start=parsed_since,
        end=parsed_until,
        exchange=cast(Any, exchange),
        as_of=datetime.now(UTC),
        limit=min(limit, _OHLCV_LIMIT_CAP),
        order="asc" if range_mode else "desc",
    )
    return to_call_tool_result(
        success=True,
        error_code=None,
        message=f"Returned {len(rows)} candles.",
        details=sanitize_output(
            {
                "candles": [_serialize_candle_row(row) for row in rows],
                "mode": "range" if range_mode else "latest_as_of",
            }
        ),
    )


async def _list_recent_signals_tool(
    *,
    repository_getter: Callable[[], Repository | None],
    claims_getter: Callable[[], TokenClaims],
    since: str,
    instrument: str | None,
    strategy: str | None,
    exchange: str | None,
    wallet_public_id: str | None,
    limit: int,
) -> CallToolResult:
    """Run the recent-signals MCP read path."""
    access = _get_tool_access_or_envelope(
        claims_getter=claims_getter,
        repository_getter=repository_getter,
        permission=Permission.READ_SIGNALS,
    )
    if isinstance(access, CallToolResult):
        return access
    if limit < 0:
        return _invalid_argument_result("limit must be a non-negative integer.", {"limit": limit})
    if not since:
        return _invalid_argument_result(
            "since is required (ISO 8601 UTC timestamp).",
            {"since": since},
        )
    try:
        parsed_since = _parse_iso8601_utc(since)
    except ValueError as exc:
        return _invalid_argument_result(
            "since must be an ISO 8601 UTC string (e.g. '2026-04-25T10:00:00Z').",
            {"since": since, "error": str(exc)},
        )
    now = datetime.now(UTC)
    wallet_ids, scope_envelope = await _resolve_wallet_ids_or_envelope(
        claims=access.claims,
        repo=access.repo,
        wallet_public_id=wallet_public_id,
        as_of=now,
        error_code="signal_not_found",
        message="No signals found for the given filters.",
    )
    if scope_envelope is not None:
        return scope_envelope
    rows = await access.repo.get_signals(
        since=parsed_since,
        limit=min(limit, _LIST_SIGNALS_LIMIT_CAP),
        as_of=now,
        instrument=instrument,
        strategy=strategy,
        exchange=exchange,
        wallet_public_ids=wallet_ids,
    )
    return to_call_tool_result(
        success=True,
        error_code=None,
        message=f"Returned {len(rows)} matching signals.",
        details=sanitize_output(
            {
                "signals": [_serialize_signal_row(row) for row in rows],
                "count": len(rows),
            }
        ),
    )


def register_mcp_tools(
    mcp_server: FastMCP,
    *,
    repository_getter: Callable[[], Repository | None],
    caps_enforcer_getter: Callable[[], TradingCapsEnforcer | None],
    claims_getter: Callable[[], TokenClaims],
    tracker_getter: Callable[[], SequenceTracker | None] | None = None,
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
        tracker_getter: Optional zero-arg callable returning the shared
            :class:`SequenceTracker` (typically ``app.state.rest_tracker``)
            for write tools that emit sequenced rows. ``None`` falls
            back to a per-call :class:`SequenceTracker` instance — fine
            for MCP write paths because each call has its own envelope
            scope and the existing partial-unique idempotency indices
            prevent cross-call collisions.
    """
    _tracker_getter: Callable[[], SequenceTracker | None] = tracker_getter or (lambda: None)

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
        return await _list_instruments_tool(
            repository_getter=repository_getter,
            claims_getter=claims_getter,
            exchange=exchange,
        )

    @mcp_server.tool()
    async def submit_manual_order(
        exchange: str,
        instrument: str,
        instrument_public_id: str,
        side: str,
        order_type: str,
        quantity: float,
        idempotency_key: str,
        wallet_public_id: str | None = None,
        price: float | None = None,
        stop_price: float | None = None,
        operator_public_id: str | None = None,
        ai_review_public_id: str | None = None,
    ) -> object:
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
            order_type: One of ``market``, ``limit``, ``stop``
                ``stop_limit``.
            quantity: Order size in base asset units.
            idempotency_key: Client-supplied uniqueness key; required
                because MCP clients are the most
                likely source of accidental retries.
            wallet_public_id: Optional UUID7 of the wallet the order
                attaches to. When omitted, the caller must have
                exactly one accessible live wallet.
            price: Limit price. Required for ``limit`` and
                ``stop_limit`` order types.
            stop_price: Trigger price. Required for ``stop`` and
                ``stop_limit`` order types; persisted on the command
                row so the executor submits the trigger to the venue.
            operator_public_id: Optional operator scope. Omit to
                inherit the caller's primary operator.
            ai_review_public_id: Optional UUID7 of the
                ``ai_reviews`` row that
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
                :data:`Permission.CREATE_ORDERS`.
            ValueError: if order params fail the manual-order rule
                (unknown ``order_type``, ``limit``/``stop_limit``
                without ``price``, ``stop``/``stop_limit`` without
                ``stop_price``, ``market`` WITH ``price`` — the field
                is a limit price per this contract, and on the paper
                venue the fill reference is resolved server-side) —
                same evaluator rule as REST 422.
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
            stop_price=stop_price,
            operator_public_id=operator_public_id,
            ai_review_public_id=ai_review_public_id,
        )
        prepared = await _prepare_manual_order(
            repository_getter=repository_getter,
            caps_enforcer_getter=caps_enforcer_getter,
            claims_getter=claims_getter,
            order=order,
            tracker=_tracker_getter() or SequenceTracker(),
        )
        if isinstance(prepared, CallToolResult):
            return prepared
        async with prepared.enforcer.guard(prepared.submission) as caps_guard:
            plan_public_id = await _insert_execution_plan_or_raise_conflict(
                prepared.repo,
                prepared.plan_row,
            )
            cmd_row = _build_manual_order_command_row(
                prepared,
                plan_public_id,
                submitted_notional_usd=caps_guard.submitted_notional_usd,
            )
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
                    prepared.tracker,
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
        """AI delegate decision endpoint for a CONSULT request.

        Wraps :meth:`AiReviewService.submit_decision` for the MCP
        transport. Expects an AI_DELEGATE caller (the role narrows
        the permission set; ``CREATE_ORDERS`` is the explicit gate
        since the decision affects the trade path the strategy is
        awaiting).

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
            :class:`mcp.types.CallToolResult` carrying the canonical
            envelope (``success``, ``error_code``, ``message``,
            ``details``). Idempotent retries (same delegate + same
            decision after a successful resolve) surface as
            ``success=True, error_code="decision_already_recorded"``.

        Raises:
            PermissionError: if the caller lacks
                :data:`Permission.CREATE_ORDERS` (caught by FastMCP
                and surfaced to the client as a tool error). All
                other failure modes flow through the envelope —
                FastMCP NEVER sees an exception for a known
                :class:`AiReviewDecisionResult` outcome.
        """
        return await _submit_ai_review_decision_tool(
            repository_getter=repository_getter,
            claims_getter=claims_getter,
            review_id=review_id,
            decision=decision,
            rationale=rationale,
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
        """List orders with optional filters and pagination.

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
            Canonical envelope. ``details`` carries ``orders``
            (list of ``OrderRow`` dicts) and ``total_count`` (int —
            cardinality of matching rows BEFORE limit/offset).
        """
        return await _list_orders_tool(
            repository_getter=repository_getter,
            claims_getter=claims_getter,
            wallet_public_id=wallet_public_id,
            status=status,
            exchange=exchange,
            instrument=instrument,
            limit=limit,
            offset=offset,
        )

    @mcp_server.tool()
    async def get_order_status(command_public_id: str) -> CallToolResult:
        """Fetch full state of a single order by command_public_id.

        Args:
            command_public_id: UUID7 returned by ``submit_manual_order``
                (or any trade-command insert). The lookup is keyed on
                ``trade_commands.public_id``; the ORDER row is reached
                via the command's scoped ``client_order_id`` (echoed
                onto the orders row by the executor), so plan-less
                strategy commands resolve too.

        Returns:
            Canonical envelope. On success ``details`` carries the
            full ``OrderRow`` plus ``execution_history`` (list of
            ``ExecutionRow`` dicts ordered oldest-first). When the
            command exists but the exchange has not ACK'd a row yet,
            the envelope returns ``status="pending_dispatch"`` with
            ``plan_public_id`` populated from the trade-command row
            (synthetic placeholder). When the command itself is
            unknown OR not in the caller's wallet scope, returns
            ``error_code="order_not_found"`` (anti-enumeration).
        """
        return await _get_order_status_tool(
            repository_getter=repository_getter,
            claims_getter=claims_getter,
            command_public_id=command_public_id,
        )

    @mcp_server.tool()
    async def list_positions(
        wallet_public_id: str | None = None,
        exchange: str | None = None,
        instrument: str | None = None,
    ) -> CallToolResult:
        """List active positions with optional filters.

        Args:
            wallet_public_id: Filter to one wallet. Caller must have
                scope; mismatch returns an empty list with
                ``error_code="position_not_found"`` (anti-enumeration:
                a caller cannot tell whether the wallet exists or
                simply isn't theirs). ``None`` returns positions across
                every wallet the caller can see (ADMIN: every wallet;
                non-admin: every wallet reachable through any operator
                the caller's claims hold membership in).
            exchange: Optional native exchange filter, applied
                post-fetch (``Repository.get_positions`` does not push
                exchange into SQL).
            instrument: Optional native venue symbol filter, applied
                post-fetch.

        Returns:
            Canonical envelope. ``details`` carries ``positions``
            (list of ``PositionRow`` dicts with ISO-8601 ``timestamp``)
            and ``count`` (length of the filtered list).
        """
        return await _list_positions_tool(
            repository_getter=repository_getter,
            claims_getter=claims_getter,
            wallet_public_id=wallet_public_id,
            exchange=exchange,
            instrument=instrument,
        )

    @mcp_server.tool()
    async def list_venue_account_states(
        wallet_public_id: str | None = None,
        exchange: str | None = None,
    ) -> CallToolResult:
        """List truthful venue account and reconciliation states (PnL Phase 4).

        Returns the active per-venue account-truth rows for the
        accessible wallets, each mapped through the fail-closed read
        surface (the EFFECTIVE status is derived over the stored value
        and ``is_authoritative`` is set only when it is exactly
        ``observed``). Loader-detected duplicate active rows corrupt the
        whole account presentation, clear its account payloads, and also
        corrupt its nested ``reconciliation`` object. Otherwise the nested
        object is independently revalidated from its durable lineage.
        Requires ``READ_ACCOUNT_STATE``; AI delegates do NOT hold it by
        default, so a delegate call returns
        ``error_code="permission_denied"``.

        Args:
            wallet_public_id: Filter to one wallet. Caller must have
                scope; a mismatch returns an empty result with
                ``error_code="account_state_not_found"``
                (anti-enumeration: a caller cannot tell whether the
                wallet exists or simply isn't theirs). ``None`` returns
                states across every wallet the caller can see (ADMIN:
                every wallet, an unfiltered scan mirroring
                ``list_positions``; non-admin: every wallet reachable
                through any operator the caller's claims hold membership
                in — a non-admin with no accessible wallet resolves to an
                empty result).
            exchange: Optional native exchange filter, applied post-fetch.

        Returns:
            Canonical envelope. ``details`` carries ``account_states``
            (list of ``PortfolioAccountState`` dicts) and ``count``.
            Consumers must trust each account and reconciliation object's
            ``effective_status`` and ``is_authoritative`` fields, not their
            raw stored statuses.
        """
        return await _list_venue_account_states_tool(
            repository_getter=repository_getter,
            claims_getter=claims_getter,
            wallet_public_id=wallet_public_id,
            exchange=exchange,
        )

    @mcp_server.tool()
    async def get_position_cycle(cycle_public_id: str) -> CallToolResult:
        """Fetch a position cycle by its public_id.

        Position cycles are Snapper's canonical "open→close lifetime"
        record for a position on a given (instrument, exchange, mode,
        wallet) shard. A position can have multiple cycles
        (open→reduce→close→reopen→close); use this tool for the full
        lifecycle audit trail.

        Args:
            cycle_public_id: UUID7 of the position cycle.

        Returns:
            Canonical envelope. On success ``details`` carries the
            full ``PositionCycleRow`` (datetime fields ISO-8601
            stringified). When the cycle is unknown OR not in the
            caller's wallet scope, returns
            ``error_code="position_cycle_not_found"`` (anti-enumeration).
        """
        return await _get_position_cycle_tool(
            repository_getter=repository_getter,
            claims_getter=claims_getter,
            cycle_public_id=cycle_public_id,
        )

    @mcp_server.tool()
    async def get_ai_review_aftermath(review_public_id: str) -> CallToolResult:
        """Return what happened after a terminal AI review was created.

        This tool is read-only. It never reopens or updates the review and
        emits no audit event. It resolves activity by the review's immutable
        wallet and instrument keys over the inclusive ``[created_at, as_of]``
        window, using one temporal anchor for every bitemporal read.

        Args:
            review_public_id: Public identifier of the terminal AI review.

        Returns:
            Canonical envelope whose ``details.aftermath`` contains the full
            terminal review row, window anchors, orders, executions, explicit
            position-cycle transitions, and current position snapshots. An
            unknown or out-of-scope identifier returns ``review_not_found``;
            a pending review returns ``review_not_terminal``.
        """
        return await _get_ai_review_aftermath_tool(
            repository_getter=repository_getter,
            claims_getter=claims_getter,
            review_public_id=review_public_id,
        )

    @mcp_server.tool()
    async def cancel_order(
        plan_public_id: str,
        idempotency_key: str,
    ) -> CallToolResult:
        """Cancel an active execution plan.

        WRITE operation. Wraps :class:`PlansCancelService` for the MCP
        transport so callers reuse the same cancel-by-plan-public-id
        algorithm REST goes through, without HTTP/CSRF coupling. Same
        ``idempotency_key`` on retries returns the current plan state
        without re-executing the cancel; a different key targeting a
        plan that has already claimed one surfaces as
        ``error_code="idempotency_key_conflict"``.

        Args:
            plan_public_id: UUID7 of the execution plan. NOT the
                command_public_id — execution plans hold the cancel
                lifecycle state. Multi-child plans (bracket orders,
                trailing stops) cancel their entire plan tree
                atomically through the existing executor flow.
            idempotency_key: Caller-generated dedup key. Generate ONCE
                per cancel intent; reuse unchanged on retries (UUID4/7
                convention). Must be non-empty.

        Returns:
            Canonical envelope. On success ``details`` carries the
            updated ``ExecutionPlanRow`` (datetime fields ISO-8601
            stringified). Failure paths surface as one of:
            ``order_not_found`` (plan missing OR caller out of scope —
            anti-enumeration collapse), ``already_terminal`` (plan in a
            terminal status), ``cancel_in_progress`` (plan already in
            ``cancel_requested`` with a different key),
            ``idempotency_key_conflict`` (different key claimed first),
            ``caps_violation``, or ``service_unavailable`` (lifespan
            not ready / cancel command emit failed).
        """
        return await _cancel_order_tool(
            repository_getter=repository_getter,
            caps_enforcer_getter=caps_enforcer_getter,
            claims_getter=claims_getter,
            tracker_getter=_tracker_getter,
            plan_public_id=plan_public_id,
            idempotency_key=idempotency_key,
        )

    @mcp_server.tool()
    async def get_ohlcv(
        exchange: str,
        instrument: str,
        timeframe: str,
        since: str | None = None,
        until: str | None = None,
        limit: int = 200,
    ) -> CallToolResult:
        """Fetch OHLCV candles for a venue + instrument.

        Two query modes:

        - **Range** — both ``since`` and ``until`` supplied. Returns
          every candle whose ``open_at`` falls inside the closed
          window, in ASC chronological order, up to ``limit``.
        - **Latest-as-of** — both ``since`` and ``until`` ``None``.
          Returns the most recent ``limit`` candles in DESC order.

        Mixed modes (only one of ``since`` / ``until`` supplied) are
        rejected with ``error_code="invalid_argument"`` because the
        repository's range scan demands both bounds.

        Args:
            exchange: Canonical exchange name (e.g. ``kraken``,
                ``kraken_equities``).
            instrument: Native venue symbol (e.g. ``BTC-USD``).
            timeframe: One of ``"1m"``, ``"5m"``, ``"15m"``, ``"1h"``,
                ``"4h"``, ``"1d"``. Other values surface as
                ``invalid_argument``.
            since: Optional ISO 8601 UTC timestamp string. Bare Unix
                integer strings (e.g. ``"1745000000"``) are rejected.
            until: Optional ISO 8601 UTC timestamp string. If
                supplied, ``since`` must also be supplied.
            limit: Maximum candles. Default 200, clamped to
                ``_OHLCV_LIMIT_CAP`` (1000). Negative values surface
                as ``invalid_argument``.

        Returns:
            Canonical envelope. ``details`` carries ``candles``
            (list of ``[open_at_iso, open, high, low, close, volume]``
            tuples) and ``mode`` (``"range"`` / ``"latest_as_of"``).
            Market-data is public — ``READ_MARKET_DATA`` is the only
            permission gate; no wallet scope filter applies.
        """
        return await _get_ohlcv_tool(
            repository_getter=repository_getter,
            claims_getter=claims_getter,
            exchange=exchange,
            instrument=instrument,
            timeframe=timeframe,
            since=since,
            until=until,
            limit=limit,
        )

    @mcp_server.tool()
    async def list_recent_signals(
        since: str,
        instrument: str | None = None,
        strategy: str | None = None,
        exchange: str | None = None,
        wallet_public_id: str | None = None,
        limit: int = 50,
    ) -> CallToolResult:
        """List recent strategy signals fired after a watermark.

        Args:
            since: REQUIRED ISO 8601 UTC timestamp string. Filters to
                signals fired strictly after this watermark. Bare
                Unix integer strings rejected. Recommended ≤24h
                window for tractable response sizes.
            instrument: Optional native venue symbol filter.
            strategy: Optional strategy-name filter.
            exchange: Optional canonical exchange filter.
            wallet_public_id: Optional wallet filter. Caller must
                have scope; mismatch returns the structured
                ``signal_not_found`` envelope (anti-enumeration —
                callers cannot tell whether the wallet exists or
                simply isn't theirs). ``None`` returns signals across
                every wallet the caller can see.
            limit: Maximum rows. Default 50, clamped to
                ``_LIST_SIGNALS_LIMIT_CAP`` (200). Negative values
                surface as ``invalid_argument``.

        Returns:
            Canonical envelope. ``details`` carries ``signals``
            (list of ``SignalRow`` dicts ordered by ``fired_at``
            descending) and ``count`` (length of the returned list).
        """
        return await _list_recent_signals_tool(
            repository_getter=repository_getter,
            claims_getter=claims_getter,
            since=since,
            instrument=instrument,
            strategy=strategy,
            exchange=exchange,
            wallet_public_id=wallet_public_id,
            limit=limit,
        )
