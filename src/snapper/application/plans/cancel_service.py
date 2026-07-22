"""Cancel service for plan-based cancellation.

Domain-level cancel facade used by both REST and MCP transports.
Raises specific domain exceptions instead of :class:`HTTPException`;
each caller maps those to its surface contract (REST 4xx, MCP envelope).

Idempotency: :meth:`PlansCancelService.cancel_by_plan_public_id` accepts
an optional caller-supplied ``idempotency_key`` that is persisted on the
new SCD2 row via :meth:`Repository.claim_execution_plan_cancel` — a
CAS-style claim that holds a row-level lock for the duration of the
precondition check + close-and-insert so two concurrent callers with
different keys cannot both succeed. A second call with the SAME key
returns the current plan state without re-executing the cancel; a call
with a DIFFERENT key surfaces as
:class:`PlanCancelIdempotencyKeyMismatchError` regardless of the plan's
current status (key-mismatch precedence beats terminal). The
partial-unique index ``uq_ep_active_cancel_idempotency_key`` on
``(operator_public_id, cancel_idempotency_key)`` additionally prevents
the same key from being claimed across two distinct plans
concurrently — caught as :class:`IntegrityError` and reclassified.

Source-surface attribution: the ``source_surface`` parameter (REST
threads ``"rest"``, MCP defaults to ``"mcp"``) is stamped on both the
caps submission and the inserted cancel ``TradeCommand`` so audit
downstream can attribute REST-initiated and MCP-initiated cancels
distinctly.

Sequence-stream choice: the cancel SCD2 transition + the cancel
``TradeCommand`` are stamped from the ``service.cancel`` stream
(``_CANCEL_STREAM``) regardless of caller — REST callers' response
envelopes still use their REST stream via the route's response
builder, but the venue-side audit trail is uniform across transports
because cancels are a distinct stream class (deliberate unification —
was per-transport in the legacy REST code).
"""

import datetime as dt
from datetime import UTC
from datetime import datetime
from typing import cast

from loguru import logger
from sqlalchemy.exc import IntegrityError

from snapper.application.plans.params import core_order_type_from_plan_params
from snapper.application.trade.caps_enforcer import CapsViolationError
from snapper.application.trade.caps_enforcer import TradingCapsEnforcer
from snapper.application.trade.submission import TradeCommandSubmission
from snapper.auth.domain.permissions import Permission
from snapper.auth.domain.permissions import role_grants_permission
from snapper.auth.schemas.principal import AuthPrincipal
from snapper.core.json_types import JsonObject
from snapper.core.types import TradeCommandStatusEnum
from snapper.data.repository import Repository
from snapper.data.repository_types import CancelClaimResult
from snapper.data.repository_types import ExecutionPlanRow
from snapper.data.repository_types import TradeCommandInsertRow
from snapper.messaging.infrastructure.publisher import SequenceTracker

_TERMINAL_STATUSES: frozenset[str] = frozenset({"completed", "cancelled", "failed", "expired"})
_CANCEL_REQUESTED_STATUS = "cancel_requested"
_CANCEL_STREAM = "service.cancel"
_CANCEL_IDEMPOTENCY_KEY_INDEX = "uq_ep_active_cancel_idempotency_key"
_CANCEL_IDEMPOTENCY_KEY_COLUMN = "cancel_idempotency_key"
_SQLITE_CONSTRAINT_UNIQUE_EXTCODE = 2067


def _is_cancel_idempotency_key_unique_violation(exc: IntegrityError) -> bool:
    """Return ``True`` when ``exc`` is a unique-violation on the cancel key index.

    Both PostgreSQL and SQLite surface unique-constraint violations
    with stable error codes; distinguishing the offending index
    requires inspecting the driver-specific error payload:

    - PG (asyncpg / psycopg3): ``orig.constraint_name`` (or
      ``orig.diag.constraint_name``) matches
      ``uq_ep_active_cancel_idempotency_key``.
    - SQLite (aiosqlite): ``orig.sqlite_errorcode == 2067`` AND the
      formatted error mentions the ``cancel_idempotency_key`` column.
      SQLite's unique-violation message lists the offending columns
      rather than the index name, so we match on the column instead.

    Errs on the conservative side: returns ``False`` for any other
    IntegrityError so the caller falls through to
    :class:`PlanConcurrentChangeError` (retryable) rather than
    declaring a permanent ``idempotency_key_conflict``.
    """
    orig = exc.orig
    if orig is None:
        return False
    constraint_name = getattr(orig, "constraint_name", None)
    if constraint_name == _CANCEL_IDEMPOTENCY_KEY_INDEX:
        return True
    diag = getattr(orig, "diag", None)
    if diag is not None and getattr(diag, "constraint_name", None) == _CANCEL_IDEMPOTENCY_KEY_INDEX:
        return True
    sqlite_errorcode = getattr(orig, "sqlite_errorcode", None)
    if sqlite_errorcode == _SQLITE_CONSTRAINT_UNIQUE_EXTCODE:
        return _CANCEL_IDEMPOTENCY_KEY_COLUMN in str(exc)
    return _CANCEL_IDEMPOTENCY_KEY_INDEX in str(exc)


class PlanCancelError(Exception):
    """Base class for cancel-service domain errors."""


class PlanNotFoundError(PlanCancelError):
    """Plan with the supplied public_id does not exist at as_of=now."""

    def __init__(self, plan_public_id: str) -> None:
        """Build the error with the missing plan's public_id."""
        super().__init__(f"Execution plan not found: {plan_public_id!r}")
        self.plan_public_id = plan_public_id


class PlanScopeError(PlanCancelError):
    """Caller does not have wallet scope for the plan.

    REST callers map this to HTTP 403; MCP callers collapse it to the
    entity-specific not-found code (``order_not_found``) per the project
    anti-enumeration rule so callers cannot distinguish "wallet not
    yours" from "wallet does not exist."
    """

    def __init__(self, plan_public_id: str) -> None:
        """Build the error with the out-of-scope plan's public_id."""
        super().__init__(f"Caller is not in scope for plan {plan_public_id!r}")
        self.plan_public_id = plan_public_id


class PlanAlreadyTerminalError(PlanCancelError):
    """Plan is in a terminal status (``completed`` / ``cancelled`` / ``failed`` / ``expired``)."""

    def __init__(self, plan_public_id: str, status: str) -> None:
        """Build the error with the terminal plan's public_id and current status."""
        super().__init__(f"Plan {plan_public_id!r} is already in terminal status {status!r}")
        self.plan_public_id = plan_public_id
        self.status = status


class PlanCancelInProgressError(PlanCancelError):
    """Plan is already in ``cancel_requested`` status with no matching idempotency key.

    Raised when the caller did not supply an ``idempotency_key`` that
    matches the one persisted on the active row. Distinct from
    :class:`PlanCancelIdempotencyKeyMismatchError` so the surface can
    decide whether to expose the difference.
    """

    def __init__(self, plan_public_id: str) -> None:
        """Build the error for a plan whose cancel is already in flight."""
        super().__init__(f"Plan {plan_public_id!r} cancel already in progress")
        self.plan_public_id = plan_public_id


class PlanCancelIdempotencyKeyMismatchError(PlanCancelError):
    """Plan has a ``cancel_idempotency_key`` different from the supplied one."""

    def __init__(self, plan_public_id: str) -> None:
        """Build the error for a key-conflict on the cancel transition."""
        super().__init__(
            f"Plan {plan_public_id!r} cancel_idempotency_key does not match supplied key"
        )
        self.plan_public_id = plan_public_id


class PlanConcurrentChangeError(PlanCancelError):
    """SCD2 update lost the race against a concurrent status transition."""

    def __init__(self, plan_public_id: str) -> None:
        """Build the error for a CAS race on the SCD2 transition."""
        super().__init__(f"Plan {plan_public_id!r} status changed concurrently")
        self.plan_public_id = plan_public_id


class PlanCancelEmitError(PlanCancelError):
    """Cancel TradeCommand insert failed; plan compensated to ``failed``."""

    def __init__(self, plan_public_id: str, cause: BaseException) -> None:
        """Build the error wrapping the underlying insert failure cause."""
        super().__init__(f"Failed to emit cancel command for plan {plan_public_id!r}: {cause}")
        self.plan_public_id = plan_public_id
        self.__cause__ = cause


class PlanPostCancelReloadError(PlanCancelEmitError):
    """Plan disappeared between the cancel transition and the post-cancel reload.

    Inherits from :class:`PlanCancelEmitError` so existing callers
    (e.g. MCP) that catch the parent continue to handle this race
    without code change. REST distinguishes it to preserve the legacy
    ``"Plan updated but not found"`` 500 detail string.
    """

    def __init__(self, plan_public_id: str) -> None:
        """Build the error for the rare reload-after-transition disappearance."""
        super().__init__(
            plan_public_id,
            RuntimeError("Plan disappeared after cancel transition"),
        )
        self.plan_public_id = plan_public_id


def _json_str_param(params: JsonObject, key: str) -> str | None:
    """Return a string JSON param or ``None`` when absent or not a string."""
    value = params.get(key)
    if isinstance(value, str):
        return value
    return None


def _build_cancel_submission(
    plan: ExecutionPlanRow,
    principal: AuthPrincipal,
    *,
    source_surface: str,
) -> TradeCommandSubmission:
    """Build the caps-enforcer submission for the cancel action.

    Args:
        plan: SCD2-active plan row being cancelled.
        principal: Authenticated caller used for caps attribution.
        source_surface: Origin transport (``"mcp"`` or ``"rest"``)
            stamped on the submission so caps audit can attribute the
            attempt distinctly.
    """
    order_type = core_order_type_from_plan_params(plan["params"])
    return TradeCommandSubmission(
        user_public_id=principal.user_public_id,
        operator_public_id=plan["operator_public_id"],
        wallet_public_id=plan["wallet_public_id"],
        instrument_public_id=plan["instrument_public_id"],
        command_type="cancel",
        side=plan["side"],
        order_type=order_type,
        quantity=None,
        price=None,
        source_surface=source_surface,
        idempotency_key=None,
    )


def _build_cancel_trade_command(
    *,
    plan: ExecutionPlanRow,
    principal: AuthPrincipal,
    child_client_order_id: str,
    native_instrument: str,
    exchange_order_id: str | None,
    bus_time: datetime,
    now: datetime,
    session_id: str,
    sequence_id: int,
    source_surface: str,
) -> TradeCommandInsertRow:
    """Build the venue-facing cancel TradeCommand for a child order.

    Args:
        plan: Plan being cancelled.
        principal: Authenticated caller used for ``user_public_id``
            attribution.
        child_client_order_id: Child order's client ID.
        native_instrument: Venue-native instrument identifier.
        exchange_order_id: Venue-assigned exchange order ID, or
            ``None`` when the venue has not yet ACK'd the original
            order.
        bus_time: Bus-time UTC timestamp for the SCD2 row.
        now: Wall-clock UTC for ``created_at``.
        session_id: Provenance session for the cancel command.
        sequence_id: Provenance sequence for the cancel command.
        source_surface: Origin transport (``"mcp"`` or ``"rest"``)
            stamped on the command so audit can attribute the cancel
            distinctly. MCP and REST callers thread their own value
            through; the service does not assume a default.
    """
    params = plan["params"]
    return TradeCommandInsertRow(
        command_type="cancel",
        shard_key=plan["shard_key"],
        exchange=plan["exchange"],
        instrument=native_instrument,
        mode=plan["mode"],
        strategy_id="manual",
        client_order_id=child_client_order_id,
        venue_client_id=child_client_order_id,
        side=plan["side"],
        order_type=core_order_type_from_plan_params(params),
        quantity=plan["total_quantity"],
        price=cast(float | None, params.get("price")),
        leverage=cast(int | None, params.get("leverage")),
        reduce_only=False,
        status=TradeCommandStatusEnum.CREATED,
        created_at=now,
        correlation_id=plan["public_id"],
        session_id=session_id,
        sequence_id=sequence_id,
        timestamp=bus_time,
        wallet_public_id=plan["wallet_public_id"],
        operator_public_id=plan["operator_public_id"],
        user_public_id=principal.user_public_id or principal.username,
        plan_public_id=plan["public_id"],
        exchange_order_id=exchange_order_id,
        source_surface=source_surface,
    )


async def _enforce_scope(
    *,
    repo: Repository,
    principal: AuthPrincipal,
    plan: ExecutionPlanRow,
    as_of: datetime,
) -> None:
    """Raise :class:`PlanScopeError` when the caller cannot reach the plan's wallet."""
    if role_grants_permission(principal.role, Permission.IMPERSONATE_OPERATOR):
        return
    accessible = await repo.list_accessible_wallets_for_operators(
        list(principal.operator_public_ids), as_of
    )
    if plan["wallet_public_id"] not in {row["public_id"] for row in accessible}:
        raise PlanScopeError(plan["public_id"])


def _check_idempotency_replay(
    *,
    plan: ExecutionPlanRow,
    idempotency_key: str | None,
) -> bool:
    """Return ``True`` when the supplied key marks an idempotent replay.

    A replay match short-circuits the cancel: the caller receives the
    current plan state without re-executing the cancel transition and
    without raising ``PlanAlreadyTerminalError`` on terminal plans.
    """
    if idempotency_key is None:
        return False
    existing = plan.get("cancel_idempotency_key")
    return existing is not None and existing == idempotency_key


def _enforce_idempotency_key_consistency(
    *,
    plan: ExecutionPlanRow,
    idempotency_key: str | None,
) -> None:
    """Raise :class:`PlanCancelIdempotencyKeyMismatchError` on conflicting keys.

    Called BEFORE terminal/cancel-in-progress checks so the caller gets
    the most informative error code: a different supplied key on a
    plan that already claimed one is a contract violation regardless of
    the plan's current status.
    """
    if idempotency_key is None:
        return
    existing = plan.get("cancel_idempotency_key")
    if existing is not None and existing != idempotency_key:
        raise PlanCancelIdempotencyKeyMismatchError(plan["public_id"])


class PlansCancelService:
    """Domain-level cancel facade for plan-based cancellation."""

    @staticmethod
    async def cancel_by_plan_public_id(
        *,
        plan_public_id: str,
        idempotency_key: str | None,
        principal: AuthPrincipal,
        repo: Repository,
        tracker: SequenceTracker,
        caps_enforcer: TradingCapsEnforcer,
        source_surface: str = "mcp",
    ) -> ExecutionPlanRow:
        """Cancel a plan and return the updated row.

        Args:
            plan_public_id: UUID7 of the execution plan to cancel.
            idempotency_key: Caller-supplied dedup key. ``None`` skips
                the idempotency claim entirely (legacy REST path);
                non-``None`` is persisted on the new SCD2 row, and
                replays the current state on subsequent same-key calls.
            principal: Authenticated caller used for wallet-scope check
                and cancel-command provenance.
            repo: Repository dependency.
            tracker: Per-component :class:`SequenceTracker` for the
                cancel-command + status-transition session/sequence
                stamping.
            caps_enforcer: Per-user :class:`TradingCapsEnforcer` that
                gates the cancel-command insert against user caps.
            source_surface: Origin transport stamped on the cancel
                ``TradeCommand`` + caps submission. Defaults to
                ``"mcp"`` for the historical caller; the REST route
                threads ``"rest"`` so audit attribution stays
                surface-distinct.

        Returns:
            Updated :class:`ExecutionPlanRow` after the SCD2 cancel
            transition. On idempotent replay, the current row is
            returned unchanged.

        Raises:
            PlanNotFoundError: plan not present.
            PlanScopeError: caller out of wallet scope.
            PlanAlreadyTerminalError: plan terminal AND no idempotent replay.
            PlanCancelInProgressError: plan in ``cancel_requested`` AND no replay.
            PlanCancelIdempotencyKeyMismatchError: conflicting key.
            PlanConcurrentChangeError: SCD2 race.
            CapsViolationError: caps rejected the emit.
            PlanCancelEmitError: child cancel command insert failed.
        """
        now = datetime.now(UTC)
        plan = await repo.get_execution_plan(plan_public_id, as_of=now)
        if plan is None:
            raise PlanNotFoundError(plan_public_id)
        await _enforce_scope(repo=repo, principal=principal, plan=plan, as_of=now)
        if _check_idempotency_replay(plan=plan, idempotency_key=idempotency_key):
            return plan
        _enforce_idempotency_key_consistency(plan=plan, idempotency_key=idempotency_key)
        if plan["status"] in _TERMINAL_STATUSES:
            raise PlanAlreadyTerminalError(plan["public_id"], plan["status"])
        if plan["status"] == _CANCEL_REQUESTED_STATUS:
            raise PlanCancelInProgressError(plan["public_id"])
        return await PlansCancelService._execute_cancel(
            plan=plan,
            idempotency_key=idempotency_key,
            principal=principal,
            repo=repo,
            tracker=tracker,
            caps_enforcer=caps_enforcer,
            now=now,
            source_surface=source_surface,
        )

    @staticmethod
    async def _execute_cancel(
        *,
        plan: ExecutionPlanRow,
        idempotency_key: str | None,
        principal: AuthPrincipal,
        repo: Repository,
        tracker: SequenceTracker,
        caps_enforcer: TradingCapsEnforcer,
        now: datetime,
        source_surface: str,
    ) -> ExecutionPlanRow:
        """Run the SCD2 transition + (conditional) cancel command emit."""
        params = plan["params"]
        child_client_order_id = _json_str_param(params, "child_client_order_id")
        native_instrument = _json_str_param(params, "native_instrument")
        bus_time = dt.datetime.now(dt.UTC)
        session_id = tracker.session_id
        plan_public_id = plan["public_id"]
        if child_client_order_id is not None and native_instrument is not None:
            exchange_order_id = await repo.get_exchange_order_id_for_client_order_id(
                child_client_order_id, as_of=now
            )
            try:
                submission = _build_cancel_submission(
                    plan, principal, source_surface=source_surface
                )
                async with caps_enforcer.guard(submission):
                    replay = await PlansCancelService._claim_cancel_transition(
                        repo=repo,
                        plan_public_id=plan_public_id,
                        bus_time=bus_time,
                        session_id=session_id,
                        tracker=tracker,
                        cancel_requested_at=now,
                        idempotency_key=idempotency_key,
                    )
                    if replay is not None:
                        return replay
                    cancel_cmd = _build_cancel_trade_command(
                        plan=plan,
                        principal=principal,
                        child_client_order_id=child_client_order_id,
                        native_instrument=native_instrument,
                        exchange_order_id=exchange_order_id,
                        bus_time=bus_time,
                        now=now,
                        session_id=session_id,
                        sequence_id=tracker.next_sequence(_CANCEL_STREAM),
                        source_surface=source_surface,
                    )
                    try:
                        await repo.insert_trade_command(cancel_cmd, ownership=None)
                    except Exception as exc:
                        await PlansCancelService._compensate_failed_cancel(
                            repo=repo,
                            plan_public_id=plan_public_id,
                            bus_time=bus_time,
                            session_id=session_id,
                            tracker=tracker,
                            exc=exc,
                        )
                        raise PlanCancelEmitError(plan_public_id, exc) from exc
            except CapsViolationError:
                replay_on_caps = await PlansCancelService._reclassify_post_caps_rejection(
                    repo=repo,
                    plan_public_id=plan_public_id,
                    idempotency_key=idempotency_key,
                )
                if replay_on_caps is not None:
                    return replay_on_caps
                raise
        else:
            replay = await PlansCancelService._claim_cancel_transition(
                repo=repo,
                plan_public_id=plan_public_id,
                bus_time=bus_time,
                session_id=session_id,
                tracker=tracker,
                cancel_requested_at=now,
                idempotency_key=idempotency_key,
            )
            if replay is not None:
                return replay
        updated = await repo.get_execution_plan(plan_public_id, as_of=bus_time)
        if updated is None:
            raise PlanPostCancelReloadError(plan_public_id)
        return updated

    @staticmethod
    async def _claim_cancel_transition(
        *,
        repo: Repository,
        plan_public_id: str,
        bus_time: datetime,
        session_id: str,
        tracker: SequenceTracker,
        cancel_requested_at: datetime,
        idempotency_key: str | None,
    ) -> ExecutionPlanRow | None:
        """Run the CAS-style cancel claim and translate outcomes.

        Delegates to :meth:`Repository.claim_execution_plan_cancel`
        which holds a row-level lock for the duration of the
        precondition check + SCD2 close-and-insert. This closes the
        same-plan-different-key race that a blind
        :meth:`Repository.update_execution_plan_status` (used by REST)
        cannot prevent — caller B's transition cannot overwrite caller
        A's just-claimed row because B's pre-condition check sees A's
        key already persisted under the FOR UPDATE lock.

        Returns ``None`` when the caller is the CAS winner and should
        proceed to emit the cancel command; returns the existing plan
        row when the outcome is an idempotent replay (caller
        short-circuits without emitting another cancel command).

        Maps CAS outcomes to domain exceptions where appropriate:

        - ``not_found`` → :class:`PlanConcurrentChangeError` (the
          active row disappeared between :meth:`get_execution_plan`
          and the CAS — extremely rare; retry is the right answer).
        - ``key_mismatch`` → :class:`PlanCancelIdempotencyKeyMismatchError`.
        - ``terminal`` → :class:`PlanAlreadyTerminalError`.
        - ``in_progress`` → :class:`PlanCancelInProgressError`.

        Cross-plan key reuse on ``(operator_public_id, cancel_idempotency_key)``
        still surfaces as :class:`IntegrityError` from the partial-unique
        index — caught here, classified via
        :meth:`_reclassify_race_loser` so the caller still receives
        :class:`PlanCancelIdempotencyKeyMismatchError` rather than a
        raw exception.
        """
        try:
            result = await repo.claim_execution_plan_cancel(
                public_id=plan_public_id,
                idempotency_key=idempotency_key,
                bus_time=bus_time,
                session_id=session_id,
                sequence_id=tracker.next_sequence(_CANCEL_STREAM),
                cancel_requested_at=cancel_requested_at,
            )
        except IntegrityError as exc:
            replay = await PlansCancelService._reclassify_race_loser(
                repo=repo,
                plan_public_id=plan_public_id,
                idempotency_key=idempotency_key,
                cause=exc,
            )
            if replay is not None:
                return replay
            raise PlanConcurrentChangeError(plan_public_id) from exc
        return PlansCancelService._dispatch_cancel_claim(
            plan_public_id=plan_public_id, result=result
        )

    @staticmethod
    def _dispatch_cancel_claim(
        *, plan_public_id: str, result: CancelClaimResult
    ) -> ExecutionPlanRow | None:
        """Map a :class:`CancelClaimResult` to the service's contract."""
        outcome = result["outcome"]
        plan = result["plan"]
        if outcome == "claimed":
            return None
        if outcome == "replay":
            assert plan is not None
            return plan
        if outcome == "key_mismatch":
            raise PlanCancelIdempotencyKeyMismatchError(plan_public_id)
        if outcome == "terminal":
            assert plan is not None
            raise PlanAlreadyTerminalError(plan_public_id, plan["status"])
        if outcome == "in_progress":
            raise PlanCancelInProgressError(plan_public_id)
        raise PlanConcurrentChangeError(plan_public_id)

    @staticmethod
    async def _reclassify_race_loser(
        *,
        repo: Repository,
        plan_public_id: str,
        idempotency_key: str | None,
        cause: IntegrityError,
    ) -> ExecutionPlanRow | None:
        """Reload the plan post-race and decide the loser's outcome.

        Returns the post-race :class:`ExecutionPlanRow` when the
        caller's outcome is an idempotent replay (same key persisted on
        either an active ``cancel_requested`` or a terminal row).
        Returns ``None`` and lets the outer call raise
        :class:`PlanConcurrentChangeError` when no specific contract
        applies. Raises a precise domain error otherwise.

        Cross-plan key reuse (post-reload plan still has no key but the
        caller passed one) is reported as
        :class:`PlanCancelIdempotencyKeyMismatchError` ONLY when the
        ``IntegrityError`` is the partial-unique
        ``uq_ep_active_cancel_idempotency_key`` violation; other
        integrity errors (transient deadlocks, foreign-key checks)
        fall through to :class:`PlanConcurrentChangeError` so callers
        can retry without declaring their key permanently unusable.

        Reload uses ``datetime.now(UTC)`` rather than the loser's
        ``bus_time`` because the winner's SCD2 row has a strictly later
        ``timestamp``; querying with the stale ``bus_time`` would still
        see the OLD active row and the reclassifier would mis-route as
        a generic concurrent change.
        """
        as_of = datetime.now(UTC)
        latest = await repo.get_execution_plan(plan_public_id, as_of=as_of)
        if latest is None:
            return None
        existing_key = latest.get("cancel_idempotency_key")
        replay_match = (
            idempotency_key is not None
            and existing_key is not None
            and existing_key == idempotency_key
        )
        if replay_match:
            return latest
        if (
            idempotency_key is not None
            and existing_key is not None
            and existing_key != idempotency_key
        ):
            raise PlanCancelIdempotencyKeyMismatchError(plan_public_id)
        if latest["status"] in _TERMINAL_STATUSES:
            raise PlanAlreadyTerminalError(plan_public_id, latest["status"])
        if latest["status"] == _CANCEL_REQUESTED_STATUS:
            raise PlanCancelInProgressError(plan_public_id)
        if (
            idempotency_key is not None
            and existing_key is None
            and _is_cancel_idempotency_key_unique_violation(cause)
        ):
            raise PlanCancelIdempotencyKeyMismatchError(plan_public_id)
        return None

    @staticmethod
    async def _reclassify_post_caps_rejection(
        *,
        repo: Repository,
        plan_public_id: str,
        idempotency_key: str | None,
    ) -> ExecutionPlanRow | None:
        """Reclassify a caps rejection against the post-rejection plan state.

        Mirrors the priority order of :meth:`_reclassify_race_loser`
        (minus the unique-violation check, which is irrelevant after a
        caps gate rejection): same key → return row (replay success);
        different existing key → ``IdempotencyKeyMismatchError``;
        terminal status → ``AlreadyTerminalError``;
        ``cancel_requested`` → ``InProgressError``; otherwise return
        ``None`` so the caller re-raises the original
        :class:`CapsViolationError`.

        Without this step a slow caller hitting the
        ``max_cancels_per_minute`` cap behind a concurrent winner would
        surface ``caps_violation`` even when the actual contract
        outcome is ``idempotency_key_conflict`` /
        ``already_terminal`` / ``cancel_in_progress`` — i.e., the
        cancel state already moved past their attempt.
        """
        latest = await repo.get_execution_plan(plan_public_id, as_of=datetime.now(UTC))
        if latest is None:
            return None
        existing_key = latest.get("cancel_idempotency_key")
        if (
            idempotency_key is not None
            and existing_key is not None
            and existing_key == idempotency_key
        ):
            return latest
        if (
            idempotency_key is not None
            and existing_key is not None
            and existing_key != idempotency_key
        ):
            raise PlanCancelIdempotencyKeyMismatchError(plan_public_id)
        if latest["status"] in _TERMINAL_STATUSES:
            raise PlanAlreadyTerminalError(plan_public_id, latest["status"])
        if latest["status"] == _CANCEL_REQUESTED_STATUS:
            raise PlanCancelInProgressError(plan_public_id)
        return None

    @staticmethod
    async def _compensate_failed_cancel(
        *,
        repo: Repository,
        plan_public_id: str,
        bus_time: datetime,
        session_id: str,
        tracker: SequenceTracker,
        exc: BaseException,
    ) -> None:
        """Mark plan ``failed`` after a cancel-command insert error.

        Best-effort compensation: a secondary failure here is logged but
        does not mask the original cause; PlanExecutorService recovery
        re-emits the cancel on restart per existing semantics.
        """
        logger.error("Failed to insert cancel command for plan {}: {}", plan_public_id, exc)
        try:
            await repo.update_execution_plan_status(
                public_id=plan_public_id,
                new_status="failed",
                bus_time=bus_time,
                session_id=session_id,
                sequence_id=tracker.next_sequence(_CANCEL_STREAM),
                last_error=f"Cancel command insert failed: {exc}",
            )
        except Exception as comp_exc:
            logger.error(
                "Failed to compensate plan {} to failed after cancel insert "
                "error: {}; PlanExecutorService recovery re-emits the cancel "
                "on restart",
                plan_public_id,
                comp_exc,
            )
