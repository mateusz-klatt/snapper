"""Tests for :class:`PlansCancelService`.

The service is the domain-level cancel-by-plan-public-id facade that
the MCP ``cancel_order`` tool calls. REST keeps the legacy helper
in :mod:`snapper.server.order_routes` untouched (deferred REST
refactor); these tests exercise the service in isolation against an
in-memory mock repository.
"""

import asyncio
import datetime as dt
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC
from datetime import datetime
from typing import Any
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import pytest
from sqlalchemy.exc import IntegrityError

from snapper.application.plans.cancel_service import PlanAlreadyTerminalError
from snapper.application.plans.cancel_service import PlanCancelEmitError
from snapper.application.plans.cancel_service import PlanCancelIdempotencyKeyMismatchError
from snapper.application.plans.cancel_service import PlanCancelInProgressError
from snapper.application.plans.cancel_service import PlanConcurrentChangeError
from snapper.application.plans.cancel_service import PlanNotFoundError
from snapper.application.plans.cancel_service import PlansCancelService
from snapper.application.plans.cancel_service import PlanScopeError
from snapper.application.plans.cancel_service import _is_cancel_idempotency_key_unique_violation
from snapper.application.trade.caps_enforcer import CapsViolationError
from snapper.application.trade.caps_enforcer import TradingCapsEnforcer
from snapper.application.trade.submission import TradeCommandSubmission
from snapper.auth.domain.roles import UserRole
from snapper.auth.schemas.principal import AuthPrincipal
from snapper.messaging.infrastructure.publisher import SequenceTracker


def _make_plan(
    *,
    public_id: str = "plan-1",
    status: str = "active",
    cancel_idempotency_key: str | None = None,
    wallet_public_id: str = "wallet-1",
    operator_public_id: str | None = "op-1",
    child_client_order_id: str | None = "cid-1",
    native_instrument: str | None = "BTC-USD",
) -> dict[str, Any]:
    """Build a minimal :class:`ExecutionPlanRow` dict for service tests."""
    now = datetime(2026, 4, 27, tzinfo=UTC)
    params: dict[str, Any] = {}
    if child_client_order_id is not None:
        params["child_client_order_id"] = child_client_order_id
    if native_instrument is not None:
        params["native_instrument"] = native_instrument
    return {
        "public_id": public_id,
        "timestamp": now,
        "session_id": "s",
        "sequence_id": 1,
        "plan_type": "manual_once",
        "created_by_user_id": "user-1",
        "created_by_strategy": None,
        "created_via": "api",
        "instrument_public_id": "inst-1",
        "exchange": "kraken",
        "mode": "live",
        "shard_key": "kraken.BTC-USD.live",
        "wallet_public_id": wallet_public_id,
        "operator_public_id": operator_public_id,
        "total_quantity": 1.0,
        "filled_quantity": 0.0,
        "side": "buy",
        "parent_plan_public_id": None,
        "position_cycle_public_id": None,
        "params": params,
        "status": status,
        "created_at": now,
        "started_at": None,
        "completed_at": None,
        "expires_at": None,
        "cancel_requested_at": None,
        "last_evaluated_at": None,
        "last_error": None,
        "idempotency_key": "create-key-1",
        "cancel_idempotency_key": cancel_idempotency_key,
    }


def _make_principal(
    role: UserRole = UserRole.AI_DELEGATE,
    operator_public_ids: list[str] | None = None,
) -> AuthPrincipal:
    """Build an :class:`AuthPrincipal` for service tests."""
    return AuthPrincipal(
        username="caller",
        role=role,
        user_public_id="user-1",
        operator_public_ids=operator_public_ids if operator_public_ids is not None else ["op-1"],
        primary_operator_public_id="op-1",
    )


def _make_tracker() -> SequenceTracker:
    """Fresh tracker per test (deterministic sequence numbers)."""
    return SequenceTracker()


def _admit_enforcer() -> Any:
    """Build a caps enforcer whose ``guard`` admits the submission."""

    @asynccontextmanager
    async def _admit(submission: TradeCommandSubmission) -> AsyncIterator[None]:
        del submission
        yield None

    enforcer = MagicMock(spec=TradingCapsEnforcer)
    enforcer.guard = _admit
    return enforcer


def _reject_enforcer() -> Any:
    """Build a caps enforcer whose ``guard`` raises :class:`CapsViolationError`."""

    @asynccontextmanager
    async def _reject(submission: TradeCommandSubmission) -> AsyncIterator[None]:
        """Asynccontextmanager body that raises before reaching the yield."""
        del submission
        raise CapsViolationError(
            cap_type="cancel_rate", attempted=11, limit=10, detail="too many cancels"
        )
        yield None

    enforcer = MagicMock(spec=TradingCapsEnforcer)
    enforcer.guard = _reject
    return enforcer


def _build_repo(
    *,
    plan: dict[str, Any] | None,
    accessible_wallets: list[str] | None = None,
    update_returns: int | None = 99,
    exchange_order_id: str | None = "exo-1",
    insert_command_raises: BaseException | None = None,
    claim_outcome: str = "claimed",
    claim_plan: dict[str, Any] | None | str = "_default",
    claim_raises: BaseException | None = None,
) -> Any:
    """Build a mock :class:`Repository` parameterised for the test.

    The cancel-claim CAS method (``claim_execution_plan_cancel``) is
    mocked separately from the legacy SCD2
    ``update_execution_plan_status``: the service path now goes through
    the CAS, but the compensation path on a failed cancel-command
    insert still uses the legacy SCD2 builder.
    """
    repo = AsyncMock()
    repo.get_execution_plan = AsyncMock(return_value=plan)
    repo.list_accessible_wallets_for_operators = AsyncMock(
        return_value=[{"public_id": w} for w in (accessible_wallets or ["wallet-1"])]
    )
    repo.update_execution_plan_status = AsyncMock(return_value=update_returns)
    repo.get_exchange_order_id_for_client_order_id = AsyncMock(return_value=exchange_order_id)
    if insert_command_raises is not None:
        repo.insert_trade_command = AsyncMock(side_effect=insert_command_raises)
    else:
        repo.insert_trade_command = AsyncMock(return_value=(1, "cmd-cancel-1"))
    if claim_raises is not None:
        repo.claim_execution_plan_cancel = AsyncMock(side_effect=claim_raises)
    else:
        if claim_plan == "_default":
            claim_plan_dict: dict[str, Any] | None = (
                _make_plan(status="cancel_requested", cancel_idempotency_key="key-1")
                if plan is not None
                else None
            )
        else:
            claim_plan_dict = claim_plan
        repo.claim_execution_plan_cancel = AsyncMock(
            return_value={"outcome": claim_outcome, "plan": claim_plan_dict}
        )
    return repo


class TestPlansCancelService:
    """Coverage for the cancel-by-plan-public-id flow."""

    @pytest.mark.asyncio
    async def test_happy_path_with_child_command(self) -> None:
        """Active plan with child order → caps guard + CAS claim + cancel emit."""
        plan = _make_plan(status="active")
        cancelled = _make_plan(status="cancel_requested", cancel_idempotency_key="key-1")
        repo = _build_repo(plan=plan, claim_plan=cancelled)
        repo.get_execution_plan.side_effect = [plan, cancelled]
        result = await PlansCancelService.cancel_by_plan_public_id(
            plan_public_id="plan-1",
            idempotency_key="key-1",
            principal=_make_principal(),
            repo=repo,
            tracker=_make_tracker(),
            caps_enforcer=_admit_enforcer(),
        )
        assert result["status"] == "cancel_requested"
        assert result["cancel_idempotency_key"] == "key-1"
        repo.claim_execution_plan_cancel.assert_awaited_once()
        repo.insert_trade_command.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_plan_not_found_raises(self) -> None:
        """Missing plan → :class:`PlanNotFoundError`."""
        repo = _build_repo(plan=None)
        with pytest.raises(PlanNotFoundError):
            await PlansCancelService.cancel_by_plan_public_id(
                plan_public_id="missing",
                idempotency_key="key-1",
                principal=_make_principal(),
                repo=repo,
                tracker=_make_tracker(),
                caps_enforcer=_admit_enforcer(),
            )

    @pytest.mark.asyncio
    async def test_wallet_outside_scope_raises(self) -> None:
        """Plan whose wallet is not accessible → :class:`PlanScopeError`."""
        plan = _make_plan(wallet_public_id="wallet-X")
        repo = _build_repo(plan=plan, accessible_wallets=["wallet-1"])
        with pytest.raises(PlanScopeError):
            await PlansCancelService.cancel_by_plan_public_id(
                plan_public_id="plan-1",
                idempotency_key="key-1",
                principal=_make_principal(),
                repo=repo,
                tracker=_make_tracker(),
                caps_enforcer=_admit_enforcer(),
            )
        repo.update_execution_plan_status.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_admin_bypasses_scope_check(self) -> None:
        """ADMIN principal skips :meth:`list_accessible_wallets_for_operators`."""
        plan = _make_plan(wallet_public_id="wallet-X")
        cancelled = _make_plan(status="cancel_requested", wallet_public_id="wallet-X")
        repo = _build_repo(plan=plan)
        repo.get_execution_plan.side_effect = [plan, cancelled]
        await PlansCancelService.cancel_by_plan_public_id(
            plan_public_id="plan-1",
            idempotency_key="key-1",
            principal=_make_principal(role=UserRole.ADMIN),
            repo=repo,
            tracker=_make_tracker(),
            caps_enforcer=_admit_enforcer(),
        )
        repo.list_accessible_wallets_for_operators.assert_not_called()

    @pytest.mark.asyncio
    async def test_terminal_plan_raises(self) -> None:
        """Terminal plan → :class:`PlanAlreadyTerminalError`."""
        plan = _make_plan(status="cancelled")
        repo = _build_repo(plan=plan)
        with pytest.raises(PlanAlreadyTerminalError) as exc:
            await PlansCancelService.cancel_by_plan_public_id(
                plan_public_id="plan-1",
                idempotency_key="key-new",
                principal=_make_principal(),
                repo=repo,
                tracker=_make_tracker(),
                caps_enforcer=_admit_enforcer(),
            )
        assert exc.value.status == "cancelled"

    @pytest.mark.asyncio
    async def test_idempotency_replay_returns_current_state(self) -> None:
        """Same key + plan already has it → return current row, no re-execute."""
        plan = _make_plan(status="cancel_requested", cancel_idempotency_key="key-1")
        repo = _build_repo(plan=plan)
        result = await PlansCancelService.cancel_by_plan_public_id(
            plan_public_id="plan-1",
            idempotency_key="key-1",
            principal=_make_principal(),
            repo=repo,
            tracker=_make_tracker(),
            caps_enforcer=_admit_enforcer(),
        )
        assert result["public_id"] == plan["public_id"]
        repo.update_execution_plan_status.assert_not_awaited()
        repo.insert_trade_command.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_idempotency_replay_on_terminal_plan_returns_state(self) -> None:
        """Replay key matches even when plan is terminal — caller learns final state."""
        plan = _make_plan(status="cancelled", cancel_idempotency_key="key-1")
        repo = _build_repo(plan=plan)
        result = await PlansCancelService.cancel_by_plan_public_id(
            plan_public_id="plan-1",
            idempotency_key="key-1",
            principal=_make_principal(),
            repo=repo,
            tracker=_make_tracker(),
            caps_enforcer=_admit_enforcer(),
        )
        assert result["status"] == "cancelled"

    @pytest.mark.asyncio
    async def test_different_idempotency_key_raises_mismatch(self) -> None:
        """Plan claimed key A; caller supplies key B → mismatch."""
        plan = _make_plan(status="cancel_requested", cancel_idempotency_key="key-A")
        repo = _build_repo(plan=plan)
        with pytest.raises(PlanCancelIdempotencyKeyMismatchError):
            await PlansCancelService.cancel_by_plan_public_id(
                plan_public_id="plan-1",
                idempotency_key="key-B",
                principal=_make_principal(),
                repo=repo,
                tracker=_make_tracker(),
                caps_enforcer=_admit_enforcer(),
            )

    @pytest.mark.asyncio
    async def test_cancel_in_progress_without_key_raises(self) -> None:
        """Plan in cancel_requested but no key yet → InProgress error."""
        plan = _make_plan(status="cancel_requested", cancel_idempotency_key=None)
        repo = _build_repo(plan=plan)
        with pytest.raises(PlanCancelInProgressError):
            await PlansCancelService.cancel_by_plan_public_id(
                plan_public_id="plan-1",
                idempotency_key="key-1",
                principal=_make_principal(),
                repo=repo,
                tracker=_make_tracker(),
                caps_enforcer=_admit_enforcer(),
            )

    @pytest.mark.asyncio
    async def test_concurrent_change_during_transition(self) -> None:
        """CAS returns ``not_found`` → :class:`PlanConcurrentChangeError`."""
        plan = _make_plan(status="active")
        repo = _build_repo(plan=plan, claim_outcome="not_found", claim_plan=None)
        with pytest.raises(PlanConcurrentChangeError):
            await PlansCancelService.cancel_by_plan_public_id(
                plan_public_id="plan-1",
                idempotency_key="key-1",
                principal=_make_principal(),
                repo=repo,
                tracker=_make_tracker(),
                caps_enforcer=_admit_enforcer(),
            )

    @pytest.mark.asyncio
    async def test_caps_violation_propagates(self) -> None:
        """Caps enforcer rejects → :class:`CapsViolationError` re-raised."""
        plan = _make_plan(status="active")
        repo = _build_repo(plan=plan)
        with pytest.raises(CapsViolationError):
            await PlansCancelService.cancel_by_plan_public_id(
                plan_public_id="plan-1",
                idempotency_key="key-1",
                principal=_make_principal(),
                repo=repo,
                tracker=_make_tracker(),
                caps_enforcer=_reject_enforcer(),
            )

    @pytest.mark.asyncio
    async def test_cancel_command_emit_failure_compensates_to_failed(self) -> None:
        """Insert failure → plan compensated to ``failed`` + :class:`PlanCancelEmitError`."""
        plan = _make_plan(status="active")
        repo = _build_repo(
            plan=plan,
            insert_command_raises=IntegrityError("stmt", {}, Exception("dup-cmd")),
        )
        with pytest.raises(PlanCancelEmitError):
            await PlansCancelService.cancel_by_plan_public_id(
                plan_public_id="plan-1",
                idempotency_key="key-1",
                principal=_make_principal(),
                repo=repo,
                tracker=_make_tracker(),
                caps_enforcer=_admit_enforcer(),
            )
        repo.update_execution_plan_status.assert_awaited_once()
        compensation_kwargs = repo.update_execution_plan_status.await_args.kwargs
        assert compensation_kwargs["new_status"] == "failed"

    @pytest.mark.asyncio
    async def test_compensation_failure_does_not_mask_emit_error(self) -> None:
        """Best-effort compensation: secondary failure logged but original cause raised."""
        plan = _make_plan(status="active")
        repo = _build_repo(
            plan=plan,
            insert_command_raises=IntegrityError("stmt", {}, Exception("dup-cmd")),
        )
        repo.update_execution_plan_status = AsyncMock(side_effect=RuntimeError("compensate failed"))
        with pytest.raises(PlanCancelEmitError):
            await PlansCancelService.cancel_by_plan_public_id(
                plan_public_id="plan-1",
                idempotency_key="key-1",
                principal=_make_principal(),
                repo=repo,
                tracker=_make_tracker(),
                caps_enforcer=_admit_enforcer(),
            )

    @pytest.mark.asyncio
    async def test_plan_without_child_skips_cancel_command(self) -> None:
        """Plan without child_client_order_id → status transition only, no command emit."""
        plan = _make_plan(status="active", child_client_order_id=None, native_instrument=None)
        cancelled = _make_plan(
            status="cancel_requested", child_client_order_id=None, native_instrument=None
        )
        repo = _build_repo(plan=plan)
        repo.get_execution_plan.side_effect = [plan, cancelled]
        result = await PlansCancelService.cancel_by_plan_public_id(
            plan_public_id="plan-1",
            idempotency_key="key-1",
            principal=_make_principal(),
            repo=repo,
            tracker=_make_tracker(),
            caps_enforcer=_admit_enforcer(),
        )
        assert result["status"] == "cancel_requested"
        repo.insert_trade_command.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_updated_plan_disappears_raises_emit_error(self) -> None:
        """Reload returns None after transition → :class:`PlanCancelEmitError`."""
        plan = _make_plan(status="active", child_client_order_id=None, native_instrument=None)
        repo = _build_repo(plan=plan)
        repo.get_execution_plan.side_effect = [plan, None]
        with pytest.raises(PlanCancelEmitError):
            await PlansCancelService.cancel_by_plan_public_id(
                plan_public_id="plan-1",
                idempotency_key="key-1",
                principal=_make_principal(),
                repo=repo,
                tracker=_make_tracker(),
                caps_enforcer=_admit_enforcer(),
            )

    @pytest.mark.asyncio
    async def test_idempotency_replay_when_no_caller_key(self) -> None:
        """Caller passes ``None`` key → no replay short-circuit; proceed normally."""
        plan = _make_plan(status="active")
        cancelled = _make_plan(status="cancel_requested")
        repo = _build_repo(plan=plan)
        repo.get_execution_plan.side_effect = [plan, cancelled]
        result = await PlansCancelService.cancel_by_plan_public_id(
            plan_public_id="plan-1",
            idempotency_key=None,
            principal=_make_principal(),
            repo=repo,
            tracker=_make_tracker(),
            caps_enforcer=_admit_enforcer(),
        )
        assert result["status"] == "cancel_requested"


def test_compensate_module_logs_secondary_failure(caplog: Any) -> None:
    """Direct invocation of :meth:`PlansCancelService._compensate_failed_cancel`.

    Given: a repository whose ``update_execution_plan_status`` raises
        :class:`RuntimeError` during the compensation write,
    When: the helper is driven directly,
    Then: the secondary-failure logging branch is exercised (best-effort
        compensation does not propagate a secondary error so the
        original cancel-emit cause stays the visible one).
    """
    repo = AsyncMock()
    repo.update_execution_plan_status = AsyncMock(side_effect=RuntimeError("comp-fail"))
    tracker = _make_tracker()

    async def _drive() -> None:
        await PlansCancelService._compensate_failed_cancel(
            repo=repo,
            plan_public_id="plan-x",
            bus_time=dt.datetime.now(dt.UTC),
            session_id="s",
            tracker=tracker,
            exc=Exception("orig"),
        )

    asyncio.run(_drive())
    repo.update_execution_plan_status.assert_awaited_once()


class TestPlansCancelServiceR1:
    """Follow-up coverage for the cancel-claim race-reclassification paths."""

    @pytest.mark.asyncio
    async def test_cancel_command_stamps_source_surface_mcp(self) -> None:
        """Cancel TradeCommand carries ``source_surface='mcp'``.

        Given: an active plan with a child order,
        When: :meth:`PlansCancelService.cancel_by_plan_public_id` emits
            the cancel command,
        Then: the inserted ``TradeCommandInsertRow`` carries
            ``source_surface='mcp'`` so audit can attribute the cancel
            to the MCP transport instead of the schema-default ``rest``.
        """
        plan = _make_plan(status="active")
        cancelled = _make_plan(status="cancel_requested", cancel_idempotency_key="key-1")
        repo = _build_repo(plan=plan)
        repo.get_execution_plan.side_effect = [plan, cancelled]
        await PlansCancelService.cancel_by_plan_public_id(
            plan_public_id="plan-1",
            idempotency_key="key-1",
            principal=_make_principal(),
            repo=repo,
            tracker=_make_tracker(),
            caps_enforcer=_admit_enforcer(),
        )
        cmd_row = repo.insert_trade_command.await_args.args[0]
        assert cmd_row["source_surface"] == "mcp"

    @pytest.mark.asyncio
    async def test_non_integrity_insert_failure_compensates_and_raises(self) -> None:
        """Arbitrary insert exception → compensate + PlanCancelEmitError.

        Given: a repository that raises ``RuntimeError`` (NOT an
            :class:`IntegrityError`) on ``insert_trade_command``,
        When: the cancel emit runs,
        Then: the plan is still compensated to ``failed`` (best-effort)
            AND the caller receives :class:`PlanCancelEmitError` so the
            MCP envelope path can map to ``service_unavailable``.
            Without this, REST behaviour was equivalent (catches
            generic ``Exception``) but the service used to only catch
            ``IntegrityError``.
        """
        plan = _make_plan(status="active")
        repo = _build_repo(
            plan=plan,
            insert_command_raises=RuntimeError("connection reset"),
        )
        with pytest.raises(PlanCancelEmitError):
            await PlansCancelService.cancel_by_plan_public_id(
                plan_public_id="plan-1",
                idempotency_key="key-1",
                principal=_make_principal(),
                repo=repo,
                tracker=_make_tracker(),
                caps_enforcer=_admit_enforcer(),
            )
        repo.update_execution_plan_status.assert_awaited_once()
        compensation_kwargs = repo.update_execution_plan_status.await_args.kwargs
        assert compensation_kwargs["new_status"] == "failed"

    @pytest.mark.asyncio
    async def test_race_loser_with_different_key_reclassifies_to_mismatch(self) -> None:
        """Race loser with conflicting key → IdempotencyKeyMismatchError.

        Given: two callers with different keys race a plan with no
            ``cancel_idempotency_key`` yet — the SCD2 update raises
            :class:`IntegrityError` for the loser because the partial
            unique index ``uq_ep_active_cancel_idempotency_key`` admits
            only the winner's row,
        When: the loser's ``update_execution_plan_status`` call
            surfaces the integrity error,
        Then: the service reloads the plan and reclassifies — different
            key now claimed → :class:`PlanCancelIdempotencyKeyMismatchError`.
            Original behaviour leaked the raw IntegrityError.
        """
        plan_pre = _make_plan(status="active", cancel_idempotency_key=None)
        plan_post = _make_plan(status="cancel_requested", cancel_idempotency_key="winner-key")
        repo = _build_repo(plan=plan_pre, claim_outcome="key_mismatch", claim_plan=plan_post)
        with pytest.raises(PlanCancelIdempotencyKeyMismatchError):
            await PlansCancelService.cancel_by_plan_public_id(
                plan_public_id="plan-1",
                idempotency_key="loser-key",
                principal=_make_principal(),
                repo=repo,
                tracker=_make_tracker(),
                caps_enforcer=_admit_enforcer(),
            )

    @pytest.mark.asyncio
    async def test_race_loser_terminal_post_reload_with_no_key_reclassifies(self) -> None:
        """Race loser sees a terminal plan w/ no key → AlreadyTerminalError.

        Given: the loser's SCD2 update fails AND the post-reload plan
            shows a terminal status with NO ``cancel_idempotency_key``
            (a REST-initiated cancel without a key won the race and ran
            to terminal),
        When: the loser reloads,
        Then: :class:`PlanAlreadyTerminalError` is raised.
        """
        plan_pre = _make_plan(status="active")
        plan_terminal = _make_plan(status="cancelled", cancel_idempotency_key=None)
        repo = _build_repo(plan=plan_pre, claim_outcome="terminal", claim_plan=plan_terminal)
        with pytest.raises(PlanAlreadyTerminalError):
            await PlansCancelService.cancel_by_plan_public_id(
                plan_public_id="plan-1",
                idempotency_key="loser-key",
                principal=_make_principal(),
                repo=repo,
                tracker=_make_tracker(),
                caps_enforcer=_admit_enforcer(),
            )

    @pytest.mark.asyncio
    async def test_race_loser_against_keyless_cancel_reclassifies_to_in_progress(
        self,
    ) -> None:
        """post-reload shows cancel_requested w/ no key → InProgress.

        Given: the loser's SCD2 update fails AND the post-reload plan
            is in ``cancel_requested`` with NO ``cancel_idempotency_key``
            (a REST-initiated cancel with no key claimed first),
        When: the loser reloads,
        Then: :class:`PlanCancelInProgressError` is raised so the
            caller learns the cancel is already in flight without a
            replay handle for them.
        """
        plan_pre = _make_plan(status="active")
        plan_in_progress = _make_plan(status="cancel_requested", cancel_idempotency_key=None)
        repo = _build_repo(plan=plan_pre, claim_outcome="in_progress", claim_plan=plan_in_progress)
        with pytest.raises(PlanCancelInProgressError):
            await PlansCancelService.cancel_by_plan_public_id(
                plan_public_id="plan-1",
                idempotency_key="loser-key",
                principal=_make_principal(),
                repo=repo,
                tracker=_make_tracker(),
                caps_enforcer=_admit_enforcer(),
            )

    @pytest.mark.asyncio
    async def test_race_loser_with_matching_key_after_reload_returns_replay_success(
        self,
    ) -> None:
        """post-reload key matches caller → idempotent replay success.

        Given: two callers send the SAME ``cancel_idempotency_key``
            concurrently; the first wins and writes the SCD2 cancel
            row; the second's :class:`IntegrityError` lands at the
            partial-unique index,
        When: the loser reloads,
        Then: the post-race row carries the matching key, so the
            reclassifier returns it as a replay-success row,
            ``_claim_cancel_transition`` short-circuits the caller's
            ``insert_trade_command`` step, and the service returns the
            current row WITHOUT raising. This honours the
            idempotent-replay contract.
        """
        plan_pre = _make_plan(status="active")
        plan_post = _make_plan(status="cancel_requested", cancel_idempotency_key="loser-key")
        repo = _build_repo(plan=plan_pre, claim_outcome="replay", claim_plan=plan_post)
        result = await PlansCancelService.cancel_by_plan_public_id(
            plan_public_id="plan-1",
            idempotency_key="loser-key",
            principal=_make_principal(),
            repo=repo,
            tracker=_make_tracker(),
            caps_enforcer=_admit_enforcer(),
        )
        assert result["public_id"] == plan_pre["public_id"]
        assert result["cancel_idempotency_key"] == "loser-key"
        repo.insert_trade_command.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_race_loser_post_reload_still_active_signals_cross_plan_conflict(
        self,
    ) -> None:
        """post-reload plan still active w/ no key → cross-plan reuse.

        Given: the SCD2 update raises an :class:`IntegrityError` AND
            the post-reload plan is still in an actionable status
            with no ``cancel_idempotency_key`` claimed,
        When: the reclassifier reloads,
        Then: the IntegrityError MUST have come from the
            partial-unique index ``uq_ep_active_cancel_idempotency_key``
            on ``(operator_public_id, cancel_idempotency_key)`` —
            i.e., another plan under the same operator already claimed
            this key. Mapping to :class:`PlanCancelIdempotencyKeyMismatchError`
            tells the caller their key is unusable here; retrying
            wouldn't change that.
        """
        plan_pre = _make_plan(status="active")
        plan_post = _make_plan(status="active", cancel_idempotency_key=None)
        repo = _build_repo(
            plan=plan_pre,
            claim_raises=IntegrityError(
                "stmt",
                {},
                Exception("UNIQUE constraint failed: uq_ep_active_cancel_idempotency_key"),
            ),
        )
        repo.get_execution_plan.side_effect = [plan_pre, plan_post]
        with pytest.raises(PlanCancelIdempotencyKeyMismatchError):
            await PlansCancelService.cancel_by_plan_public_id(
                plan_public_id="plan-1",
                idempotency_key="loser-key",
                principal=_make_principal(),
                repo=repo,
                tracker=_make_tracker(),
                caps_enforcer=_admit_enforcer(),
            )

    @pytest.mark.asyncio
    async def test_race_loser_non_unique_integrity_falls_through_to_concurrent_change(
        self,
    ) -> None:
        """non-unique IntegrityError + caller key → fall-through.

        Given: the SCD2 update raises an :class:`IntegrityError` whose
            cause does NOT carry the
            ``uq_ep_active_cancel_idempotency_key`` constraint name
            (e.g., a foreign-key violation, a transient deadlock
            translated to IntegrityError),
        When: the reclassifier reloads and the post-race plan is still
            active without a key,
        Then: the cross-plan-key-reuse inference is skipped because we
            cannot confirm the partial-unique index was the offender.
            The helper falls through, and
            ``_claim_cancel_transition`` raises
            :class:`PlanConcurrentChangeError` so callers can retry.
        """
        plan_pre = _make_plan(status="active")
        plan_post = _make_plan(status="active", cancel_idempotency_key=None)
        repo = _build_repo(
            plan=plan_pre,
            claim_raises=IntegrityError("stmt", {}, Exception("FOREIGN KEY constraint failed")),
        )
        repo.get_execution_plan.side_effect = [plan_pre, plan_post]
        with pytest.raises(PlanConcurrentChangeError):
            await PlansCancelService.cancel_by_plan_public_id(
                plan_public_id="plan-1",
                idempotency_key="loser-key",
                principal=_make_principal(),
                repo=repo,
                tracker=_make_tracker(),
                caps_enforcer=_admit_enforcer(),
            )

    @pytest.mark.asyncio
    async def test_caps_violation_with_replay_returns_success(self) -> None:
        """Caps rejection w/ same-key replay match → success.

        Given: a same-key concurrent caller's first call already
            claimed the cancel; a retry that gets rate-limited would
            normally surface :class:`CapsViolationError`,
        When: the post-race plan carries the caller's key,
        Then: the service short-circuits to a replay-success row
            instead of bubbling the caps error. Idempotent-replay
            contract.
        """
        plan_pre = _make_plan(status="active")
        plan_post = _make_plan(status="cancel_requested", cancel_idempotency_key="loser-key")
        repo = _build_repo(plan=plan_pre)
        repo.get_execution_plan.side_effect = [plan_pre, plan_post]
        result = await PlansCancelService.cancel_by_plan_public_id(
            plan_public_id="plan-1",
            idempotency_key="loser-key",
            principal=_make_principal(),
            repo=repo,
            tracker=_make_tracker(),
            caps_enforcer=_reject_enforcer(),
        )
        assert result["public_id"] == plan_pre["public_id"]
        assert result["cancel_idempotency_key"] == "loser-key"
        repo.update_execution_plan_status.assert_not_awaited()
        repo.insert_trade_command.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_caps_violation_without_replay_propagates(self) -> None:
        """Caps rejection without replay match → CapsViolationError raised.

        Given: caps rejects the cancel guard AND the post-race plan
            does NOT carry the caller's key,
        When: the service runs,
        Then: the original :class:`CapsViolationError` propagates
            unchanged (replay-success short-circuit only applies to
            same-key idempotent retries).
        """
        plan_pre = _make_plan(status="active", cancel_idempotency_key=None)
        repo = _build_repo(plan=plan_pre)
        repo.get_execution_plan.side_effect = [plan_pre, plan_pre]
        with pytest.raises(CapsViolationError):
            await PlansCancelService.cancel_by_plan_public_id(
                plan_public_id="plan-1",
                idempotency_key="caller-key",
                principal=_make_principal(),
                repo=repo,
                tracker=_make_tracker(),
                caps_enforcer=_reject_enforcer(),
            )

    @pytest.mark.asyncio
    async def test_caps_violation_with_different_existing_key_returns_mismatch(
        self,
    ) -> None:
        """Caps + post-rejection has DIFFERENT key → IdempotencyKeyMismatchError.

        Given: a different-key caller hits the cancel-rate cap; the
            post-rejection reload shows a winner already claimed a
            DIFFERENT key,
        When: the reclassifier inspects the post-rejection state,
        Then: maps to :class:`PlanCancelIdempotencyKeyMismatchError`
            instead of bubbling the caps error. MCP envelope is
            ``idempotency_key_conflict``, which is the contractually
            correct outcome — the caller's key cannot be claimed here.
        """
        plan_pre = _make_plan(status="active")
        plan_post = _make_plan(status="cancel_requested", cancel_idempotency_key="winner-key")
        repo = _build_repo(plan=plan_pre)
        repo.get_execution_plan.side_effect = [plan_pre, plan_post]
        with pytest.raises(PlanCancelIdempotencyKeyMismatchError):
            await PlansCancelService.cancel_by_plan_public_id(
                plan_public_id="plan-1",
                idempotency_key="loser-key",
                principal=_make_principal(),
                repo=repo,
                tracker=_make_tracker(),
                caps_enforcer=_reject_enforcer(),
            )

    @pytest.mark.asyncio
    async def test_caps_violation_post_rejection_terminal_returns_already_terminal(
        self,
    ) -> None:
        """Caps + post-rejection terminal → AlreadyTerminalError.

        Given: a caller hits the cap AND the winner ran the cancel to
            terminal between the caller's pre-check and the cap
            rejection,
        When: the reclassifier inspects the post-rejection state,
        Then: maps to :class:`PlanAlreadyTerminalError` instead of
            ``caps_violation``.
        """
        plan_pre = _make_plan(status="active")
        plan_terminal = _make_plan(status="cancelled", cancel_idempotency_key=None)
        repo = _build_repo(plan=plan_pre)
        repo.get_execution_plan.side_effect = [plan_pre, plan_terminal]
        with pytest.raises(PlanAlreadyTerminalError):
            await PlansCancelService.cancel_by_plan_public_id(
                plan_public_id="plan-1",
                idempotency_key="caller-key",
                principal=_make_principal(),
                repo=repo,
                tracker=_make_tracker(),
                caps_enforcer=_reject_enforcer(),
            )

    @pytest.mark.asyncio
    async def test_caps_violation_post_rejection_in_progress_returns_in_progress(
        self,
    ) -> None:
        """Caps + post-rejection cancel_requested w/ no key → InProgressError.

        Given: a caller hits the cap AND the post-rejection plan is in
            ``cancel_requested`` with NO key (REST-style winner
            claimed it without a key),
        When: the reclassifier inspects the post-rejection state,
        Then: maps to :class:`PlanCancelInProgressError` instead of
            ``caps_violation``.
        """
        plan_pre = _make_plan(status="active")
        plan_in_progress = _make_plan(status="cancel_requested", cancel_idempotency_key=None)
        repo = _build_repo(plan=plan_pre)
        repo.get_execution_plan.side_effect = [plan_pre, plan_in_progress]
        with pytest.raises(PlanCancelInProgressError):
            await PlansCancelService.cancel_by_plan_public_id(
                plan_public_id="plan-1",
                idempotency_key="caller-key",
                principal=_make_principal(),
                repo=repo,
                tracker=_make_tracker(),
                caps_enforcer=_reject_enforcer(),
            )

    @pytest.mark.asyncio
    async def test_caps_violation_with_no_caller_key_propagates(self) -> None:
        """Caller without key → caps violation always propagates.

        ``_reclassify_post_caps_rejection`` falls through (caller has
        no replay handle and the plan is still actionable), so REST-
        shaped callers that go through the service still see caps
        violations as normal failures.
        """
        plan_pre = _make_plan(status="active", cancel_idempotency_key=None)
        repo = _build_repo(plan=plan_pre)
        repo.get_execution_plan.side_effect = [plan_pre, plan_pre]
        with pytest.raises(CapsViolationError):
            await PlansCancelService.cancel_by_plan_public_id(
                plan_public_id="plan-1",
                idempotency_key=None,
                principal=_make_principal(),
                repo=repo,
                tracker=_make_tracker(),
                caps_enforcer=_reject_enforcer(),
            )

    @pytest.mark.asyncio
    async def test_caps_violation_replay_lookup_returns_none_propagates(self) -> None:
        """Caps violation + plan disappeared → caps violation propagates.

        Covers the ``latest is None`` branch in
        :meth:`_maybe_caps_replay`.
        """
        plan_pre = _make_plan(status="active")
        repo = _build_repo(plan=plan_pre)
        repo.get_execution_plan.side_effect = [plan_pre, None]
        with pytest.raises(CapsViolationError):
            await PlansCancelService.cancel_by_plan_public_id(
                plan_public_id="plan-1",
                idempotency_key="caller-key",
                principal=_make_principal(),
                repo=repo,
                tracker=_make_tracker(),
                caps_enforcer=_reject_enforcer(),
            )

    @pytest.mark.asyncio
    async def test_race_loser_with_no_caller_key_falls_through_to_concurrent_change(
        self,
    ) -> None:
        """Caller without a key + post-reload still active → fall-through.

        Given: a (legacy / REST) caller passes ``idempotency_key=None``
            yet the SCD2 update raises an :class:`IntegrityError`
            (e.g., from a different unique constraint) AND the
            post-reload plan is still active without a key,
        When: the reclassifier reloads,
        Then: cross-plan key-reuse cannot apply (caller has no key);
            no other branch matches; the helper returns ``None`` and
            ``_claim_cancel_transition`` raises
            :class:`PlanConcurrentChangeError`. Covers the final
            fall-through branch in :meth:`_reclassify_race_loser`.
        """
        plan_pre = _make_plan(status="active")
        plan_post = _make_plan(status="active", cancel_idempotency_key=None)
        repo = _build_repo(
            plan=plan_pre,
            claim_raises=IntegrityError("stmt", {}, Exception("transient")),
        )
        repo.get_execution_plan.side_effect = [plan_pre, plan_post]
        with pytest.raises(PlanConcurrentChangeError):
            await PlansCancelService.cancel_by_plan_public_id(
                plan_public_id="plan-1",
                idempotency_key=None,
                principal=_make_principal(),
                repo=repo,
                tracker=_make_tracker(),
                caps_enforcer=_admit_enforcer(),
            )

    @pytest.mark.asyncio
    async def test_race_loser_replay_on_keyless_plan_skips_command_emit(self) -> None:
        """Replay branch covered for plans without child cmd.

        Given: a plan WITHOUT ``child_client_order_id`` (so the cancel
            flow takes the no-cmd-emit branch) AND a same-key race
            loser scenario,
        When: the reclassifier returns a replay row,
        Then: the no-cmd-emit branch returns the replay row directly
            instead of falling through to a fresh
            :meth:`get_execution_plan` reload.
        """
        plan_pre = _make_plan(status="active", child_client_order_id=None, native_instrument=None)
        plan_post = _make_plan(
            status="cancel_requested",
            cancel_idempotency_key="loser-key",
            child_client_order_id=None,
            native_instrument=None,
        )
        repo = _build_repo(plan=plan_pre, claim_outcome="replay", claim_plan=plan_post)
        result = await PlansCancelService.cancel_by_plan_public_id(
            plan_public_id="plan-1",
            idempotency_key="loser-key",
            principal=_make_principal(),
            repo=repo,
            tracker=_make_tracker(),
            caps_enforcer=_admit_enforcer(),
        )
        assert result["status"] == "cancel_requested"
        assert result["cancel_idempotency_key"] == "loser-key"
        repo.insert_trade_command.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_race_loser_with_matching_key_on_terminal_plan_returns_replay(
        self,
    ) -> None:
        """Replay match wins over terminal status.

        Given: the loser sees a terminal post-race row that carries
            their key (the winner's cancel ran to terminal under the
            shared key),
        When: the reclassifier reloads,
        Then: the matching key short-circuits BEFORE the terminal
            check, so the caller gets a replay-success row carrying
            ``status='cancelled'`` instead of an
            :class:`PlanAlreadyTerminalError`.
        """
        plan_pre = _make_plan(status="active")
        plan_terminal = _make_plan(status="cancelled", cancel_idempotency_key="loser-key")
        repo = _build_repo(plan=plan_pre, claim_outcome="replay", claim_plan=plan_terminal)
        result = await PlansCancelService.cancel_by_plan_public_id(
            plan_public_id="plan-1",
            idempotency_key="loser-key",
            principal=_make_principal(),
            repo=repo,
            tracker=_make_tracker(),
            caps_enforcer=_admit_enforcer(),
        )
        assert result["status"] == "cancelled"
        assert result["cancel_idempotency_key"] == "loser-key"

    @pytest.mark.asyncio
    async def test_race_loser_plan_disappeared_falls_through_to_concurrent_change(
        self,
    ) -> None:
        """post-reload returns None → reclassifier returns silently.

        Outer ``_claim_cancel_transition`` raises
        :class:`PlanConcurrentChangeError`. Covers the
        ``latest is None`` branch in :meth:`_reclassify_race_loser`.
        """
        plan_pre = _make_plan(status="active")
        repo = _build_repo(
            plan=plan_pre,
            claim_raises=IntegrityError("stmt", {}, Exception("partial unique")),
        )
        repo.get_execution_plan.side_effect = [plan_pre, None]
        with pytest.raises(PlanConcurrentChangeError):
            await PlansCancelService.cancel_by_plan_public_id(
                plan_public_id="plan-1",
                idempotency_key="loser-key",
                principal=_make_principal(),
                repo=repo,
                tracker=_make_tracker(),
                caps_enforcer=_admit_enforcer(),
            )

    @pytest.mark.asyncio
    async def test_integrity_error_reload_matching_key_returns_replay(self) -> None:
        """Reclassifier: IntegrityError + matching key on reload → replay.

        Cross-plan key reuse can fail the partial-unique index. If
        the post-reload row coincidentally carries the caller's key
        (e.g., the caller's earlier successful claim against this
        plan persisted before a cross-plan loser-pair raced), the
        reclassifier returns the row as replay-success and the caller
        skips the cancel emit.
        """
        plan_pre = _make_plan(status="active")
        plan_post = _make_plan(status="cancel_requested", cancel_idempotency_key="loser-key")
        repo = _build_repo(
            plan=plan_pre,
            claim_raises=IntegrityError(
                "stmt",
                {},
                Exception("UNIQUE constraint failed: uq_ep_active_cancel_idempotency_key"),
            ),
        )
        repo.get_execution_plan.side_effect = [plan_pre, plan_post]
        result = await PlansCancelService.cancel_by_plan_public_id(
            plan_public_id="plan-1",
            idempotency_key="loser-key",
            principal=_make_principal(),
            repo=repo,
            tracker=_make_tracker(),
            caps_enforcer=_admit_enforcer(),
        )
        assert result["public_id"] == plan_pre["public_id"]
        repo.insert_trade_command.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_integrity_error_reload_different_key_returns_mismatch(self) -> None:
        """Reclassifier: IntegrityError + different existing key → mismatch."""
        plan_pre = _make_plan(status="active", cancel_idempotency_key=None)
        plan_post = _make_plan(status="cancel_requested", cancel_idempotency_key="winner-key")
        repo = _build_repo(
            plan=plan_pre,
            claim_raises=IntegrityError(
                "stmt", {}, Exception("UNIQUE constraint failed: column unrelated")
            ),
        )
        repo.get_execution_plan.side_effect = [plan_pre, plan_post]
        with pytest.raises(PlanCancelIdempotencyKeyMismatchError):
            await PlansCancelService.cancel_by_plan_public_id(
                plan_public_id="plan-1",
                idempotency_key="loser-key",
                principal=_make_principal(),
                repo=repo,
                tracker=_make_tracker(),
                caps_enforcer=_admit_enforcer(),
            )

    @pytest.mark.asyncio
    async def test_integrity_error_reload_terminal_returns_terminal(self) -> None:
        """Reclassifier: IntegrityError + terminal post-reload → AlreadyTerminal."""
        plan_pre = _make_plan(status="active")
        plan_terminal = _make_plan(status="cancelled", cancel_idempotency_key=None)
        repo = _build_repo(
            plan=plan_pre,
            claim_raises=IntegrityError("stmt", {}, Exception("UNIQUE constraint failed: other")),
        )
        repo.get_execution_plan.side_effect = [plan_pre, plan_terminal]
        with pytest.raises(PlanAlreadyTerminalError):
            await PlansCancelService.cancel_by_plan_public_id(
                plan_public_id="plan-1",
                idempotency_key="loser-key",
                principal=_make_principal(),
                repo=repo,
                tracker=_make_tracker(),
                caps_enforcer=_admit_enforcer(),
            )

    @pytest.mark.asyncio
    async def test_integrity_error_reload_in_progress_returns_in_progress(self) -> None:
        """Reclassifier: IntegrityError + cancel_requested-no-key → InProgress."""
        plan_pre = _make_plan(status="active")
        plan_in_progress = _make_plan(status="cancel_requested", cancel_idempotency_key=None)
        repo = _build_repo(
            plan=plan_pre,
            claim_raises=IntegrityError("stmt", {}, Exception("UNIQUE constraint failed: other")),
        )
        repo.get_execution_plan.side_effect = [plan_pre, plan_in_progress]
        with pytest.raises(PlanCancelInProgressError):
            await PlansCancelService.cancel_by_plan_public_id(
                plan_public_id="plan-1",
                idempotency_key="loser-key",
                principal=_make_principal(),
                repo=repo,
                tracker=_make_tracker(),
                caps_enforcer=_admit_enforcer(),
            )


class TestIsCancelIdempotencyKeyUniqueViolation:
    """Coverage for the unique-violation classifier helper.

    Drives the three driver-specific branches that
    :meth:`PlansCancelService._reclassify_race_loser` consults to
    decide whether an :class:`IntegrityError` is the partial-unique
    cancel-idempotency-key violation rather than an unrelated
    constraint failure.
    """

    def test_returns_false_when_orig_is_none(self) -> None:
        """Lock the no-orig branch.

        Given: an IntegrityError without a driver-side ``orig`` payload,
        When: the classifier inspects it,
        Then: returns ``False`` so cross-plan inference does NOT fire.
        """
        exc = IntegrityError("stmt", {}, None)
        assert _is_cancel_idempotency_key_unique_violation(exc) is False

    def test_matches_psycopg_constraint_name_attribute(self) -> None:
        """Lock the asyncpg/psycopg attribute branch.

        Given: an asyncpg/psycopg-style ``orig.constraint_name``,
        When: the classifier inspects it,
        Then: returns ``True`` for the partial-unique cancel index name.
        """

        class _OrigError(Exception):
            constraint_name = "uq_ep_active_cancel_idempotency_key"

        exc = IntegrityError("stmt", {}, _OrigError())
        assert _is_cancel_idempotency_key_unique_violation(exc) is True

    def test_matches_psycopg_diag_constraint_name(self) -> None:
        """Lock the psycopg ``diag`` attribute branch.

        Given: a psycopg-style ``orig.diag.constraint_name`` (no top-level attr),
        When: the classifier inspects it,
        Then: returns ``True`` for the partial-unique cancel index name.
        """

        class _Diag:
            constraint_name = "uq_ep_active_cancel_idempotency_key"

        class _OrigError(Exception):
            diag = _Diag()

        exc = IntegrityError("stmt", {}, _OrigError())
        assert _is_cancel_idempotency_key_unique_violation(exc) is True

    def test_matches_sqlite_via_errorcode_and_column_name(self) -> None:
        """Lock the SQLite branch using errorcode + column-name substring.

        Given: an aiosqlite-style payload with ``sqlite_errorcode=2067``
            and the offending column listed in the error message
            (SQLite's unique-violation reports columns, not index name),
        When: the classifier inspects it,
        Then: returns ``True``.
        """

        class _SqliteOrigError(Exception):
            sqlite_errorcode = 2067

        exc = IntegrityError(
            "stmt",
            {},
            _SqliteOrigError(
                "UNIQUE constraint failed: execution_plans.operator_public_id, "
                "execution_plans.cancel_idempotency_key"
            ),
        )
        assert _is_cancel_idempotency_key_unique_violation(exc) is True

    def test_returns_false_when_sqlite_unique_on_unrelated_column(self) -> None:
        """Lock the SQLite-conservative branch.

        SQLite ``sqlite_errorcode=2067`` for a different column does
        NOT trigger cross-plan inference, so callers can retry rather
        than receive a permanent ``idempotency_key_conflict``.
        """

        class _SqliteOrigError(Exception):
            sqlite_errorcode = 2067

        exc = IntegrityError(
            "stmt",
            {},
            _SqliteOrigError("UNIQUE constraint failed: trade_commands.client_order_id"),
        )
        assert _is_cancel_idempotency_key_unique_violation(exc) is False

    def test_matches_pg_via_message_substring_when_no_constraint_name(self) -> None:
        """Lock the PG-non-asyncpg fallback path: index name in message string."""
        exc = IntegrityError(
            "stmt",
            {},
            Exception(
                "duplicate key value violates unique constraint "
                '"uq_ep_active_cancel_idempotency_key"'
            ),
        )
        assert _is_cancel_idempotency_key_unique_violation(exc) is True

    def test_returns_false_for_unrelated_integrity_error(self) -> None:
        """Lock the conservative-fall-through branch.

        Given: a generic IntegrityError (e.g., FK violation),
        When: the classifier inspects it,
        Then: returns ``False`` so cross-plan inference is skipped.
        """
        exc = IntegrityError("stmt", {}, Exception("FOREIGN KEY constraint failed"))
        assert _is_cancel_idempotency_key_unique_violation(exc) is False
