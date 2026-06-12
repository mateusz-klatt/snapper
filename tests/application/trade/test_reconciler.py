"""Tests for ReconciliationLoop — periodic reconciliation + lifecycle fold."""

import asyncio
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from typing import Any
from typing import cast
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import pytest

from snapper.application.trade.reconciler import ReconciliationLoop
from snapper.application.trade.reconciler import _fold_lifecycle_advance
from snapper.application.trade.trade_service import TradeService
from snapper.core.partitioning import ShardOwnership
from snapper.core.types import TradeCommandStatusEnum
from snapper.data.repository_types import TradeCommandRow
from snapper.data.repository_types import VenueEventRow


def _make_cmd(**overrides: Any) -> TradeCommandRow:
    """Build a full TradeCommandRow with sane defaults for fold tests."""
    now = datetime.now(UTC)
    row: dict[str, Any] = {
        "public_id": "cmd-1",
        "timestamp": now,
        "session_id": "s1",
        "sequence_id": 7,
        "command_type": "create",
        "shard_key": "kraken.BTC-USD.live",
        "exchange": "kraken",
        "instrument": "BTC-USD",
        "mode": "live",
        "strategy_id": "strat-1",
        "client_order_id": "cid-1",
        "venue_client_id": "cid-1",
        "idempotency_key": None,
        "side": "buy",
        "order_type": "limit",
        "quantity": 1.0,
        "price": 100.0,
        "leverage": None,
        "reduce_only": False,
        "status": "dispatched",
        "attempt_count": 1,
        "last_error": None,
        "created_at": now,
        "dispatched_at": now,
        "acked_at": None,
        "terminal_at": None,
        "exchange_order_id": None,
        "supersedes_command_id": None,
        "correlation_id": "corr-1",
        "wallet_public_id": "wallet-1",
        "operator_public_id": None,
        "user_public_id": None,
        "source_surface": None,
        "plan_public_id": None,
    }
    row.update(overrides)
    return cast(TradeCommandRow, row)


def _make_event(**overrides: Any) -> VenueEventRow:
    """Build a full VenueEventRow with sane defaults for fold tests."""
    now = datetime.now(UTC)
    row: dict[str, Any] = {
        "id": 1,
        "public_id": "ve-1",
        "timestamp": now,
        "session_id": "s1",
        "sequence_id": 1,
        "event_type": "order_accepted",
        "shard_key": "kraken.BTC-USD.live",
        "command_public_id": None,
        "exchange": "kraken",
        "instrument": "BTC-USD",
        "mode": "live",
        "exchange_order_id": "ex-1",
        "client_order_id": "cid-1",
        "venue_client_id": None,
        "side": "buy",
        "status": None,
        "fill_price": None,
        "fill_size": None,
        "cum_fill_size": None,
        "fee": None,
        "fee_asset": None,
        "exec_id": None,
        "trade_id": None,
        "error": None,
        "venue_timestamp": None,
        "received_at": now,
        "liquidity_role": "unknown",
        "paired_group_id": None,
    }
    row.update(overrides)
    return cast(VenueEventRow, row)


def _make_repo(
    cmds: list[TradeCommandRow] | None = None,
    events: list[VenueEventRow] | None = None,
) -> Any:
    """Build a repo mock with explicit fold-method stubs."""
    repo = AsyncMock()
    repo.get_active_commands_for_exchange = AsyncMock(return_value=cmds or [])
    repo.get_order_lifecycle_events = AsyncMock(return_value=events or [])
    repo.advance_trade_command_lifecycle = AsyncMock(return_value=True)
    repo.get_rejected_commands_with_later_live_evidence = AsyncMock(return_value=[])
    return repo


def _make_loop(repo: Any, interval_seconds: float = 60.0) -> tuple[ReconciliationLoop, Any]:
    """Build a ReconciliationLoop with a mocked trade service."""
    trade_svc = MagicMock(spec=TradeService)
    recon = ReconciliationLoop(
        exchange_name="kraken",
        repository=repo,
        trade_service=trade_svc,
        interval_seconds=interval_seconds,
    )
    return recon, trade_svc


class TestFoldLifecycleAdvance:
    """Pure fold of venue events into a command status advance."""

    def test_no_events_is_noop(self) -> None:
        """No evidence folds to no advance.

        Given: a dispatched command with zero venue events,
        When: the fold runs,
        Then: no advance is produced.
        """
        assert _fold_lifecycle_advance(_make_cmd(), []) is None

    def test_accepted_event_advances_to_accepted(self) -> None:
        """An order_accepted row folds to ACCEPTED with ack fields.

        Given: a dispatched command with one order_accepted event,
        When: the fold runs,
        Then: target is ACCEPTED carrying acked_at and exchange_order_id.
        """
        received = datetime.now(UTC)
        advance = _fold_lifecycle_advance(
            _make_cmd(), [_make_event(received_at=received, exchange_order_id="ex-9")]
        )
        assert advance is not None
        assert advance.status == TradeCommandStatusEnum.ACCEPTED
        assert advance.acked_at == received
        assert advance.exchange_order_id == "ex-9"
        assert advance.terminal_at is None
        assert advance.last_error is None

    def test_duplicate_accepts_keep_first_ack_time(self) -> None:
        """Duplicate accept rows collapse to the first observation.

        Given: two order_accepted events (the at-least-once duplicate),
        When: the fold runs,
        Then: acked_at is the FIRST event's received_at and the second's
            missing exchange id does not clear the first's.
        """
        first = datetime.now(UTC)
        second = first + timedelta(seconds=5)
        advance = _fold_lifecycle_advance(
            _make_cmd(),
            [
                _make_event(id=1, received_at=first, exchange_order_id="ex-9"),
                _make_event(id=2, received_at=second, exchange_order_id=None),
            ],
        )
        assert advance is not None
        assert advance.acked_at == first
        assert advance.exchange_order_id == "ex-9"

    def test_partial_fill_advances_to_partially_filled(self) -> None:
        """A cumulative fill below quantity folds to PARTIALLY_FILLED.

        Given: a dispatched command (quantity=1.0) with cum 0.4,
        When: the fold runs,
        Then: target is PARTIALLY_FILLED.
        """
        advance = _fold_lifecycle_advance(
            _make_cmd(),
            [_make_event(event_type="fill_observed", cum_fill_size=0.4, exchange_order_id=None)],
        )
        assert advance is not None
        assert advance.status == TradeCommandStatusEnum.PARTIALLY_FILLED

    def test_complete_fill_advances_to_filled(self) -> None:
        """A cumulative fill covering quantity folds to FILLED.

        Given: max cum across fills equals the command quantity,
        When: the fold runs,
        Then: target is FILLED and the exchange id comes from the fill row.
        """
        advance = _fold_lifecycle_advance(
            _make_cmd(),
            [
                _make_event(
                    id=1, event_type="fill_observed", cum_fill_size=0.5, exchange_order_id="ex-f"
                ),
                _make_event(
                    id=2, event_type="fill_observed", cum_fill_size=1.0, exchange_order_id="ex-f"
                ),
                _make_event(
                    id=3, event_type="fill_observed", cum_fill_size=None, exchange_order_id=None
                ),
                _make_event(
                    id=4, event_type="fill_observed", cum_fill_size=0.7, exchange_order_id=None
                ),
            ],
        )
        assert advance is not None
        assert advance.status == TradeCommandStatusEnum.FILLED
        assert advance.exchange_order_id == "ex-f"

    def test_zero_quantity_with_fills_stays_partial(self) -> None:
        """A falsy command quantity never claims completeness.

        Given: a command row with quantity 0 and a positive cum fill,
        When: the fold runs,
        Then: target is PARTIALLY_FILLED (no fabricated FILLED).
        """
        advance = _fold_lifecycle_advance(
            _make_cmd(quantity=0.0),
            [_make_event(event_type="fill_observed", cum_fill_size=0.2, exchange_order_id=None)],
        )
        assert advance is not None
        assert advance.status == TradeCommandStatusEnum.PARTIALLY_FILLED

    def test_rejected_event_advances_to_rejected(self) -> None:
        """An order_rejected row folds terminally to REJECTED.

        Given: a dispatched command with a rejection carrying an error,
        When: the fold runs,
        Then: target is REJECTED with the venue error and terminal_at.
        """
        received = datetime.now(UTC)
        advance = _fold_lifecycle_advance(
            _make_cmd(),
            [
                _make_event(
                    event_type="order_rejected", error="insufficient funds", received_at=received
                )
            ],
        )
        assert advance is not None
        assert advance.status == TradeCommandStatusEnum.REJECTED
        assert advance.last_error == "insufficient funds"
        assert advance.terminal_at == received

    def test_rejected_without_error_gets_fallback_reason(self) -> None:
        """A rejection without venue error text still records a reason.

        Given: an order_rejected event whose error field is None,
        When: the fold runs,
        Then: last_error falls back to a generic venue-rejection note.
        """
        advance = _fold_lifecycle_advance(
            _make_cmd(), [_make_event(event_type="order_rejected", error=None)]
        )
        assert advance is not None
        assert advance.last_error == "rejected by venue"

    def test_breaker_open_event_advances_to_failed(self) -> None:
        """An order_breaker_open row folds to FAILED (infra, not venue).

        Given: a dispatched command with a breaker-open event,
        When: the fold runs,
        Then: target is FAILED with the circuit_breaker_open reason.
        """
        advance = _fold_lifecycle_advance(
            _make_cmd(), [_make_event(event_type="order_breaker_open")]
        )
        assert advance is not None
        assert advance.status == TradeCommandStatusEnum.FAILED
        assert advance.last_error == "circuit_breaker_open"

    def test_terminal_event_maps_known_statuses(self) -> None:
        """An order_terminal row maps its status onto the command enum.

        Given: a terminal event with status canceled,
        When: the fold runs,
        Then: target is CANCELLED with no error note.
        """
        advance = _fold_lifecycle_advance(
            _make_cmd(), [_make_event(event_type="order_terminal", status="canceled")]
        )
        assert advance is not None
        assert advance.status == TradeCommandStatusEnum.CANCELLED
        assert advance.last_error is None

    def test_terminal_event_unmapped_status_is_cancelled_with_note(self) -> None:
        """An unknown terminal status degrades honestly to CANCELLED.

        Given: a terminal event with an unmapped status string,
        When: the fold runs,
        Then: target is CANCELLED and last_error names the raw status.
        """
        advance = _fold_lifecycle_advance(
            _make_cmd(), [_make_event(event_type="order_terminal", status="vaporized")]
        )
        assert advance is not None
        assert advance.status == TradeCommandStatusEnum.CANCELLED
        assert advance.last_error == "unmapped terminal status 'vaporized'"

    def test_terminal_outranks_fills_and_accept(self) -> None:
        """The last terminal-class event wins over accept/fill evidence.

        Given: accept, partial fill, then a filled terminal event,
        When: the fold runs,
        Then: target is FILLED via the terminal mapping.
        """
        advance = _fold_lifecycle_advance(
            _make_cmd(),
            [
                _make_event(id=1, event_type="order_accepted"),
                _make_event(id=2, event_type="fill_observed", cum_fill_size=0.5),
                _make_event(id=3, event_type="order_terminal", status="filled"),
            ],
        )
        assert advance is not None
        assert advance.status == TradeCommandStatusEnum.FILLED

    def test_unknown_only_evidence_is_noop(self) -> None:
        """order_submit_unknown alone advances nothing.

        Given: a dispatched command whose only evidence is UNKNOWN,
        When: the fold runs,
        Then: no advance — the executor verification sweep owns it.
        """
        advance = _fold_lifecycle_advance(
            _make_cmd(), [_make_event(event_type="order_submit_unknown")]
        )
        assert advance is None

    def test_later_accept_supersedes_earlier_rejection(self) -> None:
        """A retried command's acceptance overrides the old rejection.

        Given: order_rejected (id=1) then order_accepted (id=2) — the
            outbox legally retried because rejections are excluded from
            duplicate-submit evidence,
        When: the fold runs,
        Then: target is ACCEPTED, never the stale REJECTED.
        """
        advance = _fold_lifecycle_advance(
            _make_cmd(),
            [
                _make_event(id=1, event_type="order_rejected", error="nope"),
                _make_event(id=2, event_type="order_accepted"),
            ],
        )
        assert advance is not None
        assert advance.status == TradeCommandStatusEnum.ACCEPTED
        assert advance.terminal_at is None
        assert advance.last_error is None

    def test_later_fill_supersedes_earlier_rejection(self) -> None:
        """Fill evidence after a rejection proves the retry went live.

        Given: order_rejected (id=1) then fill_observed (id=2),
        When: the fold runs,
        Then: target reflects the fill, not the stale rejection.
        """
        advance = _fold_lifecycle_advance(
            _make_cmd(),
            [
                _make_event(id=1, event_type="order_rejected"),
                _make_event(id=2, event_type="fill_observed", cum_fill_size=0.3),
            ],
        )
        assert advance is not None
        assert advance.status == TradeCommandStatusEnum.PARTIALLY_FILLED

    def test_rejection_after_acceptance_stays_terminal(self) -> None:
        """A rejection observed AFTER acceptance is the newer truth.

        Given: order_accepted (id=1) then order_rejected (id=2),
        When: the fold runs,
        Then: target is REJECTED (observation order decides).
        """
        advance = _fold_lifecycle_advance(
            _make_cmd(),
            [
                _make_event(id=1, event_type="order_accepted"),
                _make_event(id=2, event_type="order_rejected", error="post-ack reject"),
            ],
        )
        assert advance is not None
        assert advance.status == TradeCommandStatusEnum.REJECTED

    def test_non_rejected_terminal_is_never_superseded(self) -> None:
        """Only rejections have a legal retry path.

        Given: order_terminal canceled (id=1) then order_accepted (id=2,
            an out-of-order or anomalous write),
        When: the fold runs,
        Then: target stays CANCELLED — terminal-with-evidence commands
            are never retried, so later accept rows cannot resurrect.
        """
        advance = _fold_lifecycle_advance(
            _make_cmd(),
            [
                _make_event(id=1, event_type="order_terminal", status="canceled"),
                _make_event(id=2, event_type="order_accepted"),
            ],
        )
        assert advance is not None
        assert advance.status == TradeCommandStatusEnum.CANCELLED

    def test_fill_status_filled_is_authoritative_completeness(self) -> None:
        """The executor-written fill status decides completeness.

        Given: a fill_observed row with status filled whose cum sits
            inside the executor's looser tolerance but outside the
            fold's strict cum-vs-quantity check (0.9999995 of 1.0),
        When: the fold runs,
        Then: target is FILLED, matching the executor's live decision.
        """
        advance = _fold_lifecycle_advance(
            _make_cmd(),
            [_make_event(event_type="fill_observed", cum_fill_size=0.9999995, status="filled")],
        )
        assert advance is not None
        assert advance.status == TradeCommandStatusEnum.FILLED

    def test_rank_monotonicity_blocks_regression(self) -> None:
        """A target that does not outrank the current status is a no-op.

        Given: an already-accepted command and only accept evidence,
        When: the fold runs,
        Then: no advance is produced (equal rank never re-applies).
        """
        advance = _fold_lifecycle_advance(
            _make_cmd(status="accepted"), [_make_event(event_type="order_accepted")]
        )
        assert advance is None


class TestReconcileCycleFold:
    """One-cycle integration of the fold inside _reconcile_cycle."""

    @pytest.mark.asyncio
    async def test_cycle_advances_command_from_evidence(self) -> None:
        """A dispatched command with accept evidence advances durably.

        Given: one dispatched create command and an order_accepted event,
        When: one reconciliation cycle runs,
        Then: advance_trade_command_lifecycle is called with ACCEPTED and
            the command's own session/sequence, and success is recorded.
        """
        received = datetime.now(UTC)
        cmd = _make_cmd()
        repo = _make_repo(
            cmds=[cmd], events=[_make_event(received_at=received, exchange_order_id="ex-9")]
        )
        recon, trade_svc = _make_loop(repo)
        await recon._reconcile_cycle()
        kwargs = repo.advance_trade_command_lifecycle.await_args.kwargs
        assert kwargs["public_id"] == "cmd-1"
        assert kwargs["expected_status"] == "dispatched"
        assert kwargs["new_status"] == TradeCommandStatusEnum.ACCEPTED
        assert kwargs["session_id"] == "s1"
        assert kwargs["sequence_id"] == 7
        assert kwargs["acked_at"] == received
        assert kwargs["exchange_order_id"] == "ex-9"
        trade_svc.record_recon_success.assert_called_with("kraken.BTC-USD.live")

    @pytest.mark.asyncio
    async def test_cycle_skips_non_fold_command_types(self) -> None:
        """Cancel commands are outside the fold's scope.

        Given: one active cancel command only,
        When: one reconciliation cycle runs,
        Then: no lifecycle events are read and no advance happens.
        """
        repo = _make_repo(cmds=[_make_cmd(command_type="cancel")])
        recon, _ = _make_loop(repo)
        await recon._reconcile_cycle()
        repo.get_order_lifecycle_events.assert_not_awaited()
        repo.advance_trade_command_lifecycle.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_cycle_skips_created_commands(self) -> None:
        """CREATED rows stay the outbox's territory.

        Given: a created command with accept evidence,
        When: one reconciliation cycle runs,
        Then: the fold never touches it.
        """
        repo = _make_repo(cmds=[_make_cmd(status="created")], events=[_make_event()])
        recon, _ = _make_loop(repo)
        await recon._reconcile_cycle()
        repo.advance_trade_command_lifecycle.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_cycle_ignores_events_without_client_order_id(self) -> None:
        """Events lacking a client order id cannot be attributed.

        Given: a dispatched command and an event row with cid None,
        When: one reconciliation cycle runs,
        Then: no advance happens.
        """
        repo = _make_repo(cmds=[_make_cmd()], events=[_make_event(client_order_id=None)])
        recon, _ = _make_loop(repo)
        await recon._reconcile_cycle()
        repo.advance_trade_command_lifecycle.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_cycle_survives_lost_cas(self) -> None:
        """A lost lifecycle CAS is a clean skip, not an error.

        Given: an advance whose CAS returns False (row moved concurrently),
        When: one reconciliation cycle runs,
        Then: the cycle completes and records success.
        """
        repo = _make_repo(cmds=[_make_cmd()], events=[_make_event()])
        repo.advance_trade_command_lifecycle = AsyncMock(return_value=False)
        recon, trade_svc = _make_loop(repo)
        await recon._reconcile_cycle()
        repo.advance_trade_command_lifecycle.assert_awaited_once()
        trade_svc.record_recon_success.assert_called()

    @pytest.mark.asyncio
    async def test_stale_without_evidence_warns(self) -> None:
        """An over-age command with zero evidence is the true anomaly.

        Given: a dispatched command created long ago with no events,
        When: one reconciliation cycle runs,
        Then: the cycle completes (WARN path) without any advance.
        """
        old = datetime.now(UTC) - timedelta(seconds=500)
        repo = _make_repo(cmds=[_make_cmd(created_at=old)])
        recon, trade_svc = _make_loop(repo)
        await recon._reconcile_cycle()
        repo.advance_trade_command_lifecycle.assert_not_awaited()
        trade_svc.record_recon_success.assert_called()

    @pytest.mark.asyncio
    async def test_stale_with_unknown_only_evidence_is_info(self) -> None:
        """Unknown-only evidence reports at INFO, never WARN.

        Given: an over-age command whose only event is order_submit_unknown,
        When: one reconciliation cycle runs,
        Then: the cycle completes without an advance (INFO path).
        """
        old = datetime.now(UTC) - timedelta(seconds=500)
        repo = _make_repo(
            cmds=[_make_cmd(created_at=old)],
            events=[_make_event(event_type="order_submit_unknown")],
        )
        recon, _ = _make_loop(repo)
        await recon._reconcile_cycle()
        repo.advance_trade_command_lifecycle.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_stale_cancel_command_keeps_legacy_warn(self) -> None:
        """The create's evidence cannot silence a stuck cancel command.

        Given: an over-age CANCEL command sharing the create's cid, with
            real accept evidence stored under that cid,
        When: one reconciliation cycle runs,
        Then: the stale report receives an EMPTY event list for the
            cancel (legacy WARN), not the create's evidence.
        """
        old = datetime.now(UTC) - timedelta(seconds=500)
        cancel_cmd = _make_cmd(command_type="cancel", created_at=old)
        create_cmd = _make_cmd(public_id="cmd-2", created_at=old, status="accepted")
        repo = _make_repo(cmds=[cancel_cmd, create_cmd], events=[_make_event()])
        recon, _ = _make_loop(repo)
        report_stale = MagicMock(return_value=True)
        recon._report_stale = report_stale
        await recon._reconcile_cycle()
        events_by_call = {
            call.args[0]["command_type"]: call.args[2] for call in report_stale.call_args_list
        }
        assert events_by_call["cancel"] == []
        assert len(events_by_call["create"]) == 1

    @pytest.mark.asyncio
    async def test_stale_with_real_evidence_is_silent(self) -> None:
        """Evidence-bearing over-age commands are healthy open orders.

        Given: an over-age ACCEPTED command with accept evidence only,
        When: one reconciliation cycle runs,
        Then: it is neither WARNed nor advanced (rank-equal target).
        """
        old = datetime.now(UTC) - timedelta(seconds=500)
        repo = _make_repo(
            cmds=[_make_cmd(created_at=old, status="accepted")], events=[_make_event()]
        )
        recon, trade_svc = _make_loop(repo)
        await recon._reconcile_cycle()
        repo.advance_trade_command_lifecycle.assert_not_awaited()
        trade_svc.record_recon_success.assert_called()


@pytest.mark.asyncio
async def test_reconcile_cycle_records_success() -> None:
    """Successful reconciliation cycle records success on trade service.

    Given: a ReconciliationLoop with a repo returning one fresh command,
    When: one reconciliation cycle runs,
    Then: trade_service.record_recon_success is called.
    """
    repo = _make_repo(cmds=[_make_cmd()])
    trade_svc = MagicMock(spec=TradeService)
    recon = ReconciliationLoop(
        exchange_name="kraken", repository=repo, trade_service=trade_svc, interval_seconds=0.01
    )
    task = asyncio.create_task(recon.run())
    await asyncio.sleep(0.05)
    recon.stop()
    await asyncio.wait_for(task, timeout=1.0)
    trade_svc.record_recon_success.assert_called_with("kraken.BTC-USD.live")


@pytest.mark.asyncio
async def test_reconcile_cycle_records_failure_on_error() -> None:
    """Reconciliation cycle records failure when DB query raises.

    Given: a ReconciliationLoop with a repo that raises on query,
    When: one reconciliation cycle runs,
    Then: trade_service.record_recon_failure is called.
    """
    repo = AsyncMock()
    repo.get_active_commands_for_exchange = AsyncMock(side_effect=RuntimeError("DB error"))
    trade_svc = MagicMock(spec=TradeService)
    trade_svc.known_shard_keys.return_value = {"kraken.BTC-USD.live"}
    trade_svc.record_recon_failure = MagicMock(return_value=False)
    recon = ReconciliationLoop(
        exchange_name="kraken", repository=repo, trade_service=trade_svc, interval_seconds=0.01
    )
    task = asyncio.create_task(recon.run())
    await asyncio.sleep(0.05)
    recon.stop()
    await asyncio.wait_for(task, timeout=1.0)
    trade_svc.record_recon_failure.assert_called_with("kraken.BTC-USD.live")


@pytest.mark.asyncio
async def test_reconcile_detects_stale_commands() -> None:
    """Reconciliation detects commands older than 3x interval.

    Given: a ReconciliationLoop with a command created 200s ago and interval=60s,
    When: one reconciliation cycle runs,
    Then: the stale command is logged (cycle still succeeds).
    """
    old_cmd = _make_cmd(public_id="cmd-old", created_at=datetime.now(UTC) - timedelta(seconds=200))
    repo = _make_repo(cmds=[old_cmd])
    trade_svc = MagicMock(spec=TradeService)
    recon = ReconciliationLoop(
        exchange_name="kraken", repository=repo, trade_service=trade_svc, interval_seconds=0.01
    )
    task = asyncio.create_task(recon.run())
    await asyncio.sleep(0.05)
    recon.stop()
    await asyncio.wait_for(task, timeout=1.0)
    trade_svc.record_recon_success.assert_called()


@pytest.mark.asyncio
async def test_reconcile_cancellation() -> None:
    """Reconciliation loop handles task cancellation cleanly.

    Given: a running ReconciliationLoop,
    When: the asyncio task is cancelled,
    Then: CancelledError propagates cleanly.
    """
    repo = _make_repo()
    trade_svc = MagicMock(spec=TradeService)
    recon = ReconciliationLoop(
        exchange_name="kraken", repository=repo, trade_service=trade_svc, interval_seconds=10.0
    )
    task = asyncio.create_task(recon.run())
    await asyncio.sleep(0.02)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


@pytest.mark.asyncio
async def test_reconcile_skips_terminal_commands() -> None:
    """Reconciliation skips commands in terminal status.

    Given: a ReconciliationLoop with a filled command,
    When: one reconciliation cycle runs,
    Then: the terminal command is skipped (not flagged as stale).
    """
    terminal_cmd = _make_cmd(
        public_id="cmd-done",
        status="filled",
        created_at=datetime.now(UTC) - timedelta(seconds=9999),
    )
    repo = _make_repo(cmds=[terminal_cmd])
    trade_svc = MagicMock(spec=TradeService)
    recon = ReconciliationLoop(
        exchange_name="kraken", repository=repo, trade_service=trade_svc, interval_seconds=0.01
    )
    task = asyncio.create_task(recon.run())
    await asyncio.sleep(0.05)
    recon.stop()
    await asyncio.wait_for(task, timeout=1.0)
    trade_svc.record_recon_success.assert_called()


@pytest.mark.asyncio
async def test_reconcile_failure_triggers_halt() -> None:
    """Repeated reconciliation failures trigger shard halt.

    Given: a ReconciliationLoop where record_recon_failure returns True (halt),
    When: reconciliation cycle fails,
    Then: the shard is halted via trade_service.
    """
    repo = AsyncMock()
    repo.get_active_commands_for_exchange = AsyncMock(side_effect=RuntimeError("DB down"))
    trade_svc = MagicMock(spec=TradeService)
    trade_svc.known_shard_keys.return_value = {"kraken.BTC-USD.live"}
    trade_svc.record_recon_failure = MagicMock(return_value=True)
    recon = ReconciliationLoop(
        exchange_name="kraken", repository=repo, trade_service=trade_svc, interval_seconds=0.01
    )
    task = asyncio.create_task(recon.run())
    await asyncio.sleep(0.05)
    recon.stop()
    await asyncio.wait_for(task, timeout=1.0)
    trade_svc.record_recon_failure.assert_called_with("kraken.BTC-USD.live")


@pytest.mark.asyncio
async def test_reconcile_failure_does_not_target_unknown_phantom_shard() -> None:
    """Failure recording targets known real shards, never the legacy unknown shard.

    Given: known in-memory shards for kraken and kraken_futures,
    When: the kraken reconciliation scan fails before loading commands,
    Then: only the kraken real shard receives a failure count.
    """
    repo = AsyncMock()
    repo.get_active_commands_for_exchange = AsyncMock(side_effect=RuntimeError("DB down"))
    trade_svc = MagicMock(spec=TradeService)
    trade_svc.known_shard_keys.return_value = {
        "kraken.BTC-USD.live",
        "kraken_futures.PF_XBTUSD.live",
    }
    trade_svc.record_recon_failure = MagicMock(return_value=False)
    recon = ReconciliationLoop(
        exchange_name="kraken",
        repository=repo,
        trade_service=trade_svc,
        interval_seconds=60.0,
    )
    await recon._reconcile_cycle()
    recorded = {call.args[0] for call in trade_svc.record_recon_failure.call_args_list}
    assert recorded == {"kraken.BTC-USD.live"}
    assert "kraken.unknown.live" not in recorded


@pytest.mark.asyncio
async def test_reconcile_failure_without_known_real_shards_records_nothing() -> None:
    """Failure recording does not fabricate a shard when none is known.

    Given: no known shards and no prior successful cycle,
    When: the reconciliation scan fails before loading commands,
    Then: no reconciliation failure is recorded on a phantom shard.
    """
    repo = AsyncMock()
    repo.get_active_commands_for_exchange = AsyncMock(side_effect=RuntimeError("DB down"))
    trade_svc = MagicMock(spec=TradeService)
    trade_svc.known_shard_keys.return_value = set()
    trade_svc.record_recon_failure = MagicMock(return_value=False)
    recon = ReconciliationLoop(
        exchange_name="kraken",
        repository=repo,
        trade_service=trade_svc,
        interval_seconds=60.0,
    )
    await recon._reconcile_cycle()
    trade_svc.record_recon_failure.assert_not_called()


@pytest.mark.asyncio
async def test_reconcile_failure_uses_last_seen_real_shards() -> None:
    """A scan failure still marks shards seen in the previous successful cycle.

    Given: a successful scan previously observed a real kraken shard,
    When: a later cycle fails and the trade service reports no known shards,
    Then: the previously seen real shard receives the failure count.
    """
    repo = _make_repo(cmds=[_make_cmd(shard_key="kraken.ETH-USD.live")])
    recon, trade_svc = _make_loop(repo)
    trade_svc.known_shard_keys.return_value = set()
    await recon._reconcile_cycle()
    repo.get_active_commands_for_exchange = AsyncMock(side_effect=RuntimeError("DB down"))
    trade_svc.record_recon_failure = MagicMock(return_value=False)
    await recon._reconcile_cycle()
    trade_svc.record_recon_failure.assert_called_with("kraken.ETH-USD.live")


@pytest.mark.asyncio
async def test_reconcile_fresh_command_not_stale() -> None:
    """Fresh non-terminal command is not flagged as stale.

    Given: a ReconciliationLoop with a recently created dispatched command,
    When: one reconciliation cycle runs,
    Then: the command is not flagged as stale and cycle succeeds.
    """
    repo = _make_repo(cmds=[_make_cmd(public_id="cmd-fresh")])
    trade_svc = MagicMock(spec=TradeService)
    recon = ReconciliationLoop(
        exchange_name="kraken", repository=repo, trade_service=trade_svc, interval_seconds=0.01
    )
    task = asyncio.create_task(recon.run())
    await asyncio.sleep(0.05)
    recon.stop()
    await asyncio.wait_for(task, timeout=1.0)
    trade_svc.record_recon_success.assert_called()


class TestFalseRejectionResurrection:
    """Durable backstop restoring falsely-rejected commands (#145 E2)."""

    @pytest.mark.asyncio
    async def test_rejected_row_with_live_evidence_is_resurrected(self) -> None:
        """A REJECTED row with later live evidence CAS-es back to ACCEPTED.

        Given: the resurrection query returning one rejected command,
        When: one reconciliation cycle runs,
        Then: the lifecycle CAS restores it with the terminal stamp
            cleared, ready for the next cycle's fold.
        """
        repo = _make_repo()
        rejected = _make_cmd(status="rejected")
        repo.get_rejected_commands_with_later_live_evidence = AsyncMock(return_value=[rejected])
        recon, trade_svc = _make_loop(repo)
        await recon._reconcile_cycle()
        kwargs = repo.advance_trade_command_lifecycle.await_args.kwargs
        assert kwargs["expected_status"] == TradeCommandStatusEnum.REJECTED
        assert kwargs["new_status"] == TradeCommandStatusEnum.ACCEPTED
        assert kwargs["clear_terminal_at"] is True
        assert kwargs["public_id"] == "cmd-1"

    @pytest.mark.asyncio
    async def test_resurrection_respects_ownership(self) -> None:
        """Foreign-shard rejected rows are left to their owning instance.

        Given: a resurrection candidate on a foreign shard under N=2,
        When: one reconciliation cycle runs,
        Then: no CAS is attempted.
        """
        foreign_shard = "kraken.FOREIGN.live"
        foreign_owner = ShardOwnership._hash(foreign_shard) % 2
        repo = _make_repo()
        repo.get_rejected_commands_with_later_live_evidence = AsyncMock(
            return_value=[_make_cmd(status="rejected", shard_key=foreign_shard)]
        )
        trade_svc = MagicMock(spec=TradeService)
        recon = ReconciliationLoop(
            exchange_name="kraken",
            repository=repo,
            trade_service=trade_svc,
            interval_seconds=60.0,
            ownership=ShardOwnership(instance_id=(foreign_owner + 1) % 2, instance_count=2),
        )
        await recon._reconcile_cycle()
        repo.advance_trade_command_lifecycle.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_lost_resurrection_cas_is_clean_skip(self) -> None:
        """A lost resurrection CAS skips without error.

        Given: a resurrection candidate whose CAS returns False,
        When: one reconciliation cycle runs,
        Then: the cycle completes and records success.
        """
        repo = _make_repo()
        repo.get_rejected_commands_with_later_live_evidence = AsyncMock(
            return_value=[_make_cmd(status="rejected")]
        )
        repo.advance_trade_command_lifecycle = AsyncMock(return_value=False)
        recon, _ = _make_loop(repo)
        await recon._reconcile_cycle()
        repo.advance_trade_command_lifecycle.assert_awaited_once()
