"""Tests for ReconciliationLoop — periodic reconciliation + lifecycle fold."""

import asyncio
from collections.abc import Iterator
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from typing import Any
from typing import cast
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import pytest
from loguru import logger

from snapper.application.notify.rules.critical_system_error import CriticalSystemErrorRule
from snapper.application.trade import reconciler as reconciler_module
from snapper.application.trade.cancel_target import CancelHydrationStamp
from snapper.application.trade.cancel_target import hydrate_awaiting_target_cancel
from snapper.application.trade.cancel_target import resolve_and_hydrate_awaiting_target_cancel
from snapper.application.trade.reconciler import DEFAULT_AWAITING_TARGET_STALE_THRESHOLD_S
from snapper.application.trade.reconciler import ReconciliationLoop
from snapper.application.trade.reconciler import fold_lifecycle_advance
from snapper.application.trade.trade_service import TradeService
from snapper.core.partitioning import ShardOwnership
from snapper.core.types import HealthStatusEnum
from snapper.core.types import TradeCommandStatusEnum
from snapper.data.repository_types import TradeCommandRow
from snapper.data.repository_types import VenueEventRow
from snapper.messaging.schemas.data import HeartbeatData
from snapper.messaging.topics.validation import validate_topic


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
    repo.get_exchange_order_id_for_client_order_id = AsyncMock(return_value=None)
    repo.get_active_create_command_by_client_order_id = AsyncMock(return_value=None)
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
        assert fold_lifecycle_advance(_make_cmd(), []) is None

    def test_accepted_event_advances_to_accepted(self) -> None:
        """An order_accepted row folds to ACCEPTED with ack fields.

        Given: a dispatched command with one order_accepted event,
        When: the fold runs,
        Then: target is ACCEPTED carrying acked_at and exchange_order_id.
        """
        received = datetime.now(UTC)
        advance = fold_lifecycle_advance(
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
        advance = fold_lifecycle_advance(
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
        advance = fold_lifecycle_advance(
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
        advance = fold_lifecycle_advance(
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
        advance = fold_lifecycle_advance(
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
        advance = fold_lifecycle_advance(
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
        advance = fold_lifecycle_advance(
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
        advance = fold_lifecycle_advance(
            _make_cmd(), [_make_event(event_type="order_breaker_open")]
        )
        assert advance is not None
        assert advance.status == TradeCommandStatusEnum.FAILED
        assert advance.last_error == "circuit_breaker_open"

    def test_interlock_blocked_event_advances_to_failed(self) -> None:
        """An order_interlock_blocked row folds to FAILED (interlock, not venue).

        Given: a dispatched command with an interlock-blocked event carrying
            the interlock's error text,
        When: the fold runs,
        Then: target is FAILED with the interlock's error and terminal_at —
            the live-trading interlock rejects the submit before the venue.
        """
        received = datetime.now(UTC)
        advance = fold_lifecycle_advance(
            _make_cmd(),
            [
                _make_event(
                    event_type="order_interlock_blocked",
                    error="live trading disabled",
                    received_at=received,
                )
            ],
        )
        assert advance is not None
        assert advance.status == TradeCommandStatusEnum.FAILED
        assert advance.last_error == "live trading disabled"
        assert advance.terminal_at == received

    def test_interlock_blocked_without_error_gets_fallback_reason(self) -> None:
        """An interlock rejection without error text still records a reason.

        Given: an order_interlock_blocked event whose error field is None,
        When: the fold runs,
        Then: last_error falls back to the generic order_interlock_blocked note.
        """
        advance = fold_lifecycle_advance(
            _make_cmd(), [_make_event(event_type="order_interlock_blocked", error=None)]
        )
        assert advance is not None
        assert advance.status == TradeCommandStatusEnum.FAILED
        assert advance.last_error == "order_interlock_blocked"

    def test_interlock_blocked_is_never_superseded_by_later_live_evidence(self) -> None:
        """An interlock-killed command is never resurrected by a stray accept.

        Given: order_interlock_blocked (id=1) then order_accepted (id=2, an
            out-of-order or anomalous write),
        When: the fold runs,
        Then: target stays FAILED — unlike a plain REJECTED, a FAILED interlock
            terminal is never cleared by later live evidence, so a late accept
            cannot resurrect a command the interlock already killed.
        """
        advance = fold_lifecycle_advance(
            _make_cmd(),
            [
                _make_event(id=1, event_type="order_interlock_blocked"),
                _make_event(id=2, event_type="order_accepted"),
            ],
        )
        assert advance is not None
        assert advance.status == TradeCommandStatusEnum.FAILED

    def test_terminal_event_maps_known_statuses(self) -> None:
        """An order_terminal row maps its status onto the command enum.

        Given: a terminal event with status canceled,
        When: the fold runs,
        Then: target is CANCELLED with no error note.
        """
        advance = fold_lifecycle_advance(
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
        advance = fold_lifecycle_advance(
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
        advance = fold_lifecycle_advance(
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
        advance = fold_lifecycle_advance(
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
        advance = fold_lifecycle_advance(
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
        advance = fold_lifecycle_advance(
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
        advance = fold_lifecycle_advance(
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
        advance = fold_lifecycle_advance(
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
        advance = fold_lifecycle_advance(
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
        advance = fold_lifecycle_advance(
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

        Given: one active cancel command only, whose target has no create
            or submit command,
        When: one reconciliation cycle runs,
        Then: no lifecycle events are read, the retirement pass looks the
            target up only, and no advance happens.
        """
        repo = _make_repo(cmds=[_make_cmd(command_type="cancel")])
        repo.get_active_create_command_by_client_order_id = AsyncMock(return_value=None)
        repo.get_latest_venue_event_received_at = AsyncMock(return_value=None)
        recon, _ = _make_loop(repo)
        await recon._reconcile_cycle()
        repo.get_order_lifecycle_events.assert_not_awaited()
        repo.get_active_create_command_by_client_order_id.assert_awaited_once_with(
            "cid-1", "kraken"
        )
        repo.get_latest_venue_event_received_at.assert_not_awaited()
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


async def _run_loop_until_called(recon: ReconciliationLoop, expected: MagicMock) -> None:
    """Run the real loop until the expected trade-service call happened, then stop it.

    The loop sleeps its interval before every cycle, so a fixed wall-clock sleep races
    the first cycle on a loaded parallel worker. Polling the mock with a bounded deadline
    keeps every loop test deterministic without changing the loop's own timing.

    Args:
        recon: Loop under test, started here and stopped before returning.
        expected: Trade-service mock method the first completed cycle must call.

    Raises:
        AssertionError: When the loop exits cleanly without calling ``expected``.
        TimeoutError: When no cycle made the expected call within five seconds.
    """
    task = asyncio.create_task(recon.run())
    awaited = False
    try:
        async with asyncio.timeout(5.0):
            while not expected.called and not task.done():
                await asyncio.sleep(0.005)
        if task.done() and not expected.called:
            awaited = True
            await task
            raise AssertionError("ReconciliationLoop exited without calling expected mock")
    finally:
        recon.stop()
        if not awaited:
            await asyncio.wait_for(task, timeout=5.0)


@pytest.fixture
def loguru_warnings() -> Iterator[list[str]]:
    """Capture WARNING-and-above loguru messages for reconciliation assertions."""
    messages: list[str] = []

    def _sink(message: object) -> None:
        """Collect one rendered loguru record."""
        messages.append(str(message))

    handler_id = logger.add(_sink, format="{level.name}|{message}", level="WARNING")
    try:
        yield messages
    finally:
        logger.remove(handler_id)


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
    await _run_loop_until_called(recon, trade_svc.record_recon_success)
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
    await _run_loop_until_called(recon, trade_svc.record_recon_failure)
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
    await _run_loop_until_called(recon, trade_svc.record_recon_success)
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
    """Reconciliation skips advancing commands in terminal status.

    In production the repository query ``get_active_commands_for_exchange`` excludes
    terminal rows before ``ReconciliationLoop`` receives them; the loop has no
    separate terminal-status filter for stale reporting (only ``_command_advance``
    skips statuses outside ``_FOLD_SOURCE_STATUSES``).

    Given: a ReconciliationLoop with a filled command,
    When: one reconciliation cycle runs,
    Then: the terminal command is not advanced and the cycle records success.
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
    await _run_loop_until_called(recon, trade_svc.record_recon_success)
    trade_svc.record_recon_success.assert_called()


@pytest.mark.asyncio
async def test_reconcile_failure_triggers_halt(loguru_warnings: list[str]) -> None:
    """Repeated reconciliation failures trigger shard halt.

    Given: a ReconciliationLoop where record_recon_failure returns True (halt),
    When: reconciliation cycle fails,
    Then: the shard is halted via trade_service and a HALTED error log is emitted.
    """
    repo = AsyncMock()
    repo.get_active_commands_for_exchange = AsyncMock(side_effect=RuntimeError("DB down"))
    trade_svc = MagicMock(spec=TradeService)
    trade_svc.known_shard_keys.return_value = {"kraken.BTC-USD.live"}
    trade_svc.record_recon_failure = MagicMock(return_value=True)
    recon = ReconciliationLoop(
        exchange_name="kraken", repository=repo, trade_service=trade_svc, interval_seconds=0.01
    )
    await _run_loop_until_called(recon, trade_svc.record_recon_failure)
    trade_svc.record_recon_failure.assert_called_with("kraken.BTC-USD.live")
    assert any(
        msg.startswith("ERROR|") and "kraken.BTC-USD.live" in msg and "HALTED" in msg
        for msg in loguru_warnings
    )


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
async def test_reconcile_fresh_command_not_stale(
    monkeypatch: pytest.MonkeyPatch, loguru_warnings: list[str]
) -> None:
    """Fresh non-terminal command is not flagged as stale.

    Given: a ReconciliationLoop with a dispatched command created at the frozen cycle time,
    When: one reconciliation cycle runs,
    Then: the command is not flagged as stale, no stale warning is logged, and cycle succeeds.
    """
    frozen_now = datetime(2026, 11, 10, 12, 0, tzinfo=UTC)

    class _FrozenDatetime:
        """Datetime stand-in pinning the reconciler cycle's now() read."""

        @staticmethod
        def now(tz: object = None) -> datetime:
            """Return the frozen UTC instant regardless of tz argument."""
            del tz
            return frozen_now

    monkeypatch.setattr(reconciler_module, "datetime", _FrozenDatetime)
    repo = _make_repo(cmds=[_make_cmd(public_id="cmd-fresh", created_at=frozen_now)])
    trade_svc = MagicMock(spec=TradeService)
    recon = ReconciliationLoop(
        exchange_name="kraken", repository=repo, trade_service=trade_svc, interval_seconds=0.01
    )
    await _run_loop_until_called(recon, trade_svc.record_recon_success)
    trade_svc.record_recon_success.assert_called()
    assert not any("stale" in msg.lower() for msg in loguru_warnings)


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


_CANCEL_CREATED_AT = datetime(2026, 11, 10, 12, 0, tzinfo=UTC)
_ENDED_AFTER = _CANCEL_CREATED_AT + timedelta(seconds=30)
_ENDED_BEFORE = _CANCEL_CREATED_AT - timedelta(seconds=30)

type _EvidenceKey = tuple[tuple[str, ...], tuple[str, ...] | None]


def _cancel_cmd(status: str = "dispatched") -> TradeCommandRow:
    """Build an over-age active cancel command sharing the target's cid.

    Args:
        status: Cancel command status.

    Returns:
        The cancel command row.
    """
    return _make_cmd(
        public_id="cancel-1",
        command_type="cancel",
        status=status,
        created_at=_CANCEL_CREATED_AT,
    )


def _retirement_repo(
    cmds: list[TradeCommandRow],
    target: TradeCommandRow | None,
    evidence: dict[_EvidenceKey, datetime],
) -> Any:
    """Build a repo whose target lookup and venue evidence are configured.

    Args:
        cmds: Active commands of the cycle.
        target: Create or submit command of the cancel's target, or ``None``.
        evidence: Newest ``received_at`` per (event types, statuses) query.

    Returns:
        The repository double.
    """
    repo = _make_repo(cmds=cmds)
    repo.get_active_create_command_by_client_order_id = AsyncMock(return_value=target)

    async def latest(
        client_order_id: str,
        event_types: tuple[str, ...],
        statuses: tuple[str, ...] | None = None,
    ) -> datetime | None:
        """Return the configured evidence time for one query shape."""
        return evidence.get((tuple(event_types), statuses))

    repo.get_latest_venue_event_received_at = AsyncMock(side_effect=latest)
    return repo


_CANCELED_TERMINAL: _EvidenceKey = (("order_terminal",), ("canceled", "cancelled"))
_FILLED_TERMINAL: _EvidenceKey = (("order_terminal",), ("filled", "closed"))
_FILL_OBSERVED: _EvidenceKey = (("fill_observed",), None)
_EXPIRED_TERMINAL: _EvidenceKey = (("order_terminal",), ("expired",))


class TestCancelRetirement:
    """Item G2: retire a cancel command whose target order has ended."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("target_status", "evidence", "new_status", "last_error", "terminal_at"),
        [
            (
                "cancelled",
                {_CANCELED_TERMINAL: _ENDED_AFTER},
                TradeCommandStatusEnum.CANCELLED,
                "target order ended canceled",
                _ENDED_AFTER,
            ),
            (
                "cancelled",
                {_CANCELED_TERMINAL: _ENDED_BEFORE},
                TradeCommandStatusEnum.REJECTED,
                "target order was already canceled",
                _ENDED_BEFORE,
            ),
            (
                "filled",
                {_FILLED_TERMINAL: _ENDED_BEFORE, _FILL_OBSERVED: _ENDED_AFTER},
                TradeCommandStatusEnum.REJECTED,
                "target order ended filled",
                _ENDED_BEFORE,
            ),
            (
                "filled",
                {_FILL_OBSERVED: _ENDED_AFTER},
                TradeCommandStatusEnum.REJECTED,
                "target order ended filled",
                _ENDED_AFTER,
            ),
            (
                "expired",
                {_EXPIRED_TERMINAL: _ENDED_AFTER},
                TradeCommandStatusEnum.REJECTED,
                "target order expired",
                _ENDED_AFTER,
            ),
        ],
    )
    async def test_table_row_retires_cancel(
        self,
        target_status: str,
        evidence: dict[_EvidenceKey, datetime],
        new_status: TradeCommandStatusEnum,
        last_error: str,
        terminal_at: datetime,
    ) -> None:
        """Each row of the G2 table retires the cancel from its target's evidence.

        Given: an over-age dispatched cancel whose target ended with venue
            evidence, a filled target either by a terminal or by fills only,
        When: one reconciliation cycle runs,
        Then: the cancel is CAS-ed with the table's status and error, the
            newest qualifying observation as terminal_at (not the cycle
            time), and it is left out of the stale report.
        """
        repo = _retirement_repo(
            [_cancel_cmd()],
            _make_cmd(public_id="create-1", status=target_status),
            evidence,
        )
        recon, trade_svc = _make_loop(repo)
        report_stale = MagicMock(return_value=True)
        recon._report_stale = report_stale
        await recon._reconcile_cycle()
        kwargs = repo.advance_trade_command_lifecycle.await_args.kwargs
        assert (kwargs["public_id"], kwargs["expected_status"]) == ("cancel-1", "dispatched")
        assert (kwargs["new_status"], kwargs["last_error"]) == (new_status, last_error)
        assert kwargs["terminal_at"] == terminal_at
        assert (kwargs["session_id"], kwargs["sequence_id"]) == ("s1", 7)
        report_stale.assert_not_called()
        trade_svc.record_recon_success.assert_called_with("kraken.BTC-USD.live")

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("target", "evidence"),
        [
            (None, {}),
            (_make_cmd(public_id="create-1", status="accepted"), {}),
            (_make_cmd(public_id="create-1", status="rejected"), {}),
            (_make_cmd(public_id="create-1", status="failed"), {}),
            (_make_cmd(public_id="create-1", status="cancelled"), {}),
            (_make_cmd(public_id="create-1", status="expired"), {}),
            (_make_cmd(public_id="create-1", status="filled"), {}),
        ],
    )
    async def test_cancel_stays_without_venue_evidenced_end(
        self, target: TradeCommandRow | None, evidence: dict[_EvidenceKey, datetime]
    ) -> None:
        """A cancel keeps its stale warning unless its target ended with venue evidence.

        Given: an over-age cancel whose target is missing, still open,
            rejected, failed, or terminal without venue evidence (cancelled
            by the guard scanner, expired by the outbox, filled locally),
        When: one reconciliation cycle runs,
        Then: no CAS is attempted and the cancel is still reported stale.
        """
        repo = _retirement_repo([_cancel_cmd()], target, evidence)
        recon, _ = _make_loop(repo)
        report_stale = MagicMock(return_value=True)
        recon._report_stale = report_stale
        await recon._reconcile_cycle()
        repo.advance_trade_command_lifecycle.assert_not_awaited()
        assert [call.args[0]["public_id"] for call in report_stale.call_args_list] == ["cancel-1"]

    @pytest.mark.asyncio
    async def test_raising_lookup_skips_the_cancel_this_cycle(self) -> None:
        """A failing target lookup skips the cancel without failing the cycle.

        Given: an over-age cancel whose target lookup raises,
        When: one reconciliation cycle runs,
        Then: no CAS is attempted, the cycle still records success, and the
            cancel is still reported stale.
        """
        repo = _retirement_repo([_cancel_cmd()], None, {})
        repo.get_active_create_command_by_client_order_id = AsyncMock(
            side_effect=RuntimeError("multiple active create/submit commands")
        )
        recon, trade_svc = _make_loop(repo)
        report_stale = MagicMock(return_value=True)
        recon._report_stale = report_stale
        await recon._reconcile_cycle()
        repo.advance_trade_command_lifecycle.assert_not_awaited()
        trade_svc.record_recon_success.assert_called_with("kraken.BTC-USD.live")
        assert report_stale.call_count == 1

    @pytest.mark.asyncio
    async def test_lost_retirement_cas_is_skipped_and_still_reported(self) -> None:
        """A lost CAS leaves the cancel for the next cycle and its stale report.

        Given: a cancel whose target ended with evidence, but whose CAS loses,
        When: one reconciliation cycle runs,
        Then: the cancel is still reported stale in this cycle.
        """
        repo = _retirement_repo(
            [_cancel_cmd()],
            _make_cmd(public_id="create-1", status="cancelled"),
            {_CANCELED_TERMINAL: _ENDED_AFTER},
        )
        repo.advance_trade_command_lifecycle = AsyncMock(return_value=False)
        recon, _ = _make_loop(repo)
        report_stale = MagicMock(return_value=True)
        recon._report_stale = report_stale
        await recon._reconcile_cycle()
        repo.advance_trade_command_lifecycle.assert_awaited_once()
        assert report_stale.call_count == 1

    @pytest.mark.asyncio
    async def test_created_cancel_is_untouched(self) -> None:
        """Pre-dispatch created cancel stays the outbox's territory.

        Given: an over-age cancel still in created status,
        When: one reconciliation cycle runs,
        Then: neither retirement nor hydration reads or advances it.
        """
        repo = _retirement_repo([_cancel_cmd(status="created")], None, {})
        recon, _ = _make_loop(repo)
        await recon._reconcile_cycle()
        repo.get_active_create_command_by_client_order_id.assert_not_awaited()
        repo.advance_trade_command_lifecycle.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_unresolved_awaiting_target_cancel_stays_held(self) -> None:
        """An awaiting_target cancel with no resolved venue id stays held.

        Given: an awaiting_target cancel whose target has no venue id yet,
        When: one reconciliation cycle runs,
        Then: target retirement evidence is not queried and lifecycle CAS does not run.
        """
        repo = _retirement_repo([_cancel_cmd(status="awaiting_target")], None, {})
        recon, _ = _make_loop(repo)
        await recon._reconcile_cycle()
        repo.get_latest_venue_event_received_at.assert_not_awaited()
        repo.advance_trade_command_lifecycle.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_cancel_only_cycle_retires_without_lifecycle_read(self) -> None:
        """A cycle in which only the cancel is active still retires it.

        Given: one accepted cancel as the only active command, its target
            cancelled with venue evidence after the cancel was created,
        When: one reconciliation cycle runs,
        Then: the cancel is retired as cancelled and the lifecycle fold
            reads no events.
        """
        repo = _retirement_repo(
            [_cancel_cmd(status="accepted")],
            _make_cmd(public_id="create-1", status="cancelled"),
            {_CANCELED_TERMINAL: _ENDED_AFTER},
        )
        recon, _ = _make_loop(repo)
        await recon._reconcile_cycle()
        repo.get_order_lifecycle_events.assert_not_awaited()
        kwargs = repo.advance_trade_command_lifecycle.await_args.kwargs
        assert kwargs["new_status"] == TradeCommandStatusEnum.CANCELLED
        assert kwargs["expected_status"] == "accepted"

    def test_terminal_event_of_a_confirmed_cancel_folds_the_create_to_cancelled(self) -> None:
        """The G1 terminal row advances the create command through the existing fold.

        Given: an accepted create command and the order_terminal row with
            status canceled that G1 records for a confirmed cancel,
        When: the lifecycle fold runs,
        Then: the create advances to cancelled with the terminal's time.
        """
        received = datetime(2026, 11, 10, 12, 0, tzinfo=UTC)
        advance = fold_lifecycle_advance(
            _make_cmd(status="accepted"),
            [
                _make_event(id=1, event_type="order_accepted", received_at=received),
                _make_event(
                    id=2, event_type="order_terminal", status="canceled", received_at=received
                ),
            ],
        )
        assert advance is not None
        assert advance.status == TradeCommandStatusEnum.CANCELLED
        assert advance.terminal_at == received


def _make_heartbeat_publisher() -> tuple[MagicMock, list[tuple[str, HeartbeatData]]]:
    """Create a typed publisher stub recording emitted heartbeat topics and frames."""
    sent: list[tuple[str, HeartbeatData]] = []
    seq = 0

    def _next_sequence(_stream: str) -> int:
        nonlocal seq
        seq += 1
        return seq

    tracker = MagicMock()
    tracker.session_id = "sess-hb"
    tracker.next_sequence = MagicMock(side_effect=_next_sequence)

    async def _send(topic: str, frame: HeartbeatData) -> None:
        sent.append((topic, frame))

    publisher = MagicMock()
    publisher.tracker = tracker
    publisher.send = AsyncMock(side_effect=_send)
    return publisher, sent


def _awaiting_target_cmd(public_id: str, age_seconds: float) -> TradeCommandRow:
    """Build an awaiting_target cancel row aged by ``age_seconds``."""
    return _make_cmd(
        public_id=public_id,
        command_type="cancel",
        status=TradeCommandStatusEnum.AWAITING_TARGET.value,
        created_at=datetime.now(UTC) - timedelta(seconds=age_seconds),
        plan_public_id="plan-1",
    )


class TestStaleAwaitingTargetMonitoring:
    """Periodic visibility, episode dedup, metrics, and health heartbeats for stuck holds."""

    def test_constructor_validates_held_cancel_monitor_configuration(self) -> None:
        """Threshold must be finite and strictly positive and cap at least 1.

        Given: ReconciliationLoop constructor arguments,
        When: The monitor receives invalid threshold or cap values,
        Then: ``ValueError`` is raised.
        """
        recon, _ = _make_loop(_make_repo())
        assert recon._awaiting_target_stale_threshold_s == DEFAULT_AWAITING_TARGET_STALE_THRESHOLD_S
        for bad in (0.0, -5.0, float("nan"), float("inf")):
            with pytest.raises(ValueError, match="finite and positive"):
                ReconciliationLoop(
                    "kraken", _make_repo(), MagicMock(spec=TradeService), stale_threshold_s=bad
                )
        with pytest.raises(ValueError, match="max_hydrate_per_cycle"):
            ReconciliationLoop(
                "kraken", _make_repo(), MagicMock(spec=TradeService), max_hydrate_per_cycle=0
            )

    @pytest.mark.asyncio
    async def test_fresh_awaiting_target_is_silent_and_publishes_healthy_heartbeat(
        self, loguru_warnings: list[str]
    ) -> None:
        """Held cancels younger than threshold do not trigger generic stale WARNs.

        Given: An awaiting_target cancel aged 300s (older than 3x interval, below 900s),
        When: Multiple reconciliation cycles run with a heartbeat publisher,
        Then: Zero WARNING logs are emitted, stale count is 0, and heartbeat is HEALTHY.
        """
        cmd = _awaiting_target_cmd("cancel-fresh", age_seconds=300.0)
        repo = _make_repo(cmds=[cmd])
        trade_svc = TradeService()
        publisher, sent = _make_heartbeat_publisher()
        recon = ReconciliationLoop(
            "kraken",
            repo,
            trade_svc,
            stale_threshold_s=900.0,
            msg_publisher=publisher,
        )

        await recon._reconcile_cycle()

        assert [msg for msg in loguru_warnings if msg.startswith("WARNING|")] == []
        assert recon.stale_awaiting_target_count == 0
        assert trade_svc.stale_awaiting_target_count == 0
        assert trade_svc.stale_awaiting_target_counts == {"kraken": 0}
        assert len(sent) == 1
        topic, frame = sent[0]
        valid, err = validate_topic(topic)
        assert valid, err
        assert topic == "system.heartbeats.reconciler.kraken"
        assert frame.component == "reconciler.kraken"
        assert frame.status == HealthStatusEnum.HEALTHY
        assert frame.meta["stale_awaiting_target_count"] == 0

    @pytest.mark.asyncio
    async def test_stale_awaiting_target_warns_once_per_row_per_episode_and_closes(
        self, loguru_warnings: list[str]
    ) -> None:
        """Stale held cancels emit one WARNING per row per episode and reopen cleanly.

        Given: Two awaiting_target cancels older than the 900s threshold,
        When: Cycle 1 and 2 run while stuck, cycle 3 sees them resolved, and cycle 4 sees
            the first row enter a fresh stale hold episode,
        Then: Cycle 1 logs 2 WARNINGs (one per row), cycle 2 logs 0 more, cycle 3 drops
            metric to 0 with HEALTHY heartbeat, and cycle 4 logs 1 WARNING for the new episode.
        """
        stale_one = _awaiting_target_cmd("cancel-stuck-1", age_seconds=950.0)
        stale_two = _awaiting_target_cmd("cancel-stuck-2", age_seconds=1000.0)
        repo = _make_repo(cmds=[stale_one, stale_two])
        trade_svc = TradeService()
        publisher, sent = _make_heartbeat_publisher()
        recon = ReconciliationLoop(
            "kraken",
            repo,
            trade_svc,
            stale_threshold_s=900.0,
            msg_publisher=publisher,
        )

        await recon._reconcile_cycle()
        warns = [m for m in loguru_warnings if m.startswith("WARNING|")]
        assert len(warns) == 2
        assert any("cancel-stuck-1" in m for m in warns)
        assert any("cancel-stuck-2" in m for m in warns)
        assert recon.stale_awaiting_target_count == 2
        assert trade_svc.stale_awaiting_target_count == 2
        assert sent[-1][1].status == HealthStatusEnum.WARNING
        assert sent[-1][1].meta["stale_awaiting_target_count"] == 2

        await recon._reconcile_cycle()
        assert len([m for m in loguru_warnings if m.startswith("WARNING|")]) == 2

        repo.get_active_commands_for_exchange = AsyncMock(return_value=[])
        await recon._reconcile_cycle()
        assert recon.stale_awaiting_target_count == 0
        assert trade_svc.stale_awaiting_target_count == 0
        assert sent[-1][1].status == HealthStatusEnum.HEALTHY

        repo.get_active_commands_for_exchange = AsyncMock(return_value=[stale_one])
        await recon._reconcile_cycle()
        warns_after_reopen = [m for m in loguru_warnings if m.startswith("WARNING|")]
        assert len(warns_after_reopen) == 3

    @pytest.mark.asyncio
    async def test_reconciler_heartbeat_feeds_critical_system_error_rule_without_flood(
        self,
    ) -> None:
        """Three WARNING ticks open one episode alert and three HEALTHY ticks close it.

        Given: A stale awaiting_target cancel driving reconciler heartbeats into
            ``CriticalSystemErrorRule``,
        When: Four WARNING cycles execute followed by three HEALTHY cycles,
        Then: Cycle 3 raises one deduplicated alert, cycle 4 stays silent, and cycles
            5-7 close cleanly.
        """
        stale = _awaiting_target_cmd("cancel-alert-1", age_seconds=1200.0)
        repo = _make_repo(cmds=[stale])
        publisher, sent = _make_heartbeat_publisher()
        recon = ReconciliationLoop(
            "kraken",
            repo,
            TradeService(),
            stale_threshold_s=900.0,
            msg_publisher=publisher,
        )
        notify_repo = AsyncMock()
        notify_repo.list_users_with_permission = AsyncMock(return_value=["admin-1"])
        notify_repo.list_alert_events_with_dedup_key = AsyncMock(return_value=[])
        rule = CriticalSystemErrorRule()
        t0 = datetime.now(UTC)

        alerts: list[object] = []
        for idx in range(4):
            await recon._reconcile_cycle()
            topic, frame = sent[-1]
            rows = await rule.evaluate(
                topic,
                frame.to_json().encode("utf-8"),
                notify_repo,
                t0 + timedelta(seconds=60 * idx),
            )
            alerts.extend(rows)

        assert len(alerts) == 1

        repo.get_active_commands_for_exchange = AsyncMock(return_value=[])
        for idx in range(3):
            await recon._reconcile_cycle()
            topic, frame = sent[-1]
            rows = await rule.evaluate(
                topic,
                frame.to_json().encode("utf-8"),
                notify_repo,
                t0 + timedelta(seconds=240 + 60 * idx),
            )
            assert rows == []
        assert rule._episode_closed(("reconciler", "kraken")) is True

    @pytest.mark.asyncio
    async def test_heartbeat_publish_failure_does_not_fail_reconcile_cycle(self) -> None:
        """A failing heartbeat send logs without tripping shard reconciliation failures.

        Given: A ReconciliationLoop whose heartbeat publisher raises on send,
        When: One reconciliation cycle runs,
        Then: ``record_recon_failure`` is not called.
        """
        cmd = _awaiting_target_cmd("cancel-err-hb", age_seconds=950.0)
        repo = _make_repo(cmds=[cmd])
        trade_svc = TradeService()
        publisher = MagicMock()
        publisher.tracker = MagicMock(session_id="s-err", next_sequence=MagicMock(return_value=1))
        publisher.send = AsyncMock(side_effect=RuntimeError("zmq down"))
        recon = ReconciliationLoop(
            "kraken",
            repo,
            trade_svc,
            stale_threshold_s=900.0,
            msg_publisher=publisher,
        )

        await recon._reconcile_cycle()

        assert recon.stale_awaiting_target_count == 1
        assert trade_svc._shards["kraken.BTC-USD.live"].recon_failure_count == 0


class TestRuntimeAwaitingTargetHydration:
    """Runtime hydration of held awaiting_target cancels inside ReconciliationLoop."""

    @pytest.mark.asyncio
    async def test_resolution_error_leaves_held_and_logs_once_per_episode(
        self, loguru_warnings: list[str]
    ) -> None:
        """A resolution failure leaves the row held, logs ERROR once, and heals on recovery.

        Given: An awaiting_target cancel whose venue-id lookup raises across cycles 1-2
            and resolves cleanly on cycle 3,
        When: Three reconciliation cycles execute,
        Then: Cycle 1 logs 1 ERROR, cycle 2 emits 0 additional ERRORs, the row stays held
            and triggers no recon_failure, and cycle 3 hydrates it to created.
        """
        held = _awaiting_target_cmd("cancel-err-res", age_seconds=950.0)
        repo = _make_repo(cmds=[held])
        repo.get_exchange_order_id_for_client_order_id = AsyncMock(
            side_effect=RuntimeError("lookup down")
        )
        trade_svc = TradeService()
        recon = ReconciliationLoop("kraken", repo, trade_svc, stale_threshold_s=900.0)

        await recon._reconcile_cycle()
        errors_c1 = [m for m in loguru_warnings if m.startswith("ERROR|")]
        assert len(errors_c1) == 1
        assert "cancel-err-res" in errors_c1[0]
        repo.advance_trade_command_lifecycle.assert_not_awaited()
        assert recon.stale_awaiting_target_count == 1

        await recon._reconcile_cycle()
        errors_c2 = [m for m in loguru_warnings if m.startswith("ERROR|")]
        assert len(errors_c2) == 1
        assert trade_svc._shards["kraken.BTC-USD.live"].recon_failure_count == 0

        repo.get_exchange_order_id_for_client_order_id = AsyncMock(return_value="ex-recovered")
        await recon._reconcile_cycle()
        kwargs = repo.advance_trade_command_lifecycle.await_args.kwargs
        assert kwargs["public_id"] == "cancel-err-res"
        assert kwargs["expected_status"] == TradeCommandStatusEnum.AWAITING_TARGET
        assert kwargs["new_status"] == TradeCommandStatusEnum.CREATED
        assert kwargs["exchange_order_id"] == "ex-recovered"
        assert recon.stale_awaiting_target_count == 0
        assert recon._hydration_error_logged == set()

    @pytest.mark.asyncio
    async def test_hydration_is_bounded_per_cycle(self) -> None:
        """Runtime hydration processes at most ``max_hydrate_per_cycle`` rows per pass.

        Given: Three awaiting_target cancels ready to resolve and ``max_hydrate_per_cycle=2``,
        When: One reconciliation cycle runs,
        Then: Exactly the first two are CAS-hydrated in cycle 1 and the third waits for cycle 2.
        """
        c1 = _awaiting_target_cmd("cancel-cap-1", age_seconds=30.0)
        c2 = _awaiting_target_cmd("cancel-cap-2", age_seconds=20.0)
        c3 = _awaiting_target_cmd("cancel-cap-3", age_seconds=10.0)
        repo = _make_repo(cmds=[c1, c2, c3])
        repo.get_exchange_order_id_for_client_order_id = AsyncMock(return_value="ex-cap")
        recon = ReconciliationLoop("kraken", repo, TradeService(), max_hydrate_per_cycle=2)

        await recon._reconcile_cycle()
        hydrated_pids = [
            call.kwargs["public_id"]
            for call in repo.advance_trade_command_lifecycle.await_args_list
        ]
        assert hydrated_pids == ["cancel-cap-1", "cancel-cap-2"]

        repo.get_active_commands_for_exchange = AsyncMock(return_value=[c3])
        await recon._reconcile_cycle()
        assert repo.advance_trade_command_lifecycle.await_args.kwargs["public_id"] == "cancel-cap-3"

    @pytest.mark.asyncio
    async def test_shared_hydration_helper_handles_cas_error_race_and_quiet_log(
        self, loguru_warnings: list[str]
    ) -> None:
        """Shared helper distinguishes race_lost, CAS error, and log_error=False.

        Given: A held cancel row and mocked repository,
        When: CAS raises with ``log_error=True`` vs ``False``, or returns ``False``,
        Then: ``error`` respects ``log_error`` and lost CAS returns ``race_lost``.
        """
        cmd = _awaiting_target_cmd("cancel-helper-1", age_seconds=50.0)
        stamp = CancelHydrationStamp(bus_time=datetime.now(UTC), session_id="s1", sequence_id=9)

        store = _make_repo()
        store.advance_trade_command_lifecycle = AsyncMock(side_effect=RuntimeError("cas boom"))
        assert (
            await hydrate_awaiting_target_cancel(store, cmd, "ex-1", stamp, log_error=True)
            == "error"
        )
        assert len([m for m in loguru_warnings if m.startswith("ERROR|")]) == 1

        assert (
            await hydrate_awaiting_target_cancel(store, cmd, "ex-1", stamp, log_error=False)
            == "error"
        )
        assert len([m for m in loguru_warnings if m.startswith("ERROR|")]) == 1

        store.get_exchange_order_id_for_client_order_id = AsyncMock(
            side_effect=RuntimeError("boom")
        )
        assert (
            await resolve_and_hydrate_awaiting_target_cancel(store, cmd, stamp, log_error=False)
            == "error"
        )
        assert len([m for m in loguru_warnings if m.startswith("ERROR|")]) == 1

        store.get_exchange_order_id_for_client_order_id = AsyncMock(return_value="ex-2")
        store.advance_trade_command_lifecycle = AsyncMock(return_value=False)
        assert await resolve_and_hydrate_awaiting_target_cancel(store, cmd, stamp) == "race_lost"
