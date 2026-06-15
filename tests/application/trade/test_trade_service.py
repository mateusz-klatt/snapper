"""Tests for TradeService — trade domain service."""

from collections import OrderedDict
from datetime import UTC
from datetime import datetime

import pytest

from snapper.application.trade.trade_service import FillProjection
from snapper.application.trade.trade_service import TradeService
from snapper.data.repository_types import AccrualLedgerRow
from snapper.data.repository_types import VenueEventRow

_USE_DEFAULT_EXEC_ID = "__default__"


def _make_venue_event(
    event_id: int,
    event_type: str = "fill_observed",
    shard_key: str = "kraken.BTC-USD.live",
    side: str | None = "buy",
    fill_price: float | None = 50000.0,
    fill_size: float | None = 0.5,
    status: str | None = "filled",
    exec_id: str | None = _USE_DEFAULT_EXEC_ID,
    trade_id: str | None = None,
    exchange_order_id: str | None = None,
) -> VenueEventRow:
    """Build a minimal VenueEventRow for testing.

    Given: default field values for a fill_observed event,
    When: caller overrides specific fields,
    Then: a complete VenueEventRow dict is returned.
    """
    now = datetime.now(UTC)
    return {
        "id": event_id,
        "public_id": f"ve-{event_id}",
        "timestamp": now,
        "session_id": "s1",
        "sequence_id": event_id,
        "event_type": event_type,
        "shard_key": shard_key,
        "command_public_id": None,
        "exchange": "kraken",
        "instrument": "BTC-USD",
        "mode": "live",
        "exchange_order_id": exchange_order_id,
        "client_order_id": None,
        "venue_client_id": None,
        "side": side,
        "status": status,
        "fill_price": fill_price,
        "fill_size": fill_size,
        "cum_fill_size": fill_size,
        "fee": 0.5,
        "fee_asset": "USD",
        "exec_id": f"exec-{event_id}" if exec_id == _USE_DEFAULT_EXEC_ID else exec_id,
        "trade_id": trade_id,
        "error": None,
        "venue_timestamp": now,
        "received_at": now,
    }


def test_get_position_returns_default() -> None:
    """Get position returns zero defaults for unknown shard.

    Given: a fresh TradeService with no applied events,
    When: get_position is called for a non-existent shard key,
    Then: position_qty is 0.0 and entry_price is None.
    """
    svc = TradeService()
    pos = svc.get_position("nonexistent.shard")
    assert pos.position_qty == 0.0
    assert pos.entry_price is None


def test_get_command_state_returns_default() -> None:
    """Get command state returns no in-flight for unknown shard.

    Given: a fresh TradeService with no registered commands,
    When: get_command_state is called for a non-existent shard key,
    Then: in_flight is False and command_public_id is None.
    """
    svc = TradeService()
    cmd = svc.get_command_state("nonexistent.shard")
    assert cmd.in_flight is False
    assert cmd.command_public_id is None


def test_known_shard_keys_returns_materialized_shards() -> None:
    """Known shard keys returns already materialized in-memory shards.

    Given: a fresh TradeService with two shards touched by read-model calls,
    When: known_shard_keys is called,
    Then: the returned set contains those shard keys and is a snapshot copy.
    """
    svc = TradeService()
    svc.get_position("kraken.BTC-USD.live")
    svc.get_command_state("kraken.ETH-USD.live")
    keys = svc.known_shard_keys()
    keys.add("mutated")
    assert svc.known_shard_keys() == {"kraken.BTC-USD.live", "kraken.ETH-USD.live"}


def test_apply_fill_buy() -> None:
    """Apply venue event processes a buy fill into position state.

    Given: a fresh TradeService with no position,
    When: a buy fill event for 0.5 BTC at 50000 is applied,
    Then: position_qty is 0.5 and entry_price is 50000.
    """
    svc = TradeService()
    event = _make_venue_event(
        event_id=1, side="buy", fill_price=50000.0, fill_size=0.5, status="filled"
    )
    svc.apply_venue_event(event)
    pos = svc.get_position("kraken.BTC-USD.live")
    assert pos.position_qty == 0.5
    assert pos.entry_price == 50000.0


def test_apply_fill_sell_clears_position() -> None:
    """Sell fill reduces position to zero and records realized PnL.

    Given: a TradeService with a 1.0 BTC long position at 50000,
    When: a sell fill for 1.0 BTC at 51000 is applied,
    Then: position_qty is 0.0, entry_price is None, and realized_pnl is 1000.
    """
    svc = TradeService()
    svc.apply_venue_event(
        _make_venue_event(event_id=1, side="buy", fill_price=50000.0, fill_size=1.0)
    )
    svc.apply_venue_event(
        _make_venue_event(
            event_id=2, side="sell", fill_price=51000.0, fill_size=1.0, exec_id="exec-2"
        )
    )
    pos = svc.get_position("kraken.BTC-USD.live")
    assert pos.position_qty == 0.0
    assert pos.entry_price is None
    assert pos.realized_pnl == 1000.0


def test_apply_fill_dedup() -> None:
    """Duplicate fills with the same exec_id are ignored.

    Given: a TradeService that already applied a fill with exec_id "exec-dup",
    When: a second fill event with the same exec_id is applied,
    Then: position_qty remains 0.5 (only the first fill counted).
    """
    svc = TradeService()
    event = _make_venue_event(event_id=1, exec_id="exec-dup")
    svc.apply_venue_event(event)
    event2 = _make_venue_event(event_id=2, exec_id="exec-dup")
    svc.apply_venue_event(event2)
    pos = svc.get_position("kraken.BTC-USD.live")
    assert pos.position_qty == 0.5


def test_apply_fill_dedup_cross_plane_trade_id() -> None:
    """A fill replayed across planes dedupes on the shared trade_id.

    Given: a fill first applied from the execution plane (trade_id only,
        exec_id None — the full-replay shape),
    When: the SAME fill is replayed from the venue-events plane carrying
        BOTH an exec_id and the same trade_id (the delta-replay shape after
        a conservative recovery watermark replays it),
    Then: position_qty stays 0.5 — the shared trade_id catches the duplicate
        even though the preferred exec_id key was never recorded by the
        execution-plane application.
    """
    svc = TradeService()
    svc.apply_venue_event(_make_venue_event(event_id=1, exec_id=None, trade_id="T-1"))
    svc.apply_venue_event(_make_venue_event(event_id=2, exec_id="E-1", trade_id="T-1"))
    pos = svc.get_position("kraken.BTC-USD.live")
    assert pos.position_qty == 0.5


def test_apply_fill_idless_fallback_key_is_identity_shaped() -> None:
    """Id-less fills dedupe by (client order, size, price), not row PK.

    Given: two venue events with NO exec/trade id carrying the same
        client_order_id, fill_size, and fill_price but DIFFERENT row ids
        (a recovery republish of the same Walutomat fill lands in replay
        next to the original row),
    When: both are applied,
    Then: only the first counts — a row-PK fallback key would apply both
        and double the position after a coordinator restart.
    """
    svc = TradeService()
    event = _make_venue_event(event_id=1, exec_id=None)
    svc.apply_venue_event(event)
    event2 = _make_venue_event(event_id=2, exec_id=None)
    svc.apply_venue_event(event2)
    pos = svc.get_position("kraken.BTC-USD.live")
    assert pos.position_qty == 0.5


def test_apply_fill_idless_distinct_quantities_both_apply() -> None:
    """Id-less fills with different sizes are NOT false-deduped.

    Given: two id-less venue events for the same order whose fill sizes
        differ (0.5 then 0.25),
    When: both are applied,
    Then: both count — the identity-shaped fallback key includes the
        quantity, so distinct fills never collide on it.
    """
    svc = TradeService()
    event = _make_venue_event(event_id=1, exec_id=None)
    svc.apply_venue_event(event)
    event2 = _make_venue_event(event_id=2, exec_id=None, fill_size=0.25)
    svc.apply_venue_event(event2)
    pos = svc.get_position("kraken.BTC-USD.live")
    assert pos.position_qty == pytest.approx(0.75)


def test_apply_order_accepted() -> None:
    """Order accepted event updates command state with exchange order id.

    Given: a fresh TradeService with no command state,
    When: an order_accepted venue event with exchange_order_id "ex-123" is applied,
    Then: command status is "accepted" and exchange_order_id is "ex-123".
    """
    svc = TradeService()
    event = _make_venue_event(
        event_id=1,
        event_type="order_accepted",
        exchange_order_id="ex-123",
        side=None,
        fill_price=None,
        fill_size=None,
        status=None,
    )
    svc.apply_venue_event(event)
    cmd = svc.get_command_state("kraken.BTC-USD.live")
    assert cmd.status == "accepted"
    assert cmd.exchange_order_id == "ex-123"


def test_apply_order_rejected_clears_in_flight() -> None:
    """Order rejected event clears the in-flight flag.

    Given: a TradeService with a registered in-flight command on a shard,
    When: an order_rejected venue event is applied for that shard,
    Then: in_flight is False and status is "rejected".
    """
    svc = TradeService()
    svc.register_command(
        "kraken.BTC-USD.live",
        {
            "public_id": "cmd-1",
            "timestamp": datetime.now(UTC),
            "session_id": "s1",
            "sequence_id": 1,
            "command_type": "submit",
            "shard_key": "kraken.BTC-USD.live",
            "exchange": "kraken",
            "instrument": "BTC-USD",
            "mode": "live",
            "strategy_id": "engine-buy",
            "client_order_id": "cid-1",
            "venue_client_id": "vcid-1",
            "idempotency_key": None,
            "side": "buy",
            "order_type": "market",
            "quantity": 0.5,
            "price": None,
            "stop_price": None,
            "leverage": None,
            "reduce_only": False,
            "status": "created",
            "attempt_count": 0,
            "last_error": None,
            "created_at": datetime.now(UTC),
            "dispatched_at": None,
            "acked_at": None,
            "terminal_at": None,
            "exchange_order_id": None,
            "supersedes_command_id": None,
            "correlation_id": "corr-1",
            "wallet_public_id": None,
            "operator_public_id": None,
            "user_public_id": None,
            "source_surface": "strategy",
        },
    )
    assert svc.get_command_state("kraken.BTC-USD.live").in_flight is True
    event = _make_venue_event(
        event_id=1,
        event_type="order_rejected",
        side=None,
        fill_price=None,
        fill_size=None,
        status=None,
    )
    svc.apply_venue_event(event)
    assert svc.get_command_state("kraken.BTC-USD.live").in_flight is False
    assert svc.get_command_state("kraken.BTC-USD.live").status == "rejected"


def test_apply_order_submit_unknown_holds_in_flight() -> None:
    """Ambiguous submit event holds command state and advances the watermark.

    Given: a TradeService with a registered in-flight command on a shard,
    When: an order_submit_unknown venue event is applied, followed by a
        replayed event with the same id and then a genuine rejection,
    Then: the unknown event leaves in_flight True and status unchanged,
        the same-id replay is deduped by the advanced watermark, and the
        later rejection still resolves the command terminally.
    """
    svc = TradeService()
    svc.register_command(
        "kraken.BTC-USD.live",
        {
            "public_id": "cmd-1",
            "timestamp": datetime.now(UTC),
            "session_id": "s1",
            "sequence_id": 1,
            "command_type": "submit",
            "shard_key": "kraken.BTC-USD.live",
            "exchange": "kraken",
            "instrument": "BTC-USD",
            "mode": "live",
            "strategy_id": "engine-buy",
            "client_order_id": "cid-1",
            "venue_client_id": "vcid-1",
            "idempotency_key": None,
            "side": "buy",
            "order_type": "market",
            "quantity": 0.5,
            "price": None,
            "stop_price": None,
            "leverage": None,
            "reduce_only": False,
            "status": "created",
            "attempt_count": 0,
            "last_error": None,
            "created_at": datetime.now(UTC),
            "dispatched_at": None,
            "acked_at": None,
            "terminal_at": None,
            "exchange_order_id": None,
            "supersedes_command_id": None,
            "correlation_id": "corr-1",
            "wallet_public_id": None,
            "operator_public_id": None,
            "user_public_id": None,
            "source_surface": "strategy",
        },
    )
    unknown_event = _make_venue_event(
        event_id=1,
        event_type="order_submit_unknown",
        side=None,
        fill_price=None,
        fill_size=None,
        status=None,
    )
    svc.apply_venue_event(unknown_event)
    cmd = svc.get_command_state("kraken.BTC-USD.live")
    assert cmd.in_flight is True
    assert cmd.status == "created"
    replay_reject_same_id = _make_venue_event(
        event_id=1,
        event_type="order_rejected",
        side=None,
        fill_price=None,
        fill_size=None,
        status=None,
    )
    svc.apply_venue_event(replay_reject_same_id)
    assert svc.get_command_state("kraken.BTC-USD.live").in_flight is True
    resolution = _make_venue_event(
        event_id=2,
        event_type="order_rejected",
        side=None,
        fill_price=None,
        fill_size=None,
        status=None,
    )
    svc.apply_venue_event(resolution)
    assert svc.get_command_state("kraken.BTC-USD.live").in_flight is False
    assert svc.get_command_state("kraken.BTC-USD.live").status == "rejected"


def test_watermark_prevents_reprocessing() -> None:
    """Events with id at or below the watermark are skipped.

    Given: a TradeService that already processed event_id=5,
    When: an event with event_id=3 is applied,
    Then: position_qty remains 0.5 (the older event was ignored).
    """
    svc = TradeService()
    svc.apply_venue_event(_make_venue_event(event_id=5))
    svc.apply_venue_event(_make_venue_event(event_id=3, exec_id="exec-earlier"))
    pos = svc.get_position("kraken.BTC-USD.live")
    assert pos.position_qty == 0.5


def test_restore_from_checkpoint() -> None:
    """Checkpoint restoration sets all shard state from persisted snapshot.

    Given: a fresh TradeService with no state,
    When: restore_from_checkpoint is called with position, cash, equity, and command data,
    Then: position, command, equity, and peak equity all reflect the restored values.
    """
    svc = TradeService()
    svc.restore_from_checkpoint(
        shard_key="kraken.BTC-USD.live",
        position_qty=1.5,
        entry_price=49000.0,
        cash=5000.0,
        peak_equity=12000.0,
        realized_pnl=500.0,
        turnover=50000.0,
        last_venue_event_id=100,
        open_command_ids=["cmd-42"],
        seen_exec_ids=OrderedDict.fromkeys(["exec-1", "exec-2"]),
    )
    pos = svc.get_position("kraken.BTC-USD.live")
    assert pos.position_qty == 1.5
    assert pos.entry_price == 49000.0
    cmd = svc.get_command_state("kraken.BTC-USD.live")
    assert cmd.in_flight is True
    assert cmd.command_public_id == "cmd-42"
    assert svc.get_equity("kraken.BTC-USD.live") == 5000.0
    assert svc.get_peak_equity("kraken.BTC-USD.live") == 12000.0


def test_circuit_breaker_halt() -> None:
    """Circuit breaker halts shard after reaching max reconciliation failures.

    Given: a TradeService with a shard that has recorded 2 reconciliation failures,
    When: a third failure is recorded with max_failures=3,
    Then: the shard is halted and record_recon_failure returns True.
    """
    svc = TradeService()
    svc.record_recon_failure("kraken.BTC-USD.live", max_failures=3)
    assert svc.is_halted("kraken.BTC-USD.live") is False
    svc.record_recon_failure("kraken.BTC-USD.live", max_failures=3)
    assert svc.is_halted("kraken.BTC-USD.live") is False
    halted = svc.record_recon_failure("kraken.BTC-USD.live", max_failures=3)
    assert halted is True
    assert svc.is_halted("kraken.BTC-USD.live") is True


def test_circuit_breaker_unhalt() -> None:
    """Operator can un-halt a shard that was halted by the circuit breaker.

    Given: a TradeService with a halted shard (3 consecutive failures),
    When: unhalt_shard is called for that shard,
    Then: the shard is no longer halted.
    """
    svc = TradeService()
    for _ in range(3):
        svc.record_recon_failure("kraken.BTC-USD.live", max_failures=3)
    assert svc.is_halted("kraken.BTC-USD.live") is True
    svc.unhalt_shard("kraken.BTC-USD.live")
    assert svc.is_halted("kraken.BTC-USD.live") is False


def test_recon_success_resets_counter() -> None:
    """Reconciliation success resets the failure counter to zero.

    Given: a TradeService with 2 consecutive reconciliation failures on a shard,
    When: record_recon_success is called followed by one more failure,
    Then: the shard is not halted because the counter was reset.
    """
    svc = TradeService()
    svc.record_recon_failure("kraken.BTC-USD.live", max_failures=3)
    svc.record_recon_failure("kraken.BTC-USD.live", max_failures=3)
    svc.record_recon_success("kraken.BTC-USD.live")
    halted = svc.record_recon_failure("kraken.BTC-USD.live", max_failures=3)
    assert halted is False


def test_reason_scoped_unhalt_releases_only_its_reason() -> None:
    """A reason-scoped un-halt releases exactly its halt source.

    Given: a shard halted for two distinct reasons,
    When: unhalt_shard runs with the first reason, then with the second,
    Then: the shard stays halted while any reason remains and un-halts only
        when the last one is released.
    """
    svc = TradeService()
    svc.halt_shard("kraken.BTC-USD.live", "paired-execution:w:s:k")
    svc.halt_shard("kraken.BTC-USD.live", "manual operator hold")
    svc.unhalt_shard("kraken.BTC-USD.live", "paired-execution:w:s:k")
    assert svc.is_halted("kraken.BTC-USD.live") is True
    svc.unhalt_shard("kraken.BTC-USD.live", "manual operator hold")
    assert svc.is_halted("kraken.BTC-USD.live") is False


def test_reason_scoped_unhalt_is_failsafe_for_unregistered_reason() -> None:
    """An un-halt for a reason that was never registered is a strict no-op.

    Given: a shard halted by the reconciliation circuit breaker (a dynamic
        reason string the paired guard never knows),
    When: unhalt_shard runs with a paired-execution reason key,
    Then: the shard stays halted — an automated paired un-halt can never clear
        someone else's halt.
    """
    svc = TradeService()
    for _ in range(3):
        svc.record_recon_failure("kraken.BTC-USD.live", max_failures=3)
    assert svc.is_halted("kraken.BTC-USD.live") is True
    svc.unhalt_shard("kraken.BTC-USD.live", "paired-execution:w:s:k")
    assert svc.is_halted("kraken.BTC-USD.live") is True


def test_reason_scoped_unhalt_keeps_recon_counter() -> None:
    """Releasing the last reason un-halts but never resets the recon counter.

    Given: a shard with 2 accumulated recon failures (not yet halted) that the
        paired guard then halts and releases,
    When: one more recon failure arrives after the reason-scoped release,
    Then: the shard halts at the 3-failure threshold — the scoped un-halt did
        not erase the failure history the way the blunt operator clear does.
    """
    svc = TradeService()
    svc.record_recon_failure("kraken.BTC-USD.live", max_failures=3)
    svc.record_recon_failure("kraken.BTC-USD.live", max_failures=3)
    svc.halt_shard("kraken.BTC-USD.live", "paired-execution:w:s:k")
    svc.unhalt_shard("kraken.BTC-USD.live", "paired-execution:w:s:k")
    assert svc.is_halted("kraken.BTC-USD.live") is False
    halted = svc.record_recon_failure("kraken.BTC-USD.live", max_failures=3)
    assert halted is True


def test_blunt_unhalt_clears_all_reasons_and_recon_counter() -> None:
    """The operator's blunt un-halt clears every reason and the recon counter.

    Given: a shard halted for both a paired reason and 3 recon failures,
    When: unhalt_shard runs without a reason,
    Then: the shard un-halts wholesale and the next recon failure starts the
        count from zero (no immediate re-halt).
    """
    svc = TradeService()
    svc.halt_shard("kraken.BTC-USD.live", "paired-execution:w:s:k")
    for _ in range(3):
        svc.record_recon_failure("kraken.BTC-USD.live", max_failures=3)
    svc.unhalt_shard("kraken.BTC-USD.live")
    assert svc.is_halted("kraken.BTC-USD.live") is False
    halted = svc.record_recon_failure("kraken.BTC-USD.live", max_failures=3)
    assert halted is False


def test_shard_halt_reasons_with_prefix_filters_and_sorts() -> None:
    """The prefix read model returns only matching reasons, deterministically.

    Given: two shards carrying a mix of paired-execution and other halt reasons,
    When: shard_halt_reasons_with_prefix runs with the paired prefix,
    Then: only the paired pairs come back, sorted per shard.
    """
    svc = TradeService()
    svc.halt_shard("kraken.BTC-USD.live", "paired-execution:w:s:b")
    svc.halt_shard("kraken.BTC-USD.live", "paired-execution:w:s:a")
    svc.halt_shard("kraken.BTC-USD.live", "3 consecutive reconciliation failures")
    svc.halt_shard("kraken.ETH-USD.live", "paired-execution:w:s:c")
    pairs = svc.shard_halt_reasons_with_prefix("paired-execution:")
    assert ("kraken.BTC-USD.live", "paired-execution:w:s:a") in pairs
    assert ("kraken.BTC-USD.live", "paired-execution:w:s:b") in pairs
    assert ("kraken.ETH-USD.live", "paired-execution:w:s:c") in pairs
    assert all(reason.startswith("paired-execution:") for _, reason in pairs)
    assert len(pairs) == 3


def test_mark_to_market() -> None:
    """Mark-to-market updates equity and tracks peak correctly.

    Given: a TradeService with initial_cash=10000 and a 0.1 BTC position at 50000,
    When: mark_to_market is called with a mark price of 60000,
    Then: equity exceeds 10000 and peak equity is at least the current equity.
    """
    svc = TradeService(initial_cash=10000.0)
    svc.apply_venue_event(
        _make_venue_event(event_id=1, side="buy", fill_price=50000.0, fill_size=0.1)
    )
    equity = svc.mark_to_market("kraken.BTC-USD.live", 60000.0)
    assert equity > 10000.0
    assert svc.get_peak_equity("kraken.BTC-USD.live") >= equity


def test_snapshot_for_checkpoint() -> None:
    """Snapshot for checkpoint returns a serializable dict of shard state.

    Given: a TradeService with one applied fill event,
    When: snapshot_for_checkpoint is called for that shard,
    Then: the dict contains position_qty, last_venue_event_id, and checkpoint_at.
    """
    svc = TradeService()
    svc.apply_venue_event(_make_venue_event(event_id=1))
    snap = svc.snapshot_for_checkpoint("kraken.BTC-USD.live")
    assert snap["position_qty"] == 0.5
    assert snap["last_venue_event_id"] == 1
    assert "checkpoint_at" in snap


def test_apply_order_terminal_cancel() -> None:
    """Order terminal event with cancel status clears in-flight and sets cancelled.

    Given: a TradeService with an in-flight command,
    When: an order_terminal event with status='cancelled' is applied,
    Then: command is no longer in-flight and status is 'cancelled'.
    """
    svc = TradeService()
    svc._get_or_create_shard("kraken.BTC-USD.live").command.in_flight = True
    event = _make_venue_event(
        event_id=1,
        event_type="order_terminal",
        side=None,
        fill_price=None,
        fill_size=None,
        status="cancelled",
    )
    svc.apply_venue_event(event)
    cmd = svc.get_command_state("kraken.BTC-USD.live")
    assert cmd.in_flight is False
    assert cmd.status == "cancelled"


def test_apply_unknown_event_type() -> None:
    """Unknown venue event type is logged but does not crash.

    Given: a TradeService with no state,
    When: a venue event with an unknown event_type is applied,
    Then: no exception is raised and watermark still advances.
    """
    svc = TradeService()
    event = _make_venue_event(
        event_id=1,
        event_type="unknown_type",
        side=None,
        fill_price=None,
        fill_size=None,
        status=None,
    )
    svc.apply_venue_event(event)
    shard = svc._get_or_create_shard("kraken.BTC-USD.live")
    assert shard.last_venue_event_id == 1


def test_apply_fill_partial() -> None:
    """Partial fill updates position but keeps command in-flight.

    Given: a TradeService with no prior state,
    When: a fill_observed event with status='partial' is applied,
    Then: position is updated and command status is 'partially_filled'.
    """
    svc = TradeService()
    event = _make_venue_event(
        event_id=1, side="buy", fill_price=50000.0, fill_size=0.3, status="partial"
    )
    svc.apply_venue_event(event)
    pos = svc.get_position("kraken.BTC-USD.live")
    assert pos.position_qty == 0.3
    cmd = svc.get_command_state("kraken.BTC-USD.live")
    assert cmd.status == "partially_filled"


def test_apply_fill_averages_entry_price_on_add() -> None:
    """Adding to an existing position recalculates average entry price.

    Given: a TradeService with a 0.5 BTC position at 50000,
    When: a second buy fill of 0.5 at 52000 is applied,
    Then: position is 1.0 BTC and entry price is the volume-weighted average 51000.
    """
    svc = TradeService()
    svc.apply_venue_event(
        _make_venue_event(event_id=1, side="buy", fill_price=50000.0, fill_size=0.5)
    )
    svc.apply_venue_event(
        _make_venue_event(
            event_id=2, side="buy", fill_price=52000.0, fill_size=0.5, exec_id="exec-2"
        )
    )
    pos = svc.get_position("kraken.BTC-USD.live")
    assert pos.position_qty == 1.0
    assert pos.entry_price is not None
    assert abs(pos.entry_price - 51000.0) < 0.01


def test_snapshot_with_open_command() -> None:
    """Snapshot includes open command IDs when a command is in-flight.

    Given: a TradeService with an in-flight command registered,
    When: snapshot_for_checkpoint is called,
    Then: open_command_ids contains the command public_id.
    """
    svc = TradeService()
    svc.register_command(
        "kraken.BTC-USD.live",
        {
            "public_id": "cmd-99",
            "timestamp": datetime.now(UTC),
            "session_id": "s1",
            "sequence_id": 1,
            "command_type": "submit",
            "shard_key": "kraken.BTC-USD.live",
            "exchange": "kraken",
            "instrument": "BTC-USD",
            "mode": "live",
            "strategy_id": "engine-buy",
            "client_order_id": "cid-99",
            "venue_client_id": "vcid-99",
            "idempotency_key": None,
            "side": "buy",
            "order_type": "market",
            "quantity": 0.5,
            "price": None,
            "stop_price": None,
            "leverage": None,
            "reduce_only": False,
            "status": "created",
            "attempt_count": 0,
            "last_error": None,
            "created_at": datetime.now(UTC),
            "dispatched_at": None,
            "acked_at": None,
            "terminal_at": None,
            "exchange_order_id": None,
            "supersedes_command_id": None,
            "correlation_id": "corr-99",
            "wallet_public_id": None,
            "operator_public_id": None,
            "user_public_id": None,
            "source_surface": "strategy",
        },
    )
    snap = svc.snapshot_for_checkpoint("kraken.BTC-USD.live")
    assert snap["open_command_ids"] is not None
    assert "cmd-99" in str(snap["open_command_ids"])


def test_apply_fill_sell_partial_position() -> None:
    """Selling part of a position reduces qty but keeps entry price.

    Given: a TradeService with 1.0 BTC position at 50000,
    When: a sell fill of 0.3 BTC is applied,
    Then: position is 0.7 BTC and entry_price is still set.
    """
    svc = TradeService()
    svc.apply_venue_event(
        _make_venue_event(event_id=1, side="buy", fill_price=50000.0, fill_size=1.0)
    )
    svc.apply_venue_event(
        _make_venue_event(
            event_id=2, side="sell", fill_price=51000.0, fill_size=0.3, exec_id="exec-2"
        )
    )
    pos = svc.get_position("kraken.BTC-USD.live")
    assert abs(pos.position_qty - 0.7) < 1e-9
    assert pos.entry_price is not None


def test_apply_fill_sell_without_entry_price() -> None:
    """Sell fill without prior entry price opens a short position.

    Given: a TradeService shard with position_qty=0 and no entry_price,
    When: a sell fill is applied directly,
    Then: position goes negative with entry_price set to fill price.
    """
    svc = TradeService()
    svc.apply_venue_event(
        _make_venue_event(event_id=1, side="sell", fill_price=50000.0, fill_size=0.5)
    )
    pos = svc.get_position("kraken.BTC-USD.live")
    assert pos.position_qty == -0.5
    assert pos.entry_price == 50000.0


def test_apply_fill_short_cover() -> None:
    """Buying to cover a short position computes positive PnL on price drop.

    Given: a TradeService with a short position of -1.0 BTC at entry 50000,
    When: a buy fill of 1.0 at 48000 covers the short,
    Then: position is flat, realized PnL = 1.0 * (50000-48000) = 2000.
    """
    svc = TradeService()
    svc.apply_venue_event(
        _make_venue_event(event_id=1, side="sell", fill_price=50000.0, fill_size=1.0)
    )
    svc.apply_venue_event(
        _make_venue_event(
            event_id=2, side="buy", fill_price=48000.0, fill_size=1.0, exec_id="exec-2"
        )
    )
    pos = svc.get_position("kraken.BTC-USD.live")
    assert pos.position_qty == 0.0
    assert pos.entry_price is None
    assert pos.realized_pnl == 2000.0


def test_apply_fill_reversal_resets_entry_price() -> None:
    """Selling more than position reverses through flat and resets entry price.

    Given: a TradeService with a long position of 1.0 BTC at entry 50000,
    When: a sell fill of 2.0 at 51000 is applied (over-close + open short),
    Then: position is -1.0, realized PnL is 1000 (on the closed 1.0), entry_price is 51000.
    """
    svc = TradeService()
    svc.apply_venue_event(
        _make_venue_event(event_id=1, side="buy", fill_price=50000.0, fill_size=1.0)
    )
    svc.apply_venue_event(
        _make_venue_event(
            event_id=2, side="sell", fill_price=51000.0, fill_size=2.0, exec_id="exec-2"
        )
    )
    pos = svc.get_position("kraken.BTC-USD.live")
    assert pos.position_qty == -1.0
    assert pos.entry_price == 51000.0
    assert pos.realized_pnl == 1000.0


def test_apply_fill_dedup_adds_both_ids() -> None:
    """Fill with both exec_id and trade_id adds both to dedup set.

    Given: a TradeService with no prior state,
    When: a fill with exec_id="e1" and trade_id="t1" is applied,
    Then: a second fill with only trade_id="t1" is rejected as duplicate.
    """
    svc = TradeService()
    svc.apply_venue_event(_make_venue_event(event_id=1, exec_id="e1", trade_id="t1"))
    svc.apply_venue_event(_make_venue_event(event_id=2, exec_id=None, trade_id="t1"))
    pos = svc.get_position("kraken.BTC-USD.live")
    assert pos.position_qty == 0.5


def test_apply_fill_trade_id_only() -> None:
    """Fill with only trade_id (no exec_id) is deduplicated by trade_id.

    Given: a fresh TradeService,
    When: a fill with exec_id=None and trade_id="t1" is applied,
    Then: position is updated and trade_id is added to dedup set.
    """
    svc = TradeService()
    svc.apply_venue_event(_make_venue_event(event_id=1, exec_id=None, trade_id="t1"))
    pos = svc.get_position("kraken.BTC-USD.live")
    assert pos.position_qty == 0.5


def test_seen_exec_ids_evicts_oldest_at_capacity() -> None:
    """OrderedDict evicts oldest entry when exceeding 10 000 capacity.

    Given: a shard with 10 000 seen exec IDs,
    When: one more fill is applied,
    Then: oldest ID is evicted, newest is retained, size stays at 10 000.
    """
    svc = TradeService()
    shard = svc._get_or_create_shard("kraken.BTC-USD.live")
    for i in range(10_000):
        shard.seen_exec_ids[f"id-{i}"] = None
    svc.apply_venue_event(_make_venue_event(event_id=99999, exec_id="id-new", trade_id=None))
    assert "id-new" in shard.seen_exec_ids
    assert "id-0" not in shard.seen_exec_ids
    assert len(shard.seen_exec_ids) == 10_000


def test_apply_fill_no_status() -> None:
    """Fill event with no status field does not change command status.

    Given: a TradeService with no prior state,
    When: a fill_observed event with status=None is applied,
    Then: position is updated but command status stays None.
    """
    svc = TradeService()
    event = _make_venue_event(event_id=1, status=None)
    svc.apply_venue_event(event)
    pos = svc.get_position("kraken.BTC-USD.live")
    assert pos.position_qty == 0.5
    cmd = svc.get_command_state("kraken.BTC-USD.live")
    assert cmd.status is None


def test_snapshot_without_in_flight_command() -> None:
    """Snapshot without in-flight command sets open_command_ids to None.

    Given: a TradeService with a shard that has no in-flight commands,
    When: snapshot_for_checkpoint is called,
    Then: open_command_ids is None.
    """
    svc = TradeService()
    svc.apply_venue_event(_make_venue_event(event_id=1))
    svc.get_command_state("kraken.BTC-USD.live").in_flight = False
    snap = svc.snapshot_for_checkpoint("kraken.BTC-USD.live")
    assert snap["open_command_ids"] is None


def test_restore_with_empty_command_ids() -> None:
    """Restoring with empty open_command_ids leaves command not in-flight.

    Given: a fresh TradeService,
    When: restore_from_checkpoint is called with empty open_command_ids,
    Then: command.in_flight is False.
    """
    svc = TradeService()
    svc.restore_from_checkpoint(
        shard_key="kraken.BTC-USD.live",
        position_qty=0.0,
        entry_price=None,
        cash=10000.0,
        peak_equity=10000.0,
        realized_pnl=0.0,
        turnover=0.0,
        last_venue_event_id=0,
        open_command_ids=[],
        seen_exec_ids=OrderedDict(),
    )
    assert svc.get_command_state("kraken.BTC-USD.live").in_flight is False


def test_apply_fill_unknown_side() -> None:
    """Fill with unknown side still updates watermark but skips position logic.

    Given: a TradeService with no prior state,
    When: a fill_observed event with side='unknown' is applied,
    Then: position remains at zero and no error is raised.
    """
    svc = TradeService()
    event = _make_venue_event(event_id=1, side="unknown", fill_price=50000.0, fill_size=0.5)
    svc.apply_venue_event(event)
    pos = svc.get_position("kraken.BTC-USD.live")
    assert pos.position_qty == 0.0


def test_snapshot_no_command_registered() -> None:
    """Snapshot with default command state returns None for open_command_ids.

    Given: a TradeService with a shard created but no command registered,
    When: snapshot_for_checkpoint is called,
    Then: open_command_ids is None because command_public_id is None.
    """
    svc = TradeService()
    svc._get_or_create_shard("kraken.BTC-USD.live")
    snap = svc.snapshot_for_checkpoint("kraken.BTC-USD.live")
    assert snap["open_command_ids"] is None


def test_mark_to_market_below_peak() -> None:
    """Mark-to-market below peak does not lower peak equity.

    Given: a TradeService with initial_cash=10000 and a 0.1 BTC position at 50000,
    When: mark_to_market is called with a price of 40000 (below entry),
    Then: equity is below initial and peak stays at initial_cash.
    """
    svc = TradeService(initial_cash=10000.0)
    svc.apply_venue_event(
        _make_venue_event(event_id=1, side="buy", fill_price=50000.0, fill_size=0.1)
    )
    equity = svc.mark_to_market("kraken.BTC-USD.live", 40000.0)
    assert equity < 10000.0
    assert svc.get_peak_equity("kraken.BTC-USD.live") == 10000.0


def test_apply_fill_close_without_entry_price() -> None:
    """Closing a position when entry_price is None skips PnL calculation.

    Given: a TradeService shard with position_qty=1.0 but entry_price=None,
    When: a sell fill of 0.5 is applied,
    Then: position reduces to 0.5 and realized_pnl stays 0.
    """
    svc = TradeService()
    shard = svc._get_or_create_shard("kraken.BTC-USD.live")
    shard.position.position_qty = 1.0
    shard.position.entry_price = None
    svc.apply_venue_event(
        _make_venue_event(event_id=1, side="sell", fill_price=50000.0, fill_size=0.5)
    )
    pos = svc.get_position("kraken.BTC-USD.live")
    assert abs(pos.position_qty - 0.5) < 1e-9
    assert pos.realized_pnl == 0.0


def test_apply_fill_buy_into_negative_position() -> None:
    """Buy fill into a negative position where total_qty <= 0 skips avg price calc.

    Given: a TradeService shard with position_qty=-1.0 at entry_price=50000,
    When: a buy fill of 0.5 is applied (total_qty = -0.5, still negative),
    Then: position goes to -0.5 and entry_price is unchanged (guard prevents division).
    """
    svc = TradeService()
    shard = svc._get_or_create_shard("kraken.BTC-USD.live")
    shard.position.position_qty = -1.0
    shard.position.entry_price = 50000.0
    svc.apply_venue_event(
        _make_venue_event(event_id=1, side="buy", fill_price=51000.0, fill_size=0.5)
    )
    pos = svc.get_position("kraken.BTC-USD.live")
    assert abs(pos.position_qty - (-0.5)) < 1e-9
    assert pos.entry_price == 50000.0


def test_apply_fill_stamps_position_opened_at_on_open() -> None:
    """Opening a position from flat stamps position_opened_at with venue_timestamp.

    Given: a fresh TradeService at flat,
    When: a buy fill arrives with a known venue_timestamp,
    Then: position_opened_at on the projection equals that venue_timestamp.
    """
    svc = TradeService()
    venue_time = datetime(2026, 4, 6, 12, 0, 0, tzinfo=UTC)
    event = _make_venue_event(event_id=1, side="buy")
    event["venue_timestamp"] = venue_time
    svc.apply_venue_event(event)
    pos = svc.get_position("kraken.BTC-USD.live")
    assert pos.position_opened_at == venue_time


def test_position_opened_at_carries_through_vwap_add() -> None:
    """Adding to an existing same-direction position preserves position_opened_at.

    Given: a long position opened at t1,
    When: a second buy fill at t2 adds to the position via VWAP,
    Then: position_opened_at remains t1, not t2.
    """
    svc = TradeService()
    t1 = datetime(2026, 4, 6, 12, 0, 0, tzinfo=UTC)
    t2 = datetime(2026, 4, 6, 12, 5, 0, tzinfo=UTC)
    e1 = _make_venue_event(event_id=1, side="buy", fill_price=50000.0, fill_size=0.4)
    e1["venue_timestamp"] = t1
    e2 = _make_venue_event(event_id=2, side="buy", fill_price=51000.0, fill_size=0.6)
    e2["venue_timestamp"] = t2
    svc.apply_venue_event(e1)
    svc.apply_venue_event(e2)
    pos = svc.get_position("kraken.BTC-USD.live")
    assert pos.position_opened_at == t1


def test_position_opened_at_resets_on_close() -> None:
    """Closing a position to flat resets position_opened_at to None.

    Given: a long position with a stamped position_opened_at,
    When: a sell fill closes the position fully,
    Then: position_opened_at is reset to None alongside entry_price.
    """
    svc = TradeService()
    t1 = datetime(2026, 4, 6, 12, 0, 0, tzinfo=UTC)
    e1 = _make_venue_event(event_id=1, side="buy", fill_size=1.0)
    e1["venue_timestamp"] = t1
    svc.apply_venue_event(e1)
    e2 = _make_venue_event(event_id=2, side="sell", fill_size=1.0, exec_id="exec-2")
    svc.apply_venue_event(e2)
    pos = svc.get_position("kraken.BTC-USD.live")
    assert pos.position_qty == 0.0
    assert pos.entry_price is None
    assert pos.position_opened_at is None


def test_position_opened_at_resets_on_flip() -> None:
    """A flip transition stamps position_opened_at with the flipping fill time.

    Given: a long position opened at t1,
    When: a sell fill larger than the position arrives at t2 (flips to short),
    Then: position_opened_at is reset to t2 (the new short cycle starts here).
    """
    svc = TradeService()
    t1 = datetime(2026, 4, 6, 12, 0, 0, tzinfo=UTC)
    t2 = datetime(2026, 4, 6, 13, 0, 0, tzinfo=UTC)
    e1 = _make_venue_event(event_id=1, side="buy", fill_size=1.0)
    e1["venue_timestamp"] = t1
    svc.apply_venue_event(e1)
    e2 = _make_venue_event(
        event_id=2, side="sell", fill_size=2.0, fill_price=49000.0, exec_id="exec-2"
    )
    e2["venue_timestamp"] = t2
    svc.apply_venue_event(e2)
    pos = svc.get_position("kraken.BTC-USD.live")
    assert pos.position_qty == -1.0
    assert pos.position_opened_at == t2


def test_position_opened_at_falls_back_to_received_at_when_venue_ts_missing() -> None:
    """When venue_timestamp is None the loop falls back to received_at.

    Given: a fill event with venue_timestamp=None,
    When: the fill opens a fresh position,
    Then: position_opened_at is the event's received_at value.
    """
    svc = TradeService()
    received = datetime(2026, 4, 6, 14, 0, 0, tzinfo=UTC)
    event = _make_venue_event(event_id=1, side="buy")
    event["venue_timestamp"] = None
    event["received_at"] = received
    svc.apply_venue_event(event)
    pos = svc.get_position("kraken.BTC-USD.live")
    assert pos.position_opened_at == received


def test_snapshot_for_checkpoint_includes_position_opened_at() -> None:
    """snapshot_for_checkpoint surfaces position_opened_at for persistence.

    Given: a TradeService with an open position stamped at t1,
    When: snapshot_for_checkpoint is called,
    Then: the returned dict contains position_opened_at == t1.
    """
    svc = TradeService()
    t1 = datetime(2026, 4, 6, 15, 0, 0, tzinfo=UTC)
    e1 = _make_venue_event(event_id=1, side="buy")
    e1["venue_timestamp"] = t1
    svc.apply_venue_event(e1)
    snap = svc.snapshot_for_checkpoint("kraken.BTC-USD.live")
    assert snap["position_opened_at"] == t1


def test_restore_from_checkpoint_restores_position_opened_at() -> None:
    """restore_from_checkpoint accepts position_opened_at and assigns it.

    Given: a fresh TradeService,
    When: restore_from_checkpoint is called with a non-None position_opened_at,
    Then: the projection's position_opened_at equals that value.
    """
    svc = TradeService()
    t1 = datetime(2026, 4, 6, 16, 0, 0, tzinfo=UTC)
    svc.restore_from_checkpoint(
        shard_key="kraken.BTC-USD.live",
        position_qty=1.0,
        entry_price=50000.0,
        cash=5000.0,
        peak_equity=10000.0,
        realized_pnl=0.0,
        turnover=50000.0,
        last_venue_event_id=10,
        open_command_ids=[],
        seen_exec_ids=OrderedDict(),
        position_opened_at=t1,
    )
    pos = svc.get_position("kraken.BTC-USD.live")
    assert pos.position_opened_at == t1


def test_restore_from_checkpoint_default_position_opened_at_is_none() -> None:
    """Old checkpoints without position_opened_at restore as None.

    Given: a fresh TradeService,
    When: restore_from_checkpoint is called without position_opened_at,
    Then: the projection's position_opened_at is None (safe default for
        pre-funding-fee-model checkpoints).
    """
    svc = TradeService()
    svc.restore_from_checkpoint(
        shard_key="kraken.BTC-USD.live",
        position_qty=1.0,
        entry_price=50000.0,
        cash=5000.0,
        peak_equity=10000.0,
        realized_pnl=0.0,
        turnover=50000.0,
        last_venue_event_id=10,
        open_command_ids=[],
        seen_exec_ids=OrderedDict(),
    )
    pos = svc.get_position("kraken.BTC-USD.live")
    assert pos.position_opened_at is None


def test_apply_fill_short_open_stamps_position_opened_at() -> None:
    """Opening a short from flat stamps position_opened_at.

    Given: a fresh TradeService at flat,
    When: a sell fill arrives that opens a short cycle,
    Then: position_opened_at on the projection equals the venue
        timestamp of that fill.
    """
    svc = TradeService()
    venue_time = datetime(2026, 4, 6, 12, 0, 0, tzinfo=UTC)
    event = _make_venue_event(event_id=1, side="sell", fill_size=0.5)
    event["venue_timestamp"] = venue_time
    svc.apply_venue_event(event)
    pos = svc.get_position("kraken.BTC-USD.live")
    assert pos.position_qty == -0.5
    assert pos.position_opened_at == venue_time


def test_position_opened_at_carries_through_short_vwap_add() -> None:
    """Adding to an existing short position preserves position_opened_at.

    Given: a short position opened at t1,
    When: a second sell fill at t2 adds to the short via VWAP,
    Then: position_opened_at remains t1, not t2.
    """
    svc = TradeService()
    t1 = datetime(2026, 4, 6, 12, 0, 0, tzinfo=UTC)
    t2 = datetime(2026, 4, 6, 12, 5, 0, tzinfo=UTC)
    e1 = _make_venue_event(event_id=1, side="sell", fill_price=50000.0, fill_size=0.4)
    e1["venue_timestamp"] = t1
    e2 = _make_venue_event(
        event_id=2, side="sell", fill_price=51000.0, fill_size=0.6, exec_id="exec-2"
    )
    e2["venue_timestamp"] = t2
    svc.apply_venue_event(e1)
    svc.apply_venue_event(e2)
    pos = svc.get_position("kraken.BTC-USD.live")
    assert pos.position_qty == -1.0
    assert pos.position_opened_at == t1


def test_position_opened_at_preserved_on_partial_short_cover() -> None:
    """Partially covering a short does NOT reset position_opened_at.

    Given: a short position of -1.0 opened at t1,
    When: a buy fill of 0.4 covers part of the short at t2,
    Then: position drops to -0.6 and position_opened_at remains t1
        (the open cycle has not yet closed).
    """
    svc = TradeService()
    t1 = datetime(2026, 4, 6, 12, 0, 0, tzinfo=UTC)
    t2 = datetime(2026, 4, 6, 12, 30, 0, tzinfo=UTC)
    e1 = _make_venue_event(event_id=1, side="sell", fill_size=1.0)
    e1["venue_timestamp"] = t1
    svc.apply_venue_event(e1)
    e2 = _make_venue_event(event_id=2, side="buy", fill_size=0.4, exec_id="exec-2")
    e2["venue_timestamp"] = t2
    svc.apply_venue_event(e2)
    pos = svc.get_position("kraken.BTC-USD.live")
    assert pos.position_qty == pytest.approx(-0.6)
    assert pos.position_opened_at == t1


def test_position_opened_at_resets_on_full_short_cover() -> None:
    """Fully covering a short to flat resets position_opened_at to None.

    Given: a short position with a stamped position_opened_at,
    When: a buy fill exactly covers the short,
    Then: position_opened_at is reset to None alongside entry_price.
    """
    svc = TradeService()
    t1 = datetime(2026, 4, 6, 12, 0, 0, tzinfo=UTC)
    e1 = _make_venue_event(event_id=1, side="sell", fill_size=1.0)
    e1["venue_timestamp"] = t1
    svc.apply_venue_event(e1)
    e2 = _make_venue_event(event_id=2, side="buy", fill_size=1.0, exec_id="exec-2")
    svc.apply_venue_event(e2)
    pos = svc.get_position("kraken.BTC-USD.live")
    assert pos.position_qty == 0.0
    assert pos.entry_price is None
    assert pos.position_opened_at is None


def test_position_opened_at_resets_on_short_to_long_flip() -> None:
    """A short-to-long flip stamps position_opened_at with the flip fill time.

    Given: a short position of -1.0 opened at t1,
    When: a buy fill of 2.0 flips the position to +1.0 at t2,
    Then: position_opened_at is reset to t2 (new long cycle starts here).
    """
    svc = TradeService()
    t1 = datetime(2026, 4, 6, 12, 0, 0, tzinfo=UTC)
    t2 = datetime(2026, 4, 6, 13, 0, 0, tzinfo=UTC)
    e1 = _make_venue_event(event_id=1, side="sell", fill_size=1.0)
    e1["venue_timestamp"] = t1
    svc.apply_venue_event(e1)
    e2 = _make_venue_event(
        event_id=2, side="buy", fill_size=2.0, fill_price=51000.0, exec_id="exec-2"
    )
    e2["venue_timestamp"] = t2
    svc.apply_venue_event(e2)
    pos = svc.get_position("kraken.BTC-USD.live")
    assert pos.position_qty == 1.0
    assert pos.position_opened_at == t2


def test_restore_then_delta_close_resets_position_opened_at() -> None:
    """A delta-replay close after restore clears position_opened_at.

    Given: a checkpoint restored with a stamped position_opened_at,
    When: a delta venue event closes the position to flat,
    Then: position_opened_at is reset to None and would NOT carry over
        a stale value into the funding accrual loop. This protects
        against the "checkpoint had position open, delta closed it"
        recovery edge case.
    """
    svc = TradeService()
    t1 = datetime(2026, 4, 6, 12, 0, 0, tzinfo=UTC)
    svc.restore_from_checkpoint(
        shard_key="kraken.BTC-USD.live",
        position_qty=1.0,
        entry_price=50000.0,
        cash=5000.0,
        peak_equity=10000.0,
        realized_pnl=0.0,
        turnover=50000.0,
        last_venue_event_id=10,
        open_command_ids=[],
        seen_exec_ids=OrderedDict(),
        position_opened_at=t1,
    )
    delta = _make_venue_event(event_id=11, side="sell", fill_size=1.0, exec_id="delta-1")
    svc.apply_venue_event(delta)
    pos = svc.get_position("kraken.BTC-USD.live")
    assert pos.position_qty == 0.0
    assert pos.position_opened_at is None


class TestAddFundingAccrual:
    """Tests for TradeService.add_funding_accrual."""

    def test_positive_charge_reduces_cash_and_pnl(self) -> None:
        """Verify positive amount reduces both cash and realized_pnl.

        Given: Shard with default cash (10000),
        When: add_funding_accrual called with positive amount,
        Then: Cash and realized_pnl decrease by amount.
        """
        svc = TradeService()
        svc.add_funding_accrual("s1", 25.0)
        shard = svc._shards["s1"]
        assert shard.cash == pytest.approx(10_000.0 - 25.0)
        assert shard.position.realized_pnl == pytest.approx(-25.0)

    def test_negative_amount_credits_cash_and_pnl(self) -> None:
        """Verify negative amount increases both cash and realized_pnl.

        Given: Shard with default cash,
        When: add_funding_accrual called with negative amount,
        Then: Cash and realized_pnl increase (charge is a credit).
        """
        svc = TradeService()
        svc.add_funding_accrual("s1", -10.0)
        shard = svc._shards["s1"]
        assert shard.cash == pytest.approx(10_010.0)
        assert shard.position.realized_pnl == pytest.approx(10.0)

    def test_accrual_propagates_to_snapshot(self) -> None:
        """Verify accrual mutation is visible in snapshot_for_checkpoint.

        Given: Shard with applied accrual,
        When: snapshot_for_checkpoint called,
        Then: Snapshot reflects the post-accrual cash and pnl.
        """
        svc = TradeService()
        svc.add_funding_accrual("s1", 50.0)
        snap = svc.snapshot_for_checkpoint("s1")
        assert snap["cash"] == pytest.approx(9_950.0)
        assert snap["realized_pnl"] == pytest.approx(-50.0)


class TestReplayFundingAccruals:
    """Tests for TradeService.replay_funding_accruals."""

    def test_replays_multiple_accruals(self) -> None:
        """Verify replay applies all accrual rows in order.

        Given: Two accrual rows,
        When: replay_funding_accruals called,
        Then: Cumulative effect on cash and pnl.
        """
        svc = TradeService()
        now = datetime.now(UTC)
        rows: list[AccrualLedgerRow] = [
            AccrualLedgerRow(
                public_id="a1",
                instrument_public_id="inst1",
                mode="live",
                accrual_type="rollover",
                accrued_at=now,
                amount=10.0,
                amount_asset="USD",
                rate=0.00025,
                notional=40000.0,
                position_quantity_at_accrual=1.0,
                exchange="kraken",
                timestamp=now,
                session_id="s",
                sequence_id=1,
            ),
            AccrualLedgerRow(
                public_id="a2",
                instrument_public_id="inst1",
                mode="live",
                accrual_type="rollover",
                accrued_at=now,
                amount=5.0,
                amount_asset="USD",
                rate=0.00025,
                notional=20000.0,
                position_quantity_at_accrual=0.5,
                exchange="kraken",
                timestamp=now,
                session_id="s",
                sequence_id=2,
            ),
        ]
        svc.replay_funding_accruals("s1", rows)
        shard = svc._shards["s1"]
        assert shard.cash == pytest.approx(10_000.0 - 15.0)
        assert shard.position.realized_pnl == pytest.approx(-15.0)

    def test_empty_list_is_noop(self) -> None:
        """Verify empty accrual list does not create shard."""
        svc = TradeService()
        svc.replay_funding_accruals("s1", [])
        assert "s1" not in svc._shards


class TestDetectCycleTransition:
    """Tests for TradeService._detect_cycle_transition pure classifier."""

    def test_flat_to_flat_is_none(self) -> None:
        """Given 0 -> 0, When classified, Then None (no-op)."""
        assert TradeService._detect_cycle_transition(0.0, 0.0) is None

    def test_flat_to_long_is_open(self) -> None:
        """Given 0 -> +, When classified, Then 'open'."""
        assert TradeService._detect_cycle_transition(0.0, 1.5) == "open"

    def test_flat_to_short_is_open(self) -> None:
        """Given 0 -> -, When classified, Then 'open' (sign-agnostic)."""
        assert TradeService._detect_cycle_transition(0.0, -2.0) == "open"

    def test_long_to_flat_is_close(self) -> None:
        """Given + -> 0, When classified, Then 'close'."""
        assert TradeService._detect_cycle_transition(1.5, 0.0) == "close"

    def test_short_to_flat_is_close(self) -> None:
        """Given - -> 0, When classified, Then 'close'."""
        assert TradeService._detect_cycle_transition(-2.0, 0.0) == "close"

    def test_long_to_short_is_flip(self) -> None:
        """Given + -> -, When classified, Then 'flip'."""
        assert TradeService._detect_cycle_transition(1.5, -2.0) == "flip"

    def test_short_to_long_is_flip(self) -> None:
        """Given - -> +, When classified, Then 'flip'."""
        assert TradeService._detect_cycle_transition(-2.0, 1.5) == "flip"

    def test_long_scale_up_is_scale_up(self) -> None:
        """Given +1 -> +2, When classified, Then 'scale_up'."""
        assert TradeService._detect_cycle_transition(1.0, 2.0) == "scale_up"

    def test_short_scale_up_is_scale_up(self) -> None:
        """Given -1 -> -2 (more negative), When classified, Then 'scale_up'."""
        assert TradeService._detect_cycle_transition(-1.0, -2.0) == "scale_up"

    def test_long_scale_down_is_none(self) -> None:
        """Given +2 -> +1, When classified, Then None (no DB write needed)."""
        assert TradeService._detect_cycle_transition(2.0, 1.0) is None

    def test_short_scale_down_is_none(self) -> None:
        """Given -2 -> -1, When classified, Then None."""
        assert TradeService._detect_cycle_transition(-2.0, -1.0) is None

    def test_same_qty_same_sign_is_none(self) -> None:
        """Given +1.5 -> +1.5, When classified, Then None (equal holds)."""
        assert TradeService._detect_cycle_transition(1.5, 1.5) is None

    def test_epsilon_old_qty_treated_as_flat(self) -> None:
        """Given |old| below 1e-12, When classified, Then open (treated as flat)."""
        assert TradeService._detect_cycle_transition(1e-13, 1.5) == "open"

    def test_epsilon_new_qty_treated_as_flat(self) -> None:
        """Given |new| below 1e-12, When classified, Then close (treated as flat)."""
        assert TradeService._detect_cycle_transition(1.5, 1e-13) == "close"

    def test_negative_epsilon_also_flat(self) -> None:
        """Given old = -1e-13, When classified, Then open (sign doesn't matter near zero)."""
        assert TradeService._detect_cycle_transition(-1e-13, -2.0) == "open"


class TestShardStateCycleCache:
    """Tests for ShardState.active_cycle_* cache fields."""

    def test_default_cache_is_unset(self) -> None:
        """Freshly-created shard has no active cycle cached."""
        svc = TradeService()
        shard = svc._get_or_create_shard("kraken.BTC-USD.live")
        assert shard.active_cycle_public_id is None
        assert shard.active_cycle_max_qty == pytest.approx(0.0)

    def test_cache_is_writable(self) -> None:
        """Trader can set + clear the cache fields on ShardState directly.

        This validates ShardState carries the cache but TradeService does
        not mutate it -- the trader is responsible for hydration.
        """
        svc = TradeService()
        shard = svc._get_or_create_shard("kraken.BTC-USD.live")
        shard.active_cycle_public_id = "cycle-1"
        shard.active_cycle_max_qty = 2.5
        assert shard.active_cycle_public_id == "cycle-1"
        assert shard.active_cycle_max_qty == pytest.approx(2.5)
        shard.active_cycle_public_id = None
        shard.active_cycle_max_qty = 0.0
        assert shard.active_cycle_public_id is None

    def test_apply_fill_does_not_touch_cache(self) -> None:
        """Position fills mutate position_qty but leave cycle cache untouched.

        The cache is trader-owned; TradeService stays in-memory-only and
        does not know about position_cycles.
        """
        svc = TradeService()
        shard = svc._get_or_create_shard("kraken.BTC-USD.live")
        shard.active_cycle_public_id = "cycle-existing"
        shard.active_cycle_max_qty = 1.0
        event = _make_venue_event(1, side="buy", fill_size=0.5, fill_price=50000.0)
        svc.apply_venue_event(event)
        assert shard.position.position_qty == pytest.approx(0.5)
        assert shard.active_cycle_public_id == "cycle-existing"
        assert shard.active_cycle_max_qty == pytest.approx(1.0)


def test_apply_breaker_open_is_rejection_equivalent_terminal() -> None:
    """order_breaker_open clears in-flight like a rejection.

    Given: a TradeService with an in-flight command whose shard then
        receives an order_breaker_open venue event (status 'failed'),
    When: the event is applied,
    Then: in_flight clears and the command status reads 'failed' — replay
        and checkpoint recovery converge with the live REJECTED publish.
    """
    svc = TradeService()
    event = _make_venue_event(event_id=1, event_type="order_breaker_open", status="failed")
    svc.apply_venue_event(event)
    cmd = svc.get_command_state("kraken.BTC-USD.live")
    assert cmd.in_flight is False
    assert cmd.status == "failed"


def test_reset_shard_reseeds_initial_cash() -> None:
    """reset_shard replaces a shard with a fresh projection at initial cash.

    Given: a shard that has applied a fill (non-zero position and watermark),
    When: reset_shard is called,
    Then: position, watermark and dedup set clear and cash/peak return to the
        service's configured initial cash.
    """
    svc = TradeService(initial_cash=5000.0)
    svc.apply_venue_event(_make_venue_event(event_id=1, fill_size=0.5))
    svc.reset_shard("kraken.BTC-USD.live")
    shard = svc._shards["kraken.BTC-USD.live"]
    assert shard.position.position_qty == 0.0
    assert shard.cash == 5000.0
    assert shard.peak_equity == 5000.0
    assert shard.last_venue_event_id == 0
    assert len(shard.seen_exec_ids) == 0


def test_project_fill_state_from_events_no_global_mutation() -> None:
    """project_fill_state_from_events replays into a throwaway shard.

    Given: a shard key that has no stored state,
    When: project_fill_state_from_events replays a buy fill for it,
    Then: it returns the fill-derived projection WITHOUT creating an entry in
        the service's shard map.
    """
    svc = TradeService()
    shard_key = "kraken.BTC-USD.live"
    events = [_make_venue_event(event_id=1, side="buy", fill_size=0.5, fill_price=100.0)]
    projection = svc.project_fill_state_from_events(shard_key, events)
    assert shard_key not in svc._shards
    assert projection["position_qty"] == pytest.approx(0.5)
    assert projection["last_venue_event_id"] == 1


def test_overlay_fill_state_preserves_command_and_peak() -> None:
    """overlay_fill_state copies fill-derived fields only.

    Given: a live shard with an in-flight command and a checkpoint peak equity,
    When: overlay_fill_state applies a gap-corrected projection,
    Then: position, cash, turnover, dedup set and watermark are overlaid while
        command identity and peak_equity stay exactly as they were.
    """
    svc = TradeService()
    shard_key = "kraken.BTC-USD.live"
    shard = svc._get_or_create_shard(shard_key)
    shard.command.in_flight = True
    shard.command.command_public_id = "cmd-1"
    shard.peak_equity = 12345.0
    projection: FillProjection = {
        "position_qty": 0.7,
        "entry_price": 100.0,
        "position_opened_at": None,
        "realized_pnl": 5.0,
        "cash": 9000.0,
        "turnover": 70.0,
        "seen_exec_ids": OrderedDict.fromkeys(["x"]),
        "last_venue_event_id": 9,
    }
    svc.overlay_fill_state(shard_key, projection)
    assert shard.position.position_qty == pytest.approx(0.7)
    assert shard.cash == pytest.approx(9000.0)
    assert shard.turnover == pytest.approx(70.0)
    assert shard.last_venue_event_id == 9
    assert "x" in shard.seen_exec_ids
    assert shard.peak_equity == 12345.0
    assert shard.command.in_flight is True
    assert shard.command.command_public_id == "cmd-1"


def test_dedup_fill_events_collapses_duplicate_identity() -> None:
    """Redelivered duplicate fills (same identity) collapse to the first.

    Given: two fill events sharing exec_id X and a distinct fill Y,
    When: dedup_fill_events runs,
    Then: only the first X and the Y survive, in order.
    """
    events = [
        _make_venue_event(event_id=1, exec_id="X", trade_id="X", fill_size=0.5),
        _make_venue_event(event_id=2, exec_id="X", trade_id="X", fill_size=0.5),
        _make_venue_event(event_id=3, exec_id="Y", trade_id="Y", fill_size=0.3),
    ]
    out = TradeService.dedup_fill_events(events)
    assert [event["id"] for event in out] == [1, 3]


def test_dedup_fill_events_passes_through_non_fill_events() -> None:
    """Non-fill lifecycle events are not deduped.

    Given: order_accepted and order_terminal events,
    When: dedup_fill_events runs,
    Then: both pass through untouched.
    """
    events = [
        _make_venue_event(event_id=1, event_type="order_accepted", exec_id=None, trade_id=None),
        _make_venue_event(event_id=2, event_type="order_terminal", exec_id=None, trade_id=None),
    ]
    out = TradeService.dedup_fill_events(events)
    assert [event["id"] for event in out] == [1, 2]


def test_dedup_fill_events_idless_fallback_identity() -> None:
    """Id-less fills dedup on the client_order_id+size+price fallback.

    Given: two id-less fills with identical coid+size+price and one with a
        different price,
    When: dedup_fill_events runs,
    Then: the identical pair collapses and the differing one survives.
    """
    events = [
        _make_venue_event(event_id=1, exec_id=None, trade_id=None, fill_size=0.5, fill_price=100.0),
        _make_venue_event(event_id=2, exec_id=None, trade_id=None, fill_size=0.5, fill_price=100.0),
        _make_venue_event(event_id=3, exec_id=None, trade_id=None, fill_size=0.5, fill_price=101.0),
    ]
    out = TradeService.dedup_fill_events(events)
    assert [event["id"] for event in out] == [1, 3]


def test_project_fill_state_dedups_duplicate_beyond_window() -> None:
    """A duplicate fill does not double-book the throwaway projection.

    Given: the same fill (exec_id X) recorded twice,
    When: project_fill_state_from_events replays it,
    Then: the position reflects a single application of the fill.
    """
    svc = TradeService()
    shard_key = "kraken.BTC-USD.live"
    events = [
        _make_venue_event(
            event_id=1, exec_id="X", trade_id="X", side="buy", fill_size=0.5, fill_price=100.0
        ),
        _make_venue_event(
            event_id=2, exec_id="X", trade_id="X", side="buy", fill_size=0.5, fill_price=100.0
        ),
    ]
    projection = svc.project_fill_state_from_events(shard_key, events)
    assert projection["position_qty"] == pytest.approx(0.5)
