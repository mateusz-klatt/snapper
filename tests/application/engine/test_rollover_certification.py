"""Exact legacy rollover reconstruction refuses every unproven charge shape."""

import math
from dataclasses import dataclass
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from typing import Literal

import pytest

from snapper.application.engine.rollover_certification import project_legacy_spot_rollovers
from snapper.application.trade.trade_service import FillProjection
from snapper.application.trade.trade_service import TradeService
from snapper.data.repository_types import AccrualLedgerRow
from snapper.data.repository_types import TradeProjectionCheckpointRow
from snapper.data.repository_types import VenueEventRow
from tests.application.engine.test_checkpoint_recovery import _make_checkpoint
from tests.application.engine.test_checkpoint_recovery import _make_venue_event

_OPENED_AT = datetime(2026, 11, 8, 19, tzinfo=UTC)
_BOUNDARY = datetime(2026, 11, 8, 20, tzinfo=UTC)
_INSERTED_AT = _BOUNDARY + timedelta(seconds=1)
_CHECKPOINT_AT = datetime(2026, 11, 9, 0, 30, tzinfo=UTC)


@dataclass
class _History:
    """Keep each test's fully typed replay and ledger inputs together."""

    projection: FillProjection
    events: list[VenueEventRow]
    accruals: list[AccrualLedgerRow]
    checkpoint: TradeProjectionCheckpointRow

    def project(self) -> FillProjection | None:
        """Evaluate this history without mutating its inputs."""
        return project_legacy_spot_rollovers(
            self.projection, self.events, self.accruals, self.checkpoint
        )


@pytest.fixture
def history() -> _History:
    """Reproduce the supplied XRP checkpoint's one spot buy and rollover."""
    event = _make_venue_event(event_id=1, fill_size=4.07, fill_price=1.00769)
    event["instrument"] = "XRP-EUR"
    event["shard_key"] = "kraken.XRP-EUR.live"
    event["venue_timestamp"] = _OPENED_AT
    event["received_at"] = _OPENED_AT
    event["timestamp"] = _OPENED_AT
    event["fee"] = 0.03281039
    event["fee_asset"] = "EUR"
    projection = TradeService().project_fill_state_from_events([event])
    row: AccrualLedgerRow = {
        "public_id": "accrual-1",
        "instrument_public_id": "instrument-xrp-eur",
        "mode": "live",
        "accrual_type": "rollover",
        "accrued_at": _BOUNDARY,
        "amount": 0.001025324575,
        "amount_asset": "EUR",
        "rate": 0.00025,
        "notional": 4.1012983,
        "position_quantity_at_accrual": 4.07,
        "exchange": "kraken",
        "timestamp": _INSERTED_AT,
        "session_id": "s-test",
        "sequence_id": 1,
    }
    checkpoint = _make_checkpoint(checkpoint_at=_CHECKPOINT_AT)
    return _History(projection, [event], [row], checkpoint)


def _next_charge(history: _History) -> AccrualLedgerRow:
    """Build the next strictly ordered charge for the unchanged position."""
    row = history.accruals[-1].copy()
    row["public_id"] = "accrual-2"
    row["sequence_id"] += 1
    row["accrued_at"] += timedelta(hours=4)
    row["timestamp"] += timedelta(hours=4)
    return row


def test_reproduces_exact_production_cash_and_realized(history: _History) -> None:
    """Forward subtraction matches the checkpoint despite an inexact cash delta.

    Given: the XRP spot buy and its exact recorded rollover charge.
    When: the helper forward-folds the charge into the fill replay.
    Then: cash and realized PnL match production without mutating the replay.
    """
    original = history.projection.copy()

    expected = history.project()

    assert expected is not None
    assert history.projection["cash"] == 9995.86589131
    assert expected["cash"] == 9995.864865985426
    assert expected["realized_pnl"] == -0.001025324575
    assert history.projection["cash"] - expected["cash"] != history.accruals[0]["amount"]
    assert history.projection == original
    assert (
        expected | {"cash": original["cash"], "realized_pnl": original["realized_pnl"]} == original
    )


def test_sequential_charges_preserve_every_other_replay_field(history: _History) -> None:
    """Multiple unchanged-position charges retain their original subtraction order.

    Given: two ordered rollover charges against the same XRP position.
    When: the helper reconstructs the expected checkpoint state.
    Then: both costs are subtracted sequentially and fill identities are retained.
    """
    history.accruals.append(_next_charge(history))

    expected = history.project()

    assert expected is not None
    assert expected["cash"] == (
        history.projection["cash"] - history.accruals[0]["amount"] - history.accruals[1]["amount"]
    )
    assert expected["realized_pnl"] == -0.00205064915
    assert expected["seen_exec_ids"] == history.projection["seen_exec_ids"]


def test_same_insertion_time_keeps_strict_sequence_and_boundary_order(history: _History) -> None:
    """One catch-up iteration can persist several boundaries with one bus time.

    Given: two charges with distinct ordered boundaries and one insertion time.
    When: the helper verifies their source sequence and chronology.
    Then: the common insertion time does not prevent exact reconstruction.
    """
    second = _next_charge(history)
    history.accruals[0]["timestamp"] = second["timestamp"]
    history.accruals.append(second)

    assert history.project() is not None


def test_duplicate_fill_observations_do_not_invent_an_extra_trade(history: _History) -> None:
    """Two durable observations of one execution retain the one-fill proof.

    Given: the same uniquely identified fill appears twice in durable history.
    When: the helper deduplicates the fill observations.
    Then: the one-opening-fill rollover proof remains eligible.
    """
    history.events.append(history.events[0].copy())

    assert history.project() is not None


def test_lifecycle_observations_do_not_invent_an_extra_fill(history: _History) -> None:
    """Order-only history is ignored when counting unique economic fills.

    Given: a buy fill followed by an order-accepted lifecycle observation.
    When: the helper counts unique economic fills.
    Then: the lifecycle observation does not invalidate the single-buy proof.
    """
    event = history.events[0].copy()
    event["event_type"] = "order_accepted"
    history.events.append(event)

    assert history.project() is not None


def test_empty_ledger_has_no_rollover_explanation(history: _History) -> None:
    """An adjustment requires at least one actual ledger row.

    Given: a fill replay whose accrual ledger is empty.
    When: the helper searches for an exact historical rollover explanation.
    Then: it returns no adjusted projection.
    """
    history.accruals.clear()

    assert history.project() is None


def test_missing_fill_history_refuses(history: _History) -> None:
    """A charge cannot explain a position without its sole opening fill.

    Given: a recorded charge without any durable opening-fill history.
    When: the helper evaluates the position evidence.
    Then: it refuses to construct an adjusted projection.
    """
    history.events.clear()

    assert history.project() is None


def test_second_unique_fill_refuses(history: _History) -> None:
    """Interleaved fill and accrual arithmetic remains outside this narrow proof.

    Given: durable history containing two distinct execution identities.
    When: the helper evaluates the legacy one-buy adjustment.
    Then: multiple unique fills prevent an adjusted projection.
    """
    event = history.events[0].copy()
    event["exec_id"] = "other-execution"
    event["trade_id"] = "other-trade"
    history.events.append(event)

    assert history.project() is None


@pytest.mark.parametrize("side", ["sell", None])
def test_only_positive_buy_history_is_supported(history: _History, side: str | None) -> None:
    """A short or unidentified direction cannot use the legacy cash-buy rule.

    Given: a fill whose side is sell or missing.
    When: the helper checks the single opening trade.
    Then: it refuses the adjustment reserved for a positive cash buy.
    """
    history.events[0]["side"] = side

    assert history.project() is None


@pytest.mark.parametrize("instrument", ["XRPEUR", "XRP-EUR-extra", "-EUR", "XRP-"])
def test_ambiguous_quote_asset_refuses(history: _History, instrument: str) -> None:
    """The ledger denomination must derive from a standard unambiguous symbol.

    Given: an instrument symbol with missing or ambiguous base/quote components.
    When: the helper resolves the ledger's required quote denomination.
    Then: the uncertain symbol prevents an adjusted projection.
    """
    history.events[0]["instrument"] = instrument

    assert history.project() is None


@pytest.mark.parametrize("field", ["position_qty", "entry_price", "cash", "realized_pnl"])
@pytest.mark.parametrize("value", [float("nan"), float("inf"), float("-inf")])
def test_nonfinite_replay_values_refuse(
    history: _History,
    field: Literal["position_qty", "entry_price", "cash", "realized_pnl"],
    value: float,
) -> None:
    """Finite charge arithmetic cannot explain a corrupt nonfinite replay.

    Given: NaN or infinity in one of the fill replay's financial values.
    When: the helper checks the position and monetary basis.
    Then: it rejects the nonfinite replay without applying an adjustment.
    """
    history.projection[field] = value

    assert history.project() is None


@pytest.mark.parametrize("field", ["position_qty", "entry_price"])
@pytest.mark.parametrize("value", [0.0, -1.0])
def test_nonpositive_buy_basis_refuses(
    history: _History, field: Literal["position_qty", "entry_price"], value: float
) -> None:
    """The single opening buy must produce positive quantity and entry.

    Given: a zero or negative replay quantity or entry price.
    When: the helper validates the single-buy position basis.
    Then: it refuses a projection outside the positive spot-buy path.
    """
    history.projection[field] = value

    assert history.project() is None


def test_missing_entry_price_refuses(history: _History) -> None:
    """A missing entry price cannot establish the exact charge notional.

    Given: an otherwise valid replay with no entry price.
    When: the helper checks the charge's position-notional basis.
    Then: the missing price prevents an adjusted projection.
    """
    history.projection["entry_price"] = None

    assert history.project() is None


def test_missing_opening_time_refuses(history: _History) -> None:
    """The charge boundary needs an identified earlier position opening.

    Given: an otherwise valid replay with no position-opening timestamp.
    When: the helper establishes whether the charge follows the opening.
    Then: the missing temporal anchor prevents an adjusted projection.
    """
    history.projection["position_opened_at"] = None

    assert history.project() is None


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("accrual_type", "funding"),
        ("accrual_type", "borrow"),
        ("exchange", "other-exchange"),
        ("mode", "paper"),
        ("instrument_public_id", ""),
        ("amount_asset", "USD"),
    ],
)
def test_foreign_or_unknown_charge_identity_refuses(
    history: _History,
    field: Literal["accrual_type", "exchange", "mode", "instrument_public_id", "amount_asset"],
    value: str,
) -> None:
    """Only this instrument's exact rollover denomination can explain the charge.

    Given: a charge with a foreign, unknown or incompatible identity field.
    When: the helper compares its type, scope and denomination with the fill.
    Then: it refuses the charge rather than explaining the checkpoint with it.
    """
    history.accruals[0][field] = value

    assert history.project() is None


@pytest.mark.parametrize("field", ["amount", "rate", "notional"])
@pytest.mark.parametrize("value", [0.0, -1.0, float("nan"), float("inf"), float("-inf")])
def test_nonpositive_or_nonfinite_charge_numbers_refuse(
    history: _History, field: Literal["amount", "rate", "notional"], value: float
) -> None:
    """Every charge component must be finite and strictly positive.

    Given: a zero, negative or nonfinite amount, rate or notional.
    When: the helper validates the internal rollover arithmetic.
    Then: it rejects the invalid charge component.
    """
    history.accruals[0][field] = value

    assert history.project() is None


def test_inexact_charge_product_refuses(history: _History) -> None:
    """A positive finite amount still needs exact rate-times-notional evidence.

    Given: a recorded amount increased by an additional 1e-15.
    When: the helper compares it with the exact notional-times-rate product.
    Then: the extra amount prevents an adjusted projection.
    """
    history.accruals[0]["amount"] += 1e-15

    assert history.project() is None


@pytest.mark.parametrize("value", [4.0700000001, -4.07, float("nan"), float("inf")])
def test_different_position_quantity_refuses(history: _History, value: float) -> None:
    """Even tiny quantity differences invalidate the ledger's position identity.

    Given: a charge carrying a different or nonfinite position quantity.
    When: the helper compares the ledger position with the sole buy's replay.
    Then: it refuses the charge without a quantity tolerance.
    """
    history.accruals[0]["position_quantity_at_accrual"] = value

    assert history.project() is None


def test_inexact_position_notional_refuses(history: _History) -> None:
    """A consistent amount and rate cannot excuse a different entry-price basis.

    Given: a changed notional with its amount recomputed consistently.
    When: the helper compares that notional with replay quantity times entry.
    Then: the altered position basis prevents an adjusted projection.
    """
    row = history.accruals[0]
    row["notional"] += 1e-12
    row["amount"] = row["notional"] * row["rate"]

    assert history.project() is None


def test_missing_ledger_session_refuses(history: _History) -> None:
    """An empty original ledger session cannot establish charge provenance.

    Given: a rollover row whose original session identifier is empty.
    When: the helper evaluates the source ordering evidence.
    Then: the unknown ledger session prevents an adjusted projection.
    """
    history.accruals[0]["session_id"] = ""

    assert history.project() is None


def test_refreshed_checkpoint_session_preserves_ledger_history(history: _History) -> None:
    """A restart may refresh the checkpoint session without changing recorded costs.

    Given: a checkpoint written by a later session than its historical charges.
    When: the helper validates the original ledger session and ordering.
    Then: the refreshed checkpoint session does not invalidate reconstruction.
    """
    history.checkpoint["session_id"] = "next-coordinator-session"

    assert history.project() is not None


def test_identical_charges_across_original_sessions_are_supported(history: _History) -> None:
    """Identical subtraction operands do not need a cross-session charge order.

    Given: two identical charges from distinct sessions with the same sequence.
    When: the helper reconstructs the complete historical rollover costs.
    Then: both charges are retained without comparing cross-session counters.
    """
    second = _next_charge(history)
    second["session_id"] = "other-ledger-session"
    second["sequence_id"] = 1
    history.accruals.append(second)

    expected = history.project()

    assert expected is not None
    assert expected["cash"] == history.projection["cash"] - second["amount"] - second["amount"]
    assert expected["realized_pnl"] == -0.00205064915


def test_single_session_preserves_differing_exact_charge_amounts(history: _History) -> None:
    """A proven single-session order continues to support unequal charge amounts.

    Given: ordered charges in one session with distinct exact rate products.
    When: the helper applies the historical charges in their proven order.
    Then: the existing single-session behavior retains both exact amounts.
    """
    first = history.accruals[0]
    second = _next_charge(history)
    second["rate"] = math.nextafter(first["rate"], math.inf)
    second["amount"] = second["notional"] * second["rate"]
    history.accruals.append(second)

    expected = history.project()

    assert second["amount"] == math.nextafter(first["amount"], math.inf)
    assert expected is not None
    assert expected["cash"] == history.projection["cash"] - first["amount"] - second["amount"]
    assert expected["realized_pnl"] == -first["amount"] - second["amount"]


def test_multiple_sessions_refuse_one_bit_amount_difference(history: _History) -> None:
    """A one-bit operand difference leaves cross-session subtraction unproven.

    Given: two sessions whose charges differ by one bit despite exact products.
    When: the helper evaluates whether charge order is financially irrelevant.
    Then: it refuses the unequal operands without introducing a tolerance.
    """
    first = history.accruals[0]
    second = _next_charge(history)
    second["session_id"] = "other-ledger-session"
    second["sequence_id"] = 1
    second["rate"] = math.nextafter(first["rate"], math.inf)
    second["amount"] = second["notional"] * second["rate"]
    history.accruals.append(second)

    assert second["amount"] == math.nextafter(first["amount"], math.inf)
    assert history.project() is None


@pytest.mark.parametrize("order", [(0, 1, 2), (2, 0, 1), (0, 2, 1)])
def test_identical_charges_preserve_each_session_order_across_permutations(
    history: _History, order: tuple[int, int, int]
) -> None:
    """Session grouping cannot change a fold of identical subtraction operands.

    Given: three identical charges and a proven order within one source session.
    When: the other session appears before, after or between that session's rows.
    Then: every valid permutation reconstructs exactly the same full projection.
    """
    history.accruals.append(_next_charge(history))
    third = _next_charge(history)
    third["public_id"] = "accrual-3"
    third["session_id"] = "other-ledger-session"
    third["sequence_id"] = 1
    history.accruals.append(third)
    history.checkpoint["checkpoint_at"] += timedelta(hours=4)
    original = history.project()
    history.accruals = [history.accruals[index] for index in order]

    expected = history.project()

    assert original is not None
    assert expected == original


def test_duplicate_boundary_across_sessions_refuses(history: _History) -> None:
    """Identical charges still require globally unique economic boundaries.

    Given: two sessions recording the same rollover boundary with equal amounts.
    When: the helper checks the complete ledger's distinct charge evidence.
    Then: it refuses the duplicate boundary instead of counting it twice.
    """
    second = _next_charge(history)
    second["session_id"] = "other-ledger-session"
    second["sequence_id"] = 1
    second["accrued_at"] = history.accruals[0]["accrued_at"]
    history.accruals.append(second)

    assert history.project() is None


@pytest.mark.parametrize(
    "problem",
    ["duplicate-sequence", "reversed-sequence", "reversed-boundary", "reversed-insertion"],
)
def test_interleaved_session_cannot_hide_invalid_original_order(
    history: _History,
    problem: Literal[
        "duplicate-sequence", "reversed-sequence", "reversed-boundary", "reversed-insertion"
    ],
) -> None:
    """A different intervening session cannot reset the original session proof.

    Given: two rows from one session separated by an identical foreign-session row.
    When: their sequences, boundaries or insertion timestamps contradict order.
    Then: the helper refuses despite equal amounts and valid individual bounds.
    """
    first = history.accruals[0]
    second = _next_charge(history)
    second["session_id"] = "other-ledger-session"
    second["sequence_id"] = 1
    history.accruals.append(second)
    third = _next_charge(history)
    third["public_id"] = "accrual-3"
    third["session_id"] = first["session_id"]
    history.accruals.append(third)
    history.checkpoint["checkpoint_at"] += timedelta(hours=4)
    if problem == "duplicate-sequence":
        third["sequence_id"] = first["sequence_id"]
    elif problem == "reversed-sequence":
        first["sequence_id"] = third["sequence_id"] + 1
    elif problem == "reversed-boundary":
        third["accrued_at"] = first["accrued_at"] - timedelta(seconds=1)
    else:
        first["timestamp"] = third["timestamp"] + timedelta(seconds=1)

    assert history.project() is None


def test_multiple_sessions_still_require_nonempty_session_ids(history: _History) -> None:
    """An unknown source session cannot use the identical-charge allowance.

    Given: equal charges with one nonempty and one missing source session.
    When: the helper validates each row's source identity.
    Then: it refuses the incomplete session proof.
    """
    second = _next_charge(history)
    second["session_id"] = ""
    history.accruals.append(second)

    assert history.project() is None


@pytest.mark.parametrize("amount", [0.0, -1.0, float("nan"), float("inf"), float("-inf")])
def test_multiple_sessions_still_require_finite_positive_amounts(
    history: _History, amount: float
) -> None:
    """Equal invalid operands cannot establish a supported historical charge.

    Given: equal nonpositive or nonfinite amounts recorded by two sessions.
    When: the helper checks the complete rollover ledger.
    Then: the invalid charge values prevent an adjusted projection.
    """
    second = _next_charge(history)
    second["session_id"] = "other-ledger-session"
    history.accruals.append(second)
    for row in history.accruals:
        row["amount"] = amount

    assert history.project() is None


@pytest.mark.parametrize("sequence", [0, -1])
def test_nonpositive_sequence_refuses(history: _History, sequence: int) -> None:
    """An actual source sequence starts above zero.

    Given: a charge whose source sequence is zero or negative.
    When: the helper evaluates its original ordering evidence.
    Then: it refuses the invalid sequence rather than inferring an order.
    """
    history.accruals[0]["sequence_id"] = sequence

    assert history.project() is None


@pytest.mark.parametrize("boundary", [_OPENED_AT, _OPENED_AT - timedelta(seconds=1)])
def test_boundary_must_follow_opening(history: _History, boundary: datetime) -> None:
    """A charge at or before this position's opening cannot belong to its cycle.

    Given: a rollover boundary equal to or earlier than the position opening.
    When: the helper verifies the charge's economic chronology.
    Then: the boundary cannot explain this position's checkpoint.
    """
    history.accruals[0]["accrued_at"] = boundary

    assert history.project() is None


@pytest.mark.parametrize(
    "inserted_at", [_BOUNDARY - timedelta(seconds=1), _CHECKPOINT_AT + timedelta(seconds=1)]
)
def test_insertion_must_follow_boundary_and_precede_checkpoint(
    history: _History, inserted_at: datetime
) -> None:
    """Future insertions and insertions before their economic boundary refuse.

    Given: a charge inserted before its boundary or after the checkpoint.
    When: the helper validates the ledger insertion window.
    Then: it refuses a charge that cannot be part of the checkpoint state.
    """
    history.accruals[0]["timestamp"] = inserted_at

    assert history.project() is None


def test_insertion_at_checkpoint_is_included(history: _History) -> None:
    """A boundary charge persisted at the checkpoint bound may have been captured.

    Given: a valid charge inserted exactly at the checkpoint timestamp.
    When: the helper evaluates the inclusive insertion upper bound.
    Then: the charge remains eligible for exact reconstruction.
    """
    history.accruals[0]["timestamp"] = _CHECKPOINT_AT

    assert history.project() is not None


def test_different_instrument_between_charges_refuses(history: _History) -> None:
    """A ledger assembled from two instrument identities cannot explain one shard.

    Given: two otherwise valid charges carrying different instrument identities.
    When: the helper verifies their common position ownership.
    Then: it refuses a ledger that spans two instruments.
    """
    second = _next_charge(history)
    second["instrument_public_id"] = "other-instrument"
    history.accruals.append(second)

    assert history.project() is None


@pytest.mark.parametrize("sequence", [1, 0])
def test_duplicate_or_reversed_charge_sequence_refuses(history: _History, sequence: int) -> None:
    """Charge order must be strictly increasing within its original session.

    Given: two charges whose source sequences fail to increase.
    When: the helper checks the original ledger order.
    Then: the nonmonotonic sequence prevents an adjusted projection.
    """
    second = _next_charge(history)
    history.accruals[0]["sequence_id"] = 2
    second["sequence_id"] = sequence
    history.accruals.append(second)

    assert history.project() is None


@pytest.mark.parametrize("boundary", [_BOUNDARY, _BOUNDARY - timedelta(seconds=1)])
def test_duplicate_or_reversed_boundary_refuses(history: _History, boundary: datetime) -> None:
    """An identified charge sequence must also preserve economic boundary order.

    Given: a later charge with an equal or earlier economic boundary.
    When: the helper validates the boundaries alongside their source sequence.
    Then: the duplicate or reversed boundary prevents reconstruction.
    """
    second = _next_charge(history)
    second["accrued_at"] = boundary
    history.accruals.append(second)

    assert history.project() is None


def test_reversed_insertion_order_refuses(history: _History) -> None:
    """A timestamp ordering contradiction cannot be repaired by sequence alone.

    Given: increasing charge sequences whose insertion timestamps decrease.
    When: the helper checks the complete ledger chronology.
    Then: it refuses the contradictory insertion order.
    """
    second = _next_charge(history)
    history.accruals[0]["timestamp"] = second["timestamp"] + timedelta(seconds=1)
    history.accruals.append(second)

    assert history.project() is None
