"""Finality scenarios across the spot anchor bootstrap and the history certificate.

Without order-finality seals, an owned order's economics that straddle the cut
must never produce a false seal or a false certified range: economics the
venue's totals already reflect at capture refuse at bootstrap, and economics
that become visible only later seal and then refuse in the unchanged anchored
history certificate. Each scenario runs the real pure assembly, ownership
witness, bootstrap, and certificate modules on one owned BUY EUR/PLN order.
"""

from dataclasses import replace
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from decimal import Decimal

from snapper.application.portfolio.spot_anchor_assembly import build_owned_witnesses_from_reads
from snapper.application.portfolio.spot_anchor_assembly import venue_history_tip_from_raw
from snapper.application.portfolio.spot_anchor_bootstrap import SpotAnchorObservation
from snapper.application.portfolio.spot_anchor_bootstrap import build_spot_anchor
from snapper.application.portfolio.spot_anchor_bootstrap import spot_anchor_bootstrap_refusals
from snapper.application.portfolio.walutomat_history_certificate import HistoryRangeEvidence
from snapper.application.portfolio.walutomat_history_certificate import (
    certify_walutomat_history_range,
)
from snapper.data.repository_types import SpotOwnedOrderPopulation
from snapper.data.repository_types import SpotReconciliationAnchorRow
from snapper.infrastructure.exchanges.contracts import VenueAccountHistoryItem
from snapper.infrastructure.exchanges.contracts import VenueAccountHistoryTip
from snapper.infrastructure.exchanges.contracts import VenueOrderFillLegs

_T = datetime(2026, 11, 1, 8, 0, tzinfo=UTC)
_ORDER_PUBLIC_ID = "00000000-0000-7000-8000-000000000601"
_SEAL_BALANCES = {"EUR": Decimal("106"), "PLN": Decimal("70")}
_POPULATION: SpotOwnedOrderPopulation = {
    "owned_order_ids": frozenset({"O1"}),
    "resolved_orders": ({"order_public_id": _ORDER_PUBLIC_ID, "exchange_order_id": "O1"},),
    "unresolved_lineage": (),
    "blocked_order_ids": frozenset(),
    "other_exchange_unidentified_orders": (),
    "commands": (
        {
            "public_id": "00000000-0000-7000-8000-000000000701",
            "status": "filled",
            "exchange_order_id": "O1",
        },
    ),
}


def _row(
    item_id: int,
    operation_type: str,
    amount: str,
    balance_after: str,
    currency: str,
) -> VenueAccountHistoryItem:
    """Return one raw API-placed history row of order ``O1``'s first transaction."""
    return VenueAccountHistoryItem(
        item_id=item_id,
        operation_type=operation_type,
        operation_amount=Decimal(amount),
        balance_after=Decimal(balance_after),
        currency=currency,
        transaction_id="tx-1",
        ordered_by="API/key-1",
        order_id="O1",
    )


def _totals(order_id: str, bought: str, sold: str, commission: str = "0") -> VenueOrderFillLegs:
    """Return one order's cumulative BUY EUR/PLN venue totals."""
    return VenueOrderFillLegs(
        order_id=order_id,
        bought_amount=Decimal(bought),
        sold_amount=Decimal(sold),
        commission_amount=Decimal(commission),
        bought_currency="EUR",
        sold_currency="PLN",
        commission_currency="EUR",
        buy_sell="BUY",
    )


def _seal_tip() -> VenueAccountHistoryTip:
    """Return the seal page: the first fill of order ``O1``, cut at ``H0 = 490``."""
    return VenueAccountHistoryTip(
        item_id=490,
        items=(
            _row(490, "MARKET_FX", "6", "106", "EUR"),
            _row(489, "MARKET_FX", "-30", "70", "PLN"),
        ),
        reached_genesis=True,
    )


def _observation(totals_at_capture: VenueOrderFillLegs) -> SpotAnchorObservation:
    """Return the bootstrap observation for the seal page and the captured totals."""
    raw_tip = _seal_tip()
    outcome = build_owned_witnesses_from_reads(
        raw_tip,
        [(1, "O1", 600_000_000, False)],
        {"O1": totals_at_capture},
        _POPULATION["owned_order_ids"],
        {1: "O1"},
    )
    return SpotAnchorObservation(
        public_id="00000000-0000-7000-8000-000000000301",
        wallet_public_id="00000000-0000-7000-8000-000000000101",
        exchange="walutomat",
        mode="live",
        anchor_exists=False,
        balance_status="observed",
        balances_are_venue_raw=True,
        balance_read_bound_to_scope=True,
        venue_account_state_public_id="00000000-0000-7000-8000-000000000201",
        balance_observation_id=41,
        session_id="00000000-0000-7000-8000-000000000501",
        sequence_id=1,
        balances_1=dict(_SEAL_BALANCES),
        balances_2=dict(_SEAL_BALANCES),
        balances_reserved={"EUR": Decimal("0"), "PLN": Decimal("0")},
        source_watermark=1,
        watermark_unchanged=True,
        source_chain_tip="ab" * 32,
        tip_0=venue_history_tip_from_raw(raw_tip),
        tip_1_item_id=raw_tip.item_id,
        history_window_reached_genesis=raw_tip.reached_genesis,
        precision_certified={"EUR": True, "PLN": True},
        margin_signal=False,
        execution_witnesses=outcome.witnesses,
        venue_cursor_requested_at=_T,
        venue_cursor_observed_at=_T + timedelta(seconds=1),
        source_watermark_requested_at=_T + timedelta(seconds=2),
        source_watermark_captured_at=_T + timedelta(seconds=3),
        first_request_started_at=_T + timedelta(seconds=4),
        first_request_completed_at=_T + timedelta(seconds=5),
        second_request_started_at=_T + timedelta(seconds=6),
        second_request_completed_at=_T + timedelta(seconds=7),
        venue_cursor_confirmed_at=_T + timedelta(seconds=8),
        timestamp=_T + timedelta(seconds=9),
        balances_reserved_2={"EUR": Decimal("0"), "PLN": Decimal("0")},
        ownership=_POPULATION,
        ownership_guarantee_version="ownership-review-test:" + "d" * 64,
        external_only_operations_initiated=False,
        witness_refusals=outcome.refusals,
    )


def _sealed_anchor() -> SpotReconciliationAnchorRow:
    """Seal the anchor from totals that match the page exactly."""
    return build_spot_anchor(_observation(_totals("O1", "6", "30")))


def test_a_straddling_fill_already_in_the_totals_refuses_at_bootstrap() -> None:
    """Economics the totals already reflect beyond the cut refuse the seal.

    Given: The seal page holding order ``O1``'s first fill while the venue's
        totals at capture already include a second fill beyond the cut.
    When: The owned witnesses and the bootstrap are evaluated.
    Then: The lifetime-total comparison refuses the witness and the seal
        refuses rather than sealing an incomplete order.
    """
    observation = _observation(_totals("O1", "10", "50"))
    assert observation.witness_refusals == ("order_base_legs_disagree_with_total",)
    refusals = spot_anchor_bootstrap_refusals(observation)
    assert "venue_fill_not_ingested" in refusals
    assert "local_execution_not_in_venue_history" in refusals


def test_a_straddling_fill_visible_only_later_seals_then_the_certificate_refuses() -> None:
    """A fill the totals did not yet show seals, and the certificate catches it.

    Given: The anchor sealed from totals showing only the first fill, then the
        second fill of ``O1`` arriving after the cut with its execution.
    When: The anchored history certificate checks the post-cut range against
        the order's lifetime totals.
    Then: The range is not certified.
    """
    anchor = _sealed_anchor()
    rows = (
        replace(_row(494, "MARKET_FX", "-20", "50", "PLN"), transaction_id="tx-2"),
        replace(_row(495, "MARKET_FX", "4", "110", "EUR"), transaction_id="tx-2"),
    )
    outcome = certify_walutomat_history_range(
        HistoryRangeEvidence(tip_item_id=495, confirming_tip_item_id=495, rows=rows),
        anchor,
        {"EUR": Decimal("110"), "PLN": Decimal("50")},
        [(2, "O1", 1_000_000_000, False)],
        {"O1": _totals("O1", "10", "50")},
        True,
        2,
    )
    assert outcome.certified is False
    assert "venue_history_execution_unmapped" in outcome.refusals


def test_a_delayed_commission_refuses_in_the_certificate() -> None:
    """A fee arriving after the cut for an order without post-cut totals refuses.

    Given: The anchor sealed before ``O1``'s commission was charged, then the
        COMMISSION row arriving after the cut with no new execution.
    When: The anchored history certificate checks the post-cut range.
    Then: The fee is not attributable and the range is not certified.
    """
    anchor = _sealed_anchor()
    rows = (replace(_row(491, "COMMISSION", "-0.02", "105.98", "EUR"), transaction_id=None),)
    outcome = certify_walutomat_history_range(
        HistoryRangeEvidence(tip_item_id=491, confirming_tip_item_id=491, rows=rows),
        anchor,
        {"EUR": Decimal("105.98"), "PLN": Decimal("70")},
        [],
        {},
        True,
        1,
    )
    assert outcome.certified is False
    assert outcome.refusals == ("venue_history_manual_unattributed",)


def test_an_owned_order_entirely_after_the_cut_certifies_unchanged() -> None:
    """An order placed and filled after the cut certifies normally.

    Given: The sealed anchor, then a new API order ``O3`` that fills after the
        cut with its committed execution and matching lifetime totals.
    When: The anchored history certificate checks the post-cut range.
    Then: The range is certified.
    """
    anchor = _sealed_anchor()
    rows = (
        replace(_row(494, "MARKET_FX", "-25", "45", "PLN"), order_id="O3", transaction_id="tx-3"),
        replace(_row(495, "MARKET_FX", "5", "111", "EUR"), order_id="O3", transaction_id="tx-3"),
    )
    outcome = certify_walutomat_history_range(
        HistoryRangeEvidence(tip_item_id=495, confirming_tip_item_id=495, rows=rows),
        anchor,
        {"EUR": Decimal("111"), "PLN": Decimal("45")},
        [(2, "O3", 500_000_000, False)],
        {"O3": _totals("O3", "5", "25")},
        True,
        2,
    )
    assert outcome.certified is True
    assert outcome.refusals == ()
