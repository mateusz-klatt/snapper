"""Unit tests for the pure walutomat venue-cursor range certificate."""

import hashlib
from collections.abc import Mapping
from collections.abc import Sequence
from dataclasses import replace
from datetime import UTC
from datetime import datetime
from decimal import Decimal

import pytest

from snapper.application.portfolio.walutomat_history_certificate import CertificateOutcome
from snapper.application.portfolio.walutomat_history_certificate import HistoryRangeEvidence
from snapper.application.portfolio.walutomat_history_certificate import (
    certify_walutomat_history_range,
)
from snapper.data.repository_types import SpotReconciliationAnchorRow
from snapper.infrastructure.exchanges.contracts import VenueAccountHistoryItem
from snapper.infrastructure.exchanges.contracts import VenueOrderFillLegs

_T = datetime(2026, 7, 18, 8, 0, tzinfo=UTC)
_SCHEME = "walutomat:api-v2.0.0:account/history:v1"
_EMPTY_RANGE_DIGEST = "4f53cda18c2baa0c0354bb5f9a3ecbe5ed12ab4d8e11ba873c2f11161202b945"


def _anchor(
    *,
    venue_cursor_kind: str = "account_history_item_id",
    venue_cursor_scheme: str = _SCHEME,
    venue_cursor_value: str = "100",
    source_watermark: int = 1,
    balances_json: str = '{"EUR":"100","PLN":"500"}',
) -> SpotReconciliationAnchorRow:
    """Build a sealed anchor row with overridable cursor and seed fields."""
    return {
        "public_id": "00000000-0000-7000-8000-000000000301",
        "wallet_public_id": "00000000-0000-7000-8000-000000000101",
        "exchange": "walutomat",
        "mode": "live",
        "venue_account_state_public_id": "00000000-0000-7000-8000-000000000201",
        "balance_observation_id": 41,
        "source_watermark_kind": "scope_sequence",
        "source_watermark": source_watermark,
        "balances_json": balances_json,
        "first_request_started_at": _T,
        "first_request_completed_at": _T,
        "second_request_started_at": _T,
        "second_request_completed_at": _T,
        "boundary_status": "cursor_certified",
        "inventory_status": "venue_reported_full",
        "margin_status": "cash",
        "provenance": f"spot_anchor_bootstrap:v1:{_SCHEME}:windowed",
        "session_id": "00000000-0000-7000-8000-000000000501",
        "sequence_id": 1,
        "timestamp": _T,
        "source_chain_tip": "a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0c1d2e3f4a5b6c7d8e9f0a1b2",
        "venue_cursor_kind": venue_cursor_kind,
        "venue_cursor_scheme": venue_cursor_scheme,
        "venue_cursor_value": venue_cursor_value,
        "venue_cursor_requested_at": _T,
        "venue_cursor_observed_at": _T,
        "venue_cursor_confirmed_at": _T,
        "source_watermark_requested_at": _T,
        "source_watermark_captured_at": _T,
    }


def _row(
    item_id: int,
    operation_type: str,
    amount: str,
    currency: str,
    balance_after: str,
    *,
    order_id: str | None = "O1",
    transaction_id: str | None = "T1",
    ordered_by: str = "API/key",
    correcting_entry: bool = False,
) -> VenueAccountHistoryItem:
    """Build one raw venue account-history row for a certificate scenario."""
    return VenueAccountHistoryItem(
        item_id=item_id,
        operation_type=operation_type,
        operation_amount=Decimal(amount),
        balance_after=Decimal(balance_after),
        currency=currency,
        transaction_id=transaction_id,
        ordered_by=ordered_by,
        order_id=order_id,
        correcting_entry=correcting_entry,
    )


_FILL_ROWS = (
    _row(101, "MARKET_FX", "6", "EUR", "106"),
    _row(102, "MARKET_FX", "-30", "PLN", "470"),
    _row(103, "COMMISSION", "-0.02", "EUR", "105.98", transaction_id=None, ordered_by=""),
)
_FILL_EVIDENCE = HistoryRangeEvidence(tip_item_id=103, confirming_tip_item_id=103, rows=_FILL_ROWS)
_FILL_BALANCES = {"EUR": Decimal("105.98"), "PLN": Decimal("470")}
_FILL_EXECUTIONS = ((2, "O1", 600_000_000, False),)
_EMPTY_EVIDENCE = HistoryRangeEvidence(tip_item_id=100, confirming_tip_item_id=100, rows=())
_SEED_BALANCES = {"EUR": Decimal("100"), "PLN": Decimal("500")}


def _fill_totals() -> dict[str, VenueOrderFillLegs]:
    """Return the canonical BUY EURPLN order totals for the one-fill range."""
    return {
        "O1": VenueOrderFillLegs(
            order_id="O1",
            bought_amount=Decimal("6"),
            sold_amount=Decimal("30"),
            commission_amount=Decimal("0.02"),
            bought_currency="EUR",
            sold_currency="PLN",
            commission_currency="EUR",
            buy_sell="BUY",
        )
    }


def _certify_fill(
    *,
    evidence: HistoryRangeEvidence | None = _FILL_EVIDENCE,
    anchor: SpotReconciliationAnchorRow | None = None,
    venue_balances: Mapping[str, Decimal] | None = None,
    parsed_executions: Sequence[tuple[int, str, int, bool]] = _FILL_EXECUTIONS,
    order_totals: Mapping[str, VenueOrderFillLegs] | None = None,
    watermark_unchanged: bool = True,
    boundary_watermark: int = 2,
) -> CertificateOutcome:
    """Certify the one-fill baseline range with keyword overrides."""
    return certify_walutomat_history_range(
        evidence=evidence,
        anchor=anchor if anchor is not None else _anchor(),
        venue_balances=venue_balances if venue_balances is not None else dict(_FILL_BALANCES),
        parsed_executions=parsed_executions,
        order_totals=order_totals if order_totals is not None else _fill_totals(),
        watermark_unchanged=watermark_unchanged,
        boundary_watermark=boundary_watermark,
    )


def _certify_empty(
    *,
    evidence: HistoryRangeEvidence | None = _EMPTY_EVIDENCE,
    anchor: SpotReconciliationAnchorRow | None = None,
    venue_balances: Mapping[str, Decimal] | None = None,
    watermark_unchanged: bool = True,
) -> CertificateOutcome:
    """Certify the empty-range baseline (W_E == W_anchor) with overrides."""
    return certify_walutomat_history_range(
        evidence=evidence,
        anchor=anchor if anchor is not None else _anchor(),
        venue_balances=venue_balances if venue_balances is not None else dict(_SEED_BALANCES),
        parsed_executions=(),
        order_totals={},
        watermark_unchanged=watermark_unchanged,
        boundary_watermark=1,
    )


def test_empty_range_certifies_as_fold_identity() -> None:
    """An empty range at an unmoved tip certifies with the empty-fold digest.

    Given: Evidence with no rows at H_E == H_anchor, venue balances equal to
        the anchor seed, and no executions past the anchor watermark.
    When: The range is certified.
    Then: The outcome is certified with the scheme-bound cursor digesting the
        empty canonical fold.
    """
    outcome = _certify_empty()
    assert outcome == CertificateOutcome(
        certified=True,
        venue_cursor=f"{_SCHEME}:100:{_EMPTY_RANGE_DIGEST}",
        refusals=(),
    )


def test_one_fill_range_certifies_through_real_witness_composition() -> None:
    """A one-fill range certifies end-to-end through the shared composition.

    Given: A BUY EURPLN fill's two MARKET_FX legs plus its COMMISSION row, the
        exec-id-decoded execution, lifetime order totals, and agreeing venue
        balances.
    When: The range is certified.
    Then: The outcome is certified and the cursor commits to the exact
        canonical fold of the delivered rows.
    """
    canonical = '[[101,"EUR","6","106"],[102,"PLN","-30","470"],[103,"EUR","-0.02","105.98"]]'
    digest = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
    outcome = _certify_fill()
    assert outcome == CertificateOutcome(
        certified=True,
        venue_cursor=f"{_SCHEME}:103:{digest}",
        refusals=(),
    )


def test_absent_evidence_refuses_history_unavailable() -> None:
    """Missing range evidence fails closed on the single unavailable refusal.

    Given: No captured range evidence (a degraded observer read).
    When: The range is certified.
    Then: The outcome is uncertified with exactly ``venue_history_unavailable``.
    """
    outcome = _certify_empty(evidence=None)
    assert outcome == CertificateOutcome(
        certified=False, venue_cursor=None, refusals=("venue_history_unavailable",)
    )


def test_wrong_cursor_kind_refuses_malformed() -> None:
    """An anchor cursor of a foreign kind is refused as malformed.

    Given: An anchor whose sealed cursor kind is not the item-id kind.
    When: The range is certified.
    Then: Exactly ``venue_cursor_malformed`` is returned.
    """
    outcome = _certify_empty(anchor=_anchor(venue_cursor_kind="order_sequence"))
    assert outcome.refusals == ("venue_cursor_malformed",)


@pytest.mark.parametrize(
    "value",
    ["", "0", "-5", "1o1", "１００"],
    ids=["empty", "zero", "negative", "alpha", "fullwidth_digits"],
)
def test_non_positive_or_non_ascii_digit_cursor_value_refuses_malformed(value: str) -> None:
    """A cursor value that is not a positive ASCII-digit integer is malformed.

    Given: An anchor cursor value that is empty, zero, negative, alphabetic,
        or made of unicode digits ``int`` would accept.
    When: The range is certified.
    Then: Exactly ``venue_cursor_malformed`` is returned.
    """
    outcome = _certify_empty(anchor=_anchor(venue_cursor_value=value))
    assert outcome.refusals == ("venue_cursor_malformed",)


def test_unregistered_cursor_scheme_refuses() -> None:
    """An anchor cursor scheme outside the registry is refused by name.

    Given: An anchor sealed under a scheme absent from the bootstrap registry.
    When: The range is certified.
    Then: Exactly ``venue_cursor_scheme_unregistered`` is returned.
    """
    outcome = _certify_empty(anchor=_anchor(venue_cursor_scheme=f"{_SCHEME}:vNext"))
    assert outcome.refusals == ("venue_cursor_scheme_unregistered",)


def test_anchor_cursor_above_tip_refuses_regressed() -> None:
    """An anchor cursor beyond the observed tip is a cursor regression.

    Given: An anchor sealed at item id 150 while the observed tip is 100.
    When: The range is certified.
    Then: Exactly ``venue_cursor_regressed`` is returned.
    """
    outcome = _certify_empty(anchor=_anchor(venue_cursor_value="150"))
    assert outcome.refusals == ("venue_cursor_regressed",)


def test_missing_confirming_tip_refuses_cursor_unavailable() -> None:
    """A failed confirming tip re-read fails closed on the double-read.

    Given: Evidence whose confirming tip read is absent.
    When: The range is certified.
    Then: Exactly ``venue_cursor_unavailable`` is returned.
    """
    outcome = _certify_empty(evidence=replace(_EMPTY_EVIDENCE, confirming_tip_item_id=None))
    assert outcome.refusals == ("venue_cursor_unavailable",)


def test_confirming_tip_below_is_regressed_and_advanced() -> None:
    """A confirming tip below the first read is both regressed and advanced.

    Given: Evidence whose confirming tip is 99 against a first read of 100.
    When: The range is certified.
    Then: Both ``venue_cursor_regressed`` and ``venue_history_advanced`` fire.
    """
    outcome = _certify_empty(evidence=replace(_EMPTY_EVIDENCE, confirming_tip_item_id=99))
    assert outcome.refusals == ("venue_cursor_regressed", "venue_history_advanced")


def test_confirming_tip_above_refuses_history_advanced() -> None:
    """A confirming tip above the first read proves in-flight venue activity.

    Given: Evidence whose confirming tip is 101 against a first read of 100.
    When: The range is certified.
    Then: Exactly ``venue_history_advanced`` is returned.
    """
    outcome = _certify_empty(evidence=replace(_EMPTY_EVIDENCE, confirming_tip_item_id=101))
    assert outcome.refusals == ("venue_history_advanced",)


def test_watermark_motion_refuses_watermark_advanced() -> None:
    """A watermark that moved across the boundary reads refuses by name.

    Given: The boundary reports the committed watermark did not hold still.
    When: The range is certified.
    Then: Exactly ``watermark_advanced`` is returned.
    """
    outcome = _certify_empty(watermark_unchanged=False)
    assert outcome.refusals == ("watermark_advanced",)


def test_non_increasing_row_ids_refuse_range_incomplete() -> None:
    """Rows delivered out of strict ascending id order break the range proof.

    Given: The one-fill rows with the two MARKET_FX legs swapped.
    When: The range is certified.
    Then: Exactly ``venue_history_range_incomplete`` is returned.
    """
    swapped = (_FILL_ROWS[1], _FILL_ROWS[0], _FILL_ROWS[2])
    outcome = _certify_fill(evidence=replace(_FILL_EVIDENCE, rows=swapped))
    assert outcome.refusals == ("venue_history_range_incomplete",)


def test_range_not_reaching_the_tip_refuses_incomplete() -> None:
    """A delivered range whose last row is short of H_E is a short delivery.

    Given: The one-fill rows ending at 103 while the tip reads 104.
    When: The range is certified.
    Then: Exactly ``venue_history_range_incomplete`` is returned.
    """
    outcome = _certify_fill(
        evidence=replace(_FILL_EVIDENCE, tip_item_id=104, confirming_tip_item_id=104)
    )
    assert outcome.refusals == ("venue_history_range_incomplete",)


def test_row_at_or_below_the_anchor_cursor_refuses_incomplete() -> None:
    """A row inside the sealed pre-anchor window violates windowed scoping.

    Given: An anchor sealed at item id 101 while the range starts at 101.
    When: The range is certified.
    Then: Exactly ``venue_history_range_incomplete`` is returned.
    """
    outcome = _certify_fill(anchor=_anchor(venue_cursor_value="101"))
    assert outcome.refusals == ("venue_history_range_incomplete",)


def test_empty_rows_with_an_advanced_tip_refuse_incomplete() -> None:
    """An advanced tip with no delivered rows is a short delivery.

    Given: Evidence whose tip advanced to 101 past the anchor at 100 with no
        rows.
    When: The range is certified.
    Then: Exactly ``venue_history_range_incomplete`` is returned.
    """
    outcome = _certify_empty(
        evidence=HistoryRangeEvidence(tip_item_id=101, confirming_tip_item_id=101, rows=())
    )
    assert outcome.refusals == ("venue_history_range_incomplete",)


def test_row_above_the_tip_refuses_incomplete() -> None:
    """A row beyond the pre-watermark tip is outside the certified window.

    Given: The one-fill rows reaching 103 while the tip reads 102.
    When: The range is certified.
    Then: Exactly ``venue_history_range_incomplete`` is returned (the fee row
        above the tip stays countable for the fee cross-check, so the witness
        composition itself still holds).
    """
    outcome = _certify_fill(
        evidence=replace(_FILL_EVIDENCE, tip_item_id=102, confirming_tip_item_id=102)
    )
    assert outcome.refusals == ("venue_history_range_incomplete",)


@pytest.mark.parametrize("operation_type", ["PAYIN", "PAYOUT", "TRANSFER", "DIRECT_FX"])
def test_external_operation_refuses_unsupported(operation_type: str) -> None:
    """Any operation beyond MARKET_FX/COMMISSION is an unsupported external flow.

    Given: The one-fill range extended by one external-operation row that folds
        cleanly into the venue balances.
    When: The range is certified.
    Then: Exactly ``venue_history_unsupported_operation`` is returned.
    """
    external = _row(
        104,
        operation_type,
        "10",
        "EUR",
        "115.98",
        order_id=None,
        transaction_id=None,
        ordered_by="",
    )
    outcome = _certify_fill(
        evidence=HistoryRangeEvidence(
            tip_item_id=104, confirming_tip_item_id=104, rows=(*_FILL_ROWS, external)
        ),
        venue_balances={"EUR": Decimal("115.98"), "PLN": Decimal("470")},
    )
    assert outcome.refusals == ("venue_history_unsupported_operation",)


def test_correcting_entry_refuses_unsupported() -> None:
    """A venue correcting entry refuses even on an otherwise supported type.

    Given: The one-fill range whose COMMISSION row is flagged as a correcting
        entry.
    When: The range is certified.
    Then: Exactly ``venue_history_unsupported_operation`` is returned.
    """
    corrected = replace(_FILL_ROWS[2], correcting_entry=True)
    outcome = _certify_fill(
        evidence=replace(_FILL_EVIDENCE, rows=(_FILL_ROWS[0], _FILL_ROWS[1], corrected))
    )
    assert outcome.refusals == ("venue_history_unsupported_operation",)


def test_witness_composition_refusal_maps_to_execution_unmapped() -> None:
    """A refused witness composition collapses to the dispatch-layer name.

    Given: A commission-only range for an execution whose order reports a
        filled base total, so the base-leg cross-check refuses inside the
        shared composition.
    When: The range is certified.
    Then: Exactly ``venue_history_execution_unmapped`` is returned.
    """
    fee_only = (
        _row(101, "COMMISSION", "-0.02", "EUR", "99.98", transaction_id=None, ordered_by=""),
    )
    outcome = _certify_fill(
        evidence=HistoryRangeEvidence(tip_item_id=101, confirming_tip_item_id=101, rows=fee_only),
        venue_balances={"EUR": Decimal("99.98"), "PLN": Decimal("500")},
    )
    assert outcome.refusals == ("venue_history_execution_unmapped",)


def test_refused_composition_uncovers_the_range_fills() -> None:
    """A refused composition also strips coverage from the range MARKET_FX rows.

    Given: The one-fill range against order totals reporting a larger base
        total, so no witness map is produced.
    When: The range is certified.
    Then: Both ``venue_history_execution_unmapped`` and
        ``venue_history_manual_unattributed`` are returned — with no witness
        map the fills are unproven regardless of their ``orderedBy``.
    """
    totals = _fill_totals()
    totals["O1"] = replace(totals["O1"], bought_amount=Decimal("7"))
    outcome = _certify_fill(order_totals=totals)
    assert outcome.refusals == (
        "venue_history_execution_unmapped",
        "venue_history_manual_unattributed",
    )


def test_unwitnessed_expected_execution_refuses_unmapped() -> None:
    """Every expected replay execution must be witnessed by the range.

    Given: A boundary watermark of 3 while only scope sequence 2 composes a
        witness from the delivered range.
    When: The range is certified.
    Then: Exactly ``venue_history_execution_unmapped`` is returned.
    """
    outcome = _certify_fill(boundary_watermark=3)
    assert outcome.refusals == ("venue_history_execution_unmapped",)


def test_manual_market_fx_outside_the_witness_map_refuses() -> None:
    """A manual venue fill in range is never attributable to the replay.

    Given: The one-fill range extended by a GUI-ordered fill's two MARKET_FX
        legs that fold cleanly into the venue balances.
    When: The range is certified.
    Then: Exactly ``venue_history_manual_unattributed`` is returned.
    """
    manual_rows = (
        *_FILL_ROWS,
        _row(
            104,
            "MARKET_FX",
            "5",
            "EUR",
            "110.98",
            order_id=None,
            transaction_id="T9",
            ordered_by="GUI/user@example.com",
        ),
        _row(
            105,
            "MARKET_FX",
            "-25",
            "PLN",
            "445",
            order_id=None,
            transaction_id="T9",
            ordered_by="GUI/user@example.com",
        ),
    )
    outcome = _certify_fill(
        evidence=HistoryRangeEvidence(
            tip_item_id=105, confirming_tip_item_id=105, rows=manual_rows
        ),
        venue_balances={"EUR": Decimal("110.98"), "PLN": Decimal("445")},
    )
    assert outcome.refusals == ("venue_history_manual_unattributed",)


def test_api_attributed_fill_outside_the_witness_map_refuses() -> None:
    """An uncovered MARKET_FX row refuses regardless of its API attribution.

    Given: The one-fill range extended by an API-attributed fill of an order
        with no replay execution and no order totals.
    When: The range is certified.
    Then: Exactly ``venue_history_manual_unattributed`` is returned — the
        ``orderedBy`` prefix alone is never attribution.
    """
    unknown_order_rows = (
        *_FILL_ROWS,
        _row(104, "MARKET_FX", "5", "EUR", "110.98", order_id="O2", transaction_id="T9"),
        _row(105, "MARKET_FX", "-25", "PLN", "445", order_id="O2", transaction_id="T9"),
    )
    outcome = _certify_fill(
        evidence=HistoryRangeEvidence(
            tip_item_id=105, confirming_tip_item_id=105, rows=unknown_order_rows
        ),
        venue_balances={"EUR": Decimal("110.98"), "PLN": Decimal("445")},
    )
    assert outcome.refusals == ("venue_history_manual_unattributed",)


def test_commission_for_an_unknown_order_refuses_unattributed() -> None:
    """A fee row is attributed only through its order-id join.

    Given: The one-fill range extended by a COMMISSION row whose order id is
        outside the read order totals.
    When: The range is certified.
    Then: Exactly ``venue_history_manual_unattributed`` is returned.
    """
    alien_fee = _row(
        104,
        "COMMISSION",
        "-0.01",
        "EUR",
        "105.97",
        order_id="OX",
        transaction_id=None,
        ordered_by="",
    )
    outcome = _certify_fill(
        evidence=HistoryRangeEvidence(
            tip_item_id=104, confirming_tip_item_id=104, rows=(*_FILL_ROWS, alien_fee)
        ),
        venue_balances={"EUR": Decimal("105.97"), "PLN": Decimal("470")},
    )
    assert outcome.refusals == ("venue_history_manual_unattributed",)


def test_commission_without_an_order_id_refuses_unattributed() -> None:
    """A fee row missing its order id has no attribution at all.

    Given: The one-fill range extended by a COMMISSION row carrying no order
        id.
    When: The range is certified.
    Then: Exactly ``venue_history_manual_unattributed`` is returned.
    """
    orphan_fee = _row(
        104,
        "COMMISSION",
        "-0.01",
        "EUR",
        "105.97",
        order_id=None,
        transaction_id=None,
        ordered_by="",
    )
    outcome = _certify_fill(
        evidence=HistoryRangeEvidence(
            tip_item_id=104, confirming_tip_item_id=104, rows=(*_FILL_ROWS, orphan_fee)
        ),
        venue_balances={"EUR": Decimal("105.97"), "PLN": Decimal("470")},
    )
    assert outcome.refusals == ("venue_history_manual_unattributed",)


def test_broken_per_row_fold_refuses_balance_chain_mismatch() -> None:
    """A row whose post-event balance disagrees with the fold refuses.

    Given: The one-fill range with the PLN leg reporting 471 after a fold of
        470.
    When: The range is certified.
    Then: Exactly ``venue_history_balance_chain_mismatch`` is returned.
    """
    broken = replace(_FILL_ROWS[1], balance_after=Decimal("471"))
    outcome = _certify_fill(
        evidence=replace(_FILL_EVIDENCE, rows=(_FILL_ROWS[0], broken, _FILL_ROWS[2]))
    )
    assert outcome.refusals == ("venue_history_balance_chain_mismatch",)


def test_terminal_fold_disagreeing_with_venue_refuses() -> None:
    """The terminal fold must equal the venue's whole balance read.

    Given: The one-fill range against a venue read reporting PLN 469 where the
        fold ends at 470.
    When: The range is certified.
    Then: Exactly ``venue_history_balance_chain_mismatch`` is returned.
    """
    outcome = _certify_fill(venue_balances={"EUR": Decimal("105.98"), "PLN": Decimal("469")})
    assert outcome.refusals == ("venue_history_balance_chain_mismatch",)


def test_venue_only_currency_refuses_balance_chain_mismatch() -> None:
    """A venue balance in a currency the fold never produced refuses.

    Given: The one-fill range against a venue read carrying an extra nonzero
        USD balance.
    When: The range is certified.
    Then: Exactly ``venue_history_balance_chain_mismatch`` is returned.
    """
    outcome = _certify_fill(
        venue_balances={"EUR": Decimal("105.98"), "PLN": Decimal("470"), "USD": Decimal("5")}
    )
    assert outcome.refusals == ("venue_history_balance_chain_mismatch",)


def test_nonzero_final_missing_from_venue_refuses() -> None:
    """A folded currency absent from the venue read must have reached zero.

    Given: The one-fill range against a venue read missing the nonzero PLN
        balance.
    When: The range is certified.
    Then: Exactly ``venue_history_balance_chain_mismatch`` is returned.
    """
    outcome = _certify_fill(venue_balances={"EUR": Decimal("105.98")})
    assert outcome.refusals == ("venue_history_balance_chain_mismatch",)


def test_empty_range_balance_drift_refuses() -> None:
    """An empty range demands the venue balances equal the anchor seed.

    Given: No rows at an unmoved tip while the venue EUR balance drifted off
        the sealed seed.
    When: The range is certified.
    Then: Exactly ``venue_history_balance_chain_mismatch`` is returned.
    """
    outcome = _certify_empty(venue_balances={"EUR": Decimal("101"), "PLN": Decimal("500")})
    assert outcome.refusals == ("venue_history_balance_chain_mismatch",)


def test_all_applicable_reasons_accumulate_sorted() -> None:
    """Several independent violations are all returned, ordered by the union.

    Given: A foreign cursor kind, an unregistered scheme, a missing confirming
        tip, and a moved watermark at once.
    When: The range is certified.
    Then: All four names are returned in the closed union's alphabetical order
        with no cursor produced.
    """
    outcome = _certify_empty(
        evidence=replace(_EMPTY_EVIDENCE, confirming_tip_item_id=None),
        anchor=_anchor(venue_cursor_kind="order_sequence", venue_cursor_scheme="walutomat:v9"),
        watermark_unchanged=False,
    )
    assert outcome == CertificateOutcome(
        certified=False,
        venue_cursor=None,
        refusals=(
            "venue_cursor_malformed",
            "venue_cursor_scheme_unregistered",
            "venue_cursor_unavailable",
            "watermark_advanced",
        ),
    )
