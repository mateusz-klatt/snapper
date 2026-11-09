"""Offline contracts for published margin excerpts and separately synthetic fees."""

from decimal import Decimal

import pytest

from tests.helpers.margin_ledger_contract import MarginLedgerContract
from tests.helpers.margin_ledger_contract import margin_ledger_contract


@pytest.fixture
def contract() -> MarginLedgerContract:
    """Load bounded samples without contacting a venue or proprietary data store."""
    return margin_ledger_contract()


def test_excerpt_membership_does_not_claim_original_capture(
    contract: MarginLedgerContract,
) -> None:
    """Published aliases are complete only for this explicitly bounded sample set.

    Given: The locally published sanitized ledger, trade and accrual samples.
    When: Their typed excerpt contract is loaded without original native pages.
    Then: Exact sample membership and the lack of completeness evidence remain explicit.
    """
    assert contract.provenance == "published_sanitized_excerpt"
    assert contract.coverage == "samples_only_not_complete_native_history"
    assert contract.recorded_pages_available is False
    assert len(contract.missing_recorded_evidence) == 4
    assert {row.alias for row in contract.ledgers} == {f"L{i}" for i in range(1, 35)}
    assert {row.alias for row in contract.trades} == {f"T{i}" for i in range(1, 17)}
    assert {row.alias for row in contract.internal_accruals} == {f"A{i}" for i in range(1, 5)}


def test_original_unlevered_holding_is_distinct_from_internal_charges(
    contract: MarginLedgerContract,
) -> None:
    """The 4.07 XRP spot execution supplies no margin financing evidence.

    Given: The original spot buy and the three internal rollover charges.
    When: Their published financing fields and exact quantities are inspected.
    Then: The unlevered holding remains distinct from the internal charge total.
    """
    opening = contract.trades[0]
    ledger = contract.ledgers[0]
    original_charges = contract.internal_accruals[:3]

    assert opening.alias == "T1"
    assert opening.leverage == 0
    assert Decimal(opening.margin) == 0
    assert ledger.refid == opening.alias
    assert ledger.type == "trade"
    assert ledger.amount == "4.07000000"
    assert all(row.quantity == "4.07" for row in original_charges)
    assert sum(Decimal(row.amount) for row in original_charges) == Decimal("0.003075973725")


def test_consumed_native_dust_and_remaining_internal_residual_are_separate(
    contract: MarginLedgerContract,
) -> None:
    """L16 consumes real spot dust while A4 still refers to a nonzero book offset.

    Given: The spot maker residual, its collateral conversion and the A4 charge.
    When: Native dust consumption and the recorded book quantities are compared.
    Then: Native XRP reaches zero while the nonzero internal residual stays visible.
    """
    dust = contract.ledgers[11]
    consumed = contract.ledgers[15]
    residual_charge = contract.internal_accruals[3]

    assert dust.alias == "L12"
    assert consumed.alias == "L16"
    assert dust.balance == "0.00000001"
    assert consumed.asset == dust.asset == "XXRP"
    assert Decimal(consumed.amount) == -Decimal(dust.balance)
    assert Decimal(consumed.balance) == 0
    assert residual_charge.quantity == "9.99999993922529e-09"
    assert residual_charge.amount == "2.4345249852042447e-12"
    assert Decimal(residual_charge.quantity) > 0
    assert Decimal(residual_charge.amount) > 0


def test_margin_open_and_close_keep_fee_precision_and_observed_link(
    contract: MarginLedgerContract,
) -> None:
    """Posted fees differ from trade fee values and principal is not a spot credit.

    Given: The published T7 opening, T8 close and their native margin postings.
    When: Position state, close linkage, principal and fee strings are inspected.
    Then: The closed leveraged lot and the independently posted fee precision are retained.
    """
    opening = contract.trades[6]
    closing = contract.trades[7]
    open_ledger = contract.ledgers[12]
    close_ledger = contract.ledgers[16]

    assert opening.alias == "T7"
    assert opening.position_state == "closed"
    assert opening.leverage == closing.leverage == 3
    assert closing.closing is True
    assert closing.opening_trade_alias == opening.alias
    assert open_ledger.type == "margin"
    assert open_ledger.amount == "0.000000"
    assert open_ledger.fee == "0.040003"
    assert closing.fee_value == "0.03902480"
    assert close_ledger.fee == "0.039025"
    assert Decimal(closing.fee_value) != Decimal(close_ledger.fee)


def test_published_cash_legs_preserve_raw_fee_assets_without_parity(
    contract: MarginLedgerContract,
) -> None:
    """Exact excerpt arithmetic keeps EURC, ZEUR and XXRP as separate assets.

    Given: The published C-cycle native ledger legs with original monetary strings.
    When: Their amounts and fees are summed separately for each raw asset.
    Then: The stated native totals hold without assuming EURC and EUR parity.
    """
    cycle = contract.ledgers[4:27]
    margin_eurc = [row for row in cycle if row.type == "margin" and row.asset == "EURC"]
    conversion_eurc = [
        row for row in cycle if row.type == "collateralconversion" and row.asset == "EURC"
    ]

    assert sum(Decimal(row.amount) for row in margin_eurc) == Decimal("-0.005000")
    assert sum(Decimal(row.fee) for row in margin_eurc) == Decimal("0.118996")
    assert sum(Decimal(row.amount) for row in conversion_eurc) == Decimal("0.123996")
    assert sum(
        Decimal(row.amount) - Decimal(row.fee) for row in cycle if row.asset == "ZEUR"
    ) == Decimal("-0.3185")
    assert sum(Decimal(row.amount) - Decimal(row.fee) for row in cycle if row.asset == "XXRP") == 0


def test_native_rollovers_remain_distinct_from_internal_accruals(
    contract: MarginLedgerContract,
) -> None:
    """Historical posted fees retain their raw assets and do not absorb A1-A4.

    Given: Historical native rollover samples and separate internal accrual samples.
    When: Their families, raw fee assets and economic dates are inspected.
    Then: Genuine historical postings remain distinct from the four internal charges.
    """
    rollovers = [row for row in contract.ledgers if row.type == "rollover"]

    assert [(row.alias, row.asset, row.fee) for row in rollovers] == [
        ("L30", "ZEUR", "0.0473"),
        ("L32", "SOL.F", "0.0008411100"),
    ]
    assert all(Decimal(row.amount) == 0 and Decimal(row.fee) > 0 for row in rollovers)
    assert all(row.at.year < 2026 for row in rollovers)
    assert all(row.amount_asset == "EUR" for row in contract.internal_accruals)


def test_synthetic_tiny_posted_native_fee_is_preserved_exactly(
    contract: MarginLedgerContract,
) -> None:
    """A hypothetical real fee stays chargeable at any size in the fixture contract.

    Given: An explicitly synthetic native posting with a tiny positive fee.
    When: Its signed cash effect is calculated using exact decimal arithmetic.
    Then: The fee remains nonzero without being suppressed by a size threshold.
    """
    row = contract.synthetic_native_rollover
    posted_delta = Decimal(row.amount) - Decimal(row.fee)

    assert row.provenance == "synthetic"
    assert row.type == "rollover"
    assert Decimal(row.fee) == Decimal("0.0000000001")
    assert posted_delta == Decimal("-0.0000000001")
    assert posted_delta != 0


def test_settlement_has_no_invented_trade_join(contract: MarginLedgerContract) -> None:
    """R1 retains its native cash legs while its missing lifecycle join stays unknown.

    Given: The two published settlement legs sharing the opaque R1 reference.
    When: Their assets and membership in the trade sample set are inspected.
    Then: Both cash legs remain available without manufacturing a native trade join.
    """
    settlements = [row for row in contract.ledgers if row.type == "settled"]

    assert {row.refid for row in settlements} == {"R1"}
    assert {row.asset for row in settlements} == {"ZEUR", "XXBT"}
    assert "R1" not in {row.alias for row in contract.trades}


def test_terminal_orders_do_not_invent_command_specific_cancel_outcomes(
    contract: MarginLedgerContract,
) -> None:
    """Published stale commands preserve unresolved causality and separate venues.

    Given: Published Kraken and Walutomat command states with associated order history.
    When: Command outcomes, order states and terminal-time evidence are compared.
    Then: Cancel causality stays unresolved and unknown failed-create timing stays omitted.
    """
    kraken = [row for row in contract.commands if row.venue == "kraken"]
    walutomat = [row for row in contract.commands if row.venue == "walutomat"]

    assert len(kraken) == 6
    assert {row.order_alias for row in kraken} == {"O19", "O10", "O14"}
    assert all(row.order_status == "canceled" and row.filled_size == "0.0" for row in kraken)
    unresolved = [row for row in contract.commands if row.status in {"accepted", "dispatched"}]
    assert all(row.terminal_at is None for row in unresolved)
    assert all("terminal_at" in row.model_fields_set for row in unresolved)
    assert all(row.causal_cancel_acknowledgment is False for row in contract.commands)
    assert {row.status for row in kraken if row.type == "create"} == {"accepted"}
    assert {row.status for row in kraken if row.type == "cancel"} == {"dispatched"}
    assert [(row.type, row.status) for row in walutomat] == [
        ("create", "failed"),
        ("cancel", "dispatched"),
    ]
    assert "terminal_at" not in walutomat[0].model_fields_set
    assert all(row.order_alias is None and row.order_status is None for row in walutomat)
