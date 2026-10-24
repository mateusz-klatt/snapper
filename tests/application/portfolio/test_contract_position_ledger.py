"""Golden derivative source folds, fixed arithmetic policy and explicit unit-profile continuity."""

from decimal import Decimal
from decimal import localcontext

import pytest

from snapper.application.portfolio.contract_position_ledger import ContractPositionValue
from snapper.application.portfolio.contract_position_ledger import fold_position_source
from snapper.application.portfolio.futures_account_log import FuturesAccountLogEntry
from snapper.application.portfolio.futures_position_source import FuturesPositionSourceLeg
from snapper.application.portfolio.futures_position_source import native_futures_position_leg
from snapper.core.execution_unit_snapshot import ExecutionUnitSnapshotPayload
from tests.application.portfolio.test_contract_economics import _OBSERVED
from tests.application.portfolio.test_contract_economics import _changed
from tests.application.portfolio.test_contract_economics import _snapshot


def _unit(
    quantity: str = "1", price: str = "100", inverse: bool = False, multiplier: str = "1"
) -> ExecutionUnitSnapshotPayload:
    """Provide structurally valid explicit units without claiming any durable source authority."""
    return _changed(
        _snapshot(quantity, price, "inverse" if inverse else "linear", multiplier),
        "venue_identity",
        {"wsname": "provider-contract"},
    )


def _source(
    unit: ExecutionUnitSnapshotPayload,
    before: tuple[str, str | None],
    after: tuple[str, str | None],
) -> FuturesPositionSourceLeg:
    """Retain native signed quantities and source-reported bases as independent golden operands."""
    entry = FuturesAccountLogEntry.model_validate(
        {
            "source_log_id": 17,
            "booking_uid": "Booking-Original",
            "asset": "provider-contract",
            "contract": "provider-contract",
            "collateral": "USD",
            "margin_account": "account-1",
            "info": "futures trade",
            "occurred_at": _OBSERVED,
            "old_balance": Decimal(before[0]),
            "new_balance": Decimal(after[0]),
            "old_average_entry_price": Decimal(before[1]) if before[1] is not None else Decimal(0),
            "new_average_entry_price": Decimal(after[1]) if after[1] is not None else Decimal(0),
            "target_json_pointer": "/new_entry",
            "execution": "venue-fill-1",
            **dict.fromkeys(
                (
                    "trade_price",
                    "mark_price",
                    "realized_pnl",
                    "fee",
                    "realized_funding",
                    "liquidation_fee",
                    "conversion_spread_percentage",
                )
            ),
        }
    )
    return native_futures_position_leg(entry, "provider-contract", unit.scope.instrument_public_id)


def _value(quantity: str, price: str | None) -> ContractPositionValue:
    """Provide an explicit source opening, never a default synthetic flat position."""
    return ContractPositionValue(
        quantity=Decimal(quantity),
        entry_price=Decimal(price) if price is not None else None,
        realized_pnl_since_anchor=Decimal(0),
        quote_turnover_since_anchor=Decimal(0),
    )


def test_inverse_closing_realizes_base_currency_economics_without_cash_or_fee_netting() -> None:
    """Keep inverse settlement P&L distinct from quote turnover and native cash.

    Given: Two inverse contracts with USD100 face value entered at USD100 per base unit.
    When: One is sold at USD200 with a source-reported remaining basis of USD100.
    Then: Economic realized P&L is 0.5 base units and turnover is USD100, excluding fees.
    """
    unit = _unit("-1", "200", inverse=True, multiplier="100")
    derived, observed = fold_position_source(
        _value("2", "100"), unit, _source(unit, ("2", "100"), ("1", "100"))
    )
    assert derived == observed
    assert observed.quantity == Decimal(1)
    assert observed.realized_pnl_since_anchor == Decimal("0.5")
    assert observed.quote_turnover_since_anchor == Decimal(100)


def test_linear_multiplier_applies_to_pnl_and_turnover_without_transferring_principal() -> None:
    """Use declared monetary units rather than spot quantity-times-price cash arithmetic.

    Given: Two contracts each representing two base units at an entry price of 100.
    When: One is sold at 150 and the actual source retains the remaining entry basis.
    Then: Realized quote P&L is 100 and quote turnover is 300.
    """
    unit = _unit("-1", "150", multiplier="2")
    derived, observed = fold_position_source(
        _value("2", "100"), unit, _source(unit, ("2", "100"), ("1", "100"))
    )
    assert derived == observed
    assert observed.realized_pnl_since_anchor == Decimal(100)
    assert observed.quote_turnover_since_anchor == Decimal(300)


def test_inverse_average_prediction_is_replay_stable_and_does_not_replace_reported_basis() -> None:
    """Persist source rounding differences without inheriting an ambient arithmetic context.

    Given: An inverse position gains an equal contract at twice the original entry price.
    When: Replay occurs under different caller Decimal precisions.
    Then: The fixed model policy yields the same harmonic basis and retains the source's rounded basis.
    """
    unit = _unit("1", "200", inverse=True)
    source = _source(unit, ("1", "100"), ("2", "133.33333333333333"))
    results = []
    for precision in (6, 77):
        with localcontext() as context:
            context.prec = precision
            results.append(fold_position_source(_value("1", "100"), unit, source))
    assert results[0] == results[1]
    assert results[0][0].entry_price == Decimal("133." + "3" * 47)
    assert results[0][1].entry_price == Decimal("133.33333333333333")


def test_existing_position_profile_cannot_change_multiplier_between_fills() -> None:
    """Keep the original retained unit profile as the interpretation of the whole position pool.

    Given: One position was seeded under a multiplier of one.
    When: A later fill witness presents a multiplier of two for the same instrument UUID.
    Then: The fold refuses the changed economic profile rather than silently reinterpreting inventory.
    """
    original = _unit(multiplier="1")
    changed = _unit(multiplier="2")
    source = _source(changed, ("1", "100"), ("2", "100"))
    with pytest.raises(ValueError, match="payoff_changed"):
        fold_position_source(_value("1", "100"), changed, source, original)


@pytest.mark.parametrize("field", ("quantity", "entry_price"))
def test_actual_native_predecessor_must_match_the_retained_position_before_folding(
    field: str,
) -> None:
    """Refuse missing source history instead of silently resetting quantity or entry basis.

    Given: The native old position contradicts one retained predecessor field.
    When: The next actual fill is folded.
    Then: The discontinuity refuses before any proposed economic state is returned.
    """
    unit = _unit()
    before = _value("1", "100")
    source = _source(unit, ("2", "100") if field == "quantity" else ("1", "90"), ("3", "100"))
    with pytest.raises(ValueError, match="predecessor contradicts"):
        fold_position_source(before, unit, source)
