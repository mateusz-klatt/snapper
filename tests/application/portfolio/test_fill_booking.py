"""Tests for caller-side position quantity booking rules."""

import pytest

from snapper.application.portfolio.fill_booking import booked_signed_quantity
from snapper.application.portfolio.fill_booking import cash_fee
from snapper.application.portfolio.fill_booking import native_base_asset
from snapper.application.portfolio.fill_booking import resolve_position_quantity_unit


def test_base_fee_reduces_received_buy_quantity() -> None:
    """A base-asset BUY fee lowers the received inventory by the fee.

    Given: A 20.04 EUR BUY charged a 0.04 EUR base fee,
    When: its booked position delta is resolved,
    Then: only the 20.00 EUR actually received is booked long.
    """
    assert booked_signed_quantity("buy", 20.04, 0.04, "EUR", "EUR", "base_asset") == pytest.approx(
        20.0
    )


def test_base_fee_increases_delivered_sell_quantity() -> None:
    """A base-asset SELL fee costs extra base beyond the sold size.

    Given: A 20.00 EUR SELL charged a 0.04 EUR base fee,
    When: its booked position delta is resolved,
    Then: the base balance falls by the sold size plus the fee.
    """
    assert booked_signed_quantity("sell", 20.0, 0.04, "EUR", "EUR", "base_asset") == pytest.approx(
        -20.04
    )


def test_quote_denominated_fee_leaves_quantity_gross() -> None:
    """A fee charged in the quote asset never changes booked base quantity.

    Given: A BUY whose fee is charged in the quote asset,
    When: its booked position delta is resolved,
    Then: the full gross base fill is booked.
    """
    assert booked_signed_quantity("buy", 20.04, 0.04, "PLN", "EUR", "base_asset") == pytest.approx(
        20.04
    )


def test_fee_at_least_fill_size_never_flips_direction() -> None:
    """A base fee larger than the fill cannot cross zero into a spurious flip.

    Given: A 2.0 BUY whose base fee exceeds the fill size,
    When: its booked position delta is resolved,
    Then: it falls back to the gross long instead of flipping short.
    """
    assert booked_signed_quantity("buy", 2.0, 3.0, "EUR", "EUR", "base_asset") == pytest.approx(2.0)


def test_cash_fee_zero_when_fee_booked_against_base_quantity() -> None:
    """A base fee already in quantity is not charged again to cash.

    Given: A 20.04 EUR BUY whose 0.04 EUR base fee reduced booked quantity,
    When: its cash-leg fee is resolved,
    Then: the cash leg charges no fee, avoiding a double count.
    """
    assert cash_fee(20.04, 0.04, "EUR", "EUR", "base_asset") == pytest.approx(0.0)


def test_cash_fee_full_for_quote_denominated_fee() -> None:
    """A quote fee that left quantity gross is charged in full to cash.

    Given: A BUY whose fee is charged in the quote asset,
    When: its cash-leg fee is resolved,
    Then: the full fee is charged to the cash leg.
    """
    assert cash_fee(20.04, 0.04, "PLN", "EUR", "base_asset") == pytest.approx(0.04)


def test_cash_fee_full_when_fee_not_booked_against_base_quantity() -> None:
    """A base fee too large to book against quantity stays on the cash leg.

    Given: A 2.0 BUY whose base fee exceeds the fill size,
    When: its cash-leg fee is resolved,
    Then: the full fee is charged to cash since quantity kept the gross fill.
    """
    assert cash_fee(2.0, 3.0, "EUR", "EUR", "base_asset") == pytest.approx(3.0)


@pytest.mark.parametrize("instrument", ["EURPLN", "-PLN", "EUR-"])
def test_native_base_asset_rejects_malformed_symbols(instrument: str) -> None:
    """Malformed native symbols do not invent a base-asset identity.

    Given: A symbol missing its separator, base, or quote component,
    When: its base asset is resolved for fee booking,
    Then: resolution fails closed with an empty identity.
    """
    assert native_base_asset(instrument) == ""


def test_native_base_asset_resolves_canonical_pair() -> None:
    """A canonical pair exposes its exact base identity.

    Given: Snapper's canonical EUR-PLN native symbol,
    When: its base asset is resolved,
    Then: the exact canonical EUR identity is returned.
    """
    assert native_base_asset("EUR-PLN") == "EUR"


def test_empty_asset_identities_never_match_as_a_base_fee() -> None:
    """Missing asset identities cannot turn an unclassified fee into a base fee.

    Given: A BUY whose base and fee asset identities are both unresolved,
    When: its signed booked quantity is calculated,
    Then: the full gross fill is retained without a guessed fee deduction.
    """
    assert booked_signed_quantity("buy", 2.0, 0.1, "", "", "base_asset") == pytest.approx(2.0)


@pytest.mark.parametrize("fee", [float("nan"), float("inf")])
def test_non_finite_base_fee_preserves_gross_quantity(fee: float) -> None:
    """A corrupt fee cannot introduce a non-finite position delta.

    Given: A finite BUY size and a non-finite fee labelled as the base asset,
    When: its booked position quantity is resolved,
    Then: quantity retains the pre-existing gross behavior without poisoning.
    """
    assert booked_signed_quantity("buy", 2.0, fee, "EUR", "EUR", "base_asset") == pytest.approx(2.0)


def test_overflowing_gross_plus_fee_magnitude_stays_on_cash_leg() -> None:
    """A fee whose gross-plus-fee magnitude overflows falls back to the cash leg.

    Given: A finite SELL smaller than its base fee's overflowing combined magnitude,
    When: its booked quantity and cash-leg fee are resolved,
    Then: quantity keeps the finite gross SELL delta and the fee books to cash,
        so the fee is still counted exactly once rather than vanishing.
    """
    assert booked_signed_quantity(
        "sell", 1e308, 9e307, "EUR", "EUR", "base_asset"
    ) == pytest.approx(-1e308)
    assert cash_fee(1e308, 9e307, "EUR", "EUR", "base_asset") == pytest.approx(9e307)


def test_zero_size_fill_cannot_become_inventory_from_a_fee() -> None:
    """A status-only execution frame never creates position quantity.

    Given: A zero-size BUY frame carrying a nonzero base-labelled fee,
    When: its position delta is resolved,
    Then: it remains a zero-quantity status frame.
    """
    assert booked_signed_quantity("buy", 0.0, 0.1, "EUR", "EUR", "base_asset") == pytest.approx(0.0)


def test_contract_count_position_ignores_base_asset_fee() -> None:
    """A collateral fee never changes derivative contract exposure.

    Given: A five-contract futures BUY with a fee denominated in ETH,
    When: the symbol base is also ETH but quantity is contract count,
    Then: the booked position remains five contracts.
    """
    assert booked_signed_quantity(
        "buy", 5.0, 0.001, "ETH", "ETH", "contract_count"
    ) == pytest.approx(5.0)


def test_quantity_unit_resolution_gates_derivative_venue() -> None:
    """Quantity-unit resolution distinguishes derivative and spot venues.

    Given: Execution rows from the futures venue and a spot venue,
    When: their position quantity units are resolved,
    Then: futures book contracts while spot venues book base inventory.
    """
    assert resolve_position_quantity_unit("kraken_futures") == "contract_count"
    assert resolve_position_quantity_unit("walutomat") == "base_asset"
