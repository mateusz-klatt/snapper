"""Product execution policy stays independent of stale capability advertisements."""

from unittest.mock import MagicMock

import pytest

import snapper.application.trade.execution_products as product_policy
from snapper.application.trade.execution_products import product_execution_refusal


@pytest.mark.parametrize("instrument", ["AAPL", "AAPLX-EUR", "SPV"])
def test_known_token_product_refuses_unimplemented_accounting(
    monkeypatch: pytest.MonkeyPatch, instrument: str
) -> None:
    """Catalog provenance identifies tokens without relying on their symbol suffix.

    Given: A known Kraken token product with any supported native spelling,
    When: Execution support is evaluated independently of cached can_trade,
    Then: The decision names Snapper's missing unit and rebase accounting.
    """
    lookup = MagicMock(return_value="tokenized_asset")
    monkeypatch.setattr(product_policy, "native_to_kraken_asset_class", lookup)
    refusal = product_execution_refusal(instrument, "kraken")
    assert refusal is not None
    assert refusal.error_code == "tokenized_unit_accounting_unavailable"
    assert "Snapper cannot send" in refusal.message
    assert "corporate-action rebase accounting" in refusal.message
    assert refusal.details == {
        "instrument": instrument,
        "exchange": "kraken",
        "product_type": "tokenized_asset",
    }
    lookup.assert_called_once_with(instrument)


@pytest.mark.parametrize("instrument", ["BTC-USD-PERP", "ETH-USD-FUT-20261225"])
def test_futures_refuses_before_any_spot_catalog_lookup(
    monkeypatch: pytest.MonkeyPatch, instrument: str
) -> None:
    """A complete derivative spec does not certify the spot-shaped cash and engine ledgers.

    Given: A Kraken Futures perpetual or dated contract,
    When: Its execution product support is requested,
    Then: Snapper's derivative accounting barrier is explicit without consulting Spot aliases.
    """
    lookup = MagicMock(side_effect=AssertionError("No Spot lookup for derivatives"))
    monkeypatch.setattr(product_policy, "native_to_kraken_asset_class", lookup)
    refusal = product_execution_refusal(instrument, "kraken_futures")
    assert refusal is not None
    assert refusal.error_code == "derivative_accounting_unavailable"
    assert "cash, margin, and engine accounting" in refusal.message
    assert refusal.details["product_type"] == "derivative"
    lookup.assert_not_called()


@pytest.mark.parametrize("exchange", ["paper", "walutomat", "kraken_equities"])
def test_other_execution_paths_do_not_inherit_a_product_refusal(
    monkeypatch: pytest.MonkeyPatch, exchange: str
) -> None:
    """The selected execution path determines whether the real-money product barrier applies.

    Given: A product selected for another execution path, including paper,
    When: This narrow accounting policy is evaluated,
    Then: No product refusal or Kraken Spot catalog lookup is introduced.
    """
    lookup = MagicMock(side_effect=AssertionError("No Spot lookup for other execution paths"))
    monkeypatch.setattr(product_policy, "native_to_kraken_asset_class", lookup)
    assert product_execution_refusal("AAPL", exchange) is None
    lookup.assert_not_called()


def test_currency_class_is_not_reclassified_by_token_looking_name(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A ticker's appearance cannot override the actual Spot catalog classification.

    Given: A currency-class Spot product with a token-looking native spelling,
    When: Its product accounting policy is evaluated,
    Then: It receives no token execution refusal.
    """
    monkeypatch.setattr(product_policy, "native_to_kraken_asset_class", lambda _: "currency")
    assert product_execution_refusal("LOOKSx-USD", "kraken") is None


def test_unknown_catalog_identity_refuses_before_transport_acquisition(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Unknown products cannot reach a queued execution through a retained CCXT alias.

    Given: A native identity absent from the active Spot catalog,
    When: This product accounting policy checks it before transport,
    Then: It returns an explicit identity-unavailable refusal without guessing its class.
    """
    lookup = MagicMock(side_effect=ValueError("Unknown Kraken order asset class"))
    monkeypatch.setattr(product_policy, "native_to_kraken_asset_class", lookup)
    refusal = product_execution_refusal("UNKNOWN", "kraken")
    assert refusal is not None
    assert refusal.error_code == "execution_product_identity_unavailable"
    assert refusal.details == {
        "instrument": "UNKNOWN",
        "exchange": "kraken",
        "product_type": "unknown",
    }
    lookup.assert_called_once_with("UNKNOWN")


def test_catalog_failures_propagate_before_a_caller_can_send(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Infrastructure failures cannot silently become an ordinary-currency classification.

    Given: An unexpected catalog read failure,
    When: The accounting policy needs the product classification,
    Then: The failure propagates before the caller reaches its exchange client.
    """
    lookup = MagicMock(side_effect=RuntimeError("catalog unavailable"))
    monkeypatch.setattr(product_policy, "native_to_kraken_asset_class", lookup)
    with pytest.raises(RuntimeError, match="catalog unavailable"):
        product_execution_refusal("AAPL", "kraken")


@pytest.mark.parametrize("asset_class", ["", "equity", "TOKENIZED_ASSET", "future_asset_class"])
def test_unrecognized_catalog_class_never_inherits_currency_admission(
    monkeypatch: pytest.MonkeyPatch, asset_class: str
) -> None:
    """Only a positively classified currency may use ordinary Spot accounting.

    Given: A catalog class outside the two implemented values,
    When: Product execution is checked,
    Then: Unknown semantics are refused instead of defaulting to the currency path.
    """
    monkeypatch.setattr(product_policy, "native_to_kraken_asset_class", lambda _: asset_class)
    refusal = product_execution_refusal("OPAQUE", "kraken")
    assert refusal is not None
    assert refusal.error_code == "execution_product_identity_unavailable"
    assert refusal.details["product_type"] == "unknown"
