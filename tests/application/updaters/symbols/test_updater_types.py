"""Tests for symbol updater TypedDict payload definitions.

Verifies that TypedDict records can be constructed with required keys,
optional keys are truly optional, and key access patterns work correctly.
"""

from dataclasses import FrozenInstanceError
from datetime import UTC
from datetime import datetime
from decimal import Decimal

import pytest

from snapper.application.updaters.symbols.types import InstrumentMetadataInput
from snapper.application.updaters.symbols.types import KrakenSymbolRecord
from snapper.application.updaters.symbols.types import PolygonSymbolRecord
from snapper.application.updaters.symbols.types import WalutomatSymbolRecord


def test_instrument_metadata_input_is_frozen() -> None:
    """Atomic metadata cannot be mutated after construction.

    Given: A complete instrument metadata payload,
    When: A caller attempts to replace one field,
    Then: The frozen dataclass rejects the mutation.
    """
    metadata = InstrumentMetadataInput(
        tick_size=0.5,
        lot_size=2.0,
        min_order_size=2.0,
        max_order_size=None,
        cost_decimals=None,
        qty_decimals=0,
        margin_initial=None,
        position_limit_long=None,
        position_limit_short=None,
        status="active",
        contract_size=Decimal("2"),
        quantity_unit="contract_count",
        spec_source="kraken_futures:rest.get_instruments",
        spec_version="s2a-v1:test",
        spec_observed_at=datetime(2026, 7, 14, tzinfo=UTC),
        unit_certified=True,
    )
    with pytest.raises(FrozenInstanceError):
        metadata.__setattr__("tick_size", 1.0)


class TestWalutomatSymbolRecord:
    """WalutomatSymbolRecord requires all five fields."""

    def test_all_required_keys_present(self) -> None:
        """Construct a complete record and verify key access."""
        record: WalutomatSymbolRecord = {
            "native_symbol": "EUR-PLN",
            "base": "EUR",
            "quote": "PLN",
            "symbol": "EUR_PLN",
            "walutomat_rest_symbol": "EURPLN",
        }
        assert record["native_symbol"] == "EUR-PLN"
        assert record["base"] == "EUR"
        assert record["quote"] == "PLN"
        assert record["symbol"] == "EUR_PLN"
        assert record["walutomat_rest_symbol"] == "EURPLN"


class TestPolygonSymbolRecord:
    """PolygonSymbolRecord only requires ticker."""

    def test_ticker_only(self) -> None:
        """Construct with only the required ticker field."""
        record: PolygonSymbolRecord = {"ticker": "AAPL"}
        assert record["ticker"] == "AAPL"

    def test_with_optional_currency_metadata(self) -> None:
        """Construct with all optional currency fields."""
        record: PolygonSymbolRecord = {
            "ticker": "X:BTCUSD",
            "base_currency_symbol": "BTC",
            "currency_symbol": "USD",
            "currency_name": "US Dollar",
        }
        assert record["base_currency_symbol"] == "BTC"
        assert record["currency_symbol"] == "USD"
        assert record["currency_name"] == "US Dollar"


class TestKrakenSymbolRecord:
    """KrakenSymbolRecord has required base/quote and optional aliases."""

    def test_required_keys_only(self) -> None:
        """Construct with only required fields."""
        record: KrakenSymbolRecord = {
            "native_symbol": "BTC-USD",
            "base_currency": "BTC",
            "quote_currency": "USD",
        }
        assert record["native_symbol"] == "BTC-USD"
        assert record["base_currency"] == "BTC"
        assert record["quote_currency"] == "USD"

    def test_with_all_optional_aliases(self) -> None:
        """Construct with all optional alias and class fields."""
        record: KrakenSymbolRecord = {
            "native_symbol": "BTC-USD",
            "base_currency": "BTC",
            "quote_currency": "USD",
            "asset_class": "currency",
            "ws_only": "false",
            "kraken_websocket_symbol": "BTC/USD",
            "kraken_rest_symbol": "XXBTZUSD",
            "ccxt_symbol": "BTC/USD",
        }
        assert record["kraken_websocket_symbol"] == "BTC/USD"
        assert record["kraken_rest_symbol"] == "XXBTZUSD"
        assert record["asset_class"] == "currency"
        assert record["ws_only"] == "false"
        assert record["ccxt_symbol"] == "BTC/USD"

    def test_tokenized_asset_ccxt_symbol_none(self) -> None:
        """Tokenized assets produce ccxt_symbol=None from _extract_tokenized_pair."""
        record: KrakenSymbolRecord = {
            "native_symbol": "NVDA-USD",
            "base_currency": "NVDA",
            "quote_currency": "USD",
            "asset_class": "tokenized_asset",
            "ccxt_symbol": None,
        }
        assert record["ccxt_symbol"] is None
        assert record["asset_class"] == "tokenized_asset"
