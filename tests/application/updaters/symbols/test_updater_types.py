"""Tests for symbol updater TypedDict payload definitions.

Verifies that TypedDict records can be constructed with required keys,
optional keys are truly optional, and key access patterns work correctly.
"""

from snapper.application.updaters.symbols.types import KrakenSymbolRecord
from snapper.application.updaters.symbols.types import PolygonSymbolRecord
from snapper.application.updaters.symbols.types import WalutomatSymbolRecord
from snapper.application.updaters.symbols.types import ZondaSymbolRecord


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


class TestZondaSymbolRecord:
    """ZondaSymbolRecord has optional ccxt_symbol."""

    def test_required_keys_only(self) -> None:
        """Construct without optional ccxt_symbol."""
        record: ZondaSymbolRecord = {
            "native_symbol": "BTC-PLN",
            "base": "BTC",
            "quote": "PLN",
            "zonda_symbol": "BTC-PLN",
        }
        assert record["native_symbol"] == "BTC-PLN"
        assert record["zonda_symbol"] == "BTC-PLN"

    def test_with_optional_ccxt_symbol(self) -> None:
        """Construct with optional ccxt_symbol included."""
        record: ZondaSymbolRecord = {
            "native_symbol": "BTC-PLN",
            "base": "BTC",
            "quote": "PLN",
            "zonda_symbol": "BTC-PLN",
            "ccxt_symbol": "BTC/PLN",
        }
        assert record["ccxt_symbol"] == "BTC/PLN"


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
