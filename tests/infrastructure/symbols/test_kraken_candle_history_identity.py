"""Verify which Kraken products closed candle history can name over REST."""

from types import SimpleNamespace

import pytest

from snapper.infrastructure.symbols import functions


def test_history_identity_requires_a_generic_or_catalog_rest_name(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A product needs a generic market or a catalog REST alias to have history.

    Given: A currency pair, a tokenized product and a product with a WebSocket alias only,
    When: Each is checked for a REST pair name,
    Then: Only the first two can be requested.
    """
    mapper = SimpleNamespace(
        native_to_ccxt={"BTC-USD": "BTC/USD"},
        native_to_kraken_rest={"BTC-USD": "XXBTZUSD", "AAPL": "AAPLxUSD"},
    )
    monkeypatch.setattr(functions, "_get_db_mapper", lambda: mapper)
    assert functions.has_kraken_candle_history_identity("BTC-USD") is True
    assert functions.has_kraken_candle_history_identity("AAPL") is True
    assert functions.has_kraken_candle_history_identity("AAPLX-USD") is False
