"""Recognise Kraken relays of Bitnomial products for the order boundary (catalog item C4)."""

from types import SimpleNamespace

import pytest

from snapper.infrastructure.symbols import functions


def _install_mapper(monkeypatch: pytest.MonkeyPatch, websocket: dict[str, str]) -> None:
    """Point the symbol functions at a mapper holding only the given WebSocket aliases.

    Args:
        monkeypatch: Pytest monkeypatch fixture.
        websocket: Native symbol to Kraken WebSocket alias.
    """
    mapper = SimpleNamespace(native_to_kraken_ws=websocket)
    monkeypatch.setattr(functions, "_get_db_mapper", lambda: mapper)


def test_native_suffix_marks_a_relay_without_a_websocket_alias(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A native -BTNL symbol is a relay even when the WebSocket lookup would fail.

    Given: A native symbol ending in -BTNL and a catalog without its WebSocket alias,
    When: The product is checked,
    Then: It is a Bitnomial relay.
    """
    _install_mapper(monkeypatch, {})
    assert functions.is_kraken_bitnomial_product("RENDER-USD-BTNL") is True


def test_websocket_suffix_marks_a_relay_under_another_native_name(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A :BTNL WebSocket alias marks a relay whose native name lacks the suffix.

    Given: A native symbol without the suffix whose WebSocket alias ends in :BTNL,
    When: The product is checked,
    Then: It is a Bitnomial relay.
    """
    _install_mapper(monkeypatch, {"RENDERREL": "RENDER/USD:BTNL"})
    assert functions.is_kraken_bitnomial_product("RENDERREL") is True


def test_ordinary_products_are_not_relays(monkeypatch: pytest.MonkeyPatch) -> None:
    """Ordinary products stay ordinary, with or without a WebSocket alias.

    Given: A spot pair with an ordinary WebSocket alias, and a product without any,
    When: Each is checked,
    Then: Neither is a Bitnomial relay.
    """
    _install_mapper(monkeypatch, {"BTC-USD": "BTC/USD"})
    assert functions.is_kraken_bitnomial_product("BTC-USD") is False
    assert functions.is_kraken_bitnomial_product("AAPL") is False
