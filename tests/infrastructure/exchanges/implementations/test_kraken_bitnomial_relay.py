"""Kraken client paths for Bitnomial relay products (Kraken catalog spec, item C4).

A relay gets a catalog REST alias equal to its wire name, so closed candle
history can be requested. That alias would also give the native order path a
pair and the product the class ``currency``, so ``create_order`` refuses relays
before either submit path. The RENDER answer used here is a trimmed verbatim
copy of a public OHLC response saved on 2026-11-11.
"""

import json
from collections.abc import Callable
from datetime import UTC
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import pytest

from snapper.infrastructure.exchanges.contracts import ExchangeOrderRequest
from snapper.infrastructure.exchanges.contracts import ExchangeOrderSnapshot
from snapper.infrastructure.exchanges.contracts import ExchangeOrderTypeEnum
from snapper.infrastructure.exchanges.contracts import OrderSideEnum
from snapper.infrastructure.exchanges.implementations.kraken import KrakenExchangeClient
from snapper.infrastructure.exchanges.kraken_closed_candles import ClosedCandleRequest
from snapper.infrastructure.exchanges.kraken_closed_candles import parse_closed_candles
from snapper.infrastructure.exchanges.kraken_rest_egress import KrakenRestOperationKind
from snapper.infrastructure.exchanges.kraken_rest_egress import RestProxyTarget
from snapper.infrastructure.symbols import functions

_FIXTURE = Path(__file__).resolve().parents[3] / "fixtures" / "kraken" / "ohlc_render_usd_btnl.json"
_RENDER = "RENDER-USD-BTNL"
_WIRE = "RENDER/USD:BTNL"
_FIRST_MINUTE = datetime.fromtimestamp(1794370260, UTC)
_LAST_MINUTE = datetime.fromtimestamp(1794372480, UTC)
_FETCHED_AT = datetime.fromtimestamp(1794413495, UTC)


def _install_catalog(
    monkeypatch: pytest.MonkeyPatch, *, ccxt: dict[str, str], websocket: dict[str, str]
) -> None:
    """Point the symbol functions at a catalog holding the relay's identities.

    Args:
        monkeypatch: Pytest monkeypatch fixture.
        ccxt: Native symbol to CCXT alias.
        websocket: Native symbol to Kraken WebSocket alias.
    """
    mapper = SimpleNamespace(
        native_to_ccxt=ccxt,
        native_to_kraken_ws=websocket,
        native_to_kraken_rest={_RENDER: _WIRE},
        kraken_asset_classes={_RENDER: "currency"},
    )
    monkeypatch.setattr(functions, "_get_db_mapper", lambda: mapper)


def _request(symbol: str) -> ExchangeOrderRequest:
    """Build one limit buy order request.

    Args:
        symbol: Native symbol to order.

    Returns:
        The request.
    """
    return ExchangeOrderRequest(
        symbol=symbol,
        side=OrderSideEnum.BUY,
        type=ExchangeOrderTypeEnum.LIMIT,
        amount=1.0,
        price=1.6,
        client_order_id="client-relay",
    )


def _guarded_client(monkeypatch: pytest.MonkeyPatch) -> tuple[KrakenExchangeClient, MagicMock]:
    """Build a credentialed client whose private paths are recorded, not run.

    Args:
        monkeypatch: Pytest monkeypatch fixture.

    Returns:
        The client and the recorder of every private-path entry.
    """
    client = KrakenExchangeClient(api_key="key", api_secret="secret")
    private = MagicMock()
    monkeypatch.setattr(client, "_create_order_via_ccxt", AsyncMock(side_effect=private.ccxt))
    monkeypatch.setattr(client, "_create_order_via_native", AsyncMock(side_effect=private.native))
    monkeypatch.setattr(client, "_get_trade_client", private.trade_client)
    monkeypatch.setattr(client, "_with_retry", AsyncMock(side_effect=private.retry))
    return client, private


@pytest.mark.asyncio
@pytest.mark.parametrize("ccxt", [{}, {_RENDER: "RENDER/USD"}])
async def test_relay_order_is_refused_before_any_private_path(
    monkeypatch: pytest.MonkeyPatch, ccxt: dict[str, str]
) -> None:
    """A relay order is refused with or without a CCXT mapping.

    Given: A Bitnomial relay with its REST alias, with and without a CCXT alias,
    When: An order for it is created,
    Then: It is refused as a definitive reject before any submit path, private
        client or request.
    """
    _install_catalog(monkeypatch, ccxt=ccxt, websocket={_RENDER: _WIRE})
    client, private = _guarded_client(monkeypatch)

    with pytest.raises(ValueError, match="Bitnomial relay products are market data only"):
        await client.create_order(_request(_RENDER))

    assert private.mock_calls == []


@pytest.mark.asyncio
async def test_relay_known_only_by_its_websocket_alias_is_refused(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A relay whose native name lacks the suffix is refused through its alias.

    Given: A native symbol without -BTNL whose WebSocket alias ends in :BTNL,
    When: An order for it is created,
    Then: It is refused before any private path.
    """
    _install_catalog(monkeypatch, ccxt={}, websocket={"RENDERREL": _WIRE})
    client, private = _guarded_client(monkeypatch)

    with pytest.raises(ValueError, match="Bitnomial relay products are market data only"):
        await client.create_order(_request("RENDERREL"))

    assert private.mock_calls == []


@pytest.mark.asyncio
async def test_ordinary_order_without_a_websocket_alias_keeps_its_path(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An ordinary product without a WebSocket alias is not refused.

    Given: An ordinary product absent from the WebSocket aliases,
    When: An order for it is created,
    Then: It reaches the CCXT submit path as before.
    """
    _install_catalog(monkeypatch, ccxt={}, websocket={})
    client = KrakenExchangeClient(api_key="key", api_secret="secret")
    snapshot = MagicMock(spec=ExchangeOrderSnapshot)
    via_ccxt = AsyncMock(return_value=snapshot)
    monkeypatch.setattr(client, "_create_order_via_ccxt", via_ccxt)

    assert await client.create_order(_request("AAPL")) is snapshot

    via_ccxt.assert_awaited_once()


@pytest.mark.asyncio
async def test_relay_history_is_requested_by_its_wire_name_without_a_class(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Closed history for a relay uses its catalog REST alias and no asset class.

    Given: A relay without a CCXT alias, its REST alias, the derived class
        currency, and the saved RENDER answer as the venue's reply,
    When: Closed one-minute candles are requested through the client,
    Then: The request names the exact wire string with interval 1 and since,
        sends no asset class, and the unchanged parser returns the closed minutes.
    """
    _install_catalog(monkeypatch, ccxt={}, websocket={_RENDER: _WIRE})
    client = KrakenExchangeClient.__new__(KrakenExchangeClient)
    native_get = MagicMock(return_value=json.loads(_FIXTURE.read_text(encoding="utf-8")))
    market = MagicMock()
    client._ccxt_client = SimpleNamespace(market=market, publicGetOHLC=native_get)

    async def dispatch(
        *,
        operation: str,
        kind: KrakenRestOperationKind,
        target: RestProxyTarget,
        sync_call: Callable[[], object],
    ) -> object:
        """Run the routed call inline."""
        return sync_call()

    client._dispatch_routed_rest = AsyncMock(side_effect=dispatch)

    candles = await client.get_closed_1m_candles(_RENDER, _FIRST_MINUTE, _LAST_MINUTE)

    market.assert_not_called()
    native_get.assert_called_once_with(
        {"pair": _WIRE, "interval": 1, "since": int(_FIRST_MINUTE.timestamp()) - 60}
    )
    assert len(candles) == 4


def test_saved_render_answer_passes_the_unchanged_parser() -> None:
    """The venue's RENDER answer parses into closed candles under its wire name.

    Given: The saved RENDER/USD:BTNL answer, three untraded rows, two traded
        rows and the unfinished last row,
    When: It is parsed for the window up to the last row's minute,
    Then: Four closed candles come back, the untraded ones flat with zero
        volume and the traded one with its price, volume and count.
    """
    request = ClosedCandleRequest(_RENDER, _WIRE, _FIRST_MINUTE, _LAST_MINUTE, _FETCHED_AT)

    candles = parse_closed_candles(json.loads(_FIXTURE.read_text(encoding="utf-8")), request)

    assert [(candle.close, candle.volume, candle.trades) for candle in candles] == [
        (1.62, 0.0, 0),
        (1.62, 0.0, 0),
        (1.62, 0.0, 0),
        (1.612, 90.9146, 1),
    ]
    assert all(candle.symbol == _RENDER for candle in candles)
