"""Pinned SDK decoding reaches actual private callbacks before parsing and queue backpressure."""

import asyncio
import base64
import json
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC
from datetime import datetime
from types import SimpleNamespace
from typing import cast
from unittest.mock import MagicMock
from uuid import UUID

import kraken.futures.websocket as futures_websocket
import kraken.spot.websocket.connectors as spot_websocket
import pytest
from kraken.futures import FuturesWSClient
from kraken.spot.websocket import SpotWSClientBase
from websockets.datastructures import Headers

import snapper.infrastructure.exchanges.adapters.kraken as spot_adapter
import snapper.infrastructure.exchanges.adapters.kraken_futures as futures_adapter
import snapper.infrastructure.exchanges.implementations.kraken as spot_module
import snapper.infrastructure.exchanges.implementations.kraken_futures as futures_module
from snapper.infrastructure.exchanges.contracts import ExchangeOrderStatusEnum
from snapper.infrastructure.exchanges.contracts import ExchangeOrderTypeEnum
from snapper.infrastructure.exchanges.contracts import ExecutionUpdate
from snapper.infrastructure.exchanges.contracts import OrderSideEnum
from snapper.infrastructure.exchanges.contracts import PrivateExecutionObservation
from snapper.infrastructure.exchanges.execution_wire_receipts import PrivateExecutionJsonCodec
from snapper.infrastructure.exchanges.implementations.kraken import KrakenExchangeClient
from snapper.infrastructure.exchanges.implementations.kraken_futures import (
    KrakenFuturesExchangeClient,
)

type _Client = KrakenExchangeClient | KrakenFuturesExchangeClient


def _symbol(value: str) -> str:
    """Supply an isolated catalog alias without connecting the symbol repository."""
    return {"BTC/USD": "BTC-USD", "PF_XBTUSD": "BTC-USD-PERP"}[value]


@pytest.fixture(params=("spot", "futures"))
async def client(
    request: pytest.FixtureRequest, monkeypatch: pytest.MonkeyPatch
) -> AsyncIterator[_Client]:
    """Build actual clients with no network calls and independent known symbol aliases."""
    monkeypatch.setattr(spot_module, "get_settings", MagicMock(return_value=SimpleNamespace()))
    monkeypatch.setattr(spot_adapter, "kraken_websocket_to_native", _symbol)
    monkeypatch.setattr(futures_module, "kraken_futures_ws_to_native", _symbol)
    actual: _Client
    if request.param == "spot":
        actual = KrakenExchangeClient(api_key="synthetic", api_secret="c3ludGhldGlj")
    else:
        actual = KrakenFuturesExchangeClient(api_key="synthetic", api_secret="c3ludGhldGlj")
    try:
        yield actual
    finally:
        await actual.disconnect()


def _record(client: _Client, identity: object = "fill-1") -> dict[str, object]:
    """Construct provider-specific real execution fields with exact original amount text."""
    if isinstance(client, KrakenExchangeClient):
        return {
            "order_id": "order-1",
            "exec_id": identity,
            "exec_type": "trade",
            "symbol": "BTC/USD",
            "side": "buy",
            "order_type": "limit",
            "order_status": "partially_filled",
            "timestamp": "2026-10-23T09:00:00Z",
            "last_qty": "0.100000000000000001",
            "last_price": "100.000000000000001",
            "cum_qty": 1.0,
            "cum_cost": 100.0,
            "avg_price": 100.0,
            "order_qty": 2.0,
            "fees": [{"asset": "USD", "qty": "0.010000000000000001"}],
        }
    return {
        "fill_id": identity,
        "order_id": "order-1",
        "instrument": "PF_XBTUSD",
        "buy": True,
        "qty": "0.100000000000000001",
        "price": "100.000000000000001",
        "time": 1743676200000,
        "fee_paid": "0.010000000000000001",
        "fee_currency": "USD",
    }


def _frame(client: _Client, records: list[object]) -> dict[str, object]:
    """Keep records in their actual channel envelope before any selected-leaf parsing."""
    if isinstance(client, KrakenExchangeClient):
        return {"channel": "executions", "type": "update", "data": records}
    return {"feed": "fills", "account": "venue-account-1", "fills": records}


def _wire(client: _Client, records: list[object]) -> str:
    """Use actual JSON numeric tokens rather than quoted strings at the SDK boundary."""
    source = json.dumps(_frame(client, records), separators=(",", ":"))
    for value in ("0.100000000000000001", "100.000000000000001", "0.010000000000000001"):
        source = source.replace('"' + value + '"', value)
    return source


async def _deliver(client: _Client, message: dict[str, object]) -> None:
    """Call the actual installed application callback for the selected SDK."""
    if isinstance(client, KrakenExchangeClient):
        await client._on_message(message)
    else:
        await client._on_execution_message(message)


class _Socket:
    """One real SDK decoder input with an explicit synthetic handshake and no network."""

    def __init__(self, source: str, futures: bool) -> None:
        self.source = source
        self.sent: list[str] = []
        self.reads = 0
        self.request = SimpleNamespace(
            path="/ws/v1" if futures else "/v2",
            headers=Headers({"Host": "futures.kraken.com" if futures else "ws-auth.kraken.com"}),
        )

    async def recv(self) -> str:
        """Deliver the exact payload once and refuse an accidental extra receive."""
        self.reads += 1
        assert self.reads == 1
        return self.source

    async def send(self, message: str) -> None:
        """Accept only the SDK's ordinary outbound control JSON."""
        self.sent.append(message)


async def _run_pinned_decoder(
    client: _Client, source: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Exercise the pinned connector's actual JSON loads and awaited application callback."""
    futures = isinstance(client, KrakenFuturesExchangeClient)
    socket = _Socket(source, futures)

    async def callback(message: dict[str, object]) -> None:
        await _deliver(client, message)
        connector.keep_alive = False

    async def token() -> dict[str, str]:
        return {"token": "synthetic-local-only"}

    @asynccontextmanager
    async def connect(*args: object, **kwargs: object) -> AsyncIterator[_Socket]:
        assert args[0] == (
            "wss://futures.kraken.com/ws/v1" if futures else "wss://ws-auth.kraken.com/v2"
        )
        yield socket

    connector: futures_websocket.ConnectFuturesWebsocket | spot_websocket.ConnectSpotWebsocket
    if futures:
        connector = futures_websocket.ConnectFuturesWebsocket(
            client=cast(FuturesWSClient, SimpleNamespace()),
            endpoint="futures.kraken.com/ws/v1",
            callback=callback,
        )
        monkeypatch.setattr(futures_websocket, "connect", connect)
        method = "_ConnectFuturesWebsocket__run"
    else:
        connector = spot_websocket.ConnectSpotWebsocket(
            client=cast(SpotWSClientBase, SimpleNamespace(get_ws_token=token)),
            endpoint="wss://ws-auth.kraken.com/v2",
            callback=callback,
            is_auth=True,
        )
        monkeypatch.setattr(spot_websocket, "connect", connect)
        method = "_ConnectSpotWebsocketBase__run"
    await getattr(connector, method)(asyncio.Event())
    assert socket.reads == 1


async def test_pinned_sdk_decoder_preserves_full_wire_selector_and_exact_execution_operands(
    client: _Client, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The real installed connector reaches actual capture and parsing with original precise numbers.

    Given: A pinned SDK connector receiving a real JSON numeric payload through a fake socket,
    When: Its decoder calls the actual client callback,
    Then: Original full bytes, selector, receipt identity and exact execution values survive.
    """
    captured: list[PrivateExecutionObservation] = []

    async def sink(observation: PrivateExecutionObservation) -> str:
        captured.append(observation)
        return str(UUID(int=len(captured)))

    client.set_execution_observation_sink(sink)
    source = _wire(client, [_record(client)])
    await _run_pinned_decoder(client, source, monkeypatch)
    assert len(captured) == 1
    observation = captured[0]
    assert observation.representation == "wire_bytes_base64"
    assert base64.b64decode(observation.material) == source.encode()
    assert observation.context.target_json_pointer == (
        "/data/0" if isinstance(client, KrakenExchangeClient) else "/fills/0"
    )
    update = client._execution_queue.get_nowait()
    assert update.observation_public_id == str(UUID(int=1))
    assert update.source_observation is observation
    assert update.last_qty_decimal == "0.100000000000000001"
    assert update.last_price_decimal == "100.000000000000001"
    if isinstance(client, KrakenExchangeClient):
        assert update.fees is not None
        assert update.fees[0].quantity_decimal == "0.010000000000000001"
    else:
        assert update.fee_usd_equiv_decimal == "0.010000000000000001"


async def test_every_record_is_captured_before_full_execution_queue_can_block_or_cancel(
    client: _Client,
) -> None:
    """Backpressure preserves both old updates and later records in the same received frame.

    Given: A full bounded execution queue and two records in one original message,
    When: The callback waits to enqueue its first parsed record,
    Then: Both receipts are already retained, cancellation propagates and the old item survives.
    """
    old = ExecutionUpdate(
        order_id="old",
        exec_type="trade",
        symbol="BTC-USD",
        side=OrderSideEnum.BUY,
        order_type=ExchangeOrderTypeEnum.LIMIT,
        order_status=ExchangeOrderStatusEnum.OPEN,
        timestamp=datetime(2026, 10, 23, tzinfo=UTC),
    )
    client._execution_queue = asyncio.Queue(maxsize=1)
    client._execution_queue.put_nowait(old)
    captured: list[PrivateExecutionObservation] = []
    all_captured = asyncio.Event()

    async def sink(observation: PrivateExecutionObservation) -> str:
        captured.append(observation)
        if len(captured) == 2:
            all_captured.set()
        return str(UUID(int=len(captured)))

    client.set_execution_observation_sink(sink)
    message = PrivateExecutionJsonCodec.loads(
        _wire(client, [_record(client), _record(client, "fill-2")])
    )
    assert isinstance(message, dict)
    task = asyncio.create_task(_deliver(client, message))
    try:
        await asyncio.wait_for(all_captured.wait(), timeout=2)
        assert not task.done()
        assert client._execution_queue.qsize() == 1
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    assert client._execution_queue.get_nowait() is old
    assert len(captured) == 2
    pointer = captured[1].context.target_json_pointer
    assert pointer is not None and pointer.endswith("/1")


async def test_sink_failure_propagates_before_any_frame_record_is_published(
    client: _Client,
) -> None:
    """A failed durable write cannot be acknowledged as a parsed/queued execution.

    Given: Two records whose second durable capture fails,
    When: The actual private callback processes the original whole frame,
    Then: Failure propagates, the first receipt retains the whole frame and no update queues.
    """
    captured: list[PrivateExecutionObservation] = []

    async def sink(observation: PrivateExecutionObservation) -> str:
        captured.append(observation)
        if len(captured) == 2:
            raise RuntimeError("synthetic storage failure")
        return str(UUID(int=1))

    client.set_execution_observation_sink(sink)
    source = _wire(client, [_record(client), _record(client, "fill-2")])
    message = PrivateExecutionJsonCodec.loads(source)
    assert isinstance(message, dict)
    with pytest.raises(RuntimeError, match="synthetic storage failure"):
        await _deliver(client, message)
    assert client._execution_queue.empty()
    assert base64.b64decode(captured[0].material) == source.encode()


@pytest.mark.parametrize("identity", ("   ", None, 23, " exact-id "))
async def test_malformed_or_padded_ids_do_not_prevent_raw_capture(
    client: _Client, identity: object
) -> None:
    """Bad metadata cannot make the source receipt disappear before interpretation.

    Given: A blank, missing, nonstring or nonblank padded provider execution id,
    When: The actual callback first captures its selected record,
    Then: Only nonblank exact text is projected while the full original evidence survives.
    """
    captured: list[PrivateExecutionObservation] = []

    async def sink(observation: PrivateExecutionObservation) -> str:
        captured.append(observation)
        return str(UUID(int=1))

    client.set_execution_observation_sink(sink)
    message = PrivateExecutionJsonCodec.loads(_wire(client, [_record(client, identity)]))
    assert isinstance(message, dict)
    await _deliver(client, message)
    assert len(captured) == 1
    assert captured[0].context.source_identity == (
        identity if isinstance(identity, str) and identity.strip() else None
    )
    assert captured[0].representation == "wire_bytes_base64"


@pytest.mark.parametrize(
    "event", ("challenge", "subscribed", "subscribed_failed", "unsubscribed_failed")
)
async def test_authentication_and_subscription_controls_are_never_captured(
    client: _Client, event: str
) -> None:
    """Known control envelopes cannot acquire financial receipt metadata or durable capture.

    Given: An authenticated feed/channel name inside a known control response,
    When: It reaches the actual client callback,
    Then: No raw financial observation or execution update is created.
    """
    captured: list[PrivateExecutionObservation] = []

    async def sink(observation: PrivateExecutionObservation) -> str:
        captured.append(observation)
        return str(UUID(int=1))

    client.set_execution_observation_sink(sink)
    await _deliver(client, {**_frame(client, [_record(client)]), "event": event})
    assert not captured
    assert client._execution_queue.empty()


async def test_futures_account_log_uses_actual_new_entry_and_snapshot_selectors() -> None:
    """Incremental account logs and history snapshots retain different real source selectors.

    Given: An actual account_log.new_entry response and an account_log_snapshot.logs array,
    When: Both full payloads reach the real Futures callback,
    Then: Their exact booking identities and pointers survive without creating fill updates.
    """
    client = KrakenFuturesExchangeClient()
    captured: list[PrivateExecutionObservation] = []

    async def sink(observation: PrivateExecutionObservation) -> str:
        captured.append(observation)
        return str(UUID(int=len(captured)))

    client.set_execution_observation_sink(sink)
    try:
        for feed, records in (
            ("account_log", {"new_entry": {"booking_uid": "booking-1", "amount": "1.25"}}),
            ("account_log_snapshot", {"logs": [{"booking_uid": "booking-2", "amount": "2.50"}]}),
        ):
            source = json.dumps({"feed": feed, "account": "venue-account-1", **records})
            message = PrivateExecutionJsonCodec.loads(source)
            assert isinstance(message, dict)
            await client._on_execution_message(message)
        assert [o.context.target_json_pointer for o in captured] == ["/new_entry", "/logs/0"]
        assert [o.context.source_identity for o in captured] == ["booking-1", "booking-2"]
        assert all(o.context.venue_account_id == "venue-account-1" for o in captured)
        assert client._execution_queue.empty()
    finally:
        await client.disconnect()


@pytest.mark.parametrize("quantity", (0.1, True))
async def test_reconstructed_callback_keeps_weaker_receipt_and_no_exact_numeric_claim(
    client: _Client, quantity: float | bool
) -> None:
    """Reconstructed callback fields cannot acquire precision merely by parsing them again.

    Given: Reconstructed numeric fields containing a binary float or a boolean,
    When: Its real callback captures and parses the record,
    Then: The receipt is explicitly reconstructed and exact execution operands remain absent.
    """
    captured: list[PrivateExecutionObservation] = []

    async def sink(observation: PrivateExecutionObservation) -> str:
        captured.append(observation)
        return str(UUID(int=1))

    client.set_execution_observation_sink(sink)
    message = json.loads(_wire(client, [_record(client)]))
    message["data" if isinstance(client, KrakenExchangeClient) else "fills"][0][
        "last_qty" if isinstance(client, KrakenExchangeClient) else "qty"
    ] = quantity
    await _deliver(client, message)
    assert captured[0].representation == "parsed_fields_json"
    if isinstance(quantity, bool):
        assert client._execution_queue.empty()
        assert "true" in captured[0].material
        return
    update = client._execution_queue.get_nowait()
    assert update.last_qty_decimal is None
    assert update.last_price_decimal is None
    assert update.source_observation is captured[0]


@pytest.mark.parametrize("boolean", (True, False))
def test_exact_operand_boundary_refuses_boolean_numeric_evidence(boolean: bool) -> None:
    """Boolean operands cannot acquire exact financial quantity evidence through integer subtyping.

    Given: A boolean presented at either provider's raw numeric evidence boundary,
    When: The exact operand helper is called independently of the strict response schema,
    Then: Neither helper produces an exact numeric claim.
    """
    assert spot_adapter._raw_execution_decimal({"last_qty": boolean}, "last_qty") is None
    assert futures_adapter._raw_fill_decimal(boolean) is None


async def test_actual_client_route_projects_proved_sdk_endpoints(client: _Client) -> None:
    """Concrete route overrides expose the actual constructed production endpoints.

    Given: A real SDK and CCXT client pair constructed without network traffic,
    When: The application requests its public execution route,
    Then: Its provider-specific override returns the agreed production origin.
    """
    route = client.execution_api_route()
    assert route is not None
    assert route.environment == "production"
    assert route.public_api_origin == (
        "https://api.kraken.com"
        if isinstance(client, KrakenExchangeClient)
        else "https://futures.kraken.com"
    )


@pytest.mark.parametrize("client", ("spot",), indirect=True)
async def test_spot_channel_dispatch_preserves_durable_capture_before_publication(
    client: _Client,
) -> None:
    """The channel-dispatch entry point preserves the same private evidence contract.

    Given: A genuine execution envelope passed through the generic channel dispatcher,
    When: Its private record reaches the execution handler,
    Then: The queued update references its committed full-frame receipt.
    """
    assert isinstance(client, KrakenExchangeClient)
    captured: list[PrivateExecutionObservation] = []

    async def sink(observation: PrivateExecutionObservation) -> str:
        captured.append(observation)
        return str(UUID(int=1))

    client.set_execution_observation_sink(sink)
    await client._handle_channel_data(_frame(client, [_record(client)]))
    update = client._execution_queue.get_nowait()
    assert update.source_observation is captured[0]
    assert update.observation_public_id == str(UUID(int=1))


@pytest.mark.parametrize("malformed", (None, {"unrecognized": "record"}))
async def test_unparseable_records_remain_durable_without_suppressing_later_valid_fill(
    client: _Client, malformed: object
) -> None:
    """A malformed first leaf neither disappears nor shifts a later fill's original selector.

    Given: A malformed record followed by a genuine fill in the same original frame,
    When: The actual callback captures all leaves before parsing,
    Then: Both receipts survive and only the valid record queues with its original index.
    """
    captured: list[PrivateExecutionObservation] = []

    async def sink(observation: PrivateExecutionObservation) -> str:
        captured.append(observation)
        return str(UUID(int=len(captured)))

    client.set_execution_observation_sink(sink)
    message = PrivateExecutionJsonCodec.loads(_wire(client, [malformed, _record(client)]))
    assert isinstance(message, dict)
    await _deliver(client, message)
    assert len(captured) == 2
    assert client._execution_queue.qsize() == 1
    update = client._execution_queue.get_nowait()
    assert update.observation_public_id == str(UUID(int=2))
    assert update.source_observation is captured[1]


@pytest.mark.parametrize("records", ([], "malformed"))
async def test_empty_or_malformed_financial_container_is_retained_as_unknown(
    client: _Client, records: object
) -> None:
    """An uninterpretable financial envelope retains source evidence instead of disappearing.

    Given: An empty or malformed private financial record container,
    When: The actual callback receives the original full message,
    Then: One unknown observation survives and no fill is fabricated.
    """
    captured: list[PrivateExecutionObservation] = []

    async def sink(observation: PrivateExecutionObservation) -> str:
        captured.append(observation)
        return str(UUID(int=1))

    client.set_execution_observation_sink(sink)
    message = _frame(client, [])
    message["data" if isinstance(client, KrakenExchangeClient) else "fills"] = records
    await _deliver(client, message)
    assert len(captured) == 1
    assert captured[0].context.kind == "unknown"
    assert client._execution_queue.empty()


@pytest.mark.parametrize("sandbox", (False, True))
async def test_actual_futures_client_configures_ccxt_and_sdk_to_the_same_environment(
    sandbox: bool,
) -> None:
    """The actual constructor changes transport URLs before it can report a demo selection.

    Given: A real Futures client configured for production or demo,
    When: Its selected CCXT and authenticated SDK instances are inspected,
    Then: Routing proof agrees with all actual endpoint fields without a network request.
    """
    client = KrakenFuturesExchangeClient(
        sandbox=sandbox, api_key="synthetic", api_secret="c3ludGhldGlj"
    )
    try:
        route = client.execution_api_route()
        assert route is not None
        assert route.environment == ("demo" if sandbox else "production")
        assert route.public_api_origin == (
            "https://demo-futures.kraken.com" if sandbox else "https://futures.kraken.com"
        )
    finally:
        await client.disconnect()
