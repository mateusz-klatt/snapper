"""Actual signed SDK send responses must not become definitive rejection without placement evidence."""

from collections.abc import AsyncIterator
from collections.abc import Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass
from io import BytesIO
from urllib.parse import parse_qs

import pytest
from requests import PreparedRequest
from requests import Response
from requests import Session
from requests.adapters import BaseAdapter
from urllib3.response import HTTPResponse

import snapper.infrastructure.exchanges.implementations.kraken_futures as implementation
import snapper.infrastructure.exchanges.kraken_rest_egress as routing
from snapper.infrastructure.exchanges.contracts import ExchangeOrderRequest
from snapper.infrastructure.exchanges.contracts import ExchangeOrderTypeEnum
from snapper.infrastructure.exchanges.contracts import OrderSideEnum
from snapper.infrastructure.exchanges.errors import AmbiguousOrderSubmitError
from snapper.infrastructure.exchanges.errors import CircuitBreakerOpenError
from snapper.infrastructure.exchanges.implementations.kraken_futures import (
    KrakenFuturesExchangeClient,
)


class _ClosedFuturesReplyAdapter(BaseAdapter):
    """Return exact fixture HTTP responses only after the real SDK prepared a signed CREATE request."""

    def __init__(self, status: int, body: bytes) -> None:
        """Retain synthetic provider bytes and actual local dispatch observations."""
        super().__init__()
        self.status = status
        self.body = body
        self.calls: list[PreparedRequest] = []
        self.responses: list[Response] = []

    def send(self, request: PreparedRequest, *args: object, **kwargs: object) -> Response:
        """Intercept the real Requests adapter boundary without opening any network socket.

        Args:
            request: Actual signed SDK request with synthetic fixture credentials.
            args: Requests adapter positional arguments.
            kwargs: Requests timeout, proxy and streaming arguments.

        Returns:
            A real original HTTP response consumed by the pinned SDK decoder.
        """
        assert request.method == "POST"
        assert request.url == "https://demo-futures.kraken.com/derivatives/api/v3/sendorder"
        assert request.headers.get("APIKey") == "synthetic-send-key"
        assert request.headers.get("Authent")
        assert isinstance(request.body, str)
        assert parse_qs(request.body)["cliOrdId"] == ["synthetic-send-cid"]
        self.calls.append(request)
        response = Response()
        response.status_code = self.status
        response.url = request.url
        response.request = request
        response.raw = HTTPResponse(
            body=BytesIO(self.body), status=self.status, preload_content=False
        )
        self.responses.append(response)
        return response

    def close(self) -> None:
        """Close only owned in-memory responses, without affecting another client or transport."""
        for response in self.responses:
            response.close()


@dataclass
class _ClosedFuturesSend:
    """One real exchange/Trade SDK and its entirely local HTTP reply source."""

    client: KrakenFuturesExchangeClient
    adapter: _ClosedFuturesReplyAdapter


@asynccontextmanager
async def _closed_send(
    monkeypatch: pytest.MonkeyPatch, status: int, body: bytes
) -> AsyncIterator[_ClosedFuturesSend]:
    """Own actual SDK sessions and the real routed worker while prohibiting any external HTTP."""
    client = KrakenFuturesExchangeClient(
        sandbox=True, api_key="synthetic-send-key", api_secret="c3ludGhldGljLXNlY3JldA=="
    )
    adapter = _ClosedFuturesReplyAdapter(status, body)
    sessions: list[Session] = []
    for sdk in (client._trade_client, client._user_client):
        assert sdk is not None
        session = getattr(sdk, "_FuturesClient__session")
        assert isinstance(session, Session)
        session.trust_env = False
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        sessions.append(session)
    monkeypatch.setattr(routing, "get_egress_pool", lambda: None)
    monkeypatch.setattr(implementation, "native_to_kraken_futures_ws", lambda symbol: "PF_XBTUSD")
    try:
        yield _ClosedFuturesSend(client, adapter)
    finally:
        await client.disconnect()
        for session in sessions:
            session.close()
        adapter.close()


def _request() -> ExchangeOrderRequest:
    """Preserve the original correlation identity and ordinary supported contract order operands."""
    return ExchangeOrderRequest(
        symbol="BTC-USD-PERP",
        side=OrderSideEnum.BUY,
        type=ExchangeOrderTypeEnum.LIMIT,
        amount=1.0,
        price=100.0,
        client_order_id="synthetic-send-cid",
    )


@pytest.mark.parametrize(
    ("status", "body"),
    [
        (503, b"synthetic-provider-private-marker"),
        (200, b'{"incomplete":"synthetic-provider-private-marker"'),
        (200, b"null"),
        (200, b"[]"),
    ],
)
async def test_actual_sdk_post_send_decoder_failures_remain_ambiguous(
    monkeypatch: pytest.MonkeyPatch, status: int, body: bytes
) -> None:
    """Keep SDK HTTP/decoder failures out of the executor's definitive rejection exception path.

    Given: Real signed SDK CREATE reaches a closed local adapter before an unusable HTTP response.
    When: The actual SDK raises or returns a malformed top-level value after that send.
    Then: The client reports an ambiguous outcome with original identity and no response-body material.
    """
    async with _closed_send(monkeypatch, status, body) as scenario:
        with pytest.raises(AmbiguousOrderSubmitError) as caught:
            await scenario.client.create_order(_request())
        assert len(scenario.adapter.calls) == 1
        assert caught.value.client_order_id == "synthetic-send-cid"
        assert caught.value.instrument == "BTC-USD-PERP"
        assert "synthetic-provider-private-marker" not in str(caught.value)
        assert caught.value.__cause__ is None


@pytest.mark.parametrize(
    "body",
    [
        b'{"extra":"synthetic-provider-private-marker"}',
        (
            b'{"sendStatus":{"order_id":"actual-local-order","status":"unknown"},'
            b'"extra":"synthetic-provider-private-marker"}'
        ),
    ],
)
async def test_actual_unclassified_response_does_not_copy_provider_body_into_durable_error(
    monkeypatch: pytest.MonkeyPatch, body: bytes
) -> None:
    """Retain honest placement uncertainty without serializing unclassified provider fields in errors.

    Given: Actual signed SDK HTTP returns an unknown placement response with extra provider material.
    When: Snapper checks the original decoded placement evidence.
    Then: Ambiguity carries only stable local context, not the arbitrary original provider body.
    """
    async with _closed_send(monkeypatch, 200, body) as scenario:
        with pytest.raises(AmbiguousOrderSubmitError) as caught:
            await scenario.client.create_order(_request())
        assert len(scenario.adapter.calls) == 1
        assert caught.value.venue_answered is True
        assert "synthetic-provider-private-marker" not in str(caught.value)


async def test_actual_usable_sdk_response_preserves_original_placement_identity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Keep proven local placement separate from unusable responses after the same real SDK path.

    Given: The real signed SDK request receives a closed HTTP response with usable placed evidence.
    When: Snapper decodes and checks that exact response.
    Then: The original client identity and actual returned order identity survive unchanged.
    """
    body = b'{"result":"success","sendStatus":{"order_id":"local-order","status":"placed"}}'
    async with _closed_send(monkeypatch, 200, body) as scenario:
        order = await scenario.client.create_order(_request())
        assert len(scenario.adapter.calls) == 1
        assert order.id == "local-order"
        assert order.client_order_id == "synthetic-send-cid"


async def test_pre_dispatch_circuit_breaker_preserves_its_explicit_unsent_disposition(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Do not convert a separately proved breaker refusal into an ambiguous SDK response.

    Given: The actual client is ready but the dispatch boundary raises its explicit breaker-open error.
    When: A supported request reaches that boundary before the SDK call can run.
    Then: The original breaker type propagates and the real local HTTP adapter sees no request.
    """
    async with _closed_send(monkeypatch, 200, b"{}") as scenario:

        async def blocked[ResultT](func: Callable[[], ResultT], /) -> ResultT:
            """Refuse before executing the actual submitted SDK callable."""
            raise CircuitBreakerOpenError("synthetic-pre-dispatch-breaker")

        monkeypatch.setattr(scenario.client, "_dispatch_blocking", blocked)
        with pytest.raises(CircuitBreakerOpenError):
            await scenario.client.create_order(_request())
        assert scenario.adapter.calls == []
