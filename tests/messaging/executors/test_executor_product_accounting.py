"""Unsupported accounting cannot reach order creation even with stale capabilities."""

from datetime import UTC
from datetime import datetime
from typing import cast
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import pytest

import snapper.application.trade.execution_products as product_policy
import snapper.infrastructure.symbols.functions as symbol_functions
import snapper.messaging.executors.base as executor_module
from snapper.core.command_material import TradeCommandMaterialUnavailableError
from snapper.core.types import ExecutionMode
from snapper.core.types import OrderEventEnum
from snapper.core.types import OrderExchange
from snapper.data.repository import Repository
from snapper.data.repository_types import TradeCommandRow
from snapper.infrastructure.exchanges.base import ExchangeClientBase
from snapper.infrastructure.exchanges.contracts import ExchangeOrderSnapshot
from snapper.infrastructure.exchanges.contracts import ExchangeOrderStatusEnum
from snapper.infrastructure.exchanges.contracts import ExchangeOrderTypeEnum
from snapper.infrastructure.exchanges.contracts import OrderSideEnum
from snapper.infrastructure.exchanges.implementations.kraken import KrakenExchangeClient
from snapper.infrastructure.symbols.mapper import SymbolMapperService
from snapper.messaging.executors.base import ExchangeExecutorService
from snapper.messaging.schemas.data import OrderCancelData
from snapper.messaging.schemas.data import OrderRequestData
from tests.helpers.command_material import MATERIAL_WALLET


class ProductGuardExecutor(ExchangeExecutorService[ExchangeClientBase]):
    """Use the real submit and cancel methods with a transport double."""

    def __init__(self, exchange: OrderExchange, client: ExchangeClientBase) -> None:
        """Bind a selected execution venue without starting any service."""
        super().__init__(wallet_public_id=MATERIAL_WALLET)
        self.exchange = exchange
        self.exchange_client = client

    def _create_exchange_client(self) -> ExchangeClientBase:
        """Return the injected transport double."""
        assert self.exchange_client is not None
        return self.exchange_client

    def _get_exchange_name(self) -> OrderExchange:
        """Return the actual execution venue rather than the signal's market-data venue."""
        return self.exchange


def _request(exchange: OrderExchange, instrument: str) -> OrderRequestData:
    """Build one fresh command without credentials or an external request."""
    return OrderRequestData(
        public_id="product-policy-command",
        timestamp=datetime.now(UTC),
        session_id="product-policy-test",
        sequence_id=1,
        type="order_request",
        exchange=exchange,
        instrument=instrument,
        side="buy",
        order_type="limit",
        quantity=1.0,
        price=150.0,
        client_order_id="product-policy-client-id",
        strategy_id="product-policy-strategy",
        mode="live" if exchange != "paper" else "paper",
        wallet_public_id=MATERIAL_WALLET,
    )


def _arranged_product_intent(exchange: OrderExchange, instrument: str) -> TradeCommandRow:
    """Arrange explicit durable product-test intent independently of the incoming frame.

    Args:
        exchange: Actual selected execution venue.
        instrument: Product named by this test's Given clause.

    Returns:
        Fixed matching durable intent with its own logical UUID and account scope.
    """
    now = datetime.now(UTC)
    mode: ExecutionMode = "paper" if exchange == "paper" else "live"
    return TradeCommandRow(
        public_id="00000000-0000-7000-8000-000000000299",
        timestamp=now,
        session_id="arranged-product-intent",
        sequence_id=1,
        command_type="create",
        shard_key=f"{exchange}.{instrument}.{mode}.w000000000211",
        exchange=exchange,
        instrument=instrument,
        mode=mode,
        strategy_id="product-policy-strategy",
        client_order_id="product-policy-client-id",
        venue_client_id="product-policy-client-id",
        idempotency_key=None,
        side="buy",
        order_type="limit",
        quantity=1.0,
        price=150.0,
        stop_price=None,
        leverage=None,
        reduce_only=False,
        post_only=False,
        status="created",
        attempt_count=0,
        last_error=None,
        created_at=now,
        dispatched_at=None,
        acked_at=None,
        terminal_at=None,
        exchange_order_id=None,
        supersedes_command_id=None,
        correlation_id="00000000-0000-7000-8000-000000000299",
        wallet_public_id=MATERIAL_WALLET,
        operator_public_id=None,
        user_public_id=None,
        source_surface="rest",
    )


def _bind_product_intent(executor: ProductGuardExecutor, command: TradeCommandRow) -> None:
    """Bind explicit fixed Given intent through the Repository contract without bypassing proof.

    Args:
        executor: Actual submit boundary under test.
        command: Previously arranged durable expectation, never copied from a processing frame.
    """

    async def lookup(client_order_id: str, exchange: str) -> TradeCommandRow | None:
        """Resolve the fixed Given client and actual venue.

        Args:
            client_order_id: Requested native correlation identity.
            exchange: Actual execution venue requesting proof.

        Returns:
            The authored intent only when both identifiers match.
        """
        if (client_order_id, exchange) != (command["client_order_id"], command["exchange"]):
            return None
        return command.copy()

    async def require(command_public_id: str) -> TradeCommandRow:
        """Return only the exact logical intent arranged by this test.

        Args:
            command_public_id: Logical intent identity requiring proof.

        Returns:
            The previously authored request material.

        Raises:
            TradeCommandMaterialUnavailableError: If the identity was never arranged.
        """
        if command_public_id != command["public_id"]:
            raise TradeCommandMaterialUnavailableError(command_public_id, "unexpected_fixture_uuid")
        return command.copy()

    repository = MagicMock(spec=Repository)
    repository.get_active_create_command_by_client_order_id = AsyncMock(side_effect=lookup)
    repository.require_consistent_trade_command_material = AsyncMock(side_effect=require)
    executor.repository = repository


def _snapshot(instrument: str, status: ExchangeOrderStatusEnum) -> ExchangeOrderSnapshot:
    """Return an injected venue result with a stable order identity."""
    return ExchangeOrderSnapshot(
        id="existing-product-order",
        client_order_id="product-policy-client-id",
        symbol=instrument,
        side=OrderSideEnum.BUY,
        type=ExchangeOrderTypeEnum.LIMIT,
        amount=1.0,
        price=150.0,
        status=status,
        filled=0.0,
        remaining=1.0,
        timestamp=0.0,
    )


@pytest.mark.parametrize(
    ("exchange", "instrument", "code"),
    [
        ("kraken", "AAPL", "tokenized_unit_accounting_unavailable"),
        ("kraken_futures", "BTC-USD-PERP", "derivative_accounting_unavailable"),
    ],
)
async def test_stale_trade_permission_cannot_send_or_enter_ambiguous_retry(
    monkeypatch: pytest.MonkeyPatch, exchange: OrderExchange, instrument: str, code: str
) -> None:
    """The real executor refuses before transport even when all prior gates permit creation.

    Given: A stale can_trade=True and enabled live submit for an unsupported product,
    When: The real submit path receives the same command twice,
    Then: Both attempts reject definitively without a venue call or ambiguous retry state.
    """
    client = MagicMock(spec=ExchangeClientBase)
    client.create_order = AsyncMock(
        return_value=_snapshot(instrument, ExchangeOrderStatusEnum.OPEN)
    )
    client.find_order_by_client_id = AsyncMock()
    executor = ProductGuardExecutor(exchange, cast(ExchangeClientBase, client))
    for method in (
        "_is_duplicate_submit",
        "_reject_if_replay_origin",
        "_reject_if_stale",
        "_is_live_trading_interlocked",
    ):
        monkeypatch.setattr(executor, method, AsyncMock(return_value=False))
    monkeypatch.setattr(executor_module, "is_tradeable", lambda _instrument, _exchange: True)
    monkeypatch.setattr(product_policy, "native_to_kraken_asset_class", lambda _: "tokenized_asset")
    publish = AsyncMock(return_value=True)
    record = AsyncMock(return_value=True)
    ambiguous = AsyncMock()
    accepted = AsyncMock()
    monkeypatch.setattr(executor, "_publish_order_status", publish)
    monkeypatch.setattr(executor, "_record_venue_event", record)
    monkeypatch.setattr(executor, "_handle_ambiguous_submit", ambiguous)
    monkeypatch.setattr(executor, "_finalize_accepted_submit", accepted)
    _bind_product_intent(executor, _arranged_product_intent(exchange, instrument))
    order = _request(exchange, instrument)
    await executor._process_order(order)
    await executor._process_order(order)
    client.create_order.assert_not_awaited()
    client.find_order_by_client_id.assert_not_awaited()
    ambiguous.assert_not_awaited()
    accepted.assert_not_awaited()
    assert executor.pending_orders == {}
    assert executor.client_by_exchange == {}
    assert [entry.args[1] for entry in publish.await_args_list] == [
        OrderEventEnum.SUBMITTED,
        OrderEventEnum.REJECTED,
        OrderEventEnum.SUBMITTED,
        OrderEventEnum.REJECTED,
    ]
    assert len(record.await_args_list) == 2
    for entry in record.await_args_list:
        assert entry.args[0]["event_type"] == "order_rejected"
        assert code in entry.args[0]["error"]


@pytest.mark.parametrize("exchange", ["kraken", "kraken_futures"])
async def test_direct_submit_cannot_bypass_product_accounting_policy(
    monkeypatch: pytest.MonkeyPatch, exchange: OrderExchange
) -> None:
    """Direct use of the executor's last create boundary still applies product policy.

    Given: An unsupported product and a directly invoked create method,
    When: No command-dispatch capability checks run first,
    Then: A definitive ValueError is raised without acquiring the exchange client.
    """
    client = MagicMock(spec=ExchangeClientBase)
    executor = ProductGuardExecutor(exchange, cast(ExchangeClientBase, client))
    acquire = MagicMock(side_effect=AssertionError("No client acquisition on product refusal"))
    monkeypatch.setattr(executor, "_require_exchange_client", acquire)
    monkeypatch.setattr(product_policy, "native_to_kraken_asset_class", lambda _: "tokenized_asset")
    with pytest.raises(ValueError, match="accounting_unavailable"):
        await executor._execute_live_order(_request(exchange, "AAPL"))
    acquire.assert_not_called()


@pytest.mark.parametrize(
    ("exchange", "instrument"), [("paper", "AAPL"), ("paper", "BTC-USD"), ("kraken", "BTC-USD")]
)
async def test_paper_and_ordinary_spot_keep_their_existing_create_path(
    monkeypatch: pytest.MonkeyPatch, exchange: OrderExchange, instrument: str
) -> None:
    """The accounting policy leaves supported execution paths able to reach the injected client.

    Given: Paper execution or a currency-class Kraken Spot product,
    When: The executor's last create boundary runs,
    Then: The existing transport request is submitted once and its order id is returned.
    """
    client = MagicMock(spec=ExchangeClientBase)
    client.create_order = AsyncMock(
        return_value=_snapshot(instrument, ExchangeOrderStatusEnum.OPEN)
    )
    executor = ProductGuardExecutor(exchange, cast(ExchangeClientBase, client))
    monkeypatch.setattr(product_policy, "native_to_kraken_asset_class", lambda _: "currency")
    _bind_product_intent(executor, _arranged_product_intent(exchange, instrument))
    order_id = await executor._execute_live_order(_request(exchange, instrument))
    assert order_id == "existing-product-order"
    client.create_order.assert_awaited_once()


@pytest.mark.parametrize("exchange", ["kraken", "kraken_futures"])
async def test_existing_unsupported_product_order_can_still_be_cancelled(
    monkeypatch: pytest.MonkeyPatch, exchange: OrderExchange
) -> None:
    """Freezing new exposure cannot prevent cancellation of a previously placed order.

    Given: An existing tokenized or derivative order and an injected successful cancel response,
    When: The real cancellation path runs,
    Then: The client receives the cancellation and no create-product policy is consulted.
    """
    client = MagicMock(spec=ExchangeClientBase)
    client.cancel_order = AsyncMock(
        return_value=_snapshot("AAPL", ExchangeOrderStatusEnum.CANCELED)
    )
    executor = ProductGuardExecutor(exchange, cast(ExchangeClientBase, client))
    policy = MagicMock(side_effect=AssertionError("No creation policy on cancellation"))
    monkeypatch.setattr(executor_module, "product_execution_refusal", policy)
    publish = AsyncMock()
    monkeypatch.setattr(executor, "_publish_cancel_event", publish)
    cancel = OrderCancelData(
        public_id="product-policy-cancel",
        timestamp=datetime.now(UTC),
        session_id="product-policy-test",
        sequence_id=2,
        type="order_cancel",
        exchange=exchange,
        instrument="AAPL",
        exchange_order_id="existing-product-order",
        client_order_id="product-policy-client-id",
    )
    await executor._process_cancel(cancel)
    client.cancel_order.assert_awaited_once_with("existing-product-order", "AAPL")
    publish.assert_awaited_once_with(cancel, OrderEventEnum.CANCELLED)
    policy.assert_not_called()


@pytest.mark.parametrize(
    ("instrument", "ccxt_symbol"),
    [("AAPL", "AAPLx/USD"), ("AAPLX-EUR", "AAPLx/EUR"), ("OPAQUE-SPV", "opaqueSPV/USD")],
)
@pytest.mark.parametrize("cache_state", ["cold", "mixed", "unrecognized"])
async def test_queued_create_with_usable_ccxt_alias_and_unknown_product_class_refuses(
    monkeypatch: pytest.MonkeyPatch, instrument: str, ccxt_symbol: str, cache_state: str
) -> None:
    """An independently usable CCXT alias cannot bypass unknown product accounting.

    Given: A queued Kraken order with a valid CCXT alias but cold, mixed or unknown class metadata,
    When: The real executor reaches its final create boundary without REST or MCP preflight,
    Then: It refuses definitively before client acquisition and either transport can be used.
    """
    mapper = object.__new__(SymbolMapperService)
    mapper.native_to_ccxt = {instrument: ccxt_symbol}
    mapper.kraken_asset_classes = {
        "cold": {},
        "mixed": {"BTC-USD": "currency"},
        "unrecognized": {instrument: "future_asset_class"},
    }[cache_state]
    monkeypatch.setattr(symbol_functions, "_get_db_mapper", lambda: mapper)
    client = object.__new__(KrakenExchangeClient)
    client.api_key = "test-key"
    client.api_secret = "test-secret"
    ccxt_send = AsyncMock(side_effect=AssertionError("Unknown product cannot reach CCXT"))
    native_send = AsyncMock(side_effect=AssertionError("Unknown product cannot reach native API"))
    monkeypatch.setattr(client, "_submit_ccxt_order", ccxt_send)
    monkeypatch.setattr(client, "_create_order_via_native", native_send)
    executor = ProductGuardExecutor("kraken", client)
    acquire = MagicMock(return_value=client)
    monkeypatch.setattr(executor, "_require_exchange_client", acquire)
    order = _request("kraken", instrument)
    wire_request = executor_module._exchange_order_request_from_core(order, "")
    symbol, order_type, params = client._build_ccxt_order_submit(wire_request)
    assert symbol == ccxt_symbol
    assert order_type == "limit"
    assert params["clientOrderId"] == order.client_order_id
    with pytest.raises(ValueError, match="execution_product_identity_unavailable"):
        await executor._execute_live_order(order)
    acquire.assert_not_called()
    ccxt_send.assert_not_awaited()
    native_send.assert_not_awaited()
    assert executor.pending_orders == {}
    assert executor.client_by_exchange == {}
