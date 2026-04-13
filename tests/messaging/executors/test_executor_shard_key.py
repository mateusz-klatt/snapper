"""Tests for executor shard_key wallet segment alignment with coordinator."""

from datetime import UTC
from datetime import datetime
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock

import pytest

from snapper.application.engine.service import TradingEngineService
from snapper.data.repository import SQLAlchemyRepository
from snapper.infrastructure.exchanges.contracts import ExecutionUpdate
from snapper.infrastructure.exchanges.contracts import OrderSideEnum
from snapper.infrastructure.exchanges.contracts import OrderStatusEnum
from snapper.infrastructure.exchanges.contracts import OrderTypeEnum
from snapper.messaging.executors.base import ExchangeExecutorService
from snapper.messaging.schemas.data import OrderRequestData

WALLET_UUID = "01968a3b-7c4d-7e0f-8a1b-2c3d4e5f6a7b"
WALLET_SHORT = WALLET_UUID.replace("-", "")[:12].lower()


class ShardKeyExecutor(ExchangeExecutorService[Any]):
    """Minimal executor stub for shard_key tests."""

    def __init__(self, wallet_public_id: str = "") -> None:
        """Initialize with configurable wallet."""
        super().__init__(wallet_public_id=wallet_public_id)
        self.settings = SimpleNamespace(
            db_url="sqlite:///:memory:",
            zmq_broker_xpub="xpub",
            zmq_broker_xsub="xsub",
            master_password=None,
            use_venue_reconciliation=False,
            use_durable_commands=False,
        )

    def _create_exchange_client(self) -> Any:
        return SimpleNamespace()

    def _get_exchange_name(self) -> str:
        return "kraken"


def _mock_repo() -> AsyncMock:
    """Create a mock SQLAlchemyRepository with insert_venue_event."""
    repo = AsyncMock(spec=SQLAlchemyRepository)
    repo.insert_venue_event = AsyncMock(return_value=1)
    return repo


@pytest.mark.asyncio
async def test_venue_event_shard_key_includes_wallet() -> None:
    """Executor with wallet produces shard_key with wallet segment.

    Given: an executor with wallet_public_id set,
    When: _record_venue_event is called for a live instrument,
    Then: shard_key contains .w{wallet_short} segment.
    """
    ex: Any = ShardKeyExecutor(wallet_public_id=WALLET_UUID)
    repo = _mock_repo()
    ex.repository = repo
    await ex._record_venue_event(
        {
            "event_type": "fill_observed",
            "exchange_name": "kraken",
            "instrument": "BTC-USD",
        }
    )
    call_dict = repo.insert_venue_event.call_args.args[0]
    assert call_dict["shard_key"] == f"kraken.BTC-USD.live.w{WALLET_SHORT}"


@pytest.mark.asyncio
async def test_venue_event_shard_key_paper_with_wallet_and_strategy() -> None:
    """Paper mode with wallet and strategy_tag produces 5-segment shard_key.

    Given: an executor with wallet_public_id set,
    When: _record_venue_event is called for paper exchange with strategy_tag,
    Then: shard_key is {exchange}.{instrument}.{mode}.w{short}.{tag}.
    """
    ex: Any = ShardKeyExecutor(wallet_public_id=WALLET_UUID)
    repo = _mock_repo()
    ex.repository = repo
    await ex._record_venue_event(
        {
            "event_type": "fill_observed",
            "exchange_name": "paper",
            "instrument": "BTC-USD",
            "side": "buy",
            "strategy_tag": "scalp",
        }
    )
    call_dict = repo.insert_venue_event.call_args.args[0]
    assert call_dict["shard_key"] == f"paper.BTC-USD.paper.w{WALLET_SHORT}.scalp"


@pytest.mark.asyncio
async def test_venue_event_shard_key_paper_with_wallet_no_strategy() -> None:
    """Paper mode with wallet but no strategy_tag produces 4-segment shard_key.

    Given: an executor with wallet_public_id set,
    When: _record_venue_event is called for paper exchange without strategy_tag,
    Then: shard_key is {exchange}.{instrument}.{mode}.w{short}.
    """
    ex: Any = ShardKeyExecutor(wallet_public_id=WALLET_UUID)
    repo = _mock_repo()
    ex.repository = repo
    await ex._record_venue_event(
        {
            "event_type": "order_accepted",
            "exchange_name": "paper",
            "instrument": "ETH-USD",
        }
    )
    call_dict = repo.insert_venue_event.call_args.args[0]
    assert call_dict["shard_key"] == f"paper.ETH-USD.paper.w{WALLET_SHORT}"


@pytest.mark.asyncio
async def test_venue_event_shard_key_matches_coordinator() -> None:
    """Executor and coordinator produce identical shard_keys for same params.

    Given: same exchange, instrument, mode, wallet, strategy,
    When: executor writes a venue event and coordinator computes _shard_key,
    Then: both shard_keys are identical.
    """
    ex: Any = ShardKeyExecutor(wallet_public_id=WALLET_UUID)
    repo = _mock_repo()
    ex.repository = repo
    await ex._record_venue_event(
        {
            "event_type": "fill_observed",
            "exchange_name": "paper",
            "instrument": "BTC-USD",
            "strategy_tag": "scalp",
        }
    )
    executor_shard = repo.insert_venue_event.call_args.args[0]["shard_key"]

    engine = TradingEngineService(
        instrument="BTC-USD",
        execution_socket=AsyncMock(),
        exchange="paper",
        strategy_tag="scalp",
        wallet_public_id=WALLET_UUID,
    )
    assert executor_shard == engine._shard_key


@pytest.mark.asyncio
async def test_venue_event_shard_key_backwards_compatible() -> None:
    """Executor without wallet produces legacy 3-segment shard_key.

    Given: an executor with empty wallet_public_id (legacy),
    When: _record_venue_event is called,
    Then: shard_key is the original 3-segment format.
    """
    ex: Any = ShardKeyExecutor(wallet_public_id="")
    repo = _mock_repo()
    ex.repository = repo
    await ex._record_venue_event(
        {
            "event_type": "fill_observed",
            "exchange_name": "kraken",
            "instrument": "BTC-USD",
        }
    )
    call_dict = repo.insert_venue_event.call_args.args[0]
    assert call_dict["shard_key"] == "kraken.BTC-USD.live"


@pytest.mark.asyncio
async def test_venue_event_shard_key_wallet_short_format() -> None:
    """Wallet short is UUID stripped of dashes, first 12 chars, lowercase.

    Given: a known UUID7 wallet_public_id,
    When: shard_key is computed,
    Then: wallet segment matches expected transformation.
    """
    ex: Any = ShardKeyExecutor(wallet_public_id=WALLET_UUID)
    repo = _mock_repo()
    ex.repository = repo
    await ex._record_venue_event(
        {
            "event_type": "order_accepted",
            "exchange_name": "kraken",
            "instrument": "ETH-USD",
        }
    )
    call_dict = repo.insert_venue_event.call_args.args[0]
    expected_short = "01968a3b7c4d"
    assert f".w{expected_short}" in call_dict["shard_key"]
    assert call_dict["shard_key"] == f"kraken.ETH-USD.live.w{expected_short}"


def _make_order(**overrides: Any) -> OrderRequestData:
    """Create an OrderRequestData for _build_execution_data tests."""
    return OrderRequestData(
        session_id="",
        sequence_id=0,
        public_id="test-pub",
        timestamp=datetime(2026, 1, 1, tzinfo=UTC),
        type="order_request",
        exchange="kraken",
        instrument=overrides.get("instrument", "BTC-USD"),
        side=overrides.get("side", "buy"),
        order_type=overrides.get("order_type", "limit"),
        price=overrides.get("price", 50000.0),
        quantity=overrides.get("quantity", 1.0),
        strategy_id=overrides.get("strategy_id", "s1"),
        client_order_id=overrides.get("client_order_id", "c1"),
        mode=overrides.get("mode", "live"),
        signaled_at=datetime.now(tz=UTC),
        operator_public_id=overrides.get("operator_public_id"),
    )


def _make_execution(**overrides: Any) -> ExecutionUpdate:
    """Create an ExecutionUpdate for _build_execution_data tests."""
    defaults: dict[str, Any] = {
        "order_id": "ex-1",
        "exec_type": "trade",
        "symbol": "BTC-USD",
        "side": OrderSideEnum.BUY,
        "order_type": OrderTypeEnum.LIMIT,
        "order_status": OrderStatusEnum.OPEN,
        "timestamp": datetime.now(UTC),
        "cum_qty": 1.0,
        "average_price": 50000.0,
        "last_qty": 1.0,
        "last_price": 50000.0,
    }
    defaults.update(overrides)
    return ExecutionUpdate(**defaults)


@pytest.mark.asyncio
async def test_build_execution_data_propagates_wallet() -> None:
    """ExecutionData from _build_execution_data includes wallet_public_id.

    Given: an executor with wallet_public_id set,
    When: _build_execution_data is called,
    Then: the resulting ExecutionData carries wallet_public_id.
    """
    ex: Any = ShardKeyExecutor(wallet_public_id=WALLET_UUID)
    order = _make_order(client_order_id="wd-1", operator_public_id="op-123")
    _topic, fill = ex._build_execution_data(_make_execution(), "ex-1", order, "kraken")
    assert fill.wallet_public_id == WALLET_UUID
    assert fill.operator_public_id == "op-123"


@pytest.mark.asyncio
async def test_build_execution_data_empty_wallet_backward_compat() -> None:
    """ExecutionData without wallet preserves empty string default.

    Given: an executor with empty wallet_public_id (legacy),
    When: _build_execution_data is called with order that has no wallet,
    Then: wallet_public_id is empty string.
    """
    ex: Any = ShardKeyExecutor(wallet_public_id="")
    order = _make_order(client_order_id="legacy-1")
    _topic, fill = ex._build_execution_data(
        _make_execution(order_id="ex-2"), "ex-2", order, "kraken"
    )
    assert fill.wallet_public_id == ""
    assert fill.operator_public_id is None


@pytest.mark.asyncio
async def test_build_execution_data_prefers_order_wallet_over_executor() -> None:
    """Order wallet takes precedence over executor wallet.

    Given: a legacy executor (empty wallet) processing a wallet-tagged order,
    When: _build_execution_data is called,
    Then: fill carries the order's wallet_public_id, not the executor's empty one.
    """
    ex: Any = ShardKeyExecutor(wallet_public_id="")
    order = _make_order(client_order_id="mixed-1")
    order.wallet_public_id = WALLET_UUID
    _topic, fill = ex._build_execution_data(
        _make_execution(order_id="ex-3"), "ex-3", order, "kraken"
    )
    assert fill.wallet_public_id == WALLET_UUID


@pytest.mark.asyncio
async def test_build_execution_data_executor_wallet_fallback() -> None:
    """Executor wallet used when order has no wallet.

    Given: a wallet-scoped executor processing an order without wallet,
    When: _build_execution_data is called,
    Then: fill carries the executor's wallet_public_id.
    """
    ex: Any = ShardKeyExecutor(wallet_public_id=WALLET_UUID)
    order = _make_order(client_order_id="fallback-1")
    _topic, fill = ex._build_execution_data(
        _make_execution(order_id="ex-4"), "ex-4", order, "kraken"
    )
    assert fill.wallet_public_id == WALLET_UUID
