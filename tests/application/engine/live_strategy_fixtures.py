"""Test fixtures for routing with a prevalidated live strategy policy."""

from decimal import Decimal
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import pytest

from snapper.application.engine.service import InstrumentSpec
from snapper.application.engine.service import TradingEngineService
from snapper.application.trade.outbox import OutboxDispatcher
from snapper.core.strategy_order_cap import LiveStrategyOrderPolicy
from snapper.data.repository import Repository
from snapper.data.repository import SQLAlchemyRepository
from snapper.messaging.schemas.data import SignalData

WALLET = "01975a8b-3c7d-7000-8000-aaaaaaaaaaaa"


def live_order_policy(wallet: str = WALLET) -> LiveStrategyOrderPolicy:
    """Use a trusted limit above the quantities in legacy engine tests."""
    return LiveStrategyOrderPolicy("test_strategy", wallet, "test-operator", Decimal("10"))


def stub_live_signal_policy(monkeypatch: pytest.MonkeyPatch) -> None:
    """Supply the policy input to tests focused on independent routing behavior."""

    async def resolve(_repository: Repository, signal: SignalData) -> LiveStrategyOrderPolicy:
        return live_order_policy(signal.wallet_public_id)

    monkeypatch.setattr("snapper.application.engine.trader.resolve_live_signal_policy", resolve)


def wire_live_order_backend(engine: TradingEngineService) -> MagicMock:
    """Wire real sizing inputs and observe durable inserts without broker I/O."""
    repository = MagicMock(spec=SQLAlchemyRepository)
    repository.insert_trade_command = AsyncMock(return_value=(1, "cmd-live"))
    engine._repository = repository
    engine._outbox = MagicMock(spec=OutboxDispatcher)
    engine.instrument_specs[engine.instrument] = InstrumentSpec(
        public_id="instrument",
        lot_size=1e-8,
        quantity_unit="base_asset",
        native_order_quantity_unit="base_asset",
    )
    return repository
