"""Synthetic boundary scenarios cannot post live Kraken schedule estimates.

The original quantity and A4 book residual come from the local replay plan.
Native lifecycle labels below describe synthetic test scenarios, not captured
venue pages or a reconstruction of the unavailable recorded reports.
"""

from dataclasses import dataclass
from dataclasses import replace
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from typing import Literal
from typing import cast
from unittest.mock import AsyncMock

import pytest

from snapper.application.engine.config import EngineConfigModel
from snapper.application.engine.service import TradingEngineService
from snapper.application.engine.trader import TraderCoordinator
from snapper.application.trade.trade_service import PositionProjection
from snapper.application.trade.trade_service import ShardState
from snapper.core.types import ExchangeEnum
from snapper.core.types import OrderExchange
from snapper.data.repository import SQLAlchemyRepository
from snapper.data.repository_types import AccrualLedgerInsertRow
from snapper.messaging.infrastructure.publisher import MessagePublisher
from tests.application.engine.test_engine_trader import _configure_settings
from tests.application.engine.test_engine_trader import _make_rate_row
from tests.application.engine.test_engine_trader import _make_spec

_OPENED = datetime(2026, 11, 9, tzinfo=UTC)
_NOW = _OPENED + timedelta(hours=5)


@dataclass
class _AccrualHarness:
    """Hold synthetic state and independently observable posting side effects."""

    coordinator: TraderCoordinator
    engine: TradingEngineService
    repository: AsyncMock
    shard: ShardState
    checkpoint: AsyncMock
    publisher: AsyncMock
    margin: AsyncMock


def _make_harness(
    monkeypatch: pytest.MonkeyPatch,
    quantity: float,
    financing: Literal["spot", "margin"] | None,
    exchange: OrderExchange = ExchangeEnum.KRAKEN,
) -> _AccrualHarness:
    """Prepare a due schedule without requiring venue or database access."""
    _configure_settings(monkeypatch)
    coordinator = TraderCoordinator()
    engine = TradingEngineService(
        instrument="XRP-EUR",
        execution_socket=AsyncMock(spec=MessagePublisher),
        exchange=exchange,
        cfg=EngineConfigModel(initial_cash=100.0),
        wallet_public_id="synthetic-wallet",
    )
    repository = AsyncMock(spec=SQLAlchemyRepository)
    repository.get_instrument_public_id_by_symbol.return_value = "inst-1"
    repository.get_instrument_spec.return_value = _make_spec()
    repository.get_last_accrual.return_value = None
    rate = _make_rate_row()
    rate["notional_asset"] = "EUR"
    repository.get_funding_rates.return_value = [rate]
    repository.get_shard_fill_financing.return_value = financing
    coordinator.repository = repository
    shard = ShardState(
        position=PositionProjection(
            position_qty=quantity,
            entry_price=1.00769,
            position_opened_at=_OPENED,
        ),
        cash=100.0,
    )
    coordinator.trade_service._shards[engine._shard_key] = shard
    checkpoint = AsyncMock()
    margin = AsyncMock(return_value=financing == "margin")
    publisher = AsyncMock(spec=MessagePublisher)
    coordinator.msg_publisher = publisher
    monkeypatch.setattr(coordinator, "_persist_checkpoint", checkpoint)
    monkeypatch.setattr(coordinator, "_position_uses_margin", margin)
    return _AccrualHarness(coordinator, engine, repository, shard, checkpoint, publisher, margin)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("quantity", "financing"),
    [
        pytest.param(4.07, "spot", id="original-unlevered-holding"),
        pytest.param(9.99999993922529e-09, "margin", id="consumed-native-dust-A4-book-residual"),
        pytest.param(0.0, "margin", id="closed-margin-lots"),
        pytest.param(1e-8, "spot", id="real-nonzero-unborrowed-spot-dust"),
        pytest.param(4.07, None, id="unknown-financing"),
        pytest.param(4.07, "margin", id="margin-intent-is-not-native-cost-authority"),
    ],
)
async def test_live_rollover_scan_cannot_post_estimate(
    monkeypatch: pytest.MonkeyPatch,
    quantity: float,
    financing: Literal["spot", "margin"] | None,
) -> None:
    """Live quantity and historical financing never authorize schedule costs.

    Given: a synthetic due boundary, including the locally reported A4 quantity,
    When: the live Kraken coordinator scans its rollover schedule,
    Then: no estimate changes the ledger, cash, checkpoint, or published events.
    """
    harness = _make_harness(monkeypatch, quantity, financing)
    position_before = replace(harness.shard.position)

    await harness.coordinator._accrue_engine("synthetic-engine", harness.engine, _NOW)

    harness.repository.get_last_accrual.assert_not_awaited()
    harness.repository.get_funding_rates.assert_not_awaited()
    harness.repository.insert_accrual.assert_not_awaited()
    harness.margin.assert_not_awaited()
    harness.checkpoint.assert_not_awaited()
    harness.publisher.send.assert_not_awaited()
    assert harness.shard.position == position_before
    assert harness.shard.cash == 100.0
    assert harness.engine.portfolio.cash == 100.0
    assert harness.engine.portfolio.positions == {}


@pytest.mark.asyncio
async def test_close_before_boundary_write_cannot_charge_stale_snapshot(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A pre-close snapshot cannot bypass the boundary writer's containment.

    Given: synthetic margin exposure selected before a close at the boundary,
    When: the close finishes before that stale snapshot reaches the writer,
    Then: the writer refuses the schedule cost before its first asynchronous read.
    """
    harness = _make_harness(monkeypatch, 4.07, "margin")
    stale_position = replace(harness.shard.position)
    harness.shard.position = PositionProjection()

    await harness.coordinator._accrue_one_boundary(
        engine=harness.engine,
        instrument_public_id="inst-1",
        spec=_make_spec(),
        accrual_type="rollover",
        boundary=_OPENED + timedelta(hours=4),
        position=stale_position,
        now=_NOW,
    )

    harness.repository.get_funding_rates.assert_not_awaited()
    harness.repository.insert_accrual.assert_not_awaited()
    harness.checkpoint.assert_not_awaited()
    harness.publisher.send.assert_not_awaited()
    assert harness.shard.position == PositionProjection()
    assert harness.shard.cash == 100.0
    assert harness.engine.portfolio.cash == 100.0
    assert harness.engine.portfolio.positions == {}


@pytest.mark.asyncio
async def test_paper_rollover_model_still_posts_and_updates_projection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Paper financing preserves the complete existing schedule model path.

    Given: synthetic paper exposure with margin financing and a due model rate,
    When: the coordinator processes the boundary,
    Then: it persists and publishes the model cost and updates both cash views.
    """
    harness = _make_harness(monkeypatch, 4.07, "margin", exchange=ExchangeEnum.PAPER)
    charge = 4.07 * 1.00769 * 0.00025

    await harness.coordinator._accrue_engine("synthetic-engine", harness.engine, _NOW)

    harness.margin.assert_awaited_once_with(harness.engine, _OPENED)
    harness.repository.insert_accrual.assert_awaited_once()
    inserted = cast(AccrualLedgerInsertRow, harness.repository.insert_accrual.call_args.args[0])
    assert inserted["mode"] == "paper"
    assert inserted["amount"] == charge
    assert inserted["position_quantity_at_accrual"] == 4.07
    assert harness.shard.cash == 100.0 - charge
    assert harness.shard.position.realized_pnl == -charge
    assert harness.engine.portfolio.cash == 100.0 - charge
    assert harness.engine.portfolio.positions["XRP-EUR"].realized_pnl == -charge
    harness.checkpoint.assert_awaited_once_with(harness.engine._shard_key)
    harness.publisher.send.assert_awaited_once()
