"""Signal construction preserves an owner installed while metadata reads await."""

from datetime import UTC
from datetime import datetime
from typing import Literal
from typing import cast
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import pytest

import snapper.application.engine.trader as trader_module
from snapper.application.engine.service import InstrumentSpec
from snapper.application.engine.service import TradingEngineService
from snapper.application.engine.trader import SignalRoutingContext
from snapper.application.engine.trader import TraderCoordinator
from snapper.messaging.infrastructure.publisher import MessagePublisher
from snapper.messaging.infrastructure.publisher import SequenceTracker
from snapper.messaging.schemas.data import ExecutionData
from snapper.messaging.schemas.data import SignalData

WALLET = "019aaa00-0000-7000-8000-123456789abc"
SUFFIX_TWIN = "019bbb00-0000-7000-8000-123456789abc"
NOW = datetime(2026, 11, 9, 12, tzinfo=UTC)


@pytest.mark.asyncio
@pytest.mark.parametrize("owner_kind", ["matching", "different_shard", "suffix_twin"])
async def test_signal_creation_rechecks_owner_after_metadata_await(
    monkeypatch: pytest.MonkeyPatch,
    owner_kind: Literal["matching", "different_shard", "suffix_twin"],
) -> None:
    """A delayed signal retains a filled owner or refuses conflicting identity.

    Given: Manual dispatch installs and fills an engine while signal metadata loads.
    When: Signal construction resumes after that owner becomes registered.
    Then: It preserves the matching filled owner or fails closed on conflicting identity.
    """
    settings = MagicMock(
        db_url="sqlite:///:memory:",
        risk_r_per_trade=0.01,
        risk_max_leverage=2.0,
        risk_max_drawdown=0.15,
    )
    monkeypatch.setattr(trader_module, "get_settings", lambda: settings)
    monkeypatch.setattr(trader_module, "get_repository", lambda _url: MagicMock())
    coord = TraderCoordinator()
    publisher = MagicMock(tracker=SequenceTracker(), send=AsyncMock())
    coord.msg_publisher = cast(MessagePublisher, publisher)
    monkeypatch.setattr(coord, "_ensure_instrument", AsyncMock())
    context = SignalRoutingContext(
        exchange="kraken",
        mode="live",
        strategy_tag=None,
        wallet_public_id=WALLET,
        operator_public_id="signal-operator",
        engine_key="XRP/EUR@kraken-live-w123456789abc",
        shard_key="kraken.XRP/EUR.live.w123456789abc",
    )
    signal = SignalData(
        public_id="delayed-signal",
        timestamp=NOW,
        session_id="signal-session",
        sequence_id=1,
        instrument="XRP/EUR",
        exchange="kraken",
        side="buy",
        strength=1.0,
        reason="test",
        price=2.1,
        fired_at=NOW,
        wallet_public_id=WALLET,
    )
    owner_instrument = "ETH/EUR" if owner_kind == "different_shard" else "XRP/EUR"
    owner_wallet = SUFFIX_TWIN if owner_kind == "suffix_twin" else WALLET
    incumbent = TradingEngineService(
        owner_instrument,
        execution_socket=coord.msg_publisher,
        exchange="kraken",
        wallet_public_id=owner_wallet,
        operator_public_id="manual-operator",
    )

    async def install_manual_owner(instrument: str, exchange: str) -> dict[str, InstrumentSpec]:
        """Finish manual ownership and its fill before the signal constructor resumes."""
        assert instrument == "XRP/EUR"
        assert exchange == "kraken"
        assert context.engine_key not in coord.engines
        coord.engines[context.engine_key] = incumbent
        coord.last_signal_time[context.engine_key] = 123.0
        assert incumbent.apply_fill(
            ExecutionData(
                public_id="manual-fill",
                timestamp=NOW,
                session_id="manual-session",
                sequence_id=2,
                trade_id="manual-native-trade",
                exchange_order_id="manual-venue-order",
                client_order_id="manual-client-order",
                instrument=owner_instrument,
                exchange="kraken",
                side="buy",
                size=4.07,
                price=2.1,
                last_size=4.07,
                last_price=2.1,
                fee=0.0,
                fee_asset="EUR",
                status="filled",
                executed_at=NOW,
                wallet_public_id=owner_wallet,
            )
        )
        return {}

    monkeypatch.setattr(coord, "_resolve_instrument_specs", install_manual_owner)

    if owner_kind == "matching":
        owner = await coord._get_or_create_signal_engine(signal, context)
        assert owner is incumbent
    else:
        with pytest.raises(RuntimeError, match="signal owner conflicts"):
            await coord._get_or_create_signal_engine(signal, context)

    assert coord.engines[context.engine_key] is incumbent
    assert incumbent.wallet_public_id == owner_wallet
    assert incumbent.position_qty == pytest.approx(4.07)
    assert incumbent.entry_price == pytest.approx(2.1)
    assert incumbent.portfolio.position_qty(owner_instrument) == pytest.approx(4.07)
    assert "manual-native-trade" in incumbent.seen_exec_ids
    assert coord.last_signal_time[context.engine_key] == 123.0
    assert coord._recovery_certification_failed is (owner_kind != "matching")
    publisher.send.assert_not_awaited()
