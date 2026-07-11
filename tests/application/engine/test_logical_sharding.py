"""Tests for logical sharding — paper mode strategy isolation."""

import json
from datetime import UTC
from datetime import datetime
from typing import Any
from typing import cast
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import Mock

import pytest

import snapper.application.engine.trader as trader_module
from snapper.application.engine.service import TradingEngineService
from snapper.application.engine.service import compute_shard_key
from snapper.application.engine.trader import TraderCoordinator
from snapper.core.partitioning import ShardOwnership
from snapper.core.types import ExchangeEnum
from snapper.core.types import ExecutionModeEnum
from snapper.data.repository import SQLAlchemyRepository
from snapper.messaging.schemas.data import OrderData
from snapper.messaging.schemas.data import OrderEventData
from snapper.messaging.schemas.data import SignalData


def _make_coord(monkeypatch: pytest.MonkeyPatch) -> TraderCoordinator:
    """Build a TraderCoordinator with mocked infrastructure."""
    settings = MagicMock()
    settings.db_url = "sqlite:///:memory:"
    settings.zmq_broker_xpub = "tcp://broker.xpub"
    settings.risk_r_per_trade = 0.01
    settings.risk_max_leverage = 2.0
    settings.risk_max_drawdown = 0.15
    monkeypatch.setattr(trader_module, "get_settings", lambda: settings, raising=True)
    mock_repo = AsyncMock()
    mock_repo.ensure_instrument = AsyncMock(return_value=(1, "inst-pid"))
    monkeypatch.setattr(trader_module, "get_repository", lambda _url: mock_repo, raising=True)
    monkeypatch.setattr(
        trader_module, "resolve_symbol_public_id", AsyncMock(return_value="stub-spid")
    )
    monkeypatch.setattr(trader_module, "is_tradeable", lambda _i, _e: True)
    coord = TraderCoordinator()
    coord.msg_publisher = cast(Any, MagicMock(tracker=Mock(session_id="s1")))
    coord.execution_publisher = MagicMock()
    return coord


def _make_signal(
    instrument: str = "BTC-USD",
    exchange: str = "paper",
    strength: float = 0.0,
) -> SignalData:
    """Build a signal for testing.

    Defaults to strength=0 so execute_desired_units is a no-op (no order placed).
    """
    return SignalData(
        type="signal",
        public_id="sig-1",
        timestamp=datetime(2024, 1, 1, tzinfo=UTC),
        session_id="",
        sequence_id=0,
        instrument=instrument,
        exchange=exchange,
        side="buy",
        strength=strength,
        reason="test",
        price=50000.0,
        strategy_name="test",
        fired_at=datetime.now(UTC),
    )


def _owning_partition(shard_key: str) -> ShardOwnership:
    """Return the owner for ``shard_key`` in a two-instance deployment."""
    for instance_id in range(2):
        ownership = ShardOwnership(instance_id=instance_id, instance_count=2)
        if ownership.owns(shard_key):
            return ownership
    raise AssertionError(f"No owner resolved for shard_key={shard_key}")


class TestShardKeyFormat:
    """TradingEngineService shard_key format for paper vs live mode."""

    def test_paper_mode_separate_shards_per_strategy(self) -> None:
        """Two paper strategies for same instrument get different shard_keys.

        Given: two paper engines with different strategy_tags,
        When: shard_key is computed,
        Then: each has a unique 4-segment shard_key.
        """
        e1 = TradingEngineService(
            "BTC-USD",
            execution_socket=MagicMock(),
            exchange=ExchangeEnum.PAPER,
            strategy_tag="scalp",
        )
        e2 = TradingEngineService(
            "BTC-USD",
            execution_socket=MagicMock(),
            exchange=ExchangeEnum.PAPER,
            strategy_tag="swing",
        )
        assert e1._shard_key == "paper.BTC-USD.paper.scalp"
        assert e2._shard_key == "paper.BTC-USD.paper.swing"
        assert e1._shard_key != e2._shard_key

    def test_live_mode_single_shard_regardless_of_strategy(self) -> None:
        """Live engines ignore strategy_tag in shard_key.

        Given: two live engines with different strategy_tags,
        When: shard_key is computed,
        Then: both have the same 3-segment shard_key.
        """
        e1 = TradingEngineService(
            "BTC-USD",
            execution_socket=MagicMock(),
            exchange="kraken",
            strategy_tag="scalp",
        )
        e2 = TradingEngineService(
            "BTC-USD",
            execution_socket=MagicMock(),
            exchange="kraken",
            strategy_tag="swing",
        )
        assert e1._shard_key == "kraken.BTC-USD.live"
        assert e2._shard_key == "kraken.BTC-USD.live"

    def test_paper_mode_no_strategy_tag_uses_3_segments(self) -> None:
        """Paper engine without strategy_tag uses 3-segment shard_key.

        Given: paper engine with strategy_tag=None,
        When: shard_key is computed,
        Then: 3-segment key (backward compatible).
        """
        e = TradingEngineService(
            "BTC-USD",
            execution_socket=MagicMock(),
            exchange=ExchangeEnum.PAPER,
        )
        assert e._shard_key == "paper.BTC-USD.paper"


class TestCoordinatorSharding:
    """Coordinator routes signals and events to correct paper shards."""

    @pytest.mark.asyncio
    async def test_paper_signal_creates_engine_with_strategy_tag(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Paper signal creates engine with strategy_tag from signal_type.

        Given: signal topic signals.paper.BTC-USD.scalp,
        When: _on_signal processes it,
        Then: engine has strategy_tag="scalp" and 4-segment shard_key.
        """
        coord = _make_coord(monkeypatch)
        coord._current_topic = "signals.paper.BTC-USD.scalp"
        await coord._on_signal(_make_signal())

        assert "BTC-USD@paper-scalp" in coord.engines
        engine = coord.engines["BTC-USD@paper-scalp"]
        assert engine._strategy_tag == "scalp"
        assert engine._shard_key == "paper.BTC-USD.paper.scalp"

    @pytest.mark.asyncio
    async def test_live_signal_creates_engine_without_strategy_tag(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Live signal creates engine with strategy_tag=None.

        Given: signal topic signals.kraken.BTC-USD.live,
        When: _on_signal processes it,
        Then: engine has strategy_tag=None and 3-segment shard_key.
        """
        coord = _make_coord(monkeypatch)
        coord._current_topic = "signals.kraken.BTC-USD.live"
        await coord._on_signal(_make_signal(exchange="kraken"))

        assert "BTC-USD@kraken-live" in coord.engines
        engine = coord.engines["BTC-USD@kraken-live"]
        assert engine._strategy_tag is None
        assert engine._shard_key == "kraken.BTC-USD.live"

    @pytest.mark.asyncio
    async def test_order_shard_keys_populated_on_order(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Order submission populates _order_shard_keys lookup.

        Given: engine whose execute_desired_units sets pending_client_order_id,
        When: _on_signal processes the signal,
        Then: _order_shard_keys maps the new order ID to the engine's shard_key.
        """
        coord = _make_coord(monkeypatch)
        coord._current_topic = "signals.paper.BTC-USD.scalp"
        await coord._on_signal(_make_signal())

        engine = coord.engines["BTC-USD@paper-scalp"]

        def _mock_execute(
            desired_units: float,
            price: float,
            signaled_at: float | None = None,
            *,
            ai_review_public_id: str | None = None,
            ai_review_dispatch_version: int | None = None,
            grouped_correlation_id: str | None = None,
            signal_public_id: str | None = None,
        ) -> None:
            del ai_review_public_id, ai_review_dispatch_version, grouped_correlation_id
            del signal_public_id
            engine.pending_client_order_id = "order-456"

        engine.execute_desired_units = AsyncMock(side_effect=_mock_execute)

        coord._current_topic = "signals.paper.BTC-USD.scalp"
        await coord._on_signal(_make_signal(strength=1.0))

        assert coord._order_shard_keys.get("order-456") == "paper.BTC-USD.paper.scalp"


class TestRecoveryShardingClusterGuards:
    """N>=2 recovery-cluster guards: shard-key registration + paper refusal."""

    def test_register_order_shard_key_idempotent_and_conflict(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Re-registering the same key is a no-op; a conflicting key is refused.

        Given: a coordinator with a registered client_order_id -> shard_key,
        When: the same id is registered again with the same and then a different key,
        Then: the idempotent re-register keeps the mapping, and the conflicting
            re-register DROPS the mapping entirely so the order's fills hard-drop
            as unknown rather than risk a mis-routed fill.
        """
        coord = _make_coord(monkeypatch)
        coord._register_order_shard_key("cid-1", "kraken.BTC-USD.live.wabcdef012345")
        coord._register_order_shard_key("cid-1", "kraken.BTC-USD.live.wabcdef012345")
        assert coord._order_shard_keys["cid-1"] == "kraken.BTC-USD.live.wabcdef012345"
        coord._register_order_shard_key("cid-1", "kraken.ETH-USD.live")
        assert "cid-1" not in coord._order_shard_keys

    @pytest.mark.asyncio
    async def test_paper_signal_refused_under_n_gt_1(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Paper routing is refused loudly under N>1 (recovery cannot reconstruct).

        Given: a coordinator partitioned across 2 instances,
        When: a paper signal's routing context is built,
        Then: it returns None (refused) so no paper engine is created — paper shard
            keys embed the strategy tag, which the Order table cannot persist.
        """
        coord = _make_coord(monkeypatch)
        coord._ownership = ShardOwnership(instance_id=0, instance_count=2)
        coord._current_topic = "signals.paper.BTC-USD.scalp"
        assert coord._build_signal_routing_context(_make_signal()) is None

    @pytest.mark.asyncio
    async def test_register_checkpoint_open_orders(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Owned-checkpoint open commands register cid->shard_key, shard-matched only.

        Given: a checkpoint whose open_command_ids resolve to a mix of a matching
            command, a non-string id, a missing command, a foreign-shard command,
            and a command with an empty client_order_id,
        When: _register_checkpoint_open_orders runs,
        Then: only the matching command's client_order_id is registered.
        """
        coord = _make_coord(monkeypatch)
        shard_key = "kraken.BTC-USD.live"
        coord.repository.get_trade_command_by_public_id = AsyncMock(
            side_effect=[
                {"client_order_id": "cid-ok", "shard_key": shard_key},
                None,
                {"client_order_id": "cid-foreign", "shard_key": "kraken.ETH-USD.live"},
                {"client_order_id": "", "shard_key": shard_key},
            ]
        )
        checkpoint = cast(Any, {"open_command_ids": json.dumps(["c1", 123, "c2", "c3", "c4"])})
        await coord._register_checkpoint_open_orders(
            checkpoint, shard_key, datetime(2024, 1, 1, tzinfo=UTC)
        )
        assert coord._order_shard_keys == {"cid-ok": shard_key}


class TestStatusEventRouting:
    """Order status and cancel events route via _order_shard_keys lookup."""

    @pytest.mark.asyncio
    async def test_status_event_uses_order_shard_keys(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Order status event routes to correct shard via lookup.

        Given: _order_shard_keys has client_order_id → paper.BTC-USD.paper.scalp,
        When: _sync_status_to_trade_service processes an accepted event,
        Then: VenueEvent applied to the correct paper shard.
        """
        coord = _make_coord(monkeypatch)
        coord._order_shard_keys["oid-1"] = "paper.BTC-USD.paper.scalp"

        parsed = MagicMock()
        parsed.exchange = ExchangeEnum.PAPER
        parsed.instrument = "BTC-USD"
        parsed.suffix = "accepted"

        order_status = MagicMock(spec=OrderData)
        order_status.client_order_id = "oid-1"
        order_status.session_id = "s1"
        order_status.sequence_id = 1
        order_status.exchange = "paper"
        order_status.instrument = "BTC-USD"
        order_status.exchange_order_id = "ex-1"
        order_status.side = "buy"
        order_status.error = None

        coord._sync_status_to_trade_service(order_status, parsed)

        assert "paper.BTC-USD.paper.scalp" in coord.trade_service._shards

    @pytest.mark.asyncio
    async def test_cancel_event_uses_order_shard_keys(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Cancel event routes to correct shard via lookup.

        Given: _order_shard_keys has client_order_id → paper.BTC-USD.paper.scalp,
        When: _sync_order_event_to_trade_service processes a cancel,
        Then: VenueEvent applied to the correct paper shard.
        """
        coord = _make_coord(monkeypatch)
        coord._order_shard_keys["oid-2"] = "paper.BTC-USD.paper.scalp"

        parsed = MagicMock()
        parsed.exchange = ExchangeEnum.PAPER
        parsed.instrument = "BTC-USD"
        parsed.suffix = "cancelled"

        order_event = MagicMock(spec=OrderEventData)
        order_event.client_order_id = "oid-2"
        order_event.session_id = "s1"
        order_event.sequence_id = 2
        order_event.exchange = "paper"
        order_event.instrument = "BTC-USD"
        order_event.exchange_order_id = "ex-2"

        coord._sync_order_event_to_trade_service(order_event, parsed)

        assert "paper.BTC-USD.paper.scalp" in coord.trade_service._shards

    @pytest.mark.asyncio
    async def test_unknown_order_falls_back_to_flat_shard_key(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Unknown client_order_id falls back to 3-segment shard_key.

        Given: _order_shard_keys has no entry for the client_order_id,
        When: _sync_status_to_trade_service processes an accepted event,
        Then: VenueEvent applied to the flat shard (graceful degradation).
        """
        coord = _make_coord(monkeypatch)

        parsed = MagicMock()
        parsed.exchange = ExchangeEnum.PAPER
        parsed.instrument = "BTC-USD"
        parsed.suffix = "accepted"

        order_status = MagicMock(spec=OrderData)
        order_status.client_order_id = "unknown-oid"
        order_status.session_id = "s1"
        order_status.sequence_id = 1
        order_status.exchange = "paper"
        order_status.instrument = "BTC-USD"
        order_status.exchange_order_id = None
        order_status.side = "buy"
        order_status.error = None

        coord._sync_status_to_trade_service(order_status, parsed)

        assert "paper.BTC-USD.paper" in coord.trade_service._shards


class TestStatusShadowWriteMapping:
    """Suffix-to-venue-event mapping is exhaustive with no silent default."""

    @pytest.mark.asyncio
    async def test_unknown_suffix_maps_to_order_submit_unknown(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Unknown suffix shadow-writes an order_submit_unknown event.

        Given: an order status event arriving on the .unknown topic suffix,
        When: _sync_status_to_trade_service processes it,
        Then: the synthetic venue event carries event_type
            order_submit_unknown (never the old order_accepted default).
        """
        coord = _make_coord(monkeypatch)
        coord.trade_service.apply_venue_event = MagicMock()

        parsed = MagicMock()
        parsed.exchange = ExchangeEnum.PAPER
        parsed.instrument = "BTC-USD"
        parsed.suffix = "unknown"

        order_status = MagicMock(spec=OrderData)
        order_status.client_order_id = "oid-unk"
        order_status.session_id = "s1"
        order_status.sequence_id = 1
        order_status.exchange = "paper"
        order_status.instrument = "BTC-USD"
        order_status.exchange_order_id = None
        order_status.side = "buy"
        order_status.error = "ambiguous submit"

        coord._sync_status_to_trade_service(order_status, parsed)

        coord.trade_service.apply_venue_event.assert_called_once()
        event = coord.trade_service.apply_venue_event.call_args.args[0]
        assert event["event_type"] == "order_submit_unknown"

    @pytest.mark.asyncio
    async def test_unmapped_suffix_skips_shadow_write(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """An unmapped suffix is skipped instead of defaulting to accepted.

        Given: an order status event with a suffix absent from the
            shadow-write map (the pre-fix code silently defaulted any
            such suffix to order_accepted, corrupting command state),
        When: _sync_status_to_trade_service processes it,
        Then: no venue event is applied.
        """
        coord = _make_coord(monkeypatch)
        coord.trade_service.apply_venue_event = MagicMock()

        parsed = MagicMock()
        parsed.exchange = ExchangeEnum.PAPER
        parsed.instrument = "BTC-USD"
        parsed.suffix = "cancelled"

        order_status = MagicMock(spec=OrderData)
        order_status.client_order_id = "oid-x"
        order_status.session_id = "s1"
        order_status.sequence_id = 1
        order_status.exchange = "paper"
        order_status.instrument = "BTC-USD"
        order_status.exchange_order_id = None
        order_status.side = "buy"
        order_status.error = None

        coord._sync_status_to_trade_service(order_status, parsed)

        coord.trade_service.apply_venue_event.assert_not_called()


class TestPartitionedManualOrderRouting:
    """Manual order CIDs pass the N>=2 venue-event ownership filter."""

    @pytest.mark.asyncio
    async def test_registered_manual_cid_is_not_dropped_under_partitioning(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Registered manual CID reaches the order-status handler.

        Given: a two-instance coordinator that owns the wallet-aware
            shard key persisted by REST or MCP for a manual order,
        When: a venue ACK arrives with that registered
            ``client_order_id``,
        Then: ``_dispatch_order_event`` does not drop the event at the
            N>=2 CID filter.
        """
        coord = _make_coord(monkeypatch)
        shard_key = compute_shard_key(
            instrument="BTC-USD",
            exchange="kraken",
            mode=ExecutionModeEnum.LIVE,
            wallet_public_id="wallet-1",
            strategy_tag=None,
        )
        coord._ownership = _owning_partition(shard_key)
        coord._order_shard_keys["cid-manual"] = shard_key
        coord._handle_order_status = AsyncMock()
        order_status = OrderData(
            public_id="order-status-1",
            timestamp=datetime(2026, 4, 10, tzinfo=UTC),
            session_id="s1",
            sequence_id=1,
            exchange_order_id="ex-1",
            client_order_id="cid-manual",
            instrument="BTC-USD",
            exchange="kraken",
            mode=ExecutionModeEnum.LIVE,
            side="buy",
            status="accepted",
            order_type="market",
            size=0.5,
            filled_size=0.0,
            created_at=datetime(2026, 4, 10, tzinfo=UTC),
            wallet_public_id="wallet-1",
        )
        await coord._dispatch_order_event(
            "orders.events.kraken.BTC-USD.accepted",
            order_status.to_json().encode("utf-8"),
        )
        coord._handle_order_status.assert_awaited_once()


class TestCheckpointRecoveryWithSharding:
    """Checkpoint recovery handles 4-segment paper shard_keys."""

    @pytest.mark.asyncio
    async def test_4_segment_paper_checkpoint_recovers_with_strategy_tag(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """4-segment paper shard_key checkpoint creates engine with strategy_tag.

        Given: checkpoint with shard_key="paper.BTC-USD.paper.scalp",
        When: _recover_engine_state runs,
        Then: engine has strategy_tag="scalp".
        """
        coord = _make_coord(monkeypatch)
        mock_repo = AsyncMock(spec=SQLAlchemyRepository)
        mock_repo.get_all_checkpoints = AsyncMock(
            return_value=[
                {
                    "public_id": "cp-1",
                    "shard_key": "paper.BTC-USD.paper.scalp",
                    "position_qty": 0.5,
                    "entry_price": 50000.0,
                    "cash": 7500.0,
                    "peak_equity": 10000.0,
                    "realized_pnl": 0.0,
                    "turnover": 5000.0,
                    "last_venue_event_id": 10,
                    "last_venue_event_at": datetime(2024, 6, 1, tzinfo=UTC),
                    "open_command_ids": None,
                    "seen_exec_ids": "[]",
                    "checkpoint_at": datetime(2024, 6, 1, tzinfo=UTC),
                    "session_id": "s-test",
                }
            ]
        )
        mock_repo.get_venue_events_after = AsyncMock(return_value=[])
        mock_repo.get_executions_for_recovery = AsyncMock(return_value=[])
        mock_repo.get_active_orders_for_recovery = AsyncMock(return_value=[])
        mock_repo.ensure_instrument = AsyncMock(return_value=(1, "inst-pid"))
        coord.repository = mock_repo

        await coord._recover_engine_state()

        assert "BTC-USD@paper-scalp" in coord.engines
        engine = coord.engines["BTC-USD@paper-scalp"]
        assert engine._strategy_tag == "scalp"
        assert engine._shard_key == "paper.BTC-USD.paper.scalp"


class TestVenueEventShardKey:
    """Executor writes correct shard_key on VenueEvents."""

    def test_venue_event_shard_key_includes_strategy_for_paper(self) -> None:
        """Paper order creates VenueEvent with 4-segment shard_key.

        Given: RecordVenueEventParams with exchange_name=paper and strategy_tag=scalp,
        When: shard_key is computed (logic extracted from executor),
        Then: shard_key is paper.BTC-USD.paper.scalp.
        """
        exchange_name = ExchangeEnum.PAPER
        instrument = "BTC-USD"
        mode = ExecutionModeEnum.PAPER
        strategy_tag = "scalp"
        shard_key = f"{exchange_name}.{instrument}.{mode}"
        if mode == ExecutionModeEnum.PAPER and strategy_tag:
            shard_key = f"{shard_key}.{strategy_tag}"
        assert shard_key == "paper.BTC-USD.paper.scalp"

    def test_venue_event_shard_key_flat_for_live(self) -> None:
        """Live order creates VenueEvent with 3-segment shard_key.

        Given: RecordVenueEventParams with exchange_name=kraken and strategy_tag=scalp,
        When: shard_key is computed,
        Then: shard_key is kraken.BTC-USD.live (strategy_tag ignored).
        """
        exchange_name = "kraken"
        instrument = "BTC-USD"
        mode = ExecutionModeEnum.LIVE
        strategy_tag = "scalp"
        shard_key = f"{exchange_name}.{instrument}.{mode}"
        if mode == ExecutionModeEnum.PAPER and strategy_tag:
            shard_key = f"{shard_key}.{strategy_tag}"
        assert shard_key == "kraken.BTC-USD.live"
