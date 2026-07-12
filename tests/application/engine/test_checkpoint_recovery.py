"""Tests for checkpoint recovery in TraderCoordinator."""

from collections import OrderedDict
from datetime import UTC
from datetime import datetime
from typing import Any
from typing import cast
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import Mock
from unittest.mock import patch

import pytest

import snapper.application.engine.trader as trader_module
from snapper.application.engine.trader import TraderCoordinator
from snapper.application.portfolio.models import PositionStateModel
from snapper.application.trade.trade_service import TradeService
from snapper.core.wallet_short import compute_wallet_short
from snapper.data.repository import SQLAlchemyRepository
from snapper.data.repository_types import ExecutionRow
from snapper.data.repository_types import TradeProjectionCheckpointRow
from snapper.data.repository_types import VenueEventRow


def _make_checkpoint(
    shard_key: str = "kraken.BTC-USD.live",
    position_qty: float = 0.5,
    entry_price: float | None = 50000.0,
    position_opened_at: datetime | None = None,
    cash: float = 7500.0,
    peak_equity: float = 10000.0,
    realized_pnl: float = 100.0,
    turnover: float = 25000.0,
    last_venue_event_id: int | None = 10,
    open_command_ids: str | None = None,
    seen_exec_ids: str = '["t1", "t2"]',
    checkpoint_at: datetime | None = None,
) -> TradeProjectionCheckpointRow:
    """Build a checkpoint row for testing."""
    return {
        "public_id": "cp-1",
        "shard_key": shard_key,
        "position_qty": position_qty,
        "entry_price": entry_price,
        "position_opened_at": position_opened_at,
        "cash": cash,
        "peak_equity": peak_equity,
        "realized_pnl": realized_pnl,
        "turnover": turnover,
        "last_venue_event_id": last_venue_event_id,
        "last_venue_event_at": datetime(2024, 6, 1, tzinfo=UTC),
        "open_command_ids": open_command_ids,
        "seen_exec_ids": seen_exec_ids,
        "checkpoint_at": checkpoint_at or datetime(2024, 6, 1, tzinfo=UTC),
        "session_id": "s-test",
        "operator_public_id": None,
    }


def _make_venue_event(
    event_id: int = 11,
    shard_key: str = "kraken.BTC-USD.live",
    event_type: str = "fill_observed",
    fill_price: float = 51000.0,
    fill_size: float = 0.1,
    side: str = "buy",
    exec_id: str | None = "t3",
    trade_id: str | None = "t3",
) -> VenueEventRow:
    """Build a venue event row for testing."""
    return {
        "id": event_id,
        "public_id": f"ve-{event_id}",
        "timestamp": datetime(2024, 6, 1, 1, tzinfo=UTC),
        "session_id": "s-test",
        "sequence_id": event_id,
        "event_type": event_type,
        "shard_key": shard_key,
        "command_public_id": None,
        "exchange": "kraken",
        "instrument": "BTC-USD",
        "mode": "live",
        "exchange_order_id": "ex-1",
        "client_order_id": "c-1",
        "venue_client_id": None,
        "side": side,
        "status": "filled",
        "fill_price": fill_price,
        "fill_size": fill_size,
        "cum_fill_size": fill_size,
        "fee": 0.01,
        "fee_asset": "USD",
        "exec_id": exec_id,
        "trade_id": trade_id,
        "error": None,
        "venue_timestamp": datetime(2024, 6, 1, 1, tzinfo=UTC),
        "received_at": datetime(2024, 6, 1, 1, tzinfo=UTC),
    }


def _make_coord(monkeypatch: pytest.MonkeyPatch) -> TraderCoordinator:
    """Build a TraderCoordinator with mocked infrastructure."""
    settings = MagicMock()
    settings.db_url = "sqlite:///:memory:"
    settings.zmq_broker_xpub = "tcp://broker.xpub"
    settings.risk_r_per_trade = 0.01
    settings.risk_max_leverage = 2.0
    settings.risk_max_drawdown = 0.15
    monkeypatch.setattr(trader_module, "get_settings", lambda: settings, raising=True)
    monkeypatch.setattr(trader_module, "get_repository", lambda _url: MagicMock(), raising=True)
    monkeypatch.setattr(
        trader_module, "resolve_symbol_public_id", AsyncMock(return_value="stub-spid")
    )
    coord = TraderCoordinator()
    coord.msg_publisher = cast(Any, MagicMock(tracker=Mock(session_id="s1")))
    return coord


def _set_sqlalchemy_repo(coord: TraderCoordinator, mock_repo: AsyncMock) -> None:
    """Assign an AsyncMock with SQLAlchemyRepository spec to the coordinator."""
    mock_repo.ensure_instrument = AsyncMock(return_value=(1, "inst-pid"))
    mock_repo.resolve_wallet_public_id_by_short = AsyncMock(return_value=None)
    mock_repo.get_open_position_cycles_for_shards = AsyncMock(return_value={})
    mock_repo.insert_position_cycle = AsyncMock(return_value=(1, "cycle-pid"))
    mock_repo.get_shard_keys_with_fills = AsyncMock(return_value=[])
    mock_repo.shard_has_fill_gap = AsyncMock(return_value=False)
    mock_repo.shard_has_accruals = AsyncMock(return_value=False)
    coord.repository = mock_repo


class TestCheckpointRecovery:
    """Checkpoint recovery restores TradeService, engine, and portfolio."""

    @pytest.mark.asyncio
    async def test_checkpoint_recovery_restores_position(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Checkpoint with position data restores engine position and entry price.

        Given: a checkpoint with qty=0.5 and entry_price=50000,
        When: _recover_engine_state runs,
        Then: engine has correct position and entry price.
        """
        coord = _make_coord(monkeypatch)
        mock_repo = AsyncMock(spec=SQLAlchemyRepository)
        mock_repo.get_all_checkpoints = AsyncMock(return_value=[_make_checkpoint()])
        mock_repo.get_venue_events_after = AsyncMock(return_value=[])
        mock_repo.get_executions_for_recovery = AsyncMock(return_value=[])
        mock_repo.get_active_orders_for_recovery = AsyncMock(return_value=[])
        _set_sqlalchemy_repo(coord, mock_repo)

        await coord._recover_engine_state()

        assert "BTC-USD@kraken-live" in coord.engines
        engine = coord.engines["BTC-USD@kraken-live"]
        assert engine.position_qty == pytest.approx(0.5)
        assert engine.entry_price == pytest.approx(50000.0)

    @pytest.mark.asyncio
    async def test_checkpoint_recovery_restores_position_opened_at(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Checkpoint position_opened_at flows into the restored projection.

        Given: a checkpoint carrying a non-NULL position_opened_at,
        When: _recover_engine_state runs,
        Then: TradeService projection has the same position_opened_at
            (so the funding accrual loop can clamp catch-up boundaries).
        """
        coord = _make_coord(monkeypatch)
        mock_repo = AsyncMock(spec=SQLAlchemyRepository)
        opened_at = datetime(2026, 4, 6, 10, 0, 0, tzinfo=UTC)
        mock_repo.get_all_checkpoints = AsyncMock(
            return_value=[_make_checkpoint(position_opened_at=opened_at)]
        )
        mock_repo.get_venue_events_after = AsyncMock(return_value=[])
        mock_repo.get_executions_for_recovery = AsyncMock(return_value=[])
        mock_repo.get_active_orders_for_recovery = AsyncMock(return_value=[])
        _set_sqlalchemy_repo(coord, mock_repo)

        await coord._recover_engine_state()

        shard = coord.trade_service._shards["kraken.BTC-USD.live"]
        assert shard.position.position_opened_at == opened_at

    @pytest.mark.asyncio
    async def test_checkpoint_recovery_handles_legacy_null_position_opened_at(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Old checkpoints without position_opened_at restore as None.

        Given: a checkpoint where position_opened_at is None (legacy
            row written before this feature shipped),
        When: _recover_engine_state runs,
        Then: TradeService projection has position_opened_at=None and
            recovery does not crash.
        """
        coord = _make_coord(monkeypatch)
        mock_repo = AsyncMock(spec=SQLAlchemyRepository)
        mock_repo.get_all_checkpoints = AsyncMock(
            return_value=[_make_checkpoint(position_opened_at=None)]
        )
        mock_repo.get_venue_events_after = AsyncMock(return_value=[])
        mock_repo.get_executions_for_recovery = AsyncMock(return_value=[])
        mock_repo.get_active_orders_for_recovery = AsyncMock(return_value=[])
        _set_sqlalchemy_repo(coord, mock_repo)

        await coord._recover_engine_state()

        shard = coord.trade_service._shards["kraken.BTC-USD.live"]
        assert shard.position.position_opened_at is None

    @pytest.mark.asyncio
    async def test_checkpoint_recovery_replays_delta(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Delta VenueEvents after checkpoint watermark are replayed.

        Given: checkpoint at watermark 10, delta event 11 is a fill,
        When: _recover_engine_state runs,
        Then: TradeService has the delta fill applied.
        """
        coord = _make_coord(monkeypatch)
        mock_repo = AsyncMock(spec=SQLAlchemyRepository)
        cp = _make_checkpoint(last_venue_event_id=10)
        mock_repo.get_all_checkpoints = AsyncMock(return_value=[cp])
        mock_repo.get_venue_events_after = AsyncMock(return_value=[_make_venue_event(event_id=11)])
        mock_repo.get_executions_for_recovery = AsyncMock(return_value=[])
        mock_repo.get_active_orders_for_recovery = AsyncMock(return_value=[])
        _set_sqlalchemy_repo(coord, mock_repo)

        await coord._recover_engine_state()

        mock_repo.get_venue_events_after.assert_called_once_with(
            shard_key="kraken.BTC-USD.live", after_id=10
        )
        shard = coord.trade_service._shards.get("kraken.BTC-USD.live")
        assert shard is not None
        assert shard.last_venue_event_id == 11

    def test_recovery_seeds_consumed_watermark_from_delta(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Recovery seeds the consumed watermark to the max applied DB event id.

        Given: a checkpoint at watermark 10 plus delta events 11 and 13,
        When: the shard is restored,
        Then: the per-shard consumed watermark is 13 (max of checkpoint and
            delta ids) so the next checkpoint can never persist a watermark
            below what was recovered and silently drop a fill.
        """
        coord = _make_coord(monkeypatch)
        shard = "kraken.BTC-USD.live"
        coord._restore_trade_service_from_checkpoint(
            _make_checkpoint(last_venue_event_id=10),
            shard,
            [_make_venue_event(event_id=11), _make_venue_event(event_id=13)],
        )
        assert coord._consumed_venue_event_watermarks[shard] == 13

    def test_recovery_seeds_consumed_watermark_without_delta(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """With no delta events the consumed watermark seeds to the checkpoint id."""
        coord = _make_coord(monkeypatch)
        shard = "kraken.BTC-USD.live"
        coord._restore_trade_service_from_checkpoint(
            _make_checkpoint(last_venue_event_id=10), shard, []
        )
        assert coord._consumed_venue_event_watermarks[shard] == 10

    @pytest.mark.asyncio
    async def test_checkpoint_recovery_restores_seen_exec_ids(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Dedup set is restored from checkpoint so duplicate fills are rejected.

        Given: checkpoint with seen_exec_ids=["t1", "t2"],
        When: _recover_engine_state runs,
        Then: engine and TradeService shard contain t1 and t2.
        """
        coord = _make_coord(monkeypatch)
        mock_repo = AsyncMock(spec=SQLAlchemyRepository)
        mock_repo.get_all_checkpoints = AsyncMock(
            return_value=[_make_checkpoint(seen_exec_ids='["t1", "t2"]')]
        )
        mock_repo.get_venue_events_after = AsyncMock(return_value=[])
        mock_repo.get_executions_for_recovery = AsyncMock(return_value=[])
        mock_repo.get_active_orders_for_recovery = AsyncMock(return_value=[])
        _set_sqlalchemy_repo(coord, mock_repo)

        await coord._recover_engine_state()

        engine = coord.engines["BTC-USD@kraken-live"]
        assert "t1" in engine.seen_exec_ids
        assert "t2" in engine.seen_exec_ids
        shard = coord.trade_service._shards["kraken.BTC-USD.live"]
        assert "t1" in shard.seen_exec_ids

    @pytest.mark.asyncio
    async def test_no_checkpoint_falls_back_to_full_replay(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Shards without checkpoints use existing full execution replay.

        Given: no checkpoints, one execution row in DB,
        When: _recover_engine_state runs,
        Then: engine is created via full replay path.
        """
        coord = _make_coord(monkeypatch)
        mock_repo = AsyncMock(spec=SQLAlchemyRepository)
        mock_repo.get_all_checkpoints = AsyncMock(return_value=[])
        mock_repo.get_executions_for_recovery = AsyncMock(
            return_value=[
                {
                    "public_id": "exe-1",
                    "timestamp": datetime(2024, 1, 1, tzinfo=UTC),
                    "session_id": "s1",
                    "sequence_id": 1,
                    "trade_id": "t1",
                    "exchange_order_id": "ex-1",
                    "client_order_id": "c1",
                    "instrument": "BTC-USD",
                    "exchange": "kraken",
                    "side": "buy",
                    "size": 0.5,
                    "price": 50000.0,
                    "fee": 0.5,
                    "fee_asset": "USD",
                    "status": "filled",
                    "executed_at": datetime(2024, 1, 1, tzinfo=UTC),
                }
            ]
        )
        mock_repo.get_active_orders_for_recovery = AsyncMock(return_value=[])
        _set_sqlalchemy_repo(coord, mock_repo)

        await coord._recover_engine_state()

        assert "BTC-USD@kraken-live" in coord.engines
        engine = coord.engines["BTC-USD@kraken-live"]
        assert engine.position_qty == pytest.approx(0.5)

    @pytest.mark.asyncio
    async def test_full_replay_seeds_consumed_watermark_to_matched_fills(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Full-replay seeds the watermark from MATCHED fills, never DB-max.

        Given: no checkpoint but one execution to full-replay whose durable
            venue event resolves to id 42 via identifier matching,
        When: _recover_engine_state runs,
        Then: the per-shard consumed watermark is 42 — derived from the
            fill actually replayed, so a recorded-but-unreplayed venue
            event with a higher id can never be over-claimed and skipped
            on the next restart.
        """
        coord = _make_coord(monkeypatch)
        mock_repo = AsyncMock(spec=SQLAlchemyRepository)
        mock_repo.get_all_checkpoints = AsyncMock(return_value=[])
        mock_repo.get_executions_for_recovery = AsyncMock(
            return_value=[
                {
                    "public_id": "exe-1",
                    "timestamp": datetime(2024, 1, 1, tzinfo=UTC),
                    "session_id": "s1",
                    "sequence_id": 1,
                    "trade_id": "t1",
                    "exchange_order_id": "ex-1",
                    "client_order_id": "c1",
                    "instrument": "BTC-USD",
                    "exchange": "kraken",
                    "side": "buy",
                    "size": 0.5,
                    "price": 50000.0,
                    "fee": 0.5,
                    "fee_asset": "USD",
                    "status": "filled",
                    "executed_at": datetime(2024, 1, 1, tzinfo=UTC),
                }
            ]
        )
        mock_repo.get_active_orders_for_recovery = AsyncMock(return_value=[])
        mock_repo.get_consumed_fill_venue_event_id = AsyncMock(return_value=42)
        _set_sqlalchemy_repo(coord, mock_repo)

        await coord._recover_engine_state()

        assert coord._consumed_venue_event_watermarks["kraken.BTC-USD.live"] == 42
        matched_call = mock_repo.get_consumed_fill_venue_event_id.await_args.kwargs
        assert matched_call["client_order_id"] == "c1"
        assert matched_call["trade_id"] == "t1"

    @pytest.mark.asyncio
    async def test_full_replay_seed_skipped_when_no_venue_events(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A full-replay shard with no durable venue events leaves the watermark unset.

        Given: full replay of a shard whose ``get_latest_venue_event_id`` is None,
        When: _recover_engine_state runs,
        Then: no watermark entry is seeded (it defaults to 0 at next checkpoint).
        """
        coord = _make_coord(monkeypatch)
        mock_repo = AsyncMock(spec=SQLAlchemyRepository)
        mock_repo.get_all_checkpoints = AsyncMock(return_value=[])
        mock_repo.get_executions_for_recovery = AsyncMock(
            return_value=[
                {
                    "public_id": "exe-1",
                    "timestamp": datetime(2024, 1, 1, tzinfo=UTC),
                    "session_id": "s1",
                    "sequence_id": 1,
                    "trade_id": "t1",
                    "exchange_order_id": "ex-1",
                    "client_order_id": "c1",
                    "instrument": "BTC-USD",
                    "exchange": "kraken",
                    "side": "buy",
                    "size": 0.5,
                    "price": 50000.0,
                    "fee": 0.5,
                    "fee_asset": "USD",
                    "status": "filled",
                    "executed_at": datetime(2024, 1, 1, tzinfo=UTC),
                }
            ]
        )
        mock_repo.get_active_orders_for_recovery = AsyncMock(return_value=[])
        mock_repo.get_latest_venue_event_id = AsyncMock(return_value=None)
        _set_sqlalchemy_repo(coord, mock_repo)

        await coord._recover_engine_state()

        assert "kraken.BTC-USD.live" not in coord._consumed_venue_event_watermarks

    @pytest.mark.asyncio
    async def test_checkpoint_snapshot_includes_seen_exec_ids(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """snapshot_for_checkpoint serializes seen_exec_ids as sorted JSON.

        Given: TradeService shard with seen_exec_ids containing t2 and t1,
        When: snapshot_for_checkpoint is called,
        Then: result has seen_exec_ids as '["t1", "t2"]'.
        """
        ts = TradeService()
        ts.restore_from_checkpoint(
            shard_key="test.X.live",
            position_qty=1.0,
            entry_price=100.0,
            cash=9000.0,
            peak_equity=10000.0,
            realized_pnl=0.0,
            turnover=100.0,
            last_venue_event_id=5,
            open_command_ids=[],
            seen_exec_ids=OrderedDict.fromkeys(["t2", "t1"]),
        )
        snap = ts.snapshot_for_checkpoint("test.X.live")
        assert snap["seen_exec_ids"] == '["t1", "t2"]'

    @pytest.mark.asyncio
    async def test_checkpoint_restore_handles_null_open_command_ids(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Checkpoint with open_command_ids=None does not crash.

        Given: checkpoint where open_command_ids is None,
        When: _recover_engine_state runs,
        Then: recovery completes without error.
        """
        coord = _make_coord(monkeypatch)
        mock_repo = AsyncMock(spec=SQLAlchemyRepository)
        cp = _make_checkpoint(open_command_ids=None)
        mock_repo.get_all_checkpoints = AsyncMock(return_value=[cp])
        mock_repo.get_venue_events_after = AsyncMock(return_value=[])
        mock_repo.get_executions_for_recovery = AsyncMock(return_value=[])
        mock_repo.get_active_orders_for_recovery = AsyncMock(return_value=[])
        _set_sqlalchemy_repo(coord, mock_repo)

        await coord._recover_engine_state()

        assert "BTC-USD@kraken-live" in coord.engines

    @pytest.mark.asyncio
    async def test_checkpoint_restore_handles_null_seen_exec_ids(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Checkpoint with seen_exec_ids=None (legacy) gives empty set.

        Given: checkpoint where seen_exec_ids is None (pre-migration rows),
        When: _recover_engine_state runs,
        Then: engine has empty seen_exec_ids and recovery completes.
        """
        coord = _make_coord(monkeypatch)
        mock_repo = AsyncMock(spec=SQLAlchemyRepository)
        cp = _make_checkpoint()
        cp["seen_exec_ids"] = cast(str, None)
        mock_repo.get_all_checkpoints = AsyncMock(return_value=[cp])
        mock_repo.get_venue_events_after = AsyncMock(return_value=[])
        mock_repo.get_executions_for_recovery = AsyncMock(return_value=[])
        mock_repo.get_active_orders_for_recovery = AsyncMock(return_value=[])
        _set_sqlalchemy_repo(coord, mock_repo)

        await coord._recover_engine_state()

        engine = coord.engines["BTC-USD@kraken-live"]
        assert engine.seen_exec_ids == OrderedDict()

    @pytest.mark.asyncio
    async def test_checkpoint_wallet_short_prefers_temporal_repository_lookup(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Checkpoint recovery resolves wallet_short at checkpoint time.

        Given: A checkpoint shard key with a wallet_short and a stale
            process-local cache entry,
        When: Recovery rebuilds the engine,
        Then: The repository temporal lookup at ``checkpoint_at`` wins
            over the stale cache.
        """
        coord = _make_coord(monkeypatch)
        wallet_id = "018f0000-0000-7000-8000-abcdefabcdef"
        wallet_short = compute_wallet_short(wallet_id)
        stale_wallet_id = "018f1111-2222-7333-8444-555566667777"
        coord._wallet_short_to_id = {wallet_short: stale_wallet_id}
        mock_repo = AsyncMock(spec=SQLAlchemyRepository)
        checkpoint_at = datetime(2024, 6, 1, 2, tzinfo=UTC)
        cp = _make_checkpoint(
            shard_key=f"kraken.BTC-USD.live.w{wallet_short}",
            checkpoint_at=checkpoint_at,
        )
        mock_repo.get_all_checkpoints = AsyncMock(return_value=[cp])
        mock_repo.get_venue_events_after = AsyncMock(return_value=[])
        mock_repo.get_executions_for_recovery = AsyncMock(return_value=[])
        mock_repo.get_active_orders_for_recovery = AsyncMock(return_value=[])
        _set_sqlalchemy_repo(coord, mock_repo)
        mock_repo.resolve_wallet_public_id_by_short = AsyncMock(return_value=wallet_id)

        await coord._recover_engine_state()

        mock_repo.resolve_wallet_public_id_by_short.assert_awaited_once_with(
            wallet_short,
            checkpoint_at,
        )
        assert "BTC-USD@kraken-live-wabcdefabcdef" in coord.engines
        assert coord.engines["BTC-USD@kraken-live-wabcdefabcdef"].wallet_public_id == wallet_id

    @pytest.mark.asyncio
    async def test_engine_state_restored_from_checkpoint(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Engine internal state matches checkpoint values after restore.

        Given: checkpoint with specific position/cash/peak_equity,
        When: _recover_engine_state runs,
        Then: engine attributes match the checkpoint.
        """
        coord = _make_coord(monkeypatch)
        mock_repo = AsyncMock(spec=SQLAlchemyRepository)
        cp = _make_checkpoint(
            position_qty=2.0,
            entry_price=45000.0,
            cash=8000.0,
            peak_equity=12000.0,
        )
        mock_repo.get_all_checkpoints = AsyncMock(return_value=[cp])
        mock_repo.get_venue_events_after = AsyncMock(return_value=[])
        mock_repo.get_executions_for_recovery = AsyncMock(return_value=[])
        mock_repo.get_active_orders_for_recovery = AsyncMock(return_value=[])
        _set_sqlalchemy_repo(coord, mock_repo)

        await coord._recover_engine_state()

        engine = coord.engines["BTC-USD@kraken-live"]
        assert engine.position_qty == pytest.approx(2.0)
        assert engine.entry_price == pytest.approx(45000.0)
        assert engine.peak_equity == pytest.approx(12000.0)

    @pytest.mark.asyncio
    async def test_portfolio_tracker_restored_from_checkpoint(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Engine portfolio has correct cash, turnover, and position after restore.

        Given: checkpoint with cash=8000, turnover=50000, position,
        When: _recover_engine_state runs,
        Then: engine.portfolio matches checkpoint (not initial_cash).
        """
        coord = _make_coord(monkeypatch)
        mock_repo = AsyncMock(spec=SQLAlchemyRepository)
        cp = _make_checkpoint(
            position_qty=1.5,
            entry_price=40000.0,
            cash=8000.0,
            turnover=50000.0,
            realized_pnl=200.0,
        )
        mock_repo.get_all_checkpoints = AsyncMock(return_value=[cp])
        mock_repo.get_venue_events_after = AsyncMock(return_value=[])
        mock_repo.get_executions_for_recovery = AsyncMock(return_value=[])
        mock_repo.get_active_orders_for_recovery = AsyncMock(return_value=[])
        _set_sqlalchemy_repo(coord, mock_repo)

        await coord._recover_engine_state()

        engine = coord.engines["BTC-USD@kraken-live"]
        assert engine.portfolio.cash == pytest.approx(8000.0)
        assert engine.portfolio.turnover == pytest.approx(50000.0)
        pos = engine.portfolio.positions.get("BTC-USD")
        assert pos is not None
        assert pos.quantity == pytest.approx(1.5)
        assert pos.average_price == pytest.approx(40000.0)
        assert pos.realized_pnl == pytest.approx(200.0)

    @pytest.mark.asyncio
    async def test_equivalence_checkpoint_vs_full_replay(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Checkpoint restore + delta replay produces identical state to full replay.

        Given: TradeService with two paths (full replay vs checkpoint + delta),
        When: both paths process the same set of venue events,
        Then: final position, cash, peak_equity, and seen_exec_ids match.
        """
        events: list[VenueEventRow] = []
        for i in range(1, 11):
            events.append(
                _make_venue_event(
                    event_id=i,
                    fill_price=50000.0 + i * 100,
                    fill_size=0.1,
                    side="buy",
                    exec_id=f"t{i}",
                    trade_id=f"t{i}",
                )
            )

        ts_full = TradeService()
        for ev in events:
            ts_full.apply_venue_event(ev)
        full_shard = ts_full._shards["kraken.BTC-USD.live"]

        snap_at_5: dict[str, float | int | set[str] | None] = {
            "position_qty": 0.0,
            "entry_price": None,
            "cash": 0.0,
            "peak_equity": 10000.0,
            "realized_pnl": 0.0,
            "turnover": 0.0,
            "last_venue_event_id": 0,
            "seen_exec_ids": cast(Any, OrderedDict()),
        }
        ts_mid = TradeService()
        for ev in events[:5]:
            ts_mid.apply_venue_event(ev)
        mid_shard = ts_mid._shards["kraken.BTC-USD.live"]
        snap_at_5["position_qty"] = mid_shard.position.position_qty
        snap_at_5["entry_price"] = mid_shard.position.entry_price
        snap_at_5["cash"] = mid_shard.cash
        snap_at_5["peak_equity"] = mid_shard.peak_equity
        snap_at_5["realized_pnl"] = mid_shard.position.realized_pnl
        snap_at_5["turnover"] = mid_shard.turnover
        snap_at_5["last_venue_event_id"] = mid_shard.last_venue_event_id
        snap_at_5["seen_exec_ids"] = OrderedDict(mid_shard.seen_exec_ids)

        ts_checkpoint = TradeService()
        ts_checkpoint.restore_from_checkpoint(
            shard_key="kraken.BTC-USD.live",
            position_qty=snap_at_5["position_qty"],
            entry_price=snap_at_5["entry_price"],
            cash=snap_at_5["cash"],
            peak_equity=snap_at_5["peak_equity"],
            realized_pnl=snap_at_5["realized_pnl"],
            turnover=snap_at_5["turnover"],
            last_venue_event_id=snap_at_5["last_venue_event_id"],
            open_command_ids=[],
            seen_exec_ids=snap_at_5["seen_exec_ids"],
        )
        for ev in events[5:]:
            ts_checkpoint.apply_venue_event(ev)
        cp_shard = ts_checkpoint._shards["kraken.BTC-USD.live"]

        assert cp_shard.position.position_qty == pytest.approx(full_shard.position.position_qty)
        assert cp_shard.position.entry_price == pytest.approx(full_shard.position.entry_price)
        assert cp_shard.cash == pytest.approx(full_shard.cash)
        assert cp_shard.peak_equity == pytest.approx(full_shard.peak_equity)
        assert cp_shard.turnover == pytest.approx(full_shard.turnover)
        assert cp_shard.seen_exec_ids == full_shard.seen_exec_ids

    @pytest.mark.asyncio
    async def test_checkpoint_skipped_for_non_sqlalchemy_repo(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Non-SQLAlchemy repository skips checkpoint recovery entirely.

        Given: repository that is not SQLAlchemyRepository,
        When: _recover_from_checkpoints runs,
        Then: returns empty set (no shards recovered).
        """
        coord = _make_coord(monkeypatch)
        coord.repository = MagicMock()
        mock_repo_for_exec = AsyncMock()
        mock_repo_for_exec.get_executions_for_recovery = AsyncMock(return_value=[])
        mock_repo_for_exec.get_active_orders_for_recovery = AsyncMock(return_value=[])

        result = await coord._recover_from_checkpoints(datetime.now(UTC))
        assert result == set()

    @pytest.mark.asyncio
    async def test_checkpoint_db_error_falls_back_gracefully(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """DB error during checkpoint query falls back to full replay.

        Given: get_all_checkpoints raises an exception,
        When: _recover_from_checkpoints runs,
        Then: returns empty set, no crash.
        """
        coord = _make_coord(monkeypatch)
        mock_repo = AsyncMock(spec=SQLAlchemyRepository)
        mock_repo.get_all_checkpoints = AsyncMock(side_effect=RuntimeError("DB down"))
        _set_sqlalchemy_repo(coord, mock_repo)

        result = await coord._recover_from_checkpoints(datetime.now(UTC))
        assert result == set()

    @pytest.mark.asyncio
    async def test_checkpoint_shard_skipped_on_delta_replay_error(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Delta replay error for one shard does not block other shards.

        Given: two checkpoints, first has delta replay error,
        When: _recover_engine_state runs,
        Then: second shard is recovered, first falls back to full replay.
        """
        coord = _make_coord(monkeypatch)
        mock_repo = AsyncMock(spec=SQLAlchemyRepository)
        cp1 = _make_checkpoint(shard_key="kraken.BTC-USD.live")
        cp2 = _make_checkpoint(
            shard_key="kraken.ETH-USD.live",
            position_qty=10.0,
            entry_price=3000.0,
        )

        async def _side_effect(shard_key: str, after_id: int) -> list[VenueEventRow]:
            if shard_key == "kraken.BTC-USD.live":
                raise RuntimeError("Corrupt events")
            return []

        mock_repo.get_all_checkpoints = AsyncMock(return_value=[cp1, cp2])
        mock_repo.get_venue_events_after = AsyncMock(side_effect=_side_effect)
        mock_repo.get_executions_for_recovery = AsyncMock(return_value=[])
        mock_repo.get_active_orders_for_recovery = AsyncMock(return_value=[])
        _set_sqlalchemy_repo(coord, mock_repo)

        await coord._recover_engine_state()

        assert "ETH-USD@kraken-live" in coord.engines
        assert "BTC-USD@kraken-live" not in coord.engines

    @pytest.mark.asyncio
    async def test_checkpoint_recovered_shard_skips_full_replay(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Shard recovered from checkpoint is not replayed from executions.

        Given: checkpoint for BTC-USD, execution rows also exist for BTC-USD,
        When: _recover_engine_state runs,
        Then: engine is from checkpoint path, not full replay.
        """
        coord = _make_coord(monkeypatch)
        mock_repo = AsyncMock(spec=SQLAlchemyRepository)
        cp = _make_checkpoint(
            position_qty=0.5,
            entry_price=50000.0,
            cash=7500.0,
        )
        mock_repo.get_all_checkpoints = AsyncMock(return_value=[cp])
        mock_repo.get_venue_events_after = AsyncMock(return_value=[])
        mock_repo.get_executions_for_recovery = AsyncMock(
            return_value=[
                {
                    "public_id": "exe-1",
                    "timestamp": datetime(2024, 1, 1, tzinfo=UTC),
                    "session_id": "s1",
                    "sequence_id": 1,
                    "trade_id": "t99",
                    "exchange_order_id": "ex-1",
                    "client_order_id": "c1",
                    "instrument": "BTC-USD",
                    "exchange": "kraken",
                    "side": "buy",
                    "size": 999.0,
                    "price": 1.0,
                    "fee": 0.0,
                    "fee_asset": "USD",
                    "status": "filled",
                    "executed_at": datetime(2024, 1, 1, tzinfo=UTC),
                }
            ]
        )
        mock_repo.get_active_orders_for_recovery = AsyncMock(return_value=[])
        _set_sqlalchemy_repo(coord, mock_repo)

        await coord._recover_engine_state()

        engine = coord.engines["BTC-USD@kraken-live"]
        assert engine.position_qty == pytest.approx(0.5)
        assert engine.portfolio.cash == pytest.approx(7500.0)

    @pytest.mark.asyncio
    async def test_flat_position_no_portfolio_position(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Flat checkpoint (qty=0) does not create portfolio position entry.

        Given: checkpoint with position_qty=0 and entry_price=None,
        When: _recover_engine_state runs,
        Then: engine portfolio has no position for the instrument.
        """
        coord = _make_coord(monkeypatch)
        mock_repo = AsyncMock(spec=SQLAlchemyRepository)
        cp = _make_checkpoint(position_qty=0.0, entry_price=None)
        mock_repo.get_all_checkpoints = AsyncMock(return_value=[cp])
        mock_repo.get_venue_events_after = AsyncMock(return_value=[])
        mock_repo.get_executions_for_recovery = AsyncMock(return_value=[])
        mock_repo.get_active_orders_for_recovery = AsyncMock(return_value=[])
        _set_sqlalchemy_repo(coord, mock_repo)

        await coord._recover_engine_state()

        engine = coord.engines["BTC-USD@kraken-live"]
        assert engine.position_qty == pytest.approx(0.0)
        assert "BTC-USD" not in engine.portfolio.positions

    @pytest.mark.asyncio
    async def test_invalid_shard_key_format_skipped(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Checkpoint with malformed shard_key (fewer than 3 dot-segments) is skipped.

        Given: checkpoint with shard_key="bad.key",
        When: _recover_from_checkpoints runs,
        Then: shard is not recovered, no crash.
        """
        coord = _make_coord(monkeypatch)
        mock_repo = AsyncMock(spec=SQLAlchemyRepository)
        cp = _make_checkpoint(shard_key="bad.key")
        mock_repo.get_all_checkpoints = AsyncMock(return_value=[cp])
        mock_repo.get_venue_events_after = AsyncMock(return_value=[])
        mock_repo.get_executions_for_recovery = AsyncMock(return_value=[])
        mock_repo.get_active_orders_for_recovery = AsyncMock(return_value=[])
        _set_sqlalchemy_repo(coord, mock_repo)

        await coord._recover_engine_state()

        assert len(coord.engines) == 0

    @pytest.mark.asyncio
    async def test_checkpoint_with_unknown_wallet_short_logs_warning(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Unknown wallet_short in checkpoint logs a warning.

        Given: A checkpoint whose shard_key carries a ``w{wallet_short}``
            segment not present in ``self._wallet_short_to_id`` (e.g.
            credential removed between checkpoint persistence and
            coordinator restart),
        When: ``_recover_from_checkpoints`` runs,
        Then: The unknown-wallet branch fires — the shard is still
            recovered but with empty wallet_public_id so legacy
            recovery paths keep working and the operator sees the
            warning in the log.
        """
        coord = _make_coord(monkeypatch)
        coord._wallet_short_to_id = {}
        mock_repo = AsyncMock(spec=SQLAlchemyRepository)
        cp = _make_checkpoint(shard_key="kraken.BTC-USD.live.w01975a8b3c7d")
        mock_repo.get_all_checkpoints = AsyncMock(return_value=[cp])
        mock_repo.get_venue_events_after = AsyncMock(return_value=[])
        mock_repo.get_executions_for_recovery = AsyncMock(return_value=[])
        mock_repo.get_active_orders_for_recovery = AsyncMock(return_value=[])
        _set_sqlalchemy_repo(coord, mock_repo)

        await coord._recover_engine_state()

        assert "BTC-USD@kraken-live" in coord.engines

    @pytest.mark.asyncio
    async def test_resolve_checkpoint_wallet_public_id_returns_cached_public_id(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Cached wallet_short values resolve to their public wallet id."""
        coord = _make_coord(monkeypatch)
        coord._wallet_short_to_id = {"01975a8b3c7d": "wallet-public-id"}

        wallet_public_id = await coord._resolve_checkpoint_wallet_public_id(
            "kraken.BTC-USD.live.w01975a8b3c7d",
            "01975a8b3c7d",
            datetime(2024, 6, 1, tzinfo=UTC),
        )

        assert wallet_public_id == "wallet-public-id"

    @pytest.mark.asyncio
    async def test_lookup_checkpoint_wallet_public_id_returns_empty_on_repository_error(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Temporal wallet-short lookup fails soft on repository errors.

        Given: The repository raises while resolving a wallet_short,
        When: Checkpoint recovery asks for temporal wallet attribution,
        Then: The helper returns an empty wallet id so the caller can
            continue through the existing degraded recovery path.
        """
        coord = _make_coord(monkeypatch)
        mock_repo = AsyncMock(spec=SQLAlchemyRepository)
        mock_repo.resolve_wallet_public_id_by_short = AsyncMock(
            side_effect=RuntimeError("db unavailable")
        )
        coord.repository = mock_repo

        wallet_public_id = await coord._lookup_checkpoint_wallet_public_id(
            "01975a8b3c7d",
            datetime(2024, 6, 1, tzinfo=UTC),
        )

        assert wallet_public_id == ""

    @pytest.mark.asyncio
    async def test_load_checkpoint_delta_events_returns_none_for_non_sqlalchemy_repository(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Checkpoint delta replay returns None when repository is not SQLAlchemy-backed."""
        coord = _make_coord(monkeypatch)
        coord.repository = cast(SQLAlchemyRepository, object())

        delta_events = await coord._load_checkpoint_delta_events(
            _make_checkpoint(last_venue_event_id=10),
            "kraken.BTC-USD.live",
        )

        assert delta_events is None

    @pytest.mark.asyncio
    async def test_engine_creation_failure_skips_shard(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Checkpoint for invalid exchange skips engine creation gracefully.

        Given: checkpoint with exchange that fails engine creation,
        When: _recover_from_checkpoints runs,
        Then: shard is not recovered, no crash.
        """
        coord = _make_coord(monkeypatch)
        mock_repo = AsyncMock(spec=SQLAlchemyRepository)
        cp = _make_checkpoint(shard_key="INVALID_EXCHANGE.BTC-USD.live")
        mock_repo.get_all_checkpoints = AsyncMock(return_value=[cp])
        mock_repo.get_venue_events_after = AsyncMock(return_value=[])
        mock_repo.get_executions_for_recovery = AsyncMock(return_value=[])
        mock_repo.get_active_orders_for_recovery = AsyncMock(return_value=[])
        _set_sqlalchemy_repo(coord, mock_repo)

        await coord._recover_engine_state()

        assert len(coord.engines) == 0
        assert "INVALID_EXCHANGE.BTC-USD.live" not in coord.trade_service._shards

    @pytest.mark.asyncio
    async def test_engine_creation_returns_none_skips_without_shard_leak(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Engine creation failure after restore does not leave orphaned shard.

        Given: valid exchange checkpoint but _create_engine_for_recovery returns None,
        When: _recover_from_checkpoints runs,
        Then: shard is restored in TradeService but engine is not created.
        """
        coord = _make_coord(monkeypatch)
        mock_repo = AsyncMock(spec=SQLAlchemyRepository)
        cp = _make_checkpoint()
        mock_repo.get_all_checkpoints = AsyncMock(return_value=[cp])
        mock_repo.get_venue_events_after = AsyncMock(return_value=[])
        mock_repo.get_executions_for_recovery = AsyncMock(return_value=[])
        mock_repo.get_active_orders_for_recovery = AsyncMock(return_value=[])
        _set_sqlalchemy_repo(coord, mock_repo)

        with patch.object(
            TraderCoordinator, "_create_engine_for_recovery", AsyncMock(return_value=None)
        ):
            await coord._recover_engine_state()

        assert len(coord.engines) == 0

    @pytest.mark.asyncio
    async def test_checkpoint_with_null_watermark_falls_back_to_full_replay(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Checkpoint with last_venue_event_id=None falls back to full replay.

        Given: checkpoint where last_venue_event_id is None,
        When: _recover_engine_state runs,
        Then: shard is NOT recovered from checkpoint, falls back to execution replay.
        """
        coord = _make_coord(monkeypatch)
        mock_repo = AsyncMock(spec=SQLAlchemyRepository)
        cp = _make_checkpoint(last_venue_event_id=None)
        mock_repo.get_all_checkpoints = AsyncMock(return_value=[cp])
        mock_repo.get_venue_events_after = AsyncMock(return_value=[])
        mock_repo.get_executions_for_recovery = AsyncMock(return_value=[])
        mock_repo.get_active_orders_for_recovery = AsyncMock(return_value=[])
        _set_sqlalchemy_repo(coord, mock_repo)

        await coord._recover_engine_state()

        assert "BTC-USD@kraken-live" not in coord.engines
        mock_repo.get_venue_events_after.assert_not_called()
        assert "kraken.BTC-USD.live" not in coord.trade_service._shards

    @pytest.mark.asyncio
    async def test_delta_replay_error_does_not_leave_stale_shard(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Failed delta replay does not leave stale state in TradeService.

        Given: checkpoint where get_venue_events_after raises,
        When: _recover_from_checkpoints runs,
        Then: TradeService has no shard for the failed key.
        """
        coord = _make_coord(monkeypatch)
        mock_repo = AsyncMock(spec=SQLAlchemyRepository)
        cp = _make_checkpoint()
        mock_repo.get_all_checkpoints = AsyncMock(return_value=[cp])
        mock_repo.get_venue_events_after = AsyncMock(side_effect=RuntimeError("DB error"))
        mock_repo.get_executions_for_recovery = AsyncMock(return_value=[])
        mock_repo.get_active_orders_for_recovery = AsyncMock(return_value=[])
        _set_sqlalchemy_repo(coord, mock_repo)

        await coord._recover_engine_state()

        assert "BTC-USD@kraken-live" not in coord.engines
        assert "kraken.BTC-USD.live" not in coord.trade_service._shards

    @pytest.mark.asyncio
    async def test_delta_replay_updates_engine_state(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Engine reflects post-delta state, not raw checkpoint values.

        Given: checkpoint with position=0.5 at watermark 10, delta fill at event 11,
        When: _recover_engine_state runs,
        Then: engine position includes the delta fill (not just checkpoint values).
        """
        coord = _make_coord(monkeypatch)
        mock_repo = AsyncMock(spec=SQLAlchemyRepository)
        cp = _make_checkpoint(
            position_qty=0.5,
            entry_price=50000.0,
            cash=7500.0,
            seen_exec_ids='["t1", "t2"]',
        )
        delta_fill = _make_venue_event(
            event_id=11,
            fill_price=51000.0,
            fill_size=0.1,
            side="buy",
            exec_id="t3",
            trade_id="t3",
        )
        mock_repo.get_all_checkpoints = AsyncMock(return_value=[cp])
        mock_repo.get_venue_events_after = AsyncMock(return_value=[delta_fill])
        mock_repo.get_executions_for_recovery = AsyncMock(return_value=[])
        mock_repo.get_active_orders_for_recovery = AsyncMock(return_value=[])
        _set_sqlalchemy_repo(coord, mock_repo)

        await coord._recover_engine_state()

        engine = coord.engines["BTC-USD@kraken-live"]
        assert engine.position_qty != pytest.approx(0.5)
        assert "t3" in engine.seen_exec_ids
        assert engine.portfolio.cash != pytest.approx(7500.0)


class TestR9GapRecovery:
    """R9: gap-aware recovery corrects fills dropped under the scalar watermark."""

    @pytest.mark.asyncio
    async def test_checkpoint_gap_overlay_corrects_position(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A checkpoint shard with a recorded>consumed gap is corrected by overlay.

        Given: a checkpoint at watermark 10 with empty delta (so checkpoint+delta
            position is 0.5), a recorded>consumed fill gap, no funding, and a full
            venue history summing to 0.9,
        When: recovery runs,
        Then: the shard position is overlaid to the venue-replay value 0.9 while
            the checkpoint peak_equity is preserved.
        """
        coord = _make_coord(monkeypatch)
        mock_repo = AsyncMock(spec=SQLAlchemyRepository)
        mock_repo.get_all_checkpoints = AsyncMock(
            return_value=[_make_checkpoint(position_qty=0.5, peak_equity=10000.0)]
        )

        def venue_events(shard_key: str, after_id: int) -> list[VenueEventRow]:
            if after_id == 0:
                return [
                    _make_venue_event(event_id=5, fill_size=0.5, exec_id="a", trade_id="a"),
                    _make_venue_event(event_id=8, fill_size=0.4, exec_id="b", trade_id="b"),
                ]
            return []

        mock_repo.get_venue_events_after = AsyncMock(side_effect=venue_events)
        mock_repo.get_executions_for_recovery = AsyncMock(return_value=[])
        mock_repo.get_active_orders_for_recovery = AsyncMock(return_value=[])
        _set_sqlalchemy_repo(coord, mock_repo)
        mock_repo.shard_has_fill_gap = AsyncMock(return_value=True)
        mock_repo.shard_has_accruals = AsyncMock(return_value=False)

        await coord._recover_engine_state()

        shard = coord.trade_service._shards["kraken.BTC-USD.live"]
        assert shard.position.position_qty == pytest.approx(0.9)
        assert shard.peak_equity == pytest.approx(10000.0)
        assert coord._consumed_venue_event_watermarks["kraken.BTC-USD.live"] == 8

    @pytest.mark.asyncio
    async def test_checkpoint_gap_skipped_when_funding(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A funding (futures) shard's gap is left to status-quo recovery.

        Given: the same gap as above but the shard carries funding accruals,
        When: recovery runs,
        Then: no overlay happens and the position stays the checkpoint+delta value
            0.5 (the venue rebuild is spot-scoped).
        """
        coord = _make_coord(monkeypatch)
        mock_repo = AsyncMock(spec=SQLAlchemyRepository)
        mock_repo.get_all_checkpoints = AsyncMock(return_value=[_make_checkpoint(position_qty=0.5)])

        def venue_events(shard_key: str, after_id: int) -> list[VenueEventRow]:
            if after_id == 0:
                return [
                    _make_venue_event(event_id=5, fill_size=0.5, exec_id="a", trade_id="a"),
                    _make_venue_event(event_id=8, fill_size=0.4, exec_id="b", trade_id="b"),
                ]
            return []

        mock_repo.get_venue_events_after = AsyncMock(side_effect=venue_events)
        mock_repo.get_executions_for_recovery = AsyncMock(return_value=[])
        mock_repo.get_active_orders_for_recovery = AsyncMock(return_value=[])
        _set_sqlalchemy_repo(coord, mock_repo)
        mock_repo.shard_has_fill_gap = AsyncMock(return_value=True)
        mock_repo.shard_has_accruals = AsyncMock(return_value=True)

        await coord._recover_engine_state()

        shard = coord.trade_service._shards["kraken.BTC-USD.live"]
        assert shard.position.position_qty == pytest.approx(0.5)

    @pytest.mark.asyncio
    async def test_pass3_orphan_venue_only_shard_rebuilt(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A venue-only shard with no executions is discovered and rebuilt.

        Given: no checkpoint and no execution rows (so the execution-replay pass
            early-returns and never visits the shard), but a gapped shard with a
            full venue history of one buy fill 0.5,
        When: recovery runs,
        Then: Pass 3 discovers the shard, creates the engine, and rebuilds its
            position to 0.5.
        """
        coord = _make_coord(monkeypatch)
        mock_repo = AsyncMock(spec=SQLAlchemyRepository)
        mock_repo.get_all_checkpoints = AsyncMock(return_value=[])
        mock_repo.get_executions_for_recovery = AsyncMock(return_value=[])
        mock_repo.get_active_orders_for_recovery = AsyncMock(return_value=[])
        mock_repo.get_venue_events_after = AsyncMock(
            return_value=[_make_venue_event(event_id=5, fill_size=0.5, exec_id="a", trade_id="a")]
        )
        _set_sqlalchemy_repo(coord, mock_repo)
        mock_repo.get_shard_keys_with_fills = AsyncMock(return_value=["kraken.BTC-USD.live"])
        mock_repo.shard_has_fill_gap = AsyncMock(return_value=True)
        mock_repo.shard_has_accruals = AsyncMock(return_value=False)

        await coord._recover_engine_state()

        assert "BTC-USD@kraken-live" in coord.engines
        engine = coord.engines["BTC-USD@kraken-live"]
        assert engine.position_qty == pytest.approx(0.5)
        assert coord._consumed_venue_event_watermarks["kraken.BTC-USD.live"] == 5

    @pytest.mark.asyncio
    async def test_pass3_skips_funding_shard(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Pass 3 leaves a funding shard to status-quo recovery (no rebuild).

        Given: an orphan gapped shard that carries funding accruals,
        When: recovery runs,
        Then: no engine is rebuilt for it.
        """
        coord = _make_coord(monkeypatch)
        mock_repo = AsyncMock(spec=SQLAlchemyRepository)
        mock_repo.get_all_checkpoints = AsyncMock(return_value=[])
        mock_repo.get_executions_for_recovery = AsyncMock(return_value=[])
        mock_repo.get_active_orders_for_recovery = AsyncMock(return_value=[])
        mock_repo.get_venue_events_after = AsyncMock(return_value=[])
        _set_sqlalchemy_repo(coord, mock_repo)
        mock_repo.get_shard_keys_with_fills = AsyncMock(return_value=["kraken.BTC-USD.live"])
        mock_repo.shard_has_fill_gap = AsyncMock(return_value=True)
        mock_repo.shard_has_accruals = AsyncMock(return_value=True)

        await coord._recover_engine_state()

        assert "BTC-USD@kraken-live" not in coord.engines

    @pytest.mark.asyncio
    async def test_pass3_skips_checkpoint_recovered_shard(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Pass 3 excludes shards already recovered (and corrected) by Pass 1.

        Given: a shard key returned by discovery that is in the checkpoint-
            recovered set,
        When: _recover_venue_event_gaps runs,
        Then: the per-shard rebuild helper is not invoked for it.
        """
        coord = _make_coord(monkeypatch)
        mock_repo = AsyncMock(spec=SQLAlchemyRepository)
        mock_repo.get_shard_keys_with_fills = AsyncMock(return_value=["kraken.BTC-USD.live"])
        coord.repository = mock_repo
        coord._checkpoint_recovered_shard_keys = {"kraken.BTC-USD.live"}
        coord._rebuild_shard_if_gapped = AsyncMock()

        await coord._recover_venue_event_gaps(datetime(2024, 6, 1, tzinfo=UTC))

        coord._rebuild_shard_if_gapped.assert_not_called()

    @pytest.mark.asyncio
    async def test_pass3_skips_foreign_shard(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Pass 3 skips shards this instance does not own (N>1 partitioning).

        Given: a discovered shard not owned by this coordinator,
        When: _recover_venue_event_gaps runs,
        Then: the per-shard rebuild helper is not invoked for it.
        """
        coord = _make_coord(monkeypatch)
        mock_repo = AsyncMock(spec=SQLAlchemyRepository)
        mock_repo.get_shard_keys_with_fills = AsyncMock(return_value=["kraken.BTC-USD.live"])
        coord.repository = mock_repo
        coord._ownership = MagicMock()
        coord._ownership.owns = MagicMock(return_value=False)
        coord._rebuild_shard_if_gapped = AsyncMock()

        await coord._recover_venue_event_gaps(datetime(2024, 6, 1, tzinfo=UTC))

        coord._rebuild_shard_if_gapped.assert_not_called()

    @pytest.mark.asyncio
    async def test_correct_checkpoint_fill_gap_non_sqlalchemy_noop(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """_correct_checkpoint_fill_gap is a no-op without a SQLAlchemy repository.

        Given: a coordinator whose repository is not a SQLAlchemyRepository,
        When: _correct_checkpoint_fill_gap is called,
        Then: it returns without touching shard state.
        """
        coord = _make_coord(monkeypatch)
        coord.repository = MagicMock()
        await coord._correct_checkpoint_fill_gap(
            "kraken.BTC-USD.live", "", "kraken", "live", datetime(2024, 6, 1, tzinfo=UTC)
        )
        assert "kraken.BTC-USD.live" not in coord.trade_service._shards

    @pytest.mark.asyncio
    async def test_recover_venue_event_gaps_non_sqlalchemy_noop(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """_recover_venue_event_gaps is a no-op without a SQLAlchemy repository."""
        coord = _make_coord(monkeypatch)
        coord.repository = MagicMock()
        coord._rebuild_shard_if_gapped = AsyncMock()
        await coord._recover_venue_event_gaps(datetime(2024, 6, 1, tzinfo=UTC))
        coord._rebuild_shard_if_gapped.assert_not_called()

    @pytest.mark.asyncio
    async def test_recover_venue_event_gaps_query_error_noop(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A discovery-query failure is logged and skips gap recovery.

        Given: get_shard_keys_with_fills raises,
        When: _recover_venue_event_gaps runs,
        Then: it swallows the error and invokes no per-shard rebuild.
        """
        coord = _make_coord(monkeypatch)
        mock_repo = AsyncMock(spec=SQLAlchemyRepository)
        mock_repo.get_shard_keys_with_fills = AsyncMock(side_effect=RuntimeError("boom"))
        coord.repository = mock_repo
        coord._rebuild_shard_if_gapped = AsyncMock()
        await coord._recover_venue_event_gaps(datetime(2024, 6, 1, tzinfo=UTC))
        coord._rebuild_shard_if_gapped.assert_not_called()

    @pytest.mark.asyncio
    async def test_rebuild_shard_if_gapped_non_sqlalchemy_noop(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """_rebuild_shard_if_gapped is a no-op without a SQLAlchemy repository."""
        coord = _make_coord(monkeypatch)
        coord.repository = MagicMock()
        await coord._rebuild_shard_if_gapped(
            "kraken.BTC-USD.live", datetime(2024, 6, 1, tzinfo=UTC)
        )
        assert "kraken.BTC-USD.live" not in coord.trade_service._shards

    @pytest.mark.asyncio
    async def test_rebuild_shard_if_gapped_bad_shard_key(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A shard key with too few segments is skipped before any gap query."""
        coord = _make_coord(monkeypatch)
        mock_repo = AsyncMock(spec=SQLAlchemyRepository)
        _set_sqlalchemy_repo(coord, mock_repo)
        await coord._rebuild_shard_if_gapped("garbage", datetime(2024, 6, 1, tzinfo=UTC))
        mock_repo.shard_has_fill_gap.assert_not_called()

    @pytest.mark.asyncio
    async def test_rebuild_shard_if_gapped_no_gap_is_noop(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A discovered shard with no gap does not reach the funding/rebuild steps."""
        coord = _make_coord(monkeypatch)
        mock_repo = AsyncMock(spec=SQLAlchemyRepository)
        _set_sqlalchemy_repo(coord, mock_repo)
        await coord._rebuild_shard_if_gapped(
            "kraken.BTC-USD.live", datetime(2024, 6, 1, tzinfo=UTC)
        )
        mock_repo.shard_has_accruals.assert_not_called()
        assert "BTC-USD@kraken-live" not in coord.engines

    @pytest.mark.asyncio
    async def test_rebuild_shard_if_gapped_create_returns_none(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """When no engine can be created (invalid scope) the shard is skipped.

        Given: a gapped, non-funding shard but engine creation returns None,
        When: _rebuild_shard_if_gapped runs,
        Then: it skips before resetting the shard.
        """
        coord = _make_coord(monkeypatch)
        mock_repo = AsyncMock(spec=SQLAlchemyRepository)
        _set_sqlalchemy_repo(coord, mock_repo)
        mock_repo.shard_has_fill_gap = AsyncMock(return_value=True)
        coord._create_engine_for_recovery = AsyncMock(return_value=None)
        await coord._rebuild_shard_if_gapped(
            "kraken.BTC-USD.live", datetime(2024, 6, 1, tzinfo=UTC)
        )
        assert "kraken.BTC-USD.live" not in coord.trade_service._shards

    @pytest.mark.asyncio
    async def test_rebuild_shard_if_gapped_reuses_existing_engine(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A shard with an engine from Pass 2 is reused and re-slaved, not re-registered.

        Given: an engine already registered for a gapped shard (execution-group
            recovery), and a venue history of one buy fill 0.5,
        When: _rebuild_shard_if_gapped runs,
        Then: the SAME engine object is reused, re-slaved to the rebuilt shard,
            and no second engine is created.
        """
        coord = _make_coord(monkeypatch)
        mock_repo = AsyncMock(spec=SQLAlchemyRepository)
        mock_repo.get_venue_events_after = AsyncMock(
            return_value=[_make_venue_event(event_id=5, fill_size=0.5, exec_id="a", trade_id="a")]
        )
        _set_sqlalchemy_repo(coord, mock_repo)
        mock_repo.shard_has_fill_gap = AsyncMock(return_value=True)
        existing = await coord._create_engine_for_recovery(
            "BTC-USD", "kraken", strategy_tag=None, wallet_public_id="", operator_public_id=""
        )
        assert existing is not None
        coord._register_recovered_engine("BTC-USD@kraken-live", existing)
        coord._create_engine_for_recovery = AsyncMock()
        await coord._rebuild_shard_if_gapped(
            "kraken.BTC-USD.live", datetime(2024, 6, 1, tzinfo=UTC)
        )
        coord._create_engine_for_recovery.assert_not_called()
        assert coord.engines["BTC-USD@kraken-live"] is existing
        assert existing.position_qty == pytest.approx(0.5)

    @pytest.mark.asyncio
    async def test_checkpoint_gap_correction_failure_preserves_checkpoint(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A gap-detection query failure leaves checkpoint-restored state intact.

        Given: a recovered checkpoint shard whose shard_has_fill_gap query raises,
        When: recovery runs,
        Then: the failure is swallowed and the checkpoint+delta position stands
            (recovery does not abort).
        """
        coord = _make_coord(monkeypatch)
        mock_repo = AsyncMock(spec=SQLAlchemyRepository)
        mock_repo.get_all_checkpoints = AsyncMock(return_value=[_make_checkpoint(position_qty=0.5)])
        mock_repo.get_venue_events_after = AsyncMock(return_value=[])
        mock_repo.get_executions_for_recovery = AsyncMock(return_value=[])
        mock_repo.get_active_orders_for_recovery = AsyncMock(return_value=[])
        _set_sqlalchemy_repo(coord, mock_repo)
        mock_repo.shard_has_fill_gap = AsyncMock(side_effect=RuntimeError("boom"))

        await coord._recover_engine_state()

        shard = coord.trade_service._shards["kraken.BTC-USD.live"]
        assert shard.position.position_qty == pytest.approx(0.5)

    @pytest.mark.asyncio
    async def test_recover_venue_event_gaps_rebuild_error_continues(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A per-shard rebuild failure is swallowed so other shards still recover."""
        coord = _make_coord(monkeypatch)
        mock_repo = AsyncMock(spec=SQLAlchemyRepository)
        mock_repo.get_shard_keys_with_fills = AsyncMock(return_value=["kraken.BTC-USD.live"])
        coord.repository = mock_repo
        coord._rebuild_shard_if_gapped = AsyncMock(side_effect=RuntimeError("boom"))
        await coord._recover_venue_event_gaps(datetime(2024, 6, 1, tzinfo=UTC))
        coord._rebuild_shard_if_gapped.assert_called_once()

    @pytest.mark.asyncio
    async def test_pass3_reuse_clears_stale_flat_position(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Reusing an engine and rebuilding to flat clears the stale portfolio position.

        Given: an engine with a stale non-flat portfolio position and a venue
            history that nets to flat (buy 0.5 then sell 0.5),
        When: _rebuild_shard_if_gapped reuses the engine,
        Then: the engine is flat and carries no portfolio position entry, so a
            later live fill cannot book from a stale quantity/price.
        """
        coord = _make_coord(monkeypatch)
        mock_repo = AsyncMock(spec=SQLAlchemyRepository)
        mock_repo.get_venue_events_after = AsyncMock(
            return_value=[
                _make_venue_event(
                    event_id=5,
                    side="buy",
                    fill_price=100.0,
                    fill_size=0.5,
                    exec_id="a",
                    trade_id="a",
                ),
                _make_venue_event(
                    event_id=6,
                    side="sell",
                    fill_price=110.0,
                    fill_size=0.5,
                    exec_id="b",
                    trade_id="b",
                ),
            ]
        )
        _set_sqlalchemy_repo(coord, mock_repo)
        mock_repo.shard_has_fill_gap = AsyncMock(return_value=True)
        existing = await coord._create_engine_for_recovery(
            "BTC-USD", "kraken", strategy_tag=None, wallet_public_id="", operator_public_id=""
        )
        assert existing is not None
        existing.portfolio.positions["BTC-USD"] = PositionStateModel(
            quantity=0.5, average_price=100.0, realized_pnl=0.0
        )
        coord._register_recovered_engine("BTC-USD@kraken-live", existing)

        await coord._rebuild_shard_if_gapped(
            "kraken.BTC-USD.live", datetime(2024, 6, 1, tzinfo=UTC)
        )

        assert coord.engines["BTC-USD@kraken-live"] is existing
        assert existing.position_qty == pytest.approx(0.0)
        assert "BTC-USD" not in existing.portfolio.positions

    @pytest.mark.asyncio
    async def test_rebuild_shard_if_gapped_read_failure_does_not_wipe_shard(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A venue-event read failure happens BEFORE reset, so the shard is intact.

        Given: a gapped shard with existing (Pass 2) TradeService state and a
            get_venue_events_after that raises,
        When: _rebuild_shard_if_gapped runs,
        Then: it raises (caught fail-soft by the caller) WITHOUT having reset the
            live shard — the prior position is preserved.
        """
        coord = _make_coord(monkeypatch)
        mock_repo = AsyncMock(spec=SQLAlchemyRepository)
        mock_repo.get_venue_events_after = AsyncMock(side_effect=RuntimeError("db down"))
        _set_sqlalchemy_repo(coord, mock_repo)
        mock_repo.shard_has_fill_gap = AsyncMock(return_value=True)
        shard = coord.trade_service._get_or_create_shard("kraken.BTC-USD.live")
        shard.position.position_qty = 0.5

        with pytest.raises(RuntimeError):
            await coord._rebuild_shard_if_gapped(
                "kraken.BTC-USD.live", datetime(2024, 6, 1, tzinfo=UTC)
            )

        assert coord.trade_service._shards[
            "kraken.BTC-USD.live"
        ].position.position_qty == pytest.approx(0.5)


class TestRecoveryCertification:
    """Positive-certification paths of the projection trust model."""

    @pytest.mark.asyncio
    async def test_checkpoint_happy_path_grants_trust(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A fully certain checkpoint recovery certifies its shard.

        Given: a checkpoint shard with no fill gap, certain accruals,
            no accrual-ledger rows, and a registered identity,
        When: _recover_checkpoint_row completes,
        Then: the shard joins the trusted set.
        """
        coord = _make_coord(monkeypatch)
        mock_repo = AsyncMock(spec=SQLAlchemyRepository)
        mock_repo.get_venue_events_after = AsyncMock(return_value=[])
        mock_repo.get_instrument_public_id_by_symbol = AsyncMock(return_value="inst-pid")
        mock_repo.get_accruals = AsyncMock(return_value=[])
        mock_repo.shard_has_any_accruals = AsyncMock(return_value=False)
        _set_sqlalchemy_repo(coord, mock_repo)
        coord._projection_identities["kraken.BTC-USD.live"] = ("inst-pid", "live", "w-1")
        engine_key = await coord._recover_checkpoint_row(_make_checkpoint(), datetime.now(UTC))
        assert engine_key is not None
        assert "kraken.BTC-USD.live" in coord._trusted_recovery_shards

    @pytest.mark.asyncio
    async def test_certification_probe_failure_refuses_trust(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A failing accrual existence probe leaves the shard untrusted.

        Given: the clock-free accrual probe raises,
        When: _recover_checkpoint_row completes,
        Then: the shard is NOT trusted (uncertain accrual state).
        """
        coord = _make_coord(monkeypatch)
        mock_repo = AsyncMock(spec=SQLAlchemyRepository)
        mock_repo.get_venue_events_after = AsyncMock(return_value=[])
        mock_repo.get_instrument_public_id_by_symbol = AsyncMock(return_value="inst-pid")
        mock_repo.get_accruals = AsyncMock(return_value=[])
        mock_repo.shard_has_any_accruals = AsyncMock(side_effect=RuntimeError("db down"))
        _set_sqlalchemy_repo(coord, mock_repo)
        coord._projection_identities["kraken.BTC-USD.live"] = ("inst-pid", "live", "w-1")
        engine_key = await coord._recover_checkpoint_row(_make_checkpoint(), datetime.now(UTC))
        assert engine_key is not None
        assert "kraken.BTC-USD.live" not in coord._trusted_recovery_shards

    @pytest.mark.asyncio
    async def test_checkpoint_row_exception_records_and_continues(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A crashing checkpoint candidate blocks certification, not boot.

        Given: _recover_checkpoint_row raises for the only checkpoint
            and the candidate cannot be canonically attributed,
        When: _recover_from_checkpoints runs,
        Then: recovery survives, nothing is recovered, and the global
            certification flag trips.
        """
        coord = _make_coord(monkeypatch)
        mock_repo = AsyncMock(spec=SQLAlchemyRepository)
        mock_repo.get_all_checkpoints = AsyncMock(return_value=[_make_checkpoint()])
        mock_repo.get_instrument_public_id_by_symbol = AsyncMock(return_value=None)
        _set_sqlalchemy_repo(coord, mock_repo)
        with patch.object(
            coord, "_recover_checkpoint_row", AsyncMock(side_effect=RuntimeError("boom"))
        ):
            recovered = await coord._recover_from_checkpoints(datetime.now(UTC))
        assert recovered == set()
        assert coord._recovery_certification_failed is True

    @pytest.mark.asyncio
    async def test_invalid_exchange_checkpoint_records_failure(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """An invalid-exchange candidate is denied, never ignored.

        Given: a checkpoint whose shard key names an unknown exchange
            and cannot be canonically attributed,
        When: _recover_checkpoint_row runs,
        Then: it returns None and the global certification flag trips.
        """
        coord = _make_coord(monkeypatch)
        mock_repo = AsyncMock(spec=SQLAlchemyRepository)
        mock_repo.get_instrument_public_id_by_symbol = AsyncMock(return_value=None)
        _set_sqlalchemy_repo(coord, mock_repo)
        result = await coord._recover_checkpoint_row(
            _make_checkpoint(shard_key="nope.BTC-USD.live"), datetime.now(UTC)
        )
        assert result is None
        assert coord._recovery_certification_failed is True

    @pytest.mark.asyncio
    async def test_recorder_lazily_initializes_failure_sets(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The failure recorder tolerates bare coordinator instances.

        Given: a coordinator whose failure sets were never initialized,
        When: the recorder attributes a canonical failure,
        Then: both sets materialize and carry the identity.
        """
        coord = _make_coord(monkeypatch)
        mock_repo = AsyncMock(spec=SQLAlchemyRepository)
        mock_repo.get_instrument_public_id_by_symbol = AsyncMock(return_value="inst-pid")
        _set_sqlalchemy_repo(coord, mock_repo)
        coord._wallet_short_to_id = {"aabbccddeeff": "w-full"}
        del coord._failed_recovery_shard_prefixes
        del coord._failed_recovery_identities
        await coord._record_recovery_shard_failure("kraken.BTC-USD.live.waabbccddeeff")
        assert ("kraken", "BTC-USD", "live", "aabbccddeeff") in (
            coord._failed_recovery_shard_prefixes
        )
        assert ("inst-pid", "live", "w-full") in coord._failed_recovery_identities

    @pytest.mark.asyncio
    async def test_gap_rebuild_with_registration_grants_trust(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A certified gap rebuild with a registered identity certifies.

        Given: the gapped rebuild succeeds and the shard registered an
            identity,
        When: _recover_venue_event_gaps runs,
        Then: the shard joins the trusted set.
        """
        coord = _make_coord(monkeypatch)
        mock_repo = AsyncMock(spec=SQLAlchemyRepository)
        _set_sqlalchemy_repo(coord, mock_repo)
        mock_repo.get_shard_keys_with_fills = AsyncMock(return_value=["kraken.BTC-USD.live"])
        coord._projection_identities["kraken.BTC-USD.live"] = ("inst-pid", "live", "w-1")
        with patch.object(coord, "_rebuild_shard_if_gapped", AsyncMock(return_value=True)):
            await coord._recover_venue_event_gaps(datetime.now(UTC))
        assert "kraken.BTC-USD.live" in coord._trusted_recovery_shards

    @pytest.mark.asyncio
    async def test_gap_rebuild_without_registration_records_failure(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A rebuilt-but-unregistered gap shard is denied certification.

        Given: the gapped rebuild succeeds but no identity registered,
        When: _recover_venue_event_gaps runs,
        Then: the shard is not trusted and the unattributable candidate
            trips the global flag.
        """
        coord = _make_coord(monkeypatch)
        mock_repo = AsyncMock(spec=SQLAlchemyRepository)
        mock_repo.get_instrument_public_id_by_symbol = AsyncMock(return_value=None)
        _set_sqlalchemy_repo(coord, mock_repo)
        mock_repo.get_shard_keys_with_fills = AsyncMock(return_value=["kraken.BTC-USD.live"])
        with patch.object(coord, "_rebuild_shard_if_gapped", AsyncMock(return_value=True)):
            await coord._recover_venue_event_gaps(datetime.now(UTC))
        assert "kraken.BTC-USD.live" not in coord._trusted_recovery_shards
        assert coord._recovery_certification_failed is True

    @pytest.mark.asyncio
    async def test_execution_replay_registry_miss_and_probe_failure(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Execution replay denies trust on probe failure or no registry.

        Given: a live execution group whose accrual probe raises, and a
            second run with a clean probe but no registered identity,
        When: _recover_engine_state replays executions,
        Then: neither run certifies the shard; the registry miss records
            an (unattributable) failure.
        """
        coord = _make_coord(monkeypatch)
        mock_repo = AsyncMock(spec=SQLAlchemyRepository)
        mock_repo.get_all_checkpoints = AsyncMock(return_value=[])
        mock_repo.get_executions_for_recovery = AsyncMock(
            return_value=[
                {
                    "public_id": "exe-1",
                    "timestamp": datetime(2024, 1, 1, tzinfo=UTC),
                    "session_id": "s1",
                    "sequence_id": 1,
                    "trade_id": "t1",
                    "exchange_order_id": "ex-1",
                    "client_order_id": "c1",
                    "instrument": "BTC-USD",
                    "exchange": "kraken",
                    "side": "buy",
                    "size": 0.5,
                    "price": 50000.0,
                    "fee": 0.5,
                    "fee_asset": "USD",
                    "status": "filled",
                    "executed_at": datetime(2024, 1, 1, tzinfo=UTC),
                }
            ]
        )
        mock_repo.get_active_orders_for_recovery = AsyncMock(return_value=[])
        mock_repo.get_consumed_fill_venue_event_id = AsyncMock(side_effect=RuntimeError("db"))
        mock_repo.shard_has_any_accruals = AsyncMock(side_effect=RuntimeError("db"))
        mock_repo.get_instrument_public_id_by_symbol = AsyncMock(return_value=None)
        _set_sqlalchemy_repo(coord, mock_repo)
        await coord._recover_engine_state()
        assert "kraken.BTC-USD.live" not in coord._trusted_recovery_shards
        coord2 = _make_coord(monkeypatch)
        mock_repo2 = AsyncMock(spec=SQLAlchemyRepository)
        mock_repo2.get_all_checkpoints = AsyncMock(return_value=[])
        mock_repo2.get_executions_for_recovery = mock_repo.get_executions_for_recovery
        mock_repo2.get_active_orders_for_recovery = AsyncMock(return_value=[])
        mock_repo2.get_consumed_fill_venue_event_id = AsyncMock(return_value=None)
        mock_repo2.shard_has_any_accruals = AsyncMock(return_value=False)
        mock_repo2.get_instrument_public_id_by_symbol = AsyncMock(return_value=None)
        _set_sqlalchemy_repo(coord2, mock_repo2)
        await coord2._recover_engine_state()
        assert "kraken.BTC-USD.live" not in coord2._trusted_recovery_shards
        assert coord2._recovery_certification_failed is True


class TestDurableExecutionLineage:
    """S5: execution replay follows durable venue-event shard keys."""

    @pytest.mark.asyncio
    async def test_tagged_paper_recovery_projects_without_phantom_twin(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The prod regression: no untagged twin, identity projects.

        Given: a TAGGED paper checkpoint shard plus execution rows whose
            fills carry the SAME durable tagged shard key in
            venue_events,
        When: _recover_engine_state runs,
        Then: the execution bucket collides with the checkpoint-recovered
            engine key and is skipped (no phantom untagged sibling), the
            tagged shard is certified, and the identity's truthful
            position row is PROJECTED.
        """
        tagged = "paper.BTC-USD.paper.waabbccddeeff.heartbeat"
        wallet = "00000000-0000-7000-8000-aabbccddeeff"
        coord = _make_coord(monkeypatch)
        coord._wallet_short_to_id = {"aabbccddeeff": wallet}
        mock_repo = AsyncMock(spec=SQLAlchemyRepository)
        checkpoint = _make_checkpoint(shard_key=tagged)
        checkpoint["wallet_public_id"] = wallet
        mock_repo.get_all_checkpoints = AsyncMock(return_value=[checkpoint])
        mock_repo.get_venue_events_after = AsyncMock(return_value=[])
        mock_repo.get_instrument_public_id_by_symbol = AsyncMock(return_value="inst-pid")
        mock_repo.get_accruals = AsyncMock(return_value=[])
        mock_repo.shard_has_any_accruals = AsyncMock(return_value=False)
        mock_repo.get_executions_for_recovery = AsyncMock(
            return_value=[
                {
                    "public_id": "exe-1",
                    "timestamp": datetime(2024, 6, 1, tzinfo=UTC),
                    "session_id": "s1",
                    "sequence_id": 1,
                    "trade_id": "t1",
                    "exchange_order_id": "ex-1",
                    "client_order_id": "c-1",
                    "instrument": "BTC-USD",
                    "exchange": "paper",
                    "side": "buy",
                    "size": 0.5,
                    "price": 50000.0,
                    "fee": 0.5,
                    "fee_asset": "USD",
                    "status": "filled",
                    "executed_at": datetime(2024, 6, 1, tzinfo=UTC),
                    "wallet_public_id": wallet,
                    "operator_public_id": None,
                }
            ]
        )
        mock_repo.get_fill_shard_keys_by_client_order_ids = AsyncMock(
            return_value=({"c-1": (tagged, wallet)}, set())
        )
        mock_repo.get_consumed_fill_venue_event_id = AsyncMock(return_value=42)
        mock_repo.get_active_orders_for_recovery = AsyncMock(return_value=[])
        mock_repo.get_active_position_identities = AsyncMock(return_value=[])
        mock_repo.upsert_position_projection = AsyncMock(return_value=1)
        mock_repo.close_position_projection = AsyncMock(return_value=True)
        mock_repo.resolve_source_instrument_public_id = AsyncMock(
            return_value={"valuation_public_id": "src-pid", "is_paper": True, "mapped": True}
        )
        mock_repo.get_active_market_snapshot_price = AsyncMock(
            return_value=(50100.0, datetime(2024, 6, 1, tzinfo=UTC))
        )
        _set_sqlalchemy_repo(coord, mock_repo)
        coord._projection_identities[tagged] = ("inst-pid", "paper", wallet)

        await coord._recover_engine_state()

        assert tagged in coord._trusted_recovery_shards
        untagged = "paper.BTC-USD.paper.waabbccddeeff"
        assert untagged not in coord.trade_service.known_shard_keys()
        mock_repo.upsert_position_projection.assert_awaited_once()
        row = mock_repo.upsert_position_projection.await_args.args[0]
        assert row["instrument_public_id"] == "inst-pid"
        assert row["wallet_public_id"] == wallet
        assert row["quantity"] == 0.5

    @pytest.mark.asyncio
    async def test_durable_paper_group_without_checkpoint_certifies(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Durable-lineage paper execution replay is now certifiable.

        Given: a paper execution bucket whose durable tagged lineage
            resolved from venue events, no checkpoint, and a registered
            identity,
        When: _recover_execution_group replays it,
        Then: the tagged shard is trusted (the round-5 paper denial
            applies only to RECONSTRUCTED lineage).
        """
        tagged = "paper.ETH-USD.paper.waabbccddeeff.momo"
        wallet = "00000000-0000-7000-8000-aabbccddeeff"
        coord = _make_coord(monkeypatch)
        mock_repo = AsyncMock(spec=SQLAlchemyRepository)
        mock_repo.get_instrument_public_id_by_symbol = AsyncMock(return_value="inst-eth")
        mock_repo.get_consumed_fill_venue_event_id = AsyncMock(return_value=7)
        mock_repo.shard_has_any_accruals = AsyncMock(return_value=False)
        _set_sqlalchemy_repo(coord, mock_repo)
        coord._projection_identities[tagged] = ("inst-eth", "paper", wallet)
        await coord._recover_execution_group(
            engine_key="ETH-USD@paper-momo",
            fills=[
                cast(
                    ExecutionRow,
                    {
                        "public_id": "exe-2",
                        "timestamp": datetime(2024, 6, 1, tzinfo=UTC),
                        "session_id": "s1",
                        "sequence_id": 1,
                        "trade_id": "t2",
                        "exchange_order_id": None,
                        "client_order_id": "c-2",
                        "instrument": "ETH-USD",
                        "exchange": "paper",
                        "side": "buy",
                        "size": 1.0,
                        "price": 2000.0,
                        "fee": 0.1,
                        "fee_asset": "USD",
                        "status": "filled",
                        "executed_at": datetime(2024, 6, 1, tzinfo=UTC),
                        "wallet_public_id": wallet,
                        "operator_public_id": None,
                    },
                )
            ],
            wallet_public_id=wallet,
            operator_public_id="",
            strategy_tag="momo",
            durable_lineage=True,
            expected_shard_key=tagged,
        )
        assert tagged in coord._trusted_recovery_shards

    @pytest.mark.asyncio
    async def test_shard_buckets_are_injective_for_pathological_tags(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Buckets key on exact shard keys, never lossy engine keys.

        Given: one row whose durable lineage carries a strategy tag
            literally spelled "paper" and another row with no lineage
            (reconstructed untagged live) for the same instrument and
            wallet,
        When: grouping runs,
        Then: the rows land in two DISTINCT buckets keyed by their
            exact shard keys with correct lineage flags — a lossy
            engine key would have coalesced tag="paper" with
            mode-labelled buckets and certified a falsely netted
            aggregate.
        """
        wallet = "00000000-0000-7000-8000-aabbccddeeff"
        tagged_paper = "paper.BTC-USD.paper.waabbccddeeff.paper"
        coord = _make_coord(monkeypatch)
        coord._ownership = None

        def _row(cid: str) -> ExecutionRow:
            return cast(
                ExecutionRow,
                {
                    "public_id": f"exe-{cid}",
                    "timestamp": datetime(2024, 6, 1, tzinfo=UTC),
                    "session_id": "s1",
                    "sequence_id": 1,
                    "trade_id": cid,
                    "exchange_order_id": None,
                    "client_order_id": cid,
                    "instrument": "BTC-USD",
                    "exchange": "paper",
                    "side": "buy",
                    "size": 0.5,
                    "price": 50000.0,
                    "fee": 0.1,
                    "fee_asset": "USD",
                    "status": "filled",
                    "executed_at": datetime(2024, 6, 1, tzinfo=UTC),
                    "wallet_public_id": wallet,
                    "operator_public_id": None,
                },
            )

        fills, _wallets, _ops, lineage = coord._group_execution_recovery_rows(
            [_row("cid-tagged"), _row("cid-bare")],
            {"cid-tagged": (tagged_paper, wallet)},
        )
        assert set(fills) == {tagged_paper, f"paper.BTC-USD.live.w{wallet[-12:]}"}
        assert lineage[tagged_paper][1] == "paper"
        assert lineage[tagged_paper][2] is True
        assert lineage[f"paper.BTC-USD.live.w{wallet[-12:]}"][2] is False
        assert coord._recovery_certification_failed is False

    @pytest.mark.asyncio
    async def test_engine_key_collision_never_skips_a_distinct_shard(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Checkpoint skip matches EXACT shards, never lossy engine keys.

        Given: a checkpoint-recovered UNTAGGED paper shard and an
            execution bucket for a DISTINCT durable shard whose strategy
            tag is literally "paper" — both collapse to the same lossy
            engine key,
        When: the execution pass runs,
        Then: the tagged bucket is NOT skipped; its engine replays.
        """
        wallet = "00000000-0000-7000-8000-aabbccddeeff"
        untagged = "paper.BTC-USD.paper.waabbccddeeff"
        tagged = "paper.BTC-USD.paper.waabbccddeeff.paper"
        coord = _make_coord(monkeypatch)
        coord._ownership = None
        mock_repo = AsyncMock(spec=SQLAlchemyRepository)
        mock_repo.get_instrument_public_id_by_symbol = AsyncMock(return_value="inst-pid")
        mock_repo.get_consumed_fill_venue_event_id = AsyncMock(return_value=9)
        mock_repo.shard_has_any_accruals = AsyncMock(return_value=False)
        mock_repo.get_executions_for_recovery = AsyncMock(
            return_value=[
                {
                    "public_id": "exe-9",
                    "timestamp": datetime(2024, 6, 1, tzinfo=UTC),
                    "session_id": "s1",
                    "sequence_id": 1,
                    "trade_id": "t9",
                    "exchange_order_id": None,
                    "client_order_id": "c-9",
                    "instrument": "BTC-USD",
                    "exchange": "paper",
                    "side": "buy",
                    "size": 0.25,
                    "price": 50000.0,
                    "fee": 0.1,
                    "fee_asset": "USD",
                    "status": "filled",
                    "executed_at": datetime(2024, 6, 1, tzinfo=UTC),
                    "wallet_public_id": wallet,
                    "operator_public_id": None,
                }
            ]
        )
        mock_repo.get_fill_shard_keys_by_client_order_ids = AsyncMock(
            return_value=({"c-9": (tagged, wallet)}, set())
        )
        _set_sqlalchemy_repo(coord, mock_repo)
        coord._checkpoint_recovered_shard_keys.add(untagged)
        with patch.object(coord, "_recover_execution_group", new_callable=AsyncMock) as group:
            await coord._recover_from_executions(datetime.now(UTC), {"BTC-USD@paper-paper"})
        group.assert_awaited_once()
        assert group.await_args is not None
        assert group.await_args.kwargs["expected_shard_key"] == tagged

    @pytest.mark.asyncio
    async def test_lineage_failure_modes_deny_certification(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Every lineage failure mode denies, never launders.

        Given: (1) a lineage lookup that raises, (2) an ambiguous cid,
            (3) a durable wallet contradicting the execution row, and
            (4) an intra-bucket full-wallet disagreement,
        When: the execution recovery entry and grouping run,
        Then: (1) falls back with NO flag, while (2)-(4) each trip the
            global certification flag.
        """
        wallet = "00000000-0000-7000-8000-aabbccddeeff"
        other_wallet = "00000000-0000-7000-8000-ffffaabbccdd"

        def _exe(cid: str, w: str) -> ExecutionRow:
            row = {
                "public_id": f"exe-{cid}",
                "timestamp": datetime(2024, 6, 1, tzinfo=UTC),
                "session_id": "s1",
                "sequence_id": 1,
                "trade_id": cid,
                "exchange_order_id": None,
                "client_order_id": cid,
                "instrument": "BTC-USD",
                "exchange": "kraken",
                "side": "buy",
                "size": 0.5,
                "price": 50000.0,
                "fee": 0.1,
                "fee_asset": "USD",
                "status": "filled",
                "executed_at": datetime(2024, 6, 1, tzinfo=UTC),
                "wallet_public_id": w,
                "operator_public_id": None,
            }
            return cast(ExecutionRow, row)

        coord = _make_coord(monkeypatch)
        coord._ownership = None
        mock_repo = AsyncMock(spec=SQLAlchemyRepository)
        mock_repo.get_all_checkpoints = AsyncMock(return_value=[])
        mock_repo.get_executions_for_recovery = AsyncMock(return_value=[_exe("c-1", wallet)])
        mock_repo.get_fill_shard_keys_by_client_order_ids = AsyncMock(
            side_effect=RuntimeError("db down")
        )
        _set_sqlalchemy_repo(coord, mock_repo)
        with patch.object(coord, "_recover_execution_group", new_callable=AsyncMock):
            await coord._recover_from_executions(datetime.now(UTC), set())
        assert coord._recovery_certification_failed is False

        coord2 = _make_coord(monkeypatch)
        coord2._ownership = None
        mock_repo.get_fill_shard_keys_by_client_order_ids = AsyncMock(return_value=({}, {"c-1"}))
        _set_sqlalchemy_repo(coord2, mock_repo)
        with patch.object(coord2, "_recover_execution_group", new_callable=AsyncMock):
            await coord2._recover_from_executions(datetime.now(UTC), set())
        assert coord2._recovery_certification_failed is True

        coord3 = _make_coord(monkeypatch)
        coord3._ownership = None
        result3 = coord3._classify_execution_recovery_row(
            _exe("c-3", wallet),
            ("kraken.BTC-USD.live.wffffaabbccdd", other_wallet),
        )
        assert result3 is None
        assert coord3._recovery_certification_failed is True

        coord4 = _make_coord(monkeypatch)
        coord4._ownership = None
        shard = "kraken.BTC-USD.live.waabbccddeeff"
        suffix_twin = "11111111-1111-7111-8111-aabbccddeeff"
        coord4._group_execution_recovery_rows(
            [
                _exe("c-4", wallet),
                _exe("c-5", suffix_twin),
            ],
            {"c-4": (shard, wallet), "c-5": (shard, suffix_twin)},
        )
        assert coord4._recovery_certification_failed is True

    @pytest.mark.asyncio
    async def test_engine_divergence_records_both_and_skips_replay(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A recreated engine diverging from its bucket never replays.

        Given: an execution bucket whose expected shard key the
            recreated engine does not reproduce,
        When: _recover_execution_group runs,
        Then: BOTH identities are recorded as failed, no replay state
            materializes, and nothing is certified.
        """
        wallet = "00000000-0000-7000-8000-aabbccddeeff"
        expected = "kraken.BTC-USD.live.waabbccddeeff.ghost"
        coord = _make_coord(monkeypatch)
        coord._wallet_short_to_id = {"aabbccddeeff": wallet}
        mock_repo = AsyncMock(spec=SQLAlchemyRepository)
        mock_repo.get_instrument_public_id_by_symbol = AsyncMock(return_value="inst-pid")
        _set_sqlalchemy_repo(coord, mock_repo)
        await coord._recover_execution_group(
            engine_key="BTC-USD@kraken-ghost",
            fills=[
                cast(
                    ExecutionRow,
                    {
                        "public_id": "exe-d",
                        "timestamp": datetime(2024, 6, 1, tzinfo=UTC),
                        "session_id": "s1",
                        "sequence_id": 1,
                        "trade_id": "t-d",
                        "exchange_order_id": None,
                        "client_order_id": "c-d",
                        "instrument": "BTC-USD",
                        "exchange": "kraken",
                        "side": "buy",
                        "size": 0.5,
                        "price": 50000.0,
                        "fee": 0.1,
                        "fee_asset": "USD",
                        "status": "filled",
                        "executed_at": datetime(2024, 6, 1, tzinfo=UTC),
                        "wallet_public_id": wallet,
                        "operator_public_id": None,
                    },
                )
            ],
            wallet_public_id=wallet,
            operator_public_id="",
            strategy_tag=None,
            durable_lineage=True,
            expected_shard_key=expected,
        )
        assert expected not in coord._trusted_recovery_shards
        assert "kraken.BTC-USD.live.waabbccddeeff" not in coord._trusted_recovery_shards
        assert ("inst-pid", "live", wallet) in coord._failed_recovery_identities
        assert "kraken.BTC-USD.live.waabbccddeeff" not in coord.trade_service.known_shard_keys()

    @pytest.mark.asyncio
    async def test_engine_key_collision_quarantines_instead_of_overwriting(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A registry-key collision across shards fails certification.

        Given: an incumbent engine serving one exact shard and a
            recovered engine for a DIFFERENT shard colliding on the same
            registry key,
        When: registration runs,
        Then: the incumbent is kept and the global flag trips — silent
            overwrite would mis-route later fills into the wrong shard.
        """
        coord = _make_coord(monkeypatch)
        incumbent = MagicMock()
        incumbent._shard_key = "paper.BTC-USD.paper.waabbccddeeff"
        coord.engines["BTC-USD@paper-paper"] = incumbent
        newcomer = MagicMock()
        newcomer._shard_key = "paper.BTC-USD.paper.waabbccddeeff.paper"
        coord._register_recovered_engine("BTC-USD@paper-paper", newcomer)
        assert coord.engines["BTC-USD@paper-paper"] is incumbent
        assert coord._recovery_certification_failed is True

    @pytest.mark.asyncio
    async def test_suffix_twin_wallet_guards_fail_certification(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Suffix-twin wallet collisions on shard strings never certify.

        Given: (1) a checkpoint-recovered shard whose execution lineage
            belongs to a DIFFERENT full wallet with the same 48-bit
            suffix, (2) a checkpoint whose delta replay carries a
            foreign wallet's events, and (3) a checkpoint whose durable
            wallet disagrees with the shard's embedded segment,
        When: recovery runs each case,
        Then: every case trips the global certification flag.
        """
        wallet_a = "00000000-0000-7000-8000-aabbccddeeff"
        wallet_b = "11111111-1111-7111-8111-aabbccddeeff"
        shard = "paper.BTC-USD.paper.waabbccddeeff"
        coord = _make_coord(monkeypatch)
        coord._ownership = None
        mock_repo = AsyncMock(spec=SQLAlchemyRepository)
        mock_repo.get_executions_for_recovery = AsyncMock(
            return_value=[
                {
                    "public_id": "exe-t",
                    "timestamp": datetime(2024, 6, 1, tzinfo=UTC),
                    "session_id": "s1",
                    "sequence_id": 1,
                    "trade_id": "t-t",
                    "exchange_order_id": None,
                    "client_order_id": "c-t",
                    "instrument": "BTC-USD",
                    "exchange": "paper",
                    "side": "buy",
                    "size": 0.5,
                    "price": 50000.0,
                    "fee": 0.1,
                    "fee_asset": "USD",
                    "status": "filled",
                    "executed_at": datetime(2024, 6, 1, tzinfo=UTC),
                    "wallet_public_id": wallet_b,
                    "operator_public_id": None,
                }
            ]
        )
        mock_repo.get_fill_shard_keys_by_client_order_ids = AsyncMock(
            return_value=({"c-t": (shard, wallet_b)}, set())
        )
        _set_sqlalchemy_repo(coord, mock_repo)
        coord._checkpoint_recovered_shard_keys.add(shard)
        coord._checkpoint_recovered_shard_wallets[shard] = wallet_a
        await coord._recover_from_executions(datetime.now(UTC), set())
        assert coord._recovery_certification_failed is True

        coord2 = _make_coord(monkeypatch)
        coord2._wallet_short_to_id = {"aabbccddeeff": wallet_a}
        mock_repo2 = AsyncMock(spec=SQLAlchemyRepository)
        mock_repo2.get_venue_events_after = AsyncMock(
            return_value=[_make_venue_event(shard_key=shard) | {"wallet_public_id": wallet_b}]
        )
        mock_repo2.get_instrument_public_id_by_symbol = AsyncMock(return_value="inst-pid")
        mock_repo2.get_accruals = AsyncMock(return_value=[])
        mock_repo2.shard_has_any_accruals = AsyncMock(return_value=False)
        _set_sqlalchemy_repo(coord2, mock_repo2)
        checkpoint = _make_checkpoint(shard_key=shard)
        checkpoint["wallet_public_id"] = wallet_a
        await coord2._recover_checkpoint_row(checkpoint, datetime.now(UTC))
        assert coord2._recovery_certification_failed is True

        coord3 = _make_coord(monkeypatch)
        coord3._wallet_short_to_id = {}
        mock_repo3 = AsyncMock(spec=SQLAlchemyRepository)
        mock_repo3.get_venue_events_after = AsyncMock(return_value=[])
        mock_repo3.get_instrument_public_id_by_symbol = AsyncMock(return_value="inst-pid")
        mock_repo3.get_accruals = AsyncMock(return_value=[])
        mock_repo3.shard_has_any_accruals = AsyncMock(return_value=False)
        _set_sqlalchemy_repo(coord3, mock_repo3)
        mismatched = _make_checkpoint(shard_key=shard)
        mismatched["wallet_public_id"] = "22222222-2222-7222-8222-ffffffffffff"
        await coord3._recover_checkpoint_row(mismatched, datetime.now(UTC))
        assert coord3._recovery_certification_failed is True

    @pytest.mark.asyncio
    async def test_gap_replay_and_rebuild_reject_foreign_wallet_history(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Full-history replays never mix suffix-twin wallets.

        Given: (1) a checkpoint gap correction whose full-history events
            include a foreign wallet's fill, (2) a venue-only rebuild
            whose events span two wallets, and (3) an active-order
            engine reuse across wallets,
        When: each path runs,
        Then: every case trips the global flag without mutating state.
        """
        wallet_a = "00000000-0000-7000-8000-aabbccddeeff"
        wallet_b = "11111111-1111-7111-8111-aabbccddeeff"
        shard = "kraken.BTC-USD.live.waabbccddeeff"
        coord = _make_coord(monkeypatch)
        mock_repo = AsyncMock(spec=SQLAlchemyRepository)
        _set_sqlalchemy_repo(coord, mock_repo)
        mock_repo.shard_has_fill_gap = AsyncMock(return_value=True)
        mock_repo.shard_has_accruals = AsyncMock(return_value=False)
        mock_repo.get_venue_events_after = AsyncMock(
            return_value=[_make_venue_event(shard_key=shard) | {"wallet_public_id": wallet_b}]
        )
        certain = await coord._correct_checkpoint_fill_gap(
            shard, wallet_a, "kraken", "live", datetime.now(UTC)
        )
        assert certain is False
        assert coord._recovery_certification_failed is True

        coord2 = _make_coord(monkeypatch)
        coord2._wallet_short_to_id = {"aabbccddeeff": wallet_a}
        mock_repo2 = AsyncMock(spec=SQLAlchemyRepository)
        _set_sqlalchemy_repo(coord2, mock_repo2)
        mock_repo2.shard_has_fill_gap = AsyncMock(return_value=True)
        mock_repo2.shard_has_accruals = AsyncMock(return_value=False)
        mock_repo2.get_venue_events_after = AsyncMock(
            return_value=[
                _make_venue_event(event_id=1, shard_key=shard) | {"wallet_public_id": wallet_a},
                _make_venue_event(event_id=2, shard_key=shard) | {"wallet_public_id": wallet_b},
            ]
        )
        rebuilt = await coord2._rebuild_shard_if_gapped(shard, datetime.now(UTC))
        assert rebuilt is False
        assert coord2._recovery_certification_failed is True
        assert shard not in coord2.trade_service.known_shard_keys()

        coord3 = _make_coord(monkeypatch)
        incumbent = MagicMock()
        incumbent.wallet_public_id = wallet_a
        coord3.engines["BTC-USD@kraken-live"] = incumbent
        reused = await coord3._get_or_create_active_order_engine(
            engine_key="BTC-USD@kraken-live",
            db_order=cast(Any, {"instrument": "BTC-USD", "exchange": "kraken"}),
            wallet_public_id=wallet_b,
            operator_public_id="",
        )
        assert reused is None
        assert coord3._recovery_certification_failed is True
