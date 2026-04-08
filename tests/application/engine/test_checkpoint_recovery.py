"""Tests for Phase 3b: checkpoint recovery in TraderCoordinator."""

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
from snapper.application.trade.trade_service import TradeService
from snapper.data.repository import SQLAlchemyRepository
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
    coord.repository = mock_repo


class TestCheckpointRecovery:
    """Phase 3b: checkpoint recovery restores TradeService, engine, and portfolio."""

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
            row written before this plan shipped),
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
            seen_exec_ids={"t2", "t1"},
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
        assert engine.seen_exec_ids == set()

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
            "seen_exec_ids": set(),
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
        snap_at_5["seen_exec_ids"] = set(mid_shard.seen_exec_ids)

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
        """Phase 0c.4: unknown wallet_short in checkpoint logs a warning.

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
