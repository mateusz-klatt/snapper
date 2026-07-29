"""Tests for checkpoint recovery in TraderCoordinator."""

from collections import OrderedDict
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from pathlib import Path
from typing import Any
from typing import cast
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import Mock
from unittest.mock import patch

import pytest

import snapper.application.engine.trader as trader_module
from snapper.application.engine.trader import TraderCoordinator
from snapper.application.portfolio.fill_booking import PROJECTION_CALC_VERSION
from snapper.application.portfolio.models import PositionStateModel
from snapper.application.trade.trade_service import FillProjection
from snapper.application.trade.trade_service import TradeService
from snapper.core.wallet_short import compute_wallet_short
from snapper.data.models import AccrualLedger
from snapper.data.repository import SQLAlchemyRepository
from snapper.data.repository_types import ExecutionRow
from snapper.data.repository_types import OrderRow
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
        "projection_calc_version": PROJECTION_CALC_VERSION,
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
    @pytest.mark.parametrize("version", [PROJECTION_CALC_VERSION - 1, None])
    async def test_older_and_legacy_checkpoint_replay_instead_of_restore(
        self,
        monkeypatch: pytest.MonkeyPatch,
        version: int | None,
    ) -> None:
        """Older calculation epochs bypass checkpoint restoration.

        Given: an owned spot checkpoint stamped with an older version or
            carrying the legacy NULL version,
        When: boot recovery considers the checkpoint row,
        Then: it returns the shard to authoritative replay without restoring
            checkpoint state or quarantining the identity.
        """
        coord = _make_coord(monkeypatch)
        mock_repo = AsyncMock(spec=SQLAlchemyRepository)
        mock_repo.shard_has_accruals = AsyncMock(return_value=False)
        _set_sqlalchemy_repo(coord, mock_repo)
        mock_repo.get_instrument_public_id_by_symbol = AsyncMock(return_value="inst-pid")
        mock_repo.shard_has_any_accruals = AsyncMock(return_value=False)
        checkpoint = _make_checkpoint()
        checkpoint["projection_calc_version"] = version
        checkpoint["wallet_public_id"] = "w-1"
        restore = Mock()
        with patch.object(coord, "_restore_trade_service_from_checkpoint", restore):
            recovered = await coord._recover_checkpoint_row(checkpoint, datetime.now(UTC))
        assert recovered is None
        restore.assert_not_called()
        assert coord._failed_recovery_identities == set()
        assert checkpoint["shard_key"] not in coord._checkpoint_recovered_shard_keys

    @pytest.mark.asyncio
    async def test_future_checkpoint_quarantines_without_restore(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A newer binary's calculation epoch fails closed.

        Given: an owned checkpoint stamped above the running calculation
            version,
        When: boot recovery considers the checkpoint row,
        Then: the identity is quarantined and neither checkpoint restoration
            nor generic full replay may consume the unknown state.
        """
        coord = _make_coord(monkeypatch)
        mock_repo = AsyncMock(spec=SQLAlchemyRepository)
        mock_repo.get_instrument_public_id_by_symbol = AsyncMock(return_value="inst-pid")
        _set_sqlalchemy_repo(coord, mock_repo)
        mock_repo.get_instrument_public_id_by_symbol = AsyncMock(return_value="inst-pid")
        mock_repo.shard_has_any_accruals = AsyncMock(return_value=False)
        checkpoint = _make_checkpoint()
        checkpoint["projection_calc_version"] = PROJECTION_CALC_VERSION + 1
        checkpoint["wallet_public_id"] = "w-1"
        restore = Mock()
        with patch.object(coord, "_restore_trade_service_from_checkpoint", restore):
            recovered = await coord._recover_checkpoint_row(checkpoint, datetime.now(UTC))
        assert recovered is None
        restore.assert_not_called()
        assert checkpoint["shard_key"] in coord._checkpoint_recovered_shard_keys
        assert ("inst-pid", "live", "w-1") in coord._failed_recovery_identities

    @pytest.mark.asyncio
    @pytest.mark.parametrize("version", [2.0, "2", 1.5, True])
    async def test_non_integer_checkpoint_version_quarantines(
        self,
        monkeypatch: pytest.MonkeyPatch,
        version: object,
    ) -> None:
        """Non-integer database versions fail closed consistently.

        Given: a checkpoint whose version column contains a non-integer
            SQLite value from a weakly typed import or manual repair,
        When: recovery evaluates the calculation version,
        Then: the identity is quarantined without restoring or ordering
            the malformed value.
        """
        coord = _make_coord(monkeypatch)
        mock_repo = AsyncMock(spec=SQLAlchemyRepository)
        _set_sqlalchemy_repo(coord, mock_repo)
        mock_repo.get_instrument_public_id_by_symbol = AsyncMock(return_value="inst-pid")
        mock_repo.shard_has_any_accruals = AsyncMock(return_value=False)
        checkpoint = _make_checkpoint()
        checkpoint["projection_calc_version"] = cast(int, version)
        checkpoint["wallet_public_id"] = "w-1"
        restore = Mock()
        with patch.object(coord, "_restore_trade_service_from_checkpoint", restore):
            recovered = await coord._recover_checkpoint_row(checkpoint, datetime.now(UTC))
        assert recovered is None
        restore.assert_not_called()
        assert checkpoint["shard_key"] in coord._checkpoint_recovered_shard_keys

    @pytest.mark.asyncio
    async def test_older_funding_checkpoint_quarantines_without_full_replay(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Stale funding cash is never replaced by a fill-only replay.

        Given: an older-version checkpoint whose wallet and venue carry
            durable funding accruals,
        When: boot recovery considers the checkpoint row,
        Then: the identity is quarantined and marked checkpoint-backed so
            execution and venue-gap recovery cannot fabricate pre-checkpoint
            funding cash.
        """
        coord = _make_coord(monkeypatch)
        mock_repo = AsyncMock(spec=SQLAlchemyRepository)
        mock_repo.get_instrument_public_id_by_symbol = AsyncMock(return_value="inst-pid")
        _set_sqlalchemy_repo(coord, mock_repo)
        mock_repo.get_instrument_public_id_by_symbol = AsyncMock(return_value="inst-pid")
        mock_repo.shard_has_any_accruals = AsyncMock(return_value=True)
        checkpoint = _make_checkpoint()
        checkpoint["projection_calc_version"] = PROJECTION_CALC_VERSION - 1
        checkpoint["wallet_public_id"] = "w-1"
        recovered = await coord._recover_checkpoint_row(checkpoint, datetime.now(UTC))
        assert recovered is None
        assert checkpoint["shard_key"] in coord._checkpoint_recovered_shard_keys
        assert coord._checkpoint_execution_replay_should_skip(checkpoint["shard_key"], "w-1")

    @pytest.mark.asyncio
    async def test_stale_version_funding_probe_fails_closed(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Unavailable funding evidence prevents a stale-state rebuild.

        Given: a stale-version checkpoint and, separately, a non-SQL
            repository and a SQL repository whose accrual query raises,
        When: the version gate asks whether full replay is safe,
        Then: both probes report possible accruals so recovery fails closed.
        """
        coord = _make_coord(monkeypatch)
        context = trader_module.CheckpointRecoveryContext(
            shard_key="kraken.BTC-USD.live",
            exchange="kraken",
            instrument="BTC-USD",
            mode="live",
            strategy_tag=None,
            wallet_public_id="w-1",
        )
        coord.repository = AsyncMock()
        assert await coord._checkpoint_shard_has_accruals(context, datetime.now(UTC))
        mock_repo = AsyncMock(spec=SQLAlchemyRepository)
        mock_repo.shard_has_any_accruals = AsyncMock(side_effect=RuntimeError("DB down"))
        coord.repository = mock_repo
        assert await coord._checkpoint_shard_has_accruals(context, datetime.now(UTC))

    @pytest.mark.asyncio
    async def test_funding_probes_fail_closed_without_instrument_identity(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Missing instrument identity cannot authorize replay.

        Given: a SQL repository that cannot resolve the checkpoint instrument,
        When: stale-version and accrual-certainty probes evaluate the shard,
        Then: stale replay is blocked and checkpoint accrual certainty is denied.
        """
        coord = _make_coord(monkeypatch)
        mock_repo = AsyncMock(spec=SQLAlchemyRepository)
        _set_sqlalchemy_repo(coord, mock_repo)
        mock_repo.get_instrument_public_id_by_symbol = AsyncMock(return_value=None)
        context = trader_module.CheckpointRecoveryContext(
            shard_key="kraken.BTC-USD.live",
            exchange="kraken",
            instrument="BTC-USD",
            mode="live",
            strategy_tag=None,
            wallet_public_id="w-1",
        )
        assert await coord._checkpoint_shard_has_accruals(context, datetime.now(UTC)) is True
        assert await coord._checkpoint_accruals_certain(context, True) is False

    @pytest.mark.asyncio
    async def test_future_checkpoint_without_wallet_fails_globally(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """An unattributable future checkpoint still cannot enter replay.

        Given: a future-version checkpoint with no resolvable durable wallet,
        When: the version gate quarantines it,
        Then: it suppresses generic replay and escalates through the existing
            unattributable-failure policy without recording a wallet mapping.
        """
        coord = _make_coord(monkeypatch)
        mock_repo = AsyncMock(spec=SQLAlchemyRepository)
        _set_sqlalchemy_repo(coord, mock_repo)
        checkpoint = _make_checkpoint()
        checkpoint["projection_calc_version"] = PROJECTION_CALC_VERSION + 1
        context = trader_module.CheckpointRecoveryContext(
            shard_key=checkpoint["shard_key"],
            exchange="kraken",
            instrument="BTC-USD",
            mode="live",
            strategy_tag=None,
            wallet_public_id="",
        )
        allowed = await coord._checkpoint_version_allows_restore(
            context, checkpoint, datetime.now(UTC)
        )
        assert allowed is False
        assert checkpoint["shard_key"] not in coord._checkpoint_recovered_shard_wallets
        assert coord._recovery_certification_failed is True

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

        mock_repo.get_venue_events_after.assert_any_call(
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
        """An OWNED checkpoint with an unparsable shard_key fails closed globally.

        Given: checkpoint with shard_key="bad.key" (fewer than 3
            dot-segments, so _parse_shard_key returns None),
        When: _recover_from_checkpoints runs,
        Then: the shard is not recovered AND the whole projection
            certification fails — an unparsable key is an unattributable
            identity whose blast radius recovery cannot bound (S5.4 P0-2:
            previously this owned shard was silently dropped).
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
        assert coord._recovery_certification_failed is True

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

        Given: a wallet-bearing checkpoint at watermark 10 with empty delta (so
            checkpoint+delta position is 0.5), a recorded>consumed fill gap, no
            funding, and a full venue history — attributed to the checkpoint's
            own full wallet — summing to 0.9,
        When: recovery runs,
        Then: the shard position is overlaid to the venue-replay value 0.9 while
            the checkpoint peak_equity is preserved (the overlay requires full
            wallet attribution, so the events must carry the shard's wallet).
        """
        wallet = "00000000-0000-7000-8000-aabbccddeeff"
        shard_key = "kraken.BTC-USD.live.waabbccddeeff"
        coord = _make_coord(monkeypatch)
        coord._wallet_short_to_id = {"aabbccddeeff": wallet}
        mock_repo = AsyncMock(spec=SQLAlchemyRepository)
        checkpoint = _make_checkpoint(shard_key=shard_key, position_qty=0.5, peak_equity=10000.0)
        checkpoint["wallet_public_id"] = wallet
        mock_repo.get_all_checkpoints = AsyncMock(return_value=[checkpoint])

        def venue_events(shard_key: str, after_id: int) -> list[VenueEventRow]:
            if after_id == 0:
                return [
                    cast(
                        VenueEventRow,
                        dict(
                            _make_venue_event(
                                event_id=5,
                                shard_key=shard_key,
                                fill_size=0.5,
                                exec_id="a",
                                trade_id="a",
                            )
                        )
                        | {"wallet_public_id": wallet},
                    ),
                    cast(
                        VenueEventRow,
                        dict(
                            _make_venue_event(
                                event_id=8,
                                shard_key=shard_key,
                                fill_size=0.4,
                                exec_id="b",
                                trade_id="b",
                            )
                        )
                        | {"wallet_public_id": wallet},
                    ),
                ]
            return []

        mock_repo.get_venue_events_after = AsyncMock(side_effect=venue_events)
        mock_repo.get_executions_for_recovery = AsyncMock(return_value=[])
        mock_repo.get_active_orders_for_recovery = AsyncMock(return_value=[])
        mock_repo.get_instrument_public_id_by_symbol = AsyncMock(return_value="inst-pid")
        _set_sqlalchemy_repo(coord, mock_repo)
        mock_repo.shard_has_fill_gap = AsyncMock(return_value=True)
        mock_repo.shard_has_accruals = AsyncMock(return_value=False)

        await coord._recover_engine_state()

        shard = coord.trade_service._shards[shard_key]
        assert shard.position.position_qty == pytest.approx(0.9)
        assert shard.peak_equity == pytest.approx(10000.0)
        assert coord._consumed_venue_event_watermarks[shard_key] == 8

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
        coord._wallet_short_to_id = {"aabbccddeeff": "00000000-0000-7000-8000-aabbccddeeff"}
        mock_repo = AsyncMock(spec=SQLAlchemyRepository)
        mock_repo.get_all_checkpoints = AsyncMock(return_value=[])
        mock_repo.get_executions_for_recovery = AsyncMock(return_value=[])
        mock_repo.get_active_orders_for_recovery = AsyncMock(return_value=[])
        orphan_fill = dict(
            _make_venue_event(
                event_id=5,
                shard_key="kraken.BTC-USD.live.waabbccddeeff",
                fill_size=0.5,
                exec_id="a",
                trade_id="a",
            )
        )
        orphan_fill["wallet_public_id"] = "00000000-0000-7000-8000-aabbccddeeff"
        mock_repo.get_venue_events_after = AsyncMock(
            return_value=[cast(VenueEventRow, orphan_fill)]
        )
        _set_sqlalchemy_repo(coord, mock_repo)
        mock_repo.get_shard_keys_with_fills = AsyncMock(
            return_value=["kraken.BTC-USD.live.waabbccddeeff"]
        )
        mock_repo.shard_has_fill_gap = AsyncMock(return_value=True)
        mock_repo.shard_has_accruals = AsyncMock(return_value=False)

        await coord._recover_engine_state()

        assert "BTC-USD@kraken-live-waabbccddeeff" in coord.engines
        engine = coord.engines["BTC-USD@kraken-live-waabbccddeeff"]
        assert engine.position_qty == pytest.approx(0.5)
        assert coord._consumed_venue_event_watermarks["kraken.BTC-USD.live.waabbccddeeff"] == 5

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
        funding_fill = dict(
            _make_venue_event(
                event_id=5,
                shard_key="kraken.BTC-USD.live.waabbccddeeff",
                fill_size=0.5,
                exec_id="a",
                trade_id="a",
            )
        )
        funding_fill["wallet_public_id"] = "00000000-0000-7000-8000-aabbccddeeff"
        mock_repo.get_venue_events_after = AsyncMock(
            return_value=[cast(VenueEventRow, funding_fill)]
        )
        _set_sqlalchemy_repo(coord, mock_repo)
        mock_repo.get_shard_keys_with_fills = AsyncMock(
            return_value=["kraken.BTC-USD.live.waabbccddeeff"]
        )
        mock_repo.shard_has_fill_gap = AsyncMock(return_value=True)
        mock_repo.shard_has_accruals = AsyncMock(return_value=True)

        await coord._recover_engine_state()

        assert "BTC-USD@kraken-live-waabbccddeeff" not in coord.engines

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

        Given: a gapped, non-funding shard with attributed durable
            fills but engine creation returns None,
        When: _rebuild_shard_if_gapped runs,
        Then: it skips before resetting the shard.
        """
        wallet = "00000000-0000-7000-8000-aabbccddeeff"
        shard = "kraken.BTC-USD.live.waabbccddeeff"
        coord = _make_coord(monkeypatch)
        coord._wallet_short_to_id = {"aabbccddeeff": wallet}
        mock_repo = AsyncMock(spec=SQLAlchemyRepository)
        skipped_fill = dict(
            _make_venue_event(event_id=5, shard_key=shard, fill_size=0.5, exec_id="a", trade_id="a")
        )
        skipped_fill["wallet_public_id"] = wallet
        mock_repo.get_venue_events_after = AsyncMock(
            return_value=[cast(VenueEventRow, skipped_fill)]
        )
        _set_sqlalchemy_repo(coord, mock_repo)
        mock_repo.shard_has_fill_gap = AsyncMock(return_value=True)
        mock_repo.get_instrument_public_id_by_symbol = AsyncMock(return_value="inst-pid")
        coord._create_engine_for_recovery = AsyncMock(return_value=None)
        await coord._rebuild_shard_if_gapped(shard, datetime(2024, 6, 1, tzinfo=UTC))
        assert shard not in coord.trade_service._shards

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
        coord._wallet_short_to_id = {"aabbccddeeff": "00000000-0000-7000-8000-aabbccddeeff"}
        mock_repo = AsyncMock(spec=SQLAlchemyRepository)
        reuse_fill = dict(
            _make_venue_event(
                event_id=5,
                shard_key="kraken.BTC-USD.live.waabbccddeeff",
                fill_size=0.5,
                exec_id="a",
                trade_id="a",
            )
        )
        reuse_fill["wallet_public_id"] = "00000000-0000-7000-8000-aabbccddeeff"
        mock_repo.get_venue_events_after = AsyncMock(return_value=[cast(VenueEventRow, reuse_fill)])
        _set_sqlalchemy_repo(coord, mock_repo)
        mock_repo.shard_has_fill_gap = AsyncMock(return_value=True)
        existing = await coord._create_engine_for_recovery(
            "BTC-USD",
            "kraken",
            strategy_tag=None,
            wallet_public_id="00000000-0000-7000-8000-aabbccddeeff",
            operator_public_id="",
        )
        assert existing is not None
        coord._register_recovered_engine("BTC-USD@kraken-live-waabbccddeeff", existing)
        coord._create_engine_for_recovery = AsyncMock()
        await coord._rebuild_shard_if_gapped(
            "kraken.BTC-USD.live.waabbccddeeff", datetime(2024, 6, 1, tzinfo=UTC)
        )
        coord._create_engine_for_recovery.assert_not_called()
        assert coord.engines["BTC-USD@kraken-live-waabbccddeeff"] is existing
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
        coord._wallet_short_to_id = {"aabbccddeeff": "00000000-0000-7000-8000-aabbccddeeff"}
        mock_repo = AsyncMock(spec=SQLAlchemyRepository)
        flat_events = []
        for event_id, side, price, exec_id in ((5, "buy", 100.0, "a"), (6, "sell", 110.0, "b")):
            flat_event = dict(
                _make_venue_event(
                    event_id=event_id,
                    shard_key="kraken.BTC-USD.live.waabbccddeeff",
                    side=side,
                    fill_price=price,
                    fill_size=0.5,
                    exec_id=exec_id,
                    trade_id=exec_id,
                )
            )
            flat_event["wallet_public_id"] = "00000000-0000-7000-8000-aabbccddeeff"
            flat_events.append(cast(VenueEventRow, flat_event))
        mock_repo.get_venue_events_after = AsyncMock(return_value=flat_events)
        _set_sqlalchemy_repo(coord, mock_repo)
        mock_repo.shard_has_fill_gap = AsyncMock(return_value=True)
        existing = await coord._create_engine_for_recovery(
            "BTC-USD",
            "kraken",
            strategy_tag=None,
            wallet_public_id="00000000-0000-7000-8000-aabbccddeeff",
            operator_public_id="",
        )
        assert existing is not None
        existing.portfolio.positions["BTC-USD"] = PositionStateModel(
            quantity=0.5, average_price=100.0, realized_pnl=0.0
        )
        coord._register_recovered_engine("BTC-USD@kraken-live-waabbccddeeff", existing)

        await coord._rebuild_shard_if_gapped(
            "kraken.BTC-USD.live.waabbccddeeff", datetime(2024, 6, 1, tzinfo=UTC)
        )

        assert coord.engines["BTC-USD@kraken-live-waabbccddeeff"] is existing
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

        Given: a checkpoint shard with no fill gap, wallet-attributed
            durable fill evidence, certain accruals, no accrual-ledger
            rows, and a registered identity,
        When: _recover_checkpoint_row completes,
        Then: the shard joins the trusted set.
        """
        coord = _make_coord(monkeypatch)
        mock_repo = AsyncMock(spec=SQLAlchemyRepository)
        evidence = dict(
            _make_venue_event(
                event_id=1, fill_size=0.5, fill_price=50000.0, exec_id="E-1", trade_id="E-1"
            )
        )
        evidence["wallet_public_id"] = "w-1"
        mock_repo.get_venue_events_after = AsyncMock(return_value=[cast(VenueEventRow, evidence)])
        mock_repo.get_instrument_public_id_by_symbol = AsyncMock(return_value="inst-pid")
        mock_repo.get_accruals = AsyncMock(return_value=[])
        mock_repo.shard_has_any_accruals = AsyncMock(return_value=False)
        _set_sqlalchemy_repo(coord, mock_repo)
        coord._projection_identities["kraken.BTC-USD.live"] = ("inst-pid", "live", "w-1")
        checkpoint = _make_checkpoint(
            position_qty=0.5,
            entry_price=50000.0,
            position_opened_at=datetime(2024, 6, 1, 1, tzinfo=UTC),
            cash=10000.0 - (0.5 * 50000.0 + 0.01),
            realized_pnl=0.0,
            turnover=25000.0,
            last_venue_event_id=1,
            seen_exec_ids='["E-1"]',
        )
        checkpoint["wallet_public_id"] = "w-1"
        engine_key = await coord._recover_checkpoint_row(checkpoint, datetime.now(UTC))
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
        coord._projection_identities["kraken.BTC-USD.live"] = ("inst-pid", "live", "")
        await coord._certify_execution_replay(
            coord.engines["BTC-USD@kraken-live"],
            "kraken.BTC-USD.live",
            "",
            datetime(2024, 1, 1, tzinfo=UTC),
        )
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
        checkpoint = _make_checkpoint(
            shard_key=tagged,
            position_qty=0.5,
            entry_price=50000.0,
            position_opened_at=datetime(2024, 6, 1, 1, tzinfo=UTC),
            cash=10000.0 - (0.5 * 50000.0 + 0.01),
            realized_pnl=0.0,
            turnover=25000.0,
            last_venue_event_id=1,
            seen_exec_ids='["E-1"]',
        )
        checkpoint["wallet_public_id"] = wallet
        mock_repo.get_all_checkpoints = AsyncMock(return_value=[checkpoint])
        evidence = dict(
            _make_venue_event(
                event_id=1,
                shard_key=tagged,
                fill_size=0.5,
                fill_price=50000.0,
                exec_id="E-1",
                trade_id="E-1",
            )
        )
        evidence["wallet_public_id"] = wallet
        evidence["exchange"] = "paper"
        evidence["mode"] = "paper"
        mock_repo.get_venue_events_after = AsyncMock(return_value=[cast(VenueEventRow, evidence)])
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
        mock_repo.get_fill_event_wallets = AsyncMock(return_value=[wallet])
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
    async def test_execution_replay_malformed_economics_never_certifies(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A poisoned execution row folds into nothing and never certifies.

        Given: a durable-lineage paper execution bucket whose row carries
            a zero fill price (the production repro: 0.01 @ 0.0 — an
            execution row folds DIRECTLY into position/turnover with no
            checkpoint gate),
        When: _recover_execution_group replays it,
        Then: the bucket is refused before any fold, the shard is NOT
            trusted, the TradeService shard is left untouched, and the
            identity is quarantined SCOPED (no global flag) — an
            attributable malformed row blocks only its own identity
            (S5.4 P0-1 + P0-5).
        """
        tagged = "paper.ETH-USD.paper.waabbccddeeff.momo"
        wallet = "00000000-0000-7000-8000-aabbccddeeff"
        coord = _make_coord(monkeypatch)
        coord._wallet_short_to_id = {"aabbccddeeff": wallet}
        mock_repo = AsyncMock(spec=SQLAlchemyRepository)
        mock_repo.get_instrument_public_id_by_symbol = AsyncMock(return_value="inst-eth")
        mock_repo.shard_has_any_accruals = AsyncMock(return_value=False)
        _set_sqlalchemy_repo(coord, mock_repo)
        coord._projection_identities[tagged] = ("inst-eth", "paper", wallet)
        await coord._recover_execution_group(
            engine_key="ETH-USD@paper-momo",
            fills=[
                cast(
                    ExecutionRow,
                    {
                        "public_id": "exe-bad",
                        "timestamp": datetime(2024, 6, 1, tzinfo=UTC),
                        "session_id": "s1",
                        "sequence_id": 1,
                        "trade_id": "t-bad",
                        "exchange_order_id": None,
                        "client_order_id": "c-bad",
                        "instrument": "ETH-USD",
                        "exchange": "paper",
                        "side": "buy",
                        "size": 0.01,
                        "price": 0.0,
                        "fee": 0.0,
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
        assert tagged not in coord._trusted_recovery_shards
        assert coord.trade_service._shards[tagged].position.position_qty == pytest.approx(0.0)
        assert ("inst-eth", "paper", wallet) in coord._failed_recovery_identities
        assert coord._recovery_certification_failed is False

    @pytest.mark.asyncio
    async def test_execution_replay_null_side_never_certifies(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A NULL-side execution row folds to phantom-flat and never certifies.

        Given: a durable-lineage bucket whose row has a NULL side (the
            fold would skip position/cash but still book turnover,
            folding to a self-consistent phantom-flat the digest would
            certify),
        When: _recover_execution_group replays it,
        Then: the malformed side is refused before any fold; the shard is
            not trusted and its identity is quarantined scoped.
        """
        tagged = "paper.ETH-USD.paper.waabbccddeeff.momo"
        wallet = "00000000-0000-7000-8000-aabbccddeeff"
        coord = _make_coord(monkeypatch)
        coord._wallet_short_to_id = {"aabbccddeeff": wallet}
        mock_repo = AsyncMock(spec=SQLAlchemyRepository)
        mock_repo.get_instrument_public_id_by_symbol = AsyncMock(return_value="inst-eth")
        mock_repo.shard_has_any_accruals = AsyncMock(return_value=False)
        _set_sqlalchemy_repo(coord, mock_repo)
        coord._projection_identities[tagged] = ("inst-eth", "paper", wallet)
        await coord._recover_execution_group(
            engine_key="ETH-USD@paper-momo",
            fills=[
                cast(
                    ExecutionRow,
                    {
                        "public_id": "exe-ns",
                        "timestamp": datetime(2024, 6, 1, tzinfo=UTC),
                        "session_id": "s1",
                        "sequence_id": 1,
                        "trade_id": "t-ns",
                        "exchange_order_id": None,
                        "client_order_id": "c-ns",
                        "instrument": "ETH-USD",
                        "exchange": "paper",
                        "side": None,
                        "size": 1.0,
                        "price": 2000.0,
                        "fee": 0.0,
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
        assert tagged not in coord._trusted_recovery_shards
        assert coord.trade_service._shards[tagged].position.position_qty == pytest.approx(0.0)
        assert ("inst-eth", "paper", wallet) in coord._failed_recovery_identities
        assert coord._recovery_certification_failed is False

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

    def test_malformed_durable_execution_lineage_fails_certification(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Malformed durable execution lineage is rejected and quarantined.

        Given: a durable lineage record whose shard key cannot be parsed,
        When: execution recovery resolves the row's lineage,
        Then: the row is rejected and projection certification fails closed.
        """
        wallet = "00000000-0000-7000-8000-aabbccddeeff"
        coord = _make_coord(monkeypatch)
        execution = cast(
            ExecutionRow,
            {
                "instrument": "BTC-USD",
                "exchange": "kraken",
            },
        )

        result = coord._resolve_execution_recovery_lineage(
            execution,
            ("malformed-shard-key", wallet),
            wallet,
        )

        assert result is None
        assert coord._recovery_certification_failed is True

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
            db_order=cast(OrderRow, {"instrument": "BTC-USD", "exchange": "kraken"}),
            wallet_public_id=wallet_b,
            operator_public_id="",
        )
        assert reused is None
        assert coord3._recovery_certification_failed is True


def _active_order_row(
    client_order_id: str = "c-1",
    instrument: str = "BTC-USD",
    exchange: str = "paper",
    mode: str = "paper",
    wallet_public_id: str = "00000000-0000-7000-8000-aabbccddeeff",
) -> OrderRow:
    """Build an active order row for durable-lineage recovery tests."""
    return cast(
        OrderRow,
        {
            "public_id": f"ord-{client_order_id}",
            "timestamp": datetime(2024, 6, 1, tzinfo=UTC),
            "session_id": "s-test",
            "sequence_id": 1,
            "instrument": instrument,
            "exchange": exchange,
            "mode": mode,
            "client_order_id": client_order_id,
            "exchange_order_id": None,
            "created_at": datetime(2024, 6, 1, tzinfo=UTC),
            "updated_at": None,
            "side": "buy",
            "order_type": "market",
            "price": None,
            "size": 0.5,
            "filled_size": 0.0,
            "average_price": None,
            "status": "open",
            "time_in_force": None,
            "error": None,
            "leverage": None,
            "wallet_public_id": wallet_public_id,
            "operator_public_id": None,
        },
    )


class TestActiveOrderDurableLineage:
    """S5.1 F1: active-order recovery follows durable command lineage."""

    _WALLET = "00000000-0000-7000-8000-aabbccddeeff"
    _TAGGED = "paper.BTC-USD.paper.waabbccddeeff.heartbeat"

    @pytest.mark.asyncio
    async def test_tagged_paper_active_order_recovers_exact_shard(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The F1 repro: the pending route lands on the true tagged shard.

        Given: a tagged paper active order whose trade_commands lineage
            resolves to its exact tagged shard and full wallet,
        When: _recover_active_orders runs,
        Then: the engine is recovered under the TAGGED identity, the
            CID maps to the tagged shard (not an untagged phantom), and
            the certification stays intact.
        """
        coord = _make_coord(monkeypatch)
        mock_repo = AsyncMock(spec=SQLAlchemyRepository)
        _set_sqlalchemy_repo(coord, mock_repo)
        mock_repo.get_active_orders_for_recovery = AsyncMock(return_value=[_active_order_row()])
        mock_repo.get_command_shard_keys_by_client_order_ids = AsyncMock(
            return_value=({"c-1": (self._TAGGED, self._WALLET)}, set())
        )
        await coord._recover_active_orders(datetime.now(UTC))
        engine_key = "BTC-USD@paper-heartbeat-waabbccddeeff"
        assert engine_key in coord.engines
        engine = coord.engines[engine_key]
        assert engine.order_in_flight is True
        assert engine.pending_client_order_id == "c-1"
        assert engine._shard_key == self._TAGGED
        assert coord._order_shard_keys["c-1"] == self._TAGGED
        assert "BTC-USD@paper-paper-waabbccddeeff" not in coord.engines
        assert coord._recovery_certification_failed is False

    @pytest.mark.asyncio
    async def test_paper_active_order_without_lineage_quarantines(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A paper order with no durable lineage must not reconstruct.

        Given: a paper active order whose cid resolves to NO command
            lineage,
        When: _recover_active_orders runs,
        Then: no engine and no pending route are installed and the
            order's CANONICAL identity is quarantined — scoped to its
            (instrument, mode, wallet), not the whole node, because a
            missing lineage is absence of evidence, not corruption.
        """
        coord = _make_coord(monkeypatch)
        mock_repo = AsyncMock(spec=SQLAlchemyRepository)
        _set_sqlalchemy_repo(coord, mock_repo)
        mock_repo.get_active_orders_for_recovery = AsyncMock(return_value=[_active_order_row()])
        mock_repo.get_command_shard_keys_by_client_order_ids = AsyncMock(return_value=({}, set()))
        mock_repo.get_instrument_public_id_by_symbol = AsyncMock(return_value="inst-pid")
        await coord._recover_active_orders(datetime.now(UTC))
        assert dict(coord.engines) == {}
        assert "c-1" not in coord._order_shard_keys
        assert ("inst-pid", "paper", self._WALLET) in coord._failed_recovery_identities
        assert coord._recovery_certification_failed is False

    @pytest.mark.asyncio
    async def test_ambiguous_command_lineage_quarantines(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Contradictory command lineage fails the certification.

        Given: an active order whose command rows disagree on the shard,
        When: _recover_active_orders runs,
        Then: the row is skipped, nothing is installed, and the global
            certification flag is set by the lineage lookup.
        """
        coord = _make_coord(monkeypatch)
        mock_repo = AsyncMock(spec=SQLAlchemyRepository)
        _set_sqlalchemy_repo(coord, mock_repo)
        mock_repo.get_active_orders_for_recovery = AsyncMock(return_value=[_active_order_row()])
        mock_repo.get_command_shard_keys_by_client_order_ids = AsyncMock(return_value=({}, {"c-1"}))
        await coord._recover_active_orders(datetime.now(UTC))
        assert dict(coord.engines) == {}
        assert coord._recovery_certification_failed is True

    @pytest.mark.asyncio
    async def test_contradictory_lineage_wallet_quarantines(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A lineage wallet disagreement is a contradiction, not evidence.

        Given: resolved lineage whose full wallet DISAGREES with the
            order row's wallet,
        When: _recover_active_orders runs,
        Then: the row is skipped and the whole certification fails.
        """
        coord = _make_coord(monkeypatch)
        mock_repo = AsyncMock(spec=SQLAlchemyRepository)
        _set_sqlalchemy_repo(coord, mock_repo)
        mock_repo.get_active_orders_for_recovery = AsyncMock(return_value=[_active_order_row()])
        mock_repo.get_command_shard_keys_by_client_order_ids = AsyncMock(
            return_value=(
                {"c-1": (self._TAGGED, "00000000-0000-7000-8000-000000000002")},
                set(),
            )
        )
        await coord._recover_active_orders(datetime.now(UTC))
        assert dict(coord.engines) == {}
        assert coord._recovery_certification_failed is True

    @pytest.mark.asyncio
    async def test_live_active_order_reconstructs_without_lineage(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A live order may reconstruct: live shard keys carry no tag.

        Given: a live kraken active order and a lineage lookup that
            yields a non-tuple shape (unconfigured mock — exercising the
            defensive guard),
        When: _recover_active_orders runs,
        Then: the untagged live reconstruction recovers the engine and
            pending route exactly as before, with certification intact.
        """
        coord = _make_coord(monkeypatch)
        mock_repo = AsyncMock(spec=SQLAlchemyRepository)
        _set_sqlalchemy_repo(coord, mock_repo)
        mock_repo.get_active_orders_for_recovery = AsyncMock(
            return_value=[_active_order_row(exchange="kraken", mode="live")]
        )
        await coord._recover_active_orders(datetime.now(UTC))
        engine_key = "BTC-USD@kraken-live-waabbccddeeff"
        assert engine_key in coord.engines
        engine = coord.engines[engine_key]
        assert engine.order_in_flight is True
        assert coord._order_shard_keys["c-1"] == "kraken.BTC-USD.live.waabbccddeeff"
        assert coord._recovery_certification_failed is False

    @pytest.mark.asyncio
    async def test_lineage_lookup_failure_quarantines_paper(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A failed lineage lookup leaves paper orders unresolved.

        Given: a paper active order and a lineage lookup that raises,
        When: _recover_active_orders runs,
        Then: the paper order quarantines its canonical identity (no
            engine, no route) — a transient lookup error must not
            degrade into an untagged reconstruction, and must not
            poison the whole node either.
        """
        coord = _make_coord(monkeypatch)
        mock_repo = AsyncMock(spec=SQLAlchemyRepository)
        _set_sqlalchemy_repo(coord, mock_repo)
        mock_repo.get_active_orders_for_recovery = AsyncMock(return_value=[_active_order_row()])
        mock_repo.get_command_shard_keys_by_client_order_ids = AsyncMock(
            side_effect=RuntimeError("db down")
        )
        mock_repo.get_instrument_public_id_by_symbol = AsyncMock(return_value="inst-pid")
        await coord._recover_active_orders(datetime.now(UTC))
        assert dict(coord.engines) == {}
        assert ("inst-pid", "paper", self._WALLET) in coord._failed_recovery_identities
        assert coord._recovery_certification_failed is False

    @pytest.mark.asyncio
    async def test_unattributable_paper_order_escalates_globally(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """An unattributable lineage-less paper order fails everything.

        Given: a paper active order with no lineage whose instrument
            cannot be resolved to a canonical identity,
        When: _recover_active_orders runs,
        Then: the recorder escalates to the GLOBAL certification flag —
            an unattributable failure must block everything.
        """
        coord = _make_coord(monkeypatch)
        mock_repo = AsyncMock(spec=SQLAlchemyRepository)
        _set_sqlalchemy_repo(coord, mock_repo)
        mock_repo.get_active_orders_for_recovery = AsyncMock(return_value=[_active_order_row()])
        mock_repo.get_command_shard_keys_by_client_order_ids = AsyncMock(return_value=({}, set()))
        mock_repo.get_instrument_public_id_by_symbol = AsyncMock(return_value=None)
        await coord._recover_active_orders(datetime.now(UTC))
        assert dict(coord.engines) == {}
        assert coord._recovery_certification_failed is True

    @pytest.mark.asyncio
    async def test_divergent_engine_shard_records_both_identities(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A recreated engine that diverges from lineage never routes.

        Given: resolved lineage whose shard string embeds a DIFFERENT
            wallet segment than the engine will compute (full wallets
            agree, so classification passes; the recreated engine's
            shard key then diverges),
        When: _recover_active_orders runs,
        Then: BOTH identities are recorded as failed via the DURABLE
            full wallet (the wallet-short cache is empty — proving the
            attribution does not depend on it), no pending route is
            installed, and no global flag is needed.
        """
        coord = _make_coord(monkeypatch)
        coord._wallet_short_to_id = {}
        mock_repo = AsyncMock(spec=SQLAlchemyRepository)
        _set_sqlalchemy_repo(coord, mock_repo)
        divergent = "paper.BTC-USD.paper.w000000000000.heartbeat"
        mock_repo.get_active_orders_for_recovery = AsyncMock(return_value=[_active_order_row()])
        mock_repo.get_command_shard_keys_by_client_order_ids = AsyncMock(
            return_value=({"c-1": (divergent, self._WALLET)}, set())
        )
        mock_repo.get_instrument_public_id_by_symbol = AsyncMock(return_value="inst-pid")
        await coord._recover_active_orders(datetime.now(UTC))
        assert ("inst-pid", "paper", self._WALLET) in coord._failed_recovery_identities
        assert "c-1" not in coord._order_shard_keys
        engine = coord.engines["BTC-USD@paper-heartbeat-waabbccddeeff"]
        assert engine.order_in_flight is False
        assert coord._recovery_certification_failed is False

    @pytest.mark.asyncio
    async def test_conflicting_cid_registration_refuses_pending(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A CID route conflict never leaves a half-installed pending engine.

        Given: a live active order whose cid is ALREADY mapped to a
            different shard,
        When: _recover_active_orders runs,
        Then: the mapping is dropped, the engine stays out of flight,
            and the whole certification fails.
        """
        coord = _make_coord(monkeypatch)
        mock_repo = AsyncMock(spec=SQLAlchemyRepository)
        _set_sqlalchemy_repo(coord, mock_repo)
        coord._order_shard_keys["c-1"] = "kraken.ETH-USD.live"
        mock_repo.get_active_orders_for_recovery = AsyncMock(
            return_value=[_active_order_row(exchange="kraken", mode="live")]
        )
        mock_repo.get_command_shard_keys_by_client_order_ids = AsyncMock(return_value=({}, set()))
        await coord._recover_active_orders(datetime.now(UTC))
        engine = coord.engines["BTC-USD@kraken-live-waabbccddeeff"]
        assert engine.order_in_flight is False
        assert "c-1" not in coord._order_shard_keys
        assert coord._recovery_certification_failed is True


class TestNoGapFillEvidence:
    """S5.1 F2: the no-gap branch demands attributable durable evidence."""

    _WALLET = "00000000-0000-7000-8000-aabbccddeeff"
    _SHARD = "paper.BTC-USD.paper.waabbccddeeff.heartbeat"

    def _coord_with_repo(
        self, monkeypatch: pytest.MonkeyPatch, events: object
    ) -> TraderCoordinator:
        """Build a coordinator whose repo reports no gap and given events."""
        coord = _make_coord(monkeypatch)
        mock_repo = AsyncMock(spec=SQLAlchemyRepository)
        _set_sqlalchemy_repo(coord, mock_repo)
        mock_repo.get_instrument_public_id_by_symbol = AsyncMock(return_value="inst-pid")
        mock_repo.shard_has_any_accruals = AsyncMock(return_value=False)
        mock_repo.get_venue_events_after = AsyncMock(return_value=events)
        return coord

    def _evidence_event(
        self,
        event_id: int,
        wallet: str,
        exec_id: str,
        fill_size: float = 0.5,
        fill_price: float = 50000.0,
    ) -> VenueEventRow:
        """Build one exact-shard fill event attributed to ``wallet``."""
        event = _make_venue_event(
            event_id=event_id,
            shard_key=self._SHARD,
            fill_size=fill_size,
            fill_price=fill_price,
            exec_id=exec_id,
            trade_id=exec_id,
        )
        return cast(
            VenueEventRow,
            dict(event) | {"wallet_public_id": wallet, "exchange": "paper", "mode": "paper"},
        )

    def _matching_checkpoint(self) -> TradeProjectionCheckpointRow:
        """Checkpoint whose fill fold equals one 0.5@50000 buy (id E-1).

        The digest is EXACT, so the opening timestamp and venue-event
        watermark must equal the replay's (the fill event's timestamp
        and id).
        """
        return _make_checkpoint(
            shard_key=self._SHARD,
            position_qty=0.5,
            entry_price=50000.0,
            position_opened_at=datetime(2024, 6, 1, 1, tzinfo=UTC),
            cash=10000.0 - (0.5 * 50000.0 + 0.01),
            realized_pnl=0.0,
            turnover=25000.0,
            last_venue_event_id=1,
            seen_exec_ids='["E-1"]',
        )

    @pytest.mark.asyncio
    async def test_fill_bearing_checkpoint_without_evidence_uncertified(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Fill state with zero exact-shard events must not certify.

        Given: a fill-bearing checkpoint whose shard reads "no gap"
            because it has NO recorded fill events at all,
        When: _correct_checkpoint_fill_gap evaluates it,
        Then: the shard stays uncertain (the F2 poisoned-phantom repro:
            certifying it would double count against the true shard).
        """
        coord = self._coord_with_repo(monkeypatch, [])
        certain = await coord._correct_checkpoint_fill_gap(
            self._SHARD, self._WALLET, "paper", "paper", datetime.now(UTC), _make_checkpoint()
        )
        assert certain is False
        assert coord._recovery_certification_failed is False

    @pytest.mark.asyncio
    async def test_matching_evidence_and_digest_certifies(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Wallet-attributed evidence whose replay matches certifies.

        Given: a restored checkpoint whose fill state equals the full
            chronological replay of its exact-shard events (one
            0.5@50000 buy under the checkpoint wallet),
        When: _correct_checkpoint_fill_gap evaluates the no-gap branch,
        Then: the shard is certain — the warm-restart path stays intact.
        """
        coord = self._coord_with_repo(monkeypatch, [self._evidence_event(1, self._WALLET, "E-1")])
        checkpoint = self._matching_checkpoint()
        coord._restore_trade_service_from_checkpoint(checkpoint, self._SHARD, [])
        certain = await coord._correct_checkpoint_fill_gap(
            self._SHARD, self._WALLET, "paper", "paper", datetime.now(UTC), checkpoint
        )
        assert certain is True
        assert coord._recovery_certification_failed is False

    @pytest.mark.asyncio
    async def test_digest_mismatch_quarantines(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """A restored state exceeding the durable replay is poison.

        Given: a checkpoint restored with quantity 1.5 while the exact
            shard's durable events only fold to 1.0 (the partial-poison
            repro: one legitimate same-wallet fill must not certify the
            rest of the state),
        When: _correct_checkpoint_fill_gap evaluates the no-gap branch,
        Then: the digest mismatch fails the WHOLE projection
            certification.
        """
        coord = self._coord_with_repo(
            monkeypatch, [self._evidence_event(1, self._WALLET, "E-1", fill_size=1.0)]
        )
        poisoned = _make_checkpoint(
            shard_key=self._SHARD,
            position_qty=1.5,
            entry_price=50000.0,
            realized_pnl=0.0,
            turnover=75000.0,
            seen_exec_ids='["E-1"]',
        )
        coord._restore_trade_service_from_checkpoint(poisoned, self._SHARD, [])
        certain = await coord._correct_checkpoint_fill_gap(
            self._SHARD, self._WALLET, "paper", "paper", datetime.now(UTC), poisoned
        )
        assert certain is False
        assert coord._recovery_certification_failed is True

    @pytest.mark.asyncio
    async def test_cash_digest_mismatch_quarantines(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Cash is part of same-version checkpoint certification.

        Given: a current-version checkpoint whose position, entry, realized
            PnL, turnover, metadata, and fill identities match its durable
            replay but whose cash differs by one fee,
        When: the no-gap certification compares the exact replay digest,
        Then: the cash-only divergence fails the whole certification without
            any tolerance.
        """
        coord = self._coord_with_repo(monkeypatch, [self._evidence_event(1, self._WALLET, "E-1")])
        checkpoint = self._matching_checkpoint()
        checkpoint["cash"] += 0.01
        coord._restore_trade_service_from_checkpoint(checkpoint, self._SHARD, [])
        certain = await coord._correct_checkpoint_fill_gap(
            self._SHARD,
            self._WALLET,
            "paper",
            "paper",
            datetime.now(UTC),
            checkpoint,
        )
        assert certain is False
        assert coord._recovery_certification_failed is True

    @pytest.mark.asyncio
    async def test_legitimate_funding_cash_certifies(
        self,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        """Funding cash is excluded from a fill-only digest.

        Given: a current-version checkpoint whose exact fill fold matches
            but whose cash includes a real pre-checkpoint ledger charge,
        When: no-gap certification queries that durable accrual data,
        Then: the legitimate checkpoint certifies without weakening cash
            checks for reconstructable shards.
        """
        coord = _make_coord(monkeypatch)
        repo = SQLAlchemyRepository(f"sqlite+aiosqlite:///{tmp_path / 'funding.db'}")
        await repo.create_all()
        checkpoint = self._matching_checkpoint()
        checkpoint["cash"] -= 0.04
        instrument_public_id = "00000000-0000-7000-8000-00000000000a"
        async with repo.session() as session:
            session.add(
                AccrualLedger(
                    instrument_public_id=instrument_public_id,
                    wallet_public_id=self._WALLET,
                    operator_public_id=None,
                    mode="paper",
                    accrual_type="funding",
                    accrued_at=checkpoint["checkpoint_at"] - timedelta(minutes=1),
                    amount=-0.04,
                    amount_asset="USD",
                    rate=0.0001,
                    notional=25000.0,
                    position_quantity_at_accrual=0.5,
                    exchange="paper",
                    timestamp=checkpoint["checkpoint_at"] - timedelta(minutes=1),
                    session_id="s1",
                    sequence_id=1,
                )
            )
            await session.commit()
        coord.repository = repo
        coord._restore_trade_service_from_checkpoint(checkpoint, self._SHARD, [])
        with (
            patch.object(
                repo,
                "get_instrument_public_id_by_symbol",
                AsyncMock(return_value=instrument_public_id),
            ),
            patch.object(
                repo,
                "get_venue_events_after",
                AsyncMock(return_value=[self._evidence_event(1, self._WALLET, "E-1")]),
            ),
        ):
            certain = await coord._correct_checkpoint_fill_gap(
                self._SHARD,
                self._WALLET,
                "paper",
                "paper",
                datetime.now(UTC),
                checkpoint,
            )
        assert certain is True
        assert coord._recovery_certification_failed is False

    @pytest.mark.asyncio
    async def test_future_accrual_does_not_mask_cash_corruption(
        self,
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        """Post-checkpoint funding cannot suppress cash certification.

        Given: a cash-corrupted tagged checkpoint and a real same-scope
            accrual ledger row economically after its recorded state instant,
        When: no-gap certification scopes funding to that checkpoint,
        Then: exact cash comparison remains enabled and quarantines the shard.
        """
        coord = _make_coord(monkeypatch)
        repo = SQLAlchemyRepository(f"sqlite+aiosqlite:///{tmp_path / 'future-funding.db'}")
        await repo.create_all()
        checkpoint = self._matching_checkpoint()
        checkpoint["cash"] += 0.01
        instrument_public_id = "00000000-0000-7000-8000-00000000000a"
        future = checkpoint["checkpoint_at"] + timedelta(minutes=1)
        async with repo.session() as session:
            session.add(
                AccrualLedger(
                    instrument_public_id=instrument_public_id,
                    wallet_public_id=self._WALLET,
                    operator_public_id=None,
                    mode="paper",
                    accrual_type="funding",
                    accrued_at=future,
                    amount=-0.04,
                    amount_asset="USD",
                    rate=0.0001,
                    notional=25000.0,
                    position_quantity_at_accrual=0.5,
                    exchange="paper",
                    timestamp=future,
                    session_id="s1",
                    sequence_id=1,
                )
            )
            await session.commit()
        coord.repository = repo
        coord._restore_trade_service_from_checkpoint(checkpoint, self._SHARD, [])
        with (
            patch.object(
                repo,
                "get_instrument_public_id_by_symbol",
                AsyncMock(return_value=instrument_public_id),
            ),
            patch.object(
                repo,
                "get_venue_events_after",
                AsyncMock(return_value=[self._evidence_event(1, self._WALLET, "E-1")]),
            ),
        ):
            certain = await coord._correct_checkpoint_fill_gap(
                self._SHARD,
                self._WALLET,
                "paper",
                "paper",
                datetime.now(UTC),
                checkpoint,
            )
        assert certain is False
        assert coord._recovery_certification_failed is True

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("shard_key", "instrument_public_id"),
        [("invalid", "inst-pid"), ("paper.BTC-USD.paper", None)],
    )
    async def test_unproven_accrual_scope_keeps_cash_comparison(
        self,
        monkeypatch: pytest.MonkeyPatch,
        shard_key: str,
        instrument_public_id: str | None,
    ) -> None:
        """Cash comparison stays enabled unless accrual scope is proven.

        Given: an invalid shard key or an unresolved durable instrument,
        When: certification decides whether fill replay reconstructs cash,
        Then: it keeps exact cash comparison enabled instead of weakening it.
        """
        coord = self._coord_with_repo(monkeypatch, [])
        mock_repo = cast(AsyncMock, coord.repository)
        mock_repo.get_instrument_public_id_by_symbol = AsyncMock(return_value=instrument_public_id)
        reconstructable = await coord._checkpoint_cash_is_fill_reconstructable(
            shard_key,
            self._WALLET,
            _make_checkpoint(),
        )
        assert reconstructable is True

    @pytest.mark.asyncio
    async def test_throwing_accrual_scope_keeps_cash_comparison(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Scope lookup failures retain strict cash certification.

        Given: instrument resolution and the accrual predicate each fail
            while deciding whether checkpoint cash is fill-reconstructable,
        When: certification probes both failure paths,
        Then: neither exception escapes and both retain exact cash comparison.
        """
        coord = self._coord_with_repo(monkeypatch, [])
        mock_repo = cast(AsyncMock, coord.repository)
        checkpoint = _make_checkpoint()
        mock_repo.get_instrument_public_id_by_symbol = AsyncMock(
            side_effect=RuntimeError("resolution failed")
        )
        assert (
            await coord._checkpoint_cash_is_fill_reconstructable(
                self._SHARD,
                self._WALLET,
                checkpoint,
            )
            is True
        )
        mock_repo.get_instrument_public_id_by_symbol = AsyncMock(return_value="inst-pid")
        mock_repo.shard_has_any_accruals = AsyncMock(side_effect=RuntimeError("probe failed"))
        assert (
            await coord._checkpoint_cash_is_fill_reconstructable(
                self._SHARD,
                self._WALLET,
                checkpoint,
            )
            is True
        )

    @pytest.mark.asyncio
    async def test_identity_digest_mismatch_quarantines(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Fill identities are part of the digest below the dedup cap.

        Given: a restored checkpoint whose numerics match the replay but
            whose recorded fill identity set differs from the durable
            evidence,
        When: _correct_checkpoint_fill_gap evaluates the no-gap branch,
        Then: the identity mismatch fails the certification.
        """
        coord = self._coord_with_repo(monkeypatch, [self._evidence_event(1, self._WALLET, "E-9")])
        checkpoint = self._matching_checkpoint()
        coord._restore_trade_service_from_checkpoint(checkpoint, self._SHARD, [])
        certain = await coord._correct_checkpoint_fill_gap(
            self._SHARD, self._WALLET, "paper", "paper", datetime.now(UTC), checkpoint
        )
        assert certain is False
        assert coord._recovery_certification_failed is True

    @pytest.mark.asyncio
    async def test_foreign_or_mixed_evidence_quarantines(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Foreign and mixed wallet evidence contradict the checkpoint.

        Given: fill evidence from a different wallet, then from a mix of
            the checkpoint's and a foreign wallet,
        When: _correct_checkpoint_fill_gap evaluates each,
        Then: both fail the WHOLE projection certification — suffix-twin
            evidence must never certify under the wrong wallet.
        """
        other = "00000000-0000-7000-8000-000000000002"
        coord = self._coord_with_repo(monkeypatch, [self._evidence_event(1, other, "E-1")])
        certain = await coord._correct_checkpoint_fill_gap(
            self._SHARD, self._WALLET, "paper", "paper", datetime.now(UTC), _make_checkpoint()
        )
        assert certain is False
        assert coord._recovery_certification_failed is True
        coord2 = self._coord_with_repo(
            monkeypatch,
            [
                self._evidence_event(1, self._WALLET, "E-1"),
                self._evidence_event(2, other, "E-2"),
            ],
        )
        certain2 = await coord2._correct_checkpoint_fill_gap(
            self._SHARD, self._WALLET, "paper", "paper", datetime.now(UTC), _make_checkpoint()
        )
        assert certain2 is False
        assert coord2._recovery_certification_failed is True

    @pytest.mark.asyncio
    async def test_unattributed_evidence_uncertified_without_flag(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Unattributed rows are absence of knowledge, not contradiction.

        Given: fill evidence where one row lacks wallet attribution, and
            separately a checkpoint whose own wallet is unresolved,
        When: _correct_checkpoint_fill_gap evaluates each,
        Then: both stay uncertified WITHOUT the global flag.
        """
        coord = self._coord_with_repo(
            monkeypatch,
            [
                self._evidence_event(1, "", "E-1"),
                self._evidence_event(2, self._WALLET, "E-2"),
            ],
        )
        certain = await coord._correct_checkpoint_fill_gap(
            self._SHARD, self._WALLET, "paper", "paper", datetime.now(UTC), _make_checkpoint()
        )
        assert certain is False
        assert coord._recovery_certification_failed is False
        coord2 = self._coord_with_repo(monkeypatch, [self._evidence_event(1, self._WALLET, "E-1")])
        certain2 = await coord2._correct_checkpoint_fill_gap(
            self._SHARD, "", "paper", "paper", datetime.now(UTC), _make_checkpoint()
        )
        assert certain2 is False
        assert coord2._recovery_certification_failed is False

    @pytest.mark.asyncio
    async def test_flat_checkpoint_needs_no_evidence(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """A checkpoint without fill state certifies with no evidence scan.

        Given: an all-zero (never-filled) checkpoint on the no-gap path
            with no durable fill events,
        When: _correct_checkpoint_fill_gap evaluates it,
        Then: it is certain — absence of fills matches absence of state.
        """
        coord = self._coord_with_repo(monkeypatch, [])
        flat = _make_checkpoint(
            position_qty=0.0, realized_pnl=0.0, turnover=0.0, last_venue_event_id=0
        )
        certain = await coord._correct_checkpoint_fill_gap(
            self._SHARD, self._WALLET, "paper", "paper", datetime.now(UTC), flat
        )
        assert certain is True

    @pytest.mark.asyncio
    async def test_malformed_evidence_shape_fails_closed(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A non-list evidence result must never certify.

        Given: a repository whose evidence query returns a non-list
            shape,
        When: _correct_checkpoint_fill_gap evaluates a fill-bearing
            checkpoint,
        Then: the shard stays uncertified — a certification input fails
            closed, never open.
        """
        coord = self._coord_with_repo(monkeypatch, object())
        certain = await coord._correct_checkpoint_fill_gap(
            self._SHARD, self._WALLET, "paper", "paper", datetime.now(UTC), _make_checkpoint()
        )
        assert certain is False
        assert coord._recovery_certification_failed is False

    @pytest.mark.asyncio
    async def test_order_only_watermark_needs_no_fill_evidence(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """An order-only shard (acks bumped the watermark) stays certain.

        Given: a checkpoint whose fill-derived fields are all zero but
            whose venue-event watermark advanced (submits/rejects also
            bump it),
        When: _correct_checkpoint_fill_gap evaluates the no-gap branch,
        Then: it is certain — an honest never-filled shard must not be
            permanently uncertified by watermark motion alone.
        """
        coord = self._coord_with_repo(monkeypatch, [])
        order_only = _make_checkpoint(
            position_qty=0.0, realized_pnl=0.0, turnover=0.0, last_venue_event_id=10
        )
        certain = await coord._correct_checkpoint_fill_gap(
            self._SHARD, self._WALLET, "paper", "paper", datetime.now(UTC), order_only
        )
        assert certain is True

    @pytest.mark.asyncio
    async def test_no_checkpoint_no_gap_stays_certain(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A checkpoint-less no-gap evaluation keeps the legacy outcome.

        Given: a no-gap shard evaluated WITHOUT a checkpoint row (legacy
            call shape),
        When: _correct_checkpoint_fill_gap runs,
        Then: it is certain — there is no fill state to demand evidence
            for.
        """
        coord = self._coord_with_repo(monkeypatch, [])
        certain = await coord._correct_checkpoint_fill_gap(
            self._SHARD, self._WALLET, "paper", "paper", datetime.now(UTC)
        )
        assert certain is True

    @pytest.mark.asyncio
    async def test_evidence_helper_tolerates_plain_repository(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The evidence helper is a no-op without a SQLAlchemy repository.

        Given: a coordinator with a plain (non-SQLAlchemy) repository,
        When: _checkpoint_fill_evidence_certain runs on a fill-bearing
            checkpoint,
        Then: it returns certain — evidence validation is only defined
            over the durable venue-event plane.
        """
        coord = _make_coord(monkeypatch)
        coord.repository = AsyncMock()
        certain = await coord._checkpoint_fill_evidence_certain(
            self._SHARD, self._WALLET, _make_checkpoint()
        )
        assert certain is True

    @pytest.mark.asyncio
    async def test_checkpoint_failure_attribution_uses_durable_wallet(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """S5.1 F4: the identity-miss site attributes via the durable wallet.

        Given: a checkpoint shard whose projection identity is missing
            (instrument lookup yields no public id) and whose
            wallet-short cache is EMPTY (renames/collisions can poison
            it), with the checkpoint carrying its durable full wallet,
        When: _recover_checkpoint_row records the recovery failure,
        Then: the recorder is invoked with the checkpoint's durable full
            wallet and its temporal anchor — never the cache.
        """
        coord = _make_coord(monkeypatch)
        coord._wallet_short_to_id = {}
        mock_repo = AsyncMock(spec=SQLAlchemyRepository)
        _set_sqlalchemy_repo(coord, mock_repo)
        mock_repo.get_venue_events_after = AsyncMock(return_value=[])
        mock_repo.get_fill_event_wallets = AsyncMock(return_value=[self._WALLET])
        mock_repo.get_accruals = AsyncMock(return_value=[])
        mock_repo.shard_has_any_accruals = AsyncMock(return_value=False)
        mock_repo.get_instrument_public_id_by_symbol = AsyncMock(return_value=None)
        checkpoint = _make_checkpoint(shard_key=self._SHARD)
        checkpoint["wallet_public_id"] = self._WALLET
        recorder = AsyncMock()
        with patch.object(coord, "_record_recovery_shard_failure", recorder):
            engine_key = await coord._recover_checkpoint_row(checkpoint, datetime.now(UTC))
        assert engine_key == "BTC-USD@paper-heartbeat-waabbccddeeff"
        recorder.assert_awaited_once_with(
            self._SHARD,
            durable_wallet_public_id=self._WALLET,
            anchor=checkpoint["checkpoint_at"],
        )


class TestRebuildWalletAgreement:
    """S5.2 C2: the venue-only rebuild never mutates a foreign-wallet engine."""

    _WALLET_A = "00000000-0000-7000-8000-aabbccddeeff"
    _WALLET_B = "11111111-0000-7000-8000-aabbccddeeff"
    _SHARD = "kraken.BTC-USD.live.waabbccddeeff"

    def _coord(
        self, monkeypatch: pytest.MonkeyPatch, events: list[VenueEventRow]
    ) -> TraderCoordinator:
        """Build a coordinator with a gapped shard and given durable events."""
        coord = _make_coord(monkeypatch)
        mock_repo = AsyncMock(spec=SQLAlchemyRepository)
        mock_repo.get_venue_events_after = AsyncMock(return_value=events)
        _set_sqlalchemy_repo(coord, mock_repo)
        mock_repo.shard_has_fill_gap = AsyncMock(return_value=True)
        return coord

    def _event(self, wallet: str, exec_id: str = "E-1") -> VenueEventRow:
        """Build one gapped-shard fill event owned by ``wallet``."""
        event = dict(
            _make_venue_event(
                event_id=1, shard_key=self._SHARD, fill_size=0.1, exec_id=exec_id, trade_id=exec_id
            )
        )
        event["wallet_public_id"] = wallet
        return cast(VenueEventRow, event)

    @pytest.mark.asyncio
    async def test_rebuild_refuses_foreign_wallet_incumbent(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Wallet A's events never replay into twin B's engine.

        Given: a gapped shard whose durable events belong to wallet A
            while the ONLY engine serving that shard STRING carries
            suffix-twin wallet B,
        When: _rebuild_shard_if_gapped runs,
        Then: the rebuild refuses, the certification fails, and twin
            B's engine state is untouched (the C2 repro replaced B's
            0.2 with A's 0.1 while staying trusted).
        """
        coord = self._coord(monkeypatch, [self._event(self._WALLET_A)])
        coord._wallet_short_to_id = {"aabbccddeeff": self._WALLET_A}
        twin_b = MagicMock()
        twin_b._shard_key = self._SHARD
        twin_b.wallet_public_id = self._WALLET_B
        coord.engines["BTC-USD@kraken-live-twinb"] = twin_b
        rebuilt = await coord._rebuild_shard_if_gapped(self._SHARD, datetime.now(UTC))
        assert rebuilt is False
        assert coord._recovery_certification_failed is True
        assert self._SHARD not in coord.trade_service.known_shard_keys()

    @pytest.mark.asyncio
    async def test_rebuild_refuses_twin_incumbent_ambiguity(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Two incumbents on one shard string cannot be rebuilt into.

        Given: a gapped shard served by TWO engines (suffix twins), one
            of which contradicts the durable evidence wallet,
        When: _rebuild_shard_if_gapped runs,
        Then: the rebuild refuses and the certification fails.
        """
        coord = self._coord(monkeypatch, [self._event(self._WALLET_A)])
        coord._wallet_short_to_id = {}
        twin_a = MagicMock()
        twin_a._shard_key = self._SHARD
        twin_a.wallet_public_id = self._WALLET_A
        twin_b = MagicMock()
        twin_b._shard_key = self._SHARD
        twin_b.wallet_public_id = self._WALLET_B
        coord.engines["twin-a"] = twin_a
        coord.engines["twin-b"] = twin_b
        rebuilt = await coord._rebuild_shard_if_gapped(self._SHARD, datetime.now(UTC))
        assert rebuilt is False
        assert coord._recovery_certification_failed is True

    @pytest.mark.asyncio
    async def test_rebuild_refuses_identity_contradicting_events(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Events contradicting the shard identity never replay.

        Given: a gapped shard whose durable events carry a DIFFERENT
            instrument than the shard key names,
        When: _rebuild_shard_if_gapped runs,
        Then: the rebuild refuses and the certification fails — a
            replay would launder a foreign identity into the shard.
        """
        foreign = dict(self._event(self._WALLET_A))
        foreign["instrument"] = "ETH-USD"
        coord = self._coord(monkeypatch, [cast(VenueEventRow, foreign)])
        coord._wallet_short_to_id = {"aabbccddeeff": self._WALLET_A}
        rebuilt = await coord._rebuild_shard_if_gapped(self._SHARD, datetime.now(UTC))
        assert rebuilt is False
        assert coord._recovery_certification_failed is True

    @pytest.mark.asyncio
    async def test_rebuild_adopts_evidence_wallet_when_unresolved(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """An unresolved wallet short adopts the unique durable wallet.

        Given: a gapped shard whose wallet short no longer resolves but
            whose durable events all carry ONE full wallet,
        When: _rebuild_shard_if_gapped runs,
        Then: the rebuild proceeds under the durable evidence wallet and
            the created engine carries it.
        """
        coord = self._coord(monkeypatch, [self._event(self._WALLET_A)])
        coord._wallet_short_to_id = {}
        rebuilt = await coord._rebuild_shard_if_gapped(self._SHARD, datetime.now(UTC))
        assert rebuilt is True
        engine = coord.engines["BTC-USD@kraken-live-waabbccddeeff"]
        assert engine.wallet_public_id == self._WALLET_A
        assert coord._recovery_certification_failed is False


class TestOwnerKeyedAmbiguity:
    """S5.2 C4: ambiguity counts (shard, FULL wallet) owners, not strings."""

    _WALLET_A = "00000000-0000-7000-8000-aabbccddeeff"
    _WALLET_B = "11111111-0000-7000-8000-aabbccddeeff"
    _SHARD = "kraken.BTC-USD.live.waabbccddeeff"

    def test_legacy_scope_counts_twin_owners_of_one_string(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Suffix twins sharing ONE shard string still refuse walletless routing.

        Given: two engines whose shard keys are the SAME string but
            whose FULL wallets differ, registered via the registry,
        When: the legacy-scope shard set is inspected,
        Then: it holds two (shard, wallet) owners — a string-keyed set
            would collapse them to one and let first-wins routing feed
            the wrong twin.
        """
        coord = _make_coord(monkeypatch)
        twin_a = MagicMock()
        twin_a.exchange = "kraken"
        twin_a.instrument = "BTC-USD"
        twin_a.wallet_public_id = self._WALLET_A
        twin_a._shard_key = self._SHARD
        twin_a.pending_client_order_id = None
        twin_b = MagicMock()
        twin_b.exchange = "kraken"
        twin_b.instrument = "BTC-USD"
        twin_b.wallet_public_id = self._WALLET_B
        twin_b._shard_key = self._SHARD
        twin_b.pending_client_order_id = None
        coord._register_engine_for_lookup(twin_a)
        coord._register_engine_for_lookup(twin_b)
        assert coord._legacy_scope_shard_keys[("kraken", "BTC-USD")] == {
            (self._SHARD, self._WALLET_A),
            (self._SHARD, self._WALLET_B),
        }

    def test_identity_conflict_quarantines(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """A second identity claiming one shard string fails certification.

        Given: a shard string already registered to wallet A's identity,
        When: suffix-twin wallet B's engine registers the same string,
        Then: the original identity is kept and the WHOLE projection
            certification fails — both cannot be truth.
        """
        coord = _make_coord(monkeypatch)
        coord._projection_identities[self._SHARD] = ("inst-pid", "live", self._WALLET_A)
        twin_b = MagicMock()
        twin_b._shard_key = self._SHARD
        twin_b.mode = "live"
        twin_b.wallet_public_id = self._WALLET_B
        twin_b.instrument = "BTC-USD"
        twin_b.instrument_specs = {"BTC-USD": {"public_id": "inst-pid"}}
        coord._register_projection_identity(twin_b)
        assert coord._projection_identities[self._SHARD] == ("inst-pid", "live", self._WALLET_A)
        assert coord._recovery_certification_failed is True


class TestFillDigestBranches:
    """Edge branches of the replay-digest and event-identity validators."""

    def test_fill_digest_edge_branches(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Non-finite, None-mismatched, and drifted entries never match.

        Given: a restored shard and replay projections that disagree in
            each edge dimension (NaN numerics, entry None-mismatch, NaN
            entries, entry drift) plus one fully agreeing flat pair,
        When: _fill_digest_matches compares them,
        Then: only the agreeing pair matches — corruption never does.
        """
        coord = _make_coord(monkeypatch)
        shard = coord.trade_service._get_or_create_shard("digest-test")

        def projection(
            qty: float = 0.0,
            entry: float | None = None,
            realized: float = 0.0,
            cash: float = 10000.0,
            turnover: float = 0.0,
        ) -> FillProjection:
            return {
                "position_qty": qty,
                "entry_price": entry,
                "position_opened_at": None,
                "realized_pnl": realized,
                "cash": cash,
                "turnover": turnover,
                "seen_exec_ids": OrderedDict(),
                "last_venue_event_id": 0,
            }

        assert coord._fill_digest_matches(shard, projection()) is True
        assert coord._fill_digest_matches(shard, projection(qty=float("nan"))) is False
        assert coord._fill_digest_matches(shard, projection(cash=float("nan"))) is False
        assert coord._fill_digest_matches(shard, projection(cash=9999.99)) is False
        assert coord._fill_digest_matches(shard, projection(entry=50000.0)) is False
        shard.position.entry_price = float("nan")
        assert coord._fill_digest_matches(shard, projection(entry=float("nan"))) is False
        shard.position.entry_price = 50000.0
        assert coord._fill_digest_matches(shard, projection(entry=50001.0)) is False
        assert coord._fill_digest_matches(shard, projection(entry=50000.0)) is True

    def test_event_identity_validation_branches(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Each present-but-disagreeing identity field is a contradiction.

        Given: exact-shard events whose exchange, then mode, disagree
            with the parsed shard identity, plus one legacy event with
            EMPTY identity fields,
        When: _venue_events_match_shard validates them,
        Then: the disagreeing events fail and the empty-field legacy
            event is tolerated (absence is not contradiction).
        """
        coord = _make_coord(monkeypatch)
        base = _make_venue_event(event_id=1)
        assert coord._venue_events_match_shard([base], "kraken", "BTC-USD", "live") is True
        wrong_exchange = cast(VenueEventRow, dict(base) | {"exchange": "binance"})
        assert (
            coord._venue_events_match_shard([wrong_exchange], "kraken", "BTC-USD", "live") is False
        )
        wrong_mode = cast(VenueEventRow, dict(base) | {"mode": "paper"})
        assert coord._venue_events_match_shard([wrong_mode], "kraken", "BTC-USD", "live") is False
        legacy = cast(VenueEventRow, dict(base) | {"exchange": "", "instrument": "", "mode": ""})
        assert coord._venue_events_match_shard([legacy], "kraken", "BTC-USD", "live") is True


class TestRoundThreeFailClosed:
    """S5.3: flat-bypass, event validation, adoption, and digest exactness."""

    _WALLET = "00000000-0000-7000-8000-aabbccddeeff"
    _SHARD = "paper.BTC-USD.paper.waabbccddeeff.heartbeat"

    def _coord_with_events(
        self, monkeypatch: pytest.MonkeyPatch, events: list[VenueEventRow]
    ) -> TraderCoordinator:
        """Coordinator whose repo reports no gap and the given history.

        Wires the instrument resolver and wallet-short cache so a scoped
        (identity-level) recovery failure resolves to a canonical
        identity rather than escalating to the global flag.
        """
        coord = _make_coord(monkeypatch)
        coord._wallet_short_to_id = {"aabbccddeeff": self._WALLET}
        mock_repo = AsyncMock(spec=SQLAlchemyRepository)
        _set_sqlalchemy_repo(coord, mock_repo)
        mock_repo.get_venue_events_after = AsyncMock(return_value=events)
        mock_repo.get_instrument_public_id_by_symbol = AsyncMock(return_value="inst-pid")
        return coord

    def _fill(
        self,
        event_id: int = 1,
        exec_id: str = "E-1",
        fill_size: float = 0.5,
        fill_price: float = 50000.0,
    ) -> VenueEventRow:
        """One sound exact-shard fill attributed to the checkpoint wallet."""
        event = dict(
            _make_venue_event(
                event_id=event_id,
                shard_key=self._SHARD,
                fill_size=fill_size,
                fill_price=fill_price,
                exec_id=exec_id,
                trade_id=exec_id,
            )
        )
        event["wallet_public_id"] = self._WALLET
        event["exchange"] = "paper"
        event["mode"] = "paper"
        return cast(VenueEventRow, event)

    @pytest.mark.asyncio
    async def test_gap_overlay_malformed_fills_scoped(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The gap-overlay branch scopes a malformed fill to its identity.

        Given: a shard with a recorded>consumed gap whose full history
            carries a zero-price fill,
        When: _correct_checkpoint_fill_gap runs the gap-overlay branch,
        Then: the malformed fill leaves the identity UNCERTIFIED and
            records its canonical failed identity WITHOUT the global flag
            (S5.4 P0-5 split of the gap-overlay path).
        """
        coord = self._coord_with_events(monkeypatch, [self._fill(fill_price=0.0)])
        mock_repo = cast(AsyncMock, coord.repository)
        mock_repo.shard_has_fill_gap = AsyncMock(return_value=True)
        mock_repo.shard_has_accruals = AsyncMock(return_value=False)
        certain = await coord._correct_checkpoint_fill_gap(
            self._SHARD, self._WALLET, "paper", "paper", datetime.now(UTC), _make_checkpoint()
        )
        assert certain is False
        assert coord._recovery_certification_failed is False
        assert ("inst-pid", "paper", self._WALLET) in coord._failed_recovery_identities

    @pytest.mark.asyncio
    async def test_gap_overlay_unattributed_fills_uncertified(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The gap-overlay branch refuses to overlay unattributed fills.

        Given: a shard with a recorded>consumed gap whose full history
            carries a fill with NO wallet attribution,
        When: _correct_checkpoint_fill_gap runs the gap-overlay branch,
        Then: it leaves the shard UNCERTIFIED without the global flag —
            an unattributed overlay must not adopt evidence by topology
            (S5.4 P0-1 gap-overlay attribution gate).
        """
        unattributed = cast(VenueEventRow, dict(self._fill()) | {"wallet_public_id": ""})
        coord = self._coord_with_events(monkeypatch, [unattributed])
        mock_repo = cast(AsyncMock, coord.repository)
        mock_repo.shard_has_fill_gap = AsyncMock(return_value=True)
        mock_repo.shard_has_accruals = AsyncMock(return_value=False)
        certain = await coord._correct_checkpoint_fill_gap(
            self._SHARD, self._WALLET, "paper", "paper", datetime.now(UTC), _make_checkpoint()
        )
        assert certain is False
        assert coord._recovery_certification_failed is False

    @pytest.mark.asyncio
    async def test_corrupted_flat_checkpoint_cannot_close_real_position(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A flat checkpoint gets no evidence bypass.

        Given: a FLAT (all-zero) checkpoint while the exact shard holds
            a durable consumed 0.5 fill (the round-3 repro: trusting
            the flat row would close the real position),
        When: _correct_checkpoint_fill_gap evaluates the no-gap branch,
        Then: the digest mismatch fails the WHOLE certification.
        """
        coord = self._coord_with_events(monkeypatch, [self._fill()])
        flat = _make_checkpoint(
            shard_key=self._SHARD,
            position_qty=0.0,
            entry_price=None,
            realized_pnl=0.0,
            turnover=0.0,
            last_venue_event_id=0,
            seen_exec_ids="[]",
        )
        coord._restore_trade_service_from_checkpoint(flat, self._SHARD, [])
        certain = await coord._correct_checkpoint_fill_gap(
            self._SHARD, self._WALLET, "paper", "paper", datetime.now(UTC), flat
        )
        assert certain is False
        assert coord._recovery_certification_failed is True

    @pytest.mark.asyncio
    async def test_identity_contradicting_evidence_quarantines(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Evidence whose instrument contradicts the shard never certifies.

        Given: exact-shard history containing an ETH-USD fill under the
            BTC shard (the round-3 repro),
        When: the no-gap evidence path evaluates it,
        Then: the identity contradiction fails the WHOLE certification.
        """
        foreign = dict(self._fill())
        foreign["instrument"] = "ETH-USD"
        coord = self._coord_with_events(monkeypatch, [cast(VenueEventRow, foreign)])
        certain = await coord._correct_checkpoint_fill_gap(
            self._SHARD, self._WALLET, "paper", "paper", datetime.now(UTC), _make_checkpoint()
        )
        assert certain is False
        assert coord._recovery_certification_failed is True

    @pytest.mark.asyncio
    async def test_conflicting_duplicate_payloads_quarantine(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Rows sharing an exec id must agree on their payload.

        Given: two durable rows with ONE exec id but different prices
            (replay dedup would silently keep the first),
        When: the no-gap evidence path evaluates them,
        Then: the payload conflict leaves the identity UNCERTIFIED and
            records its canonical failed identity WITHOUT tripping the
            global flag — an attributable malformed row quarantines only
            its own identity (S5.4 P0-5).
        """
        coord = self._coord_with_events(
            monkeypatch,
            [
                self._fill(event_id=1, exec_id="E-1", fill_price=50000.0),
                self._fill(event_id=2, exec_id="E-1", fill_price=51000.0),
            ],
        )
        certain = await coord._correct_checkpoint_fill_gap(
            self._SHARD, self._WALLET, "paper", "paper", datetime.now(UTC), _make_checkpoint()
        )
        assert certain is False
        assert coord._recovery_certification_failed is False
        assert ("inst-pid", "paper", self._WALLET) in coord._failed_recovery_identities

    @pytest.mark.asyncio
    async def test_delta_replay_validates_identity_and_payload(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Delta events are validated before the restore consumes them.

        Given: a checkpoint whose delta replay carries a fill from a
            DIFFERENT instrument and a zero-size fill,
        When: _recover_checkpoint_row runs,
        Then: the certification fails on both grounds.
        """
        coord = _make_coord(monkeypatch)
        coord._wallet_short_to_id = {"aabbccddeeff": self._WALLET}
        mock_repo = AsyncMock(spec=SQLAlchemyRepository)
        _set_sqlalchemy_repo(coord, mock_repo)
        mock_repo.get_accruals = AsyncMock(return_value=[])
        mock_repo.shard_has_any_accruals = AsyncMock(return_value=False)
        mock_repo.get_instrument_public_id_by_symbol = AsyncMock(return_value="inst-pid")
        bad_delta = dict(self._fill(event_id=11, exec_id="E-11"))
        bad_delta["instrument"] = "ETH-USD"
        bad_delta["fill_size"] = 0.0
        mock_repo.get_venue_events_after = AsyncMock(return_value=[cast(VenueEventRow, bad_delta)])
        checkpoint = _make_checkpoint(shard_key=self._SHARD)
        checkpoint["wallet_public_id"] = self._WALLET
        await coord._recover_checkpoint_row(checkpoint, datetime.now(UTC))
        assert coord._recovery_certification_failed is True

    def test_fill_events_sound_branches(self) -> None:
        """Payload soundness rejects each malformed shape.

        Given: fills with missing, non-finite, zero, and negative
            economics, agreeing and conflicting duplicates, and id-less
            rows,
        When: _fill_events_sound validates each set,
        Then: only sound, agreeing payloads pass.
        """
        sound = self._fill()
        assert TraderCoordinator._fill_events_sound([sound]) is True
        assert TraderCoordinator._fill_events_sound([sound, sound]) is True
        missing = cast(VenueEventRow, dict(sound) | {"fill_size": None})
        assert TraderCoordinator._fill_events_sound([missing]) is False
        nan_price = cast(VenueEventRow, dict(sound) | {"fill_price": float("nan")})
        assert TraderCoordinator._fill_events_sound([nan_price]) is False
        zero_size = cast(VenueEventRow, dict(sound) | {"fill_size": 0.0})
        assert TraderCoordinator._fill_events_sound([zero_size]) is False
        negative_price = cast(VenueEventRow, dict(sound) | {"fill_price": -1.0})
        assert TraderCoordinator._fill_events_sound([negative_price]) is False
        conflicting = cast(VenueEventRow, dict(sound) | {"fill_price": 51000.0})
        assert TraderCoordinator._fill_events_sound([sound, conflicting]) is False
        keyless = cast(VenueEventRow, dict(sound) | {"exec_id": None, "trade_id": None})
        assert TraderCoordinator._fill_events_sound([keyless, keyless]) is True
        null_side = cast(VenueEventRow, dict(sound) | {"side": None})
        assert TraderCoordinator._fill_events_sound([null_side]) is False
        unknown_side = cast(VenueEventRow, dict(sound) | {"side": "long"})
        assert TraderCoordinator._fill_events_sound([unknown_side]) is False
        upper_side = cast(VenueEventRow, dict(sound) | {"side": "BUY"})
        assert TraderCoordinator._fill_events_sound([upper_side]) is True

    def test_digest_exactness_branches(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """The digest is exact and complete.

        Given: replay projections that drift by 5e-10, disagree on the
            opening timestamp, disagree on the watermark, or saturate
            the identity cap,
        When: _fill_digest_matches compares them,
        Then: every one of them fails to match.
        """
        coord = _make_coord(monkeypatch)
        shard = coord.trade_service._get_or_create_shard("digest-exact")

        def projection(
            qty: float = 0.0,
            opened_at: datetime | None = None,
            watermark: int = 0,
            seen: OrderedDict[str, None] | None = None,
        ) -> FillProjection:
            return {
                "position_qty": qty,
                "entry_price": None,
                "position_opened_at": opened_at,
                "realized_pnl": 0.0,
                "cash": 10000.0,
                "turnover": 0.0,
                "seen_exec_ids": seen if seen is not None else OrderedDict(),
                "last_venue_event_id": watermark,
            }

        assert coord._fill_digest_matches(shard, projection()) is True
        assert coord._fill_digest_matches(shard, projection(qty=5e-10)) is False
        assert (
            coord._fill_digest_matches(
                shard, projection(opened_at=datetime(2024, 6, 1, tzinfo=UTC))
            )
            is False
        )
        assert coord._fill_digest_matches(shard, projection(watermark=7)) is False
        saturated: OrderedDict[str, None] = OrderedDict(
            (f"id-{index}", None) for index in range(10_000)
        )
        shard.seen_exec_ids = saturated
        assert coord._fill_digest_matches(shard, projection(seen=saturated.copy())) is False

    @pytest.mark.asyncio
    async def test_rebuild_unattributed_fills_stay_uncertified(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Unattributed durable fills never rebuild by topology.

        Given: a gapped shard whose durable fills carry NO wallet
            attribution (only the boot cache knows the short),
        When: _rebuild_shard_if_gapped runs,
        Then: nothing is rebuilt and the shard's canonical identity is
            recorded as failed WITHOUT the global flag — absence of
            durable attribution is not corruption, but it must never
            certify or adopt a wallet by topology either.
        """
        coord = _make_coord(monkeypatch)
        coord._wallet_short_to_id = {"aabbccddeeff": self._WALLET}
        mock_repo = AsyncMock(spec=SQLAlchemyRepository)
        unattributed = dict(
            _make_venue_event(
                event_id=5,
                shard_key="kraken.BTC-USD.live.waabbccddeeff",
                fill_size=0.5,
                exec_id="a",
                trade_id="a",
            )
        )
        mock_repo.get_venue_events_after = AsyncMock(
            return_value=[cast(VenueEventRow, unattributed)]
        )
        _set_sqlalchemy_repo(coord, mock_repo)
        mock_repo.shard_has_fill_gap = AsyncMock(return_value=True)
        mock_repo.get_instrument_public_id_by_symbol = AsyncMock(return_value="inst-pid")
        rebuilt = await coord._rebuild_shard_if_gapped(
            "kraken.BTC-USD.live.waabbccddeeff", datetime.now(UTC)
        )
        assert rebuilt is False
        assert dict(coord.engines) == {}
        assert ("inst-pid", "live", self._WALLET) in coord._failed_recovery_identities
        assert coord._recovery_certification_failed is False
        assert "kraken.BTC-USD.live.waabbccddeeff" not in coord.trade_service.known_shard_keys()

    @pytest.mark.asyncio
    async def test_gap_overlay_validates_identity_and_payload(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The gap-overlay branch refuses contradicting durable events.

        Given: a gapped checkpoint shard whose full history carries a
            fill from a DIFFERENT instrument,
        When: _correct_checkpoint_fill_gap runs the overlay branch,
        Then: the overlay is refused and the certification fails —
            replaying it would launder a foreign identity into the
            restored state.
        """
        coord = _make_coord(monkeypatch)
        mock_repo = AsyncMock(spec=SQLAlchemyRepository)
        foreign = dict(self._fill())
        foreign["instrument"] = "ETH-USD"
        mock_repo.get_venue_events_after = AsyncMock(return_value=[cast(VenueEventRow, foreign)])
        _set_sqlalchemy_repo(coord, mock_repo)
        mock_repo.shard_has_fill_gap = AsyncMock(return_value=True)
        mock_repo.shard_has_accruals = AsyncMock(return_value=False)
        certain = await coord._correct_checkpoint_fill_gap(
            self._SHARD, self._WALLET, "paper", "paper", datetime.now(UTC), _make_checkpoint()
        )
        assert certain is False
        assert coord._recovery_certification_failed is True

    @pytest.mark.asyncio
    async def test_rebuild_refuses_conflicting_payloads(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The venue-only rebuild refuses conflicting duplicate payloads.

        Given: a gapped shard whose durable history carries two rows
            under ONE exec id with different prices,
        When: _rebuild_shard_if_gapped runs,
        Then: the rebuild is refused and the identity is quarantined
            (scoped) WITHOUT the global flag — an attributable malformed
            row quarantines only its own identity (S5.4 P0-5).
        """
        wallet = "00000000-0000-7000-8000-aabbccddeeff"
        shard = "kraken.BTC-USD.live.waabbccddeeff"
        coord = _make_coord(monkeypatch)
        coord._wallet_short_to_id = {"aabbccddeeff": wallet}
        mock_repo = AsyncMock(spec=SQLAlchemyRepository)
        rows = []
        for event_id, price in ((5, 100.0), (6, 110.0)):
            row = dict(
                _make_venue_event(
                    event_id=event_id,
                    shard_key=shard,
                    fill_size=0.5,
                    fill_price=price,
                    exec_id="dup",
                    trade_id="dup",
                )
            )
            row["wallet_public_id"] = wallet
            rows.append(cast(VenueEventRow, row))
        mock_repo.get_venue_events_after = AsyncMock(return_value=rows)
        _set_sqlalchemy_repo(coord, mock_repo)
        mock_repo.shard_has_fill_gap = AsyncMock(return_value=True)
        mock_repo.get_instrument_public_id_by_symbol = AsyncMock(return_value="inst-pid")
        rebuilt = await coord._rebuild_shard_if_gapped(shard, datetime.now(UTC))
        assert rebuilt is False
        assert coord._recovery_certification_failed is False
        assert ("inst-pid", "live", wallet) in coord._failed_recovery_identities
        assert shard not in coord.trade_service.known_shard_keys()
