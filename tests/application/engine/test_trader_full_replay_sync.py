"""Regression tests for full-replay recovery rebuilding TradeService.

Pins the invariant that ``TraderCoordinator._recover_from_executions``
shadow-replays DB executions through ``TradeService.apply_venue_event``
so the projection (engine + TradeService shard) stays in sync. Before
this refactor the method updated engine state directly and left
TradeService flat, producing a latent ``old_qty == 0`` hazard on the
first live fill into a non-flat recovered shard.

Follow-up to the position-cycle recovery work.
"""

from collections.abc import Iterable
from datetime import UTC
from datetime import datetime
from typing import Any
from typing import cast
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import Mock

import pytest

import snapper.application.engine.trader as trader_module
from snapper.application.engine.trader import TraderCoordinator
from snapper.application.portfolio.fill_booking import PROJECTION_CALC_VERSION
from snapper.data.repository import SQLAlchemyRepository
from snapper.data.repository_types import ExecutionRow
from snapper.data.repository_types import TradeProjectionCheckpointRow
from snapper.data.repository_types import VenueEventRow


def _make_execution(
    *,
    trade_id: str,
    side: str,
    size: float,
    price: float,
    instrument: str = "BTC-USD",
    exchange: str = "kraken",
    fee: float = 0.0,
    fee_asset: str = "USD",
    wallet_public_id: str | None = None,
    timestamp_minute: int = 0,
) -> ExecutionRow:
    """Build an ExecutionRow for full-replay tests.

    Uses a fixed 2026-01-01 base timestamp plus an optional minute offset
    so successive fills stay monotonic without colliding.
    """
    ts = datetime(2026, 1, 1, 12, timestamp_minute, tzinfo=UTC)
    return {
        "public_id": f"exe-{trade_id}",
        "timestamp": ts,
        "session_id": "s-seed",
        "sequence_id": timestamp_minute + 1,
        "trade_id": trade_id,
        "exchange_order_id": f"ex-{trade_id}",
        "client_order_id": f"c-{trade_id}",
        "instrument": instrument,
        "exchange": exchange,
        "side": side,
        "size": size,
        "price": price,
        "fee": fee,
        "fee_asset": fee_asset,
        "status": "filled",
        "executed_at": ts,
        "wallet_public_id": wallet_public_id,
        "operator_public_id": None,
    }


def _make_checkpoint(
    *,
    shard_key: str,
    position_qty: float = 0.5,
    entry_price: float | None = 50000.0,
    last_venue_event_id: int | None = 10,
    seen_exec_ids: str = "[]",
) -> TradeProjectionCheckpointRow:
    """Build a checkpoint row for full-replay vs checkpoint-skip tests."""
    return {
        "public_id": "cp-1",
        "shard_key": shard_key,
        "projection_calc_version": PROJECTION_CALC_VERSION,
        "position_qty": position_qty,
        "entry_price": entry_price,
        "position_opened_at": datetime(2026, 1, 1, tzinfo=UTC),
        "cash": 9500.0,
        "peak_equity": 10000.0,
        "realized_pnl": 0.0,
        "turnover": 500.0,
        "last_venue_event_id": last_venue_event_id,
        "last_venue_event_at": datetime(2026, 1, 1, tzinfo=UTC),
        "open_command_ids": None,
        "seen_exec_ids": seen_exec_ids,
        "checkpoint_at": datetime(2026, 1, 1, tzinfo=UTC),
        "session_id": "s-cp",
        "operator_public_id": None,
    }


def _make_coord(monkeypatch: pytest.MonkeyPatch) -> TraderCoordinator:
    """Build a TraderCoordinator with the full-replay dependencies stubbed."""
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


def _stub_repo(coord: TraderCoordinator, executions: Iterable[ExecutionRow]) -> AsyncMock:
    """Attach a stub repository returning the given executions for full replay."""
    repo = AsyncMock(spec=SQLAlchemyRepository)
    repo.get_executions_for_recovery = AsyncMock(return_value=list(executions))
    repo.get_all_checkpoints = AsyncMock(return_value=[])
    repo.get_venue_events_after = AsyncMock(return_value=[])
    repo.get_active_orders_for_recovery = AsyncMock(return_value=[])
    repo.ensure_instrument = AsyncMock(return_value=(1, "inst-pid"))
    coord.repository = repo
    return repo


_EPS = 1e-9


class TestFullReplaySync:
    """Full-replay recovery keeps engine and TradeService in lockstep."""

    @pytest.mark.asyncio
    async def test_full_replay_populates_trade_service(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """TradeService shard matches engine.position_qty after full replay.

        Given: two BUY executions for the same shard (1 @ 100, 2 @ 110),
        When: _recover_engine_state runs with no checkpoint,
        Then: both engine and TradeService shard carry position_qty=3
            and entry_price ≈ 106.67 (VWAP of 1*100 + 2*110).
        """
        coord = _make_coord(monkeypatch)
        _stub_repo(
            coord,
            [
                _make_execution(trade_id="t1", side="buy", size=1.0, price=100.0),
                _make_execution(
                    trade_id="t2", side="buy", size=2.0, price=110.0, timestamp_minute=1
                ),
            ],
        )

        await coord._recover_engine_state()

        engine_key = "BTC-USD@kraken-live"
        assert engine_key in coord.engines
        engine = coord.engines[engine_key]
        shard = coord.trade_service._shards[engine._shard_key]

        assert engine.position_qty == pytest.approx(3.0)
        assert shard.position.position_qty == pytest.approx(3.0)
        expected_vwap = (1.0 * 100.0 + 2.0 * 110.0) / 3.0
        assert engine.entry_price == pytest.approx(expected_vwap)
        assert shard.position.entry_price == pytest.approx(expected_vwap)

    @pytest.mark.asyncio
    async def test_full_replay_rebuilds_net_base_fee_quantity(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Fresh immutable-ledger replay self-heals an overstated position.

        Given: The production-shaped EUR-PLN BUY ledger fill of 20.04 EUR with
            a 0.04 EUR base fee and no checkpoint,
        When: full recovery rebuilds the engine and TradeService projection,
        Then: both book the venue-received 20.00 EUR without mutating the fill.
        """
        coord = _make_coord(monkeypatch)
        execution = _make_execution(
            trade_id="eur-base-fee",
            side="buy",
            size=20.04,
            price=4.3836,
            instrument="EUR-PLN",
            exchange="walutomat",
            fee=0.04,
            fee_asset="EUR",
        )
        _stub_repo(coord, [execution])

        await coord._recover_engine_state()

        engine = coord.engines["EUR-PLN@walutomat-live"]
        shard = coord.trade_service._shards[engine._shard_key]
        assert engine.position_qty == pytest.approx(20.0)
        assert engine.portfolio.position_qty("EUR-PLN") == pytest.approx(20.0)
        assert shard.position.position_qty == pytest.approx(20.0)
        assert execution["size"] == pytest.approx(20.04)
        assert execution["fee"] == pytest.approx(0.04)

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("scenario", "executions", "expected_qty", "expected_entry"),
        [
            (
                "long_only",
                [
                    dict(trade_id="l1", side="buy", size=1.0, price=100.0, timestamp_minute=0),
                    dict(trade_id="l2", side="buy", size=1.0, price=120.0, timestamp_minute=1),
                ],
                2.0,
                110.0,
            ),
            (
                "short_only",
                [
                    dict(trade_id="s1", side="sell", size=1.0, price=100.0, timestamp_minute=0),
                    dict(trade_id="s2", side="sell", size=1.0, price=90.0, timestamp_minute=1),
                ],
                -2.0,
                95.0,
            ),
            (
                "scale_then_reduce",
                [
                    dict(trade_id="r1", side="buy", size=2.0, price=100.0, timestamp_minute=0),
                    dict(trade_id="r2", side="buy", size=2.0, price=110.0, timestamp_minute=1),
                    dict(trade_id="r3", side="sell", size=1.0, price=120.0, timestamp_minute=2),
                ],
                3.0,
                105.0,
            ),
        ],
    )
    async def test_full_replay_trade_service_matches_engine_after_recovery(
        self,
        monkeypatch: pytest.MonkeyPatch,
        scenario: str,
        executions: list[dict[str, Any]],
        expected_qty: float,
        expected_entry: float,
    ) -> None:
        """Engine and TradeService stay in lockstep across fill patterns.

        Given: a parametrised sequence of BUY/SELL executions that
            covers long-only, short-only, and scale-then-reduce flows,
        When: _recover_engine_state runs with no checkpoint,
        Then: engine.position_qty == shard.position.position_qty AND
            engine.entry_price == shard.position.entry_price within
            floating-point tolerance (1e-9). This pins the parity
            invariant that the previous bypass-TradeService path
            silently violated.
        """
        coord = _make_coord(monkeypatch)
        _stub_repo(coord, [_make_execution(**row) for row in executions])

        await coord._recover_engine_state()

        engine = coord.engines["BTC-USD@kraken-live"]
        shard = coord.trade_service._shards[engine._shard_key]

        assert engine.position_qty == pytest.approx(expected_qty, abs=_EPS), scenario
        assert shard.position.position_qty == pytest.approx(expected_qty, abs=_EPS), scenario
        assert engine.entry_price == pytest.approx(expected_entry, abs=_EPS), scenario
        assert shard.position.entry_price == pytest.approx(expected_entry, abs=_EPS), scenario
        assert abs(cast(float, engine.entry_price) - cast(float, shard.position.entry_price)) < _EPS

    @pytest.mark.asyncio
    async def test_full_replay_dedup_preserves_trade_ids(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A live fill carrying a replayed trade_id is deduped post-recovery.

        Given: a seeded execution with trade_id='trade-abc-123' replayed
            via full replay (no exec_id on ExecutionRow, so dedup keys
            on trade_id alone),
        When: a synthetic live venue event with the same trade_id is
            dispatched through TradeService.apply_venue_event,
        Then: the shard position does not double-count — the second
            event is rejected by _dedup_fill's seen_exec_ids check that
            was seeded during the replay.
        """
        coord = _make_coord(monkeypatch)
        _stub_repo(
            coord,
            [_make_execution(trade_id="trade-abc-123", side="buy", size=1.5, price=100.0)],
        )

        await coord._recover_engine_state()

        engine = coord.engines["BTC-USD@kraken-live"]
        shard_key = engine._shard_key
        shard = coord.trade_service._shards[shard_key]
        assert shard.position.position_qty == pytest.approx(1.5)
        assert "trade-abc-123" in shard.seen_exec_ids

        duplicate: VenueEventRow = {
            "id": shard.last_venue_event_id + 1,
            "public_id": "ve-dup",
            "timestamp": datetime(2026, 1, 1, 13, 0, tzinfo=UTC),
            "session_id": "s-live",
            "sequence_id": 999,
            "event_type": "fill_observed",
            "shard_key": shard_key,
            "command_public_id": None,
            "exchange": "kraken",
            "instrument": "BTC-USD",
            "mode": "live",
            "exchange_order_id": "ex-live",
            "client_order_id": "c-live",
            "venue_client_id": None,
            "side": "buy",
            "status": "filled",
            "fill_price": 100.0,
            "fill_size": 1.5,
            "cum_fill_size": None,
            "fee": 0.0,
            "fee_asset": "USD",
            "exec_id": None,
            "trade_id": "trade-abc-123",
            "error": None,
            "venue_timestamp": datetime(2026, 1, 1, 13, 0, tzinfo=UTC),
            "received_at": datetime(2026, 1, 1, 13, 0, tzinfo=UTC),
        }
        coord.trade_service.apply_venue_event(duplicate)

        assert shard.position.position_qty == pytest.approx(1.5)

    @pytest.mark.asyncio
    async def test_full_replay_skips_engines_recovered_from_checkpoint(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Full replay honours the checkpoint-recovered skip_keys set.

        Given: a checkpoint exists for shard A (BTC-USD) and executions
            exist for BOTH shard A and shard B (ETH-USD),
        When: _recover_engine_state runs,
        Then: shard A's engine is restored from the checkpoint path
            (engine.position_qty matches the checkpoint value, NOT a
            re-applied executions chain) AND shard B's engine is
            populated via full-replay (position_qty derived from its
            executions with both engine and TradeService in sync).
        """
        coord = _make_coord(monkeypatch)
        checkpoint = _make_checkpoint(
            shard_key="kraken.BTC-USD.live",
            position_qty=0.75,
            entry_price=48000.0,
        )
        shard_a_execution = _make_execution(
            trade_id="a1", side="buy", size=5.0, price=48000.0, instrument="BTC-USD"
        )
        shard_b_execution = _make_execution(
            trade_id="b1",
            side="buy",
            size=0.25,
            price=3000.0,
            instrument="ETH-USD",
            timestamp_minute=1,
        )

        repo = AsyncMock(spec=SQLAlchemyRepository)
        repo.get_all_checkpoints = AsyncMock(return_value=[checkpoint])
        repo.get_venue_events_after = AsyncMock(return_value=[])
        repo.get_executions_for_recovery = AsyncMock(
            return_value=[shard_a_execution, shard_b_execution]
        )
        repo.get_active_orders_for_recovery = AsyncMock(return_value=[])
        repo.ensure_instrument = AsyncMock(return_value=(1, "inst-pid"))
        coord.repository = repo

        await coord._recover_engine_state()

        shard_a_engine = coord.engines["BTC-USD@kraken-live"]
        shard_b_engine = coord.engines["ETH-USD@kraken-live"]
        assert shard_a_engine.position_qty == pytest.approx(0.75)
        assert shard_b_engine.position_qty == pytest.approx(0.25)

        shard_a = coord.trade_service._shards[shard_a_engine._shard_key]
        shard_b = coord.trade_service._shards[shard_b_engine._shard_key]
        assert shard_a.position.position_qty == pytest.approx(0.75)
        assert shard_b.position.position_qty == pytest.approx(0.25)
        assert shard_b.position.entry_price == pytest.approx(3000.0)
