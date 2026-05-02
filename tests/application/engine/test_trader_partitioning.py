"""Tests for :class:`TraderCoordinator` shard-ownership wiring.

Covers:
    - :meth:`TraderCoordinator._build_ownership` validation contract.
    - :meth:`TraderCoordinator._on_signal` drops foreign shards.
    - BOTH engine construction paths populate ``engine._ownership``
      (signal at :2243 + recovery at :1055).
    - :meth:`TraderCoordinator._dispatch_order_event` CID guard
      matrix: empty CID under N>1 → drop, unknown CID under N>1 →
      drop, unknown CID under N=1 → fallthrough, foreign shard under
      any N → drop.

Uses the ``settings=`` kwarg on
:class:`TraderCoordinator.__init__` to inject per-coordinator
ownership.
"""

from datetime import UTC
from datetime import datetime
from types import SimpleNamespace
from typing import Any
from typing import cast
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import Mock

import pytest

import snapper.application.engine.trader as trader_module
from snapper.application.engine.trader import TraderCoordinator
from snapper.config.app import AppSettings
from snapper.core.partitioning import ShardOwnership
from snapper.data.repository import SQLAlchemyRepository
from snapper.messaging.schemas.data import OrderEventData
from snapper.messaging.schemas.data import SignalData


def _build_injected_settings(instance_id: int = 0, instance_count: int = 1) -> SimpleNamespace:
    """Build an AppSettings-shaped SimpleNamespace for test injection.

    Populates every field ``TraderCoordinator.start()``
    (and ``_build_ownership``) will read through its settings object.
    """
    return SimpleNamespace(
        db_url="sqlite:///:memory:",
        zmq_broker_xsub="tcp://broker.xsub",
        zmq_broker_xpub="tcp://broker.xpub",
        risk_r_per_trade=0.01,
        risk_max_leverage=2.0,
        risk_max_drawdown=0.15,
        has_db_access=True,
        coordinator_instance_id=instance_id,
        coordinator_instance_count=instance_count,
        coordinator_outbox_max_scan_rows=1000,
    )


def _make_coordinator_with_ownership(
    monkeypatch: pytest.MonkeyPatch,
    *,
    instance_id: int = 0,
    instance_count: int = 1,
) -> TraderCoordinator:
    """Build a coordinator with `_ownership` pre-populated (no ``start()``).

    Shortcut for tests that exercise ``_on_signal`` /
    ``_dispatch_order_event`` without running the full startup
    pipeline. Mirrors what ``start()`` would have produced.
    """
    settings = _build_injected_settings(instance_id, instance_count)
    mock_repo = AsyncMock()
    mock_repo.ensure_instrument = AsyncMock(return_value=(1, "inst-pid"))
    monkeypatch.setattr(trader_module, "get_repository", lambda _url: mock_repo, raising=True)
    monkeypatch.setattr(trader_module, "is_tradeable", lambda _i, _e: True)
    monkeypatch.setattr(
        trader_module,
        "resolve_symbol_public_id",
        AsyncMock(return_value="stub-spid"),
    )
    coord = TraderCoordinator(settings=cast(AppSettings, settings))
    coord._ownership = ShardOwnership(instance_id=instance_id, instance_count=instance_count)
    coord.msg_publisher = cast(Any, MagicMock(tracker=Mock(session_id="s1")))
    coord.execution_publisher = MagicMock()
    return coord


class TestInitializeSettingsInjectedBranch:
    """``_initialize_settings`` — test-injection path bypasses DB service."""

    @pytest.mark.asyncio
    async def test_injected_settings_short_circuits_db_upgrade(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """With ``settings=`` kwarg, DB service upgrade is skipped.

        The  contract requires ``_initialize_settings`` to
        take the injected-settings branch when ``self._injected_settings``
        is non-None: ``self.settings`` becomes the injected object, the
        DB-backed ``get_settings_with_service`` path is NOT taken, and
        ``get_settings_service`` is NEVER awaited.
        """
        injected = _build_injected_settings(instance_id=1, instance_count=3)
        monkeypatch.setattr(
            trader_module,
            "get_repository",
            lambda _url: AsyncMock(spec=SQLAlchemyRepository),
        )
        mock_service_factory = AsyncMock(
            side_effect=AssertionError(
                "get_settings_service must not be called on the injected path"
            )
        )
        monkeypatch.setattr(trader_module, "get_settings_service", mock_service_factory)
        coord = TraderCoordinator(settings=cast(AppSettings, injected))
        monkeypatch.setattr(coord, "_build_wallet_short_cache", AsyncMock(return_value=None))
        await coord._initialize_settings()
        assert coord.settings is injected
        mock_service_factory.assert_not_called()


class TestBuildOwnership:
    """``_build_ownership`` derives ``ShardOwnership`` from settings."""

    def test_default_single_instance(self) -> None:
        """Default settings (0/1) produce a valid single-instance ownership."""
        coord = TraderCoordinator(settings=cast(AppSettings, _build_injected_settings(0, 1)))
        coord.settings = cast(AppSettings, coord._injected_settings)
        ownership = coord._build_ownership()
        assert ownership.instance_id == 0
        assert ownership.instance_count == 1

    def test_multi_instance_values_threaded(self) -> None:
        """Injected (2, 4) is threaded verbatim into :class:`ShardOwnership`."""
        coord = TraderCoordinator(settings=cast(AppSettings, _build_injected_settings(2, 4)))
        coord.settings = cast(AppSettings, coord._injected_settings)
        ownership = coord._build_ownership()
        assert ownership.instance_id == 2
        assert ownership.instance_count == 4

    def test_zero_instance_count_raises(self) -> None:
        """``instance_count < 1`` fails fast."""
        coord = TraderCoordinator(settings=cast(AppSettings, _build_injected_settings(0, 0)))
        coord.settings = cast(AppSettings, coord._injected_settings)
        with pytest.raises(ValueError, match="instance_count must be >= 1"):
            coord._build_ownership()

    def test_instance_id_out_of_range_raises(self) -> None:
        """``instance_id >= instance_count`` fails fast."""
        coord = TraderCoordinator(settings=cast(AppSettings, _build_injected_settings(5, 2)))
        coord.settings = cast(AppSettings, coord._injected_settings)
        with pytest.raises(ValueError, match="instance_id 5 out of range"):
            coord._build_ownership()

    def test_negative_instance_id_raises(self) -> None:
        """Negative ``instance_id`` fails fast."""
        coord = TraderCoordinator(settings=cast(AppSettings, _build_injected_settings(-1, 2)))
        coord.settings = cast(AppSettings, coord._injected_settings)
        with pytest.raises(ValueError, match="instance_id -1 out of range"):
            coord._build_ownership()


class TestOnSignalOwnershipFilter:
    """``_on_signal`` drops signals for foreign shards at N>1."""

    def _signal(self, instrument: str = "BTC-USD") -> SignalData:
        """Build a valid signal for the test matrix."""
        return SignalData(
            type="signal",
            public_id="sig-1",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            session_id="",
            sequence_id=0,
            instrument=instrument,
            exchange="kraken",
            side="buy",
            strength=0.5,
            reason="test",
            price=50000.0,
            strategy_name="test",
            fired_at=datetime.now(UTC),
        )

    @pytest.mark.asyncio
    async def test_owned_shard_creates_engine(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """At N=1 every shard is owned → engine created as before."""
        coord = _make_coordinator_with_ownership(monkeypatch, instance_id=0, instance_count=1)
        coord._current_topic = "signals.kraken.BTC-USD.live"
        await coord._on_signal(self._signal())
        assert len(coord.engines) == 1

    @pytest.mark.asyncio
    async def test_foreign_shard_drops_signal(
        self, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
    ) -> None:
        """At N=2 a non-owned shard produces NO engine + debug log."""
        shard_key = "kraken.BTC-USD.live"
        hash_val = ShardOwnership._hash(shard_key)
        foreign_id = 0 if hash_val % 2 == 1 else 1
        coord = _make_coordinator_with_ownership(
            monkeypatch, instance_id=foreign_id, instance_count=2
        )
        coord._current_topic = "signals.kraken.BTC-USD.live"
        await coord._on_signal(self._signal())
        assert coord.engines == {}

    @pytest.mark.asyncio
    async def test_owned_shard_at_n2_creates_engine_with_ownership(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Owned shard under N=2 → engine gets ``self._ownership`` plumbed."""
        shard_key = "kraken.BTC-USD.live"
        hash_val = ShardOwnership._hash(shard_key)
        owner_id = hash_val % 2
        coord = _make_coordinator_with_ownership(
            monkeypatch, instance_id=owner_id, instance_count=2
        )
        coord._current_topic = "signals.kraken.BTC-USD.live"
        await coord._on_signal(self._signal())
        assert len(coord.engines) == 1
        engine = next(iter(coord.engines.values()))
        assert engine._ownership is not None
        assert engine._ownership.instance_id == owner_id
        assert engine._ownership.instance_count == 2

    @pytest.mark.asyncio
    async def test_halt_guard_uses_canonical_shard_key_on_cold_path(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Halt check for a brand-new signal uses the same shard_key as engine creation.

        Regression guard: Copilot R1 review of  flagged that
        ``halt_key`` on the no-engine path previously used
        ``parsed.signal_type`` (i.e., the strategy tag for paper
        signals) instead of the canonical execution mode. For a paper
        signal like ``signals.paper.BTC-USD.momentum`` that would
        produce ``paper.BTC-USD.momentum`` while the actual shard_key
        is ``paper.BTC-USD.paper.momentum`` — letting a halted shard
        bypass :meth:`TradeService.is_halted` and proceed to engine
        construction.
        """
        coord = _make_coordinator_with_ownership(monkeypatch, instance_id=0, instance_count=1)
        coord._current_topic = "signals.paper.BTC-USD.momentum"
        expected_shard_key = "paper.BTC-USD.paper.momentum"
        coord.trade_service.halt_shard(expected_shard_key, reason="test")
        paper_signal = SignalData(
            type="signal",
            public_id="sig-halt",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            session_id="",
            sequence_id=0,
            instrument="BTC-USD",
            exchange="paper",
            side="buy",
            strength=0.5,
            reason="test",
            price=50000.0,
            strategy_name="test",
            fired_at=datetime.now(UTC),
        )
        await coord._on_signal(paper_signal)
        assert coord.engines == {}

    @pytest.mark.asyncio
    async def test_n_equals_one_preserves_pre_phase4_behavior(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """At N=1 ALL signals are owned → never-drop invariant."""
        coord = _make_coordinator_with_ownership(monkeypatch, instance_id=0, instance_count=1)
        for suffix in ("A", "B", "C", "D", "E"):
            coord._current_topic = f"signals.kraken.BTC-USD-{suffix}.live"
            await coord._on_signal(self._signal(instrument=f"BTC-USD-{suffix}"))
        assert len(coord.engines) == 5


class TestDispatchOrderEventCIDGuard:
    """``_dispatch_order_event`` — CID edge-case matrix."""

    def _order_event(self, cid: str) -> OrderEventData:
        """Build a minimal :class:`OrderEventData` carrying ``cid``."""
        return OrderEventData(
            type="order_event",
            public_id="oe-1",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            session_id="",
            sequence_id=0,
            event="submitted",
            client_order_id=cid,
            exchange_order_id="x-1",
            instrument="BTC-USD",
            exchange="kraken",
        )

    @pytest.mark.asyncio
    async def test_empty_cid_under_n2_dropped(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Empty CID under N>1 is dropped — cannot route deterministically."""
        coord = _make_coordinator_with_ownership(monkeypatch, instance_id=0, instance_count=2)
        handled_event = MagicMock()
        handled_status = MagicMock()
        handled_execution = AsyncMock()
        monkeypatch.setattr(coord, "_handle_order_event", handled_event)
        monkeypatch.setattr(coord, "_handle_order_status", handled_status)
        monkeypatch.setattr(coord, "_handle_execution_fill", handled_execution)
        msg = self._order_event(cid="")
        payload = msg.to_json().encode("utf-8")
        await coord._dispatch_order_event("orders.events.kraken.BTC-USD", payload)
        assert handled_event.call_count == 0
        assert handled_status.call_count == 0
        assert handled_execution.call_count == 0

    @pytest.mark.asyncio
    async def test_unknown_cid_under_n2_dropped(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Unknown CID under N>1 is dropped — belongs to another instance."""
        coord = _make_coordinator_with_ownership(monkeypatch, instance_id=0, instance_count=2)
        handled_event = MagicMock()
        monkeypatch.setattr(coord, "_handle_order_event", handled_event)
        msg = self._order_event(cid="unknown-cid")
        payload = msg.to_json().encode("utf-8")
        await coord._dispatch_order_event("orders.events.kraken.BTC-USD", payload)
        assert handled_event.call_count == 0

    @pytest.mark.asyncio
    async def test_unknown_cid_under_n1_fallsthrough(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Unknown CID under N=1 falls through to the handler (pre-Phase-4 behavior)."""
        coord = _make_coordinator_with_ownership(monkeypatch, instance_id=0, instance_count=1)
        handled_event = MagicMock()
        monkeypatch.setattr(coord, "_handle_order_event", handled_event)
        msg = self._order_event(cid="unknown-cid")
        payload = msg.to_json().encode("utf-8")
        await coord._dispatch_order_event("orders.events.kraken.BTC-USD", payload)
        assert handled_event.call_count == 1

    @pytest.mark.asyncio
    async def test_known_cid_on_foreign_shard_dropped(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Known CID pointing at a foreign shard is dropped."""
        foreign_shard = "kraken.FOREIGN.live"
        hash_val = ShardOwnership._hash(foreign_shard)
        owner_id = 1 - (hash_val % 2)
        coord = _make_coordinator_with_ownership(
            monkeypatch, instance_id=owner_id, instance_count=2
        )
        coord._order_shard_keys["cid-foreign"] = foreign_shard
        handled_event = MagicMock()
        monkeypatch.setattr(coord, "_handle_order_event", handled_event)
        msg = self._order_event(cid="cid-foreign")
        payload = msg.to_json().encode("utf-8")
        await coord._dispatch_order_event("orders.events.kraken.FOREIGN", payload)
        assert handled_event.call_count == 0

    @pytest.mark.asyncio
    async def test_known_cid_on_owned_shard_dispatches(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Known CID on an owned shard is dispatched to the handler."""
        owned_shard = "kraken.OWNED.live"
        hash_val = ShardOwnership._hash(owned_shard)
        owner_id = hash_val % 2
        coord = _make_coordinator_with_ownership(
            monkeypatch, instance_id=owner_id, instance_count=2
        )
        coord._order_shard_keys["cid-owned"] = owned_shard
        handled_event = MagicMock()
        monkeypatch.setattr(coord, "_handle_order_event", handled_event)
        msg = self._order_event(cid="cid-owned")
        payload = msg.to_json().encode("utf-8")
        await coord._dispatch_order_event("orders.events.kraken.OWNED", payload)
        assert handled_event.call_count == 1


def _make_checkpoint_row(shard_key: str) -> dict[str, Any]:
    """Minimal checkpoint row for recovery ownership filter tests.

    Uses ``last_venue_event_id=None`` so the checkpoint flow takes the
    early-return path (falls back to full replay). This keeps the
    fixture small while still exercising the ownership-filter guard
    at the top of :meth:`_recover_from_checkpoints`.
    """
    return {
        "public_id": "cp-1",
        "shard_key": shard_key,
        "position_qty": 0.0,
        "entry_price": None,
        "position_opened_at": None,
        "cash": 10000.0,
        "peak_equity": 10000.0,
        "realized_pnl": 0.0,
        "turnover": 0.0,
        "last_venue_event_id": None,
        "last_venue_event_at": datetime(2024, 6, 1, tzinfo=UTC),
        "open_command_ids": None,
        "seen_exec_ids": "[]",
        "checkpoint_at": datetime(2024, 6, 1, tzinfo=UTC),
        "session_id": "s-test",
        "operator_public_id": None,
    }


def _make_execution_row(
    instrument: str = "BTC-USD",
    exchange: str = "kraken",
) -> dict[str, Any]:
    """Minimal execution row — only fields ``_recover_from_executions`` reads."""
    return {
        "public_id": "ex-1",
        "timestamp": datetime(2024, 1, 1, tzinfo=UTC),
        "session_id": "s-test",
        "sequence_id": 0,
        "trade_id": "t1",
        "exchange_order_id": "x-1",
        "client_order_id": "cid-1",
        "instrument": instrument,
        "exchange": exchange,
        "side": "buy",
        "size": 0.1,
        "price": 50000.0,
        "fee": 0.5,
        "fee_asset": "USD",
        "status": "filled",
        "executed_at": datetime(2024, 1, 1, tzinfo=UTC),
        "wallet_public_id": "",
        "operator_public_id": None,
    }


def _make_order_row(
    instrument: str = "BTC-USD",
    exchange: str = "kraken",
) -> dict[str, Any]:
    """Minimal active-order row — only fields ``_recover_active_orders`` reads."""
    return {
        "public_id": "o-1",
        "timestamp": datetime(2024, 1, 1, tzinfo=UTC),
        "session_id": "s-test",
        "sequence_id": 0,
        "instrument": instrument,
        "exchange": exchange,
        "mode": "live",
        "client_order_id": "cid-1",
        "exchange_order_id": None,
        "created_at": datetime(2024, 1, 1, tzinfo=UTC),
        "updated_at": None,
        "side": "buy",
        "order_type": "market",
        "price": None,
        "size": 0.1,
        "filled_size": 0.0,
        "average_price": None,
        "status": "submitted",
        "time_in_force": None,
        "error": None,
        "leverage": None,
        "reduce_only": False,
        "wallet_public_id": "",
        "operator_public_id": None,
    }


class TestRecoveryOwnershipFilters:
    """``_recover_engine_state`` — per-sub-method filters."""

    @pytest.mark.asyncio
    async def test_checkpoint_foreign_shard_skipped_under_n2(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Foreign-shard checkpoints are skipped under N>1."""
        foreign_shard = "kraken.FOREIGN.live"
        hash_val = ShardOwnership._hash(foreign_shard)
        owner_id = 1 - (hash_val % 2)
        coord = _make_coordinator_with_ownership(
            monkeypatch, instance_id=owner_id, instance_count=2
        )

        mock_repo = AsyncMock(spec=SQLAlchemyRepository)
        mock_repo.get_all_checkpoints = AsyncMock(
            return_value=[_make_checkpoint_row(foreign_shard)]
        )
        mock_repo.get_executions_for_recovery = AsyncMock(return_value=[])
        mock_repo.get_active_orders_for_recovery = AsyncMock(return_value=[])
        mock_repo.ensure_instrument = AsyncMock(return_value=(1, "inst-pid"))
        coord.repository = cast(Any, mock_repo)
        await coord._recover_engine_state()
        assert coord.engines == {}

    @pytest.mark.asyncio
    async def test_execution_paper_skipped_under_n2(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Under N>1, paper executions are skipped (strategy_tag unavailable).

        ExecutionRow has no ``strategy_tag`` so the runtime-correct
        paper shard_key is unrecoverable. Paper under N>1 relies on
        checkpoints (sub-method 1).
        """
        coord = _make_coordinator_with_ownership(monkeypatch, instance_id=0, instance_count=2)
        mock_repo = AsyncMock(spec=SQLAlchemyRepository)
        mock_repo.get_all_checkpoints = AsyncMock(return_value=[])
        mock_repo.get_executions_for_recovery = AsyncMock(
            return_value=[_make_execution_row(exchange="paper")]
        )
        mock_repo.get_active_orders_for_recovery = AsyncMock(return_value=[])
        coord.repository = cast(Any, mock_repo)
        await coord._recover_engine_state()
        assert coord.engines == {}

    @pytest.mark.asyncio
    async def test_active_order_paper_skipped_under_n2(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Under N>1, paper active orders are skipped (symmetric with executions)."""
        coord = _make_coordinator_with_ownership(monkeypatch, instance_id=0, instance_count=2)
        mock_repo = AsyncMock(spec=SQLAlchemyRepository)
        mock_repo.get_all_checkpoints = AsyncMock(return_value=[])
        mock_repo.get_executions_for_recovery = AsyncMock(return_value=[])
        mock_repo.get_active_orders_for_recovery = AsyncMock(
            return_value=[_make_order_row(exchange="paper")]
        )
        coord.repository = cast(Any, mock_repo)
        await coord._recover_engine_state()
        assert coord.engines == {}

    @pytest.mark.asyncio
    async def test_active_order_live_owned_shard_recovered_under_n2(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Live active order for an owned shard passes the filter under N>1."""
        owned_shard = "kraken.OWNED.live"
        hash_val = ShardOwnership._hash(owned_shard)
        owner_id = hash_val % 2
        coord = _make_coordinator_with_ownership(
            monkeypatch, instance_id=owner_id, instance_count=2
        )
        mock_repo = AsyncMock(spec=SQLAlchemyRepository)
        mock_repo.get_all_checkpoints = AsyncMock(return_value=[])
        mock_repo.get_executions_for_recovery = AsyncMock(return_value=[])
        mock_repo.get_active_orders_for_recovery = AsyncMock(
            return_value=[_make_order_row(instrument="OWNED", exchange="kraken")]
        )
        coord.repository = cast(Any, mock_repo)
        await coord._recover_engine_state()
        assert "OWNED@kraken-live" in coord.engines

    @pytest.mark.asyncio
    async def test_active_order_live_foreign_shard_skipped_under_n2(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Live active order for a foreign shard is skipped under N>1."""
        foreign_shard = "kraken.FOREIGN.live"
        hash_val = ShardOwnership._hash(foreign_shard)
        owner_id = 1 - (hash_val % 2)
        coord = _make_coordinator_with_ownership(
            monkeypatch, instance_id=owner_id, instance_count=2
        )
        mock_repo = AsyncMock(spec=SQLAlchemyRepository)
        mock_repo.get_all_checkpoints = AsyncMock(return_value=[])
        mock_repo.get_executions_for_recovery = AsyncMock(return_value=[])
        mock_repo.get_active_orders_for_recovery = AsyncMock(
            return_value=[_make_order_row(instrument="FOREIGN", exchange="kraken")]
        )
        coord.repository = cast(Any, mock_repo)
        await coord._recover_engine_state()
        assert coord.engines == {}

    @pytest.mark.asyncio
    async def test_execution_live_foreign_shard_skipped_under_n2(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Live execution for a foreign-shard is skipped under N>1."""
        foreign_shard = "kraken.FOREIGN.live"
        hash_val = ShardOwnership._hash(foreign_shard)
        owner_id = 1 - (hash_val % 2)
        coord = _make_coordinator_with_ownership(
            monkeypatch, instance_id=owner_id, instance_count=2
        )
        mock_repo = AsyncMock(spec=SQLAlchemyRepository)
        mock_repo.get_all_checkpoints = AsyncMock(return_value=[])
        mock_repo.get_executions_for_recovery = AsyncMock(
            return_value=[_make_execution_row(instrument="FOREIGN", exchange="kraken")]
        )
        mock_repo.get_active_orders_for_recovery = AsyncMock(return_value=[])
        coord.repository = cast(Any, mock_repo)
        await coord._recover_engine_state()
        assert coord.engines == {}
