"""Drop blank-wallet signals before persistence without stopping valid intake."""

import asyncio
from dataclasses import dataclass
from dataclasses import field
from datetime import UTC
from datetime import datetime
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import pytest

from snapper.application.engine.service import TradingEngineService
from snapper.application.engine.trader import TraderCoordinator
from snapper.config.app import AppSettings
from snapper.core.partitioning import ShardOwnership
from snapper.data.repository import SQLAlchemyRepository
from snapper.messaging.schemas.data import SignalData

_NOW = datetime(2026, 10, 1, 12, tzinfo=UTC)
_WALLET = "01975a8b-3c7d-7000-8000-abcdef123456"
_TOPIC = "signals.kraken.BTC-USD.live"
_BLANK = [pytest.param(None, id="omitted"), "", " ", "\t\n"]


def _signal(wallet: str | None, sequence: int) -> SignalData:
    """Create a complete grouped signal whose only questionable field is wallet."""
    return SignalData(
        public_id=f"signal-{sequence}",
        timestamp=_NOW,
        fired_at=_NOW,
        session_id=_WALLET,
        sequence_id=sequence,
        instrument="BTC-USD",
        exchange="kraken",
        side="buy",
        strength=0.5,
        reason="wallet refusal regression",
        price=100.0,
        strategy_name="wallet-test",
        wallet_public_id=wallet if wallet is not None else "",
        paired_group_id="group-1",
        paired_group_size=2,
        paired_group_index=0,
        paired_group_policy="simultaneous",
        paired_group_key="kraken:BTC-USD:live|kraken:ETH-USD:live",
    )


@dataclass
class _RoutingHarness:
    """Retain actual coordinator routing while replacing external write boundaries."""

    coordinator: TraderCoordinator
    repository: MagicMock
    engine: MagicMock
    engine_factory: AsyncMock


@pytest.fixture
def routing(monkeypatch: pytest.MonkeyPatch) -> _RoutingHarness:
    """Build a real coordinator with inert settings and persistence collaborators."""
    settings = MagicMock(spec=AppSettings)
    settings.db_url = "sqlite+aiosqlite:///:memory:"
    repository = MagicMock(spec=SQLAlchemyRepository)
    repository.get_active_paired_execution_halt = AsyncMock(return_value=None)
    repository.ensure_paired_execution_group = AsyncMock(return_value="group-1")
    monkeypatch.setattr("snapper.application.engine.trader.get_settings", lambda: settings)
    monkeypatch.setattr("snapper.application.engine.trader.get_repository", lambda _url: repository)
    monkeypatch.setattr(
        "snapper.application.engine.trader.is_tradeable", lambda _symbol, _venue: True
    )
    coordinator = TraderCoordinator()
    coordinator._current_topic = _TOPIC
    coordinator.execution_publisher = MagicMock()
    engine = MagicMock(spec=TradingEngineService)
    engine.pending_client_order_id = None
    engine.execute_desired_units = AsyncMock(return_value=None)
    engine_factory = AsyncMock(return_value=engine)
    coordinator._get_or_create_signal_engine = engine_factory
    return _RoutingHarness(coordinator, repository, engine, engine_factory)


@pytest.mark.parametrize("wallet", _BLANK)
async def test_blank_wallet_refuses_before_engine_or_group_work(
    routing: _RoutingHarness, wallet: str | None
) -> None:
    """Refuse only the blank wallet before any grouped signal write path.

    Given: An otherwise valid grouped signal whose wallet is omitted or blank,
    When: The actual signal handler and routing-context builder process it,
    Then: No halt lookup, engine construction, group persistence or execution occurs.
    """
    routing.engine_factory.side_effect = AssertionError("blank wallet reached engine creation")
    await routing.coordinator._on_signal(_signal(wallet, 1))
    routing.engine_factory.assert_not_awaited()
    routing.repository.get_active_paired_execution_halt.assert_not_awaited()
    routing.repository.ensure_paired_execution_group.assert_not_awaited()
    routing.engine.execute_desired_units.assert_not_awaited()
    assert routing.coordinator.engines == {}
    assert routing.coordinator.last_signal_time == {}


@pytest.mark.parametrize(
    "wallet",
    [
        _WALLET,
        _WALLET.upper(),
        _WALLET.replace("-", ""),
        "12345678-1234-4234-8234-123456789abc",
        "legacy-wallet",
        " padded-wallet ",
    ],
)
async def test_nonblank_wallet_routes_verbatim(routing: _RoutingHarness, wallet: str) -> None:
    """Preserve existing nonblank wallet spelling and grouped execution behavior.

    Given: A valid signal with canonical, alternate UUID or legacy wallet spelling,
    When: Actual routing and signal execution reach injected external boundaries,
    Then: The same wallet enters engine context and group persistence unchanged.
    """
    signal = _signal(wallet, 1)
    await routing.coordinator._on_signal(signal)
    routing.engine_factory.assert_awaited_once()
    context = routing.engine_factory.call_args.args[1]
    assert context.wallet_public_id == wallet
    routing.repository.ensure_paired_execution_group.assert_awaited_once()
    assert (
        routing.repository.ensure_paired_execution_group.call_args.args[0]["wallet_public_id"]
        == wallet
    )
    routing.engine.execute_desired_units.assert_awaited_once()
    args, kwargs = routing.engine.execute_desired_units.call_args
    assert args == (0.5, 100.0)
    assert kwargs["signal_public_id"] == signal.public_id
    assert kwargs["grouped_correlation_id"] == "group-1"


@dataclass
class _SignalStream:
    """Serve finite serialized frames then expose an explicitly cancellable wait."""

    frames: list[tuple[str, bytes]]
    blocked: asyncio.Event = field(default_factory=asyncio.Event)
    release: asyncio.Event = field(default_factory=asyncio.Event)

    async def recv_multipart(self) -> tuple[str, bytes]:
        """Deliver each frame once and rendezvous after the final frame."""
        if self.frames:
            return self.frames.pop(0)
        self.blocked.set()
        await self.release.wait()
        raise AssertionError("test stream unexpectedly resumed after its final frame")


@pytest.mark.parametrize("wallet", _BLANK)
async def test_blank_then_valid_signal_keeps_real_listener_alive(
    routing: _RoutingHarness, wallet: str | None
) -> None:
    """Expected blank input must not kill the next valid signal or hide cancellation.

    Given: A real listener receives a blank grouped signal then a valid grouped signal,
    When: Both frames pass through actual schema parsing and signal routing,
    Then: Only valid execution occurs and explicit listener cancellation still propagates.
    """
    invalid = _signal(wallet, 1)
    invalid_json = invalid.model_dump_json(
        exclude={"wallet_public_id"} if wallet is None else set()
    )
    valid = _signal(_WALLET, 2)
    stream = _SignalStream(
        [(_TOPIC, invalid_json.encode()), (_TOPIC, valid.model_dump_json().encode())]
    )
    subscriber = MagicMock()
    subscriber.recv_multipart = stream.recv_multipart
    routing.coordinator.signal_subscriber = subscriber
    listener = asyncio.create_task(routing.coordinator._listen_signals())
    reached_idle = asyncio.create_task(stream.blocked.wait())
    try:
        done, _ = await asyncio.wait(
            {listener, reached_idle}, timeout=1, return_when=asyncio.FIRST_COMPLETED
        )
        assert reached_idle in done, "listener exited before consuming the valid successor frame"
        assert not listener.done()
        routing.engine.execute_desired_units.assert_awaited_once()
        assert (
            routing.engine.execute_desired_units.call_args.kwargs["signal_public_id"]
            == valid.public_id
        )
        routing.engine_factory.assert_awaited_once()
        routing.repository.get_active_paired_execution_halt.assert_awaited_once()
        routing.repository.ensure_paired_execution_group.assert_awaited_once()
        assert (
            routing.repository.ensure_paired_execution_group.call_args.args[0]["wallet_public_id"]
            == _WALLET
        )
        listener.cancel()
        with pytest.raises(asyncio.CancelledError):
            await listener
    finally:
        listener.cancel()
        reached_idle.cancel()
        await asyncio.gather(listener, reached_idle, return_exceptions=True)


@pytest.mark.parametrize("earlier_refusal", ["payload", "paper-partition"])
def test_existing_routing_refusals_precede_wallet_guard(
    routing: _RoutingHarness, monkeypatch: pytest.MonkeyPatch, earlier_refusal: str
) -> None:
    """Retain established payload and paper-partition refusal priority.

    Given: A blank-wallet signal also violates a preexisting routing gate,
    When: The actual routing-context builder evaluates it,
    Then: That earlier refusal is reported before the new wallet guard.
    """
    logs = MagicMock()
    monkeypatch.setattr("snapper.application.engine.trader.logger", logs)
    signal = _signal(" ", 1)
    if earlier_refusal == "payload":
        signal = signal.model_copy(update={"price": 0.0})
    else:
        signal = signal.model_copy(update={"exchange": "paper"})
        routing.coordinator._current_topic = "signals.paper.BTC-USD.wallet-test"
        routing.coordinator._ownership = ShardOwnership(instance_id=0, instance_count=2)
    assert routing.coordinator._build_signal_routing_context(signal) is None
    if earlier_refusal == "payload":
        logs.warning.assert_called_once()
        assert "Invalid signal" in logs.warning.call_args.args[0]
        logs.error.assert_not_called()
    else:
        logs.error.assert_called_once()
        assert "N>1 partitioning" in logs.error.call_args.args[0]
        logs.warning.assert_not_called()
