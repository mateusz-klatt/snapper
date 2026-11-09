"""Manual commands acquire truthful owners before dispatch and fail within scope."""

import asyncio
from copy import deepcopy
from datetime import UTC
from datetime import datetime
from typing import cast
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import pytest

import snapper.application.engine.trader as trader_module
from snapper.application.engine.service import TradingEngineService
from snapper.application.engine.service import compute_shard_key
from snapper.application.engine.trader import TraderCoordinator
from snapper.core.partitioning import ShardOwnership
from snapper.core.types import ExecutionModeEnum
from snapper.core.types import OrderExchange
from snapper.data.repository_types import TradeCommandRow
from snapper.messaging.infrastructure.publisher import MessagePublisher
from snapper.messaging.infrastructure.publisher import SequenceTracker
from snapper.messaging.schemas.data import ExecutionData
from snapper.messaging.schemas.data import OrderCancelData
from snapper.messaging.schemas.data import OrderData
from snapper.messaging.schemas.data import OrderRequestData
from snapper.messaging.schemas.data import SignalData

WALLET = "019aaa00-0000-7000-8000-123456789abc"
SUFFIX_TWIN = "019bbb00-0000-7000-8000-123456789abc"
OTHER_WALLET = "019ccc00-0000-7000-8000-987654321def"
NOW = datetime(2026, 11, 9, 12, tzinfo=UTC)


@pytest.fixture
def coordinator(monkeypatch: pytest.MonkeyPatch) -> tuple[TraderCoordinator, MagicMock]:
    """Wire real engines and projections without external persistence or transport."""
    settings = MagicMock(
        db_url="sqlite:///:memory:",
        risk_r_per_trade=0.01,
        risk_max_leverage=2.0,
        risk_max_drawdown=0.15,
    )
    monkeypatch.setattr(trader_module, "get_settings", lambda: settings)
    repository = MagicMock(
        get_open_position_cycle=AsyncMock(return_value=None),
        get_instrument_public_id_by_symbol=AsyncMock(return_value=None),
    )
    monkeypatch.setattr(trader_module, "get_repository", lambda _url: repository)
    monkeypatch.setattr(trader_module, "is_tradeable", MagicMock(return_value=True))
    coord = TraderCoordinator()
    publisher = MagicMock(tracker=SequenceTracker(), send=AsyncMock())
    coord.msg_publisher = cast(MessagePublisher, publisher)
    monkeypatch.setattr(coord, "_ensure_instrument", AsyncMock())
    monkeypatch.setattr(coord, "_resolve_instrument_specs", AsyncMock(return_value={}))
    return coord, publisher


def _command(
    instrument: str = "XRP/EUR", exchange: OrderExchange = "kraken", wallet: str = WALLET
) -> TradeCommandRow:
    """Build one complete manual command with a canonical full-wallet shard."""
    mode = ExecutionModeEnum.PAPER if exchange == "paper" else ExecutionModeEnum.LIVE
    return {
        "public_id": f"manual-command-{instrument}",
        "timestamp": NOW,
        "session_id": "manual-session",
        "sequence_id": 1,
        "command_type": "create",
        "shard_key": compute_shard_key(
            instrument=instrument,
            exchange=exchange,
            mode=mode,
            wallet_public_id=wallet,
            strategy_tag=None,
        ),
        "exchange": exchange,
        "instrument": instrument,
        "mode": mode,
        "strategy_id": "manual",
        "client_order_id": f"manual-client-{instrument}",
        "venue_client_id": f"manual-client-{instrument}",
        "idempotency_key": None,
        "side": "buy",
        "order_type": "market",
        "quantity": 4.07,
        "price": None,
        "stop_price": None,
        "leverage": None,
        "reduce_only": False,
        "post_only": False,
        "status": "created",
        "attempt_count": 0,
        "last_error": None,
        "created_at": NOW,
        "dispatched_at": None,
        "acked_at": None,
        "terminal_at": None,
        "exchange_order_id": "venue-manual-order",
        "supersedes_command_id": None,
        "correlation_id": "manual-correlation",
        "wallet_public_id": wallet or None,
        "operator_public_id": "manual-operator",
        "user_public_id": "manual-user",
        "source_surface": "rest",
        "plan_public_id": "manual-plan",
    }


def _fill(cmd: TradeCommandRow) -> ExecutionData:
    """Represent the incident's completed spot buy with an explicit delta."""
    return ExecutionData(
        public_id="manual-execution",
        timestamp=NOW,
        session_id="executor-session",
        sequence_id=2,
        trade_id="manual-native-trade",
        exchange_order_id="venue-manual-order",
        client_order_id=cmd["client_order_id"],
        exchange=cast(OrderExchange, cmd["exchange"]),
        instrument=cmd["instrument"],
        side="buy",
        size=4.07,
        price=2.1,
        last_size=4.07,
        last_price=2.1,
        fee=0.01,
        fee_asset="EUR",
        status="filled",
        executed_at=NOW,
        wallet_public_id=cmd["wallet_public_id"] or "",
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("exchange", ["kraken", "paper"])
@pytest.mark.parametrize("command_type", ["create", "submit"])
@pytest.mark.parametrize("wallet", ["", WALLET])
async def test_manual_fill_reaches_owner_installed_before_publish(
    coordinator: tuple[TraderCoordinator, MagicMock],
    exchange: OrderExchange,
    command_type: str,
    wallet: str,
) -> None:
    """A dispatched manual fill books once into its actual engine and shadow state.

    Given: A manual command with no existing engine.
    When: The command publishes and its completed fill arrives twice.
    Then: Its exact owner exists before publication and both position stores book once.
    """
    coord, publisher = coordinator
    cmd = _command(exchange=exchange, wallet=wallet)
    cmd["command_type"] = command_type

    async def observe_publish(topic: str, payload: object) -> None:
        """Assert ownership exists at the first point the venue could see the order."""
        assert topic == f"orders.commands.{exchange}.XRP/EUR.submit"
        assert isinstance(payload, OrderRequestData)
        owner = coord._find_engine_for_fill(_fill(cmd))
        assert owner is not None
        assert owner._shard_key == cmd["shard_key"]
        assert owner.wallet_public_id == wallet

    publisher.send.side_effect = observe_publish
    await coord._outbox_publish(cmd)
    fill = _fill(cmd)
    topic = f"orders.events.{exchange}.XRP/EUR.executed"
    await coord._handle_execution_fill(topic, fill)
    await coord._handle_execution_fill(topic, fill)
    owner = coord._find_engine_for_fill(fill)
    assert owner is not None
    assert owner.position_qty == pytest.approx(4.07)
    assert owner.entry_price == pytest.approx(2.1)
    assert owner.operator_public_id == "manual-operator"
    assert coord.trade_service.get_position(cmd["shard_key"]).position_qty == pytest.approx(4.07)
    assert coord._recovery_certification_failed is False
    publisher.send.assert_awaited_once()


@pytest.mark.asyncio
async def test_manual_dispatch_preserves_existing_owner_state(
    coordinator: tuple[TraderCoordinator, MagicMock],
) -> None:
    """A second manual order keeps the incumbent position and another pending intent.

    Given: An existing owner with a position and another pending client id.
    When: A manual command dispatches into the same shard.
    Then: The owner, position, entry price, and pending client id remain intact.
    """
    coord, _publisher = coordinator
    cmd = _command()
    await coord._outbox_publish(cmd)
    owner = coord._find_engine_for_fill(_fill(cmd))
    assert owner is not None
    owner.position_qty = 1.5
    owner.entry_price = 2.0
    owner.rearm_pending_intent("other-pending-order")
    await coord._outbox_publish(cmd)
    assert coord._find_engine_for_fill(_fill(cmd)) is owner
    assert owner.position_qty == 1.5
    assert owner.entry_price == 2.0
    assert owner.pending_client_order_id == "other-pending-order"


@pytest.mark.asyncio
@pytest.mark.parametrize("mapped", [False, True])
@pytest.mark.parametrize("wallet", ["", WALLET])
async def test_unowned_fill_refuses_only_its_instrument(
    coordinator: tuple[TraderCoordinator, MagicMock], mapped: bool, wallet: str
) -> None:
    """Missing topology blocks the affected instrument but permits another create and cancel.

    Given: A walleted or legacy fill whose engine is absent.
    When: The fill arrives with or without a dispatched shard mapping.
    Then: Only its instrument refuses creates while unrelated creates and cancels publish.
    """
    coord, publisher = coordinator
    cmd = _command(wallet=wallet)
    if mapped:
        coord._register_order_shard_key(cmd["client_order_id"], cmd["shard_key"], wallet)
    await coord._handle_execution_fill("orders.events.kraken.XRP/EUR.executed", _fill(cmd))
    assert coord._unowned_fill_scopes == {("kraken", "XRP/EUR")}
    assert coord._recovery_certification_failed is False
    assert coord.trade_service.known_shard_keys() == set()
    with pytest.raises(RuntimeError, match="instrument has an unowned fill"):
        await coord._outbox_publish(cmd)
    publisher.send.assert_not_awaited()
    await coord._outbox_publish(_command("BTC-USD"))
    cmd["command_type"] = "cancel"
    await coord._outbox_publish(cmd)
    assert publisher.send.await_count == 2
    assert isinstance(publisher.send.await_args.args[1], OrderCancelData)


@pytest.mark.asyncio
async def test_unowned_instrument_stops_signal_before_engine_work(
    coordinator: tuple[TraderCoordinator, MagicMock], monkeypatch: pytest.MonkeyPatch
) -> None:
    """Strategy creates consult the same scoped refusal as manual outbox creates.

    Given: An instrument quarantined because its fill has no owner.
    When: Strategy signals arrive for that instrument and an unrelated instrument.
    Then: Only the unrelated signal proceeds into ownership routing.
    """
    coord, _publisher = coordinator
    coord._unowned_fill_scopes.add(("kraken", "XRP/EUR"))
    foreign_guard = MagicMock(return_value=True)
    monkeypatch.setattr(coord, "_should_drop_signal_for_foreign_shard", foreign_guard)
    signal = SignalData(
        public_id="signal-xrp",
        timestamp=NOW,
        session_id="signal-session",
        sequence_id=1,
        fired_at=NOW,
        instrument="XRP/EUR",
        exchange="kraken",
        side="buy",
        strength=1.0,
        reason="manual-owner regression",
        price=2.1,
        wallet_public_id=WALLET,
    )
    coord._current_topic = "signals.kraken.XRP/EUR.live"
    await coord._on_signal(signal)
    foreign_guard.assert_not_called()
    coord._current_topic = "signals.kraken.BTC-USD.live"
    await coord._on_signal(signal.model_copy(update={"instrument": "BTC-USD"}))
    foreign_guard.assert_called_once()
    assert coord.engines == {}


@pytest.mark.asyncio
async def test_unowned_fill_refuses_instrument_projection_only(
    coordinator: tuple[TraderCoordinator, MagicMock],
) -> None:
    """A valid sibling instrument remains certifiable after the missing-owner fill.

    Given: A booked Bitcoin position and an unowned XRP fill.
    When: Projection certification evaluates each instrument's registered components.
    Then: XRP is refused while the materialized Bitcoin position remains certifiable.
    """
    coord, _publisher = coordinator
    cmd = _command("BTC-USD")
    await coord._outbox_publish(cmd)
    await coord._handle_execution_fill("orders.events.kraken.BTC-USD.executed", _fill(cmd))
    btc_identity = ("instrument-btc", "live", WALLET)
    xrp_identity = ("instrument-xrp", "live", WALLET)
    coord._projection_identities[cmd["shard_key"]] = btc_identity
    coord._projection_identities["invalid-key"] = xrp_identity
    coord._projection_identities[_command()["shard_key"]] = xrp_identity
    await coord._handle_execution_fill("orders.events.kraken.XRP/EUR.executed", _fill(_command()))
    assert coord._certified_projection_components(xrp_identity) is None
    assert coord._certified_projection_components(btc_identity) == [cmd["shard_key"]]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "invalid_shard", ["invalid", "kraken.ETH-USD.live", "kraken.XRP/EUR.paper"]
)
async def test_command_scope_contradiction_never_publishes(
    coordinator: tuple[TraderCoordinator, MagicMock], invalid_shard: str
) -> None:
    """Malformed or contradictory durable scope retains the global fail-closed policy.

    Given: A command whose persisted shard is malformed or disagrees with its scope.
    When: The outbox prepares that command for publication.
    Then: Global certification fails before an engine is installed or an order is sent.
    """
    coord, publisher = coordinator
    cmd = _command()
    cmd["shard_key"] = invalid_shard
    with pytest.raises(RuntimeError, match="command scope disagrees"):
        await coord._outbox_publish(cmd)
    assert coord._recovery_certification_failed is True
    assert coord.engines == {}
    publisher.send.assert_not_awaited()


@pytest.mark.asyncio
async def test_tagged_plan_command_installs_exact_paper_owner(
    coordinator: tuple[TraderCoordinator, MagicMock],
) -> None:
    """A tagged plan command retains its strategy shard through owner creation.

    Given: A paper plan command with an explicit strategy tag in its durable shard.
    When: The command dispatches without an existing owner.
    Then: Its new owner retains the exact tagged shard and receives subsequent fills.
    """
    coord, publisher = coordinator
    cmd = _command(exchange="paper")
    cmd["shard_key"] += ".bracket-owner"
    cmd["strategy_id"] = "bracket"
    await coord._outbox_publish(cmd)
    owner = coord._find_engine_for_fill(_fill(cmd))
    assert owner is not None
    assert owner._shard_key == cmd["shard_key"]
    assert owner._strategy_tag == "bracket-owner"
    publisher.send.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("mapped_shard", ["invalid", "kraken.ETH-USD.live", "paper.XRP/EUR.paper"])
async def test_mapped_fill_scope_contradiction_is_global(
    coordinator: tuple[TraderCoordinator, MagicMock], mapped_shard: str
) -> None:
    """A dispatched owner contradicted by the fill cannot become a topology-only refusal.

    Given: A fill whose dispatched shard is malformed or contradicts its scope.
    When: The coordinator attempts to route the fill.
    Then: Global certification fails and even an unrelated create remains refused.
    """
    coord, publisher = coordinator
    cmd = _command()
    coord._register_order_shard_key(cmd["client_order_id"], mapped_shard, WALLET)
    await coord._handle_execution_fill("orders.events.kraken.XRP/EUR.executed", _fill(cmd))
    assert coord._recovery_certification_failed is True
    with pytest.raises(RuntimeError, match="recovery certification failed"):
        await coord._outbox_publish(_command("BTC-USD"))
    publisher.send.assert_not_awaited()


@pytest.mark.asyncio
async def test_command_wallet_contradiction_never_publishes(
    coordinator: tuple[TraderCoordinator, MagicMock],
) -> None:
    """A wallet-bearing shard cannot be dispatched without its full wallet identity.

    Given: A command naming a wallet-bearing shard but omitting its full wallet id.
    When: The outbox validates ownership before dispatch.
    Then: Global certification fails and no order publishes.
    """
    coord, publisher = coordinator
    cmd = _command()
    cmd["wallet_public_id"] = None
    with pytest.raises(RuntimeError, match="command wallet disagrees"):
        await coord._outbox_publish(cmd)
    assert coord._recovery_certification_failed is True
    publisher.send.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("conflict", ["shard", "wallet"])
async def test_incumbent_owner_contradiction_never_publishes(
    coordinator: tuple[TraderCoordinator, MagicMock], conflict: str
) -> None:
    """A registry-key collision preserves its incumbent and globally refuses the create.

    Given: An incumbent engine whose exact shard or full wallet contradicts the command.
    When: A manual command attempts to reuse that registry entry.
    Then: The incumbent survives, global certification fails, and no order publishes.
    """
    coord, publisher = coordinator
    cmd = _command()
    owner = TradingEngineService(
        "ETH-USD" if conflict == "shard" else "XRP/EUR",
        execution_socket=cast(MessagePublisher, publisher),
        exchange="kraken",
        wallet_public_id=SUFFIX_TWIN if conflict == "wallet" else WALLET,
    )
    key = coord._build_engine_key("XRP/EUR", "kraken", "live", WALLET)
    coord.engines[key] = owner
    with pytest.raises(RuntimeError, match="command owner conflicts"):
        await coord._outbox_publish(cmd)
    assert coord.engines[key] is owner
    assert coord._recovery_certification_failed is True
    publisher.send.assert_not_awaited()


@pytest.mark.asyncio
async def test_missing_command_owner_prevents_send(
    coordinator: tuple[TraderCoordinator, MagicMock], monkeypatch: pytest.MonkeyPatch
) -> None:
    """A constructor refusal cannot emit an ownerless create.

    Given: An engine constructor that cannot create the command's owner.
    When: The outbox prepares a manual create.
    Then: Dispatch raises without installing a client route or publishing an order.
    """
    coord, publisher = coordinator
    monkeypatch.setattr(coord, "_create_engine_for_recovery", AsyncMock(return_value=None))
    with pytest.raises(RuntimeError, match="no owning engine can be created"):
        await coord._outbox_publish(_command())
    assert coord._recovery_certification_failed is False
    assert coord._order_shard_keys == {}
    publisher.send.assert_not_awaited()


@pytest.mark.asyncio
async def test_command_owner_creation_preserves_concurrent_signal_owner(
    coordinator: tuple[TraderCoordinator, MagicMock], monkeypatch: pytest.MonkeyPatch
) -> None:
    """An engine appearing during awaited construction keeps its position and identity.

    Given: A signal engine registers while manual owner construction awaits metadata.
    When: The manual constructor returns a second engine for the same exact owner.
    Then: The signal engine and its existing position survive publication.
    """
    coord, publisher = coordinator
    cmd = _command()
    winner = TradingEngineService(
        "XRP/EUR", cast(MessagePublisher, publisher), exchange="kraken", wallet_public_id=WALLET
    )
    winner.position_qty = 7.0
    key = coord._build_engine_key("XRP/EUR", "kraken", "live", WALLET)

    async def install_during_creation(
        instrument: str,
        exchange: str,
        strategy_tag: str | None,
        wallet_public_id: str,
        operator_public_id: str,
    ) -> TradingEngineService:
        """Model a signal registering its owner while construction awaits metadata."""
        assert instrument == "XRP/EUR"
        assert exchange == "kraken"
        assert strategy_tag is None
        assert wallet_public_id == WALLET
        assert operator_public_id == "manual-operator"
        coord.engines[key] = winner
        return TradingEngineService(
            instrument,
            cast(MessagePublisher, publisher),
            exchange="kraken",
            wallet_public_id=wallet_public_id,
        )

    monkeypatch.setattr(coord, "_create_engine_for_recovery", install_during_creation)
    await coord._outbox_publish(cmd)
    assert coord.engines[key] is winner
    assert winner.position_qty == 7.0
    publisher.send.assert_awaited_once()


@pytest.mark.asyncio
async def test_conflicting_client_route_prevents_send(
    coordinator: tuple[TraderCoordinator, MagicMock],
) -> None:
    """A poisoned client id cannot dispatch even with a valid command owner.

    Given: A client id already mapped to a different instrument shard.
    When: A manual command attempts to install a conflicting route for that id.
    Then: The id is poisoned, global certification fails, and nothing publishes.
    """
    coord, publisher = coordinator
    cmd = _command()
    coord._register_order_shard_key(
        cmd["client_order_id"], _command("BTC-USD")["shard_key"], WALLET
    )
    with pytest.raises(RuntimeError, match="conflicting command route"):
        await coord._outbox_publish(cmd)
    assert coord._recovery_certification_failed is True
    assert cmd["client_order_id"] in coord._poisoned_client_order_ids
    publisher.send.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("failure_scope", ["global", "instrument"])
async def test_dispatch_rechecks_certification_after_awaited_owner_creation(
    coordinator: tuple[TraderCoordinator, MagicMock],
    monkeypatch: pytest.MonkeyPatch,
    failure_scope: str,
) -> None:
    """A failure arriving while metadata resolves prevents subsequent publication.

    Given: Manual owner construction is suspended during an asynchronous metadata read.
    When: Another event invalidates global certification or the command's instrument.
    Then: Dispatch checks the new failure after construction and sends no order.
    """
    coord, publisher = coordinator
    construction_started = asyncio.Event()
    construction_resume = asyncio.Event()

    async def wait_for_metadata(
        instrument: str,
        exchange: str,
        strategy_tag: str | None,
        wallet_public_id: str,
        operator_public_id: str,
    ) -> TradingEngineService:
        """Suspend owner construction while another event invalidates certification."""
        construction_started.set()
        await construction_resume.wait()
        return TradingEngineService(
            instrument,
            cast(MessagePublisher, publisher),
            exchange=cast(OrderExchange, exchange),
            strategy_tag=strategy_tag,
            wallet_public_id=wallet_public_id,
            operator_public_id=operator_public_id,
        )

    monkeypatch.setattr(coord, "_create_engine_for_recovery", wait_for_metadata)
    dispatch = asyncio.create_task(coord._outbox_publish(_command()))
    await construction_started.wait()
    if failure_scope == "global":
        coord._recovery_certification_failed = True
        expected_error = "recovery certification failed"
    else:
        coord._unowned_fill_scopes.add(("kraken", "XRP/EUR"))
        expected_error = "instrument has an unowned fill"
    construction_resume.set()
    with pytest.raises(RuntimeError, match=expected_error):
        await dispatch
    publisher.send.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("instance_count", [1, 2])
async def test_local_expiry_publication_does_not_reapply_its_terminal_event(
    coordinator: tuple[TraderCoordinator, MagicMock],
    monkeypatch: pytest.MonkeyPatch,
    instance_count: int,
) -> None:
    """The plan-facing expiry echo neither creates a flat shard nor applies a second terminal.

    Given: A walleted command with pending engine and TradeService intent.
    When: Local TTL expiry publishes and its frame returns through the broker dispatcher.
    Then: Intent releases once and only the original wallet shard retains terminal state.
    """
    coord, publisher = coordinator
    cmd = _command()
    coord._ownership = ShardOwnership(
        instance_id=ShardOwnership._hash(cmd["shard_key"]) % instance_count,
        instance_count=instance_count,
    )
    cmd["leverage"] = 3
    cmd["reduce_only"] = True
    await coord._ensure_command_engine(cmd)
    owner = coord.engines[coord._build_engine_key("XRP/EUR", "kraken", "live", WALLET)]
    assert owner.rearm_pending_intent(cmd["client_order_id"])
    coord.trade_service.register_command(cmd["shard_key"], cmd)
    apply_event = MagicMock(wraps=coord.trade_service.apply_venue_event)
    monkeypatch.setattr(coord.trade_service, "apply_venue_event", apply_event)
    await coord._on_command_expired(cmd)
    assert owner.order_in_flight is False
    assert owner.pending_client_order_id is None
    assert coord.trade_service.get_command_state(cmd["shard_key"]).status == "expired"
    assert coord.trade_service.get_command_state(cmd["shard_key"]).in_flight is False
    assert cmd["client_order_id"] not in coord._order_shard_keys
    publisher.send.assert_awaited_once()
    topic, publication = publisher.send.await_args.args
    assert topic == "orders.events.kraken.XRP/EUR.expired"
    assert isinstance(publication, OrderData)
    assert publication.status == "expired"
    assert publication.wallet_public_id == WALLET
    assert publication.plan_public_id == "manual-plan"
    assert publication.leverage == 3
    assert publication.reduce_only is True
    state_before_echo = deepcopy(coord.trade_service._shards[cmd["shard_key"]])
    apply_event.assert_called_once()
    await coord._dispatch_order_event(topic, publication.to_json().encode())
    assert coord.trade_service.known_shard_keys() == {cmd["shard_key"]}
    assert coord.trade_service._shards[cmd["shard_key"]] == state_before_echo
    apply_event.assert_called_once()
    assert owner.order_in_flight is False


@pytest.mark.asyncio
@pytest.mark.parametrize("command_type", ["create", "submit"])
async def test_failed_recovery_shard_refuses_create_before_owner_construction(
    coordinator: tuple[TraderCoordinator, MagicMock],
    monkeypatch: pytest.MonkeyPatch,
    command_type: str,
) -> None:
    """A failed shard cannot acquire a new flat owner through manual dispatch.

    Given: Recovery recorded a failed shard prefix without failing global certification.
    When: A manual create or submit targets that same exchange, instrument, mode, and wallet.
    Then: Dispatch refuses before owner construction or publication and preserves the failure.
    """
    coord, publisher = coordinator
    cmd = _command()
    cmd["command_type"] = command_type
    parsed = coord._parse_shard_key(cmd["shard_key"])
    assert parsed is not None
    coord._failed_recovery_shard_prefixes.add(parsed[:4])
    create_owner = AsyncMock()
    monkeypatch.setattr(coord, "_create_engine_for_recovery", create_owner)
    with pytest.raises(RuntimeError, match="shard recovery uncertified"):
        await coord._outbox_publish(cmd)
    create_owner.assert_not_awaited()
    publisher.send.assert_not_awaited()
    assert coord.engines == {}
    assert coord._order_shard_keys == {}
    assert coord._failed_recovery_shard_prefixes == {parsed[:4]}
    assert coord._recovery_certification_failed is False


@pytest.mark.asyncio
async def test_failed_recovery_shard_permits_other_scopes_and_cancel(
    coordinator: tuple[TraderCoordinator, MagicMock],
) -> None:
    """A failed shard preserves risk-reducing cancels and healthy wallet or instrument creates.

    Given: Only the first wallet's XRP recovery prefix failed certification.
    When: Another instrument and wallet create orders and the failed shard requests a cancel.
    Then: All three commands publish while the original recovery quarantine remains intact.
    """
    coord, publisher = coordinator
    cmd = _command()
    parsed = coord._parse_shard_key(cmd["shard_key"])
    assert parsed is not None
    coord._failed_recovery_shard_prefixes.add(parsed[:4])
    await coord._outbox_publish(_command("BTC-USD"))
    other_wallet_cmd = _command(wallet=OTHER_WALLET)
    other_wallet_cmd["client_order_id"] += "-other-wallet"
    other_wallet_cmd["venue_client_id"] = other_wallet_cmd["client_order_id"]
    await coord._outbox_publish(other_wallet_cmd)
    cmd["command_type"] = "cancel"
    await coord._outbox_publish(cmd)
    assert publisher.send.await_count == 3
    assert isinstance(publisher.send.await_args.args[1], OrderCancelData)
    assert coord._failed_recovery_shard_prefixes == {parsed[:4]}
    assert coord._recovery_certification_failed is False
    assert coord._refuse_failed_recovery_shard("invalid") is False


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("instrument", "wallet", "refused"),
    [("XRP/EUR", WALLET, True), ("BTC-USD", WALLET, False), ("XRP/EUR", OTHER_WALLET, False)],
)
async def test_failed_recovery_shard_scopes_strategy_refusal(
    coordinator: tuple[TraderCoordinator, MagicMock],
    monkeypatch: pytest.MonkeyPatch,
    instrument: str,
    wallet: str,
    refused: bool,
) -> None:
    """Strategy routing honors a failed recovery prefix without blocking unrelated scopes.

    Given: Recovery quarantined one wallet's XRP shard without a global failure.
    When: A strategy signal targets the failed scope or a different wallet or instrument.
    Then: The failed scope stops before engine work while healthy scopes reach ownership routing.
    """
    coord, publisher = coordinator
    parsed = coord._parse_shard_key(_command()["shard_key"])
    assert parsed is not None
    coord._failed_recovery_shard_prefixes.add(parsed[:4])
    foreign_guard = MagicMock(return_value=True)
    create_owner = AsyncMock()
    monkeypatch.setattr(coord, "_should_drop_signal_for_foreign_shard", foreign_guard)
    monkeypatch.setattr(coord, "_get_or_create_signal_engine", create_owner)
    signal = SignalData(
        public_id="signal-failed-recovery",
        timestamp=NOW,
        session_id="signal-session",
        sequence_id=1,
        fired_at=NOW,
        instrument=instrument,
        exchange="kraken",
        side="buy",
        strength=1.0,
        reason="failed-recovery ownership regression",
        price=2.1,
        wallet_public_id=wallet,
    )
    coord._current_topic = f"signals.kraken.{instrument}.live"
    await coord._on_signal(signal)
    assert foreign_guard.call_count == (0 if refused else 1)
    create_owner.assert_not_awaited()
    publisher.send.assert_not_awaited()
    assert coord._recovery_certification_failed is False
