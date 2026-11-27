"""Exercise the size limit and durable scope of live strategy orders."""

import asyncio
import json
from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from decimal import Decimal
from pathlib import Path
from unittest.mock import DEFAULT
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from uuid import UUID
from uuid import uuid4

import pytest
from loguru import logger
from pydantic import ValidationError
from sqlalchemy import func
from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from snapper.application.engine.service import InstrumentSpec
from snapper.application.engine.service import TradingEngineService
from snapper.application.engine.service import _LiveQuantity
from snapper.application.engine.trader import TraderCoordinator
from snapper.application.process_manager.config_resolver import load_current_process_configs
from snapper.application.process_manager.config_resolver import read_current_process_configs
from snapper.application.process_manager.launcher import ProcessLauncherService
from snapper.application.process_manager.process_parameters import StrategyProcessParameters
from snapper.application.process_manager.registry_syncer import ProcessRegistrySyncer
from snapper.application.process_manager.strategy_admission import LiveStrategyScope
from snapper.application.process_manager.strategy_admission import resolve_live_signal_policy
from snapper.application.process_manager.strategy_admission import resolve_live_strategy_start
from snapper.application.process_manager.strategy_scope import StrategyScopeError
from snapper.application.trade.caps_enforcer import CapsViolationError
from snapper.application.trade.caps_enforcer import TradingCapsEnforcer
from snapper.application.trade.outbox import OutboxDispatcher
from snapper.config.settings import AppSettings
from snapper.core.json_types import JsonObject
from snapper.core.partitioning import ShardOwnershipError
from snapper.core.strategy_order_cap import LiveStrategyOrderPolicy
from snapper.core.types import ExchangeEnum
from snapper.core.types import ProcessLifecycleEnum
from snapper.core.types import ProcessRoleEnum
from snapper.data.models import Instrument
from snapper.data.models import InstrumentSpec as InstrumentSpecModel
from snapper.data.models import Operator
from snapper.data.models import PairedExecutionGroup
from snapper.data.models import Setting
from snapper.data.models import Symbol
from snapper.data.models import TradeCommand
from snapper.data.models import Wallet
from snapper.data.models import WalletOperatorScopeGrant
from snapper.data.repository import SQLAlchemyRepository
from snapper.data.repository import begin_process_config_write
from snapper.messaging.infrastructure.publisher import MessagePublisher
from snapper.messaging.infrastructure.publisher import SequenceTracker
from snapper.messaging.infrastructure.validated_socket import ValidatedSubscriber
from snapper.messaging.schemas.data import SignalData
from snapper.strategies.models import StrategyConfig

_INSTRUMENT = "BTC-EUR"
_WALLET = "8b60e3ff-eb15-4fcb-9159-aa751edbb28d"
_OPERATOR = "19e4896d-3505-4f47-b53d-e15b50d8357a"
_INSTRUMENT_ID = "2fba5f8b-18e7-48cd-94d0-f554d4d00b82"
_TOPIC = "signals.kraken.BTC-EUR.live"
_QUANTITY = 0.00006


@dataclass(frozen=True)
class _Config:
    """Convenient access to the SQLite-backed process writer and settings."""

    repo: SQLAlchemyRepository
    settings: AppSettings
    writer: ProcessRegistrySyncer


def _params(name: str = "process_btc") -> JsonObject:
    """Provide a strategy whose process name includes the literal prefix."""
    return {
        "name": name,
        "inputs": ["market.kraken.BTC-EUR.ticks"],
        "outputs": [_INSTRUMENT],
        "exchange": "kraken",
        "operator_public_id": "label:trader",
        "wallet_public_id": "label:cash",
        "max_order_base_qty": _QUANTITY,
    }


async def _seed_scope(repo: SQLAlchemyRepository) -> None:
    """Insert one live wallet, its scoped operator and real Kraken lot evidence."""
    now = datetime.now(UTC)
    provenance = {"timestamp": now, "session_id": "scope", "sequence_id": 1}
    symbol_id = str(uuid4())
    async with repo.session() as session:
        session.add_all(
            [
                Wallet(public_id=_WALLET, label="cash", is_paper=False, **provenance),
                Operator(public_id=_OPERATOR, label="trader", **provenance),
                Symbol(
                    public_id=symbol_id,
                    native_symbol=_INSTRUMENT,
                    base="BTC",
                    quote="EUR",
                    created_at=now,
                    **provenance,
                ),
                Instrument(
                    public_id=_INSTRUMENT_ID,
                    symbol_public_id=symbol_id,
                    exchange="kraken",
                    **provenance,
                ),
                WalletOperatorScopeGrant(
                    wallet_public_id=_WALLET,
                    operator_public_id=_OPERATOR,
                    granted_by_user_public_id=str(uuid4()),
                    scope_kind="instrument",
                    instrument_public_id=_INSTRUMENT_ID,
                    **provenance,
                ),
                InstrumentSpecModel(
                    instrument_public_id=_INSTRUMENT_ID,
                    tick_size=0.1,
                    lot_size=1e-8,
                    quantity_unit="base_asset",
                    native_order_quantity_unit="base_asset",
                    **provenance,
                ),
            ]
        )
        await session.commit()


@pytest.fixture
async def live_db(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> AsyncIterator[_Config]:
    """Use file-backed SQLite so competing mutations use separate transactions."""
    url = f"sqlite+aiosqlite:///{tmp_path}/strategy.db"
    settings = MagicMock(spec=AppSettings)
    settings.db_url = url
    settings.risk_r_per_trade = 0.01
    settings.risk_max_leverage = 2.0
    settings.risk_max_drawdown = 0.2
    repo = SQLAlchemyRepository(url)
    monkeypatch.setattr(
        "snapper.application.process_manager.registry_syncer.get_repository", lambda _url: repo
    )
    monkeypatch.setattr(
        "snapper.application.process_manager.launcher.get_repository", lambda _url: repo
    )
    monkeypatch.setattr("snapper.application.engine.trader.get_repository", lambda _url: repo)
    monkeypatch.setattr("snapper.application.engine.trader.get_settings", lambda: settings)
    monkeypatch.setattr("snapper.application.engine.trader.is_tradeable", lambda *_args: True)
    try:
        await repo.create_all()
        await _seed_scope(repo)
        yield _Config(repo, settings, ProcessRegistrySyncer(settings))
    finally:
        await repo.engine.dispose()


async def _create(
    config: _Config, name: str, *, enabled: bool = False, parameters: JsonObject | None = None
) -> None:
    """Persist a strategy through the real process writer."""
    await config.writer.create_process_config(
        name=name,
        class_path="snapper.strategies.process_wrapper.StrategyProcess",
        method="start",
        enabled=enabled,
        mode="thread",
        parameters=_params(name) if parameters is None else parameters,
        lifecycle=ProcessLifecycleEnum.LONG_RUNNING,
        role=ProcessRoleEnum.STRATEGY,
        tags=(),
    )


async def _seed_setting(repo: SQLAlchemyRepository, key: str, value: str) -> None:
    """Insert a legacy setting without going through the validated writer."""
    async with repo.session() as session:
        session.add(
            Setting(
                key=key,
                value=value,
                category="process",
                timestamp=datetime.now(UTC),
                session_id="migration",
                sequence_id=1,
            )
        )
        await session.commit()


def _signal(*, strength: float = _QUANTITY, grouped: bool = False) -> SignalData:
    """Build a live signal matching the seeded scope."""
    now = datetime.now(UTC)
    signal = SignalData(
        public_id=str(uuid4()),
        timestamp=now,
        session_id="signal",
        sequence_id=1,
        instrument=_INSTRUMENT,
        exchange="kraken",
        side="buy",
        strength=strength,
        reason="test",
        price=50_000.0,
        strategy_name="process_btc",
        fired_at=now,
        wallet_public_id=_WALLET,
        operator_public_id=_OPERATOR,
    )
    if grouped:
        return signal.model_copy(
            update={
                "paired_group_id": str(uuid4()),
                "paired_group_size": 2,
                "paired_group_index": 0,
                "paired_group_policy": "simultaneous",
                "paired_group_key": "kraken:BTC-EUR:live|kraken:ETH-EUR:live",
            }
        )
    return signal


def _trader(config: _Config) -> TraderCoordinator:
    """Wire the real trader, repository and outbox without a broker socket."""
    trader = TraderCoordinator()
    trader.msg_publisher = MagicMock(spec=MessagePublisher)
    trader.msg_publisher.tracker = SequenceTracker()
    trader.msg_publisher.send = AsyncMock()
    trader.execution_publisher = MagicMock()
    trader.outbox = OutboxDispatcher(config.repo)
    trader._current_topic = _TOPIC
    return trader


@pytest.mark.parametrize(
    "invalid", [None, True, False, "1", 0, -1, -0.25, float("inf"), float("-inf"), float("nan"), []]
)
def test_live_cap_requires_positive_finite_number(
    invalid: object, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Live cap requires positive finite number.

    Given: An invalid cap.
    When: Constructing either config.
    Then: Validation fails.
    """
    monkeypatch.setattr("snapper.strategies.models.is_tradeable", lambda *_args: True)
    with pytest.raises(ValidationError, match="max_order_base_qty"):
        StrategyProcessParameters.model_validate({**_params(), "max_order_base_qty": invalid})
    with pytest.raises(ValueError, match="max_order_base_qty"):
        StrategyConfig(
            name="strategy",
            strategy_class="Strategy",
            inputs=["market.kraken.BTC-EUR.ticks"],
            outputs=[_INSTRUMENT],
            exchange="kraken",
            max_order_base_qty=invalid,
        )


@pytest.mark.parametrize("cap", [1, 0.00006])
def test_live_cap_persists_as_json_number(cap: int | float) -> None:
    """Live cap persists as JSON number.

    Given: A valid cap.
    When: Encoding process parameters.
    Then: It remains numeric.
    """
    validated = StrategyProcessParameters.model_validate({**_params(), "max_order_base_qty": cap})
    assert json.loads(validated.model_dump_json())["max_order_base_qty"] == cap


async def test_live_cap_refuses_before_paired_group_and_command_writes(live_db: _Config) -> None:
    """Live cap refuses before paired group and command writes.

    Given: A paired signal one lot over cap.
    When: Routed.
    Then: No intent is written.
    """
    await _create(live_db, "process_btc", enabled=True)
    trader = _trader(live_db)
    assert trader.outbox is not None
    await trader._on_signal(_signal(strength=0.00006001, grouped=True))
    async with live_db.repo.session() as session:
        assert await session.scalar(select(func.count()).select_from(PairedExecutionGroup)) == 0
        assert await session.scalar(select(func.count()).select_from(TradeCommand)) == 0
    assert not trader.outbox._wake.is_set()
    engine = next(iter(trader.engines.values()))
    assert not engine.order_in_flight
    assert engine.pending_client_order_id is None


async def test_missing_live_policy_refuses_before_paired_assembly(
    live_db: _Config, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Missing live policy refuses before paired assembly.

    Given: No live policy.
    When: Handling a paired signal.
    Then: Reserve no group.
    """
    trader = _trader(live_db)
    monkeypatch.setattr(
        "snapper.application.engine.trader.resolve_live_signal_policy",
        AsyncMock(return_value=None),
    )
    await trader._on_signal(_signal(grouped=True))
    async with live_db.repo.session() as session:
        assert await session.scalar(select(func.count()).select_from(PairedExecutionGroup)) == 0
        assert await session.scalar(select(func.count()).select_from(TradeCommand)) == 0


async def test_live_cap_reads_backfilled_policy_before_next_order(live_db: _Config) -> None:
    """Live cap reads backfilled policy before next order.

    Given: A cap edit.
    When: The same engine receives a signal.
    Then: It uses the new cap.
    """
    name = "process_btc"
    low = {**_params(), "max_order_base_qty": 0.00005}
    await _create(live_db, name, enabled=True, parameters=low)
    trader = _trader(live_db)
    await trader._on_signal(_signal())
    await live_db.writer.update_process_config_parameters(
        name=name, parameters=_params(), updated_by="operator"
    )
    await trader._on_signal(_signal())
    assert trader.outbox is not None and trader.outbox._wake.is_set()
    async with live_db.repo.session() as session:
        command = (await session.scalars(select(TradeCommand))).one()
        assert (command.quantity, command.operator_public_id) == (_QUANTITY, _OPERATOR)


async def test_live_concurrent_enables_claim_only_one_wallet_output(live_db: _Config) -> None:
    """Live concurrent enables claim only one wallet output.

    Given: Overlapping disabled configs.
    When: Enabled together.
    Then: Only one commits.
    """
    names = ("alpha", "beta")
    for name in names:
        await _create(live_db, name)
    outcomes = await asyncio.gather(
        *(
            live_db.writer.update_process_config(name=name, enabled=True, updated_by="operator")
            for name in names
        ),
        return_exceptions=True,
    )
    assert sum(result is None for result in outcomes) == 1
    assert sum(isinstance(result, StrategyScopeError) for result in outcomes) == 1
    async with live_db.repo.session() as session:
        current = await load_current_process_configs(session)
    assert sum(current[name]["enabled"] is True for name in names) == 1


async def test_live_legacy_row_can_disable_and_backfill(live_db: _Config) -> None:
    """Live legacy row can disable and backfill.

    Given: An invalid legacy cap.
    When: Edited.
    Then: Disable and a valid backfill work.
    """
    name = "process_btc"
    params = _params()
    del params["max_order_base_qty"]
    row = {"class": "Strategy", "enabled": True, "role": "strategy", "parameters": params}
    await _seed_setting(live_db.repo, f"process_{name}", json.dumps(row))
    with pytest.raises(StrategyScopeError, match="max_order_base_qty"):
        await live_db.writer.update_process_config(name=name, restart_nonce="new", updated_by="ops")
    await live_db.writer.update_process_config(name=name, enabled=False, updated_by="ops")
    with pytest.raises(StrategyScopeError, match="max_order_base_qty"):
        await live_db.writer.update_process_config(name=name, enabled=True, updated_by="ops")
    await live_db.writer.update_process_config_parameters(
        name=name, parameters=_params(), updated_by="ops"
    )
    await live_db.writer.update_process_config(name=name, enabled=True, updated_by="ops")
    launch = await resolve_live_strategy_start(live_db.repo, name, [_OPERATOR])
    assert launch["max_order_base_qty"] == _QUANTITY
    assert launch["wallet_public_id"] == _WALLET


async def test_live_start_rejects_disabled_and_runtime_overrides(live_db: _Config) -> None:
    """Live start rejects disabled and runtime overrides.

    Given: A disabled live row.
    When: Launching it.
    Then: Only a persisted enable can qualify.
    """
    name = "process_btc"
    await _create(live_db, name)
    launcher = ProcessLauncherService(live_db.settings)
    disabled = await launcher.start_process_by_name(name)
    assert "before starting" in disabled.message
    await live_db.writer.update_process_config(name=name, enabled=True, updated_by="ops")
    generic_start = await launcher.start_process_by_name(name, allow_strategy=False)
    assert "requires a strategy start" in generic_start.message
    override = await launcher.start_process_by_name(name, parameters=_params())
    assert "overrides" in override.message


@pytest.mark.parametrize("value", ["not-json", "[]", '{"enabled": true}'])
async def test_live_policy_refuses_malformed_current_rows(live_db: _Config, value: str) -> None:
    """Live policy refuses malformed current rows.

    Given: Corrupt persisted process data.
    When: Resolving a signal.
    Then: Admission fails.
    """
    await _create(live_db, "process_btc", enabled=True)
    await _seed_setting(live_db.repo, "process_stray", value)
    with pytest.raises((ValueError, KeyError)):
        await resolve_live_signal_policy(live_db.repo, _signal())
    await _trader(live_db)._on_signal(_signal())
    async with live_db.repo.session() as session:
        assert await session.scalar(select(func.count()).select_from(TradeCommand)) == 0


async def test_live_policy_requires_unique_named_owner(live_db: _Config) -> None:
    """Live policy requires unique named owner.

    Given: Zero, mismatched or duplicate owners.
    When: A signal arrives.
    Then: It is refused.
    """
    with pytest.raises(StrategyScopeError, match="found 0"):
        await resolve_live_signal_policy(live_db.repo, _signal())
    await _seed_setting(live_db.repo, "processXignored", "not-json")
    await _seed_setting(live_db.repo, "Process_ignored", "not-json")
    await _create(live_db, "process_btc", enabled=True)
    assert (
        await resolve_live_signal_policy(live_db.repo, _signal())
    ).max_order_base_qty == Decimal("0.00006")
    wrong = _signal().model_copy(update={"operator_public_id": str(uuid4())})
    with pytest.raises(StrategyScopeError, match="strategy and operator"):
        await resolve_live_signal_policy(live_db.repo, wrong)
    duplicate = {"class": "Strategy", "enabled": True, "role": "strategy", "parameters": _params()}
    await _seed_setting(live_db.repo, "process_other", json.dumps(duplicate))
    with pytest.raises(StrategyScopeError, match="found 2"):
        await resolve_live_signal_policy(live_db.repo, _signal())
    with pytest.raises(StrategyScopeError, match="overlap"):
        await resolve_live_strategy_start(live_db.repo, "process_btc")


async def test_current_config_loader_uses_open_ended_version(live_db: _Config) -> None:
    """Resolve the committed cap across a process writer's clock skew.

    Given: A historical config that closes in the future and an open-ended successor.
    When: The current process catalogue is read before the successor's timestamp.
    Then: The catalogue uses the successor's cap.
    """
    now = datetime.now(UTC)
    future = now + timedelta(minutes=5)
    prior = {"class": "Strategy", "parameters": {"max_order_base_qty": 1.0}}
    current = {"class": "Strategy", "parameters": {"max_order_base_qty": _QUANTITY}}
    async with live_db.repo.session() as session:
        session.add_all(
            [
                Setting(
                    key="process_btc",
                    value=json.dumps(prior),
                    category="process",
                    timestamp=now,
                    known_to=future,
                    session_id="process-writer",
                    sequence_id=1,
                ),
                Setting(
                    key="process_btc",
                    value=json.dumps(current),
                    category="process",
                    timestamp=future,
                    session_id="process-writer",
                    sequence_id=2,
                ),
            ]
        )
        await session.commit()
    async with live_db.repo.session() as session:
        catalogue = await load_current_process_configs(session)
    assert catalogue["btc"] == current


async def _assert_listener_recovers(
    trader: TraderCoordinator, bad_frame: bytes | list[bytes]
) -> list[str]:
    """Run the real listener through a failing frame and a durable follow-up command."""
    followup = _signal()
    socket = MagicMock()
    topic = _TOPIC.encode()
    socket.recv_multipart = AsyncMock(
        side_effect=[
            [topic, bad_frame] if isinstance(bad_frame, bytes) else bad_frame,
            [topic, followup.publish_to(_TOPIC)],
            asyncio.CancelledError,
        ]
    )
    trader.signal_subscriber = ValidatedSubscriber(socket)
    errors: list[str] = []
    sink = logger.add(errors.append, format="{message}", level="ERROR")
    try:
        with pytest.raises(asyncio.CancelledError):
            await trader._listen_signals()
    finally:
        logger.remove(sink)
    assert socket.recv_multipart.await_count == 3
    async with trader.repository.session() as session:
        command = (await session.scalars(select(TradeCommand))).one()
        assert command.signal_public_id == followup.public_id
    return errors


async def test_signal_listener_survives_config_database_error(
    live_db: _Config, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Signal listener survives config database error.

    Given: A transient DB read error.
    When: The next signal arrives.
    Then: It still submits.
    """
    await _create(live_db, "process_btc", enabled=True)
    read = AsyncMock(
        wraps=read_current_process_configs, side_effect=[SQLAlchemyError("read failed"), DEFAULT]
    )
    monkeypatch.setattr(
        "snapper.application.process_manager.strategy_admission.read_current_process_configs", read
    )
    errors = await _assert_listener_recovers(_trader(live_db), _signal().publish_to(_TOPIC))
    assert read.await_count == 2
    assert any("reason=read failed" in message for message in errors)


@pytest.mark.parametrize(
    ("site", "error"),
    [
        pytest.param("ensure_paired_execution_group", OSError("connection lost"), id="os-error"),
        pytest.param("insert_trade_command", TimeoutError(), id="timeout"),
        pytest.param("insert_trade_command", ShardOwnershipError("shard", 0, 2), id="shard"),
        pytest.param("_get_or_create_signal_engine", RuntimeError("engine conflict"), id="runtime"),
    ],
)
async def test_signal_listener_survives_downstream_refusal(
    live_db: _Config, monkeypatch: pytest.MonkeyPatch, site: str, error: Exception
) -> None:
    """The signal listener survives downstream refusals.

    Given: A downstream operation fails after resolving the real persisted policy.
    When: Another valid live signal follows the refused signal.
    Then: The listener logs the refusal and submits only the follow-up order.
    """
    await _create(live_db, "process_btc", enabled=True)
    trader = _trader(live_db)
    owner = trader if site == "_get_or_create_signal_engine" else live_db.repo
    operation = AsyncMock(wraps=getattr(owner, site), side_effect=[error, DEFAULT])
    monkeypatch.setattr(owner, site, operation)
    signal = _signal(grouped=site == "ensure_paired_execution_group")
    errors = await _assert_listener_recovers(trader, signal.publish_to(_TOPIC))
    assert operation.await_count >= 1
    reason = str(error) or type(error).__name__
    assert any(
        f"strategy={signal.strategy_name} instrument={signal.instrument}" in message
        and f"reason={reason}" in message
        for message in errors
    )


async def test_signal_listener_survives_ai_cap_refusal(
    live_db: _Config, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The signal listener survives an AI-attributed user cap refusal.

    Given: The AI guard rejects an order admitted by the persisted strategy cap.
    When: A valid live signal follows the refused order.
    Then: The listener submits only the follow-up order.
    """
    await _create(live_db, "process_btc", enabled=True)
    trader = _trader(live_db)
    enforcer = trader._build_caps_enforcer()
    assert enforcer is not None
    trader._caps_enforcer = enforcer
    guard = MagicMock()
    guard.return_value.__aenter__.side_effect = CapsViolationError("max_open_orders")
    monkeypatch.setattr(enforcer, "guard_with_ai_review_attribution", guard)
    signal = _signal().model_copy(update={"ai_review_public_id": str(uuid4())})
    errors = await _assert_listener_recovers(trader, signal.publish_to(_TOPIC))
    guard.assert_called_once()
    assert any(
        f"strategy={signal.strategy_name} instrument={signal.instrument}" in message
        and "reason=cap max_open_orders violated" in message
        for message in errors
    )


@pytest.mark.parametrize(
    ("payload", "reason"),
    [
        pytest.param(b"\xff", "UnicodeDecodeError", id="payload-utf8"),
        pytest.param(b"{", "ValidationError", id="invalid-json"),
        pytest.param(b"{}", "ValidationError", id="invalid-envelope"),
        pytest.param([b"signals.\xff", b"{}"], "'utf-8' codec can't decode", id="topic-utf8"),
        pytest.param([b"signals."], "Expected 2-part message", id="multipart"),
    ],
)
async def test_signal_listener_survives_corrupt_frame(
    live_db: _Config, payload: bytes | list[bytes], reason: str
) -> None:
    """The signal listener survives corrupt frames.

    Given: Invalid wire framing, UTF-8, JSON, or signal schema.
    When: A valid signal follows the corrupt frame.
    Then: The listener reports a frame error and submits the valid order.
    """
    await _create(live_db, "process_btc", enabled=True)
    errors = await _assert_listener_recovers(_trader(live_db), payload)
    assert any(f"reason={reason}" in message for message in errors)


@pytest.mark.parametrize("error", [TypeError, AttributeError])
async def test_policy_programming_error_propagates(
    live_db: _Config, monkeypatch: pytest.MonkeyPatch, error: type[Exception]
) -> None:
    """Programming errors in policy resolution propagate.

    Given: A programming error while evaluating the persisted policy.
    When: The trader routes a live signal.
    Then: The trader surfaces the original error.
    """
    await _create(live_db, "process_btc", enabled=True)
    read = AsyncMock(side_effect=error("programming error"))
    monkeypatch.setattr(
        "snapper.application.process_manager.strategy_admission.read_current_process_configs", read
    )
    with pytest.raises(error, match="programming error"):
        await _trader(live_db)._on_signal(_signal())
    read.assert_awaited_once()


async def test_resolved_live_owner_requires_cap(
    live_db: _Config, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A resolved live owner still requires a cap.

    Given: A resolved owner whose model bypassed normal cap validation.
    When: The live policy is constructed.
    Then: The resolver raises a scope error.
    """
    await _create(live_db, "process_btc", enabled=True)
    parameters = StrategyProcessParameters.model_construct(
        name="process_btc",
        inputs=["market.kraken.BTC-EUR.ticks"],
        outputs=[_INSTRUMENT],
        exchange=ExchangeEnum.KRAKEN,
        wallet_public_id=_WALLET,
        operator_public_id=_OPERATOR,
    )
    scope = LiveStrategyScope("process_btc", parameters, UUID(_WALLET))
    monkeypatch.setattr(
        "snapper.application.process_manager.strategy_admission._resolve_live_scope",
        AsyncMock(return_value=scope),
    )
    with pytest.raises(StrategyScopeError, match="max_order_base_qty must be configured"):
        await resolve_live_signal_policy(live_db.repo, _signal())


def _engine(
    spec: InstrumentSpec | None = None,
) -> TradingEngineService:
    """Build a live engine with observable side-effect boundaries."""
    publisher = MagicMock(spec=MessagePublisher)
    publisher.tracker = MagicMock(spec=SequenceTracker)
    publisher.send = AsyncMock()
    return TradingEngineService(
        _INSTRUMENT,
        publisher,
        exchange="kraken",
        wallet_public_id=_WALLET,
        repository=MagicMock(spec=SQLAlchemyRepository),
        outbox=MagicMock(spec=OutboxDispatcher),
        instrument_specs={
            _INSTRUMENT: (
                spec
                if spec is not None
                else {
                    "public_id": _INSTRUMENT_ID,
                    "lot_size": 1e-8,
                    "quantity_unit": "base_asset",
                    "native_order_quantity_unit": "base_asset",
                }
            )
        },
    )


def _policy(cap: str = "0.00006") -> LiveStrategyOrderPolicy:
    """Build the typed limit normally delivered by persisted signal admission."""
    return LiveStrategyOrderPolicy("process_btc", _WALLET, _OPERATOR, Decimal(cap))


@pytest.mark.parametrize("ai", [None, "review"])
@pytest.mark.parametrize(
    "position,target", [(0.0, 0.00006001), (0.00006001, 0.0), (0.00003, -0.00003001)]
)
async def test_over_cap_opens_closes_and_flips_leave_order_state_untouched(
    ai: str | None, position: float, target: float
) -> None:
    """Over cap opens closes and flips leave order state untouched.

    Given: An over-cap delta.
    When: Submitted.
    Then: Ids and writes are untouched.
    """
    engine = _engine()
    enforcer = MagicMock(spec=TradingCapsEnforcer)
    engine._caps_enforcer = enforcer
    engine.position_qty = position
    peak = engine.peak_equity
    assert (
        await engine.execute_desired_units(target, 100.0, ai_review_public_id=ai, policy=_policy())
        is None
    )
    assert not engine.order_in_flight and engine.pending_client_order_id is None
    assert engine.peak_equity == peak
    engine.execution_socket.tracker.next_sequence.assert_not_called()
    assert engine._repository is not None and engine._outbox is not None
    engine._repository.insert_trade_command.assert_not_awaited()
    engine._outbox.notify.assert_not_called()
    engine.execution_socket.send.assert_not_awaited()
    enforcer.guard_with_ai_review_attribution.assert_not_called()
    enforcer.guard_service_principal.assert_not_called()


@pytest.mark.parametrize("backend", ["repository", "outbox", "policy"])
async def test_live_send_fails_closed_when_required_state_is_missing(backend: str) -> None:
    """Live send fails closed when required state is missing.

    Given: A missing dependency.
    When: Attempting a live send.
    Then: No ids are allocated.
    """
    engine = _engine()
    if backend == "repository":
        engine._repository = None
    if backend == "outbox":
        engine._outbox = None
    policy = None if backend == "policy" else _policy()
    assert await engine._send_order("buy", _QUANTITY, 100.0, "test", policy=policy) is None
    engine.execution_socket.tracker.next_sequence.assert_not_called()


@pytest.mark.parametrize(
    "override",
    [
        {"public_id": ""},
        {"lot_size": None},
        {"lot_size": True},
        {"lot_size": 0.0},
        {"lot_size": float("nan")},
        {"lot_size": float("inf")},
        {"quantity_unit": "contracts"},
        {"native_order_quantity_unit": None},
        {"native_order_quantity_unit": "contracts"},
    ],
)
async def test_live_send_fails_closed_without_base_asset_spec(override: InstrumentSpec) -> None:
    """Live send fails closed without base asset spec.

    Given: Incomplete venue metadata.
    When: Sizing a live order.
    Then: Admission fails.
    """
    engine = _engine()
    engine.instrument_specs[_INSTRUMENT].update(override)
    assert await engine.execute_desired_units(_QUANTITY, 100.0, policy=_policy()) is None
    engine.execution_socket.tracker.next_sequence.assert_not_called()


async def test_live_order_rejects_float_round_trip_loss() -> None:
    """Live order rejects float round trip loss.

    Given: A close with extra exact digits.
    When: Float would change it.
    Then: It is refused.
    """
    engine = _engine()
    engine.instrument_specs[_INSTRUMENT]["lot_size"] = 1e-16
    engine.position_qty = 2.000000000000001
    assert await engine.execute_desired_units(1e-16, 100.0, policy=_policy("5")) is None
    engine.execution_socket.tracker.next_sequence.assert_not_called()


async def test_live_maybe_stop_cannot_submit_without_signal_policy() -> None:
    """Live maybe stop cannot submit without signal policy.

    Given: A live stop trigger.
    When: Checked.
    Then: It creates no pending intent.
    """
    engine = _engine()
    engine.position_qty = _QUANTITY
    engine.entry_price = 100.0
    assert not await engine._maybe_stop(20.0)
    assert not engine.order_in_flight and engine.pending_client_order_id is None
    engine.execution_socket.tracker.next_sequence.assert_not_called()


@pytest.mark.parametrize("bad_field", ["strategy", "wallet", "operator", "exchange", "output"])
async def test_live_policy_rejects_a_different_signal_scope(
    live_db: _Config, bad_field: str
) -> None:
    """Live policy rejects a different signal scope.

    Given: A stale signal identity.
    When: Resolving its policy.
    Then: It cannot match.
    """
    changed = {
        "strategy": {"strategy_name": "other"},
        "wallet": {"wallet_public_id": str(uuid4())},
        "operator": {"operator_public_id": str(uuid4())},
        "exchange": {"exchange": "walutomat"},
        "output": {"instrument": "ETH-EUR"},
    }
    await _create(live_db, "process_btc", enabled=True)
    with pytest.raises(StrategyScopeError):
        await resolve_live_signal_policy(
            live_db.repo, _signal().model_copy(update=changed[bad_field])
        )


@pytest.mark.parametrize(
    "topic, origin",
    [
        ("signals.kraken.BTC-EUR.paper", "live"),
        ("signals.kraken.ETH-EUR.live", "live"),
        ("signals.walutomat.BTC-EUR.live", "live"),
        (_TOPIC, "replay"),
    ],
)
async def test_live_routing_refuses_spoofed_envelopes(
    live_db: _Config, topic: str, origin: str
) -> None:
    """Live routing refuses spoofed envelopes.

    Given: A routing or provenance mismatch.
    When: Processed.
    Then: No order engine is selected.
    """
    await _create(live_db, "process_btc", enabled=True)
    trader = _trader(live_db)
    trader._current_topic = topic
    await trader._on_signal(_signal().model_copy(update={"origin": origin}))
    assert not trader.engines
    async with live_db.repo.session() as session:
        assert await session.scalar(select(func.count()).select_from(TradeCommand)) == 0


async def test_live_recovered_engine_refuses_over_cap_close(live_db: _Config) -> None:
    """Live recovered engine refuses over cap close.

    Given: A recovered position above the cap.
    When: A close arrives.
    Then: No command is written.
    """
    await _create(live_db, "process_btc", enabled=True)
    trader = _trader(live_db)
    engine = await trader._create_engine_for_recovery(
        _INSTRUMENT, "kraken", wallet_public_id=_WALLET
    )
    assert engine is not None
    engine.position_qty = 0.00006001
    key = trader._build_engine_key(_INSTRUMENT, "kraken", "live", _WALLET)
    trader.engines[key] = engine
    await trader._on_signal(_signal(strength=0))
    assert trader.engines[key] is engine
    assert engine.pending_client_order_id is None
    async with live_db.repo.session() as session:
        assert await session.scalar(select(func.count()).select_from(TradeCommand)) == 0


@pytest.mark.parametrize(
    "position,target,reduce", [(0.00006, 0.0, True), (0.00003, -0.00003, False)]
)
async def test_exact_cap_live_close_and_flip_emit_full_snapped_size(
    position: float, target: float, reduce: bool
) -> None:
    """Exact cap live close and flip emit full snapped size.

    Given: An exact-cap close or flip.
    When: Sent.
    Then: The full eligible delta is accepted.
    """
    engine = _engine()
    assert engine._repository is not None
    engine._repository.insert_trade_command.return_value = (1, "order-command")
    engine.position_qty = position
    assert await engine.execute_desired_units(target, 100.0, policy=_policy()) == "order-command"
    command = engine._repository.insert_trade_command.call_args.args[0]
    assert command["quantity"] == _QUANTITY
    assert command["side"] == "sell"
    assert command["reduce_only"] is reduce
    assert engine.order_in_flight


async def test_live_flip_snaps_only_after_open_and_close_are_combined() -> None:
    """Live flip snaps only after open and close are combined.

    Given: Two sub-lot portions.
    When: Flipping.
    Then: Their combined lot is accepted.
    """
    engine = _engine()
    assert engine._repository is not None
    engine._repository.insert_trade_command.return_value = (1, "order-command")
    engine.position_qty = 6e-9
    result = await engine.execute_desired_units(-4e-9, 100.0, policy=_policy("1e-8"))
    assert result == "order-command"
    assert engine._repository.insert_trade_command.call_args.args[0]["quantity"] == 1e-8


@pytest.mark.parametrize("state", ["position", "pending", "in_flight"])
async def test_live_prepared_order_refuses_when_engine_intent_moves(state: str) -> None:
    """Live prepared order refuses when engine intent moves.

    Given: A prepared order.
    When: Position or intent changes before send.
    Then: It is stale.
    """
    engine = _engine()
    policy = _policy()
    prepared = engine.prepare_desired_units(_QUANTITY, 100.0, policy)
    assert prepared is not None
    if state == "position":
        engine.position_qty = 1e-8
    if state == "pending":
        engine.pending_client_order_id = "other"
    if state == "in_flight":
        engine.order_in_flight = True
    assert (
        await engine.execute_desired_units(_QUANTITY, 100.0, policy=policy, prepared=prepared)
        is None
    )
    engine.execution_socket.tracker.next_sequence.assert_not_called()


async def test_live_send_rejects_prepared_order_above_cap_before_allocating_ids() -> None:
    """Live send rejects prepared order above cap before allocating IDs.

    Given: A previously admitted size.
    When: The supplied limit is lower.
    Then: Send refuses.
    """
    engine = _engine()
    prepared = engine.prepare_desired_units(_QUANTITY, 100.0, _policy())
    assert prepared is not None
    assert (
        await engine._send_order(
            prepared.side, prepared.quantity, 100.0, "test", policy=_policy("0.00005")
        )
        is None
    )
    engine.execution_socket.tracker.next_sequence.assert_not_called()


@pytest.mark.parametrize("qty", [0.0, -1.0, float("nan"), float("inf")])
async def test_live_send_refuses_invalid_order_quantity(qty: float) -> None:
    """Live send refuses invalid order quantity.

    Given: An invalid quantity.
    When: Sent.
    Then: No order identity is allocated.
    """
    engine = _engine()
    assert await engine._send_order("buy", qty, 100.0, "test", policy=_policy()) is None
    engine.execution_socket.tracker.next_sequence.assert_not_called()


@pytest.mark.parametrize("cap", ["NaN", "0", "-1", "Infinity"])
async def test_live_send_rejects_invalid_internal_policy(cap: str) -> None:
    """Live send rejects invalid internal policy.

    Given: A corrupt typed policy.
    When: Sent.
    Then: The live gate rejects it.
    """
    engine = _engine()
    assert await engine._send_order("buy", _QUANTITY, 100.0, "test", policy=_policy(cap)) is None
    engine.execution_socket.tracker.next_sequence.assert_not_called()


async def test_paper_send_rejects_live_quantity() -> None:
    """Paper sends reject live quantities.

    Given: A paper engine and a quantity prepared for a live order.
    When: The quantity reaches the send boundary.
    Then: The engine raises a type error before allocating order IDs.
    """
    engine = _engine()
    engine.exchange = ExchangeEnum.PAPER
    with pytest.raises(TypeError, match="Paper orders require a paper quantity"):
        await engine._send_order("buy", _LiveQuantity(Decimal("0.00006"), _QUANTITY), 100.0, "test")
    engine.execution_socket.tracker.next_sequence.assert_not_called()


async def test_invalid_live_create_and_parameter_update_do_not_commit(live_db: _Config) -> None:
    """Invalid live create and parameter update do not commit.

    Given: An invalid cap.
    When: Creating or replacing parameters.
    Then: No version changes.
    """
    invalid = {**_params(), "max_order_base_qty": "0.001"}
    with pytest.raises(StrategyScopeError, match="max_order_base_qty"):
        await _create(live_db, "invalid", parameters=invalid)
    await _create(live_db, "process_btc")
    with pytest.raises(StrategyScopeError, match="max_order_base_qty"):
        await live_db.writer.update_process_config_parameters(
            name="process_btc", parameters=invalid, updated_by="ops"
        )
    async with live_db.repo.session() as session:
        current = await load_current_process_configs(session)
        assert await session.scalar(select(func.count()).select_from(Setting)) == 1
    assert current["process_btc"]["parameters"] == _params()


async def test_live_restart_loads_current_cap_from_persistence(
    live_db: _Config, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Live restart loads current cap from persistence.

    Given: A changed cap.
    When: The watchdog restarts a strategy.
    Then: It reapplies the limit.
    """
    launcher = ProcessLauncherService(live_db.settings)
    spawn = AsyncMock()
    monkeypatch.setattr(launcher, "start_process", spawn)
    monkeypatch.setattr(launcher, "_start_native_process_monitoring", MagicMock())
    name = "process_btc"
    await _create(live_db, name, enabled=True)
    await launcher.start_strategy_by_name(name, None, [_OPERATOR])
    assert spawn.await_args is not None
    original = spawn.await_args.args[0]
    assert original.parameters["max_order_base_qty"] == _QUANTITY
    await launcher._check_live_config_before_spawn(original)
    await live_db.writer.update_process_config_parameters(
        name=name, parameters={**_params(), "max_order_base_qty": 0.00004}, updated_by="ops"
    )
    with pytest.raises(StrategyScopeError, match="configuration changed"):
        await launcher._check_live_config_before_spawn(original)
    await launcher._restart_configured_process(original)
    assert spawn.await_args is not None
    assert spawn.await_args.args[0].parameters["max_order_base_qty"] == 0.00004
    await live_db.writer.update_process_config(name=name, enabled=False, updated_by="ops")
    with pytest.raises(StrategyScopeError, match="before starting"):
        await launcher._restart_configured_process(original)


async def test_postgres_process_fence_uses_transaction_lock(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """PostgreSQL process fence uses transaction lock.

    Given: A PostgreSQL process writer.
    When: Reserving it.
    Then: Lock across all process names.
    """
    session = MagicMock(spec=AsyncSession)
    session.get_bind.return_value.dialect.name = "postgresql"
    reserve = AsyncMock()
    monkeypatch.setattr("snapper.data.repository.begin_scd2_write", reserve)
    await begin_process_config_write(session)
    reserve.assert_awaited_once_with(session)
    session.execute.assert_awaited_once()
    assert "pg_advisory_xact_lock" in str(session.execute.call_args.args[0])


async def test_active_scope_edit_rejects_new_wallet_overlap(live_db: _Config) -> None:
    """Active scope edit rejects new wallet overlap.

    Given: Two enabled wallets.
    When: One strategy moves to the other's wallet.
    Then: Reject.
    """
    other_wallet = "2044c43c-b148-498a-a41b-142e568534af"
    provenance = {"timestamp": datetime.now(UTC), "session_id": "scope", "sequence_id": 2}
    async with live_db.repo.session() as session:
        session.add_all(
            [
                Wallet(public_id=other_wallet, label="other", is_paper=False, **provenance),
                WalletOperatorScopeGrant(
                    wallet_public_id=other_wallet,
                    operator_public_id=_OPERATOR,
                    granted_by_user_public_id=str(uuid4()),
                    scope_kind="instrument",
                    instrument_public_id=_INSTRUMENT_ID,
                    **provenance,
                ),
            ]
        )
        await session.commit()
    await _create(live_db, "process_btc", enabled=True)
    params = {**_params("other"), "wallet_public_id": "label:other"}
    await _create(live_db, "other", enabled=True, parameters=params)
    with pytest.raises(StrategyScopeError, match="overlap"):
        await live_db.writer.update_process_config_parameters(
            name="other", parameters=_params("other"), updated_by="ops"
        )
    assert (await resolve_live_strategy_start(live_db.repo, "other"))[
        "wallet_public_id"
    ] == other_wallet


@pytest.mark.parametrize("target", [5e-9, float("inf")])
async def test_live_sizing_refuses_dust_or_nonfinite_target(target: float) -> None:
    """Live sizing refuses dust or nonfinite target.

    Given: Dust or a nonfinite target.
    When: Sized.
    Then: No order identity is allocated.
    """
    engine = _engine()
    assert await engine.execute_desired_units(target, 100.0, policy=_policy()) is None
    engine.execution_socket.tracker.next_sequence.assert_not_called()


async def test_live_drawdown_preserves_cap_eligible_close() -> None:
    """Live drawdown preserves cap eligible close.

    Given: A flip under drawdown.
    When: Sized.
    Then: Only the close is submitted.
    """
    engine = _engine()
    assert engine._repository is not None
    engine._repository.insert_trade_command.return_value = (1, "order-command")
    engine.position_qty = _QUANTITY
    engine.peak_equity = 100_000.0
    result = await engine.execute_desired_units(-_QUANTITY, 100.0, policy=_policy())
    assert result == "order-command"
    row = engine._repository.insert_trade_command.call_args.args[0]
    assert row["quantity"] == _QUANTITY and row["reduce_only"] is True


async def test_execute_leaves_pending_intent_when_final_send_refuses() -> None:
    """Execute leaves pending intent when final send refuses.

    Given: A send-time refusal.
    When: Executing the prepared order.
    Then: Intent stays clear.
    """
    engine = _engine()
    engine._send_order = AsyncMock(return_value=None)
    assert await engine.execute_desired_units(_QUANTITY, 100.0, policy=_policy()) is None
    engine._send_order.assert_awaited_once()
    assert not engine.order_in_flight and engine.pending_client_order_id is None


def test_stale_paper_prepared_order_is_discarded() -> None:
    """Stale paper prepared order is discarded.

    Given: A prepared paper order with changed state.
    When: Checked.
    Then: It is stale.
    """
    engine = TradingEngineService(_INSTRUMENT, MagicMock(spec=MessagePublisher))
    prepared = engine.prepare_desired_units(_QUANTITY, 100.0)
    assert prepared is not None
    engine.position_qty = 1.0
    assert not engine._prepared_order_is_current(prepared, None)


async def test_live_route_refuses_conflicting_recovered_engine_wallet(
    live_db: _Config,
) -> None:
    """Live route refuses conflicting recovered engine wallet.

    Given: A conflicting engine owner at the routed key.
    When: Signaled.
    Then: Refuse.
    """
    await _create(live_db, "process_btc", enabled=True)
    trader = _trader(live_db)
    engine = _engine()
    engine.wallet_public_id = str(uuid4())
    trader.engines[trader._build_engine_key(_INSTRUMENT, "kraken", "live", _WALLET)] = engine
    await trader._on_signal(_signal(grouped=True))
    async with live_db.repo.session() as session:
        assert await session.scalar(select(func.count()).select_from(TradeCommand)) == 0
        assert await session.scalar(select(func.count()).select_from(PairedExecutionGroup)) == 0


@pytest.mark.parametrize(
    "settings",
    [
        {"class": ""},
        {"class": "Strategy", "enabled": "true"},
        {"class": "Strategy", "parameters": []},
        {"class": "Strategy", "template": 123},
    ],
)
async def test_strict_config_loader_rejects_invalid_shape(
    live_db: _Config, settings: JsonObject
) -> None:
    """Strict config loader rejects invalid shape.

    Given: Malformed process fields.
    When: Reading the current catalogue.
    Then: Report it.
    """
    await _seed_setting(live_db.repo, "process_malformed", json.dumps(settings))
    async with live_db.repo.session() as session:
        with pytest.raises(ValueError, match="Invalid"):
            await load_current_process_configs(session)


@pytest.mark.parametrize("trusted", [False, True])
async def test_named_paper_start_needs_no_cap(
    live_db: _Config, monkeypatch: pytest.MonkeyPatch, trusted: bool
) -> None:
    """Named paper start needs no cap.

    Given: A paper strategy without a cap.
    When: Started by name.
    Then: It can spawn.
    """
    launcher = ProcessLauncherService(live_db.settings)
    spawn = AsyncMock()
    monkeypatch.setattr(launcher, "start_process", spawn)
    monkeypatch.setattr(launcher, "_start_native_process_monitoring", MagicMock())
    parameters = {"name": "paper", "inputs": ["ticks"], "outputs": [_INSTRUMENT]}
    await _create(live_db, "paper", parameters=parameters)
    if trusted:
        await launcher.start_strategy_by_name("paper", None, [])
    else:
        await launcher.start_process_by_name("paper")
    spawn.assert_awaited_once()
    assert spawn.await_args is not None
    assert spawn.await_args.args[0].parameters == parameters
    await live_db.writer.update_process_config(name="paper", enabled=True, updated_by="ops")
    with pytest.raises(StrategyScopeError, match="no longer a live strategy"):
        await resolve_live_strategy_start(live_db.repo, "paper")


async def test_trusted_start_never_launches_a_nonstrategy_row(live_db: _Config) -> None:
    """Trusted start never launches a nonstrategy row.

    Given: A named row changed to a generic process.
    When: Strategy START runs.
    Then: Refuse.
    """
    launcher = ProcessLauncherService(live_db.settings)
    await _seed_setting(live_db.repo, "process_changed", '{"class": "Core", "role": "core"}')
    result = await launcher.start_strategy_by_name("changed", None, [])
    assert "no longer a strategy" in result.message


async def test_invalid_supplied_paper_cap_is_rejected_at_write(live_db: _Config) -> None:
    """Invalid supplied paper cap is rejected at write.

    Given: An optional but invalid paper cap.
    When: Persisted.
    Then: Validation fails.
    """
    params = {
        "name": "paper",
        "inputs": ["ticks"],
        "outputs": [_INSTRUMENT],
        "max_order_base_qty": 0,
    }
    with pytest.raises(StrategyScopeError, match="max_order_base_qty"):
        await _create(live_db, "paper", parameters=params)
