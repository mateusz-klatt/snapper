"""Tests of the cancel-target repair (G3-C): evaluation, prediction, preview and write."""

import json
from collections.abc import AsyncIterator
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from pathlib import Path
from typing import cast
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest
import sqlalchemy as sa

from snapper.application.engine.service import compute_shard_key
from snapper.application.trade import cancel_target_repair as repair
from snapper.application.trade.cancel_target_repair import CancelTargetRepairRefusedError
from snapper.application.trade.cancel_target_repair import ExecutionClass
from snapper.application.trade.cancel_target_repair import RepairAction
from snapper.application.trade.reconciler import ReconciliationLoop
from snapper.application.trade.trade_service import TradeService
from snapper.core.json_types import JsonValue
from snapper.core.types import ExchangeEnum
from snapper.core.types import ExecutionModeEnum
from snapper.core.types import TradeCommandStatusEnum
from snapper.data.models import KNOWN_TO_MAX
from snapper.data.models import Order
from snapper.data.models import TradeCommand
from snapper.data.models import TradeProjectionCheckpoint
from snapper.data.models import VenueEvent
from snapper.data.repository import SQLAlchemyRepository
from snapper.data.repository_types import ExecutionEvidenceRow
from snapper.data.repository_types import OrderVersionRow
from snapper.data.repository_types import TradeCommandRow
from snapper.data.repository_types import VenueEventInsertRow
from snapper.messaging.executors.base import ExchangeExecutorService
from snapper.messaging.infrastructure.publisher import SequenceTracker
from tests.helpers.cancel_target_repair import CANCEL_AT
from tests.helpers.cancel_target_repair import CANCELED_AT
from tests.helpers.cancel_target_repair import CREATED_AT
from tests.helpers.cancel_target_repair import KRAKEN_SHARD
from tests.helpers.cancel_target_repair import OTHER_WALLET
from tests.helpers.cancel_target_repair import SESSION
from tests.helpers.cancel_target_repair import WALLET
from tests.helpers.cancel_target_repair import SeededPair
from tests.helpers.cancel_target_repair import add_rows
from tests.helpers.cancel_target_repair import checkpoint
from tests.helpers.cancel_target_repair import command
from tests.helpers.cancel_target_repair import execution
from tests.helpers.cancel_target_repair import order_version
from tests.helpers.cancel_target_repair import seed_kraken_pair
from tests.helpers.cancel_target_repair import seed_walutomat_pair
from tests.helpers.cancel_target_repair import sqlite_repository
from tests.helpers.cancel_target_repair import venue_event

DATABASE = {"dialect": "sqlite", "path": "test.db"}
CODE = {"snapper/application/trade/cancel_target_repair.py": "0" * 64}
WRITE_TIME = datetime(2026, 11, 15, 12, 0, tzinfo=UTC)


@pytest.fixture
async def repository(tmp_path: Path) -> AsyncIterator[SQLAlchemyRepository]:
    """Provide an empty SQLite repository owned by one test."""
    async with sqlite_repository(tmp_path / "repair.db") as created:
        yield created


async def _plan(repository: SQLAlchemyRepository, *cancel_ids: str) -> repair.RepairPlan:
    """Plan the repair of the given cancels with the fixed binding."""
    return await repair.plan_repair(repository, list(cancel_ids), DATABASE, CODE)


async def _decision(repository: SQLAlchemyRepository, cancel_id: str) -> repair.Decision:
    """Return the decision of one cancel."""
    plan = await _plan(repository, cancel_id)
    return plan.lines[0].decision


def _session(plan: repair.RepairPlan) -> repair.WriteSession:
    """Open a write session over a plan, as prepare_write would."""
    return repair.WriteSession(
        plan=plan,
        session_id="00000000-0000-7000-8000-0000000000ab",
        write_time=WRITE_TIME,
        results=[],
    )


async def _write(
    repository: SQLAlchemyRepository, cancel_id: str, voted: Path, digest: str
) -> repair.WriteSession:
    """Prepare and apply a voted write of one cancel."""
    session = await repair.prepare_write(
        repository, [cancel_id], (voted, digest), (DATABASE, CODE), WRITE_TIME
    )
    await repair.apply_write(repository, session)
    return session


def _vote(tmp_path: Path, plan: repair.RepairPlan) -> Path:
    """Store a plan's preview and summary as a voted run's directory."""
    voted = tmp_path / "voted"
    voted.mkdir()
    (voted / "preview.json").write_text(json.dumps(plan.preview))
    (voted / "summary.json").write_text(json.dumps(plan.summary))
    return voted


async def _terminals(repository: SQLAlchemyRepository, client_order_id: str) -> list[VenueEvent]:
    """Read the order_terminal rows of one client order id."""
    async with repository.session() as session:
        result = await session.execute(
            sa.select(VenueEvent).where(
                VenueEvent.client_order_id == client_order_id,
                VenueEvent.event_type == "order_terminal",
            )
        )
        return list(result.scalars().all())


async def _status(repository: SQLAlchemyRepository, public_id: str) -> tuple[str, str | None]:
    """Read a command's active status and last error."""
    async with repository.session() as session:
        result = await session.execute(
            sa.select(TradeCommand).where(
                TradeCommand.public_id == public_id, TradeCommand.known_to == KNOWN_TO_MAX
            )
        )
        row = result.scalars().one()
        return row.status, row.last_error


async def _seed_three_pairs(repository: SQLAlchemyRepository) -> list[SeededPair]:
    """Seed production's three Kraken pairs, all on the one shard."""
    return [await seed_kraken_pair(repository, index) for index in (1, 2, 3)]


async def _record_first_terminal(repository: SQLAlchemyRepository, plan: repair.RepairPlan) -> None:
    """Record the first line's terminal, as an earlier, partly applied run would have."""
    row = repair.terminal_fields(cast(repair.KrakenCase, plan.lines[0].decision.kraken))
    row["timestamp"] = WRITE_TIME
    row["session_id"] = SESSION
    row["sequence_id"] = 1
    assert await repository.insert_order_terminal_once(row)


async def _retire(repository: SQLAlchemyRepository, create_id: str) -> None:
    """Retire one accepted create to cancelled, as the reconciler's fold would."""
    assert await repository.advance_trade_command_lifecycle(
        create_id,
        "accepted",
        TradeCommandStatusEnum.CANCELLED,
        WRITE_TIME,
        SESSION,
        9,
        terminal_at=CANCELED_AT,
    )


async def _move_to_wallet(repository: SQLAlchemyRepository, pair: SeededPair, wallet: str) -> str:
    """Move one seeded pair onto another wallet's XRP-EUR shard, and return that shard."""
    shard: str = compute_shard_key(
        instrument="XRP-EUR",
        exchange=ExchangeEnum.KRAKEN,
        mode=ExecutionModeEnum.LIVE,
        wallet_public_id=wallet,
        strategy_tag=None,
    )
    async with repository.session() as session:
        for model in (TradeCommand, VenueEvent):
            await session.execute(
                sa.update(model)
                .where(model.client_order_id == pair.client_order_id)
                .values(wallet_public_id=wallet, shard_key=shard)
            )
        await session.execute(
            sa.update(Order)
            .where(Order.client_order_id == pair.client_order_id)
            .values(wallet_public_id=wallet)
        )
        await session.commit()
    return shard


def _line_results(session: repair.WriteSession) -> list[JsonValue]:
    """Return each applied line's result string."""
    return [entry["result"] for entry in session.results if isinstance(entry, dict)]


def _version(status: str, version_id: int, known_to: datetime = KNOWN_TO_MAX) -> OrderVersionRow:
    """Build one order version row for the pure helpers."""
    return {
        "id": version_id,
        "public_id": "order",
        "instrument_public_id": "instrument",
        "mode": "live",
        "wallet_public_id": WALLET,
        "client_order_id": "cid",
        "exchange_order_id": "OX-1",
        "status": status,
        "filled_size": 0.0,
        "leverage": None,
        "timestamp": CREATED_AT + timedelta(seconds=version_id),
        "known_to": known_to,
    }


def _execution_row(
    size: float, fee: float, exec_id: str | None = None, trade_id: str | None = None
) -> ExecutionEvidenceRow:
    """Build one execution evidence row for the classifier."""
    return {
        "id": 1,
        "order_public_id": "order",
        "status": "filled",
        "size": size,
        "fee": fee,
        "price": 0.0,
        "exec_id": exec_id,
        "trade_id": trade_id,
        "numeric_provenance": "legacy_float",
        "price_decimal": None,
        "size_decimal": None,
        "fee_decimal": None,
        "timestamp": CREATED_AT,
    }


@pytest.mark.parametrize(
    ("row", "expected"),
    [
        (_execution_row(0.0, 0.0), ExecutionClass.EMPTY),
        (_execution_row(0.0, 0.0, exec_id="  "), ExecutionClass.COUNTED),
        (_execution_row(0.0, 0.0, exec_id="E-1"), ExecutionClass.COUNTED),
        (_execution_row(0.0, 0.0, trade_id="T-1"), ExecutionClass.COUNTED),
        (_execution_row(1.0, 0.0), ExecutionClass.COUNTED),
        (_execution_row(1.0, -0.1), ExecutionClass.ANOMALOUS),
        (_execution_row(0.0, 0.5), ExecutionClass.ANOMALOUS),
        (_execution_row(-1.0, 0.0), ExecutionClass.ANOMALOUS),
        (_execution_row(float("nan"), 0.0), ExecutionClass.ANOMALOUS),
        (_execution_row(0.0, float("inf")), ExecutionClass.ANOMALOUS),
    ],
)
def test_classify_execution_applies_condition_one_in_order(
    row: ExecutionEvidenceRow, expected: ExecutionClass
) -> None:
    """Condition 1 classifies each row, anomalous first, then counted, then empty.

    Given: An execution row,
    When: It is classified,
    Then: Anomalous values win over identities, a blank id is absent, and the rest is empty.
    """
    assert repair.classify_execution(row) is expected


def test_canceled_version_is_the_start_of_the_trailing_canceled_run() -> None:
    """The canceled version is the first of the canceled run that ends the history.

    Given: Versions open, canceled, canceled, and a history whose last version is open,
    When: The canceled version is taken,
    Then: It is the first canceled version of the trailing run, or None.
    """
    versions = [_version("open", 1), _version("canceled", 2), _version("canceled", 3)]
    assert repair.canceled_version(versions) is versions[1]
    assert repair.canceled_version([_version("canceled", 1), _version("open", 2)]) is None
    only_canceled = [_version("canceled", 1), _version("canceled", 2)]
    assert repair.canceled_version(only_canceled) is only_canceled[0]


@pytest.mark.asyncio
async def test_record_terminal_end_to_end_matches_the_reconciler(
    repository: SQLAlchemyRepository, tmp_path: Path
) -> None:
    """A voted write records the terminal once, and one cycle advances both commands.

    Given: A Kraken create in accepted, its cancel in dispatched, the order canceled
        4 s after the cancel, no execution and only order_accepted,
    When: The plan is voted, written and one reconciliation cycle runs,
    Then: One terminal like the executor's exists, and the cycle's advances equal the prediction.
    """
    pair = await seed_kraken_pair(repository)
    plan = await _plan(repository, pair.cancel_id)
    line = plan.lines[0]
    assert line.decision.action is RepairAction.RECORD_TERMINAL
    predicted = line.preview["record_terminal"]
    assert isinstance(predicted, dict)
    assert predicted["predicted"] == {
        "target": {"from": "accepted", "to": "cancelled"},
        "cancel": {
            "from": "dispatched",
            "to": "cancelled",
            "last_error": "target order ended canceled",
        },
    }
    assert predicted["gap_seconds"] == pytest.approx(4.0)

    voted = _vote(tmp_path, plan)
    written = await _write(repository, pair.cancel_id, voted, plan.digest)
    assert written.plan.digest == plan.digest
    terminals = await _terminals(repository, pair.client_order_id)
    assert len(terminals) == 1
    terminal = terminals[0]
    assert written.results == [
        {
            "cancel_public_id": pair.cancel_id,
            "result": "recorded",
            "sequence_id": 1,
            "venue_event_id": terminal.id,
            "venue_event_public_id": terminal.public_id,
        }
    ]
    receipt = repair.receipt_document(written)
    assert (receipt["session_id"], receipt["write_time"]) == (
        written.session_id,
        WRITE_TIME.isoformat(),
    )
    assert terminal.session_id == written.session_id
    assert (terminal.status, terminal.shard_key, terminal.wallet_public_id) == (
        "canceled",
        KRAKEN_SHARD,
        WALLET,
    )
    assert terminal.received_at == CANCELED_AT
    assert terminal.timestamp == WRITE_TIME
    assert terminal.sequence_id == 1
    assert (terminal.fill_size, terminal.cum_fill_size, terminal.command_public_id) == (
        None,
        None,
        None,
    )

    loop = ReconciliationLoop(
        exchange_name="kraken",
        repository=repository,
        trade_service=MagicMock(spec=TradeService),
        interval_seconds=60.0,
    )
    await loop._reconcile_cycle()
    assert await _status(repository, pair.create_id) == ("cancelled", None)
    assert await _status(repository, pair.cancel_id) == (
        "cancelled",
        "target order ended canceled",
    )

    rerun = await _plan(repository, pair.cancel_id)
    assert rerun.lines[0].decision.action is RepairAction.ALREADY_RECORDED


@pytest.mark.asyncio
async def test_planned_terminal_matches_the_executor_row(repository: SQLAlchemyRepository) -> None:
    """The planned row has the fields the executor's own builder gives a confirmed cancel.

    Given: A Kraken pair ready for a terminal,
    When: The planned fields are compared with _venue_event_row for the same inputs,
    Then: They agree on every field except the write-time ones.
    """
    pair = await seed_kraken_pair(repository)
    plan = await _plan(repository, pair.cancel_id)
    case = plan.lines[0].decision.kraken
    assert case is not None
    planned = repair.terminal_fields(case)
    executor = MagicMock()
    executor.wallet_public_id = WALLET
    executor._tracker = SequenceTracker()
    built = ExchangeExecutorService._venue_event_row(
        executor,
        {
            "event_type": "order_terminal",
            "exchange_name": "kraken",
            "instrument": "XRP-EUR",
            "exchange_order_id": "OX-1",
            "client_order_id": pair.client_order_id,
            "status": "canceled",
        },
    )
    assert built is not None
    write_time_fields = {"received_at", "timestamp", "session_id", "sequence_id"}
    built_fields: dict[str, object] = dict(built)
    planned_fields: dict[str, object] = dict(planned)
    assert set(planned_fields) | {"timestamp", "session_id", "sequence_id"} == set(built_fields)
    for key, value in planned_fields.items():
        if key not in write_time_fields:
            assert built_fields[key] == value, key


@pytest.mark.asyncio
async def test_fail_cancel_end_to_end(repository: SQLAlchemyRepository, tmp_path: Path) -> None:
    """A never-submitted Walutomat cancel becomes failed with target_never_submitted.

    Given: A Walutomat create failed by the interlock, with no exchange order id and
        no submit evidence, and its cancel in dispatched,
    When: The plan is voted and written,
    Then: The cancel is failed with target_never_submitted, and a rerun says already_failed.
    """
    pair = await seed_walutomat_pair(repository)
    plan = await _plan(repository, pair.cancel_id)
    assert plan.lines[0].decision.action is RepairAction.FAIL_CANCEL
    voted = _vote(tmp_path, plan)
    written = await _write(repository, pair.cancel_id, voted, plan.digest)
    assert written.results == [
        {"cancel_public_id": pair.cancel_id, "result": "failed", "sequence_id": 1}
    ]
    assert await _status(repository, pair.cancel_id) == ("failed", "target_never_submitted")
    rerun = await _plan(repository, pair.cancel_id)
    assert rerun.lines[0].decision.action is RepairAction.ALREADY_FAILED


@pytest.mark.asyncio
async def test_fail_cancel_takes_the_target_terminal_time_when_present(
    repository: SQLAlchemyRepository,
) -> None:
    """The failed cancel's terminal time is the target's own when it has one.

    Given: A Walutomat target with terminal_at set,
    When: The cancel is failed,
    Then: The compare-and-set receives that terminal time.
    """
    pair = await seed_walutomat_pair(repository)
    async with repository.session() as session:
        await session.execute(
            sa.update(TradeCommand)
            .where(TradeCommand.public_id == pair.create_id)
            .values(terminal_at=CREATED_AT)
        )
        await session.commit()
    plan = await _plan(repository, pair.cancel_id)
    with patch.object(
        repository, "advance_trade_command_lifecycle", AsyncMock(return_value=False)
    ) as cas:
        session = _session(plan)
        await repair.apply_write(repository, session)
    assert cas.call_args.kwargs["terminal_at"] == CREATED_AT
    assert session.results == [
        {"cancel_public_id": pair.cancel_id, "result": "lost_compare_and_set", "sequence_id": 1}
    ]


@pytest.mark.asyncio
async def test_first_failed_time_returns_none_without_a_failed_version() -> None:
    """Without a failed version there is no fallback terminal time.

    Given: No command versions,
    When: The first failed time is read,
    Then: It is None.
    """
    assert repair._first_failed_time([]) is None


@pytest.mark.asyncio
async def test_first_failed_time_skips_earlier_versions(repository: SQLAlchemyRepository) -> None:
    """The fallback terminal time is the first failed version's, after earlier ones.

    Given: A command with a closed dispatched version and a later failed one,
    When: The first failed time is read from its versions,
    Then: It is the failed version's timestamp.
    """
    public_id = "00000000-0000-7000-8000-0000000000b1"
    dispatched = command(public_id, command_type="create", status="dispatched", client_order_id="c")
    dispatched.known_to = CANCEL_AT
    failed = command(
        public_id,
        command_type="create",
        status="failed",
        client_order_id="c",
        created_at=CANCEL_AT,
    )
    await add_rows(repository, [dispatched])
    await add_rows(repository, [failed])
    versions = await repair.reads.get_trade_command_versions(repository, public_id)
    assert repair._first_failed_time(versions) == CANCEL_AT


@pytest.mark.asyncio
async def test_missing_and_foreign_cancels_refuse(repository: SQLAlchemyRepository) -> None:
    """Stage A refuses a missing cancel and a command that is not a cancel.

    Given: An unknown public id and a create command,
    When: Each is evaluated,
    Then: They refuse as cancel_missing and not_a_cancel.
    """
    pair = await seed_kraken_pair(repository)
    plan = await _plan(repository, "00000000-0000-7000-8000-0000000000ff", pair.create_id)
    assert [line.decision.reason for line in plan.lines] == ["cancel_missing", "not_a_cancel"]


@pytest.mark.asyncio
async def test_missing_and_ambiguous_targets_refuse(repository: SQLAlchemyRepository) -> None:
    """Stage B refuses a cancel without a target, and one with two active targets.

    Given: A cancel whose client order id has no create, and one with two creates,
    When: Each is evaluated,
    Then: They refuse as target_missing and target_ambiguous.
    """
    lonely = command(
        "00000000-0000-7000-8000-0000000000e1",
        command_type="cancel",
        status="dispatched",
        client_order_id="cid-none",
    )
    pair = await seed_kraken_pair(repository)
    twin = command(
        "00000000-0000-7000-8000-0000000000e2",
        command_type="submit",
        status="accepted",
        client_order_id=pair.client_order_id,
    )
    await add_rows(repository, [lonely, twin])
    plan = await _plan(repository, lonely.public_id, pair.cancel_id)
    assert [line.decision.reason for line in plan.lines] == ["target_missing", "target_ambiguous"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("status", "expected"),
    [
        ("filled", (RepairAction.REFUSE, "terminal_exists_filled")),
        (None, (RepairAction.REFUSE, "terminal_exists_none")),
        ("CANCELLED", (RepairAction.ALREADY_RECORDED, None)),
    ],
)
async def test_existing_terminals_decide_before_the_branch(
    repository: SQLAlchemyRepository,
    status: str | None,
    expected: tuple[RepairAction, str | None],
) -> None:
    """Stage C refuses a non-cancel terminal and reports a cancel terminal as recorded.

    Given: A Kraken pair with an existing order_terminal of the given status,
    When: The cancel is evaluated,
    Then: The decision follows the terminal, case-insensitively.
    """
    pair = await seed_kraken_pair(repository)
    await add_rows(
        repository,
        [venue_event(pair.client_order_id, event_type="order_terminal", status=status)],
    )
    decision = await _decision(repository, pair.cancel_id)
    assert (decision.action, decision.reason) == expected


@pytest.mark.asyncio
async def test_a_non_cancel_terminal_wins_over_a_cancel_terminal(
    repository: SQLAlchemyRepository,
) -> None:
    """A filled terminal refuses even when a canceled one exists as well.

    Given: Both a canceled and a filled order_terminal for the order,
    When: The cancel is evaluated,
    Then: It refuses as terminal_exists_filled.
    """
    pair = await seed_kraken_pair(repository)
    await add_rows(
        repository,
        [
            venue_event(pair.client_order_id, event_type="order_terminal", status="canceled"),
            venue_event(pair.client_order_id, event_type="order_terminal", status="filled"),
        ],
    )
    decision = await _decision(repository, pair.cancel_id)
    assert decision.reason == "terminal_exists_filled"


async def _set_command(
    repository: SQLAlchemyRepository, public_id: str, **values: str | int | None
) -> None:
    """Update one seeded command's columns in place."""
    async with repository.session() as session:
        await session.execute(
            sa.update(TradeCommand).where(TradeCommand.public_id == public_id).values(**values)
        )
        await session.commit()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("values", "reason"),
    [
        ({"status": "cancelled"}, "cancel_not_retirable"),
        ({"status": "failed", "last_error": "other"}, "cancel_not_retirable"),
    ],
)
async def test_cancel_states_that_g2_would_not_retire_refuse(
    repository: SQLAlchemyRepository, values: dict[str, str | int | None], reason: str
) -> None:
    """Stage E refuses a cancel outside G2's source statuses.

    Given: A cancel already cancelled, or failed for another reason,
    When: It is evaluated,
    Then: It refuses as cancel_not_retirable.
    """
    pair = await seed_kraken_pair(repository)
    await _set_command(repository, pair.cancel_id, **values)
    decision = await _decision(repository, pair.cancel_id)
    assert decision.reason == reason


@pytest.mark.asyncio
async def test_an_unsupported_exchange_refuses(repository: SQLAlchemyRepository) -> None:
    """Stage F refuses a target on another exchange.

    Given: A pair whose commands are on kraken_futures,
    When: The cancel is evaluated,
    Then: It refuses as unsupported_exchange.
    """
    pair = await seed_kraken_pair(repository)
    await _set_command(repository, pair.create_id, exchange="kraken_futures")
    await _set_command(repository, pair.cancel_id, exchange="kraken_futures")
    decision = await _decision(repository, pair.cancel_id)
    assert decision.reason == "unsupported_exchange"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("values", "reason"),
    [
        ({"mode": "paper"}, "not_kraken_spot"),
        ({"leverage": 2}, "not_kraken_spot"),
        ({"status": "created"}, "target_not_foldable"),
        ({"wallet_public_id": OTHER_WALLET}, "order_missing"),
    ],
)
async def test_kraken_target_checks_refuse(
    repository: SQLAlchemyRepository, values: dict[str, str | int | None], reason: str
) -> None:
    """Stage H refuses a target that is not live spot, not foldable, or without its order.

    Given: A Kraken target changed as stated,
    When: The cancel is evaluated,
    Then: It refuses with the stated reason.
    """
    pair = await seed_kraken_pair(repository)
    await _set_command(repository, pair.create_id, **values)
    decision = await _decision(repository, pair.cancel_id)
    assert decision.reason == reason


async def _set_orders(repository: SQLAlchemyRepository, public_id: str, **values: object) -> None:
    """Update every version of one seeded order in place."""
    async with repository.session() as session:
        await session.execute(sa.update(Order).where(Order.public_id == public_id).values(**values))
        await session.commit()


async def _active_order_status(
    repository: SQLAlchemyRepository, public_id: str, status: str
) -> None:
    """Set the status of one seeded order's active version."""
    async with repository.session() as session:
        await session.execute(
            sa.update(Order)
            .where(Order.public_id == public_id, Order.known_to == KNOWN_TO_MAX)
            .values(status=status)
        )
        await session.commit()


@pytest.mark.asyncio
async def test_two_orders_for_one_target_refuse(repository: SQLAlchemyRepository) -> None:
    """Stage H refuses when the target's client order id names two orders.

    Given: A second order with the same client order id, wallet and mode,
    When: The cancel is evaluated,
    Then: It refuses as order_ambiguous.
    """
    pair = await seed_kraken_pair(repository)
    second = order_version(
        "00000000-0000-7000-8000-0000000000d2",
        status="canceled",
        client_order_id=pair.client_order_id,
        timestamp=CANCELED_AT,
    )
    second.instrument_public_id = "00000000-0000-7000-8000-0000000000c2"
    await add_rows(repository, [second])
    decision = await _decision(repository, pair.cancel_id)
    assert decision.reason == "order_ambiguous"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("change", "reason"),
    [
        ("leverage", "not_kraken_spot"),
        ("not_canceled", "order_not_canceled"),
        ("same_time", "canceled_not_after_cancel"),
        ("closed_last", "order_versions_inconsistent"),
    ],
)
async def test_order_history_checks_refuse(
    repository: SQLAlchemyRepository, change: str, reason: str
) -> None:
    """Stage H refuses margin orders, inconsistent histories and late or missing cancels.

    Given: The seeded order changed as stated,
    When: The cancel is evaluated,
    Then: It refuses with the stated reason; an equal time refuses because G2 is strict.
    """
    pair = await seed_kraken_pair(repository)
    if change == "leverage":
        await _set_orders(repository, pair.order_id, leverage=2)
    elif change == "not_canceled":
        await _active_order_status(repository, pair.order_id, "open")
    elif change == "same_time":
        async with repository.session() as session:
            await session.execute(
                sa.update(TradeCommand)
                .where(TradeCommand.public_id == pair.cancel_id)
                .values(created_at=CANCELED_AT)
            )
            await session.commit()
    else:
        await add_rows(
            repository,
            [
                order_version(
                    pair.order_id,
                    status="canceled",
                    client_order_id=pair.client_order_id,
                    timestamp=CANCELED_AT + timedelta(seconds=1),
                    known_to=CANCELED_AT + timedelta(seconds=2),
                )
            ],
        )
    decision = await _decision(repository, pair.cancel_id)
    assert decision.reason == reason


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("shape", "reason"),
    [
        ((1.0, 0.0, None, None), "has_execution"),
        ((0.0, 0.0, "E-1", None), "has_execution"),
        ((0.0, 0.0, None, "T-1"), "has_execution"),
        ((0.0, 0.5, None, None), "anomalous_execution"),
    ],
)
async def test_executions_refuse_under_condition_one(
    repository: SQLAlchemyRepository,
    shape: tuple[float, float, str | None, str | None],
    reason: str,
) -> None:
    """Condition 1 refuses a counted or anomalous execution.

    Given: One execution of the stated size, fee, exec id and trade id,
    When: The cancel is evaluated,
    Then: It refuses with the stated reason.
    """
    size, fee, exec_id, trade_id = shape
    pair = await seed_kraken_pair(repository)
    await add_rows(
        repository,
        [execution(pair.order_id, size=size, fee=fee, exec_id=exec_id, trade_id=trade_id)],
    )
    decision = await _decision(repository, pair.cancel_id)
    assert decision.reason == reason


@pytest.mark.asyncio
async def test_an_empty_execution_is_listed_and_does_not_refuse(
    repository: SQLAlchemyRepository,
) -> None:
    """A zero-size row with no id, like row 5, is listed as empty and does not refuse.

    Given: One execution of size 0, fee 0 and no ids,
    When: The cancel is evaluated,
    Then: The line records the terminal, and its preview lists the row as empty.
    """
    pair = await seed_kraken_pair(repository)
    await add_rows(repository, [execution(pair.order_id)])
    plan = await _plan(repository, pair.cancel_id)
    assert plan.lines[0].decision.action is RepairAction.RECORD_TERMINAL
    executions = plan.lines[0].preview["executions"]
    assert isinstance(executions, list) and isinstance(executions[0], dict)
    assert executions[0]["class"] == "empty"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "change", ["no_wallet", "no_exchange_order_id", "bad_shard", "other_shard", "order_mode"]
)
async def test_lineage_checks_refuse(repository: SQLAlchemyRepository, change: str) -> None:
    """Stage H refuses when the wallet, the venue id or the shard key cannot be reproduced.

    Given: The seeded lineage changed as stated,
    When: The cancel is evaluated,
    Then: It refuses as lineage_mismatch.
    """
    pair = await seed_kraken_pair(repository)
    if change == "no_wallet":
        await _set_command(repository, pair.create_id, wallet_public_id="")
        await _set_orders(repository, pair.order_id, wallet_public_id="")
    elif change == "no_exchange_order_id":
        await _set_orders(repository, pair.order_id, exchange_order_id="  ")
    elif change == "bad_shard":
        await _set_command(repository, pair.create_id, shard_key="kraken")
    elif change == "other_shard":
        await _set_command(repository, pair.create_id, shard_key="kraken.XRP-EUR.live")
    else:
        await _active_order_status(repository, pair.order_id, "canceled")
        async with repository.session() as session:
            await session.execute(
                sa.update(Order)
                .where(Order.public_id == pair.order_id, Order.known_to != KNOWN_TO_MAX)
                .values(mode="paper")
            )
            await session.commit()
    decision = await _decision(repository, pair.cancel_id)
    assert decision.reason == "lineage_mismatch"


@pytest.mark.asyncio
@pytest.mark.parametrize("command_type", ["create", "submit", "replace"])
async def test_another_open_command_on_the_shard_refuses(
    repository: SQLAlchemyRepository, command_type: str
) -> None:
    """Stage H refuses when the shard holds another open command of any open type.

    Given: Another active, non-terminal command of the stated type on the shard,
    When: The cancel is evaluated,
    Then: It refuses as shard_has_other_open_command.
    """
    pair = await seed_kraken_pair(repository)
    await add_rows(
        repository,
        [
            command(
                "00000000-0000-7000-8000-0000000000f1",
                command_type=command_type,
                status="accepted",
                client_order_id="cid-newer",
            )
        ],
    )
    decision = await _decision(repository, pair.cancel_id)
    assert decision.reason == "shard_has_other_open_command"


@pytest.mark.asyncio
async def test_a_checkpoint_holding_another_command_refuses(
    repository: SQLAlchemyRepository,
) -> None:
    """Stage H refuses when the shard checkpoint holds another open command.

    Given: A checkpoint whose open command ids name another command,
    When: The cancel is evaluated,
    Then: It refuses as shard_checkpoint_open_command.
    """
    pair = await seed_kraken_pair(repository)
    await add_rows(repository, [checkpoint(open_command_ids=json.dumps(["other-command"]))])
    decision = await _decision(repository, pair.cancel_id)
    assert decision.reason == "shard_checkpoint_open_command"


@pytest.mark.asyncio
async def test_a_checkpoint_holding_only_the_target_passes(
    repository: SQLAlchemyRepository,
) -> None:
    """A checkpoint that holds only the target does not refuse.

    Given: A checkpoint whose open command ids are the target alone,
    When: The cancel is evaluated,
    Then: The line records the terminal.
    """
    pair = await seed_kraken_pair(repository)
    await add_rows(repository, [checkpoint(open_command_ids=json.dumps([pair.create_id]))])
    decision = await _decision(repository, pair.cancel_id)
    assert decision.action is RepairAction.RECORD_TERMINAL


@pytest.mark.asyncio
async def test_a_second_wallet_on_the_shard_refuses(repository: SQLAlchemyRepository) -> None:
    """Stage H refuses when the shard names a wallet other than the target's.

    Given: A venue event on the shard from another wallet,
    When: The cancel is evaluated,
    Then: It refuses as shard_wallet_mismatch.
    """
    pair = await seed_kraken_pair(repository)
    await add_rows(
        repository,
        [venue_event("cid-other", event_type="order_accepted", wallet_public_id=OTHER_WALLET)],
    )
    decision = await _decision(repository, pair.cancel_id)
    assert decision.reason == "shard_wallet_mismatch"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("change", "reason"),
    [
        ("not_interlock", "target_not_interlock_blocked"),
        ("exchange_order_id", "target_has_exchange_order_id"),
        ("accepted_event", "target_has_submit_evidence"),
    ],
)
async def test_walutomat_checks_refuse(
    repository: SQLAlchemyRepository, change: str, reason: str
) -> None:
    """Stage G refuses unless the local records prove the target was never submitted.

    Given: The Walutomat pair changed as stated,
    When: The cancel is evaluated,
    Then: It refuses with the stated reason.
    """
    pair = await seed_walutomat_pair(repository)
    if change == "not_interlock":
        await _set_command(repository, pair.create_id, last_error="circuit_breaker_open")
    elif change == "exchange_order_id":
        walutomat_order = order_version(
            pair.order_id,
            status="canceled",
            client_order_id=pair.client_order_id,
            timestamp=CANCELED_AT,
        )
        walutomat_order.exchange_order_id = "W-1"
        await add_rows(repository, [walutomat_order])
    else:
        await add_rows(
            repository,
            [venue_event(pair.client_order_id, event_type="order_accepted", status="open")],
        )
    decision = await _decision(repository, pair.cancel_id)
    assert decision.reason == reason


@pytest.mark.asyncio
async def test_a_stale_vote_and_a_changed_state_refuse(
    repository: SQLAlchemyRepository, tmp_path: Path
) -> None:
    """A write refuses on a digest mismatch, and on a state that moved since the vote.

    Given: A voted plan,
    When: The write is given another digest, or a terminal appears before it,
    Then: Both refuse and nothing is written.
    """
    pair = await seed_kraken_pair(repository)
    plan = await _plan(repository, pair.cancel_id)
    voted = _vote(tmp_path, plan)
    with pytest.raises(CancelTargetRepairRefusedError, match="does not match"):
        await repair.prepare_write(
            repository, [pair.cancel_id], (voted, "0" * 64), (DATABASE, CODE), WRITE_TIME
        )
    await add_rows(
        repository,
        [venue_event(pair.client_order_id, event_type="order_terminal", status="filled")],
    )
    with pytest.raises(CancelTargetRepairRefusedError, match="differs from the vote"):
        await repair.prepare_write(
            repository, [pair.cancel_id], (voted, plan.digest), (DATABASE, CODE), WRITE_TIME
        )
    assert [row.status for row in await _terminals(repository, pair.client_order_id)] == ["filled"]


@pytest.mark.asyncio
async def test_a_plan_with_a_refused_line_does_not_write(
    repository: SQLAlchemyRepository, tmp_path: Path
) -> None:
    """A plan holding a refusal is refused whole.

    Given: A voted plan of one valid cancel and one unknown cancel,
    When: The write is prepared,
    Then: It refuses before anything is written.
    """
    pair = await seed_kraken_pair(repository)
    unknown = "00000000-0000-7000-8000-0000000000ff"
    plan = await _plan(repository, pair.cancel_id, unknown)
    voted = _vote(tmp_path, plan)
    with pytest.raises(CancelTargetRepairRefusedError, match="refused line"):
        await repair.prepare_write(
            repository,
            [pair.cancel_id, unknown],
            (voted, plan.digest),
            (DATABASE, CODE),
            WRITE_TIME,
        )
    assert await _terminals(repository, pair.client_order_id) == []


@pytest.mark.asyncio
async def test_a_change_before_the_insert_skips_the_line(
    repository: SQLAlchemyRepository,
) -> None:
    """The recheck before each insert skips a line whose shard changed.

    Given: A planned record_terminal line, then a newer open command on the shard,
    When: The stale plan is applied,
    Then: The line reports changed_before_write and no terminal is written.
    """
    pair = await seed_kraken_pair(repository)
    plan = await _plan(repository, pair.cancel_id)
    await add_rows(
        repository,
        [
            command(
                "00000000-0000-7000-8000-0000000000f2",
                command_type="create",
                status="accepted",
                client_order_id="cid-newer",
            )
        ],
    )
    session = _session(plan)
    await repair.apply_write(repository, session)
    assert session.results == [
        {"cancel_public_id": pair.cancel_id, "result": "changed_before_write"}
    ]
    assert await _terminals(repository, pair.client_order_id) == []


@pytest.mark.asyncio
async def test_a_terminal_recorded_concurrently_counts_as_already_recorded(
    repository: SQLAlchemyRepository,
) -> None:
    """An insert that finds a terminal already recorded is reported as such.

    Given: A planned line whose record-once insert returns False,
    When: The plan is applied,
    Then: The line reports already_recorded.
    """
    pair = await seed_kraken_pair(repository)
    plan = await _plan(repository, pair.cancel_id)
    session = _session(plan)
    with patch.object(repository, "insert_order_terminal_once", AsyncMock(return_value=False)):
        await repair.apply_write(repository, session)
    assert session.results == [
        {"cancel_public_id": pair.cancel_id, "result": "already_recorded", "sequence_id": 1}
    ]


@pytest.mark.asyncio
async def test_an_insert_that_cannot_be_read_back_is_unconfirmed(
    repository: SQLAlchemyRepository,
) -> None:
    """An insert reported as done but not found by the read-back is not counted as recorded.

    Given: A planned line whose record-once insert reports success but writes no row,
    When: The plan is applied,
    Then: The line reports recorded_unconfirmed with its sequence id.
    """
    pair = await seed_kraken_pair(repository)
    plan = await _plan(repository, pair.cancel_id)
    session = _session(plan)
    with patch.object(repository, "insert_order_terminal_once", AsyncMock(return_value=True)):
        await repair.apply_write(repository, session)
    assert session.results == [
        {"cancel_public_id": pair.cancel_id, "result": "recorded_unconfirmed", "sequence_id": 1}
    ]


@pytest.mark.asyncio
async def test_a_change_before_the_compare_and_set_skips_the_line(
    repository: SQLAlchemyRepository,
) -> None:
    """The recheck before the compare-and-set skips a Walutomat line whose evidence changed.

    Given: A planned fail_cancel line, then submit evidence for its target,
    When: The stale plan is applied,
    Then: The line reports changed_before_write and the cancel stays dispatched.
    """
    pair = await seed_walutomat_pair(repository)
    plan = await _plan(repository, pair.cancel_id)
    assert plan.lines[0].decision.action is RepairAction.FAIL_CANCEL
    await add_rows(
        repository,
        [venue_event(pair.client_order_id, event_type="order_accepted", status="open")],
    )
    session = _session(plan)
    await repair.apply_write(repository, session)
    assert session.results == [
        {"cancel_public_id": pair.cancel_id, "result": "changed_before_write"}
    ]
    assert await _status(repository, pair.cancel_id) == ("dispatched", None)


@pytest.mark.asyncio
async def test_an_interrupted_write_keeps_the_results_of_the_applied_lines(
    repository: SQLAlchemyRepository,
) -> None:
    """A write that raises part-way keeps the results of the lines before the failure.

    Given: A plan whose first line fails a Walutomat cancel and whose second line's
        insert raises,
    When: The plan is applied,
    Then: The error propagates, the first line's result is kept, and the second line
        stays in_flight with the sequence id it was given.
    """
    walutomat = await seed_walutomat_pair(repository)
    kraken = await seed_kraken_pair(repository)
    plan = await _plan(repository, walutomat.cancel_id, kraken.cancel_id)
    assert [line.decision.action for line in plan.lines] == [
        RepairAction.FAIL_CANCEL,
        RepairAction.RECORD_TERMINAL,
    ]
    session = _session(plan)
    failing_insert = AsyncMock(side_effect=RuntimeError("insert failed"))
    with (
        patch.object(repository, "insert_order_terminal_once", failing_insert),
        pytest.raises(RuntimeError, match="insert failed"),
    ):
        await repair.apply_write(repository, session)
    assert session.results == [
        {"cancel_public_id": walutomat.cancel_id, "result": "failed", "sequence_id": 1},
        {"cancel_public_id": kraken.cancel_id, "result": "in_flight", "sequence_id": 2},
    ]
    assert session.sequence == 2
    receipt = repair.receipt_document(session)
    assert receipt["results"] == session.results and receipt["digest"] == plan.digest


@pytest.mark.asyncio
async def test_a_failure_after_the_insert_keeps_the_written_line_in_flight(
    repository: SQLAlchemyRepository,
) -> None:
    """A line whose insert committed before the run failed stays in the results.

    Given: A planned line whose insert commits and whose read-back then raises,
    When: The plan is applied,
    Then: The error propagates, the line stays in_flight with its sequence id, and the
        row it wrote carries the session id.
    """
    pair = await seed_kraken_pair(repository)
    plan = await _plan(repository, pair.cancel_id)
    session = _session(plan)
    failing_read = AsyncMock(side_effect=ConnectionError("connection dropped"))
    with (
        patch.object(repair.reads, "get_terminal_written_by_session", failing_read),
        pytest.raises(ConnectionError, match="connection dropped"),
    ):
        await repair.apply_write(repository, session)
    assert session.results == [
        {"cancel_public_id": pair.cancel_id, "result": "in_flight", "sequence_id": 1}
    ]
    [terminal] = await _terminals(repository, pair.client_order_id)
    assert (terminal.session_id, terminal.sequence_id) == (session.session_id, 1)


@pytest.mark.asyncio
async def test_prepare_write_opens_a_fresh_session_per_run(
    repository: SQLAlchemyRepository, tmp_path: Path
) -> None:
    """Each prepared write gets its own session id and starts with no results.

    Given: One voted plan,
    When: The write is prepared twice,
    Then: The sessions differ in id, carry the write time and hold no results.
    """
    pair = await seed_kraken_pair(repository)
    plan = await _plan(repository, pair.cancel_id)
    voted = _vote(tmp_path, plan)
    sessions = [
        await repair.prepare_write(
            repository, [pair.cancel_id], (voted, plan.digest), (DATABASE, CODE), WRITE_TIME
        )
        for _ in range(2)
    ]
    assert sessions[0].session_id != sessions[1].session_id
    assert all(item.write_time == WRITE_TIME and item.results == [] for item in sessions)
    assert all(item.plan.digest == plan.digest for item in sessions)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("read", "message"),
    [
        ("get_active_trade_command", "several active versions"),
        ("get_active_checkpoint", "several active checkpoints"),
    ],
)
async def test_several_active_rows_refuse_the_run(
    repository: SQLAlchemyRepository, read: str, message: str
) -> None:
    """A cancel or checkpoint with more than one active row refuses the whole run.

    Given: A read that finds more than one active row,
    When: The repair is planned,
    Then: It refuses naming the duplicated row kind.
    """
    pair = await seed_kraken_pair(repository)
    with (
        patch.object(repair.reads, read, AsyncMock(side_effect=RuntimeError("several"))),
        pytest.raises(CancelTargetRepairRefusedError, match=message),
    ):
        await _plan(repository, pair.cancel_id)


@pytest.mark.asyncio
async def test_already_done_lines_write_nothing(repository: SQLAlchemyRepository) -> None:
    """Lines already recorded or failed apply as nothing to write.

    Given: A pair whose terminal already exists,
    When: Its plan is applied,
    Then: The line reports nothing_to_write.
    """
    pair = await seed_kraken_pair(repository)
    await add_rows(
        repository,
        [venue_event(pair.client_order_id, event_type="order_terminal", status="canceled")],
    )
    plan = await _plan(repository, pair.cancel_id)
    session = _session(plan)
    await repair.apply_write(repository, session)
    assert session.results == [{"cancel_public_id": pair.cancel_id, "result": "nothing_to_write"}]
    assert session.sequence == 0


def test_refuse_duplicates_refuses_empty_and_repeated_lists() -> None:
    """The cancel list must be non-empty and free of repeats.

    Given: An empty list and a repeated id,
    When: They are checked,
    Then: Both refuse.
    """
    with pytest.raises(CancelTargetRepairRefusedError, match="no --cancel"):
        repair.refuse_duplicates([])
    with pytest.raises(CancelTargetRepairRefusedError, match="twice"):
        repair.refuse_duplicates(["a", "a"])


def test_voted_digest_refuses_an_unreadable_vote(tmp_path: Path) -> None:
    """A voted directory without its files cannot be matched.

    Given: An empty directory,
    When: Its digest is read,
    Then: The run refuses with the error class only.
    """
    with pytest.raises(CancelTargetRepairRefusedError, match="FileNotFoundError"):
        repair.voted_digest(tmp_path)


def test_code_revision_hashes_every_deciding_module() -> None:
    """The code revision hashes the repair, its reads and every module it relies on.

    Given: No extra module,
    When: The code revision is taken,
    Then: It names the repair, its reads, the repository and the reconciler.
    """
    revision = repair.code_revision([])
    for name in (
        "snapper/application/trade/cancel_target_repair.py",
        "snapper/data/cancel_target_repair_reads.py",
        "snapper/data/repository.py",
        "snapper/application/trade/reconciler.py",
    ):
        assert isinstance(revision[name], str) and len(revision[name]) == 64


def test_checkpoint_ids_without_a_checkpoint_are_empty() -> None:
    """A shard without a checkpoint holds no open command id.

    Given: No checkpoint,
    When: Its open command ids are read,
    Then: The list is empty.
    """
    assert repair.checkpoint_open_command_ids(None) == []


@pytest.mark.asyncio
async def test_three_pairs_on_one_shard_are_repaired_together(
    repository: SQLAlchemyRepository, tmp_path: Path
) -> None:
    """Production's three Kraken pairs on one shard pass together, are written and retire.

    Given: Three Kraken pairs on one shard, each create accepted and each cancel dispatched,
    When: All three cancels are planned, voted and written, and one reconciliation cycle runs,
    Then: Every line records its terminal, the shard facts name the run's targets, and all
        six commands end cancelled.
    """
    pairs = await _seed_three_pairs(repository)
    cancel_ids = [pair.cancel_id for pair in pairs]
    real_read = repair.read_shard
    with patch.object(repair, "read_shard", AsyncMock(side_effect=real_read)) as shard_reads:
        plan = await _plan(repository, *cancel_ids)
    assert shard_reads.await_count == 1
    assert [line.decision.action for line in plan.lines] == [RepairAction.RECORD_TERMINAL] * 3
    targets = sorted(pair.create_id for pair in pairs)
    assert all(
        line.preview["shard"]
        == {
            "non_target_open_commands": [],
            "non_target_checkpoint_open_ids": [],
            "run_targets_on_shard": targets,
            "wallets": [WALLET],
        }
        for line in plan.lines
    )
    assert plan.summary["shards"] == {
        KRAKEN_SHARD: {
            "open_commands": [
                {
                    "public_id": pair.create_id,
                    "command_type": "create",
                    "status": "accepted",
                    "client_order_id": pair.client_order_id,
                }
                for pair in pairs
            ],
            "checkpoint": None,
            "wallets": [WALLET],
            "run_targets": targets,
        }
    }
    voted = _vote(tmp_path, plan)
    session = await repair.prepare_write(
        repository, cancel_ids, (voted, plan.digest), (DATABASE, CODE), WRITE_TIME
    )
    await repair.apply_write(repository, session)
    assert _line_results(session) == ["recorded"] * 3
    for pair in pairs:
        assert len(await _terminals(repository, pair.client_order_id)) == 1

    loop = ReconciliationLoop(
        exchange_name="kraken",
        repository=repository,
        trade_service=MagicMock(spec=TradeService),
        interval_seconds=60.0,
    )
    await loop._reconcile_cycle()
    for pair in pairs:
        assert await _status(repository, pair.create_id) == ("cancelled", None)
        assert await _status(repository, pair.cancel_id) == (
            "cancelled",
            "target order ended canceled",
        )


@pytest.mark.asyncio
async def test_a_run_naming_only_some_pairs_on_a_shard_refuses_each(
    repository: SQLAlchemyRepository,
) -> None:
    """Leaving one pair of a shard out of the run refuses the pairs that are named.

    Given: Three Kraken pairs on one shard,
    When: Only two of their cancels are planned,
    Then: Both lines refuse as shard_has_other_open_command, naming the left-out create.
    """
    pairs = await _seed_three_pairs(repository)
    plan = await _plan(repository, pairs[0].cancel_id, pairs[1].cancel_id)
    assert [line.decision.reason for line in plan.lines] == ["shard_has_other_open_command"] * 2
    assert all(
        line.preview["shard"]
        == {
            "non_target_open_commands": [pairs[2].create_id],
            "non_target_checkpoint_open_ids": [],
            "run_targets_on_shard": sorted([pairs[0].create_id, pairs[1].create_id]),
            "wallets": [WALLET],
        }
        for line in plan.lines
    )


@pytest.mark.asyncio
async def test_one_non_target_open_command_refuses_every_line_on_its_shard(
    repository: SQLAlchemyRepository,
) -> None:
    """An open command that no named cancel targets refuses the whole shard.

    Given: Three Kraken pairs on one shard and one more open create on it,
    When: All three cancels are planned,
    Then: Every line refuses as shard_has_other_open_command.
    """
    pairs = await _seed_three_pairs(repository)
    await add_rows(
        repository,
        [
            command(
                "00000000-0000-7000-8000-0000000000f3",
                command_type="create",
                status="accepted",
                client_order_id="cid-live",
            )
        ],
    )
    plan = await _plan(repository, *[pair.cancel_id for pair in pairs])
    assert [line.decision.reason for line in plan.lines] == ["shard_has_other_open_command"] * 3


@pytest.mark.asyncio
async def test_a_checkpoint_holding_run_targets_passes_and_another_id_refuses(
    repository: SQLAlchemyRepository,
) -> None:
    """A checkpoint's open ids are covered when they are run targets, and refuse otherwise.

    Given: Three Kraken pairs on one shard and a checkpoint naming two of their creates,
    When: All three cancels are planned, then again with a checkpoint naming another id,
    Then: The first plan records all three, and the second refuses all three.
    """
    pairs = await _seed_three_pairs(repository)
    cancel_ids = [pair.cancel_id for pair in pairs]
    covered = checkpoint(open_command_ids=json.dumps([pairs[1].create_id, pairs[2].create_id]))
    await add_rows(repository, [covered])
    plan = await _plan(repository, *cancel_ids)
    assert [line.decision.action for line in plan.lines] == [RepairAction.RECORD_TERMINAL] * 3
    async with repository.session() as session:
        await session.execute(
            sa.update(TradeProjectionCheckpoint).values(
                open_command_ids=json.dumps([pairs[1].create_id, "other-command"])
            )
        )
        await session.commit()
    refused = await _plan(repository, *cancel_ids)
    assert [line.decision.reason for line in refused.lines] == ["shard_checkpoint_open_command"] * 3
    shard = refused.lines[0].preview["shard"]
    assert isinstance(shard, dict) and shard["non_target_checkpoint_open_ids"] == ["other-command"]


@pytest.mark.asyncio
async def test_a_target_retired_between_inserts_still_lets_the_later_lines_write(
    repository: SQLAlchemyRepository,
) -> None:
    """A reconciler cycle that retires an already written target changes no later line.

    Given: Three Kraken pairs on one shard, planned together,
    When: The first pair's create is retired to cancelled right after its terminal is inserted,
    Then: The other two lines still pass their recheck and are recorded.
    """
    pairs = await _seed_three_pairs(repository)
    plan = await _plan(repository, *[pair.cancel_id for pair in pairs])
    real_insert = repository.insert_order_terminal_once

    async def insert_then_retire(row: VenueEventInsertRow) -> bool:
        """Insert the terminal, then retire the first create as the reconciler would."""
        inserted: bool = await real_insert(row)
        if row["client_order_id"] == pairs[0].client_order_id:
            assert await repository.advance_trade_command_lifecycle(
                pairs[0].create_id,
                "accepted",
                TradeCommandStatusEnum.CANCELLED,
                WRITE_TIME,
                SESSION,
                9,
                terminal_at=CANCELED_AT,
            )
        return inserted

    session = _session(plan)
    with patch.object(repository, "insert_order_terminal_once", insert_then_retire):
        await repair.apply_write(repository, session)
    assert _line_results(session) == ["recorded"] * 3
    assert await _status(repository, pairs[0].create_id) == ("cancelled", None)


@pytest.mark.asyncio
async def test_a_partly_applied_shard_is_retried_with_the_written_cancel_still_named(
    repository: SQLAlchemyRepository,
) -> None:
    """A retry that names the written cancel again keeps its still open target covered.

    Given: Three pairs on one shard whose first terminal is already recorded, its create
        still accepted and named in the checkpoint,
    When: A retry names only the two remaining cancels, then all three,
    Then: The first retry refuses both lines, and the second writes only the remaining two.
    """
    pairs = await _seed_three_pairs(repository)
    cancel_ids = [pair.cancel_id for pair in pairs]
    await _record_first_terminal(repository, await _plan(repository, *cancel_ids))
    await add_rows(repository, [checkpoint(open_command_ids=json.dumps([pairs[0].create_id]))])
    partial = await _plan(repository, *cancel_ids[1:])
    assert [line.decision.reason for line in partial.lines] == ["shard_has_other_open_command"] * 2
    retry = await _plan(repository, *cancel_ids)
    assert [line.decision.action for line in retry.lines] == [
        RepairAction.ALREADY_RECORDED,
        RepairAction.RECORD_TERMINAL,
        RepairAction.RECORD_TERMINAL,
    ]
    session = _session(retry)
    await repair.apply_write(repository, session)
    assert _line_results(session) == ["nothing_to_write", "recorded", "recorded"]


@pytest.mark.asyncio
async def test_a_retry_after_the_reconciler_retired_the_written_target_still_covers_it(
    repository: SQLAlchemyRepository,
) -> None:
    """A retired target that the checkpoint still names is covered when its cancel is named.

    Given: Three pairs on one shard whose first terminal is recorded and whose first create
        the reconciler already retired, while the checkpoint still names it,
    When: A retry names only the two remaining cancels, then all three,
    Then: The first retry refuses on the checkpoint, and the second records the two.
    """
    pairs = await _seed_three_pairs(repository)
    cancel_ids = [pair.cancel_id for pair in pairs]
    await _record_first_terminal(repository, await _plan(repository, *cancel_ids))
    await _retire(repository, pairs[0].create_id)
    await add_rows(repository, [checkpoint(open_command_ids=json.dumps([pairs[0].create_id]))])
    partial = await _plan(repository, *cancel_ids[1:])
    assert [line.decision.reason for line in partial.lines] == ["shard_checkpoint_open_command"] * 2
    retry = await _plan(repository, *cancel_ids)
    assert [line.decision.action for line in retry.lines] == [
        RepairAction.ALREADY_RECORDED,
        RepairAction.RECORD_TERMINAL,
        RepairAction.RECORD_TERMINAL,
    ]
    assert pairs[0].create_id in retry.run_targets.shard_keys
    session = _session(retry)
    await repair.apply_write(repository, session)
    assert _line_results(session) == ["nothing_to_write", "recorded", "recorded"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("variant", "first_reason"),
    [
        ("not_a_cancel", "not_a_cancel"),
        ("ambiguous", "target_ambiguous"),
        ("created", "target_not_foldable"),
    ],
)
async def test_targets_that_cannot_retire_cover_nothing(
    repository: SQLAlchemyRepository, variant: str, first_reason: str
) -> None:
    """A line that fails stage A or B, or whose target is open but not foldable, covers nothing.

    Given: Three pairs on one shard, the first line changed as stated,
    When: The three are planned together,
    Then: The first line refuses as stated, its create is no run target, and the other two
        refuse because that create is still open on their shard.
    """
    pairs = await _seed_three_pairs(repository)
    first = pairs[0].create_id if variant == "not_a_cancel" else pairs[0].cancel_id
    if variant == "created":
        await _set_command(repository, pairs[0].create_id, status="created")
    real_lookup = repository.get_active_create_command_by_client_order_id

    async def lookup(client_order_id: str, exchange: str) -> TradeCommandRow | None:
        """Make the first pair's target ambiguous in the ambiguous variant."""
        if variant == "ambiguous" and client_order_id == pairs[0].client_order_id:
            raise RuntimeError("several active creates")
        return await real_lookup(client_order_id, exchange)

    with patch.object(repository, "get_active_create_command_by_client_order_id", lookup):
        plan = await _plan(repository, first, pairs[1].cancel_id, pairs[2].cancel_id)
    assert [line.decision.reason for line in plan.lines] == [
        first_reason,
        "shard_has_other_open_command",
        "shard_has_other_open_command",
    ]
    assert sorted(plan.run_targets.shard_keys) == sorted([pairs[1].create_id, pairs[2].create_id])
    assert list(cast(dict[str, JsonValue], plan.summary["shards"])) == [KRAKEN_SHARD]


@pytest.mark.asyncio
async def test_a_checkpoint_naming_a_run_target_of_another_shard_refuses(
    repository: SQLAlchemyRepository,
) -> None:
    """A run target covers only its own shard.

    Given: One pair on the Kraken shard, a second pair on another wallet's shard, and a
        checkpoint on the first shard that names the second pair's create,
    When: Both cancels are planned together,
    Then: The first line refuses on the checkpoint, the second records, and the summary
        lists both shards.
    """
    first = await seed_kraken_pair(repository, 1)
    second = await seed_kraken_pair(repository, 4)
    other_shard = await _move_to_wallet(repository, second, OTHER_WALLET)
    await add_rows(repository, [checkpoint(open_command_ids=json.dumps([second.create_id]))])
    plan = await _plan(repository, first.cancel_id, second.cancel_id)
    assert [line.decision.reason for line in plan.lines] == ["shard_checkpoint_open_command", None]
    assert plan.lines[1].decision.action is RepairAction.RECORD_TERMINAL
    assert plan.run_targets.covers(second.create_id, other_shard)
    assert not plan.run_targets.covers(second.create_id, KRAKEN_SHARD)
    assert list(cast(dict[str, JsonValue], plan.summary["shards"])) == sorted(
        [KRAKEN_SHARD, other_shard]
    )


@pytest.mark.asyncio
async def test_two_cancels_targeting_one_order_refuse_the_run(
    repository: SQLAlchemyRepository,
) -> None:
    """Two named cancels whose targets share a client order id refuse the whole run.

    Given: A pair and a second cancel of the same client order id,
    When: Both cancels are planned,
    Then: The run refuses before any line is evaluated.
    """
    pair = await seed_kraken_pair(repository)
    second_cancel = "00000000-0000-7000-8000-0000000001cb"
    await add_rows(
        repository,
        [
            command(
                second_cancel,
                command_type="cancel",
                status="dispatched",
                client_order_id=pair.client_order_id,
                created_at=CANCEL_AT,
            )
        ],
    )
    with pytest.raises(CancelTargetRepairRefusedError, match="the same order"):
        await _plan(repository, pair.cancel_id, second_cancel)


@pytest.mark.asyncio
async def test_a_watermark_change_after_the_vote_refuses_the_write(
    repository: SQLAlchemyRepository, tmp_path: Path
) -> None:
    """The summary binds each shard's checkpoint watermark to the vote.

    Given: Three pairs on one shard with a checkpoint, planned and voted,
    When: The checkpoint's watermark moves before the write is prepared,
    Then: The recomputed summary differs and the write refuses.
    """
    pairs = await _seed_three_pairs(repository)
    cancel_ids = [pair.cancel_id for pair in pairs]
    await add_rows(repository, [checkpoint()])
    plan = await _plan(repository, *cancel_ids)
    voted = _vote(tmp_path, plan)
    async with repository.session() as session:
        await session.execute(sa.update(TradeProjectionCheckpoint).values(last_venue_event_id=3))
        await session.commit()
    with pytest.raises(CancelTargetRepairRefusedError, match="differs from the vote"):
        await repair.prepare_write(
            repository, cancel_ids, (voted, plan.digest), (DATABASE, CODE), WRITE_TIME
        )


@pytest.mark.asyncio
async def test_a_plan_without_any_stage_b_target_lists_no_shard(
    repository: SQLAlchemyRepository,
) -> None:
    """Lines that stop at stage A or B read no shard and leave the summary's shards empty.

    Given: A create named as a cancel, and an id that names no command,
    When: Both are planned,
    Then: They refuse as not_a_cancel and cancel_missing, no shard is read, and the
        summary's shards and the run's targets are empty.
    """
    pair = await seed_kraken_pair(repository)
    missing = "00000000-0000-7000-8000-0000000000fe"
    with patch.object(repair, "read_shard", AsyncMock()) as shard_reads:
        plan = await _plan(repository, pair.create_id, missing)
    assert [line.decision.reason for line in plan.lines] == ["not_a_cancel", "cancel_missing"]
    shard_reads.assert_not_awaited()
    assert plan.summary["shards"] == {}
    assert plan.run_targets.shard_keys == {}
