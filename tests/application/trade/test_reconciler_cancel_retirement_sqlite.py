"""Item G2 cancel retirement through the real fold on a SQLite repository.

Every case starts from stored commands of one order, an active create command
and an active cancel sharing its client order id, stores the venue evidence,
and runs one real reconciliation cycle. The lifecycle fold advances the create
command before the retirement pass reads it. A mocked, already-terminal target
cannot show that ordering: a retirement pass that ran before the fold would
see the create still accepted and leave the cancel alone.
"""

from collections.abc import AsyncIterator
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from pathlib import Path
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest

from snapper.application.trade.reconciler import ReconciliationLoop
from snapper.application.trade.trade_service import TradeService
from snapper.data.repository import SQLAlchemyRepository
from snapper.data.repository_types import TradeCommandInsertRow
from snapper.data.repository_types import TradeCommandRow

_EXCHANGE = "kraken"
_SHARD = "kraken.BTC-USD.live"
_CID = "cid-target"
_WALLET = "00000000-0000-7000-8000-000000000001"
_SESSION = "00000000-0000-7000-8000-0000000000a1"
_ACCEPTED_OFFSET = -595
"""Seconds from the cancel's creation to the create's venue acceptance."""


@dataclass(frozen=True)
class _Observation:
    """One stored venue observation of the target order.

    Attributes:
        event_type: Venue event type.
        offset_seconds: Observation time, in seconds after the cancel's creation.
        status: Event status as the executor writes it.
        cum_fill_size: Cumulative fill of a fill observation.
    """

    event_type: str
    offset_seconds: int
    status: str | None = None
    cum_fill_size: float | None = None


_ACCEPTED = _Observation("order_accepted", _ACCEPTED_OFFSET)


@dataclass(frozen=True)
class _Timeline:
    """Creation times of one case, anchored on the wall clock the cycle reads.

    Attributes:
        create_created: Creation time of the create command.
        cancel_created: Creation time of the cancel command.
    """

    create_created: datetime
    cancel_created: datetime

    def after_cancel(self, seconds: int) -> datetime:
        """Return the time ``seconds`` after the cancel's creation.

        Args:
            seconds: Offset from the cancel's creation, negative for earlier.

        Returns:
            The absolute time.
        """
        return self.cancel_created + timedelta(seconds=seconds)


def _timeline() -> _Timeline:
    """Anchor a case in the recent past, so both commands are over age.

    Whole seconds keep every stored time distinct from the cycle's own time,
    which the reconciliation loop reads from the wall clock.

    Returns:
        The case's creation times.
    """
    now = datetime.now(UTC).replace(microsecond=0)
    return _Timeline(
        create_created=now - timedelta(minutes=20),
        cancel_created=now - timedelta(minutes=10),
    )


@pytest.fixture
async def repository(tmp_path: Path) -> AsyncIterator[SQLAlchemyRepository]:
    """Provide an empty SQLite repository owned by one test."""
    created = SQLAlchemyRepository(f"sqlite+aiosqlite:///{tmp_path / 'cancel_retirement.db'}")
    await created.create_all()
    try:
        yield created
    finally:
        await created.engine.dispose()


async def _insert_command(
    repository: SQLAlchemyRepository,
    command_type: str,
    status: str,
    created_at: datetime,
) -> str:
    """Store one command of the target order and return its public id.

    Args:
        repository: Repository to write.
        command_type: ``create`` or ``cancel``.
        status: Stored command status.
        created_at: Creation and version time of the command.

    Returns:
        The command's public id.
    """
    row: TradeCommandInsertRow = {
        "command_type": command_type,
        "shard_key": _SHARD,
        "exchange": _EXCHANGE,
        "instrument": "BTC-USD",
        "mode": "live",
        "strategy_id": "engine-buy",
        "client_order_id": _CID,
        "venue_client_id": _CID,
        "side": "buy",
        "order_type": "limit",
        "quantity": 1.0,
        "price": 50000.0,
        "status": status,
        "created_at": created_at,
        "correlation_id": f"corr-{command_type}",
        "session_id": _SESSION,
        "sequence_id": 1 if command_type == "create" else 2,
        "timestamp": created_at,
        "wallet_public_id": _WALLET,
    }
    _, public_id = await repository.insert_trade_command(row)
    return public_id


async def _store(
    repository: SQLAlchemyRepository,
    times: _Timeline,
    observations: Sequence[_Observation],
) -> None:
    """Store the venue observations of the target order in the given order.

    Args:
        repository: Repository to write.
        times: The case's creation times.
        observations: Observations to store, oldest id first.
    """
    for sequence_id, observation in enumerate(observations, start=1):
        received_at = times.after_cancel(observation.offset_seconds)
        await repository.insert_venue_event(
            {
                "event_type": observation.event_type,
                "shard_key": _SHARD,
                "exchange": _EXCHANGE,
                "instrument": "BTC-USD",
                "mode": "live",
                "received_at": received_at,
                "session_id": _SESSION,
                "sequence_id": sequence_id,
                "timestamp": received_at,
                "client_order_id": _CID,
                "venue_client_id": _CID,
                "exchange_order_id": "ex-target",
                "status": observation.status,
                "cum_fill_size": observation.cum_fill_size,
                "wallet_public_id": _WALLET,
                "liquidity_role": "unknown",
            }
        )


def _loop(repository: SQLAlchemyRepository) -> ReconciliationLoop:
    """Build the reconciliation loop of the target's exchange.

    Args:
        repository: Repository holding the case's state.

    Returns:
        The loop.
    """
    return ReconciliationLoop(
        exchange_name=_EXCHANGE,
        repository=repository,
        trade_service=MagicMock(spec=TradeService),
        interval_seconds=60.0,
    )


async def _command(repository: SQLAlchemyRepository, public_id: str) -> TradeCommandRow:
    """Read the current version of one command.

    Args:
        repository: Repository to read.
        public_id: The command's public id.

    Returns:
        The command's current row.
    """
    row = await repository.get_trade_command_by_public_id(public_id, as_of=datetime.now(UTC))
    assert row is not None
    return row


def _reported_ids(report_stale: MagicMock) -> list[str]:
    """Return the public ids the stale report was asked about, in order.

    Args:
        report_stale: Spy wrapping the loop's stale report.

    Returns:
        The reported command public ids.
    """
    return [call.args[0]["public_id"] for call in report_stale.call_args_list]


@dataclass(frozen=True)
class _TableRow:
    """One row of the G2 table, as stored evidence and expected outcome.

    Attributes:
        observations: Venue evidence of the target after its acceptance.
        create_status: Status the fold gives the create command.
        cancel_status: Status the retirement gives the cancel.
        last_error: The cancel's ``last_error`` after retirement.
        ended_offset: Seconds after the cancel's creation of the newest
            qualifying observation, the cancel's expected ``terminal_at``.
    """

    observations: tuple[_Observation, ...]
    create_status: str
    cancel_status: str
    last_error: str
    ended_offset: int


_TABLE_ROWS = [
    pytest.param(
        _TableRow(
            (_Observation("order_terminal", 30, "canceled"),),
            "cancelled",
            "cancelled",
            "target order ended canceled",
            30,
        ),
        id="cancelled-after-the-cancel",
    ),
    pytest.param(
        _TableRow(
            (_Observation("order_terminal", -30, "canceled"),),
            "cancelled",
            "rejected",
            "target order was already canceled",
            -30,
        ),
        id="cancelled-before-the-cancel",
    ),
    pytest.param(
        _TableRow(
            (_Observation("order_terminal", 30, "filled"),),
            "filled",
            "rejected",
            "target order ended filled",
            30,
        ),
        id="filled-by-a-terminal-only",
    ),
    pytest.param(
        _TableRow(
            (
                _Observation("fill_observed", 20, "filled", 1.0),
                _Observation("fill_observed", 40, "partially_filled", 0.4),
            ),
            "filled",
            "rejected",
            "target order ended filled",
            40,
        ),
        id="filled-by-fills-then-a-later-partial-observation",
    ),
    pytest.param(
        _TableRow(
            (_Observation("order_terminal", 30, "expired"),),
            "expired",
            "rejected",
            "target order expired",
            30,
        ),
        id="expired",
    ),
]


@pytest.mark.asyncio
@pytest.mark.parametrize("row", _TABLE_ROWS)
async def test_one_cycle_folds_the_create_then_retires_the_cancel(
    repository: SQLAlchemyRepository, row: _TableRow
) -> None:
    """Each row of the G2 table, from an accepted create, in one real cycle.

    Given: an accepted create command and a dispatched cancel of one order,
        and the row's venue evidence after the acceptance (for the end by
        fills only, a completing fill followed by a later partial
        observation),
    When: one reconciliation cycle runs on the stored state,
    Then: the fold advances the create to the row's terminal status first,
        the retirement pass then moves the cancel to the row's status and
        error with the newest qualifying observation as terminal_at, which
        is not the cycle time, and the cancel is left out of the stale
        report.
    """
    times = _timeline()
    create_id = await _insert_command(repository, "create", "accepted", times.create_created)
    cancel_id = await _insert_command(repository, "cancel", "dispatched", times.cancel_created)
    await _store(repository, times, [_ACCEPTED, *row.observations])
    loop = _loop(repository)

    with (
        patch.object(
            repository,
            "advance_trade_command_lifecycle",
            wraps=repository.advance_trade_command_lifecycle,
        ) as cas,
        patch.object(loop, "_report_stale", wraps=loop._report_stale) as report_stale,
    ):
        await loop._reconcile_cycle()

    assert [call.kwargs["public_id"] for call in cas.await_args_list] == [create_id, cancel_id]
    assert (await _command(repository, create_id))["status"] == row.create_status
    cancel = await _command(repository, cancel_id)
    assert (cancel["status"], cancel["last_error"]) == (row.cancel_status, row.last_error)
    assert cancel["terminal_at"] == times.after_cancel(row.ended_offset)
    assert cancel_id not in _reported_ids(report_stale)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("create_status", "observations"),
    [
        pytest.param("cancelled", [], id="cancelled-by-the-guard-scanner"),
        pytest.param("expired", [], id="expired-by-the-outbox"),
        pytest.param("accepted", [_ACCEPTED], id="still-open"),
        pytest.param("rejected", [_Observation("order_rejected", -590)], id="rejected"),
        pytest.param("failed", [_Observation("order_breaker_open", -590)], id="failed"),
        pytest.param(None, [], id="missing"),
    ],
)
async def test_cancel_without_a_venue_evidenced_end_stays_and_is_reported(
    repository: SQLAlchemyRepository,
    create_status: str | None,
    observations: list[_Observation],
) -> None:
    """A cancel keeps its stale warning unless its target ended at the venue.

    Given: a dispatched cancel whose create command was cancelled by the
        guard scanner or expired by the outbox without venue evidence, is
        still open, rejected or failed, or is missing,
    When: one reconciliation cycle runs on the stored state,
    Then: the cancel stays dispatched, the create keeps its status, and the
        cancel is still reported stale.
    """
    times = _timeline()
    create_id = (
        None
        if create_status is None
        else await _insert_command(repository, "create", create_status, times.create_created)
    )
    cancel_id = await _insert_command(repository, "cancel", "dispatched", times.cancel_created)
    await _store(repository, times, observations)
    loop = _loop(repository)

    with patch.object(loop, "_report_stale", wraps=loop._report_stale) as report_stale:
        await loop._reconcile_cycle()

    assert (await _command(repository, cancel_id))["status"] == "dispatched"
    if create_id is not None:
        assert (await _command(repository, create_id))["status"] == create_status
    assert cancel_id in _reported_ids(report_stale)


@pytest.mark.asyncio
async def test_cycle_with_only_the_cancel_active_retires_it(
    repository: SQLAlchemyRepository,
) -> None:
    """Retire a cancel whose target an earlier cycle already folded.

    Given: a create command already cancelled, its venue terminal observed
        after the cancel's creation, and an accepted cancel as the only
        active command,
    When: one reconciliation cycle runs on the stored state,
    Then: the cancel is retired as cancelled at the terminal's observation
        time.
    """
    times = _timeline()
    await _insert_command(repository, "create", "cancelled", times.create_created)
    cancel_id = await _insert_command(repository, "cancel", "accepted", times.cancel_created)
    await _store(repository, times, [_ACCEPTED, _Observation("order_terminal", 30, "canceled")])

    await _loop(repository)._reconcile_cycle()

    cancel = await _command(repository, cancel_id)
    assert (cancel["status"], cancel["last_error"]) == ("cancelled", "target order ended canceled")
    assert cancel["terminal_at"] == times.after_cancel(30)
