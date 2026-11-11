"""Bounded startup/outage reconciliation preserves data and live-event boundaries."""

import asyncio
from collections.abc import Iterator
from dataclasses import replace
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import cast
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest
from sqlalchemy import select
from sqlalchemy.exc import DBAPIError

from snapper.config.app import AppSettings
from snapper.core.types import ExchangeEnum
from snapper.data.models import KNOWN_TO_MAX
from snapper.data.models import Candle
from snapper.data.repository import Repository
from snapper.data.repository import SQLAlchemyRepository
from snapper.data.repository_types import CandleRow
from snapper.data.repository_types import CandleUpsertRow
from snapper.infrastructure.exchanges._trade_candle_builder import TradeCandleBuilder
from snapper.infrastructure.exchanges.contracts import CandleUpdate
from snapper.infrastructure.exchanges.contracts import TradeUpdate
from snapper.infrastructure.exchanges.implementations.kraken import KrakenExchangeClient
from snapper.messaging.infrastructure.publisher import SequenceTracker
from snapper.messaging.publishers.base import MarketDataPublisherService
from snapper.messaging.publishers.base import _CandleRepairKey
from snapper.messaging.publishers.base import _PendingCandleRepair
from snapper.messaging.publishers.candle_aggregator import CandleAggregator
from snapper.messaging.publishers.kraken import KrakenMarketDataPublisher
from snapper.messaging.publishers.kraken_candle_catchup import CatchupCallbacks
from snapper.messaging.publishers.kraken_candle_catchup import CatchupDeferredError
from snapper.messaging.publishers.kraken_candle_catchup import CatchupPolicy
from snapper.messaging.publishers.kraken_candle_catchup import KrakenCandleCatchup
from snapper.messaging.publishers.kraken_candle_catchup import _failure_label
from snapper.messaging.publishers.kraken_candle_catchup import complete_repair_windows
from snapper.messaging.publishers.kraken_candle_catchup import repair_read_start
from snapper.messaging.publishers.native_candle_finalizer import NativeCandleFinalizer
from snapper.messaging.publishers.native_candle_finalizer import is_complete_historical_minute

NOW = datetime(2026, 10, 28, 21, 35, 30, tzinfo=UTC)
START = NOW.replace(minute=25, second=0)


def _candle(symbol: str = "BTC-USD", minute: int = 25) -> CandleUpdate:
    """Build a full-field native candle inside the frozen historical window."""
    return CandleUpdate(
        symbol, 10.0, 12.0, 9.0, 11.0, 10.5, 7, 3.25, START.replace(minute=minute), 60
    )


def _row(minute: int) -> CandleRow:
    """Build one committed source minute with every native business field."""
    return {
        "open_at": START.replace(minute=minute),
        "timeframe": "1m",
        "open": 10.0,
        "high": 12.0,
        "low": 9.0,
        "close": 11.0,
        "volume": 3.25,
        "vwap": 10.5,
        "trades": 7,
        "source": "native",
        "complete": True,
        "public_id": f"minute-{minute}",
        "timestamp": NOW,
        "session_id": "fixture",
        "sequence_id": minute,
    }


class _Fixture:
    """Controllable external boundaries around the actual bounded coordinator."""

    def __init__(self) -> None:
        """Keep durable rows separate from committed repair notifications."""
        self.running = True
        self.symbols = ["BTC-USD"]
        self.resolve = AsyncMock(return_value="instrument")
        self.client = MagicMock(spec=KrakenExchangeClient)
        self.client.get_closed_1m_candles = AsyncMock(return_value=[_candle()])
        self.repository = MagicMock(spec=Repository)
        self.repository.filter_missing_candles = AsyncMock(side_effect=lambda rows: list(rows))
        self.repository.insert_missing_candles = AsyncMock(return_value=[])
        self.repository.correct_false_zero_candles = AsyncMock(return_value=[])
        self.committed = AsyncMock()
        self.tracker = SequenceTracker()

    def worker(self, policy: CatchupPolicy = CatchupPolicy()) -> KrakenCandleCatchup:
        """Bind actual reconciliation to deterministic clocks and finite boundaries."""
        return KrakenCandleCatchup(
            self.repository,
            self.client,
            CatchupCallbacks(
                lambda: self.symbols,
                self.resolve,
                self.committed,
                lambda: self.running,
                self.tracker,
                lambda: NOW,
            ),
            policy,
        )


@pytest.mark.parametrize(
    "field,value",
    [
        ("history_minutes", 0),
        ("history_minutes", 720),
        ("priority_history_minutes", 119),
        ("priority_history_minutes", 720),
        ("priority_history_minutes", float("inf")),
        ("symbols_per_pass", -1),
        ("symbols_per_pass", 1),
        ("attempt_seconds", float("inf")),
        ("pass_seconds", float("nan")),
        ("minimum_spacing_seconds", 61.0),
        ("sweep_spacing_seconds", 0.0),
    ],
)
def test_policy_rejects_unbounded_or_unpaced_workers(field: str, value: float) -> None:
    """Every resource boundary remains finite and meaningful.

    Given: A nonpositive, infinite or incompatible policy bound.
    When: The bounded worker policy is constructed.
    Then: Configuration is refused before any acquisition.
    """
    with pytest.raises(ValueError):
        replace(CatchupPolicy(), **{field: value})


@pytest.mark.asyncio
async def test_startup_preserves_native_fields_author_and_committed_repair_order() -> None:
    """Startup preserves historical values and actual knowledge time.

    Given: One native candle learned after its business minute closed.
    When: The initial bounded reconciliation commits it.
    Then: All native fields and genuine provenance precede repair notification.
    """
    fixture = _Fixture()
    observations: list[str] = []

    async def insert(rows: list[CandleUpsertRow]) -> list[CandleUpsertRow]:
        observations.append("committed")
        return rows

    async def repair(
        symbol: str, start: datetime, end: datetime, changed: frozenset[datetime]
    ) -> None:
        assert observations == ["committed"]
        observations.append("repair")

    fixture.repository.insert_missing_candles.side_effect = insert
    fixture.committed.side_effect = repair
    await fixture.worker().run_pass()
    rows = fixture.repository.insert_missing_candles.call_args.args[0]
    row = rows[0]
    assert (row["open"], row["high"], row["low"], row["close"]) == (10.0, 12.0, 9.0, 11.0)
    assert (row["vwap"], row["volume"], row["trades"]) == (10.5, 3.25, 7)
    assert row["timestamp"] == NOW > row["open_at"]
    assert (row["source"], row["complete"]) == ("native", True)
    assert row["session_id"] == fixture.tracker.session_id
    assert row["sequence_id"] == 1
    fixture.client.get_closed_1m_candles.assert_awaited_once_with(
        "BTC-USD", NOW.replace(second=0) - timedelta(minutes=719), NOW.replace(second=0)
    )
    assert observations == ["committed", "repair"]
    fixture.committed.assert_awaited_once_with(
        "BTC-USD",
        NOW.replace(second=0) - timedelta(minutes=719),
        NOW.replace(second=0),
        frozenset({START}),
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("publisher_callback", [False, True])
@pytest.mark.parametrize("insert_count", [0, 1])
async def test_repairs_require_newly_inserted_rows(
    publisher_callback: bool, insert_count: int
) -> None:
    """Both insertion paths gate repairs on actual inserts rather than acquired rows.

    Given: Two acquired minutes with either no insert or one newly inserted row.
    When: Reconciliation uses the publisher callback or the repository fallback.
    Then: Only a nonempty insertion schedules one repair over the original window.

    Args:
        publisher_callback: Whether to use the publisher's identity-aware insertion callback.
        insert_count: Number of acquired rows the repository actually inserts.
    """
    fixture = _Fixture()
    fixture.client.get_closed_1m_candles.return_value = [_candle(), _candle(minute=26)]

    async def insert(rows: list[CandleUpsertRow]) -> list[CandleUpsertRow]:
        assert len(rows) == 2
        return rows[:insert_count]

    fixture.repository.insert_missing_candles.side_effect = insert
    worker = fixture.worker()
    if publisher_callback:
        publisher = _publisher()
        publisher.repository = fixture.repository
        worker._callbacks = replace(
            worker._callbacks, insert_missing=publisher._insert_catchup_candle_rows
        )
    await worker._reconcile_symbol("BTC-USD", START, NOW)
    fixture.repository.insert_missing_candles.assert_awaited_once()
    if insert_count:
        fixture.committed.assert_awaited_once_with(
            "BTC-USD", START, NOW, frozenset({START.replace(minute=26)})
        )
    else:
        fixture.committed.assert_not_awaited()


@pytest.mark.asyncio
async def test_empty_roster_and_stopped_worker_never_request_history() -> None:
    """No resolved persistence roster means no exchange acquisition.

    Given: An empty roster or a stopped publisher.
    When: A reconciliation pass is requested.
    Then: The exchange is never called.
    """
    fixture = _Fixture()
    fixture.symbols = []
    worker = fixture.worker()
    await worker.run_pass()
    fixture.symbols = ["BTC-USD"]
    fixture.running = False
    await worker.run_pass()
    fixture.client.get_closed_1m_candles.assert_not_awaited()


@pytest.mark.asyncio
async def test_symbol_budget_is_round_robin_and_survives_one_failed_symbol() -> None:
    """A bad symbol cannot starve the remaining persisted roster.

    Given: Four symbols, a two-symbol budget and one acquisition failure.
    When: Two bounded passes complete.
    Then: Every symbol is visited once and empty responses schedule no repairs.
    """
    fixture = _Fixture()
    fixture.symbols = ["D", "C", "B", "A", "A"]
    fixture.client.get_closed_1m_candles.side_effect = [RuntimeError("unavailable"), [], [], []]
    worker = fixture.worker(CatchupPolicy(symbols_per_pass=2))
    await worker.run_pass()
    await worker.run_pass()
    assert [call.args[0] for call in fixture.client.get_closed_1m_candles.call_args_list] == [
        "A",
        "B",
        "C",
        "D",
    ]
    fixture.repository.insert_missing_candles.assert_not_awaited()
    assert fixture.repository.filter_missing_candles.await_count == 3
    fixture.committed.assert_not_awaited()


@pytest.mark.asyncio
async def test_unresolved_identity_never_fetches_or_writes() -> None:
    """An unmapped instrument stays an explicit unresolved hole.

    Given: A persisted symbol without a canonical instrument identity.
    When: Its bounded reconciliation runs.
    Then: Neither acquisition nor insertion is attempted.
    """
    fixture = _Fixture()
    fixture.resolve.return_value = None
    await fixture.worker().run_pass()
    fixture.client.get_closed_1m_candles.assert_not_awaited()
    fixture.repository.insert_missing_candles.assert_not_awaited()


@pytest.mark.asyncio
async def test_attempt_timeout_does_not_retry_the_same_symbol_inline() -> None:
    """A stalled request is cancelled by its finite attempt budget.

    Given: A request that never produces a response.
    When: Its finite attempt deadline expires.
    Then: The symbol remains unresolved without an inline retry.
    """
    fixture = _Fixture()

    async def stall(symbol: str, start: datetime, end: datetime) -> list[CandleUpdate]:
        await asyncio.Event().wait()
        return []

    fixture.client.get_closed_1m_candles.side_effect = stall
    await fixture.worker(CatchupPolicy(attempt_seconds=0.001)).run_pass()
    assert fixture.client.get_closed_1m_candles.await_count == 1
    fixture.committed.assert_not_awaited()


@pytest.mark.asyncio
async def test_pass_deadline_propagates_without_exceeding_selected_budget() -> None:
    """The entire pass remains bounded when an individual request is slow.

    Given: A stalled request and a shorter whole-pass deadline.
    When: The bounded pass runs.
    Then: The whole-pass timeout propagates promptly.
    """
    fixture = _Fixture()

    async def stall(symbol: str, start: datetime, end: datetime) -> list[CandleUpdate]:
        await asyncio.Event().wait()
        return []

    fixture.client.get_closed_1m_candles.side_effect = stall
    with pytest.raises(TimeoutError):
        await fixture.worker(CatchupPolicy(pass_seconds=0.001)).run_pass()


@pytest.mark.asyncio
async def test_unknown_commit_ack_reconciles_existing_rows_on_the_next_pass() -> None:
    """An empty replay after an uncertain commit schedules no repairs.

    Given: Durable insertion succeeded but its acknowledgement was lost.
    When: The next independent pass finds the existing key.
    Then: No row is rewritten and no repair notification is sent.
    """
    fixture = _Fixture()
    durable: list[CandleUpsertRow] = []

    async def insert(rows: list[CandleUpsertRow]) -> list[CandleUpsertRow]:
        if not durable:
            durable.extend(rows)
            raise ConnectionError("commit acknowledgement unavailable")
        return []

    fixture.repository.insert_missing_candles.side_effect = insert
    worker = fixture.worker()
    await worker.run_pass()
    fixture.committed.assert_not_awaited()
    await worker.run_pass()
    assert len(durable) == 1
    fixture.committed.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("work", ["run_pass", "run_sweep"])
async def test_supervisor_coalesces_requests_and_survives_a_failed_pass(work: str) -> None:
    """Reconnect requests stay paced and failures do not kill catchup.

    Given: Repeated reconnect notifications and one failed pass or full sweep.
    When: The supervisor runs until the publisher stops.
    Then: Requests coalesce and the following pass or sweep still executes.

    Args:
        work: The supervised operation that fails once.
    """
    fixture = _Fixture()
    worker = fixture.worker(
        CatchupPolicy(
            interval_seconds=0.003, minimum_spacing_seconds=0.001, sweep_spacing_seconds=0.001
        )
    )
    calls = 0

    async def run_pass() -> None:
        nonlocal calls
        calls += 1
        worker.request()
        worker.request()
        if calls == 1:
            raise RuntimeError("temporary bounded pass failure")
        fixture.running = False

    with patch.object(worker, work, side_effect=run_pass):
        await worker.run()
    assert calls == 2


@pytest.mark.asyncio
async def test_shutdown_cancellation_propagates_through_a_running_acquisition() -> None:
    """Publisher shutdown propagates actual worker cancellation.

    Given: Catchup is awaiting a real asynchronous acquisition boundary.
    When: Shutdown cancels its owned task.
    Then: CancelledError propagates without scheduling a repair.
    """
    fixture = _Fixture()
    entered = asyncio.Event()

    async def stall(symbol: str, start: datetime, end: datetime) -> list[CandleUpdate]:
        entered.set()
        await asyncio.Event().wait()
        return []

    fixture.client.get_closed_1m_candles.side_effect = stall
    task = asyncio.create_task(fixture.worker().run())
    await entered.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    fixture.committed.assert_not_awaited()


def test_repair_windows_require_exact_complete_contiguous_input() -> None:
    """Sparse, duplicate and unfinished source minutes never become repaired bars.

    Given: A closed five-minute window and variations in its source grid.
    When: Complete-window proofs are requested.
    Then: Only the exact complete contiguous grid is accepted.
    """
    rows = [_row(i) for i in range(25, 30)]
    end = START.replace(minute=30)
    windows = complete_repair_windows(rows, START, end, ["5m"])
    assert len(windows) == 1
    assert windows[0].begin == START
    assert windows[0].minutes == frozenset(row["open_at"] for row in rows)
    assert complete_repair_windows(rows[:-1], START, end, ["5m"]) == []
    assert complete_repair_windows([*rows, rows[0]], START, end, ["5m"]) == []
    assert complete_repair_windows(rows, START, end - timedelta(seconds=1), ["5m"]) == []


@pytest.mark.parametrize(
    "field,value",
    [
        ("complete", False),
        ("timeframe", "5m"),
        ("vwap", None),
        ("vwap", -1.0),
        ("vwap", True),
        ("volume", -1.0),
        ("volume", True),
        ("open", True),
        ("complete", 1),
        ("low", 0.0),
        ("open", 1.0),
        ("high", 1.0),
        ("trades", None),
        ("trades", -1),
        ("trades", True),
        ("close", float("nan")),
        ("open_at", START + timedelta(seconds=1)),
    ],
)
def test_invalid_committed_minute_refuses_the_entire_repair_window(
    field: str, value: object
) -> None:
    """Malformed durable minutes cannot bypass complete-window validation.

    Given: One invalid field in an otherwise complete five-minute source grid.
    When: The committed grid is checked for repair eligibility.
    Then: The whole window is withheld.
    """
    rows = [_row(i) for i in range(25, 30)]
    changed: dict[str, object] = dict(rows[0])
    changed[field] = value
    rows[0] = cast(CandleRow, changed)
    assert complete_repair_windows(rows, START, START.replace(minute=30), ["5m"]) == []


def test_repair_read_includes_full_enclosing_windows_and_empty_selection() -> None:
    """Higher-timeframe proofs include their enclosing source prefix.

    Given: A mid-hour recovery bound and selected higher timeframes.
    When: The durable read lower bound is resolved.
    Then: It includes the whole enclosing hour and preserves empty selections.
    """
    assert repair_read_start(START, []) == START
    assert repair_read_start(START, ["5m", "1h"]) == START.replace(minute=0)


@pytest.mark.parametrize(
    "field,value",
    [
        ("volume", 10**400),
        ("open", "10.0"),
        ("open_at", START.replace(tzinfo=None)),
        ("open_at", START.replace(second=1)),
        ("open_at", START.replace(microsecond=1)),
        ("open_at", NOW.replace(minute=36, second=0)),
    ],
)
def test_historical_minute_validator_refuses_invalid_number_and_closed_boundary(
    field: str, value: object
) -> None:
    """The shared first and second proof reject malformed numbers and open intervals.

    Given: One otherwise valid complete minute with a malformed numeric or temporal field.
    When: Historical eligibility is checked at the actual observation horizon.
    Then: Neither a non-number, overflow, naive clock nor unclosed minute is eligible.
    """
    changed: dict[str, object] = dict(_row(25))
    changed[field] = value
    assert not is_complete_historical_minute(cast(CandleRow, changed), NOW)


def test_historical_minute_validator_requires_aware_horizon_and_preserves_zero_vwap() -> None:
    """Strict evidence checks preserve the existing nonnegative native VWAP convention.

    Given: A canonical completed source minute with explicit zero VWAP and volume.
    When: An aware closed horizon and a naive horizon are compared.
    Then: Only the aware read is eligible and original zero values remain valid.
    """
    row = _row(25)
    row.update({"vwap": 0.0, "volume": 0.0})
    assert is_complete_historical_minute(row, START + timedelta(minutes=1))
    assert not is_complete_historical_minute(row, NOW.replace(tzinfo=None))


def _publisher() -> KrakenMarketDataPublisher:
    """Create a stopped publisher with deterministic settings and source boundaries."""
    publisher = KrakenMarketDataPublisher(["BTC-USD"])
    settings = MagicMock(spec=AppSettings)
    settings.timeframes = ["1m", "5m"]
    settings.spot_candle_source = "trade_built"
    settings.candle_minute_completion = False
    settings.trade_built_finalize_grace_seconds = 12
    settings.candle_identity_window_snapshot = False
    publisher.settings = settings
    return publisher


@pytest.fixture(autouse=True)
def _every_symbol_has_a_rest_name() -> Iterator[MagicMock]:
    """Keep roster tests independent of the symbol catalog unless they override it."""
    with patch(
        "snapper.messaging.publishers.kraken.has_kraken_candle_history_identity",
        return_value=True,
    ) as identity:
        yield identity


def test_catchup_roster_leaves_out_products_without_a_rest_name() -> None:
    """History cannot be requested for a product known only by its WebSocket alias.

    Given: A roster where one product has no REST pair name, with and without a policy.
    When: Catchup resolves its roster.
    Then: That product is left out and the others remain.
    """
    publisher = _publisher()
    named = "snapper.messaging.publishers.kraken.has_kraken_candle_history_identity"
    with patch(named, side_effect=lambda symbol: symbol != "AAPLX-USD"):
        assert publisher._catchup_symbols(["AAPLX-USD", "BTC-USD"]) == ["BTC-USD"]
        policy = MagicMock()
        policy.iter_persisted_instruments.return_value = [
            (ExchangeEnum.KRAKEN, "AAPLX-USD"),
            (ExchangeEnum.KRAKEN, "BTC-USD"),
        ]
        policy.should_persist.return_value = True
        publisher.set_persist_policy(policy)
        assert publisher._catchup_symbols(["*"]) == ["BTC-USD"]


def test_failure_label_explains_validation_refusals_only() -> None:
    """A validation refusal is logged with its reason; other failures by class alone.

    Given: A validation refusal and a transport failure carrying remote text.
    When: Each is named for the unresolved log line.
    Then: Only the validation refusal includes its message.
    """
    assert _failure_label(ValueError("no REST identity")) == "ValueError: no REST identity"
    assert _failure_label(TimeoutError("remote text")) == "TimeoutError"


def test_catchup_roster_without_policy_matches_live_persistence() -> None:
    """Catchup covers what live writes persist when no policy was injected.

    Given: A subprocess-mode publisher without an injected persistence policy.
    When: Catchup resolves its roster for wildcard and explicit subscriptions.
    Then: It returns every available or subscribed symbol, deduplicated and sorted.
    """
    publisher = _publisher()
    with patch(
        "snapper.messaging.publishers.kraken.get_available_kraken_symbols",
        return_value=["ETH-USD", "BTC-USD"],
    ):
        assert publisher._catchup_symbols(["*"]) == ["BTC-USD", "ETH-USD"]
    assert publisher._catchup_symbols(["SOL-USD", "BTC-USD", "SOL-USD"]) == ["BTC-USD", "SOL-USD"]


def test_catchup_roster_is_persisted_subscribed_and_excludes_policy_denials() -> None:
    """Catchup cannot expand an injected or excluded persistence policy.

    Given: Wildcard persistence, an excluded pair and an explicit subscription.
    When: Catchup resolves its permitted roster.
    Then: Only subscribed persisted Spot instruments are returned.
    """
    publisher = _publisher()
    policy = MagicMock()
    policy.iter_persisted_instruments.return_value = [
        (ExchangeEnum.KRAKEN, "*"),
        (ExchangeEnum.KRAKEN_FUTURES, "OTHER"),
    ]
    policy.should_persist.side_effect = lambda exchange, kind, symbol: symbol != "EXCLUDED"
    publisher.set_persist_policy(policy)
    with patch(
        "snapper.messaging.publishers.kraken.get_available_kraken_symbols",
        return_value=["BTC-USD", "ETH-USD", "EXCLUDED"],
    ):
        assert publisher._catchup_symbols(["BTC-USD"]) == ["BTC-USD"]
        assert publisher._catchup_symbols(["*"]) == ["BTC-USD", "ETH-USD"]
    policy.iter_persisted_instruments.return_value = [(ExchangeEnum.KRAKEN, "BTC-USD")]
    assert publisher._catchup_symbols(["*"]) == ["BTC-USD"]


@pytest.mark.asyncio
async def test_startup_background_task_never_blocks_live_consumers() -> None:
    """Bounded catchup is scheduled independently of live consumer startup.

    Given: A configured trade-built publisher with an existing client.
    When: Extra background tasks start and a feed break follows.
    Then: A supervised worker starts and the client observation epoch resets.
    """
    publisher = _publisher()
    publisher.repository = MagicMock(spec=Repository)
    publisher._exchange_client = MagicMock(spec=KrakenExchangeClient)
    with patch.object(KrakenCandleCatchup, "run", new_callable=AsyncMock) as run:
        tasks = await publisher._start_extra_background_tasks(["BTC-USD"])
        assert len(tasks) == 1
        await asyncio.gather(*tasks)
        run.assert_awaited_once()
    catchup = publisher._candle_catchup
    assert catchup is not None
    assert catchup._callbacks.insert_missing == publisher._insert_catchup_candle_rows
    assert catchup._callbacks.correct_zero == publisher._correct_catchup_candle_rows
    publisher._suspend_candle_synthesis_across_break()
    publisher._exchange_client.reset_trade_candle_epoch.assert_called_once()


@pytest.mark.asyncio
async def test_committed_reconciliation_schedules_historical_only_full_grid() -> None:
    """Only committed complete contiguous source sets schedule historical repair.

    Given: A complete durable five-minute grid after missing-key reconciliation.
    When: Higher-timeframe repair dependencies are reconciled.
    Then: The pending repair requires every minute and suppresses live publication.
    """
    publisher = _publisher()
    await publisher._reconcile_catchup_repairs(
        "BTC-USD", START, START.replace(minute=30), frozenset()
    )
    publisher._candle_aggregator = CandleAggregator(["5m"])
    repository = MagicMock(spec=Repository)
    repository.get_candles = AsyncMock(return_value=[_row(i) for i in range(25, 30)])
    publisher.repository = repository
    await publisher._reconcile_catchup_repairs(
        "BTC-USD", START, START.replace(minute=30), frozenset()
    )
    key = _CandleRepairKey("BTC-USD", "5m", START)
    assert publisher._pending_candle_repairs[key].historical_only is True
    assert publisher._pending_candle_repairs[key].expected_source_minutes == frozenset(
        START.replace(minute=i) for i in range(25, 30)
    )
    publisher.settings.timeframes = ["1m"]
    repository.get_candles.reset_mock()
    await publisher._reconcile_catchup_repairs(
        "BTC-USD", START, START.replace(minute=30), frozenset()
    )
    repository.get_candles.assert_not_awaited()


@pytest.mark.asyncio
async def test_only_windows_holding_a_changed_minute_start_a_new_repair_generation() -> None:
    """A catchup change revises the pending repairs of its own windows only.

    Given: Pending historical repairs for two complete five-minute windows.
    When: Catchup reports one changed minute in the second window.
    Then: The second window gets a new pending object; the first keeps its own.
    """
    publisher = _publisher()
    publisher._candle_aggregator = CandleAggregator(["5m"])
    repository = MagicMock(spec=Repository)
    repository.get_candles = AsyncMock(return_value=[_row(i) for i in range(25, 35)])
    publisher.repository = repository
    end = START.replace(minute=35)
    await publisher._reconcile_catchup_repairs("BTC-USD", START, end, frozenset())
    first = _CandleRepairKey("BTC-USD", "5m", START)
    second = _CandleRepairKey("BTC-USD", "5m", START.replace(minute=30))
    before = {key: publisher._pending_candle_repairs[key] for key in (first, second)}
    before[second].attempts = 2
    await publisher._reconcile_catchup_repairs(
        "BTC-USD", START, end, frozenset({START.replace(minute=32)})
    )
    assert publisher._pending_candle_repairs[first] is before[first]
    revised = publisher._pending_candle_repairs[second]
    assert revised is not before[second]
    assert revised.attempts == 0
    assert revised.expected_source_minutes == before[second].expected_source_minutes
    assert revised.historical_only is True


@pytest.mark.asyncio
@pytest.mark.parametrize("revised", [False, True])
async def test_attempt_that_read_a_corrected_minute_cannot_clear_its_revised_repair(
    revised: bool,
) -> None:
    """A correction during a running repair keeps the repair pending for another run.

    Given: A historical repair whose transaction is in flight with an aggregate
        derived before a source minute was corrected.
    When: Catchup schedules the same window again, with or without the revision.
    Then: Without the revision the old attempt clears the request; with it the
        request stays pending as a new object, ready to derive again.
    """
    publisher = _publisher()
    publisher._ensure_instrument = AsyncMock(return_value="instrument")
    publisher._publish_message = AsyncMock()
    publisher._candle_aggregator = CandleAggregator(["5m"])
    key = _CandleRepairKey("BTC-USD", "5m", START)
    minutes = frozenset(START.replace(minute=i) for i in range(25, 30))
    publisher._schedule_seeded_incomplete_candle_repair_key(key, minutes, historical_only=True)
    publisher._derive_repaired_synthesized_candle = AsyncMock(return_value=_candle())
    repository = MagicMock(spec=Repository)
    original = publisher._pending_candle_repairs[key]

    async def commit(rows: list[CandleUpsertRow]) -> int:
        publisher._schedule_seeded_incomplete_candle_repair_key(
            key, minutes, historical_only=True, revised=revised
        )
        return len(rows)

    repository.upsert_historical_synthesized_candles = AsyncMock(side_effect=commit)
    publisher.repository = repository
    await publisher._run_due_candle_repairs(ExchangeEnum.KRAKEN)
    if revised:
        assert publisher._pending_candle_repairs[key] is not original
        assert publisher._candle_repair_event.is_set()
    else:
        assert key not in publisher._pending_candle_repairs
    publisher._publish_message.assert_not_awaited()


@pytest.mark.asyncio
async def test_historical_repair_commits_without_publishing_and_waits_for_ack() -> None:
    """Historical corrections await guarded commits without fresh market events.

    Given: A historical-only repair with a proved complete source grid.
    When: Its guarded historical transaction waits for COMMIT acknowledgement.
    Then: Pending work survives until ACK and no live message or queue row appears.
    """
    publisher = _publisher()
    publisher._ensure_instrument = AsyncMock(return_value="instrument")
    publisher._publish_message = AsyncMock()
    publisher._candle_aggregator = CandleAggregator(["5m"])
    key = _CandleRepairKey("BTC-USD", "5m", START)
    minutes = frozenset(START.replace(minute=i) for i in range(25, 30))
    publisher._schedule_seeded_incomplete_candle_repair_key(key, minutes, historical_only=True)
    publisher._schedule_seeded_incomplete_candle_repair_key(key, minutes)
    publisher._derive_repaired_synthesized_candle = AsyncMock(return_value=_candle())
    repository = MagicMock(spec=Repository)
    entered, acknowledged = asyncio.Event(), asyncio.Event()

    async def commit(rows: list[CandleUpsertRow]) -> int:
        entered.set()
        await acknowledged.wait()
        return len(rows)

    repository.upsert_historical_synthesized_candles = AsyncMock(side_effect=commit)
    publisher.repository = repository
    task = asyncio.create_task(publisher._run_due_candle_repairs(ExchangeEnum.KRAKEN))
    await entered.wait()
    publisher._publish_message.assert_not_awaited()
    assert key in publisher._pending_candle_repairs
    assert publisher._pending_candle_repairs[key].output_sequence is None
    assert publisher._candle_write_queue.empty()
    acknowledged.set()
    await task
    assert key not in publisher._pending_candle_repairs
    row = repository.upsert_historical_synthesized_candles.call_args.args[0][0]
    assert row["source"] == "synthesized"
    assert row["complete"] is True
    assert row["session_id"] == publisher._tracker.session_id
    assert row["open_at"] == START


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", ["cancel", "unknown", "new_generation", "replacement"])
async def test_historical_commit_keeps_pending_on_uncertainty_or_new_generation(fault: str) -> None:
    """A transaction return cannot clear uncertain or newly dependent repair work.

    Given: Historical work whose commit is cancelled, unknown or superseded.
    When: The real repair lifecycle awaits the guarded repository operation.
    Then: Its pending dependency remains and no live market event is sent.
    """
    publisher = _publisher()
    publisher._ensure_instrument = AsyncMock(return_value="instrument")
    publisher._publish_message = AsyncMock()
    publisher._candle_aggregator = CandleAggregator(["5m"])
    key = _CandleRepairKey("BTC-USD", "5m", START)
    minutes = frozenset(START.replace(minute=i) for i in range(25, 30))
    publisher._schedule_seeded_incomplete_candle_repair_key(key, minutes, historical_only=True)
    publisher._derive_repaired_synthesized_candle = AsyncMock(return_value=_candle())
    repository = MagicMock(spec=Repository)

    async def commit(rows: list[CandleUpsertRow]) -> int:
        if fault == "cancel":
            raise asyncio.CancelledError
        if fault == "unknown":
            raise ConnectionError("commit acknowledgement unavailable")
        if fault == "replacement":
            publisher._set_pending_candle_repair(
                key, _PendingCandleRepair(expected_source_minutes=minutes, historical_only=True)
            )
        else:
            publisher._replace_candle_write_sequence_reference(None, 123)
            publisher._pending_candle_repairs[key].required_sequence = 123
        return 0

    repository.upsert_historical_synthesized_candles = AsyncMock(side_effect=commit)
    publisher.repository = repository
    if fault == "cancel":
        with pytest.raises(asyncio.CancelledError):
            await publisher._run_due_candle_repairs(ExchangeEnum.KRAKEN)
    else:
        await publisher._run_due_candle_repairs(ExchangeEnum.KRAKEN)
    assert key in publisher._pending_candle_repairs
    publisher._publish_message.assert_not_awaited()
    assert publisher._candle_write_queue.empty()


@pytest.mark.asyncio
@pytest.mark.parametrize("policy_allowed", [False, True])
async def test_historical_commit_refuses_excluded_or_unresolved_identity(
    policy_allowed: bool,
) -> None:
    """Historical persistence cannot bypass publisher identity or persistence policy.

    Given: An excluded instrument or an unresolved canonical instrument identity.
    When: A guarded historical repair commit is requested.
    Then: It returns unresolved without calling the repository.
    """
    publisher = _publisher()
    publisher._ensure_instrument = AsyncMock(return_value=None)
    publisher.repository = MagicMock(spec=Repository)
    with patch.object(publisher, "_should_persist_row", return_value=policy_allowed):
        assert not await publisher._commit_historical_candle_repair(
            _candle(), ExchangeEnum.KRAKEN, "5m"
        )
    publisher.repository.upsert_historical_synthesized_candles.assert_not_called()


def test_default_acquisition_clock_is_actual_aware_utc() -> None:
    """Production provenance uses an actual aware clock by default.

    Given: Callbacks constructed without a test clock override.
    When: The provenance clock is read.
    Then: It returns a current aware UTC acquisition instant.
    """
    fixture = _Fixture()
    callbacks = CatchupCallbacks(
        lambda: [], fixture.resolve, fixture.committed, lambda: False, fixture.tracker
    )
    before = datetime.now(UTC)
    stamp = callbacks.clock()
    assert before <= stamp <= datetime.now(UTC)
    assert stamp.utcoffset() == timedelta(0)


def test_priority_policy_rejects_blank_names() -> None:
    """An explicit priority roster cannot silently contain a blank identity.

    Given: A typed policy with an empty priority symbol.
    When: The policy is constructed.
    Then: The malformed configuration is refused.
    """
    with pytest.raises(ValueError, match="canonical"):
        CatchupPolicy(priority_symbols=(" ",))


@pytest.mark.asyncio
async def test_wildcard_defaults_are_immediate_and_fair_pool_keeps_rotating() -> None:
    """A large wildcard universe cannot hide the default priority symbols.

    Given: Fifteen hundred other persisted symbols and the five default priority symbols.
    When: Three fixed-budget passes run.
    Then: The P&L valuation pairs run first, the rest of the priority pool follows,
        and independent fair work advances every later pass.
    """
    fixture = _Fixture()
    fixture.symbols = [f"ALT{i:04}-USD" for i in range(1500)] + [
        "BTC-USD",
        "ETH-USD",
        "EUR-USD",
        "BTC-EUR",
        "XRP-EUR",
    ]
    fixture.client.get_closed_1m_candles.return_value = []
    worker = fixture.worker()
    await worker.run_pass()
    await worker.run_pass()
    await worker.run_pass()
    visited = [call.args[0] for call in fixture.client.get_closed_1m_candles.call_args_list]
    assert visited == [
        "XRP-EUR",
        "EUR-USD",
        "BTC-EUR",
        "ALT0000-USD",
        "BTC-USD",
        "ETH-USD",
        "ALT0001-USD",
        "XRP-EUR",
        "EUR-USD",
    ]
    assert len(visited) == 3 * 3


@pytest.mark.asyncio
async def test_budget_exhaustion_advances_only_attempted_pool_and_cannot_starve_fair_work() -> None:
    """Cancelled priority and fair requests cannot skip unattempted roster entries.

    Given: A first-pass priority stall followed by a fair-slot stall.
    When: Two pass deadlines expire and subsequent bounded passes resume.
    Then: The fair cursor advances once per attempt and remaining priorities retain order.
    """
    fixture = _Fixture()
    fixture.symbols = ["BTC-USD", "ETH-USD", "EUR-USD", "A", "B", "C"]
    calls = 0

    async def fetch(symbol: str, start: datetime, end: datetime) -> list[CandleUpdate]:
        nonlocal calls
        calls += 1
        if calls <= 2:
            await asyncio.Event().wait()
        return []

    fixture.client.get_closed_1m_candles.side_effect = fetch
    worker = fixture.worker(CatchupPolicy(pass_seconds=0.003))
    for _ in range(2):
        with pytest.raises(TimeoutError):
            await worker.run_pass()
    await worker.run_pass()
    await worker.run_pass()
    visited = [call.args[0] for call in fixture.client.get_closed_1m_candles.call_args_list]
    assert visited == ["EUR-USD", "A", "B", "BTC-USD", "ETH-USD", "C", "EUR-USD", "BTC-USD"]


@pytest.mark.asyncio
@pytest.mark.parametrize("symbols", [["BTC-USD", "ETH-USD", "EUR-USD"], ["BTC-USD", "A", "B"]])
async def test_priority_worker_reuses_spare_slots_without_exceeding_total_budget(
    symbols: list[str],
) -> None:
    """A small priority or fair pool cannot waste the remaining bounded capacity.

    Given: A three-symbol roster with one of its scheduling pools small or empty.
    When: Initial and ordinary passes run.
    Then: Every pass still visits exactly the three available symbols.
    """
    fixture = _Fixture()
    fixture.symbols = symbols
    fixture.client.get_closed_1m_candles.return_value = []
    worker = fixture.worker()
    await worker.run_pass()
    await worker.run_pass()
    visited = [call.args[0] for call in fixture.client.get_closed_1m_candles.call_args_list]
    assert set(visited[:3]) == set(symbols) == set(visited[3:])


@pytest.mark.asyncio
async def test_removed_persistence_permission_refuses_a_response_learned_in_flight() -> None:
    """A persistence policy update takes effect before the acquired rows are written.

    Given: A permitted symbol removed from the roster during acquisition.
    When: Its native response returns.
    Then: No insertion or historical repair follows that response.
    """
    fixture = _Fixture()

    async def fetch(symbol: str, start: datetime, end: datetime) -> list[CandleUpdate]:
        fixture.symbols.clear()
        return [_candle()]

    fixture.client.get_closed_1m_candles.side_effect = fetch
    await fixture.worker().run_pass()
    fixture.repository.insert_missing_candles.assert_not_awaited()
    fixture.committed.assert_not_awaited()


@pytest.mark.parametrize("timeframe,width", [("1h", 60), ("1d", 1440)])
def test_settled_historical_windows_are_ready_without_a_live_frontier(
    timeframe: str, width: int
) -> None:
    """A complete historical hour or day does not wait for a new live rollover.

    Given: A settled full source grid and an empty live aggregator frontier.
    When: Historical-only readiness is evaluated.
    Then: Repair is eligible while the ordinary live repair stays gated.
    """
    publisher = _publisher()
    aggregator = CandleAggregator([timeframe])
    begin = NOW.replace(hour=0, minute=0, second=0) - timedelta(days=1)
    key = _CandleRepairKey("BTC-USD", timeframe, begin)
    minutes = frozenset(begin + timedelta(minutes=i) for i in range(width))
    pending = _PendingCandleRepair(expected_source_minutes=minutes)
    assert not publisher._candle_repair_window_ready(key, pending, aggregator)
    pending.historical_only = True
    with patch("snapper.messaging.publishers.base.datetime") as clock:
        clock.now.return_value = NOW
        assert publisher._candle_repair_window_ready(key, pending, aggregator)
        publisher._candle_aggregator = aggregator
        publisher._set_pending_candle_repair(key, pending)
        assert ("BTC-USD", begin) in publisher._due_candle_repair_groups()


def test_historical_readiness_requires_canonical_full_grid_and_elapsed_grace() -> None:
    """Historical-only status cannot bypass window closure or full-grid proof.

    Given: A just-ended five-minute window and variants of its source grid.
    When: Its canonical readiness is checked before and after settle grace.
    Then: Only the full aligned grid after the grace boundary is eligible.
    """
    publisher = _publisher()
    aggregator = CandleAggregator(["5m"])
    begin = START.replace(minute=30)
    key = _CandleRepairKey("BTC-USD", "5m", begin)
    minutes = frozenset(begin + timedelta(minutes=i) for i in range(5))
    pending = _PendingCandleRepair(expected_source_minutes=minutes, historical_only=True)
    with patch("snapper.messaging.publishers.base.datetime") as clock:
        clock.now.return_value = NOW.replace(second=2)
        assert not publisher._candle_repair_window_ready(key, pending, aggregator)
        clock.now.return_value = NOW.replace(second=5)
        assert publisher._candle_repair_window_ready(key, pending, aggregator)
        pending.expected_source_minutes = minutes - {begin}
        assert not publisher._candle_repair_window_ready(key, pending, aggregator)
        pending.expected_source_minutes = None
        assert not publisher._candle_repair_window_ready(key, pending, aggregator)
        unaligned = _CandleRepairKey("BTC-USD", "5m", begin + timedelta(minutes=1))
        assert not publisher._candle_repair_window_ready(unaligned, pending, aggregator)


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", ["none_expected", "duplicate", "wrong_minute", "incomplete"])
async def test_historical_derivation_rechecks_actual_committed_source_grid(fault: str) -> None:
    """The actual repair read independently refuses changed or duplicated inputs.

    Given: A scheduled historical repair and a malformed later durable read.
    When: The real repair derivation re-reads its source window.
    Then: No synthesized candle is returned from the incomplete evidence.
    """
    publisher = _publisher()
    publisher._candle_aggregator = CandleAggregator(["5m"])
    key = _CandleRepairKey("BTC-USD", "5m", START)
    minutes = frozenset(START.replace(minute=i) for i in range(25, 30))
    publisher._set_pending_candle_repair(
        key, _PendingCandleRepair(expected_source_minutes=minutes, historical_only=True)
    )
    rows = [_row(i) for i in range(25, 30)]
    expected: frozenset[datetime] | None = minutes
    if fault == "none_expected":
        expected = None
    elif fault == "duplicate":
        rows.append(rows[0])
    elif fault == "wrong_minute":
        rows[0] = _row(24)
    else:
        rows[0]["complete"] = False
    repository = MagicMock(spec=Repository)
    repository.get_candles = AsyncMock(return_value=rows)
    publisher.repository = repository
    assert await publisher._derive_repaired_synthesized_candle(key, expected) is None


async def _publish_and_commit_live_candle(
    publisher: KrakenMarketDataPublisher, repository: SQLAlchemyRepository, candle: CandleUpdate
) -> CandleUpsertRow:
    """Drive real publication and its queued durable write without a running writer."""
    await publisher._publish_synthesized_candle(candle, ExchangeEnum.KRAKEN, "5m")
    row = publisher._candle_write_queue.get_nowait()
    await repository.upsert_candles([row])
    publisher._candle_write_queue.task_done()
    return row


@pytest.mark.asyncio
@pytest.mark.parametrize("outcome", [0, 1, "failed"])
async def test_historical_commit_does_not_consume_live_topic_sequences(
    outcome: int | str,
) -> None:
    """Historical attempt provenance stays independent even for no-ops and failures.

    Given: An already numbered live candle stream and quiet historical work.
    When: The historical repository operation changes, skips or refuses its row.
    Then: The next real live event receives the immediately following sequence.
    """
    publisher = _publisher()
    publisher._ensure_instrument = AsyncMock(return_value="instrument")
    repository = MagicMock(spec=Repository)
    publisher.repository = repository
    topic = publisher._build_data_topic("BTC-USD", "candles", timeframe="5m")
    assert publisher._tracker.next_sequence(topic) == 1
    if outcome == "failed":
        repository.upsert_historical_synthesized_candles = AsyncMock(
            side_effect=ConnectionError("historical transaction failed")
        )
        with pytest.raises(ConnectionError):
            await publisher._commit_historical_candle_repair(_candle(), ExchangeEnum.KRAKEN, "5m")
    else:
        repository.upsert_historical_synthesized_candles = AsyncMock(return_value=outcome)
        assert await publisher._commit_historical_candle_repair(
            _candle(), ExchangeEnum.KRAKEN, "5m"
        )
    assert publisher._tracker.next_sequence(topic) == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("outcome", ["noop", "changed", "failed"])
async def test_historical_commit_preserves_live_identity_and_event_sequence(
    tmp_path: Path, outcome: str
) -> None:
    """Quiet historical persistence cannot rewrite live identity or consume event numbers.

    Given: A real committed live candle and a historical no-op, write or failure.
    When: Historical work runs between two ordinary live candle publications.
    Then: The live logical ID still equals storage and its sequence remains contiguous.
    """
    repository = SQLAlchemyRepository(f"sqlite+aiosqlite:///{tmp_path / 'publisher-history.db'}")
    await repository.create_all()
    publisher = _publisher()
    publisher.repository = repository
    publisher._ensure_instrument = AsyncMock(return_value="00000000-0000-4000-8000-000000000501")
    publisher._publish_message = AsyncMock()
    live = replace(_candle(), interval_begin=START + timedelta(minutes=10), interval=300)
    try:
        first = await _publish_and_commit_live_candle(publisher, repository, live)
        if outcome == "noop":
            native = dict(first)
            native.update({"open_at": START, "source": "native"})
            native.pop("public_id")
            await repository.upsert_candles([cast(CandleUpsertRow, native)])
        cache_before = dict(publisher._candle_id_cache)
        guarded = repository.upsert_historical_synthesized_candles
        with patch.object(
            repository, "upsert_historical_synthesized_candles", wraps=guarded
        ) as write:
            if outcome == "failed":
                write.side_effect = ConnectionError("historical transaction failed")
                with pytest.raises(ConnectionError):
                    await publisher._commit_historical_candle_repair(
                        _candle(), ExchangeEnum.KRAKEN, "5m"
                    )
            else:
                assert await publisher._commit_historical_candle_repair(
                    _candle(), ExchangeEnum.KRAKEN, "5m"
                )
            historical_row = write.call_args.args[0][0]
        assert publisher._candle_id_cache == cache_before
        assert "public_id" not in historical_row
        assert historical_row["session_id"] == publisher._tracker.session_id
        assert historical_row["sequence_id"] == 1
        assert publisher._tracker._counters["candles.historical_repair"] == 1
        second = await _publish_and_commit_live_candle(
            publisher, repository, replace(live, close=11.5)
        )
        assert second["public_id"] == first["public_id"]
        assert [
            call.args[1].sequence_id for call in publisher._publish_message.await_args_list
        ] == [1, 2]
        async with repository.session() as session:
            stored = (
                await session.execute(
                    select(Candle.public_id).where(
                        Candle.open_at == live.interval_begin, Candle.known_to == KNOWN_TO_MAX
                    )
                )
            ).scalar_one()
        assert second["public_id"] == stored
    finally:
        await repository.engine.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "field,value",
    [
        ("vwap", None),
        ("trades", None),
        ("trades", True),
        ("volume", True),
        ("open", True),
        ("complete", 1),
        ("vwap", float("inf")),
        ("close", float("nan")),
        ("timeframe", "5m"),
        ("open_at", START + timedelta(seconds=1)),
    ],
)
async def test_historical_reread_refuses_changed_business_fields_and_keeps_pending(
    field: str, value: object
) -> None:
    """A later durable read must re-prove original fields rather than invent defaults.

    Given: A source grid accepted by the initial complete-window proof.
    When: One field changes before the historical derivation re-reads storage.
    Then: No historical write or live output occurs and pending work remains.
    """
    publisher = _publisher()
    publisher._candle_aggregator = CandleAggregator(["5m"])
    rows = [_row(i) for i in range(25, 30)]
    publisher._ensure_instrument = AsyncMock(return_value="instrument")
    assert len(complete_repair_windows(rows, START, START.replace(minute=30), ["5m"])) == 1
    key = _CandleRepairKey("BTC-USD", "5m", START)
    minutes = frozenset(row["open_at"] for row in rows)
    publisher._schedule_seeded_incomplete_candle_repair_key(key, minutes, historical_only=True)
    changed: dict[str, object] = dict(rows[0])
    changed[field] = value
    rows[0] = cast(CandleRow, changed)
    repository = MagicMock(spec=Repository)
    repository.get_candles = AsyncMock(return_value=rows)
    publisher.repository = repository
    publisher._publish_message = AsyncMock()
    await publisher._run_due_candle_repairs(ExchangeEnum.KRAKEN)
    assert key in publisher._pending_candle_repairs
    repository.upsert_historical_synthesized_candles.assert_not_awaited()
    publisher._publish_message.assert_not_awaited()
    assert publisher._candle_write_queue.empty()


def _trusted_closed_trade_minute(minute_number: int = 34, clock: datetime = NOW) -> CandleUpdate:
    """Complete a real builder minute after its independent live cold-start fence."""
    builder = TradeCandleBuilder()
    minute = NOW.replace(minute=minute_number, second=0)
    for offset in (-1, 0):
        observed = minute + timedelta(minutes=offset, seconds=1)
        builder.update(
            TradeUpdate("BTC-USD", "buy", 3.0, 10.0, "market", observed),
            observed_at=observed,
        )
    completed = builder.pop_completed(clock)
    assert len(completed) == 1
    assert completed[0].interval_begin == minute
    return completed[0]


async def _enqueue_trusted_live_minute(
    publisher: KrakenMarketDataPublisher, candle: CandleUpdate, author: datetime
) -> CandleUpsertRow:
    """Use the actual publisher and finalizer before the independently scheduled writer."""
    with patch("snapper.messaging.publishers.base.datetime") as clock:
        clock.now.return_value = author
        assert await publisher._handle_candle_stream_item(
            candle, ExchangeEnum.KRAKEN, "kraken", "1m"
        )
    finalizer = publisher._native_finalizer
    assert finalizer is not None
    publisher._enqueue_finalized_candles(finalizer.flush(author), ExchangeEnum.KRAKEN, "kraken")
    return publisher._candle_write_queue.get_nowait()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "ordering", ["live_then_catchup", "catchup_then_live", "live_advanced_then_catchup"]
)
async def test_native_catchup_and_trusted_live_share_the_stored_minute_identity(
    tmp_path: Path, ordering: str
) -> None:
    """A legitimate live minute must retain one ID across native missing-only insertion.

    Given: A fully trusted builder minute and independently scheduled native catchup.
    When: Catchup either commits first or defers until the queued live writer acknowledges.
    Then: The published minute identity equals the actual committed logical identity.
    """
    repository = SQLAlchemyRepository(f"sqlite+aiosqlite:///{tmp_path / 'native-live-id.db'}")
    await repository.create_all()
    publisher = _publisher()
    publisher.repository = repository
    publisher._ensure_instrument = AsyncMock(return_value="00000000-0000-4000-8000-000000000501")
    publisher._publish_message = AsyncMock()
    publisher._native_finalizer = NativeCandleFinalizer(
        persist_intermediate=False, flush_grace_seconds=12
    )
    candle = _trusted_closed_trade_minute()
    advanced = ordering == "live_advanced_then_catchup"
    catchup_at = NOW + timedelta(minutes=1) if advanced else NOW
    client = MagicMock(spec=KrakenExchangeClient)
    client.get_closed_1m_candles = AsyncMock(return_value=[candle])
    worker = KrakenCandleCatchup(
        repository,
        client,
        CatchupCallbacks(
            lambda: ["BTC-USD"],
            publisher._ensure_instrument,
            AsyncMock(),
            lambda: True,
            publisher._tracker,
            lambda: catchup_at,
            insert_missing=publisher._insert_catchup_candle_rows,
        ),
    )
    try:
        if ordering != "catchup_then_live":
            queued = await _enqueue_trusted_live_minute(
                publisher, candle, NOW - timedelta(seconds=1)
            )
            if advanced:
                advanced_row = await _enqueue_trusted_live_minute(
                    publisher,
                    _trusted_closed_trade_minute(35, catchup_at),
                    catchup_at - timedelta(seconds=1),
                )
                publisher._candle_write_queue.task_done()
            with pytest.raises(CatchupDeferredError, match="outstanding="):
                await worker._reconcile_symbol("BTC-USD", candle.interval_begin, NOW)
            assert await repository.upsert_candles([queued]) == 1
            publisher._mark_candle_writes_committed([queued])
            if advanced:
                assert await repository.upsert_candles([advanced_row]) == 1
                publisher._mark_candle_writes_committed([advanced_row])
            await worker._reconcile_symbol("BTC-USD", candle.interval_begin, NOW)
        else:
            await worker._reconcile_symbol("BTC-USD", candle.interval_begin, NOW)
            queued = await _enqueue_trusted_live_minute(
                publisher, candle, NOW + timedelta(seconds=1)
            )
        changed = await repository.upsert_candles([queued])
        publisher._candle_write_queue.task_done()
        assert changed == (1 if ordering == "catchup_then_live" else 0)
        async with repository.session() as session:
            stored_id = (
                await session.execute(
                    select(Candle.public_id).where(
                        Candle.open_at == candle.interval_begin, Candle.known_to == KNOWN_TO_MAX
                    )
                )
            ).scalar_one()
        assert publisher._publish_message.await_count == (2 if advanced else 1)
        published = publisher._publish_message.await_args_list[0].args[1]
        assert published.public_id == stored_id
        if advanced:
            assert publisher._candle_id_cache[(queued["instrument_public_id"], "1m")][0] == (
                candle.interval_begin + timedelta(minutes=1)
            )
    finally:
        await repository.engine.dispose()


def _identity_publisher() -> tuple[KrakenMarketDataPublisher, MagicMock]:
    """Provide explicit canonical absence without substituting a mock object for an ID."""
    publisher = _publisher()
    repository = MagicMock(spec=Repository)
    repository.filter_missing_candles = AsyncMock(side_effect=lambda rows: list(rows))
    repository.get_canonical_candle_public_id = AsyncMock(return_value=None)
    repository.insert_missing_candles = AsyncMock(return_value=[])
    repository.correct_false_zero_candles = AsyncMock(return_value=[])
    publisher.repository = repository
    return publisher, repository


async def _resolve_test_identity(publisher: KrakenMarketDataPublisher, offset: int = 0) -> str:
    """Exercise the actual gated resolver on one fixed test-only natural key."""
    return await publisher._resolve_live_candle_public_id(
        "instrument", "1m", START + timedelta(minutes=offset), "BTC-USD"
    )


def _native_identity_row(publisher: KrakenMarketDataPublisher) -> CandleUpsertRow:
    """Build a real native candidate without assigning any logical identity."""
    fixture = _Fixture()
    callbacks = CatchupCallbacks(
        lambda: ["BTC-USD"],
        fixture.resolve,
        fixture.committed,
        lambda: True,
        publisher._tracker,
        lambda: NOW,
    )
    worker = KrakenCandleCatchup(fixture.repository, fixture.client, callbacks)
    return worker._row(_candle(), "instrument", NOW)


@pytest.mark.asyncio
async def test_identity_selection_serializes_native_commit_but_not_rest_acquisition() -> None:
    """REST acquisition remains independent while ID selection owns the commit gate.

    Given: A live canonical lookup paused before its first UUID selection.
    When: Native acquisition completes and requests its quiet commit.
    Then: REST completes, commit waits, and the selected live UUID is reused.
    """
    publisher, repository = _identity_publisher()
    entered, released, fetched = asyncio.Event(), asyncio.Event(), asyncio.Event()

    async def canonical(*args: object) -> None:
        entered.set()
        await released.wait()

    repository.get_canonical_candle_public_id.side_effect = canonical
    client = MagicMock(spec=KrakenExchangeClient)

    async def fetch(*args: object) -> list[CandleUpdate]:
        fetched.set()
        return [_candle()]

    client.get_closed_1m_candles = AsyncMock(side_effect=fetch)
    callbacks = CatchupCallbacks(
        lambda: ["BTC-USD"],
        AsyncMock(return_value="instrument"),
        AsyncMock(),
        lambda: True,
        publisher._tracker,
        lambda: NOW,
        publisher._insert_catchup_candle_rows,
    )
    worker = KrakenCandleCatchup(repository, client, callbacks)
    live = asyncio.create_task(_resolve_test_identity(publisher))
    await entered.wait()
    native = asyncio.create_task(worker._reconcile_symbol("BTC-USD", START, NOW))
    await fetched.wait()
    repository.insert_missing_candles.assert_not_awaited()
    released.set()
    public_id = await live
    await native
    assert repository.insert_missing_candles.await_args.args[0][0]["public_id"] == public_id
    assert not publisher._candle_identity_gate.locked()


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", ["noop", "failed", "cancelled", "unknown"])
async def test_quiet_commit_fate_preserves_unresolved_identity_and_releases_local_gate(
    fault: str,
) -> None:
    """Uncertain outcomes neither lose a published UUID nor leave the local gate locked.

    Given: An unresolved live UUID followed by a quiet native operation.
    When: The operation skips, fails, is cancelled or loses its commit acknowledgement.
    Then: The reservation survives cache advancement and the gate is released safely.
    """
    publisher, repository = _identity_publisher()
    public_id = await _resolve_test_identity(publisher)
    await _resolve_test_identity(publisher, 1)
    entered = asyncio.Event()

    async def insert(rows: list[CandleUpsertRow]) -> list[CandleUpsertRow]:
        entered.set()
        if fault == "cancelled":
            await asyncio.Event().wait()
        if fault != "noop":
            raise ConnectionError("quiet commit outcome unavailable")
        return []

    repository.insert_missing_candles.side_effect = insert
    task = asyncio.create_task(
        publisher._insert_catchup_candle_rows([_native_identity_row(publisher)])
    )
    await entered.wait()
    if fault == "cancelled":
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    elif fault == "noop":
        await task
    else:
        with pytest.raises(ConnectionError):
            await task
    assert not publisher._candle_identity_gate.locked()
    assert await _resolve_test_identity(publisher) == public_id


@pytest.mark.asyncio
async def test_unknown_native_commit_requires_fenced_lookup_before_any_new_identity() -> None:
    """A released local gate cannot justify guessing an unknown database outcome.

    Given: A native commit with an unknown acknowledgement and no local live reservation.
    When: The database participation fence refuses its canonical identity lookup.
    Then: No UUID, latest cache entry or live event is created.
    """
    publisher, repository = _identity_publisher()
    repository.insert_missing_candles.side_effect = ConnectionError("unknown commit")
    with pytest.raises(ConnectionError):
        await publisher._insert_catchup_candle_rows([_native_identity_row(publisher)])
    repository.get_canonical_candle_public_id.side_effect = TimeoutError("writer fence busy")
    with pytest.raises(TimeoutError):
        await _resolve_test_identity(publisher)
    assert publisher._pending_candle_identities == {}
    assert publisher._candle_id_cache == {}
    assert not publisher._candle_identity_gate.locked()


@pytest.mark.asyncio
async def test_reservations_follow_writer_capacity_and_never_evict_unacknowledged_keys() -> None:
    """Writer-derived capacity refuses new identities while retaining unresolved ones.

    Given: A one-row queue and one-row in-flight batch with two unresolved UUIDs.
    When: A third key arrives, and later an exact writer commit is acknowledged.
    Then: It is first refused without eviction and admitted only after the real ACK.
    """
    publisher, _repository = _identity_publisher()
    publisher._candle_write_queue = asyncio.Queue(maxsize=1)
    publisher._candle_batch_max_rows = 1
    first = await _resolve_test_identity(publisher)
    second = await _resolve_test_identity(publisher, 1)
    with pytest.raises(RuntimeError, match="bounded writer capacity"):
        await _resolve_test_identity(publisher, 2)
    assert len(publisher._pending_candle_identities) == 2
    assert await _resolve_test_identity(publisher) == first
    row = _native_identity_row(publisher)
    row["public_id"] = first
    publisher._mark_candle_writes_committed([row])
    assert await _resolve_test_identity(publisher, 2) not in {first, second}
    assert len(publisher._pending_candle_identities) == 2


@pytest.mark.asyncio
async def test_nonpersisted_live_events_require_no_durable_identity_reservation() -> None:
    """Read-only streamed symbols do not fill reservations that can never receive an ACK.

    Given: A symbol whose candle events are excluded by the persistence policy.
    When: Its ordinary live identity is selected.
    Then: Cache reuse works without database reads or unresolved durable reservations.
    """
    publisher, repository = _identity_publisher()
    with patch.object(publisher, "_should_persist_row", return_value=False):
        first = await _resolve_test_identity(publisher)
        assert await _resolve_test_identity(publisher) == first
    repository.get_canonical_candle_public_id.assert_not_awaited()
    assert publisher._pending_candle_identities == {}


@pytest.mark.asyncio
async def test_acknowledged_and_older_canonical_ids_preserve_the_latest_cache_boundary() -> None:
    """Committed historical IDs remain usable without rewinding the latest live window.

    Given: An acknowledged first window followed by an unresolved newer live window.
    When: A quiet candidate reuses the first ID and a later old-window read adopts it.
    Then: Both use the stored ID while the latest-window cache remains on the newer bar.
    """
    publisher, repository = _identity_publisher()
    first = await _resolve_test_identity(publisher)
    original = _native_identity_row(publisher)
    original["public_id"] = first
    publisher._mark_candle_writes_committed([original])
    assert await _resolve_test_identity(publisher) == first
    source = _native_identity_row(publisher)
    prepared = publisher._bind_pending_candle_identities([source])
    assert prepared[0]["public_id"] == first
    assert "public_id" not in source
    second = await _resolve_test_identity(publisher, 1)
    repository.get_canonical_candle_public_id.return_value = first
    assert await _resolve_test_identity(publisher) == first
    assert publisher._candle_id_cache[("instrument", "1m")] == (
        START + timedelta(minutes=1),
        second,
    )


@pytest.mark.asyncio
async def test_nonpositive_identity_capacity_refuses_before_any_event_allocation() -> None:
    """A malformed writer capacity never turns reservation storage into an unbounded map.

    Given: No positive queue or in-flight writer capacity.
    When: A new unresolved natural key requires an event identity.
    Then: Selection fails closed with no reservation or latest cache entry.
    """
    publisher, _repository = _identity_publisher()
    publisher._candle_write_queue = asyncio.Queue(maxsize=0)
    publisher._candle_batch_max_rows = 0
    with pytest.raises(RuntimeError, match="bounded writer capacity"):
        await _resolve_test_identity(publisher)
    assert publisher._pending_candle_identities == {}
    assert publisher._candle_id_cache == {}


class _MonotonicClock:
    """Settable stand-in for the publisher module's monotonic clock."""

    def __init__(self, now: float = 100.0) -> None:
        """Start the clock at a fixed reading.

        Args:
            now: Initial monotonic reading in seconds.
        """
        self.now = now

    def __call__(self) -> float:
        """Return the current reading.

        Returns:
            The settable monotonic reading.
        """
        return self.now


@pytest.fixture
def monotonic_clock(monkeypatch: pytest.MonkeyPatch) -> _MonotonicClock:
    """Freeze the publisher module's monotonic clock under test control."""
    clock = _MonotonicClock()
    monkeypatch.setattr("snapper.messaging.publishers.kraken.monotonic", clock)
    return clock


def _snapshot_publisher(
    identities: dict[str, str] | None = None,
) -> tuple[KrakenMarketDataPublisher, MagicMock]:
    """Provide a resolver with window snapshots on and a known two-instrument roster.

    Args:
        identities: Stored identities the window read reports.

    Returns:
        The publisher and its repository double.
    """
    publisher, repository = _identity_publisher()
    publisher.settings.candle_identity_window_snapshot = True
    publisher._instrument_cache = {"BTC-USD": "instrument", "ETH-USD": "other-instrument"}
    repository.get_canonical_candle_public_ids = AsyncMock(return_value=dict(identities or {}))
    return publisher, repository


async def _resolve_other_identity(publisher: KrakenMarketDataPublisher) -> str:
    """Resolve the second roster instrument at the shared test opening."""
    return await publisher._resolve_live_candle_public_id(
        "other-instrument", "1m", START, "ETH-USD"
    )


def _forget_local_identity(publisher: KrakenMarketDataPublisher) -> None:
    """Drop pending reservations and the newest-key cache, as after a later minute's commit."""
    publisher._pending_candle_identities.clear()
    publisher._candle_id_cache.clear()


@pytest.mark.asyncio
async def test_window_snapshot_answers_a_whole_window_with_one_read(
    monotonic_clock: _MonotonicClock,
) -> None:
    """One fenced window read replaces one fenced lookup per key.

    Given: A two-instrument roster, one of which has a stored identity.
    When: Both keys of one window are resolved.
    Then: One batch read answers both, the stored ID is adopted, the other key
        reserves a new ID, and no per-key lookup runs.
    """
    publisher, repository = _snapshot_publisher({"other-instrument": "stored-id"})
    reserved = await _resolve_test_identity(publisher)
    assert await _resolve_other_identity(publisher) == "stored-id"
    repository.get_canonical_candle_public_ids.assert_awaited_once_with(
        ["instrument", "other-instrument"], "1m", START
    )
    repository.get_canonical_candle_public_id.assert_not_awaited()
    assert publisher._identity_snapshots[("1m", START)].identities["instrument"] == reserved
    assert publisher._identity_counters == {"snapshot_answers": 2, "snapshot_reads": 1}


@pytest.mark.asyncio
async def test_a_snapshot_reservation_is_returned_again_within_the_bound(
    monotonic_clock: _MonotonicClock,
) -> None:
    """A key reserved from the snapshot keeps its ID after the local state moved on.

    Given: A key reserved from a window snapshot.
    When: Its pending reservation and the newest-key cache are gone and it
        is resolved again one second later.
    Then: The snapshot returns the same ID without a second read.
    """
    publisher, repository = _snapshot_publisher()
    reserved = await _resolve_test_identity(publisher)
    _forget_local_identity(publisher)
    monotonic_clock.now += 1.0
    assert await _resolve_test_identity(publisher) == reserved
    repository.get_canonical_candle_public_ids.assert_awaited_once()
    repository.get_canonical_candle_public_id.assert_not_awaited()


@pytest.mark.asyncio
async def test_an_expired_snapshot_is_read_again(monotonic_clock: _MonotonicClock) -> None:
    """No answer is used more than two monotonic seconds after its read began."""
    publisher, repository = _snapshot_publisher()
    await _resolve_test_identity(publisher)
    _forget_local_identity(publisher)
    repository.get_canonical_candle_public_ids.return_value = {"instrument": "committed-id"}
    monotonic_clock.now += 2.5
    assert await _resolve_test_identity(publisher) == "committed-id"
    assert repository.get_canonical_candle_public_ids.await_count == 2


@pytest.mark.asyncio
async def test_a_key_pending_at_the_read_is_excluded_from_its_snapshot(
    monotonic_clock: _MonotonicClock,
) -> None:
    """A key whose own live write could land during the read never trusts that read.

    Given: A pending reservation for one key when its window is read.
    When: The reservation is released, as by its writer's acknowledgement,
        and the key is resolved again within the bound.
    Then: The snapshot excludes it and the per-key lookup adopts the commit.
    """
    publisher, repository = _snapshot_publisher()
    key = ("instrument", "1m", START)
    publisher._pending_candle_identities[key] = "pending-id"
    await _resolve_other_identity(publisher)
    assert publisher._identity_snapshots[("1m", START)].excluded == frozenset({"instrument"})
    assert await _resolve_test_identity(publisher) == "pending-id"
    _forget_local_identity(publisher)
    repository.get_canonical_candle_public_id.return_value = "pending-id"
    assert await _resolve_test_identity(publisher) == "pending-id"
    repository.get_canonical_candle_public_id.assert_awaited_once_with(*key)
    assert publisher._identity_counters["per_key_lookups.excluded_pending"] == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("writer", ["insert", "correct", "historical"])
@pytest.mark.parametrize("outcome", ["commit", "failure"])
async def test_reconciliation_writes_drop_their_window_snapshot_first(
    monotonic_clock: _MonotonicClock, writer: str, outcome: str
) -> None:
    """A reconciliation write never leaves a read taken before it able to answer.

    Given: A live window snapshot and a reconciliation write into that window.
    When: The write commits or raises after starting, as on a lost acknowledgement.
    Then: The snapshot is gone before the write began, and the next resolution
        reads the window again.
    """
    publisher, repository = _snapshot_publisher()
    await _resolve_test_identity(publisher)
    _forget_local_identity(publisher)
    row = _native_identity_row(publisher)
    seen: list[bool] = []

    async def _write(*_args: object, **_kwargs: object) -> list[CandleUpsertRow]:
        """Record whether the snapshot was already dropped, then commit or fail."""
        seen.append(("1m", START) in publisher._identity_snapshots)
        if outcome == "failure":
            raise ConnectionError("lost acknowledgement")
        return []

    async def _correct(rows: list[CandleUpsertRow], dry_run: bool = False) -> list[CandleUpsertRow]:
        """Report the row as correctable on the preflight, then write it."""
        if dry_run:
            return [rows[0]]
        return await _write()

    repository.insert_missing_candles = AsyncMock(side_effect=_write)
    repository.correct_false_zero_candles = AsyncMock(side_effect=_correct)
    repository.upsert_historical_synthesized_candles = AsyncMock(side_effect=_write)
    repository.get_canonical_candle_public_id = AsyncMock(return_value=None)
    call = {
        "insert": publisher._insert_catchup_candle_rows,
        "correct": publisher._correct_catchup_candle_rows,
        "historical": publisher._persist_historical_candle_rows,
    }[writer]
    if outcome == "failure":
        with pytest.raises(ConnectionError):
            await call([row])
    else:
        await call([row])
    assert seen == [False]
    assert ("1m", START) not in publisher._identity_snapshots
    assert publisher._identity_counters["snapshot_drops_by_reconciliation"] == 1
    await _resolve_test_identity(publisher)
    assert repository.get_canonical_candle_public_ids.await_count == 2


@pytest.mark.asyncio
async def test_a_failed_read_falls_back_and_is_not_retried_within_the_bound(
    monotonic_clock: _MonotonicClock,
) -> None:
    """A window read that fails installs nothing and costs at most one read per bound.

    Given: A window read that raises, as on conflicting identities of one instrument.
    When: Two keys of the window are resolved, then another after the bound.
    Then: Each key takes the per-key lookup, the read is retried only after the bound.
    """
    publisher, repository = _snapshot_publisher()
    repository.get_canonical_candle_public_ids.side_effect = RuntimeError("conflicting")
    await _resolve_test_identity(publisher)
    await _resolve_other_identity(publisher)
    assert repository.get_canonical_candle_public_ids.await_count == 1
    assert repository.get_canonical_candle_public_id.await_count == 2
    assert publisher._identity_snapshots == {}
    assert publisher._identity_counters["per_key_lookups.read_failed"] == 2
    assert publisher._identity_counters["snapshot_read_failures"] == 1
    monotonic_clock.now += 2.5
    await _resolve_test_identity(publisher, 1)
    assert repository.get_canonical_candle_public_ids.await_count == 2


@pytest.mark.asyncio
async def test_a_cancelled_read_propagates_and_installs_nothing(
    monotonic_clock: _MonotonicClock,
) -> None:
    """Cancellation is not a read failure: it propagates and leaves no snapshot."""
    publisher, repository = _snapshot_publisher()
    repository.get_canonical_candle_public_ids.side_effect = asyncio.CancelledError()
    with pytest.raises(asyncio.CancelledError):
        await _resolve_test_identity(publisher)
    assert publisher._identity_snapshots == {}
    assert publisher._identity_read_failures == {}
    assert not publisher._candle_identity_gate.locked()


@pytest.mark.asyncio
async def test_keys_outside_the_snapshot_take_the_per_key_lookup(
    monotonic_clock: _MonotonicClock,
) -> None:
    """An instrument the read did not cover, or no known roster at all, looks itself up."""
    publisher, repository = _snapshot_publisher()
    await publisher._resolve_live_candle_public_id("new-instrument", "1m", START, "SOL-USD")
    assert publisher._identity_counters["per_key_lookups.not_covered"] == 1
    empty, empty_repository = _snapshot_publisher()
    empty._instrument_cache = {}
    await _resolve_test_identity(empty)
    empty_repository.get_canonical_candle_public_ids.assert_not_awaited()
    assert empty._identity_counters == {"per_key_lookups.not_covered": 1}
    assert repository.get_canonical_candle_public_id.await_count == 1


@pytest.mark.asyncio
async def test_the_disabled_setting_restores_the_per_key_lookup(
    monotonic_clock: _MonotonicClock,
) -> None:
    """With the setting off every unsettled key takes today's fenced per-key lookup."""
    publisher, repository = _snapshot_publisher()
    publisher.settings.candle_identity_window_snapshot = False
    await _resolve_test_identity(publisher)
    await _resolve_other_identity(publisher)
    repository.get_canonical_candle_public_ids.assert_not_awaited()
    assert repository.get_canonical_candle_public_id.await_count == 2
    assert publisher._identity_counters == {"per_key_lookups.disabled": 2}


@pytest.mark.asyncio
async def test_old_snapshots_and_failures_are_pruned_when_a_new_read_installs(
    monotonic_clock: _MonotonicClock,
) -> None:
    """Expired snapshots and remembered failures never accumulate."""
    publisher, _repository = _snapshot_publisher()
    publisher._identity_read_failures[("5m", START)] = monotonic_clock.now
    await _resolve_test_identity(publisher)
    monotonic_clock.now += 1.0
    await _resolve_test_identity(publisher, 2)
    assert set(publisher._identity_snapshots) == {
        ("1m", START),
        ("1m", START + timedelta(minutes=2)),
    }
    monotonic_clock.now += 2.5
    publisher._identity_read_failures[("15m", START)] = monotonic_clock.now - 0.5
    await _resolve_test_identity(publisher, 1)
    assert set(publisher._identity_snapshots) == {("1m", START + timedelta(minutes=1))}
    assert publisher._identity_read_failures == {("15m", START): monotonic_clock.now - 0.5}


@pytest.mark.asyncio
async def test_the_heartbeat_carries_the_identity_ledger_and_sample(
    monotonic_clock: _MonotonicClock,
) -> None:
    """Counters, read latency and the newest snapshot reservations reach the heartbeat."""
    publisher, _repository = _snapshot_publisher()
    reserved = await _resolve_test_identity(publisher)
    ledger = publisher._venue_feed_health().meta["candle_identity"]
    assert ledger == {
        "counters": {"snapshot_answers": 1, "snapshot_reads": 1},
        "last_read_ms": 0.0,
        "snapshots": 1,
        "sample": [
            {
                "instrument_public_id": "instrument",
                "timeframe": "1m",
                "open_at": START.isoformat(),
                "public_id": reserved,
            }
        ],
    }


@pytest.mark.asyncio
async def test_default_historical_boundary_uses_guarded_repository_without_events() -> None:
    """The generic historical hook keeps its direct guarded persistence contract.

    Given: A full historical candidate owned by a publisher without custom coordination.
    When: The base implementation persists that candidate.
    Then: It delegates to the guarded historical repository and emits no market event.
    """
    publisher, repository = _identity_publisher()
    row = _native_identity_row(publisher)
    await MarketDataPublisherService._persist_historical_candle_rows(publisher, [row])
    repository.upsert_historical_synthesized_candles.assert_awaited_once_with([row])


@pytest.mark.asyncio
@pytest.mark.parametrize("already_stored", [False, True])
async def test_acknowledged_historical_write_retires_dropped_live_identity(
    tmp_path: Path, already_stored: bool
) -> None:
    """A real quiet commit or replay resolves an emitted row whose writer dropped it.

    Given: An emitted higher candle whose queued durable row is subsequently dropped.
    When: Guarded historical synthesis acknowledges the matching stored UUID.
    Then: Both a real insert and a no-op replay retire that exact reservation.
    """
    repository = SQLAlchemyRepository(f"sqlite+aiosqlite:///{tmp_path / 'quiet-historical-id.db'}")
    await repository.create_all()
    publisher = _publisher()
    publisher.repository = repository
    instrument = "00000000-0000-4000-8000-000000000501"
    publisher._ensure_instrument = AsyncMock(return_value=instrument)
    publisher._publish_message = AsyncMock()
    try:
        await publisher._publish_synthesized_candle(_candle(), ExchangeEnum.KRAKEN, "5m")
        dropped = publisher._candle_write_queue.get_nowait()
        publisher._forget_candle_write_sequence(dropped)
        publisher._candle_write_queue.task_done()
        key = (instrument, "5m", START)
        assert publisher._pending_candle_identities[key] == dropped["public_id"]
        if already_stored:
            assert await repository.upsert_historical_synthesized_candles([dropped]) == 1
        await publisher._persist_historical_candle_rows([dropped])
        assert await repository.get_canonical_candle_public_id(*key) == dropped["public_id"]
        assert key not in publisher._pending_candle_identities
        assert publisher._publish_message.await_count == 1
    finally:
        await repository.engine.dispose()


@pytest.mark.asyncio
async def test_native_replay_after_real_commit_and_lost_ack_retires_live_identity(
    tmp_path: Path,
) -> None:
    """An empty native replay still confirms the identity committed before ACK loss.

    Given: A live event and a native insert that commits before losing its acknowledgement.
    When: Native reconciliation retries and returns an empty inserted-row list.
    Then: Fenced canonical readback retires the matching live reservation.
    """
    repository = SQLAlchemyRepository(f"sqlite+aiosqlite:///{tmp_path / 'native-replay-id.db'}")
    await repository.create_all()
    publisher = _publisher()
    publisher.repository = repository
    instrument = "00000000-0000-4000-8000-000000000501"
    publisher._ensure_instrument = AsyncMock(return_value=instrument)
    publisher._publish_message = AsyncMock()
    publisher._native_finalizer = NativeCandleFinalizer(
        persist_intermediate=False, flush_grace_seconds=12
    )
    try:
        candle = _trusted_closed_trade_minute()
        dropped = await _enqueue_trusted_live_minute(publisher, candle, NOW)
        publisher._forget_candle_write_sequence(dropped)
        publisher._candle_write_queue.task_done()
        row = _native_identity_row(publisher)
        row.update({"instrument_public_id": instrument, "open_at": candle.interval_begin})
        key = (instrument, "1m", candle.interval_begin)
        public_id = publisher._pending_candle_identities[key]
        original = repository.insert_missing_candles

        async def committed_without_ack(rows: list[CandleUpsertRow]) -> list[CandleUpsertRow]:
            assert len(await original(rows)) == 1
            raise ConnectionError("acknowledgement lost after real commit")

        with (
            patch.object(repository, "insert_missing_candles", new=committed_without_ack),
            pytest.raises(ConnectionError),
        ):
            await publisher._insert_catchup_candle_rows([row])
        assert publisher._pending_candle_identities[key] == public_id

        async def replay(rows: list[CandleUpsertRow]) -> list[CandleUpsertRow]:
            result = await original(rows)
            assert result == []
            return result

        with patch.object(repository, "insert_missing_candles", new=replay):
            assert await publisher._insert_catchup_candle_rows([row]) == []
        assert await repository.get_canonical_candle_public_id(*key) == public_id
        assert key not in publisher._pending_candle_identities
    finally:
        await repository.engine.dispose()


@pytest.mark.asyncio
async def test_small_identity_capacity_survives_repeated_acknowledged_quiet_cycles(
    tmp_path: Path,
) -> None:
    """Durable quiet repair cycles cannot consume the finite publication budget forever.

    Given: Capacity for only one queued and one in-flight unresolved live identity.
    When: Five emitted rows are dropped and subsequently committed by quiet synthesis.
    Then: Every matching durable UUID retires and later publications remain admitted.
    """
    repository = SQLAlchemyRepository(f"sqlite+aiosqlite:///{tmp_path / 'quiet-id-cycles.db'}")
    await repository.create_all()
    publisher = _publisher()
    publisher.repository = repository
    publisher._candle_write_queue = asyncio.Queue(maxsize=1)
    publisher._candle_batch_max_rows = 1
    publisher._ensure_instrument = AsyncMock(return_value="00000000-0000-4000-8000-000000000501")
    publisher._publish_message = AsyncMock()
    try:
        for offset in range(5):
            candle = replace(_candle(), interval_begin=START + timedelta(minutes=5 * offset))
            await publisher._publish_synthesized_candle(candle, ExchangeEnum.KRAKEN, "5m")
            dropped = publisher._candle_write_queue.get_nowait()
            publisher._forget_candle_write_sequence(dropped)
            publisher._candle_write_queue.task_done()
            await publisher._persist_historical_candle_rows([dropped])
            assert publisher._pending_candle_identities == {}
        assert publisher._publish_message.await_count == 5
    finally:
        await repository.engine.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["native", "historical"])
@pytest.mark.parametrize(
    "outcome", ["absent", "different", "error", "ambiguous", "cancelled", "replacement"]
)
async def test_acknowledged_quiet_write_keeps_unproven_or_replaced_reservation(
    kind: str, outcome: str
) -> None:
    """An acknowledged operation cannot retire an unresolved or newly replaced UUID.

    Given: One affected live reservation and a successful quiet write or replay.
    When: Canonical readback is absent, different, failed, cancelled or superseded.
    Then: The exact unresolved current reservation remains and the local gate releases.
    """
    publisher, repository = _identity_publisher()
    row = _native_identity_row(publisher)
    if kind == "historical":
        row.update({"timeframe": "5m", "source": "synthesized"})
    key = (row["instrument_public_id"], row["timeframe"], row["open_at"])
    public_id = await publisher._resolve_live_candle_public_id(*key, "BTC-USD")
    repository.get_canonical_candle_public_id.reset_mock()

    async def canonical(*args: object) -> str | None:
        if outcome == "error":
            raise ConnectionError("canonical outcome unavailable")
        if outcome == "ambiguous":
            raise RuntimeError("conflicting canonical identities")
        if outcome == "cancelled":
            raise asyncio.CancelledError
        if outcome == "replacement":
            publisher._pending_candle_identities[key] = "replacement-identity"
            return public_id
        return "different-canonical-identity" if outcome == "different" else None

    repository.get_canonical_candle_public_id.side_effect = canonical
    operation = (
        publisher._insert_catchup_candle_rows
        if kind == "native"
        else publisher._persist_historical_candle_rows
    )
    if outcome in {"error", "ambiguous", "cancelled"}:
        error = {
            "error": ConnectionError,
            "ambiguous": RuntimeError,
            "cancelled": asyncio.CancelledError,
        }[outcome]
        with pytest.raises(error):
            await operation([row])
    else:
        await operation([row])
    assert publisher._pending_candle_identities[key] == (
        "replacement-identity" if outcome == "replacement" else public_id
    )
    repository.get_canonical_candle_public_id.assert_awaited_once_with(*key)
    assert not publisher._candle_identity_gate.locked()


@pytest.mark.asyncio
async def test_quiet_retirement_deduplicates_only_affected_pending_keys_and_rechecks_them() -> None:
    """Only affected reservations are read, once, and a concurrently acknowledged key is skipped.

    Given: Two affected pending keys, repeated candidates and one unaffected reservation.
    When: The first canonical lookup independently resolves both affected reservations.
    Then: The second key is rechecked and no unrelated identity or duplicate is queried.
    """
    publisher, repository = _identity_publisher()
    for offset in range(3):
        await _resolve_test_identity(publisher, offset)
    first = _native_identity_row(publisher)
    second = dict(first)
    second["open_at"] = START + timedelta(minutes=1)
    rows = [first, first, cast(CandleUpsertRow, second)]

    async def canonical(*args: object) -> str:
        key = cast(tuple[str, str, datetime], args)
        expected = publisher._pending_candle_identities[key]
        affected = {("instrument", "1m", START), ("instrument", "1m", START + timedelta(minutes=1))}
        for other in affected - {key}:
            publisher._pending_candle_identities.pop(other)
        return expected

    repository.get_canonical_candle_public_id.reset_mock()
    repository.get_canonical_candle_public_id.side_effect = canonical
    async with publisher._candle_identity_gate:
        await publisher._retire_acknowledged_candle_identities(rows)
    assert repository.get_canonical_candle_public_id.await_count == 1
    assert set(publisher._pending_candle_identities) == {
        ("instrument", "1m", START + timedelta(minutes=2))
    }


@pytest.mark.asyncio
async def test_sweep_visits_every_symbol_once_and_survives_one_failure() -> None:
    """A restart sweep reaches the whole roster, not a fair slice of it.

    Given: Four permitted symbols, one of whose acquisitions fails.
    When: One sweep runs.
    Then: Every symbol is requested once over the full history window, the
        three others are committed, and spacing follows the initial regular
        pass and each attempt.
    """
    fixture = _Fixture()
    fixture.symbols = ["XRP-USD", "ADA-USD", "DOT-USD", "SOL-USD"]

    async def acquire(symbol: str, start: datetime, end: datetime) -> list[CandleUpdate]:
        if symbol == "DOT-USD":
            raise RuntimeError("temporary venue failure")
        return [_candle(symbol)]

    fixture.client.get_closed_1m_candles.side_effect = acquire

    async def insert(rows: list[CandleUpsertRow]) -> list[CandleUpsertRow]:
        return rows

    fixture.repository.insert_missing_candles.side_effect = insert
    worker = fixture.worker(CatchupPolicy(symbols_per_pass=2, sweep_spacing_seconds=0.001))
    with (
        patch("asyncio.sleep", new=AsyncMock()) as sleep,
        patch.object(worker, "run_pass", new=AsyncMock()),
    ):
        await worker.run_sweep()
    requested = [call.args[0] for call in fixture.client.get_closed_1m_candles.await_args_list]
    assert requested == ["ADA-USD", "DOT-USD", "SOL-USD", "XRP-USD"]
    end = NOW.replace(second=0)
    assert {call.args[1:] for call in fixture.client.get_closed_1m_candles.await_args_list} == {
        (end - timedelta(minutes=120), end)
    }
    assert [call.args[0] for call in fixture.committed.await_args_list] == [
        "ADA-USD",
        "SOL-USD",
        "XRP-USD",
    ]
    assert [call.args[0] for call in sleep.await_args_list if call.args[0]] == [0.001] * 5


@pytest.mark.asyncio
async def test_sweep_stops_when_the_publisher_stops() -> None:
    """Shutdown after the initial regular pass prevents any sweep acquisition.

    Given: Two permitted symbols and a publisher that stops during the first pause.
    When: One sweep runs.
    Then: No alphabetical request follows shutdown.
    """
    fixture = _Fixture()
    fixture.symbols = ["BTC-USD", "ETH-USD"]

    async def stop(_: float) -> None:
        fixture.running = False

    worker = fixture.worker(CatchupPolicy(sweep_spacing_seconds=0.001))
    with (
        patch("asyncio.sleep", new=AsyncMock(side_effect=stop)),
        patch.object(worker, "run_pass", new=AsyncMock()),
    ):
        await worker.run_sweep()
    fixture.client.get_closed_1m_candles.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("evidence", ["trades", "no_trades", "unavailable"])
async def test_publisher_catchup_corrects_zero_only_with_trades_and_replays_quietly(
    tmp_path: Path, evidence: str
) -> None:
    """Native acquisition and the identity-gated write respect the false-zero boundary.

    Given: A durable synthesized zero and native trades, no trades or unavailable history.
    When: Two actual catchup passes use the publisher reconciliation callback.
    Then: Only trade evidence creates one correction and one repair, with no live event.
    """
    repository = SQLAlchemyRepository(f"sqlite+aiosqlite:///{tmp_path / 'catchup-zero.db'}")
    await repository.create_all()
    publisher = _publisher()
    publisher.repository = repository
    publisher._publish_message = AsyncMock()
    fixture = _Fixture()
    instrument = "00000000-0000-4000-8000-000000000501"
    fixture.resolve.return_value = instrument
    worker = KrakenCandleCatchup(
        repository,
        fixture.client,
        CatchupCallbacks(
            lambda: fixture.symbols,
            fixture.resolve,
            fixture.committed,
            lambda: True,
            publisher._tracker,
            lambda: NOW,
            publisher._insert_catchup_candle_rows,
            publisher._correct_catchup_candle_rows,
        ),
    )
    zero = worker._row(_candle(), instrument, NOW - timedelta(hours=1))
    zero.update(
        {
            "source": "synthesized",
            "high": 10.0,
            "low": 10.0,
            "close": 10.0,
            "volume": 0.0,
            "vwap": 10.0,
            "trades": 0,
        }
    )
    if evidence == "unavailable":
        fixture.client.get_closed_1m_candles.side_effect = ConnectionError("unavailable")
    elif evidence == "no_trades":
        fixture.client.get_closed_1m_candles.return_value = [replace(_candle(), trades=0)]
    try:
        await repository.upsert_candles([zero])
        await worker.run_pass()
        await worker.run_pass()
        async with repository.session() as session:
            versions = (
                (await session.execute(select(Candle).order_by(Candle.timestamp))).scalars().all()
            )
        assert versions[0].trades == 0 and versions[0].source == "synthesized"
        assert len(versions) == (2 if evidence == "trades" else 1)
        if evidence == "trades":
            assert versions[0].known_to == versions[1].timestamp == NOW
            assert versions[1].public_id == versions[0].public_id
            assert versions[1].trades == 7 and versions[1].source == "native"
        else:
            assert versions[0].known_to == KNOWN_TO_MAX
        assert fixture.committed.await_count == (1 if evidence == "trades" else 0)
        publisher._publish_message.assert_not_awaited()
        assert publisher._candle_write_queue.empty()
    finally:
        await repository.engine.dispose()


@pytest.mark.asyncio
async def test_committed_insert_still_repairs_when_the_following_correction_fails() -> None:
    """An acknowledged insert schedules repairs even if the chunk's correction step fails.

    Given: A chunk whose absent minutes commit and whose correction then defers.
    When: The symbol is reconciled.
    Then: The deferral propagates, and the committed insert still schedules repairs.
    """
    fixture = _Fixture()
    fixture.repository.insert_missing_candles.side_effect = lambda rows: list(rows)
    fixture.repository.correct_false_zero_candles.side_effect = CatchupDeferredError(
        "live writer outstanding=1"
    )
    with pytest.raises(CatchupDeferredError):
        await fixture.worker()._reconcile_symbol("BTC-USD", START, NOW)
    fixture.committed.assert_awaited_once_with("BTC-USD", START, NOW, frozenset({START}))


@pytest.mark.asyncio
async def test_default_reconciliation_inserts_before_correcting_and_repairs_a_correction() -> None:
    """Without a publisher callback the worker still inserts first, then corrects.

    Given: A repository that inserts nothing and corrects every row of the chunk.
    When: One symbol is reconciled through the worker's own repository boundary.
    Then: Insertion precedes correction and the correction alone schedules repairs.
    """
    fixture = _Fixture()
    order: list[str] = []

    async def insert(rows: list[CandleUpsertRow]) -> list[CandleUpsertRow]:
        order.append("insert")
        return []

    async def correct(rows: list[CandleUpsertRow]) -> list[CandleUpsertRow]:
        order.append("correct")
        return list(rows)

    fixture.repository.insert_missing_candles.side_effect = insert
    fixture.repository.correct_false_zero_candles.side_effect = correct
    await fixture.worker()._reconcile_symbol("BTC-USD", START, NOW)
    assert order == ["insert", "correct"]
    fixture.committed.assert_awaited_once_with("BTC-USD", START, NOW, frozenset({START}))


@pytest.mark.asyncio
async def test_startup_priority_failure_retries_during_a_paced_large_sweep() -> None:
    """A failed valuation pair is retried before the long alphabetical sweep completes.

    Given: A large roster and the first EUR-USD acquisition failing.
    When: The startup sweep runs with its paced regular passes.
    Then: EUR-USD runs first and is retried by a regular pass during the sweep.
    """
    fixture = _Fixture()
    fixture.symbols = [f"ALT{i:03}-USD" for i in range(45)] + ["EUR-USD", "BTC-USD", "ETH-USD"]
    elapsed = 0.0
    observations: list[tuple[str, float]] = []
    pauses: list[float] = []
    euro_attempts = 0

    async def acquire(symbol: str, start: datetime, end: datetime) -> list[CandleUpdate]:
        nonlocal euro_attempts
        observations.append((symbol, elapsed))
        if symbol == "EUR-USD":
            euro_attempts += 1
            if euro_attempts == 1:
                raise TimeoutError("native history temporarily unavailable")
        return []

    async def pause(seconds: float) -> None:
        nonlocal elapsed
        pauses.append(seconds)
        elapsed += seconds
        if fixture.client.get_closed_1m_candles.await_count >= len(fixture.symbols) + 9:
            fixture.running = False

    fixture.client.get_closed_1m_candles.side_effect = acquire
    worker = fixture.worker()
    with (
        patch("asyncio.sleep", new=AsyncMock(side_effect=pause)),
        patch(
            "snapper.messaging.publishers.kraken_candle_catchup.monotonic",
            side_effect=lambda: elapsed,
        ),
    ):
        await worker.run_sweep()
    visited = [symbol for symbol, _ in observations]
    assert visited[:3] == ["EUR-USD", "BTC-USD", "ETH-USD"]
    retry = next(i for i in range(1, len(visited)) if visited[i] == "EUR-USD")
    assert retry < visited.index("ALT044-USD")
    assert 60 <= observations[retry][1] <= 63
    assert set(pauses) == {worker._policy.sweep_spacing_seconds}
    assert len(visited) <= len(fixture.symbols) + 3 * (1 + int(elapsed // 60))


@pytest.mark.asyncio
async def test_sweep_survives_a_failed_pass_and_stops_during_an_interleaved_pass() -> None:
    """A whole-pass failure cannot cancel the sweep or allow work after shutdown.

    Given: One pass timeout and a later pass that stops the publisher.
    When: A sweep reaches its next regular pass boundary.
    Then: Alphabetical acquisition resumes after the failure and ends after shutdown.
    """
    fixture = _Fixture()
    fixture.symbols = ["A", "B", "C"]
    worker = fixture.worker()
    calls = 0

    async def bounded_pass() -> None:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise TimeoutError("whole pass deadline")
        fixture.running = False

    with (
        patch.object(worker, "run_pass", new=AsyncMock(side_effect=bounded_pass)),
        patch("asyncio.sleep", new=AsyncMock()),
        patch(
            "snapper.messaging.publishers.kraken_candle_catchup.monotonic",
            side_effect=[0.0, 1.0, 61.0, 61.0],
        ),
    ):
        await worker.run_sweep()
    assert calls == 2
    assert [call.args[0] for call in fixture.client.get_closed_1m_candles.await_args_list] == ["A"]


@pytest.mark.asyncio
async def test_priority_restart_recovers_a_missed_minute_older_than_the_ordinary_window() -> None:
    """Restarting does not discard a priority gap after the ordinary lookback expires.

    Given: A failed valuation-pair request followed by a restart four hours later.
    When: The new worker requests bounded native history.
    Then: The old minute is recovered while ordinary pairs keep a two-hour bound.
    """
    fixture = _Fixture()
    fixture.symbols = ["EUR-USD", "ALT-USD"]
    fixture.client.get_closed_1m_candles.side_effect = ConnectionError("unavailable")
    await fixture.worker().run_pass()
    recovered: list[CandleUpsertRow] = []
    restarted_at = NOW + timedelta(hours=4)
    missed = _candle("EUR-USD")

    async def acquire(symbol: str, start: datetime, end: datetime) -> list[CandleUpdate]:
        if symbol == "EUR-USD" and start <= missed.interval_begin < end:
            return [missed]
        return []

    async def commit(rows: list[CandleUpsertRow]) -> list[CandleUpsertRow]:
        recovered.extend(rows)
        return rows

    fixture.client.get_closed_1m_candles.reset_mock()
    fixture.client.get_closed_1m_candles.side_effect = acquire
    fixture.repository.insert_missing_candles.side_effect = commit
    restarted = fixture.worker()
    restarted._callbacks = replace(restarted._callbacks, clock=lambda: restarted_at)
    await restarted.run_pass()
    assert [row["open_at"] for row in recovered] == [missed.interval_begin]
    assert recovered[0]["timestamp"] == restarted_at
    requested = fixture.client.get_closed_1m_candles.await_args_list
    assert {
        call.args[0]: (call.args[2] - call.args[1]).total_seconds() / 60 for call in requested
    } == {
        "EUR-USD": 719,
        "ALT-USD": 120,
    }


@pytest.mark.asyncio
async def test_supervisor_sweeps_at_start_and_after_each_feed_break() -> None:
    """Startup and every reconnect run a full sweep; other wakeups run fair passes.

    Given: A supervisor whose second unit of work receives a reconnect request.
    When: It runs four units of work.
    Then: The order is sweep, pass, sweep, pass.
    """
    fixture = _Fixture()
    worker = fixture.worker(
        CatchupPolicy(
            interval_seconds=0.003, minimum_spacing_seconds=0.001, sweep_spacing_seconds=0.001
        )
    )
    order: list[str] = []

    async def record(kind: str) -> None:
        order.append(kind)
        if len(order) == 2:
            worker.request()
        if len(order) == 4:
            fixture.running = False

    async def sweep() -> None:
        await record("sweep")

    async def fair_pass() -> None:
        await record("pass")

    with (
        patch.object(worker, "run_sweep", side_effect=sweep),
        patch.object(worker, "run_pass", side_effect=fair_pass),
    ):
        await worker.run()
    assert order == ["sweep", "pass", "sweep", "pass"]


def test_default_sweep_fits_the_history_window_for_the_spot_roster() -> None:
    """A full default sweep still reaches every restart gap inside the window.

    Given: The default policy and the 1,853-symbol Kraken spot roster.
    When: The paced sweep duration is compared with the history window.
    Then: It leaves at least fifteen minutes for feed startup and the gap itself.
    """
    policy = CatchupPolicy()
    sweep_minutes = 1853 * (policy.sweep_spacing_seconds + 0.1) / 60
    assert sweep_minutes + 15 <= policy.history_minutes


@pytest.mark.asyncio
async def test_native_worker_chunks_commits_and_repairs_after_a_later_failure() -> None:
    """An acknowledged first chunk still schedules grid-verified repair after a later failure.

    Given: A response with 201 minutes and a second-transaction failure.
    When: The worker acknowledges its first chunk.
    Then: Each transaction stays bounded and committed inputs still trigger repair.
    """
    fixture = _Fixture()
    candles = [
        replace(_candle(), interval_begin=START - timedelta(minutes=offset))
        for offset in range(201)
    ]
    fixture.client.get_closed_1m_candles.return_value = candles
    calls: list[int] = []

    async def insert(rows: list[CandleUpsertRow]) -> list[CandleUpsertRow]:
        """Acknowledge the first independent transaction and fail the second."""
        calls.append(len(rows))
        if len(calls) == 2:
            raise ConnectionError("second transaction unavailable")
        return rows

    fixture.repository.insert_missing_candles.side_effect = insert
    worker = fixture.worker()
    with pytest.raises(ConnectionError, match="second transaction"):
        await worker._reconcile_symbol("BTC-USD", START, NOW)

    assert calls == [100, 100]
    assert len(fixture.repository.filter_missing_candles.await_args.args[0]) == 201
    fixture.committed.assert_awaited_once_with(
        "BTC-USD",
        START,
        NOW,
        frozenset(candle.interval_begin for candle in candles[:100]),
    )


@pytest.mark.asyncio
async def test_native_worker_validates_full_response_before_first_chunk() -> None:
    """A malformed suffix prevents every partial write rather than failing after one commit.

    Given: Whole-response validation refuses a malformed input.
    When: Reconciliation attempts to process that response.
    Then: No insertion or repair acknowledgement occurs.
    """
    fixture = _Fixture()
    fixture.repository.filter_missing_candles.side_effect = ValueError("invalid response suffix")
    with pytest.raises(ValueError, match="invalid response suffix"):
        await fixture.worker()._reconcile_symbol("BTC-USD", START, NOW)
    fixture.repository.insert_missing_candles.assert_not_awaited()
    fixture.committed.assert_not_awaited()


@pytest.mark.asyncio
async def test_present_native_replay_avoids_local_identity_gate() -> None:
    """A fully present replay with no reservation completes while live identity owns its gate.

    Given: All native keys already exist and the local identity gate is held.
    When: Read-only preflight confirms their presence.
    Then: Replay completes without waiting for that gate or entering writer participation.
    """
    publisher, repository = _identity_publisher()
    repository.filter_missing_candles.return_value = []
    repository.filter_missing_candles.side_effect = None
    async with publisher._candle_identity_gate:
        async with asyncio.timeout(1):
            assert (
                await publisher._insert_catchup_candle_rows([_native_identity_row(publisher)]) == []
            )
    repository.insert_missing_candles.assert_not_awaited()
    repository.get_canonical_candle_public_id.assert_not_awaited()


@pytest.mark.asyncio
async def test_native_preflight_runs_outside_identity_gate() -> None:
    """Presence reads do not hold the gate needed to choose and acknowledge live identities.

    Given: Missing candidates require a presence read before native insertion.
    When: The repository preflight is awaited.
    Then: The local identity gate remains available to live publication.
    """
    publisher, repository = _identity_publisher()

    async def preflight(rows: list[CandleUpsertRow]) -> list[CandleUpsertRow]:
        """Inspect the actual gate while the awaited repository boundary runs."""
        assert not publisher._candle_identity_gate.locked()
        return rows

    repository.filter_missing_candles.side_effect = preflight
    await publisher._insert_catchup_candle_rows([_native_identity_row(publisher)])


@pytest.mark.asyncio
@pytest.mark.parametrize("backlog", ["queued", "inflight", "errors", "backoff"])
async def test_native_catchup_explicitly_defers_until_live_writer_recovers(backlog: str) -> None:
    """Queued and retained in-flight live writes take priority without reporting false success.

    Given: A queued, in-flight or recovering live writer.
    When: Native reconciliation requests admission before and after recovery.
    Then: The first attempt explicitly defers and the healthy retry enters insertion.
    """
    publisher, repository = _identity_publisher()
    row = _native_identity_row(publisher)
    if backlog == "queued":
        publisher._candle_write_queue.put_nowait(row)
    elif backlog == "inflight":
        publisher._assign_candle_write_sequence(row)
    elif backlog == "errors":
        publisher._flush_errors["candle"] = 1
    else:
        publisher._candle_writer_backoff_s = 2.0
    with pytest.raises(CatchupDeferredError, match="live writer"):
        await publisher._insert_catchup_candle_rows([row])
    repository.insert_missing_candles.assert_not_awaited()
    assert not publisher._candle_identity_gate.locked()
    if backlog == "queued":
        publisher._candle_write_queue.get_nowait()
        publisher._candle_write_queue.task_done()
    elif backlog == "inflight":
        publisher._forget_candle_write_sequence(row)
    publisher._flush_errors["candle"] = 0
    publisher._candle_writer_backoff_s = 1.0
    await publisher._insert_catchup_candle_rows([row])
    repository.insert_missing_candles.assert_awaited_once()


@pytest.mark.asyncio
async def test_nothing_to_correct_skips_the_identity_gate_and_the_write() -> None:
    """A chunk without a correctable zero never waits for the local gate.

    Given: A preflight that finds no correctable zero while live identity holds the gate.
    When: The publisher correction step runs.
    Then: It returns at once, without a write call.
    """
    publisher, repository = _identity_publisher()
    async with publisher._candle_identity_gate:
        async with asyncio.timeout(1):
            assert (
                await publisher._correct_catchup_candle_rows([_native_identity_row(publisher)])
                == []
            )
    repository.correct_false_zero_candles.assert_awaited_once()
    assert repository.correct_false_zero_candles.await_args.kwargs == {"dry_run": True}


@pytest.mark.asyncio
async def test_correction_takes_the_gate_and_releases_the_corrected_identity() -> None:
    """A correction keeps the zero's ID and settles a reservation that carried it.

    Given: A correctable zero whose public ID is still a pending live reservation.
    When: The publisher correction step runs.
    Then: The repository corrects under the gate and the reservation is released.
    """
    publisher, repository = _identity_publisher()
    row = _native_identity_row(publisher)
    key = (row["instrument_public_id"], row["timeframe"], row["open_at"])
    publisher._pending_candle_identities[key] = "zero-id"
    corrected = [row | {"public_id": "zero-id"}]

    async def correct(
        rows: list[CandleUpsertRow], *, dry_run: bool = False
    ) -> list[CandleUpsertRow]:
        """Report the zero in preflight and correct it only under the gate."""
        if dry_run:
            assert not publisher._candle_identity_gate.locked()
            return list(rows)
        assert publisher._candle_identity_gate.locked()
        return corrected

    repository.correct_false_zero_candles.side_effect = correct
    assert await publisher._correct_catchup_candle_rows([row]) == corrected
    assert key not in publisher._pending_candle_identities
    assert not publisher._candle_identity_gate.locked()


@pytest.mark.asyncio
@pytest.mark.parametrize("phase", ["before_gate", "inside_gate"])
async def test_correction_defers_to_live_writes_before_and_inside_the_gate(phase: str) -> None:
    """A correction yields to queued live writes, also when they appear while it waits.

    Given: A correctable zero and a live write that is queued either before the
        correction asks for the gate or while it waits for the gate.
    When: The publisher correction step runs.
    Then: It defers without a write call and leaves the gate free.
    """
    publisher, repository = _identity_publisher()
    row = _native_identity_row(publisher)
    preflight_done = asyncio.Event()

    def correctable(rows: list[CandleUpsertRow], dry_run: bool = False) -> list[CandleUpsertRow]:
        """Report the zero as correctable and tell the test the preflight returned."""
        preflight_done.set()
        return list(rows)

    repository.correct_false_zero_candles.side_effect = correctable
    if phase == "before_gate":
        publisher._candle_write_queue.put_nowait(row)
        with pytest.raises(CatchupDeferredError, match="live writer"):
            await publisher._correct_catchup_candle_rows([row])
    else:
        async with publisher._candle_identity_gate:
            task = asyncio.create_task(publisher._correct_catchup_candle_rows([row]))
            await preflight_done.wait()
            publisher._candle_write_queue.put_nowait(row)
        with pytest.raises(CatchupDeferredError, match="live writer"):
            await task
    assert repository.correct_false_zero_candles.await_count == 1
    assert not publisher._candle_identity_gate.locked()


@pytest.mark.asyncio
@pytest.mark.parametrize("phase", ["pass", "sweep"])
async def test_native_scheduler_records_live_deferral_without_repair_success(phase: str) -> None:
    """An admission hold remains an explicit retryable deferral in either scheduling phase.

    Given: Native insertion yields to a busy live writer.
    When: A periodic pass or startup sweep visits that symbol.
    Then: No repair or successful insertion is reported and the bounded visit finishes.
    """
    fixture = _Fixture()
    worker = fixture.worker(CatchupPolicy(sweep_spacing_seconds=0.001))
    insert = AsyncMock(side_effect=CatchupDeferredError("live writer outstanding=1"))
    worker._callbacks = replace(worker._callbacks, insert_missing=insert)
    with patch("snapper.messaging.publishers.kraken_candle_catchup.logger.info") as logged:
        if phase == "sweep":
            with patch.object(worker, "run_pass", new=AsyncMock()):
                await worker.run_sweep()
        else:
            await worker.run_pass()
    insert.assert_awaited_once()
    fixture.committed.assert_not_awaited()
    assert "deferred" in logged.call_args.args[0]


@pytest.mark.asyncio
async def test_native_worker_acknowledges_all_bounded_chunks_once() -> None:
    """A complete response preserves every minute across independent bounded commits.

    Given: A response contains 201 valid minute observations.
    When: All independent native transactions acknowledge successfully.
    Then: Three bounded chunks preserve input order and trigger one grid-verified repair.
    """
    fixture = _Fixture()
    fixture.client.get_closed_1m_candles.return_value = [
        replace(_candle(), interval_begin=START - timedelta(minutes=offset))
        for offset in range(201)
    ]
    fixture.repository.insert_missing_candles.side_effect = lambda rows: rows
    await fixture.worker()._reconcile_symbol("BTC-USD", START, NOW)
    batches = [call.args[0] for call in fixture.repository.insert_missing_candles.await_args_list]
    assert list(map(len, batches)) == [100, 100, 1]
    minutes = [
        candle.interval_begin for candle in fixture.client.get_closed_1m_candles.return_value
    ]
    assert [row["open_at"] for batch in batches for row in batch] == minutes
    fixture.committed.assert_awaited_once_with("BTC-USD", START, NOW, frozenset(minutes))


@pytest.mark.asyncio
async def test_native_worker_commits_the_newest_chunk_first() -> None:
    """A deadline or a deferral leaves the oldest minutes, never the newest ones.

    Given: An oldest-first response of 201 minutes and a deferral on the second chunk.
    When: The worker reconciles the symbol.
    Then: The first commit holds the newest 100 minutes, newest first, and only
        those minutes reach the repair callback.
    """
    fixture = _Fixture()
    candles = [
        replace(_candle(), interval_begin=START - timedelta(minutes=offset))
        for offset in reversed(range(201))
    ]
    fixture.client.get_closed_1m_candles.return_value = candles
    batches: list[list[datetime]] = []

    async def insert(rows: list[CandleUpsertRow]) -> list[CandleUpsertRow]:
        """Acknowledge the first chunk and defer the second."""
        batches.append([row["open_at"] for row in rows])
        if len(batches) == 2:
            raise CatchupDeferredError("live writer outstanding=1")
        return rows

    fixture.repository.insert_missing_candles.side_effect = insert
    with pytest.raises(CatchupDeferredError):
        await fixture.worker()._reconcile_symbol("BTC-USD", START, NOW)
    newest = sorted((candle.interval_begin for candle in candles), reverse=True)
    assert batches[0] == newest[:100]
    assert batches[1] == newest[100:200]
    fixture.committed.assert_awaited_once_with("BTC-USD", START, NOW, frozenset(newest[:100]))


def _lock_timeout() -> DBAPIError:
    """Build the driver error PostgreSQL raises when the candle fence lock times out."""
    return DBAPIError("LOCK TABLE public.candles", {}, SimpleNamespace(sqlstate="55P03"))


@pytest.mark.asyncio
@pytest.mark.parametrize("step", ["insert", "correct"])
async def test_fence_lock_timeout_defers_the_catchup_write(step: str) -> None:
    """A catchup write that cannot take the candle fence in time yields like a deferral.

    Given: A missing minute, or a correctable zero, whose write hits the lock timeout.
    When: The publisher runs the insert or the correction step.
    Then: The step raises a deferral chained to the database error and frees the gate.
    """
    publisher, repository = _identity_publisher()
    row = _native_identity_row(publisher)
    if step == "insert":
        repository.insert_missing_candles.side_effect = _lock_timeout()
        operation = publisher._insert_catchup_candle_rows
    else:

        async def correct(
            rows: list[CandleUpsertRow], *, dry_run: bool = False
        ) -> list[CandleUpsertRow]:
            """Report the zero in preflight and time out on the fenced write."""
            if dry_run:
                return list(rows)
            raise _lock_timeout()

        repository.correct_false_zero_candles.side_effect = correct
        operation = publisher._correct_catchup_candle_rows
    with pytest.raises(CatchupDeferredError, match="lock timeout") as deferred:
        await operation([row])
    assert isinstance(deferred.value.__cause__, DBAPIError)
    assert not publisher._candle_identity_gate.locked()


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["other_sqlstate", "after_commit"])
async def test_other_database_errors_and_post_commit_lock_timeouts_stay_errors(
    failure: str,
) -> None:
    """Only a lock timeout of the write itself is a deferral.

    Given: A write failing with another SQLSTATE, or a completed insert that
        leaves a pending identity whose lookup afterwards hits the lock timeout.
    When: The publisher runs the insert step.
    Then: The original database error propagates unchanged.
    """
    publisher, repository = _identity_publisher()
    row = _native_identity_row(publisher)
    if failure == "other_sqlstate":
        repository.insert_missing_candles.side_effect = DBAPIError(
            "INSERT", {}, SimpleNamespace(sqlstate="40001")
        )
    else:
        key = (row["instrument_public_id"], row["timeframe"], row["open_at"])
        publisher._pending_candle_identities[key] = "pending-id"
        repository.insert_missing_candles.return_value = []
        repository.get_canonical_candle_public_id.side_effect = _lock_timeout()
    with pytest.raises(DBAPIError):
        await publisher._insert_catchup_candle_rows([row])
    assert not publisher._candle_identity_gate.locked()
