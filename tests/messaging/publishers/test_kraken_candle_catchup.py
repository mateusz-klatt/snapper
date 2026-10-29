"""Bounded startup/outage reconciliation preserves data and live-event boundaries."""

import asyncio
from dataclasses import replace
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from typing import cast
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest

from snapper.config.app import AppSettings
from snapper.core.types import ExchangeEnum
from snapper.data.repository import Repository
from snapper.data.repository_types import CandleRow
from snapper.data.repository_types import CandleUpsertRow
from snapper.infrastructure.exchanges.contracts import CandleUpdate
from snapper.infrastructure.exchanges.implementations.kraken import KrakenExchangeClient
from snapper.messaging.infrastructure.publisher import SequenceTracker
from snapper.messaging.publishers.base import _CandleRepairKey
from snapper.messaging.publishers.base import _PendingCandleRepair
from snapper.messaging.publishers.candle_aggregator import CandleAggregator
from snapper.messaging.publishers.kraken import KrakenMarketDataPublisher
from snapper.messaging.publishers.kraken_candle_catchup import CatchupCallbacks
from snapper.messaging.publishers.kraken_candle_catchup import CatchupPolicy
from snapper.messaging.publishers.kraken_candle_catchup import KrakenCandleCatchup
from snapper.messaging.publishers.kraken_candle_catchup import complete_repair_windows
from snapper.messaging.publishers.kraken_candle_catchup import repair_read_start

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
        self.repository.insert_missing_candles = AsyncMock(return_value=[])
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
        ("symbols_per_pass", -1),
        ("symbols_per_pass", 1),
        ("attempt_seconds", float("inf")),
        ("pass_seconds", float("nan")),
        ("minimum_spacing_seconds", 61.0),
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

    async def repair(symbol: str, start: datetime, end: datetime) -> None:
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
        "BTC-USD", NOW.replace(hour=19, second=0), NOW.replace(second=0)
    )
    assert observations == ["committed", "repair"]


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
    Then: Every symbol is visited once and successful symbols reconcile.
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
    assert fixture.committed.await_count == 3


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
    """Later reconciliation discovers an uncertain committed insertion.

    Given: Durable insertion succeeded but its acknowledgement was lost.
    When: The next independent pass finds the existing key.
    Then: No row is rewritten and committed repair reconciliation resumes.
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
    fixture.committed.assert_awaited_once()


@pytest.mark.asyncio
async def test_supervisor_coalesces_requests_and_survives_a_failed_pass() -> None:
    """Reconnect requests stay paced and failures do not kill catchup.

    Given: Repeated reconnect notifications and one failed bounded pass.
    When: The supervisor runs until the publisher stops.
    Then: Requests coalesce and the following pass still executes.
    """
    fixture = _Fixture()
    worker = fixture.worker(CatchupPolicy(interval_seconds=0.003, minimum_spacing_seconds=0.001))
    calls = 0

    async def run_pass() -> None:
        nonlocal calls
        calls += 1
        worker.request()
        worker.request()
        if calls == 1:
            raise RuntimeError("temporary bounded pass failure")
        fixture.running = False

    with patch.object(worker, "run_pass", side_effect=run_pass):
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
        ("volume", -1.0),
        ("low", 0.0),
        ("open", 1.0),
        ("high", 1.0),
        ("trades", None),
        ("trades", -1),
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


def _publisher() -> KrakenMarketDataPublisher:
    """Create a stopped publisher with deterministic settings and source boundaries."""
    publisher = KrakenMarketDataPublisher(["BTC-USD"])
    settings = MagicMock(spec=AppSettings)
    settings.timeframes = ["1m", "5m"]
    settings.spot_candle_source = "trade_built"
    settings.candle_minute_completion = False
    settings.trade_built_finalize_grace_seconds = 12
    publisher.settings = settings
    return publisher


def test_catchup_roster_is_persisted_subscribed_and_fail_closed() -> None:
    """Catchup cannot expand unresolved or excluded persistence policy.

    Given: Wildcard persistence, an excluded pair and an explicit subscription.
    When: Catchup resolves its permitted roster.
    Then: Only subscribed persisted Spot instruments are returned.
    """
    publisher = _publisher()
    assert publisher._catchup_symbols(["*"]) == []
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
    await publisher._reconcile_catchup_repairs("BTC-USD", START, START.replace(minute=30))
    publisher._candle_aggregator = CandleAggregator(["5m"])
    repository = MagicMock(spec=Repository)
    repository.get_candles = AsyncMock(return_value=[_row(i) for i in range(25, 30)])
    publisher.repository = repository
    await publisher._reconcile_catchup_repairs("BTC-USD", START, START.replace(minute=30))
    key = _CandleRepairKey("BTC-USD", "5m", START)
    assert publisher._pending_candle_repairs[key].historical_only is True
    assert publisher._pending_candle_repairs[key].expected_source_minutes == frozenset(
        START.replace(minute=i) for i in range(25, 30)
    )
    publisher.settings.timeframes = ["1m"]
    repository.get_candles.reset_mock()
    await publisher._reconcile_catchup_repairs("BTC-USD", START, START.replace(minute=30))
    repository.get_candles.assert_not_awaited()


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
            publisher._pending_candle_repairs[key] = _PendingCandleRepair(
                expected_source_minutes=minutes, historical_only=True
            )
        else:
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
    """A large wildcard universe cannot hide the three default dashboard pairs.

    Given: Fifteen hundred other persisted symbols and the default priority trio.
    When: Three fixed-budget passes run.
    Then: Defaults run immediately and independent fair work advances every later pass.
    """
    fixture = _Fixture()
    fixture.symbols = [f"ALT{i:04}-USD" for i in range(1500)] + ["BTC-USD", "ETH-USD", "EUR-USD"]
    fixture.client.get_closed_1m_candles.return_value = []
    worker = fixture.worker()
    await worker.run_pass()
    await worker.run_pass()
    await worker.run_pass()
    visited = [call.args[0] for call in fixture.client.get_closed_1m_candles.call_args_list]
    assert visited == [
        "BTC-USD",
        "ETH-USD",
        "EUR-USD",
        "ALT0000-USD",
        "BTC-USD",
        "ETH-USD",
        "ALT0001-USD",
        "EUR-USD",
        "BTC-USD",
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
    assert visited == ["BTC-USD", "A", "B", "ETH-USD", "EUR-USD", "C", "BTC-USD", "ETH-USD"]


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
        publisher._pending_candle_repairs[key] = pending
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
    publisher._pending_candle_repairs[key] = _PendingCandleRepair(
        expected_source_minutes=minutes, historical_only=True
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
