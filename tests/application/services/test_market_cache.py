"""Unit tests for :class:`snapper.application.services.market_cache.MarketCacheService`.

Coverage targets: closed-bar dedup state machine, timeframe filter,
exchange filter, prewarm path, stale-key prune, listener lifecycle,
recv decode, ingest loop end-to-end against a fake subscriber.

The dedup state machine drives this service: tests for cold start
(no current slot), upsert (same open_at), promote (newer open_at →
prior current closed by construction), and drop (older open_at)
each have their own assertion.
"""

import asyncio
import contextlib
from collections.abc import Sequence
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from typing import Any
from typing import cast
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import pytest

from snapper.application.services.market_cache import _PREWARM_INSTRUMENT_SLICE
from snapper.application.services.market_cache import _PREWARM_MAX_CONCURRENCY
from snapper.application.services.market_cache import _PREWARM_SLICE_FAILURE_BUDGET
from snapper.application.services.market_cache import MarketCacheService
from snapper.application.services.market_cache import PairStats
from snapper.application.services.market_cache import _format_stale_age
from snapper.application.services.market_cache import _snap_from_candle_data
from snapper.core.types import ExchangeEnum
from snapper.data.repository_types import CandleRow
from snapper.messaging.schemas.data import CandleData


def _candle(
    *,
    instrument: str = "BTC-USD",
    exchange: str = ExchangeEnum.KRAKEN,
    open_at: datetime,
    timeframe: str = "1m",
    open_: float = 1.0,
    high: float = 1.5,
    low: float = 0.9,
    close: float = 1.2,
    volume: float = 100.0,
) -> CandleData:
    """Build a minimal valid :class:`CandleData` for the dispatcher tests."""
    return CandleData(
        public_id="00000000-0000-7000-8000-000000000001",
        timestamp=open_at,
        session_id="sess-test",
        sequence_id=1,
        instrument=instrument,
        exchange=cast(Any, exchange),
        timeframe=timeframe,
        open_at=open_at,
        open=open_,
        high=high,
        low=low,
        close=close,
        volume=volume,
    )


def _row(
    *,
    open_at: datetime,
    open_: float = 1.0,
    close: float = 1.1,
) -> CandleRow:
    """Build a stub :class:`CandleRow` for the prewarm path."""
    return CandleRow(
        open_at=open_at,
        timeframe="1m",
        open=open_,
        high=open_ + 0.1,
        low=open_ - 0.1,
        close=close,
        volume=50.0,
        vwap=None,
        trades=None,
        source="native",
        complete=True,
        public_id="00000000-0000-7000-8000-0000000000aa",
        timestamp=open_at,
        session_id="seed",
        sequence_id=1,
    )


def _pid(native_symbol: str) -> str:
    """Return the fake instrument public ID the test resolver assigns."""
    return f"pid-{native_symbol}"


def _build_service(
    *,
    persist_pairs: list[tuple[Any, str]] | None = None,
    candles_by_instrument: dict[str, list[CandleRow]] | None = None,
    instruments_by_exchange: dict[str, list[str]] | None = None,
    unresolvable_symbols: set[str] | None = None,
) -> tuple[MarketCacheService, MagicMock, MagicMock]:
    """Build a :class:`MarketCacheService` against AsyncMock repo + stub policy.

    The repository fakes model the batched prewarm path: symbols resolve
    to ``pid-<symbol>`` unless listed in ``unresolvable_symbols`` (the
    alias-only case), and the batch candle reader honours
    ``open_at_floor`` so the two-pass floor logic is exercised for real
    rather than stubbed away.
    """
    repo = MagicMock()
    candles_by_instrument = candles_by_instrument or {}
    instruments_by_exchange = instruments_by_exchange or {}
    unresolvable_symbols = unresolvable_symbols or set()

    async def _fake_get_candles(*, instrument: str, **kwargs: Any) -> list[CandleRow]:
        del kwargs
        return candles_by_instrument.get(instrument, [])

    async def _fake_get_exchange_instruments(*, exchange: str, **kwargs: Any) -> list[str]:
        del kwargs
        return list(instruments_by_exchange.get(exchange, []))

    async def _fake_resolve(
        native_symbols: set[str], exchange: str, as_of: datetime
    ) -> dict[str, str]:
        del exchange, as_of
        return {
            symbol: _pid(symbol) for symbol in native_symbols if symbol not in unresolvable_symbols
        }

    async def _fake_batch_candles(
        instrument_public_ids: Sequence[str],
        timeframe: str,
        as_of: datetime,
        limit_per_instrument: int,
        open_at_floor: datetime | None = None,
    ) -> dict[str, list[CandleRow]]:
        del timeframe, as_of
        batch: dict[str, list[CandleRow]] = {}
        for instrument_public_id in instrument_public_ids:
            rows = candles_by_instrument.get(instrument_public_id.removeprefix("pid-"), [])
            if open_at_floor is not None:
                rows = [row for row in rows if row["open_at"] >= open_at_floor]
            rows = rows[:limit_per_instrument]
            if rows:
                batch[instrument_public_id] = rows
        return batch

    repo.get_candles = AsyncMock(side_effect=_fake_get_candles)
    repo.get_exchange_instruments = AsyncMock(side_effect=_fake_get_exchange_instruments)
    repo.get_instrument_public_ids_by_symbols = AsyncMock(side_effect=_fake_resolve)
    repo.get_latest_candles_for_instruments = AsyncMock(side_effect=_fake_batch_candles)
    policy = MagicMock()
    policy.iter_persisted_instruments.return_value = iter(persist_pairs or [])
    service = MarketCacheService(
        repository=cast(Any, repo),
        persist_policy=cast(Any, policy),
    )
    return service, repo, policy


class TestDedupStateMachine:
    """Closed-bar dedup state machine drives the cache."""

    @pytest.mark.asyncio
    async def test_cold_start_seeds_current_slot(self) -> None:
        """First frame populates ``_current``; deque stays empty."""
        service, _, _ = _build_service()
        candle = _candle(open_at=datetime(2026, 5, 13, 10, 0, tzinfo=UTC))

        await service.record_candle_for_test(candle)

        result = await service.get_1m_candles(ExchangeEnum.KRAKEN, "BTC-USD", limit=10)
        assert result == []
        assert (ExchangeEnum.KRAKEN, "BTC-USD") in service._current

    @pytest.mark.asyncio
    async def test_same_open_at_upserts_current_slot(self) -> None:
        """Second frame with same open_at replaces the current slot in place."""
        service, _, _ = _build_service()
        open_at = datetime(2026, 5, 13, 10, 0, tzinfo=UTC)
        first = _candle(open_at=open_at, close=1.5)
        second = _candle(open_at=open_at, close=2.0)

        await service.record_candle_for_test(first)
        await service.record_candle_for_test(second)

        result = await service.get_1m_candles(ExchangeEnum.KRAKEN, "BTC-USD", limit=10)
        assert result == []
        snap = service._current[(ExchangeEnum.KRAKEN, "BTC-USD")]
        assert snap.close == 2.0

    @pytest.mark.asyncio
    async def test_newer_open_at_promotes_prior_current_to_deque(self) -> None:
        """Newer open_at promotes the prior current to the deque (now closed)."""
        service, _, _ = _build_service()
        first_open = datetime(2026, 5, 13, 10, 0, tzinfo=UTC)
        second_open = first_open + timedelta(minutes=1)
        first = _candle(open_at=first_open, close=1.5)
        second = _candle(open_at=second_open, close=1.6)

        await service.record_candle_for_test(first)
        await service.record_candle_for_test(second)

        result = await service.get_1m_candles(ExchangeEnum.KRAKEN, "BTC-USD", limit=10)
        assert len(result) == 1
        assert result[0].close == 1.5
        assert service._current[(ExchangeEnum.KRAKEN, "BTC-USD")].close == 1.6

    @pytest.mark.asyncio
    async def test_older_open_at_is_dropped_as_stale(self) -> None:
        """An out-of-order older frame is silently dropped."""
        service, _, _ = _build_service()
        newer = _candle(open_at=datetime(2026, 5, 13, 10, 1, tzinfo=UTC), close=1.5)
        older = _candle(open_at=datetime(2026, 5, 13, 10, 0, tzinfo=UTC), close=99.0)

        await service.record_candle_for_test(newer)
        await service.record_candle_for_test(older)

        assert service._current[(ExchangeEnum.KRAKEN, "BTC-USD")].close == 1.5
        result = await service.get_1m_candles(ExchangeEnum.KRAKEN, "BTC-USD", limit=10)
        assert result == []


class TestFilters:
    """Cache rejects non-1m timeframes + unknown exchanges before dispatch."""

    @pytest.mark.asyncio
    async def test_non_one_minute_timeframe_is_dropped(self) -> None:
        """A 5m frame must not pollute the 1m deque."""
        service, _, _ = _build_service()
        five_m = _candle(open_at=datetime(2026, 5, 13, 10, 0, tzinfo=UTC), timeframe="5m")

        await service.record_candle_for_test(five_m)

        assert (ExchangeEnum.KRAKEN, "BTC-USD") not in service._current


class TestReadAccessor:
    """``get_1m_candles`` returns chronological slice limited by ``limit``."""

    @pytest.mark.asyncio
    async def test_limit_caps_result_to_most_recent(self) -> None:
        """When the deque has more than ``limit`` bars, return the latest slice."""
        service, _, _ = _build_service()
        base = datetime(2026, 5, 13, 10, 0, tzinfo=UTC)
        for minute in range(5):
            await service.record_candle_for_test(
                _candle(open_at=base + timedelta(minutes=minute), close=float(minute))
            )
        await service.record_candle_for_test(
            _candle(open_at=base + timedelta(minutes=5), close=99.0)
        )

        result = await service.get_1m_candles(ExchangeEnum.KRAKEN, "BTC-USD", limit=2)
        assert [s.close for s in result] == [3.0, 4.0]

    @pytest.mark.asyncio
    async def test_unseen_instrument_returns_empty(self) -> None:
        """An instrument that never produced a frame returns an empty list."""
        service, _, _ = _build_service()
        result = await service.get_1m_candles(ExchangeEnum.KRAKEN, "NEVER-SEEN", limit=10)
        assert result == []

    @pytest.mark.asyncio
    async def test_limit_greater_than_deque_returns_all(self) -> None:
        """``limit >= len`` returns the full deque."""
        service, _, _ = _build_service()
        base = datetime(2026, 5, 13, 10, 0, tzinfo=UTC)
        for minute in range(3):
            await service.record_candle_for_test(
                _candle(open_at=base + timedelta(minutes=minute), close=float(minute))
            )
        await service.record_candle_for_test(
            _candle(open_at=base + timedelta(minutes=3), close=99.0)
        )

        result = await service.get_1m_candles(ExchangeEnum.KRAKEN, "BTC-USD", limit=100)
        assert [s.close for s in result] == [0.0, 1.0, 2.0]


class TestPrewarm:
    """Prewarm seeds the cache from the persisted set + DB."""

    @pytest.mark.asyncio
    async def test_prewarm_pushes_rows_into_deque(self) -> None:
        """Repository rows land on the deque in chronological order."""
        base = datetime(2026, 5, 13, 9, 0, tzinfo=UTC)
        rows = [
            _row(open_at=base + timedelta(minutes=2), close=3.0),
            _row(open_at=base + timedelta(minutes=1), close=2.0),
            _row(open_at=base, close=1.0),
        ]
        service, repo, _ = _build_service(
            persist_pairs=[(ExchangeEnum.KRAKEN, "BTC-USD")],
            candles_by_instrument={"BTC-USD": rows},
        )

        await service._prewarm()

        result = await service.get_1m_candles(ExchangeEnum.KRAKEN, "BTC-USD", limit=100)
        assert [s.close for s in result] == [1.0, 2.0, 3.0]
        repo.get_candles.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_prewarm_empty_result_skips_instrument(self) -> None:
        """An instrument with zero DB rows produces no deque entry."""
        service, _, _ = _build_service(
            persist_pairs=[(ExchangeEnum.KRAKEN, "EMPTY")],
            candles_by_instrument={"EMPTY": []},
        )

        await service._prewarm()

        result = await service.get_1m_candles(ExchangeEnum.KRAKEN, "EMPTY", limit=10)
        assert result == []

    @pytest.mark.asyncio
    async def test_prewarm_slice_failure_falls_back_per_instrument(self) -> None:
        """A slice-level raise degrades to the per-instrument path.

        Given: A batch reader that raises whenever a bad instrument is in
            the slice but succeeds for a single-instrument read of the
            good one,
        When: prewarm runs,
        Then: The bad symbol stays cold, the good symbol still warms, and
            the failure never escapes ``_prewarm``.
        """
        row = _row(open_at=datetime(2026, 5, 13, 10, 0, tzinfo=UTC), close=5.0)

        async def _fail_on_bad(
            instrument_public_ids: Sequence[str],
            timeframe: str,
            as_of: datetime,
            limit_per_instrument: int,
            open_at_floor: datetime | None = None,
        ) -> dict[str, list[CandleRow]]:
            del timeframe, as_of, limit_per_instrument, open_at_floor
            if _pid("BAD") in instrument_public_ids:
                raise RuntimeError("DB hiccup")
            return {_pid("GOOD"): [row]}

        service, repo, _ = _build_service(
            persist_pairs=[
                (ExchangeEnum.KRAKEN, "BAD"),
                (ExchangeEnum.KRAKEN, "GOOD"),
            ],
        )
        repo.get_latest_candles_for_instruments = AsyncMock(side_effect=_fail_on_bad)

        await service._prewarm()

        bad = await service.get_1m_candles(ExchangeEnum.KRAKEN, "BAD", limit=10)
        good = await service.get_1m_candles(ExchangeEnum.KRAKEN, "GOOD", limit=10)
        assert bad == []
        assert len(good) == 1

    @pytest.mark.asyncio
    async def test_prewarm_never_raises_out_of_start(self) -> None:
        """Every prewarm failure path is contained inside ``start``.

        Given: A repository whose resolve raises for one exchange while
            the other resolves normally, and whose batch candle read and
            per-instrument fallback then both raise,
        When: ``start`` runs with no broker address,
        Then: It returns normally and still spawns the prune loop, so a
            prewarm fault degrades the cache instead of aborting the
            application lifespan.

        The resolve failure is scoped to ONE exchange deliberately: if it
        raised for every exchange no target would survive, the batch
        reader would never be awaited, and this test would silently stop
        exercising the candle-read failure path it exists to cover.
        """

        async def _resolve(
            native_symbols: set[str], exchange: str, as_of: datetime
        ) -> dict[str, str]:
            del as_of
            if exchange == ExchangeEnum.KRAKEN:
                raise RuntimeError("resolve exploded")
            return {symbol: _pid(symbol) for symbol in native_symbols}

        service, repo, _ = _build_service(
            persist_pairs=[
                (ExchangeEnum.KRAKEN, "BTC-USD"),
                (ExchangeEnum.WALUTOMAT, "EUR-PLN"),
            ],
        )
        repo.get_instrument_public_ids_by_symbols = AsyncMock(side_effect=_resolve)
        repo.get_latest_candles_for_instruments = AsyncMock(
            side_effect=RuntimeError("batch exploded")
        )

        await service.start("")

        assert repo.get_latest_candles_for_instruments.await_count > 0
        assert service._prune_task is not None
        await service.stop()

    @pytest.mark.asyncio
    async def test_prewarm_unresolved_symbol_never_queries_candles(self) -> None:
        """Alias-only symbols are dropped before any candle read.

        Given: Two persisted symbols of which one has no active instrument,
        When: prewarm runs,
        Then: The unresolved symbol's public ID never reaches the batch
            candle reader and it warms nothing.
        """
        row = _row(open_at=datetime(2026, 5, 13, 10, 0, tzinfo=UTC), close=7.0)
        service, repo, _ = _build_service(
            persist_pairs=[
                (ExchangeEnum.KRAKEN, "REAL"),
                (ExchangeEnum.KRAKEN, "ALIAS-ONLY"),
            ],
            candles_by_instrument={"REAL": [row], "ALIAS-ONLY": [row]},
            unresolvable_symbols={"ALIAS-ONLY"},
        )

        await service._prewarm()

        queried = {
            instrument_public_id
            for call in repo.get_latest_candles_for_instruments.await_args_list
            for instrument_public_id in call.args[0]
        }
        assert _pid("ALIAS-ONLY") not in queried
        assert await service.get_1m_candles(ExchangeEnum.KRAKEN, "ALIAS-ONLY", limit=10) == []
        assert len(await service.get_1m_candles(ExchangeEnum.KRAKEN, "REAL", limit=10)) == 1

    @pytest.mark.asyncio
    async def test_prewarm_issues_one_resolve_query_per_exchange(self) -> None:
        """The batched path is O(1) per exchange, not O(n) per instrument.

        Given: Two exchanges each wildcard-expanding to 80 symbols,
        When: prewarm runs,
        Then: Resolution costs one call per exchange, the per-instrument
            ``get_candles`` path is never used, and every batch candle
            read carries more than one instrument. This is the guard that
            would have caught the 2948-sequential-query startup stall.
        """
        base = datetime(2026, 5, 13, 10, 0, tzinfo=UTC)
        kraken = [f"K{index}" for index in range(80)]
        walutomat = [f"W{index}" for index in range(80)]
        service, repo, _ = _build_service(
            persist_pairs=[
                (ExchangeEnum.KRAKEN, "*"),
                (ExchangeEnum.WALUTOMAT, "*"),
            ],
            instruments_by_exchange={
                ExchangeEnum.KRAKEN: kraken,
                ExchangeEnum.WALUTOMAT: walutomat,
            },
            candles_by_instrument={
                symbol: [_row(open_at=base, close=1.0)] for symbol in kraken + walutomat
            },
        )

        await service._prewarm()

        assert repo.get_exchange_instruments.await_count == 2
        assert repo.get_instrument_public_ids_by_symbols.await_count == 2
        repo.get_candles.assert_not_awaited()
        assert all(
            len(call.args[0]) > 1
            for call in repo.get_latest_candles_for_instruments.await_args_list
        )

    @pytest.mark.asyncio
    async def test_prewarm_second_pass_is_unfloored_and_preserves_coverage(self) -> None:
        """An instrument outside the floor window is still warmed.

        Given: A symbol whose only rows predate the first pass's
            ``open_at`` floor,
        When: prewarm runs,
        Then: The floored pass returns nothing, a second pass runs with no
            floor, and the rows land in the deque — the floor is a pure
            optimisation and can never shrink cache coverage.
        """
        stale = datetime(2026, 5, 13, 10, 0, tzinfo=UTC)
        service, repo, _ = _build_service(
            persist_pairs=[(ExchangeEnum.KRAKEN, "OLD")],
            candles_by_instrument={"OLD": [_row(open_at=stale, close=9.0)]},
        )

        await service._prewarm()

        calls = repo.get_latest_candles_for_instruments.await_args_list
        assert len(calls) == 2
        assert calls[0].args[4] is not None
        assert calls[1].args[4] is None
        warmed = await service.get_1m_candles(ExchangeEnum.KRAKEN, "OLD", limit=10)
        assert [s.close for s in warmed] == [9.0]

    @pytest.mark.asyncio
    async def test_prewarm_failing_second_pass_keeps_first_pass_rows_and_status(self) -> None:
        """A failing unfloored pass must not disown rows the floored pass installed.

        Given: An instrument the floored pass warms with 40 bars, whose
            unfloored re-read then raises in both the slice and the
            per-instrument fallback,
        When: prewarm runs,
        Then: The deque still holds the 40 bars and the instrument is
            tallied as warmed, not failed — the completion log must
            describe the cache's actual contents.
        """
        recent = datetime.now(UTC)
        rows = [_row(open_at=recent - timedelta(minutes=index)) for index in range(40)]

        async def _floored_only(
            instrument_public_ids: Sequence[str],
            timeframe: str,
            as_of: datetime,
            limit_per_instrument: int,
            open_at_floor: datetime | None = None,
        ) -> dict[str, list[CandleRow]]:
            del timeframe, as_of, limit_per_instrument
            if open_at_floor is None:
                raise RuntimeError("statement timeout on the unfloored read")
            return dict.fromkeys(instrument_public_ids, rows)

        service, repo, _ = _build_service(persist_pairs=[(ExchangeEnum.KRAKEN, "PART")])
        repo.get_latest_candles_for_instruments = AsyncMock(side_effect=_floored_only)

        await service._prewarm()

        cached = await service.get_1m_candles(ExchangeEnum.KRAKEN, "PART", limit=100)
        assert len(cached) == 40
        assert await service.instruments_cached() == 1

    @pytest.mark.asyncio
    async def test_prewarm_skips_second_pass_when_first_fills_every_deque(self) -> None:
        """A fully-warmed first pass short-circuits the unfloored re-read."""
        recent = datetime.now(UTC)
        rows = [_row(open_at=recent - timedelta(minutes=index)) for index in range(100)]
        service, repo, _ = _build_service(
            persist_pairs=[(ExchangeEnum.KRAKEN, "FULL")],
            candles_by_instrument={"FULL": rows},
        )

        await service._prewarm()

        assert repo.get_latest_candles_for_instruments.await_count == 1

    @pytest.mark.asyncio
    async def test_prewarm_stops_falling_back_when_failures_look_systemic(self) -> None:
        """A dead database must not multiply into thousands of retries.

        Given: 400 resolved instruments and a batch reader that raises
            for every call, as an unreachable database would,
        When: prewarm runs,
        Then: The per-instrument fallback stops once the slice-failure
            budget is spent, so total reads stay bounded instead of
            degenerating into one timing-out read per instrument per
            pass — which would block the port far longer than the
            sequential startup this change replaces.
        """
        symbols = [f"D{index}" for index in range(400)]
        service, repo, _ = _build_service(
            persist_pairs=[(ExchangeEnum.KRAKEN, "*")],
            instruments_by_exchange={ExchangeEnum.KRAKEN: symbols},
        )
        repo.get_latest_candles_for_instruments = AsyncMock(
            side_effect=RuntimeError("connection refused")
        )

        await service._prewarm()

        slices = -(-len(symbols) // _PREWARM_INSTRUMENT_SLICE)
        ceiling = slices + _PREWARM_SLICE_FAILURE_BUDGET * _PREWARM_INSTRUMENT_SLICE
        assert repo.get_latest_candles_for_instruments.await_count <= ceiling
        assert repo.get_latest_candles_for_instruments.await_count < len(symbols)

    @pytest.mark.asyncio
    async def test_prewarm_bounds_slice_concurrency(self) -> None:
        """Concurrent slices never exceed the configured pool-safe cap."""
        base = datetime(2026, 5, 13, 10, 0, tzinfo=UTC)
        symbols = [f"S{index}" for index in range(400)]
        in_flight = 0
        peak = 0

        async def _tracking_batch(
            instrument_public_ids: Sequence[str],
            timeframe: str,
            as_of: datetime,
            limit_per_instrument: int,
            open_at_floor: datetime | None = None,
        ) -> dict[str, list[CandleRow]]:
            del timeframe, as_of, limit_per_instrument, open_at_floor
            nonlocal in_flight, peak
            in_flight += 1
            peak = max(peak, in_flight)
            await asyncio.sleep(0)
            in_flight -= 1
            return {pid: [_row(open_at=base)] for pid in instrument_public_ids}

        service, repo, _ = _build_service(
            persist_pairs=[(ExchangeEnum.KRAKEN, "*")],
            instruments_by_exchange={ExchangeEnum.KRAKEN: symbols},
        )
        repo.get_latest_candles_for_instruments = AsyncMock(side_effect=_tracking_batch)

        await service._prewarm()

        assert peak <= _PREWARM_MAX_CONCURRENCY

    @pytest.mark.asyncio
    async def test_prewarm_dedupes_wildcard_and_explicit_symbol(self) -> None:
        """A symbol configured both explicitly and via `*` is read once per pass.

        Without the dedupe set the two persist entries would become two
        concurrent identical reads of the same instrument inside a single
        pass, which under fan-out is wasted pool capacity rather than the
        harmless sequential repeat it used to be.
        """
        base = datetime(2026, 5, 13, 10, 0, tzinfo=UTC)
        service, repo, _ = _build_service(
            persist_pairs=[
                (ExchangeEnum.KRAKEN, "*"),
                (ExchangeEnum.KRAKEN, "BTC-USD"),
            ],
            instruments_by_exchange={ExchangeEnum.KRAKEN: ["BTC-USD"]},
            candles_by_instrument={"BTC-USD": [_row(open_at=base, close=4.0)]},
        )

        await service._prewarm()

        first_pass = list(repo.get_latest_candles_for_instruments.await_args_list[0].args[0])
        assert first_pass.count(_pid("BTC-USD")) == 1

    @pytest.mark.asyncio
    async def test_prewarm_expands_wildcard_via_exchange_instruments(self) -> None:
        """A `*` persist entry resolves to per-exchange symbols + warms each.

        Given: A persist policy yielding ``(KRAKEN, "*")``,
        When: prewarm runs against a repo that lists BTC-USD + ETH-USD on KRAKEN,
        Then: Both symbols are warmed and ``get_exchange_instruments`` is consulted.
        """
        base = datetime(2026, 5, 13, 10, 0, tzinfo=UTC)
        rows = [_row(open_at=base, close=10.0)]
        service, repo, _ = _build_service(
            persist_pairs=[(ExchangeEnum.KRAKEN, "*")],
            instruments_by_exchange={ExchangeEnum.KRAKEN: ["BTC-USD", "ETH-USD"]},
            candles_by_instrument={"BTC-USD": rows, "ETH-USD": rows},
        )

        await service._prewarm()

        repo.get_exchange_instruments.assert_awaited_once()
        btc = await service.get_1m_candles(ExchangeEnum.KRAKEN, "BTC-USD", limit=10)
        eth = await service.get_1m_candles(ExchangeEnum.KRAKEN, "ETH-USD", limit=10)
        assert [s.close for s in btc] == [10.0]
        assert [s.close for s in eth] == [10.0]

    @pytest.mark.asyncio
    async def test_prewarm_instrument_resolve_failure_skips_exchange(self) -> None:
        """A resolve DB error skips that exchange and still warms the others."""
        base = datetime(2026, 5, 13, 10, 0, tzinfo=UTC)

        async def _resolve(
            native_symbols: set[str], exchange: str, as_of: datetime
        ) -> dict[str, str]:
            del as_of
            if exchange == ExchangeEnum.KRAKEN:
                raise RuntimeError("resolve failed")
            return {symbol: _pid(symbol) for symbol in native_symbols}

        service, repo, _ = _build_service(
            persist_pairs=[
                (ExchangeEnum.KRAKEN, "BTC-USD"),
                (ExchangeEnum.WALUTOMAT, "EUR-PLN"),
            ],
            candles_by_instrument={"EUR-PLN": [_row(open_at=base, close=30.0)]},
        )
        repo.get_instrument_public_ids_by_symbols = AsyncMock(side_effect=_resolve)

        await service._prewarm()

        assert await service.get_1m_candles(ExchangeEnum.KRAKEN, "BTC-USD", limit=10) == []
        walutomat = await service.get_1m_candles(ExchangeEnum.WALUTOMAT, "EUR-PLN", limit=10)
        assert [s.close for s in walutomat] == [30.0]

    @pytest.mark.asyncio
    async def test_prewarm_wildcard_expansion_failure_skips_exchange(self) -> None:
        """A wildcard expansion DB error is logged + skipped, others still warm.

        Given: A persist policy yielding wildcards for two exchanges,
        When: ``get_exchange_instruments`` raises for the first,
        Then: The first exchange is skipped and the second still resolves.
        """
        base = datetime(2026, 5, 13, 10, 0, tzinfo=UTC)
        rows = [_row(open_at=base, close=20.0)]

        async def _expand(*, exchange: str, **kwargs: Any) -> list[str]:
            del kwargs
            if exchange == ExchangeEnum.KRAKEN:
                raise RuntimeError("symbols query failed")
            return ["FOO-BAR"]

        service, repo, _ = _build_service(
            persist_pairs=[
                (ExchangeEnum.KRAKEN, "*"),
                (ExchangeEnum.WALUTOMAT, "*"),
            ],
            candles_by_instrument={"FOO-BAR": rows},
        )
        repo.get_exchange_instruments = AsyncMock(side_effect=_expand)

        await service._prewarm()

        kraken_btc = await service.get_1m_candles(ExchangeEnum.KRAKEN, "BTC-USD", limit=10)
        walutomat = await service.get_1m_candles(ExchangeEnum.WALUTOMAT, "FOO-BAR", limit=10)
        assert kraken_btc == []
        assert [s.close for s in walutomat] == [20.0]


class TestPrune:
    """Stale-key prune drops idle instruments after the cutoff."""

    @pytest.mark.asyncio
    async def test_prune_drops_stale_keys(self) -> None:
        """A key with ``last_seen_at`` older than the cutoff is removed."""
        service, _, _ = _build_service()
        await service.record_candle_for_test(
            _candle(open_at=datetime(2026, 5, 13, 10, 0, tzinfo=UTC))
        )
        key = (ExchangeEnum.KRAKEN, "BTC-USD")
        service.force_stale_for_test(key, age_s=service.staleness_window_seconds() + 60)

        await service._prune_once()

        assert key not in service._current
        assert key not in service._candles
        assert key not in service.last_seen_for_test

    @pytest.mark.asyncio
    async def test_prune_leaves_fresh_keys_untouched(self) -> None:
        """A key seen within the cutoff stays in the cache."""
        service, _, _ = _build_service()
        await service.record_candle_for_test(
            _candle(open_at=datetime(2026, 5, 13, 10, 0, tzinfo=UTC))
        )

        await service._prune_once()

        assert (ExchangeEnum.KRAKEN, "BTC-USD") in service._current


class TestTopicAndPayloadGates:
    """Cheap gates run BEFORE Pydantic validation to skip non-candle frames."""

    @pytest.mark.parametrize(
        ("topic", "expected"),
        [
            ("market.kraken.BTC-USD.candles.1m", True),
            ("market.kraken.BTC-USD.candles.5m", False),
            ("market.kraken.BTC-USD.ticks", False),
            ("market.kraken.BTC-USD.trades", False),
        ],
    )
    def test_should_ingest_topic_suffix_gate(self, topic: str, expected: bool) -> None:
        """Only ``.candles.1m`` suffixes pass the cheap gate."""
        assert MarketCacheService._should_ingest_topic(topic) is expected

    def test_parse_candle_returns_none_on_bad_payload(self) -> None:
        """Pydantic validation failure logs + returns None."""
        result = MarketCacheService._parse_candle(b"not-a-json")
        assert result is None

    def test_parse_candle_returns_object_on_valid_payload(self) -> None:
        """A valid JSON payload parses to :class:`CandleData`."""
        candle = _candle(open_at=datetime(2026, 5, 13, 10, 0, tzinfo=UTC))
        result = MarketCacheService._parse_candle(candle.model_dump_json().encode())
        assert result is not None
        assert result.instrument == "BTC-USD"


class TestRecvFrame:
    """``_recv_one_frame`` decodes bytes / str / returns None on failure."""

    @pytest.mark.asyncio
    async def test_recv_one_frame_decodes_bytes_topic_and_payload(self) -> None:
        """Bytes topic decodes via UTF-8 + bytes payload passes through."""
        service, _, _ = _build_service()
        subscriber = MagicMock()
        subscriber.recv_multipart = AsyncMock(
            return_value=(b"market.kraken.BTC-USD.candles.1m", b'{"x": 1}')
        )

        result = await service._recv_one_frame(subscriber)
        assert result == ("market.kraken.BTC-USD.candles.1m", b'{"x": 1}')

    @pytest.mark.asyncio
    async def test_recv_one_frame_accepts_str_topic_and_str_payload(self) -> None:
        """Non-bytes inputs are coerced via str / encode for forward compat."""
        service, _, _ = _build_service()
        subscriber = MagicMock()
        subscriber.recv_multipart = AsyncMock(return_value=("topic-str", 42))

        result = await service._recv_one_frame(subscriber)
        assert result == ("topic-str", b"42")

    @pytest.mark.asyncio
    async def test_recv_one_frame_returns_none_on_failure(self) -> None:
        """A non-cancellation error logs + returns None."""
        service, _, _ = _build_service()
        subscriber = MagicMock()
        subscriber.recv_multipart = AsyncMock(side_effect=RuntimeError("recv fail"))

        result = await service._recv_one_frame(subscriber)
        assert result is None

    @pytest.mark.asyncio
    async def test_recv_one_frame_propagates_cancellation(self) -> None:
        """``CancelledError`` propagates out of the helper."""
        service, _, _ = _build_service()
        subscriber = MagicMock()
        subscriber.recv_multipart = AsyncMock(side_effect=asyncio.CancelledError())

        with pytest.raises(asyncio.CancelledError):
            await service._recv_one_frame(subscriber)


class TestIngestLoop:
    """End-to-end ingest loop against a fake subscriber."""

    @pytest.mark.asyncio
    async def test_ingest_loop_dispatches_candle_then_exits(self) -> None:
        """One valid frame lands in ``_current``; loop exits on ``running=False``."""
        service, _, _ = _build_service()
        candle = _candle(open_at=datetime(2026, 5, 13, 10, 0, tzinfo=UTC))
        payload = candle.model_dump_json().encode()

        async def _recv_then_stop() -> tuple[bytes, bytes]:
            service._running = False
            return (b"market.kraken.BTC-USD.candles.1m", payload)

        subscriber = MagicMock()
        subscriber.recv_multipart = AsyncMock(side_effect=_recv_then_stop)
        service._subscriber = subscriber
        service._running = True

        await service._ingest_loop()
        assert (ExchangeEnum.KRAKEN, "BTC-USD") in service._current

    @pytest.mark.asyncio
    async def test_ingest_loop_skips_non_candle_topic(self) -> None:
        """A tick topic is gated out before parsing."""
        service, _, _ = _build_service()

        async def _recv_then_stop() -> tuple[bytes, bytes]:
            service._running = False
            return (b"market.kraken.BTC-USD.ticks", b'{"x": 1}')

        subscriber = MagicMock()
        subscriber.recv_multipart = AsyncMock(side_effect=_recv_then_stop)
        service._subscriber = subscriber
        service._running = True

        await service._ingest_loop()
        assert service._current == {}

    @pytest.mark.asyncio
    async def test_ingest_loop_skips_unparseable_payload(self) -> None:
        """A candle topic with invalid JSON is logged + skipped."""
        service, _, _ = _build_service()

        async def _recv_then_stop() -> tuple[bytes, bytes]:
            service._running = False
            return (b"market.kraken.BTC-USD.candles.1m", b"not-json")

        subscriber = MagicMock()
        subscriber.recv_multipart = AsyncMock(side_effect=_recv_then_stop)
        service._subscriber = subscriber
        service._running = True

        await service._ingest_loop()
        assert service._current == {}

    @pytest.mark.asyncio
    async def test_ingest_loop_skips_when_recv_returns_none(self) -> None:
        """A transient recv error returns None; loop continues until stopped."""
        service, _, _ = _build_service()

        async def _fail_then_stop() -> tuple[bytes, bytes]:
            service._running = False
            raise RuntimeError("boom")

        subscriber = MagicMock()
        subscriber.recv_multipart = AsyncMock(side_effect=_fail_then_stop)
        service._subscriber = subscriber
        service._running = True

        await service._ingest_loop()
        assert service._current == {}

    @pytest.mark.asyncio
    async def test_ingest_loop_returns_early_without_subscriber(self) -> None:
        """A loop without a subscriber returns immediately."""
        service, _, _ = _build_service()
        service._subscriber = None
        service._running = True

        await service._ingest_loop()

    @pytest.mark.asyncio
    async def test_ingest_loop_drops_wrong_timeframe_payload(self) -> None:
        """A 5m candle slipped through the topic gate is filtered at parse."""
        service, _, _ = _build_service()
        candle = _candle(
            open_at=datetime(2026, 5, 13, 10, 0, tzinfo=UTC),
            timeframe="5m",
        )
        payload = candle.model_dump_json().encode()

        async def _recv_then_stop() -> tuple[bytes, bytes]:
            service._running = False
            return (b"market.kraken.BTC-USD.candles.1m", payload)

        subscriber = MagicMock()
        subscriber.recv_multipart = AsyncMock(side_effect=_recv_then_stop)
        service._subscriber = subscriber
        service._running = True

        await service._ingest_loop()
        assert service._current == {}

    @pytest.mark.asyncio
    async def test_ingest_loop_propagates_cancellation(self) -> None:
        """``CancelledError`` from recv exits the loop after the info log."""
        service, _, _ = _build_service()

        async def _cancel() -> tuple[bytes, bytes]:
            raise asyncio.CancelledError()

        subscriber = MagicMock()
        subscriber.recv_multipart = AsyncMock(side_effect=_cancel)
        service._subscriber = subscriber
        service._running = True

        with pytest.raises(asyncio.CancelledError):
            await service._ingest_loop()


class TestLifecycle:
    """``start`` / ``stop`` idempotency + empty-endpoint short-circuit."""

    @pytest.mark.asyncio
    async def test_start_with_empty_broker_skips_ingest_but_prewarms(self) -> None:
        """An empty XPUB endpoint prewarms + spawns the prune loop only."""
        service, _, _ = _build_service()

        await service.start("")
        try:
            assert service._ingest_task is None
            assert service._prune_task is not None
        finally:
            await service.stop()

    @pytest.mark.asyncio
    async def test_stop_without_start_is_idempotent(self) -> None:
        """``stop`` on a never-started service is a no-op."""
        service, _, _ = _build_service()
        await service.stop()
        assert service._ingest_task is None

    @pytest.mark.asyncio
    async def test_start_with_running_task_returns_early(self) -> None:
        """A second ``start`` while a task is alive short-circuits."""
        service, _, _ = _build_service()
        live = asyncio.create_task(asyncio.sleep(10))
        service._ingest_task = live
        try:
            await service.start("inproc://nope")
            assert service._ingest_task is live
        finally:
            live.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await live

    @pytest.mark.asyncio
    async def test_start_reaps_completed_prior_task(self) -> None:
        """A done-but-not-reaped prior task triggers cleanup then no-op."""
        service, _, _ = _build_service()

        async def _done() -> None:
            return None

        finished = asyncio.create_task(_done())
        await finished
        service._ingest_task = finished

        await service.start("")
        assert service._ingest_task is None

    @pytest.mark.asyncio
    async def test_start_with_real_endpoint_creates_subscriber(self) -> None:
        """A non-empty endpoint allocates a SUB socket + spawns ingest task."""
        service, _, _ = _build_service()

        await service.start("inproc://market-cache-test")
        try:
            assert service._ingest_task is not None
            assert service._subscriber is not None
        finally:
            await service.stop()

    @pytest.mark.asyncio
    async def test_pair_stats_round_trip_through_accessors(self) -> None:
        """``set_pair_stats`` round-trips through ``get_pair_stats``."""
        service, _, _ = _build_service()
        stats = PairStats(pearson_r=0.5, pearson_n=10, is_warm=True, sample_count=10)

        await service.set_pair_stats("kraken:BTC-USD", "kraken:ETH-USD", stats)
        result = await service.get_pair_stats("kraken:BTC-USD", "kraken:ETH-USD")
        assert result is stats

    @pytest.mark.asyncio
    async def test_get_pair_stats_returns_none_for_unknown_key(self) -> None:
        """An unknown pair key returns ``None``."""
        service, _, _ = _build_service()
        assert await service.get_pair_stats("a", "b") is None

    @pytest.mark.asyncio
    async def test_pair_stats_keys_returns_snapshot(self) -> None:
        """``pair_stats_keys`` returns a snapshot of the stat map keys."""
        service, _, _ = _build_service()
        await service.set_pair_stats("a:1", "a:2", PairStats())
        await service.set_pair_stats("b:1", "b:2", PairStats())

        keys = await service.pair_stats_keys()
        assert set(keys) == {("a:1", "a:2"), ("b:1", "b:2")}

    @pytest.mark.asyncio
    async def test_instruments_cached_counts_distinct_keys(self) -> None:
        """``instruments_cached`` returns the count of distinct deque keys."""
        service, _, _ = _build_service()
        base = datetime(2026, 5, 13, 10, 0, tzinfo=UTC)
        await service.record_candle_for_test(_candle(open_at=base, instrument="BTC-USD"))
        await service.record_candle_for_test(
            _candle(open_at=base + timedelta(minutes=1), instrument="BTC-USD")
        )
        await service.record_candle_for_test(_candle(open_at=base, instrument="ETH-USD"))
        await service.record_candle_for_test(
            _candle(open_at=base + timedelta(minutes=1), instrument="ETH-USD")
        )

        assert await service.instruments_cached() == 2


class TestPruneLoopDriver:
    """The prune background task wakes the prune helper on its interval."""

    @pytest.mark.asyncio
    async def test_prune_loop_propagates_cancellation(self) -> None:
        """``CancelledError`` exits the loop after the info log."""
        service, _, _ = _build_service()
        service._running = True
        service._prune_task = asyncio.create_task(asyncio.sleep(0))

        task = asyncio.create_task(service._prune_loop())
        await asyncio.sleep(0)
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task

    @pytest.mark.asyncio
    async def test_prune_loop_runs_prune_once_each_iteration(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The loop body invokes ``_prune_once`` after each sleep tick."""
        service, _, _ = _build_service()
        service._running = True
        service._prune_task = asyncio.create_task(asyncio.sleep(0))

        sleep_calls = {"n": 0}

        async def _fast_sleep(_seconds: float) -> None:
            sleep_calls["n"] += 1
            if sleep_calls["n"] >= 2:
                service._running = False

        monkeypatch.setattr("snapper.application.services.market_cache.asyncio.sleep", _fast_sleep)
        prune_calls = {"n": 0}
        original = service._prune_once

        async def _spy_prune() -> None:
            prune_calls["n"] += 1
            await original()

        monkeypatch.setattr(service, "_prune_once", _spy_prune)

        await service._prune_loop()
        assert prune_calls["n"] >= 1


class TestSnapHelpers:
    """Dataclass + helper utilities."""

    def test_snap_from_candle_data_preserves_ohlcv(self) -> None:
        """All six OHLCV+open_at fields project to the snap."""
        open_at = datetime(2026, 5, 13, 10, 0, tzinfo=UTC)
        candle = _candle(open_at=open_at, open_=1.0, high=2.0, low=0.5, close=1.5)
        snap = _snap_from_candle_data(candle)
        assert snap.open_at_ms == int(open_at.timestamp() * 1000)
        assert (snap.open, snap.high, snap.low, snap.close) == (1.0, 2.0, 0.5, 1.5)

    def test_format_stale_age_renders_hms(self) -> None:
        """The formatter renders seconds as ``H:MM:SS``."""
        assert _format_stale_age(3600) == "1:00:00"
        assert _format_stale_age(65) == "0:01:05"

    def test_diagnostic_constants_exposed(self) -> None:
        """Public diagnostic accessors return the configured constants."""
        service, _, _ = _build_service()
        assert service.staleness_window_seconds() == 6 * 60 * 60
        assert service.cache_capacity_per_instrument() == 100

    @pytest.mark.asyncio
    async def test_record_candle_for_test_respects_timeframe_filter(self) -> None:
        """The test hook applies the 1m timeframe gate even when called directly."""
        service, _, _ = _build_service()
        await service.record_candle_for_test(
            _candle(open_at=datetime(2026, 5, 13, 10, 0, tzinfo=UTC), timeframe="5m")
        )
        assert service._current == {}
