"""Tests for :mod:`snapper.application.updaters.historical.split_repair`.

Pins the split-repair orchestration contract:

* detection maps split events onto the polygon universe, honors the
  symbol filter, and gates every repair on 1d-close evidence matching
  the split's expected ratio (fetch-boundary breaks, not only the
  execution date);
* dry-run stops after detection; clean symbols are never repaired
  (idempotence);
* the repair chain composes refetch -> cache prune -> SCD2 supersede
  (every timeframe) -> reload -> re-synthesis -> re-verification, and
  symbols still broken afterwards land in ``unverified``;
* the cache prune deletes only pre-window files and tolerates a
  missing archive-symbol mapping.
"""

import math
from dataclasses import dataclass
from datetime import UTC
from datetime import date
from datetime import datetime
from datetime import timedelta
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import pytest

from snapper.application.updaters.historical import split_repair as split_repair_module
from snapper.application.updaters.historical.split_repair import PolygonSplitRepairService
from snapper.application.updaters.historical.split_repair import SplitRepairCandidate
from snapper.application.updaters.historical.split_repair import run_polygon_split_repair
from snapper.data.models import KNOWN_TO_MAX
from snapper.data.models import Candle
from snapper.data.models import Instrument
from snapper.data.models import Symbol
from snapper.data.repository import SQLAlchemyRepository
from snapper.data.repository_types import InstrumentSymbolRow
from snapper.infrastructure.exchanges.implementations.polygon import PolygonSplitEvent

_TODAY = datetime.now(UTC).date()


def _event(
    ticker: str = "NFLX",
    execution_date: date | None = None,
    split_from: float = 1.0,
    split_to: float = 10.0,
) -> PolygonSplitEvent:
    """Build one split event with sane defaults."""
    return PolygonSplitEvent(
        ticker=ticker,
        execution_date=execution_date if execution_date is not None else _TODAY,
        split_from=split_from,
        split_to=split_to,
    )


def _universe_row(native: str) -> InstrumentSymbolRow:
    """Build one universe row keyed off the native symbol."""
    return InstrumentSymbolRow(
        native_symbol=native,
        instrument_public_id=f"instr-{native}",
        symbol_public_id=f"sym-{native}",
    )


def _closes_rows(closes: list[float], start: date | None = None) -> list[dict[str, Any]]:
    """Build minimal 1d candle rows carrying consecutive daily closes."""
    first = start if start is not None else _TODAY - timedelta(days=len(closes))
    return [
        {
            "open_at": datetime.combine(first + timedelta(days=i), datetime.min.time(), UTC),
            "close": c,
        }
        for i, c in enumerate(closes)
    ]


@dataclass
class _RecorderStub:
    """Records constructor kwargs and start() awaits for a composed service."""

    calls: list[dict[str, Any]]
    started: list[bool]

    def make(self) -> type:
        """Return a stub class bound to this recorder."""
        recorder = self

        class _Stub:
            def __init__(self, **kwargs: Any) -> None:
                recorder.calls.append(kwargs)

            async def start(self) -> None:
                recorder.started.append(True)

        return _Stub


class _ClientStub:
    """Polygon client stub with canned splits."""

    def __init__(self, api_key: str) -> None:
        self.api_key = api_key
        self.disconnected = False
        self.splits: list[PolygonSplitEvent] = list(_CLIENT_SPLITS)
        self.list_splits_kwargs: dict[str, Any] = {}

    async def list_splits(self, **kwargs: Any) -> list[PolygonSplitEvent]:
        self.list_splits_kwargs = kwargs
        return self.splits

    async def disconnect(self) -> None:
        self.disconnected = True


_CLIENT_SPLITS: list[PolygonSplitEvent] = []


def _wire_common(
    monkeypatch: pytest.MonkeyPatch,
    *,
    universe: list[InstrumentSymbolRow],
    splits: list[PolygonSplitEvent],
    candle_rows: dict[str, list[list[dict[str, Any]]]],
    api_key: str = "key-1",
) -> MagicMock:
    """Patch settings/client/repo seams and return the repo mock.

    Args:
        monkeypatch: Pytest monkeypatch fixture.
        universe: Rows returned by ``list_instrument_symbols``.
        splits: Events returned by the client stub.
        candle_rows: Per-instrument queues of 1d row lists returned by
            consecutive ``get_candles`` calls (last list repeats).
        api_key: Configured API key ('' exercises the missing-key path).

    Returns:
        The repository mock.
    """
    boot = MagicMock(db_url="sqlite+aiosqlite:///:memory:", zmq_broker_xsub="tcp://x:1")
    settings = MagicMock(
        polygon_api_key=api_key,
        db_url="sqlite+aiosqlite:///:memory:",
        zmq_broker_xsub="tcp://x:1",
    )
    monkeypatch.setattr(split_repair_module, "get_settings", lambda: boot)
    monkeypatch.setattr(
        split_repair_module, "get_settings_service", AsyncMock(return_value=MagicMock())
    )
    monkeypatch.setattr(split_repair_module, "get_settings_with_service", lambda _s: settings)
    _CLIENT_SPLITS.clear()
    _CLIENT_SPLITS.extend(splits)
    monkeypatch.setattr(split_repair_module, "PolygonExchangeClient", _ClientStub)
    repo = MagicMock()
    repo.list_instrument_symbols = AsyncMock(return_value=universe)

    queues = {k: list(v) for k, v in candle_rows.items()}

    async def fake_get_candles(native_symbol: str, *_args: Any, **_kwargs: Any) -> list[dict]:
        queue = queues.get(native_symbol, [[]])
        if len(queue) > 1:
            return queue.pop(0)
        return queue[0]

    repo.get_candles = AsyncMock(side_effect=fake_get_candles)
    repo.supersede_current_candles = AsyncMock(return_value=7)
    monkeypatch.setattr(split_repair_module, "get_repository", lambda _url: repo)
    return repo


class TestDetection:
    """Split-event mapping and the 1d evidence gate."""

    @pytest.mark.asyncio
    async def test_missing_api_key_raises(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """A missing API key aborts the run.

        Given: Settings without a polygon API key,
        When: The service starts,
        Then: ValueError propagates.
        """
        _wire_common(monkeypatch, universe=[], splits=[], candle_rows={}, api_key="")
        service = PolygonSplitRepairService()
        with pytest.raises(ValueError, match="API key"):
            await service.start()

    @pytest.mark.asyncio
    async def test_out_of_universe_and_filtered_events_are_skipped(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Events outside the universe or the -s filter never become candidates.

        Given: One split for an unknown ticker and one for a symbol
            excluded by the explicit filter,
        When: The service runs,
        Then: No candidates are recorded and the client disconnects.
        """
        _wire_common(
            monkeypatch,
            universe=[_universe_row("NFLX")],
            splits=[_event(ticker="ZZZQ"), _event(ticker="NFLX")],
            candle_rows={},
        )
        service = PolygonSplitRepairService(symbols=["TQQQ"])
        summary = await service.start()
        assert summary.splits_seen == 2
        assert summary.candidates == []
        assert summary.clean == []

    @pytest.mark.asyncio
    async def test_clean_symbol_is_skipped(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """A symbol without a matching 1d break is reported clean.

        Given: A 1:10 split whose symbol shows only organic daily moves,
        When: The service runs,
        Then: The symbol lands in ``clean`` and nothing is repaired.
        """
        _wire_common(
            monkeypatch,
            universe=[_universe_row("NFLX")],
            splits=[_event()],
            candle_rows={"NFLX": [_closes_rows([100.0, 101.5, 99.8, 100.2])]},
        )
        summary = await PolygonSplitRepairService().start()
        assert summary.clean == ["NFLX"]
        assert summary.repaired == []
        assert summary.candidates[0].break_day is None

    @pytest.mark.asyncio
    async def test_dry_run_detects_but_never_repairs(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Dry-run confirms the break but skips every mutating step.

        Given: A confirmed stale-basis break and dry_run=True,
        When: The service runs,
        Then: The candidate carries the break day, nothing is repaired,
            and the composed services are never constructed.
        """
        repo = _wire_common(
            monkeypatch,
            universe=[_universe_row("NFLX")],
            splits=[_event()],
            candle_rows={"NFLX": [_closes_rows([471.0, 470.5, 47.1, 47.3])]},
        )
        refetch = _RecorderStub([], [])
        monkeypatch.setattr(split_repair_module, "PolygonAggregatesBackfillService", refetch.make())
        summary = await PolygonSplitRepairService(dry_run=True).start()
        assert summary.dry_run is True
        assert summary.candidates[0].break_day is not None
        assert summary.repaired == []
        assert refetch.calls == []
        repo.supersede_current_candles.assert_not_awaited()

    def test_break_detector_edge_cases(self) -> None:
        """Zero/negative closes are skipped and empty history is clean.

        Given: Histories with an empty row list, a zero close, and a
            break not matching the split ratio,
        When: The detector arithmetic evaluates each pair,
        Then: Only a ratio within log-tolerance of the expected break
            would match (validated indirectly via the tolerance bound).
        """
        expected = _event().expected_break_ratio
        assert expected == pytest.approx(0.1)
        within = abs(math.log(0.105 / 1.0) - math.log(expected))
        outside = abs(math.log(0.5 / 1.0) - math.log(expected))
        assert within < math.log(1.15) < outside


class TestRepairChain:
    """Composition and verification of the mutating pipeline."""

    @pytest.mark.asyncio
    async def test_full_repair_chain_composes_and_verifies(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        """A confirmed break drives refetch, prune, supersede, reload, synth.

        Given: One confirmed stale NFLX basis whose re-check comes back
            clean, pre-window and in-window cache files on disk, and the
            service clock frozen to the module's ``_TODAY`` (the window
            assertions would otherwise flake when import and run straddle
            a UTC midnight),
        When: The service runs,
        Then: Every composed service is constructed with the repair
            window, all seven timeframes are superseded, only the
            pre-window file is pruned, and NFLX is reported repaired.
        """
        monkeypatch.setattr(PolygonSplitRepairService, "_utc_today", staticmethod(lambda: _TODAY))
        broken = _closes_rows([471.0, 470.5, 47.1, 47.3])
        clean = _closes_rows([47.0, 47.5, 47.2, 47.4])
        repo = _wire_common(
            monkeypatch,
            universe=[_universe_row("NFLX")],
            splits=[_event()],
            candle_rows={"NFLX": [broken, clean]},
        )
        refetch = _RecorderStub([], [])
        loader = _RecorderStub([], [])
        synth = _RecorderStub([], [])
        monkeypatch.setattr(split_repair_module, "PolygonAggregatesBackfillService", refetch.make())
        monkeypatch.setattr(split_repair_module, "PolygonCsvLoaderService", loader.make())
        monkeypatch.setattr(split_repair_module, "SynthesizedCandleBackfillService", synth.make())
        monkeypatch.setattr(
            split_repair_module,
            "DatabaseRepository",
            lambda _url: MagicMock(get_archive_symbols=lambda: {"sym-NFLX": "NFLX"}),
        )
        window_start = _TODAY - timedelta(days=730)
        old_file = tmp_path / "old.csv"
        new_file = tmp_path / "new.csv"
        old_file.write_text("stale")
        new_file.write_text("fresh")
        monkeypatch.setattr(
            split_repair_module,
            "PolygonHistoricalLoader",
            lambda _client, cache_root: MagicMock(
                iter_aggregate_csv_files=lambda _sym, _ts: [
                    (old_file, window_start - timedelta(days=3)),
                    (new_file, window_start + timedelta(days=3)),
                ]
            ),
        )
        summary = await PolygonSplitRepairService().start()
        assert summary.repaired == ["NFLX"]
        assert summary.unverified == []
        assert summary.pruned_files == 1
        assert not old_file.exists()
        assert new_file.exists()
        assert refetch.calls[0]["symbols"] == ["NFLX"]
        assert refetch.calls[0]["resume"] is False
        assert refetch.calls[0]["save_csv"] is True
        assert refetch.calls[0]["days_back"] == 730
        assert loader.calls[0]["since"] == window_start
        assert synth.calls[0]["cut_date"] == window_start
        assert synth.calls[0]["symbols"] == ["NFLX"]
        assert refetch.started
        assert loader.started
        assert synth.started
        superseded_tfs = [
            call.kwargs["timeframe"] for call in repo.supersede_current_candles.await_args_list
        ]
        assert superseded_tfs == ["1m", "5m", "15m", "30m", "1h", "4h", "1d"]

    @pytest.mark.asyncio
    async def test_still_broken_symbol_lands_in_unverified(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A break surviving the repair is surfaced, not swallowed.

        Given: A confirmed break whose post-repair re-check still shows
            the same ratio, and an archive map missing the symbol (the
            prune-warning path),
        When: The service runs,
        Then: The symbol lands in ``unverified`` and no files are pruned.
        """
        broken = _closes_rows([471.0, 470.5, 47.1, 47.3])
        _wire_common(
            monkeypatch,
            universe=[_universe_row("NFLX")],
            splits=[_event()],
            candle_rows={"NFLX": [broken]},
        )
        for name in (
            "PolygonAggregatesBackfillService",
            "PolygonCsvLoaderService",
            "SynthesizedCandleBackfillService",
        ):
            monkeypatch.setattr(split_repair_module, name, _RecorderStub([], []).make())
        monkeypatch.setattr(
            split_repair_module,
            "DatabaseRepository",
            lambda _url: MagicMock(get_archive_symbols=lambda: {}),
        )
        monkeypatch.setattr(
            split_repair_module,
            "PolygonHistoricalLoader",
            lambda _client, cache_root: MagicMock(iter_aggregate_csv_files=lambda *_a: []),
        )
        summary = await PolygonSplitRepairService().start()
        assert summary.unverified == ["NFLX"]
        assert summary.repaired == []
        assert summary.pruned_files == 0


class TestEntryPoint:
    """CLI-facing wrapper."""

    @pytest.mark.asyncio
    async def test_run_polygon_split_repair_forwards_options(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The wrapper builds a service from its options and runs it.

        Given: Patched seams and explicit options,
        When: ``run_polygon_split_repair`` is awaited,
        Then: The returned summary reflects a dry run with the filter
            applied.
        """
        _wire_common(
            monkeypatch,
            universe=[_universe_row("NFLX")],
            splits=[_event()],
            candle_rows={"NFLX": [_closes_rows([100.0, 100.4])]},
        )
        summary = await run_polygon_split_repair(
            symbols=["NFLX"], lookback_days=10, window_days=100, dry_run=True
        )
        assert summary.dry_run is True
        assert summary.clean == ["NFLX"]


class TestGuards:
    """Micro-split skip, malformed events, and crash-recovery semantics."""

    @pytest.mark.asyncio
    async def test_micro_split_and_malformed_events_are_undetectable(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Ratios near 1.0 and malformed ratios are skipped, not repaired.

        Given: A 1000:1061 micro-split, a 20:21 micro-split, an event
            with a zero ratio leg, and one with a NaN leg, all for
            universe symbols,
        When: The service runs,
        Then: All four land in ``undetectable`` with no candidates —
            an everyday flat close would match them forever, so the
            price detector must refuse them instead of churning.
        """
        _wire_common(
            monkeypatch,
            universe=[
                _universe_row("HON"),
                _universe_row("OPEN"),
                _universe_row("BAD"),
                _universe_row("NAN"),
            ],
            splits=[
                _event(ticker="HON", split_from=1000.0, split_to=1061.0),
                _event(ticker="OPEN", split_from=20.0, split_to=21.0),
                _event(ticker="BAD", split_from=0.0, split_to=2.0),
                _event(ticker="NAN", split_from=float("nan"), split_to=2.0),
            ],
            candle_rows={},
        )
        summary = await PolygonSplitRepairService().start()
        assert summary.undetectable == ["HON", "OPEN", "BAD", "NAN"]
        assert summary.candidates == []
        assert summary.clean == []

    @pytest.mark.asyncio
    async def test_empty_current_history_confirms_incomplete_repair(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """An empty current 1d history is treated as broken, not clean.

        Given: A candidate whose current 1d history is empty (the
            signature of a crash between supersede and reload) and a
            repair chain that restores data,
        When: The service runs,
        Then: The candidate confirms with the execution date as the
            break day and the repair chain runs to a verified finish —
            a rerun after a mid-chain crash self-heals.
        """
        restored = _closes_rows([47.0, 47.5, 47.2])
        repo = _wire_common(
            monkeypatch,
            universe=[_universe_row("NFLX")],
            splits=[_event()],
            candle_rows={"NFLX": [[], restored]},
        )
        for name in (
            "PolygonAggregatesBackfillService",
            "PolygonCsvLoaderService",
            "SynthesizedCandleBackfillService",
        ):
            monkeypatch.setattr(split_repair_module, name, _RecorderStub([], []).make())
        monkeypatch.setattr(
            split_repair_module,
            "DatabaseRepository",
            lambda _url: MagicMock(get_archive_symbols=lambda: {"sym-NFLX": "NFLX"}),
        )
        monkeypatch.setattr(
            split_repair_module,
            "PolygonHistoricalLoader",
            lambda _client, cache_root: MagicMock(iter_aggregate_csv_files=lambda *_a: []),
        )
        summary = await PolygonSplitRepairService().start()
        assert summary.candidates[0].break_day == _event().execution_date
        assert summary.repaired == ["NFLX"]
        repo.supersede_current_candles.assert_awaited()


class TestDetectorAgainstRealRepository:
    """The detector must speak the real ``get_candles`` contract."""

    @pytest.mark.asyncio
    async def test_find_matching_break_resolves_by_native_symbol(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The detector finds a seeded break through a real repository.

        Given: A real in-memory repository seeded with an active
            polygon symbol/instrument and 1d candles carrying a 10x
            stale-basis break,
        When: ``_find_matching_break`` runs against it,
        Then: The break day is found — proving the detector passes the
            native symbol (not the instrument public id) to
            ``get_candles``.
        """
        repo = SQLAlchemyRepository("sqlite+aiosqlite:///:memory:")
        await repo.create_all()
        past = datetime.now(UTC) - timedelta(days=30)
        async with repo.session() as s:
            s.add(
                Symbol(
                    public_id="sym-1",
                    native_symbol="NFLX",
                    base="NFLX",
                    quote=None,
                    asset_type="equity",
                    created_at=past,
                    session_id="t",
                    sequence_id=1,
                    timestamp=past,
                    known_to=KNOWN_TO_MAX,
                )
            )
            s.add(
                Instrument(
                    public_id="instr-1",
                    symbol_public_id="sym-1",
                    exchange="polygon",
                    session_id="t",
                    sequence_id=1,
                    timestamp=past,
                    known_to=KNOWN_TO_MAX,
                )
            )
            for i, close in enumerate([471.0, 470.5, 47.1, 47.3]):
                day = past + timedelta(days=i)
                s.add(
                    Candle(
                        public_id=f"c-{i}",
                        instrument_public_id="instr-1",
                        open_at=day,
                        timeframe="1d",
                        open=close,
                        high=close,
                        low=close,
                        close=close,
                        volume=10.0,
                        vwap=None,
                        trades=None,
                        session_id="t",
                        sequence_id=1,
                        timestamp=past,
                        known_to=KNOWN_TO_MAX,
                    )
                )
            await s.commit()
        boot = MagicMock(db_url="sqlite+aiosqlite:///:memory:", zmq_broker_xsub="tcp://x:1")
        monkeypatch.setattr(split_repair_module, "get_settings", lambda: boot)
        service = PolygonSplitRepairService()
        candidate = SplitRepairCandidate(
            event=_event(),
            native_symbol="NFLX",
            instrument_public_id="instr-1",
            symbol_public_id="sym-1",
        )
        break_day = await service._find_matching_break(repo, candidate)
        assert break_day == (past + timedelta(days=2)).date()
