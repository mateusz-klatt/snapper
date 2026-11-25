"""Compare the rebuild with the live publisher and the historical fold."""

from collections.abc import AsyncIterator
from collections.abc import Iterable
from collections.abc import Mapping
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from itertools import count
from pathlib import Path
from random import Random
from typing import cast
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from uuid import uuid7

import pytest
import pytest_asyncio
from sqlalchemy import insert
from sqlalchemy import select

from scripts.backfill_synth_candles import _Bucket
from scripts.backfill_synth_candles import _build_rows
from scripts.backfill_synth_candles import _fold_minute
from snapper.application.updaters.historical import synthesized_candle_rebuild as rebuild
from snapper.config.app import AppSettings
from snapper.core.types import ExchangeEnum
from snapper.data.models import Candle
from snapper.data.repository import SQLAlchemyRepository
from snapper.data.repository_types import CandleRow
from snapper.data.repository_types import CandleUpsertRow
from snapper.data.repository_types import CandleVersionRow
from snapper.messaging.publishers.base import MarketDataPublisherService
from snapper.messaging.publishers.base import _CandleRepairKey
from snapper.messaging.publishers.base import _PendingCandleRepair
from snapper.messaging.publishers.candle_aggregator import CandleAggregator
from snapper.messaging.publishers.kraken import KrakenMarketDataPublisher
from tests.helpers.candle_repair import XRP
from tests.helpers.candle_repair import candle
from tests.helpers.candle_repair import seed_instrument

_DAY = datetime(2025, 1, 5, tzinfo=UTC)
_AUTHOR = _DAY + timedelta(days=2)
_SYMBOL = "XRP-EUR"
_WIDTHS = dict(zip(rebuild.TIMEFRAMES, (300, 900, 1800, 3600, 14400, 86400), strict=True))
_GRID_SHAPES = ("full", "first_missing", "middle_missing", "last_missing")


def _grid(width: int) -> frozenset[datetime]:
    """Return every minute required for an exact historical repair."""
    return frozenset(_DAY + timedelta(minutes=i) for i in range(width // 60))


def _random_rows(
    width: int, seed: int, openings: Iterable[datetime], *, zero_volume: bool = False
) -> list[CandleUpsertRow]:
    """Use deterministic irregular floats so comparison checks rounding and summation order."""
    rng = Random(seed + width)
    return [
        candle(
            XRP,
            opening,
            timestamp=_AUTHOR,
            sequence_id=sequence,
            source="native",
            open=rng.uniform(10.0, 19.0),
            high=rng.uniform(25.0, 30.0),
            low=rng.uniform(1.0, 5.0),
            close=rng.uniform(10.0, 20.0),
            volume=0.0 if zero_volume else rng.random() * 321.0,
            vwap=rng.uniform(8.0, 24.0),
            trades=0 if zero_volume else rng.randrange(1, 91),
        )
        for sequence, opening in enumerate(sorted(openings), 1)
    ]


def _payload(row: CandleUpsertRow) -> tuple[object, ...]:
    """Compare all business fields and every float's exact binary representation."""
    fields: Mapping[str, object] = row
    values = (fields.get(field) for field in rebuild._BUSINESS)
    return tuple(value.hex() if isinstance(value, float) else value for value in values)


def _window(timeframe: str) -> rebuild._Window:
    """Construct a scope window whose input rows will be supplied explicitly."""
    return rebuild._Window((XRP, timeframe, _DAY), _SYMBOL, 1, "no_input")


@pytest.mark.parametrize("timeframe", rebuild.TIMEFRAMES)
@pytest.mark.parametrize(
    ("seed", "gapped", "zero_volume"), [(17, False, False), (24, True, False), (90, True, True)]
)
def test_both_fold_implementations_agree_bit_for_bit(
    timeframe: str, seed: int, gapped: bool, zero_volume: bool
) -> None:
    """Compare arithmetic for full, randomly gapped, and zero-volume grids at each scale."""
    width = _WIDTHS[timeframe]
    openings = sorted(_grid(width))
    if gapped:
        openings = sorted(Random(seed).sample(openings, k=max(1, len(openings) // 3)))
    rows = _random_rows(width, seed, openings, zero_volume=zero_volume)
    actual = rebuild._fold(_window(timeframe), cast(list[CandleVersionRow], rows))
    buckets: dict[tuple[str, int], _Bucket] = {}
    for row in rows:
        _fold_minute(buckets, cast(CandleRow, row), {timeframe: width})
    at = int(_DAY.timestamp())
    (expected,) = _build_rows(
        {timeframe: [(at, buckets[timeframe, at])]}, XRP, _AUTHOR, count(1), "fixture"
    )
    assert _payload(actual) == _payload(expected)


@pytest_asyncio.fixture
async def repository(tmp_path: Path) -> AsyncIterator[SQLAlchemyRepository]:
    """Use a disposable database with a real Kraken catalog identity."""
    repo = SQLAlchemyRepository(f"sqlite+aiosqlite:///{tmp_path / 'parity.db'}")
    await repo.create_all()
    try:
        await seed_instrument(repo, XRP, _SYMBOL, "XXRPZEUR")
        yield repo
    finally:
        await repo.engine.dispose()


async def _seed_grid(
    repository: SQLAlchemyRepository, timeframe: str, shape: str
) -> list[CandleVersionRow]:
    """Store the day's committed source grid with one specified gap or no gaps."""
    width = _WIDTHS[timeframe]
    openings = sorted(_grid(width))
    omissions = {"first_missing": 0, "middle_missing": len(openings) // 2, "last_missing": -1}
    if shape in omissions:
        openings.pop(omissions[shape])
    rows = _random_rows(width, 19, openings)
    async with repository.session() as session:
        await session.execute(insert(Candle), rows)
        await session.commit()
        result = await session.execute(select(Candle.__table__).order_by(Candle.open_at))
        return [cast(CandleVersionRow, dict(row)) for row in result.mappings()]


def _publisher(repository: SQLAlchemyRepository, timeframe: str) -> KrakenMarketDataPublisher:
    """Wire a stopped real publisher to storage with transport and catalog scheduling stubbed."""
    publisher = KrakenMarketDataPublisher([_SYMBOL])
    settings = MagicMock(spec=AppSettings)
    settings.timeframes = ["1m", timeframe]
    settings.spot_candle_source = "trade_built"
    settings.candle_minute_completion = False
    settings.trade_built_finalize_grace_seconds = 12
    settings.candle_identity_window_snapshot = False
    settings.candle_identity_deterministic_from = None
    publisher.settings = settings
    publisher.repository = repository
    publisher._ensure_instrument = AsyncMock(return_value=XRP)
    publisher._publish_message = AsyncMock()
    publisher._candle_aggregator = CandleAggregator([timeframe], live_epoch=_DAY, forward_fill=True)
    return publisher


async def _install_rebuild(
    repository: SQLAlchemyRepository, timeframe: str, inputs: list[CandleVersionRow]
) -> CandleUpsertRow:
    """Store the output through the real planner and guarded writer."""
    planned = rebuild._classify(_window(timeframe), [], inputs, _AUTHOR)
    assert planned.action == "insert"
    row = rebuild._write_row(planned, str(uuid7()), _AUTHOR, 1)
    outcome = await repository.rebuild_synthesized_candles(
        [row], expected={planned.key: rebuild._expectation(planned)}
    )
    assert outcome.outcomes[planned.key].status == "inserted"
    return row


async def _ordinary_repair_after_noop(
    publisher: KrakenMarketDataPublisher, original: CandleVersionRow, key: _CandleRepairKey
) -> CandleUpsertRow:
    """Release a real pending ordinary repair by acknowledging a business-equal late row."""
    repository = publisher._require_repository()
    tracker = publisher._candle_aggregator
    assert tracker is not None
    tracker.fold(
        MarketDataPublisherService._candle_update_from_row(cast(CandleRow, original), _SYMBOL)
    )
    tracker.flush(_DAY + timedelta(seconds=_WIDTHS[key.timeframe]))
    late = cast(CandleUpsertRow, {name: item for name, item in original.items() if name != "id"})
    late.update({"timestamp": datetime.now(UTC), "sequence_id": 1200})
    sequence = publisher._assign_candle_write_sequence(late)
    publisher._set_pending_candle_repair(key, _PendingCandleRepair(required_sequence=sequence))
    await publisher._run_due_candle_repairs(ExchangeEnum.KRAKEN)
    assert publisher._candle_write_queue.empty()
    assert await repository.upsert_candles([late]) == 0
    publisher._mark_candle_writes_committed([late])
    await publisher._run_due_candle_repairs(ExchangeEnum.KRAKEN)
    output = publisher._candle_write_queue.get_nowait()
    assert await repository.upsert_candles([output]) == 0
    publisher._mark_candle_writes_committed([output])
    publisher._candle_write_queue.task_done()
    assert key not in publisher._pending_candle_repairs
    return output


async def _historical_repair(
    publisher: KrakenMarketDataPublisher,
    timeframe: str,
    shape: str,
    expected_row: CandleUpsertRow,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Require the complete grid and compare the actual historical commit candidate."""
    expected_minutes = _grid(_WIDTHS[timeframe])
    key = _CandleRepairKey(_SYMBOL, timeframe, _DAY)
    pending = _PendingCandleRepair(expected_source_minutes=expected_minutes, historical_only=True)
    publisher._set_pending_candle_repair(key, pending)
    result = await publisher._derive_repaired_synthesized_candle(key, expected_minutes)
    if shape != "full":
        assert result is None
        return
    assert result is not None
    repository = publisher._require_repository()
    persist = AsyncMock(wraps=repository.upsert_historical_synthesized_candles)
    monkeypatch.setattr(repository, "upsert_historical_synthesized_candles", persist)
    assert await publisher._commit_historical_candle_repair(result, ExchangeEnum.KRAKEN, timeframe)
    persist.assert_awaited_once()
    assert _payload(persist.call_args.args[0][0]) == _payload(expected_row)


@pytest.mark.parametrize("timeframe", rebuild.TIMEFRAMES)
@pytest.mark.parametrize("shape", _GRID_SHAPES)
async def test_live_and_historical_repair_match_the_guarded_rebuild(
    repository: SQLAlchemyRepository, timeframe: str, shape: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Match durable ordinary output across gaps and historical output on an exact full grid."""
    inputs = await _seed_grid(repository, timeframe, shape)
    candidate = await _install_rebuild(repository, timeframe, inputs)
    publisher = _publisher(repository, timeframe)
    key = _CandleRepairKey(_SYMBOL, timeframe, _DAY)
    ordinary = await _ordinary_repair_after_noop(publisher, inputs[0], key)
    assert _payload(ordinary) == _payload(candidate)
    await _historical_repair(publisher, timeframe, shape, candidate, monkeypatch)
    async with repository.session() as session:
        versions = (
            await session.execute(select(Candle.id).where(Candle.timeframe == timeframe))
        ).all()
    assert len(versions) == 1


def test_scope_floors_midnight_and_the_last_minute_at_every_timeframe() -> None:
    """Keep all scales UTC-aligned and attribute 23:59 to the ending UTC day."""
    last = _DAY + timedelta(hours=23, minutes=59)
    following = _DAY + timedelta(days=1)
    counted = rebuild._count_windows([(XRP, _DAY), (XRP, last), (XRP, following)])
    for timeframe, width in _WIDTHS.items():
        assert counted[XRP, timeframe, following] == 1
        assert counted[XRP, timeframe, following - timedelta(seconds=width)] == (
            2 if width == 86400 else 1
        )
        assert counted[XRP, timeframe, _DAY] == (2 if width == 86400 else 1)
