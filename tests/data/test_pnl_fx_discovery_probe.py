"""Preserve FX discovery evidence boundaries within bounded candle-open intervals."""

from datetime import timedelta
from typing import Literal
from typing import cast

import pytest

from snapper.data.repository import SQLAlchemyRepository
from snapper.data.repository_types import PnlFxRatePlane
from tests.data.test_pnl_fx_rate_repository import _M
from tests.data.test_pnl_fx_rate_repository import _NOW
from tests.data.test_pnl_fx_rate_repository import _TS
from tests.data.test_pnl_fx_rate_repository import _candle
from tests.data.test_pnl_fx_rate_repository import _instrument
from tests.data.test_pnl_fx_rate_repository import _original_discovery_query
from tests.data.test_pnl_fx_rate_repository import _symbol
from tests.data.test_pnl_fx_rate_repository import repository as _repository_fixture

repository = _repository_fixture


async def _assert_original_planes(
    repository: SQLAlchemyRepository,
    minute_offsets: tuple[int, int],
    expected: list[PnlFxRatePlane],
) -> None:
    """Compare the real discovery result and order with the frozen candle-join query."""
    start = _M + timedelta(minutes=minute_offsets[0])
    end = _M + timedelta(minutes=minute_offsets[1])
    pairs = [("CAD", "CHF"), ("CAD", "CHF")]
    actual = await repository.get_pnl_fx_rate_exchanges(pairs, start, end, _NOW)
    async with repository.session() as session:
        result = await session.execute(_original_discovery_query(pairs, start, end, _NOW))
        original = [(base, cast(str, quote), exchange) for base, quote, exchange in result.all()]
    assert actual == original == expected


@pytest.mark.parametrize("band", ["instrument", "symbol"])
@pytest.mark.parametrize(
    ("lower_offset", "upper_offset", "eligible"),
    [(1, 60, False), (0, 60, True), (-60, 0, False), (-60, 1, True)],
)
async def test_discovery_preserves_each_author_band_edge(
    repository: SQLAlchemyRepository,
    band: Literal["instrument", "symbol"],
    lower_offset: int,
    upper_offset: int,
    eligible: bool,
) -> None:
    """Require a candle author inside both inclusive-lower, exclusive-upper bands.

    Given: One metadata band starts or ends at the candle's authoring instant.
    When: Discovery looks for evidence at that candle's exact open minute.
    Then: Only an author inside both bands certifies the plane, as in the original join.
    """
    symbol = _symbol("sym-probe-band", "CAD-CHF", "CAD", "CHF")
    instrument = _instrument("ins-probe-band", "sym-probe-band", "walutomat")
    owner = instrument if band == "instrument" else symbol
    owner.timestamp = _M + timedelta(seconds=lower_offset)
    owner.known_to = _M + timedelta(seconds=upper_offset)
    async with repository.session() as session:
        session.add_all([symbol, instrument, _candle("ins-probe-band", _M, 1.2)])
        await session.commit()
    expected = [("CAD", "CHF", "walutomat")] if eligible else []
    await _assert_original_planes(repository, (0, 0), expected)


@pytest.mark.parametrize("invalid", ["incomplete", "timeframe", "future", "retired"])
async def test_discovery_ignores_ineligible_first_candle(
    repository: SQLAlchemyRepository,
    invalid: Literal["incomplete", "timeframe", "future", "retired"],
) -> None:
    """Require a candle satisfying every evidence predicate to certify a candidate.

    Given: The earliest candle is unfinished, non-minute, future-authored, or retired.
    When: Discovery first sees only that row, then a later valid candle is added.
    Then: The first read excludes the plane and the second includes it exactly once.
    """
    candle = _candle("ins-probe-first", _M, 1.2)
    if invalid == "incomplete":
        candle.complete = False
    elif invalid == "timeframe":
        candle.timeframe = "5m"
    elif invalid == "future":
        candle.timestamp = _NOW + timedelta(seconds=1)
    else:
        candle.known_to = _NOW
    async with repository.session() as session:
        session.add_all(
            [
                _symbol("sym-probe-first", "CAD-CHF", "CAD", "CHF"),
                _instrument("ins-probe-first", "sym-probe-first", "walutomat"),
                candle,
            ]
        )
        await session.commit()
    await _assert_original_planes(repository, (0, 1), [])
    async with repository.session() as session:
        session.add(_candle("ins-probe-first", _M + timedelta(minutes=1), 1.3))
        await session.commit()
    await _assert_original_planes(repository, (0, 1), [("CAD", "CHF", "walutomat")])


@pytest.mark.parametrize(
    ("author_offset", "retired_offset", "eligible"),
    [(0, 1, True), (1, 2, False), (-1, 0, False), (-1, 1, True)],
)
async def test_discovery_keeps_exact_candle_horizon_edges(
    repository: SQLAlchemyRepository,
    author_offset: int,
    retired_offset: int,
    eligible: bool,
) -> None:
    """Keep inclusive author knowledge and exclusive retirement at the as-of instant.

    Given: Candle versions lie one second before, at, or after the horizon.
    When: Discovery reads a fixed horizon independent of the candle open minute.
    Then: The active version rules match the original join at both boundaries.
    """
    candle = _candle("ins-probe-horizon", _M, 1.2)
    candle.timestamp = _NOW + timedelta(seconds=author_offset)
    candle.known_to = _NOW + timedelta(seconds=retired_offset)
    async with repository.session() as session:
        session.add_all(
            [
                _symbol("sym-probe-horizon", "CAD-CHF", "CAD", "CHF"),
                _instrument("ins-probe-horizon", "sym-probe-horizon", "walutomat"),
                candle,
            ]
        )
        await session.commit()
    expected = [("CAD", "CHF", "walutomat")] if eligible else []
    await _assert_original_planes(repository, (0, 0), expected)


async def test_discovery_deduplicates_multiple_metadata_versions_of_one_plane(
    repository: SQLAlchemyRepository,
) -> None:
    """Retain every version's author ownership while returning distinct ordered planes.

    Given: Two symbol and instrument versions certify one venue and another venue exists.
    When: A single discovery window spans candles owned by both metadata versions.
    Then: Each venue appears once in the exact original ordering, including historical evidence.
    """
    changed_at = _M + timedelta(minutes=1)
    old_instrument = _instrument("ins-probe-versions", "sym-probe-versions", "walutomat")
    old_instrument.known_to = changed_at
    async with repository.session() as session:
        session.add_all(
            [
                _symbol("sym-probe-versions", "CAD-CHF", "CAD", "CHF", known_to=changed_at),
                _symbol("sym-probe-versions", "CAD-CHF", "CAD", "CHF", timestamp=changed_at),
                old_instrument,
                _instrument(
                    "ins-probe-versions", "sym-probe-versions", "walutomat", timestamp=changed_at
                ),
                _instrument("ins-probe-other", "sym-probe-versions", "kraken", timestamp=_TS),
                _candle("ins-probe-versions", _M, 1.2),
                _candle("ins-probe-versions", _M + timedelta(minutes=2), 1.3),
                _candle("ins-probe-other", _M, 1.4),
            ]
        )
        await session.commit()
    await _assert_original_planes(
        repository, (0, 2), [("CAD", "CHF", "kraken"), ("CAD", "CHF", "walutomat")]
    )


async def _seed_interval_plane(repository: SQLAlchemyRepository, minute_offset: int) -> None:
    """Store one otherwise eligible plane with evidence at the requested open."""
    async with repository.session() as session:
        session.add_all(
            [
                _symbol("sym-interval", "CAD-CHF", "CAD", "CHF"),
                _instrument("ins-interval", "sym-interval", "walutomat"),
                _candle("ins-interval", _M + timedelta(minutes=minute_offset), 1.2),
            ]
        )
        await session.commit()


@pytest.mark.parametrize(
    ("minute_offset", "eligible"),
    [(-3, False), (-2, True), (-1, True), (0, False), (1, True), (2, True), (3, False)],
)
async def test_discovery_intervals_keep_only_inclusive_union_evidence(
    repository: SQLAlchemyRepository,
    minute_offset: int,
    eligible: bool,
) -> None:
    """Limit plane evidence to a union of inclusive candle-open intervals.

    Given: One valid candle lies at, within, between, or beyond two intervals.
    When: Discovery intersects its broad bounds with that sparse interval union.
    Then: Only candles inside either interval, including both edges, certify a plane.
    """
    await _seed_interval_plane(repository, minute_offset)
    actual = await repository.get_pnl_fx_rate_exchanges(
        [("CAD", "CHF")],
        _M - timedelta(minutes=3),
        _M + timedelta(minutes=3),
        _NOW,
        open_intervals=(
            (_M - timedelta(minutes=2), _M - timedelta(minutes=1)),
            (_M + timedelta(minutes=1), _M + timedelta(minutes=2)),
        ),
    )
    assert actual == ([("CAD", "CHF", "walutomat")] if eligible else [])


@pytest.mark.parametrize(
    ("minute_offset", "eligible"), [(-2, False), (-1, True), (1, True), (2, False)]
)
async def test_discovery_intervals_cannot_expand_outer_bounds(
    repository: SQLAlchemyRepository,
    minute_offset: int,
    eligible: bool,
) -> None:
    """Keep the original inclusive outer bounds even when an interval extends farther.

    Given: A requested interval surrounds the complete discovery range.
    When: A candle falls just outside or exactly on that range's endpoints.
    Then: The outer bounds still exclude outside evidence and retain both endpoints.
    """
    await _seed_interval_plane(repository, minute_offset)
    actual = await repository.get_pnl_fx_rate_exchanges(
        [("CAD", "CHF")],
        _M - timedelta(minutes=1),
        _M + timedelta(minutes=1),
        _NOW,
        open_intervals=((_M - timedelta(minutes=3), _M + timedelta(minutes=3)),),
    )
    assert actual == ([("CAD", "CHF", "walutomat")] if eligible else [])


async def test_empty_discovery_intervals_retain_original_range(
    repository: SQLAlchemyRepository,
) -> None:
    """An empty interval sequence preserves the original full-range evidence read.

    Given: A valid plane has evidence anywhere inside the requested range.
    When: Discovery receives no interval restriction explicitly or by default.
    Then: Both calls return the same original ordered plane set.
    """
    await _seed_interval_plane(repository, 0)
    default = await repository.get_pnl_fx_rate_exchanges([("CAD", "CHF")], _M, _M, _NOW)
    explicit = await repository.get_pnl_fx_rate_exchanges(
        [("CAD", "CHF")], _M, _M, _NOW, open_intervals=()
    )
    assert explicit == default == [("CAD", "CHF", "walutomat")]
