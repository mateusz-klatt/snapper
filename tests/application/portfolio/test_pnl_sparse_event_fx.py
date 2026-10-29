"""Sparse event evidence must not materialize unrelated dense FX minutes."""

from collections.abc import AsyncIterator
from collections.abc import Sequence
from datetime import datetime
from datetime import timedelta
from pathlib import Path
from typing import cast

import pytest
from sqlalchemy import create_engine
from sqlalchemy import update

from snapper.application.portfolio.fx_conversion_shadow import FxShadowPinContext
from snapper.application.portfolio.fx_conversion_shadow import activate_fx_shadow_context
from snapper.application.portfolio.fx_rates import currency_pair_key
from snapper.application.portfolio.pnl_timeline_service import _load_request_fx_rates
from snapper.application.portfolio.pnl_timeline_service import _sparse_event_fx_intervals
from snapper.application.portfolio.pnl_timeline_service import build_wallet_pnl_series
from snapper.data.models import Candle
from snapper.data.models import Instrument
from snapper.data.models import Symbol
from snapper.data.repository import Repository
from snapper.data.repository import SQLAlchemyRepository
from snapper.data.repository_types import PnlFxRatePlane
from snapper.data.repository_types import PnlFxRateRow
from tests.application.portfolio.test_pnl_timeline_service import _I1
from tests.application.portfolio.test_pnl_timeline_service import _W1
from tests.application.portfolio.test_pnl_timeline_service import FakeRepo
from tests.application.portfolio.test_pnl_timeline_service import _candle
from tests.application.portfolio.test_pnl_timeline_service import _exec_row
from tests.application.portfolio.test_pnl_timeline_service import _fx_row
from tests.application.portfolio.test_pnl_timeline_service import _m
from tests.application.portfolio.test_pnl_timeline_service import _ref
from tests.data.test_pnl_fx_rate_repository import _candle as _fx_candle
from tests.data.test_pnl_fx_rate_repository import _instrument
from tests.data.test_pnl_fx_rate_repository import _symbol

_SEVEN_DAYS = 7 * 24 * 60
_OLD_MINUTE = -_SEVEN_DAYS


class _SparseEventRepo(FakeRepo):
    """Exercise real FX SQL while recording the rows it actually returns."""

    def __init__(self, repository: SQLAlchemyRepository, *, late_correction: bool = False) -> None:
        """Place two foreign-price fills seven days apart before one chart point."""
        super().__init__(
            executions=[
                _exec_row(_I1, 1, _OLD_MINUTE, "buy", 1.0, 10.0, 0.0, "EUR"),
                _exec_row(_I1, 2, 0, "buy", 1.0, 11.0, 0.0, "EUR"),
            ],
            refs=[_ref(_I1, "XRP-EUR", "EUR", valid_from=_m(_OLD_MINUTE - 100))],
            candles=[
                _candle(_m(minute - 1), 12.0)
                for minute in (range(-60, 1) if late_correction else (0,))
            ],
            fx_repository=repository,
        )
        self.returned_fx_rows: list[int] = []
        self.fx_database = repository
        self.late_correction = late_correction

    async def get_pnl_fx_rate_candles(
        self,
        planes: Sequence[PnlFxRatePlane],
        start: datetime,
        end: datetime,
        as_of: datetime,
        *,
        open_intervals: Sequence[tuple[datetime, datetime]] = (),
    ) -> list[PnlFxRateRow]:
        """Record materialized row counts without substituting the query result."""
        rows = await super().get_pnl_fx_rate_candles(
            planes, start, end, as_of, open_intervals=open_intervals
        )
        self.returned_fx_rows.append(len(rows))
        if self.late_correction and len(self.returned_fx_rows) == 1:
            await _publish_backdated_correction(self.fx_database)
        return rows


async def _publish_backdated_correction(repository: SQLAlchemyRepository) -> None:
    """Commit a real backdated FX correction after the mark read completes."""
    async with repository.session() as session:
        await session.execute(
            update(Candle)
            .where(
                Candle.instrument_public_id == "ins-sparse-eurpln",
                Candle.open_at == _m(-61),
            )
            .values(known_to=_m(-30))
        )
        replacement = _fx_candle("ins-sparse-eurpln", _m(-61), 5.0)
        replacement.timestamp = _m(-30)
        session.add(replacement)
        await session.commit()


@pytest.fixture
async def dense_fx_repository(tmp_path: Path) -> AsyncIterator[SQLAlchemyRepository]:
    """Store every real one-minute FX candle across the seven-day event span."""
    database = tmp_path / "dense-event-fx.db"
    schema = create_engine(f"sqlite:///{database}")
    Symbol.__table__.create(schema)
    Instrument.__table__.create(schema)
    Candle.__table__.create(schema)
    schema.dispose()
    repository = SQLAlchemyRepository(f"sqlite+aiosqlite:///{database}")
    async with repository.session() as session:
        session.add_all(
            [
                _symbol(
                    "sym-sparse-eurpln",
                    "EUR-PLN",
                    "EUR",
                    "PLN",
                    timestamp=_m(_OLD_MINUTE - 100),
                ),
                _instrument(
                    "ins-sparse-eurpln",
                    "sym-sparse-eurpln",
                    "walutomat",
                    timestamp=_m(_OLD_MINUTE - 100),
                ),
            ]
        )
        await session.flush()
        session.add_all(
            _fx_candle("ins-sparse-eurpln", _m(minute), 4.0)
            for minute in range(_OLD_MINUTE - 21, 1)
        )
        await session.commit()
    try:
        yield repository
    finally:
        await repository.engine.dispose()


@pytest.mark.parametrize("shadow", [False, True])
async def test_sparse_event_sql_keeps_two_carry_windows_and_exact_series(
    dense_fx_repository: SQLAlchemyRepository,
    shadow: bool,
) -> None:
    """Event evidence excludes the unrelated dense minutes between two fills.

    Given: Two foreign-price fills seven days apart and every intervening FX minute.
    When: One chart point reconstructs both execution prices through real SQLite reads.
    Then: Reads remain separate and only non-shadow consumers prune unrelated event rows.
    """
    repository = _SparseEventRepo(dense_fx_repository)
    context = FxShadowPinContext(calculation_version="5B.2", evaluations=[]) if shadow else None
    with activate_fx_shadow_context(context):
        result = await build_wallet_pnl_series(
            cast(Repository, repository), _W1, "live", _m(0), _m(0), "1m", _m(1), "PLN"
        )
    point = result.points[0]
    assert len(result.points) == 1
    assert (
        point.realized_pnl,
        point.fee_pnl,
        point.accrual_pnl,
        point.unrealized_pnl,
        point.net_pnl,
        point.valuation_status,
        point.incompleteness_reasons,
    ) == (0.0, 0.0, 0.0, 12.0, 12.0, "complete", ())
    assert repository.fx_candidate_range_calls == [
        (_m(-21), _m(0)),
        (_m(_OLD_MINUTE - 21), _m(0)),
    ]
    assert repository.fx_range_calls == repository.fx_candidate_range_calls
    assert repository.fx_candidate_pair_calls == [
        [("EUR", "PLN"), ("PLN", "EUR")],
        [("EUR", "PLN"), ("PLN", "EUR")],
    ]
    assert repository.returned_fx_rows == [22, 10102 if shadow else 44]
    if context is not None:
        assert len(context.evaluations) == 1
        assert len(context.evaluations[0].rows) == 10124
        assert context.evaluations[0].required_minutes == frozenset({_m(_OLD_MINUTE), _m(0)})


@pytest.mark.parametrize("shadow", [False, True])
async def test_mark_only_backdated_correction_between_reads_stays_withheld(
    dense_fx_repository: SQLAlchemyRepository,
    shadow: bool,
) -> None:
    """Pruned event evidence retains cross-read conflicts at mark-only minutes.

    Given: A mark read sees an old FX close before a backdated correction commits.
    When: The later event read uses the same knowledge horizon and sees that correction.
    Then: Both sparse and original shadow reads withhold the conflicting mark-only minute.
    """
    repository = _SparseEventRepo(dense_fx_repository, late_correction=True)
    context = FxShadowPinContext(calculation_version="5B.2", evaluations=[]) if shadow else None
    with activate_fx_shadow_context(context):
        result = await build_wallet_pnl_series(
            cast(Repository, repository), _W1, "live", _m(-60), _m(0), "1m", _m(1), "PLN"
        )
    point = result.points[0]
    assert point.point_time == _m(-60)
    assert point.valuation_status == "incomplete"
    assert point.unrealized_pnl is None
    assert point.net_pnl is None
    assert [reason.reason for reason in point.incompleteness_reasons] == ["fx_conversion_unproven"]
    assert result.points[-1].net_pnl == 12.0
    assert repository.returned_fx_rows == [82, 10102 if shadow else 104]


@pytest.mark.parametrize(
    ("minutes", "expected"),
    [
        ((), ()),
        ((0,), ()),
        ((0, 22), ()),
        ((0, 23), ((-21, 0), (2, 23))),
        ((0, 1, 60), ((-21, 1), (39, 60))),
    ],
)
def test_sparse_intervals_preserve_dense_fallback_and_exact_carry_bounds(
    minutes: tuple[int, ...], expected: tuple[tuple[int, int], ...]
) -> None:
    """Conservative intervals preserve existing dense queries and exact carry bounds.

    Given: Empty, dense, adjacent, or genuinely separated requirement minutes.
    When: The bounded event predicate is planned without querying a database.
    Then: Only disjoint reads gain intervals and every lower bound retains 21 minutes.
    """
    actual = _sparse_event_fx_intervals(
        {currency_pair_key("EUR", "PLN"): {_m(minute) for minute in minutes}}
    )
    assert actual == tuple((_m(lower), _m(upper)) for lower, upper in expected)


@pytest.mark.parametrize("count", [16, 17])
def test_fragmented_event_requirements_have_a_fixed_interval_ceiling(count: int) -> None:
    """Fragmented inputs cannot create an unbounded SQL predicate.

    Given: Sixteen or seventeen disjoint event carry windows.
    When: A sparse predicate is planned.
    Then: Sixteen intervals remain bounded and the seventeenth restores the original read.
    """
    intervals = _sparse_event_fx_intervals(
        {currency_pair_key("EUR", "PLN"): {_m(index * 60) for index in range(count)}}
    )
    assert len(intervals) == (16 if count == 16 else 0)
    if intervals:
        assert intervals[0] == (_m(-21), _m(0))
        assert intervals[-1] == (_m(879), _m(900))


def test_fractional_pair_requirements_keep_their_original_datetime_bounds() -> None:
    """Interval pruning preserves subminute precision across the global pair union.

    Given: Different currency pairs need fractional minutes in separated windows.
    When: Their conservative event intervals are constructed.
    Then: Bounds retain every original microsecond instead of rounding evidence away.
    """
    first = _m(0) + timedelta(microseconds=250_000)
    second = _m(60) + timedelta(microseconds=750_000)
    intervals = _sparse_event_fx_intervals(
        {
            currency_pair_key("EUR", "PLN"): {first},
            currency_pair_key("GBP", "USD"): {second},
        }
    )
    assert intervals == (
        (_m(-21) + timedelta(microseconds=250_000), first),
        (_m(39) + timedelta(microseconds=750_000), second),
    )


@pytest.mark.parametrize("age", [20, 21])
async def test_sparse_owned_evidence_retains_the_existing_carry_boundary(age: int) -> None:
    """Sparse SQL bounds cannot extend canonical plane election freshness.

    Given: Two separated event windows and one canonical close at age twenty or twenty-one.
    When: The sparse event read loads evidence without global candidate discovery.
    Then: Only the original twenty-minute boundary can make the owned plane electable.
    """
    pair = currency_pair_key("EUR", "PLN")
    plane = ("EUR", "PLN", "walutomat")
    repository = FakeRepo(fx_rows=[_fx_row("EUR", "PLN", -age, 4.0, "walutomat")])
    rates, selected, used = await _load_request_fx_rates(
        cast(Repository, repository),
        {},
        {"owner": {pair: {_m(_OLD_MINUTE), _m(0)}}},
        {"owner": plane},
        _m(1),
        "EUR",
    )
    assert repository.fx_candidate_pair_calls == []
    assert repository.fx_range_calls == [(_m(_OLD_MINUTE - 21), _m(0))]
    assert selected == {"owner": {pair: plane} if age == 20 else {}}
    assert used == ({(pair, plane)} if age == 20 else set())
    assert bool(rates) is (age == 20)
