"""Carried valuation serves real earlier evidence and discloses every use of it."""

from collections.abc import Sequence
from dataclasses import replace
from datetime import datetime
from datetime import timedelta
from typing import cast

import pytest

from snapper.application.portfolio import pnl_timeline_service
from snapper.application.portfolio.pnl_carry import CARRY_ANCHOR_LIMIT
from snapper.application.portfolio.pnl_read_timing import observe_pnl_read
from snapper.application.portfolio.pnl_timeline import PnlCarriedInput
from snapper.application.portfolio.pnl_timeline import PnlTimelinePoint
from snapper.application.portfolio.pnl_timeline import canonical_carried_inputs
from snapper.application.portfolio.pnl_timeline_service import PnlSeriesReadPolicy
from snapper.application.portfolio.pnl_timeline_service import PnlWalletSeriesResult
from snapper.application.portfolio.pnl_timeline_service import _election_fx_minutes
from snapper.application.portfolio.pnl_timeline_service import _load_gap_predecessors
from snapper.application.portfolio.pnl_timeline_service import _record_carry_timing
from snapper.application.portfolio.pnl_timeline_service import build_wallet_pnl_series
from snapper.data.repository import Repository
from snapper.data.repository_types import InstrumentSymbolRefRow
from snapper.data.repository_types import PnlFxRatePlane
from snapper.data.repository_types import PnlTimelineCandleRow
from tests.application.portfolio.test_pnl_timeline_service import _I1
from tests.application.portfolio.test_pnl_timeline_service import _W1
from tests.application.portfolio.test_pnl_timeline_service import FakeRepo
from tests.application.portfolio.test_pnl_timeline_service import _accrual_row
from tests.application.portfolio.test_pnl_timeline_service import _candle
from tests.application.portfolio.test_pnl_timeline_service import _exec_row
from tests.application.portfolio.test_pnl_timeline_service import _fx_row
from tests.application.portfolio.test_pnl_timeline_service import _m
from tests.application.portfolio.test_pnl_timeline_service import _ref

_CARRY = PnlSeriesReadPolicy(carry_valuation=True)
_EXACT = PnlSeriesReadPolicy()
_KRAKEN_LABEL = "EUR/USD@kraken"


class _RangeRepo(FakeRepo):
    """Repository double whose mark read honours its bounds like the real query."""

    async def get_pnl_timeline_candles(
        self,
        refs: Sequence[InstrumentSymbolRefRow],
        start: datetime,
        end: datetime,
        as_of: datetime,
    ) -> list[PnlTimelineCandleRow]:
        """Return only candles whose open time lies inside the requested range."""
        rows = await super().get_pnl_timeline_candles(refs, start, end, as_of)
        return [row for row in rows if start <= row["open_at"] <= end]


def _candles(*closes: tuple[int, float | None]) -> list[PnlTimelineCandleRow]:
    """Build mark candles from ``(closing grid minute, close)`` pairs."""
    return [_candle(_m(minute - 1), close) for minute, close in closes]


async def _series(
    repo: FakeRepo,
    window: tuple[int, int],
    *,
    policy: PnlSeriesReadPolicy = _CARRY,
    valuation_ccy: str = "USD",
) -> PnlWalletSeriesResult:
    """Build one minute series over a relative window."""
    start, end = window
    return await build_wallet_pnl_series(
        cast(Repository, repo),
        _W1,
        "live",
        _m(start),
        _m(end),
        "1m",
        _m(end + 1),
        valuation_ccy,
        policy=policy,
    )


def _usd_position(candles: list[PnlTimelineCandleRow]) -> _RangeRepo:
    """Hold one USD-quoted instrument from minute one onward."""
    return _RangeRepo(
        executions=[_exec_row(_I1, 1, 1, "buy", 2.0, 100.0, 0.0, "USD")],
        refs=[_ref(_I1, "BTC-USD", "USD")],
        candles=candles,
    )


def _eur_position(candles: list[PnlTimelineCandleRow], fx_rows: list) -> _RangeRepo:
    """Hold one EUR-quoted instrument from minute one, valued in USD."""
    return _RangeRepo(
        executions=[_exec_row(_I1, 1, 1, "buy", 2.0, 100.0, 0.0, "USD")],
        refs=[_ref(_I1, "BTC-EUR", "EUR")],
        candles=candles,
        fx_rows=fx_rows,
    )


def _point(result: PnlWalletSeriesResult, minute: int) -> PnlTimelinePoint:
    """Return the series point at a relative grid minute."""
    return next(point for point in result.points if point.point_time == _m(minute))


def _reasons(point: PnlTimelinePoint) -> set[str]:
    """Return the reason codes stamped on a point."""
    return {entry.reason for entry in point.incompleteness_reasons}


async def test_held_minute_without_a_bar_values_at_its_last_close() -> None:
    """A gap in an instrument's candles no longer withholds the point.

    Given: A held USD instrument whose candles stop after minute two.
    When: The carried series reaches minutes three and four.
    Then: Both are complete at the last close and disclose the mark's source and age.
    """
    result = await _series(_usd_position(_candles((1, 100.0), (2, 110.0))), (1, 4))
    assert _point(result, 2).carried_inputs == ()
    for minute, age in ((3, 1), (4, 2)):
        point = _point(result, minute)
        assert point.valuation_status == "complete"
        assert point.unrealized_pnl == pytest.approx(20.0)
        assert point.carried_inputs == (PnlCarriedInput(_I1, "mark", _m(2), age),)
        assert point.max_carry_age_minutes == age


async def test_exact_only_policy_keeps_the_previous_behavior() -> None:
    """A read that does not ask for carry still withholds and discloses nothing.

    Given: The same gap read under the default policy.
    When: The series reaches a minute without a bar.
    Then: The point is incomplete for the mark as before.
    """
    result = await _series(_usd_position(_candles((1, 100.0), (2, 110.0))), (1, 3), policy=_EXACT)
    point = _point(result, 3)
    assert point.valuation_status == "incomplete"
    assert _reasons(point) == {"mark_unavailable"}
    assert point.carried_inputs == ()


async def test_healthy_evidence_is_bit_identical_and_costs_no_extra_read() -> None:
    """Exact evidence yields the same numbers with no additional query.

    Given: Every held minute has its own candle and FX rate.
    When: Carried and exact-only series are built.
    Then: Their points are equal and each issues the same reads.
    """
    candles = _candles(*((minute, 100.0 + minute) for minute in range(1, 6)))
    fx = [_fx_row("EUR", "USD", minute, 1.1 + minute / 100) for minute in range(1, 6)]
    carried_repo = _eur_position(candles, fx)
    exact_repo = _eur_position(candles, fx)
    carried = await _series(carried_repo, (1, 5))
    exact = await _series(exact_repo, (1, 5), policy=_EXACT)
    assert carried.points == exact.points
    assert all(point.carried_inputs == () for point in carried.points)
    assert len(carried_repo.candle_calls) == len(exact_repo.candle_calls)
    assert len(carried_repo.fx_range_calls) == len(exact_repo.fx_range_calls)


@pytest.mark.parametrize(
    ("close_age", "expected_status", "expected_reasons"),
    [(1440, "complete", set[str]()), (1441, "incomplete", {"mark_unavailable"})],
)
async def test_mark_bound_is_one_day(
    close_age: int,
    expected_status: str,
    expected_reasons: set[str],
) -> None:
    """An instrument silent for over a day has no defensible price.

    Given: A held position whose only close is at or just past the bound.
    When: The series values the minute.
    Then: The bound decides between a carried value and an honest gap.
    """
    repo = _RangeRepo(
        executions=[_exec_row(_I1, 1, -2000, "buy", 2.0, 100.0, 0.0, "USD")],
        refs=[_ref(_I1, "BTC-USD", "USD", valid_from=_m(-4000))],
        candles=_candles((10 - close_age, 120.0)),
    )
    point = _point(await _series(repo, (10, 10)), 10)
    assert point.valuation_status == expected_status
    assert _reasons(point) == expected_reasons


async def test_leading_gap_widens_reads_and_stops_at_the_first_usable_close() -> None:
    """A gap at the window start is one bounded search, not one per minute.

    Given: A held position whose latest close is four hours before the window.
    When: Three consecutive minutes need it.
    Then: The one-hour window misses, the six-hour window serves every minute.
    """
    repo = _RangeRepo(
        executions=[_exec_row(_I1, 1, -1000, "buy", 2.0, 100.0, 0.0, "USD")],
        refs=[_ref(_I1, "BTC-USD", "USD")],
        candles=_candles((-240, 130.0)),
    )
    result = await _series(repo, (0, 2))
    assert len(repo.candle_calls) == 3
    assert [point.max_carry_age_minutes for point in result.points] == [240, 241, 242]


async def test_no_usable_close_in_any_window_stays_withheld() -> None:
    """Exhausting the widening reads is an honest refusal.

    Given: A held position with no candle anywhere and an unusable old close.
    When: The series values the window.
    Then: Every planned window is read and the point stays mark-incomplete.
    """
    repo = _RangeRepo(
        executions=[_exec_row(_I1, 1, -1000, "buy", 2.0, 100.0, 0.0, "USD")],
        refs=[_ref(_I1, "BTC-USD", "USD")],
        candles=_candles((-30, 0.0)),
    )
    result = await _series(repo, (0, 0))
    assert len(repo.candle_calls) == 4
    assert _reasons(_point(result, 0)) == {"mark_unavailable"}


async def test_invalid_exact_close_is_not_covered_by_an_older_price() -> None:
    """A bar that printed an unusable close stays withheld (rule 4).

    Given: A held minute whose own bar has a zero close and a good earlier close.
    When: The series values that minute.
    Then: It is incomplete and the next minute without a bar carries normally.
    """
    result = await _series(_usd_position(_candles((1, 100.0), (2, 110.0), (3, 0.0))), (1, 4))
    assert _reasons(_point(result, 3)) == {"mark_unavailable"}
    assert _point(result, 4).carried_inputs == (PnlCarriedInput(_I1, "mark", _m(2), 2),)


async def test_exact_mark_with_a_missing_rate_carries_the_fx_only() -> None:
    """Only the input that is actually older gets disclosed.

    Given: Exact candles and an FX plane that stops at minute two.
    When: Minute three is valued.
    Then: The mark is exact and the rate is carried from its plane.
    """
    candles = _candles((1, 100.0), (2, 100.0), (3, 120.0))
    fx = [_fx_row("EUR", "USD", 1, 1.1), _fx_row("EUR", "USD", 2, 1.5)]
    result = await _series(_eur_position(candles, fx), (1, 3))
    point = _point(result, 3)
    assert point.valuation_status == "complete"
    assert point.unrealized_pnl == pytest.approx(2.0 * (120.0 * 1.5 - 110.0))
    assert point.carried_inputs == (PnlCarriedInput(_I1, "fx", _m(2), 1, _KRAKEN_LABEL),)


async def test_carried_mark_uses_the_rate_at_the_valued_minute() -> None:
    """A carried price converts at today's rate, not at the price's own minute.

    Given: A carried close with a different FX rate published at the valued minute.
    When: The minute is valued.
    Then: The current rate applies and only the mark is disclosed.
    """
    candles = _candles((1, 100.0), (2, 120.0))
    fx = [
        _fx_row("EUR", "USD", 1, 1.1),
        _fx_row("EUR", "USD", 2, 1.2),
        _fx_row("EUR", "USD", 3, 2.0),
    ]
    point = _point(await _series(_eur_position(candles, fx), (1, 3)), 3)
    assert point.unrealized_pnl == pytest.approx(2.0 * (120.0 * 2.0 - 110.0))
    assert point.carried_inputs == (PnlCarriedInput(_I1, "mark", _m(2), 1),)


async def test_conflicted_exact_rate_stays_withheld() -> None:
    """Two disagreeing closes at a minute are evidence, not a gap (rule 4).

    Given: Conflicting closes at minute three and a valid rate one minute earlier.
    When: Minute three is valued.
    Then: The FX failure is reported and the earlier rate is not used.
    """
    candles = _candles((1, 100.0), (2, 100.0), (3, 120.0))
    fx = [
        _fx_row("EUR", "USD", 1, 1.1),
        _fx_row("EUR", "USD", 2, 1.2),
        _fx_row("EUR", "USD", 3, 1.3),
        _fx_row("EUR", "USD", 3, 1.4),
    ]
    point = _point(await _series(_eur_position(candles, fx), (1, 3)), 3)
    assert _reasons(point) == {"fx_conversion_unproven"}


async def test_carried_event_rates_do_not_poison_the_instrument_and_stay_disclosed() -> None:
    """A flow converted by carry is valued, and later cumulatives keep saying so (rule 3).

    Given: A EUR fill, EUR fee and EUR accrual at minutes without their own rate.
    When: The series reaches later minutes.
    Then: No point is untrusted and the event's carried rate stays disclosed.
    """
    repo = _RangeRepo(
        executions=[_exec_row(_I1, 1, 2, "buy", 2.0, 100.0, 1.0, "EUR")],
        accruals=[_accrual_row(_I1, 3, 0.5, "EUR")],
        refs=[_ref(_I1, "BTC-EUR", "EUR")],
        candles=_candles(*((minute, 100.0) for minute in range(0, 6))),
        fx_rows=[_fx_row("EUR", "USD", 0, 1.1), _fx_row("EUR", "USD", 5, 1.3)],
    )
    result = await _series(repo, (0, 5))
    for point in result.points:
        assert point.valuation_status == "complete"
    later = _point(result, 4)
    events = [item for item in later.carried_inputs if item.role == "event"]
    assert events == [PnlCarriedInput(_I1, "event", _m(0), 3, _KRAKEN_LABEL)]
    assert later.realized_pnl is not None
    assert later.fee_pnl == pytest.approx(-1.1)
    assert later.accrual_pnl == pytest.approx(-0.55)
    last = _point(result, 5)
    assert [item.role for item in last.carried_inputs] == ["event"]


async def test_event_rate_beyond_the_bound_keeps_the_existing_refusal() -> None:
    """Carry does not repair a flow with no rate inside the bound.

    Given: A foreign fee whose only rate is older than four days.
    When: The series reaches the fill.
    Then: The instrument is untrusted for the FX reason as before.
    """
    repo = _RangeRepo(
        executions=[_exec_row(_I1, 1, 2, "buy", 2.0, 100.0, 1.0, "EUR")],
        refs=[_ref(_I1, "BTC-USD", "USD")],
        candles=_candles((1, 100.0), (2, 100.0), (3, 100.0)),
        fx_rows=[_fx_row("EUR", "USD", 2 - 5761, 1.1)],
    )
    point = _point(await _series(repo, (1, 3)), 3)
    assert _reasons(point) == {"fx_conversion_unproven"}


async def test_one_rate_per_day_values_every_point_and_flags_it() -> None:
    """The operator's coarse-FX requirement holds across a leading gap.

    Given: One FX rate per UTC day and exact own candles for ten hours.
    When: A window starting ten hours after the first rate is built.
    Then: Every point is complete, every point is flagged, and the planes are reported.
    """
    candles = _candles(*((minute, 100.0) for minute in range(600, 661)))
    fx = [_fx_row("EUR", "USD", 0, 1.1)]
    repo = _RangeRepo(
        executions=[_exec_row(_I1, 1, 590, "buy", 2.0, 100.0, 0.0, "USD")],
        refs=[_ref(_I1, "BTC-EUR", "EUR")],
        candles=candles,
        fx_rows=fx,
    )
    result = await _series(repo, (600, 660))
    assert len(result.points) == 61
    for offset, point in enumerate(result.points):
        assert point.valuation_status == "complete"
        assert point.carried_inputs == (
            PnlCarriedInput(_I1, "event", _m(0), 590, _KRAKEN_LABEL),
            PnlCarriedInput(_I1, "fx", _m(0), 600 + offset, _KRAKEN_LABEL),
        )
    assert [source.exchange for source in result.rate_sources] == ["kraken"]


async def test_carried_minutes_never_move_the_exact_plane_election() -> None:
    """A plane with many carried-only minutes cannot displace the exact winner.

    Given: Alpha covers both exact mark minutes, beta one of them plus many carried ones.
    When: Carried marks raise the required minutes.
    Then: Alpha is still elected and exact minutes use its rate.
    """
    alpha: PnlFxRatePlane = ("EUR", "USD", "alpha")
    candles = _candles((1, 100.0), (2, 100.0))
    fx = [
        _fx_row("EUR", "USD", 1, 1.1, "alpha"),
        _fx_row("EUR", "USD", 2, 1.2, "alpha"),
        _fx_row("EUR", "USD", 1, 9.0, "beta"),
        *(_fx_row("EUR", "USD", minute, 9.0, "beta") for minute in range(3, 11)),
    ]
    repo = _eur_position(candles, fx)
    result = await _series(repo, (1, 10))
    assert [source.exchange for source in result.rate_sources] == [alpha[2]]
    assert _point(result, 2).carried_inputs == ()
    assert _point(result, 2).unrealized_pnl == pytest.approx(2.0 * (100.0 * 1.2 - 110.0))


async def test_pair_with_only_old_rates_pins_the_freshest_plane() -> None:
    """A pair with no nearby evidence still elects one disclosed plane.

    Given: Two planes of the pair whose only closes are far before the window.
    When: The carried election runs.
    Then: The fresher plane is pinned, reported and used.
    """
    candles = _candles((1, 100.0), (2, 100.0))
    fx = [_fx_row("EUR", "USD", -500, 1.1, "alpha"), _fx_row("EUR", "USD", -100, 1.5, "beta")]
    repo = _RangeRepo(
        executions=[_exec_row(_I1, 1, 1, "buy", 2.0, 100.0, 0.0, "USD")],
        refs=[_ref(_I1, "BTC-EUR", "EUR")],
        candles=candles,
        fx_rows=fx,
    )
    result = await _series(repo, (1, 2))
    assert [source.exchange for source in result.rate_sources] == ["beta"]
    assert _point(result, 2).carried_inputs == (
        PnlCarriedInput(_I1, "event", _m(-100), 101, "EUR/USD@beta"),
        PnlCarriedInput(_I1, "fx", _m(-100), 102, "EUR/USD@beta"),
    )


async def test_owned_pair_with_only_old_rates_pins_its_own_plane() -> None:
    """An instrument's own series stays its only candidate under carry.

    Given: A held EUR-USD valued in EUR, with its own plane old and a rival newer.
    When: The carried election runs.
    Then: The instrument's own plane is pinned rather than the rival.
    """
    repo = _RangeRepo(
        executions=[_exec_row(_I1, 1, 1, "buy", 2.0, 1.1, 0.0, "USD")],
        refs=[_ref(_I1, "EUR-USD", "USD", base="EUR")],
        candles=_candles((1, 1.1), (2, 1.1)),
        fx_rows=[
            _fx_row("EUR", "USD", -500, 1.05, "kraken"),
            _fx_row("EUR", "USD", -100, 1.5, "rival"),
        ],
    )
    result = await _series(repo, (1, 2), valuation_ccy="EUR")
    assert {source.exchange for source in result.rate_sources} == {"kraken"}
    assert _point(result, 2).valuation_status == "complete"


async def test_pair_without_any_plane_stays_withheld() -> None:
    """Carry cannot invent a rate for a pair with no plane at all.

    Given: A EUR-quoted position and no FX rows.
    When: The carried election finds no candidate.
    Then: The mark conversion failure is reported.
    """
    point = _point(await _series(_eur_position(_candles((1, 100.0), (2, 100.0)), []), (1, 2)), 2)
    assert _reasons(point) == {"fx_conversion_unproven"}


async def test_candidate_without_any_evidence_cannot_be_pinned() -> None:
    """A discovered plane with nothing inside the bound stays unpinned.

    Given: A candidate plane whose only close is older than the bound.
    When: The carried election searches every window.
    Then: Nothing is pinned and the conversion failure is reported.
    """
    repo = _eur_position(
        _candles((1, 100.0), (2, 100.0)),
        [_fx_row("EUR", "USD", -6000, 1.1)],
    )
    point = _point(await _series(repo, (1, 2)), 2)
    assert _reasons(point) == {"fx_conversion_unproven"}


async def test_owned_pair_without_evidence_searches_every_window_then_refuses() -> None:
    """An owned plane with no usable rate is searched once, to the bound, and refused.

    Given: A held EUR-USD valued in EUR whose own plane has no rates at all.
    When: The carried election searches behind the gap.
    Then: All four widening windows are read and the conversion stays withheld.
    """
    repo = _RangeRepo(
        executions=[_exec_row(_I1, 1, 1, "buy", 2.0, 1.1, 0.0, "USD")],
        refs=[_ref(_I1, "EUR-USD", "USD", base="EUR")],
        candles=_candles((1, 1.1), (2, 1.1)),
    )
    result = await _series(repo, (1, 2), valuation_ccy="EUR")
    assert _reasons(_point(result, 2)) == {"fx_conversion_unproven"}
    assert repo.fx_range_calls[-4][1] == _m(1) - timedelta(minutes=2)
    assert repo.fx_range_calls[-1][0] == _m(1) - timedelta(minutes=1 + 96 * 60)


def test_carried_minutes_vote_only_on_pairs_without_exact_consumers() -> None:
    """A carried minute may elect a plane only where no exact minute depends on one.

    Given: One pair with an exact consumer and another with only carried minutes.
    When: Voting minutes are computed.
    Then: The first keeps its exact minutes alone and the second votes.
    """
    exact_pair = ("EUR", "USD")
    carried_pair = ("EUR", "PLN")
    exact = {"a": {exact_pair: {_m(1)}}}
    carried = {"b": {exact_pair: {_m(2)}, carried_pair: {_m(3)}}}
    assert _election_fx_minutes(exact, carried) == {
        "a": {exact_pair: {_m(1)}},
        "b": {carried_pair: {_m(3)}},
    }


async def test_gap_on_a_pinned_plane_searches_behind_it_once() -> None:
    """A pinned plane with an early gap reads behind that gap, once.

    Given: Exact rates at the end of the window and an old close before it.
    When: Minutes before the first exact rate are valued.
    Then: They carry the old close and one widening search served them.
    """
    candles = _candles(*((minute, 100.0) for minute in range(1, 41)))
    fx = [_fx_row("EUR", "USD", -300, 1.1), _fx_row("EUR", "USD", 40, 1.2)]
    repo = _eur_position(candles, fx)
    result = await _series(repo, (1, 40))
    assert _point(result, 1).carried_inputs == (
        PnlCarriedInput(_I1, "event", _m(-300), 301, _KRAKEN_LABEL),
        PnlCarriedInput(_I1, "fx", _m(-300), 301, _KRAKEN_LABEL),
    )
    assert [item.role for item in _point(result, 40).carried_inputs] == ["event"]
    assert repo.fx_range_calls[-2:] == [
        (_m(1) - timedelta(minutes=61), _m(1) - timedelta(minutes=2)),
        (_m(1) - timedelta(minutes=361), _m(1) - timedelta(minutes=62)),
    ]


async def test_a_found_close_serves_the_next_gap_without_another_search() -> None:
    """Separate gaps inside the bound of one close cost one search.

    Given: Two distant required minutes and one close before the first.
    When: The gaps are searched oldest first.
    Then: Only the first gap reads, and the first window that held a close ends it.
    """
    plane: PnlFxRatePlane = ("EUR", "USD", "kraken")
    repo = FakeRepo(fx_rows=[_fx_row("EUR", "USD", -5, 1.1)])
    rows, searched = await _load_gap_predecessors(
        cast(Repository, repo), {plane: {_m(10), _m(200)}}, [], _m(300)
    )
    assert searched == [_m(10)]
    assert len(repo.fx_range_calls) == 1
    assert [row["close"] for row in rows] == [1.1]


async def test_read_budget_stops_at_the_limit_and_warns(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Pathologically scattered evidence cannot make a request unbounded.

    Given: More unservable gaps than the read budget allows.
    When: They are searched.
    Then: The search stops at the budget, oldest first, and logs the truncation.
    """
    warnings: list[str] = []

    def capture(message: str, *args: object) -> None:
        """Keep the rendered warning instead of writing to the log sink."""
        warnings.append(message.format(*args))

    monkeypatch.setattr(pnl_timeline_service.logger, "warning", capture)
    plane: PnlFxRatePlane = ("EUR", "USD", "kraken")
    minutes = {_m(minute * 10000) for minute in range(CARRY_ANCHOR_LIMIT + 3)}
    _, searched = await _load_gap_predecessors(
        cast(Repository, FakeRepo()), {plane: minutes}, [], _m(10_000_000)
    )
    assert searched == sorted(minutes)[:CARRY_ANCHOR_LIMIT]
    assert warnings == [f"P&L FX carry gaps exceed the read budget: searched={CARRY_ANCHOR_LIMIT}"]


async def test_read_budget_spends_on_the_oldest_gaps_across_all_planes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Exhausting the budget cannot starve a plane because of its name.

    Given: A plane sorting first with later gaps and one sorting last with the oldest.
    When: A budget of two searches is spent.
    Then: Gaps are taken strictly by time across both planes.
    """
    monkeypatch.setattr(pnl_timeline_service, "CARRY_ANCHOR_LIMIT", 2)
    first: PnlFxRatePlane = ("AUD", "USD", "kraken")
    last: PnlFxRatePlane = ("EUR", "USD", "kraken")
    needed = {first: {_m(30), _m(9000)}, last: {_m(10)}}
    _, searched = await _load_gap_predecessors(cast(Repository, FakeRepo()), needed, [], _m(10_000))
    assert searched == [_m(10), _m(30)]


async def test_blocked_exact_minute_needs_no_search() -> None:
    """Contradictory exact evidence is never treated as a gap to search behind.

    Given: A conflicted close at the only required minute.
    When: Gaps are searched.
    Then: No read is issued.
    """
    plane: PnlFxRatePlane = ("EUR", "USD", "kraken")
    rows = [_fx_row("EUR", "USD", 3, 1.1), _fx_row("EUR", "USD", 3, 1.2)]
    repo = FakeRepo()
    assert await _load_gap_predecessors(cast(Repository, repo), {plane: {_m(3)}}, rows, _m(9)) == (
        [],
        [],
    )
    assert repo.fx_range_calls == []


def _timing_point(minute: int, inputs: tuple[PnlCarriedInput, ...]) -> PnlTimelinePoint:
    """Build a complete point that carries the given inputs."""
    return PnlTimelinePoint(
        point_time=_m(minute),
        realized_pnl=0.0,
        fee_pnl=0.0,
        accrual_pnl=0.0,
        unrealized_pnl=0.0,
        net_pnl=0.0,
        valuation_status="complete",
        incompleteness_reasons=(),
        per_instrument=(),
        attribution=(),
        carried_inputs=inputs,
    )


async def test_carry_counters_reach_the_read_timing_log(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Operators can see carry use from logs without API access.

    Given: A read with carried marks, carried rates and a clean point.
    When: Its timing summary is emitted.
    Then: The counters and largest age appear; an exact read records none.
    """
    messages: list[str] = []

    def capture(message: str, *args: object) -> None:
        """Keep the rendered timing summary."""
        messages.append(message.format(*args))

    monkeypatch.setattr(
        "snapper.application.portfolio.pnl_read_timing.logger.info",
        capture,
    )
    points = [
        _timing_point(1, (PnlCarriedInput(_I1, "mark", _m(0), 1),)),
        _timing_point(2, (PnlCarriedInput(_I1, "event", _m(-7), 9, _KRAKEN_LABEL),)),
        _timing_point(3, ()),
    ]

    @observe_pnl_read("series")
    async def handler(carry_valuation: bool) -> None:
        """Record one read's carry usage."""
        _record_carry_timing(points, carry_valuation=carry_valuation)

    await handler(True)
    await handler(False)
    assert "carried_mark_points=1 carried_fx_points=1 max_carry_age_minutes=9" in messages[0]
    assert "max_carry_age_minutes=unobserved" in messages[1]


def test_carried_input_must_be_older_than_its_minute() -> None:
    """An exact input can never be reported as carried.

    Given: A carried input with no age.
    When: It is constructed.
    Then: The contract refuses it.
    """
    with pytest.raises(ValueError, match="older than the minute"):
        PnlCarriedInput(_I1, "mark", _m(0), 0)


def test_canonical_inputs_keep_the_oldest_per_instrument_and_role() -> None:
    """A long history cannot inflate each point, and the worst age survives.

    Given: Several inputs for one role plus another role and instrument.
    When: They are canonicalized in any order.
    Then: One oldest entry per instrument and role remains in stable order.
    """
    older = PnlCarriedInput(_I1, "event", _m(-20), 20, "a")
    newer = PnlCarriedInput(_I1, "event", _m(-3), 3, "a")
    tied = PnlCarriedInput(_I1, "event", _m(-20), 20, "b")
    mark = PnlCarriedInput(_I1, "mark", _m(-1), 1)
    other = PnlCarriedInput("other", "fx", _m(-2), 2, "a")
    expected = (tied, mark, other)
    assert canonical_carried_inputs([older, newer, tied, mark, other]) == expected
    assert canonical_carried_inputs([other, mark, tied, newer, older]) == expected


def test_point_rejects_inputs_that_are_not_canonical() -> None:
    """The public point states each instrument and role once, sorted.

    Given: A complete point and a non-canonical input tuple.
    When: The point is rebuilt.
    Then: It is refused, and a point with no inputs has no age.
    """
    point = _timing_point(1, ())
    assert point.max_carry_age_minutes is None
    first = PnlCarriedInput(_I1, "mark", _m(0), 1)
    second = PnlCarriedInput(_I1, "fx", _m(0), 1, "a")
    with pytest.raises(ValueError, match="one per instrument and role"):
        replace(point, carried_inputs=(first, second))
