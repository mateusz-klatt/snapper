"""Shared FX elections preserve exact flow proof ahead of mark coverage."""

from typing import cast

import pytest

from snapper.application.portfolio.fx_conversion_shadow import FxShadowPinContext
from snapper.application.portfolio.fx_conversion_shadow import activate_fx_shadow_context
from snapper.application.portfolio.fx_rates import convert_amount
from snapper.application.portfolio.fx_rates import currency_pair_key
from snapper.application.portfolio.pnl_timeline_service import _load_request_fx_rates
from snapper.application.portfolio.pnl_timeline_service import _resolve_fx_planes
from snapper.data.repository import Repository
from snapper.data.repository_types import PnlFxRatePlane
from snapper.data.repository_types import PnlFxRateRow
from tests.application.portfolio.test_pnl_timeline_service import FakeRepo
from tests.application.portfolio.test_pnl_timeline_service import _fx_row
from tests.application.portfolio.test_pnl_timeline_service import _m

_PAIR = currency_pair_key("EUR", "USD")
_KRAKEN = ("EUR", "USD", "kraken")
_WALUTOMAT = ("EUR", "USD", "walutomat")
_EVENTS = tuple(range(-100, 0, 10))
_MARKS = tuple(range(31))


def _production_rows(kraken_events: int) -> list[PnlFxRateRow]:
    """Build sparse execution evidence and a much denser rival mark plane."""
    return [
        *(_fx_row("EUR", "USD", minute, 1.2, "kraken") for minute in _EVENTS[:kraken_events]),
        *(_fx_row("EUR", "USD", minute, 1.1, "walutomat") for minute in _EVENTS[:4]),
        *(_fx_row("EUR", "USD", minute, 1.1, "walutomat") for minute in _MARKS),
    ]


@pytest.mark.parametrize("reverse", [False, True])
async def test_all_ten_execution_minutes_outrank_more_mark_minutes(reverse: bool) -> None:
    """A complete execution plane wins despite a rival's much larger mark count.

    Given: Kraken proves ten execution minutes and Walutomat proves four plus 31 marks.
    When: The real bounded loader elects the shared plane in either row order.
    Then: Kraken alone supplies every event, and missing marks remain withheld.
    """
    rows = _production_rows(10)
    repo = FakeRepo(fx_rows=list(reversed(rows)) if reverse else rows)
    events = {_m(minute) for minute in _EVENTS}
    marks = {_m(minute) for minute in _MARKS}
    context = FxShadowPinContext(calculation_version="test", evaluations=[], manifest_limit=None)
    with activate_fx_shadow_context(context):
        rates, selected, used = await _load_request_fx_rates(
            cast(Repository, repo),
            {"consumer": {_PAIR: marks}},
            {"consumer": {_PAIR: events}},
            {},
            _m(200),
            "USD",
        )
    assert selected == {"consumer": {_PAIR: _KRAKEN}}
    assert used == {(_PAIR, _KRAKEN)}
    assert all(
        convert_amount(1.0, "EUR", "USD", minute, rates, selected["consumer"]) == 1.2
        for minute in events
    )
    assert convert_amount(1.0, "EUR", "USD", _m(0), rates, selected["consumer"]) is None
    assert {key[:3] for key in rates} == {_KRAKEN}
    assert context.evaluations[0].event_minutes == frozenset(events)
    assert context.evaluations[0].required_minutes == frozenset(events | marks)


async def test_no_complete_event_plane_keeps_the_combined_coverage_winner() -> None:
    """Carry coverage cannot make a nine-of-ten execution plane complete.

    Given: Kraken can carry its missing tenth minute, while Walutomat has more exact marks.
    When: No eligible plane proves the complete execution subset exactly.
    Then: Original combined coverage selects Walutomat and its missing event stays unknown.
    """
    repo = FakeRepo(fx_rows=_production_rows(9))
    events = {_m(minute) for minute in _EVENTS}
    rates, selected, used = await _load_request_fx_rates(
        cast(Repository, repo),
        {"consumer": {_PAIR: {_m(minute) for minute in _MARKS}}},
        {"consumer": {_PAIR: events}},
        {},
        _m(200),
        "USD",
    )
    assert selected == {"consumer": {_PAIR: _WALUTOMAT}}
    assert used == {(_PAIR, _WALUTOMAT)}
    assert convert_amount(1.0, "EUR", "USD", _m(-10), rates, selected["consumer"]) is None


async def test_mark_only_request_keeps_the_original_coverage_election() -> None:
    """An empty event subset cannot change a mark-only winner.

    Given: Walutomat covers more required mark minutes than Kraken.
    When: A request has no flow conversion requirements.
    Then: The original mark coverage winner remains selected.
    """
    repo = FakeRepo(fx_rows=_production_rows(10))
    marks = {_m(minute) for minute in (*_EVENTS, *_MARKS)}
    _, selected, _ = await _load_request_fx_rates(
        cast(Repository, repo), {"consumer": {_PAIR: marks}}, {}, {}, _m(200), "USD"
    )
    assert selected == {"consumer": {_PAIR: _WALUTOMAT}}


@pytest.mark.parametrize("eligible", [False, True])
def test_only_eligible_oriented_planes_can_supply_complete_event_proof(eligible: bool) -> None:
    """Loaded evidence from an ineligible complete rival cannot constrain the election.

    Given: A ten-of-ten Kraken plane and a candidate without usable evidence.
    When: Kraken is either included in or excluded from the eligible set.
    Then: Only an eligible complete plane can displace the combined coverage winner.
    """
    events = {_m(minute) for minute in _EVENTS}
    planes = {_WALUTOMAT, ("EUR", "USD", "missing")}
    if eligible:
        planes.add(_KRAKEN)
    resolved = _resolve_fx_planes(
        {_PAIR: events | {_m(minute) for minute in _MARKS}},
        {_PAIR: planes},
        _production_rows(10),
        "USD",
        {_PAIR: events},
    )
    assert resolved == {_PAIR: _KRAKEN if eligible else _WALUTOMAT}


@pytest.mark.parametrize("reverse", [False, True])
def test_conflicting_event_minute_cannot_establish_completeness(reverse: bool) -> None:
    """Conflicting closes remove exact proof regardless of publication order.

    Given: Kraken's tenth event has conflicting published closes.
    When: Election receives both conflicts in either row order.
    Then: Neither plane is event-complete and the old combined coverage winner survives.
    """
    rows = [*_production_rows(10), _fx_row("EUR", "USD", -10, 1.3, "kraken")]
    events = {_m(minute) for minute in _EVENTS}
    resolved = _resolve_fx_planes(
        {_PAIR: events | {_m(minute) for minute in _MARKS}},
        {_PAIR: {_KRAKEN, _WALUTOMAT}},
        list(reversed(rows)) if reverse else rows,
        "USD",
        {_PAIR: events},
    )
    assert resolved == {_PAIR: _WALUTOMAT}


@pytest.mark.parametrize(
    ("valuation", "quote", "marks", "planes", "expected"),
    [
        (
            "EUR",
            "PLN",
            (20, 30),
            (("EUR", "PLN", "alpha"), ("EUR", "PLN", "walutomat")),
            ("EUR", "PLN", "walutomat"),
        ),
        (
            "EUR",
            "PLN",
            (20,),
            (("EUR", "PLN", "alpha"), ("EUR", "PLN", "walutomat")),
            ("EUR", "PLN", "alpha"),
        ),
        (
            "USD",
            "USD",
            (20, 30),
            (("EUR", "USD", "alpha"), ("USD", "EUR", "alpha")),
            ("EUR", "USD", "alpha"),
        ),
        (
            "EUR",
            "USD",
            (20,),
            (("EUR", "USD", "alpha"), ("USD", "EUR", "alpha")),
            ("USD", "EUR", "alpha"),
        ),
    ],
)
def test_event_complete_existing_winner_preserves_venue_and_orientation_rules(
    valuation: str,
    quote: str,
    marks: tuple[int, ...],
    planes: tuple[PnlFxRatePlane, ...],
    expected: PnlFxRatePlane,
) -> None:
    """Already complete winners retain their exact old venue and orientation decisions.

    Given: Event-complete candidates tied on full or partial combined coverage.
    When: The same input is elected with and without an event subset.
    Then: PLN preference, lexical venue order, and direct orientation stay identical.
    """
    pair = currency_pair_key("EUR", quote)
    events = {_m(0), _m(10)}
    requirements = {pair: events | {_m(20), _m(30)}}
    candidates = {pair: set(planes)}
    rows = [
        _fx_row(base, quoted, minute, 1.2, exchange)
        for base, quoted, exchange in planes
        for minute in (0, 10, *marks)
    ]
    original = _resolve_fx_planes(requirements, candidates, rows, valuation, {})
    guarded = _resolve_fx_planes(requirements, candidates, rows, valuation, {pair: events})
    assert original == guarded == {pair: expected}


def test_combined_coverage_still_ranks_within_the_event_complete_subset() -> None:
    """Event completeness filters candidates without changing subsequent mark ranking.

    Given: Two complete flow planes with different exact mark coverage.
    When: Both candidates pass the complete-event guard.
    Then: More combined coverage beats lexical venue order as before.
    """
    rows = [
        _fx_row("EUR", "USD", 0, 1.1, "alpha"),
        _fx_row("EUR", "USD", 0, 1.2, "zeta"),
        _fx_row("EUR", "USD", 10, 1.2, "zeta"),
    ]
    candidates = {_PAIR: {("EUR", "USD", "alpha"), ("EUR", "USD", "zeta")}}
    requirements = {_PAIR: {_m(0), _m(10)}}
    original = _resolve_fx_planes(requirements, candidates, rows, "USD", {})
    guarded = _resolve_fx_planes(requirements, candidates, rows, "USD", {_PAIR: {_m(0)}})
    assert original == guarded == {_PAIR: ("EUR", "USD", "zeta")}


def test_complementary_orientations_cannot_combine_into_event_completeness() -> None:
    """One oriented plane must independently prove the entire flow subset.

    Given: Two Kraken orientations split the events and each has many extra marks.
    When: A sparse Walutomat orientation proves both event minutes exactly.
    Then: Walutomat wins instead of treating either partial Kraken plane as complete.
    """
    inverse = ("USD", "EUR", "kraken")
    rows = [
        _fx_row("EUR", "USD", 0, 1.2, "kraken"),
        _fx_row("USD", "EUR", 10, 0.8, "kraken"),
        _fx_row("EUR", "USD", 0, 1.1, "walutomat"),
        _fx_row("EUR", "USD", 10, 1.1, "walutomat"),
        *(_fx_row("EUR", "USD", minute, 1.2, "kraken") for minute in range(20, 31)),
        *(_fx_row("USD", "EUR", minute, 0.8, "kraken") for minute in range(20, 31)),
    ]
    events = {_m(0), _m(10)}
    resolved = _resolve_fx_planes(
        {_PAIR: events | {_m(minute) for minute in range(20, 31)}},
        {_PAIR: {_KRAKEN, inverse, _WALUTOMAT}},
        rows,
        "USD",
        {_PAIR: events},
    )
    assert resolved == {_PAIR: _WALUTOMAT}


async def test_owned_events_cannot_steer_a_general_consumers_plane() -> None:
    """The shared event subset excludes the canonical owner's own-pair flows.

    Given: Only Kraken proves an owner's old event, while both prove the peer event.
    When: Walutomat covers more of the peer's marks.
    Then: The peer elects Walutomat and the owner retains its Kraken identity plane.
    """
    repo = FakeRepo(
        fx_rows=[
            _fx_row("EUR", "USD", -100, 1.2, "kraken"),
            _fx_row("EUR", "USD", -10, 1.2, "kraken"),
            _fx_row("EUR", "USD", -10, 1.1, "walutomat"),
            *(_fx_row("EUR", "USD", minute, 1.1, "walutomat") for minute in range(3)),
        ]
    )
    context = FxShadowPinContext(calculation_version="test", evaluations=[])
    with activate_fx_shadow_context(context):
        _, selected, _ = await _load_request_fx_rates(
            cast(Repository, repo),
            {"peer": {_PAIR: {_m(0), _m(1), _m(2)}}},
            {"owner": {_PAIR: {_m(-100)}}, "peer": {_PAIR: {_m(-10)}}},
            {"owner": _KRAKEN},
            _m(200),
            "USD",
        )
    assert selected == {"owner": {_PAIR: _KRAKEN}, "peer": {_PAIR: _WALUTOMAT}}
    shared = [item for item in context.evaluations if item.scope_kind == "shared_pair"]
    assert len(shared) == 1
    assert shared[0].event_minutes == frozenset({_m(-10)})


async def test_complete_rival_cannot_replace_an_incomplete_owned_plane() -> None:
    """Complete flow coverage never overrides an instrument's canonical restriction.

    Given: Kraken owns the instrument but misses one flow that Walutomat proves.
    When: A general peer also needs the same pair.
    Then: The owner keeps Kraken and withholds the missing flow despite its complete rival.
    """
    repo = FakeRepo(
        fx_rows=[
            _fx_row("EUR", "USD", -100, 1.2, "kraken"),
            _fx_row("EUR", "USD", -100, 1.1, "walutomat"),
            _fx_row("EUR", "USD", -50, 1.1, "walutomat"),
            _fx_row("EUR", "USD", 0, 1.1, "walutomat"),
        ]
    )
    rates, selected, _ = await _load_request_fx_rates(
        cast(Repository, repo),
        {"peer": {_PAIR: {_m(0)}}},
        {"owner": {_PAIR: {_m(-100), _m(-50)}}},
        {"owner": _KRAKEN},
        _m(200),
        "USD",
    )
    assert selected == {"owner": {_PAIR: _KRAKEN}, "peer": {_PAIR: _WALUTOMAT}}
    assert convert_amount(1.0, "EUR", "USD", _m(-100), rates, selected["owner"]) == 1.2
    assert convert_amount(1.0, "EUR", "USD", _m(-50), rates, selected["owner"]) is None
