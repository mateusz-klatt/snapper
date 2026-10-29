"""Preserve carry election semantics while bounding published-minute scans."""

import math
from collections.abc import Iterable
from collections.abc import Iterator
from datetime import UTC
from datetime import datetime
from datetime import timedelta

import pytest

from snapper.application.portfolio import pnl_timeline_service
from snapper.application.portfolio.pnl_timeline_service import _FX_CARRY_WINDOW
from snapper.application.portfolio.pnl_timeline_service import _carried_minutes_for_plane
from snapper.application.portfolio.pnl_timeline_service import _resolve_fx_planes
from snapper.application.portfolio.pnl_timeline_service import _resolve_identity_fx_planes
from snapper.data.models import KNOWN_TO_MAX
from snapper.data.repository_types import PnlFxRatePlane
from snapper.data.repository_types import PnlFxRateRow

_T0 = datetime(2026, 10, 28, 17, 33, tzinfo=UTC)
_OWN = "00000000-0000-7000-8000-000000000101"
_STALE_OWN = "00000000-0000-7000-8000-000000000102"


def _at(seconds: int = 0, microseconds: int = 0) -> datetime:
    """Build an exact offset without rounding sub-minute timestamps."""
    return _T0 + timedelta(seconds=seconds, microseconds=microseconds)


def _original_carried_minutes(
    published: set[datetime],
    required: set[datetime],
) -> dict[datetime, int]:
    """Keep the literal deployed 5f carry scan as the independent oracle."""
    carried: dict[datetime, int] = {}
    for minute in required:
        if minute in published:
            continue
        earliest = minute - _FX_CARRY_WINDOW
        reachable = [observed for observed in published if earliest <= observed < minute]
        if reachable:
            carried[minute] = int((minute - max(reachable)).total_seconds() // 60)
    return carried


class _ObservedMinutes(set[datetime]):
    """Count visits to actual published elements without elapsed-time assertions."""

    def __init__(self, minutes: Iterable[datetime]) -> None:
        """Initialize an ordinary datetime set with a read-only work counter."""
        super().__init__(minutes)
        self.iteration_visits = 0

    def __iter__(self) -> Iterator[datetime]:
        """Count the source elements inspected by every published-set traversal."""
        for minute in super().__iter__():
            self.iteration_visits += 1
            yield minute


@pytest.mark.parametrize(
    ("published", "required", "expected"),
    [
        pytest.param(set(), {_T0}, {}, id="no-published-evidence"),
        pytest.param({_T0}, set(), {}, id="no-requirements"),
        pytest.param({_T0}, {_T0}, {}, id="exact-is-never-carried"),
        pytest.param({_at(-180), _at(-60), _at(60)}, {_T0}, {_T0: 1}, id="nearest-past"),
        pytest.param({_at(-1200)}, {_T0}, {_T0: 20}, id="inclusive-twenty-minutes"),
        pytest.param({_at(-1200, -1)}, {_T0}, {}, id="one-microsecond-too-old"),
        pytest.param({_at(0, 1)}, {_T0}, {}, id="future-is-never-used"),
        pytest.param({_at(-1199)}, {_T0}, {_T0: 19}, id="fractional-distance-is-floored"),
        pytest.param({_at(-59)}, {_T0}, {_T0: 0}, id="sub-minute-distance-is-preserved"),
        pytest.param(
            {_at(-31), _at(-15), _at(30)},
            {_at(15)},
            {_at(15): 0},
            id="fractional-requirement-keeps-nearest-predecessor",
        ),
        pytest.param(
            {_at(-1260), _at(-60), _T0, _at(120)},
            {_at(-1200), _T0, _at(60), _at(1380)},
            {_at(-1200): 1, _at(60): 1},
            id="independent-missing-exact-and-expired-minutes",
        ),
    ],
)
def test_carry_matches_original_boundary_semantics(
    published: set[datetime],
    required: set[datetime],
    expected: dict[datetime, int],
) -> None:
    """Nearest predecessor, exact precedence and the original bound stay identical.

    Given: Exact, missing, future, expired and fractional publication cases.
    When: Original and optimized carry lookup consume the same input sets.
    Then: Both return the explicit expected distances without mutating inputs.
    """
    original_published = published.copy()
    original_required = required.copy()
    assert _original_carried_minutes(published, required) == expected
    assert _carried_minutes_for_plane(published, required) == expected
    assert published == original_published
    assert required == original_required


def test_carry_preserves_original_unrepresentable_lower_bound() -> None:
    """An underflowing requested datetime still refuses instead of being accepted.

    Given: A missing requirement whose carry lower bound is unrepresentable.
    When: Original and optimized lookup calculate the required lower bound.
    Then: Both preserve the original overflow refusal.
    """
    required = {datetime.min.replace(tzinfo=UTC)}
    with pytest.raises(OverflowError):
        _original_carried_minutes(set(), required)
    with pytest.raises(OverflowError):
        _carried_minutes_for_plane(set(), required)


def test_sparse_carry_traverses_published_history_at_most_once() -> None:
    """Many missing requirements cannot multiply traversal of unrelated history.

    Given: 128 missing requirements among 8,192 published datetimes.
    When: Lookup resolves them while published-set traversal is counted.
    Then: Results match the original oracle with at most one history traversal.
    """
    published = {_at(minute * 120) for minute in range(8192)}
    required = {_at(minute * 120 + 60) for minute in range(0, 8192, 64)}
    observed = _ObservedMinutes(published)
    expected = _original_carried_minutes(published, required)
    assert len(expected) == 128
    assert set(expected.values()) == {1}
    assert _carried_minutes_for_plane(observed, required) == expected
    assert observed.iteration_visits <= len(published)


def test_exact_coverage_never_traverses_published_history() -> None:
    """A fully exact election does no carry search regardless of history size.

    Given: Every required datetime already has exact published coverage.
    When: Carry lookup receives the complete exact requirement set.
    Then: It returns no carries without traversing publication history.
    """
    published = {_at(minute * 60) for minute in range(1024)}
    observed = _ObservedMinutes(published)
    assert _carried_minutes_for_plane(observed, published.copy()) == {}
    assert observed.iteration_visits == 0


def _row(
    plane: PnlFxRatePlane,
    minute: datetime,
    close: float,
    identity: int,
) -> PnlFxRateRow:
    """Supply full frozen candle provenance for the unchanged election consumers."""
    base, quote, exchange = plane
    return {
        "base": base,
        "quote": quote,
        "exchange": exchange,
        "open_at": minute - timedelta(minutes=1),
        "close": close,
        "native_symbol": f"{base}-{quote}",
        "instrument_public_id": f"00000000-0000-7000-8000-{identity:012d}",
        "candle_id": identity,
        "candle_public_id": f"10000000-0000-7000-8000-{identity:012d}",
        "candle_session_id": "20000000-0000-7000-8000-000000000001",
        "candle_sequence_id": identity,
        "candle_timestamp": _T0,
        "candle_known_to": KNOWN_TO_MAX,
    }


@pytest.mark.parametrize("include_exact", [False, True])
def test_shared_and_owned_elections_match_original_oracle(
    monkeypatch: pytest.MonkeyPatch,
    include_exact: bool,
) -> None:
    """Invalid/conflicting rates and rival planes cannot change election ownership.

    Given: Owned and shared consumers with stale, conflicting and exact rivals.
    When: Their real election functions use original and optimized carry lookup.
    Then: Plane choices match and an expired owner cannot borrow a rival.
    """
    canonical = ("EUR", "USD", "alpha")
    stale = ("EUR", "USD", "expired")
    fresh = ("EUR", "USD", "gamma")
    conflicted = ("EUR", "USD", "beta")
    reverse = ("USD", "EUR", "alpha")
    rows = [
        _row(canonical, _at(-1080), 1.1, 1),
        _row(stale, _at(-1260), 1.2, 2),
        _row(fresh, _at(-180), 1.3, 3),
        _row(conflicted, _at(-120), 1.4, 4),
        _row(conflicted, _at(-120), 1.5, 5),
        _row(("EUR", "USD", "zero"), _T0, 0.0, 6),
        _row(("EUR", "USD", "nan"), _T0, math.nan, 7),
        _row(("EUR", "USD", "negative"), _T0, -1.0, 8),
    ]
    if include_exact:
        rows.append(_row(reverse, _T0, 0.9, 9))
    pair = ("EUR", "USD")
    minutes = {_T0, _at(60), _at(1260)}
    candidates = {pair: {canonical, stale, fresh, conflicted, reverse}}
    identities = {_OWN: canonical, _STALE_OWN: stale}
    requirements = {_OWN: {pair: minutes}, _STALE_OWN: {pair: minutes}}
    shared = _resolve_fx_planes({pair: minutes}, candidates, rows, "USD")
    owned = _resolve_identity_fx_planes(requirements, identities, rows)
    with monkeypatch.context() as original:
        original.setattr(
            pnl_timeline_service, "_carried_minutes_for_plane", _original_carried_minutes
        )
        assert shared == _resolve_fx_planes({pair: minutes}, candidates, rows, "USD")
        assert owned == _resolve_identity_fx_planes(requirements, identities, rows)
    assert shared == {pair: reverse if include_exact else fresh}
    assert owned == {_OWN: canonical}
