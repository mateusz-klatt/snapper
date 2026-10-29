"""Validate bounded full-field native Kraken history without inventing missing minutes."""

from dataclasses import replace
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from datetime import timezone

import pytest

from snapper.infrastructure.exchanges.kraken_closed_candles import ClosedCandleRequest
from snapper.infrastructure.exchanges.kraken_closed_candles import parse_closed_candles


def _request() -> ClosedCandleRequest:
    """Freeze one canonical request and its independent current acquisition boundary."""
    return ClosedCandleRequest(
        symbol="BTC-USD",
        pair="XXBTZUSD",
        start=datetime(2026, 10, 28, 21, 18, tzinfo=UTC),
        end=datetime(2026, 10, 28, 21, 30, tzinfo=UTC),
        observed_at=datetime(2026, 10, 28, 21, 30, 42, tzinfo=UTC),
    )


def _row(open_at: datetime) -> list[object]:
    """Keep native volume and VWAP at their distinct eight-field wire positions."""
    return [int(open_at.timestamp()), "100.0", "103.0", "99.0", "102.0", "101.25", "1.75", 7]


def _payload(rows: object) -> dict[str, object]:
    """Supply only the expected canonical product and native cursor metadata."""
    return {"error": [], "result": {"XXBTZUSD": rows, "last": 0}}


def test_closed_native_fields_and_event_window_are_preserved() -> None:
    """Full-field history preserves VWAP/count while excluding prior and future minutes.

    Given: A canonical eight-field response containing rows outside the requested window,
    When: The closed native history is parsed,
    Then: Only requested closed minutes retain their native economics and identity.
    """
    request = _request()
    openings = [
        request.start - timedelta(minutes=1),
        request.start,
        request.end - timedelta(minutes=1),
        request.end,
        request.end + timedelta(minutes=1),
        request.end + timedelta(minutes=2),
    ]

    candles = parse_closed_candles(_payload([_row(at) for at in openings]), request)

    assert [c.interval_begin for c in candles] == openings[1:3]
    assert all(c.symbol == "BTC-USD" and c.interval == 60 and c.complete for c in candles)
    assert [(c.open, c.high, c.low, c.close, c.vwap, c.volume, c.trades) for c in candles] == [
        (100.0, 103.0, 99.0, 102.0, 101.25, 1.75, 7)
    ] * 2
    assert request.observed_at == datetime(2026, 10, 28, 21, 30, 42, tzinfo=UTC)


def test_final_wire_row_is_excluded_even_inside_the_requested_horizon() -> None:
    """A timestamp alone never turns the final unfinished entry into closed evidence.

    Given: The final raw row has a timestamp inside the requested closed window,
    When: The native response is parsed,
    Then: That final unfinished entry is still discarded.
    """
    request = _request()
    candles = parse_closed_candles(
        _payload([_row(request.start), _row(request.start + timedelta(minutes=1))]), request
    )
    assert [c.interval_begin for c in candles] == [request.start]


def test_721_wire_entries_keep_the_bounded_closed_inclusive_edge() -> None:
    """Allow the observed inclusive edge without expanding the bounded request.

    Given: A response contains 720 closed rows and one final unfinished row,
    When: A bounded 719-minute recent interval is parsed,
    Then: Its complete closed window is preserved without including the unfinished edge.
    """
    request = _request()
    request = replace(request, start=request.end - timedelta(minutes=719))
    rows = [_row(request.end - timedelta(minutes=720 - index)) for index in range(721)]

    candles = parse_closed_candles(_payload(rows), request)

    assert len(candles) == 719
    assert candles[0].interval_begin == request.start
    assert candles[-1].interval_begin == request.end - timedelta(minutes=1)


def test_only_an_unfinished_row_returns_no_closed_history() -> None:
    """A legitimate current-only response does not fabricate older candles.

    Given: A native response contains only its final unfinished minute,
    When: Closed history is requested,
    Then: No closed candle is returned.
    """
    request = _request()
    assert parse_closed_candles(_payload([_row(request.end)]), request) == []


@pytest.mark.parametrize("identity", ["", " ", " BTC-USD", "BTC-USD "])
@pytest.mark.parametrize("field", ["symbol", "pair"])
def test_request_requires_exact_nonempty_product_identity(field: str, identity: str) -> None:
    """Reject empty or padded product identities before any request can be made.

    Given: A symbol or native pair identity is empty or padded,
    When: A closed-history request is constructed,
    Then: Validation fails before transport acquisition.
    """
    values = {field: identity}
    with pytest.raises(ValueError, match="canonical product identity"):
        replace(_request(), **values)


@pytest.mark.parametrize("field", ["start", "end", "observed_at"])
@pytest.mark.parametrize("offset", [None, timezone(timedelta(hours=1))])
def test_request_requires_aware_utc_bounds(field: str, offset: timezone | None) -> None:
    """Do not infer UTC from an ambiguous or differently zoned request instant.

    Given: A request contains a naive or non-UTC bound,
    When: The fixed acquisition request is validated,
    Then: The ambiguous clock is rejected.
    """
    request = _request()
    instant = getattr(request, field).replace(tzinfo=offset)
    with pytest.raises(ValueError, match="UTC bounds"):
        replace(request, **{field: instant})


@pytest.mark.parametrize("field", ["start", "end"])
@pytest.mark.parametrize("delta", [timedelta(seconds=1), timedelta(microseconds=1)])
def test_request_bounds_are_whole_minutes(field: str, delta: timedelta) -> None:
    """An unaligned boundary cannot silently select a partial business minute.

    Given: A business-time request bound has seconds or microseconds,
    When: The request is constructed,
    Then: It is refused instead of rounding the boundary.
    """
    request = _request()
    with pytest.raises(ValueError, match="whole-minute bounds"):
        replace(request, **{field: getattr(request, field) + delta})


@pytest.mark.parametrize("window", ["empty", "reversed", "unfinished", "too_wide"])
def test_request_rejects_nonclosed_or_unbounded_windows(window: str) -> None:
    """Only positive bounded intervals of already closed minutes are valid.

    Given: An empty, reversed, unfinished or excessively wide requested interval,
    When: The closed-history request is validated,
    Then: The unsupported interval is rejected.
    """
    request = _request()
    replacements = {
        "empty": {"start": request.end},
        "reversed": {"start": request.end + timedelta(minutes=1)},
        "unfinished": {"end": request.end + timedelta(minutes=1)},
        "too_wide": {"start": request.end - timedelta(minutes=720)},
    }
    with pytest.raises(ValueError, match="closed minutes|bounded recent window"):
        replace(request, **replacements[window])


@pytest.mark.parametrize(
    "payload",
    [
        None,
        [],
        {},
        {"error": ["venue_error"], "result": {}},
        {"error": [], "result": []},
        {"error": [], "result": {"wrong_product": [], "last": 0}},
        {"error": [], "result": {"XXBTZUSD": [], "other_product": [], "last": 0}},
        {"error": [], "result": {"XXBTZUSD": []}},
    ],
)
def test_malformed_or_mismatched_envelopes_fail_closed(payload: object) -> None:
    """A successful HTTP response cannot substitute another product or an error envelope.

    Given: An untrusted response has errors, malformed structure or mismatched identity,
    When: Native closed history is parsed,
    Then: The response fails without emitting any candle.
    """
    with pytest.raises(ValueError, match="envelope|identity"):
        parse_closed_candles(payload, _request())


@pytest.mark.parametrize("rows", [None, {}, [], [None] * 722])
def test_native_row_count_remains_bounded(rows: object) -> None:
    """Require the unfinished edge and enforce the verified wire-row bound.

    Given: Native rows are absent, malformed or exceed 721 entries,
    When: The native history envelope is parsed,
    Then: The response is rejected before any row can be used.
    """
    with pytest.raises(ValueError, match="bounded native contract"):
        parse_closed_candles(_payload(rows), _request())


@pytest.mark.parametrize("row", [None, {}, (), [0] * 7, [0] * 9])
def test_native_rows_require_all_eight_fields(row: object) -> None:
    """A generic OHLCV representation is insufficient native evidence.

    Given: A native row lacks the exact eight-field shape,
    When: The response is parsed,
    Then: Missing volume, VWAP or count semantics cannot be inferred.
    """
    with pytest.raises(ValueError, match="eight native fields"):
        parse_closed_candles(_payload([row]), _request())


@pytest.mark.parametrize(
    ("field", "value"),
    [(0, "0"), (0, True), (0, 1), (7, "7"), (7, True), (7, 7.0), (7, -1)],
)
def test_minutes_and_trade_counts_are_native_integers(field: int, value: object) -> None:
    """Reject coercible, boolean, fractional-minute and negative-count identities.

    Given: A native minute or count violates its integer contract,
    When: The native row is parsed,
    Then: The invalid business identity or count is rejected.
    """
    row = _row(_request().start)
    row[field] = value
    with pytest.raises(ValueError, match="minute or trade count"):
        parse_closed_candles(_payload([row]), _request())


@pytest.mark.parametrize("stamp", [253402300800, 60 * 10**100, -60 * 10**100])
def test_native_minutes_must_be_representable_utc_instants(stamp: int) -> None:
    """An aligned integer outside the calendar remains invalid venue evidence.

    Given: A minute-aligned integer cannot represent a supported UTC instant,
    When: The native row is parsed,
    Then: It fails with a bounded minute-validation diagnostic.
    """
    row = _row(_request().start)
    row[0] = stamp
    with pytest.raises(ValueError, match="Invalid Kraken OHLC minute"):
        parse_closed_candles(_payload([row]), _request())


@pytest.mark.parametrize("field", range(1, 7))
@pytest.mark.parametrize(
    "value", [None, True, {}, "NaN", "Infinity", "-Infinity", "invalid", 10**1000]
)
def test_all_native_numeric_fields_are_finite(field: int, value: object) -> None:
    """Every price, VWAP and volume must be a real finite native number.

    Given: One native numeric field is malformed or non-finite,
    When: The full eight-field row is parsed,
    Then: No candle is produced from the invalid field.
    """
    row = _row(_request().start)
    row[field] = value
    with pytest.raises(ValueError):
        parse_closed_candles(_payload([row]), _request())


@pytest.mark.parametrize(
    ("field", "value"), [(3, "0"), (3, "101"), (2, "101"), (5, "-1"), (6, "-1")]
)
def test_native_price_geometry_and_nonnegative_amounts(field: int, value: str) -> None:
    """Finite values must form valid OHLC bounds and nonnegative native fields.

    Given: A finite row violates its price geometry or volume/VWAP sign,
    When: The native row is parsed,
    Then: The impossible candle is rejected.
    """
    row = _row(_request().start)
    row[field] = value
    with pytest.raises(ValueError, match="price bounds|volume or VWAP"):
        parse_closed_candles(_payload([row]), _request())


@pytest.mark.parametrize("duplicate", [True, False])
def test_native_business_minutes_are_strictly_ascending(duplicate: bool) -> None:
    """Duplicate and descending wire rows are rejected.

    Given: A native response repeats a minute or moves backward in business time,
    When: The response is parsed,
    Then: Its ambiguous ordering fails without deduplication or sorting.
    """
    request = _request()
    second = request.start if duplicate else request.start - timedelta(minutes=1)
    with pytest.raises(ValueError, match="duplicate or unordered"):
        parse_closed_candles(_payload([_row(request.start), _row(second)]), request)


def test_finite_native_json_numbers_are_preserved() -> None:
    """Accept finite JSON numeric fields without changing native field positions.

    Given: A native row uses finite JSON numbers alongside decimal strings,
    When: The native closed history is parsed,
    Then: Its independent price, VWAP, volume and count fields are retained.
    """
    request = _request()
    row = _row(request.start)
    row[1], row[5], row[6] = 100, 101.25, 0
    candles = parse_closed_candles(_payload([row, _row(request.end)]), request)
    assert candles[0].open == 100.0
    assert candles[0].vwap == 101.25
    assert candles[0].volume == 0.0
    assert candles[0].trades == 7
