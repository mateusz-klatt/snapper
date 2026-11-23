"""Tests for Kraken ledger parsing, paging, and count reconciliation."""

from collections.abc import Awaitable
from collections.abc import Callable
from datetime import UTC
from datetime import datetime

import ccxt
import pytest

from snapper.infrastructure.exchanges.kraken_ledger import KRAKEN_LEDGER_CALL_COST
from snapper.infrastructure.exchanges.kraken_ledger import KrakenLedgerEntry
from snapper.infrastructure.exchanges.kraken_ledger import KrakenLedgerPage
from snapper.infrastructure.exchanges.kraken_ledger import KrakenLedgerPermissionMissingError
from snapper.infrastructure.exchanges.kraken_ledger import LedgerCollectRequest
from snapper.infrastructure.exchanges.kraken_ledger import LedgerPageQuery
from snapper.infrastructure.exchanges.kraken_ledger import classify_ledger_window
from snapper.infrastructure.exchanges.kraken_ledger import collect_ledger_window
from snapper.infrastructure.exchanges.kraken_ledger import entry_in_window
from snapper.infrastructure.exchanges.kraken_ledger import is_kraken_ledger_permission_error
from snapper.infrastructure.exchanges.kraken_ledger import is_permission_message
from snapper.infrastructure.exchanges.kraken_ledger import ledger_unix_second
from snapper.infrastructure.exchanges.kraken_ledger import parse_ledgers_payload
from snapper.infrastructure.exchanges.kraken_ledger import parse_trade_balance_payload
from snapper.infrastructure.exchanges.kraken_ledger import trade_balance_text

_START = 1_700_000_100
_END = 1_700_000_110


def _row(**overrides: object) -> dict[str, object]:
    """Return one valid raw ledger object, with optional field replacements."""
    row: dict[str, object] = {
        "refid": "R",
        "time": "1700000100.9",
        "type": "margin",
        "subtype": "",
        "aclass": "currency",
        "asset": "ZEUR",
        "amount": "1.2500",
        "fee": "0.0100",
        "balance": "8.0000",
    }
    row.update(overrides)
    return row


def _payload(
    ledger: dict[object, object], count: object, error: object = None
) -> dict[str, object]:
    """Return one Ledgers envelope."""
    body: dict[str, object] = {"result": {"ledger": ledger, "count": count}}
    if error is not None:
        body["error"] = error
    return body


def _entry(
    ledger_id: str,
    second: int,
    *,
    amount: str = "1.2500",
    fraction: str = "",
) -> KrakenLedgerEntry:
    """Return one parsed entry at an exact or fractional unix second."""
    return KrakenLedgerEntry(
        venue_ledger_id=ledger_id,
        refid="",
        ledger_time=f"{second}{fraction}",
        ledger_second=second,
        ledger_type="margin",
        subtype="",
        aclass="currency",
        asset="ZEUR",
        amount=amount,
        fee="0.0000",
        balance="8.0000",
    )


def _request(
    budget: int, page_size: int, cost: int = KRAKEN_LEDGER_CALL_COST
) -> LedgerCollectRequest:
    """Return a frozen window with an explicit counter budget."""
    return LedgerCollectRequest(
        start=_START,
        end=_END,
        budget_points=budget,
        call_cost=cost,
        page_size=page_size,
    )


def test_parse_ledgers_preserves_venue_text_and_floors_time() -> None:
    """Keep venue decimal text and floor the ledger second.

    Given string amounts, a fractional time, an empty refid, and a negative amount,
    When the Ledgers envelope is parsed,
    Then the original strings are retained and the second is the floor of time.
    """
    page = parse_ledgers_payload(
        _payload(
            {
                "L1": _row(refid="", amount="-1.2500"),
                "L2": _row(time=1700000100.5, amount=5, subtype="rollover"),
            },
            2,
        )
    )
    assert page.count == 2
    first, second = page.entries
    assert first.venue_ledger_id == "L1"
    assert first.refid == ""
    assert first.amount == "-1.2500"
    assert first.ledger_time == "1700000100.9"
    assert first.ledger_second == 1700000100
    assert second.amount == "5"
    assert second.ledger_time == "1700000100.5"
    assert second.subtype == "rollover"


def test_parse_ledgers_accepts_a_missing_or_empty_error_list() -> None:
    """Treat a missing or empty Kraken error list as success.

    Given one envelope with no error key and one whose error is null,
    When each envelope is parsed,
    Then both return the reported count.
    """
    missing = parse_ledgers_payload(_payload({}, 0))
    explicit_null = parse_ledgers_payload(_payload({}, 0, error=None))
    assert missing.count == 0
    assert explicit_null.count == 0


def test_parse_ledgers_rejects_malformed_envelopes() -> None:
    """Reject envelopes that are not a faithful Ledgers page.

    Given malformed bodies, ids, counts, and rows,
    When each payload is parsed,
    Then parsing raises ValueError and does not invent a posting.
    """
    payloads: tuple[object, ...] = (
        "nope",
        {"error": "bad"},
        {"error": [""], "result": {}},
        {"error": ["EGeneral:Invalid arguments"]},
        {"error": [], "result": []},
        {"error": [], "result": {"ledger": [], "count": 0}},
        {"error": [], "result": {"ledger": {"": _row()}, "count": 1}},
        {"error": [], "result": {"ledger": {1: _row()}, "count": 1}},
        {"error": [], "result": {"ledger": {}, "count": True}},
        {"error": [], "result": {"ledger": {}, "count": -1}},
        {"error": [], "result": {"ledger": {}, "count": "1"}},
        {"error": [], "result": {"ledger": {"L": "x"}, "count": 1}},
        {"error": [], "result": {"ledger": {"L": _row(type="")}, "count": 1}},
        {"error": [], "result": {"ledger": {"L": _row(type=1)}, "count": 1}},
        {"error": [], "result": {"ledger": {"L": _row(amount=True)}, "count": 1}},
        {"error": [], "result": {"ledger": {"L": _row(amount=None)}, "count": 1}},
        {"error": [], "result": {"ledger": {"L": _row(amount="abc")}, "count": 1}},
        {"error": [], "result": {"ledger": {"L": _row(amount="Infinity")}, "count": 1}},
        {"error": [], "result": {"ledger": {"L": _row(time=float("nan"))}, "count": 1}},
        {"error": [], "result": {"ledger": {"L": _row(time=float("inf"))}, "count": 1}},
        {"error": [], "result": {"ledger": {"L": _row(time="-1")}, "count": 1}},
    )
    for payload in payloads:
        with pytest.raises(ValueError):
            parse_ledgers_payload(payload)


def test_parse_ledgers_reports_missing_permission_from_the_error_list() -> None:
    """A permission error in the envelope is not a retryable parse failure.

    Given an envelope whose error list names invalid permissions,
    When the envelope is parsed,
    Then KrakenLedgerPermissionMissingError is raised.
    """
    with pytest.raises(KrakenLedgerPermissionMissingError, match="Invalid permissions"):
        parse_ledgers_payload({"error": ["EAPI:Invalid permissions"], "result": {}})


def test_permission_detection_accepts_the_exception_class_and_message() -> None:
    """Detect a missing ledger permission from the type or the text.

    Given a PermissionDenied subclass, a permission message, and an unrelated error,
    When each error is classified,
    Then only the permission cases match, including every documented marker.
    """

    class WrappedError(ccxt.PermissionDenied):
        pass

    assert is_kraken_ledger_permission_error(WrappedError("nope"))
    assert is_kraken_ledger_permission_error(RuntimeError("Permission denied"))
    assert not is_kraken_ledger_permission_error(RuntimeError("nope"))
    assert is_permission_message("EAPI:Invalid permissions")
    assert is_permission_message("Invalid permissions")
    assert is_permission_message("Permission denied")
    assert not is_permission_message("EGeneral:Invalid arguments")


def test_trade_balance_text_is_stable_and_required() -> None:
    """Keep the seven TradeBalance evidence fields as stable JSON.

    Given a complete TradeBalance envelope,
    When it is parsed and encoded,
    Then every field is retained in sorted-key JSON and a missing field is rejected.
    """
    balance = parse_trade_balance_payload(
        {
            "error": [],
            "result": {"eb": "1", "tb": "2", "m": "3", "n": "4", "c": "5", "v": "6", "ml": "7"},
        }
    )
    assert (
        trade_balance_text(balance)
        == '{"c":"5","eb":"1","m":"3","ml":"7","n":"4","tb":"2","v":"6"}'
    )
    with pytest.raises(KrakenLedgerPermissionMissingError):
        parse_trade_balance_payload({"error": ["Permission denied"], "result": {}})
    with pytest.raises(ValueError):
        parse_trade_balance_payload({"error": [], "result": {"eb": "1"}})


def test_ledger_unix_second_requires_an_aware_timestamp() -> None:
    """Floor an aware timestamp and reject a naive one.

    Given one UTC instant with a fractional second and one naive datetime,
    When each is converted to a ledger second,
    Then the aware value floors and the naive value raises ValueError.
    """
    instant = datetime(2026, 11, 23, 18, 0, 0, 900000, tzinfo=UTC)
    assert ledger_unix_second(instant) == int(instant.timestamp())
    with pytest.raises(ValueError, match="timezone-aware"):
        ledger_unix_second(datetime(2026, 11, 23, 18, 0, 0))


def test_entry_window_is_inclusive_of_the_boundary_second() -> None:
    """Include the start second and exclude the second after end.

    Given entries at the start, inside the end second, and just outside,
    When each entry is tested against the frozen window,
    Then only the boundary second and its fraction are inside.
    """
    assert entry_in_window(_entry("A", _START, fraction=".0"), _START, _END)
    assert entry_in_window(_entry("B", _END, fraction=".9"), _START, _END)
    assert not entry_in_window(_entry("C", _START - 1, fraction=".9"), _START, _END)
    assert not entry_in_window(_entry("D", _END + 1), _START, _END)


@pytest.mark.parametrize(
    ("seen", "reported", "exhausted", "unfaithful", "expected"),
    [
        (0, None, True, False, "ledger_backlog"),
        (1, 3, True, False, "ledger_backlog"),
        (1, 3, True, True, "ledger_cursor_gap"),
        (2, 2, False, True, "ledger_cursor_gap"),
        (1, 2, False, False, "ledger_cursor_gap"),
        (2, 2, False, False, "ok"),
        (3, 2, True, False, "ledger_cursor_gap"),
        (1, 2, False, True, "ledger_cursor_gap"),
    ],
)
def test_classify_ledger_window_distinguishes_gap_and_backlog(
    seen: int,
    reported: int | None,
    exhausted: bool,
    unfaithful: bool,
    expected: str,
) -> None:
    """Separate a finished count mismatch from a budget stop.

    Given unique ids, Kraken's count, and whether the walk stopped early,
    When the window is classified,
    Then a short budget is backlog and every unfaithful or unequal count is a gap.
    """
    assert classify_ledger_window(seen, reported, exhausted, unfaithful) == expected


@pytest.mark.asyncio
async def test_collect_pages_under_a_fixed_end_until_the_count_is_met() -> None:
    """Page with ofs until a short page reconciles to Kraken's count.

    Given two pages under one frozen end whose combined ids equal count,
    When the collector walks the window,
    Then the second request continues at the first page length and the cursor overlaps
        the newest second.
    """
    pages = [
        KrakenLedgerPage(entries=(_entry("A", _START), _entry("B", _END)), count=3),
        KrakenLedgerPage(entries=(_entry("C", _END),), count=3),
    ]
    calls: list[LedgerPageQuery] = []

    async def fetch(query: LedgerPageQuery) -> KrakenLedgerPage:
        calls.append(query)
        return pages.pop(0)

    outcome = await collect_ledger_window(_request(budget=6, page_size=2), fetch)
    assert [call.offset for call in calls] == [0, 2]
    assert {call.end for call in calls} == {_END}
    assert outcome.status == "ok"
    assert outcome.reported_count == 3
    assert [entry.venue_ledger_id for entry in outcome.entries] == ["A", "B", "C"]
    assert outcome.cursor_second == _END
    assert outcome.cursor_ids == ("B", "C")
    assert outcome.calls == 2


@pytest.mark.asyncio
async def test_collect_overlap_includes_the_cursor_second() -> None:
    """The next window starts on the previous cursor second.

    Given a reconciled window whose newest second is the following start,
    When that second is fetched again with a newer posting,
    Then both the overlapped id and the new id are in the window.
    """
    first = await collect_ledger_window(
        _request(budget=2, page_size=50),
        _pages(KrakenLedgerPage(entries=(_entry("OLD", _END),), count=1)),
    )
    assert first.cursor_second == _END
    second_request = LedgerCollectRequest(
        start=first.cursor_second,
        end=_END + 10,
        budget_points=2,
        call_cost=2,
        page_size=50,
    )
    second = await collect_ledger_window(
        second_request,
        _pages(
            KrakenLedgerPage(
                entries=(_entry("OLD", _END), _entry("NEW", _END + 10)),
                count=2,
            )
        ),
    )
    assert second.status == "ok"
    assert {entry.venue_ledger_id for entry in second.entries} == {"OLD", "NEW"}


@pytest.mark.asyncio
async def test_collect_count_mismatch_after_a_finished_walk_is_a_gap() -> None:
    """A finished walk that disagrees with count does not advance.

    Given a short page whose unique ids are fewer than Kraken's count,
    When the collector finishes that page,
    Then the status is ledger_cursor_gap and no success cursor is set.
    """
    outcome = await collect_ledger_window(
        _request(budget=2, page_size=50),
        _pages(KrakenLedgerPage(entries=(_entry("A", _END),), count=2)),
    )
    assert outcome.status == "ledger_cursor_gap"
    assert outcome.cursor_second is None
    assert outcome.reported_count == 2
    assert outcome.calls == 1


@pytest.mark.asyncio
async def test_collect_count_change_does_not_absorb_the_disagreeing_page() -> None:
    """A count that moves mid-walk is a gap.

    Given a second page whose count differs from the first page,
    When the collector sees that page,
    Then the new ids are ignored and the status is ledger_cursor_gap.
    """
    pages = [
        KrakenLedgerPage(entries=(_entry("A", _START), _entry("B", _END)), count=4),
        KrakenLedgerPage(entries=(_entry("C", _END), _entry("D", _END)), count=5),
    ]
    outcome = await collect_ledger_window(_request(budget=6, page_size=2), _pages(*pages))
    assert outcome.status == "ledger_cursor_gap"
    assert [entry.venue_ledger_id for entry in outcome.entries] == ["A", "B"]
    assert outcome.reported_count == 4


@pytest.mark.asyncio
async def test_collect_stops_on_permission_without_another_call() -> None:
    """A missing ledger permission consumes one call and stops.

    Given a fetcher that denies the first page,
    When the collector requests the window,
    Then it records ledger_permission_missing and does not ask for another page.
    """
    calls: list[int] = []

    async def fetch(query: LedgerPageQuery) -> KrakenLedgerPage:
        calls.append(query.offset)
        raise KrakenLedgerPermissionMissingError("EAPI:Invalid permissions")

    outcome = await collect_ledger_window(_request(budget=6, page_size=50), fetch)
    assert calls == [0]
    assert outcome.status == "ledger_permission_missing"
    assert outcome.calls == 1
    assert outcome.points_used == KRAKEN_LEDGER_CALL_COST
    assert outcome.entries == ()


@pytest.mark.asyncio
async def test_collect_budget_stops_as_backlog_before_count_is_reached() -> None:
    """The counter budget stops paging and leaves the cursor unadvanced.

    Given a full page whose count still needs another page and a budget for one call,
    When the collector spends that call,
    Then the status is ledger_backlog and no further page is requested.
    """
    calls: list[int] = []

    async def fetch(query: LedgerPageQuery) -> KrakenLedgerPage:
        calls.append(query.offset)
        return KrakenLedgerPage(entries=(_entry("A", _START), _entry("B", _END)), count=4)

    outcome = await collect_ledger_window(_request(budget=2, page_size=2), fetch)
    assert calls == [0]
    assert outcome.status == "ledger_backlog"
    assert outcome.cursor_second is None
    assert outcome.points_used == 2


@pytest.mark.asyncio
async def test_collect_zero_budget_makes_no_call() -> None:
    """A budget below one Ledgers call records backlog without a request.

    Given a positive window and zero remaining counter points,
    When the collector runs,
    Then it makes no call and reports ledger_backlog.
    """

    async def fetch(query: LedgerPageQuery) -> KrakenLedgerPage:
        raise AssertionError(query.offset)

    outcome = await collect_ledger_window(_request(budget=0, page_size=50), fetch)
    assert outcome.status == "ledger_backlog"
    assert outcome.calls == 0
    assert outcome.reported_count is None


@pytest.mark.asyncio
async def test_collect_rejects_a_non_positive_call_cost() -> None:
    """A non-positive call cost would loop without spending budget.

    Given a collector request whose call cost is zero,
    When collection starts,
    Then ValueError is raised before any page request.
    """
    with pytest.raises(ValueError, match="call cost"):
        await collect_ledger_window(_request(budget=2, page_size=50, cost=0), _pages())


@pytest.mark.asyncio
async def test_collect_identical_duplicate_stays_faithful() -> None:
    """An identical repeated id counts once.

    Given one page that repeats the same posting and reports that single id,
    When the page is absorbed,
    Then the window reconciles and keeps one row.
    """
    entry = _entry("A", _END)
    outcome = await collect_ledger_window(
        _request(budget=2, page_size=50),
        _pages(KrakenLedgerPage(entries=(entry, entry), count=1)),
    )
    assert outcome.status == "ok"
    assert outcome.entries == (entry,)


@pytest.mark.asyncio
async def test_collect_conflicting_duplicate_or_outside_row_is_a_gap() -> None:
    """A contradictory or out-of-window row makes the page unfaithful.

    Given a repeated id with a different amount and a row before the window,
    When each page is collected,
    Then both results are ledger_cursor_gap.
    """
    conflicted = _entry("A", _END, amount="9")
    original = _entry("A", _END)
    conflict = await collect_ledger_window(
        _request(budget=2, page_size=50),
        _pages(KrakenLedgerPage(entries=(original, conflicted), count=1)),
    )
    outside = await collect_ledger_window(
        _request(budget=2, page_size=50),
        _pages(KrakenLedgerPage(entries=(_entry("B", _START - 5),), count=1)),
    )
    assert conflict.status == "ledger_cursor_gap"
    assert outside.status == "ledger_cursor_gap"
    assert outside.entries == ()


@pytest.mark.asyncio
async def test_collect_empty_reconciled_window_advances_to_end() -> None:
    """An empty count advances the cursor to the frozen end.

    Given a page with no rows and a count of zero,
    When the collector finishes,
    Then the status is ok and the cursor second is the window end.
    """
    outcome = await collect_ledger_window(
        _request(budget=2, page_size=50),
        _pages(KrakenLedgerPage(entries=(), count=0)),
    )
    assert outcome.status == "ok"
    assert outcome.cursor_second == _END
    assert outcome.cursor_ids == ()


@pytest.mark.asyncio
async def test_collect_full_page_that_meets_count_stops() -> None:
    """A full page stops once the unique ids reach count.

    Given one full page whose ids equal the reported count,
    When the collector absorbs it,
    Then it does not request the next offset.
    """
    calls: list[int] = []

    async def fetch(query: LedgerPageQuery) -> KrakenLedgerPage:
        calls.append(query.offset)
        return KrakenLedgerPage(entries=(_entry("A", _END), _entry("B", _END)), count=2)

    outcome = await collect_ledger_window(_request(budget=6, page_size=2), fetch)
    assert calls == [0]
    assert outcome.status == "ok"


def _pages(
    *pages: KrakenLedgerPage,
) -> Callable[[LedgerPageQuery], Awaitable[KrakenLedgerPage]]:
    """Return a fetcher that yields the given pages in order."""
    remaining = list(pages)

    async def fetch(query: LedgerPageQuery) -> KrakenLedgerPage:
        del query
        return remaining.pop(0)

    return fetch


def test_unrecognized_ledger_type_is_retained_as_evidence() -> None:
    """Observation preserves types that a future evaluator may not understand.

    Given an unknown type and subtype with exact venue decimal strings,
    When the ledger envelope is parsed,
    Then both fields survive without inventing a balance verdict.
    """
    page = parse_ledgers_payload(
        _payload({"L": _row(type="future_type", subtype="future_subtype")}, 1)
    )
    assert page.entries[0].ledger_type == "future_type"
    assert page.entries[0].subtype == "future_subtype"
