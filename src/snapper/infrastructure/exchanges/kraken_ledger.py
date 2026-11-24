"""Kraken spot ledger and trade-balance parsing, paging, and pass budget.

Ledgers calls cost 2 Kraken API-counter points. TradeBalance costs 1. One
account-observer pass may spend at most ``KRAKEN_LEDGER_PASS_BUDGET_POINTS``.
Paging keeps ``end`` fixed and reconciles unique ledger ids with Kraken's
``count``. A budget-stopped walk can resume at its persisted offset. The cursor
is the newest second in a reconciled window plus the ids at that second, so the
next pass overlaps that second.
"""

import json
import math
from collections.abc import Awaitable
from collections.abc import Callable
from collections.abc import Mapping
from dataclasses import dataclass
from dataclasses import field
from datetime import datetime
from decimal import ROUND_FLOOR
from decimal import Decimal
from decimal import InvalidOperation
from typing import Final
from typing import Literal
from typing import cast

KRAKEN_LEDGER_PASS_BUDGET_POINTS: Final = 6
KRAKEN_LEDGER_CALL_COST: Final = 2
KRAKEN_TRADE_BALANCE_CALL_COST: Final = 1
KRAKEN_LEDGER_PAGE_SIZE: Final = 50

type LedgerPassStatus = Literal[
    "ok",
    "ledger_cursor_gap",
    "ledger_backlog",
    "ledger_permission_missing",
]

_MALFORMED_ENVELOPE = "Kraken ledgers envelope is malformed"
_MALFORMED_TRADE_BALANCE = "Kraken trade balance envelope is malformed"
_PERMISSION_MARKERS: Final = (
    "EAPI:Invalid permissions",
    "Invalid permissions",
    "Permission denied",
)
_TRADE_BALANCE_FIELDS: Final = ("eb", "tb", "m", "n", "c", "v")
_TRADE_BALANCE_OPTIONAL_MARGIN_LEVEL: Final = "ml"


class KrakenLedgerPermissionMissingError(RuntimeError):
    """The Kraken key cannot query ledger entries."""


class KrakenLedgerReadPausedError(RuntimeError):
    """In-flight private REST activity suspended a budgeted observation window."""


@dataclass(frozen=True)
class KrakenLedgerEntry:
    """One Kraken ledger posting retained with the venue's decimal text."""

    venue_ledger_id: str
    refid: str
    ledger_time: str
    ledger_second: int
    ledger_type: str
    subtype: str
    aclass: str
    asset: str
    amount: str
    fee: str
    balance: str


@dataclass(frozen=True)
class KrakenLedgerPage:
    """One Ledgers response page and the window ``count`` Kraken reported."""

    entries: tuple[KrakenLedgerEntry, ...]
    count: int


@dataclass(frozen=True)
class KrakenTradeBalance:
    """TradeBalance evidence fields. They are not a reconciliation input.

    ``ml`` (margin level) is ``None`` when Kraken omits it, which the venue does
    whenever the account holds no open margin position.
    """

    eb: str
    tb: str
    m: str
    n: str
    c: str
    v: str
    ml: str | None


@dataclass(frozen=True)
class LedgerPageQuery:
    """One Ledgers request under a frozen ``[start, end]`` second window."""

    start: int
    end: int
    offset: int


@dataclass(frozen=True)
class LedgerCollectRequest:
    """Budget, frozen window, and prior progress for one Ledgers paging walk."""

    start: int
    end: int
    budget_points: int
    call_cost: int
    page_size: int
    offset: int = 0
    expected_count: int | None = None
    received_count: int = 0
    window_cursor_second: int | None = None
    window_cursor_ids: tuple[str, ...] = ()


@dataclass(frozen=True)
class LedgerWindowOutcome:
    """Paging result. ``cursor_second`` is set only when ``status`` is ``ok``."""

    status: LedgerPassStatus
    entries: tuple[KrakenLedgerEntry, ...]
    reported_count: int | None
    points_used: int
    calls: int
    cursor_second: int | None
    cursor_ids: tuple[str, ...]
    next_ofs: int
    expected_count: int | None
    received_count: int
    window_cursor_second: int | None
    window_cursor_ids: tuple[str, ...]


@dataclass
class _Absorb:
    """Unique in-window entries and whether the page was unfaithful."""

    entries: dict[str, KrakenLedgerEntry] = field(default_factory=dict)
    unfaithful: bool = False


@dataclass
class _Collect:
    """Mutable walk state for one frozen Ledgers window."""

    absorb: _Absorb = field(default_factory=_Absorb)
    reported: int | None = None
    points: int = 0
    calls: int = 0
    offset: int = 0
    exhausted: bool = False
    permission: bool = False
    paused: bool = False
    timed_out: bool = False


def ledger_unix_second(value: datetime) -> int:
    """Return the unix second of an aware timestamp.

    Args:
        value: Timezone-aware instant.

    Returns:
        The second containing ``value``.

    Raises:
        ValueError: When ``value`` is naive.
    """
    if value.utcoffset() is None:
        raise ValueError("ledger timestamp must be timezone-aware")
    return int(value.timestamp())


def is_kraken_ledger_permission_error(exc: BaseException) -> bool:
    """Return whether an exception is a missing ledger-query permission.

    Args:
        exc: Error raised by the private Kraken call.

    Returns:
        True when the key lacks Query Ledger Entries permission.
    """
    if any(item.__name__ == "PermissionDenied" for item in type(exc).mro()):
        return True
    return is_permission_message(str(exc))


def is_permission_message(message: str) -> bool:
    """Return whether Kraken error text reports a missing permission.

    Args:
        message: Joined Kraken ``error`` text or exception string.

    Returns:
        True when the text names a permission failure.
    """
    return any(marker in message for marker in _PERMISSION_MARKERS)


def parse_ledgers_payload(payload: object) -> KrakenLedgerPage:
    """Parse one raw Kraken Ledgers envelope.

    Args:
        payload: JSON object returned by the private Ledgers call.

    Returns:
        Entries in response order and the reported window count.

    Raises:
        KrakenLedgerPermissionMissingError: When the envelope denies ledger reads.
        ValueError: When the envelope or a row is malformed.
    """
    body = _as_mapping(payload, _MALFORMED_ENVELOPE)
    _raise_for_kraken_errors(body, _MALFORMED_ENVELOPE)
    result = _as_mapping(body.get("result"), _MALFORMED_ENVELOPE)
    ledger = _as_mapping(result.get("ledger"), _MALFORMED_ENVELOPE)
    entries = tuple(_parse_ledger_entry(ledger_id, raw) for ledger_id, raw in _ledger_items(ledger))
    return KrakenLedgerPage(entries=entries, count=_parse_count(result.get("count")))


def parse_trade_balance_payload(payload: object) -> KrakenTradeBalance:
    """Parse one raw Kraken TradeBalance envelope.

    Args:
        payload: JSON object returned by the private TradeBalance call.

    Returns:
        The evidence fields ``eb``, ``tb``, ``m``, ``n``, ``c``, ``v``, and ``ml``.
        ``ml`` is ``None`` when absent: Kraken reports the margin level only while
        a margin position is open. A present ``ml`` must still be valid decimal text.

    Raises:
        KrakenLedgerPermissionMissingError: When the envelope denies the read.
        ValueError: When the envelope, a required field, or a present ``ml`` is malformed.
    """
    body = _as_mapping(payload, _MALFORMED_TRADE_BALANCE)
    _raise_for_kraken_errors(body, _MALFORMED_TRADE_BALANCE)
    result = _as_mapping(body.get("result"), _MALFORMED_TRADE_BALANCE)
    values = {
        field_name: _decimal_text(result.get(field_name), field_name)
        for field_name in _TRADE_BALANCE_FIELDS
    }
    raw_margin_level = result.get(_TRADE_BALANCE_OPTIONAL_MARGIN_LEVEL)
    margin_level = (
        None
        if raw_margin_level is None
        else _decimal_text(raw_margin_level, _TRADE_BALANCE_OPTIONAL_MARGIN_LEVEL)
    )
    return KrakenTradeBalance(
        eb=values["eb"],
        tb=values["tb"],
        m=values["m"],
        n=values["n"],
        c=values["c"],
        v=values["v"],
        ml=margin_level,
    )


def trade_balance_text(balance: KrakenTradeBalance) -> str:
    """Encode trade-balance evidence as stable JSON text.

    Args:
        balance: Parsed TradeBalance fields.

    Returns:
        Canonical JSON object of the seven evidence fields; an absent ``ml`` is ``null``.
    """
    payload = {
        "c": balance.c,
        "eb": balance.eb,
        "m": balance.m,
        "ml": balance.ml,
        "n": balance.n,
        "tb": balance.tb,
        "v": balance.v,
    }
    return json.dumps(payload, separators=(",", ":"), sort_keys=True)


def classify_ledger_window(
    seen: int,
    reported: int | None,
    exhausted: bool,
    unfaithful: bool,
) -> LedgerPassStatus:
    """Classify a finished or budget-stopped window.

    Args:
        seen: Unique in-window ledger ids.
        reported: Kraken ``count`` for the frozen window, if a page returned one.
        exhausted: Whether another page would exceed the counter budget.
        unfaithful: Whether a row was outside the window or contradicted an earlier row.

    Returns:
        ``ok`` when the unique ids equal ``count``, ``ledger_backlog`` when the
        budget stops short, otherwise ``ledger_cursor_gap``.
    """
    if reported is None or (exhausted and seen < reported and not unfaithful):
        return "ledger_backlog"
    if unfaithful or seen != reported:
        return "ledger_cursor_gap"
    return "ok"


def cursor_after_success(
    entries: tuple[KrakenLedgerEntry, ...],
    end: int,
) -> tuple[int, tuple[str, ...]]:
    """Return the overlap cursor for a reconciled window.

    Args:
        entries: Unique in-window entries.
        end: Inclusive end second of the window. Used when the window is empty.

    Returns:
        The newest ledger second and the sorted ids at that second.
    """
    if not entries:
        return end, ()
    newest = max(entry.ledger_second for entry in entries)
    ids = tuple(sorted(entry.venue_ledger_id for entry in entries if entry.ledger_second == newest))
    return newest, ids


async def collect_ledger_window(
    request: LedgerCollectRequest,
    fetch_page: Callable[[LedgerPageQuery], Awaitable[KrakenLedgerPage]],
) -> LedgerWindowOutcome:
    """Page Ledgers under a fixed end until the count is met or the budget stops.

    Args:
        request: Frozen window and remaining API-counter points.
        fetch_page: Calls the executor's Kraken client. Offset is newest-first.

    Returns:
        Entries, evidence status, points used, and the success cursor.

    Raises:
        ValueError: When the call cost is not positive, which would not spend budget.
    """
    if request.call_cost <= 0:
        raise ValueError("ledger call cost must be positive")
    state = _Collect(
        reported=request.expected_count,
        offset=request.offset,
    )
    while state.points + request.call_cost <= request.budget_points:
        page = await _fetch_page(request, fetch_page, state)
        if page is None:
            return _outcome(state, request)
        if not _accept_count(state, page.count):
            return _outcome(state, request, forced="ledger_cursor_gap")
        _absorb_page(state.absorb, page, request)
        if _page_completes(
            len(page.entries),
            _received_count(state, request),
            page.count,
            request.page_size,
        ):
            return _outcome(state, request)
    state.exhausted = True
    return _outcome(state, request)


def entry_in_window(entry: KrakenLedgerEntry, start: int, end: int) -> bool:
    """Return whether a ledger time falls in the inclusive second window.

    Args:
        entry: Parsed ledger row.
        start: Inclusive start second.
        end: Inclusive end second.

    Returns:
        True when the venue time is inside ``[start, end + 1)``.
    """
    value = Decimal(entry.ledger_time)
    return Decimal(start) <= value < Decimal(end + 1)


def _as_mapping(value: object, message: str) -> dict[str, object]:
    """Return a string-keyed object or raise the caller's envelope error."""
    if not isinstance(value, dict):
        raise ValueError(message)
    return cast(dict[str, object], value)


def _raise_for_kraken_errors(body: Mapping[str, object], malformed: str) -> None:
    """Raise when Kraken's ``error`` list is present and non-empty."""
    errors = body.get("error", [])
    if errors is None or errors == []:
        return
    if not isinstance(errors, list):
        raise ValueError(malformed)
    message = " ".join(str(item) for item in errors)
    if is_permission_message(message):
        raise KrakenLedgerPermissionMissingError(message)
    raise ValueError(message or malformed)


def _ledger_items(ledger: Mapping[str, object]) -> tuple[tuple[str, object], ...]:
    """Return ledger id and raw row pairs, rejecting blank ids."""
    items: list[tuple[str, object]] = []
    for ledger_id, raw in ledger.items():
        if not isinstance(ledger_id, str) or not ledger_id.strip():
            raise ValueError("Kraken ledger id is invalid")
        items.append((ledger_id, raw))
    return tuple(items)


def _parse_count(value: object) -> int:
    """Return a non-negative Kraken ``count``."""
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError("Kraken ledger count is invalid")
    return value


def _parse_ledger_entry(ledger_id: str, raw: object) -> KrakenLedgerEntry:
    """Parse one ledger object keyed by its venue id."""
    row = _as_mapping(raw, "Kraken ledger row is malformed")
    ledger_time = _decimal_text(row.get("time"), "time")
    return KrakenLedgerEntry(
        venue_ledger_id=ledger_id,
        refid=_text_field(row.get("refid"), "refid", allow_empty=True),
        ledger_time=ledger_time,
        ledger_second=_floor_second(ledger_time),
        ledger_type=_text_field(row.get("type"), "type", allow_empty=False),
        subtype=_text_field(row.get("subtype"), "subtype", allow_empty=True),
        aclass=_text_field(row.get("aclass"), "aclass", allow_empty=False),
        asset=_text_field(row.get("asset"), "asset", allow_empty=False),
        amount=_decimal_text(row.get("amount"), "amount"),
        fee=_decimal_text(row.get("fee"), "fee"),
        balance=_decimal_text(row.get("balance"), "balance"),
    )


def _text_field(value: object, field_name: str, *, allow_empty: bool) -> str:
    """Return a venue string field."""
    if not isinstance(value, str) or (not allow_empty and not value.strip()):
        raise ValueError(f"Kraken ledger field {field_name} is invalid")
    return value


def _decimal_text(value: object, field_name: str) -> str:
    """Return exact decimal text, preserving a venue string unchanged."""
    rendered = _render_decimal(value, field_name)
    try:
        parsed = Decimal(rendered)
    except InvalidOperation as exc:
        raise ValueError(f"Kraken ledger field {field_name} is invalid") from exc
    if not parsed.is_finite() or _negative_time(parsed, field_name):
        raise ValueError(f"Kraken ledger field {field_name} is invalid")
    return rendered


def _render_decimal(value: object, field_name: str) -> str:
    """Render a JSON decimal without changing a venue string."""
    if isinstance(value, str):
        return value
    if isinstance(value, bool) or not isinstance(value, int | float):
        raise ValueError(f"Kraken ledger field {field_name} is invalid")
    if isinstance(value, float):
        return _float_decimal_text(value, field_name)
    return str(value)


def _float_decimal_text(value: float, field_name: str) -> str:
    """Render a finite JSON number through its shortest decimal text."""
    if not math.isfinite(value):
        raise ValueError(f"Kraken ledger field {field_name} is invalid")
    return format(Decimal(str(value)), "f")


def _negative_time(parsed: Decimal, field_name: str) -> bool:
    """Return whether a ledger time is negative."""
    return parsed < 0 and field_name == "time"


def _floor_second(ledger_time: str) -> int:
    """Return the unix second containing a non-negative ledger time."""
    return int(Decimal(ledger_time).to_integral_value(rounding=ROUND_FLOOR))


def _absorb_page(absorb: _Absorb, page: KrakenLedgerPage, request: LedgerCollectRequest) -> None:
    """Keep unique in-window rows and flag contradictory or outside rows."""
    for entry in page.entries:
        if not entry_in_window(entry, request.start, request.end):
            absorb.unfaithful = True
            continue
        current = absorb.entries.get(entry.venue_ledger_id)
        if current is None:
            absorb.entries[entry.venue_ledger_id] = entry
            continue
        if current != entry:
            absorb.unfaithful = True


def _accept_count(state: _Collect, count: int) -> bool:
    """Record the first count and refuse a later page whose count moved."""
    if state.reported is not None and state.reported != count:
        return False
    state.reported = count
    return True


def _page_completes(page_len: int, seen: int, reported: int, page_size: int) -> bool:
    """Return whether another offset page cannot add ids for this count."""
    if page_len == 0 or page_len < page_size:
        return True
    return seen >= reported


async def _fetch_page(
    request: LedgerCollectRequest,
    fetch_page: Callable[[LedgerPageQuery], Awaitable[KrakenLedgerPage]],
    state: _Collect,
) -> KrakenLedgerPage | None:
    """Fetch one page, charging permission and timeout failures but not a pause."""
    query = LedgerPageQuery(start=request.start, end=request.end, offset=state.offset)
    page: KrakenLedgerPage | None
    try:
        page = await fetch_page(query)
    except KrakenLedgerPermissionMissingError:
        state.permission = True
        page = None
    except KrakenLedgerReadPausedError:
        state.paused = True
        return None
    except TimeoutError:
        state.timed_out = True
        page = None
    state.points += request.call_cost
    state.calls += 1
    if page is not None:
        state.offset += len(page.entries)
    return page


def _received_count(state: _Collect, request: LedgerCollectRequest) -> int:
    """Return unique in-window rows received in prior and current passes."""
    return request.received_count + len(state.absorb.entries)


def _window_cursor(
    state: _Collect,
    request: LedgerCollectRequest,
) -> tuple[int | None, tuple[str, ...]]:
    """Keep the newest second and all ids observed there across passes."""
    previous_second = request.window_cursor_second
    previous_ids = request.window_cursor_ids
    if not state.absorb.entries:
        return previous_second, previous_ids
    current_second, current_ids = cursor_after_success(
        tuple(state.absorb.entries.values()),
        request.end,
    )
    if previous_second is None or current_second > previous_second:
        return current_second, current_ids
    if current_second < previous_second:
        return previous_second, previous_ids
    return previous_second, tuple(sorted(set(previous_ids) | set(current_ids)))


def _outcome(
    state: _Collect,
    request: LedgerCollectRequest,
    *,
    forced: LedgerPassStatus | None = None,
) -> LedgerWindowOutcome:
    """Project walk state into paging progress and a possible success cursor."""
    received = _received_count(state, request)
    status = _status(state, received, forced)
    entries = tuple(sorted(state.absorb.entries.values(), key=_entry_sort_key))
    window_second, window_ids = _window_cursor(state, request)
    next_ofs = state.offset
    expected_count = state.reported
    cursor_second: int | None = None
    cursor_ids: tuple[str, ...] = ()
    if status == "ledger_cursor_gap":
        next_ofs = 0
        expected_count = None
        received = 0
        window_second = None
        window_ids = ()
    if status == "ok":
        cursor_second = request.end if received == 0 else window_second
        cursor_ids = () if received == 0 else window_ids
    return LedgerWindowOutcome(
        status=status,
        entries=entries,
        reported_count=state.reported,
        points_used=state.points,
        calls=state.calls,
        cursor_second=cursor_second,
        cursor_ids=cursor_ids,
        next_ofs=next_ofs,
        expected_count=expected_count,
        received_count=received,
        window_cursor_second=window_second,
        window_cursor_ids=window_ids,
    )


def _status(
    state: _Collect,
    received: int,
    forced: LedgerPassStatus | None,
) -> LedgerPassStatus:
    """Resolve permission, interruption, a forced gap, or count classification."""
    if state.permission:
        return "ledger_permission_missing"
    if state.paused or state.timed_out:
        return "ledger_backlog"
    if forced is not None:
        return forced
    return classify_ledger_window(
        received,
        state.reported,
        state.exhausted,
        state.absorb.unfaithful,
    )


def _entry_sort_key(entry: KrakenLedgerEntry) -> tuple[Decimal, str]:
    """Sort retained rows by exact numeric venue time then id."""
    return (Decimal(entry.ledger_time), entry.venue_ledger_id)
