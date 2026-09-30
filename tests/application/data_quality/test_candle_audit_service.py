"""Tests for bounded persisted candle-window audits."""

from dataclasses import replace
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from pathlib import Path
from typing import cast
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import pytest
from sqlalchemy import event

import snapper.application.data_quality.candle_audit_service as candle_audit_service
from snapper.application.data_quality.candle_audit import CandleAnomalyType
from snapper.application.data_quality.candle_audit_service import MAX_CANDLE_AUDIT_SLOTS
from snapper.application.data_quality.candle_audit_service import CandleAuditRequest
from snapper.application.data_quality.candle_audit_service import CandleAuditRequestError
from snapper.application.data_quality.candle_audit_service import CandleAuditTargetError
from snapper.application.data_quality.candle_audit_service import audit_candle_window
from snapper.application.data_quality.candle_audit_service import build_candle_audit_report
from snapper.application.data_quality.candle_audit_service import candle_audit_report_document
from snapper.application.data_quality.candle_audit_service import validate_candle_audit_request
from snapper.core.types import AllExchange
from snapper.data.models import Candle
from snapper.data.models import Instrument
from snapper.data.models import Symbol
from snapper.data.repository import Repository
from snapper.data.repository import SQLAlchemyRepository
from snapper.data.repository_types import CandleRow
from snapper.data.repository_types import CandleWindowQuery
from snapper.infrastructure.symbols.functions import resolve_symbol_public_id

_EXCHANGE: AllExchange = "polygon"
_START = datetime(2026, 7, 1, 13, 30, tzinfo=UTC)
_END = _START + timedelta(minutes=2)
_AS_OF = _END + timedelta(minutes=1)


def _request() -> CandleAuditRequest:
    """Build one valid three-slot audit request."""
    return CandleAuditRequest(
        exchange=_EXCHANGE,
        symbol="AAPL",
        timeframe="1m",
        window_start=_START,
        window_end=_END,
        as_of=_AS_OF,
    )


def _candle(
    open_at: datetime,
    *,
    complete: bool = True,
    low: float = 99.0,
    timeframe: str = "1m",
) -> CandleRow:
    """Build one otherwise-valid persisted candle row."""
    return CandleRow(
        open_at=open_at,
        timeframe=timeframe,
        open=100.0,
        high=101.0,
        low=low,
        close=100.5,
        volume=10.0,
        vwap=None,
        trades=1,
        source="native",
        complete=complete,
        public_id=f"candle-{open_at.isoformat()}",
        timestamp=open_at,
        session_id="session",
        sequence_id=1,
    )


def _stored_candle(
    instrument_public_id: str,
    open_at: datetime,
    *,
    timeframe: str = "1m",
    timestamp: datetime | None = None,
    known_to: datetime | None = None,
) -> Candle:
    """Build one persisted candle model with explicit temporal coordinates."""
    candle = Candle(
        instrument_public_id=instrument_public_id,
        open_at=open_at,
        timeframe=timeframe,
        open=100.0,
        high=101.0,
        low=99.0,
        close=100.5,
        volume=10.0,
        vwap=100.25,
        trades=1,
        source="native",
        complete=True,
        timestamp=timestamp or open_at,
        session_id="session",
        sequence_id=1,
    )
    if known_to is not None:
        candle.known_to = known_to
    return candle


@pytest.mark.parametrize(
    ("audit_request", "message"),
    [
        (replace(_request(), symbol=""), "symbol must be non-empty"),
        (replace(_request(), symbol=" AAPL"), "symbol must be non-empty"),
        (replace(_request(), window_start=_START.replace(tzinfo=None)), "must include UTC offsets"),
        (replace(_request(), window_end=_END.replace(tzinfo=None)), "must include UTC offsets"),
        (replace(_request(), as_of=_AS_OF.replace(tzinfo=None)), "must include UTC offsets"),
        (replace(_request(), timeframe="2m"), "unsupported timeframe"),
        (replace(_request(), anchor_offset_seconds=-1), "anchor offset"),
        (replace(_request(), anchor_offset_seconds=60), "anchor offset"),
        (
            replace(_request(), timeframe="1d", anchor_offset_seconds=1),
            "daily audits require",
        ),
        (replace(_request(), window_start=_END, window_end=_START), "must not follow"),
        (
            replace(
                _request(),
                window_start=_START + timedelta(seconds=1),
            ),
            "timeframe grid",
        ),
        (
            replace(
                _request(),
                window_start=_START + timedelta(microseconds=1),
            ),
            "timeframe grid",
        ),
        (replace(_request(), window_end=_END + timedelta(seconds=1)), "timeframe grid"),
        (
            replace(
                _request(),
                timeframe="1d",
                window_end=_START + timedelta(days=1, minutes=1),
                as_of=_START + timedelta(days=3),
            ),
            "whole timeframe intervals",
        ),
        (
            replace(
                _request(),
                timeframe="1d",
                window_end=_START + timedelta(days=1, microseconds=800_000),
                as_of=_START + timedelta(days=3),
            ),
            "whole timeframe intervals",
        ),
        (
            replace(
                _request(),
                timeframe="1d",
                window_end=_START + timedelta(days=1),
                as_of=_START + timedelta(days=2),
            ),
            "calendar-aware",
        ),
        (replace(_request(), as_of=_END), "closed by as_of"),
        (
            replace(
                _request(),
                window_end=_START + timedelta(minutes=MAX_CANDLE_AUDIT_SLOTS),
                as_of=_START + timedelta(minutes=MAX_CANDLE_AUDIT_SLOTS + 1),
            ),
            "maximum",
        ),
    ],
)
def test_invalid_requests_fail_before_database_use(
    audit_request: CandleAuditRequest,
    message: str,
) -> None:
    """Every ambiguous or unsafe request is rejected by the pure validator.

    Given: A request with one unsafe or ambiguous field.
    When: The pure request validator runs.
    Then: It refuses before any repository can be used.
    """
    with pytest.raises(CandleAuditRequestError, match=message):
        validate_candle_audit_request(audit_request)


def test_request_datetimes_are_normalized_to_utc() -> None:
    """Offset-bearing inputs share one normalized UTC contract.

    Given: Equivalent window coordinates expressed at UTC+02:00.
    When: The request is validated.
    Then: Every coordinate is normalized to the expected UTC instant.
    """
    plus_two = timezone(timedelta(hours=2))
    request = replace(
        _request(),
        window_start=datetime(2026, 7, 1, 15, 30, tzinfo=plus_two),
        window_end=datetime(2026, 7, 1, 15, 32, tzinfo=plus_two),
        as_of=datetime(2026, 7, 1, 15, 33, tzinfo=plus_two),
    )
    normalized = validate_candle_audit_request(request)
    assert normalized.window_start == _START
    assert normalized.window_end == _END
    assert normalized.as_of == _AS_OF


def test_request_datetime_normalization_refuses_utc_underflow() -> None:
    """Aware inputs outside UTC's representable range fail as request errors.

    Given: A minimum-year datetime whose positive offset underflows in UTC.
    When: The pure request validator normalizes the request.
    Then: It raises a bounded request error instead of leaking OverflowError.
    """
    plus_fourteen = timezone(timedelta(hours=14))
    request = replace(
        _request(),
        window_start=datetime(1, 1, 1, tzinfo=plus_fourteen),
    )
    with pytest.raises(CandleAuditRequestError, match="cannot be normalized"):
        validate_candle_audit_request(request)


def test_single_daily_slot_is_allowed_without_guessing_a_venue_anchor() -> None:
    """The daily refusal is limited to calendar-dependent multi-slot windows.

    Given: One daily candle at a non-epoch-aligned session open and offset zero.
    When: The bounded report is built for that single expected open.
    Then: The report is clean because no cross-session calendar is required.
    """
    request = replace(
        _request(),
        timeframe="1d",
        window_end=_START,
        as_of=_START + timedelta(days=1),
    )
    report = build_candle_audit_report(
        request,
        [_candle(_START, timeframe="1d")],
    )
    assert report.status == "clean"
    assert report.slots_expected == 1


def test_exact_maximum_slot_window_is_accepted() -> None:
    """The documented materialization ceiling is inclusive.

    Given: A one-minute request containing exactly the maximum slot count.
    When: The request is validated.
    Then: It remains valid rather than being rejected one slot too early.
    """
    window_end = _START + timedelta(minutes=MAX_CANDLE_AUDIT_SLOTS - 1)
    request = replace(
        _request(),
        window_end=window_end,
        as_of=window_end + timedelta(minutes=1),
    )
    assert validate_candle_audit_request(request).window_end == window_end


def test_explicit_venue_anchor_is_validated_and_reported() -> None:
    """An operator-provided offset admits one non-epoch venue grid.

    Given: Hourly candles anchored at half past with the canonical 1800-second offset.
    When: The request and report are built.
    Then: The window is clean and discloses the exact grid anchor it used.
    """
    end = _START + timedelta(hours=2)
    request = replace(
        _request(),
        timeframe="1h",
        window_end=end,
        as_of=end + timedelta(hours=1),
        anchor_offset_seconds=1800,
    )
    candles = [_candle(_START + timedelta(hours=index), timeframe="1h") for index in range(3)]
    report = build_candle_audit_report(request, candles)
    assert report.status == "clean"
    assert report.anchor_offset_seconds == 1800
    assert candle_audit_report_document(report)["anchor_offset_seconds"] == 1800


def test_nonzero_venue_grid_requires_its_explicit_anchor() -> None:
    """A shifted venue grid is never guessed from its timestamps.

    Given: An hourly window opening at half past with the default zero offset.
    When: The request is validated.
    Then: It is rejected before any rows can be treated as aligned.
    """
    request = replace(
        _request(),
        timeframe="1h",
        window_end=_START + timedelta(hours=1),
        as_of=_START + timedelta(hours=2),
    )
    with pytest.raises(CandleAuditRequestError, match="timeframe grid"):
        validate_candle_audit_request(request)


def test_largest_canonical_hourly_anchor_is_accepted() -> None:
    """The upper valid offset remains available without aliasing the next grid.

    Given: A single hourly slot at second 3599 with anchor offset 3599.
    When: The request is validated.
    Then: The canonical upper-bound offset is retained.
    """
    start = datetime(2026, 7, 1, 13, 59, 59, tzinfo=UTC)
    request = replace(
        _request(),
        timeframe="1h",
        window_start=start,
        window_end=start,
        as_of=start + timedelta(hours=1),
        anchor_offset_seconds=3599,
    )
    assert validate_candle_audit_request(request).anchor_offset_seconds == 3599


def test_clean_exact_window_has_a_stable_report_document() -> None:
    """A complete exact window reports clean with stable machine fields.

    Given: One complete candle at every expected slot.
    When: The report and JSON-shaped document are built.
    Then: The status is clean and every contract field is deterministic.
    """
    candles = [_candle(_START + timedelta(minutes=index)) for index in range(3)]
    report = build_candle_audit_report(_request(), candles)
    document = candle_audit_report_document(report)
    assert report.status == "clean"
    assert report.anomalies == ()
    assert document == {
        "schema_version": 1,
        "status": "clean",
        "exchange": "polygon",
        "symbol": "AAPL",
        "timeframe": "1m",
        "window_start": _START.isoformat(),
        "window_end": _END.isoformat(),
        "as_of": _AS_OF.isoformat(),
        "gap_policy": "unsuppressed",
        "split_threshold": 0.3,
        "anchor_offset_seconds": 0,
        "slots_expected": 3,
        "candles_scanned": 3,
        "oldest_open_at": _START.isoformat(),
        "newest_open_at": _END.isoformat(),
        "anomaly_count": 0,
        "anomalies": [],
    }


def test_empty_window_fails_closed_at_the_requested_start() -> None:
    """No returned rows is an explicit anomaly rather than a clean audit.

    Given: A valid request whose persisted range contains no rows.
    When: The report is built.
    Then: An EMPTY_WINDOW finding replaces the pure core's empty-clean result.
    """
    report = build_candle_audit_report(_request(), [])
    assert report.status == "anomalies"
    assert [(item.type, item.open_at) for item in report.anomalies] == [
        (CandleAnomalyType.EMPTY_WINDOW, _START)
    ]
    document = candle_audit_report_document(report)
    assert document["oldest_open_at"] is None
    assert document["newest_open_at"] is None


def test_missing_edges_incomplete_rows_and_core_findings_are_sorted() -> None:
    """Service and core findings share one deterministic chronological order.

    Given: One corrupt incomplete middle candle with both boundaries absent.
    When: The service report combines window and pure-core findings.
    Then: All findings appear in deterministic timestamp and type order.
    """
    middle = _START + timedelta(minutes=1)
    report = build_candle_audit_report(
        _request(),
        [_candle(middle, complete=False, low=0.0)],
    )
    assert [item.type for item in report.anomalies] == [
        CandleAnomalyType.WINDOW_BOUNDARY_GAP,
        CandleAnomalyType.INCOMPLETE_CANDLE,
        CandleAnomalyType.NON_POSITIVE_PRICE,
        CandleAnomalyType.WINDOW_BOUNDARY_GAP,
    ]
    assert report.oldest_open_at == middle
    assert report.newest_open_at == middle


def test_present_boundaries_do_not_hide_an_interior_gap() -> None:
    """Unsuppressed continuity checks cover the inside of the named window.

    Given: Both requested edge candles with the middle slot absent.
    When: The service report is built.
    Then: The complete boundaries cannot make the interior omission clean.
    """
    report = build_candle_audit_report(
        _request(),
        [_candle(_START), _candle(_END)],
    )
    assert report.status == "anomalies"
    assert CandleAnomalyType.GAP in {item.type for item in report.anomalies}
    assert CandleAnomalyType.WINDOW_BOUNDARY_GAP not in {item.type for item in report.anomalies}


@pytest.mark.parametrize(
    ("candles", "missing_edge"),
    [
        ([_candle(_START + timedelta(minutes=1)), _candle(_END)], _START),
        ([_candle(_START), _candle(_START + timedelta(minutes=1))], _END),
    ],
)
def test_each_missing_boundary_is_reported_independently(
    candles: list[CandleRow],
    missing_edge: datetime,
) -> None:
    """Either window edge can fail without depending on the other edge.

    Given: A complete adjacent pair missing exactly one requested boundary.
    When: The service report is built.
    Then: Exactly that edge carries one boundary-gap finding.
    """
    report = build_candle_audit_report(_request(), candles)
    boundary_findings = [
        item for item in report.anomalies if item.type is CandleAnomalyType.WINDOW_BOUNDARY_GAP
    ]
    assert [(item.type, item.open_at) for item in boundary_findings] == [
        (CandleAnomalyType.WINDOW_BOUNDARY_GAP, missing_edge)
    ]


@pytest.mark.asyncio
async def test_service_uses_one_horizon_and_exact_native_symbol_range() -> None:
    """The atomic repository read receives the exact inclusive request.

    Given: An active native symbol and a complete three-candle range.
    When: The service audits the request.
    Then: One repository call carries the horizon and inclusive coordinates.
    """
    repository = AsyncMock(spec=Repository)
    candles = [_candle(_START + timedelta(minutes=index)) for index in range(3)]
    repository.get_candle_window_for_active_symbol.return_value = candles
    report = await audit_candle_window(cast(Repository, repository), _request())
    assert report.status == "clean"
    repository.get_candle_window_for_active_symbol.assert_awaited_once_with(
        CandleWindowQuery(
            native_symbol="AAPL",
            timeframe="1m",
            window_start=_START,
            window_end=_END,
            exchange="polygon",
            as_of=_AS_OF,
            limit=MAX_CANDLE_AUDIT_SLOTS + 1,
        )
    )


@pytest.mark.asyncio
async def test_unknown_symbol_is_distinct_from_an_active_empty_window() -> None:
    """An unresolved native symbol cannot masquerade as an empty clean range.

    Given: The atomic repository read reports no active target.
    When: The service audits the requested symbol and window.
    Then: It refuses rather than treating the result as active-empty.
    """
    repository = AsyncMock(spec=Repository)
    repository.get_candle_window_for_active_symbol.return_value = None
    audit_repository = cast(Repository, repository)
    request = _request()
    with pytest.raises(CandleAuditTargetError, match="not active"):
        await audit_candle_window(audit_repository, request)
    repository.get_candle_window_for_active_symbol.assert_awaited_once()


@pytest.mark.asyncio
async def test_oversized_repository_response_is_never_reported_clean() -> None:
    """A backend violating the materialization cap makes the audit incomplete.

    Given: A repository response larger than the hard audit cap.
    When: The service checks the materialized result.
    Then: It refuses instead of auditing a truncated or unbounded range.
    """
    repository = AsyncMock(spec=Repository)
    repository.get_candle_window_for_active_symbol.return_value = [_candle(_START)] * (
        MAX_CANDLE_AUDIT_SLOTS + 1
    )
    audit_repository = cast(Repository, repository)
    request = _request()
    with pytest.raises(CandleAuditRequestError, match="truncated or oversized"):
        await audit_candle_window(audit_repository, request)


@pytest.mark.asyncio
async def test_exact_maximum_repository_response_reaches_the_report_builder(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The repository materialization ceiling is inclusive too.

    Given: An active target whose boundary returns exactly the maximum row count.
    When: The service applies its truncation guard.
    Then: It delegates to the report builder rather than refusing one row early.
    """
    repository = AsyncMock(spec=Repository)
    candles = [_candle(_START)] * MAX_CANDLE_AUDIT_SLOTS
    repository.get_candle_window_for_active_symbol.return_value = candles
    expected = build_candle_audit_report(
        _request(),
        [_candle(_START), _candle(_START + timedelta(minutes=1)), _candle(_END)],
    )
    builder = MagicMock(return_value=expected)
    monkeypatch.setattr(candle_audit_service, "build_candle_audit_report", builder)
    report = await audit_candle_window(cast(Repository, repository), _request())
    assert report is expected
    builder.assert_called_once_with(_request(), candles)


@pytest.mark.asyncio
async def test_sqlite_service_resolves_the_native_symbol_and_exact_range(tmp_path: Path) -> None:
    """The real repository path resolves a native symbol rather than an instrument id.

    Given: A SQLite store with AAPL mapped to one instrument and three candles.
    When: The service audits the native-symbol request.
    Then: The exact rows resolve and the completed report is clean.
    """
    repository = SQLAlchemyRepository(f"sqlite+aiosqlite:///{tmp_path / 'audit.db'}")
    await repository.create_all()
    seeded_at = _START - timedelta(days=1)
    try:
        async with repository.session() as session:
            session.add(
                Symbol(
                    native_symbol="AAPL",
                    base="AAPL",
                    quote="USD",
                    asset_type="equity",
                    created_at=seeded_at,
                    timestamp=seeded_at,
                    session_id="session",
                    sequence_id=1,
                )
            )
            await session.commit()
        symbol_public_id = await resolve_symbol_public_id(repository, "AAPL", as_of=_AS_OF)
        assert symbol_public_id is not None
        _, instrument_public_id = await repository.ensure_instrument(
            symbol_public_id=symbol_public_id,
            exchange="polygon",
            session_id="session",
            sequence_id=1,
            timestamp=seeded_at,
        )
        statements: list[str] = []

        def record_statement(*args: object) -> None:
            statements.append(cast(str, args[2]))

        event.listen(repository.engine.sync_engine, "before_cursor_execute", record_statement)
        try:
            empty_report = await audit_candle_window(repository, _request())
        finally:
            event.remove(repository.engine.sync_engine, "before_cursor_execute", record_statement)
        assert empty_report.status == "anomalies"
        assert [item.type for item in empty_report.anomalies] == [CandleAnomalyType.EMPTY_WINDOW]
        assert len(statements) == 1
        assert "LEFT OUTER JOIN" in statements[0]
        missing_request = replace(_request(), symbol="MISSING")
        with pytest.raises(CandleAuditTargetError, match="not active"):
            await audit_candle_window(repository, missing_request)
        rows = [
            {
                "instrument_public_id": instrument_public_id,
                "timeframe": "1m",
                "open_at": _START + timedelta(minutes=index),
                "timestamp": _START + timedelta(minutes=index),
                "open": 100.0,
                "high": 101.0,
                "low": 99.0,
                "close": 100.5,
                "volume": 10.0,
                "vwap": 100.25,
                "trades": 1,
                "session_id": "session",
                "sequence_id": index + 1,
                "complete": True,
            }
            for index in range(3)
        ]
        assert await repository.upsert_candles(rows) == 3
        report = await audit_candle_window(repository, _request())
        assert report.status == "clean"
        assert report.candles_scanned == 3
    finally:
        await repository.engine.dispose()


@pytest.mark.asyncio
async def test_repository_refuses_overlapping_historical_target_identities(tmp_path: Path) -> None:
    """A corrupt historical overlap cannot be resolved by arbitrary row order.

    Given: Two Polygon instruments for one symbol whose validity overlaps the horizon.
    When: The atomic candle-window boundary resolves the native target.
    Then: It fails closed instead of selecting the lower database id.
    """
    repository = SQLAlchemyRepository(f"sqlite+aiosqlite:///{tmp_path / 'overlap.db'}")
    await repository.create_all()
    seeded_at = _START - timedelta(days=1)
    try:
        async with repository.session() as session:
            session.add(
                Symbol(
                    native_symbol="AAPL",
                    base="AAPL",
                    quote="USD",
                    asset_type="equity",
                    created_at=seeded_at,
                    timestamp=seeded_at,
                    session_id="session",
                    sequence_id=1,
                )
            )
            await session.commit()
        symbol_public_id = await resolve_symbol_public_id(repository, "AAPL", as_of=_AS_OF)
        assert symbol_public_id is not None
        await repository.ensure_instrument(
            symbol_public_id=symbol_public_id,
            exchange="polygon",
            session_id="session",
            sequence_id=1,
            timestamp=seeded_at,
        )
        async with repository.session() as session:
            session.add(
                Instrument(
                    symbol_public_id=symbol_public_id,
                    exchange="polygon",
                    session_id="overlap",
                    sequence_id=2,
                    timestamp=seeded_at,
                    known_to=_AS_OF + timedelta(minutes=1),
                )
            )
            await session.commit()
        query = CandleWindowQuery(
            native_symbol="AAPL",
            timeframe="1m",
            window_start=_START,
            window_end=_END,
            exchange="polygon",
            as_of=_AS_OF,
            limit=4,
        )
        with pytest.raises(RuntimeError, match="temporally ambiguous"):
            await repository.get_candle_window_for_active_symbol(query)
    finally:
        await repository.engine.dispose()


@pytest.mark.asyncio
async def test_repository_window_filters_temporal_and_coordinate_distractors(
    tmp_path: Path,
) -> None:
    """Every target and candle predicate contributes independently to the read.

    Given: Inactive identities plus future, closed, wrong-grid, and out-of-window rows.
    When: The atomic boundary reads the exact target with generous and small limits.
    Then: Only visible matching rows remain, in order, and the positive limit is honored.
    """
    repository = SQLAlchemyRepository(f"sqlite+aiosqlite:///{tmp_path / 'filters.db'}")
    await repository.create_all()
    seeded_at = _START - timedelta(days=1)
    read_as_of = _START + timedelta(hours=1)
    try:
        async with repository.session() as session:
            session.add(
                Symbol(
                    native_symbol="AAPL",
                    base="AAPL",
                    quote="USD",
                    asset_type="equity",
                    created_at=seeded_at,
                    timestamp=read_as_of,
                    session_id="session",
                    sequence_id=1,
                )
            )
            await session.commit()
        symbol_public_id = await resolve_symbol_public_id(repository, "AAPL", as_of=read_as_of)
        assert symbol_public_id is not None
        _, instrument_public_id = await repository.ensure_instrument(
            symbol_public_id=symbol_public_id,
            exchange="polygon",
            session_id="session",
            sequence_id=1,
            timestamp=read_as_of,
        )
        async with repository.session() as session:
            expired_symbol = Symbol(
                native_symbol="AAPL",
                base="STALE",
                quote="USD",
                asset_type="equity",
                created_at=seeded_at,
                timestamp=seeded_at,
                known_to=read_as_of,
                session_id="expired-symbol",
                sequence_id=2,
            )
            future_symbol = Symbol(
                native_symbol="AAPL",
                base="FUTURE",
                quote="USD",
                asset_type="equity",
                created_at=seeded_at,
                timestamp=read_as_of + timedelta(minutes=1),
                known_to=read_as_of + timedelta(hours=1),
                session_id="future-symbol",
                sequence_id=3,
            )
            session.add_all([expired_symbol, future_symbol])
            await session.flush()
            session.add_all(
                [
                    Instrument(
                        symbol_public_id=expired_symbol.public_id,
                        exchange="polygon",
                        session_id="expired-symbol",
                        sequence_id=2,
                        timestamp=seeded_at,
                    ),
                    Instrument(
                        symbol_public_id=symbol_public_id,
                        exchange="polygon",
                        session_id="expired-instrument",
                        sequence_id=3,
                        timestamp=seeded_at,
                        known_to=read_as_of,
                    ),
                    Instrument(
                        symbol_public_id=future_symbol.public_id,
                        exchange="polygon",
                        session_id="future-symbol",
                        sequence_id=4,
                        timestamp=seeded_at,
                        known_to=read_as_of + timedelta(hours=1),
                    ),
                    Instrument(
                        symbol_public_id=symbol_public_id,
                        exchange="polygon",
                        session_id="future-instrument",
                        sequence_id=5,
                        timestamp=read_as_of + timedelta(minutes=1),
                        known_to=read_as_of + timedelta(hours=1),
                    ),
                    Instrument(
                        symbol_public_id=symbol_public_id,
                        exchange="kraken",
                        session_id="wrong-exchange",
                        sequence_id=6,
                        timestamp=seeded_at,
                    ),
                    _stored_candle(
                        instrument_public_id,
                        _START + timedelta(minutes=2),
                        timestamp=read_as_of,
                    ),
                    _stored_candle(instrument_public_id, _START + timedelta(minutes=3)),
                    _stored_candle(instrument_public_id, _START + timedelta(minutes=4)),
                    _stored_candle(
                        instrument_public_id,
                        _START,
                        timestamp=read_as_of + timedelta(minutes=1),
                    ),
                    _stored_candle(
                        instrument_public_id,
                        _START + timedelta(minutes=1),
                        known_to=read_as_of,
                    ),
                    _stored_candle(instrument_public_id, _START, timeframe="5m"),
                    _stored_candle(instrument_public_id, _START - timedelta(minutes=1)),
                    _stored_candle(instrument_public_id, _START + timedelta(minutes=5)),
                ]
            )
            await session.commit()
        query = CandleWindowQuery(
            native_symbol="AAPL",
            timeframe="1m",
            window_start=_START,
            window_end=_START + timedelta(minutes=4),
            exchange="polygon",
            as_of=read_as_of,
            limit=10,
        )
        statements: list[str] = []

        def record_statement(*args: object) -> None:
            statements.append(cast(str, args[2]))

        event.listen(repository.engine.sync_engine, "before_cursor_execute", record_statement)
        try:
            rows = await repository.get_candle_window_for_active_symbol(query)
        finally:
            event.remove(repository.engine.sync_engine, "before_cursor_execute", record_statement)
        assert rows is not None
        assert len(statements) == 1
        normalized_sql = " ".join(statements[0].split())
        assert "ORDER BY candles.open_at ASC, candles.id ASC" in normalized_sql
        assert [row["open_at"] for row in rows] == [
            _START + timedelta(minutes=2),
            _START + timedelta(minutes=3),
            _START + timedelta(minutes=4),
        ]
        limited = await repository.get_candle_window_for_active_symbol(replace(query, limit=2))
        assert limited is not None
        assert [row["open_at"] for row in limited] == [
            _START + timedelta(minutes=2),
            _START + timedelta(minutes=3),
        ]
        for invalid_limit in (0, -1):
            invalid_query = replace(query, limit=invalid_limit)
            with pytest.raises(ValueError, match="limit must be positive"):
                await repository.get_candle_window_for_active_symbol(invalid_query)
    finally:
        await repository.engine.dispose()
