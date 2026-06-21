"""Repository tests for shadow candle SCD2 upserts."""

from datetime import UTC
from datetime import datetime
from datetime import timedelta
from types import SimpleNamespace
from types import TracebackType
from typing import cast

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from snapper.data.models import KNOWN_TO_MAX
from snapper.data.models import ShadowCandle
from snapper.data.repository import SQLAlchemyRepository
from snapper.data.repository_types import ShadowCandleUpsertRow

_INSTRUMENT_PUBLIC_ID = "00000000-0000-7000-8000-000000000101"
_SESSION_ID = "00000000-0000-7000-8000-000000000201"
_OPEN_AT = datetime(2026, 6, 20, 12, 0, tzinfo=UTC)


class _ScalarResult:
    """Scalar result fake returning one optional row."""

    def __init__(self, row: ShadowCandle | None) -> None:
        """Store the row returned by ``first``.

        Args:
            row: Optional shadow candle row.
        """
        self._row = row

    def first(self) -> ShadowCandle | None:
        """Return the configured row.

        Returns:
            Shadow candle row or None.
        """
        return self._row


class _ExecuteResult:
    """Execute result fake with a scalars facade."""

    def __init__(self, row: ShadowCandle | None) -> None:
        """Store the row returned through scalars.

        Args:
            row: Optional shadow candle row.
        """
        self._row = row

    def scalars(self) -> _ScalarResult:
        """Return a scalar-result facade.

        Returns:
            Scalar result fake.
        """
        return _ScalarResult(self._row)


class _ShadowSession:
    """Async session fake for shadow candle upsert tests."""

    def __init__(self, selected_rows: list[ShadowCandle | None]) -> None:
        """Initialize selected rows and call tracking.

        Args:
            selected_rows: Rows returned by successive SELECT calls.
        """
        self._selected_rows = selected_rows
        self.execute_calls: list[object] = []
        self.added: list[ShadowCandle] = []
        self.commit_called = False
        self.rollback_called = False

    async def execute(self, stmt: object) -> _ExecuteResult:
        """Record an execute call and return the next configured row.

        Args:
            stmt: SQLAlchemy statement object.

        Returns:
            Execute result fake.
        """
        self.execute_calls.append(stmt)
        if len(self.execute_calls) <= len(self._selected_rows):
            return _ExecuteResult(self._selected_rows[len(self.execute_calls) - 1])
        return _ExecuteResult(None)

    def add(self, row: ShadowCandle) -> None:
        """Record an ORM row scheduled for insert.

        Args:
            row: Shadow candle ORM object.

        Returns:
            None.
        """
        self.added.append(row)

    async def commit(self) -> None:
        """Record a commit call.

        Returns:
            None.
        """
        self.commit_called = True

    async def rollback(self) -> None:
        """Record a rollback call.

        Returns:
            None.
        """
        self.rollback_called = True


class _SessionContext:
    """Async context manager returning a configured session fake."""

    def __init__(self, session: _ShadowSession) -> None:
        """Store the session returned by the context manager.

        Args:
            session: Session fake.
        """
        self._session = session

    async def __aenter__(self) -> _ShadowSession:
        """Return the configured session.

        Returns:
            Session fake.
        """
        return self._session

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        """Leave the context manager.

        Args:
            exc_type: Exception type, if one was raised.
            exc: Exception instance, if one was raised.
            tb: Traceback, if one was raised.

        Returns:
            None.
        """
        return None


def _repo_for(session: _ShadowSession) -> SQLAlchemyRepository:
    """Build a repository object using the supplied session context.

    Args:
        session: Session fake returned from repository.session().

    Returns:
        SQLAlchemy repository shell.
    """
    repository = SQLAlchemyRepository.__new__(SQLAlchemyRepository)
    repository.session_factory = lambda: _SessionContext(session)
    return repository


def _shadow_row(
    *,
    open_at: datetime = _OPEN_AT,
    timestamp: datetime = _OPEN_AT,
    close: float = 101.0,
    source: str | None = "calculated",
    sequence_id: int = 1,
) -> ShadowCandleUpsertRow:
    """Build a shadow candle upsert row.

    Args:
        open_at: Candle window start.
        timestamp: SCD2 bus timestamp.
        close: Closing price.
        source: Optional source tag.
        sequence_id: Row sequence ID.

    Returns:
        Shadow candle upsert row.
    """
    row: ShadowCandleUpsertRow = {
        "instrument_public_id": _INSTRUMENT_PUBLIC_ID,
        "open_at": open_at,
        "timestamp": timestamp,
        "timeframe": "1m",
        "open": 100.0,
        "high": 102.0,
        "low": 99.0,
        "close": close,
        "volume": 5.5,
        "vwap": 100.8,
        "trades": 7,
        "complete": True,
        "session_id": _SESSION_ID,
        "sequence_id": sequence_id,
    }
    if source is not None:
        row["source"] = source
    return row


def _existing_shadow(
    *,
    close: float = 101.0,
    source: str = "calculated",
    complete: bool = True,
    public_id: str = "existing-shadow-public-id",
) -> ShadowCandle:
    """Build an existing shadow candle row facade.

    Args:
        close: Existing close value.
        source: Existing source tag.
        complete: Existing completeness flag.
        public_id: Existing public identity.

    Returns:
        Shadow candle facade.
    """
    return cast(
        ShadowCandle,
        SimpleNamespace(
            id=42,
            public_id=public_id,
            open=100.0,
            high=102.0,
            low=99.0,
            close=close,
            volume=5.5,
            vwap=100.8,
            trades=7,
            source=source,
            complete=complete,
        ),
    )


@pytest.mark.asyncio
async def test_upsert_shadow_candles_empty_batch_is_noop() -> None:
    """Empty shadow candle batches are no-ops.

    Given: A repository shell,
    When: upsert_shadow_candles receives an empty batch,
    Then: it returns zero and never opens a session.

    Returns:
        None.
    """
    session = _ShadowSession([])
    repository = _repo_for(session)
    assert await repository.upsert_shadow_candles([]) == 0
    assert session.execute_calls == []
    assert session.added == []


@pytest.mark.asyncio
async def test_upsert_shadow_candles_inserts_and_defaults_identity() -> None:
    """A new shadow candle row is inserted with repository defaults.

    Given: A shadow row without public_id or known_to,
    When: it is upserted,
    Then: the repository fills both defaults and schedules one insert.

    Returns:
        None.
    """
    session = _ShadowSession([None])
    repository = _repo_for(session)
    row = _shadow_row()
    count = await repository.upsert_shadow_candles([row])
    assert count == 1
    assert row["known_to"] == KNOWN_TO_MAX
    assert row["public_id"]
    assert len(session.added) == 1
    assert session.added[0].public_id == row["public_id"]
    assert session.commit_called is True


@pytest.mark.asyncio
async def test_upsert_shadow_candles_identical_reupsert_is_noop() -> None:
    """An identical shadow candle re-upsert does not create a new version.

    Given: An active shadow candle row matching the incoming values,
    When: the row is upserted,
    Then: no successor is inserted and the count is zero.

    Returns:
        None.
    """
    session = _ShadowSession([_existing_shadow()])
    repository = _repo_for(session)
    count = await repository.upsert_shadow_candles([_shadow_row()])
    assert count == 0
    assert len(session.execute_calls) == 1
    assert session.added == []
    assert session.commit_called is True


@pytest.mark.asyncio
async def test_upsert_shadow_candles_versions_changed_business_values() -> None:
    """A changed shadow candle closes the predecessor and inserts a successor.

    Given: An active shadow candle row with a different close value,
    When: a correction for the same natural key is upserted,
    Then: the old row is closed and the successor reuses its public_id.

    Returns:
        None.
    """
    session = _ShadowSession([_existing_shadow(close=100.0)])
    repository = _repo_for(session)
    count = await repository.upsert_shadow_candles(
        [_shadow_row(timestamp=_OPEN_AT + timedelta(seconds=10), close=101.5)]
    )
    assert count == 1
    assert len(session.execute_calls) == 2
    assert len(session.added) == 1
    assert session.added[0].public_id == "existing-shadow-public-id"
    assert session.added[0].close == pytest.approx(101.5)


@pytest.mark.asyncio
async def test_upsert_shadow_candles_source_distinct_rows_coexist() -> None:
    """The shadow natural key keeps sources distinct.

    Given: Two shadow rows sharing instrument, timeframe, and open_at,
    When: they use different source values,
    Then: both are scheduled as independent inserts.

    Returns:
        None.
    """
    session = _ShadowSession([None, None])
    repository = _repo_for(session)
    rows = [_shadow_row(source="calculated"), _shadow_row(source="native", sequence_id=2)]
    assert await repository.upsert_shadow_candles(rows) == 2
    assert len(session.added) == 2
    assert {row.source for row in session.added} == {"calculated", "native"}


@pytest.mark.asyncio
async def test_upsert_shadow_candles_without_source_matches_native_default() -> None:
    """Omitting source uses the native natural key for matching.

    Given: An existing native shadow row and an incoming row without source,
    When: the incoming row is upserted,
    Then: the active row is treated as identical and no-op.

    Returns:
        None.
    """
    session = _ShadowSession([_existing_shadow(source="native")])
    repository = _repo_for(session)
    assert await repository.upsert_shadow_candles([_shadow_row(source=None)]) == 0
    assert session.added == []


@pytest.mark.asyncio
async def test_upsert_shadow_candles_respects_session_ownership() -> None:
    """Caller-provided sessions remain caller-committed.

    Given: One caller-owned session and one repository-owned session,
    When: shadow rows are upserted through both paths,
    Then: the provided session is not committed and the owned path commits.

    Returns:
        None.
    """
    provided_session = _ShadowSession([None])
    repository = _repo_for(_ShadowSession([None]))
    count = await repository.upsert_shadow_candles(
        [_shadow_row(open_at=_OPEN_AT + timedelta(minutes=1))],
        session=cast(AsyncSession, provided_session),
    )
    assert count == 1
    assert provided_session.commit_called is False
    assert len(provided_session.added) == 1

    owned_session = _ShadowSession([None])
    owned_repository = _repo_for(owned_session)
    own_count = await owned_repository.upsert_shadow_candles(
        [_shadow_row(open_at=_OPEN_AT + timedelta(minutes=2))]
    )
    assert own_count == 1
    assert owned_session.commit_called is True


@pytest.mark.asyncio
async def test_upsert_shadow_candles_keeps_caller_supplied_identity() -> None:
    """Caller-supplied public_id and known_to are preserved, not defaulted.

    Given: A shadow row that already carries public_id and known_to,
    When: it is upserted as a new row,
    Then: both supplied values are kept unchanged and used for the insert.

    Returns:
        None.
    """
    session = _ShadowSession([None])
    repository = _repo_for(session)
    row = _shadow_row()
    supplied_known_to = _OPEN_AT + timedelta(days=1)
    row["public_id"] = "caller-supplied-public-id"
    row["known_to"] = supplied_known_to
    count = await repository.upsert_shadow_candles([row])
    assert count == 1
    assert row["public_id"] == "caller-supplied-public-id"
    assert row["known_to"] == supplied_known_to
    assert session.added[0].public_id == "caller-supplied-public-id"


@pytest.mark.asyncio
async def test_upsert_shadow_candles_provided_session_identical_is_noop() -> None:
    """A caller-session identical re-upsert is a no-op without committing.

    Given: A caller-owned session whose active row matches the incoming row,
    When: the row is upserted on that session,
    Then: no successor is added, the count is zero, and the caller keeps commit.

    Returns:
        None.
    """
    provided_session = _ShadowSession([_existing_shadow()])
    repository = _repo_for(_ShadowSession([]))
    count = await repository.upsert_shadow_candles(
        [_shadow_row()], session=cast(AsyncSession, provided_session)
    )
    assert count == 0
    assert provided_session.added == []
    assert provided_session.commit_called is False
