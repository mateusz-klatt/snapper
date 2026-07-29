"""Tests for :meth:`SQLAlchemyRepository.count_table_stats`.

Pins the per-kind contract:

* EVENT tables: ``total`` via :meth:`_count_total_estimate`
  (dialect-aware — exact on SQLite, planner estimate on PostgreSQL);
  ``current`` / ``closed`` = ``None``;
  ``archivable`` = half-open window count (or ``None`` when no window).
* STATE tables: ``current = COUNT(known_to == KNOWN_TO_MAX)`` except
  explicit PostgreSQL active-index estimates such as ``candles``;
  ``total`` via :meth:`_count_total_estimate` (dialect-aware);
  ``total`` clamps no lower than ``current`` before
  ``closed = max(0, total - current)`` is derived;
  ``archivable`` = closed-window count (or ``None``).

Half-open window contract: rows AT
``datetime(day_start, 0, 0, UTC)`` are INCLUDED; rows AT
``datetime(day_end + 1d, 0, 0, UTC)`` are EXCLUDED.
"""

from datetime import UTC
from datetime import date
from datetime import datetime
from datetime import timedelta
from types import TracebackType

import pytest
from sqlalchemy import update

from snapper.data import repository as repo_module
from snapper.data.db_stats_types import TableCounters
from snapper.data.db_stats_types import TableEntry
from snapper.data.models import KNOWN_TO_MAX
from snapper.data.models import Candle
from snapper.data.models import Order
from snapper.data.models import Telemetry
from snapper.data.repository import SQLAlchemyRepository


@pytest.fixture
async def _repo() -> SQLAlchemyRepository:
    """Async fixture yielding a fresh in-memory aiosqlite repository."""
    repo = SQLAlchemyRepository("sqlite+aiosqlite:///:memory:")
    await repo.create_all()
    return repo


def _telemetry_entry() -> TableEntry:
    """``TableEntry`` for the ``telemetry`` event table."""
    return TableEntry(name="telemetry", kind="event", model=Telemetry)


def _orders_entry() -> TableEntry:
    """``TableEntry`` for the ``orders`` state table."""
    return TableEntry(name="orders", kind="state", model=Order)


def _candles_entry() -> TableEntry:
    """``TableEntry`` for candles with PostgreSQL active-index current estimation."""
    return TableEntry(
        name="candles",
        kind="state",
        model=Candle,
        current_estimate_index="uq_candle_itf_open",
    )


class _ScalarOneResult:
    """Stub SQLAlchemy result returning one scalar value."""

    def __init__(self, value: int) -> None:
        """Store the scalar result value."""
        self._value = value

    def scalar_one(self) -> int:
        """Return the configured scalar value."""
        return self._value


class _CurrentCountSession:
    """Stub async session returning one exact current-count result."""

    def __init__(self, current: int) -> None:
        """Store the current count and record executed statements."""
        self._current = current
        self.statements: list[object] = []

    async def execute(self, stmt: object, params: dict[str, str] | None = None) -> _ScalarOneResult:
        """Record the statement and return the configured current count."""
        self.statements.append((stmt, params))
        return _ScalarOneResult(self._current)


class _StubSessionContext:
    """Minimal async context manager for ``SQLAlchemyRepository.session``."""

    def __init__(self, session: object) -> None:
        """Store the object yielded from ``async with``."""
        self._session = session

    async def __aenter__(self) -> object:
        """Return the configured session object."""
        return self._session

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        """Leave exception handling to the caller."""


async def _insert_telemetry_row(
    repo: SQLAlchemyRepository,
    *,
    timestamp: datetime,
    public_id: str,
    session_id: str = "session-1",
    sequence_id: int = 1,
) -> None:
    """Insert ONE telemetry row at the supplied bus ``timestamp``."""
    async with repo.session() as s:
        s.add(
            Telemetry(
                public_id=public_id,
                timestamp=timestamp,
                session_id=session_id,
                sequence_id=sequence_id,
                transport="zmq",
                direction="in",
                message_type="tick",
                payload=None,
            )
        )
        await s.commit()


async def _insert_active_order(
    repo: SQLAlchemyRepository,
    *,
    public_id: str,
    timestamp: datetime,
    session_id: str = "s-1",
    sequence_id: int = 1,
) -> None:
    """Insert ONE active SCD2 order row (``known_to=KNOWN_TO_MAX``)."""
    async with repo.session() as s:
        s.add(
            Order(
                public_id=public_id,
                timestamp=timestamp,
                session_id=session_id,
                sequence_id=sequence_id,
                instrument_public_id="instr-1",
                wallet_public_id="wallet-1",
                operator_public_id="op-1",
                client_order_id=public_id,
                side="buy",
                order_type="market",
                size=1.0,
                filled_size=0.0,
                status="open",
                created_at=timestamp,
                known_to=KNOWN_TO_MAX,
            )
        )
        await s.commit()


async def _close_order_version(
    repo: SQLAlchemyRepository,
    *,
    public_id: str,
    closed_at: datetime,
) -> None:
    """Mark an existing order's active version as closed (``known_to < KNOWN_TO_MAX``)."""
    async with repo.session() as s:
        await s.execute(
            update(Order)
            .where(Order.public_id == public_id, Order.known_to == KNOWN_TO_MAX)
            .values(known_to=closed_at)
        )
        await s.commit()


class TestEventTableSemantics:
    """Append-only event tables."""

    @pytest.mark.asyncio
    async def test_empty_db_returns_zero_total_and_null_axes(
        self, _repo: SQLAlchemyRepository
    ) -> None:
        """No rows seeded → total=0, current/closed/archivable null."""
        counters = await _repo.count_table_stats(_telemetry_entry())
        assert counters == TableCounters(total=0, current=None, closed=None, archivable=None)

    @pytest.mark.asyncio
    async def test_seeded_rows_count_as_total(self, _repo: SQLAlchemyRepository) -> None:
        """5 telemetry rows → total=5; archivable null without a window."""
        for i in range(5):
            await _insert_telemetry_row(
                _repo,
                timestamp=datetime(2026, 5, 1, 0, 0, tzinfo=UTC),
                public_id=f"tel-{i}",
                sequence_id=i + 1,
            )
        counters = await _repo.count_table_stats(_telemetry_entry())
        assert counters == TableCounters(total=5, current=None, closed=None, archivable=None)

    @pytest.mark.asyncio
    async def test_archivable_window_counts_only_in_range(
        self, _repo: SQLAlchemyRepository
    ) -> None:
        """3 rows in window + 2 outside → archivable=3, total=5."""
        in_window = datetime(2026, 4, 15, 12, 0, tzinfo=UTC)
        outside_before = datetime(2026, 3, 15, 12, 0, tzinfo=UTC)
        outside_after = datetime(2026, 4, 30, 12, 0, tzinfo=UTC)
        for i, ts in enumerate((in_window, in_window, in_window, outside_before, outside_after)):
            await _insert_telemetry_row(
                _repo,
                timestamp=ts,
                public_id=f"tel-{i}",
                sequence_id=i + 1,
            )
        counters = await _repo.count_table_stats(
            _telemetry_entry(),
            archivable_window=(date(2026, 3, 30), date(2026, 4, 29)),
        )
        assert counters.total == 5
        assert counters.archivable == 3
        assert counters.current is None
        assert counters.closed is None

    @pytest.mark.asyncio
    async def test_archivable_boundary_includes_day_start_excludes_day_end_plus_one(
        self, _repo: SQLAlchemyRepository
    ) -> None:
        """Half-open contract: ``>= day_start midnight`` AND ``< day_end + 1d midnight``."""
        day_start = date(2026, 4, 1)
        day_end = date(2026, 4, 5)
        at_day_start_midnight = datetime(2026, 4, 1, 0, 0, tzinfo=UTC)
        at_day_end_last_microsecond = datetime(2026, 4, 5, 23, 59, 59, 999999, tzinfo=UTC)
        at_day_end_plus_one_midnight = datetime(2026, 4, 6, 0, 0, tzinfo=UTC)
        before_day_start = datetime(2026, 3, 31, 23, 59, 59, 999999, tzinfo=UTC)
        await _insert_telemetry_row(
            _repo,
            timestamp=at_day_start_midnight,
            public_id="tel-start",
            sequence_id=1,
        )
        await _insert_telemetry_row(
            _repo,
            timestamp=at_day_end_last_microsecond,
            public_id="tel-end",
            sequence_id=2,
        )
        await _insert_telemetry_row(
            _repo,
            timestamp=at_day_end_plus_one_midnight,
            public_id="tel-after",
            sequence_id=3,
        )
        await _insert_telemetry_row(
            _repo,
            timestamp=before_day_start,
            public_id="tel-before",
            sequence_id=4,
        )
        counters = await _repo.count_table_stats(
            _telemetry_entry(),
            archivable_window=(day_start, day_end),
        )
        assert counters.total == 4
        assert counters.archivable == 2


class TestStateTableSemantics:
    """SCD2-versioned state tables."""

    @pytest.mark.asyncio
    async def test_empty_db_returns_zero_axes(self, _repo: SQLAlchemyRepository) -> None:
        """No rows → total=0, current=0, closed=0, archivable null without window."""
        counters = await _repo.count_table_stats(_orders_entry())
        assert counters == TableCounters(total=0, current=0, closed=0, archivable=None)

    @pytest.mark.asyncio
    async def test_one_active_order_counts_as_current(self, _repo: SQLAlchemyRepository) -> None:
        """1 active order → current=1, closed=0, total=1."""
        await _insert_active_order(
            _repo,
            public_id="ord-1",
            timestamp=datetime(2026, 5, 1, 0, 0, tzinfo=UTC),
        )
        counters = await _repo.count_table_stats(_orders_entry())
        assert counters == TableCounters(total=1, current=1, closed=0, archivable=None)

    @pytest.mark.asyncio
    async def test_after_scd2_transition_split_current_and_closed(
        self, _repo: SQLAlchemyRepository
    ) -> None:
        """1 closed v1 + 1 active v2 of same order → current=1, closed=1, total=2."""
        await _insert_active_order(
            _repo,
            public_id="ord-1",
            timestamp=datetime(2026, 4, 30, 0, 0, tzinfo=UTC),
        )
        await _close_order_version(
            _repo,
            public_id="ord-1",
            closed_at=datetime(2026, 5, 1, 0, 0, tzinfo=UTC),
        )
        await _insert_active_order(
            _repo,
            public_id="ord-1",
            timestamp=datetime(2026, 5, 1, 0, 0, tzinfo=UTC),
            sequence_id=2,
        )
        counters = await _repo.count_table_stats(_orders_entry())
        assert counters == TableCounters(total=2, current=1, closed=1, archivable=None)

    @pytest.mark.asyncio
    async def test_archivable_state_window_counts_closed_versions_only(
        self, _repo: SQLAlchemyRepository
    ) -> None:
        """Active rows excluded from archivable; closed rows in window included."""
        in_window = datetime(2026, 4, 15, 12, 0, tzinfo=UTC)
        await _insert_active_order(_repo, public_id="ord-active", timestamp=in_window)
        await _insert_active_order(
            _repo, public_id="ord-closed", timestamp=in_window, sequence_id=2
        )
        await _close_order_version(
            _repo,
            public_id="ord-closed",
            closed_at=datetime(2026, 4, 20, 0, 0, tzinfo=UTC),
        )
        counters = await _repo.count_table_stats(
            _orders_entry(),
            archivable_window=(date(2026, 4, 1), date(2026, 4, 30)),
        )
        assert counters.current == 1
        assert counters.closed == 1
        assert counters.total == 2
        assert counters.archivable == 1


class TestKnownToMaxMatching:
    """Pin the SCD2 invariant: ``known_to`` is exactly ``KNOWN_TO_MAX`` or ``< KNOWN_TO_MAX``."""

    @pytest.mark.asyncio
    async def test_default_known_to_counts_as_current(self, _repo: SQLAlchemyRepository) -> None:
        """Row inserted via the ORM default counts as ``current``."""
        await _insert_active_order(
            _repo,
            public_id="ord-default",
            timestamp=datetime(2026, 5, 1, 0, 0, tzinfo=UTC),
        )
        counters = await _repo.count_table_stats(_orders_entry())
        assert counters.current == 1
        assert counters.closed == 0

    @pytest.mark.asyncio
    async def test_explicit_known_to_below_max_counts_as_closed(
        self, _repo: SQLAlchemyRepository
    ) -> None:
        """Row with explicit ``known_to`` shy of ``KNOWN_TO_MAX`` counts as ``closed``."""
        await _insert_active_order(
            _repo,
            public_id="ord-1",
            timestamp=datetime(2026, 5, 1, 0, 0, tzinfo=UTC),
        )
        await _close_order_version(
            _repo,
            public_id="ord-1",
            closed_at=KNOWN_TO_MAX - timedelta(seconds=1),
        )
        counters = await _repo.count_table_stats(_orders_entry())
        assert counters.current == 0
        assert counters.closed == 1


class TestDialectAwareTotalEstimate:
    """``_count_total_estimate`` dialect-specific behavior.

    Pins the implementation contract:

    * PostgreSQL emits a schema-safe ``pg_class JOIN pg_namespace``
      planner-estimate query (no full table scan).
    * SQLite emits an exact ``count(*)`` (acceptable at dev scale).
    * Unknown dialects raise ``NotImplementedError`` (fail loud rather
      than silently produce zero or wrong counts).
    * State ``total`` is clamped to the exact or index-estimated
      ``current`` floor before ``closed`` is derived, so a stale PG
      estimate (``total < current``) cannot leak impossible panel
      values.
    """

    @pytest.mark.asyncio
    async def test_postgresql_path_aggregates_ordinary_and_partition_leaf_estimates(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """PG totals include ordinary roots and every leaf below partitioned roots.

        The recursive catalog walk keeps multi-schema deployments safe,
        avoids a full table scan, and does not trust the unmaintained
        ``reltuples`` value on a partitioned parent.
        """
        captured: dict[str, object] = {}

        class _StubResult:
            def scalar_one_or_none(self) -> int:
                return 12345

        class _StubSession:
            async def execute(
                self, stmt: object, params: dict[str, str] | None = None
            ) -> _StubResult:
                captured["stmt"] = str(stmt)
                captured["params"] = params
                return _StubResult()

        repo = SQLAlchemyRepository("sqlite+aiosqlite:///:memory:")
        monkeypatch.setattr(type(repo), "dialect_name", "postgresql")
        result = await repo._count_total_estimate(_StubSession(), Order)
        assert result == 12345
        sql = " ".join(str(captured["stmt"]).lower().split())
        assert sql.startswith("with recursive")
        assert "pg_class" in sql
        assert "pg_namespace" in sql
        assert "n.nspname" in sql
        assert "c.relkind in ('r', 'p')" in sql
        assert "join pg_inherits i on i.inhparent = parent.oid" in sql
        assert "join pg_class child on child.oid = i.inhrelid" in sql
        assert "where parent.relkind = 'p'" in sql
        assert "sum(greatest(reltuples, 0)) filter (where relkind = 'r')" in sql
        assert captured["params"] == {"table": "orders", "schema": "public"}

    @pytest.mark.asyncio
    async def test_postgresql_index_estimate_aggregates_partition_leaf_indexes(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """PG index estimates include ordinary indexes and partitioned index leaves."""
        captured: dict[str, object] = {}

        class _StubResult:
            def scalar_one_or_none(self) -> int:
                return 272000000

        class _StubSession:
            async def execute(
                self, stmt: object, params: dict[str, str] | None = None
            ) -> _StubResult:
                captured["stmt"] = str(stmt)
                captured["params"] = params
                return _StubResult()

        repo = SQLAlchemyRepository("sqlite+aiosqlite:///:memory:")
        monkeypatch.setattr(type(repo), "dialect_name", "postgresql")
        result = await repo._count_index_estimate(_StubSession(), Candle, "uq_candle_itf_open")
        assert result == 272000000
        raw_sql = " ".join(str(captured["stmt"]).split())
        sql = raw_sql.lower()
        assert sql.startswith("with recursive")
        assert "pg_class" in sql
        assert "pg_namespace" in sql
        assert "n.nspname" in sql
        assert "c.relkind IN ('i', 'I')" in raw_sql
        assert "join pg_inherits i on i.inhparent = parent.oid" in sql
        assert "join pg_class child on child.oid = i.inhrelid" in sql
        assert "WHERE parent.relkind = 'I'" in raw_sql
        assert "sum(greatest(reltuples, 0)) filter (where relkind = 'i')" in sql
        assert captured["params"] == {"index": "uq_candle_itf_open", "schema": "public"}

    @pytest.mark.asyncio
    async def test_postgresql_index_estimate_missing_stats_returns_zero(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Given missing PG index stats, When estimated, Then the helper returns zero."""

        class _StubResult:
            def scalar_one_or_none(self) -> int | None:
                return None

        class _StubSession:
            async def execute(
                self, _stmt: object, _params: dict[str, str] | None = None
            ) -> _StubResult:
                return _StubResult()

        repo = SQLAlchemyRepository("sqlite+aiosqlite:///:memory:")
        monkeypatch.setattr(type(repo), "dialect_name", "postgresql")
        result = await repo._count_index_estimate(_StubSession(), Candle, "uq_candle_itf_open")
        assert result == 0

    @pytest.mark.asyncio
    async def test_sqlite_path_runs_exact_count(self, _repo: SQLAlchemyRepository) -> None:
        """SQLite dialect returns an exact count over the model."""
        await _insert_telemetry_row(
            _repo,
            timestamp=datetime(2026, 5, 1, 0, 0, tzinfo=UTC),
            public_id="tel-1",
            sequence_id=1,
        )
        async with _repo.session() as s:
            total = await _repo._count_total_estimate(s, Telemetry)
        assert total == 1

    @pytest.mark.asyncio
    async def test_unknown_dialect_raises(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Loud failure on unsupported dialect (no silent zero)."""

        class _StubSession:
            async def execute(self, *args: object, **kwargs: object) -> None:
                raise AssertionError("execute should not be called for unknown dialect")

        repo = SQLAlchemyRepository("sqlite+aiosqlite:///:memory:")
        monkeypatch.setattr(type(repo), "dialect_name", "mysql")
        with pytest.raises(NotImplementedError, match="dialect=mysql"):
            await repo._count_total_estimate(_StubSession(), Telemetry)
        with pytest.raises(NotImplementedError, match="dialect=mysql"):
            await repo._count_index_estimate(_StubSession(), Candle, "uq_candle_itf_open")

    @pytest.mark.asyncio
    async def test_total_clamps_to_current_when_estimate_below_current(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Given a stale low estimate, When counted, Then total clamps to the current floor.

        Simulates a PG state where ``current`` is 10 but
        ``_count_total_estimate`` returns 5 before autoanalyze catches
        up. The implementation must return ``total == current`` and
        ``closed == 0``.
        """
        repo = SQLAlchemyRepository("sqlite+aiosqlite:///:memory:")
        session = _CurrentCountSession(current=10)

        def _session_factory() -> _StubSessionContext:
            return _StubSessionContext(session)

        async def _stale_estimate(_self: object, _s: object, _model: object) -> int:
            return 5

        monkeypatch.setattr(repo, "session", _session_factory)
        monkeypatch.setattr(type(repo), "_count_total_estimate", _stale_estimate)
        counters = await repo.count_table_stats(_orders_entry())
        assert counters.current == 10
        assert counters.closed == 0
        assert counters.total == 10
        assert len(session.statements) == 1

    @pytest.mark.asyncio
    async def test_state_total_above_current_remains_unchanged(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Given estimate >= current, When counted, Then total and closed retain that estimate."""
        repo = SQLAlchemyRepository("sqlite+aiosqlite:///:memory:")
        session = _CurrentCountSession(current=2)

        def _session_factory() -> _StubSessionContext:
            return _StubSessionContext(session)

        async def _high_estimate(_self: object, _s: object, _model: object) -> int:
            return 12

        monkeypatch.setattr(repo, "session", _session_factory)
        monkeypatch.setattr(type(repo), "_count_total_estimate", _high_estimate)
        counters = await repo.count_table_stats(_orders_entry())
        assert counters.current == 2
        assert counters.total == 12
        assert counters.closed == 10
        assert len(session.statements) == 1

    @pytest.mark.asyncio
    async def test_candle_current_uses_index_estimate_on_postgresql(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Given candles on PG, When counted, Then current comes from the active index estimate."""
        repo = SQLAlchemyRepository("sqlite+aiosqlite:///:memory:")
        index_calls: list[tuple[object, str]] = []

        def _session_factory() -> _StubSessionContext:
            return _StubSessionContext(object())

        async def _index_estimate(_self: object, _s: object, model: object, index_name: str) -> int:
            index_calls.append((model, index_name))
            return 42

        async def _total_estimate(_self: object, _s: object, _model: object) -> int:
            return 100

        monkeypatch.setattr(repo, "session", _session_factory)
        monkeypatch.setattr(type(repo), "dialect_name", "postgresql")
        monkeypatch.setattr(type(repo), "_count_index_estimate", _index_estimate)
        monkeypatch.setattr(type(repo), "_count_total_estimate", _total_estimate)
        counters = await repo.count_table_stats(_candles_entry())
        assert counters == TableCounters(total=100, current=42, closed=58, archivable=None)
        assert index_calls == [(Candle, "uq_candle_itf_open")]

    @pytest.mark.asyncio
    async def test_candle_current_stays_exact_on_sqlite(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Given candles on SQLite, When indexed for PG, Then current still uses exact count."""
        repo = SQLAlchemyRepository("sqlite+aiosqlite:///:memory:")
        session = _CurrentCountSession(current=1)

        def _session_factory() -> _StubSessionContext:
            return _StubSessionContext(session)

        async def _index_estimate(
            _self: object, _s: object, _model: object, _index_name: str
        ) -> int:
            raise AssertionError("SQLite must not read PG index estimates")

        async def _total_estimate(_self: object, _s: object, _model: object) -> int:
            return 2

        monkeypatch.setattr(repo, "session", _session_factory)
        monkeypatch.setattr(type(repo), "_count_index_estimate", _index_estimate)
        monkeypatch.setattr(type(repo), "_count_total_estimate", _total_estimate)
        counters = await repo.count_table_stats(_candles_entry())
        assert counters == TableCounters(total=2, current=1, closed=1, archivable=None)
        assert len(session.statements) == 1

    @pytest.mark.asyncio
    async def test_non_candle_state_tables_keep_exact_current_on_postgresql(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Given non-candle state on PG, When counted, Then current remains an exact count."""
        repo = SQLAlchemyRepository("sqlite+aiosqlite:///:memory:")
        session = _CurrentCountSession(current=2)

        def _session_factory() -> _StubSessionContext:
            return _StubSessionContext(session)

        async def _index_estimate(
            _self: object, _s: object, _model: object, _index_name: str
        ) -> int:
            raise AssertionError("non-candle state tables must keep exact current")

        async def _total_estimate(_self: object, _s: object, _model: object) -> int:
            return 3

        monkeypatch.setattr(repo, "session", _session_factory)
        monkeypatch.setattr(type(repo), "dialect_name", "postgresql")
        monkeypatch.setattr(type(repo), "_count_index_estimate", _index_estimate)
        monkeypatch.setattr(type(repo), "_count_total_estimate", _total_estimate)
        counters = await repo.count_table_stats(_orders_entry())
        assert counters == TableCounters(total=3, current=2, closed=1, archivable=None)
        assert len(session.statements) == 1

    @pytest.mark.asyncio
    async def test_sqlite_guard_warns_above_threshold(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Soft warning when SQLite count returns more rows than the guard limit.

        The guard is informational — execution still completes; the
        snapshotter's per-table timeout is the hard bound.
        """

        class _BigCountResult:
            def scalar_one(self) -> int:
                return repo_module._SQLITE_COUNT_GUARD_ROWS + 1

        class _StubSession:
            async def execute(self, _stmt: object) -> _BigCountResult:
                return _BigCountResult()

        warnings: list[str] = []

        def _record(msg: str, *args: object, **kwargs: object) -> None:
            warnings.append(str(msg))

        monkeypatch.setattr(repo_module.logger, "warning", _record)

        repo = SQLAlchemyRepository("sqlite+aiosqlite:///:memory:")
        result = await repo._count_total_estimate(_StubSession(), Telemetry)
        assert result == repo_module._SQLITE_COUNT_GUARD_ROWS + 1
        assert any("guard threshold" in w for w in warnings)
