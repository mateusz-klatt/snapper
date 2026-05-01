"""Tests for :meth:`SQLAlchemyRepository.count_table_stats`.

Pins the per-kind contract from
``proprietary/plans/plan_observability_cluster_b.md`` §7.2 + §3.3:

* EVENT tables: ``total = COUNT(*)``; ``current`` / ``closed`` = ``None``;
  ``archivable`` = half-open window count (or ``None`` when no window).
* STATE tables: ``current = COUNT(known_to == KNOWN_TO_MAX)``;
  ``closed = COUNT(known_to != KNOWN_TO_MAX)``;
  ``total = current + closed`` (Python addition);
  ``archivable`` = closed-only window count (or ``None``).

Half-open window contract: rows AT
``datetime(day_start, 0, 0, UTC)`` are INCLUDED; rows AT
``datetime(day_end + 1d, 0, 0, UTC)`` are EXCLUDED.
"""

from datetime import UTC
from datetime import date
from datetime import datetime
from datetime import timedelta

import pytest
from sqlalchemy import update

from snapper.data.db_stats_types import TableCounters
from snapper.data.db_stats_types import TableEntry
from snapper.data.models import KNOWN_TO_MAX
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
    """Per-plan §3.1 — append-only event tables."""

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
    """Per-plan §3.2 — SCD2-versioned state tables."""

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
