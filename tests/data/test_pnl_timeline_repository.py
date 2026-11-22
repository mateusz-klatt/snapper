"""Tests for the P&L timeline repository reads.

Exercises the two Phase-5A read surfaces on the real SQLite path:
``get_pnl_timeline_executions`` (scope-ordered executions joined to their active
Order lineage, with the exclusive ``since_scope_sequence`` watermark and the
wallet / mode filters) and ``get_accruals_for_pnl`` (funding accruals bounded by
``accrued_at <= as_of`` and ordered ascending). Rows are inserted directly
through the ORM so the read joins and filters are pinned independently of the
production ingest fence.
"""

from collections.abc import AsyncIterator
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from pathlib import Path
from typing import Final
from unittest.mock import Mock
from unittest.mock import patch

import pytest
from sqlalchemy import create_engine
from sqlalchemy.dialects import postgresql
from sqlalchemy.dialects import sqlite
from sqlalchemy.sql import Executable
from sqlalchemy.sql import Select

from snapper.data.models import AccrualLedger
from snapper.data.models import Execution
from snapper.data.models import ExecutionAnnulment
from snapper.data.models import ExecutionAnnulmentVisibility
from snapper.data.models import Order
from snapper.data.repository import SQLAlchemyRepository
from snapper.data.repository_types import PnlTimelineAccrualRow

_NOW = datetime(2026, 7, 20, 12, 0, tzinfo=UTC)
_TS = _NOW - timedelta(hours=3)
_SESSION = "00000000-0000-7000-8000-000000000901"
_WALLET = "0000face-0000-7000-8000-0000000000a1"
_OTHER_WALLET = "0000face-0000-7000-8000-0000000000a2"
_I1 = "00000000-0000-7000-8000-000000000b01"
_I2 = "00000000-0000-7000-8000-000000000b02"
_ORDER_I1 = "00000000-0000-7000-8000-000000000c01"
_ORDER_I2 = "00000000-0000-7000-8000-000000000c02"
_ORDER_PAPER = "00000000-0000-7000-8000-000000000c03"
_ORDER_OTHER = "00000000-0000-7000-8000-000000000c04"


def _order(public_id: str, instrument_public_id: str, wallet: str, mode: str) -> Order:
    """Build one active order carrying the instrument lineage the read joins."""
    return Order(
        public_id=public_id,
        instrument_public_id=instrument_public_id,
        wallet_public_id=wallet,
        mode=mode,
        created_at=_TS,
        timestamp=_TS,
        side="buy",
        order_type="limit",
        price=100.0,
        size=10.0,
        status="filled",
        session_id=_SESSION,
        sequence_id=1,
    )


def _execution(
    order_public_id: str,
    wallet: str,
    mode: str,
    exchange: str,
    scope_sequence: int,
    side: str,
    size: float,
    price: float,
    fee: float,
) -> Execution:
    """Build one active execution row for the scope-ordered read."""
    return Execution(
        order_public_id=order_public_id,
        wallet_public_id=wallet,
        operator_public_id=None,
        exchange=exchange,
        mode=mode,
        scope_sequence=scope_sequence,
        exec_id=f"exec-{exchange}-{scope_sequence}",
        trade_id=f"trade-{exchange}-{scope_sequence}",
        side=side,
        status="filled",
        price=price,
        size=size,
        fee=fee,
        fee_asset="USD",
        executed_at=None,
        liquidity_role="maker",
        timestamp=_TS,
        session_id=_SESSION,
        sequence_id=scope_sequence,
    )


def _accrual(
    instrument_public_id: str,
    wallet: str,
    mode: str,
    accrued_at: datetime,
    amount: float,
) -> AccrualLedger:
    """Build one active funding accrual row for the accrual read."""
    return AccrualLedger(
        instrument_public_id=instrument_public_id,
        wallet_public_id=wallet,
        operator_public_id=None,
        mode=mode,
        accrual_type="funding",
        accrued_at=accrued_at,
        amount=amount,
        amount_asset="USD",
        rate=0.0001,
        notional=1000.0,
        position_quantity_at_accrual=1.0,
        exchange="kraken",
        timestamp=_TS,
        session_id=_SESSION,
        sequence_id=1,
    )


@pytest.fixture
async def repository(tmp_path: Path) -> AsyncIterator[SQLAlchemyRepository]:
    """Create an isolated repository seeded with the P&L timeline read fixtures."""
    db_path = tmp_path / "pnl-timeline.db"
    schema_engine = create_engine(f"sqlite:///{db_path}")
    Order.__table__.create(schema_engine)
    Execution.__table__.create(schema_engine)
    ExecutionAnnulment.__table__.create(schema_engine)
    ExecutionAnnulmentVisibility.__table__.create(schema_engine)
    AccrualLedger.__table__.create(schema_engine)
    schema_engine.dispose()
    repo = SQLAlchemyRepository(f"sqlite+aiosqlite:///{db_path}")
    async with repo.session() as s:
        s.add_all(
            [
                _order(_ORDER_I1, _I1, _WALLET, "live"),
                _order(_ORDER_I2, _I2, _WALLET, "live"),
                _order(_ORDER_PAPER, _I1, _WALLET, "paper"),
                _order(_ORDER_OTHER, _I1, _OTHER_WALLET, "live"),
                _execution(_ORDER_I1, _WALLET, "live", "kraken", 1, "buy", 2.0, 100.0, 0.5),
                _execution(_ORDER_I1, _WALLET, "live", "kraken", 2, "sell", 1.0, 110.0, 0.3),
                _execution(_ORDER_I2, _WALLET, "live", "zonda", 1, "buy", 5.0, 50.0, 0.1),
                _execution(_ORDER_PAPER, _WALLET, "paper", "kraken", 1, "buy", 3.0, 100.0, 0.2),
                _execution(
                    _ORDER_OTHER, _OTHER_WALLET, "live", "kraken", 1, "buy", 4.0, 100.0, 0.4
                ),
                _accrual(_I2, _WALLET, "live", _NOW - timedelta(hours=1), 2.0),
                _accrual(_I1, _WALLET, "live", _NOW - timedelta(hours=2), 1.5),
                _accrual(_I1, _WALLET, "paper", _NOW - timedelta(hours=2), 9.0),
                _accrual(_I1, _OTHER_WALLET, "live", _NOW - timedelta(hours=2), 8.0),
                _accrual(_I1, _WALLET, "live", _NOW + timedelta(hours=1), 7.0),
            ]
        )
        await s.commit()
    try:
        yield repo
    finally:
        await repo.engine.dispose()


class TestGetPnlTimelineExecutions:
    """Cover the scope-ordered execution read."""

    async def test_returns_scope_ordered_rows_with_instrument_lineage(
        self, repository: SQLAlchemyRepository
    ) -> None:
        """The read returns the live scope ordered by (exchange, scope_sequence)."""
        rows = await repository.get_pnl_timeline_executions(_WALLET, "live", _NOW)
        assert [(r["exchange"], r["scope_sequence"]) for r in rows] == [
            ("kraken", 1),
            ("kraken", 2),
            ("zonda", 1),
        ]
        assert [r["instrument_public_id"] for r in rows] == [_I1, _I1, _I2]

    async def test_projects_execution_fields(self, repository: SQLAlchemyRepository) -> None:
        """The first row carries the execution economics and lineage keys."""
        rows = await repository.get_pnl_timeline_executions(_WALLET, "live", _NOW)
        first = rows[0]
        assert first["public_id"]
        assert first["order_public_id"] == _ORDER_I1
        assert first["side"] == "buy"
        assert first["status"] == "filled"
        assert first["size"] == 2.0
        assert first["price"] == 100.0
        assert first["fee"] == 0.5
        assert first["fee_asset"] == "USD"
        assert first["executed_at"] is None
        assert first["timestamp"] == _TS
        assert first["exec_id"] == "exec-kraken-1"
        assert first["trade_id"] == "trade-kraken-1"

    async def test_since_scope_sequence_is_exclusive_across_exchanges(
        self, repository: SQLAlchemyRepository
    ) -> None:
        """The scalar watermark drops every fill at or below it on all exchanges."""
        rows = await repository.get_pnl_timeline_executions(
            _WALLET, "live", _NOW, since_scope_sequence=1
        )
        assert [(r["exchange"], r["scope_sequence"]) for r in rows] == [("kraken", 2)]

    async def test_mode_filter_excludes_paper(self, repository: SQLAlchemyRepository) -> None:
        """A paper fill never appears in the live scope."""
        rows = await repository.get_pnl_timeline_executions(_WALLET, "live", _NOW)
        assert all(r["order_public_id"] != _ORDER_PAPER for r in rows)
        paper = await repository.get_pnl_timeline_executions(_WALLET, "paper", _NOW)
        assert [r["order_public_id"] for r in paper] == [_ORDER_PAPER]
        assert len(paper) == 1

    async def test_wallet_filter_excludes_other_wallet(
        self, repository: SQLAlchemyRepository
    ) -> None:
        """Another wallet's fills are never returned for this scope."""
        rows = await repository.get_pnl_timeline_executions(_WALLET, "live", _NOW)
        assert len(rows) == 3

    async def test_unknown_scope_is_empty(self, repository: SQLAlchemyRepository) -> None:
        """A wallet with no executions yields an empty list."""
        rows = await repository.get_pnl_timeline_executions(
            "0000face-0000-7000-8000-0000000000ff", "live", _NOW
        )
        assert rows == []

    async def test_clock_skew_prefix_is_not_dropped(self, repository: SQLAlchemyRepository) -> None:
        """The watermark map preserves skewed prefixes and excludes later commits.

        Kraken seq 1 is after ``as_of`` while seq 2 is before it, so the seq-2
        watermark must retain both rows. Kraken seq 3 remains excluded. Zonda's
        independent watermark retains only its seq 1. The future-stamped Order
        also proves immutable sentinel-current lineage cannot erase a prefix.
        """
        future = _NOW + timedelta(hours=1)
        skew_wallet = "0000face-0000-7000-8000-0000000000c9"
        skew_order = "00000000-0000-7000-8000-000000000d01"
        async with repository.session() as s:
            order = _order(skew_order, _I1, skew_wallet, "live")
            order.created_at = future
            order.timestamp = future
            exe1 = _execution(skew_order, skew_wallet, "live", "kraken", 1, "buy", 1.0, 100.0, 0.0)
            exe1.timestamp = future
            exe2 = _execution(skew_order, skew_wallet, "live", "kraken", 2, "sell", 1.0, 110.0, 0.0)
            exe2.timestamp = _TS
            exe3 = _execution(skew_order, skew_wallet, "live", "kraken", 3, "buy", 1.0, 120.0, 0.0)
            exe3.timestamp = future + timedelta(minutes=1)
            zonda1 = _execution(skew_order, skew_wallet, "live", "zonda", 1, "buy", 1.0, 50.0, 0.0)
            zonda1.timestamp = _TS
            zonda2 = _execution(skew_order, skew_wallet, "live", "zonda", 2, "sell", 1.0, 55.0, 0.0)
            zonda2.timestamp = future
            s.add_all([order, exe1, exe2, exe3, zonda1, zonda2])
            await s.commit()
        rows = await repository.get_pnl_timeline_executions(skew_wallet, "live", _NOW)
        assert [(r["exchange"], r["scope_sequence"]) for r in rows] == [
            ("kraken", 1),
            ("kraken", 2),
            ("zonda", 1),
        ]


class TestGetAccrualsForPnl:
    """Cover the funding accrual read."""

    async def test_returns_scope_accruals_ordered_by_accrued_at(
        self, repository: SQLAlchemyRepository
    ) -> None:
        """The read returns the live-scope accruals ascending by accrued_at."""
        rows = await repository.get_accruals_for_pnl(_WALLET, "live", _NOW)
        assert [r["instrument_public_id"] for r in rows] == [_I1, _I2]
        assert [r["amount"] for r in rows] == [1.5, 2.0]

    async def test_projects_accrual_fields(self, repository: SQLAlchemyRepository) -> None:
        """The first accrual row carries its native amount and provenance."""
        rows = await repository.get_accruals_for_pnl(_WALLET, "live", _NOW)
        first = rows[0]
        assert first["amount_asset"] == "USD"
        assert first["accrual_type"] == "funding"
        assert first["exchange"] == "kraken"
        assert first["mode"] == "live"
        assert first["accrued_at"] == _NOW - timedelta(hours=2)

    async def test_future_accrual_is_excluded(self, repository: SQLAlchemyRepository) -> None:
        """An accrual dated after as_of is bounded out of the window."""
        rows = await repository.get_accruals_for_pnl(_WALLET, "live", _NOW)
        assert all(r["accrued_at"] <= _NOW for r in rows)
        assert len(rows) == 2

    async def test_mode_and_wallet_filters(self, repository: SQLAlchemyRepository) -> None:
        """Paper and other-wallet accruals never leak into the live scope."""
        paper = await repository.get_accruals_for_pnl(_WALLET, "paper", _NOW)
        assert [r["amount"] for r in paper] == [9.0]
        other = await repository.get_accruals_for_pnl(_OTHER_WALLET, "live", _NOW)
        assert [r["amount"] for r in other] == [8.0]

    async def test_unknown_scope_is_empty(self, repository: SQLAlchemyRepository) -> None:
        """A wallet with no accruals yields an empty list."""
        rows = await repository.get_accruals_for_pnl(
            "0000face-0000-7000-8000-0000000000ff", "live", _NOW
        )
        assert rows == []

    async def test_unbounded_sql_is_byte_identical_to_baseline_for_sqlite_and_postgresql(
        self,
    ) -> None:
        """Keep the unbounded accrual SQL byte-identical when both bounds are ``None``.

        Given: ``get_accruals_for_pnl`` invoked with ``after_exclusive=None`` and
            ``through=None`` under SQLite and PostgreSQL dialects.
        When: Each executed statement is intercepted and compiled for its dialect.
        Then: The compiled SQL text matches the exact pre-bounds golden queries.
        """
        statements: list[Executable] = []
        repo = SQLAlchemyRepository("sqlite+aiosqlite:///:memory:")
        try:
            with patch.object(repo, "session", lambda: _CapturingAccrualSession(statements)):
                await repo.get_accruals_for_pnl(_WALLET, "live", _NOW)
                await repo.get_accruals_for_pnl(
                    _WALLET,
                    "live",
                    _NOW,
                    after_exclusive=None,
                    through=None,
                )
        finally:
            await repo.engine.dispose()
        assert len(statements) == 2
        for stmt in statements:
            assert isinstance(stmt, Select)
            assert str(stmt.compile(dialect=sqlite.dialect())) == _ACCRUAL_SQL_SQLITE
            assert str(stmt.compile(dialect=postgresql.dialect())) == _ACCRUAL_SQL_POSTGRESQL

    async def test_bounds_push_predicates_to_sql_and_match_python_filter(
        self, repository: SQLAlchemyRepository
    ) -> None:
        """Push ``(after_exclusive, through]`` bounds into SQL matching a Python filter.

        Given: Active accruals before, on, and after boundary instants including an
            accrual exactly at ``as_of`` and one after ``as_of``.
        When: ``get_accruals_for_pnl`` queries every combination of ``after_exclusive``
            and ``through`` bounds.
        Then: Every call emits the matching SQL predicates and returns the exact
            same ordered rows as filtering the unbounded read in Python.
        """
        t_earliest = _NOW - timedelta(hours=3)
        t_first = _NOW - timedelta(hours=2)
        t_second = _NOW - timedelta(hours=1)
        async with repository.session() as session:
            session.add_all(
                [
                    _accrual(_I1, _WALLET, "live", t_earliest, 0.5),
                    _accrual(_I2, _WALLET, "live", _NOW, 3.0),
                ]
            )
            await session.commit()

        baseline = await repository.get_accruals_for_pnl(_WALLET, "live", _NOW)
        assert [row["accrued_at"] for row in baseline] == [
            t_earliest,
            t_first,
            t_second,
            _NOW,
        ]
        bound_cases: list[tuple[datetime | None, datetime | None]] = [
            (None, None),
            (t_first, None),
            (None, t_second),
            (t_first, t_second),
            (t_first, _NOW),
            (t_second, t_second),
            (t_earliest, _NOW + timedelta(hours=2)),
        ]
        for after_exclusive, through in bound_cases:
            actual = await repository.get_accruals_for_pnl(
                _WALLET,
                "live",
                _NOW,
                after_exclusive=after_exclusive,
                through=through,
            )
            expected: list[PnlTimelineAccrualRow] = [
                row
                for row in baseline
                if (after_exclusive is None or row["accrued_at"] > after_exclusive)
                and (through is None or row["accrued_at"] <= through)
            ]
            assert actual == expected

        statements: list[Executable] = []
        with patch.object(repository, "session", lambda: _CapturingAccrualSession(statements)):
            await repository.get_accruals_for_pnl(
                _WALLET,
                "live",
                _NOW,
                after_exclusive=t_first,
                through=t_second,
            )
        compiled_bounded = str(statements[0].compile(dialect=sqlite.dialect()))
        assert "accrual_ledger.accrued_at > ?" in compiled_bounded
        assert compiled_bounded.count("accrual_ledger.accrued_at <= ?") == 2


_ACCRUAL_SQL_SQLITE: Final[str] = (
    "SELECT accrual_ledger.instrument_public_id, accrual_ledger.wallet_public_id, "
    "accrual_ledger.operator_public_id, accrual_ledger.mode, accrual_ledger.accrual_type, "
    "accrual_ledger.accrued_at, accrual_ledger.amount, accrual_ledger.amount_asset, "
    "accrual_ledger.rate, accrual_ledger.notional, "
    "accrual_ledger.position_quantity_at_accrual, accrual_ledger.exchange, "
    "accrual_ledger.id, accrual_ledger.public_id, accrual_ledger.session_id, "
    "accrual_ledger.sequence_id, accrual_ledger.timestamp, accrual_ledger.known_to \n"
    "FROM accrual_ledger \n"
    "WHERE accrual_ledger.wallet_public_id = ? AND accrual_ledger.mode = ? "
    "AND accrual_ledger.accrued_at <= ? AND accrual_ledger.timestamp <= ? "
    "AND accrual_ledger.known_to > ? ORDER BY accrual_ledger.accrued_at ASC"
)
"""Golden SQLite SQL for ``get_accruals_for_pnl`` when optional bounds are omitted."""

_ACCRUAL_SQL_POSTGRESQL: Final[str] = (
    "SELECT accrual_ledger.instrument_public_id, accrual_ledger.wallet_public_id, "
    "accrual_ledger.operator_public_id, accrual_ledger.mode, accrual_ledger.accrual_type, "
    "accrual_ledger.accrued_at, accrual_ledger.amount, accrual_ledger.amount_asset, "
    "accrual_ledger.rate, accrual_ledger.notional, "
    "accrual_ledger.position_quantity_at_accrual, accrual_ledger.exchange, "
    "accrual_ledger.id, accrual_ledger.public_id, accrual_ledger.session_id, "
    "accrual_ledger.sequence_id, accrual_ledger.timestamp, accrual_ledger.known_to \n"
    "FROM accrual_ledger \n"
    "WHERE accrual_ledger.wallet_public_id = %(wallet_public_id_1)s::UUID "
    "AND accrual_ledger.mode = %(mode_1)s::VARCHAR "
    "AND accrual_ledger.accrued_at <= %(accrued_at_1)s::TIMESTAMP WITH TIME ZONE "
    "AND accrual_ledger.timestamp <= %(timestamp_1)s::TIMESTAMP WITH TIME ZONE "
    "AND accrual_ledger.known_to > %(known_to_1)s::TIMESTAMP WITH TIME ZONE "
    "ORDER BY accrual_ledger.accrued_at ASC"
)
"""Golden PostgreSQL SQL for ``get_accruals_for_pnl`` when optional bounds are omitted."""


class _CapturingAccrualSession:
    """Async session stand-in recording compiled statements and yielding zero rows."""

    def __init__(self, statements: list[Executable]) -> None:
        """Store the list receiving executed statements.

        Args:
            statements: List collecting SQLAlchemy statements executed by the repo.
        """
        self._statements = statements

    async def __aenter__(self) -> _CapturingAccrualSession:
        """Enter the async session context.

        Returns:
            This capturing session.
        """
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        """Exit the async session context cleanly.

        Args:
            exc_info: Exception details, ignored.
        """
        del exc_info

    async def execute(self, statement: Executable) -> Mock:
        """Record an executed statement and return an empty scalar result.

        Args:
            statement: SQLAlchemy statement sent by the repository method.

        Returns:
            Mock result whose ``scalars().all()`` returns an empty list.
        """
        self._statements.append(statement)
        scalars = Mock(all=Mock(return_value=[]))
        return Mock(scalars=Mock(return_value=scalars))
