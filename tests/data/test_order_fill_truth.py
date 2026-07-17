"""Fill-truth semantics of ``update_order`` (PnL Phase 1).

Pins the repaired contract: a fill update (``filled_size`` provided)
writes both fill columns authoritatively — deriving a VWAP from the
order's active executions when the venue reported no average, but only
when the execution delta sum reproduces the venue cumulative — while a
status-only update carries both columns forward. Also pins the
clock-regression guard (``max(bus, old.timestamp)``) and the
flush-before-commit successor id.
"""

import math
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from pathlib import Path

import pytest
from sqlalchemy import select

from snapper.data.models import KNOWN_TO_MAX
from snapper.data.models import Instrument
from snapper.data.models import Order
from snapper.data.repository import SQLAlchemyRepository
from snapper.data.repository_types import OrderInsertRow

_WALLET = "00000000-0000-7000-8000-000000000001"
"""Canonical UUID wallet identity — ``insert_execution`` validates it."""


async def _active_order(repo: SQLAlchemyRepository, public_id: str) -> Order:
    """Load the single active SCD2 version of an order.

    Args:
        repo: Repository owning the session factory.
        public_id: Stable order identity.

    Returns:
        The active ORM row.
    """
    async with repo.session() as s:
        result = await s.execute(
            select(Order).where(Order.public_id == public_id, Order.known_to == KNOWN_TO_MAX)
        )
        return result.scalars().one()


def _order_row(now: datetime) -> OrderInsertRow:
    """Build a minimal open-order insert row.

    Args:
        now: Bus timestamp for created_at/updated_at.

    Returns:
        Insert row for a live BTC-USD market order of size 1.0.
    """
    return {
        "instrument_public_id": "inst-btc",
        "client_order_id": "cid-fill-1",
        "exchange_order_id": "ex-1",
        "created_at": now,
        "side": "buy",
        "order_type": "market",
        "price": None,
        "size": 1.0,
        "status": "open",
        "time_in_force": "gtc",
        "session_id": "s1",
        "sequence_id": 1,
        "timestamp": now,
        "wallet_public_id": _WALLET,
    }


async def _seeded_repo(tmp_path: Path, name: str) -> tuple[SQLAlchemyRepository, int, str]:
    """Create a repo with one open order row and its active instrument.

    The instrument leg exists because execution ingest resolves the fill's
    immutable scope from the active Order -> Instrument lineage and fails
    closed when either leg is missing.

    Args:
        tmp_path: Pytest temporary directory.
        name: Database file name.

    Returns:
        Tuple of repository, order row id, and order public_id.
    """
    repo = SQLAlchemyRepository(f"sqlite+aiosqlite:///{tmp_path / name}")
    await repo.create_all()
    now = datetime.now(UTC)
    async with repo.session() as s:
        s.add(
            Instrument(
                public_id="inst-btc",
                symbol_public_id="inst-btc",
                exchange="kraken",
                timestamp=now - timedelta(days=1),
                session_id="s1",
                sequence_id=1,
            )
        )
        await s.commit()
    order_id, order_public_id = await repo.insert_order(**_order_row(now))
    return repo, order_id, order_public_id


async def _insert_fill(
    repo: SQLAlchemyRepository,
    order_public_id: str,
    *,
    price: float,
    size: float,
    sequence_id: int,
) -> None:
    """Insert one partial-fill execution delta row.

    Args:
        repo: Target repository.
        order_public_id: Stable order identity linking the fill.
        price: Delta fill price.
        size: Delta fill size.
        sequence_id: Bus sequence for the insert.
    """
    await repo.insert_execution(
        order_public_id=order_public_id,
        timestamp=datetime.now(UTC),
        side="buy",
        status="partial",
        price=price,
        size=size,
        fee=0.0,
        fee_asset="USD",
        wallet_public_id=_WALLET,
        session_id="s1",
        sequence_id=sequence_id,
    )


@pytest.mark.asyncio
async def test_fill_update_writes_explicit_average(tmp_path: Path) -> None:
    """A fill update with a venue average writes both columns verbatim.

    Given: an open order and a venue frame reporting cumulative 0.4 at
        average 100.5,
    When: ``update_order`` runs as a fill update,
    Then: the successor row carries exactly those values.
    """
    repo, order_id, order_public_id = await _seeded_repo(tmp_path, "fill_explicit.db")
    now = datetime.now(UTC)
    new_id = await repo.update_order(
        order_id=order_id,
        status="open",
        updated_at=now,
        session_id="s1",
        sequence_id=2,
        timestamp=now,
        filled_size=0.4,
        average_price=100.5,
    )
    assert new_id != order_id
    row = await _active_order(repo, order_public_id)
    assert row.filled_size == 0.4
    assert row.average_price == 100.5


@pytest.mark.asyncio
async def test_fill_update_derives_vwap_when_sums_match(tmp_path: Path) -> None:
    """Missing venue average derives the VWAP from matching executions.

    Given: two persisted execution deltas (0.3 @ 100 and 0.1 @ 104)
        and a venue frame reporting cumulative 0.4 with NO average,
    When: ``update_order`` runs as a fill update,
    Then: the successor's average is the executions VWAP (101.0).
    """
    repo, order_id, order_public_id = await _seeded_repo(tmp_path, "fill_vwap.db")
    await _insert_fill(repo, order_public_id, price=100.0, size=0.3, sequence_id=2)
    await _insert_fill(repo, order_public_id, price=104.0, size=0.1, sequence_id=3)
    now = datetime.now(UTC)
    await repo.update_order(
        order_id=order_id,
        status="open",
        updated_at=now,
        session_id="s1",
        sequence_id=4,
        timestamp=now,
        filled_size=0.4,
        average_price=None,
    )
    row = await _active_order(repo, order_public_id)
    assert row.filled_size == 0.4
    assert row.average_price is not None
    assert math.isclose(row.average_price, 101.0)


@pytest.mark.asyncio
async def test_fill_update_nulls_average_on_sum_mismatch(tmp_path: Path) -> None:
    """A delta-sum mismatch stores an explicit NULL average.

    Given: one persisted execution delta (0.3 @ 100) but a venue frame
        reporting cumulative 0.4 (a missed frame) with no average,
    When: ``update_order`` runs as a fill update,
    Then: the successor's average is NULL — never a skewed VWAP, a
        last-delta price, or a stale predecessor value.
    """
    repo, order_id, order_public_id = await _seeded_repo(tmp_path, "fill_mismatch.db")
    await _insert_fill(repo, order_public_id, price=100.0, size=0.3, sequence_id=2)
    now = datetime.now(UTC)
    await repo.update_order(
        order_id=order_id,
        status="open",
        updated_at=now,
        session_id="s1",
        sequence_id=3,
        timestamp=now,
        filled_size=0.4,
        average_price=None,
    )
    row = await _active_order(repo, order_public_id)
    assert row.filled_size == 0.4
    assert row.average_price is None


@pytest.mark.asyncio
async def test_fill_update_nulls_average_without_executions(tmp_path: Path) -> None:
    """A fill update with no persisted executions stores NULL average.

    Given: a venue frame reporting cumulative fill with no average and
        no execution rows persisted for the order,
    When: ``update_order`` runs as a fill update,
    Then: the successor's average is NULL.
    """
    repo, order_id, order_public_id = await _seeded_repo(tmp_path, "fill_noexec.db")
    now = datetime.now(UTC)
    await repo.update_order(
        order_id=order_id,
        status="open",
        updated_at=now,
        session_id="s1",
        sequence_id=2,
        timestamp=now,
        filled_size=0.2,
        average_price=None,
    )
    row = await _active_order(repo, order_public_id)
    assert row.average_price is None


@pytest.mark.asyncio
async def test_status_only_update_carries_fill_columns_forward(tmp_path: Path) -> None:
    """A status-only transition preserves the last persisted fill truth.

    Given: an order whose active version carries filled_size 0.4 at
        average 100.5 (a prior partial),
    When: a cancel-style status-only ``update_order`` runs
        (``filled_size=None``),
    Then: the canceled successor still shows the partial fill.
    """
    repo, order_id, order_public_id = await _seeded_repo(tmp_path, "fill_carry.db")
    now = datetime.now(UTC)
    partial_id = await repo.update_order(
        order_id=order_id,
        status="open",
        updated_at=now,
        session_id="s1",
        sequence_id=2,
        timestamp=now,
        filled_size=0.4,
        average_price=100.5,
    )
    later = now + timedelta(seconds=1)
    await repo.update_order(
        order_id=partial_id,
        status="canceled",
        updated_at=later,
        session_id="s1",
        sequence_id=3,
        timestamp=later,
    )
    row = await _active_order(repo, order_public_id)
    assert row.status == "canceled"
    assert row.filled_size == 0.4
    assert row.average_price == 100.5


@pytest.mark.asyncio
async def test_clock_regression_uses_effective_timestamp(tmp_path: Path) -> None:
    """A lagging caller clock cannot invert the validity interval.

    Given: an order row stamped at bus time T and a caller whose clock
        reads T-10s,
    When: ``update_order`` runs with the stale timestamp,
    Then: both the closed version's ``known_to`` and the successor's
        ``timestamp`` use the row's own (later) bus time, keeping the
        SCD2 interval monotone and the successor readable as-of T.
    """
    repo, order_id, order_public_id = await _seeded_repo(tmp_path, "fill_clock.db")
    seeded = await _active_order(repo, order_public_id)
    seeded_ts = seeded.timestamp
    stale = seeded_ts - timedelta(seconds=10)
    await repo.update_order(
        order_id=order_id,
        status="open",
        updated_at=stale,
        session_id="s1",
        sequence_id=2,
        timestamp=stale,
        filled_size=0.1,
        average_price=99.0,
    )
    row = await _active_order(repo, order_public_id)
    assert row.filled_size == 0.1
    assert row.timestamp == seeded_ts


@pytest.mark.asyncio
async def test_status_only_update_ignores_stray_average(tmp_path: Path) -> None:
    """Contradictory input (average without cumulative) is ignored.

    Given: an order whose active version carries a persisted partial
        (0.4 @ 100.5) and a status-only ``update_order`` call that
        supplies an ``average_price`` WITHOUT ``filled_size``,
    When: the transition runs,
    Then: BOTH fill columns carry forward — a stray average can never
        detach the pair.
    """
    repo, order_id, order_public_id = await _seeded_repo(tmp_path, "fill_stray.db")
    now = datetime.now(UTC)
    partial_id = await repo.update_order(
        order_id=order_id,
        status="open",
        updated_at=now,
        session_id="s1",
        sequence_id=2,
        timestamp=now,
        filled_size=0.4,
        average_price=100.5,
    )
    later = now + timedelta(seconds=1)
    await repo.update_order(
        order_id=partial_id,
        status="canceled",
        updated_at=later,
        session_id="s1",
        sequence_id=3,
        timestamp=later,
        average_price=999.0,
    )
    row = await _active_order(repo, order_public_id)
    assert row.filled_size == 0.4
    assert row.average_price == 100.5


@pytest.mark.asyncio
async def test_two_partial_chain_via_returned_successor_ids(tmp_path: Path) -> None:
    """Two chained partials version cleanly via returned successor ids.

    Given: an open order receiving two partial fill updates, each
        keyed by the PREVIOUS call's returned successor id (the
        executor's ``pending.db_order_id`` swap),
    When: both updates run,
    Then: exactly three versions exist, the active one carries the
        second partial's truth, and no active-unique collision occurs
        — the historical stale-id landmine regression.
    """
    repo, order_id, order_public_id = await _seeded_repo(tmp_path, "fill_chain.db")
    now = datetime.now(UTC)
    first_id = await repo.update_order(
        order_id=order_id,
        status="open",
        updated_at=now,
        session_id="s1",
        sequence_id=2,
        timestamp=now,
        filled_size=0.3,
        average_price=100.0,
    )
    later = now + timedelta(seconds=1)
    second_id = await repo.update_order(
        order_id=first_id,
        status="open",
        updated_at=later,
        session_id="s1",
        sequence_id=3,
        timestamp=later,
        filled_size=0.7,
        average_price=101.0,
    )
    assert len({order_id, first_id, second_id}) == 3
    row = await _active_order(repo, order_public_id)
    assert row.filled_size == 0.7
    assert row.average_price == 101.0
