"""Bitemporal integration tests for SCD Type 2 persistence.

Validates that the close-old + insert-new pattern produces contiguous
non-overlapping half-open intervals [timestamp, known_to) for candles,
orders, and settings. Uses real SQLite in-memory databases via
SQLAlchemyRepository.
"""

from datetime import UTC
from datetime import datetime
from datetime import timedelta
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy import select
from sqlalchemy import update
from sqlalchemy.exc import IntegrityError

from snapper.data.models import KNOWN_TO_MAX
from snapper.data.models import Candle
from snapper.data.models import Order
from snapper.data.models import Setting
from snapper.data.models import Symbol
from snapper.data.models import SymbolVersion
from snapper.data.repository import SQLAlchemyRepository


def assert_contiguous_intervals(versions: list[Any]) -> None:
    """Assert temporal versions form contiguous non-overlapping half-open intervals.

    Sorts by timestamp and verifies prev.known_to == next.timestamp for each
    consecutive pair. Also verifies the last version has known_to == KNOWN_TO_MAX.

    Args:
        versions: List of ORM model instances with timestamp and known_to fields.
    """
    sorted_versions = sorted(versions, key=lambda v: v.timestamp)
    for i in range(len(sorted_versions) - 1):
        assert sorted_versions[i].known_to == sorted_versions[i + 1].timestamp
    if sorted_versions:
        assert sorted_versions[-1].known_to == KNOWN_TO_MAX


async def _create_repo_with_instrument(tmp_path: Path) -> tuple[SQLAlchemyRepository, int]:
    """Create a repository with a single instrument ready for use.

    Args:
        tmp_path: Pytest temporary directory.

    Returns:
        Tuple of (repository, instrument_id).
    """
    db_path = tmp_path / "bitemporal.db"
    repo = SQLAlchemyRepository(f"sqlite+aiosqlite:///{db_path}")
    await repo.create_all()
    async with repo.session() as s:
        s.add(Symbol(native_symbol="BTC-USD", created_at=datetime.now(UTC)))
        s.add(
            SymbolVersion(
                native_symbol="BTC-USD",
                base="BTC",
                quote="USD",
                asset_type="crypto",
                timestamp=datetime.now(UTC),
            )
        )
        await s.commit()
    inst_id = await repo.upsert_instrument(
        symbol="BTC-USD",
        base="BTC",
        quote="USD",
        exchange="kraken",
        tick_size=0.01,
        lot_size=0.001,
    )
    return repo, inst_id


def _candle_row(
    instrument_id: int,
    open_at: datetime,
    timestamp: datetime,
    close: float = 1.5,
) -> dict[str, Any]:
    """Build a candle row dict with sensible defaults.

    Args:
        instrument_id: FK to instruments table.
        open_at: Candle interval start time.
        timestamp: Bus/domain timestamp.
        close: Close price, defaults to 1.5.

    Returns:
        Dict suitable for upsert_candles.
    """
    return {
        "instrument_id": instrument_id,
        "timeframe": "1m",
        "open_at": open_at,
        "timestamp": timestamp,
        "open": 1.0,
        "high": 2.0,
        "low": 0.5,
        "close": close,
        "volume": 10.0,
        "vwap": None,
        "trades": 1,
    }


class TestCandleBitemporal:
    """Integration tests for candle SCD Type 2 bitemporal upserts."""

    @pytest.mark.asyncio
    async def test_upsert_candles_monotonic_updates_create_contiguous_intervals(
        self, tmp_path: Path
    ) -> None:
        """Upsert same candle twice creates contiguous half-open intervals.

        Given: A candle inserted at t1,
        When: The same (instrument_id, timeframe, open_at) is upserted at t2 > t1,
        Then: Two rows exist with same public_id, intervals [t1, t2) and [t2, MAX).
        """
        repo, inst_id = await _create_repo_with_instrument(tmp_path)
        open_at = datetime(2024, 6, 1, 12, 0, 0, tzinfo=UTC)
        t1 = datetime(2024, 6, 1, 12, 0, 10, tzinfo=UTC)
        t2 = datetime(2024, 6, 1, 12, 0, 20, tzinfo=UTC)

        await repo.upsert_candles([_candle_row(inst_id, open_at, t1, close=100.0)])
        await repo.upsert_candles([_candle_row(inst_id, open_at, t2, close=105.0)])

        async with repo.session() as s:
            rows = (
                (
                    await s.execute(
                        select(Candle).where(
                            Candle.instrument_id == inst_id,
                            Candle.timeframe == "1m",
                            Candle.open_at == open_at,
                        )
                    )
                )
                .scalars()
                .all()
            )

        assert len(rows) == 2
        public_ids = {r.public_id for r in rows}
        assert len(public_ids) == 1
        assert_contiguous_intervals(rows)

    @pytest.mark.asyncio
    async def test_upsert_candles_preserves_bus_timestamp_from_caller(self, tmp_path: Path) -> None:
        """Caller-supplied timestamp is stored verbatim, not overridden.

        Given: A candle row with an explicit timestamp field,
        When: upsert_candles is called,
        Then: The DB record stores exactly that timestamp.
        """
        repo, inst_id = await _create_repo_with_instrument(tmp_path)
        explicit_ts = datetime(2024, 3, 15, 8, 30, 0, tzinfo=UTC)
        open_at = datetime(2024, 3, 15, 8, 0, 0, tzinfo=UTC)

        await repo.upsert_candles([_candle_row(inst_id, open_at, explicit_ts)])

        async with repo.session() as s:
            row = (
                (
                    await s.execute(
                        select(Candle).where(
                            Candle.instrument_id == inst_id,
                            Candle.open_at == open_at,
                        )
                    )
                )
                .scalars()
                .first()
            )

        assert row is not None
        assert row.timestamp == explicit_ts

    @pytest.mark.asyncio
    async def test_upsert_candles_as_of_boundary_is_half_open(self, tmp_path: Path) -> None:
        """Half-open interval boundary: t2 is inclusive for v2, exclusive for v1.

        Given: Two versions v1=[t1, t2) and v2=[t2, MAX),
        When: Querying at boundary t2,
        Then: v2 is returned (timestamp <= t2 AND known_to > t2).
        """
        repo, inst_id = await _create_repo_with_instrument(tmp_path)
        open_at = datetime(2024, 6, 1, 12, 0, 0, tzinfo=UTC)
        t1 = datetime(2024, 6, 1, 12, 0, 10, tzinfo=UTC)
        t2 = datetime(2024, 6, 1, 12, 0, 20, tzinfo=UTC)

        await repo.upsert_candles([_candle_row(inst_id, open_at, t1, close=100.0)])
        await repo.upsert_candles([_candle_row(inst_id, open_at, t2, close=105.0)])

        async with repo.session() as s:
            at_t2 = (
                (
                    await s.execute(
                        select(Candle).where(
                            Candle.instrument_id == inst_id,
                            Candle.timeframe == "1m",
                            Candle.open_at == open_at,
                            Candle.timestamp <= t2,
                            Candle.known_to > t2,
                        )
                    )
                )
                .scalars()
                .all()
            )
            assert len(at_t2) == 1
            assert at_t2[0].close == pytest.approx(105.0)

            at_t1 = (
                (
                    await s.execute(
                        select(Candle).where(
                            Candle.instrument_id == inst_id,
                            Candle.timeframe == "1m",
                            Candle.open_at == open_at,
                            Candle.timestamp <= t1,
                            Candle.known_to > t1,
                        )
                    )
                )
                .scalars()
                .all()
            )
            assert len(at_t1) == 1
            assert at_t1[0].close == pytest.approx(100.0)

    @pytest.mark.asyncio
    async def test_get_candles_returns_single_version_for_multiple_history(
        self, tmp_path: Path
    ) -> None:
        """get_candles returns only the active version, not historical ones.

        Given: Three versions of the same candle via successive upserts,
        When: get_candles is called,
        Then: Exactly one result is returned (the active version).
        """
        repo, inst_id = await _create_repo_with_instrument(tmp_path)
        open_at = datetime(2024, 6, 1, 12, 0, 0, tzinfo=UTC)
        t1 = datetime(2024, 6, 1, 12, 0, 10, tzinfo=UTC)
        t2 = datetime(2024, 6, 1, 12, 0, 20, tzinfo=UTC)
        t3 = datetime(2024, 6, 1, 12, 0, 30, tzinfo=UTC)

        await repo.upsert_candles([_candle_row(inst_id, open_at, t1, close=100.0)])
        await repo.upsert_candles([_candle_row(inst_id, open_at, t2, close=105.0)])
        await repo.upsert_candles([_candle_row(inst_id, open_at, t3, close=110.0)])

        async with repo.session() as s:
            all_rows = (
                (
                    await s.execute(
                        select(Candle).where(
                            Candle.instrument_id == inst_id,
                            Candle.open_at == open_at,
                        )
                    )
                )
                .scalars()
                .all()
            )
            assert len(all_rows) == 3

        candles = await repo.get_candles(
            instrument="BTC-USD",
            timeframe="1m",
            start=open_at - timedelta(minutes=1),
            end=open_at + timedelta(minutes=1),
            exchange="kraken",
        )
        assert len(candles) == 1
        assert candles[0]["close"] == pytest.approx(110.0)

    @pytest.mark.asyncio
    async def test_get_latest_candle_ids_uses_active_temporal_row(self, tmp_path: Path) -> None:
        """get_latest_candle_ids returns the public_id from the active row.

        Given: Two versions of the same candle (same public_id),
        When: get_latest_candle_ids is called,
        Then: The active version's public_id is returned.
        """
        repo, inst_id = await _create_repo_with_instrument(tmp_path)
        open_at = datetime(2024, 6, 1, 12, 0, 0, tzinfo=UTC)
        t1 = datetime(2024, 6, 1, 12, 0, 10, tzinfo=UTC)
        t2 = datetime(2024, 6, 1, 12, 0, 20, tzinfo=UTC)

        await repo.upsert_candles([_candle_row(inst_id, open_at, t1, close=100.0)])
        await repo.upsert_candles([_candle_row(inst_id, open_at, t2, close=105.0)])

        async with repo.session() as s:
            active_row = (
                (
                    await s.execute(
                        select(Candle).where(
                            Candle.instrument_id == inst_id,
                            Candle.open_at == open_at,
                            Candle.known_to == KNOWN_TO_MAX,
                        )
                    )
                )
                .scalars()
                .first()
            )
            assert active_row is not None
            expected_public_id = active_row.public_id

        latest = await repo.get_latest_candle_ids()
        key = (inst_id, "1m")
        assert key in latest
        returned_open_at, returned_public_id = latest[key]
        assert returned_public_id == expected_public_id
        assert returned_open_at == open_at

    @pytest.mark.asyncio
    async def test_partial_unique_allows_closed_duplicates(self, tmp_path: Path) -> None:
        """Partial unique index allows multiple closed rows with same business key.

        Given: A candle inserted, then upserted twice (creating 3 rows total),
        When: All three rows share the same (instrument_id, timeframe, open_at),
        Then: Only one has known_to == KNOWN_TO_MAX, and all coexist without error.
        """
        repo, inst_id = await _create_repo_with_instrument(tmp_path)
        open_at = datetime(2024, 6, 1, 12, 0, 0, tzinfo=UTC)
        t1 = datetime(2024, 6, 1, 12, 0, 10, tzinfo=UTC)
        t2 = datetime(2024, 6, 1, 12, 0, 20, tzinfo=UTC)
        t3 = datetime(2024, 6, 1, 12, 0, 30, tzinfo=UTC)

        await repo.upsert_candles([_candle_row(inst_id, open_at, t1)])
        await repo.upsert_candles([_candle_row(inst_id, open_at, t2)])
        await repo.upsert_candles([_candle_row(inst_id, open_at, t3)])

        async with repo.session() as s:
            all_rows = (
                (
                    await s.execute(
                        select(Candle).where(
                            Candle.instrument_id == inst_id,
                            Candle.timeframe == "1m",
                            Candle.open_at == open_at,
                        )
                    )
                )
                .scalars()
                .all()
            )

        assert len(all_rows) == 3
        active_rows = [r for r in all_rows if r.known_to == KNOWN_TO_MAX]
        assert len(active_rows) == 1


class TestOrderBitemporal:
    """Integration tests for order SCD Type 2 bitemporal updates."""

    @pytest.mark.asyncio
    async def test_update_order_creates_contiguous_intervals(self, tmp_path: Path) -> None:
        """Order status updates create contiguous temporal intervals.

        Given: An order inserted and updated twice,
        When: All three versions are loaded from the database,
        Then: They share the same public_id and form contiguous intervals.
        """
        repo, inst_id = await _create_repo_with_instrument(tmp_path)
        base_ts = datetime(2024, 6, 1, 12, 0, 0, tzinfo=UTC)

        order_id, order_public_id = await repo.insert_order(
            instrument_id=inst_id,
            client_order_id="cli-001",
            exchange_order_id=None,
            created_at=base_ts,
            side="buy",
            order_type="limit",
            price=50000.0,
            size=1.0,
            status="new",
        )

        order_v2 = await repo.update_order(
            order_id=order_id,
            status="partially_filled",
            updated_at=base_ts + timedelta(seconds=10),
            filled_size=0.5,
            average_price=50050.0,
        )

        order_v3 = await repo.update_order(
            order_id=order_v2,
            status="filled",
            updated_at=base_ts + timedelta(seconds=20),
            exchange_order_id="ex-001",
            filled_size=1.0,
            average_price=50025.0,
        )

        assert order_v3 != order_id

        async with repo.session() as s:
            all_versions = (
                (await s.execute(select(Order).where(Order.public_id == order_public_id)))
                .scalars()
                .all()
            )

        assert len(all_versions) == 3
        assert_contiguous_intervals(all_versions)


class TestSettingBitemporal:
    """Integration tests for setting SCD Type 2 bitemporal updates."""

    @pytest.mark.asyncio
    async def test_update_setting_creates_contiguous_intervals(self, tmp_path: Path) -> None:
        """Setting value updates create contiguous temporal intervals.

        Given: A setting created and updated twice via direct ORM operations,
        When: All three versions are loaded from the database,
        Then: They share the same public_id and form contiguous intervals.
        """
        repo, _ = await _create_repo_with_instrument(tmp_path)

        async with repo.session() as s:
            now = datetime(2024, 6, 1, 12, 0, 0, tzinfo=UTC)
            v1 = Setting(
                key="test.setting",
                value="value_1",
                category="system",
                description="Test setting",
                is_encrypted=False,
                timestamp=now,
            )
            s.add(v1)
            await s.commit()
            await s.refresh(v1)
            v1_public_id = v1.public_id

        t2 = datetime(2024, 6, 1, 12, 1, 0, tzinfo=UTC)
        async with repo.session() as s:
            await s.execute(update(Setting).where(Setting.id == v1.id).values(known_to=t2))
            v2 = Setting(
                public_id=v1_public_id,
                key="test.setting",
                value="value_2",
                category="system",
                description="Test setting",
                is_encrypted=False,
                timestamp=t2,
            )
            s.add(v2)
            await s.commit()
            await s.refresh(v2)

        t3 = datetime(2024, 6, 1, 12, 2, 0, tzinfo=UTC)
        async with repo.session() as s:
            await s.execute(update(Setting).where(Setting.id == v2.id).values(known_to=t3))
            v3 = Setting(
                public_id=v1_public_id,
                key="test.setting",
                value="value_3",
                category="system",
                description="Test setting",
                is_encrypted=False,
                timestamp=t3,
            )
            s.add(v3)
            await s.commit()

        async with repo.session() as s:
            all_versions = (
                (await s.execute(select(Setting).where(Setting.public_id == v1_public_id)))
                .scalars()
                .all()
            )

        assert len(all_versions) == 3
        assert_contiguous_intervals(all_versions)


class TestExecutionDedup:
    """Integration tests for execution deduplication via order_public_id."""

    @pytest.mark.asyncio
    async def test_execution_dedup_uses_order_public_id(self, tmp_path: Path) -> None:
        """Duplicate exec_id + order_public_id is blocked by partial unique index.

        Given: An order inserted and updated (two versions, same public_id),
        When: Two executions with the same exec_id and order_public_id are inserted,
        Then: The second insert raises IntegrityError.
        """
        repo, inst_id = await _create_repo_with_instrument(tmp_path)
        base_ts = datetime(2024, 6, 1, 12, 0, 0, tzinfo=UTC)

        order_id, order_public_id = await repo.insert_order(
            instrument_id=inst_id,
            client_order_id="cli-dedup",
            exchange_order_id=None,
            created_at=base_ts,
            side="buy",
            order_type="limit",
            price=50000.0,
            size=1.0,
            status="new",
        )

        new_order_id = await repo.update_order(
            order_id=order_id,
            status="filled",
            updated_at=base_ts + timedelta(seconds=5),
            exchange_order_id="ex-dedup",
        )

        await repo.insert_execution(
            order_id=new_order_id,
            order_public_id=order_public_id,
            timestamp=base_ts + timedelta(seconds=10),
            side="buy",
            status="filled",
            price=50000.0,
            size=1.0,
            fee=5.0,
            fee_asset="USD",
            exec_id="E1",
        )

        with pytest.raises(IntegrityError):
            await repo.insert_execution(
                order_id=new_order_id,
                order_public_id=order_public_id,
                timestamp=base_ts + timedelta(seconds=15),
                side="buy",
                status="filled",
                price=50000.0,
                size=1.0,
                fee=5.0,
                fee_asset="USD",
                exec_id="E1",
            )
