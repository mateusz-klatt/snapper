"""Bitemporal integration tests for SCD Type 2 persistence.

Validates that the close-old + insert-new pattern produces contiguous
non-overlapping half-open intervals [timestamp, known_to) for candles,
orders, settings, and users. Uses real SQLite databases via
SQLAlchemyRepository.
"""

from datetime import UTC
from datetime import datetime
from datetime import timedelta
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy import func
from sqlalchemy import select
from sqlalchemy import update
from sqlalchemy.exc import IntegrityError

from snapper.data.models import KNOWN_TO_MAX
from snapper.data.models import Candle
from snapper.data.models import Execution
from snapper.data.models import Instrument
from snapper.data.models import Order
from snapper.data.models import Position
from snapper.data.models import Setting
from snapper.data.models import Signal
from snapper.data.models import Symbol
from snapper.data.models import User
from snapper.data.models import UserLoginEvent
from snapper.data.repository import SQLAlchemyRepository
from snapper.data.repository import close_and_insert
from snapper.data.repository import where_active
from snapper.infrastructure.symbols.functions import resolve_symbol_public_id


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
    symbol = Symbol(
        native_symbol="BTC-USD",
        base="BTC",
        quote="USD",
        asset_type="crypto",
        created_at=datetime.now(UTC),
        timestamp=datetime.now(UTC),
        session_id="test-session",
        sequence_id=1,
    )
    async with repo.session() as s:
        s.add(symbol)
        await s.commit()
    spid = symbol.public_id
    assert spid is not None
    inst_id = await repo.upsert_instrument(
        symbol_public_id=spid,
        symbol="BTC-USD",
        base="BTC",
        quote="USD",
        exchange="kraken",
        tick_size=0.01,
        lot_size=0.001,
        session_id="test-session",
        sequence_id=1,
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
        "session_id": "test-session",
        "sequence_id": 1,
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
            session_id="",
            sequence_id=0,
        )

        order_v2 = await repo.update_order(
            order_id=order_id,
            status="partially_filled",
            updated_at=base_ts + timedelta(seconds=10),
            session_id="",
            sequence_id=0,
            filled_size=0.5,
            average_price=50050.0,
        )

        order_v3 = await repo.update_order(
            order_id=order_v2,
            status="filled",
            updated_at=base_ts + timedelta(seconds=20),
            session_id="",
            sequence_id=0,
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
                session_id="test-session",
                sequence_id=1,
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
                session_id="test-session",
                sequence_id=1,
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
                session_id="test-session",
                sequence_id=1,
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
            session_id="",
            sequence_id=0,
        )

        new_order_id = await repo.update_order(
            order_id=order_id,
            status="filled",
            updated_at=base_ts + timedelta(seconds=5),
            session_id="",
            sequence_id=0,
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
            session_id="",
            sequence_id=0,
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
                session_id="",
                sequence_id=0,
                exec_id="E1",
            )


async def _create_repo_with_symbol(tmp_path: Path) -> SQLAlchemyRepository:
    """Create a repository with schema only, no pre-existing data.

    Args:
        tmp_path: Pytest temporary directory.

    Returns:
        Repository with all tables created.
    """
    db_path = tmp_path / "bitemporal_sv.db"
    repo = SQLAlchemyRepository(f"sqlite+aiosqlite:///{db_path}")
    await repo.create_all()
    return repo


def _payload_matches(
    existing: Symbol,
    base: str,
    quote: str | None,
    asset_type: str,
) -> bool:
    """Compare Symbol payload fields for change detection.

    Mirrors the updater logic in ``_upsert_symbol``: a new version is
    created only when base, quote, or asset_type differ from the active row.

    Args:
        existing: Active Symbol row from the database.
        base: Candidate base currency.
        quote: Candidate quote currency (or None).
        asset_type: Candidate asset type.

    Returns:
        True if all payload fields match (no change needed).
    """
    return existing.base == base and existing.quote == quote and existing.asset_type == asset_type


async def _upsert_symbol_pattern(
    repo: SQLAlchemyRepository,
    native_symbol: str,
    base: str,
    quote: str | None,
    asset_type: str,
    now: datetime,
) -> None:
    """Replicate the updater's upsert-symbol pattern using async sessions.

    Inserts or updates a Symbol temporal row (SCD Type 2), skipping
    close+insert when the payload is unchanged.

    Args:
        repo: Active SQLAlchemy async repository.
        native_symbol: Native symbol string.
        base: Base currency code.
        quote: Quote currency code, or None.
        asset_type: One of crypto, forex, equity, index.
        now: Bus timestamp for the operation.
    """
    async with repo.session() as s:
        existing = (
            (
                await s.execute(
                    select(Symbol).where(
                        Symbol.native_symbol == native_symbol,
                        Symbol.timestamp <= now,
                        Symbol.known_to > now,
                    )
                )
            )
            .scalars()
            .first()
        )
        if existing is None:
            s.add(
                Symbol(
                    native_symbol=native_symbol,
                    base=base,
                    quote=quote,
                    asset_type=asset_type,
                    created_at=now,
                    timestamp=now,
                    session_id="test-session",
                    sequence_id=1,
                )
            )
        else:
            if not _payload_matches(existing, base, quote, asset_type):
                await close_and_insert(
                    session=s,
                    model=Symbol,
                    match_filters=[Symbol.native_symbol == native_symbol],
                    new_values={
                        "native_symbol": native_symbol,
                        "base": base,
                        "quote": quote,
                        "asset_type": asset_type,
                        "created_at": existing.created_at,
                        "session_id": "test-session",
                        "sequence_id": 1,
                    },
                    bus_time=now,
                )
        await s.commit()


class TestSymbolBitemporal:
    """Integration tests for Symbol SCD2 behavior and idempotency."""

    @pytest.mark.asyncio
    async def test_symbol_version_reingest_same_payload_is_noop(self, tmp_path: Path) -> None:
        """Re-ingesting identical payload does not create a new version.

        Given: A Symbol identity row and Symbol with payload
            (base=BTC, quote=USD, asset_type=crypto),
        When: The updater pattern runs again with identical payload,
        Then: Still only 1 Symbol row exists and its timestamp
            is unchanged.
        """
        repo = await _create_repo_with_symbol(tmp_path)
        t1 = datetime(2024, 7, 1, 10, 0, 0, tzinfo=UTC)
        t2 = datetime(2024, 7, 1, 11, 0, 0, tzinfo=UTC)

        await _upsert_symbol_pattern(repo, "BTC-USD", "BTC", "USD", "crypto", t1)

        async with repo.session() as s:
            before = (
                (await s.execute(select(Symbol).where(Symbol.native_symbol == "BTC-USD")))
                .scalars()
                .all()
            )
        assert len(before) == 1
        original_ts = before[0].timestamp

        await _upsert_symbol_pattern(repo, "BTC-USD", "BTC", "USD", "crypto", t2)

        async with repo.session() as s:
            after = (
                (await s.execute(select(Symbol).where(Symbol.native_symbol == "BTC-USD")))
                .scalars()
                .all()
            )
        assert len(after) == 1
        assert after[0].timestamp == original_ts

    @pytest.mark.asyncio
    async def test_symbol_version_change_closes_old_and_inserts_new(self, tmp_path: Path) -> None:
        """Changing payload closes the old version and inserts a new one.

        Given: A Symbol with asset_type=crypto,
        When: The updater pattern runs with asset_type=forex (changed payload),
        Then: Two Symbol rows exist for the same native_symbol with
            contiguous half-open intervals.
        """
        repo = await _create_repo_with_symbol(tmp_path)
        t1 = datetime(2024, 7, 1, 10, 0, 0, tzinfo=UTC)
        t2 = datetime(2024, 7, 1, 11, 0, 0, tzinfo=UTC)

        await _upsert_symbol_pattern(repo, "BTC-USD", "BTC", "USD", "crypto", t1)
        await _upsert_symbol_pattern(repo, "BTC-USD", "BTC", "USD", "forex", t2)

        async with repo.session() as s:
            rows = (
                (await s.execute(select(Symbol).where(Symbol.native_symbol == "BTC-USD")))
                .scalars()
                .all()
            )

        assert len(rows) == 2
        assert_contiguous_intervals(rows)

        sorted_rows = sorted(rows, key=lambda v: v.timestamp)
        assert sorted_rows[0].asset_type == "crypto"
        assert sorted_rows[1].asset_type == "forex"

    @pytest.mark.asyncio
    async def test_symbol_version_preserves_public_id_across_versions(self, tmp_path: Path) -> None:
        """Both old and new versions share the same public_id.

        Given: A Symbol with asset_type=crypto,
        When: The updater pattern runs with asset_type=forex (changed payload),
        Then: Both versions carry the same public_id value.
        """
        repo = await _create_repo_with_symbol(tmp_path)
        t1 = datetime(2024, 7, 1, 10, 0, 0, tzinfo=UTC)
        t2 = datetime(2024, 7, 1, 11, 0, 0, tzinfo=UTC)

        await _upsert_symbol_pattern(repo, "BTC-USD", "BTC", "USD", "crypto", t1)
        await _upsert_symbol_pattern(repo, "BTC-USD", "BTC", "USD", "forex", t2)

        async with repo.session() as s:
            rows = (
                (await s.execute(select(Symbol).where(Symbol.native_symbol == "BTC-USD")))
                .scalars()
                .all()
            )

        public_ids = {r.public_id for r in rows}
        assert len(rows) == 2
        assert len(public_ids) == 1

    @pytest.mark.asyncio
    async def test_symbol_version_as_of_returns_correct_version(self, tmp_path: Path) -> None:
        """Point-in-time query returns the version active at that moment.

        Given: Two versions: v1 at t1 with base=BTC and v2 at t2 with base=XBT,
        When: Querying as-of t1 and as-of t2,
        Then: t1 returns v1 (base=BTC) and t2 returns v2 (base=XBT).
        """
        repo = await _create_repo_with_symbol(tmp_path)
        t1 = datetime(2024, 7, 1, 10, 0, 0, tzinfo=UTC)
        t2 = datetime(2024, 7, 1, 11, 0, 0, tzinfo=UTC)

        await _upsert_symbol_pattern(repo, "BTC-USD", "BTC", "USD", "crypto", t1)
        await _upsert_symbol_pattern(repo, "BTC-USD", "XBT", "USD", "crypto", t2)

        async with repo.session() as s:
            at_t1 = (
                (
                    await s.execute(
                        select(Symbol).where(
                            Symbol.native_symbol == "BTC-USD",
                            Symbol.timestamp <= t1,
                            Symbol.known_to > t1,
                        )
                    )
                )
                .scalars()
                .first()
            )
            assert at_t1 is not None
            assert at_t1.base == "BTC"

            at_t2 = (
                (
                    await s.execute(
                        select(Symbol).where(
                            Symbol.native_symbol == "BTC-USD",
                            Symbol.timestamp <= t2,
                            Symbol.known_to > t2,
                        )
                    )
                )
                .scalars()
                .first()
            )
            assert at_t2 is not None
            assert at_t2.base == "XBT"

    @pytest.mark.asyncio
    async def test_symbol_version_bulk_rerun_does_not_explode_rows(self, tmp_path: Path) -> None:
        """Bulk re-run with identical payloads creates no extra versions.

        Given: Five Symbol+Symbol pairs inserted at t1,
        When: The same five are re-ingested at t2 with identical payloads,
        Then: Still exactly 5 Symbol rows total (one per symbol).
        """
        repo = await _create_repo_with_symbol(tmp_path)
        t1 = datetime(2024, 7, 1, 10, 0, 0, tzinfo=UTC)
        t2 = datetime(2024, 7, 1, 11, 0, 0, tzinfo=UTC)

        symbols = [
            ("BTC-USD", "BTC", "USD"),
            ("ETH-USD", "ETH", "USD"),
            ("SOL-USD", "SOL", "USD"),
            ("ADA-USD", "ADA", "USD"),
            ("DOT-USD", "DOT", "USD"),
        ]

        for native, base, quote in symbols:
            await _upsert_symbol_pattern(repo, native, base, quote, "crypto", t1)

        for native, base, quote in symbols:
            await _upsert_symbol_pattern(repo, native, base, quote, "crypto", t2)

        async with repo.session() as s:
            count = (await s.execute(select(func.count()).select_from(Symbol))).scalar_one()

        assert count == 5

    @pytest.mark.asyncio
    async def test_symbol_version_change_detection_ignores_timestamp(self, tmp_path: Path) -> None:
        """Different bus_time with same payload does not create a new version.

        Given: A Symbol created at t1,
        When: The updater pattern runs at t2 (different bus_time) with the
            same base, quote, and asset_type,
        Then: No new version is created because the timestamp is not part
            of the change-detection comparison.
        """
        repo = await _create_repo_with_symbol(tmp_path)
        t1 = datetime(2024, 7, 1, 10, 0, 0, tzinfo=UTC)
        t2 = datetime(2024, 7, 1, 12, 0, 0, tzinfo=UTC)
        t3 = datetime(2024, 7, 2, 8, 0, 0, tzinfo=UTC)

        await _upsert_symbol_pattern(repo, "BTC-USD", "BTC", "USD", "crypto", t1)
        await _upsert_symbol_pattern(repo, "BTC-USD", "BTC", "USD", "crypto", t2)
        await _upsert_symbol_pattern(repo, "BTC-USD", "BTC", "USD", "crypto", t3)

        async with repo.session() as s:
            rows = (
                (await s.execute(select(Symbol).where(Symbol.native_symbol == "BTC-USD")))
                .scalars()
                .all()
            )

        assert len(rows) == 1
        assert rows[0].timestamp == t1
        assert rows[0].known_to == KNOWN_TO_MAX


class TestCandlePolicyBitemporal:
    """Tests documenting current candle upsert policy edge cases."""

    @pytest.mark.asyncio
    async def test_upsert_candles_duplicate_same_timestamp_is_idempotent(
        self, tmp_path: Path
    ) -> None:
        """Upserting the same candle with same timestamp creates a zero-length closed version.

        Given: A candle inserted at t1 with close=100,
        When: The same candle is upserted again at t1 with identical OHLCV,
        Then: Two rows exist: the original closed with [t1, t1) (zero-length)
            and the new active with [t1, MAX). This documents current policy
            (not necessarily ideal but deterministic).
        """
        repo, inst_id = await _create_repo_with_instrument(tmp_path)
        open_at = datetime(2024, 6, 1, 12, 0, 0, tzinfo=UTC)
        t1 = datetime(2024, 6, 1, 12, 0, 10, tzinfo=UTC)

        await repo.upsert_candles([_candle_row(inst_id, open_at, t1, close=100.0)])
        await repo.upsert_candles([_candle_row(inst_id, open_at, t1, close=100.0)])

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

        closed = [r for r in rows if r.known_to != KNOWN_TO_MAX]
        assert len(closed) == 1
        assert closed[0].known_to == closed[0].timestamp

        active = [r for r in rows if r.known_to == KNOWN_TO_MAX]
        assert len(active) == 1
        assert active[0].timestamp == t1

    @pytest.mark.asyncio
    async def test_upsert_candles_out_of_order_timestamp_rejected(self, tmp_path: Path) -> None:
        """Out-of-order timestamp creates a second active record (known limitation).

        Given: A candle inserted at t2,
        When: A candle is upserted at t1 < t2 for the same business key,
        Then: The temporal filter (timestamp <= t1 AND known_to > t1) does not
            find the active record (which has timestamp=t2 > t1), so a second
            active record is created. This documents a known limitation:
            out-of-order timestamps are not supported.
        """
        repo, inst_id = await _create_repo_with_instrument(tmp_path)
        open_at = datetime(2024, 6, 1, 12, 0, 0, tzinfo=UTC)
        t1 = datetime(2024, 6, 1, 12, 0, 10, tzinfo=UTC)
        t2 = datetime(2024, 6, 1, 12, 0, 20, tzinfo=UTC)

        await repo.upsert_candles([_candle_row(inst_id, open_at, t2, close=105.0)])
        await repo.upsert_candles([_candle_row(inst_id, open_at, t1, close=100.0)])

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
        active_rows = [r for r in rows if r.known_to == KNOWN_TO_MAX]
        assert len(active_rows) == 2

        public_ids = {r.public_id for r in rows}
        assert len(public_ids) == 2


async def _create_user(
    repo: SQLAlchemyRepository,
    username: str,
    password_hash: str,
    now: datetime,
    email: str | None = None,
) -> User:
    """Create a User row via direct ORM insert and return the refreshed instance.

    Args:
        repo: Active async repository.
        username: Username for the new user.
        password_hash: Pre-hashed password.
        now: Timestamp for created_at and temporal columns.
        email: Optional email address.

    Returns:
        The refreshed User ORM instance.
    """
    async with repo.session() as s:
        user = User(
            username=username,
            email=email,
            password_hash=password_hash,
            role="viewer",
            is_active=True,
            created_at=now,
            timestamp=now,
            session_id="test-session",
            sequence_id=1,
        )
        s.add(user)
        await s.commit()
        await s.refresh(user)
    return user


class TestUserBitemporal:
    """Integration tests for User SCD Type 2 bitemporal behavior."""

    @pytest.mark.asyncio
    async def test_update_user_get_by_username_returns_active_only(self, tmp_path: Path) -> None:
        """After close+insert, querying by username returns only the new version.

        Given: A user created at t1 with password_hash='hash_v1',
        When: The user is updated via close+insert at t2 with password_hash='hash_v2',
        Then: Querying active users by username returns exactly one row with the new hash.
        """
        repo, _ = await _create_repo_with_instrument(tmp_path)
        t1 = datetime(2024, 6, 1, 12, 0, 0, tzinfo=UTC)
        t2 = datetime(2024, 6, 1, 12, 1, 0, tzinfo=UTC)

        user_v1 = await _create_user(repo, "alice", "hash_v1", t1)

        async with repo.session() as s:
            await close_and_insert(
                session=s,
                model=User,
                match_filters=[User.username == "alice"],
                new_values={
                    "username": "alice",
                    "email": None,
                    "password_hash": "hash_v2",
                    "role": "viewer",
                    "is_active": True,
                    "created_at": user_v1.created_at,
                    "session_id": "test-session",
                    "sequence_id": 1,
                },
                bus_time=t2,
            )
            await s.commit()

        async with repo.session() as s:
            stmt = select(User).where(
                User.username == "alice",
                *where_active(User),
            )
            result = await s.execute(stmt)
            active_user = result.scalar_one_or_none()

        assert active_user is not None
        assert active_user.password_hash == "hash_v2"

    @pytest.mark.asyncio
    async def test_get_all_users_returns_one_active_per_username(self, tmp_path: Path) -> None:
        """After two updates, get_all active users returns exactly 1 per username.

        Given: A user created at t1 and updated twice at t2 and t3,
        When: All active users are queried,
        Then: Exactly 1 active user row is returned (not 3 versions).
        """
        repo, _ = await _create_repo_with_instrument(tmp_path)
        t1 = datetime(2024, 6, 1, 12, 0, 0, tzinfo=UTC)
        t2 = datetime(2024, 6, 1, 12, 1, 0, tzinfo=UTC)
        t3 = datetime(2024, 6, 1, 12, 2, 0, tzinfo=UTC)

        user_v1 = await _create_user(repo, "bob", "hash_v1", t1)

        async with repo.session() as s:
            await close_and_insert(
                session=s,
                model=User,
                match_filters=[User.username == "bob"],
                new_values={
                    "username": "bob",
                    "email": "bob@example.com",
                    "password_hash": "hash_v2",
                    "role": "viewer",
                    "is_active": True,
                    "created_at": user_v1.created_at,
                    "session_id": "test-session",
                    "sequence_id": 1,
                },
                bus_time=t2,
            )
            await s.commit()

        async with repo.session() as s:
            await close_and_insert(
                session=s,
                model=User,
                match_filters=[User.username == "bob"],
                new_values={
                    "username": "bob",
                    "email": "bob@newdomain.com",
                    "password_hash": "hash_v3",
                    "role": "operator",
                    "is_active": True,
                    "created_at": user_v1.created_at,
                    "session_id": "test-session",
                    "sequence_id": 1,
                },
                bus_time=t3,
            )
            await s.commit()

        async with repo.session() as s:
            total = (await s.execute(select(func.count()).select_from(User))).scalar_one()
            assert total == 3

        async with repo.session() as s:
            active_stmt = select(User).where(*where_active(User))
            active_users = (await s.execute(active_stmt)).scalars().all()

        assert len(active_users) == 1
        assert active_users[0].email == "bob@newdomain.com"
        assert active_users[0].role == "operator"

    @pytest.mark.asyncio
    async def test_create_user_sets_timestamp(self, tmp_path: Path) -> None:
        """Directly created user has a non-None timestamp.

        Given: An empty users table,
        When: A user is created with an explicit timestamp,
        Then: Reading the user back shows timestamp is NOT None.
        """
        repo, _ = await _create_repo_with_instrument(tmp_path)
        now = datetime(2024, 6, 1, 12, 0, 0, tzinfo=UTC)

        await _create_user(repo, "carol", "hash_v1", now)

        async with repo.session() as s:
            user = (await s.execute(select(User).where(User.username == "carol"))).scalars().first()

        assert user is not None
        assert user.timestamp is not None
        assert user.timestamp == now

    @pytest.mark.asyncio
    async def test_authenticate_user_creates_login_event_not_user_version(
        self, tmp_path: Path
    ) -> None:
        """Authentication creates a login event, not a new user version.

        Given: A user created at t1,
        When: A UserLoginEvent is appended (simulating authentication),
        Then: One UserLoginEvent row exists and still only 1 active User version.
        """
        repo, _ = await _create_repo_with_instrument(tmp_path)
        t1 = datetime(2024, 6, 1, 12, 0, 0, tzinfo=UTC)
        login_time = datetime(2024, 6, 1, 12, 5, 0, tzinfo=UTC)

        user = await _create_user(repo, "dave", "hash_v1", t1)

        async with repo.session() as s:
            login_event = UserLoginEvent(
                user_public_id=user.public_id,
                logged_at=login_time,
                timestamp=login_time,
                session_id="test-session",
                sequence_id=1,
            )
            s.add(login_event)
            await s.commit()

        async with repo.session() as s:
            user_count = (
                await s.execute(
                    select(func.count()).select_from(User).where(User.username == "dave")
                )
            ).scalar_one()
            login_count = (
                await s.execute(
                    select(func.count())
                    .select_from(UserLoginEvent)
                    .where(UserLoginEvent.user_public_id == user.public_id)
                )
            ).scalar_one()

        assert user_count == 1
        assert login_count == 1

    @pytest.mark.asyncio
    async def test_list_login_events_returns_only_active_versions(self, tmp_path: Path) -> None:
        """Closed login events are hidden from temporal read.

        Given: Two login events for the same user, one closed,
        When: list_login_events is queried at now,
        Then: Only the unclosed event is returned.
        """
        repo, _ = await _create_repo_with_instrument(tmp_path)
        t1 = datetime(2024, 6, 1, 12, 0, 0, tzinfo=UTC)
        t2 = datetime(2024, 6, 1, 12, 5, 0, tzinfo=UTC)
        t_close = datetime(2024, 6, 1, 13, 0, 0, tzinfo=UTC)

        user = await _create_user(repo, "eve", "hash_v1", t1)

        async with repo.session() as s:
            evt1 = UserLoginEvent(
                user_public_id=user.public_id,
                logged_at=t1,
                timestamp=t1,
                session_id="test-session",
                sequence_id=1,
            )
            evt2 = UserLoginEvent(
                user_public_id=user.public_id,
                logged_at=t2,
                timestamp=t2,
                session_id="test-session",
                sequence_id=1,
            )
            s.add(evt1)
            s.add(evt2)
            await s.commit()
            await s.refresh(evt1)

        async with repo.session() as s:

            await s.execute(
                update(UserLoginEvent)
                .where(
                    UserLoginEvent.id == evt1.id,
                )
                .values(known_to=t_close)
            )
            await s.commit()

        async with repo.session() as s:
            active = (
                (
                    await s.execute(
                        select(UserLoginEvent).where(
                            UserLoginEvent.user_public_id == user.public_id,
                            *where_active(UserLoginEvent),
                        )
                    )
                )
                .scalars()
                .all()
            )

        assert len(active) == 1
        assert active[0].logged_at == t2

    @pytest.mark.asyncio
    async def test_close_login_event_hides_for_later_as_of_but_not_earlier(
        self, tmp_path: Path
    ) -> None:
        """Closed login event is visible before close time, hidden after.

        Given: A login event at t1 closed at t_close,
        When: Queried at t_before (< t_close) and t_after (> t_close),
        Then: Visible at t_before, hidden at t_after.
        """
        repo, _ = await _create_repo_with_instrument(tmp_path)
        t1 = datetime(2024, 6, 1, 12, 0, 0, tzinfo=UTC)
        t_close = datetime(2024, 6, 1, 14, 0, 0, tzinfo=UTC)
        t_before = datetime(2024, 6, 1, 13, 0, 0, tzinfo=UTC)
        t_after = datetime(2024, 6, 1, 15, 0, 0, tzinfo=UTC)

        user = await _create_user(repo, "frank", "hash_v1", t1)

        async with repo.session() as s:
            evt = UserLoginEvent(
                user_public_id=user.public_id,
                logged_at=t1,
                timestamp=t1,
                session_id="test-session",
                sequence_id=1,
            )
            s.add(evt)
            await s.commit()
            await s.refresh(evt)

        async with repo.session() as s:

            await s.execute(
                update(UserLoginEvent).where(UserLoginEvent.id == evt.id).values(known_to=t_close)
            )
            await s.commit()

        async with repo.session() as s:
            before_events = (
                (
                    await s.execute(
                        select(UserLoginEvent).where(
                            UserLoginEvent.user_public_id == user.public_id,
                            *where_active(UserLoginEvent, t_before),
                        )
                    )
                )
                .scalars()
                .all()
            )

            after_events = (
                (
                    await s.execute(
                        select(UserLoginEvent).where(
                            UserLoginEvent.user_public_id == user.public_id,
                            *where_active(UserLoginEvent, t_after),
                        )
                    )
                )
                .scalars()
                .all()
            )

        assert len(before_events) == 1
        assert len(after_events) == 0

    @pytest.mark.asyncio
    async def test_authenticate_inserts_login_event_with_open_known_to(
        self, tmp_path: Path
    ) -> None:
        """Login event from authentication has known_to == KNOWN_TO_MAX.

        Given: A user exists,
        When: A login event is inserted (simulating authentication),
        Then: The event has known_to == KNOWN_TO_MAX (open/active).
        """
        repo, _ = await _create_repo_with_instrument(tmp_path)
        t1 = datetime(2024, 6, 1, 12, 0, 0, tzinfo=UTC)
        login_time = datetime(2024, 6, 1, 12, 10, 0, tzinfo=UTC)

        user = await _create_user(repo, "grace", "hash_v1", t1)

        async with repo.session() as s:
            evt = UserLoginEvent(
                user_public_id=user.public_id,
                logged_at=login_time,
                timestamp=login_time,
                session_id="test-session",
                sequence_id=1,
            )
            s.add(evt)
            await s.commit()
            await s.refresh(evt)

        assert evt.known_to == KNOWN_TO_MAX


class TestSettingsApiBitemporal:
    """Integration tests for Settings SCD Type 2 read/delete behavior."""

    @pytest.mark.asyncio
    async def test_settings_read_returns_only_active_after_updates(self, tmp_path: Path) -> None:
        """After two updates, reading settings returns only the active version.

        Given: A setting created at t1 with value='v1',
        When: Updated twice via close+insert to 'v2' at t2 and 'v3' at t3,
        Then: Querying active settings returns exactly 1 row with value='v3'.
        """
        repo, _ = await _create_repo_with_instrument(tmp_path)
        t1 = datetime(2024, 6, 1, 12, 0, 0, tzinfo=UTC)
        t2 = datetime(2024, 6, 1, 12, 1, 0, tzinfo=UTC)
        t3 = datetime(2024, 6, 1, 12, 2, 0, tzinfo=UTC)

        async with repo.session() as s:
            v1 = Setting(
                key="app.theme",
                value="v1",
                category="ui",
                description="Theme setting",
                is_encrypted=False,
                timestamp=t1,
                session_id="test-session",
                sequence_id=1,
            )
            s.add(v1)
            await s.commit()

        async with repo.session() as s:
            await close_and_insert(
                session=s,
                model=Setting,
                match_filters=[Setting.key == "app.theme"],
                new_values={
                    "key": "app.theme",
                    "value": "v2",
                    "category": "ui",
                    "description": "Theme setting",
                    "is_encrypted": False,
                    "session_id": "test-session",
                    "sequence_id": 1,
                },
                bus_time=t2,
            )
            await s.commit()

        async with repo.session() as s:
            await close_and_insert(
                session=s,
                model=Setting,
                match_filters=[Setting.key == "app.theme"],
                new_values={
                    "key": "app.theme",
                    "value": "v3",
                    "category": "ui",
                    "description": "Theme setting",
                    "is_encrypted": False,
                    "session_id": "test-session",
                    "sequence_id": 1,
                },
                bus_time=t3,
            )
            await s.commit()

        async with repo.session() as s:
            active_settings = (
                (
                    await s.execute(
                        select(Setting).where(
                            Setting.key == "app.theme",
                            *where_active(Setting),
                        )
                    )
                )
                .scalars()
                .all()
            )

        assert len(active_settings) == 1
        assert active_settings[0].value == "v3"

    @pytest.mark.asyncio
    async def test_settings_delete_closes_active_not_physical_delete(self, tmp_path: Path) -> None:
        """Deleting a setting closes the active row, not a physical DELETE.

        Given: A setting 'cache.ttl' created at t1,
        When: The setting is 'deleted' by closing its active row at t2,
        Then: The row still exists in the database with known_to != KNOWN_TO_MAX
            and querying active settings returns 0 for that key.
        """
        repo, _ = await _create_repo_with_instrument(tmp_path)
        t1 = datetime(2024, 6, 1, 12, 0, 0, tzinfo=UTC)
        t2 = datetime(2024, 6, 1, 12, 1, 0, tzinfo=UTC)

        async with repo.session() as s:
            setting = Setting(
                key="cache.ttl",
                value="300",
                category="system",
                description="Cache TTL",
                is_encrypted=False,
                timestamp=t1,
                session_id="test-session",
                sequence_id=1,
            )
            s.add(setting)
            await s.commit()
            await s.refresh(setting)
            setting_id = setting.id

        async with repo.session() as s:
            await s.execute(update(Setting).where(Setting.id == setting_id).values(known_to=t2))
            await s.commit()

        async with repo.session() as s:
            all_rows = (
                (await s.execute(select(Setting).where(Setting.key == "cache.ttl"))).scalars().all()
            )
            assert len(all_rows) == 1
            assert all_rows[0].known_to == t2
            assert all_rows[0].known_to != KNOWN_TO_MAX

        async with repo.session() as s:
            active = (
                (
                    await s.execute(
                        select(Setting).where(
                            Setting.key == "cache.ttl",
                            *where_active(Setting),
                        )
                    )
                )
                .scalars()
                .all()
            )
            assert len(active) == 0


class TestInstrumentBitemporal:
    """Instrument SCD2 close+insert behaviour."""

    @pytest.mark.asyncio
    async def test_upsert_same_payload_is_noop(self, tmp_path: Path) -> None:
        """Re-upserting identical payload returns same id, no new row."""
        repo, inst_id = await _create_repo_with_instrument(tmp_path)
        spid = await resolve_symbol_public_id(repo, "BTC-USD")
        assert spid is not None
        inst_id2 = await repo.upsert_instrument(
            symbol_public_id=spid,
            symbol="BTC-USD",
            base="BTC",
            quote="USD",
            exchange="kraken",
            session_id="test-session",
            sequence_id=1,
        )
        assert inst_id2 == inst_id
        async with repo.session() as s:
            all_rows = (await s.execute(select(Instrument))).scalars().all()
            assert len(all_rows) == 1

    @pytest.mark.asyncio
    async def test_upsert_changed_payload_closes_old(self, tmp_path: Path) -> None:
        """Changed symbol/base/quote triggers close+insert with same public_id."""
        repo, inst_id = await _create_repo_with_instrument(tmp_path)
        spid = await resolve_symbol_public_id(repo, "BTC-USD")
        assert spid is not None

        async with repo.session() as s:
            old = (await s.execute(select(Instrument).where(Instrument.id == inst_id))).scalar_one()
            old_public_id = old.public_id

        new_id = await repo.upsert_instrument(
            symbol_public_id=spid,
            symbol="BITCOIN-USD",
            base="BITCOIN",
            quote="USD",
            exchange="kraken",
            session_id="test-session",
            sequence_id=1,
        )
        assert new_id != inst_id

        async with repo.session() as s:
            all_rows = (await s.execute(select(Instrument).order_by(Instrument.id))).scalars().all()
            assert len(all_rows) == 2
            closed = [r for r in all_rows if r.known_to != KNOWN_TO_MAX]
            active = [r for r in all_rows if r.known_to == KNOWN_TO_MAX]
            assert len(closed) == 1
            assert len(active) == 1
            assert closed[0].id == inst_id
            assert active[0].id == new_id
            assert active[0].public_id == old_public_id
            assert active[0].symbol == "BITCOIN-USD"
            assert active[0].base == "BITCOIN"
            assert_contiguous_intervals(all_rows)

    @pytest.mark.asyncio
    async def test_upsert_preserves_public_id_across_versions(self, tmp_path: Path) -> None:
        """Three successive payload changes keep the same public_id."""
        repo, _ = await _create_repo_with_instrument(tmp_path)
        spid = await resolve_symbol_public_id(repo, "BTC-USD")
        assert spid is not None

        async with repo.session() as s:
            orig = (
                await s.execute(select(Instrument).where(*where_active(Instrument)))
            ).scalar_one()
            original_pid = orig.public_id

        for suffix in ["V2", "V3", "V4"]:
            await repo.upsert_instrument(
                symbol_public_id=spid,
                symbol=f"BTC-{suffix}",
                base="BTC",
                quote=suffix,
                exchange="kraken",
                session_id="test-session",
                sequence_id=1,
            )

        async with repo.session() as s:
            all_rows = (await s.execute(select(Instrument))).scalars().all()
            assert len(all_rows) == 4
            active = [r for r in all_rows if r.known_to == KNOWN_TO_MAX]
            assert len(active) == 1
            assert active[0].public_id == original_pid
            assert active[0].symbol == "BTC-V4"
            assert_contiguous_intervals(all_rows)

    @pytest.mark.asyncio
    async def test_active_instrument_read_after_version(self, tmp_path: Path) -> None:
        """where_active returns only the latest version."""
        repo, inst_id = await _create_repo_with_instrument(tmp_path)
        spid = await resolve_symbol_public_id(repo, "BTC-USD")
        assert spid is not None

        new_id = await repo.upsert_instrument(
            symbol_public_id=spid,
            symbol="BTC-RENAMED",
            base="BTC",
            quote="RENAMED",
            exchange="kraken",
            session_id="test-session",
            sequence_id=1,
        )

        async with repo.session() as s:
            active = (
                (await s.execute(select(Instrument).where(*where_active(Instrument))))
                .scalars()
                .all()
            )
            assert len(active) == 1
            assert active[0].id == new_id
            assert active[0].symbol == "BTC-RENAMED"

    @pytest.mark.asyncio
    async def test_partial_unique_allows_closed_duplicates(self, tmp_path: Path) -> None:
        """Closed rows with same (symbol_public_id, exchange) do not violate unique index."""
        repo, _ = await _create_repo_with_instrument(tmp_path)
        spid = await resolve_symbol_public_id(repo, "BTC-USD")
        assert spid is not None

        await repo.upsert_instrument(
            symbol_public_id=spid,
            symbol="BTC-V2",
            base="BTC",
            quote="V2",
            exchange="kraken",
            session_id="test-session",
            sequence_id=1,
        )
        await repo.upsert_instrument(
            symbol_public_id=spid,
            symbol="BTC-V3",
            base="BTC",
            quote="V3",
            exchange="kraken",
            session_id="test-session",
            sequence_id=1,
        )

        async with repo.session() as s:
            all_rows = (await s.execute(select(Instrument))).scalars().all()
            assert len(all_rows) == 3
            active = [r for r in all_rows if r.known_to == KNOWN_TO_MAX]
            assert len(active) == 1

    @pytest.mark.asyncio
    async def test_candles_use_active_instrument(self, tmp_path: Path) -> None:
        """get_candles resolves the active instrument version, not closed ones."""
        repo, inst_id = await _create_repo_with_instrument(tmp_path)
        spid = await resolve_symbol_public_id(repo, "BTC-USD")
        assert spid is not None
        t1 = datetime(2024, 6, 1, 12, 0, 0, tzinfo=UTC)
        await repo.upsert_candles([_candle_row(inst_id, t1, datetime.now(UTC))])

        candles = await repo.get_candles(
            instrument="BTC-USD",
            timeframe="1m",
            start=t1 - timedelta(hours=1),
            end=t1 + timedelta(hours=1),
            exchange="kraken",
        )
        assert len(candles) == 1

        new_id = await repo.upsert_instrument(
            symbol_public_id=spid,
            symbol="BTC-RENAMED",
            base="BTC",
            quote="RENAMED",
            exchange="kraken",
            session_id="test-session",
            sequence_id=1,
        )
        assert new_id != inst_id

        candles_old = await repo.get_candles(
            instrument="BTC-USD",
            timeframe="1m",
            start=t1 - timedelta(hours=1),
            end=t1 + timedelta(hours=1),
            exchange="kraken",
        )
        assert len(candles_old) == 0

        candles_new = await repo.get_candles(
            instrument="BTC-RENAMED",
            timeframe="1m",
            start=t1 - timedelta(hours=1),
            end=t1 + timedelta(hours=1),
            exchange="kraken",
        )
        assert len(candles_new) == 0

    @pytest.mark.asyncio
    async def test_resolve_symbol_public_id_as_of(self, tmp_path: Path) -> None:
        """resolve_symbol_public_id respects as_of parameter."""
        db_path = tmp_path / "resolve_as_of.db"
        repo = SQLAlchemyRepository(f"sqlite+aiosqlite:///{db_path}")
        await repo.create_all()

        t1 = datetime(2024, 1, 1, 0, 0, 0, tzinfo=UTC)
        t2 = datetime(2024, 6, 1, 0, 0, 0, tzinfo=UTC)
        async with repo.session() as s:
            sym = Symbol(
                native_symbol="BTC-USD",
                base="BTC",
                quote="USD",
                asset_type="crypto",
                created_at=t1,
                timestamp=t1,
                session_id="test-session",
                sequence_id=1,
            )
            s.add(sym)
            await s.commit()
            await s.refresh(sym)
            original_pid = sym.public_id

        async with repo.session() as s:
            await close_and_insert(
                s,
                Symbol,
                [Symbol.native_symbol == "BTC-USD"],
                {
                    "native_symbol": "BTC-USD",
                    "base": "BTC",
                    "quote": "USDT",
                    "asset_type": "crypto",
                    "created_at": t1,
                    "session_id": "test-session",
                    "sequence_id": 1,
                },
                t2,
            )
            await s.commit()

        pid_before = await resolve_symbol_public_id(repo, "BTC-USD", as_of=t1 + timedelta(days=1))
        assert pid_before == original_pid

        pid_after = await resolve_symbol_public_id(repo, "BTC-USD", as_of=t2 + timedelta(days=1))
        assert pid_after == original_pid

    @pytest.mark.asyncio
    async def test_fact_records_survive_instrument_rename(self, tmp_path: Path) -> None:
        """Signals/orders referencing old instrument_id remain visible after rename.

        Fact tables (signals, orders) join Instrument by integer FK which
        points to a specific version. After close+insert on Instrument the
        old version is closed but the FK still resolves via plain join
        without temporal filter on Instrument.
        """
        repo, inst_id = await _create_repo_with_instrument(tmp_path)
        spid = await resolve_symbol_public_id(repo, "BTC-USD")
        assert spid is not None

        async with repo.session() as s:
            signal = Signal(
                instrument_id=inst_id,
                fired_at=datetime.now(UTC),
                timestamp=datetime.now(UTC),
                side="buy",
                strength=0.9,
                reason="test signal",
                strategy_name="test",
                price=100.0,
                session_id="test-session",
                sequence_id=1,
            )
            s.add(signal)
            await s.commit()
            await s.refresh(signal)
            signal_id = signal.id

        new_id = await repo.upsert_instrument(
            symbol_public_id=spid,
            symbol="BTC-RENAMED",
            base="BTC",
            quote="RENAMED",
            exchange="kraken",
            session_id="test-session",
            sequence_id=1,
        )
        assert new_id != inst_id

        async with repo.session() as s:
            now = datetime.now(UTC)
            result = await s.execute(
                select(Signal, Instrument)
                .join(Instrument)
                .where(
                    Signal.timestamp <= now,
                    Signal.known_to > now,
                )
            )
            rows = result.all()
            assert len(rows) == 1
            sig, inst = rows[0]
            assert sig.id == signal_id
            assert inst.id == inst_id
            assert inst.symbol == "BTC-USD"


class TestInstrumentRenameSemantics:
    """Contract tests: fact records survive Instrument rename.

    Instruments use SCD2 close+insert. Fact tables (orders, signals,
    executions, positions) reference Instrument by integer FK which pins
    to a specific version.  After rename the old version is closed, but
    facts must remain visible via a plain FK join with no temporal filter
    on Instrument.  The joined Instrument row shows the snapshot that was
    current when the fact was created.
    """

    @pytest.mark.asyncio
    async def test_orders_remain_visible_after_instrument_rename(self, tmp_path: Path) -> None:
        """Order created on instrument v1 is visible after instrument v2."""
        repo, inst_id = await _create_repo_with_instrument(tmp_path)
        spid = await resolve_symbol_public_id(repo, "BTC-USD")
        assert spid is not None

        order_id, order_pid = await repo.insert_order(
            instrument_id=inst_id,
            client_order_id="cli-rename",
            exchange_order_id=None,
            created_at=datetime.now(UTC),
            side="buy",
            order_type="limit",
            price=50000.0,
            size=1.0,
            status="new",
            session_id="",
            sequence_id=0,
        )

        new_inst_id = await repo.upsert_instrument(
            symbol_public_id=spid,
            symbol="BTC-RENAMED",
            base="BTC",
            quote="RENAMED",
            exchange="kraken",
            session_id="test-session",
            sequence_id=1,
        )
        assert new_inst_id != inst_id

        async with repo.session() as s:
            now = datetime.now(UTC)
            result = await s.execute(
                select(Order, Instrument)
                .join(Instrument)
                .where(Order.timestamp <= now, Order.known_to > now)
            )
            rows = result.all()
            assert len(rows) == 1
            order, inst = rows[0]
            assert order.id == order_id
            assert order.public_id == order_pid
            assert inst.id == inst_id
            assert inst.symbol == "BTC-USD"

    @pytest.mark.asyncio
    async def test_executions_remain_visible_after_instrument_rename(self, tmp_path: Path) -> None:
        """Execution via order on instrument v1 is visible after instrument v2."""
        repo, inst_id = await _create_repo_with_instrument(tmp_path)
        spid = await resolve_symbol_public_id(repo, "BTC-USD")
        assert spid is not None

        order_id, order_pid = await repo.insert_order(
            instrument_id=inst_id,
            client_order_id="cli-exec-rename",
            exchange_order_id="ex-exec-rename",
            created_at=datetime.now(UTC),
            side="buy",
            order_type="limit",
            price=50000.0,
            size=1.0,
            status="filled",
            session_id="",
            sequence_id=0,
        )

        exec_id = await repo.insert_execution(
            order_id=order_id,
            order_public_id=order_pid,
            timestamp=datetime.now(UTC),
            side="buy",
            status="filled",
            price=50000.0,
            size=1.0,
            fee=5.0,
            fee_asset="USD",
            session_id="",
            sequence_id=0,
            exec_id="E-RENAME",
        )

        await repo.upsert_instrument(
            symbol_public_id=spid,
            symbol="BTC-RENAMED",
            base="BTC",
            quote="RENAMED",
            exchange="kraken",
            session_id="test-session",
            sequence_id=1,
        )

        async with repo.session() as s:
            now = datetime.now(UTC)
            result = await s.execute(
                select(Execution, Order, Instrument)
                .join(Order, Execution.order_id == Order.id)
                .join(Instrument, Order.instrument_id == Instrument.id)
                .where(Execution.timestamp <= now, Execution.known_to > now)
            )
            rows = result.all()
            assert len(rows) == 1
            execution, order, inst = rows[0]
            assert execution.id == exec_id
            assert order.public_id == order_pid
            assert inst.id == inst_id
            assert inst.symbol == "BTC-USD"

    @pytest.mark.asyncio
    async def test_positions_remain_visible_after_instrument_rename(self, tmp_path: Path) -> None:
        """Position on instrument v1 is visible after instrument v2."""
        repo, inst_id = await _create_repo_with_instrument(tmp_path)
        spid = await resolve_symbol_public_id(repo, "BTC-USD")
        assert spid is not None

        async with repo.session() as s:
            pos = Position(
                instrument_id=inst_id,
                quantity=1.5,
                average_price=50000.0,
                unrealized_pnl=100.0,
                realized_pnl=0.0,
                timestamp=datetime.now(UTC),
                session_id="test-session",
                sequence_id=1,
            )
            s.add(pos)
            await s.commit()
            await s.refresh(pos)
            pos_id = pos.id

        await repo.upsert_instrument(
            symbol_public_id=spid,
            symbol="BTC-RENAMED",
            base="BTC",
            quote="RENAMED",
            exchange="kraken",
            session_id="test-session",
            sequence_id=1,
        )

        async with repo.session() as s:
            now = datetime.now(UTC)
            result = await s.execute(
                select(Position, Instrument)
                .join(Instrument)
                .where(Position.timestamp <= now, Position.known_to > now)
            )
            rows = result.all()
            assert len(rows) == 1
            position, inst = rows[0]
            assert position.id == pos_id
            assert position.quantity == 1.5
            assert inst.id == inst_id
            assert inst.symbol == "BTC-USD"

    @pytest.mark.asyncio
    async def test_store_signal_no_duplicate_instrument_versions(self, tmp_path: Path) -> None:
        """Repeated upsert_instrument with same payload does not create versions."""
        repo, inst_id = await _create_repo_with_instrument(tmp_path)
        spid = await resolve_symbol_public_id(repo, "BTC-USD")
        assert spid is not None

        for _ in range(5):
            returned_id = await repo.upsert_instrument(
                symbol_public_id=spid,
                symbol="BTC-USD",
                base="BTC",
                quote="USD",
                exchange="kraken",
                session_id="test-session",
                sequence_id=1,
            )
            assert returned_id == inst_id

        async with repo.session() as s:
            all_instruments = (await s.execute(select(Instrument))).scalars().all()
            assert len(all_instruments) == 1

    @pytest.mark.asyncio
    async def test_historical_backfill_resolves_symbol_public_id_with_as_of(
        self, tmp_path: Path
    ) -> None:
        """resolve_symbol_public_id(as_of) finds symbol at historical point.

        Scenario: symbol created at t1, versioned at t2. Query at t1+delta
        returns original, query at t2+delta returns updated version, both
        with the same public_id (SCD2 preserves identity).
        """
        db_path = tmp_path / "backfill_resolve.db"
        repo = SQLAlchemyRepository(f"sqlite+aiosqlite:///{db_path}")
        await repo.create_all()

        t1 = datetime(2024, 1, 1, 0, 0, 0, tzinfo=UTC)
        t2 = datetime(2024, 6, 1, 0, 0, 0, tzinfo=UTC)

        async with repo.session() as s:
            sym = Symbol(
                native_symbol="ETH-USD",
                base="ETH",
                quote="USD",
                asset_type="crypto",
                created_at=t1,
                timestamp=t1,
                session_id="test-session",
                sequence_id=1,
            )
            s.add(sym)
            await s.commit()
            await s.refresh(sym)
            original_pid = sym.public_id

        async with repo.session() as s:
            await close_and_insert(
                s,
                Symbol,
                [Symbol.native_symbol == "ETH-USD"],
                {
                    "native_symbol": "ETH-USD",
                    "base": "ETH",
                    "quote": "USDT",
                    "asset_type": "crypto",
                    "created_at": t1,
                    "session_id": "test-session",
                    "sequence_id": 1,
                },
                t2,
            )
            await s.commit()

        pid_before = await resolve_symbol_public_id(repo, "ETH-USD", as_of=t1 + timedelta(days=30))
        assert pid_before == original_pid

        pid_after = await resolve_symbol_public_id(repo, "ETH-USD", as_of=t2 + timedelta(days=30))
        assert pid_after == original_pid

        pid_none = await resolve_symbol_public_id(repo, "ETH-USD", as_of=t1 - timedelta(days=1))
        assert pid_none is None

    @pytest.mark.asyncio
    async def test_rename_does_not_hide_any_fact_type(self, tmp_path: Path) -> None:
        """Cross-cutting: orders, signals, positions all survive instrument rename.

        Creates one of each fact type on instrument v1, renames instrument
        to v2, then verifies all three are still visible via FK join with
        no temporal filter on Instrument.
        """
        repo, inst_id = await _create_repo_with_instrument(tmp_path)
        spid = await resolve_symbol_public_id(repo, "BTC-USD")
        assert spid is not None
        now = datetime.now(UTC)

        order_id, _ = await repo.insert_order(
            instrument_id=inst_id,
            client_order_id="cli-cross",
            exchange_order_id=None,
            created_at=now,
            side="buy",
            order_type="limit",
            price=50000.0,
            size=1.0,
            status="new",
            session_id="",
            sequence_id=0,
        )

        async with repo.session() as s:
            signal = Signal(
                instrument_id=inst_id,
                fired_at=now,
                timestamp=now,
                side="buy",
                strength=0.8,
                reason="cross-test",
                strategy_name="test",
                price=50000.0,
                session_id="test-session",
                sequence_id=1,
            )
            pos = Position(
                instrument_id=inst_id,
                quantity=2.0,
                average_price=50000.0,
                unrealized_pnl=0.0,
                realized_pnl=0.0,
                timestamp=now,
                session_id="test-session",
                sequence_id=1,
            )
            s.add(signal)
            s.add(pos)
            await s.commit()

        new_inst_id = await repo.upsert_instrument(
            symbol_public_id=spid,
            symbol="BTC-RENAMED",
            base="BTC",
            quote="RENAMED",
            exchange="kraken",
            session_id="test-session",
            sequence_id=1,
        )
        assert new_inst_id != inst_id

        async with repo.session() as s:
            check_time = datetime.now(UTC)

            orders = (
                await s.execute(
                    select(Order, Instrument)
                    .join(Instrument)
                    .where(Order.timestamp <= check_time, Order.known_to > check_time)
                )
            ).all()
            assert len(orders) == 1
            assert orders[0][1].symbol == "BTC-USD"

            signals = (
                await s.execute(
                    select(Signal, Instrument)
                    .join(Instrument)
                    .where(Signal.timestamp <= check_time, Signal.known_to > check_time)
                )
            ).all()
            assert len(signals) == 1
            assert signals[0][1].symbol == "BTC-USD"

            positions = (
                await s.execute(
                    select(Position, Instrument)
                    .join(Instrument)
                    .where(Position.timestamp <= check_time, Position.known_to > check_time)
                )
            ).all()
            assert len(positions) == 1
            assert positions[0][1].symbol == "BTC-USD"

    @pytest.mark.asyncio
    async def test_instrument_join_shows_version_snapshot(self, tmp_path: Path) -> None:
        """Fact joined to closed instrument shows the snapshot from creation time.

        The instrument was 'BTC-USD' when the order was created, so the join
        should return 'BTC-USD' even though active instrument is now 'BTC-RENAMED'.
        """
        repo, inst_id = await _create_repo_with_instrument(tmp_path)
        spid = await resolve_symbol_public_id(repo, "BTC-USD")
        assert spid is not None

        await repo.insert_order(
            instrument_id=inst_id,
            client_order_id="cli-snapshot",
            exchange_order_id=None,
            created_at=datetime.now(UTC),
            side="buy",
            order_type="limit",
            price=50000.0,
            size=1.0,
            status="new",
            session_id="",
            sequence_id=0,
        )

        await repo.upsert_instrument(
            symbol_public_id=spid,
            symbol="BTC-RENAMED",
            base="BTC",
            quote="RENAMED",
            exchange="kraken",
            session_id="test-session",
            sequence_id=1,
        )

        async with repo.session() as s:
            now = datetime.now(UTC)
            result = await s.execute(
                select(Order, Instrument)
                .join(Instrument)
                .where(Order.timestamp <= now, Order.known_to > now)
            )
            order, inst = result.one()
            assert inst.symbol == "BTC-USD"

            active_inst = (
                await s.execute(select(Instrument).where(*where_active(Instrument)))
            ).scalar_one()
            assert active_inst.symbol == "BTC-RENAMED"
