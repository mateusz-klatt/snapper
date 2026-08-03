"""Tests for durable per-asset spot precision evidence persistence."""

import asyncio
import sqlite3
from collections.abc import AsyncIterator
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from pathlib import Path
from threading import Event as ThreadEvent
from unittest.mock import AsyncMock
from unittest.mock import Mock
from unittest.mock import patch

import pytest
from sqlalchemy import create_engine
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Session as SyncSession

from snapper.data.models import KNOWN_TO_MAX
from snapper.data.models import SpotAssetPrecisionEvidence
from snapper.data.repository import DatabaseRepository
from snapper.data.repository import SQLAlchemyRepository
from snapper.data.repository_types import SpotAssetPrecisionEvidenceUpsertRow

_NOW = datetime(2026, 7, 16, 8, 0, tzinfo=UTC)


@pytest.fixture
async def repository(tmp_path: Path) -> AsyncIterator[SQLAlchemyRepository]:
    """Create one isolated repository containing only the evidence table."""
    db_path = tmp_path / "precision-async.db"
    schema_engine = create_engine(f"sqlite:///{db_path}")
    SpotAssetPrecisionEvidence.__table__.create(schema_engine)
    schema_engine.dispose()
    result = SQLAlchemyRepository(f"sqlite+aiosqlite:///{db_path}")
    try:
        yield result
    finally:
        await result.engine.dispose()


def _evidence(
    asset: str = "EUR",
    timestamp: datetime = _NOW,
    balance_decimals: int | None = 2,
    fee_decimals: int | None = 2,
    balance_version: str = "balance-v1",
    fee_version: str = "fee-v1",
    *,
    exchange: str = "walutomat",
    replace_balance: bool = True,
    replace_fee: bool = True,
) -> SpotAssetPrecisionEvidenceUpsertRow:
    """Build one fully typed precision observation payload."""
    return SpotAssetPrecisionEvidenceUpsertRow(
        exchange=exchange,
        asset=asset,
        balance_decimals=balance_decimals,
        balance_source="walutomat:authenticated_balance" if replace_balance else None,
        balance_version=balance_version if replace_balance else None,
        balance_observed_at=timestamp if replace_balance else None,
        fee_decimals=fee_decimals,
        fee_source="walutomat:reviewed_fee_policy" if replace_fee else None,
        fee_version=fee_version if replace_fee else None,
        fee_observed_at=timestamp if replace_fee else None,
        session_id="00000000-0000-7000-8000-000000000001",
        sequence_id=1,
        timestamp=timestamp,
    )


@pytest.mark.asyncio
async def test_async_upsert_inserts_noops_and_revises_temporally(
    repository: SQLAlchemyRepository,
) -> None:
    """Fresh evidence inserts, exact replay no-ops, and refresh creates a successor."""
    first = _evidence(fee_decimals=None)
    first_id = await repository.upsert_spot_asset_precision_evidence(first)
    replay_id = await repository.upsert_spot_asset_precision_evidence(first)
    assert replay_id == first_id

    refreshed_at = _NOW + timedelta(hours=1)
    second = _evidence(
        timestamp=refreshed_at,
        balance_decimals=3,
        fee_decimals=4,
        balance_version="balance-v2",
        fee_version="fee-v2",
    )
    second_id = await repository.upsert_spot_asset_precision_evidence(second)
    assert second_id != first_id

    first_view = await repository.get_spot_asset_precision_evidence("walutomat", ["EUR"], _NOW)
    second_view = await repository.get_spot_asset_precision_evidence(
        "walutomat", ["EUR"], refreshed_at
    )
    stale_view = await repository.get_spot_asset_precision_evidence(
        "walutomat", ["EUR"], refreshed_at + timedelta(hours=13)
    )
    assert first_view["EUR"]["balance_decimals"] == 2
    assert first_view["EUR"]["fee_decimals"] is None
    assert second_view["EUR"] == stale_view["EUR"]
    assert second_view["EUR"]["balance_decimals"] == 3
    assert second_view["EUR"]["fee_decimals"] == 4
    assert second_view["EUR"]["balance_observed_at"] == refreshed_at
    assert second_view["EUR"]["fee_observed_at"] == refreshed_at

    async with repository.session() as session:
        rows = (
            (
                await session.execute(
                    select(SpotAssetPrecisionEvidence).order_by(
                        SpotAssetPrecisionEvidence.timestamp
                    )
                )
            )
            .scalars()
            .all()
        )
    assert len(rows) == 2
    assert rows[0].public_id == rows[1].public_id
    assert rows[0].known_to == refreshed_at
    assert rows[1].known_to == KNOWN_TO_MAX


@pytest.mark.asyncio
async def test_async_upsert_merges_older_attempt_after_newer_active_row(
    repository: SQLAlchemyRepository,
) -> None:
    """An older captured attempt merges at the active row's temporal boundary."""
    newer = _NOW + timedelta(hours=1)
    await repository.upsert_spot_asset_precision_evidence(
        _evidence(
            timestamp=newer,
            fee_decimals=4,
            fee_version="fee-newer",
            replace_balance=False,
        )
    )

    await repository.upsert_spot_asset_precision_evidence(
        _evidence(
            timestamp=_NOW,
            balance_decimals=3,
            balance_version="balance-older",
            replace_fee=False,
        )
    )

    final = await repository.get_spot_asset_precision_evidence(
        "walutomat",
        ["EUR"],
        newer,
    )
    assert final["EUR"]["balance_decimals"] == 3
    assert final["EUR"]["balance_version"] == "balance-older"
    assert final["EUR"]["balance_observed_at"] == _NOW
    assert final["EUR"]["fee_decimals"] == 4
    assert final["EUR"]["fee_version"] == "fee-newer"
    assert final["EUR"]["fee_observed_at"] == newer
    async with repository.session() as session:
        rows = (
            (
                await session.execute(
                    select(SpotAssetPrecisionEvidence).order_by(SpotAssetPrecisionEvidence.id)
                )
            )
            .scalars()
            .all()
        )
    assert len(rows) == 2
    assert rows[0].known_to == newer
    assert rows[1].timestamp == newer
    assert sum(row.known_to == KNOWN_TO_MAX for row in rows) == 1


@pytest.mark.asyncio
async def test_async_upsert_applies_chronology_independently_per_plane(
    repository: SQLAlchemyRepository,
) -> None:
    """Delayed observations cannot undo either plane's newer certification state.

    Given: A newer valid balance plane and revoked fee plane followed by an older
        revoked balance plane and valid fee plane.
    When: Both complete observations are merged under the natural-key lock.
    Then: The newer valid balance stays certified and the newer fee revocation stays.
    """
    newer = _NOW + timedelta(hours=2)
    older = _NOW + timedelta(hours=1)
    active_id = await repository.upsert_spot_asset_precision_evidence(
        _evidence(
            timestamp=newer,
            balance_decimals=3,
            fee_decimals=None,
            balance_version="balance-newer-valid",
            fee_version="fee-newer-revoked",
        )
    )

    replay_id = await repository.upsert_spot_asset_precision_evidence(
        _evidence(
            timestamp=older,
            balance_decimals=None,
            fee_decimals=4,
            balance_version="balance-older-revoked",
            fee_version="fee-older-valid",
        )
    )

    assert replay_id == active_id
    final = await repository.get_spot_asset_precision_evidence(
        "walutomat",
        ["EUR"],
        newer,
    )
    assert final["EUR"]["balance_decimals"] == 3
    assert final["EUR"]["balance_version"] == "balance-newer-valid"
    assert final["EUR"]["balance_observed_at"] == newer
    assert final["EUR"]["fee_decimals"] is None
    assert final["EUR"]["fee_version"] == "fee-newer-revoked"
    assert final["EUR"]["fee_observed_at"] == newer


@pytest.mark.asyncio
async def test_async_balance_scale_ratchet_survives_revocation_and_delayed_evidence(
    repository: SQLAlchemyRepository,
) -> None:
    """The maximum scale accumulates independently of chronological validity.

    Given: A newer revocation, then a delayed four-place valid observation, then
        a newest two-place valid observation.
    When: The balance plane is merged in arrival order.
    Then: The delayed row cannot re-certify, but recovery certifies four places.
    """
    revoked_at = _NOW + timedelta(hours=2)
    delayed_at = _NOW + timedelta(hours=1)
    recovered_at = _NOW + timedelta(hours=3)
    await repository.upsert_spot_asset_precision_evidence(
        _evidence(
            timestamp=revoked_at,
            balance_decimals=None,
            balance_version="balance-newer-revoked",
            replace_fee=False,
        )
    )
    await repository.upsert_spot_asset_precision_evidence(
        _evidence(
            timestamp=delayed_at,
            balance_decimals=4,
            balance_version="balance-delayed-valid",
            replace_fee=False,
        )
    )

    revoked = await repository.get_spot_asset_precision_evidence(
        "walutomat",
        ["EUR"],
        revoked_at,
    )
    assert revoked["EUR"]["balance_decimals"] is None
    assert revoked["EUR"]["balance_version"] == "balance-newer-revoked"
    assert revoked["EUR"]["balance_observed_at"] == revoked_at

    await repository.upsert_spot_asset_precision_evidence(
        _evidence(
            timestamp=recovered_at,
            balance_decimals=2,
            balance_version="balance-newest-valid",
            replace_fee=False,
        )
    )
    recovered = await repository.get_spot_asset_precision_evidence(
        "walutomat",
        ["EUR"],
        recovered_at,
    )
    assert recovered["EUR"]["balance_decimals"] == 4
    assert recovered["EUR"]["balance_version"] == "balance-delayed-valid"
    assert recovered["EUR"]["balance_observed_at"] == delayed_at
    async with repository.session() as session:
        active = (
            await session.execute(
                select(SpotAssetPrecisionEvidence).where(
                    SpotAssetPrecisionEvidence.known_to == KNOWN_TO_MAX
                )
            )
        ).scalar_one()
    assert active.balance_decimals == 2
    assert active.balance_version == "balance-newest-valid"
    assert active.balance_observed_at == recovered_at
    assert active.balance_decimals_max == 4
    assert active.balance_max_source == "walutomat:authenticated_balance"
    assert active.balance_max_version == "balance-delayed-valid"
    assert active.balance_max_observed_at == delayed_at


@pytest.mark.asyncio
async def test_async_equal_maximum_rebinds_covering_provenance(
    repository: SQLAlchemyRepository,
) -> None:
    """A fresh observation of the emitted scale may refresh its exact provenance."""
    refreshed_at = _NOW + timedelta(hours=1)
    await repository.upsert_spot_asset_precision_evidence(
        _evidence(
            balance_decimals=4,
            balance_version="balance-maximum-v1",
            replace_fee=False,
        )
    )
    await repository.upsert_spot_asset_precision_evidence(
        _evidence(
            timestamp=refreshed_at,
            balance_decimals=4,
            balance_version="balance-maximum-v2",
            replace_fee=False,
        )
    )

    refreshed = await repository.get_spot_asset_precision_evidence(
        "walutomat",
        ["EUR"],
        refreshed_at,
    )
    assert refreshed["EUR"]["balance_decimals"] == 4
    assert refreshed["EUR"]["balance_version"] == "balance-maximum-v2"
    assert refreshed["EUR"]["balance_observed_at"] == refreshed_at


@pytest.mark.asyncio
async def test_batched_read_preserves_aliases_and_deduplicates_requests(
    repository: SQLAlchemyRepository,
) -> None:
    """Caller-resolved alias identities stay distinct while repeated keys collapse."""
    await repository.upsert_spot_asset_precision_evidence(_evidence(asset="BTC"))
    await repository.upsert_spot_asset_precision_evidence(
        _evidence(asset="XBT", balance_decimals=8, fee_decimals=8)
    )

    rows = await repository.get_spot_asset_precision_evidence(
        "walutomat", ["XBT", "BTC", "XBT", "MISSING"], _NOW
    )
    assert set(rows) == {"BTC", "XBT"}
    assert rows["BTC"]["asset"] == "BTC"
    assert rows["XBT"]["asset"] == "XBT"
    assert rows["XBT"]["balance_decimals"] == 8
    assert await repository.get_spot_asset_precision_evidence("walutomat", [], _NOW) == {}


@pytest.mark.asyncio
async def test_batched_read_partitions_the_same_asset_by_exchange(
    repository: SQLAlchemyRepository,
) -> None:
    """The persisted exchange column remains authoritative through projection.

    Given: Independent evidence rows for the same asset on two venues.
    When: Each venue reads its exact natural-key partition.
    Then: Each projection carries its own exchange identity and precision tuple.
    """
    await repository.upsert_spot_asset_precision_evidence(
        _evidence(balance_decimals=2, fee_decimals=2)
    )
    await repository.upsert_spot_asset_precision_evidence(
        _evidence(
            balance_decimals=8,
            fee_decimals=8,
            exchange="kraken",
        )
    )

    walutomat = await repository.get_spot_asset_precision_evidence(
        "walutomat",
        ["EUR"],
        _NOW,
    )
    kraken = await repository.get_spot_asset_precision_evidence(
        "kraken",
        ["EUR"],
        _NOW,
    )

    assert walutomat["EUR"]["exchange"] == "walutomat"
    assert walutomat["EUR"]["balance_decimals"] == 2
    assert kraken["EUR"]["exchange"] == "kraken"
    assert kraken["EUR"]["balance_decimals"] == 8


@pytest.mark.asyncio
async def test_batched_read_rejects_overlapping_duplicate_evidence(
    repository: SQLAlchemyRepository,
) -> None:
    """Forged overlapping SCD2 versions fail closed instead of picking one."""
    later = _NOW + timedelta(hours=2)
    async with repository.session() as session:
        session.add_all(
            [
                SpotAssetPrecisionEvidence(
                    exchange="walutomat",
                    asset="EUR",
                    balance_decimals=2,
                    balance_decimals_max=2,
                    balance_max_source="first-balance",
                    balance_max_version="balance-v1",
                    balance_max_observed_at=_NOW,
                    balance_source="first-balance",
                    balance_version="balance-v1",
                    balance_observed_at=_NOW,
                    fee_decimals=2,
                    fee_source="first-fee",
                    fee_version="fee-v1",
                    fee_observed_at=_NOW,
                    session_id="00000000-0000-7000-8000-000000000001",
                    sequence_id=1,
                    timestamp=_NOW,
                    known_to=later,
                ),
                SpotAssetPrecisionEvidence(
                    exchange="walutomat",
                    asset="EUR",
                    balance_decimals=3,
                    balance_decimals_max=3,
                    balance_max_source="second-balance",
                    balance_max_version="balance-v2",
                    balance_max_observed_at=_NOW + timedelta(hours=1),
                    balance_source="second-balance",
                    balance_version="balance-v2",
                    balance_observed_at=_NOW + timedelta(hours=1),
                    fee_decimals=3,
                    fee_source="second-fee",
                    fee_version="fee-v2",
                    fee_observed_at=_NOW + timedelta(hours=1),
                    session_id="00000000-0000-7000-8000-000000000002",
                    sequence_id=2,
                    timestamp=_NOW + timedelta(hours=1),
                    known_to=KNOWN_TO_MAX,
                ),
            ]
        )
        await session.commit()

    with pytest.raises(RuntimeError, match="duplicate spot asset precision evidence"):
        await repository.get_spot_asset_precision_evidence(
            "walutomat", ["EUR"], _NOW + timedelta(minutes=90)
        )


def test_sync_upsert_is_atomic_caller_owned_scd2(tmp_path: Path) -> None:
    """The synchronous updater helper reports each SCD2 outcome without committing."""
    db_path = tmp_path / "precision.db"
    repository = DatabaseRepository(f"sqlite:///{db_path}")
    SpotAssetPrecisionEvidence.__table__.create(repository.engine)
    first = _evidence()
    second = _evidence(
        timestamp=_NOW + timedelta(hours=1),
        balance_decimals=4,
        balance_version="balance-v2",
        replace_fee=False,
    )
    with repository.get_session() as session:
        assert (
            DatabaseRepository.upsert_spot_asset_precision_evidence_sync(session, first)
            == "created"
        )
        assert (
            DatabaseRepository.upsert_spot_asset_precision_evidence_sync(session, first)
            == "unchanged"
        )
        assert (
            DatabaseRepository.upsert_spot_asset_precision_evidence_sync(session, second)
            == "updated"
        )
        with pytest.raises(ValueError, match="at least one"):
            DatabaseRepository.upsert_spot_asset_precision_evidence_sync(
                session,
                _evidence(replace_balance=False, replace_fee=False),
            )
        session.commit()
        rows = (
            session.execute(
                select(SpotAssetPrecisionEvidence).order_by(SpotAssetPrecisionEvidence.timestamp)
            )
            .scalars()
            .all()
        )
    assert len(rows) == 2
    assert rows[0].public_id == rows[1].public_id
    assert rows[0].known_to == second["timestamp"]
    assert rows[1].balance_decimals == 4
    assert rows[1].fee_decimals == 2
    assert rows[1].fee_version == "fee-v1"
    repository.engine.dispose()


def test_sync_upsert_merges_older_attempt_after_newer_active_row(tmp_path: Path) -> None:
    """The updater also normalizes reverse-time merges under its transaction lock."""
    db_path = tmp_path / "precision-reverse-sync.db"
    repository = DatabaseRepository(f"sqlite:///{db_path}")
    SpotAssetPrecisionEvidence.__table__.create(repository.engine)
    newer = _NOW + timedelta(hours=1)
    fee_row = _evidence(
        timestamp=newer,
        fee_decimals=4,
        fee_version="fee-newer",
        replace_balance=False,
    )
    balance_row = _evidence(
        timestamp=_NOW,
        balance_decimals=3,
        balance_version="balance-older",
        replace_fee=False,
    )
    with repository.get_session() as session:
        assert (
            DatabaseRepository.upsert_spot_asset_precision_evidence_sync(session, fee_row)
            == "created"
        )
        session.commit()
    with repository.get_session() as session:
        assert (
            DatabaseRepository.upsert_spot_asset_precision_evidence_sync(
                session,
                balance_row,
            )
            == "updated"
        )
        session.commit()
    with repository.get_session() as session:
        rows = (
            session.execute(
                select(SpotAssetPrecisionEvidence).order_by(SpotAssetPrecisionEvidence.id)
            )
            .scalars()
            .all()
        )
    assert len(rows) == 2
    assert rows[0].known_to == newer
    assert rows[1].timestamp == newer
    assert rows[1].balance_decimals == 3
    assert rows[1].balance_version == "balance-older"
    assert rows[1].balance_observed_at == _NOW
    assert rows[1].fee_decimals == 4
    assert rows[1].fee_version == "fee-newer"
    assert rows[1].fee_observed_at == newer
    assert sum(row.known_to == KNOWN_TO_MAX for row in rows) == 1
    repository.engine.dispose()


def test_sync_upsert_applies_chronology_independently_per_plane(tmp_path: Path) -> None:
    """The sync producer preserves each plane's newest observation state."""
    db_path = tmp_path / "precision-plane-chronology-sync.db"
    repository = DatabaseRepository(f"sqlite:///{db_path}")
    SpotAssetPrecisionEvidence.__table__.create(repository.engine)
    newer = _NOW + timedelta(hours=2)
    older = _NOW + timedelta(hours=1)
    newer_row = _evidence(
        timestamp=newer,
        balance_decimals=3,
        fee_decimals=None,
        balance_version="balance-newer-valid",
        fee_version="fee-newer-revoked",
    )
    older_row = _evidence(
        timestamp=older,
        balance_decimals=None,
        fee_decimals=4,
        balance_version="balance-older-revoked",
        fee_version="fee-older-valid",
    )
    with repository.get_session() as session:
        assert (
            DatabaseRepository.upsert_spot_asset_precision_evidence_sync(
                session,
                newer_row,
            )
            == "created"
        )
        session.commit()
    with repository.get_session() as session:
        assert (
            DatabaseRepository.upsert_spot_asset_precision_evidence_sync(
                session,
                older_row,
            )
            == "unchanged"
        )
        session.commit()
    with repository.get_session() as session:
        active = session.execute(
            select(SpotAssetPrecisionEvidence).where(
                SpotAssetPrecisionEvidence.known_to == KNOWN_TO_MAX
            )
        ).scalar_one()
    assert active.balance_decimals == 3
    assert active.balance_decimals_max == 3
    assert active.balance_version == "balance-newer-valid"
    assert active.balance_observed_at == newer
    assert active.fee_decimals is None
    assert active.fee_version == "fee-newer-revoked"
    assert active.fee_observed_at == newer
    repository.engine.dispose()


def test_sync_balance_scale_ratchet_keeps_covering_provenance(tmp_path: Path) -> None:
    """The sync producer never pairs the ratchet maximum with a lower-scale hash."""
    db_path = tmp_path / "precision-ratchet-provenance-sync.db"
    repository = DatabaseRepository(f"sqlite:///{db_path}")
    SpotAssetPrecisionEvidence.__table__.create(repository.engine)
    maximum_at = _NOW + timedelta(hours=1)
    refreshed_at = _NOW + timedelta(hours=2)
    maximum = _evidence(
        timestamp=maximum_at,
        balance_decimals=4,
        balance_version="balance-maximum",
        replace_fee=False,
    )
    lower_refresh = _evidence(
        timestamp=refreshed_at,
        balance_decimals=2,
        balance_version="balance-lower-refresh",
        replace_fee=False,
    )
    with repository.get_session() as session:
        assert (
            DatabaseRepository.upsert_spot_asset_precision_evidence_sync(session, maximum)
            == "created"
        )
        session.commit()
    with repository.get_session() as session:
        assert (
            DatabaseRepository.upsert_spot_asset_precision_evidence_sync(
                session,
                lower_refresh,
            )
            == "updated"
        )
        session.commit()
    with repository.get_session() as session:
        active = session.execute(
            select(SpotAssetPrecisionEvidence).where(
                SpotAssetPrecisionEvidence.known_to == KNOWN_TO_MAX
            )
        ).scalar_one()
    assert active.balance_decimals == 2
    assert active.balance_version == "balance-lower-refresh"
    assert active.balance_observed_at == refreshed_at
    assert active.balance_decimals_max == 4
    assert active.balance_max_source == "walutomat:authenticated_balance"
    assert active.balance_max_version == "balance-maximum"
    assert active.balance_max_observed_at == maximum_at
    repository.engine.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("plane", "source", "version", "observed_at"),
    (
        ("balance", "partial", None, None),
        ("balance", None, "partial", None),
        ("balance", None, None, _NOW),
        ("fee", "partial", None, None),
        ("fee", None, "partial", None),
        ("fee", None, None, _NOW),
    ),
)
async def test_upsert_rejects_partial_plane_provenance(
    repository: SQLAlchemyRepository,
    plane: str,
    source: str | None,
    version: str | None,
    observed_at: datetime | None,
) -> None:
    """A plane cannot replace persisted evidence with partial provenance."""
    row = _evidence(replace_balance=False, replace_fee=False)
    if plane == "balance":
        row["balance_source"] = source
        row["balance_version"] = version
        row["balance_observed_at"] = observed_at
    else:
        row["fee_source"] = source
        row["fee_version"] = version
        row["fee_observed_at"] = observed_at

    with pytest.raises(ValueError, match="provenance must be complete"):
        await repository.upsert_spot_asset_precision_evidence(row)


@pytest.mark.asyncio
async def test_upsert_rejects_payload_without_an_observed_plane(
    repository: SQLAlchemyRepository,
) -> None:
    """An all-preserve payload cannot create an evidence-free row."""
    with pytest.raises(ValueError, match="at least one"):
        await repository.upsert_spot_asset_precision_evidence(
            _evidence(replace_balance=False, replace_fee=False)
        )


@pytest.mark.asyncio
async def test_concurrent_same_key_plane_updates_merge_after_process_lock(
    repository: SQLAlchemyRepository,
) -> None:
    """Two deterministic contenders preserve the other independently sourced plane."""
    await repository.upsert_spot_asset_precision_evidence(_evidence())
    first_has_lock = asyncio.Event()
    second_requested_lock = asyncio.Event()
    release_first = asyncio.Event()
    real_acquire = repository._acquire_spot_asset_precision_evidence_process_lock
    acquisition_count = 0

    async def gated_acquire(exchange: str, asset: str) -> asyncio.Lock:
        """Hold the first contender while the second queues on the same key."""
        nonlocal acquisition_count
        acquisition_count += 1
        if acquisition_count == 1:
            acquired = await real_acquire(exchange, asset)
            first_has_lock.set()
            await release_first.wait()
            return acquired
        second_requested_lock.set()
        return await real_acquire(exchange, asset)

    first_at = _NOW + timedelta(hours=1)
    second_at = _NOW + timedelta(hours=2)
    with patch.object(
        repository,
        "_acquire_spot_asset_precision_evidence_process_lock",
        side_effect=gated_acquire,
    ):
        first_task = asyncio.create_task(
            repository.upsert_spot_asset_precision_evidence(
                _evidence(
                    timestamp=first_at,
                    balance_decimals=4,
                    balance_version="balance-v2",
                    replace_fee=False,
                )
            )
        )
        await first_has_lock.wait()
        second_task = asyncio.create_task(
            repository.upsert_spot_asset_precision_evidence(
                _evidence(
                    timestamp=second_at,
                    fee_decimals=6,
                    fee_version="fee-v2",
                    replace_balance=False,
                )
            )
        )
        await second_requested_lock.wait()
        release_first.set()
        first_id, second_id = await asyncio.gather(first_task, second_task)

    assert first_id != second_id
    final = await repository.get_spot_asset_precision_evidence(
        "walutomat",
        ["EUR"],
        second_at,
    )
    assert final["EUR"]["balance_decimals"] == 4
    assert final["EUR"]["balance_version"] == "balance-v2"
    assert final["EUR"]["balance_observed_at"] == first_at
    assert final["EUR"]["fee_decimals"] == 6
    assert final["EUR"]["fee_version"] == "fee-v2"
    assert final["EUR"]["fee_observed_at"] == second_at


@pytest.mark.asyncio
async def test_concurrent_absent_key_unique_conflict_retries_to_winner(
    repository: SQLAlchemyRepository,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A deterministic absent-key loser retries and returns the winning row id."""
    loser_at_commit = asyncio.Event()
    winner_committed = asyncio.Event()
    original_commit = AsyncSession.commit

    async def independent_lock(_exchange: str, _asset: str) -> asyncio.Lock:
        """Model a competing producer outside this repository's process lock."""
        lock = asyncio.Lock()
        await lock.acquire()
        return lock

    async def ordered_commit(session: AsyncSession) -> None:
        """Force one absent-key attempt to lose after the winner commits."""
        task = asyncio.current_task()
        if task is not None and task.get_name() == "precision-absent-loser":
            await session.rollback()
            loser_at_commit.set()
            await winner_committed.wait()
            raise IntegrityError("insert", {}, RuntimeError("active-key conflict"))
        await loser_at_commit.wait()
        await original_commit(session)
        winner_committed.set()

    monkeypatch.setattr(AsyncSession, "commit", ordered_commit)
    with (
        patch.object(
            repository,
            "_acquire_spot_asset_precision_evidence_process_lock",
            side_effect=independent_lock,
        ),
        patch.object(
            repository,
            "_acquire_spot_asset_precision_evidence_advisory_lock",
            new=AsyncMock(),
        ),
    ):
        loser = asyncio.create_task(
            repository.upsert_spot_asset_precision_evidence(_evidence(replace_balance=False)),
            name="precision-absent-loser",
        )
        winner = asyncio.create_task(
            repository.upsert_spot_asset_precision_evidence(_evidence(replace_balance=False)),
            name="precision-absent-winner",
        )
        loser_id, winner_id = await asyncio.gather(loser, winner)

    assert loser_id == winner_id
    async with repository.session() as session:
        rows = (await session.execute(select(SpotAssetPrecisionEvidence))).scalars().all()
    assert len(rows) == 1
    assert rows[0].fee_source == "walutomat:reviewed_fee_policy"


@pytest.mark.asyncio
async def test_absent_key_nonunique_integrity_error_is_not_retried(
    repository: SQLAlchemyRepository,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An absent-key constraint failure without a winner preserves its cause."""
    commit_calls = 0

    async def fail_commit(_session: AsyncSession) -> None:
        """Reject the insert without creating a natural-key winner."""
        nonlocal commit_calls
        commit_calls += 1
        raise IntegrityError("insert", {}, RuntimeError("check constraint failed"))

    monkeypatch.setattr(AsyncSession, "commit", fail_commit)

    with pytest.raises(IntegrityError, match="check constraint failed"):
        await repository.upsert_spot_asset_precision_evidence(_evidence())

    assert commit_calls == 1


@pytest.mark.asyncio
async def test_existing_key_integrity_error_is_not_retried(
    repository: SQLAlchemyRepository,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Only an absent-key insert conflict receives the bounded retry."""
    await repository.upsert_spot_asset_precision_evidence(_evidence())
    commit_calls = 0

    async def fail_commit(_session: AsyncSession) -> None:
        """Reject the replacement commit with a non-absence integrity failure."""
        nonlocal commit_calls
        commit_calls += 1
        raise IntegrityError("update", {}, RuntimeError("replacement conflict"))

    monkeypatch.setattr(AsyncSession, "commit", fail_commit)
    with pytest.raises(IntegrityError, match="replacement conflict"):
        await repository.upsert_spot_asset_precision_evidence(
            _evidence(
                timestamp=_NOW + timedelta(hours=1),
                balance_decimals=5,
                balance_version="balance-v2",
                replace_fee=False,
            )
        )
    assert commit_calls == 1


@pytest.mark.asyncio
async def test_async_postgresql_lock_uses_shared_advisory_identity(
    repository: SQLAlchemyRepository,
) -> None:
    """The async producer locks the exchange and asset transaction identity."""
    session = AsyncMock()
    with patch.object(type(repository), "dialect_name", new_callable=lambda: "postgresql"):
        await repository._acquire_spot_asset_precision_evidence_advisory_lock(
            session,
            "walutomat",
            "EUR",
        )

    statement, parameters = session.execute.await_args.args
    assert "pg_advisory_xact_lock" in str(statement)
    assert parameters == {"identity": "walutomat\x1fEUR"}


@pytest.mark.asyncio
async def test_async_sqlite_lock_starts_write_transaction(
    repository: SQLAlchemyRepository,
) -> None:
    """SQLite acquires its database write lock before the evidence comparison."""
    session = AsyncMock()

    await repository._acquire_spot_asset_precision_evidence_advisory_lock(
        session,
        "walutomat",
        "EUR",
    )

    statement = session.execute.await_args.args[0]
    assert str(statement) == "BEGIN IMMEDIATE"


@pytest.mark.asyncio
async def test_async_spot_precision_lock_rejects_unknown_dialect(
    repository: SQLAlchemyRepository,
) -> None:
    """An unsupported backend cannot perform an unlocked evidence merge."""
    session = AsyncMock()
    with (
        patch.object(type(repository), "dialect_name", new_callable=lambda: "mysql"),
        pytest.raises(NotImplementedError, match="mysql"),
    ):
        await repository._acquire_spot_asset_precision_evidence_advisory_lock(
            session,
            "walutomat",
            "EUR",
        )
    session.execute.assert_not_awaited()


def test_sync_postgresql_lock_uses_shared_advisory_identity() -> None:
    """The sync updater uses exactly the async producer's advisory identity."""
    session = Mock(spec=SyncSession)
    bind = Mock()
    bind.dialect.name = "postgresql"
    session.get_bind.return_value = bind

    DatabaseRepository._acquire_spot_asset_precision_evidence_advisory_lock_sync(
        session,
        "walutomat",
        "EUR",
    )

    statement, parameters = session.execute.call_args.args
    assert "pg_advisory_xact_lock" in str(statement)
    assert parameters == {"identity": "walutomat\x1fEUR"}


def test_sync_sqlite_lock_starts_write_transaction(tmp_path: Path) -> None:
    """The sync producer obtains the same SQLite database write exclusion."""
    repository = DatabaseRepository(f"sqlite:///{tmp_path / 'sync-lock.db'}")

    with repository.get_session() as session:
        driver_connection = session.connection().connection.driver_connection
        assert isinstance(driver_connection, sqlite3.Connection)
        assert driver_connection.in_transaction is False

        DatabaseRepository._acquire_spot_asset_precision_evidence_advisory_lock_sync(
            session,
            "walutomat",
            "EUR",
        )

        assert driver_connection.in_transaction is True
        session.rollback()
    repository.engine.dispose()


def test_sync_spot_precision_lock_rejects_unknown_dialect() -> None:
    """The synchronous producer also rejects unsupported unlocked backends."""
    session = Mock(spec=SyncSession)
    bind = Mock()
    bind.dialect.name = "mysql"
    session.get_bind.return_value = bind

    with pytest.raises(NotImplementedError, match="mysql"):
        DatabaseRepository._acquire_spot_asset_precision_evidence_advisory_lock_sync(
            session,
            "walutomat",
            "EUR",
        )
    session.execute.assert_not_called()


@pytest.mark.asyncio
async def test_sqlite_sync_and_async_producers_merge_under_database_lock(
    repository: SQLAlchemyRepository,
) -> None:
    """The real updater and observer paths serialize across repository types."""
    sync_ready = ThreadEvent()
    release_sync = ThreadEvent()

    def sync_writer() -> None:
        """Hold a fee-only updater transaction until the observer is waiting."""
        sync_repository = DatabaseRepository(repository.db_url)
        try:
            with sync_repository.get_session() as session:
                outcome = sync_repository.upsert_spot_asset_precision_evidence_sync(
                    session,
                    _evidence(replace_balance=False),
                )
                assert outcome == "created"
                sync_ready.set()
                release_sync.wait(timeout=5)
                session.commit()
        finally:
            sync_repository.engine.dispose()

    async_lock_attempted = asyncio.Event()
    real_lock = repository._acquire_spot_asset_precision_evidence_advisory_lock

    async def observed_lock(
        session: AsyncSession,
        exchange: str,
        asset: str,
    ) -> None:
        """Expose when the observer begins waiting for SQLite write exclusion."""
        async_lock_attempted.set()
        await real_lock(session, exchange, asset)

    executor = ThreadPoolExecutor(max_workers=1)
    sync_future = executor.submit(sync_writer)
    try:
        async with asyncio.timeout(5):
            while not sync_ready.is_set():
                await asyncio.sleep(0)
        later = _NOW + timedelta(hours=1)
        with patch.object(
            repository,
            "_acquire_spot_asset_precision_evidence_advisory_lock",
            side_effect=observed_lock,
        ):
            async_task = asyncio.create_task(
                repository.upsert_spot_asset_precision_evidence(
                    _evidence(
                        timestamp=later,
                        balance_version="balance-v2",
                        replace_fee=False,
                    )
                )
            )
            await async_lock_attempted.wait()
            assert async_task.done() is False
            release_sync.set()
            await async_task
        assert sync_future.result(timeout=5) is None
    finally:
        release_sync.set()
        executor.shutdown(wait=True)

    final = await repository.get_spot_asset_precision_evidence(
        "walutomat",
        ["EUR"],
        later,
    )
    assert final["EUR"]["balance_version"] == "balance-v2"
    assert final["EUR"]["fee_version"] == "fee-v1"


@pytest.mark.asyncio
async def test_async_upsert_skips_process_lock_on_non_sqlite_dialect(
    repository: SQLAlchemyRepository,
) -> None:
    """Verify non-SQLite dialects bypass the in-process serialization lock.

    Given: A repository whose dialect reports PostgreSQL,
    When: An evidence upsert runs,
    Then: No SQLite process lock is acquired and the locked writer result is
        returned unchanged (PostgreSQL serializes via its advisory lock).
    """
    process_lock = AsyncMock()
    once = AsyncMock(return_value="created")
    with (
        patch.object(type(repository), "dialect_name", property(lambda self: "postgresql")),
        patch.object(
            repository,
            "_acquire_spot_asset_precision_evidence_process_lock",
            process_lock,
        ),
        patch.object(repository, "_upsert_spot_asset_precision_evidence_once", once),
    ):
        result = await repository.upsert_spot_asset_precision_evidence(_evidence())
    assert result == "created"
    process_lock.assert_not_awaited()
    once.assert_awaited_once()


def test_sync_advisory_lock_rejects_foreign_sqlite_driver() -> None:
    """Verify the sync lock fails closed on a non-sqlite3 driver connection.

    Given: A session whose SQLite bind exposes a foreign driver connection,
    When: The sync advisory lock is acquired,
    Then: A TypeError is raised instead of silently skipping serialization.
    """
    bind = Mock()
    bind.dialect.name = "sqlite"
    session = Mock()
    session.get_bind.return_value = bind
    session.connection.return_value.connection.driver_connection = object()
    with pytest.raises(TypeError, match="sqlite3 connection"):
        DatabaseRepository._acquire_spot_asset_precision_evidence_advisory_lock_sync(
            session,
            "walutomat",
            "EUR",
        )
