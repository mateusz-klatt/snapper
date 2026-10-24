"""Prevent historical request mutation from regaining CREATE permission through recovery."""

import json
from collections.abc import AsyncIterator
from datetime import UTC
from datetime import datetime
from pathlib import Path
from typing import cast
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import func
from sqlalchemy import select

from snapper.application.trade.outbox import OutboxDispatcher
from snapper.core.command_material import COMMAND_MATERIAL_CONFLICT_EVENT
from snapper.core.command_material import TradeCommandMaterialChangedError
from snapper.core.command_material import TradeCommandMaterialUnavailableError
from snapper.data.models import TradeCommand
from snapper.data.models import VenueEvent
from snapper.data.repository import Repository
from snapper.data.repository import SQLAlchemyRepository
from snapper.data.repository_types import TradeCommandRow
from tests.helpers.command_material import MATERIAL_WALLET
from tests.helpers.command_material import active_command
from tests.helpers.command_material import seed_command_history


@pytest.fixture
async def material_repository(tmp_path: Path) -> AsyncIterator[SQLAlchemyRepository]:
    """Create and dispose a private SQLite database without shared fixture mutation."""
    repository = SQLAlchemyRepository(f"sqlite+aiosqlite:///{tmp_path / 'material.db'}")
    await repository.create_all()
    try:
        yield repository
    finally:
        await repository.engine.dispose()


async def test_history_conflict_commits_exact_scoped_barrier_without_rewriting_commands(
    material_repository: SQLAlchemyRepository,
) -> None:
    """A previously lost maker-only flag cannot be interpreted as a new taker permission.

    Given: One logical command has historical post_only=True and active post_only=False,
    When: Its CREATE material is checked,
    Then: A durable scoped barrier records the conflict while both original versions remain exact.
    """
    command_id = await seed_command_history(material_repository, "historical-maker", (True, False))
    with pytest.raises(TradeCommandMaterialChangedError) as refused:
        await material_repository.require_consistent_trade_command_material(command_id)
    assert refused.value.command_public_id == command_id
    assert refused.value.changed_fields == ("post_only",)
    current = await active_command(material_repository, "historical-maker")
    assert current["public_id"] == command_id
    assert current["post_only"] is False
    assert current["status"] == "created"
    async with material_repository.session() as session:
        versions = (await session.scalars(select(TradeCommand).order_by(TradeCommand.id))).all()
        assert [version.post_only for version in versions] == [True, False]
        assert [version.status for version in versions] == ["created", "created"]
        barrier = (await session.scalars(select(VenueEvent))).one()
        assert barrier.event_type == COMMAND_MATERIAL_CONFLICT_EVENT
        assert barrier.command_public_id == command_id
        assert barrier.client_order_id == "historical-maker"
        assert barrier.wallet_public_id == MATERIAL_WALLET
        assert (barrier.exchange, barrier.mode) == ("kraken", "live")
        assert barrier.fill_price is None and barrier.fill_size is None and barrier.fee is None
        assert barrier.status is None
        assert json.loads(barrier.payload_json or "{}")["changed_fields"] == ["post_only"]


@pytest.mark.parametrize("values", [(False,), (False, False), (True, True)])
async def test_new_false_intent_and_status_only_versions_remain_legal(
    material_repository: SQLAlchemyRepository, values: tuple[bool, ...]
) -> None:
    """Unchanged material remains admissible regardless of the requested maker flag.

    Given: A new ordinary intent or repeated SCD2 versions with unchanged material,
    When: An actual status transition is followed by consistency verification,
    Then: The current row remains usable and no material barrier is invented.
    """
    command_id = await seed_command_history(material_repository, "consistent", values)
    await material_repository.update_trade_command_status(
        public_id=command_id,
        new_status="dispatched",
        bus_time=datetime.now(UTC),
        session_id="next-session",
        sequence_id=99,
        attempt_count=12,
        dispatched_at=datetime.now(UTC),
        last_error="status-only diagnostic",
    )
    command = await material_repository.require_consistent_trade_command_material(command_id)
    assert command["post_only"] is values[-1]
    assert command["status"] == "dispatched"
    async with material_repository.session() as session:
        assert await session.scalar(select(func.count()).select_from(VenueEvent)) == 0


async def test_closed_marker_survives_restart_and_excludes_only_its_exact_logical_intent(
    material_repository: SQLAlchemyRepository,
) -> None:
    """A checkpoint watermark or closed marker cannot resurrect the corrupted CREATE.

    Given: A blocked command, a separate valid false intent, and a cancel sharing the blocked cid,
    When: The marker is closed and a fresh repository reads the outbox,
    Then: Only the blocked command UUID is excluded while healthy create and cancel remain available.
    """
    blocked = await seed_command_history(material_repository, "same-cid", (True, False))
    healthy = await seed_command_history(material_repository, "healthy-cid", (False,))
    cancel = await seed_command_history(
        material_repository, "same-cid", (False,), command_type="cancel"
    )
    with pytest.raises(TradeCommandMaterialChangedError):
        await material_repository.require_consistent_trade_command_material(blocked)
    async with material_repository.session() as session:
        marker = (await session.scalars(select(VenueEvent))).one()
        marker.known_to = datetime.now(UTC)
        await session.commit()
    restarted = SQLAlchemyRepository(str(material_repository.engine.url))
    try:
        rows = await restarted.get_undispatched_commands(datetime.now(UTC))
        assert {row["public_id"] for row in rows} == {healthy, cancel}
        assert (await active_command(restarted, "same-cid"))["public_id"] == blocked
        with pytest.raises(TradeCommandMaterialChangedError):
            await restarted.require_consistent_trade_command_material(blocked)
    finally:
        await restarted.engine.dispose()


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("wallet_public_id", "00000000-0000-7000-8000-000000000212"),
        ("exchange", "walutomat"),
        ("mode", "paper"),
    ],
)
async def test_foreign_scope_marker_cannot_block_an_unrelated_account_intent(
    material_repository: SQLAlchemyRepository, field: str, value: str
) -> None:
    """Logical UUID matching cannot bypass the full account-scope binding.

    Given: A healthy command and an otherwise matching barrier from a different account scope,
    When: Dispatch candidates and immutable material are read,
    Then: The foreign marker does not suppress or quarantine this account's healthy intent.
    """
    command_id = await seed_command_history(material_repository, "scope-bound", (False,))
    command = await active_command(material_repository, "scope-bound")
    now = datetime.now(UTC)
    marker = VenueEvent(
        event_type=COMMAND_MATERIAL_CONFLICT_EVENT,
        command_public_id=command_id,
        wallet_public_id=MATERIAL_WALLET,
        exchange="kraken",
        mode="live",
        instrument="BTC-USD",
        shard_key=command["shard_key"],
        received_at=now,
        timestamp=now,
        session_id="foreign-scope",
        sequence_id=1,
    )
    setattr(marker, field, value)
    async with material_repository.session() as session:
        session.add(marker)
        await session.commit()
    assert (await material_repository.get_undispatched_commands(now))[0]["public_id"] == command_id
    assert (await material_repository.require_consistent_trade_command_material(command_id))[
        "public_id"
    ] == command_id


async def test_outbox_skips_material_conflict_and_dispatches_healthy_batch_peer_without_churn(
    material_repository: SQLAlchemyRepository,
) -> None:
    """A corrupt historical command cannot starve later healthy outbox work.

    Given: A corrupted intent precedes a distinct healthy false intent in the same batch,
    When: The real outbox dispatches twice,
    Then: Only the healthy intent is published and the blocked command gains no retry versions.
    """
    blocked = await seed_command_history(material_repository, "blocked-first", (True, False))
    healthy = await seed_command_history(material_repository, "healthy-next", (False,))
    publish = AsyncMock()
    outbox = OutboxDispatcher(material_repository, publish_fn=publish)
    await outbox._dispatch_batch()
    await outbox._dispatch_batch()
    publish.assert_awaited_once()
    assert publish.await_args is not None
    published = cast(TradeCommandRow, publish.await_args.args[0])
    assert published["public_id"] == healthy and published["post_only"] is False
    async with material_repository.session() as session:
        versions = (
            await session.scalars(
                select(TradeCommand)
                .where(TradeCommand.public_id == blocked)
                .order_by(TradeCommand.id)
            )
        ).all()
        assert len(versions) == 2
        assert [row.post_only for row in versions] == [True, False]
        assert all(row.status == "created" for row in versions)


async def test_failed_marker_append_halts_outbox_without_publish_or_retry_version(
    material_repository: SQLAlchemyRepository, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Failure to preserve the barrier cannot fall through the generic retry-status path.

    Given: Historical material conflicts and durable marker append fails,
    When: The real outbox attempts dispatch,
    Then: It fails unavailable without publishing, changing history, or fabricating a terminal state.
    """
    command_id = await seed_command_history(material_repository, "failed-marker", (True, False))
    monkeypatch.setattr(
        material_repository,
        "_append_command_material_barrier",
        AsyncMock(side_effect=OSError("disk unavailable")),
    )
    retry = AsyncMock(wraps=material_repository.update_trade_command_status)
    monkeypatch.setattr(material_repository, "update_trade_command_status", retry)
    publish = AsyncMock()
    outbox = OutboxDispatcher(material_repository, publish_fn=publish)
    with pytest.raises(TradeCommandMaterialUnavailableError):
        await outbox._dispatch_batch()
    publish.assert_not_awaited()
    retry.assert_not_awaited()
    async with material_repository.session() as session:
        versions = (
            await session.scalars(
                select(TradeCommand)
                .where(TradeCommand.public_id == command_id)
                .order_by(TradeCommand.id)
            )
        ).all()
        assert len(versions) == 2
        assert [row.post_only for row in versions] == [True, False]
        assert await session.scalar(select(func.count()).select_from(VenueEvent)) == 0


async def test_missing_history_is_explicit_unavailable_instead_of_absence(
    material_repository: SQLAlchemyRepository,
) -> None:
    """Missing proof never returns a row that a caller could mistake for CREATE authority.

    Given: No stored versions exist for the requested logical UUID,
    When: Its material is required,
    Then: The dedicated unavailable exception retains that UUID.
    """
    command_id = "00000000-0000-7000-8000-000000000999"
    with pytest.raises(TradeCommandMaterialUnavailableError) as refused:
        await material_repository.require_consistent_trade_command_material(command_id)
    assert refused.value.command_public_id == command_id


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("quantity", 2.0),
        ("price", 101.0),
        ("stop_price", 90.0),
        ("leverage", 3),
        ("reduce_only", True),
        ("idempotency_key", "original-key"),
        ("source_surface", "mcp"),
        ("wallet_public_id", "00000000-0000-7000-8000-000000000212"),
        ("exchange", "walutomat"),
        ("mode", "paper"),
        ("created_at", datetime(2020, 1, 1, tzinfo=UTC)),
    ],
)
async def test_non_lifecycle_request_material_is_protected_without_a_catalog_lookup(
    material_repository: SQLAlchemyRepository, field: str, value: object
) -> None:
    """Request, scope and original intent-time fields are immutable independently of catalog state.

    Given: Historical rows contain another material change besides post_only,
    When: Their actual logical command UUID is checked,
    Then: The exact changed field is refused without deriving identity from today's catalog.
    """
    command_id = await seed_command_history(material_repository, "other-material", (False, False))
    async with material_repository.session() as session:
        original = (
            await session.scalars(
                select(TradeCommand)
                .where(TradeCommand.public_id == command_id)
                .order_by(TradeCommand.id)
            )
        ).first()
        assert original is not None
        setattr(original, field, value)
        await session.commit()
    with pytest.raises(TradeCommandMaterialChangedError) as refused:
        await material_repository.require_consistent_trade_command_material(command_id)
    assert refused.value.changed_fields == (field,)


async def test_existing_local_barrier_remains_authoritative_without_requiring_a_replay_watermark(
    material_repository: SQLAlchemyRepository,
) -> None:
    """A previously committed command barrier is independent of ordinary replay checkpoints.

    Given: An exact-scope local barrier already exists for an intent,
    When: Current material proof is requested even when retained versions now agree,
    Then: The durable barrier still refuses CREATE instead of silently clearing itself.
    """
    command_id = await seed_command_history(material_repository, "retained-barrier", (False,))
    command = await active_command(material_repository, "retained-barrier")
    now = datetime.now(UTC)
    async with material_repository.session() as session:
        session.add(
            VenueEvent(
                event_type=COMMAND_MATERIAL_CONFLICT_EVENT,
                command_public_id=command_id,
                wallet_public_id=MATERIAL_WALLET,
                exchange="kraken",
                mode="live",
                instrument="BTC-USD",
                shard_key=command["shard_key"],
                received_at=now,
                timestamp=now,
                session_id="prior-barrier",
                sequence_id=1,
            )
        )
        await session.commit()
    with pytest.raises(TradeCommandMaterialChangedError) as refused:
        await material_repository.require_consistent_trade_command_material(command_id)
    assert refused.value.changed_fields == ()
    assert refused.value.reason == "durable dispatch barrier retained"
    assert await material_repository.get_undispatched_commands(now) == []


async def test_unsupported_repository_contract_fails_closed_without_inventing_permission(
    material_repository: SQLAlchemyRepository,
) -> None:
    """Storage implementations must explicitly provide lineage and full historical material proof.

    Given: A backend has not implemented the Repository execution-proof contract,
    When: Either default contract boundary is called,
    Then: A dedicated unavailable error prevents CREATE and retains identity only when known.
    """
    with pytest.raises(TradeCommandMaterialUnavailableError) as unresolved:
        await Repository.get_active_create_command_by_client_order_id(
            material_repository, "unsupported-storage", "kraken"
        )
    assert unresolved.value.command_public_id is None
    command_id = await seed_command_history(material_repository, "known-unsupported", (False,))
    with pytest.raises(TradeCommandMaterialUnavailableError) as known:
        await Repository.require_consistent_trade_command_material(material_repository, command_id)
    assert known.value.command_public_id == command_id
    assert known.value.reason == "repository_material_proof_unsupported"


async def test_real_retry_bulk_dispatch_and_ack_writers_preserve_material_permission(
    material_repository: SQLAlchemyRepository,
) -> None:
    """Legitimate lifecycle history remains usable across every current successor writer.

    Given: A maker-only command remains materially unchanged,
    When: Real retry, bulk-dispatch and accepted lifecycle writes create successive versions,
    Then: Its actual logical UUID retains permission and no local barrier is invented.
    """
    command_id = await seed_command_history(material_repository, "all-status-writers", (True,))
    now = datetime.now(UTC)
    await material_repository.update_trade_command_status(
        public_id=command_id,
        new_status="created",
        bus_time=now,
        session_id="retry-session",
        sequence_id=10,
        attempt_count=1,
        last_error="temporary publish failure",
    )
    assert (await material_repository.require_consistent_trade_command_material(command_id))[
        "post_only"
    ] is True
    assert (
        await material_repository.bulk_dispatch_trade_commands(
            [
                {
                    "public_id": command_id,
                    "bus_time": now,
                    "session_id": "dispatch-session",
                    "sequence_id": 11,
                    "attempt_count": 2,
                    "dispatched_at": now,
                }
            ]
        )
        == 1
    )
    assert (await material_repository.require_consistent_trade_command_material(command_id))[
        "status"
    ] == "dispatched"
    assert await material_repository.advance_trade_command_lifecycle(
        public_id=command_id,
        expected_status="dispatched",
        new_status="accepted",
        bus_time=now,
        session_id="ack-session",
        sequence_id=12,
        acked_at=now,
        exchange_order_id="actual-venue-id",
    )
    assert (await material_repository.require_consistent_trade_command_material(command_id))[
        "status"
    ] == "accepted"
    async with material_repository.session() as session:
        assert await session.scalar(select(func.count()).select_from(VenueEvent)) == 0


async def test_reverted_modifier_history_does_not_silently_clear_a_material_conflict(
    material_repository: SQLAlchemyRepository,
) -> None:
    """Agreement between first and active versions cannot hide an intermediate mutation.

    Given: One logical maker-only command changed to False and later back to True,
    When: Its entire retained history is required,
    Then: The intermediate material conflict still commits a durable CREATE barrier.
    """
    command_id = await seed_command_history(
        material_repository, "reverted-history", (True, False, True)
    )
    with pytest.raises(TradeCommandMaterialChangedError) as refused:
        await material_repository.require_consistent_trade_command_material(command_id)
    assert refused.value.changed_fields == ("post_only",)
    assert (await active_command(material_repository, "reverted-history"))["post_only"] is True
