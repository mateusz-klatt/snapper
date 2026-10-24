"""Capture current account/source admission alongside unchanged v1 intent evidence.

The producer compares known economic dimensions across the original and actual
current catalogs. It records every real revision, selected account route and
source UUID. A retained witness never grants permission to resend. Product,
account, capability, sizing, financial reservations and live interlocks remain
independent requirements before venue I/O.
"""

from datetime import UTC
from datetime import datetime

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from snapper.application.trade.command_request import parse_shard_key
from snapper.core.command_material import TradeCommandMaterialUnavailableError
from snapper.core.execution_route_authority import ExecutionEvidenceIdentity
from snapper.core.execution_unit_admission import ExecutionRouteReference
from snapper.core.execution_unit_admission import SubmissionEvidenceIdentity
from snapper.core.execution_unit_admission import SubmissionUnitAdmissionPayload
from snapper.core.execution_unit_snapshot import ExecutionUnitSnapshotPayload
from snapper.core.execution_unit_snapshot import SnapshotCommandMaterial
from snapper.core.execution_unit_snapshot import SnapshotReceipt
from snapper.core.execution_unit_snapshot import SnapshotScope
from snapper.core.execution_unit_snapshot import SnapshotVenueIdentity
from snapper.core.execution_unit_snapshot import SnapshotWriteProvenance
from snapper.core.execution_unit_snapshot import SubmissionSnapshotTarget
from snapper.core.execution_unit_snapshot import command_request_digest
from snapper.data.execution_route_authority_store import require_current_route_authority
from snapper.data.execution_unit_admission_store import OriginalSubmissionSnapshot
from snapper.data.execution_unit_admission_store import SubmissionCatalogObservations
from snapper.data.execution_unit_admission_store import find_bound_submission_admission_attempt
from snapper.data.execution_unit_admission_store import insert_bound_submission_admission
from snapper.data.execution_unit_admission_store import load_original_submission_snapshot
from snapper.data.execution_unit_admission_store import observe_submission_catalogs
from snapper.data.execution_unit_admission_store import require_submission_command_history
from snapper.data.execution_unit_catalog import catalog_decimal_mirror
from snapper.data.execution_unit_catalog import catalog_rebase_context
from snapper.data.execution_unit_catalog import catalog_submission_dimensions
from snapper.data.execution_unit_catalog import snapshot_catalog
from snapper.data.execution_unit_catalog import source_catalog_observation
from snapper.data.execution_unit_snapshot_store import SnapshotBindingError
from snapper.data.execution_unit_snapshot_store import SnapshotRepository
from snapper.data.execution_unit_snapshot_store import SnapshotWriteResult
from snapper.data.execution_unit_snapshot_store import _insert_bound_snapshot
from snapper.data.execution_unit_snapshot_store import durable_command_material
from snapper.data.models import TradeCommand
from snapper.data.models import is_open_ended_version
from snapper.data.temporal_selection import one_or_none
from snapper.messaging.schemas.data import OrderRequestData


class SubmissionUnitSnapshotUnavailableError(TradeCommandMaterialUnavailableError):
    """Hold CREATE when actual current account/source/unit interpretation lacks proof."""

    error_code = "submission_unit_snapshot_unavailable"


def _require(condition: bool, reason: str) -> None:
    """Refuse incomplete evidence without guessing an intent or financial convention."""
    if not condition:
        raise SnapshotBindingError(reason)


def _frame_material(command: TradeCommand, order: OrderRequestData) -> SnapshotCommandMaterial:
    """Bind outgoing frame operands to the selected durable command identity."""
    return SnapshotCommandMaterial.model_validate(
        {
            "command_public_id": command.public_id,
            "wallet_public_id": order.wallet_public_id,
            "exchange": order.exchange,
            "mode": order.mode,
            "native_symbol": order.instrument,
            "client_order_id": order.client_order_id,
            "side": order.side,
            "order_type": order.order_type,
            "quantity": catalog_decimal_mirror(order.quantity),
            "price": catalog_decimal_mirror(order.price),
            "stop_price": catalog_decimal_mirror(order.stop_price),
            "leverage": order.leverage,
            "post_only": order.post_only,
            "reduce_only": order.reduce_only,
        }
    )


def _verify_frame_authorship(command: TradeCommand, order: OrderRequestData) -> None:
    """Recheck origin and actor context inside the locked admission transaction."""
    _require(
        (
            command.strategy_id,
            command.operator_public_id,
            command.user_public_id,
            command.origin,
            command.replay_window_start,
            command.replay_window_end,
        )
        == (
            order.strategy_id,
            order.operator_public_id,
            order.user_public_id,
            order.origin,
            order.replay_window_start,
            order.replay_window_end,
        ),
        "submission_queued_authorship_mismatch",
    )
    if command.mode == "paper":
        shard = parse_shard_key(command.shard_key)
        expected_tag = shard[4] if shard is not None else None
        _require(order.strategy_tag == expected_tag, "submission_queued_paper_scope_mismatch")


async def _current_command(
    session: AsyncSession,
    order: OrderRequestData,
    route: ExecutionRouteReference,
    interpreted_at: datetime,
) -> TradeCommand:
    """Lock current CREATE lineage and compare actual executor scope and frame."""
    candidates = (
        await session.execute(
            select(TradeCommand, TradeCommand.known_to)
            .where(
                TradeCommand.client_order_id == order.client_order_id,
                TradeCommand.exchange == route.scope.exchange,
                TradeCommand.command_type.in_(("create", "submit")),
                TradeCommand.known_to > interpreted_at,
                TradeCommand.timestamp <= interpreted_at,
            )
            .with_for_update()
        )
    ).all()
    command = one_or_none(
        [candidate for candidate, known_to in candidates if is_open_ended_version(known_to)]
    )
    if command is None:
        raise SnapshotBindingError("submission_current_command_missing")
    _require(
        command.status in {"created", "dispatched", "direct_dispatched"},
        "submission_command_not_placeable",
    )
    _require(command.origin == "live", "submission_replay_origin_not_placeable")
    _require(
        (command.wallet_public_id, command.mode)
        == (route.scope.wallet_public_id, route.scope.mode),
        "submission_actual_execution_scope_mismatch",
    )
    _require(
        durable_command_material(command) == _frame_material(command, order),
        "submission_queued_terms_mismatch",
    )
    _verify_frame_authorship(command, order)
    return command


def _provenance(
    identity: ExecutionEvidenceIdentity, interpreted_at: datetime
) -> SnapshotWriteProvenance:
    """Stamp independent record identities with the actual fenced knowledge clock."""
    return SnapshotWriteProvenance(
        public_id=identity.public_id,
        session_id=identity.session_id,
        sequence_id=identity.sequence_id,
        timestamp=interpreted_at,
    )


def _original_payload(
    command: TradeCommand,
    observations: SubmissionCatalogObservations,
    interpreted_at: datetime,
) -> ExecutionUnitSnapshotPayload:
    """Capture historical v1 intent units accompanied by a mandatory current v2 witness."""
    material = durable_command_material(command)
    original = observations.original
    return ExecutionUnitSnapshotPayload(
        target=SubmissionSnapshotTarget(
            command_public_id=command.public_id,
            client_order_id=command.client_order_id,
            request_digest=command_request_digest(material),
        ),
        scope=SnapshotScope.model_validate(
            {
                "wallet_public_id": command.wallet_public_id,
                "exchange": command.exchange,
                "mode": command.mode,
                "instrument_public_id": original.instrument.public_id,
            }
        ),
        catalog=snapshot_catalog(original),
        dimensions=catalog_submission_dimensions(material, original),
        venue_identity=SnapshotVenueIdentity(
            native_symbol=original.symbol.native_symbol,
            source="snapper:catalog.symbols",
            source_version=f"scd2:{original.symbol.public_id}",
            observed_at=original.symbol.timestamp,
        ),
        rebase=catalog_rebase_context(original),
        receipt=SnapshotReceipt(
            representation="decoded_json_text",
            material=material.model_dump_json(),
            endpoint_or_channel="snapper:trade_commands",
            observed_at=observations.first_known,
        ),
        interpreted_at=interpreted_at,
    )


async def _capture_original(
    session: AsyncSession,
    command: TradeCommand,
    observations: SubmissionCatalogObservations,
    provenance: SnapshotWriteProvenance,
) -> OriginalSubmissionSnapshot:
    """Preserve a valid baseline or atomically insert its real original observation."""
    existing = await load_original_submission_snapshot(session, command)
    if existing is not None:
        return existing
    payload = _original_payload(command, observations, provenance.timestamp)
    result = await _insert_bound_snapshot(session, payload, provenance)
    return OriginalSubmissionSnapshot(result, payload, observations.first_known)


def _admission_payload(
    route: ExecutionRouteReference,
    identity: SubmissionEvidenceIdentity,
    baseline: OriginalSubmissionSnapshot,
    command: TradeCommand,
    observations: SubmissionCatalogObservations,
) -> SubmissionUnitAdmissionPayload:
    """Record actual current catalog/source revisions and link the unchanged original."""
    current = observations.current
    return SubmissionUnitAdmissionPayload(
        attempt_public_id=identity.attempt_public_id,
        route=route,
        scope=baseline.payload.scope,
        command_snapshot_public_id=baseline.result.public_id,
        command_snapshot_content_digest=baseline.result.content_digest,
        request_material=durable_command_material(command),
        current_catalog=snapshot_catalog(current),
        current_instrument_timestamp=current.instrument.timestamp,
        current_symbol_public_id=current.symbol.public_id,
        current_symbol_timestamp=current.symbol.timestamp,
        command_source=source_catalog_observation(observations.original_source),
        current_source=source_catalog_observation(observations.current_source),
        economic_context=observations.economic_context,
        interpreted_at=observations.interpreted_at,
    )


async def _retained_attempt(
    session: AsyncSession, current: SubmissionUnitAdmissionPayload
) -> SubmissionUnitAdmissionPayload:
    """Reuse original clocks only after fresh proof establishes the same actual context."""
    retained = await find_bound_submission_admission_attempt(session, current.attempt_public_id)
    if retained is None:
        return current
    comparison = retained.model_copy(update={"interpreted_at": current.interpreted_at})
    _require(comparison == current, "submission_retained_attempt_context_changed")
    return retained


async def capture_bound_submission_unit_admission(
    session: AsyncSession,
    order: OrderRequestData,
    route: ExecutionRouteReference,
    identity: SubmissionEvidenceIdentity,
) -> SnapshotWriteResult:
    """Capture v1 baseline and actual v2 admission inside the financial transaction.

    The caller already holds its wallet execution fence and may atomically add
    reservations and a durable venue-submit outbox before committing. A retained
    witness never authorizes resend. This function performs no venue I/O.

    Args:
        session: Caller-owned execution transaction with its account fence held.
        order: Actual frame that will be translated into the venue request.
        route: Actual account/environment and immutable selected-route authority.
        identity: Distinct provenance and the actual submit-attempt UUID.

    Returns:
        Immutable v2 witness identity without committing the caller transaction.

    Raises:
        SubmissionUnitSnapshotUnavailableError: If current evidence fails proof.
    """
    command_id: str | None = None
    try:
        actual = ExecutionRouteReference.model_validate(route)
        stamp = SubmissionEvidenceIdentity.model_validate(identity)
        interpreted_at = datetime.now(UTC)
        await require_current_route_authority(session, actual, interpreted_at)
        command = await _current_command(session, order, actual, interpreted_at)
        command_id = command.public_id
        first_known = await require_submission_command_history(session, command)
        observations = await observe_submission_catalogs(
            session, command, first_known, interpreted_at
        )
        baseline = await _capture_original(
            session, command, observations, _provenance(stamp.command_snapshot, interpreted_at)
        )
        payload = await _retained_attempt(
            session, _admission_payload(actual, stamp, baseline, command, observations)
        )
        return await insert_bound_submission_admission(
            session, payload, _provenance(stamp.admission, interpreted_at)
        )
    except Exception as error:
        raise SubmissionUnitSnapshotUnavailableError(
            command_id, "current_submission_account_source_unit_evidence_unavailable"
        ) from error


async def capture_submission_unit_admission(
    repository: SnapshotRepository,
    order: OrderRequestData,
    route: ExecutionRouteReference,
    identity: SubmissionEvidenceIdentity,
) -> SnapshotWriteResult:
    """Commit an admission when no broader financial transaction is being authored.

    Args:
        repository: Real transaction and wallet-fence provider.
        order: Outgoing frame retaining durable intent operands and actor scope.
        route: Actual account and production/demo/Paper environment authority.
        identity: Original/current record provenance and actual attempt identity.

    Returns:
        Committed v2 witness independent of permission to place or resend.
    """
    actual = ExecutionRouteReference.model_validate(route)
    async with repository.session() as session:
        await repository._begin_execution_insert_transaction(session)
        await repository._acquire_execution_fence_lock(session, actual.scope.wallet_public_id)
        result = await capture_bound_submission_unit_admission(session, order, actual, identity)
        await session.commit()
        return result
