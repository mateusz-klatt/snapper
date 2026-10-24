"""Rebind original native command admission at historical clocks without requiring current catalogs."""

from datetime import datetime

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from snapper.core.native_account_authority import NativeAccountAuthorityPayload
from snapper.core.native_fill_unit_admission import NativeUnitRecordReference
from snapper.data.account_bound_submission_admission_store import BoundAccountSubmissionAdmission
from snapper.data.account_bound_submission_admission_store import (
    get_bound_account_submission_admission,
)
from snapper.data.execution_unit_admission_store import SubmissionCatalogObservations
from snapper.data.execution_unit_admission_store import _verify_current_catalog_admission
from snapper.data.execution_unit_admission_store import _verify_current_observations
from snapper.data.execution_unit_admission_store import load_original_submission_snapshot
from snapper.data.execution_unit_catalog import UnitCatalogLookup
from snapper.data.execution_unit_catalog import catalog_economic_context
from snapper.data.execution_unit_catalog import load_unit_catalog
from snapper.data.execution_unit_catalog import load_unit_source_catalog
from snapper.data.execution_unit_catalog import snapshot_catalog
from snapper.data.execution_unit_catalog import source_catalog_observation
from snapper.data.execution_unit_snapshot_history import get_bound_execution_unit_snapshot
from snapper.data.execution_unit_snapshot_store import SnapshotBindingError
from snapper.data.execution_unit_snapshot_store import durable_command_material
from snapper.data.models import Execution
from snapper.data.models import ExecutionUnitSnapshot
from snapper.data.models import TradeCommand
from snapper.data.models import VenueEvent
from snapper.data.native_position_fill_binding_v2 import BoundOriginalPositionFillV2


def _require(condition: bool, reason: str) -> None:
    """Refuse a broken historical relation without replacing its original admission or source."""
    if not condition:
        raise SnapshotBindingError(reason)


async def _original_command(
    session: AsyncSession, event: VenueEvent, interpreted_at: datetime
) -> TradeCommand:
    """Load the actual event's command revision as known at its explicit original admission."""
    command = (
        await session.execute(
            select(TradeCommand).where(
                TradeCommand.public_id == event.command_public_id,
                TradeCommand.timestamp <= interpreted_at,
                TradeCommand.known_to > interpreted_at,
            )
        )
    ).scalar_one_or_none()
    if command is None:
        raise SnapshotBindingError("native_fill_original_admitted_command_missing")
    _require(
        command.command_type in {"create", "submit"},
        "native_fill_original_command_kind_unproven",
    )
    return command


def _verify_admission_record(
    bound: BoundAccountSubmissionAdmission,
    reference: NativeUnitRecordReference,
    execution: Execution,
) -> None:
    """Bind the named immutable record and its actual local knowledge before this execution."""
    _require(
        (bound.public_id, bound.content_digest, bound.recorded_at)
        == (reference.public_id, reference.content_digest, reference.recorded_at),
        "native_fill_original_admission_reference_mismatch",
    )
    _require(
        bound.payload.units.interpreted_at <= bound.recorded_at <= execution.timestamp,
        "native_fill_original_admission_not_known_at_execution",
    )


def _verify_original_account(
    source: BoundOriginalPositionFillV2, admission: BoundAccountSubmissionAdmission
) -> None:
    """Require the actual same native account and environment across independently proved keys."""
    original = admission.authority
    current = source.authority
    _require(
        original.family == current.family == "native_accounts_v1",
        "native_fill_admission_native_family_required",
    )
    left = NativeAccountAuthorityPayload.model_validate(original.payload)
    right = NativeAccountAuthorityPayload.model_validate(current.payload)
    _require(
        left.scope == right.scope
        and (left.native_profile.account_type, left.native_profile.settlement_currency)
        == (right.native_profile.account_type, right.native_profile.settlement_currency)
        and left.venue_account_id == right.venue_account_id == source.ownership.account_uid
        and left.native_read.parser_policy == "kraken_futures_accounts_prepared_key_v2"
        and right.native_read.parser_policy == "kraken_futures_accounts_prepared_key_v2",
        "native_fill_admission_actual_account_mismatch",
    )


async def _historical_catalogs(
    session: AsyncSession,
    command: TradeCommand,
    first_known: datetime,
    interpreted_at: datetime,
) -> SubmissionCatalogObservations:
    """Resolve original and admission-time definitions even when their physical rows later closed."""
    lookup = UnitCatalogLookup(exchange=command.exchange, native_symbol=command.instrument)
    original = await load_unit_catalog(session, lookup, first_known)
    admitted = await load_unit_catalog(session, lookup, interpreted_at)
    original_source = await load_unit_source_catalog(session, original, first_known)
    admitted_source = await load_unit_source_catalog(session, admitted, interpreted_at)
    first_source = source_catalog_observation(original_source)
    later_source = source_catalog_observation(admitted_source)
    _require(
        original.instrument.public_id == admitted.instrument.public_id
        and (
            first_source.exchange,
            first_source.instrument_public_id,
            first_source.symbol_public_id,
        )
        == (
            later_source.exchange,
            later_source.instrument_public_id,
            later_source.symbol_public_id,
        ),
        "native_fill_admission_original_catalog_lineage_changed",
    )
    context = catalog_economic_context(original)
    _require(
        all(
            catalog_economic_context(value) == context
            for value in (admitted, original_source, admitted_source)
        ),
        "native_fill_admission_historical_economic_units_changed",
    )
    return SubmissionCatalogObservations(
        original, admitted, original_source, admitted_source, context, first_known, interpreted_at
    )


async def _bind_original_units(
    session: AsyncSession,
    source: BoundOriginalPositionFillV2,
    admission: BoundAccountSubmissionAdmission,
    command: TradeCommand,
) -> None:
    """Rebind original unit identity, complete historical catalog graph and exact durable request."""
    units = admission.payload.units
    _require(
        durable_command_material(command) == units.request_material,
        "native_fill_admission_original_command_material_changed",
    )
    baseline = await load_original_submission_snapshot(session, command)
    if baseline is None:
        raise SnapshotBindingError("native_fill_admission_original_units_missing")
    verified = await get_bound_execution_unit_snapshot(session, baseline.result.public_id)
    _require(
        verified == baseline.payload,
        "native_fill_admission_original_unit_history_changed",
    )
    _require(
        (baseline.result.public_id, baseline.result.content_digest, baseline.payload.scope)
        == (units.command_snapshot_public_id, units.command_snapshot_content_digest, units.scope),
        "native_fill_admission_original_unit_reference_changed",
    )
    recorded_at = (
        await session.execute(
            select(ExecutionUnitSnapshot.timestamp).where(
                ExecutionUnitSnapshot.public_id == baseline.result.public_id
            )
        )
    ).scalar_one()
    _require(
        baseline.first_known <= units.interpreted_at and recorded_at <= units.interpreted_at,
        "native_fill_admission_original_unit_knowledge_invalid",
    )
    catalogs = await _historical_catalogs(
        session, command, baseline.first_known, units.interpreted_at
    )
    _verify_current_observations(units, catalogs)
    for observation in (catalogs.current, catalogs.current_source):
        _verify_current_catalog_admission(units.request_material, observation, units.interpreted_at)
    _require(
        snapshot_catalog(catalogs.original).observed_at <= baseline.first_known
        and snapshot_catalog(catalogs.original_source).observed_at <= baseline.first_known,
        "native_fill_admission_original_catalog_knowledge_invalid",
    )
    _require(
        units.scope.instrument_public_id == source.catalog.instrument.public_id
        and units.economic_context == catalog_economic_context(source.catalog),
        "native_fill_original_admitted_economic_units_changed",
    )


async def bind_native_fill_admission(
    session: AsyncSession,
    source: BoundOriginalPositionFillV2,
    reference: NativeUnitRecordReference,
    execution: Execution,
    event: VenueEvent,
) -> BoundAccountSubmissionAdmission:
    """Bind exact historical account/unit admission without claiming its physical dispatch caused a fill.

    Args:
        session: Actual caller-owned source interpretation or financial transaction.
        source: Independently rebound original authenticated fill and selected historical authority.
        reference: Exact retained version-three admission identity, digest and original clock.
        execution: Actual unit target loaded from immutable source-backed execution lineage.
        event: Actual source event whose original command must match the admitted request.

    Returns:
        The original fully rebound admission without current order, funding or financial-owner permission.
    """
    actual = NativeUnitRecordReference.model_validate(reference)
    admission = await get_bound_account_submission_admission(session, actual.public_id)
    _verify_admission_record(admission, actual, execution)
    _verify_original_account(source, admission)
    command = await _original_command(session, event, admission.payload.units.interpreted_at)
    await _bind_original_units(session, source, admission, command)
    return admission
