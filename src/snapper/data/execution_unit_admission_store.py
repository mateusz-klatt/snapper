"""Fenced version-two admission witnesses preserving the original v1 unit evidence."""

from dataclasses import dataclass
from datetime import datetime
from datetime import timedelta
from fractions import Fraction

from sqlalchemy import or_
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from snapper.core.command_material import COMMAND_MATERIAL_CONFLICT_EVENT
from snapper.core.command_material import TRADE_COMMAND_LIFECYCLE_FIELDS
from snapper.core.execution_route_authority import ExecutionRouteAuthorityPayload
from snapper.core.execution_route_authority import account_scope_digest
from snapper.core.execution_route_authority import client_selection_from_authority
from snapper.core.execution_route_authority import route_authority_content_digest
from snapper.core.execution_unit_admission import EconomicUnitContext
from snapper.core.execution_unit_admission import SubmissionUnitAdmissionPayload
from snapper.core.execution_unit_admission import canonical_unit_admission_payload
from snapper.core.execution_unit_admission import economic_context_digest
from snapper.core.execution_unit_admission import economic_unit_context
from snapper.core.execution_unit_admission import unit_admission_content_digest
from snapper.core.execution_unit_snapshot import ExecutionUnitSnapshotPayload
from snapper.core.execution_unit_snapshot import SnapshotCommandMaterial
from snapper.core.execution_unit_snapshot import SnapshotWriteProvenance
from snapper.core.execution_unit_snapshot import SubmissionSnapshotTarget
from snapper.core.execution_unit_snapshot import canonical_snapshot_payload
from snapper.core.execution_unit_snapshot import command_request_digest
from snapper.core.execution_unit_snapshot import snapshot_content_digest
from snapper.data.execution_route_authority_store import get_bound_route_authority
from snapper.data.execution_route_authority_store import require_current_route_authority
from snapper.data.execution_unit_catalog import UnitCatalogLookup
from snapper.data.execution_unit_catalog import UnitCatalogObservation
from snapper.data.execution_unit_catalog import catalog_decimal_mirror
from snapper.data.execution_unit_catalog import catalog_economic_context
from snapper.data.execution_unit_catalog import load_unit_catalog
from snapper.data.execution_unit_catalog import load_unit_source_catalog
from snapper.data.execution_unit_catalog import snapshot_catalog
from snapper.data.execution_unit_catalog import source_catalog_observation
from snapper.data.execution_unit_snapshot_store import SnapshotBindingError
from snapper.data.execution_unit_snapshot_store import SnapshotRepository
from snapper.data.execution_unit_snapshot_store import SnapshotWriteResult
from snapper.data.execution_unit_snapshot_store import _bind_submission
from snapper.data.execution_unit_snapshot_store import _submission_knowledge_time
from snapper.data.execution_unit_snapshot_store import _verify_catalog
from snapper.data.execution_unit_snapshot_store import durable_command_material
from snapper.data.models import ExecutionUnitSnapshot
from snapper.data.models import SubmissionUnitAdmission
from snapper.data.models import TradeCommand
from snapper.data.models import VenueEvent
from snapper.data.models import is_open_ended_version
from snapper.data.temporal_selection import one_or_none
from snapper.infrastructure.exchanges.futures_spec_provenance import FUTURES_SPEC_ETL_VERSION
from snapper.infrastructure.exchanges.futures_spec_provenance import FUTURES_SPEC_SOURCE


class UnitAdmissionConflictError(ValueError):
    """One immutable admission identity or actual attempt already has different evidence."""


@dataclass(frozen=True)
class OriginalSubmissionSnapshot:
    """A real immutable v1 record and its proven command knowledge horizon."""

    result: SnapshotWriteResult
    payload: ExecutionUnitSnapshotPayload
    first_known: datetime


@dataclass(frozen=True)
class SubmissionCatalogObservations:
    """Actual original/current target and source definitions with equal known economics."""

    original: UnitCatalogObservation
    current: UnitCatalogObservation
    original_source: UnitCatalogObservation
    current_source: UnitCatalogObservation
    economic_context: EconomicUnitContext
    first_known: datetime
    interpreted_at: datetime


def _require(condition: bool, reason: str) -> None:
    """Refuse an incomplete proof graph without rewriting any retained observation."""
    if not condition:
        raise SnapshotBindingError(reason)


async def require_submission_command_history(
    session: AsyncSession, command: TradeCommand
) -> datetime:
    """Verify every immutable command column and its retained dispatch barrier in one transaction.

    Args:
        session: Existing caller-owned wallet-fenced transaction.
        command: Actual current create/submit row already locked for admission.

    Returns:
        The original durable command knowledge clock after complete material proof.
    """
    versions = (
        await session.scalars(
            select(TradeCommand).where(TradeCommand.public_id == command.public_id)
        )
    ).all()
    _require(bool(versions), "admission_command_history_missing")
    unchanged = all(
        getattr(version, column.key) == getattr(command, column.key)
        for version in versions
        for column in TradeCommand.__table__.columns
        if column.key not in TRADE_COMMAND_LIFECYCLE_FIELDS
    )
    _require(unchanged, "admission_command_material_changed")
    barrier = await session.scalar(
        select(VenueEvent.id)
        .where(
            VenueEvent.event_type == COMMAND_MATERIAL_CONFLICT_EVENT,
            VenueEvent.command_public_id == command.public_id,
            VenueEvent.wallet_public_id == command.wallet_public_id,
            VenueEvent.exchange == command.exchange,
            VenueEvent.mode == command.mode,
        )
        .limit(1)
    )
    _require(barrier is None, "admission_command_material_barrier")
    return await _submission_knowledge_time(session, durable_command_material(command))


async def _current_command(
    session: AsyncSession, request: SnapshotCommandMaterial, as_of: datetime
) -> TradeCommand:
    """Refuse terminal/current-material changes before validating an existing attempt."""
    candidates = (
        await session.execute(
            select(TradeCommand, TradeCommand.known_to)
            .where(
                TradeCommand.public_id == request.command_public_id,
                TradeCommand.known_to > as_of,
                TradeCommand.timestamp <= as_of,
            )
            .with_for_update()
        )
    ).all()
    command = one_or_none(
        [candidate for candidate, known_to in candidates if is_open_ended_version(known_to)]
    )
    if command is None:
        raise SnapshotBindingError("admission_current_command_missing")
    _require(command.command_type in {"create", "submit"}, "admission_command_not_create")
    _require(
        command.status in {"created", "dispatched", "direct_dispatched"},
        "admission_command_not_placeable",
    )
    _require(durable_command_material(command) == request, "admission_current_terms_changed")
    return command


def _verify_snapshot_projection(
    row: ExecutionUnitSnapshot, payload: ExecutionUnitSnapshotPayload
) -> SubmissionSnapshotTarget:
    """Bind every indexed original-snapshot copy to its unchanged v1 canonical payload."""
    target = payload.target
    if not isinstance(target, SubmissionSnapshotTarget):
        raise SnapshotBindingError("admission_original_snapshot_not_submission")
    _require(
        (
            row.phase,
            row.wallet_public_id,
            row.exchange,
            row.mode,
            row.instrument_public_id,
            row.command_public_id,
            row.execution_public_id,
            row.spec_public_id,
            row.spec_timestamp,
            row.spec_source,
            row.spec_version,
            row.content_digest,
            row.canonical_payload,
        )
        == (
            "submission",
            payload.scope.wallet_public_id,
            payload.scope.exchange,
            payload.scope.mode,
            payload.scope.instrument_public_id,
            target.command_public_id,
            None,
            payload.catalog.spec_public_id,
            payload.catalog.spec_timestamp,
            payload.catalog.source,
            payload.catalog.version,
            snapshot_content_digest(payload),
            canonical_snapshot_payload(payload),
        ),
        "admission_original_snapshot_projection_mismatch",
    )
    return target


async def load_original_submission_snapshot(
    session: AsyncSession, command: TradeCommand
) -> OriginalSubmissionSnapshot | None:
    """Load a real unchanged v1 observation without treating it as permission to resend.

    Args:
        session: Existing caller-owned wallet-fenced transaction.
        command: Actual command whose material and current status were independently checked.

    Returns:
        Bound original evidence, or absence when the producer must capture the real original catalog.
    """
    row = (
        await session.execute(
            select(ExecutionUnitSnapshot).where(
                ExecutionUnitSnapshot.command_public_id == command.public_id
            )
        )
    ).scalar_one_or_none()
    if row is None:
        return None
    payload = ExecutionUnitSnapshotPayload.model_validate_json(row.canonical_payload)
    target = _verify_snapshot_projection(row, payload)
    first_known = await _bind_submission(session, payload, target)
    await _verify_catalog(session, payload, first_known)
    _require(
        target.request_digest == command_request_digest(durable_command_material(command)),
        "admission_original_request_digest_mismatch",
    )
    return OriginalSubmissionSnapshot(
        SnapshotWriteResult(row.id, row.public_id, row.content_digest, False), payload, first_known
    )


async def observe_submission_catalogs(
    session: AsyncSession,
    command: TradeCommand,
    first_known: datetime,
    interpreted_at: datetime,
) -> SubmissionCatalogObservations:
    """Compare actual historical/current source economics while preserving every real revision.

    Args:
        session: Caller-owned wallet-fenced transaction.
        command: Actual durable request selected for this attempt.
        first_known: Original durable intent knowledge horizon.
        interpreted_at: Actual current interpretation horizon.

    Returns:
        Original/current real target/source observations with complete equal economic dimensions.
    """
    lookup = UnitCatalogLookup(exchange=command.exchange, native_symbol=command.instrument)
    original = await load_unit_catalog(session, lookup, first_known)
    current = await load_unit_catalog(session, lookup, interpreted_at, current=True)
    original_source = await load_unit_source_catalog(session, original, first_known)
    current_source = await load_unit_source_catalog(session, current, interpreted_at, current=True)
    _require(
        original.instrument.public_id == current.instrument.public_id,
        "admission_target_instrument_changed",
    )
    original_identity = source_catalog_observation(original_source)
    current_identity = source_catalog_observation(current_source)
    _require(
        (
            original_identity.exchange,
            original_identity.instrument_public_id,
            original_identity.symbol_public_id,
        )
        == (
            current_identity.exchange,
            current_identity.instrument_public_id,
            current_identity.symbol_public_id,
        ),
        "admission_source_instrument_changed",
    )
    context = catalog_economic_context(original)
    _require(
        all(
            catalog_economic_context(observation) == context
            for observation in (current, original_source, current_source)
        ),
        "admission_economic_context_changed",
    )
    material = durable_command_material(command)
    for observation in (current, current_source):
        _verify_current_catalog_admission(material, observation, interpreted_at)
    _require(
        snapshot_catalog(original).observed_at <= first_known
        and original_identity.catalog.observed_at <= first_known,
        "admission_original_catalog_not_observed_at_intent",
    )
    return SubmissionCatalogObservations(
        original, current, original_source, current_source, context, first_known, interpreted_at
    )


def _verify_current_catalog_admission(
    material: SnapshotCommandMaterial,
    observation: UnitCatalogObservation,
    interpreted_at: datetime,
) -> None:
    """Recheck current active status, native limits and supported derivative freshness."""
    spec = observation.spec
    _require(spec.status in {"active", "online"}, "admission_current_instrument_not_active")
    catalog = snapshot_catalog(observation)
    _require(catalog.observed_at <= interpreted_at, "admission_current_catalog_from_future")
    if spec.spec_source == FUTURES_SPEC_SOURCE:
        _require(
            (spec.spec_version or "").startswith(f"{FUTURES_SPEC_ETL_VERSION}:"),
            "admission_current_derivative_status_policy_unproven",
        )
        _require(
            interpreted_at - catalog.observed_at <= timedelta(hours=12),
            "admission_current_derivative_catalog_stale",
        )
        _require(
            spec.instrument_kind in {"future", "perpetual"},
            "admission_derivative_kind_unproven",
        )
        _require(
            (spec.expiry_at is None and spec.instrument_kind == "perpetual")
            or (spec.expiry_at is not None and spec.expiry_at > interpreted_at),
            "admission_derivative_expiry_unproven",
        )
    _verify_current_native_limits(material, observation)


def _verify_current_native_limits(
    material: SnapshotCommandMaterial, observation: UnitCatalogObservation
) -> None:
    """Keep refreshed execution limits independent from equality of economic unit dimensions."""
    spec = observation.spec
    minimum = catalog_decimal_mirror(spec.min_order_size)
    maximum = catalog_decimal_mirror(spec.max_order_size)
    _require(
        minimum is None or (minimum >= 0 and material.quantity >= minimum),
        "admission_current_minimum_quantity",
    )
    _require(
        maximum is None or (maximum > 0 and material.quantity <= maximum),
        "admission_current_maximum_quantity",
    )
    lot = catalog_decimal_mirror(spec.lot_size)
    tick = catalog_decimal_mirror(spec.tick_size)
    _require(
        material.mode == "paper" or lot is not None,
        "admission_live_quantity_quantum_unknown",
    )
    if lot is not None:
        _require(
            lot > 0 and (Fraction(material.quantity) / Fraction(lot)).denominator == 1,
            "admission_current_quantity_quantum",
        )
    prices = tuple(value for value in (material.price, material.stop_price) if value is not None)
    _require(all(price > 0 for price in prices), "admission_supported_price_domain")
    required = {
        "market": (),
        "limit": (material.price,),
        "stop": (material.stop_price,),
        "stop_limit": (material.price, material.stop_price),
    }[material.order_type]
    _require(all(value is not None for value in required), "admission_required_price_missing")
    _require(
        material.mode == "paper" or not prices or tick is not None,
        "admission_live_price_quantum_unknown",
    )
    if tick is not None:
        _require(
            tick > 0
            and all((Fraction(price) / Fraction(tick)).denominator == 1 for price in prices),
            "admission_current_price_quantum",
        )


def _verify_current_observations(
    payload: SubmissionUnitAdmissionPayload, observations: SubmissionCatalogObservations
) -> None:
    """Match the v2 payload to actual selected source and current catalog row versions."""
    current = observations.current
    _require(
        (
            current.instrument.public_id,
            current.instrument.timestamp,
            current.symbol.public_id,
            current.symbol.timestamp,
            snapshot_catalog(current),
            source_catalog_observation(observations.original_source),
            source_catalog_observation(observations.current_source),
            observations.economic_context,
        )
        == (
            payload.scope.instrument_public_id,
            payload.current_instrument_timestamp,
            payload.current_symbol_public_id,
            payload.current_symbol_timestamp,
            payload.current_catalog,
            payload.command_source,
            payload.current_source,
            payload.economic_context,
        ),
        "admission_current_catalog_or_source_mismatch",
    )


async def _bind_admission(
    session: AsyncSession, payload: SubmissionUnitAdmissionPayload, as_of: datetime
) -> None:
    """Verify the whole actual account/request/catalog graph before a fresh or retried write."""
    authority = await require_current_route_authority(session, payload.route, as_of)
    command = await _current_command(session, payload.request_material, as_of)
    await _require_command_account_continuity(session, payload, authority)
    await bind_submission_unit_graph(session, payload, command, as_of)


async def bind_submission_unit_graph(
    session: AsyncSession,
    payload: SubmissionUnitAdmissionPayload,
    command: TradeCommand,
    as_of: datetime,
) -> None:
    """Bind original units and current catalogs after the caller proves account continuity.

    Args:
        session: Caller-owned wallet-fenced transaction.
        payload: The unchanged version-two-shaped economic graph.
        command: Actual current command already locked and matched to request material.
        as_of: Actual current interpretation horizon after the execution fence.
    """
    first_known = await require_submission_command_history(session, command)
    baseline = await load_original_submission_snapshot(session, command)
    if baseline is None:
        raise SnapshotBindingError("admission_original_snapshot_missing")
    _require(
        (
            baseline.result.public_id,
            baseline.result.content_digest,
            baseline.payload.scope,
        )
        == (
            payload.command_snapshot_public_id,
            payload.command_snapshot_content_digest,
            payload.scope,
        ),
        "admission_original_snapshot_reference_mismatch",
    )
    observations = await observe_submission_catalogs(session, command, first_known, as_of)
    original = observations.original
    baseline_context = economic_unit_context(
        baseline.payload.dimensions,
        baseline.payload.rebase,
        original.spec.instrument_kind,
        original.spec.expiry_at,
    )
    _require(
        baseline_context == observations.economic_context,
        "admission_original_unit_interpretation_changed",
    )
    _verify_current_observations(payload, observations)


async def _require_command_account_continuity(
    session: AsyncSession,
    payload: SubmissionUnitAdmissionPayload,
    authority: ExecutionRouteAuthorityPayload,
) -> None:
    """Keep every admitted command/CID on its proven original account and environment.

    An earlier immutable admission pins subsequent attempts even when its
    freshness has expired. A changed selected credential requires the same
    explicit venue account identifier; an unknown identifier cannot prove that
    rotation kept the external account. The coordinator must separately pin
    the intended route before its first possible venue dispatch.
    """
    request = payload.request_material
    public_ids = (
        await session.execute(
            select(SubmissionUnitAdmission.public_id).where(
                or_(
                    SubmissionUnitAdmission.command_public_id == request.command_public_id,
                    (SubmissionUnitAdmission.exchange == request.exchange)
                    & (SubmissionUnitAdmission.client_order_id == request.client_order_id),
                )
            )
        )
    ).scalars()
    for public_id in public_ids:
        previous = await get_bound_submission_admission(session, public_id)
        _require(previous.route.scope == payload.route.scope, "admission_command_account_changed")
        previous_authority = await get_bound_route_authority(
            session, previous.route.authority_public_id
        )
        known_account = (
            previous_authority.venue_account_id is not None
            and authority.venue_account_id is not None
        )
        same_account = previous_authority.venue_account_id == authority.venue_account_id
        _require(not known_account or same_account, "admission_venue_account_changed")
        _require(
            client_selection_from_authority(previous_authority)
            == client_selection_from_authority(authority)
            or (known_account and same_account),
            "admission_credential_rotation_account_unproven",
        )


def admission_row_projection(row: SubmissionUnitAdmission) -> tuple[object, ...]:
    """Read common indexed material without interpreting a canonical payload's source family.

    Args:
        row: Actual immutable admission row.

    Returns:
        All indexed unit/account reference values except the outer canonical bytes and digest.
    """
    return (
        row.attempt_public_id,
        row.wallet_public_id,
        row.exchange,
        row.mode,
        row.instrument_public_id,
        row.command_public_id,
        row.client_order_id,
        row.authority_public_id,
        row.authority_content_digest,
        row.account_scope_digest,
        row.command_snapshot_public_id,
        row.command_snapshot_content_digest,
        row.source_exchange,
        row.source_instrument_public_id,
        row.current_spec_public_id,
        row.current_spec_timestamp,
        row.source_spec_public_id,
        row.source_spec_timestamp,
        row.economic_context_digest,
        row.request_digest,
        row.interpreted_at,
    )


def admission_material_projection(
    payload: SubmissionUnitAdmissionPayload,
) -> tuple[object, ...]:
    """Project the unchanged economic graph independently of its enclosing admission version.

    Args:
        payload: Strict common unit graph with its actual route and source references.

    Returns:
        Expected indexed values in the same order as the retained row projection.
    """
    request = payload.request_material
    return (
        payload.attempt_public_id,
        payload.scope.wallet_public_id,
        payload.scope.exchange,
        payload.scope.mode,
        payload.scope.instrument_public_id,
        request.command_public_id,
        request.client_order_id,
        payload.route.authority_public_id,
        payload.route.authority_content_digest,
        account_scope_digest(payload.route.scope),
        payload.command_snapshot_public_id,
        payload.command_snapshot_content_digest,
        payload.current_source.exchange,
        payload.current_source.instrument_public_id,
        payload.current_catalog.spec_public_id,
        payload.current_catalog.spec_timestamp,
        payload.current_source.catalog.spec_public_id,
        payload.current_source.catalog.spec_timestamp,
        economic_context_digest(payload.economic_context),
        command_request_digest(request),
        payload.interpreted_at,
    )


def _verify_admission_projection(
    row: SubmissionUnitAdmission, payload: SubmissionUnitAdmissionPayload
) -> None:
    """Check every indexed current/scope copy against its unchanged version-two payload."""
    actual = admission_row_projection(row) + (row.content_digest, row.canonical_payload)
    expected = admission_material_projection(payload) + (
        unit_admission_content_digest(payload),
        canonical_unit_admission_payload(payload),
    )
    _require(actual == expected, "admission_projection_mismatch")


async def get_bound_submission_admission(
    session: AsyncSession, public_id: str
) -> SubmissionUnitAdmissionPayload:
    """Read historical immutable evidence without granting a new placement or fresh authority.

    Args:
        session: Caller-owned replay or raw-observation transaction.
        public_id: Exact immutable admission identity.

    Returns:
        Complete verified historical evidence, even after its authority becomes stale.
    """
    row = (
        await session.execute(
            select(SubmissionUnitAdmission).where(SubmissionUnitAdmission.public_id == public_id)
        )
    ).scalar_one_or_none()
    if row is None:
        raise SnapshotBindingError("admission_missing")
    payload = SubmissionUnitAdmissionPayload.model_validate_json(row.canonical_payload)
    _verify_admission_projection(row, payload)
    authority = await get_bound_route_authority(session, payload.route.authority_public_id)
    _require(
        (
            authority.scope,
            authority.route_revision,
            route_authority_content_digest(authority),
        )
        == (
            payload.route.scope,
            payload.route.route_revision,
            payload.route.authority_content_digest,
        ),
        "admission_historical_route_mismatch",
    )
    return payload


async def find_bound_submission_admission_attempt(
    session: AsyncSession, attempt_public_id: str
) -> SubmissionUnitAdmissionPayload | None:
    """Find immutable prior attempt evidence without authorizing another venue call.

    Args:
        session: Caller-owned transaction after independent current admission checks.
        attempt_public_id: Actual stable venue-submit attempt UUID.

    Returns:
        Complete historical evidence, or absence before the first durable capture.
    """
    public_id = (
        await session.execute(
            select(SubmissionUnitAdmission.public_id).where(
                SubmissionUnitAdmission.attempt_public_id == attempt_public_id
            )
        )
    ).scalar_one_or_none()
    if public_id is None:
        return None
    return await get_bound_submission_admission(session, public_id)


async def insert_bound_submission_admission(
    session: AsyncSession,
    payload: SubmissionUnitAdmissionPayload,
    provenance: SnapshotWriteProvenance,
) -> SnapshotWriteResult:
    """Insert a v2 witness inside the caller's existing transaction and wallet fence.

    Args:
        session: Existing caller-owned execution transaction with its wallet fence held.
        payload: Actual attempt, exact submitted terms and independently bound current evidence.
        provenance: Insertion identity and actual producer knowledge clock.

    Returns:
        Immutable admission identity, reusing only an exact still-current attempt.
    """
    proven = SubmissionUnitAdmissionPayload.model_validate(payload)
    stamp = SnapshotWriteProvenance.model_validate(provenance)
    _require(stamp.timestamp >= proven.interpreted_at, "admission_insert_precedes_interpretation")
    await _bind_admission(session, proven, stamp.timestamp)
    digest = unit_admission_content_digest(proven)
    existing = (
        await session.execute(
            select(SubmissionUnitAdmission).where(
                SubmissionUnitAdmission.attempt_public_id == proven.attempt_public_id
            )
        )
    ).scalar_one_or_none()
    if existing is not None:
        if existing.content_digest != digest:
            raise UnitAdmissionConflictError("admission_attempt_evidence_changed")
        _verify_admission_projection(existing, proven)
        return SnapshotWriteResult(existing.id, existing.public_id, digest, False)
    record = _admission_record(proven, stamp)
    session.add(record)
    await session.flush()
    return SnapshotWriteResult(record.id, record.public_id, digest, True)


def admission_record_values(
    payload: SubmissionUnitAdmissionPayload, provenance: SnapshotWriteProvenance
) -> dict[str, object]:
    """Build common SQLAlchemy constructor values without choosing a canonical payload version.

    Args:
        payload: Strict common economic graph with its actual account/source references.
        provenance: Independently validated insertion identity and local knowledge clock.

    Returns:
        The shared database projection, excluding outer canonical bytes and their digest.
    """
    return {
        **provenance.model_dump(mode="python"),
        "attempt_public_id": payload.attempt_public_id,
        "wallet_public_id": payload.scope.wallet_public_id,
        "exchange": payload.scope.exchange,
        "mode": payload.scope.mode,
        "instrument_public_id": payload.scope.instrument_public_id,
        "command_public_id": payload.request_material.command_public_id,
        "client_order_id": payload.request_material.client_order_id,
        "authority_public_id": payload.route.authority_public_id,
        "authority_content_digest": payload.route.authority_content_digest,
        "account_scope_digest": account_scope_digest(payload.route.scope),
        "command_snapshot_public_id": payload.command_snapshot_public_id,
        "command_snapshot_content_digest": payload.command_snapshot_content_digest,
        "source_exchange": payload.current_source.exchange,
        "source_instrument_public_id": payload.current_source.instrument_public_id,
        "current_spec_public_id": payload.current_catalog.spec_public_id,
        "current_spec_timestamp": payload.current_catalog.spec_timestamp,
        "source_spec_public_id": payload.current_source.catalog.spec_public_id,
        "source_spec_timestamp": payload.current_source.catalog.spec_timestamp,
        "economic_context_digest": economic_context_digest(payload.economic_context),
        "request_digest": command_request_digest(payload.request_material),
        "interpreted_at": payload.interpreted_at,
    }


def _admission_record(
    payload: SubmissionUnitAdmissionPayload, provenance: SnapshotWriteProvenance
) -> SubmissionUnitAdmission:
    """Project a validated complete proof graph into its immutable indexed version-two row."""
    return SubmissionUnitAdmission(
        **admission_record_values(payload, provenance),
        content_digest=unit_admission_content_digest(payload),
        canonical_payload=canonical_unit_admission_payload(payload),
    )


async def insert_submission_unit_admission(
    repository: SnapshotRepository,
    payload: SubmissionUnitAdmissionPayload,
    provenance: SnapshotWriteProvenance,
) -> SnapshotWriteResult:
    """Bind already-durable original/current evidence and commit one actual attempt witness.

    Args:
        repository: Real transaction and wallet-fence provider.
        payload: Complete immutable version-two proof graph.
        provenance: Actual producer insertion identity and timestamp.

    Returns:
        Committed immutable admission identity without authorizing venue resend.
    """
    proven = SubmissionUnitAdmissionPayload.model_validate(payload)
    async with repository.session() as session:
        await repository._begin_execution_insert_transaction(session)
        await repository._acquire_execution_fence_lock(session, proven.scope.wallet_public_id)
        result = await insert_bound_submission_admission(session, proven, provenance)
        await session.commit()
        return result
