"""Fenced storage for proven unit snapshots, not raw foreign-fill ingestion.

The public writer opens the existing repository execution transaction and wallet
fence before reading durable target lineage. Exact existing snapshots are idempotent without re-reading mutable source
observations. New bindings use the target durable knowledge clock consistently;
venue execution time remains economic evidence. The session helper is private
for a future same-transaction producer integration; this storage slice does not wire
existing command or execution producers automatically.
"""

import hashlib
import json
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from decimal import InvalidOperation
from math import isfinite
from typing import Protocol

from pydantic import ValidationError
from sqlalchemy import Select
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from snapper.application.portfolio.execution_chain import ExecutionChainRecord
from snapper.application.portfolio.execution_chain import execution_row_digest
from snapper.core.execution_unit_snapshot import VENUE_EVENT_HASH_DOMAIN
from snapper.core.execution_unit_snapshot import ExecutionUnitSnapshotPayload
from snapper.core.execution_unit_snapshot import FillSnapshotTarget
from snapper.core.execution_unit_snapshot import SnapshotCommandMaterial
from snapper.core.execution_unit_snapshot import SnapshotReceipt
from snapper.core.execution_unit_snapshot import SnapshotWriteProvenance
from snapper.core.execution_unit_snapshot import SubmissionSnapshotTarget
from snapper.core.execution_unit_snapshot import _canonical_decimal
from snapper.core.execution_unit_snapshot import _instant_micros
from snapper.core.execution_unit_snapshot import canonical_snapshot_payload
from snapper.core.execution_unit_snapshot import command_request_digest
from snapper.core.execution_unit_snapshot import snapshot_content_digest
from snapper.data.models import Base
from snapper.data.models import Execution
from snapper.data.models import ExecutionUnitSnapshot
from snapper.data.models import Instrument
from snapper.data.models import InstrumentSpec
from snapper.data.models import Order
from snapper.data.models import Symbol
from snapper.data.models import TradeCommand
from snapper.data.models import VenueEvent


class SnapshotBindingError(ValueError):
    """Durable target or catalog evidence does not establish the claimed snapshot."""


class SnapshotContentConflictError(ValueError):
    """One immutable semantic target already has different unit evidence."""


class SnapshotRepository(Protocol):
    """Existing transaction/fence operations required by the dedicated writer."""

    def session(self) -> AbstractAsyncContextManager[AsyncSession]:
        """Open a repository session without silently changing its transaction policy.

        Returns:
            An asynchronous context manager for the repository's real database session.
        """

    async def _begin_execution_insert_transaction(self, session: AsyncSession, /) -> None:
        """Use the existing explicit isolation or SQLite BEGIN IMMEDIATE opener."""

    async def _acquire_execution_fence_lock(
        self, session: AsyncSession, wallet_public_id: str, /
    ) -> None:
        """Acquire the existing transaction-scoped execution fence for this wallet."""


@dataclass(frozen=True)
class SnapshotWriteResult:
    """Identity and retry outcome of a committed immutable snapshot."""

    id: int
    public_id: str
    content_digest: str
    created: bool


def _require(condition: bool, reason: str) -> None:
    """Fail closed before insertion when one durable binding cannot be established."""
    if not condition:
        raise SnapshotBindingError(reason)


async def _one[Model: Base](session: AsyncSession, statement: Select[Model]) -> Model | None:
    """Refuse ambiguous durable identities rather than selecting the first matching row."""
    return (await session.execute(statement)).scalar_one_or_none()


def _number(value: float | Decimal | None) -> Decimal | None:
    """Cross-check existing durable operands without calling a float mirror wire-exact."""
    return Decimal(str(value)) if value is not None else None


def durable_command_material(command: TradeCommand) -> SnapshotCommandMaterial:
    """Project immutable request fields while omitting status-only SCD2 changes.

    Args:
        command: Real durable command row selected under its account fence.

    Returns:
        Typed request material; existing float operands are explicitly decimalized.
    """
    return SnapshotCommandMaterial.model_validate(
        {
            "command_public_id": command.public_id,
            "wallet_public_id": command.wallet_public_id,
            "exchange": command.exchange,
            "mode": command.mode,
            "native_symbol": command.instrument,
            "client_order_id": command.client_order_id,
            "side": command.side,
            "order_type": command.order_type,
            "quantity": _number(command.quantity),
            "price": _number(command.price),
            "stop_price": _number(command.stop_price),
            "leverage": command.leverage,
            "post_only": command.post_only,
            "reduce_only": command.reduce_only,
        }
    )


def bound_execution_digest(execution: Execution) -> str:
    """Use the unchanged execution digest ABI rather than inventing a second ledger fold.

    Args:
        execution: The actual immutable execution row whose target is being bound.

    Returns:
        The existing canonical digest over the execution's original ledger fields.
    """
    return execution_row_digest(
        ExecutionChainRecord(
            scope_sequence=execution.scope_sequence,
            public_id=execution.public_id,
            order_public_id=execution.order_public_id,
            wallet_public_id=execution.wallet_public_id,
            operator_public_id=execution.operator_public_id,
            exchange=execution.exchange,
            mode=execution.mode,
            exec_id=execution.exec_id,
            trade_id=execution.trade_id,
            side=execution.side,
            status=execution.status,
            fee_asset=execution.fee_asset,
            price_decimal=execution.price_decimal,
            size_decimal=execution.size_decimal,
            fee_decimal=execution.fee_decimal,
            counter_amount_decimal=execution.counter_amount_decimal,
            numeric_provenance=execution.numeric_provenance,
            liquidity_role=execution.liquidity_role,
            timestamp=execution.timestamp,
            executed_at=execution.executed_at,
        )
    )


def _mirror_value(value: float | None) -> str | None:
    """Bind an existing numeric mirror without presenting it as a venue-exact decimal."""
    parsed = _number(value)
    return _canonical_decimal(parsed) if parsed is not None else None


def bound_venue_event_digest(event: VenueEvent) -> str:
    """Bind durable observation identity and exact stored receipt representation.

    Args:
        event: Existing venue observation, not an assumed immutable raw-wire record.

    Returns:
        A separate digest binding scope, correlation, times and stored receipt text.
    """
    material = (
        event.public_id,
        event.wallet_public_id,
        event.exchange,
        event.mode,
        event.instrument,
        event.event_type,
        event.command_public_id,
        event.exchange_order_id,
        event.client_order_id,
        event.venue_client_id,
        event.exec_id,
        event.trade_id,
        event.side,
        event.status,
        _instant_micros(event.timestamp),
        _instant_micros(event.received_at),
        _instant_micros(event.venue_timestamp) if event.venue_timestamp is not None else None,
        event.payload_json,
        _mirror_value(event.fill_size),
        _mirror_value(event.fill_price),
        _mirror_value(event.fee),
        event.fee_asset,
    )
    canonical = json.dumps(material, separators=(",", ":"))
    return hashlib.sha256(VENUE_EVENT_HASH_DOMAIN + canonical.encode()).hexdigest()


async def _bind_submission(
    session: AsyncSession,
    payload: ExecutionUnitSnapshotPayload,
    target: SubmissionSnapshotTarget,
) -> datetime:
    """Check durable command material known at the snapshot's interpretation timestamp."""
    command = await _one(
        session,
        select(TradeCommand).where(
            TradeCommand.public_id == target.command_public_id,
            TradeCommand.timestamp <= payload.interpreted_at,
            TradeCommand.known_to > payload.interpreted_at,
        ),
    )
    if command is None:
        raise SnapshotBindingError("submission_target_missing")
    request = durable_command_material(command)
    _require(command.command_type in {"create", "submit"}, "submission_target_not_create")
    _require(
        (
            request.wallet_public_id,
            request.exchange,
            request.mode,
            request.native_symbol,
            request.client_order_id,
        )
        == (
            payload.scope.wallet_public_id,
            payload.scope.exchange,
            payload.scope.mode,
            payload.venue_identity.native_symbol,
            target.client_order_id,
        ),
        "submission_scope_mismatch",
    )
    _require(command_request_digest(request) == target.request_digest, "submission_digest_mismatch")
    dimensions = payload.dimensions
    signed = request.quantity if request.side == "buy" else request.quantity.copy_negate()
    _require(
        (signed, request.side, request.price, request.stop_price)
        == (dimensions.signed_quantity, dimensions.side, dimensions.price, dimensions.stop_price),
        "submission_operand_mismatch",
    )
    _require(dimensions.fee is None, "submission_cannot_claim_executed_fee")
    _require(payload.receipt.representation == "decoded_json_text", "submission_receipt_not_intent")
    _require(
        payload.receipt.reported_quantity_unit is None
        and payload.receipt.reported_quantity_convention == "unknown"
        and payload.receipt.reporting_option_name is None
        and payload.receipt.reporting_option_value is None,
        "submission_receipt_cannot_claim_fill_reporting",
    )
    observed = SnapshotCommandMaterial.model_validate_json(payload.receipt.material)
    _require(observed == request, "submission_receipt_mismatch")
    return await _submission_knowledge_time(session, request)


async def _submission_knowledge_time(
    session: AsyncSession, request: SnapshotCommandMaterial
) -> datetime:
    """Require unchanged request material across the complete logical command history."""
    versions = (
        await session.scalars(
            select(TradeCommand).where(TradeCommand.public_id == request.command_public_id)
        )
    ).all()
    if not versions:
        raise SnapshotBindingError("submission_initial_knowledge_time_missing")
    _require(
        all(
            record.command_type in {"create", "submit"}
            and durable_command_material(record) == request
            for record in versions
        ),
        "submission_request_changed_between_versions",
    )
    return min(record.timestamp for record in versions)


def _verify_raw_numeric_mirror(raw: Decimal, mirror: float, field: str) -> None:
    """Require a finite compatible float projection without replacing exact evidence.

    A nonzero raw operand must remain visible in its legacy mirror. Exact zero
    fees, including signed zero, are valid; a nonzero fee rounded to zero is not.
    The canonical raw string remains the snapshot operand and digest input.

    Args:
        raw: Already validated finite canonical execution operand.
        mirror: Corresponding durable legacy float mirror.
        field: Economic operand name used in the stable refusal reason.
    """
    projected = float(raw)
    _require(isfinite(projected) and isfinite(mirror), f"fill_{field}_raw_projection_nonfinite")
    _require(raw.is_zero() or projected != 0, f"fill_{field}_raw_projection_underflow")
    _require(projected == mirror, f"fill_{field}_raw_mirror_contradiction")


def _verify_fill_numbers(
    payload: ExecutionUnitSnapshotPayload, execution: Execution, event: VenueEvent
) -> None:
    """Compare direction, quantities, prices and fees to actual canonical execution strings."""
    dimensions = payload.dimensions
    _require(execution.numeric_provenance == "venue_raw", "fill_raw_evidence_missing")
    _require(
        all(
            value is not None
            for value in (execution.size_decimal, execution.price_decimal, execution.fee_decimal)
        ),
        "fill_raw_evidence_missing",
    )
    try:
        size = Decimal(execution.size_decimal or "NaN")
        price = Decimal(execution.price_decimal or "NaN")
        fee = Decimal(execution.fee_decimal or "NaN")
    except InvalidOperation as error:
        raise SnapshotBindingError("fill_raw_evidence_invalid") from error
    _require(all(value.is_finite() for value in (size, price, fee)), "fill_raw_evidence_invalid")
    _require(size > 0, "fill_quantity_invalid")
    _verify_raw_numeric_mirror(size, execution.size, "size")
    _verify_raw_numeric_mirror(price, execution.price, "price")
    _verify_raw_numeric_mirror(fee, execution.fee, "fee")
    signed = size if execution.side == "buy" else size.copy_negate()
    _require(
        (dimensions.side, dimensions.signed_quantity, dimensions.price)
        == (execution.side, signed, price),
        "fill_quantity_price_direction_mismatch",
    )
    _require(
        (dimensions.fee, dimensions.fee_currency) == (fee, execution.fee_asset),
        "fill_fee_mismatch",
    )
    _require(dimensions.stop_price is None, "fill_stop_price_is_not_executed_price")
    _require(
        (_number(event.fill_size), _number(event.fill_price), _number(event.fee), event.fee_asset)
        == (
            _number(execution.size),
            _number(execution.price),
            _number(execution.fee),
            execution.fee_asset,
        ),
        "fill_event_numeric_mirror_contradiction",
    )


async def _bind_fill(
    session: AsyncSession, payload: ExecutionUnitSnapshotPayload, target: FillSnapshotTarget
) -> datetime:
    """Bind real execution/order/event lineage rather than trusting caller-supplied UUIDs."""
    execution = await _one(
        session, select(Execution).where(Execution.public_id == target.execution_public_id)
    )
    event = await _one(
        session, select(VenueEvent).where(VenueEvent.public_id == target.venue_event_public_id)
    )
    if execution is None or event is None:
        raise SnapshotBindingError("fill_target_missing")
    order = await _one(
        session,
        select(Order).where(
            Order.public_id == execution.order_public_id,
            Order.timestamp <= execution.timestamp,
            Order.known_to > execution.timestamp,
        ),
    )
    if order is None:
        raise SnapshotBindingError("fill_order_lineage_missing")
    scope = payload.scope
    _require(
        (execution.wallet_public_id, execution.exchange, execution.mode)
        == (scope.wallet_public_id, scope.exchange, scope.mode),
        "fill_execution_scope_mismatch",
    )
    _require(
        (order.wallet_public_id, order.mode, order.instrument_public_id)
        == (scope.wallet_public_id, scope.mode, scope.instrument_public_id),
        "fill_order_scope_mismatch",
    )
    _require(
        (event.wallet_public_id, event.exchange, event.mode, event.instrument, event.event_type)
        == (
            scope.wallet_public_id,
            scope.exchange,
            scope.mode,
            payload.venue_identity.native_symbol,
            "fill_observed",
        ),
        "fill_event_scope_mismatch",
    )
    _verify_fill_identity(event, execution, order)
    _require(
        bound_execution_digest(execution) == target.execution_digest,
        "fill_execution_digest_mismatch",
    )
    _require(
        bound_venue_event_digest(event) == target.venue_event_digest, "fill_event_digest_mismatch"
    )
    _require(event.payload_json is not None, "fill_receipt_missing")
    receipt = SnapshotReceipt.model_validate_json(event.payload_json or "null")
    _require(receipt == payload.receipt, "fill_receipt_mismatch")
    _require(
        payload.dimensions.signed_quantity_unit == receipt.reported_quantity_unit
        and payload.rebase.quantity_convention == receipt.reported_quantity_convention
        and payload.rebase.requested_option_name == receipt.reporting_option_name
        and payload.rebase.requested_option_value == receipt.reporting_option_value,
        "fill_reported_quantity_convention_mismatch",
    )
    await _verify_reported_command(session, event, execution, order)
    _verify_fill_numbers(payload, execution, event)
    _require(
        event.timestamp <= payload.interpreted_at and event.received_at <= payload.interpreted_at,
        "fill_interpretation_precedes_durable_observation",
    )
    return execution.timestamp


async def _verify_reported_command(
    session: AsyncSession, event: VenueEvent, execution: Execution, order: Order
) -> None:
    """Cross-check a reported command against actual create/submit order lineage."""
    if event.command_public_id is None:
        return
    command = await _one(
        session,
        select(TradeCommand).where(
            TradeCommand.public_id == event.command_public_id,
            TradeCommand.timestamp <= execution.timestamp,
            TradeCommand.known_to > execution.timestamp,
        ),
    )
    if command is None:
        raise SnapshotBindingError("fill_reported_command_missing")
    _require(
        command.command_type in {"create", "submit"}
        and (
            command.wallet_public_id,
            command.exchange,
            command.mode,
            command.instrument,
            command.side,
        )
        == (
            execution.wallet_public_id,
            execution.exchange,
            execution.mode,
            event.instrument,
            execution.side,
        ),
        "fill_reported_command_scope_mismatch",
    )
    durable_ids = (
        (order.client_order_id, command.client_order_id),
        (order.exchange_order_id, command.exchange_order_id),
    )
    _verify_reported_ids(
        tuple((reported, expected) for reported, expected in durable_ids if expected is not None),
        "command_order",
    )
    if event.venue_client_id is not None:
        _verify_reported_ids(
            ((event.venue_client_id, command.venue_client_id),), "command_venue_client"
        )


def _verify_reported_ids(pairs: tuple[tuple[str | None, str | None], ...], family: str) -> None:
    """Require affirmative correlation and consistency of every reported identity."""
    present = tuple((reported, expected) for reported, expected in pairs if reported is not None)
    _require(any(reported.strip() for reported, _ in present), f"fill_{family}_identity_missing")
    _require(
        all(bool(reported.strip()) and reported == expected for reported, expected in present),
        f"fill_{family}_identity_mismatch",
    )


def _verify_fill_identity(event: VenueEvent, execution: Execution, order: Order) -> None:
    """Require affirmative order/fill correlation, including every reported secondary ID."""
    _require(event.side == execution.side == order.side, "fill_order_event_direction_mismatch")
    _verify_reported_ids(
        (
            (event.exchange_order_id, order.exchange_order_id),
            (event.client_order_id, order.client_order_id),
        ),
        "order",
    )
    _verify_reported_ids(
        ((event.exec_id, execution.exec_id), (event.trade_id, execution.trade_id)),
        "external",
    )


async def _verify_catalog(
    session: AsyncSession, payload: ExecutionUnitSnapshotPayload, effective_at: datetime
) -> None:
    """Bind the copied interpretation to its exact temporal catalog and instrument observation."""
    catalog = payload.catalog
    spec = await _one(
        session,
        select(InstrumentSpec).where(
            InstrumentSpec.public_id == catalog.spec_public_id,
            InstrumentSpec.timestamp == catalog.spec_timestamp,
        ),
    )
    if spec is None:
        raise SnapshotBindingError("catalog_observation_missing")
    _require(
        (
            spec.instrument_public_id,
            spec.spec_source,
            spec.spec_version,
            spec.spec_observed_at,
        )
        == (
            payload.scope.instrument_public_id,
            catalog.source,
            catalog.version,
            catalog.observed_at,
        ),
        "catalog_observation_mismatch",
    )
    _require(spec.timestamp <= effective_at < spec.known_to, "catalog_not_effective_for_target")
    result = await session.execute(
        select(Instrument, Symbol)
        .join(Symbol, Symbol.public_id == Instrument.symbol_public_id)
        .where(
            Instrument.public_id == payload.scope.instrument_public_id,
            Instrument.timestamp <= effective_at,
            Instrument.known_to > effective_at,
            Symbol.timestamp <= effective_at,
            Symbol.known_to > effective_at,
        )
    )
    row = result.one_or_none()
    if row is None:
        raise SnapshotBindingError("catalog_instrument_lineage_missing")
    instrument, symbol = row
    dimensions = payload.dimensions
    _require(
        (instrument.exchange, symbol.native_symbol, symbol.base, symbol.quote)
        == (
            payload.scope.exchange,
            payload.venue_identity.native_symbol,
            dimensions.base_currency,
            dimensions.quote_currency,
        ),
        "catalog_instrument_identity_mismatch",
    )
    _verify_catalog_dimensions(spec, payload)


def _verify_catalog_dimensions(spec: InstrumentSpec, payload: ExecutionUnitSnapshotPayload) -> None:
    """Check copied quantum, unit and payoff fields against the exact stored specification."""
    dimensions = payload.dimensions
    _require(
        (
            spec.native_order_quantity_unit,
            spec.quantity_unit,
            _number(spec.lot_size),
            _number(spec.tick_size),
            spec.contract_size,
        )
        == (
            dimensions.native_order_quantity_unit,
            dimensions.internal_quantity_unit,
            dimensions.quantity_quantum,
            dimensions.price_quantum,
            dimensions.catalog_contract_multiplier,
        ),
        "catalog_quantity_price_unit_mismatch",
    )
    actual_payoff = (
        spec.contract_payoff_model,
        spec.contract_size if spec.contract_payoff_model is not None else None,
        spec.contract_value_currency,
        spec.settlement_currency,
    )
    expected = (
        dimensions.payoff_model,
        dimensions.contract_multiplier,
        dimensions.contract_value_currency,
        dimensions.settlement_currency,
    )
    _require(actual_payoff == expected, "catalog_payoff_mismatch")


async def _existing_snapshot(
    session: AsyncSession, payload: ExecutionUnitSnapshotPayload
) -> SnapshotWriteResult | None:
    """Resolve exact immutable retries before mutable source lineage is consulted again."""
    target = payload.target
    target_filter = (
        ExecutionUnitSnapshot.command_public_id == target.command_public_id
        if isinstance(target, SubmissionSnapshotTarget)
        else ExecutionUnitSnapshot.execution_public_id == target.execution_public_id
    )
    existing = await _one(session, select(ExecutionUnitSnapshot).where(target_filter))
    if existing is None:
        return None
    if existing.content_digest != snapshot_content_digest(
        payload
    ) or existing.canonical_payload != canonical_snapshot_payload(payload):
        raise SnapshotContentConflictError(
            "Snapshot target already has different immutable content"
        )
    return SnapshotWriteResult(existing.id, existing.public_id, existing.content_digest, False)


async def _insert_bound_snapshot(
    session: AsyncSession,
    payload: ExecutionUnitSnapshotPayload,
    provenance: SnapshotWriteProvenance,
) -> SnapshotWriteResult:
    """Insert or read one exact target while the caller holds the existing wallet fence."""
    existing = await _existing_snapshot(session, payload)
    if existing is not None:
        return existing
    target = payload.target
    if isinstance(target, SubmissionSnapshotTarget):
        effective_at = await _bind_submission(session, payload, target)
    else:
        effective_at = await _bind_fill(session, payload, target)
    _require(effective_at <= payload.interpreted_at, "interpretation_precedes_durable_target")
    await _verify_catalog(session, payload, effective_at)
    digest = snapshot_content_digest(payload)
    record = ExecutionUnitSnapshot(
        **provenance.model_dump(mode="python"),
        phase=target.phase,
        wallet_public_id=payload.scope.wallet_public_id,
        exchange=payload.scope.exchange,
        mode=payload.scope.mode,
        instrument_public_id=payload.scope.instrument_public_id,
        command_public_id=(
            target.command_public_id if isinstance(target, SubmissionSnapshotTarget) else None
        ),
        execution_public_id=(
            target.execution_public_id if isinstance(target, FillSnapshotTarget) else None
        ),
        spec_public_id=payload.catalog.spec_public_id,
        spec_timestamp=payload.catalog.spec_timestamp,
        spec_source=payload.catalog.source,
        spec_version=payload.catalog.version,
        content_digest=digest,
        canonical_payload=canonical_snapshot_payload(payload),
    )
    session.add(record)
    await session.flush()
    return SnapshotWriteResult(record.id, record.public_id, digest, True)


async def insert_execution_unit_snapshot(
    repository: SnapshotRepository,
    payload: ExecutionUnitSnapshotPayload,
    provenance: SnapshotWriteProvenance,
) -> SnapshotWriteResult:
    """Verify durable lineage and insert an immutable snapshot under the existing execution fence.

    This convenience API binds already-durable targets. Future producers that
    create an execution and snapshot in one transaction must call the private
    session helper inside their existing fenced transaction instead.

    Args:
        repository: Repository supplying the established transaction and wallet fence.
        payload: Strict typed evidence for one proven submission or real execution.
        provenance: Insertion identity and timestamp, separate from observation times.

    Returns:
        The committed immutable row identity, or its original identity on an exact retry.
    """
    validated = ExecutionUnitSnapshotPayload.model_validate(payload)
    stamp = SnapshotWriteProvenance.model_validate(provenance)
    _require(stamp.timestamp >= validated.interpreted_at, "insert_precedes_interpretation")
    async with repository.session() as session:
        await repository._begin_execution_insert_transaction(session)
        await repository._acquire_execution_fence_lock(session, validated.scope.wallet_public_id)
        try:
            result = await _insert_bound_snapshot(session, validated, stamp)
        except ValidationError as error:
            raise SnapshotBindingError(
                "Durable evidence does not satisfy the typed snapshot contract"
            ) from error
        await session.commit()
        return result
