"""Version-two submission witnesses linking original units to a current account route.

The immutable v1 snapshot remains unchanged. Each actual admission attempt
records the current catalog and source identities and proves equality of their
economic dimensions to the original command's observations. Neither a witness
nor an exact retry authorizes a second venue CREATE.
"""

import hashlib
import json
from datetime import datetime
from typing import Literal
from typing import Self

from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import field_validator
from pydantic import model_validator

from snapper.core.execution_route_authority import ExecutionAccountScope
from snapper.core.execution_route_authority import ExecutionEvidenceIdentity
from snapper.core.execution_unit_snapshot import EvidenceDigest
from snapper.core.execution_unit_snapshot import EvidenceExchange
from snapper.core.execution_unit_snapshot import EvidenceInstant
from snapper.core.execution_unit_snapshot import EvidenceText
from snapper.core.execution_unit_snapshot import EvidenceUUID
from snapper.core.execution_unit_snapshot import PositiveEvidenceDecimal
from snapper.core.execution_unit_snapshot import QuantityUnit
from snapper.core.execution_unit_snapshot import SnapshotCatalog
from snapper.core.execution_unit_snapshot import SnapshotCommandMaterial
from snapper.core.execution_unit_snapshot import SnapshotDimensions
from snapper.core.execution_unit_snapshot import SnapshotRebaseContext
from snapper.core.execution_unit_snapshot import SnapshotScope

ECONOMIC_CONTEXT_HASH_DOMAIN = b"snapper:execution-economic-context:v2\x00"
UNIT_ADMISSION_HASH_DOMAIN = b"snapper:execution-unit-admission:v2\x00"


class _FrozenAdmissionEvidence(BaseModel):
    """Reject coercion and unchecked mutable model updates in an admission proof."""

    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        strict=True,
        revalidate_instances="always",
        hide_input_in_errors=True,
    )


class ExecutionRouteReference(_FrozenAdmissionEvidence):
    """The immutable authority and actual public route currently selected by the executor."""

    authority_public_id: EvidenceUUID
    authority_content_digest: EvidenceDigest
    scope: ExecutionAccountScope
    route_revision: EvidenceDigest


class SubmissionEvidenceIdentity(_FrozenAdmissionEvidence):
    """Distinct immutable record identities for one actual attempt and its v1 baseline."""

    attempt_public_id: EvidenceUUID
    admission: ExecutionEvidenceIdentity
    command_snapshot: ExecutionEvidenceIdentity

    @model_validator(mode="after")
    def validate_distinct_records(self) -> Self:
        """Keep baseline and current-admission facts independently identifiable.

        Returns:
            The same explicit producer identities.
        """
        if self.admission.public_id == self.command_snapshot.public_id:
            raise ValueError("Original snapshot and actual admission need distinct record UUIDs")
        return self


class InstrumentSourceObservation(_FrozenAdmissionEvidence):
    """One real source instrument and exact temporal catalog, separate from a Paper target."""

    exchange: EvidenceExchange
    instrument_public_id: EvidenceUUID
    instrument_timestamp: EvidenceInstant
    symbol_public_id: EvidenceUUID
    symbol_timestamp: EvidenceInstant
    catalog: SnapshotCatalog


class EconomicUnitContext(_FrozenAdmissionEvidence):
    """Complete economic dimensions whose equality permits a benign catalog refresh."""

    base_currency: EvidenceText
    quote_currency: EvidenceText
    native_order_quantity_unit: QuantityUnit
    internal_quantity_unit: QuantityUnit
    quantity_quantum: PositiveEvidenceDecimal | None
    price_quantum: PositiveEvidenceDecimal | None
    price_currency: EvidenceText
    price_quantity_convention: Literal["quote_per_base"]
    catalog_contract_multiplier: PositiveEvidenceDecimal | None
    payoff_model: Literal["linear", "inverse"] | None
    contract_multiplier: PositiveEvidenceDecimal | None
    contract_value_currency: EvidenceText | None
    settlement_currency: EvidenceText | None
    quantity_convention: Literal["spv_token", "underlying_equity", "not_applicable"]
    instrument_kind: EvidenceText | None
    expiry_at: EvidenceInstant | None

    @model_validator(mode="after")
    def validate_economic_dimensions(self) -> Self:
        """Keep complete payoff evidence and an explicit quote-per-base price denomination.

        Returns:
            The same economically coherent context without transport or freshness clocks.
        """
        payoff = (
            self.payoff_model,
            self.contract_multiplier,
            self.contract_value_currency,
            self.settlement_currency,
        )
        if any(value is None for value in payoff) and any(value is not None for value in payoff):
            raise ValueError("Economic payoff dimensions must be all present or all absent")
        if (
            self.payoff_model is not None
            and self.contract_multiplier != self.catalog_contract_multiplier
        ):
            raise ValueError("Economic payoff and catalog multipliers must agree")
        if self.price_currency != self.quote_currency:
            raise ValueError("Economic price denomination must be the verified quote currency")
        if self.payoff_model == "linear" and (
            self.contract_value_currency,
            self.settlement_currency,
        ) != (self.base_currency, self.quote_currency):
            raise ValueError(
                "Linear payoff denomination must be base multiplier and quote settlement"
            )
        if self.payoff_model == "inverse" and (
            self.contract_value_currency,
            self.settlement_currency,
        ) != (self.quote_currency, self.base_currency):
            raise ValueError(
                "Inverse payoff denomination must be quote multiplier and base settlement"
            )
        return self


def economic_unit_context(
    dimensions: SnapshotDimensions,
    rebase: SnapshotRebaseContext,
    instrument_kind: str | None,
    expiry_at: datetime | None,
) -> EconomicUnitContext:
    """Project known unit evidence while refusing equality of unknown conventions.

    Args:
        dimensions: Exact snapshot operands and catalog dimensions.
        rebase: Explicit quantity convention without an invented rebase conversion.
        instrument_kind: Actual catalog product kind.
        expiry_at: Actual aware catalog expiry, or explicit absence.

    Returns:
        An operand-independent context with complete unit and price conventions.
    """
    return EconomicUnitContext.model_validate(
        {
            "base_currency": dimensions.base_currency,
            "quote_currency": dimensions.quote_currency,
            "native_order_quantity_unit": dimensions.native_order_quantity_unit,
            "internal_quantity_unit": dimensions.internal_quantity_unit,
            "quantity_quantum": dimensions.quantity_quantum,
            "price_quantum": dimensions.price_quantum,
            "price_currency": dimensions.price_currency,
            "price_quantity_convention": dimensions.price_quantity_convention,
            "catalog_contract_multiplier": dimensions.catalog_contract_multiplier,
            "payoff_model": dimensions.payoff_model,
            "contract_multiplier": dimensions.contract_multiplier,
            "contract_value_currency": dimensions.contract_value_currency,
            "settlement_currency": dimensions.settlement_currency,
            "quantity_convention": rebase.quantity_convention,
            "instrument_kind": instrument_kind,
            "expiry_at": expiry_at,
        }
    )


def economic_context_digest(context: EconomicUnitContext) -> str:
    """Hash units and payoff semantics while excluding benign revision/freshness changes.

    Args:
        context: Complete operand-independent economic interpretation.

    Returns:
        Versioned SHA-256 independent of catalog observation clocks and local row IDs.
    """
    proven = EconomicUnitContext.model_validate(context)
    material = json.dumps(proven.model_dump(mode="json"), sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(ECONOMIC_CONTEXT_HASH_DOMAIN + material.encode()).hexdigest()


class SubmissionUnitAdmissionPayload(_FrozenAdmissionEvidence):
    """One actual attempt's source, current catalog, original snapshot and selected account."""

    schema_version: Literal[2] = 2
    attempt_public_id: EvidenceUUID
    route: ExecutionRouteReference
    scope: SnapshotScope
    command_snapshot_public_id: EvidenceUUID
    command_snapshot_content_digest: EvidenceDigest
    request_material: SnapshotCommandMaterial
    current_catalog: SnapshotCatalog
    current_instrument_timestamp: EvidenceInstant
    current_symbol_public_id: EvidenceUUID
    current_symbol_timestamp: EvidenceInstant
    command_source: InstrumentSourceObservation
    current_source: InstrumentSourceObservation
    economic_context: EconomicUnitContext
    interpreted_at: EvidenceInstant

    @field_validator("schema_version", mode="before")
    @classmethod
    def _strict_version(cls, value: object) -> object:
        """Reject booleans and floats masquerading as the admission schema version."""
        if type(value) is not int:
            raise ValueError("Unit admission schema version must be a strict integer")
        return value

    @model_validator(mode="after")
    def validate_submission_scope(self) -> Self:
        """Bind real account, request, source and observed clocks without authorizing resend.

        Returns:
            The same coherent immutable submission witness.
        """
        request = self.request_material
        account = self.route.scope
        if (request.wallet_public_id, request.exchange, request.mode) != (
            self.scope.wallet_public_id,
            self.scope.exchange,
            self.scope.mode,
        ) or (account.wallet_public_id, account.exchange, account.mode) != (
            self.scope.wallet_public_id,
            self.scope.exchange,
            self.scope.mode,
        ):
            raise ValueError("Admission request, account and instrument scope must agree")
        self._validate_source_identity()
        observations = (
            self.current_catalog.spec_timestamp,
            self.current_catalog.observed_at,
            self.current_instrument_timestamp,
            self.current_symbol_timestamp,
            self.command_source.instrument_timestamp,
            self.command_source.symbol_timestamp,
            self.command_source.catalog.spec_timestamp,
            self.command_source.catalog.observed_at,
            self.current_source.instrument_timestamp,
            self.current_source.symbol_timestamp,
            self.current_source.catalog.spec_timestamp,
            self.current_source.catalog.observed_at,
        )
        if any(instant > self.interpreted_at for instant in observations):
            raise ValueError("Admission evidence cannot be observed after interpretation")
        return self

    def _validate_source_identity(self) -> None:
        """Prevent mutable Paper mappings or equal symbols from replacing the source UUID."""
        original = self.command_source
        current = self.current_source
        if (original.exchange, original.instrument_public_id, original.symbol_public_id) != (
            current.exchange,
            current.instrument_public_id,
            current.symbol_public_id,
        ):
            raise ValueError("Admission source identity changed after the durable intent")
        if self.scope.exchange == "paper":
            if current.exchange == "paper":
                raise ValueError("Paper admission requires an explicit real source instrument")
        elif (current.exchange, current.instrument_public_id) != (
            self.scope.exchange,
            self.scope.instrument_public_id,
        ):
            raise ValueError("Live admission source must be its actual execution instrument")


def canonical_unit_admission_payload(payload: SubmissionUnitAdmissionPayload) -> str:
    """Serialize the complete version-two proof graph without insertion-local row IDs.

    Args:
        payload: Actual attempt's immutable submitted terms and current route/catalog evidence.

    Returns:
        Canonical JSON preserving all exact Decimal values and normalized source clocks.
    """
    proven = SubmissionUnitAdmissionPayload.model_validate(payload)
    return json.dumps(proven.model_dump(mode="json"), sort_keys=True, separators=(",", ":"))


def unit_admission_content_digest(payload: SubmissionUnitAdmissionPayload) -> str:
    """Hash one attempt's complete proof graph under a separate v2 domain.

    Args:
        payload: Strict current admission evidence referencing the unchanged v1 snapshot.

    Returns:
        A versioned SHA-256 independent of insertion-local provenance.
    """
    return hashlib.sha256(
        UNIT_ADMISSION_HASH_DOMAIN + canonical_unit_admission_payload(payload).encode()
    ).hexdigest()
