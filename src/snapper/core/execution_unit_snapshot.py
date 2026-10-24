"""Typed immutable evidence for proven submission and fill unit interpretations.

The v1 schema records unknown conventions honestly and performs no unit or
corporate-action conversion. Receipt fidelity is explicit; decoded JSON is
never described as the original wire bytes. Observation times do not establish
database commit visibility.
"""

import base64
import hashlib
import json
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from decimal import Decimal
from decimal import InvalidOperation
from typing import Annotated
from typing import Literal
from typing import NoReturn
from typing import Self
from uuid import UUID

from pydantic import AfterValidator
from pydantic import AwareDatetime
from pydantic import BaseModel
from pydantic import BeforeValidator
from pydantic import ConfigDict
from pydantic import Field
from pydantic import PlainSerializer
from pydantic import StringConstraints
from pydantic import field_validator
from pydantic import model_validator

SNAPSHOT_HASH_DOMAIN = b"snapper:execution-unit-snapshot:v1\x00"
COMMAND_HASH_DOMAIN = b"snapper:execution-unit-command:v1\x00"
RECEIPT_HASH_DOMAIN = b"snapper:execution-unit-receipt:v1\x00"
VENUE_EVENT_HASH_DOMAIN = b"snapper:execution-unit-venue-event:v1\x00"
_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


def _nonblank(value: str) -> str:
    """Reject whitespace-only evidence without rewriting a raw identity."""
    if not value.strip():
        raise ValueError("Evidence text must not be blank")
    return value


def _uuid(value: str) -> str:
    """Canonicalize a validated UUID to prevent alternate-spelling scope splits."""
    return str(UUID(value))


def _exchange(value: str) -> str:
    """Reject noncanonical exchange spelling without silently changing scope."""
    if value != value.lower() or value != value.strip():
        raise ValueError("Exchange must use canonical lowercase spelling")
    return value


def _decimal(value: object) -> Decimal:
    """Parse exact decimal lexemes while refusing integers, booleans and floats."""
    if not isinstance(value, Decimal | str):
        raise ValueError("Decimal evidence must be Decimal or exact decimal text")
    try:
        result = value if isinstance(value, Decimal) else Decimal(value)
    except InvalidOperation as error:
        raise ValueError("Invalid decimal evidence") from error
    if not result.is_finite():
        raise ValueError("Decimal evidence must be finite")
    return result


def _positive(value: Decimal) -> Decimal:
    """Require a positive increment or multiplier without context arithmetic."""
    if value <= 0:
        raise ValueError("Increment or multiplier must be positive")
    return value


def _utc(value: datetime) -> datetime:
    """Normalize an already-aware observation instant for portable hashing."""
    try:
        return value.astimezone(UTC)
    except OverflowError as error:
        raise ValueError("Observation instant exceeds the supported UTC datetime range") from error


def _instant(value: object) -> datetime:
    """Accept aware instants or exact integer microseconds, never float epoch seconds."""
    if isinstance(value, datetime):
        return value
    if isinstance(value, int) and not isinstance(value, bool):
        try:
            return _EPOCH + timedelta(microseconds=value)
        except OverflowError as error:
            raise ValueError(
                "Timestamp microseconds exceed the supported datetime range"
            ) from error
    raise ValueError("Timestamp evidence requires an aware datetime or integer microseconds")


def _reject_json_constant(value: str) -> NoReturn:
    """Refuse non-standard numeric constants in a decoded JSON representation."""
    raise ValueError(f"Decoded JSON contains non-standard constant {value}")


def _unique_json_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    """Reject duplicate decoded object keys instead of silently choosing the last value."""
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"Decoded JSON contains duplicate object key {key}")
        result[key] = value
    return result


def _instant_micros(value: datetime) -> int:
    """Encode UTC instants exactly without float timestamp arithmetic."""
    if value.utcoffset() is None:
        raise ValueError("Canonical timestamps must be aware")
    delta = _utc(value) - _EPOCH
    return (delta.days * 86400 + delta.seconds) * 1000000 + delta.microseconds


def _canonical_decimal(value: Decimal) -> str:
    """Normalize decimal value tuples without context-dependent Decimal arithmetic."""
    sign, raw_digits, exponent = value.as_tuple()
    digits = list(raw_digits)
    if not isinstance(exponent, int):
        raise ValueError("Canonical decimal must be finite")
    if not any(digits):
        return "0e0"
    while len(digits) > 1 and digits[-1] == 0:
        digits.pop()
        exponent += 1
    prefix = "-" if sign else ""
    return prefix + "".join(str(digit) for digit in digits) + "e" + str(exponent)


type EvidenceText = Annotated[str, AfterValidator(_nonblank)]
type EvidenceExchange = Annotated[EvidenceText, AfterValidator(_exchange)]
type EvidenceUUID = Annotated[str, AfterValidator(_uuid)]
type EvidenceDigest = Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{64}$")]
type EvidenceDecimal = Annotated[
    Decimal,
    BeforeValidator(_decimal),
    PlainSerializer(_canonical_decimal, return_type=str, when_used="json"),
]
type PositiveEvidenceDecimal = Annotated[EvidenceDecimal, AfterValidator(_positive)]
type EvidenceInstant = Annotated[
    AwareDatetime,
    BeforeValidator(_instant),
    AfterValidator(_utc),
    PlainSerializer(_instant_micros, return_type=int, when_used="json"),
]
type QuantityUnit = Literal["base_asset", "contract_count"]


class _FrozenEvidence(BaseModel):
    """Reject unknown fields, implicit scalar coercion and mutable model updates."""

    model_config = ConfigDict(
        extra="forbid", frozen=True, strict=True, revalidate_instances="always"
    )


class SnapshotScope(_FrozenEvidence):
    """The exact durable account and instrument to which evidence may apply."""

    wallet_public_id: EvidenceUUID
    exchange: EvidenceExchange
    mode: Literal["live", "paper"]
    instrument_public_id: EvidenceUUID


class SnapshotCatalog(_FrozenEvidence):
    """An exact historical specification observation, never merely today's catalog."""

    spec_public_id: EvidenceUUID
    spec_timestamp: EvidenceInstant
    source: EvidenceText
    version: EvidenceText
    observed_at: EvidenceInstant


class SnapshotDimensions(_FrozenEvidence):
    """Exact operands and explicit economic dimensions without performing a conversion."""

    base_currency: EvidenceText
    quote_currency: EvidenceText
    native_order_quantity_unit: QuantityUnit | None
    internal_quantity_unit: QuantityUnit | None
    signed_quantity_unit: QuantityUnit | None
    side: Literal["buy", "sell"]
    signed_quantity: EvidenceDecimal
    quantity_quantum: PositiveEvidenceDecimal | None
    price: EvidenceDecimal | None
    stop_price: EvidenceDecimal | None = None
    price_quantum: PositiveEvidenceDecimal | None
    price_currency: EvidenceText | None
    price_quantity_convention: Literal["quote_per_base", "unknown"]
    catalog_contract_multiplier: PositiveEvidenceDecimal | None
    fee: EvidenceDecimal | None = None
    fee_currency: EvidenceText | None = None
    payoff_model: Literal["linear", "inverse"] | None
    contract_multiplier: PositiveEvidenceDecimal | None
    contract_value_currency: EvidenceText | None
    settlement_currency: EvidenceText | None

    @model_validator(mode="after")
    def validate_dimensions(self) -> Self:
        """Reject contradictory direction, partial payoff tuples and invented price units.

        Returns:
            The same immutable dimensions after semantic validation.
        """
        if self.signed_quantity == 0 or (self.signed_quantity > 0) != (self.side == "buy"):
            raise ValueError("Signed quantity must be nonzero and agree with side")
        payoff = (
            self.payoff_model,
            self.contract_multiplier,
            self.contract_value_currency,
            self.settlement_currency,
        )
        if any(value is None for value in payoff) and any(value is not None for value in payoff):
            raise ValueError("Payoff evidence must be entirely present or entirely unknown")
        if (
            self.payoff_model is not None
            and self.contract_multiplier != self.catalog_contract_multiplier
        ):
            raise ValueError(
                "Interpreted payoff multiplier must match the observed catalog multiplier"
            )
        if (self.fee is None) != (self.fee_currency is None):
            raise ValueError("Fee amount and denomination must be supplied together")
        if self.price_quantity_convention == "quote_per_base":
            if self.price_currency != self.quote_currency:
                raise ValueError("Quote-per-base prices require the verified quote currency")
        elif self.price_currency is not None:
            raise ValueError("Unknown price convention must not claim a currency dimension")
        return self


class SnapshotVenueIdentity(_FrozenEvidence):
    """Parser-observed identities with explicit provenance, separate from economic currencies."""

    native_symbol: EvidenceText
    source: EvidenceText
    source_version: EvidenceText
    observed_at: EvidenceInstant
    raw_base_asset_id: EvidenceText | None = None
    raw_quote_asset_id: EvidenceText | None = None
    display_base_asset: EvidenceText | None = None
    display_quote_asset: EvidenceText | None = None
    altname: EvidenceText | None = None
    wsname: EvidenceText | None = None


class SnapshotRebaseContext(_FrozenEvidence):
    """Record actual reporting options while v1 deliberately supports no rebase conversion."""

    quantity_convention: Literal["spv_token", "underlying_equity", "not_applicable", "unknown"]
    requested_option_name: EvidenceText | None = None
    requested_option_value: EvidenceText | None = None
    conversion_ratio: None = None

    @model_validator(mode="after")
    def validate_option_pair(self) -> Self:
        """Prevent a value without the actual transport option that gave it meaning.

        Returns:
            The same reporting context without inferring a conversion.
        """
        if (self.requested_option_name is None) != (self.requested_option_value is None):
            raise ValueError("Reporting option name and value must be supplied together")
        return self


class SnapshotReceipt(_FrozenEvidence):
    """An explicitly tagged receipt representation, not an unqualified raw-payload claim."""

    representation: Literal["wire_bytes_base64", "decoded_json_text"]
    material: EvidenceText
    endpoint_or_channel: EvidenceText
    observed_at: EvidenceInstant
    reported_quantity_unit: QuantityUnit | None = None
    reported_quantity_convention: Literal[
        "spv_token", "underlying_equity", "not_applicable", "unknown"
    ] = "unknown"
    reporting_option_name: EvidenceText | None = None
    reporting_option_value: EvidenceText | None = None

    @model_validator(mode="after")
    def validate_material(self) -> Self:
        """Validate representation syntax without pretending decoded JSON preserves wire bytes.

        Returns:
            The receipt with its original material preserved verbatim.
        """
        if (self.reporting_option_name is None) != (self.reporting_option_value is None):
            raise ValueError("Actual reporting option name and value must be supplied together")
        if self.representation == "wire_bytes_base64":
            try:
                raw = base64.b64decode(self.material, validate=True)
            except ValueError as error:
                raise ValueError("Wire receipt must use canonical base64") from error
            if not raw or base64.b64encode(raw).decode("ascii") != self.material:
                raise ValueError("Wire receipt must use nonempty canonical base64")
        else:
            try:
                json.loads(
                    self.material,
                    parse_float=Decimal,
                    parse_int=Decimal,
                    parse_constant=_reject_json_constant,
                    object_pairs_hook=_unique_json_object,
                )
            except ValueError as error:
                raise ValueError("Decoded receipt must contain valid JSON text") from error
        return self


class SubmissionSnapshotTarget(_FrozenEvidence):
    """One stable logical create command and its durable economic request material."""

    phase: Literal["submission"] = "submission"
    command_public_id: EvidenceUUID
    client_order_id: EvidenceText
    request_digest: EvidenceDigest


class FillSnapshotTarget(_FrozenEvidence):
    """A real execution and its actual durable source observation, independently bound."""

    phase: Literal["fill"] = "fill"
    execution_public_id: EvidenceUUID
    execution_digest: EvidenceDigest
    venue_event_public_id: EvidenceUUID
    venue_event_digest: EvidenceDigest


class ExecutionUnitSnapshotPayload(_FrozenEvidence):
    """Versioned typed interpretation of a proven intent or fill, without activation authority."""

    schema_version: Literal[1] = 1
    target: Annotated[SubmissionSnapshotTarget | FillSnapshotTarget, Field(discriminator="phase")]
    scope: SnapshotScope
    catalog: SnapshotCatalog
    dimensions: SnapshotDimensions
    venue_identity: SnapshotVenueIdentity
    rebase: SnapshotRebaseContext
    receipt: SnapshotReceipt
    interpreted_at: EvidenceInstant

    @field_validator("schema_version", mode="before")
    @classmethod
    def _strict_schema_version(cls, value: object) -> object:
        """Reject booleans and floats that compare equal to the literal version integer."""
        if type(value) is not int:
            raise ValueError("Schema version must be a strict integer")
        return value

    @model_validator(mode="after")
    def validate_observation_order(self) -> Self:
        """Keep observation provenance chronological without claiming a database commit time.

        Returns:
            The same immutable, chronologically consistent observation payload.
        """
        observations = (
            self.catalog.spec_timestamp,
            self.catalog.observed_at,
            self.venue_identity.observed_at,
            self.receipt.observed_at,
        )
        if any(instant > self.interpreted_at for instant in observations):
            raise ValueError("Evidence cannot be observed after its interpretation")
        if isinstance(self.target, SubmissionSnapshotTarget):
            if self.dimensions.signed_quantity_unit != self.dimensions.native_order_quantity_unit:
                raise ValueError("Submission quantity operand must identify the native order unit")
        return self


class SnapshotCommandMaterial(_FrozenEvidence):
    """Status-independent durable command fields that define one create request."""

    command_public_id: EvidenceUUID
    wallet_public_id: EvidenceUUID
    exchange: EvidenceExchange
    mode: Literal["live", "paper"]
    native_symbol: EvidenceText
    client_order_id: EvidenceText
    side: Literal["buy", "sell"]
    order_type: Literal["market", "limit", "stop", "stop_limit"]
    quantity: PositiveEvidenceDecimal
    price: EvidenceDecimal | None
    stop_price: EvidenceDecimal | None
    leverage: Annotated[int, Field(ge=1, le=2147483647)] | None
    post_only: bool
    reduce_only: bool


class SnapshotWriteProvenance(_FrozenEvidence):
    """Insertion provenance excluded from portable economic content identity."""

    public_id: EvidenceUUID
    session_id: EvidenceUUID
    sequence_id: Annotated[int, Field(ge=1)]
    timestamp: EvidenceInstant


def command_request_digest(command: SnapshotCommandMaterial) -> str:
    """Bind durable command economic fields independently of SCD2 status revisions.

    Args:
        command: Validated durable create-request material, with exact decimal operands.

    Returns:
        A versioned, status-independent request digest.
    """
    validated = SnapshotCommandMaterial.model_validate(command)
    material = json.dumps(validated.model_dump(mode="json"), sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(COMMAND_HASH_DOMAIN + material.encode()).hexdigest()


def canonical_snapshot_payload(payload: ExecutionUnitSnapshotPayload) -> str:
    """Serialize validated typed evidence without JSON float conversion or surrogate IDs.

    Args:
        payload: Proven-intent or proven-fill evidence to validate again at the boundary.

    Returns:
        Stable JSON containing exact Decimal strings and normalized UTC instants.
    """
    validated = ExecutionUnitSnapshotPayload.model_validate(payload)
    return json.dumps(validated.model_dump(mode="json"), sort_keys=True, separators=(",", ":"))


def snapshot_content_digest(payload: ExecutionUnitSnapshotPayload) -> str:
    """Hash canonical unit evidence under its own versioned domain.

    Args:
        payload: Strict typed evidence; no local insertion identity is included.

    Returns:
        Lowercase SHA-256 of the complete canonical payload.
    """
    return hashlib.sha256(
        SNAPSHOT_HASH_DOMAIN + canonical_snapshot_payload(payload).encode()
    ).hexdigest()


def receipt_content_digest(receipt: SnapshotReceipt) -> str:
    """Bind receipt representation and exact material without asserting wire authenticity.

    Args:
        receipt: Tagged receipt representation whose original material is preserved.

    Returns:
        A representation-sensitive digest, independent of local retry timestamps.
    """
    validated = SnapshotReceipt.model_validate(receipt)
    material = validated.representation.encode() + b"\x00" + validated.material.encode()
    return hashlib.sha256(RECEIPT_HASH_DOMAIN + material).hexdigest()
