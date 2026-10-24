"""Interpret original Kraken TradeVolume observations without inventing account or tier authority.

Pair-only current fee maps and explicitly class-qualified schedules remain
different source structures. Decimal strings retain their literal text and
percentage units. Optional active flags are observations, never a locally
inferred fee tier. This parser has no credential, request or current-funds proof.
"""

import re
from collections.abc import Callable
from collections.abc import Mapping
from decimal import Decimal
from decimal import DecimalException
from typing import Literal
from typing import Self

from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import Field
from pydantic import model_validator

from snapper.core.execution_unit_snapshot import EvidenceDecimal
from snapper.core.execution_unit_snapshot import EvidenceText
from snapper.infrastructure.exchanges.execution_wire_receipts import _CONTROL_EVENTS
from snapper.infrastructure.exchanges.execution_wire_receipts import (
    decode_private_execution_receipt,
)
from snapper.infrastructure.exchanges.private_control_decoder import _AUTH_NAMES

_DECIMAL_TEXT = re.compile(r"[+-]?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?(?:[eE][+-]?[0-9]+)?\Z")
_MAX_BYTES = 8 * 1024 * 1024
_MAX_ENTRIES = 4096
_MAX_NODES = 65536
_MAX_DEPTH = 64
_PRIVATE_FIELDS = _AUTH_NAMES | {
    "authent",
    "nonce",
    "privatekey",
    "apisecret",
    "api-key",
    "api-sign",
    "apisign",
    "otp",
}


class KrakenFeeSourceError(ValueError):
    """Original fee source syntax, privacy or supported interpretation cannot be established."""


class _FeeValue(BaseModel):
    """Immutable strict observations do not establish source provenance or financial permission."""

    model_config = ConfigDict(
        frozen=True,
        strict=True,
        extra="forbid",
        revalidate_instances="always",
        hide_input_in_errors=True,
    )


class KrakenFeeDecimal(_FeeValue):
    """One exact original decimal string, without conflating percentages and monetary quantities."""

    original_text: EvidenceText
    value: EvidenceDecimal

    @model_validator(mode="after")
    def validate_original_decimal(self) -> Self:
        """Bind the numeric projection to supported original decimal syntax.

        Returns:
            The unchanged exact source value, including signed rebates and decimal precision.
        """
        if _decimal(self.original_text) != self.value:
            raise KrakenFeeSourceError("kraken_fee_decimal_copy_mismatch")
        return self


class KrakenFeeOptional[ValueT](_FeeValue):
    """Retain absent, explicit null and actual values as distinct original source states."""

    present: bool
    value: ValueT | None

    @model_validator(mode="after")
    def validate_presence(self) -> Self:
        """Refuse an asserted absent source member carrying an actual value.

        Returns:
            The unchanged original presence distinction.
        """
        if not self.present and self.value is not None:
            raise KrakenFeeSourceError("kraken_fee_absent_value_supplied")
        return self


class KrakenCurrentPairFee(_FeeValue):
    """Provider current fee map entry, whose original response key has no asset-class member."""

    pair: EvidenceText
    fee_percent: KrakenFeeDecimal
    minimum_fee_percent: KrakenFeeDecimal
    maximum_fee_percent: KrakenFeeDecimal
    next_fee_percent: KrakenFeeOptional[KrakenFeeDecimal]
    spot_volume_tier: KrakenFeeDecimal
    futures_volume_tier: KrakenFeeOptional[KrakenFeeDecimal]
    next_spot_volume_tier: KrakenFeeOptional[KrakenFeeDecimal]
    next_futures_volume_tier: KrakenFeeOptional[KrakenFeeDecimal]
    volume_offset: KrakenFeeOptional[KrakenFeeDecimal]


class KrakenFeeTier(_FeeValue):
    """One unchanged schedule tier with actual provider activity and optional condition operands."""

    maker_fee_percent: KrakenFeeDecimal
    taker_fee_percent: KrakenFeeDecimal
    minimum_spot_volume: KrakenFeeOptional[KrakenFeeDecimal]
    minimum_futures_volume: KrakenFeeOptional[KrakenFeeDecimal]
    minimum_assets_on_platform: KrakenFeeOptional[KrakenFeeDecimal]
    active: KrakenFeeOptional[bool]


class KrakenFeeSchedule(_FeeValue):
    """An explicitly class-qualified original schedule without a locally chosen applicable tier."""

    pair: EvidenceText
    asset_class: EvidenceText
    tiers: tuple[KrakenFeeTier, ...] = Field(max_length=_MAX_ENTRIES)


class KrakenTradeFeeObservation(_FeeValue):
    """Complete relevant fee observations with no provider clock or inferred request/account identity."""

    parser_policy: Literal["kraken_spot_trade_volume_v1"] = "kraken_spot_trade_volume_v1"
    volume_currency: EvidenceText
    volume_asset_class: KrakenFeeOptional[EvidenceText]
    volume: KrakenFeeDecimal
    domain_spot_volume_30d: KrakenFeeDecimal
    domain_futures_volume_30d: KrakenFeeDecimal
    domain_assets_on_platform: KrakenFeeDecimal
    taker_fees: KrakenFeeOptional[tuple[KrakenCurrentPairFee, ...]]
    maker_fees: KrakenFeeOptional[tuple[KrakenCurrentPairFee, ...]]
    schedules: KrakenFeeOptional[tuple[KrakenFeeSchedule, ...]]


def _object(value: object) -> Mapping[str, object]:
    """Require an actual object without silently replacing absent response sections."""
    if not isinstance(value, Mapping) or not all(isinstance(key, str) for key in value):
        raise KrakenFeeSourceError("kraken_fee_object_required")
    return value


def _text(value: object) -> str:
    """Preserve exact nonempty Unicode provider identifiers without canonicalizing aliases."""
    if not isinstance(value, str) or not value.strip():
        raise KrakenFeeSourceError("kraken_fee_text_required")
    try:
        value.encode("utf-8")
    except UnicodeError:
        raise KrakenFeeSourceError("kraken_fee_text_not_unicode") from None
    return value


def _decimal(value: object) -> Decimal:
    """Require finite decimal strings, preserving signed rates without binary-float conversion."""
    text = _text(value)
    if _DECIMAL_TEXT.fullmatch(text) is None:
        raise KrakenFeeSourceError("kraken_fee_decimal_text_invalid")
    try:
        return Decimal(text)
    except DecimalException:
        raise KrakenFeeSourceError("kraken_fee_decimal_text_invalid") from None


def _number(value: object) -> KrakenFeeDecimal:
    """Retain the literal provider string alongside its exact decimal interpretation."""
    return KrakenFeeDecimal(original_text=_text(value), value=_decimal(value))


def _boolean(value: object) -> bool:
    """Keep a literal false flag distinct from absent, null and unsupported numeric flags."""
    if not isinstance(value, bool):
        raise KrakenFeeSourceError("kraken_fee_boolean_required")
    return value


def _optional[ValueT](
    source: Mapping[str, object], name: str, parse: Callable[[object], ValueT]
) -> KrakenFeeOptional[ValueT]:
    """Preserve documented optional-nullable values without supplying a default fee or flag."""
    if name not in source:
        return KrakenFeeOptional[ValueT](present=False, value=None)
    value = source[name]
    return KrakenFeeOptional[ValueT](present=True, value=None if value is None else parse(value))


def _current_fee(pair: str, value: object) -> KrakenCurrentPairFee:
    """Project every documented current fee and volume operand without inferring asset class."""
    source = _object(value)
    return KrakenCurrentPairFee(
        pair=_text(pair),
        fee_percent=_number(source.get("fee")),
        minimum_fee_percent=_number(source.get("minfee")),
        maximum_fee_percent=_number(source.get("maxfee")),
        next_fee_percent=_optional(source, "nextfee", _number),
        spot_volume_tier=_number(source.get("tiervolume")),
        futures_volume_tier=_optional(source, "tierfuturesvolume", _number),
        next_spot_volume_tier=_optional(source, "nextvolume", _number),
        next_futures_volume_tier=_optional(source, "nextfuturesvolume", _number),
        volume_offset=_optional(source, "volumeoffset", _number),
    )


def _current_fees(value: object) -> tuple[KrakenCurrentPairFee, ...]:
    """Keep every original map entry in source order with no deduplication or missing-pair fallback."""
    source = _object(value)
    if len(source) > _MAX_ENTRIES:
        raise KrakenFeeSourceError("kraken_fee_entries_limit")
    return tuple(_current_fee(pair, item) for pair, item in source.items())


def _tier(value: object) -> KrakenFeeTier:
    """Retain each original tier without inventing AND/OR semantics for its optional conditions."""
    source = _object(value)
    return KrakenFeeTier(
        maker_fee_percent=_number(source.get("maker_fee")),
        taker_fee_percent=_number(source.get("taker_fee")),
        minimum_spot_volume=_optional(source, "min_spot_volume", _number),
        minimum_futures_volume=_optional(source, "min_futures_volume", _number),
        minimum_assets_on_platform=_optional(source, "min_assets_on_platform", _number),
        active=_optional(source, "active", _boolean),
    )


def _array(value: object) -> list[object]:
    """Require an actual bounded array while retaining the distinction between empty and absent."""
    if not isinstance(value, list):
        raise KrakenFeeSourceError("kraken_fee_array_required")
    if len(value) > _MAX_ENTRIES:
        raise KrakenFeeSourceError("kraken_fee_entries_limit")
    return value


def _schedule(value: object) -> KrakenFeeSchedule:
    """Keep literal class and pair source labels separate from the original request metadata."""
    source = _object(value)
    return KrakenFeeSchedule(
        pair=_text(source.get("pair")),
        asset_class=_text(source.get("class")),
        tiers=tuple(_tier(item) for item in _array(source.get("tiers"))),
    )


def _schedules(value: object) -> tuple[KrakenFeeSchedule, ...]:
    """Retain all original schedules and tier order without choosing a preferred duplicate."""
    return tuple(_schedule(item) for item in _array(value))


def _safe_values(source: Mapping[str, object]) -> list[object]:
    """Refuse authentication and recognized control fields before retaining any original bytes."""
    for key, value in source.items():
        label = key.strip().casefold()
        if label in _PRIVATE_FIELDS or label == "python-kraken-sdk":
            raise KrakenFeeSourceError("kraken_fee_private_fields_refused")
        if label == "method" and isinstance(value, str):
            raise KrakenFeeSourceError("kraken_fee_control_source_refused")
        if (
            label == "event"
            and isinstance(value, str)
            and value.strip().casefold() in _CONTROL_EVENTS
        ):
            raise KrakenFeeSourceError("kraken_fee_control_source_refused")
    return list(source.values())


def _safe_shape(document: Mapping[str, object]) -> None:
    """Inspect every unknown child for private fields within explicit local shape bounds."""
    pending: list[tuple[object, int]] = [(document, 0)]
    count = 0
    while pending:
        node, depth = pending.pop()
        count += 1
        if count > _MAX_NODES or depth > _MAX_DEPTH:
            raise KrakenFeeSourceError("kraken_fee_shape_limit")
        if isinstance(node, Mapping):
            pending.extend((value, depth + 1) for value in _safe_values(node))
        elif isinstance(node, list):
            pending.extend((value, depth + 1) for value in node)


def _source(wire_bytes: bytes) -> Mapping[str, object]:
    """Require complete strict UTF-8 JSON and an actual successful Spot response envelope."""
    if not isinstance(wire_bytes, bytes) or len(wire_bytes) > _MAX_BYTES or b"\x00" in wire_bytes:
        raise KrakenFeeSourceError("kraken_fee_original_entity_required")
    try:
        wire_bytes.decode("utf-8")
        document = decode_private_execution_receipt(wire_bytes)
    except (ValueError, UnicodeError, RecursionError):
        raise KrakenFeeSourceError("kraken_fee_original_json_invalid") from None
    _safe_shape(document)
    if document.get("error") != []:
        raise KrakenFeeSourceError("kraken_fee_success_envelope_required")
    return _object(document.get("result"))


def parse_kraken_trade_fees(wire_bytes: bytes) -> KrakenTradeFeeObservation:
    """Interpret original current fee maps and complete schedules without granting source authority.

    Args:
        wire_bytes: Original transient Spot TradeVolume entity, never included in an error or log.

    Returns:
        Exact original relevant fee observations with no inferred class for pair-only fee maps.

    Raises:
        ValueError: If original syntax, privacy, required fields or supported decimal types fail.
    """
    source = _source(wire_bytes)
    inputs = _object(source.get("inputs"))
    return KrakenTradeFeeObservation(
        volume_currency=_text(source.get("currency")),
        volume_asset_class=_optional(source, "asset_class", _text),
        volume=_number(source.get("volume")),
        domain_spot_volume_30d=_number(inputs.get("domain_spot_volume_30d")),
        domain_futures_volume_30d=_number(inputs.get("domain_futures_volume_30d")),
        domain_assets_on_platform=_number(inputs.get("domain_assets_on_platform")),
        taker_fees=_optional(source, "fees", _current_fees),
        maker_fees=_optional(source, "fees_maker", _current_fees),
        schedules=_optional(source, "schedules", _schedules),
    )
