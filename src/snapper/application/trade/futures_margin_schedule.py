"""Project literal account-resolved margin tiers without inventing a required-margin calculation.

Only the authenticated product source has this schema meaning. Pure shape
parsing does not prove that origin: a current consumer must independently load
the actual guarded source and bind account, collateral mode and source age.
Original tier order and exact numeric observations remain distinct from any
unverified threshold-boundary, incremental-rate or reservation formula.
"""

from collections.abc import Mapping
from decimal import Decimal
from itertools import pairwise
from typing import Literal
from typing import Self
from typing import cast

from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import Field
from pydantic import model_validator

from snapper.application.trade.futures_product_source import parse_safe_futures_product_source
from snapper.application.trade.futures_trading_instruments import FuturesProductSourceError
from snapper.core.execution_unit_snapshot import EvidenceDecimal
from snapper.core.execution_unit_snapshot import EvidenceInstant
from snapper.core.execution_unit_snapshot import EvidenceText
from snapper.infrastructure.exchanges.execution_wire_receipts import (
    decode_private_execution_receipt,
)

type FuturesMarginThresholdField = Literal["contracts", "numNonContractUnits"]
type FuturesMarginInstrumentType = Literal["futures_inverse", "futures_vanilla", "flexible_futures"]

_FUTURES_TYPES = frozenset({"futures_inverse", "futures_vanilla", "flexible_futures"})
_MAX_TIERS = 4096
_MAX_CONTRACT_THRESHOLD = Decimal(2**64 - 1)


class FuturesMarginScheduleSourceError(FuturesProductSourceError):
    """The selected original product cannot establish the locally supported margin tier projection."""


class _MarginObservation(BaseModel):
    """Strict immutable literal observations without authenticated provenance or financial capability."""

    model_config = ConfigDict(
        frozen=True,
        strict=True,
        extra="forbid",
        revalidate_instances="always",
        hide_input_in_errors=True,
    )


class FuturesMarginTier(_MarginObservation):
    """One original threshold and two provider rates, without an assumed tier boundary formula."""

    threshold: EvidenceDecimal = Field(ge=0)
    initial_margin_rate: EvidenceDecimal = Field(ge=0)
    maintenance_margin_rate: EvidenceDecimal = Field(ge=0)


class FuturesMarginSchedule(_MarginObservation):
    """An ordered account-resolved schema projection whose source and applicable account remain unproven."""

    parser_policy: Literal["kraken_futures_authenticated_margin_v1"] = (
        "kraken_futures_authenticated_margin_v1"
    )
    wire_symbol: EvidenceText
    instrument_type: FuturesMarginInstrumentType
    threshold_field: FuturesMarginThresholdField
    server_time_text: EvidenceText
    server_time: EvidenceInstant
    tiers: tuple[FuturesMarginTier, ...] = Field(min_length=1, max_length=_MAX_TIERS)

    @model_validator(mode="after")
    def validate_supported_layout(self) -> Self:
        """Require one supported unit and ascending original thresholds without sorting source data.

        Returns:
            The unchanged observation under the local supported-layout policy.
        """
        thresholds = [tier.threshold for tier in self.tiers]
        if any(current >= following for current, following in pairwise(thresholds)):
            raise FuturesMarginScheduleSourceError("futures_margin_threshold_order_unsupported")
        if self.threshold_field == "contracts" and any(
            value != value.to_integral_value() or value > _MAX_CONTRACT_THRESHOLD
            for value in thresholds
        ):
            raise FuturesMarginScheduleSourceError("futures_margin_contract_threshold_invalid")
        return self


def _object(value: object) -> Mapping[str, object]:
    """Refuse missing or malformed original objects without substituting an empty tier."""
    if not isinstance(value, Mapping) or not all(isinstance(name, str) for name in value):
        raise FuturesMarginScheduleSourceError("futures_margin_object_required")
    return value


def _nonnegative(value: object) -> Decimal:
    """Keep exact JSON numeric observations without accepting strings, booleans or binary floats."""
    if isinstance(value, bool) or not isinstance(value, (int, Decimal)):
        raise FuturesMarginScheduleSourceError("futures_margin_exact_number_required")
    result = Decimal(value)
    if not result.is_finite() or result < 0:
        raise FuturesMarginScheduleSourceError("futures_margin_nonnegative_number_required")
    return result


def _threshold_field(original: Mapping[str, object]) -> FuturesMarginThresholdField:
    """Preserve the actual mutually exclusive threshold field without inferring collateral mode."""
    has_contracts = "contracts" in original
    has_non_contract_units = "numNonContractUnits" in original
    if has_contracts == has_non_contract_units:
        raise FuturesMarginScheduleSourceError("futures_margin_one_threshold_unit_required")
    return "contracts" if has_contracts else "numNonContractUnits"


def _tier(value: object, field: FuturesMarginThresholdField) -> FuturesMarginTier:
    """Require one consistent original threshold field without converting or relabeling its values."""
    original = _object(value)
    if _threshold_field(original) != field:
        raise FuturesMarginScheduleSourceError("futures_margin_tier_unit_mismatch")
    return FuturesMarginTier(
        threshold=_nonnegative(original[field]),
        initial_margin_rate=_nonnegative(original.get("initialMargin")),
        maintenance_margin_rate=_nonnegative(original.get("maintenanceMargin")),
    )


def _tiers(
    source: Mapping[str, object],
) -> tuple[FuturesMarginThresholdField, tuple[FuturesMarginTier, ...]]:
    """Keep absent, empty and malformed schedules distinct and never fall back to public tier fields."""
    if "marginLevels" not in source:
        raise FuturesMarginScheduleSourceError("futures_margin_schedule_absent")
    levels = source["marginLevels"]
    if not isinstance(levels, list):
        raise FuturesMarginScheduleSourceError("futures_margin_schedule_array_required")
    if not levels:
        raise FuturesMarginScheduleSourceError("futures_margin_schedule_empty")
    if len(levels) > _MAX_TIERS:
        raise FuturesMarginScheduleSourceError("futures_margin_schedule_size_unsupported")
    field = _threshold_field(_object(levels[0]))
    return field, tuple(_tier(level, field) for level in levels)


def _original_product(document: Mapping[str, object], wire_symbol: str) -> Mapping[str, object]:
    """Select the original row after complete parsing proved its exact unique existence and schema."""
    instruments = cast(list[dict[str, object]], document["instruments"])
    return next(row for row in instruments if row["symbol"] == wire_symbol)


def parse_futures_margin_schedule(wire_bytes: bytes, wire_symbol: str) -> FuturesMarginSchedule:
    """Interpret a selected original authenticated-product row without assuming funds or tier formulas.

    Args:
        wire_bytes: Original complete entity; actual authenticated issuance must be proved separately.
        wire_symbol: Exact unchanged exchange symbol already resolved from the intended product route.

    Returns:
        Literal ordered schedule observations, never required margin or financial authorization.

    Raises:
        ValueError: If the whole product source or selected schedule cannot establish supported evidence.
    """
    snapshot = parse_safe_futures_product_source(wire_bytes)
    selected = next((row for row in snapshot.instruments if row.wire_symbol == wire_symbol), None)
    if selected is None:
        raise FuturesMarginScheduleSourceError("futures_margin_exact_symbol_missing")
    if selected.instrument_type not in _FUTURES_TYPES:
        raise FuturesMarginScheduleSourceError("futures_margin_future_type_unsupported")
    document = decode_private_execution_receipt(wire_bytes)
    original = _original_product(document, wire_symbol)
    field, tiers = _tiers(original)
    return FuturesMarginSchedule.model_validate(
        {
            "wire_symbol": selected.wire_symbol,
            "instrument_type": selected.instrument_type,
            "threshold_field": field,
            "server_time_text": snapshot.server_time_text,
            "server_time": snapshot.server_time,
            "tiers": tiers,
        }
    )
