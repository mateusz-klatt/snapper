"""Interpret account-specific product flags without asserting identity or order permission.

The caller must retain the original response and independently prove its actual
authenticated request, selected key, account, clocks and latest-attempt status.
These pure values cannot establish any of those facts or authorize venue I/O.
Unknown provider fields remain in the original response rather than becoming
invented limits, margin requirements or collateral values.
"""

from collections.abc import Mapping
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from typing import Literal
from typing import Self

from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import Field
from pydantic import model_validator

from snapper.core.execution_unit_snapshot import EvidenceInstant
from snapper.core.execution_unit_snapshot import EvidenceText
from snapper.infrastructure.exchanges.execution_wire_receipts import (
    decode_private_execution_receipt,
)
from snapper.infrastructure.exchanges.execution_wire_receipts import is_execution_control_message

_MAX_RESPONSE_BYTES = 8 * 1024 * 1024


class FuturesProductSourceError(ValueError):
    """The response cannot establish a complete unambiguous product-flag observation."""


class _ProductObservation(BaseModel):
    """Freeze interpreted source fields while refusing coercion and unchecked copied values."""

    model_config = ConfigDict(
        frozen=True,
        strict=True,
        extra="forbid",
        revalidate_instances="always",
        hide_input_in_errors=True,
    )


class FuturesTradingInstrument(_ProductObservation):
    """One exact provider symbol with unknown flags retained as unknown rather than false."""

    wire_symbol: EvidenceText
    instrument_type: EvidenceText
    expired: bool | None
    suspended: bool | None
    post_only: bool | None
    restricted: bool | None


class FuturesTradingInstrumentsSnapshot(_ProductObservation):
    """A complete parsed response without authenticated provenance or current validity."""

    parser_policy: Literal["kraken_futures_trading_instruments_v1"] = (
        "kraken_futures_trading_instruments_v1"
    )
    server_time_text: EvidenceText
    server_time: EvidenceInstant
    instruments: tuple[FuturesTradingInstrument, ...] = Field(max_length=16384)

    @model_validator(mode="after")
    def validate_source_identity(self) -> Self:
        """Keep exact product identities and the provider's parsed/text clock mutually consistent.

        Returns:
            The same complete observation with one entry per exact exchange symbol.
        """
        names = [entry.wire_symbol for entry in self.instruments]
        if len(set(names)) != len(names):
            raise FuturesProductSourceError("futures_product_duplicate_wire_symbol")
        if _server_time(self.server_time_text)[1] != self.server_time:
            raise FuturesProductSourceError("futures_product_server_time_mismatch")
        return self


class FuturesProductRequirements(_ProductObservation):
    """Necessary additional order conditions, never a permit or proof that they are satisfied."""

    wire_symbol: EvidenceText
    instrument_type: EvidenceText
    requires_post_only: bool
    requires_position_reduction: bool


def _object(value: object) -> Mapping[str, object]:
    """Require an original JSON object without changing any provider identity."""
    if not isinstance(value, Mapping) or not all(isinstance(key, str) for key in value):
        raise FuturesProductSourceError("futures_product_object_required")
    return value


def _text(value: object) -> str:
    """Preserve exact spelling; trimming or case normalization cannot establish symbol equivalence."""
    if not isinstance(value, str) or not value.strip():
        raise FuturesProductSourceError("futures_product_text_required")
    return value


def _flag(source: Mapping[str, object], name: str) -> bool | None:
    """Retain absent/null flags as unknown and refuse numeric or textual Boolean aliases."""
    value = source.get(name)
    if value is None or isinstance(value, bool):
        return value
    raise FuturesProductSourceError("futures_product_boolean_required")


def _instrument(value: object) -> FuturesTradingInstrument:
    """Project only documented product identity and explicit restriction flags."""
    source = _object(value)
    return FuturesTradingInstrument(
        wire_symbol=_text(source.get("symbol")),
        instrument_type=_text(source.get("type")),
        expired=_flag(source, "isExpired"),
        suspended=_flag(source, "suspended"),
        post_only=_flag(source, "postOnly"),
        restricted=_flag(source, "restricted"),
    )


def _server_time(value: object) -> tuple[str, datetime]:
    """Retain the original documented UTC clock without using it as local request knowledge."""
    source = _text(value)
    try:
        parsed = datetime.fromisoformat(source)
    except ValueError:
        raise FuturesProductSourceError("futures_product_server_time_invalid") from None
    if parsed.utcoffset() != timedelta(0):
        raise FuturesProductSourceError("futures_product_server_time_not_utc")
    return source, parsed.astimezone(UTC)


def parse_futures_trading_instruments(wire_bytes: bytes) -> FuturesTradingInstrumentsSnapshot:
    """Parse an exact HTTP entity while preserving unknown restrictions and refusing ambiguity.

    Args:
        wire_bytes: Original UTF-8 response entity before JSON or numeric normalization.

    Returns:
        Source observations that still require actual authenticated provenance and fresh binding.

    Raises:
        FuturesProductSourceError: When source syntax, completeness or product identity is invalid.
    """
    if not isinstance(wire_bytes, bytes) or len(wire_bytes) > _MAX_RESPONSE_BYTES:
        raise FuturesProductSourceError("futures_product_original_entity_required")
    try:
        source = decode_private_execution_receipt(wire_bytes)
    except (ValueError, UnicodeError, RecursionError):
        raise FuturesProductSourceError("futures_product_original_json_invalid") from None
    if is_execution_control_message(source) or source.get("result") != "success":
        raise FuturesProductSourceError("futures_product_success_unproven")
    values = source.get("instruments")
    if not isinstance(values, list):
        raise FuturesProductSourceError("futures_product_complete_list_required")
    time_text, server_time = _server_time(source.get("serverTime"))
    return FuturesTradingInstrumentsSnapshot(
        server_time_text=time_text,
        server_time=server_time,
        instruments=tuple(_instrument(value) for value in values),
    )


def futures_product_requirements(
    snapshot: FuturesTradingInstrumentsSnapshot, wire_symbol: str
) -> FuturesProductRequirements:
    """Require known current-status flags and report conditions that still need independent proof.

    Args:
        snapshot: Complete strict parsed observation, independently authenticated by its caller.
        wire_symbol: Exact exchange symbol already selected for the original outgoing order.

    Returns:
        Necessary post-only and position-reduction conditions without authorizing any order.

    Raises:
        FuturesProductSourceError: When the selected product is absent, unknown, expired or suspended.
    """
    proven = FuturesTradingInstrumentsSnapshot.model_validate(snapshot)
    entries = [entry for entry in proven.instruments if entry.wire_symbol == wire_symbol]
    if len(entries) != 1:
        raise FuturesProductSourceError("futures_product_exact_symbol_missing")
    entry = entries[0]
    if None in (entry.expired, entry.suspended, entry.post_only, entry.restricted):
        raise FuturesProductSourceError("futures_product_status_unknown")
    if entry.expired or entry.suspended:
        raise FuturesProductSourceError("futures_product_not_accepting_orders")
    return FuturesProductRequirements(
        wire_symbol=entry.wire_symbol,
        instrument_type=entry.instrument_type,
        requires_post_only=entry.post_only is True,
        requires_position_reduction=entry.restricted is True,
    )
