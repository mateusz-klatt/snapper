"""Deterministic strict JSON serialization for P&L timeline opening checkpoints.

Encodes the engine's carried :class:`TimelineCheckpointOpening` state at a
settled minute boundary ``C`` into canonical strict Pydantic v2 models and
compact JSON, preserving every IEEE-754 float bit pattern via ``float.hex()``,
every :class:`ExactSum` accumulator without rounding, canonical set/map order,
and insertion order for float-summation-sensitive pool and weight mappings.
"""

import json
import math
from collections.abc import Callable
from collections.abc import Hashable
from collections.abc import Sequence
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from typing import Annotated
from typing import Final
from typing import Literal
from typing import Protocol
from typing import Self

from pydantic import AfterValidator
from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import StrictInt
from pydantic import model_validator

from snapper.application.portfolio.exact_sum import ExactSum
from snapper.application.portfolio.pnl_timeline import AttributionKey
from snapper.application.portfolio.pnl_timeline import OriginBucket
from snapper.application.portfolio.pnl_timeline import PnlIncompletenessReason
from snapper.application.portfolio.pnl_timeline import PnlIncompletenessReasonEntry
from snapper.application.portfolio.pnl_timeline import PnlWithholdingScope
from snapper.application.portfolio.pnl_timeline import PnlWithholdingTier
from snapper.application.portfolio.pnl_timeline import PoolKey
from snapper.application.portfolio.pnl_timeline import TimelineCheckpointOpening
from snapper.application.portfolio.pnl_timeline import TimelineCheckpointPool

CHECKPOINT_SCHEMA_VERSION: Final[Literal[1]] = 1
"""Current strict schema version for serialized engine opening checkpoints."""

_STRICT_CHECKPOINT_CONFIG: Final[ConfigDict] = ConfigDict(
    strict=True,
    extra="forbid",
    frozen=True,
    allow_inf_nan=False,
)
"""Strict immutable Pydantic configuration shared by all checkpoint payload models."""


class _ComparableKey(Protocol):
    """Protocol for deterministic comparison keys used in canonical ordering checks."""

    def __lt__(self, other: Self, /) -> bool:
        """Compare two keys for strict ascending order."""


def _require_nonempty(value: str) -> str:
    """Reject empty strings on canonical identifiers and venue names.

    Args:
        value: Identifier string to validate.

    Returns:
        The validated non-empty string.
    """
    if not value:
        raise ValueError("checkpoint identifier must be non-empty")
    return value


def _require_hex_float(value: str) -> str:
    """Require one canonical lowercase ``float.hex()`` representation.

    Args:
        value: Encoded IEEE-754 hexadecimal float string.

    Returns:
        The validated canonical hex float string.
    """
    try:
        decoded = float.fromhex(value)
    except (ValueError, OverflowError) as exc:
        raise ValueError("checkpoint float must be a canonical float.hex() string") from exc
    if decoded.hex() != value:
        raise ValueError("checkpoint float must be canonical float.hex()")
    return value


def _encode_float_hex(value: float) -> str:
    """Encode an int or float losslessly into ``float.hex()``, rejecting lossy values.

    Args:
        value: Numeric quantity, cost basis, or weight to encode.

    Returns:
        Canonical lowercase ``float.hex()`` string.
    """
    if isinstance(value, bool):
        raise ValueError("checkpoint numeric value cannot be a boolean")
    try:
        converted = float(value)
    except OverflowError as exc:
        raise ValueError("checkpoint numeric value overflows IEEE-754 float") from exc
    if not math.isnan(converted) and converted != value:
        raise ValueError("checkpoint numeric value cannot convert losslessly to float")
    return converted.hex()


def _require_units_str(value: str) -> str:
    """Require a canonical decimal integer string for exact unit counts.

    Args:
        value: Signed integer decimal string.

    Returns:
        The validated canonical integer string.
    """
    try:
        parsed = int(value)
    except ValueError as exc:
        raise ValueError("checkpoint exact sum units must be a decimal integer string") from exc
    if str(parsed) != value:
        raise ValueError("checkpoint exact sum units must be canonical decimal")
    return value


def _require_utc_datetime(value: datetime) -> datetime:
    """Require a timezone-aware UTC datetime normalized to ``datetime.UTC``.

    Args:
        value: Datetime to validate.

    Returns:
        The same instant with ``tzinfo=UTC``.
    """
    if value.tzinfo is None or value.utcoffset() != timedelta(0):
        raise ValueError("checkpoint datetime must be UTC")
    return value.astimezone(UTC)


def _require_utc_minute(value: datetime) -> datetime:
    """Require a UTC datetime aligned to an exact minute boundary.

    Args:
        value: Datetime to validate.

    Returns:
        The same instant with ``tzinfo=UTC``.
    """
    normalized = _require_utc_datetime(value)
    if normalized.second != 0 or normalized.microsecond != 0:
        raise ValueError("checkpoint boundary minute must be minute-aligned")
    return normalized


def _require_schema_version(value: int) -> Literal[1]:
    """Require exact supported schema version 1 without boolean coercion.

    Args:
        value: Strict integer schema version.

    Returns:
        Literal ``1``.
    """
    if value != CHECKPOINT_SCHEMA_VERSION:
        raise ValueError(f"unsupported checkpoint schema_version: {value}")
    return CHECKPOINT_SCHEMA_VERSION


def _require_infinity_sign(value: int) -> Literal[-1, 1]:
    """Require signed infinity marker ``-1`` or ``1`` without boolean coercion.

    Args:
        value: Strict integer sign marker.

    Returns:
        Literal ``-1`` or ``1``.
    """
    if value == -1:
        return -1
    if value == 1:
        return 1
    raise ValueError(f"invalid checkpoint infinity sign: {value}")


def _require_positive_scope_sequence(value: int) -> int:
    """Require a positive execution scope sequence without boolean coercion.

    Args:
        value: Strict integer scope sequence.

    Returns:
        Validated positive sequence integer.
    """
    if value < 1:
        raise ValueError("checkpoint deferred execution scope_sequence must be positive")
    return value


type NonEmptyStr = Annotated[str, AfterValidator(_require_nonempty)]
type HexFloatStr = Annotated[str, AfterValidator(_require_hex_float)]
type UnitsStr = Annotated[str, AfterValidator(_require_units_str)]
type UtcDateTime = Annotated[datetime, AfterValidator(_require_utc_datetime)]
type UtcMinuteDateTime = Annotated[datetime, AfterValidator(_require_utc_minute)]
type SchemaVersionInt = Annotated[StrictInt, AfterValidator(_require_schema_version)]
type InfinitySignInt = Annotated[StrictInt, AfterValidator(_require_infinity_sign)]
type PositiveScopeSequenceInt = Annotated[
    StrictInt, AfterValidator(_require_positive_scope_sequence)
]


def _attribution_key_tuple(attribution: AttributionKey) -> tuple[str, int, str]:
    """Return canonical deterministic sort key for one domain attribution key."""
    origin, strategy_name = attribution
    return origin, 0 if strategy_name is None else 1, strategy_name or ""


def _require_unique_keys[KeyT: Hashable](keys: Sequence[KeyT], label: str) -> None:
    """Reject duplicate keys in insertion-ordered checkpoint sequences."""
    if len(keys) != len(set(keys)):
        raise ValueError(f"duplicate keys in checkpoint {label}")


def _require_strictly_sorted[ItemT, SortKeyT: _ComparableKey](
    items: Sequence[ItemT],
    key_fn: Callable[[ItemT], SortKeyT],
    label: str,
) -> None:
    """Require items to appear in strictly increasing canonical key order."""
    keys = [key_fn(item) for item in items]
    for index in range(len(keys) - 1):
        if not keys[index] < keys[index + 1]:
            raise ValueError(f"checkpoint {label} must be strictly sorted and unique")


class ExactSumPayload(BaseModel):
    """Exact lossless serialization of one :class:`ExactSum` accumulator."""

    model_config = _STRICT_CHECKPOINT_CONFIG

    units: UnitsStr
    infinities: tuple[InfinitySignInt, ...] = ()
    saw_nan: bool = False

    @model_validator(mode="after")
    def _validate_infinities(self) -> Self:
        """Require infinity signs to be canonically sorted and unique."""
        _require_strictly_sorted(self.infinities, lambda sign: sign, "exact_sum.infinities")
        return self


class PoolKeyPayload(BaseModel):
    """Serialized durable ``(instrument_public_id, shard_key)`` pool identity."""

    model_config = _STRICT_CHECKPOINT_CONFIG

    instrument_public_id: NonEmptyStr
    shard_key: NonEmptyStr


class AttributionKeyPayload(BaseModel):
    """Serialized composite origin and signal-derived strategy key."""

    model_config = _STRICT_CHECKPOINT_CONFIG

    origin: OriginBucket
    strategy_name: str | None = None


class CheckpointPoolPayload(BaseModel):
    """Serialized carried shard pool quantity and cost basis."""

    model_config = _STRICT_CHECKPOINT_CONFIG

    instrument_public_id: NonEmptyStr
    shard_key: NonEmptyStr
    position_qty_hex: HexFloatStr
    entry_price_hex: HexFloatStr | None = None


class AttributionWeightPayload(BaseModel):
    """One attribution weight entry inside a pool's insertion-ordered map."""

    model_config = _STRICT_CHECKPOINT_CONFIG

    attribution: AttributionKeyPayload
    weight_hex: HexFloatStr


class PoolWeightsPayload(BaseModel):
    """Insertion-ordered attribution weights carried for one shard pool."""

    model_config = _STRICT_CHECKPOINT_CONFIG

    pool: PoolKeyPayload
    weights: tuple[AttributionWeightPayload, ...]

    @model_validator(mode="after")
    def _validate_unique_attributions(self) -> Self:
        """Reject duplicate attribution keys inside one pool weight map."""
        _require_unique_keys(
            [(entry.attribution.origin, entry.attribution.strategy_name) for entry in self.weights],
            "weights_by_pool.weights",
        )
        return self


class PoolBasisReasonsPayload(BaseModel):
    """Canonically sorted unknown-basis reasons carried for one open pool."""

    model_config = _STRICT_CHECKPOINT_CONFIG

    pool: PoolKeyPayload
    reasons: tuple[PnlIncompletenessReason, ...]

    @model_validator(mode="after")
    def _validate_reasons(self) -> Self:
        """Require non-empty canonically sorted unique basis reasons."""
        if not self.reasons:
            raise ValueError("checkpoint basis_reasons_by_pool entry must be non-empty")
        _require_strictly_sorted(self.reasons, lambda reason: reason, "basis_reasons_by_pool")
        return self


class IncompletenessReasonEntryPayload(BaseModel):
    """Strict serialized counterpart of :class:`PnlIncompletenessReasonEntry`."""

    model_config = _STRICT_CHECKPOINT_CONFIG

    reason: PnlIncompletenessReason
    withholding_tier: PnlWithholdingTier
    withholding_scope: PnlWithholdingScope
    trigger_instrument_public_id: str | None = None

    @model_validator(mode="after")
    def _validate_scope(self) -> Self:
        """Mirror domain guard requiring a triggering instrument for instrument scope."""
        if self.withholding_scope == "instrument" and self.trigger_instrument_public_id is None:
            raise ValueError("instrument-scoped incompleteness requires a triggering instrument")
        return self


def _pool_sort_key(pool: PoolKeyPayload) -> tuple[str, str]:
    """Return canonical sort key for one pool payload."""
    return pool.instrument_public_id, pool.shard_key


def _attribution_payload_sort_key(
    attribution: AttributionKeyPayload,
) -> tuple[str, int, str]:
    """Return canonical deterministic sort key for one attribution payload."""
    return _attribution_key_tuple((attribution.origin, attribution.strategy_name))


def _reason_entry_sort_key(
    entry: IncompletenessReasonEntryPayload,
) -> tuple[str, str, str, str]:
    """Return canonical deterministic sort key for one causal reason payload."""
    return (
        entry.withholding_scope,
        entry.trigger_instrument_public_id or "",
        entry.withholding_tier,
        entry.reason,
    )


class InstrumentUntrustedReasonsPayload(BaseModel):
    """Canonically sorted latched untrusted reasons for one instrument."""

    model_config = _STRICT_CHECKPOINT_CONFIG

    instrument_public_id: NonEmptyStr
    reasons: tuple[IncompletenessReasonEntryPayload, ...]

    @model_validator(mode="after")
    def _validate_reasons(self) -> Self:
        """Require non-empty canonically sorted unique untrusted reasons."""
        if not self.reasons:
            raise ValueError("checkpoint untrusted_reasons_by_instrument entry must be non-empty")
        _require_strictly_sorted(
            self.reasons,
            _reason_entry_sort_key,
            "untrusted_reasons_by_instrument.reasons",
        )
        return self


class ShardScopePayload(BaseModel):
    """Serialized shard-to-``(instrument, exchange)`` scope guard entry."""

    model_config = _STRICT_CHECKPOINT_CONFIG

    shard_key: NonEmptyStr
    instrument_public_id: NonEmptyStr
    exchange: NonEmptyStr


class InstrumentExchangePayload(BaseModel):
    """Serialized instrument-to-exchange scope guard entry."""

    model_config = _STRICT_CHECKPOINT_CONFIG

    instrument_public_id: NonEmptyStr
    exchange: NonEmptyStr


class InstrumentPoolKeysPayload(BaseModel):
    """Canonically sorted pool keys known for one instrument."""

    model_config = _STRICT_CHECKPOINT_CONFIG

    instrument_public_id: NonEmptyStr
    pools: tuple[PoolKeyPayload, ...]

    @model_validator(mode="after")
    def _validate_pools(self) -> Self:
        """Require pool keys to match owning instrument and appear sorted and unique."""
        for pool in self.pools:
            if pool.instrument_public_id != self.instrument_public_id:
                raise ValueError(
                    "checkpoint pool_keys_by_instrument contains mismatched instrument"
                )
        _require_strictly_sorted(self.pools, _pool_sort_key, "pool_keys_by_instrument.pools")
        return self


class InstrumentExactSumPayload(BaseModel):
    """Serialized per-instrument cumulative :class:`ExactSum` entry."""

    model_config = _STRICT_CHECKPOINT_CONFIG

    instrument_public_id: NonEmptyStr
    value: ExactSumPayload


class AttributionExactSumPayload(BaseModel):
    """Serialized per-attribution cumulative :class:`ExactSum` entry."""

    model_config = _STRICT_CHECKPOINT_CONFIG

    attribution: AttributionKeyPayload
    value: ExactSumPayload


class PoolLastEffectivePayload(BaseModel):
    """Serialized per-pool running maximum effective timestamp through ``C``.

    Retained as defence in depth, since every carried value is at or before
    ``checkpoint_minute`` and deferred fill re-passes rebuild those clamps.
    """

    model_config = _STRICT_CHECKPOINT_CONFIG

    pool: PoolKeyPayload
    last_effective_time: UtcDateTime


class ExecutionKeyPayload(BaseModel):
    """Serialized ``(exchange, scope_sequence)`` identity of one deferred fill."""

    model_config = _STRICT_CHECKPOINT_CONFIG

    exchange: NonEmptyStr
    scope_sequence: PositiveScopeSequenceInt


class PnlOpeningCheckpointState(BaseModel):
    """Version-tagged strict serialization of :class:`TimelineCheckpointOpening`."""

    model_config = _STRICT_CHECKPOINT_CONFIG

    schema_version: SchemaVersionInt
    checkpoint_minute: UtcMinuteDateTime
    activation_time: UtcMinuteDateTime | None = None
    valuation_ccy: NonEmptyStr
    pools: tuple[CheckpointPoolPayload, ...]
    weights_by_pool: tuple[PoolWeightsPayload, ...]
    seen: tuple[NonEmptyStr, ...]
    attribution_seen: tuple[AttributionKeyPayload, ...]
    basis_reasons_by_pool: tuple[PoolBasisReasonsPayload, ...]
    untrusted_reasons_by_instrument: tuple[InstrumentUntrustedReasonsPayload, ...]
    scope_by_shard: tuple[ShardScopePayload, ...]
    exchange_by_instrument: tuple[InstrumentExchangePayload, ...]
    pool_keys_by_instrument: tuple[InstrumentPoolKeysPayload, ...]
    realized_by_instrument: tuple[InstrumentExactSumPayload, ...]
    fee_by_instrument: tuple[InstrumentExactSumPayload, ...]
    accrual_by_instrument: tuple[InstrumentExactSumPayload, ...]
    realized_by_attribution: tuple[AttributionExactSumPayload, ...]
    fee_by_attribution: tuple[AttributionExactSumPayload, ...]
    accrual_by_attribution: tuple[AttributionExactSumPayload, ...]
    realized_total: ExactSumPayload
    fee_total: ExactSumPayload
    accrual_total: ExactSumPayload
    last_effective_by_pool: tuple[PoolLastEffectivePayload, ...]
    deferred_execution_keys: tuple[ExecutionKeyPayload, ...]
    carried_event_inputs: tuple[()] = ()

    @model_validator(mode="after")
    def _validate_state_invariants(self) -> Self:
        """Enforce temporal bounds, insertion-key uniqueness, pool index, and order."""
        if self.activation_time is not None and self.checkpoint_minute <= self.activation_time:
            raise ValueError("checkpoint_minute must follow activation_time")
        _require_unique_keys(
            [(pool.instrument_public_id, pool.shard_key) for pool in self.pools],
            "pools",
        )
        _require_unique_keys(
            [
                (entry.pool.instrument_public_id, entry.pool.shard_key)
                for entry in self.weights_by_pool
            ],
            "weights_by_pool",
        )
        _validate_canonical_checkpoint_collections(self)
        _validate_payload_pool_index(self)
        for entry in self.last_effective_by_pool:
            if entry.last_effective_time > self.checkpoint_minute:
                raise ValueError("checkpoint last_effective_time cannot exceed checkpoint_minute")
        return self


def _validate_payload_pool_index(state: PnlOpeningCheckpointState) -> None:
    """Require every carried pool key to match ``pool_keys_by_instrument`` and scopes."""
    pool_keys_map = {
        entry.instrument_public_id: {
            (pool.instrument_public_id, pool.shard_key) for pool in entry.pools
        }
        for entry in state.pool_keys_by_instrument
    }
    scope_map = {
        entry.shard_key: (entry.instrument_public_id, entry.exchange)
        for entry in state.scope_by_shard
    }
    exchange_map = {
        entry.instrument_public_id: entry.exchange for entry in state.exchange_by_instrument
    }
    carried_pools: tuple[PoolKey, ...] = (
        *((pool.instrument_public_id, pool.shard_key) for pool in state.pools),
        *(
            (entry.pool.instrument_public_id, entry.pool.shard_key)
            for entry in state.weights_by_pool
        ),
        *(
            (entry.pool.instrument_public_id, entry.pool.shard_key)
            for entry in state.basis_reasons_by_pool
        ),
        *(
            (entry.pool.instrument_public_id, entry.pool.shard_key)
            for entry in state.last_effective_by_pool
        ),
    )
    for instrument_public_id, shard_key in dict.fromkeys(carried_pools):
        if (instrument_public_id, shard_key) not in pool_keys_map.get(instrument_public_id, ()):
            raise ValueError("carried pool key missing from pool_keys_by_instrument")
        exchange = exchange_map.get(instrument_public_id)
        if exchange is None or scope_map.get(shard_key) != (instrument_public_id, exchange):
            raise ValueError("carried pool key has missing or inconsistent shard/exchange scope")


def _validate_canonical_checkpoint_collections(state: PnlOpeningCheckpointState) -> None:
    """Validate canonical ascending key order across all set and map collections."""
    _require_strictly_sorted(state.seen, lambda item: item, "seen")
    _require_strictly_sorted(
        state.attribution_seen, _attribution_payload_sort_key, "attribution_seen"
    )
    _require_strictly_sorted(
        state.basis_reasons_by_pool,
        lambda item: _pool_sort_key(item.pool),
        "basis_reasons_by_pool",
    )
    _require_strictly_sorted(
        state.scope_by_shard,
        lambda item: item.shard_key,
        "scope_by_shard",
    )
    _require_strictly_sorted(
        state.last_effective_by_pool,
        lambda item: _pool_sort_key(item.pool),
        "last_effective_by_pool",
    )
    _require_strictly_sorted(
        state.deferred_execution_keys,
        lambda item: (item.exchange, item.scope_sequence),
        "deferred_execution_keys",
    )
    instrument_collections: tuple[
        tuple[
            str,
            Sequence[
                InstrumentUntrustedReasonsPayload
                | InstrumentExchangePayload
                | InstrumentPoolKeysPayload
                | InstrumentExactSumPayload
            ],
        ],
        ...,
    ] = (
        ("untrusted_reasons_by_instrument", state.untrusted_reasons_by_instrument),
        ("exchange_by_instrument", state.exchange_by_instrument),
        ("pool_keys_by_instrument", state.pool_keys_by_instrument),
        ("realized_by_instrument", state.realized_by_instrument),
        ("fee_by_instrument", state.fee_by_instrument),
        ("accrual_by_instrument", state.accrual_by_instrument),
    )
    for label, entries in instrument_collections:
        _require_strictly_sorted(entries, lambda item: item.instrument_public_id, label)
    attribution_collections: tuple[tuple[str, Sequence[AttributionExactSumPayload]], ...] = (
        ("realized_by_attribution", state.realized_by_attribution),
        ("fee_by_attribution", state.fee_by_attribution),
        ("accrual_by_attribution", state.accrual_by_attribution),
    )
    for attr_label, attr_entries in attribution_collections:
        _require_strictly_sorted(
            attr_entries,
            lambda item: _attribution_payload_sort_key(item.attribution),
            attr_label,
        )


def _exact_sum_to_payload(accumulator: ExactSum) -> ExactSumPayload:
    """Encode one exact accumulator into its canonical strict payload."""
    signs: tuple[Literal[-1, 1], ...] = tuple(
        sign for sign in (-1, 1) if sign in accumulator.infinities
    )
    return ExactSumPayload(
        units=str(accumulator.units),
        infinities=signs,
        saw_nan=accumulator.saw_nan,
    )


def _exact_sum_from_payload(payload: ExactSumPayload) -> ExactSum:
    """Decode one :class:`ExactSum` accumulator from its strict payload."""
    return ExactSum(
        units=int(payload.units),
        infinities=set(payload.infinities),
        saw_nan=payload.saw_nan,
    )


def _pool_key_to_payload(pool_key: PoolKey) -> PoolKeyPayload:
    """Encode one ``(instrument_public_id, shard_key)`` tuple."""
    return PoolKeyPayload(instrument_public_id=pool_key[0], shard_key=pool_key[1])


def _pool_key_from_payload(payload: PoolKeyPayload) -> PoolKey:
    """Decode one ``(instrument_public_id, shard_key)`` tuple."""
    return (payload.instrument_public_id, payload.shard_key)


def _attribution_to_payload(key: AttributionKey) -> AttributionKeyPayload:
    """Encode one composite attribution tuple."""
    return AttributionKeyPayload(origin=key[0], strategy_name=key[1])


def _attribution_from_payload(payload: AttributionKeyPayload) -> AttributionKey:
    """Decode one composite attribution tuple."""
    return (payload.origin, payload.strategy_name)


def _encode_instrument_exact_sums(
    entries: Sequence[tuple[str, ExactSum]],
) -> tuple[InstrumentExactSumPayload, ...]:
    """Encode per-instrument exact sums in canonical instrument key order."""
    return tuple(
        InstrumentExactSumPayload(
            instrument_public_id=instrument_public_id,
            value=_exact_sum_to_payload(accumulator),
        )
        for instrument_public_id, accumulator in sorted(entries, key=lambda item: item[0])
    )


def _decode_instrument_exact_sums(
    entries: Sequence[InstrumentExactSumPayload],
) -> tuple[tuple[str, ExactSum], ...]:
    """Decode per-instrument exact sums preserving canonical tuple form."""
    return tuple(
        (entry.instrument_public_id, _exact_sum_from_payload(entry.value)) for entry in entries
    )


def _encode_attribution_exact_sums(
    entries: Sequence[tuple[AttributionKey, ExactSum]],
) -> tuple[AttributionExactSumPayload, ...]:
    """Encode per-attribution exact sums in canonical attribution order."""
    return tuple(
        AttributionExactSumPayload(
            attribution=_attribution_to_payload(key),
            value=_exact_sum_to_payload(accumulator),
        )
        for key, accumulator in sorted(entries, key=lambda item: _attribution_key_tuple(item[0]))
    )


def _decode_attribution_exact_sums(
    entries: Sequence[AttributionExactSumPayload],
) -> tuple[tuple[AttributionKey, ExactSum], ...]:
    """Decode per-attribution exact sums preserving canonical tuple form."""
    return tuple(
        (_attribution_from_payload(entry.attribution), _exact_sum_from_payload(entry.value))
        for entry in entries
    )


def _reason_to_payload(entry: PnlIncompletenessReasonEntry) -> IncompletenessReasonEntryPayload:
    """Encode one :class:`PnlIncompletenessReasonEntry` into its strict payload."""
    return IncompletenessReasonEntryPayload(
        reason=entry.reason,
        withholding_tier=entry.withholding_tier,
        withholding_scope=entry.withholding_scope,
        trigger_instrument_public_id=entry.trigger_instrument_public_id,
    )


def _reason_from_payload(payload: IncompletenessReasonEntryPayload) -> PnlIncompletenessReasonEntry:
    """Decode one :class:`PnlIncompletenessReasonEntry` from its strict payload."""
    return PnlIncompletenessReasonEntry(
        reason=payload.reason,
        withholding_tier=payload.withholding_tier,
        withholding_scope=payload.withholding_scope,
        trigger_instrument_public_id=payload.trigger_instrument_public_id,
    )


def _encode_pool_weights(
    weights_by_pool: Sequence[tuple[PoolKey, Sequence[tuple[AttributionKey, float]]]],
) -> tuple[PoolWeightsPayload, ...]:
    """Encode pool weights while preserving outer and inner insertion order."""
    return tuple(
        PoolWeightsPayload(
            pool=_pool_key_to_payload(pool_key),
            weights=tuple(
                AttributionWeightPayload(
                    attribution=_attribution_to_payload(attribution_key),
                    weight_hex=_encode_float_hex(weight),
                )
                for attribution_key, weight in entries
            ),
        )
        for pool_key, entries in weights_by_pool
    )


def _encode_untrusted_reasons(
    untrusted_by_instrument: Sequence[tuple[str, Sequence[PnlIncompletenessReasonEntry]]],
) -> tuple[InstrumentUntrustedReasonsPayload, ...]:
    """Encode latched instrument untrusted reasons in canonical order."""
    return tuple(
        InstrumentUntrustedReasonsPayload(
            instrument_public_id=instrument_public_id,
            reasons=tuple(
                _reason_to_payload(reason)
                for reason in sorted(
                    reasons,
                    key=lambda item: (
                        item.withholding_scope,
                        item.trigger_instrument_public_id or "",
                        item.withholding_tier,
                        item.reason,
                    ),
                )
            ),
        )
        for instrument_public_id, reasons in sorted(
            untrusted_by_instrument,
            key=lambda item: item[0],
        )
    )


def checkpoint_to_payload(checkpoint: TimelineCheckpointOpening) -> PnlOpeningCheckpointState:
    """Convert an engine checkpoint opening into its strict Pydantic v2 payload.

    Args:
        checkpoint: Carried engine checkpoint opening captured at minute ``C``.

    Returns:
        The validated strict Pydantic checkpoint state payload.
    """
    return PnlOpeningCheckpointState(
        schema_version=CHECKPOINT_SCHEMA_VERSION,
        checkpoint_minute=checkpoint.checkpoint_minute,
        activation_time=checkpoint.activation_time,
        valuation_ccy=checkpoint.valuation_ccy,
        pools=tuple(
            CheckpointPoolPayload(
                instrument_public_id=pool.instrument_public_id,
                shard_key=pool.shard_key,
                position_qty_hex=_encode_float_hex(pool.position_qty),
                entry_price_hex=(
                    None if pool.entry_price is None else _encode_float_hex(pool.entry_price)
                ),
            )
            for pool in checkpoint.pools
        ),
        weights_by_pool=_encode_pool_weights(checkpoint.weights_by_pool),
        seen=tuple(sorted(checkpoint.seen)),
        attribution_seen=tuple(
            _attribution_to_payload(key)
            for key in sorted(checkpoint.attribution_seen, key=_attribution_key_tuple)
        ),
        basis_reasons_by_pool=tuple(
            PoolBasisReasonsPayload(
                pool=_pool_key_to_payload(pool_key),
                reasons=tuple(sorted(reasons)),
            )
            for pool_key, reasons in sorted(
                checkpoint.basis_reasons_by_pool,
                key=lambda item: item[0],
            )
        ),
        untrusted_reasons_by_instrument=_encode_untrusted_reasons(
            checkpoint.untrusted_reasons_by_instrument
        ),
        scope_by_shard=tuple(
            ShardScopePayload(
                shard_key=shard_key,
                instrument_public_id=scope[0],
                exchange=scope[1],
            )
            for shard_key, scope in sorted(checkpoint.scope_by_shard, key=lambda item: item[0])
        ),
        exchange_by_instrument=tuple(
            InstrumentExchangePayload(instrument_public_id=instrument_public_id, exchange=exchange)
            for instrument_public_id, exchange in sorted(
                checkpoint.exchange_by_instrument,
                key=lambda item: item[0],
            )
        ),
        pool_keys_by_instrument=tuple(
            InstrumentPoolKeysPayload(
                instrument_public_id=instrument_public_id,
                pools=tuple(_pool_key_to_payload(pool_key) for pool_key in sorted(pool_keys)),
            )
            for instrument_public_id, pool_keys in sorted(
                checkpoint.pool_keys_by_instrument,
                key=lambda item: item[0],
            )
        ),
        realized_by_instrument=_encode_instrument_exact_sums(checkpoint.realized_by_instrument),
        fee_by_instrument=_encode_instrument_exact_sums(checkpoint.fee_by_instrument),
        accrual_by_instrument=_encode_instrument_exact_sums(checkpoint.accrual_by_instrument),
        realized_by_attribution=_encode_attribution_exact_sums(checkpoint.realized_by_attribution),
        fee_by_attribution=_encode_attribution_exact_sums(checkpoint.fee_by_attribution),
        accrual_by_attribution=_encode_attribution_exact_sums(checkpoint.accrual_by_attribution),
        realized_total=_exact_sum_to_payload(checkpoint.realized_total),
        fee_total=_exact_sum_to_payload(checkpoint.fee_total),
        accrual_total=_exact_sum_to_payload(checkpoint.accrual_total),
        last_effective_by_pool=tuple(
            PoolLastEffectivePayload(
                pool=_pool_key_to_payload(pool_key),
                last_effective_time=effective_time,
            )
            for pool_key, effective_time in sorted(
                checkpoint.last_effective_by_pool,
                key=lambda item: item[0],
            )
        ),
        deferred_execution_keys=tuple(
            ExecutionKeyPayload(exchange=exchange, scope_sequence=scope_sequence)
            for exchange, scope_sequence in sorted(checkpoint.deferred_execution_keys)
        ),
    )


def checkpoint_from_payload(payload: PnlOpeningCheckpointState) -> TimelineCheckpointOpening:
    """Convert a strict Pydantic v2 checkpoint payload into a :class:`TimelineCheckpointOpening`.

    Args:
        payload: Validated strict Pydantic checkpoint payload.

    Returns:
        The restored immutable engine checkpoint opening.
    """
    return TimelineCheckpointOpening(
        checkpoint_minute=payload.checkpoint_minute,
        activation_time=payload.activation_time,
        valuation_ccy=payload.valuation_ccy,
        pools=tuple(
            TimelineCheckpointPool(
                instrument_public_id=pool.instrument_public_id,
                shard_key=pool.shard_key,
                position_qty=float.fromhex(pool.position_qty_hex),
                entry_price=(
                    None if pool.entry_price_hex is None else float.fromhex(pool.entry_price_hex)
                ),
            )
            for pool in payload.pools
        ),
        weights_by_pool=tuple(
            (
                _pool_key_from_payload(entry.pool),
                tuple(
                    (
                        _attribution_from_payload(weight_entry.attribution),
                        float.fromhex(weight_entry.weight_hex),
                    )
                    for weight_entry in entry.weights
                ),
            )
            for entry in payload.weights_by_pool
        ),
        seen=payload.seen,
        attribution_seen=tuple(
            _attribution_from_payload(entry) for entry in payload.attribution_seen
        ),
        basis_reasons_by_pool=tuple(
            (_pool_key_from_payload(entry.pool), entry.reasons)
            for entry in payload.basis_reasons_by_pool
        ),
        untrusted_reasons_by_instrument=tuple(
            (
                entry.instrument_public_id,
                tuple(_reason_from_payload(reason) for reason in entry.reasons),
            )
            for entry in payload.untrusted_reasons_by_instrument
        ),
        scope_by_shard=tuple(
            (entry.shard_key, (entry.instrument_public_id, entry.exchange))
            for entry in payload.scope_by_shard
        ),
        exchange_by_instrument=tuple(
            (entry.instrument_public_id, entry.exchange) for entry in payload.exchange_by_instrument
        ),
        pool_keys_by_instrument=tuple(
            (
                entry.instrument_public_id,
                tuple(_pool_key_from_payload(pool) for pool in entry.pools),
            )
            for entry in payload.pool_keys_by_instrument
        ),
        realized_by_instrument=_decode_instrument_exact_sums(payload.realized_by_instrument),
        fee_by_instrument=_decode_instrument_exact_sums(payload.fee_by_instrument),
        accrual_by_instrument=_decode_instrument_exact_sums(payload.accrual_by_instrument),
        realized_by_attribution=_decode_attribution_exact_sums(payload.realized_by_attribution),
        fee_by_attribution=_decode_attribution_exact_sums(payload.fee_by_attribution),
        accrual_by_attribution=_decode_attribution_exact_sums(payload.accrual_by_attribution),
        realized_total=_exact_sum_from_payload(payload.realized_total),
        fee_total=_exact_sum_from_payload(payload.fee_total),
        accrual_total=_exact_sum_from_payload(payload.accrual_total),
        last_effective_by_pool=tuple(
            (_pool_key_from_payload(entry.pool), entry.last_effective_time)
            for entry in payload.last_effective_by_pool
        ),
        deferred_execution_keys=tuple(
            (entry.exchange, entry.scope_sequence) for entry in payload.deferred_execution_keys
        ),
    )


def _unique_json_pairs(pairs: list[tuple[str, object]]) -> dict[str, object]:
    """Build a JSON object while rejecting duplicate keys."""
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON key in checkpoint: {key}")
        result[key] = value
    return result


def _reject_json_constant(constant: str) -> object:
    """Reject non-standard JSON floating constants such as NaN and Infinity."""
    raise ValueError(f"invalid non-finite JSON constant in checkpoint: {constant}")


def checkpoint_to_json(checkpoint: TimelineCheckpointOpening) -> str:
    """Serialize one engine checkpoint opening into deterministic canonical JSON.

    Args:
        checkpoint: Carried engine checkpoint opening captured at minute ``C``.

    Returns:
        Compact canonical JSON string encoding every float via ``float.hex()``
        and every :class:`ExactSum` losslessly.
    """
    payload = checkpoint_to_payload(checkpoint)
    return json.dumps(
        payload.model_dump(mode="json"),
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    )


def checkpoint_from_json(raw: str) -> TimelineCheckpointOpening:
    """Strictly parse and validate an engine checkpoint opening from JSON.

    Args:
        raw: Canonical JSON representation of a checkpoint payload.

    Returns:
        The validated immutable :class:`TimelineCheckpointOpening`.

    Raises:
        ValueError: When JSON syntax, duplicate keys, schema version, strict
            types, canonical ordering, or temporal invariants fail validation.
    """
    json.loads(
        raw,
        object_pairs_hook=_unique_json_pairs,
        parse_constant=_reject_json_constant,
    )
    payload = PnlOpeningCheckpointState.model_validate_json(raw, strict=True)
    return checkpoint_from_payload(payload)
