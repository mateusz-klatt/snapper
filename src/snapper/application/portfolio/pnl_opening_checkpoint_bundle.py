"""Strict versioned envelope for engine state and service-side replay prefixes."""

import json
from collections.abc import Mapping
from collections.abc import Sequence
from datetime import datetime
from typing import Annotated
from typing import Final
from typing import Literal
from typing import Self

from pydantic import AfterValidator
from pydantic import BaseModel
from pydantic import StrictInt
from pydantic import field_validator
from pydantic import model_validator

from snapper.application.portfolio.pnl_fx_replay_state import _EventFxReplayState
from snapper.application.portfolio.pnl_fx_replay_state import _FxInstrumentRequirements
from snapper.application.portfolio.pnl_fx_replay_state import _MarkReplayState
from snapper.application.portfolio.pnl_opening_checkpoint import _STRICT_CHECKPOINT_CONFIG
from snapper.application.portfolio.pnl_opening_checkpoint import CarriedNumberPayload
from snapper.application.portfolio.pnl_opening_checkpoint import NonEmptyStr
from snapper.application.portfolio.pnl_opening_checkpoint import PnlOpeningCheckpointState
from snapper.application.portfolio.pnl_opening_checkpoint import PoolKeyPayload
from snapper.application.portfolio.pnl_opening_checkpoint import PoolLastEffectivePayload
from snapper.application.portfolio.pnl_opening_checkpoint import UtcDateTime
from snapper.application.portfolio.pnl_opening_checkpoint import _decode_carried_number
from snapper.application.portfolio.pnl_opening_checkpoint import _encode_carried_number
from snapper.application.portfolio.pnl_opening_checkpoint import _pool_key_from_payload
from snapper.application.portfolio.pnl_opening_checkpoint import _pool_key_to_payload
from snapper.application.portfolio.pnl_opening_checkpoint import _reject_json_constant
from snapper.application.portfolio.pnl_opening_checkpoint import _require_strictly_sorted
from snapper.application.portfolio.pnl_opening_checkpoint import _unique_json_pairs
from snapper.data.pnl_correction_state import IdAggregate

BUNDLE_VERSION: Final[Literal[4]] = 4
"""Current strict envelope version embedding scope, generation identity, and FX elections."""


def _require_checkpoint_mode(value: str) -> str:
    """Require a valid checkpoint trading mode without coercion.

    Args:
        value: Mode string to validate.

    Returns:
        The validated ``"live"`` or ``"paper"`` mode string.

    Raises:
        ValueError: When ``value`` is not ``"live"`` or ``"paper"``.
    """
    if value not in ("live", "paper"):
        raise ValueError("checkpoint mode must be 'live' or 'paper'")
    return value


def _require_positive_generation(value: int) -> int:
    """Require a positive source generation without boolean coercion.

    Args:
        value: Strict integer generation number.

    Returns:
        The validated positive generation integer.

    Raises:
        ValueError: When ``value`` is less than 1.
    """
    if value < 1:
        raise ValueError("checkpoint source_generation must be positive")
    return value


def _require_non_negative_int(value: int) -> int:
    """Require a non-negative integer cursor without boolean coercion.

    Args:
        value: Strict integer cursor value.

    Returns:
        The validated non-negative integer.

    Raises:
        ValueError: When ``value`` is negative.
    """
    if value < 0:
        raise ValueError("checkpoint event_index must be non-negative")
    return value


def _require_canonical_decimal_int(value: str) -> str:
    """Require one canonical base-ten integer string without coercion.

    Args:
        value: Exact integer payload to validate.

    Returns:
        The unchanged canonical decimal string.

    Raises:
        ValueError: When the value is not canonical decimal notation.
    """
    try:
        parsed = int(value)
    except ValueError as exc:
        raise ValueError("checkpoint aggregate integer must be canonical decimal") from exc
    if str(parsed) != value:
        raise ValueError("checkpoint aggregate integer must be canonical decimal")
    return value


type ExactDecimalIntStr = Annotated[str, AfterValidator(_require_canonical_decimal_int)]
"""Canonical exact decimal integer used by row-identity aggregates."""

type CheckpointModeStr = Annotated[str, AfterValidator(_require_checkpoint_mode)]
"""Strict checkpoint trading mode (``"live"`` or ``"paper"``)."""

type PositiveGenerationInt = Annotated[StrictInt, AfterValidator(_require_positive_generation)]
"""Strict positive source generation integer."""

type NonNegativeInt = Annotated[StrictInt, AfterValidator(_require_non_negative_int)]
"""Strict non-negative replay cursor integer."""


class PnlCheckpointBundleIdentityPayload(BaseModel):
    """Scope, calculation version, and generation identity bound inside ``state_json``."""

    model_config = _STRICT_CHECKPOINT_CONFIG

    wallet_public_id: NonEmptyStr
    mode: CheckpointModeStr
    valuation_ccy: NonEmptyStr
    epoch_public_id: NonEmptyStr
    calc_version: NonEmptyStr
    source_generation: PositiveGenerationInt


def _require_ordered_currency_pair(value: tuple[str, str], field_name: str) -> tuple[str, str]:
    """Require lexically ordered distinct currency legs for a canonical pair."""
    if value[0] >= value[1]:
        raise ValueError(f"checkpoint {field_name} pair must satisfy pair[0] < pair[1]")
    return value


class PnlEventFxRequirementPayload(BaseModel):
    """Exact event-FX minutes required for one normalized currency pair."""

    model_config = _STRICT_CHECKPOINT_CONFIG

    pair: tuple[NonEmptyStr, NonEmptyStr]
    minutes: tuple[UtcDateTime, ...]

    @field_validator("pair")
    @classmethod
    def _validate_pair(cls, value: tuple[str, str]) -> tuple[str, str]:
        """Require lexically ordered distinct currency legs."""
        return _require_ordered_currency_pair(value, "event_walk_state")

    @field_validator("minutes")
    @classmethod
    def _validate_minutes(cls, value: tuple[datetime, ...]) -> tuple[datetime, ...]:
        """Require event-FX requirement minutes in strictly sorted unique order."""
        _require_strictly_sorted(value, lambda minute: minute, "event_walk_state.minutes")
        return value


class PnlEventFxInstrumentRequirementsPayload(BaseModel):
    """All event-FX pair requirements carried for one instrument."""

    model_config = _STRICT_CHECKPOINT_CONFIG

    instrument_public_id: NonEmptyStr
    requirements: tuple[PnlEventFxRequirementPayload, ...]

    @field_validator("requirements")
    @classmethod
    def _validate_pairs(
        cls,
        value: tuple[PnlEventFxRequirementPayload, ...],
    ) -> tuple[PnlEventFxRequirementPayload, ...]:
        """Require pair entries in strictly sorted unique pair order."""
        _require_strictly_sorted(value, lambda item: item.pair, "event_walk_state.pairs")
        return value


class PnlEventFxPoolQuantityPayload(BaseModel):
    """Canonical carried position quantity for one durable pool."""

    model_config = _STRICT_CHECKPOINT_CONFIG

    pool: PoolKeyPayload
    position_qty: CarriedNumberPayload


class PnlEventFxUntrustedPayload(BaseModel):
    """Effective time at which one instrument became permanently untrusted."""

    model_config = _STRICT_CHECKPOINT_CONFIG

    instrument_public_id: NonEmptyStr
    untrusted_at: UtcDateTime


class PnlEventFxReplayStatePayload(BaseModel):
    """Complete carried state of the event-FX requirement walk."""

    model_config = _STRICT_CHECKPOINT_CONFIG

    requirements: tuple[PnlEventFxInstrumentRequirementsPayload, ...]
    conversion_requirements: tuple[PnlEventFxInstrumentRequirementsPayload, ...]
    last_effective: tuple[PoolLastEffectivePayload, ...]
    position_qty: tuple[PnlEventFxPoolQuantityPayload, ...]
    basis_unknown: tuple[PoolKeyPayload, ...]
    untrusted_at: tuple[PnlEventFxUntrustedPayload, ...]

    @model_validator(mode="after")
    def _validate_canonical_keys(self) -> Self:
        """Require strictly sorted unique keys across every event-walk collection."""
        _require_strictly_sorted(
            self.requirements,
            lambda item: item.instrument_public_id,
            "event_walk_state.requirements",
        )
        _require_strictly_sorted(
            self.conversion_requirements,
            lambda item: item.instrument_public_id,
            "event_walk_state.conversion_requirements",
        )
        _require_strictly_sorted(
            self.last_effective,
            lambda item: (item.pool.instrument_public_id, item.pool.shard_key),
            "event_walk_state.last_effective",
        )
        _require_strictly_sorted(
            self.position_qty,
            lambda item: (item.pool.instrument_public_id, item.pool.shard_key),
            "event_walk_state.position_qty",
        )
        _require_strictly_sorted(
            self.basis_unknown,
            lambda item: (item.instrument_public_id, item.shard_key),
            "event_walk_state.basis_unknown",
        )
        _require_strictly_sorted(
            self.untrusted_at,
            lambda item: item.instrument_public_id,
            "event_walk_state.untrusted_at",
        )
        return self


class PnlMarkPoolQuantityPayload(BaseModel):
    """Canonical carried mark-walk quantity for one instrument shard."""

    model_config = _STRICT_CHECKPOINT_CONFIG

    shard_key: NonEmptyStr
    quantity: CarriedNumberPayload


class PnlMarkInstrumentReplayStatePayload(BaseModel):
    """Complete mark replay cursor, quantities, and invalid-event latch."""

    model_config = _STRICT_CHECKPOINT_CONFIG

    instrument_public_id: NonEmptyStr
    event_index: NonNegativeInt
    pool_quantities: tuple[PnlMarkPoolQuantityPayload, ...]
    mark_eligible: bool

    @field_validator("pool_quantities")
    @classmethod
    def _validate_pool_quantities(
        cls,
        value: tuple[PnlMarkPoolQuantityPayload, ...],
    ) -> tuple[PnlMarkPoolQuantityPayload, ...]:
        """Require shard quantities in strictly sorted unique shard-key order."""
        _require_strictly_sorted(
            value,
            lambda item: item.shard_key,
            "mark_walk_state.pool_quantities",
        )
        return value


class PnlRegressionShadowPayload(BaseModel):
    """Half-open mark suppression interval from one out-of-order execution."""

    model_config = _STRICT_CHECKPOINT_CONFIG

    event_time: UtcDateTime
    previous_effective: UtcDateTime

    @model_validator(mode="after")
    def _validate_interval(self) -> Self:
        """Require a non-empty half-open regression shadow interval."""
        if self.event_time >= self.previous_effective:
            raise ValueError(
                "checkpoint regression shadow event_time must precede previous_effective"
            )
        return self


class PnlMarkFxReplayStatePayload(BaseModel):
    """Per-instrument mark states and every global regression shadow."""

    model_config = _STRICT_CHECKPOINT_CONFIG

    instruments: tuple[PnlMarkInstrumentReplayStatePayload, ...]
    regression_shadows: tuple[PnlRegressionShadowPayload, ...]

    @field_validator("instruments")
    @classmethod
    def _validate_instruments(
        cls,
        value: tuple[PnlMarkInstrumentReplayStatePayload, ...],
    ) -> tuple[PnlMarkInstrumentReplayStatePayload, ...]:
        """Require mark instrument states in strictly sorted unique instrument order."""
        _require_strictly_sorted(
            value,
            lambda item: item.instrument_public_id,
            "mark_walk_state.instruments",
        )
        return value


class PnlFxRateSourcePayload(BaseModel):
    """Strict serialized counterpart of every ``PnlFxRateSource`` field."""

    model_config = _STRICT_CHECKPOINT_CONFIG

    source_currency: NonEmptyStr
    valuation_currency: NonEmptyStr
    base_currency: NonEmptyStr
    quote_currency: NonEmptyStr
    exchange: NonEmptyStr


class PnlIdAggregatePayload(BaseModel):
    """Exact strict JSON shape of one row-identity aggregate."""

    model_config = _STRICT_CHECKPOINT_CONFIG

    count: StrictInt
    id_sum: ExactDecimalIntStr
    max_id: StrictInt
    max_timestamp: UtcDateTime | None

    @model_validator(mode="after")
    def _validate_aggregate(self) -> Self:
        """Apply the domain aggregate's non-negativity and zero coherence rules."""
        IdAggregate(
            count=self.count,
            id_sum=int(self.id_sum),
            max_id=self.max_id,
            max_timestamp=self.max_timestamp,
        )
        return self


class PnlEventFxCandlePlanePayload(BaseModel):
    """Exact candle aggregate carried for one ``(base, quote, exchange)`` plane."""

    model_config = _STRICT_CHECKPOINT_CONFIG

    base: NonEmptyStr
    quote: NonEmptyStr
    exchange: NonEmptyStr
    aggregate: PnlIdAggregatePayload


class PnlRefBindingPayload(BaseModel):
    """Canonical symbol-reference projection bound to an instrument at capture."""

    model_config = _STRICT_CHECKPOINT_CONFIG

    native_symbol: NonEmptyStr
    exchange: NonEmptyStr
    instrument_exchange: NonEmptyStr
    base_currency: NonEmptyStr
    quote_currency: NonEmptyStr


class PnlPrefixRefBindingPayload(BaseModel):
    """Trusted market-reference binding (or absence thereof) carried for one instrument."""

    model_config = _STRICT_CHECKPOINT_CONFIG

    instrument_public_id: NonEmptyStr
    binding: PnlRefBindingPayload | None


class PnlPrefixFxElectionPayload(BaseModel):
    """Exact FX plane elected at capture for one prefix consumer and minute."""

    model_config = _STRICT_CHECKPOINT_CONFIG

    instrument_public_id: NonEmptyStr
    pair: tuple[NonEmptyStr, NonEmptyStr]
    minute: UtcDateTime
    elected_plane: tuple[NonEmptyStr, NonEmptyStr, NonEmptyStr] | None

    @field_validator("pair")
    @classmethod
    def _validate_pair(cls, value: tuple[str, str]) -> tuple[str, str]:
        """Require lexically ordered distinct currency legs."""
        return _require_ordered_currency_pair(value, "prefix_fx_elections")

    @model_validator(mode="after")
    def _validate_elected_plane_pair(self) -> Self:
        """Require any elected plane to belong to the declared unordered pair."""
        if self.elected_plane is not None:
            plane_pair = tuple(sorted((self.elected_plane[0], self.elected_plane[1])))
            if plane_pair != self.pair:
                raise ValueError("checkpoint prefix_fx_elections plane does not match pair")
        return self


class PnlPrefixFxCoveragePayload(BaseModel):
    """Usable FX candle minutes observed per plane at capture without reloading prefix."""

    model_config = _STRICT_CHECKPOINT_CONFIG

    base: NonEmptyStr
    quote: NonEmptyStr
    exchange: NonEmptyStr
    minutes: tuple[UtcDateTime, ...]

    @field_validator("minutes")
    @classmethod
    def _validate_minutes(cls, value: tuple[datetime, ...]) -> tuple[datetime, ...]:
        """Require covered minutes in strictly sorted unique order."""
        _require_strictly_sorted(value, lambda minute: minute, "prefix_fx_coverage.minutes")
        return value


class PnlCheckpointStateBundle(BaseModel):
    """Versioned engine and service replay state captured at one boundary."""

    model_config = _STRICT_CHECKPOINT_CONFIG

    bundle_version: Literal[4]
    identity: PnlCheckpointBundleIdentityPayload
    engine_state: PnlOpeningCheckpointState
    price_ref_spans_by_instrument: dict[NonEmptyStr, tuple[UtcDateTime, UtcDateTime]]
    display_activity_spans_by_instrument: dict[NonEmptyStr, tuple[UtcDateTime, UtcDateTime]]
    rate_sources_prefix: tuple[PnlFxRateSourcePayload, ...]
    prefix_lineage_order_ids: tuple[NonEmptyStr, ...]
    event_walk_state: PnlEventFxReplayStatePayload
    mark_walk_state: PnlMarkFxReplayStatePayload
    event_fx_candles_by_plane: tuple[PnlEventFxCandlePlanePayload, ...]
    prefix_fx_elections: tuple[PnlPrefixFxElectionPayload, ...]
    prefix_ref_bindings: tuple[PnlPrefixRefBindingPayload, ...]
    prefix_fx_coverage: tuple[PnlPrefixFxCoveragePayload, ...]

    @field_validator("price_ref_spans_by_instrument", "display_activity_spans_by_instrument")
    @classmethod
    def _validate_spans(
        cls,
        value: dict[str, tuple[datetime, datetime]],
    ) -> dict[str, tuple[datetime, datetime]]:
        """Require every carried activity span to satisfy ``start <= end``."""
        for start, end in value.values():
            if start > end:
                raise ValueError("checkpoint span start cannot exceed end")
        return value

    @field_validator("rate_sources_prefix")
    @classmethod
    def _validate_rate_sources(
        cls, value: tuple[PnlFxRateSourcePayload, ...]
    ) -> tuple[PnlFxRateSourcePayload, ...]:
        """Require rate sources in canonical ascending field order."""
        _require_strictly_sorted(
            value,
            lambda item: (
                item.source_currency,
                item.valuation_currency,
                item.base_currency,
                item.quote_currency,
                item.exchange,
            ),
            "rate_sources_prefix",
        )
        return value

    @field_validator("prefix_lineage_order_ids")
    @classmethod
    def _validate_lineage_ids(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        """Require lineage identifiers in canonical sorted unique order."""
        _require_strictly_sorted(value, lambda item: item, "prefix_lineage_order_ids")
        return value

    @field_validator("event_fx_candles_by_plane")
    @classmethod
    def _validate_candle_planes(
        cls, value: tuple[PnlEventFxCandlePlanePayload, ...]
    ) -> tuple[PnlEventFxCandlePlanePayload, ...]:
        """Require plane aggregates in canonical ascending plane order."""
        _require_strictly_sorted(
            value,
            lambda item: (item.base, item.quote, item.exchange),
            "event_fx_candles_by_plane",
        )
        return value

    @field_validator("prefix_fx_elections")
    @classmethod
    def _validate_prefix_fx_elections(
        cls, value: tuple[PnlPrefixFxElectionPayload, ...]
    ) -> tuple[PnlPrefixFxElectionPayload, ...]:
        """Require prefix FX elections in canonical ascending consumer-pair-minute order."""
        _require_strictly_sorted(
            value,
            lambda item: (item.instrument_public_id, item.pair, item.minute),
            "prefix_fx_elections",
        )
        return value

    @field_validator("prefix_ref_bindings")
    @classmethod
    def _validate_prefix_ref_bindings(
        cls, value: tuple[PnlPrefixRefBindingPayload, ...]
    ) -> tuple[PnlPrefixRefBindingPayload, ...]:
        """Require prefix ref bindings in canonical sorted unique instrument order."""
        _require_strictly_sorted(
            value,
            lambda item: item.instrument_public_id,
            "prefix_ref_bindings",
        )
        return value

    @field_validator("prefix_fx_coverage")
    @classmethod
    def _validate_prefix_fx_coverage(
        cls, value: tuple[PnlPrefixFxCoveragePayload, ...]
    ) -> tuple[PnlPrefixFxCoveragePayload, ...]:
        """Require prefix FX coverage summaries in canonical sorted plane order."""
        _require_strictly_sorted(
            value,
            lambda item: (item.base, item.quote, item.exchange),
            "prefix_fx_coverage",
        )
        return value


def _encode_instrument_fx_requirements(
    requirements: _FxInstrumentRequirements,
) -> tuple[PnlEventFxInstrumentRequirementsPayload, ...]:
    """Encode per-instrument FX minute requirements into canonical payload entries.

    Args:
        requirements: Exact FX minutes grouped by instrument and currency pair.

    Returns:
        Sorted strict instrument requirement payloads.
    """
    return tuple(
        PnlEventFxInstrumentRequirementsPayload(
            instrument_public_id=instrument_public_id,
            requirements=tuple(
                PnlEventFxRequirementPayload(pair=pair, minutes=tuple(sorted(minutes)))
                for pair, minutes in sorted(pair_requirements.items())
            ),
        )
        for instrument_public_id, pair_requirements in sorted(requirements.items())
    )


def _decode_instrument_fx_requirements(
    entries: Sequence[PnlEventFxInstrumentRequirementsPayload],
) -> _FxInstrumentRequirements:
    """Decode canonical instrument requirement payloads into mutable minute sets.

    Args:
        entries: Validated per-instrument requirement payloads.

    Returns:
        Mutable instrument-to-pair FX requirement mapping.
    """
    return {
        instrument_entry.instrument_public_id: {
            requirement.pair: set(requirement.minutes)
            for requirement in instrument_entry.requirements
        }
        for instrument_entry in entries
    }


def event_walk_state_from_replay(state: _EventFxReplayState) -> PnlEventFxReplayStatePayload:
    """Convert a real event-FX replay state into its complete strict payload.

    Args:
        state: Mutable replay state captured at the checkpoint boundary.

    Returns:
        Canonically ordered lossless event-walk payload.
    """
    return PnlEventFxReplayStatePayload(
        requirements=_encode_instrument_fx_requirements(state.requirements),
        conversion_requirements=_encode_instrument_fx_requirements(state.conversion_requirements),
        last_effective=tuple(
            PoolLastEffectivePayload(
                pool=_pool_key_to_payload(pool_key),
                last_effective_time=effective_time,
            )
            for pool_key, effective_time in sorted(state.last_effective.items())
        ),
        position_qty=tuple(
            PnlEventFxPoolQuantityPayload(
                pool=_pool_key_to_payload(pool_key),
                position_qty=_encode_carried_number(quantity),
            )
            for pool_key, quantity in sorted(state.position_qty.items())
        ),
        basis_unknown=tuple(
            _pool_key_to_payload(pool_key) for pool_key in sorted(state.basis_unknown)
        ),
        untrusted_at=tuple(
            PnlEventFxUntrustedPayload(
                instrument_public_id=instrument_public_id,
                untrusted_at=untrusted_at,
            )
            for instrument_public_id, untrusted_at in sorted(state.untrusted_at.items())
        ),
    )


def event_walk_state_to_replay(payload: PnlEventFxReplayStatePayload) -> _EventFxReplayState:
    """Restore every event-FX replay field from its strict payload.

    Args:
        payload: Validated event-walk payload decoded from the envelope.

    Returns:
        A mutable replay state equal to the state originally captured.
    """
    return _EventFxReplayState(
        requirements=_decode_instrument_fx_requirements(payload.requirements),
        conversion_requirements=_decode_instrument_fx_requirements(payload.conversion_requirements),
        last_effective={
            _pool_key_from_payload(entry.pool): entry.last_effective_time
            for entry in payload.last_effective
        },
        position_qty={
            _pool_key_from_payload(entry.pool): _decode_carried_number(entry.position_qty)
            for entry in payload.position_qty
        },
        basis_unknown={_pool_key_from_payload(pool) for pool in payload.basis_unknown},
        untrusted_at={
            entry.instrument_public_id: entry.untrusted_at for entry in payload.untrusted_at
        },
    )


def mark_walk_state_from_replay(
    states: Mapping[str, _MarkReplayState],
    regression_shadows: Sequence[tuple[datetime, datetime]],
) -> PnlMarkFxReplayStatePayload:
    """Convert real per-instrument mark states and all regression shadows.

    Args:
        states: Final mark replay state keyed by instrument public identifier.
        regression_shadows: Global half-open suppression intervals from the prefix.

    Returns:
        Canonically ordered lossless mark-walk payload.
    """
    return PnlMarkFxReplayStatePayload(
        instruments=tuple(
            PnlMarkInstrumentReplayStatePayload(
                instrument_public_id=instrument_public_id,
                event_index=state.event_index,
                pool_quantities=tuple(
                    PnlMarkPoolQuantityPayload(
                        shard_key=shard_key,
                        quantity=_encode_carried_number(quantity),
                    )
                    for shard_key, quantity in sorted(state.pool_quantities.items())
                ),
                mark_eligible=state.mark_eligible,
            )
            for instrument_public_id, state in sorted(states.items())
        ),
        regression_shadows=tuple(
            PnlRegressionShadowPayload(event_time=event_time, previous_effective=previous_effective)
            for event_time, previous_effective in sorted(regression_shadows)
        ),
    )


def mark_walk_state_to_replay(
    payload: PnlMarkFxReplayStatePayload,
) -> tuple[dict[str, _MarkReplayState], tuple[tuple[datetime, datetime], ...]]:
    """Restore every mark replay cursor, quantity, latch, and regression shadow.

    Args:
        payload: Validated mark-walk payload decoded from the envelope.

    Returns:
        Mutable per-instrument states and the immutable global shadow tuple.
    """
    states = {
        entry.instrument_public_id: _MarkReplayState(
            event_index=entry.event_index,
            pool_quantities={
                quantity.shard_key: _decode_carried_number(quantity.quantity)
                for quantity in entry.pool_quantities
            },
            mark_eligible=entry.mark_eligible,
        )
        for entry in payload.instruments
    }
    shadows = tuple(
        (shadow.event_time, shadow.previous_effective) for shadow in payload.regression_shadows
    )
    return states, shadows


def bundle_to_json(bundle: PnlCheckpointStateBundle) -> str:
    """Serialize one checkpoint state bundle as deterministic canonical JSON.

    Args:
        bundle: Fully validated engine and service-side carried state.

    Returns:
        Compact JSON with recursively sorted object keys.
    """
    return json.dumps(
        bundle.model_dump(mode="json", by_alias=True),
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    )


def is_unsupported_bundle_version(text: str) -> bool:
    """Return whether valid JSON declares an integer ``bundle_version`` other than current.

    Args:
        text: Serialized checkpoint state envelope.

    Returns:
        ``True`` only when strict JSON parsing yields an object whose integer
        non-boolean ``bundle_version`` differs from :data:`BUNDLE_VERSION`.
    """
    try:
        raw = json.loads(
            text,
            object_pairs_hook=_unique_json_pairs,
            parse_constant=_reject_json_constant,
        )
    except ValueError:
        return False
    if not isinstance(raw, dict) or "bundle_version" not in raw:
        return False
    version = raw["bundle_version"]
    return isinstance(version, int) and not isinstance(version, bool) and version != BUNDLE_VERSION


def bundle_from_json(text: str) -> PnlCheckpointStateBundle:
    """Strictly decode a bundle while rejecting ambiguous or nonfinite JSON.

    Args:
        text: Serialized checkpoint state envelope.

    Returns:
        The fully validated immutable state bundle.

    Raises:
        ValueError: When JSON syntax, key uniqueness, constants, strict types,
            canonical values, or payload invariants violate the schema.
    """
    json.loads(
        text,
        object_pairs_hook=_unique_json_pairs,
        parse_constant=_reject_json_constant,
    )
    return PnlCheckpointStateBundle.model_validate_json(text, strict=True)
