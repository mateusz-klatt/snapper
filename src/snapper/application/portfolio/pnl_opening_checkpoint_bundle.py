"""Strict versioned envelope for engine state and service-side replay prefixes."""

import json
from collections.abc import Mapping
from collections.abc import Sequence
from datetime import datetime
from typing import Annotated
from typing import Literal
from typing import Self

from pydantic import AfterValidator
from pydantic import BaseModel
from pydantic import StrictInt
from pydantic import field_validator
from pydantic import model_validator

from snapper.application.portfolio.pnl_opening_checkpoint import _STRICT_CHECKPOINT_CONFIG
from snapper.application.portfolio.pnl_opening_checkpoint import HexFloatStr
from snapper.application.portfolio.pnl_opening_checkpoint import NonEmptyStr
from snapper.application.portfolio.pnl_opening_checkpoint import PnlOpeningCheckpointState
from snapper.application.portfolio.pnl_opening_checkpoint import PoolKeyPayload
from snapper.application.portfolio.pnl_opening_checkpoint import PoolLastEffectivePayload
from snapper.application.portfolio.pnl_opening_checkpoint import UtcDateTime
from snapper.application.portfolio.pnl_opening_checkpoint import _pool_key_from_payload
from snapper.application.portfolio.pnl_opening_checkpoint import _pool_key_to_payload
from snapper.application.portfolio.pnl_opening_checkpoint import _reject_json_constant
from snapper.application.portfolio.pnl_opening_checkpoint import _require_strictly_sorted
from snapper.application.portfolio.pnl_opening_checkpoint import _unique_json_pairs
from snapper.application.portfolio.pnl_timeline_service import _EventFxReplayState
from snapper.application.portfolio.pnl_timeline_service import _MarkReplayState
from snapper.data.pnl_correction_state import IdAggregate


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


class PnlEventFxRequirementPayload(BaseModel):
    """Exact event-FX minutes required for one normalized currency pair."""

    model_config = _STRICT_CHECKPOINT_CONFIG

    pair: tuple[NonEmptyStr, NonEmptyStr]
    minutes: tuple[UtcDateTime, ...]


class PnlEventFxInstrumentRequirementsPayload(BaseModel):
    """All event-FX pair requirements carried for one instrument."""

    model_config = _STRICT_CHECKPOINT_CONFIG

    instrument_public_id: NonEmptyStr
    requirements: tuple[PnlEventFxRequirementPayload, ...]


class PnlEventFxPoolQuantityPayload(BaseModel):
    """Canonical carried position quantity for one durable pool."""

    model_config = _STRICT_CHECKPOINT_CONFIG

    pool: PoolKeyPayload
    position_qty: HexFloatStr


class PnlEventFxUntrustedPayload(BaseModel):
    """Effective time at which one instrument became permanently untrusted."""

    model_config = _STRICT_CHECKPOINT_CONFIG

    instrument_public_id: NonEmptyStr
    untrusted_at: UtcDateTime


class PnlEventFxReplayStatePayload(BaseModel):
    """Complete carried state of the event-FX requirement walk."""

    model_config = _STRICT_CHECKPOINT_CONFIG

    requirements: tuple[PnlEventFxInstrumentRequirementsPayload, ...]
    last_effective: tuple[PoolLastEffectivePayload, ...]
    position_qty: tuple[PnlEventFxPoolQuantityPayload, ...]
    basis_unknown: tuple[PoolKeyPayload, ...]
    untrusted_at: tuple[PnlEventFxUntrustedPayload, ...]


class PnlMarkPoolQuantityPayload(BaseModel):
    """Canonical carried mark-walk quantity for one instrument shard."""

    model_config = _STRICT_CHECKPOINT_CONFIG

    shard_key: NonEmptyStr
    quantity: HexFloatStr


class PnlMarkInstrumentReplayStatePayload(BaseModel):
    """Complete mark replay cursor, quantities, and invalid-event latch."""

    model_config = _STRICT_CHECKPOINT_CONFIG

    instrument_public_id: NonEmptyStr
    event_index: StrictInt
    pool_quantities: tuple[PnlMarkPoolQuantityPayload, ...]
    mark_eligible: bool


class PnlRegressionShadowPayload(BaseModel):
    """Half-open mark suppression interval from one out-of-order execution."""

    model_config = _STRICT_CHECKPOINT_CONFIG

    event_time: UtcDateTime
    previous_effective: UtcDateTime


class PnlMarkFxReplayStatePayload(BaseModel):
    """Per-instrument mark states and every global regression shadow."""

    model_config = _STRICT_CHECKPOINT_CONFIG

    instruments: tuple[PnlMarkInstrumentReplayStatePayload, ...]
    regression_shadows: tuple[PnlRegressionShadowPayload, ...]


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


class PnlCheckpointStateBundle(BaseModel):
    """Versioned engine and service replay state captured at one boundary."""

    model_config = _STRICT_CHECKPOINT_CONFIG

    bundle_version: Literal[1]
    engine_state: PnlOpeningCheckpointState
    price_ref_spans_by_instrument: dict[NonEmptyStr, tuple[UtcDateTime, UtcDateTime]]
    display_activity_spans_by_instrument: dict[NonEmptyStr, tuple[UtcDateTime, UtcDateTime]]
    rate_sources_prefix: tuple[PnlFxRateSourcePayload, ...]
    prefix_lineage_order_ids: tuple[NonEmptyStr, ...]
    event_walk_state: PnlEventFxReplayStatePayload
    mark_walk_state: PnlMarkFxReplayStatePayload
    event_fx_candles_by_plane: tuple[PnlEventFxCandlePlanePayload, ...]

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


def event_walk_state_from_replay(state: _EventFxReplayState) -> PnlEventFxReplayStatePayload:
    """Convert a real event-FX replay state into its complete strict payload.

    Args:
        state: Mutable replay state captured at the checkpoint boundary.

    Returns:
        Canonically ordered lossless event-walk payload.
    """
    return PnlEventFxReplayStatePayload(
        requirements=tuple(
            PnlEventFxInstrumentRequirementsPayload(
                instrument_public_id=instrument_public_id,
                requirements=tuple(
                    PnlEventFxRequirementPayload(pair=pair, minutes=tuple(sorted(minutes)))
                    for pair, minutes in sorted(pair_requirements.items())
                ),
            )
            for instrument_public_id, pair_requirements in sorted(state.requirements.items())
        ),
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
                position_qty=quantity.hex(),
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
        requirements={
            instrument_entry.instrument_public_id: {
                requirement.pair: set(requirement.minutes)
                for requirement in instrument_entry.requirements
            }
            for instrument_entry in payload.requirements
        },
        last_effective={
            _pool_key_from_payload(entry.pool): entry.last_effective_time
            for entry in payload.last_effective
        },
        position_qty={
            _pool_key_from_payload(entry.pool): float.fromhex(entry.position_qty)
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
                    PnlMarkPoolQuantityPayload(shard_key=shard_key, quantity=quantity.hex())
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
                quantity.shard_key: float.fromhex(quantity.quantity)
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
        bundle.model_dump(mode="json"),
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    )


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
