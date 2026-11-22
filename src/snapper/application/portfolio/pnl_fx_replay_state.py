"""Shared mutable state types for exact event and mark FX requirement walks."""

from dataclasses import dataclass
from dataclasses import field
from datetime import datetime

from snapper.application.portfolio.fx_rates import FxPairKey

type _FxMinuteRequirements = dict[FxPairKey, set[datetime]]
"""Exact conversion minutes grouped by unordered currency pair."""

type _FxInstrumentRequirements = dict[str, _FxMinuteRequirements]
"""Exact conversion minutes grouped first by consuming instrument."""

type _PoolKey = tuple[str, str]
"""Durable instrument and shard identity used by replay state."""


@dataclass(slots=True)
class _EventFxReplayState:
    """Mutable position and trust state for event FX requirements."""

    requirements: _FxInstrumentRequirements
    last_effective: dict[_PoolKey, datetime]
    position_qty: dict[_PoolKey, float]
    basis_unknown: set[_PoolKey]
    untrusted_at: dict[str, datetime]
    conversion_requirements: _FxInstrumentRequirements = field(default_factory=dict)


@dataclass(slots=True)
class _MarkReplayState:
    """Mutable per-instrument position state while visiting mark minutes."""

    event_index: int
    pool_quantities: dict[str, float]
    mark_eligible: bool
