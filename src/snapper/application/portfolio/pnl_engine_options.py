"""Single-producer resolver for P&L engine valuation and checkpoint options.

No registered calculation version currently permits opening-checkpoint capture.
``5C.6`` enables compensated unrealized fallback valuation and all-conversion-minutes
requests while keeping checkpoint capture disabled until a dedicated capture-enable
change proves resume equivalence.
"""

from collections.abc import Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import Final


@dataclass(frozen=True)
class PnlEngineOptions:
    """Engine valuation and checkpoint-capture flags for one calculation version.

    Attributes:
        capture_permitted: Whether opening-checkpoint capture is allowed for the
            calculation version.
        compensated_unrealized_fallback: Whether compensated unrealized fallback
            valuation is active.
        request_all_conversion_minutes: Whether every conversion minute is
            requested from FX evidence.
    """

    capture_permitted: bool
    compensated_unrealized_fallback: bool
    request_all_conversion_minutes: bool


PNL_ENGINE_OPTIONS_OFF: Final[PnlEngineOptions] = PnlEngineOptions(False, False, False)
"""Explicit options for every non-pointwise read: public and exact series, anchor, and backfill."""


_ENGINE_OPTIONS_BY_CALC_VERSION: Final[Mapping[str, PnlEngineOptions]] = MappingProxyType(
    {
        "5C.5": PNL_ENGINE_OPTIONS_OFF,
        "5C.6": PnlEngineOptions(
            capture_permitted=False,
            compensated_unrealized_fallback=True,
            request_all_conversion_minutes=True,
        ),
    }
)
"""Registered calculation-version engine option profiles."""


def _engine_options_table() -> Mapping[str, PnlEngineOptions]:
    """Return the version-to-options mapping for resolver testing."""
    return _ENGINE_OPTIONS_BY_CALC_VERSION


def pnl_engine_options_for(calc_version: str) -> PnlEngineOptions:
    """Return engine valuation options for ``calc_version``.

    This function is the only producer of engine valuation options in the
    codebase. No registered version currently permits checkpoint capture: ``5C.6``
    enables the compensated unrealized fallback and the all-conversion-minutes
    request, while capture stays disabled until a dedicated capture-enable
    change proves resume equivalence. ``5C.5`` disables all three flags.

    Args:
        calc_version: Target P&L calculation version string.

    Returns:
        Frozen :class:`PnlEngineOptions` configured for ``calc_version``.

    Raises:
        ValueError: When ``calc_version`` is unknown to the resolver.
    """
    options = _engine_options_table().get(calc_version)
    if options is None:
        raise ValueError(f"Unsupported P&L calculation version: {calc_version!r}")
    return options
