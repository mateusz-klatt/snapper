"""Env-var contract for the Phase-5B portfolio equity/drawdown snapshotter.

Split out from :mod:`snapper.application.portfolio.pnl_snapshotter` so the shared
``env_contract`` allowlist can import this subsystem's ``ENV_VARS`` without
dragging the snapshotter's infrastructure dependencies (the venue-capability
registry) into the config-load import graph. This module imports nothing beyond
the standard library and never reads env or performs I/O at import time.
"""

import logging
from dataclasses import dataclass
from typing import Final
from typing import Literal

logger = logging.getLogger(__name__)

DEFAULT_INTERVAL_SECONDS: Final[int] = 60
INTERVAL_MIN_SECONDS: Final[int] = 10
INTERVAL_MAX_SECONDS: Final[int] = 3600
INTERVAL_ENV_VAR: Final[str] = "PNL_SNAPSHOTTER_INTERVAL_SECONDS"
ENABLED_ENV_VAR: Final[str] = "PNL_SNAPSHOTTER_ENABLED"
FX_SHADOW_PINNING_ENV_VAR: Final[str] = "PNL_FX_SHADOW_PINNING_ENABLED"
CHECKPOINT_MODE_ENV_VAR: Final[str] = "PNL_SNAPSHOTTER_CHECKPOINT_MODE"
type CheckpointMode = Literal["off", "shadow", "active"]
DEFAULT_CHECKPOINT_MODE: Final[CheckpointMode] = "off"
CHECKPOINT_MODES_ENABLED: Final[frozenset[str]] = frozenset({"off", "shadow", "active"})
"""Opening-checkpoint modes currently enabled in code.

``off`` remains the default when ``PNL_SNAPSHOTTER_CHECKPOINT_MODE`` is unset;
``shadow`` and ``active`` are enabled for configuration-gated rollout. Any
parsed mode outside this set resolves to ``"off"`` with one process-wide
warning.
"""
_TRUTHY_ENV_VALUES: Final[frozenset[str]] = frozenset({"1", "true", "yes"})
ENV_VARS: Final[frozenset[str]] = frozenset(
    {
        INTERVAL_ENV_VAR,
        ENABLED_ENV_VAR,
        FX_SHADOW_PINNING_ENV_VAR,
        CHECKPOINT_MODE_ENV_VAR,
    }
)
"""Public allowlist of env vars this snapshotter reads via ``os.environ``.

Consumed by :mod:`snapper.config.env_contract` to validate ``.env`` keys against
the union of every subsystem's contract.
"""


@dataclass(frozen=True, slots=True)
class ResolvedCheckpointMode:
    """Configured and effective opening-checkpoint modes resolved from env config.

    Attributes:
        configured: Canonical lowercase mode parsed from the env value.
        effective: Runtime mode gated by :data:`CHECKPOINT_MODES_ENABLED`.
    """

    configured: CheckpointMode
    effective: CheckpointMode


@dataclass
class _KillSwitchWarningState:
    """Process-local latch ensuring the disabled-mode warning logs once per process."""

    logged: bool = False


_KILL_SWITCH_WARNING_STATE: Final[_KillSwitchWarningState] = _KillSwitchWarningState()


def resolve_interval(env_value: str | None) -> int:
    """Coerce ``PNL_SNAPSHOTTER_INTERVAL_SECONDS`` to an int in ``[10, 3600]``.

    Empty or unset values fall back to :data:`DEFAULT_INTERVAL_SECONDS`. A value
    that parses as an integer outside the range, or does not parse at all, raises
    so a malformed env var fails loud at lifespan startup.

    Args:
        env_value: Raw env-var value, or ``None`` when unset.

    Returns:
        The loop interval in seconds.

    Raises:
        ValueError: When the raw value is not an integer or is out of range.
    """
    if env_value is None or env_value.strip() == "":
        return DEFAULT_INTERVAL_SECONDS
    try:
        value = int(env_value.strip())
    except ValueError as exc:
        raise ValueError(f"{INTERVAL_ENV_VAR}={env_value!r} is not an integer") from exc
    if value < INTERVAL_MIN_SECONDS or value > INTERVAL_MAX_SECONDS:
        raise ValueError(
            f"{INTERVAL_ENV_VAR}={value} out of range "
            f"[{INTERVAL_MIN_SECONDS}, {INTERVAL_MAX_SECONDS}]"
        )
    return value


def resolve_enabled(env_value: str | None) -> bool:
    """Parse ``PNL_SNAPSHOTTER_ENABLED`` as a boolean, defaulting to disabled.

    Truthy values are ``"1"``, ``"true"``, ``"yes"`` (case-insensitive);
    everything else, including ``None``, is ``False`` so the snapshotter stays
    parked until an operator explicitly enables it.

    Args:
        env_value: Raw env-var value, or ``None`` when unset.

    Returns:
        ``True`` iff the snapshotter should run its loop.
    """
    if env_value is None:
        return False
    return env_value.strip().lower() in _TRUTHY_ENV_VALUES


def _parse_checkpoint_mode(env_value: str | None) -> CheckpointMode:
    """Parse ``PNL_SNAPSHOTTER_CHECKPOINT_MODE`` into ``off``, ``shadow``, or ``active``.

    Args:
        env_value: Raw env-var value, or ``None`` when unset.

    Returns:
        The canonical lowercase :data:`CheckpointMode` literal.

    Raises:
        ValueError: When ``env_value`` is non-empty and not one of ``off``,
            ``shadow``, or ``active``.
    """
    if env_value is None or env_value.strip() == "":
        return DEFAULT_CHECKPOINT_MODE
    normalized = env_value.strip().lower()
    if normalized == "off":
        return "off"
    if normalized == "shadow":
        return "shadow"
    if normalized == "active":
        return "active"
    raise ValueError(
        f"{CHECKPOINT_MODE_ENV_VAR}={env_value!r} must be one of 'off', 'shadow', or 'active'"
    )


def resolve_checkpoint_modes(env_value: str | None) -> ResolvedCheckpointMode:
    """Resolve both configured and kill-switch-gated checkpoint modes from env config.

    Unset, empty, or whitespace-only values default to ``"off"``. Surrounding
    whitespace is stripped and matching is case-insensitive. Invalid tokens still
    raise ``ValueError`` naming ``PNL_SNAPSHOTTER_CHECKPOINT_MODE``. Any parsed
    mode outside :data:`CHECKPOINT_MODES_ENABLED` resolves to ``"off"`` and emits
    one warning per process naming both modes.

    Args:
        env_value: Raw env-var value, or ``None`` when unset.

    Returns:
        Configured and effective :data:`CheckpointMode` values.

    Raises:
        ValueError: When ``env_value`` is non-empty and not one of ``off``,
            ``shadow``, or ``active``.
    """
    configured = _parse_checkpoint_mode(env_value)
    if configured in CHECKPOINT_MODES_ENABLED:
        return ResolvedCheckpointMode(configured=configured, effective=configured)
    if not _KILL_SWITCH_WARNING_STATE.logged:
        _KILL_SWITCH_WARNING_STATE.logged = True
        logger.warning(
            "%s configured=%s resolved to effective=%s; "
            "checkpoint modes are disabled until capture is enabled",
            CHECKPOINT_MODE_ENV_VAR,
            configured,
            DEFAULT_CHECKPOINT_MODE,
        )
    return ResolvedCheckpointMode(configured=configured, effective=DEFAULT_CHECKPOINT_MODE)
