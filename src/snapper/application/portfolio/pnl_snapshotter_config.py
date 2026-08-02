"""Env-var contract for the Phase-5B portfolio equity/drawdown snapshotter.

Split out from :mod:`snapper.application.portfolio.pnl_snapshotter` so the shared
``env_contract`` allowlist can import this subsystem's ``ENV_VARS`` without
dragging the snapshotter's infrastructure dependencies (the venue-capability
registry) into the config-load import graph. This module imports nothing beyond
the standard library and never performs I/O.
"""

from typing import Final

DEFAULT_INTERVAL_SECONDS: Final[int] = 60
INTERVAL_MIN_SECONDS: Final[int] = 10
INTERVAL_MAX_SECONDS: Final[int] = 3600
INTERVAL_ENV_VAR: Final[str] = "PNL_SNAPSHOTTER_INTERVAL_SECONDS"
ENABLED_ENV_VAR: Final[str] = "PNL_SNAPSHOTTER_ENABLED"
FX_SHADOW_PINNING_ENV_VAR: Final[str] = "PNL_FX_SHADOW_PINNING_ENABLED"
_TRUTHY_ENV_VALUES: Final[frozenset[str]] = frozenset({"1", "true", "yes"})
ENV_VARS: Final[frozenset[str]] = frozenset(
    {INTERVAL_ENV_VAR, ENABLED_ENV_VAR, FX_SHADOW_PINNING_ENV_VAR}
)
"""Public allowlist of env vars this snapshotter reads via ``os.environ``.

Consumed by :mod:`snapper.config.env_contract` to validate ``.env`` keys against
the union of every subsystem's contract.
"""


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
