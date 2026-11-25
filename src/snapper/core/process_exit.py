"""Exit codes shared by the native process runner and its watchdog."""

from typing import Final

EX_TEMPFAIL: Final[int] = 75


class UpstreamUnavailableError(RuntimeError):
    """Signal a transient venue or proxy failure during market-data publisher startup."""
