"""Dependency-free absolute refresh deadline shared by supervisor and workers."""

import math
import time
from collections.abc import Callable
from dataclasses import dataclass


@dataclass(frozen=True)
class RefreshBudget:
    """One supervisor-owned absolute monotonic expiry, never renewed by a phase."""

    deadline: float
    clock: Callable[[], float] = time.monotonic

    def remaining(self, reserve: float = 0.0) -> float:
        """Return positive remaining seconds after a finite nonnegative cleanup reserve.

        Args:
            reserve: Finite nonnegative seconds retained for later cleanup inside the same deadline.

        Returns:
            Positive remaining seconds after reserving the requested cleanup interval.
        """
        now = self.clock()
        if not all(math.isfinite(value) for value in (self.deadline, now, reserve)) or reserve < 0:
            raise ValueError("Refresh deadline and reserve must be finite")
        remaining = self.deadline - now - reserve
        if remaining <= 0:
            raise TimeoutError("Refresh deadline exhausted")
        return remaining

    def timeout_seconds(self, maximum: float, reserve: float = 0.0) -> float:
        """Cap a phase timeout without changing the absolute job deadline.

        Args:
            maximum: Finite positive ceiling for this operation, expressed in seconds.
            reserve: Finite nonnegative seconds retained for later cleanup inside the same deadline.

        Returns:
            The smaller of the phase ceiling and the remaining shared budget.
        """
        if not math.isfinite(maximum) or maximum <= 0:
            raise ValueError("Refresh timeout ceiling must be finite and positive")
        return min(maximum, self.remaining(reserve))

    def timeout_ms(self, maximum: float, reserve: float = 0.0) -> int:
        """Round a database timeout down, refusing rather than extending a submillisecond budget.

        Args:
            maximum: Finite positive ceiling for this operation, expressed in seconds.
            reserve: Finite nonnegative seconds retained for later cleanup inside the same deadline.

        Returns:
            Positive whole milliseconds rounded down within the remaining shared budget.
        """
        milliseconds = int(self.timeout_seconds(maximum, reserve) * 1000)
        if milliseconds < 1:
            raise TimeoutError("Refresh database timeout budget exhausted")
        return milliseconds
