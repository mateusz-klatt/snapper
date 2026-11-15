"""Liveness bound shared by the tests that hold real worker threads on events."""

from typing import Final

LIVENESS_TIMEOUT_S: Final = 10.0
"""Upper bound for a held worker to enter, or for the test to release it.

Every wait that uses it only guards liveness: the worker sets the event as soon as it
reaches its held step, and the test releases it right after its checks. The former two- to
five-second bounds made slow CI runners fail before the worker entered, in snapper #290's
and #293's CI runs. A wait returns as soon as its event is set, so a longer bound costs
nothing when the run is healthy. Staying under the 15 s per-test timeout keeps a genuine
hang a clear failure.
"""
