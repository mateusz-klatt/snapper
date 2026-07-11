"""Per-frame market-data provenance context (PnL Phase 1).

Every market frame (candle/tick/trade) carries an immutable ``origin``
plus an optional replay window, stamped by its publisher. Strategies
record the TRIGGERING frame's provenance in a :class:`ContextVar`
right after parsing it, so everything the frame causes — including a
DETACHED consult task, which inherits a COPY of the context at
creation time and is therefore immune to later frames mutating the
var — stamps the same provenance onto its emitted
:class:`~snapper.messaging.schemas.data.SignalData`. Marker-based
session state (replay start/end events) was rejected for this job:
no production producer exists and a detached task could outlive a
session flip.
"""

from contextvars import ContextVar
from contextvars import Token
from datetime import datetime

from snapper.core.types import FrameOrigin

FrameProvenance = tuple[FrameOrigin, datetime | None, datetime | None]
"""(origin, replay_window_start, replay_window_end) of the current frame."""

_LIVE: FrameProvenance = ("live", None, None)

_frame_provenance: ContextVar[FrameProvenance] = ContextVar("frame_provenance", default=_LIVE)


def snapshot_frame_provenance() -> Token[FrameProvenance]:
    """Open a frame-scoped provenance window on the current task.

    The listener task processes MANY frames; without scoping, a replay
    frame's provenance would stick to the task and stamp everything
    later (system-message handlers, direct emissions). The caller
    snapshots before dispatching a frame and restores in a ``finally``
    — detached tasks created inside the window keep their COPY of the
    frame's provenance regardless of the parent's restore.

    Returns:
        The token to pass to :func:`restore_frame_provenance`.
    """
    return _frame_provenance.set(_frame_provenance.get())


def restore_frame_provenance(token: Token[FrameProvenance]) -> None:
    """Close a frame-scoped provenance window.

    Args:
        token: The token from :func:`snapshot_frame_provenance`.
    """
    _frame_provenance.reset(token)


def set_frame_provenance(
    origin: FrameOrigin,
    replay_window_start: datetime | None,
    replay_window_end: datetime | None,
) -> None:
    """Record the provenance of the frame being processed.

    Args:
        origin: The frame's stamped origin.
        replay_window_start: Replay window start, when replaying.
        replay_window_end: Replay window end, when replaying.
    """
    _frame_provenance.set((origin, replay_window_start, replay_window_end))


def get_frame_provenance() -> FrameProvenance:
    """Return the provenance of the frame that triggered this context.

    Returns:
        The recorded (origin, window start, window end) tuple; the
        live default when no frame has been recorded.
    """
    return _frame_provenance.get()
