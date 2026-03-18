"""Sequence gap detection for ZMQ and WebSocket message streams.

Detects gaps in message sequences by tracking per-topic state: the last
seen session_id and expected next sequence_id. Logs warnings on gaps,
info on session resets, and debug on duplicates/reorders.

Phase 1 scope invariant: exactly one active producer session per exact
topic at any given time. Multi-producer support is out of scope.
"""

from dataclasses import dataclass

from loguru import logger


@dataclass
class _StreamState:
    """Tracked state for one received topic stream."""

    last_session_id: str
    expected_sequence_id: int


@dataclass
class GapDetectorStats:
    """Telemetry counters for gap detection."""

    gaps_detected: int = 0
    session_resets: int = 0
    duplicates: int = 0
    mid_stream_joins: int = 0


class GapDetector:
    """Detect sequence gaps and producer session resets.

    Maintains per-received-topic state to detect:
    - sequence gaps (missing messages)
    - session resets (producer restarted)
    - duplicates or reordered messages
    - mid-stream joins (subscriber started after producer)

    Messages without provenance metadata (empty session_id or
    sequence_id == 0) are silently skipped for backward compatibility.
    """

    def __init__(self, name: str = "") -> None:
        """Initialize gap detector.

        Args:
            name: Optional component name for log context.
        """
        self._name = name
        self._streams: dict[str, _StreamState] = {}
        self.stats: GapDetectorStats = GapDetectorStats()

    def check(self, received_topic: str, session_id: str, sequence_id: int) -> None:
        """Check a received message for sequence gaps.

        Args:
            received_topic: The ZMQ topic the message arrived on.
            session_id: Producer session UUID from the message payload.
            sequence_id: Sequence number from the message payload.
        """
        if not session_id or sequence_id == 0:
            return

        log_prefix = f"[GapDetector:{self._name}]" if self._name else "[GapDetector]"

        state = self._streams.get(received_topic)

        if state is None:
            self._streams[received_topic] = _StreamState(
                last_session_id=session_id,
                expected_sequence_id=sequence_id + 1,
            )
            if sequence_id > 1:
                self.stats.mid_stream_joins += 1
                logger.info(
                    f"{log_prefix} Joined mid-stream on {received_topic} "
                    f"at seq={sequence_id} (session={session_id[:8]})"
                )
            return

        if state.last_session_id != session_id:
            self.stats.session_resets += 1
            logger.info(
                f"{log_prefix} Session reset on {received_topic}: "
                f"{state.last_session_id[:8]} -> {session_id[:8]}, "
                f"seq={sequence_id}"
            )
            state.last_session_id = session_id
            state.expected_sequence_id = sequence_id + 1
            return

        if sequence_id == state.expected_sequence_id:
            state.expected_sequence_id = sequence_id + 1
            return

        if sequence_id > state.expected_sequence_id:
            gap_size = sequence_id - state.expected_sequence_id
            self.stats.gaps_detected += gap_size
            logger.warning(
                f"{log_prefix} Gap on {received_topic}: "
                f"expected seq={state.expected_sequence_id}, "
                f"got seq={sequence_id} "
                f"(missing {gap_size} message(s), "
                f"session={session_id[:8]})"
            )
            state.expected_sequence_id = sequence_id + 1
            return

        self.stats.duplicates += 1
        logger.debug(
            f"{log_prefix} Duplicate/reorder on {received_topic}: "
            f"expected seq={state.expected_sequence_id}, "
            f"got seq={sequence_id} "
            f"(session={session_id[:8]})"
        )
