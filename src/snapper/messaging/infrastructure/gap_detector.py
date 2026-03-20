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
    rejected_unstamped: int = 0


class GapDetector:
    """Detect sequence gaps and producer session resets.

    Maintains per-received-topic state to detect:
    - sequence gaps (missing messages)
    - session resets (producer restarted)
    - duplicates or reordered messages
    - mid-stream joins (subscriber started after producer)

    Messages without provenance metadata (empty session_id or
    sequence_id == 0) are rejected with a warning log and counter
    increment so the condition is observable.
    """

    def __init__(self, name: str = "") -> None:
        """Initialize gap detector.

        Args:
            name: Optional component name for log context.
        """
        self._name = name
        self._log_prefix = f"[GapDetector:{name}]" if name else "[GapDetector]"
        self._streams: dict[str, _StreamState] = {}
        self.stats: GapDetectorStats = GapDetectorStats()

    def check(self, received_topic: str, session_id: str, sequence_id: int) -> bool:
        """Check a received message for sequence gaps.

        Args:
            received_topic: The ZMQ topic the message arrived on.
            session_id: Producer session UUID from the message payload.
            sequence_id: Sequence number from the message payload.

        Returns:
            True if the message carries valid provenance and was processed,
            False if the message was rejected as unstamped.
        """
        if not session_id or sequence_id == 0:
            logger.warning(
                f"{self._log_prefix} Rejected unstamped message on {received_topic} "
                f"(session_id={session_id!r}, sequence_id={sequence_id})"
            )
            self.stats.rejected_unstamped += 1
            return False

        state = self._streams.get(received_topic)

        if state is None:
            self._streams[received_topic] = _StreamState(
                last_session_id=session_id,
                expected_sequence_id=sequence_id + 1,
            )
            if sequence_id > 1:
                self.stats.mid_stream_joins += 1
                logger.info(
                    f"{self._log_prefix} Joined mid-stream on {received_topic} "
                    f"at seq={sequence_id} (session={session_id[:8]})"
                )
            return True

        if state.last_session_id != session_id:
            self.stats.session_resets += 1
            logger.info(
                f"{self._log_prefix} Session reset on {received_topic}: "
                f"{state.last_session_id[:8]} -> {session_id[:8]}, "
                f"seq={sequence_id}"
            )
            state.last_session_id = session_id
            state.expected_sequence_id = sequence_id + 1
            return True

        if sequence_id == state.expected_sequence_id:
            state.expected_sequence_id = sequence_id + 1
            return True

        if sequence_id > state.expected_sequence_id:
            gap_size = sequence_id - state.expected_sequence_id
            self.stats.gaps_detected += gap_size
            logger.warning(
                f"{self._log_prefix} Gap on {received_topic}: "
                f"expected seq={state.expected_sequence_id}, "
                f"got seq={sequence_id} "
                f"(missing {gap_size} message(s), "
                f"session={session_id[:8]})"
            )
            state.expected_sequence_id = sequence_id + 1
            return True

        self.stats.duplicates += 1
        logger.debug(
            f"{self._log_prefix} Duplicate/reorder on {received_topic}: "
            f"expected seq={state.expected_sequence_id}, "
            f"got seq={sequence_id} "
            f"(session={session_id[:8]})"
        )
        return True

    def reset_topic(self, topic: str) -> None:
        """Clear tracking state for a topic.

        Called when the bridge unsubscribes from a ZMQ topic (no more
        WS clients). Without this, resubscribing later would produce
        false gaps because messages published while unsubscribed are
        never received.

        Args:
            topic: The ZMQ topic whose state should be discarded.
        """
        removed = self._streams.pop(topic, None)
        if removed is not None:
            logger.debug(
                f"{self._log_prefix} Reset tracking for {topic} "
                f"(was at seq={removed.expected_sequence_id}, "
                f"session={removed.last_session_id[:8]})"
            )
