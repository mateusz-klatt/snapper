"""WebSocket client-side provenance gap detection.

Provides per-connection gap detection for incoming WebSocket client
messages. Each WsClientGapDetector wraps a GapDetector keyed by
session_id and logs client provenance fields for observability.

Observability-first: gaps are logged as warnings, never cause
disconnects or message rejection.
"""

import json
from typing import Any

from loguru import logger

from snapper.messaging.infrastructure.gap_detector import GapDetector


class WsClientGapDetector:
    """Per-WebSocket-connection client provenance gap detector.

    Extracts session_id, sequence_id, and public_id from raw incoming
    JSON messages and runs a GapDetector per session_id to detect
    sequence gaps. All provenance information is logged as structured
    fields for debugging.

    Attributes:
        gap_detectors: Per-session GapDetector instances for this connection.
    """

    def __init__(self) -> None:
        """Initialize with empty detector map."""
        self.gap_detectors: dict[str, GapDetector] = {}

    def inspect(self, raw_message: str) -> None:
        """Extract and check provenance fields from a raw WS message.

        Parses the JSON, extracts session_id/sequence_id/public_id,
        logs the provenance fields, and runs gap detection. Non-JSON
        or messages without provenance fields are silently skipped.

        Args:
            raw_message: Raw JSON string received from the WebSocket client.
        """
        try:
            payload: Any = json.loads(raw_message)
        except json.JSONDecodeError:
            return

        if not isinstance(payload, dict):
            return

        session_id: str = payload.get("session_id", "")
        sequence_id: int = payload.get("sequence_id", 0)
        public_id: str = payload.get("public_id", "")
        msg_type: str = payload.get("type", "unknown")

        if not session_id and sequence_id == 0 and not public_id:
            return

        logger.info(
            "WS client provenance ({msg_type}): "
            "client_public_id={client_public_id}, "
            "client_session_id={client_session_id}, "
            "client_sequence_id={client_sequence_id}",
            msg_type=msg_type,
            client_public_id=public_id,
            client_session_id=session_id,
            client_sequence_id=sequence_id,
        )

        if session_id and sequence_id > 0:
            detector = self.gap_detectors.get(session_id)
            if detector is None:
                detector = GapDetector(name=f"ws-client:{session_id[:8]}")
                self.gap_detectors[session_id] = detector
            detector.check(msg_type, session_id, sequence_id)
