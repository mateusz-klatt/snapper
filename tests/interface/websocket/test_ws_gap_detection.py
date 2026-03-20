"""Tests for WebSocket client-side provenance gap detection."""

import json

from loguru import logger

from snapper.interface.websocket.gap_detection import WsClientGapDetector
from snapper.interface.websocket.gap_detection import _client_counter_key


class TestWsClientGapDetector:
    """Tests for WsClientGapDetector provenance extraction and gap detection."""

    def test_inspect_logs_provenance_fields(self) -> None:
        """Inspect logs client provenance fields from a valid WS message.

        Given: A WsClientGapDetector,
        When: Inspecting a JSON message with provenance fields,
        Then: Info log contains session_id, sequence_id, and public_id.
        """
        messages: list[str] = []
        sink_id = logger.add(lambda msg: messages.append(str(msg)), level="INFO")
        try:
            detector = WsClientGapDetector()
            raw = json.dumps(
                {
                    "type": "subscribe",
                    "session_id": "ws-sess-aabbccdd",
                    "sequence_id": 1,
                    "public_id": "ws-pub-001",
                    "topics": ["market.tick"],
                }
            )
            detector.inspect(raw)
        finally:
            logger.remove(sink_id)
        provenance_logs = [m for m in messages if "WS client provenance" in m]
        assert len(provenance_logs) == 1
        assert "ws-sess-aabbccdd" in provenance_logs[0]
        assert "ws-pub-001" in provenance_logs[0]

    def test_inspect_detects_gap(self) -> None:
        """Gap in WS client sequence triggers warning log.

        Given: A WsClientGapDetector,
        When: Inspecting messages with sequence 1 then 5 (gap of 2),
        Then: Gap warning is logged.
        """
        messages: list[str] = []
        sink_id = logger.add(lambda msg: messages.append(str(msg)), level="WARNING")
        session = "ws-gap-aabbccdd"
        try:
            detector = WsClientGapDetector()
            detector.inspect(
                json.dumps(
                    {
                        "type": "subscribe",
                        "session_id": session,
                        "sequence_id": 1,
                        "public_id": "p1",
                        "topics": [],
                    }
                )
            )
            detector.inspect(
                json.dumps(
                    {
                        "type": "subscribe",
                        "session_id": session,
                        "sequence_id": 5,
                        "public_id": "p5",
                        "topics": [],
                    }
                )
            )
        finally:
            logger.remove(sink_id)
        gap_warnings = [m for m in messages if "Gap" in m]
        assert len(gap_warnings) == 1

    def test_sequential_messages_no_gap_warning(self) -> None:
        """Sequential WS messages produce no gap warnings.

        Given: A WsClientGapDetector,
        When: Inspecting messages with sequence 1, 2, 3,
        Then: No gap warnings are logged.
        """
        messages: list[str] = []
        sink_id = logger.add(lambda msg: messages.append(str(msg)), level="WARNING")
        session = "ws-seq-aabbccdd"
        try:
            detector = WsClientGapDetector()
            for seq in (1, 2, 3):
                detector.inspect(
                    json.dumps(
                        {
                            "type": "ping",
                            "session_id": session,
                            "sequence_id": seq,
                            "public_id": f"p{seq}",
                        }
                    )
                )
        finally:
            logger.remove(sink_id)
        gap_warnings = [m for m in messages if "Gap" in m]
        assert len(gap_warnings) == 0

    def test_inspect_skips_non_json(self) -> None:
        """Non-JSON messages are silently skipped without error.

        Given: A WsClientGapDetector,
        When: Inspecting a non-JSON string,
        Then: No exception raised and no provenance log emitted.
        """
        messages: list[str] = []
        sink_id = logger.add(lambda msg: messages.append(str(msg)), level="INFO")
        try:
            detector = WsClientGapDetector()
            detector.inspect("not json at all")
        finally:
            logger.remove(sink_id)
        assert not any("WS client provenance" in m for m in messages)

    def test_inspect_skips_message_without_provenance(self) -> None:
        """Messages without provenance fields are silently skipped.

        Given: A WsClientGapDetector,
        When: Inspecting a valid JSON dict without session_id/sequence_id/public_id,
        Then: No provenance log emitted.
        """
        messages: list[str] = []
        sink_id = logger.add(lambda msg: messages.append(str(msg)), level="INFO")
        try:
            detector = WsClientGapDetector()
            detector.inspect(json.dumps({"type": "ping"}))
        finally:
            logger.remove(sink_id)
        assert not any("WS client provenance" in m for m in messages)

    def test_inspect_skips_json_array(self) -> None:
        """JSON array body is silently skipped.

        Given: A WsClientGapDetector,
        When: Inspecting a JSON array,
        Then: No provenance log emitted.
        """
        messages: list[str] = []
        sink_id = logger.add(lambda msg: messages.append(str(msg)), level="INFO")
        try:
            detector = WsClientGapDetector()
            detector.inspect(json.dumps([1, 2, 3]))
        finally:
            logger.remove(sink_id)
        assert not any("WS client provenance" in m for m in messages)

    def test_multiple_sessions_tracked_independently(self) -> None:
        """Different session_ids are tracked independently.

        Given: A WsClientGapDetector,
        When: Receiving messages from two different sessions,
        Then: Gap detection is independent per session.
        """
        detector = WsClientGapDetector()
        detector.inspect(
            json.dumps(
                {
                    "type": "subscribe",
                    "session_id": "sess-a-aabbccdd",
                    "sequence_id": 1,
                    "public_id": "pa1",
                    "topics": [],
                }
            )
        )
        detector.inspect(
            json.dumps(
                {
                    "type": "subscribe",
                    "session_id": "sess-b-aabbccdd",
                    "sequence_id": 1,
                    "public_id": "pb1",
                    "topics": [],
                }
            )
        )
        assert "sess-a-aabbccdd" in detector.gap_detectors
        assert "sess-b-aabbccdd" in detector.gap_detectors

    def test_inspect_with_partial_provenance_logs(self) -> None:
        """Message with only public_id (no session/sequence) still logs.

        Given: A WsClientGapDetector,
        When: Inspecting a message with public_id but empty session_id and seq 0,
        Then: Provenance log is emitted (partial provenance is observable).
        """
        messages: list[str] = []
        sink_id = logger.add(lambda msg: messages.append(str(msg)), level="INFO")
        try:
            detector = WsClientGapDetector()
            detector.inspect(
                json.dumps(
                    {
                        "type": "ping",
                        "public_id": "only-pub-id",
                    }
                )
            )
        finally:
            logger.remove(sink_id)
        provenance_logs = [m for m in messages if "WS client provenance" in m]
        assert len(provenance_logs) == 1
        assert "only-pub-id" in provenance_logs[0]


class TestClientCounterKey:
    """Tests for _client_counter_key mapping."""

    def test_subscribe_maps_to_control(self) -> None:
        """Subscribe message type maps to control counter."""
        assert _client_counter_key("subscribe") == "control"

    def test_unsubscribe_maps_to_control(self) -> None:
        """Unsubscribe message type maps to control counter."""
        assert _client_counter_key("unsubscribe") == "control"

    def test_authenticate_maps_to_control(self) -> None:
        """Authenticate message type maps to control counter."""
        assert _client_counter_key("authenticate") == "control"

    def test_reauthenticate_maps_to_control(self) -> None:
        """Reauthenticate message type maps to control counter."""
        assert _client_counter_key("reauthenticate") == "control"

    def test_ping_maps_to_telemetry(self) -> None:
        """Ping message type maps to telemetry counter."""
        assert _client_counter_key("ping") == "telemetry"

    def test_heartbeat_maps_to_telemetry(self) -> None:
        """Heartbeat message type maps to telemetry counter."""
        assert _client_counter_key("heartbeat") == "telemetry"

    def test_unknown_type_returns_original(self) -> None:
        """Unknown message type passes through unchanged."""
        assert _client_counter_key("custom_event") == "custom_event"


class TestMixedCounterFamilies:
    """Tests verifying no false gaps when mixing control and telemetry."""

    def test_subscribe_then_ping_no_gap(self) -> None:
        """Interleaved subscribe and ping produce no false gaps.

        Given: A WsClientGapDetector,
        When: Subscribe(control seq=1), ping(telemetry seq=1), subscribe(control seq=2),
        Then: No gap warnings because control and telemetry are tracked separately.
        """
        messages: list[str] = []
        sink_id = logger.add(lambda msg: messages.append(str(msg)), level="WARNING")
        session = "ws-mixed-aabbccdd"
        try:
            detector = WsClientGapDetector()
            detector.inspect(
                json.dumps(
                    {
                        "type": "subscribe",
                        "session_id": session,
                        "sequence_id": 1,
                        "public_id": "p1",
                    }
                )
            )
            detector.inspect(
                json.dumps(
                    {
                        "type": "ping",
                        "session_id": session,
                        "sequence_id": 1,
                        "public_id": "p2",
                    }
                )
            )
            detector.inspect(
                json.dumps(
                    {
                        "type": "subscribe",
                        "session_id": session,
                        "sequence_id": 2,
                        "public_id": "p3",
                    }
                )
            )
        finally:
            logger.remove(sink_id)
        gap_warnings = [m for m in messages if "Gap" in m]
        assert len(gap_warnings) == 0
