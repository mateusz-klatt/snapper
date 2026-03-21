"""Tests for health endpoint gap detection statistics."""

import contextlib
import datetime as dt
from typing import Any
from unittest.mock import MagicMock

from fastapi.testclient import TestClient

from snapper.api.schemas.health import GapDetectionStats
from snapper.api.schemas.health import GapStatsSchema
from snapper.auth.dependencies import require_authentication
from snapper.auth.dependencies import validate_csrf_token
from snapper.auth.domain.roles import UserRole
from snapper.auth.schemas.user import UserProfile
from snapper.messaging.infrastructure.gap_detector import GapDetector
from snapper.messaging.infrastructure.gap_detector import GapDetectorStats
from snapper.server.app import _collect_gap_detection_stats
from snapper.server.app import _gap_detector_stats_to_schema
from snapper.server.app import create_app

_SID = "test-session"
_SEQ = 1
_TS = dt.datetime.now(dt.UTC)


class TestGapDetectorStatsToSchema:
    """Tests for _gap_detector_stats_to_schema helper."""

    def test_converts_all_fields(self) -> None:
        """All GapDetectorStats fields are mapped to GapStatsSchema."""
        stats = GapDetectorStats(
            gaps_detected=3,
            session_resets=1,
            duplicates=2,
            mid_stream_joins=4,
            rejected_unstamped=5,
        )
        schema = _gap_detector_stats_to_schema(stats, _SID, _SEQ, _TS)
        assert schema.gaps_detected == 3
        assert schema.session_resets == 1
        assert schema.duplicates == 2
        assert schema.mid_stream_joins == 4
        assert schema.rejected_unstamped == 5

    def test_zero_defaults(self) -> None:
        """Default GapDetectorStats converts to all-zero schema."""
        schema = _gap_detector_stats_to_schema(GapDetectorStats(), _SID, _SEQ, _TS)
        assert schema.gaps_detected == 0
        assert schema.session_resets == 0
        assert schema.duplicates == 0
        assert schema.mid_stream_joins == 0
        assert schema.rejected_unstamped == 0


class TestCollectGapDetectionStats:
    """Tests for _collect_gap_detection_stats aggregation."""

    def test_bridge_stats_collected(self) -> None:
        """Bridge gap detector stats are included in the response."""
        bridge_detector = GapDetector("bridge")
        bridge_detector.stats.gaps_detected = 7

        mock_bridge = MagicMock()
        mock_bridge._gap_detector = bridge_detector
        mock_manager = MagicMock()
        mock_manager.zmq_bridge = mock_bridge

        result = _collect_gap_detection_stats(mock_manager, None, _SID, _SEQ, _TS)
        assert result.bridge.gaps_detected == 7
        assert result.rest_clients == {}

    def test_rest_client_detectors_collected(self) -> None:
        """REST client gap detectors are included per session."""
        bridge_detector = GapDetector("bridge")
        mock_bridge = MagicMock()
        mock_bridge._gap_detector = bridge_detector
        mock_manager = MagicMock()
        mock_manager.zmq_bridge = mock_bridge

        client_detector = GapDetector("rest-client:abc12345")
        client_detector.stats.gaps_detected = 2
        client_detector.stats.session_resets = 1
        middleware_detectors: dict[str, Any] = {"abc-session": client_detector}

        result = _collect_gap_detection_stats(mock_manager, middleware_detectors, _SID, _SEQ, _TS)
        assert "abc-session" in result.rest_clients
        assert result.rest_clients["abc-session"].gaps_detected == 2
        assert result.rest_clients["abc-session"].session_resets == 1

    def test_empty_middleware_detectors(self) -> None:
        """Empty middleware detectors dict produces empty rest_clients."""
        bridge_detector = GapDetector("bridge")
        mock_bridge = MagicMock()
        mock_bridge._gap_detector = bridge_detector
        mock_manager = MagicMock()
        mock_manager.zmq_bridge = mock_bridge

        result = _collect_gap_detection_stats(mock_manager, {}, _SID, _SEQ, _TS)
        assert result.rest_clients == {}


class TestHealthEndpointGapStats:
    """Tests for gap_detection field in health endpoint response."""

    def setup_method(self) -> None:
        """Initialize test client with dependency overrides."""
        self.app = create_app()

        def skip_csrf_validation() -> None:
            return None

        def skip_authentication() -> UserProfile:
            return UserProfile(username="test_user", role=UserRole.ADMIN)

        self.app.dependency_overrides[validate_csrf_token] = skip_csrf_validation
        self.app.dependency_overrides[require_authentication] = skip_authentication
        self.client = TestClient(self.app)

    def teardown_method(self) -> None:
        """Clean up test client resources."""
        with contextlib.suppress(Exception):
            self.client.close()

    def test_health_response_contains_gap_detection(self) -> None:
        """Health endpoint includes gap_detection field."""
        response = self.client.get("/api/health")
        assert response.status_code == 200
        data = response.json()
        assert "gap_detection" in data
        gap = data["gap_detection"]
        assert "bridge" in gap
        assert "rest_clients" in gap

    def test_health_gap_detection_bridge_zeroes(self) -> None:
        """Bridge gap stats are zero when no messages have been processed."""
        response = self.client.get("/api/health")
        data = response.json()
        bridge = data["gap_detection"]["bridge"]
        assert bridge["gaps_detected"] == 0
        assert bridge["session_resets"] == 0
        assert bridge["duplicates"] == 0
        assert bridge["mid_stream_joins"] == 0
        assert bridge["rejected_unstamped"] == 0


class TestGapStatsSchemaValidation:
    """Tests for GapStatsSchema and GapDetectionStats Pydantic models."""

    def test_gap_stats_schema_defaults(self) -> None:
        """GapStatsSchema fields default to zero."""
        schema = GapStatsSchema()
        assert schema.gaps_detected == 0
        assert schema.session_resets == 0
        assert schema.duplicates == 0
        assert schema.mid_stream_joins == 0
        assert schema.rejected_unstamped == 0

    def test_gap_detection_stats_defaults(self) -> None:
        """GapDetectionStats has default bridge and empty rest_clients."""
        stats = GapDetectionStats()
        assert stats.bridge.gaps_detected == 0
        assert stats.rest_clients == {}

    def test_gap_detection_stats_with_data(self) -> None:
        """GapDetectionStats accepts populated bridge and rest_clients."""
        bridge = GapStatsSchema(gaps_detected=5)
        clients = {"sess-1": GapStatsSchema(duplicates=3)}
        stats = GapDetectionStats(bridge=bridge, rest_clients=clients)
        assert stats.bridge.gaps_detected == 5
        assert stats.rest_clients["sess-1"].duplicates == 3
