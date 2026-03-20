"""Tests for WebSocket control and telemetry recording in the dispatcher."""

import json
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest

from snapper.interface.websocket.dispatcher import _extract_client_provenance
from snapper.interface.websocket.dispatcher import _record_ws_control
from snapper.interface.websocket.dispatcher import _record_ws_telemetry
from snapper.messaging.infrastructure.publisher import SequenceTracker

TEST_DB_URL = "sqlite+aiosqlite:///:memory:"


def _mock_repo_session() -> tuple[MagicMock, AsyncMock]:
    """Build a mock repository and session pair for control/telemetry tests.

    Returns:
        Tuple of (mock_repo, mock_session) configured for async context
        manager usage with synchronous ``add`` method.
    """
    mock_session = AsyncMock(add=MagicMock())
    mock_ctx = AsyncMock()
    mock_ctx.__aenter__ = AsyncMock(return_value=mock_session)
    mock_ctx.__aexit__ = AsyncMock(return_value=False)
    mock_repo = MagicMock()
    mock_repo.session.return_value = mock_ctx
    return mock_repo, mock_session


class TestExtractClientProvenance:
    """Tests for _extract_client_provenance helper."""

    def test_none_payload_returns_none_pair(self) -> None:
        """None payload returns (None, None)."""
        assert _extract_client_provenance(None) == (None, None)

    def test_invalid_json_returns_none_pair(self) -> None:
        """Malformed JSON returns (None, None)."""
        assert _extract_client_provenance("not-json{") == (None, None)

    def test_non_dict_json_returns_none_pair(self) -> None:
        """JSON array returns (None, None)."""
        assert _extract_client_provenance("[1, 2]") == (None, None)

    def test_missing_fields_returns_none_pair(self) -> None:
        """Dict without provenance keys returns (None, None)."""
        assert _extract_client_provenance('{"type": "ping"}') == (None, None)

    def test_extracts_session_id_and_public_id(self) -> None:
        """Extracts both provenance fields from valid payload."""
        payload = json.dumps({"session_id": "ses-1", "public_id": "pub-1", "type": "subscribe"})
        assert _extract_client_provenance(payload) == ("ses-1", "pub-1")

    def test_extracts_session_id_only(self) -> None:
        """Extracts session_id when public_id is absent."""
        payload = json.dumps({"session_id": "ses-1", "type": "ping"})
        assert _extract_client_provenance(payload) == ("ses-1", None)

    def test_extracts_public_id_only(self) -> None:
        """Extracts public_id when session_id is absent."""
        payload = json.dumps({"public_id": "pub-1", "type": "ping"})
        assert _extract_client_provenance(payload) == (None, "pub-1")

    def test_empty_strings_treated_as_none(self) -> None:
        """Empty string provenance fields are normalized to None."""
        payload = json.dumps({"session_id": "", "public_id": "", "type": "ping"})
        assert _extract_client_provenance(payload) == (None, None)


class TestRecordWsControl:
    """Tests for _record_ws_control helper."""

    @pytest.mark.asyncio
    async def test_skips_when_db_url_is_none(self) -> None:
        """No DB call when db_url is None."""
        tracker = SequenceTracker()
        with patch(
            "snapper.interface.websocket.dispatcher.get_repository",
        ) as mock_get_repo:
            await _record_ws_control(None, tracker, "auth", "ok")
        mock_get_repo.assert_not_called()

    @pytest.mark.asyncio
    async def test_writes_control_row(self) -> None:
        """Control row is persisted with correct fields."""
        tracker = SequenceTracker()
        mock_repo, mock_session = _mock_repo_session()

        with patch(
            "snapper.interface.websocket.dispatcher.get_repository",
            return_value=mock_repo,
        ):
            await _record_ws_control(
                TEST_DB_URL,
                tracker,
                "subscribe",
                "ok",
                raw_payload='{"topics": ["market.kraken"]}',
            )

        mock_session.add.assert_called_once()
        row = mock_session.add.call_args[0][0]
        assert row.transport == "ws"
        assert row.direction == "inbound"
        assert row.message_type == "subscribe"
        assert row.outcome == "ok"
        mock_session.commit.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_error_outcome_with_detail(self) -> None:
        """Error outcome and detail are stored on the control row."""
        tracker = SequenceTracker()
        mock_repo, mock_session = _mock_repo_session()

        with patch(
            "snapper.interface.websocket.dispatcher.get_repository",
            return_value=mock_repo,
        ):
            await _record_ws_control(
                TEST_DB_URL,
                tracker,
                "error",
                "error",
                detail="Invalid message format: 2 errors",
            )

        row = mock_session.add.call_args[0][0]
        assert row.outcome == "error"
        assert row.detail == "Invalid message format: 2 errors"

    @pytest.mark.asyncio
    async def test_non_blocking_on_db_failure(self) -> None:
        """DB failure is swallowed without raising."""
        tracker = SequenceTracker()
        mock_repo, mock_session = _mock_repo_session()
        mock_session.commit = AsyncMock(side_effect=RuntimeError("db down"))

        with patch(
            "snapper.interface.websocket.dispatcher.get_repository",
            return_value=mock_repo,
        ):
            await _record_ws_control(
                TEST_DB_URL,
                tracker,
                "auth",
                "ok",
            )

    @pytest.mark.asyncio
    async def test_sensitive_payload_redacted(self) -> None:
        """Sensitive keys in WS payload are redacted."""
        tracker = SequenceTracker()
        mock_repo, mock_session = _mock_repo_session()

        with patch(
            "snapper.interface.websocket.dispatcher.get_repository",
            return_value=mock_repo,
        ):
            await _record_ws_control(
                TEST_DB_URL,
                tracker,
                "auth",
                "ok",
                raw_payload='{"token": "secret123", "type": "auth"}',
            )

        row = mock_session.add.call_args[0][0]
        parsed = json.loads(row.payload)
        assert parsed["token"] == "[REDACTED]"
        assert parsed["type"] == "auth"

    @pytest.mark.asyncio
    async def test_client_provenance_extracted_from_payload(self) -> None:
        """Client session_id and public_id are extracted and stored."""
        tracker = SequenceTracker()
        mock_repo, mock_session = _mock_repo_session()
        payload = json.dumps(
            {
                "type": "subscribe",
                "topics": ["market.kraken"],
                "session_id": "client-ses-1",
                "public_id": "client-pub-1",
            }
        )

        with patch(
            "snapper.interface.websocket.dispatcher.get_repository",
            return_value=mock_repo,
        ):
            await _record_ws_control(TEST_DB_URL, tracker, "subscribe", "ok", raw_payload=payload)

        row = mock_session.add.call_args[0][0]
        assert row.client_session_id == "client-ses-1"
        assert row.client_public_id == "client-pub-1"

    @pytest.mark.asyncio
    async def test_client_provenance_none_when_not_present(self) -> None:
        """Client provenance is None when payload lacks provenance fields."""
        tracker = SequenceTracker()
        mock_repo, mock_session = _mock_repo_session()
        payload = json.dumps({"type": "ping"})

        with patch(
            "snapper.interface.websocket.dispatcher.get_repository",
            return_value=mock_repo,
        ):
            await _record_ws_control(TEST_DB_URL, tracker, "ping", "ok", raw_payload=payload)

        row = mock_session.add.call_args[0][0]
        assert row.client_session_id is None
        assert row.client_public_id is None

    @pytest.mark.asyncio
    async def test_client_provenance_none_when_no_payload(self) -> None:
        """Client provenance is None when raw_payload is None."""
        tracker = SequenceTracker()
        mock_repo, mock_session = _mock_repo_session()

        with patch(
            "snapper.interface.websocket.dispatcher.get_repository",
            return_value=mock_repo,
        ):
            await _record_ws_control(TEST_DB_URL, tracker, "auth", "ok")

        row = mock_session.add.call_args[0][0]
        assert row.client_session_id is None
        assert row.client_public_id is None


class TestRecordWsTelemetry:
    """Tests for _record_ws_telemetry helper."""

    @pytest.mark.asyncio
    async def test_skips_when_db_url_is_none(self) -> None:
        """No DB call when db_url is None, but counter still increments."""
        tracker = SequenceTracker()
        mock_settings = MagicMock()
        mock_settings.telemetry_recording_enabled = True
        with (
            patch(
                "snapper.interface.websocket.dispatcher.get_repository",
            ) as mock_get_repo,
            patch(
                "snapper.interface.websocket.dispatcher.get_settings",
                return_value=mock_settings,
            ),
        ):
            await _record_ws_telemetry(None, tracker, "ping")
        mock_get_repo.assert_not_called()
        assert tracker.next_sequence("server.telemetry") == 2

    @pytest.mark.asyncio
    async def test_counter_increments_when_disabled(self) -> None:
        """Telemetry counter increments even when recording is disabled."""
        tracker = SequenceTracker()
        mock_settings = MagicMock()
        mock_settings.telemetry_recording_enabled = False
        with (
            patch(
                "snapper.interface.websocket.dispatcher.get_repository",
            ) as mock_get_repo,
            patch(
                "snapper.interface.websocket.dispatcher.get_settings",
                return_value=mock_settings,
            ),
        ):
            await _record_ws_telemetry(TEST_DB_URL, tracker, "ping")
        mock_get_repo.assert_not_called()
        assert tracker.next_sequence("server.telemetry") == 2

    @pytest.mark.asyncio
    async def test_writes_telemetry_row_when_enabled(self) -> None:
        """Telemetry row is persisted when recording is enabled."""
        tracker = SequenceTracker()
        mock_repo, mock_session = _mock_repo_session()
        mock_settings = MagicMock()
        mock_settings.telemetry_recording_enabled = True

        with (
            patch(
                "snapper.interface.websocket.dispatcher.get_repository",
                return_value=mock_repo,
            ),
            patch(
                "snapper.interface.websocket.dispatcher.get_settings",
                return_value=mock_settings,
            ),
        ):
            await _record_ws_telemetry(TEST_DB_URL, tracker, "ping", raw_payload='{"type": "ping"}')

        mock_session.add.assert_called_once()
        row = mock_session.add.call_args[0][0]
        assert row.transport == "ws"
        assert row.direction == "inbound"
        assert row.message_type == "ping"
        assert row.payload == '{"type": "ping"}'
        mock_session.commit.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_non_blocking_on_db_failure(self) -> None:
        """DB failure in telemetry write is swallowed."""
        tracker = SequenceTracker()
        mock_repo, mock_session = _mock_repo_session()
        mock_session.commit = AsyncMock(side_effect=RuntimeError("db down"))
        mock_settings = MagicMock()
        mock_settings.telemetry_recording_enabled = True

        with (
            patch(
                "snapper.interface.websocket.dispatcher.get_repository",
                return_value=mock_repo,
            ),
            patch(
                "snapper.interface.websocket.dispatcher.get_settings",
                return_value=mock_settings,
            ),
        ):
            await _record_ws_telemetry(TEST_DB_URL, tracker, "ping")

    @pytest.mark.asyncio
    async def test_telemetry_row_uses_pre_incremented_sequence(self) -> None:
        """Telemetry row uses the sequence assigned before the gating check."""
        tracker = SequenceTracker()
        mock_repo, mock_session = _mock_repo_session()
        mock_settings = MagicMock()
        mock_settings.telemetry_recording_enabled = True

        with (
            patch(
                "snapper.interface.websocket.dispatcher.get_repository",
                return_value=mock_repo,
            ),
            patch(
                "snapper.interface.websocket.dispatcher.get_settings",
                return_value=mock_settings,
            ),
        ):
            await _record_ws_telemetry(TEST_DB_URL, tracker, "ping")

        row = mock_session.add.call_args[0][0]
        assert row.sequence_id == 1
        assert row.session_id == tracker.session_id
