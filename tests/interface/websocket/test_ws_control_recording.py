"""Tests for WebSocket control recording in the dispatcher."""

import json
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest

from snapper.interface.websocket.dispatcher import _record_ws_control
from snapper.messaging.infrastructure.publisher import SequenceTracker


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
        mock_session = AsyncMock(add=MagicMock())
        mock_ctx = AsyncMock()
        mock_ctx.__aenter__ = AsyncMock(return_value=mock_session)
        mock_ctx.__aexit__ = AsyncMock(return_value=False)
        mock_repo = MagicMock()
        mock_repo.session.return_value = mock_ctx

        with patch(
            "snapper.interface.websocket.dispatcher.get_repository",
            return_value=mock_repo,
        ):
            await _record_ws_control(
                "sqlite+aiosqlite:///test.db",
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
        mock_session = AsyncMock(add=MagicMock())
        mock_ctx = AsyncMock()
        mock_ctx.__aenter__ = AsyncMock(return_value=mock_session)
        mock_ctx.__aexit__ = AsyncMock(return_value=False)
        mock_repo = MagicMock()
        mock_repo.session.return_value = mock_ctx

        with patch(
            "snapper.interface.websocket.dispatcher.get_repository",
            return_value=mock_repo,
        ):
            await _record_ws_control(
                "sqlite+aiosqlite:///test.db",
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
        mock_session = AsyncMock(add=MagicMock())
        mock_session.commit = AsyncMock(side_effect=RuntimeError("db down"))
        mock_ctx = AsyncMock()
        mock_ctx.__aenter__ = AsyncMock(return_value=mock_session)
        mock_ctx.__aexit__ = AsyncMock(return_value=False)
        mock_repo = MagicMock()
        mock_repo.session.return_value = mock_ctx

        with patch(
            "snapper.interface.websocket.dispatcher.get_repository",
            return_value=mock_repo,
        ):
            await _record_ws_control(
                "sqlite+aiosqlite:///test.db",
                tracker,
                "auth",
                "ok",
            )

    @pytest.mark.asyncio
    async def test_sensitive_payload_redacted(self) -> None:
        """Sensitive keys in WS payload are redacted."""
        tracker = SequenceTracker()
        mock_session = AsyncMock(add=MagicMock())
        mock_ctx = AsyncMock()
        mock_ctx.__aenter__ = AsyncMock(return_value=mock_session)
        mock_ctx.__aexit__ = AsyncMock(return_value=False)
        mock_repo = MagicMock()
        mock_repo.session.return_value = mock_ctx

        with patch(
            "snapper.interface.websocket.dispatcher.get_repository",
            return_value=mock_repo,
        ):
            await _record_ws_control(
                "sqlite+aiosqlite:///test.db",
                tracker,
                "auth",
                "ok",
                raw_payload='{"token": "secret123", "type": "auth"}',
            )

        row = mock_session.add.call_args[0][0]
        parsed = json.loads(row.payload)
        assert parsed["token"] == "[REDACTED]"
        assert parsed["type"] == "auth"
