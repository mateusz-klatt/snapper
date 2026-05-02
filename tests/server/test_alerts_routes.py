"""Tests for the iOS Push Foundation alert read routes.

Covers ``GET /api/alerts/history`` (with opaque cursor round-trip and
ownership filter) and ``GET /api/alerts/{public_id}`` (with owner-or-
404 semantics). The history cursor encoder / decoder is exercised for
well-formed, empty, tampered, and non-JSON payloads — all of which
must fall through to "page 1" rather than raising, so a stale cursor
never surfaces as a 4xx to the iOS client.
"""

import base64
import json
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import pytest
from fastapi import HTTPException
from fastapi import Request

from snapper.auth.domain.roles import UserRole
from snapper.auth.schemas.principal import AuthPrincipal
from snapper.data.repository_types import AlertEventRow
from snapper.messaging.infrastructure.publisher import SequenceTracker
from snapper.server.alerts_routes import _decode_cursor
from snapper.server.alerts_routes import _encode_cursor
from snapper.server.alerts_routes import get_alert_event
from snapper.server.alerts_routes import list_alert_history


def _ts(minutes: int = 0) -> datetime:
    """Deterministic UTC timestamp offset by ``minutes``."""
    return datetime(2026, 4, 23, 12, 0, 0, tzinfo=UTC) + timedelta(minutes=minutes)


def _make_request() -> Request:
    """Return a FastAPI ``Request`` mock with a live ``SequenceTracker``."""
    req = MagicMock(spec=Request)
    req.app.state.rest_tracker = SequenceTracker()
    return req


def _principal(user_public_id: str = "user-alpha") -> AuthPrincipal:
    """VIEWER principal bound to ``user_public_id``."""
    return AuthPrincipal(
        username="alpha",
        role=UserRole.VIEWER,
        user_public_id=user_public_id,
    )


def _alert_row(
    public_id: str,
    user_public_id: str = "user-alpha",
    alert_type: str = "order_fill_full",
    timestamp: datetime | None = None,
) -> AlertEventRow:
    """Return a fully-populated active ``AlertEventRow`` fixture."""
    return AlertEventRow(
        public_id=public_id,
        session_id="sid",
        sequence_id=1,
        timestamp=timestamp or _ts(),
        known_to=datetime(9999, 12, 31, 23, 59, 59, tzinfo=UTC),
        user_public_id=user_public_id,
        operator_public_id=None,
        wallet_public_id=None,
        alert_type=alert_type,
        priority="medium",
        is_safety_critical=False,
        title="Filled",
        body="BTC-USD 0.1 filled",
        payload=None,
        dedup_key=None,
        thread_key=None,
        source_topic=None,
    )


class TestCursorCodec:
    """Round-trip + error-tolerance of ``_encode_cursor`` / ``_decode_cursor``."""

    def test_roundtrip_preserves_timestamp_and_public_id(self) -> None:
        """Encode then decode returns the original pair intact."""
        anchor = _alert_row("pid-42", timestamp=_ts(7))

        token = _encode_cursor(anchor)
        cursor = _decode_cursor(token)

        assert cursor is not None
        assert cursor["public_id"] == "pid-42"
        assert cursor["timestamp"] == _ts(7)

    def test_none_and_empty_decode_to_none(self) -> None:
        """Both ``None`` and ``""`` map to "no cursor" (first page)."""
        assert _decode_cursor(None) is None
        assert _decode_cursor("") is None

    def test_non_base64_decodes_to_none(self) -> None:
        """Garbage input does not raise — falls through to first page."""
        assert _decode_cursor("!!! not base64 !!!") is None

    def test_valid_base64_non_json_decodes_to_none(self) -> None:
        """Well-formed base64 that isn't JSON still falls through."""
        token = base64.urlsafe_b64encode(b"not-json").decode("ascii").rstrip("=")

        assert _decode_cursor(token) is None

    def test_valid_json_wrong_shape_decodes_to_none(self) -> None:
        """A JSON array or scalar (not an object) is rejected."""
        token = base64.urlsafe_b64encode(b"[1,2,3]").decode("ascii").rstrip("=")

        assert _decode_cursor(token) is None

    def test_missing_fields_decodes_to_none(self) -> None:
        """A JSON object without ``t``/``p`` fields is rejected."""
        raw = json.dumps({"x": 1}).encode("ascii")
        token = base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")

        assert _decode_cursor(token) is None

    def test_invalid_timestamp_decodes_to_none(self) -> None:
        """A non-ISO8601 ``t`` value is rejected rather than raising."""
        raw = json.dumps({"t": "not-a-date", "p": "pid"}).encode("ascii")
        token = base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")

        assert _decode_cursor(token) is None

    def test_non_string_fields_decode_to_none(self) -> None:
        """Non-string ``t`` or ``p`` values are rejected."""
        raw = json.dumps({"t": 123, "p": "pid"}).encode("ascii")
        token = base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")

        assert _decode_cursor(token) is None


class TestListAlertHistory:
    """Behaviour of ``GET /api/alerts/history``."""

    @pytest.mark.asyncio
    async def test_returns_page_with_next_cursor_when_full(self) -> None:
        """A page exactly at limit yields a next_cursor for the last row."""
        repo = AsyncMock()
        rows = [
            _alert_row("pid-3", timestamp=_ts(3)),
            _alert_row("pid-2", timestamp=_ts(2)),
            _alert_row("pid-1", timestamp=_ts(1)),
        ]
        repo.list_recent_alerts_for_user = AsyncMock(return_value=rows)

        response = await list_alert_history(
            request=_make_request(),
            principal=_principal(),
            repo=repo,
            limit=3,
            before=None,
        )

        assert response.count == 3
        assert response.next_cursor is not None
        decoded = _decode_cursor(response.next_cursor)
        assert decoded is not None
        assert decoded["public_id"] == "pid-1"
        assert decoded["timestamp"] == _ts(1)

    @pytest.mark.asyncio
    async def test_returns_none_cursor_when_page_under_limit(self) -> None:
        """A partial page is the last page — next_cursor is None."""
        repo = AsyncMock()
        repo.list_recent_alerts_for_user = AsyncMock(
            return_value=[_alert_row("pid-1", timestamp=_ts(1))]
        )

        response = await list_alert_history(
            request=_make_request(),
            principal=_principal(),
            repo=repo,
            limit=10,
            before=None,
        )

        assert response.count == 1
        assert response.next_cursor is None

    @pytest.mark.asyncio
    async def test_passes_decoded_cursor_to_repo(self) -> None:
        """The opaque ``before`` is decoded into the internal pair."""
        repo = AsyncMock()
        repo.list_recent_alerts_for_user = AsyncMock(return_value=[])
        anchor = _alert_row("anchor-pid", timestamp=_ts(9))
        token = _encode_cursor(anchor)

        await list_alert_history(
            request=_make_request(),
            principal=_principal(),
            repo=repo,
            limit=10,
            before=token,
        )

        call_kwargs = repo.list_recent_alerts_for_user.await_args.kwargs
        assert call_kwargs["before"] is not None
        assert call_kwargs["before"]["public_id"] == "anchor-pid"
        assert call_kwargs["before"]["timestamp"] == _ts(9)

    @pytest.mark.asyncio
    async def test_malformed_cursor_returns_first_page(self) -> None:
        """A tampered cursor falls through to page 1 (not 4xx)."""
        repo = AsyncMock()
        repo.list_recent_alerts_for_user = AsyncMock(return_value=[])

        await list_alert_history(
            request=_make_request(),
            principal=_principal(),
            repo=repo,
            limit=10,
            before="@@@ totally not a cursor @@@",
        )

        call_kwargs = repo.list_recent_alerts_for_user.await_args.kwargs
        assert call_kwargs["before"] is None

    @pytest.mark.asyncio
    async def test_scopes_to_principal_user_public_id(self) -> None:
        """The repo is always queried with the caller's user_public_id."""
        repo = AsyncMock()
        repo.list_recent_alerts_for_user = AsyncMock(return_value=[])

        await list_alert_history(
            request=_make_request(),
            principal=_principal("user-beta"),
            repo=repo,
            limit=10,
            before=None,
        )

        call_kwargs = repo.list_recent_alerts_for_user.await_args.kwargs
        assert call_kwargs["user_public_id"] == "user-beta"


class TestGetAlertEvent:
    """Behaviour of ``GET /api/alerts/{public_id}``."""

    @pytest.mark.asyncio
    async def test_returns_owned_alert(self) -> None:
        """An alert owned by the caller is returned verbatim."""
        repo = AsyncMock()
        repo.get_alert_event_by_public_id = AsyncMock(
            return_value=_alert_row("pid-own", user_public_id="user-alpha")
        )

        response = await get_alert_event(
            request=_make_request(),
            alert_public_id="pid-own",
            principal=_principal(),
            repo=repo,
        )

        assert response.payload.public_id == "pid-own"
        assert response.payload.user_public_id == "user-alpha"

    @pytest.mark.asyncio
    async def test_returns_404_for_unknown_id(self) -> None:
        """Unknown id -> 404, not 403."""
        repo = AsyncMock()
        repo.get_alert_event_by_public_id = AsyncMock(return_value=None)

        with pytest.raises(HTTPException) as exc:
            await get_alert_event(
                request=_make_request(),
                alert_public_id="pid-unknown",
                principal=_principal(),
                repo=repo,
            )

        assert exc.value.status_code == 404

    @pytest.mark.asyncio
    async def test_returns_404_for_foreign_owned_alert(self) -> None:
        """An alert owned by another user is indistinguishable from unknown."""
        repo = AsyncMock()
        repo.get_alert_event_by_public_id = AsyncMock(
            return_value=_alert_row("pid-foreign", user_public_id="user-gamma")
        )

        with pytest.raises(HTTPException) as exc:
            await get_alert_event(
                request=_make_request(),
                alert_public_id="pid-foreign",
                principal=_principal("user-alpha"),
                repo=repo,
            )

        assert exc.value.status_code == 404
