"""Tests for the iOS push alert read routes.

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
from snapper.core.json_types import JsonObject
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
    payload: JsonObject | None = None,
    title: str = "Filled",
    body: str = "BTC-USD 0.1 filled",
) -> AlertEventRow:
    """Return a fully-populated active ``AlertEventRow`` fixture.

    ``payload`` and ``title`` / ``body`` are exposed so localization
    tests can pin loc_key contracts on a row without
    fabricating their own AlertEventRow shape.
    """
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
        title=title,
        body=body,
        payload=payload,
        dedup_key=None,
        thread_key=None,
        source_topic=None,
    )


def _make_repo(
    default_language: str | None = None, *, user_public_id: str = "user-alpha"
) -> MagicMock:
    """Build a repo mock with the localization ``default_language`` lookup pre-stubbed.

    Uses ``MagicMock`` as the parent (NOT ``AsyncMock``) so unstubbed
    attribute access falls back to plain MagicMock and never produces
    a dangling coroutine. Each async method the routes touch is
    individually stubbed as an ``AsyncMock`` here — adding a new repo
    call to a route therefore requires adding the matching stub here,
    which surfaces test gaps explicitly instead of silently passing on
    auto-generated AsyncMock children.

    Tests that don't otherwise care about the language path get the
    natural EN fallback via ``default_language=None``; localization
    tests pass ``default_language='pl'`` (etc) explicitly.
    """
    repo = MagicMock()
    payload: dict[str, str | None] = (
        {} if default_language is None else {user_public_id: default_language}
    )
    repo.get_default_languages_for_users = AsyncMock(return_value=payload)
    return repo


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
        repo = _make_repo()
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
        repo = _make_repo()
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
        repo = _make_repo()
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
        repo = _make_repo()
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
        repo = _make_repo()
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
        repo = _make_repo()
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
        repo = _make_repo()
        repo.get_alert_event_by_public_id = AsyncMock(return_value=None)

        error_request = _make_request()
        error_principal = _principal()
        with pytest.raises(HTTPException) as exc:
            await get_alert_event(
                request=error_request,
                alert_public_id="pid-unknown",
                principal=error_principal,
                repo=repo,
            )

        assert exc.value.status_code == 404

    @pytest.mark.asyncio
    async def test_returns_404_for_foreign_owned_alert(self) -> None:
        """An alert owned by another user is indistinguishable from unknown."""
        repo = _make_repo()
        repo.get_alert_event_by_public_id = AsyncMock(
            return_value=_alert_row("pid-foreign", user_public_id="user-gamma")
        )

        error_request = _make_request()
        error_principal = _principal("user-alpha")
        with pytest.raises(HTTPException) as exc:
            await get_alert_event(
                request=error_request,
                alert_public_id="pid-foreign",
                principal=error_principal,
                repo=repo,
            )

        assert exc.value.status_code == 404


def _phase_c_row_payload() -> JsonObject:
    """Return a payload dict shaped like the localizing notify-rule emits.

    Pinned to ``order_fill_full`` so the catalog will actually resolve
    against the Polish template; tests that need a different shape can
    override the loc-key pair inline.
    """
    return {
        "deep_link_path": "/orders/coid-1",
        "title_loc_key": "alerts.title.order_fill_full",
        "body_loc_key": "alerts.body.order_fill_full",
        "body_loc_args": ["BUY", "100", "BTCUSD", "50000.00", "Kraken"],
    }


class TestAlertHistoryLocalization:
    """Server-side ``title``/``body`` resolution + loc field exposure."""

    @pytest.mark.asyncio
    async def test_pl_user_history_returns_localized_title_and_body(self) -> None:
        """A PL user gets Polish strings AND the loc fields for in-app re-render.

        Given: ``user.default_language = 'pl'`` and a row carrying the
            localization loc_key contract.
        When: ``GET /api/alerts/history`` runs.
        Then: ``payload[0].title``/``body`` are pre-rendered in Polish
            server-side, AND ``title_loc_key`` / ``body_loc_key`` /
            ``body_loc_args`` are surfaced so iOS can re-render the row
            after an in-app locale-picker change without a round-trip.
        """
        repo = _make_repo(default_language="pl")
        repo.list_recent_alerts_for_user = AsyncMock(
            return_value=[
                _alert_row(
                    "pid-pl",
                    payload=_phase_c_row_payload(),
                    title="Order filled",
                    body="BUY 100 BTCUSD @ $50000.00 filled on Kraken",
                )
            ]
        )

        response = await list_alert_history(
            request=_make_request(),
            principal=_principal(),
            repo=repo,
            limit=10,
            before=None,
        )

        item = response.payload[0]
        assert item.title == "Zlecenie zrealizowane"
        assert "zrealizowane na Kraken" in item.body
        assert item.title_loc_key == "alerts.title.order_fill_full"
        assert item.body_loc_key == "alerts.body.order_fill_full"
        assert item.body_loc_args == [
            "BUY",
            "100",
            "BTCUSD",
            "50000.00",
            "Kraken",
        ]

    @pytest.mark.asyncio
    async def test_no_preference_returns_stored_en_strings(self) -> None:
        """A user with no language preference gets the stored EN columns.

        Given: ``user.default_language`` not set (``{}`` from the
            bulk lookup).
        When: history runs over a localization-enabled row.
        Then: response ``title``/``body`` mirror the EN columns AND
            the loc fields still flow through (iOS can re-render to
            its in-app locale even when the server has no preference
            for this user).
        """
        repo = _make_repo()
        repo.list_recent_alerts_for_user = AsyncMock(
            return_value=[
                _alert_row(
                    "pid-en",
                    payload=_phase_c_row_payload(),
                    title="Order filled",
                    body="BUY 100 BTCUSD @ $50000.00 filled on Kraken",
                )
            ]
        )

        response = await list_alert_history(
            request=_make_request(),
            principal=_principal(),
            repo=repo,
            limit=10,
            before=None,
        )

        item = response.payload[0]
        assert item.title == "Order filled"
        assert item.body == "BUY 100 BTCUSD @ $50000.00 filled on Kraken"
        assert item.title_loc_key == "alerts.title.order_fill_full"
        assert item.body_loc_key == "alerts.body.order_fill_full"

    @pytest.mark.asyncio
    async def test_legacy_row_without_loc_keys_omits_loc_fields(self) -> None:
        """Rows persisted before localization surface ``loc_key=None``.

        Given: a row whose payload predates localization (no loc_keys)
            and a PL user.
        When: history runs.
        Then: title/body fall back to EN columns; loc fields are
            ``None`` / ``[]`` so iOS knows to not attempt in-app
            re-localization (it would have no template).
        """
        repo = _make_repo(default_language="pl")
        repo.list_recent_alerts_for_user = AsyncMock(
            return_value=[
                _alert_row(
                    "pid-legacy",
                    payload={"deep_link_path": "/orders/legacy"},
                    title="Legacy stored title",
                    body="Legacy stored body",
                )
            ]
        )

        response = await list_alert_history(
            request=_make_request(),
            principal=_principal(),
            repo=repo,
            limit=10,
            before=None,
        )

        item = response.payload[0]
        assert item.title == "Legacy stored title"
        assert item.body == "Legacy stored body"
        assert item.title_loc_key is None
        assert item.body_loc_key is None
        assert item.title_loc_args == []
        assert item.body_loc_args == []

    @pytest.mark.asyncio
    async def test_malformed_body_loc_args_coerces_to_empty_list(self) -> None:
        """Non-list ``body_loc_args`` still surfaces the key with ``[]`` args.

        Given: a row whose ``body_loc_args`` is a string (corrupt or
            mis-emitted by a buggy rule).
        When: history runs.
        Then: the key is still surfaced for inspection but args are
            normalized to ``[]`` so iOS never receives a non-list on
            the wire — the resolver itself already falls back to EN
            for the title/body strings.
        """
        bad_payload = _phase_c_row_payload()
        bad_payload["body_loc_args"] = "this-is-not-a-list"
        repo = _make_repo(default_language="pl")
        repo.list_recent_alerts_for_user = AsyncMock(
            return_value=[
                _alert_row(
                    "pid-bad",
                    payload=bad_payload,
                    title="EN fallback title",
                    body="EN fallback body",
                )
            ]
        )

        response = await list_alert_history(
            request=_make_request(),
            principal=_principal(),
            repo=repo,
            limit=10,
            before=None,
        )

        item = response.payload[0]
        assert item.title == "EN fallback title"
        assert item.body == "EN fallback body"
        assert item.body_loc_key == "alerts.body.order_fill_full"
        assert item.body_loc_args == []


class TestGetAlertEventLocalization:
    """Singleton endpoint mirrors history's localization."""

    @pytest.mark.asyncio
    async def test_pl_user_singleton_returns_localized_strings(self) -> None:
        """Singleton response honors ``user.default_language`` identically.

        Server-side render must match what the sidecar emitted to APNs;
        otherwise an alert that pushed in PL would render in EN when
        re-opened from the history list.
        """
        repo = _make_repo(default_language="pl")
        repo.get_alert_event_by_public_id = AsyncMock(
            return_value=_alert_row(
                "pid-singleton",
                payload=_phase_c_row_payload(),
                title="Order filled",
                body="BUY 100 BTCUSD @ $50000.00 filled on Kraken",
            )
        )

        response = await get_alert_event(
            request=_make_request(),
            alert_public_id="pid-singleton",
            principal=_principal(),
            repo=repo,
        )

        assert response.payload.title == "Zlecenie zrealizowane"
        assert "zrealizowane na Kraken" in response.payload.body
        assert response.payload.title_loc_key == "alerts.title.order_fill_full"
        assert response.payload.body_loc_key == "alerts.body.order_fill_full"
