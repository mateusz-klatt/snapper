"""Tests for the user-level alert default routes.

Covers ``GET /api/alert_defaults`` + ``PATCH /api/alert_defaults``
in ``src/snapper/server/alert_default_routes.py``. Provenance-stamping
is exercised end to end through a real ``SequenceTracker`` on
``request.app.state``; ownership is enforced server-side via the
authenticated principal so the routes never accept a foreign
``user_public_id`` from the body.
"""

from datetime import UTC
from datetime import datetime
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import pytest
from fastapi import Request

from snapper.api.schemas.devices import UpdateUserAlertDefaultCommand
from snapper.api.schemas.devices import UserAlertDefaultBody
from snapper.auth.domain.roles import UserRole
from snapper.auth.schemas.principal import AuthPrincipal
from snapper.data.repository_types import UserAlertDefaultRow
from snapper.messaging.infrastructure.publisher import SequenceTracker
from snapper.server.alert_default_routes import list_alert_defaults
from snapper.server.alert_default_routes import router as alert_default_router
from snapper.server.alert_default_routes import update_alert_default
from snapper.server.json_body import json_body


def _ts() -> datetime:
    """Deterministic UTC now for test fixtures."""
    return datetime(2026, 4, 25, 12, 0, 0, tzinfo=UTC)


def _make_request() -> Request:
    """Return a FastAPI ``Request`` with a live ``SequenceTracker`` attached."""
    req = MagicMock(spec=Request)
    req.app.state.rest_tracker = SequenceTracker()
    return req


def _principal(user_public_id: str = "user-alpha") -> AuthPrincipal:
    """Return a minimal VIEWER principal bound to ``user_public_id``."""
    return AuthPrincipal(
        username="alpha",
        role=UserRole.VIEWER,
        user_public_id=user_public_id,
    )


def _default_row(
    public_id: str,
    user_public_id: str = "user-alpha",
    alert_type: str = "order_fill_full",
    enabled: bool = True,
    min_priority: str = "medium",
) -> UserAlertDefaultRow:
    """Return a fully-populated active ``UserAlertDefaultRow`` fixture."""
    return UserAlertDefaultRow(
        public_id=public_id,
        session_id="sid",
        sequence_id=1,
        timestamp=_ts(),
        known_to=datetime(9999, 12, 31, 23, 59, 59, tzinfo=UTC),
        user_public_id=user_public_id,
        alert_type=alert_type,
        enabled=enabled,
        min_priority=min_priority,
    )


def _update_command(
    alert_type: str = "order_fill_full",
    enabled: bool = True,
    min_priority: str = "medium",
) -> UpdateUserAlertDefaultCommand:
    """Return a default valid ``UpdateUserAlertDefaultCommand``."""
    return UpdateUserAlertDefaultCommand(
        session_id="client-sid",
        sequence_id=1,
        public_id="client-envelope-pid",
        timestamp=_ts(),
        payload=UserAlertDefaultBody(
            alert_type=alert_type,
            enabled=enabled,
            min_priority=min_priority,
        ),
    )


class TestListAlertDefaults:
    """Behaviour of ``GET /api/alert_defaults``."""

    @pytest.mark.asyncio
    async def test_returns_only_principal_owned_defaults(self) -> None:
        """The repo is queried with the caller's ``user_public_id``."""
        repo = AsyncMock()
        repo.list_user_alert_defaults = AsyncMock(
            return_value=[
                _default_row("def-1", alert_type="order_fill_full"),
                _default_row("def-2", alert_type="order_rejected", enabled=False),
            ]
        )

        response = await list_alert_defaults(
            request=_make_request(), principal=_principal(), repo=repo
        )

        assert response.count == 2
        assert {d.public_id for d in response.payload} == {"def-1", "def-2"}
        repo.list_user_alert_defaults.assert_awaited_once_with("user-alpha")

    @pytest.mark.asyncio
    async def test_returns_empty_envelope_when_no_defaults(self) -> None:
        """Empty repo projection returns count=0 envelope, not 404.

        Empty list is the legitimate "no overrides" state — the
        alert-routing layer falls through to the in-app defaults.
        """
        repo = AsyncMock()
        repo.list_user_alert_defaults = AsyncMock(return_value=[])

        response = await list_alert_defaults(
            request=_make_request(), principal=_principal(), repo=repo
        )

        assert response.count == 0
        assert response.payload == []


class TestUpdateAlertDefault:
    """Behaviour of ``PATCH /api/alert_defaults``."""

    @pytest.mark.asyncio
    async def test_synthesizes_response_from_body_and_returned_public_id(self) -> None:
        """Response is synthesized from body + repo-returned public_id.

        Mirrors the post-upsert race fix locked in for
        ``upsert_device_alert_pref``: the route trusts the
        repo-returned ``public_id`` and reconstructs the response
        from the validated body, avoiding a re-read that could race
        against concurrent writers on the same ``(user, alert_type)``
        key.
        """
        repo = AsyncMock()
        repo.upsert_user_alert_default = AsyncMock(return_value="def-pub-1")

        response = await update_alert_default(
            request=_make_request(),
            _csrf=None,
            command=_update_command(enabled=False, min_priority="high"),
            principal=_principal(),
            repo=repo,
        )

        assert response.payload.public_id == "def-pub-1"
        assert response.payload.user_public_id == "user-alpha"
        assert response.payload.alert_type == "order_fill_full"
        assert response.payload.enabled is False
        assert response.payload.min_priority == "high"
        repo.upsert_user_alert_default.assert_awaited_once()
        repo.list_user_alert_defaults.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_user_public_id_sourced_from_principal_not_body(self) -> None:
        """The body cannot specify a foreign ``user_public_id``.

        ``UserAlertDefaultBody`` deliberately omits the field — the
        route plumbs ``principal.user_public_id`` into both the upsert
        row and the response. Belt-and-braces guard against a future
        refactor that would otherwise let a caller mutate another
        user's defaults.
        """
        repo = AsyncMock()
        repo.upsert_user_alert_default = AsyncMock(return_value="def-pub-2")

        response = await update_alert_default(
            request=_make_request(),
            _csrf=None,
            command=_update_command(),
            principal=_principal(user_public_id="user-bravo"),
            repo=repo,
        )

        assert response.payload.user_public_id == "user-bravo"
        upsert_call = repo.upsert_user_alert_default.await_args
        assert upsert_call.args[0]["user_public_id"] == "user-bravo"


class _StubRequest:
    """Minimal Request stub with pre-set body bytes for ``json_body`` exercises."""

    def __init__(self, raw: bytes) -> None:
        self._raw = raw

    async def body(self) -> bytes:
        """Return pre-set raw bytes."""
        return self._raw


class TestEnvelopeWireFormatRegression:
    """Pin the contract: PATCH /api/alert_defaults accepts ISO 8601 string timestamps.

    Mirrors the regression class on ``test_device_routes`` — same root cause,
    same shape of fix. Pydantic strict-Python mode rejects ``"...Z"`` datetime
    strings, so the route MUST validate via ``json_body()``.
    """

    @pytest.mark.asyncio()
    async def test_update_alert_default_accepts_iso8601_z_timestamp_via_json_body(self) -> None:
        """``json_body(UpdateUserAlertDefaultCommand)`` accepts a ``Z``-suffixed timestamp."""
        raw = (
            b'{"type":"update_user_alert_default_command",'
            b'"sequence_id":1,'
            b'"public_id":"a1b2c3d4-e5f6-7890-1234-567890abcdef",'
            b'"timestamp":"2026-05-07T14:39:47Z",'
            b'"session_id":"client-sid",'
            b'"topic":null,'
            b'"payload":{"alert_type":"order_fill_full","enabled":true,"min_priority":"medium"}}'
        )
        dep = json_body(UpdateUserAlertDefaultCommand)
        cmd: UpdateUserAlertDefaultCommand = await dep(_StubRequest(raw))
        assert isinstance(cmd.timestamp, datetime)
        assert cmd.payload.alert_type == "order_fill_full"

    def test_patch_route_wired_through_json_body(self) -> None:
        """The PATCH /alert_defaults route's ``command`` is ``json_body``-bound."""
        patch_route = next(
            r
            for r in alert_default_router.routes
            if getattr(r, "path", "") == "/alert_defaults" and "PATCH" in r.methods
        )
        body_param = patch_route.dependant.dependencies
        assert any(
            getattr(d.call, "__qualname__", "").startswith("json_body.") for d in body_param
        ), "update_alert_default must inject body via json_body() to accept str datetime envelopes"
