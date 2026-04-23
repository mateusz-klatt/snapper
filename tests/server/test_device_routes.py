"""Tests for the iOS Push Foundation device-management routes (BE-1c).

Covers the four endpoints in ``src/snapper/server/device_routes.py``.
Each test AAA-structured, AsyncMock-backed Repository, real
``SequenceTracker`` on ``request.app.state`` so the provenance-
stamping path is exercised end to end. Ownership checks and
scope-narrowed 404s are verified explicitly — these are the paths
that protect one user's devices from another.
"""

from datetime import UTC
from datetime import datetime
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import pytest
from fastapi import HTTPException
from fastapi import Request

from snapper.api.schemas.devices import DeviceAlertPrefBody
from snapper.api.schemas.devices import RegisterDeviceBody
from snapper.api.schemas.devices import RegisterDeviceCommand
from snapper.api.schemas.devices import UpdateDevicePrefCommand
from snapper.auth.domain.roles import UserRole
from snapper.auth.schemas.principal import AuthPrincipal
from snapper.data.repository_types import DeviceAlertPrefRow
from snapper.data.repository_types import NotificationDeviceRow
from snapper.messaging.infrastructure.publisher import SequenceTracker
from snapper.server.device_routes import delete_device
from snapper.server.device_routes import list_devices
from snapper.server.device_routes import register_device
from snapper.server.device_routes import update_device_pref


def _ts() -> datetime:
    """Deterministic UTC now for test fixtures."""
    return datetime(2026, 4, 23, 12, 0, 0, tzinfo=UTC)


def _make_request() -> Request:
    """Return a FastAPI ``Request`` mock with a live ``SequenceTracker`` attached.

    The SCD2 write helpers in ``device_routes`` reach for
    ``request.app.state.rest_tracker`` to mint ``session_id`` /
    ``sequence_id`` — using a real tracker (not another mock) keeps
    the provenance-stamping code path on the happy path without
    mocking the tracker's own contract.
    """
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


def _device_row(
    public_id: str,
    user_public_id: str = "user-alpha",
    device_token: str = "a" * 64,
    device_id: str = "dev-1",
    env: str = "sandbox",
) -> NotificationDeviceRow:
    """Return a fully-populated active ``NotificationDeviceRow`` fixture."""
    return NotificationDeviceRow(
        public_id=public_id,
        session_id="sid",
        sequence_id=1,
        timestamp=_ts(),
        known_to=datetime(9999, 12, 31, 23, 59, 59, tzinfo=UTC),
        user_public_id=user_public_id,
        device_token=device_token,
        device_id=device_id,
        platform="ios",
        env=env,
        app_version=None,
        previews_mode="private",
        registered_at=_ts(),
        last_seen_at=None,
    )


def _pref_row(
    public_id: str,
    device_public_id: str,
    alert_type: str = "order_fill_full",
    operator_public_id: str | None = None,
    wallet_public_id: str | None = None,
    enabled: bool = True,
) -> DeviceAlertPrefRow:
    """Return a fully-populated active ``DeviceAlertPrefRow`` fixture."""
    return DeviceAlertPrefRow(
        public_id=public_id,
        session_id="sid",
        sequence_id=1,
        timestamp=_ts(),
        known_to=datetime(9999, 12, 31, 23, 59, 59, tzinfo=UTC),
        device_public_id=device_public_id,
        alert_type=alert_type,
        operator_public_id=operator_public_id,
        wallet_public_id=wallet_public_id,
        enabled=enabled,
        min_priority="medium",
        quiet_hours_start_min=None,
        quiet_hours_end_min=None,
        mute_until=None,
        timezone="UTC",
    )


def _register_command(token: str = "a" * 64) -> RegisterDeviceCommand:
    """Return a default valid ``RegisterDeviceCommand``."""
    return RegisterDeviceCommand(
        session_id="client-sid",
        sequence_id=1,
        public_id="client-envelope-pid",
        timestamp=_ts(),
        payload=RegisterDeviceBody(
            device_token=token,
            device_id="dev-identifier-abc",
            env="sandbox",
        ),
    )


class TestRegisterDevice:
    """Behaviour of ``POST /api/devices``."""

    @pytest.mark.asyncio
    async def test_register_returns_active_row_after_upsert(self) -> None:
        """Happy path: upsert returns a public_id that maps to an active row."""
        repo = AsyncMock()
        repo.upsert_notification_device = AsyncMock(return_value="dev-pub-1")
        repo.list_active_notification_devices_for_user = AsyncMock(
            return_value=[_device_row("dev-pub-1")]
        )

        response = await register_device(
            request=_make_request(),
            command=_register_command(),
            principal=_principal(),
            repo=repo,
        )

        assert response.payload.public_id == "dev-pub-1"
        assert response.payload.user_public_id == "user-alpha"
        repo.upsert_notification_device.assert_awaited_once()
        repo.list_active_notification_devices_for_user.assert_awaited_once_with("user-alpha")

    @pytest.mark.asyncio
    async def test_register_500_when_active_row_missing_after_upsert(self) -> None:
        """Defensive: an unexpected missing active row raises 500.

        This branch protects against a logic error downstream — the
        upsert returned a public_id but the subsequent active-list
        call didn't contain it, meaning the row was closed by a
        concurrent writer in the tiny window between the two reads.
        We fail loud rather than invent data.
        """
        repo = AsyncMock()
        repo.upsert_notification_device = AsyncMock(return_value="dev-missing")
        repo.list_active_notification_devices_for_user = AsyncMock(return_value=[])

        with pytest.raises(HTTPException) as exc:
            await register_device(
                request=_make_request(),
                command=_register_command(),
                principal=_principal(),
                repo=repo,
            )

        assert exc.value.status_code == 500


class TestListDevices:
    """Behaviour of ``GET /api/devices``."""

    @pytest.mark.asyncio
    async def test_returns_only_principal_owned_devices(self) -> None:
        """The repo is queried with the caller's ``user_public_id``."""
        repo = AsyncMock()
        repo.list_active_notification_devices_for_user = AsyncMock(
            return_value=[_device_row("dev-1"), _device_row("dev-2")]
        )

        response = await list_devices(request=_make_request(), principal=_principal(), repo=repo)

        assert response.count == 2
        assert {d.public_id for d in response.payload} == {"dev-1", "dev-2"}
        repo.list_active_notification_devices_for_user.assert_awaited_once_with("user-alpha")

    @pytest.mark.asyncio
    async def test_returns_empty_when_no_devices(self) -> None:
        """Empty repo projection returns count=0 envelope, not 404."""
        repo = AsyncMock()
        repo.list_active_notification_devices_for_user = AsyncMock(return_value=[])

        response = await list_devices(request=_make_request(), principal=_principal(), repo=repo)

        assert response.count == 0
        assert response.payload == []


class TestDeleteDevice:
    """Behaviour of ``DELETE /api/devices/{public_id}``."""

    @pytest.mark.asyncio
    async def test_soft_deletes_owned_device(self) -> None:
        """Owned device is closed via ``mark_notification_device_inactive``."""
        repo = AsyncMock()
        repo.list_active_notification_devices_for_user = AsyncMock(
            return_value=[_device_row("dev-own")]
        )
        repo.mark_notification_device_inactive = AsyncMock()

        response = await delete_device(
            request=_make_request(),
            device_public_id="dev-own",
            principal=_principal(),
            repo=repo,
        )

        assert "dev-own" in response.payload
        repo.mark_notification_device_inactive.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_returns_404_when_device_not_owned(self) -> None:
        """Foreign or unknown public_id yields 404, not 403.

        Returning 404 prevents a caller from probing whether a
        given ``public_id`` exists on another user's account.
        """
        repo = AsyncMock()
        repo.list_active_notification_devices_for_user = AsyncMock(
            return_value=[_device_row("dev-own")]
        )
        repo.mark_notification_device_inactive = AsyncMock()

        with pytest.raises(HTTPException) as exc:
            await delete_device(
                request=_make_request(),
                device_public_id="dev-other",
                principal=_principal(),
                repo=repo,
            )

        assert exc.value.status_code == 404
        repo.mark_notification_device_inactive.assert_not_awaited()


class TestUpdateDevicePref:
    """Behaviour of ``PATCH /api/devices/{public_id}/prefs``."""

    def _pref_command(
        self,
        alert_type: str = "order_fill_full",
        enabled: bool = True,
        operator_public_id: str | None = None,
        wallet_public_id: str | None = None,
    ) -> UpdateDevicePrefCommand:
        return UpdateDevicePrefCommand(
            session_id="client-sid",
            sequence_id=1,
            public_id="client-envelope-pid",
            timestamp=_ts(),
            payload=DeviceAlertPrefBody(
                alert_type=alert_type,
                enabled=enabled,
                operator_public_id=operator_public_id,
                wallet_public_id=wallet_public_id,
            ),
        )

    @pytest.mark.asyncio
    async def test_updates_owned_device_pref(self) -> None:
        """Pref upsert on an owned device returns the now-active pref row."""
        repo = AsyncMock()
        repo.list_active_notification_devices_for_user = AsyncMock(
            return_value=[_device_row("dev-own")]
        )
        repo.upsert_device_alert_pref = AsyncMock()
        repo.list_device_alert_prefs_for_user = AsyncMock(
            return_value=[
                _pref_row(
                    public_id="pref-1",
                    device_public_id="dev-own",
                    alert_type="order_fill_full",
                    enabled=False,
                )
            ]
        )

        response = await update_device_pref(
            request=_make_request(),
            device_public_id="dev-own",
            command=self._pref_command(enabled=False),
            principal=_principal(),
            repo=repo,
        )

        assert response.payload.enabled is False
        assert response.payload.alert_type == "order_fill_full"
        repo.upsert_device_alert_pref.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_returns_404_when_device_not_owned(self) -> None:
        """Pref updates are blocked when the caller doesn't own the device."""
        repo = AsyncMock()
        repo.list_active_notification_devices_for_user = AsyncMock(
            return_value=[_device_row("dev-own")]
        )
        repo.upsert_device_alert_pref = AsyncMock()

        with pytest.raises(HTTPException) as exc:
            await update_device_pref(
                request=_make_request(),
                device_public_id="dev-other",
                command=self._pref_command(),
                principal=_principal(),
                repo=repo,
            )

        assert exc.value.status_code == 404
        repo.upsert_device_alert_pref.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_500_when_pref_not_found_after_upsert(self) -> None:
        """Defensive: missing successor row after upsert raises 500."""
        repo = AsyncMock()
        repo.list_active_notification_devices_for_user = AsyncMock(
            return_value=[_device_row("dev-own")]
        )
        repo.upsert_device_alert_pref = AsyncMock()
        repo.list_device_alert_prefs_for_user = AsyncMock(return_value=[])

        with pytest.raises(HTTPException) as exc:
            await update_device_pref(
                request=_make_request(),
                device_public_id="dev-own",
                command=self._pref_command(),
                principal=_principal(),
                repo=repo,
            )

        assert exc.value.status_code == 500

    @pytest.mark.asyncio
    async def test_scope_keyed_lookup_narrows_to_matching_pref(self) -> None:
        """Scope-narrow lookup finds only the (device, alert, scope) match.

        The route's post-upsert re-read may see multiple prefs for
        the same device — the code must pick the one matching the
        incoming scope tuple, not the first by chance.
        """
        repo = AsyncMock()
        repo.list_active_notification_devices_for_user = AsyncMock(
            return_value=[_device_row("dev-own")]
        )
        repo.upsert_device_alert_pref = AsyncMock()
        repo.list_device_alert_prefs_for_user = AsyncMock(
            return_value=[
                _pref_row(
                    public_id="pref-global",
                    device_public_id="dev-own",
                    operator_public_id=None,
                    wallet_public_id=None,
                    enabled=True,
                ),
                _pref_row(
                    public_id="pref-op",
                    device_public_id="dev-own",
                    operator_public_id="op-1",
                    wallet_public_id=None,
                    enabled=False,
                ),
            ]
        )

        response = await update_device_pref(
            request=_make_request(),
            device_public_id="dev-own",
            command=self._pref_command(operator_public_id="op-1"),
            principal=_principal(),
            repo=repo,
        )

        assert response.payload.public_id == "pref-op"
        assert response.payload.operator_public_id == "op-1"
