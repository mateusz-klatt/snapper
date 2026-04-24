"""Tests for ``snapper.application.notify.routing`` (§D7 precedence cascade)."""

from datetime import UTC
from datetime import datetime
from datetime import timedelta
from typing import cast
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import pytest

from snapper.application.notify.routing import _in_quiet_hours
from snapper.application.notify.routing import _narrowest_matches
from snapper.application.notify.routing import _policy_allows
from snapper.application.notify.routing import route_alert_to_devices
from snapper.data.models import KNOWN_TO_MAX
from snapper.data.repository_types import AlertEventRow
from snapper.data.repository_types import DeviceAlertPrefRow
from snapper.data.repository_types import NotificationDeviceRow
from snapper.data.repository_types import UserAlertDefaultRow


def _now() -> datetime:
    """Fixed noon timestamp."""
    return datetime(2026, 4, 24, 12, 0, 0, tzinfo=UTC)


def _device(public_id: str = "dev-1") -> NotificationDeviceRow:
    """Build an active NotificationDeviceRow fixture."""
    return cast(
        NotificationDeviceRow,
        {
            "public_id": public_id,
            "session_id": "s",
            "sequence_id": 1,
            "timestamp": _now(),
            "known_to": KNOWN_TO_MAX,
            "user_public_id": "user-1",
            "device_token": "tok",
            "device_id": "did",
            "platform": "ios",
            "env": "sandbox",
            "app_version": None,
            "previews_mode": "private",
            "registered_at": _now(),
            "last_seen_at": None,
            "token_status": "active",
        },
    )


def _alert(
    *,
    alert_type: str = "order_fill_full",
    priority: str = "medium",
    operator: str | None = None,
    wallet: str | None = None,
    safety_critical: bool = False,
) -> AlertEventRow:
    """Build an AlertEventRow fixture."""
    return cast(
        AlertEventRow,
        {
            "public_id": "evt-1",
            "session_id": "s",
            "sequence_id": 1,
            "timestamp": _now(),
            "known_to": KNOWN_TO_MAX,
            "user_public_id": "user-1",
            "operator_public_id": operator,
            "wallet_public_id": wallet,
            "alert_type": alert_type,
            "priority": priority,
            "is_safety_critical": safety_critical,
            "title": "t",
            "body": "b",
            "payload": None,
            "dedup_key": None,
            "thread_key": None,
            "source_topic": None,
        },
    )


def _pref(
    *,
    device_public_id: str = "dev-1",
    alert_type: str = "order_fill_full",
    operator: str | None = None,
    wallet: str | None = None,
    enabled: bool = True,
    min_priority: str = "medium",
    quiet_start: int | None = None,
    quiet_end: int | None = None,
    mute_until: datetime | None = None,
) -> DeviceAlertPrefRow:
    """Build a DeviceAlertPrefRow fixture."""
    return cast(
        DeviceAlertPrefRow,
        {
            "public_id": "pref-1",
            "session_id": "s",
            "sequence_id": 1,
            "timestamp": _now(),
            "known_to": KNOWN_TO_MAX,
            "device_public_id": device_public_id,
            "alert_type": alert_type,
            "operator_public_id": operator,
            "wallet_public_id": wallet,
            "enabled": enabled,
            "min_priority": min_priority,
            "quiet_hours_start_min": quiet_start,
            "quiet_hours_end_min": quiet_end,
            "mute_until": mute_until,
            "timezone": "UTC",
        },
    )


def _user_default(
    *,
    alert_type: str = "order_fill_full",
    enabled: bool = True,
    min_priority: str = "medium",
) -> UserAlertDefaultRow:
    """Build a UserAlertDefaultRow fixture."""
    return cast(
        UserAlertDefaultRow,
        {
            "public_id": "def-1",
            "session_id": "s",
            "sequence_id": 1,
            "timestamp": _now(),
            "known_to": KNOWN_TO_MAX,
            "user_public_id": "user-1",
            "alert_type": alert_type,
            "enabled": enabled,
            "min_priority": min_priority,
        },
    )


class TestNarrowestMatches:
    """``_narrowest_matches`` orders prefs wallet > operator > device-global."""

    def test_wallet_scope_takes_precedence(self) -> None:
        """Covered by test body."""
        prefs = [
            _pref(operator=None, wallet=None),
            _pref(operator="op-1", wallet=None),
            _pref(operator="op-1", wallet="wal-1"),
        ]
        alert = _alert(operator="op-1", wallet="wal-1")

        ordered = _narrowest_matches(device=_device(), alert=alert, prefs=prefs)

        assert ordered[0]["wallet_public_id"] == "wal-1"
        assert ordered[1]["operator_public_id"] == "op-1" and ordered[1]["wallet_public_id"] is None
        assert ordered[2]["operator_public_id"] is None and ordered[2]["wallet_public_id"] is None

    def test_other_device_prefs_ignored(self) -> None:
        """Covered by test body."""
        prefs = [_pref(device_public_id="dev-other")]

        ordered = _narrowest_matches(device=_device(), alert=_alert(), prefs=prefs)

        assert ordered == []

    def test_alert_without_wallet_does_not_match_wallet_scope(self) -> None:
        """A wallet-scoped pref never matches an alert with no wallet."""
        prefs = [_pref(operator="op-1", wallet="wal-1")]

        ordered = _narrowest_matches(device=_device(), alert=_alert(operator="op-1"), prefs=prefs)

        assert ordered == []


class TestPolicyAllows:
    """``_policy_allows`` walks the 4-level cascade + safety-critical bypass."""

    def test_allow_when_pref_enabled_and_priority_meets_threshold(self) -> None:
        """Covered by test body."""
        assert (
            _policy_allows(
                device=_device(),
                alert=_alert(priority="high"),
                device_prefs=[_pref(min_priority="high")],
                user_defaults=[],
                now=_now(),
            )
            is True
        )

    def test_block_when_narrowest_pref_disabled(self) -> None:
        """Covered by test body."""
        assert (
            _policy_allows(
                device=_device(),
                alert=_alert(operator="op-1"),
                device_prefs=[_pref(operator="op-1", enabled=False)],
                user_defaults=[],
                now=_now(),
            )
            is False
        )

    def test_block_when_priority_below_threshold(self) -> None:
        """Covered by test body."""
        assert (
            _policy_allows(
                device=_device(),
                alert=_alert(priority="low"),
                device_prefs=[_pref(min_priority="medium")],
                user_defaults=[],
                now=_now(),
            )
            is False
        )

    def test_mute_until_blocks_until_time_passes(self) -> None:
        """Covered by test body."""
        future = _now() + timedelta(hours=2)
        assert (
            _policy_allows(
                device=_device(),
                alert=_alert(),
                device_prefs=[_pref(mute_until=future)],
                user_defaults=[],
                now=_now(),
            )
            is False
        )

    def test_quiet_hours_block_non_critical_but_pass_safety_critical(self) -> None:
        """Covered by test body."""
        quiet = _pref(quiet_start=11 * 60, quiet_end=13 * 60)

        assert (
            _policy_allows(
                device=_device(),
                alert=_alert(),
                device_prefs=[quiet],
                user_defaults=[],
                now=_now(),
            )
            is False
        )
        assert (
            _policy_allows(
                device=_device(),
                alert=_alert(safety_critical=True),
                device_prefs=[quiet],
                user_defaults=[],
                now=_now(),
            )
            is True
        )

    def test_falls_back_to_user_default_when_no_device_pref(self) -> None:
        """Covered by test body."""
        default = _user_default(enabled=True, min_priority="medium")

        assert (
            _policy_allows(
                device=_device(),
                alert=_alert(priority="high"),
                device_prefs=[],
                user_defaults=[default],
                now=_now(),
            )
            is True
        )

    def test_user_default_disable_blocks_alert(self) -> None:
        """Covered by test body."""
        default = _user_default(enabled=False)

        assert (
            _policy_allows(
                device=_device(),
                alert=_alert(priority="high"),
                device_prefs=[],
                user_defaults=[default],
                now=_now(),
            )
            is False
        )

    def test_default_on_for_medium_plus_when_no_pref_anywhere(self) -> None:
        """Covered by test body."""
        assert (
            _policy_allows(
                device=_device(),
                alert=_alert(priority="medium"),
                device_prefs=[],
                user_defaults=[],
                now=_now(),
            )
            is True
        )
        assert (
            _policy_allows(
                device=_device(),
                alert=_alert(priority="low"),
                device_prefs=[],
                user_defaults=[],
                now=_now(),
            )
            is False
        )

    def test_deny_wins_at_narrowest_depth(self) -> None:
        """A disabled wallet-scoped pref beats an enabled operator-scoped pref."""
        prefs = [
            _pref(operator="op-1", enabled=True),
            _pref(operator="op-1", wallet="wal-1", enabled=False),
        ]

        assert (
            _policy_allows(
                device=_device(),
                alert=_alert(operator="op-1", wallet="wal-1"),
                device_prefs=prefs,
                user_defaults=[],
                now=_now(),
            )
            is False
        )


class TestQuietHours:
    """Wrap-around + equal-bounds edge cases for ``_in_quiet_hours``."""

    def test_no_window_when_either_bound_missing(self) -> None:
        """Covered by test body."""
        pref = _pref(quiet_start=None, quiet_end=None)
        assert _in_quiet_hours(pref, _now()) is False

    def test_equal_bounds_means_disabled(self) -> None:
        """Covered by test body."""
        pref = _pref(quiet_start=120, quiet_end=120)
        assert _in_quiet_hours(pref, _now()) is False

    def test_overnight_wrap(self) -> None:
        """Start > end represents overnight — e.g. 22:00 → 07:00."""
        pref = _pref(quiet_start=22 * 60, quiet_end=7 * 60)
        early_morning = datetime(2026, 4, 24, 3, 0, tzinfo=UTC)
        evening = datetime(2026, 4, 24, 23, 0, tzinfo=UTC)
        noon = datetime(2026, 4, 24, 12, 0, tzinfo=UTC)

        assert _in_quiet_hours(pref, early_morning) is True
        assert _in_quiet_hours(pref, evening) is True
        assert _in_quiet_hours(pref, noon) is False


class TestRouteAlertToDevices:
    """End-to-end routing function — repo calls mocked."""

    @pytest.mark.asyncio
    async def test_empty_device_list_short_circuits(self) -> None:
        """Covered by test body."""
        repo = MagicMock()
        repo.list_active_notification_devices_for_user = AsyncMock(return_value=[])

        recipients = await route_alert_to_devices(alert=_alert(), repo=repo, now=_now())

        assert recipients == []
        repo.list_device_alert_prefs_for_user.assert_not_called()

    @pytest.mark.asyncio
    async def test_single_device_no_prefs_delivers_on_medium(self) -> None:
        """Covered by test body."""
        repo = MagicMock()
        repo.list_active_notification_devices_for_user = AsyncMock(return_value=[_device()])
        repo.list_device_alert_prefs_for_user = AsyncMock(return_value=[])
        repo.list_user_alert_defaults = AsyncMock(return_value=[])

        recipients = await route_alert_to_devices(
            alert=_alert(priority="medium"), repo=repo, now=_now()
        )

        assert len(recipients) == 1

    @pytest.mark.asyncio
    async def test_suppressed_device_excluded_from_recipients(self) -> None:
        """A device blocked by its pref stays out of the recipients list."""
        repo = MagicMock()
        repo.list_active_notification_devices_for_user = AsyncMock(return_value=[_device()])
        repo.list_device_alert_prefs_for_user = AsyncMock(return_value=[_pref(enabled=False)])
        repo.list_user_alert_defaults = AsyncMock(return_value=[])

        recipients = await route_alert_to_devices(
            alert=_alert(priority="high"), repo=repo, now=_now()
        )

        assert recipients == []
