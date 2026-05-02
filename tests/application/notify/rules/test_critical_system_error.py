"""Tests for ``CriticalSystemErrorRule``."""

from datetime import UTC
from datetime import datetime
from datetime import timedelta
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import pytest

from snapper.application.notify.rules.critical_system_error import CriticalSystemErrorRule
from snapper.messaging.schemas.data import HeartbeatData
from snapper.messaging.schemas.data import TickData


def _base_now() -> datetime:
    """Anchor timestamp — per-test offsets derive from this."""
    return datetime(2026, 4, 24, 12, 0, 0, tzinfo=UTC)


def _heartbeat(status: str = "warning") -> bytes:
    """Build a HeartbeatData payload matching ``system.heartbeats.*`` frames."""
    hb = HeartbeatData(
        session_id="s1",
        sequence_id=1,
        public_id="019dbb34-f439-77bd-afa8-ee5321d60307",
        timestamp=_base_now(),
        component="executor",
        sequence=1,
        status=status,
        lag_ms=0,
        meta={},
    )
    return hb.to_json().encode("utf-8")


class TestCriticalSystemErrorRule:
    """3-consecutive-WARNING gate + healthy-reset + admin fan-out + hour-dedup."""

    @pytest.mark.asyncio
    async def test_first_warning_silent(self) -> None:
        """Single WARNING never alerts (gate opens at 3 consecutive)."""
        rule = CriticalSystemErrorRule()
        repo = MagicMock()

        rows = await rule.evaluate(
            "system.heartbeats.executor.kraken",
            _heartbeat("warning"),
            repo,
            _base_now(),
        )

        assert rows == []

    @pytest.mark.asyncio
    async def test_three_consecutive_warnings_fire_fan_out(self) -> None:
        """3rd WARNING in a row fans out to every admin with READ_SYSTEM_STATUS."""
        rule = CriticalSystemErrorRule()
        repo = MagicMock()
        repo.list_users_with_permission = AsyncMock(return_value=["admin-1", "admin-2"])
        repo.list_alert_events_with_dedup_key = AsyncMock(return_value=[])
        now = _base_now()

        await rule.evaluate("system.heartbeats.executor.kraken", _heartbeat(), repo, now)
        await rule.evaluate(
            "system.heartbeats.executor.kraken",
            _heartbeat(),
            repo,
            now + timedelta(seconds=30),
        )
        rows = await rule.evaluate(
            "system.heartbeats.executor.kraken",
            _heartbeat(),
            repo,
            now + timedelta(seconds=60),
        )

        assert len(rows) == 2
        assert {r["user_public_id"] for r in rows} == {"admin-1", "admin-2"}

    @pytest.mark.asyncio
    async def test_healthy_heartbeat_resets_window(self) -> None:
        """Any HEALTHY clears the rolling window — next WARNING returns to ``1``."""
        rule = CriticalSystemErrorRule()
        repo = MagicMock()
        repo.list_users_with_permission = AsyncMock(return_value=["admin-1"])
        repo.list_alert_events_with_dedup_key = AsyncMock(return_value=[])
        now = _base_now()

        await rule.evaluate("system.heartbeats.executor.kraken", _heartbeat(), repo, now)
        await rule.evaluate(
            "system.heartbeats.executor.kraken", _heartbeat(), repo, now + timedelta(seconds=10)
        )
        await rule.evaluate(
            "system.heartbeats.executor.kraken",
            _heartbeat(status="healthy"),
            repo,
            now + timedelta(seconds=20),
        )
        rows = await rule.evaluate(
            "system.heartbeats.executor.kraken",
            _heartbeat(),
            repo,
            now + timedelta(seconds=30),
        )

        assert rows == []

    @pytest.mark.asyncio
    async def test_no_admin_users_drops_alert(self) -> None:
        """Empty admin list → no alert fires."""
        rule = CriticalSystemErrorRule()
        repo = MagicMock()
        repo.list_users_with_permission = AsyncMock(return_value=[])
        repo.list_alert_events_with_dedup_key = AsyncMock(return_value=[])
        now = _base_now()

        await rule.evaluate("system.heartbeats.executor.kraken", _heartbeat(), repo, now)
        await rule.evaluate(
            "system.heartbeats.executor.kraken", _heartbeat(), repo, now + timedelta(seconds=10)
        )
        rows = await rule.evaluate(
            "system.heartbeats.executor.kraken", _heartbeat(), repo, now + timedelta(seconds=20)
        )

        assert rows == []

    @pytest.mark.asyncio
    async def test_hour_dedup_suppresses_second_trigger_in_same_hour(self) -> None:
        """Rule skips admins whose dedup window already holds a hit this hour."""
        rule = CriticalSystemErrorRule()
        repo = MagicMock()
        repo.list_users_with_permission = AsyncMock(return_value=["admin-1"])
        repo.list_alert_events_with_dedup_key = AsyncMock(return_value=[{"public_id": "old"}])
        now = _base_now()

        await rule.evaluate("system.heartbeats.executor.kraken", _heartbeat(), repo, now)
        await rule.evaluate(
            "system.heartbeats.executor.kraken", _heartbeat(), repo, now + timedelta(seconds=10)
        )
        rows = await rule.evaluate(
            "system.heartbeats.executor.kraken", _heartbeat(), repo, now + timedelta(seconds=20)
        )

        assert rows == []

    @pytest.mark.asyncio
    async def test_malformed_topic_dropped(self) -> None:
        """Non-4-segment heartbeat topic yields [] silently."""
        rule = CriticalSystemErrorRule()
        repo = MagicMock()

        rows = await rule.evaluate(
            "system.heartbeats.executor",
            _heartbeat(),
            repo,
            _base_now(),
        )

        assert rows == []

    @pytest.mark.asyncio
    async def test_non_heartbeat_payload_dropped(self) -> None:
        """Broken JSON body yields [] without raising."""
        rule = CriticalSystemErrorRule()
        repo = MagicMock()

        rows = await rule.evaluate(
            "system.heartbeats.executor.kraken",
            b"not-json",
            repo,
            _base_now(),
        )

        assert rows == []

    @pytest.mark.asyncio
    async def test_healthy_entry_in_rolling_window_aborts_fire(self) -> None:
        """Defensive: a HEALTHY entry in the window short-circuits the fire.

        In normal operation HEALTHY triggers the window reset at the
        top of ``evaluate`` so this branch is defence-in-depth against
        a producer that ever injected a HEALTHY mid-window. The test
        manipulates ``_rolling_window`` directly to exercise the
        guard.
        """
        rule = CriticalSystemErrorRule()
        repo = MagicMock()
        rule._rolling_window[("executor", "kraken")] = [
            (_base_now(), "warning"),
            (_base_now(), "warning"),
            (_base_now(), "healthy"),
        ]

        rows = await rule.evaluate(
            "system.heartbeats.executor.kraken",
            _heartbeat("warning"),
            repo,
            _base_now(),
        )

        assert rows == []

    @pytest.mark.asyncio
    async def test_non_heartbeat_schema_dropped(self) -> None:
        """Well-formed non-HeartbeatData (e.g. TickData) yields []."""
        tick = TickData(
            session_id="s",
            sequence_id=1,
            public_id="pid",
            timestamp=_base_now(),
            instrument="BTC-USD",
            volume=0.0,
            exchange="kraken",
        )
        rule = CriticalSystemErrorRule()
        repo = MagicMock()

        rows = await rule.evaluate(
            "system.heartbeats.executor.kraken",
            tick.to_json().encode("utf-8"),
            repo,
            _base_now(),
        )

        assert rows == []
