"""Tests for ``CriticalSystemErrorRule``."""

from datetime import UTC
from datetime import datetime
from datetime import timedelta
from typing import Any
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
        payload = rows[0]["payload"]
        assert payload is not None
        assert payload["title_loc_key"] == "alerts.title.critical_system_error"
        assert payload["title_loc_args"] == ["executor"]
        assert payload["body_loc_key"] == "alerts.body.critical_system_error"
        assert payload["body_loc_args"] == ["executor", "kraken", "warning", 3]

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


class TestPerWalletExecutorTopics:
    """5-segment per-wallet executor heartbeat topics reach the rule."""

    @pytest.mark.asyncio
    async def test_five_segment_topic_fires_after_three_warnings(self) -> None:
        """Per-wallet executor frames alert exactly like 4-segment ones.

        Given: three consecutive WARNING frames on a 5-segment per-wallet
            executor topic (previously rejected at parse — executor
            heartbeats could NEVER alert),
        When: evaluated,
        Then: one row per admin fires, with the joined instance name in
            both the dedup key and the localized args.
        """
        rule = CriticalSystemErrorRule()
        repo = MagicMock()
        repo.list_users_with_permission = AsyncMock(return_value=["admin-1"])
        repo.list_alert_events_with_dedup_key = AsyncMock(return_value=[])
        topic = "system.heartbeats.executor.kraken.abc123def456"
        rows: list[Any] = []
        for i in range(3):
            rows = await rule.evaluate(
                topic,
                _heartbeat("warning"),
                repo,
                _base_now() + timedelta(seconds=i),
            )
        assert len(rows) == 1
        assert "executor" in rows[0]["dedup_key"]
        assert "kraken.abc123def456" in rows[0]["dedup_key"]
        payload = rows[0]["payload"]
        assert payload is not None
        assert "kraken.abc123def456" in payload["body_loc_args"]

    @pytest.mark.asyncio
    async def test_per_wallet_windows_are_independent(self) -> None:
        """Two wallet instances never cross-count toward the gate."""
        rule = CriticalSystemErrorRule()
        repo = MagicMock()
        repo.list_users_with_permission = AsyncMock(return_value=["admin-1"])
        repo.list_alert_events_with_dedup_key = AsyncMock(return_value=[])
        for i in range(2):
            await rule.evaluate(
                "system.heartbeats.executor.kraken.aaaa",
                _heartbeat("warning"),
                repo,
                _base_now() + timedelta(seconds=i),
            )
        rows = await rule.evaluate(
            "system.heartbeats.executor.kraken.bbbb",
            _heartbeat("warning"),
            repo,
            _base_now() + timedelta(seconds=2),
        )
        assert rows == []

    @pytest.mark.asyncio
    async def test_error_status_counts_toward_gate(self) -> None:
        """Synthetic park frames (status=error) trip the same gate."""
        rule = CriticalSystemErrorRule()
        repo = MagicMock()
        repo.list_users_with_permission = AsyncMock(return_value=["admin-1"])
        repo.list_alert_events_with_dedup_key = AsyncMock(return_value=[])
        topic = "system.heartbeats.executor.kraken.abc123def456"
        rows: list[Any] = []
        for i in range(3):
            rows = await rule.evaluate(
                topic, _heartbeat("error"), repo, _base_now() + timedelta(seconds=i)
            )
        assert len(rows) == 1

    @pytest.mark.asyncio
    async def test_rolling_cooldown_spans_hour_bucket_boundary(self) -> None:
        """A fire at 12:59 cannot re-page seconds later at 13:00.

        Given: A fire just before the hour flip and continued WARNING
            frames just after it (new hour bucket = new DB dedup key),
        When: The window re-fills,
        Then: The in-memory rolling cooldown suppresses the second fire —
            clock buckets alone allowed double pages across boundaries.
        """
        rule = CriticalSystemErrorRule()
        repo = MagicMock()
        repo.list_users_with_permission = AsyncMock(return_value=["admin-1"])
        repo.list_alert_events_with_dedup_key = AsyncMock(return_value=[])
        topic = "system.heartbeats.executor.kraken.abc123def456"
        boundary = datetime(2026, 4, 24, 12, 59, 57, tzinfo=UTC)
        rows: list[Any] = []
        for i in range(3):
            rows = await rule.evaluate(
                topic, _heartbeat("warning"), repo, boundary + timedelta(seconds=i)
            )
        assert len(rows) == 1
        after = await rule.evaluate(
            topic,
            _heartbeat("warning"),
            repo,
            datetime(2026, 4, 24, 13, 0, 1, tzinfo=UTC),
        )
        assert after == []

    @pytest.mark.asyncio
    async def test_boundary_suppression_survives_sidecar_restart(self) -> None:
        """The rolling guard holds even when in-memory state is wiped.

        Given: A fire persisted at 12:59 (DB row under the 12:00 bucket)
            and a FRESH rule instance (sidecar restarted — _last_fired
            gone) evaluating frames at 13:00,
        When: The window re-fills seconds after the boundary,
        Then: The previous-bucket DB probe suppresses the second page.
        """
        repo = MagicMock()
        repo.list_users_with_permission = AsyncMock(return_value=["admin-1"])

        async def windowed_rows(user_public_id: str, dedup_key: str, since: datetime) -> list[Any]:
            if "2026-04-24T12:00:00" in dedup_key:
                return [{"created_at": datetime(2026, 4, 24, 12, 59, 59, tzinfo=UTC)}]
            return []

        repo.list_alert_events_with_dedup_key = AsyncMock(side_effect=windowed_rows)
        fresh_rule = CriticalSystemErrorRule()
        topic = "system.heartbeats.executor.kraken.abc123def456"
        rows: list[Any] = []
        for i in range(3):
            rows = await fresh_rule.evaluate(
                topic,
                _heartbeat("warning"),
                repo,
                datetime(2026, 4, 24, 13, 0, 0, tzinfo=UTC) + timedelta(seconds=i),
            )
        assert rows == []

    @pytest.mark.asyncio
    async def test_six_and_three_segment_topics_rejected(self) -> None:
        """Out-of-shape topics still drop at parse."""
        rule = CriticalSystemErrorRule()
        repo = MagicMock()
        for topic in (
            "system.heartbeats.executor",
            "system.heartbeats.executor.kraken.aaaa.extra",
        ):
            rows = await rule.evaluate(topic, _heartbeat("warning"), repo, _base_now())
            assert rows == []

    @pytest.mark.asyncio
    async def test_healthy_five_segment_resets_window(self) -> None:
        """A real executor recovering pops the per-instance window."""
        rule = CriticalSystemErrorRule()
        repo = MagicMock()
        topic = "system.heartbeats.executor.kraken.abc123def456"
        for i in range(2):
            await rule.evaluate(
                topic, _heartbeat("warning"), repo, _base_now() + timedelta(seconds=i)
            )
        await rule.evaluate(topic, _heartbeat("healthy"), repo, _base_now() + timedelta(seconds=2))
        rows = await rule.evaluate(
            topic, _heartbeat("warning"), repo, _base_now() + timedelta(seconds=3)
        )
        assert rows == []
