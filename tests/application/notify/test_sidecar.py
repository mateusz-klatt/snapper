"""Tests for ``snapper.application.notify.sidecar.NotifySidecar`` (BE-3b rule-dispatch flow).

Exercises the new BE-3b pipeline: receive a domain event (orders.events.* /
plans.decisions.* / system.heartbeats.*) -> ``_dispatch`` runs rules ->
each produced ``AlertEventInsertRow`` is persisted + routed via the
4-level precedence cascade + fanned out to matching devices via APNs.
Uses in-memory SQLite via the real repository so SCD2 semantics get
honest exercise; the ZMQ subscriber, rule registry, and aioapns client
are mocked or swapped in per test.
"""

import asyncio
from collections.abc import AsyncGenerator
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from pathlib import Path
from typing import cast
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import pytest
from loguru import logger
from sqlalchemy import select

from snapper.application.notify.apns_client import ApnsSendResult
from snapper.application.notify.rules.base import AlertRule
from snapper.application.notify.rules.base import RuleRegistry
from snapper.application.notify.sidecar import NotifySidecar
from snapper.application.notify.sidecar import _backoff_seconds
from snapper.application.notify.sidecar import _build_apns_payload
from snapper.application.notify.sidecar import _priority_to_apns
from snapper.data.models import KNOWN_TO_MAX
from snapper.data.models import NotificationDevice
from snapper.data.models import User
from snapper.data.repository import Repository
from snapper.data.repository import SQLAlchemyRepository
from snapper.data.repository_types import AlertDeliveryInsertRow
from snapper.data.repository_types import AlertEventInsertRow
from snapper.data.repository_types import AlertEventRow
from snapper.data.repository_types import NotificationDeviceUpsertRow
from snapper.messaging.infrastructure.publisher import SequenceTracker


def _ts(minutes: int = 0) -> datetime:
    """Deterministic UTC timestamp offset by ``minutes`` from a fixed base."""
    return datetime(2026, 4, 23, 12, 0, 0, tzinfo=UTC) + timedelta(minutes=minutes)


@pytest.fixture
async def repo(tmp_path: Path) -> AsyncGenerator[SQLAlchemyRepository]:
    """Disposable on-disk SQLite repository with the full schema materialised."""
    r = SQLAlchemyRepository(f"sqlite+aiosqlite:///{tmp_path}/sidecar.db")
    await r.create_all()
    try:
        yield r
    finally:
        await r.engine.dispose()


async def _seed_user(repo: SQLAlchemyRepository, user_public_id: str) -> None:
    """Insert a minimal active ``User`` row the fanout will target."""
    async with repo.session() as s:
        s.add(
            User(
                public_id=user_public_id,
                username=f"u_{user_public_id}",
                email=f"{user_public_id}@example.test",
                password_hash="x",
                role="viewer",
                created_at=_ts(),
                session_id="t",
                sequence_id=1,
                timestamp=_ts(),
                known_to=KNOWN_TO_MAX,
            )
        )
        await s.commit()


async def _seed_device(
    repo: SQLAlchemyRepository,
    user_public_id: str,
    *,
    device_token: str = "a" * 64,
    env: str = "sandbox",
) -> str:
    """Upsert an active notification_device and return its public_id."""
    return await repo.upsert_notification_device(
        NotificationDeviceUpsertRow(
            session_id="seed",
            sequence_id=1,
            timestamp=_ts(),
            user_public_id=user_public_id,
            device_token=device_token,
            device_id="dev-1",
            env=env,
            registered_at=_ts(),
        )
    )


class _SingleRowRule(AlertRule):
    """Minimal rule emitting exactly one alert per dispatch — for flow tests."""

    def __init__(
        self,
        *,
        user_public_id: str,
        topic_prefix: str = "test.",
        alert_type: str = "order_fill_full",
        priority: str = "medium",
        safety_critical: bool = False,
    ) -> None:
        self.alert_type = alert_type
        self.subscribe_topic_prefixes = (topic_prefix,)
        self.priority = priority
        self.is_safety_critical = safety_critical
        self.thread_key_prefix = "test"
        self.suppression_window_seconds = 0
        self._user = user_public_id
        self._counter = 0

    async def evaluate(
        self, topic: str, payload: bytes, repo: Repository, now: datetime
    ) -> list[AlertEventInsertRow]:
        self._counter += 1
        return [
            AlertEventInsertRow(
                user_public_id=self._user,
                operator_public_id=None,
                wallet_public_id=None,
                alert_type=self.alert_type,
                priority=self.priority,
                is_safety_critical=self.is_safety_critical,
                title="Fixture",
                body="Fixture body",
                dedup_key=f"fixture.{self._counter}",
                thread_key=f"test.{self._counter}",
                source_topic=topic,
            )
        ]


class _NoopRule(AlertRule):
    """Rule that always returns []."""

    def __init__(self, topic_prefix: str = "noop.") -> None:
        self.alert_type = "order_fill_full"
        self.subscribe_topic_prefixes = (topic_prefix,)
        self.priority = "medium"
        self.is_safety_critical = False
        self.thread_key_prefix = "noop"
        self.suppression_window_seconds = 0

    async def evaluate(
        self, topic: str, payload: bytes, repo: Repository, now: datetime
    ) -> list[AlertEventInsertRow]:
        return []


class _RaisingRule(AlertRule):
    """Rule that raises — used to prove the dispatcher catches rule exceptions."""

    def __init__(self) -> None:
        self.alert_type = "order_fill_full"
        self.subscribe_topic_prefixes = ("boom.",)
        self.priority = "medium"
        self.is_safety_critical = False
        self.thread_key_prefix = "boom"
        self.suppression_window_seconds = 0

    async def evaluate(
        self, topic: str, payload: bytes, repo: Repository, now: datetime
    ) -> list[AlertEventInsertRow]:
        raise RuntimeError("rule explosion")


def _make_sidecar(
    repo: SQLAlchemyRepository,
    *,
    send_result: ApnsSendResult | Exception | None = None,
    registry: RuleRegistry | None = None,
) -> tuple[NotifySidecar, MagicMock]:
    """Construct a sidecar with a mock subscriber + mock APNs pool."""
    subscriber = MagicMock()
    subscriber.subscribe = MagicMock()
    apns = MagicMock()
    if send_result is None:
        default = ApnsSendResult(
            status_code=200, status="success", apns_id="apns-xyz", description=""
        )
        apns.send = AsyncMock(return_value=default)
    elif isinstance(send_result, Exception):
        apns.send = AsyncMock(side_effect=send_result)
    else:
        apns.send = AsyncMock(return_value=send_result)
    sidecar = NotifySidecar(
        subscriber=subscriber,
        repo=repo,
        apns=apns,
        apns_topic="ie.klatt.snapper",
        tracker=SequenceTracker(),
        registry=registry,
    )
    return sidecar, apns


async def _seed_alert_event(repo: SQLAlchemyRepository, user_public_id: str) -> AlertEventRow:
    """Persist a minimal AlertEvent the way the rules would, for delivery tests."""
    pid = await repo.insert_alert_event(
        AlertEventInsertRow(
            session_id="t",
            sequence_id=1,
            timestamp=_ts(),
            user_public_id=user_public_id,
            alert_type="order_fill_full",
            priority="medium",
            title="Filled",
            body="BTC-USD 0.1 filled",
        )
    )
    row = await repo.get_alert_event_by_public_id(pid)
    assert row is not None
    return row


async def _seed_queued_delivery(
    repo: SQLAlchemyRepository,
    *,
    event_public_id: str,
    device_public_id: str,
    user_public_id: str,
) -> str:
    """Seed a queued alert_delivery row directly (bypasses rule evaluation)."""
    return await repo.insert_alert_delivery(
        AlertDeliveryInsertRow(
            session_id="t",
            sequence_id=2,
            timestamp=_ts(),
            alert_event_public_id=event_public_id,
            device_public_id=device_public_id,
            user_public_id=user_public_id,
            status="queued",
            created_at=_ts(),
        )
    )


class TestBackoff:
    """``_backoff_seconds`` implements the §D5.5 exponential schedule."""

    def test_first_attempt_30s(self) -> None:
        """Covered by test body."""
        assert _backoff_seconds(1) == 30.0

    def test_second_attempt_60s(self) -> None:
        """Covered by test body."""
        assert _backoff_seconds(2) == 60.0

    def test_third_attempt_120s(self) -> None:
        """Covered by test body."""
        assert _backoff_seconds(3) == 120.0

    def test_caps_at_five_minutes(self) -> None:
        """Covered by test body."""
        assert _backoff_seconds(20) == 300.0

    def test_zero_or_negative_stays_base(self) -> None:
        """Covered by test body."""
        assert _backoff_seconds(0) == 30.0
        assert _backoff_seconds(-5) == 30.0


class TestPriorityMapping:
    """``_priority_to_apns`` maps the Literal to the APNs header value."""

    def test_high_maps_to_ten(self) -> None:
        """Covered by test body."""
        assert _priority_to_apns("high") == 10

    def test_medium_and_low_map_to_five(self) -> None:
        """Covered by test body."""
        assert _priority_to_apns("medium") == 5
        assert _priority_to_apns("low") == 5


class TestBuildApnsPayload:
    """``_build_apns_payload`` assembles the ``aps`` envelope from AlertEventRow."""

    def _row(self, **overrides: object) -> AlertEventRow:
        """Build an AlertEventRow fixture, overriding specific fields."""
        base: dict[str, object] = {
            "public_id": "pid-1",
            "session_id": "s",
            "sequence_id": 1,
            "timestamp": _ts(),
            "known_to": KNOWN_TO_MAX,
            "user_public_id": "u-1",
            "operator_public_id": None,
            "wallet_public_id": None,
            "alert_type": "order_fill_full",
            "priority": "medium",
            "is_safety_critical": False,
            "title": "Filled",
            "body": "body",
            "payload": None,
            "dedup_key": None,
            "thread_key": None,
            "source_topic": None,
        }
        base.update(overrides)
        return cast(AlertEventRow, base)

    def test_minimal_payload_has_title_and_body(self) -> None:
        """Covered by test body."""
        payload = _build_apns_payload(self._row())
        aps = payload["aps"]
        assert isinstance(aps, dict)
        alert = aps["alert"]
        assert isinstance(alert, dict)
        assert alert["title"] == "Filled"
        assert alert["body"] == "body"

    def test_thread_key_becomes_thread_id(self) -> None:
        """Covered by test body."""
        payload = _build_apns_payload(self._row(thread_key="snapper.order.coid-1"))
        aps = payload["aps"]
        assert isinstance(aps, dict)
        assert aps["thread-id"] == "snapper.order.coid-1"

    def test_custom_payload_fields_leak_through_aps_key_filtered(self) -> None:
        """Covered by test body."""
        payload = _build_apns_payload(
            self._row(payload={"deep_link_path": "/orders/1", "aps": "ignored"})
        )
        assert payload["deep_link_path"] == "/orders/1"
        assert payload["aps"] != "ignored"

    def test_alert_type_and_priority_echoed(self) -> None:
        """Covered by test body."""
        payload = _build_apns_payload(self._row(alert_type="order_rejected", priority="high"))
        assert payload["alert_type"] == "order_rejected"
        assert payload["priority"] == "high"


class TestDispatchFlow:
    """Rule-registry based dispatch: topic matches → rule emits → persist + fanout."""

    @pytest.mark.asyncio
    async def test_dispatch_persists_and_fans_out_via_routing(
        self, repo: SQLAlchemyRepository
    ) -> None:
        """Happy path: a matching rule's row is inserted + APNs called once per device."""
        user = "019dbb34-f439-77bd-afa8-ee5321d60307"
        await _seed_user(repo, user)
        await _seed_device(repo, user)
        reg = RuleRegistry()
        reg.register(_SingleRowRule(user_public_id=user, topic_prefix="test."))
        sidecar, apns = _make_sidecar(repo, registry=reg)

        await sidecar._dispatch("test.something", b"{}", _ts())

        history = await repo.list_recent_alerts_for_user(user, limit=10, before=None)
        assert len(history) == 1
        apns.send.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_unknown_topic_silent_skip(self, repo: SQLAlchemyRepository) -> None:
        """A topic no rule covers yields a silent no-op."""
        reg = RuleRegistry()
        reg.register(_NoopRule(topic_prefix="noop."))
        sidecar, apns = _make_sidecar(repo, registry=reg)

        await sidecar._dispatch("unrelated.topic", b"{}", _ts())

        apns.send.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_rule_exception_does_not_poison_loop(self, repo: SQLAlchemyRepository) -> None:
        """An evaluator raising is caught — other rules still run."""
        user = "019dbb34-f439-77bd-afa8-ee5321d60307"
        await _seed_user(repo, user)
        await _seed_device(repo, user)
        reg = RuleRegistry()
        reg.register(_RaisingRule())
        reg.register(_SingleRowRule(user_public_id=user, topic_prefix="boom."))
        sidecar, apns = _make_sidecar(repo, registry=reg)

        await sidecar._dispatch("boom.anything", b"{}", _ts())

        apns.send.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_rule_returning_empty_suppresses_alert(self, repo: SQLAlchemyRepository) -> None:
        """Evaluate -> [] means no row inserted, no APNs call."""
        user = "019dbb34-f439-77bd-afa8-ee5321d60307"
        await _seed_user(repo, user)
        await _seed_device(repo, user)
        reg = RuleRegistry()
        reg.register(_NoopRule(topic_prefix="noop."))
        sidecar, apns = _make_sidecar(repo, registry=reg)

        await sidecar._dispatch("noop.x", b"{}", _ts())

        history = await repo.list_recent_alerts_for_user(user, limit=10, before=None)
        assert history == []
        apns.send.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_no_active_devices_persists_but_no_fanout(
        self, repo: SQLAlchemyRepository
    ) -> None:
        """Rule emits, row persists, routing returns [] → no APNs call."""
        user = "019dbb34-f439-77bd-afa8-ee5321d60307"
        await _seed_user(repo, user)
        reg = RuleRegistry()
        reg.register(_SingleRowRule(user_public_id=user, topic_prefix="test."))
        sidecar, apns = _make_sidecar(repo, registry=reg)

        await sidecar._dispatch("test.topic", b"{}", _ts())

        history = await repo.list_recent_alerts_for_user(user, limit=10, before=None)
        assert len(history) == 1
        apns.send.assert_not_awaited()


class TestScopeRevalidationDispatch:
    """BE-3c: admin.scope_revoked + admin.user_deactivated routing."""

    @pytest.mark.asyncio
    async def test_scope_revoked_routed_to_revalidator(self, repo: SQLAlchemyRepository) -> None:
        """Frames on ``admin.scope_revoked`` bypass rules and hit the revalidator."""
        sidecar, _ = _make_sidecar(repo)
        sidecar._scope_revalidator = MagicMock()
        sidecar._scope_revalidator.handle_scope_revoked = AsyncMock()

        await sidecar._dispatch("admin.scope_revoked", b"{}", _ts())

        sidecar._scope_revalidator.handle_scope_revoked.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_user_deactivated_routed_to_revalidator(self, repo: SQLAlchemyRepository) -> None:
        """Frames on ``admin.user_deactivated`` hit the kill-switch handler."""
        sidecar, _ = _make_sidecar(repo)
        sidecar._scope_revalidator = MagicMock()
        sidecar._scope_revalidator.handle_user_deactivated = AsyncMock()

        await sidecar._dispatch("admin.user_deactivated", b"{}", _ts())

        sidecar._scope_revalidator.handle_user_deactivated.assert_awaited_once()


class TestScopePreSendSkip:
    """BE-3c: ``should_skip_send`` gates the APNs call in ``_attempt_once``."""

    @pytest.mark.asyncio
    async def test_skips_when_revalidator_returns_true(self, repo: SQLAlchemyRepository) -> None:
        """Cancel transition fires without calling APNs when scope is stale."""
        user = "019dbb34-f439-77bd-afa8-ee5321d60307"
        await _seed_user(repo, user)
        device_pid = await _seed_device(repo, user)
        event = await _seed_alert_event(repo, user)
        delivery_pid = await _seed_queued_delivery(
            repo,
            event_public_id=event["public_id"],
            device_public_id=device_pid,
            user_public_id=user,
        )
        sidecar, apns = _make_sidecar(repo)
        sidecar._scope_revalidator = MagicMock()
        sidecar._scope_revalidator.should_skip_send = AsyncMock(return_value=True)
        device = (await repo.list_active_notification_devices_for_user(user))[0]

        await sidecar._attempt_once(delivery_pid, device, event, _ts())

        apns.send.assert_not_awaited()
        queued = await repo.list_queued_deliveries_all()
        assert queued == []

    """APNs response → terminal / retry routing. Tests seed deliveries directly."""

    @pytest.mark.asyncio
    async def test_410_unregistered_deactivates_device(self, repo: SQLAlchemyRepository) -> None:
        """APNs 410 transitions delivery to 'unregistered' + tombstones the device."""
        user = "019dbb34-f439-77bd-afa8-ee5321d60307"
        await _seed_user(repo, user)
        device_pid = await _seed_device(repo, user)
        event = await _seed_alert_event(repo, user)
        delivery_pid = await _seed_queued_delivery(
            repo,
            event_public_id=event["public_id"],
            device_public_id=device_pid,
            user_public_id=user,
        )
        unreg = ApnsSendResult(
            status_code=410, status="unregistered", apns_id="", description="BadDeviceToken"
        )
        sidecar, _ = _make_sidecar(repo, send_result=unreg)
        device = (await repo.list_active_notification_devices_for_user(user))[0]

        await sidecar._attempt_once(delivery_pid, device, event, _ts())

        active = await repo.list_active_notification_devices_for_user(user)
        assert active == []

    @pytest.mark.asyncio
    async def test_410_path_writes_inactive_successor_row(self, repo: SQLAlchemyRepository) -> None:
        """BE-3a R1 regression: 410 writes a tombstone successor, not a gap."""
        user = "019dbb34-f439-77bd-afa8-ee5321d60307"
        await _seed_user(repo, user)
        device_pid = await _seed_device(repo, user)
        event = await _seed_alert_event(repo, user)
        delivery_pid = await _seed_queued_delivery(
            repo,
            event_public_id=event["public_id"],
            device_public_id=device_pid,
            user_public_id=user,
        )
        unreg = ApnsSendResult(
            status_code=410, status="unregistered", apns_id="", description="BadDeviceToken"
        )
        sidecar, _ = _make_sidecar(repo, send_result=unreg)
        device = (await repo.list_active_notification_devices_for_user(user))[0]

        await sidecar._attempt_once(delivery_pid, device, event, _ts())

        async with repo.session() as s:
            rows = (
                (
                    await s.execute(
                        select(NotificationDevice).where(NotificationDevice.user_public_id == user)
                    )
                )
                .scalars()
                .all()
            )
        assert len(rows) == 2
        successor = [r for r in rows if r.known_to == KNOWN_TO_MAX]
        assert len(successor) == 1
        assert successor[0].token_status == "unregistered"

    @pytest.mark.asyncio
    async def test_server_error_schedules_retry(self, repo: SQLAlchemyRepository) -> None:
        """500 on first attempt leaves delivery queued with ``next_attempt_at``."""
        user = "019dbb34-f439-77bd-afa8-ee5321d60307"
        await _seed_user(repo, user)
        device_pid = await _seed_device(repo, user)
        event = await _seed_alert_event(repo, user)
        delivery_pid = await _seed_queued_delivery(
            repo,
            event_public_id=event["public_id"],
            device_public_id=device_pid,
            user_public_id=user,
        )
        err = ApnsSendResult(status_code=500, status="server_error", apns_id="", description="")
        sidecar, _ = _make_sidecar(repo, send_result=err)
        device = (await repo.list_active_notification_devices_for_user(user))[0]

        await sidecar._attempt_once(delivery_pid, device, event, _ts())

        queued = await repo.list_queued_deliveries_all()
        assert len(queued) == 1
        assert queued[0]["status"] == "queued"
        assert queued[0]["next_attempt_at"] is not None
        assert queued[0]["attempt_count"] == 1

    @pytest.mark.asyncio
    async def test_third_server_error_gives_up(self, repo: SQLAlchemyRepository) -> None:
        """Three consecutive server_errors transition to ``failed``."""
        user = "019dbb34-f439-77bd-afa8-ee5321d60307"
        await _seed_user(repo, user)
        device_pid = await _seed_device(repo, user)
        event = await _seed_alert_event(repo, user)
        delivery_pid = await _seed_queued_delivery(
            repo,
            event_public_id=event["public_id"],
            device_public_id=device_pid,
            user_public_id=user,
        )
        err = ApnsSendResult(status_code=500, status="server_error", apns_id="", description="")
        sidecar, _ = _make_sidecar(repo, send_result=err)
        device = (await repo.list_active_notification_devices_for_user(user))[0]

        await sidecar._attempt_once(delivery_pid, device, event, _ts())
        await sidecar._attempt_once(delivery_pid, device, event, _ts())
        await sidecar._attempt_once(delivery_pid, device, event, _ts())

        queued = await repo.list_queued_deliveries_all()
        assert queued == []

    @pytest.mark.asyncio
    async def test_send_exception_schedules_retry(self, repo: SQLAlchemyRepository) -> None:
        """ApnsClientPool.send raising is mapped to a non-terminal retry."""
        user = "019dbb34-f439-77bd-afa8-ee5321d60307"
        await _seed_user(repo, user)
        device_pid = await _seed_device(repo, user)
        event = await _seed_alert_event(repo, user)
        delivery_pid = await _seed_queued_delivery(
            repo,
            event_public_id=event["public_id"],
            device_public_id=device_pid,
            user_public_id=user,
        )
        sidecar, _ = _make_sidecar(repo, send_result=RuntimeError("connection reset"))
        device = (await repo.list_active_notification_devices_for_user(user))[0]

        await sidecar._attempt_once(delivery_pid, device, event, _ts())

        queued = await repo.list_queued_deliveries_all()
        assert len(queued) == 1
        error_reason = queued[0]["error_reason"]
        assert error_reason is not None
        assert "connection reset" in error_reason

    @pytest.mark.asyncio
    async def test_retry_exhaustion_logs_warning(
        self,
        repo: SQLAlchemyRepository,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """BE-3a R1: exhaust emits ``logger.warning`` before terminal mark."""
        user = "019dbb34-f439-77bd-afa8-ee5321d60307"
        await _seed_user(repo, user)
        device_pid = await _seed_device(repo, user)
        event = await _seed_alert_event(repo, user)
        delivery_pid = await _seed_queued_delivery(
            repo,
            event_public_id=event["public_id"],
            device_public_id=device_pid,
            user_public_id=user,
        )
        err = ApnsSendResult(status_code=500, status="server_error", apns_id="", description="")
        sidecar, _ = _make_sidecar(repo, send_result=err)
        device = (await repo.list_active_notification_devices_for_user(user))[0]

        handler_id = logger.add(caplog.handler, format="{message}", level="WARNING")
        try:
            await sidecar._attempt_once(delivery_pid, device, event, _ts())
            await sidecar._attempt_once(delivery_pid, device, event, _ts())
            await sidecar._attempt_once(delivery_pid, device, event, _ts())
        finally:
            logger.remove(handler_id)

        warnings = [r for r in caplog.records if r.levelname == "WARNING"]
        exhaustion = [
            r for r in warnings if "alert delivery failed after retries" in r.getMessage()
        ]
        assert len(exhaustion) == 1
        assert delivery_pid in exhaustion[0].getMessage()
        assert "attempts=3" in exhaustion[0].getMessage()


class TestDrainOutbox:
    """``_drain_outbox`` picks up queued deliveries on startup."""

    @pytest.mark.asyncio
    async def test_drain_retries_queued_deliveries(self, repo: SQLAlchemyRepository) -> None:
        """A queued row that was never attempted gets one fresh attempt on drain."""
        user = "019dbb34-f439-77bd-afa8-ee5321d60307"
        await _seed_user(repo, user)
        device_pid = await _seed_device(repo, user)
        event = await _seed_alert_event(repo, user)
        await _seed_queued_delivery(
            repo,
            event_public_id=event["public_id"],
            device_public_id=device_pid,
            user_public_id=user,
        )
        sidecar, apns = _make_sidecar(repo)

        await sidecar._drain_outbox(_ts(5))

        queued = await repo.list_queued_deliveries_all()
        assert queued == []
        apns.send.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_drain_skips_queued_when_device_gone(self, repo: SQLAlchemyRepository) -> None:
        """Queued delivery whose device was later deactivated is marked unregistered."""
        user = "019dbb34-f439-77bd-afa8-ee5321d60307"
        await _seed_user(repo, user)
        device_pid = await _seed_device(repo, user)
        event = await _seed_alert_event(repo, user)
        await _seed_queued_delivery(
            repo,
            event_public_id=event["public_id"],
            device_public_id=device_pid,
            user_public_id=user,
        )
        await repo.deactivate_notification_device_scd2(
            device_pid,
            reason="user_unregistered",
            timestamp=_ts(1),
            session_id="close",
            sequence_id=42,
        )
        sidecar, apns = _make_sidecar(repo)

        await sidecar._drain_outbox(_ts(2))

        queued = await repo.list_queued_deliveries_all()
        assert queued == []
        apns.send.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_drain_marks_delivery_failed_when_event_missing(
        self, repo: SQLAlchemyRepository
    ) -> None:
        """Queued delivery referencing a non-existent alert_event → failed."""
        user = "019dbb34-f439-77bd-afa8-ee5321d60307"
        await _seed_user(repo, user)
        device_pid = await _seed_device(repo, user)
        await _seed_queued_delivery(
            repo,
            event_public_id="nonexistent-event-pid",
            device_public_id=device_pid,
            user_public_id=user,
        )
        sidecar, apns = _make_sidecar(repo)

        await sidecar._drain_outbox(_ts())

        queued = await repo.list_queued_deliveries_all()
        assert queued == []
        apns.send.assert_not_awaited()


class TestLoadDelivery:
    """``_load_delivery`` returns None for closed/unknown public_ids."""

    @pytest.mark.asyncio
    async def test_unknown_public_id_returns_none(self, repo: SQLAlchemyRepository) -> None:
        """Public_id that was never persisted yields None."""
        user = "019dbb34-f439-77bd-afa8-ee5321d60307"
        await _seed_user(repo, user)
        device_pid = await _seed_device(repo, user)
        event = await _seed_alert_event(repo, user)
        await _seed_queued_delivery(
            repo,
            event_public_id=event["public_id"],
            device_public_id=device_pid,
            user_public_id=user,
        )
        sidecar, _ = _make_sidecar(repo)

        result = await sidecar._load_delivery("unknown-public-id")

        assert result is None


class TestRetryLoopLifecycle:
    """``_process_retry_queue_loop`` obeys the stop event."""

    @pytest.mark.asyncio
    async def test_pre_set_stop_event_exits_without_polling_retry_queue(
        self, repo: SQLAlchemyRepository, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A loop started after ``stop()`` exits before its first retry SELECT."""
        sidecar, _ = _make_sidecar(repo)
        list_ready = AsyncMock(return_value=[])
        monkeypatch.setattr(repo, "list_deliveries_ready_for_retry", list_ready)

        await sidecar.stop()
        await asyncio.wait_for(sidecar._process_retry_queue_loop(), timeout=0.5)

        list_ready.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_stop_event_breaks_loop(
        self, repo: SQLAlchemyRepository, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Setting ``_stop_event`` terminates the loop on the next tick."""
        sidecar, _ = _make_sidecar(repo)
        monkeypatch.setattr("snapper.application.notify.sidecar._RETRY_LOOP_INTERVAL_S", 0.05)
        task = asyncio.create_task(sidecar._process_retry_queue_loop())
        await asyncio.sleep(0.02)
        await sidecar.stop()
        await asyncio.wait_for(task, timeout=2.0)


class TestSidecarStart:
    """``start`` subscribes every registry prefix + drains outbox + spawns retry loop."""

    @pytest.mark.asyncio
    async def test_start_subscribes_registry_prefixes(
        self, repo: SQLAlchemyRepository, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Every prefix from the registry reaches subscriber.subscribe."""
        reg = RuleRegistry()
        reg.register(_SingleRowRule(user_public_id="u", topic_prefix="a.b."))
        reg.register(_NoopRule(topic_prefix="x.y."))
        sidecar, _ = _make_sidecar(repo, registry=reg)

        async def _never_ending_recv() -> tuple[str, bytes]:
            """Block until the sidecar's stop event fires."""
            await asyncio.sleep(5.0)
            return ("", b"")

        sidecar._subscriber.recv_multipart = _never_ending_recv
        task = asyncio.create_task(sidecar.start())
        await asyncio.sleep(0.05)
        await sidecar.stop()
        await asyncio.wait_for(task, timeout=2.0)

        subscribed_prefixes = {c.args[0] for c in sidecar._subscriber.subscribe.call_args_list}
        assert subscribed_prefixes == {
            "a.b.",
            "x.y.",
            "admin.scope_revoked",
            "admin.user_deactivated",
        }

    @pytest.mark.asyncio
    async def test_start_cancels_running_retry_task_on_exit(
        self, repo: SQLAlchemyRepository, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """``start()`` cancels the retry task in the finally branch if still running.

        Uses a custom ``_process_retry_queue_loop`` replacement that
        ignores the stop event so the task stays running when ``start()``
        exits; the ``if not done()`` branch in the finally block then
        cancels it. Without the override, the retry loop notices the
        stop event on its next tick and exits cleanly, taking the
        "already done" branch.
        """
        sidecar, _ = _make_sidecar(repo)
        cancelled = asyncio.Event()

        async def _unstoppable_retry_loop() -> None:
            """Sleep indefinitely; only a direct task.cancel() ends us."""
            try:
                await asyncio.sleep(3600)
            except asyncio.CancelledError:
                cancelled.set()
                raise

        monkeypatch.setattr(sidecar, "_process_retry_queue_loop", _unstoppable_retry_loop)

        async def _blocks_forever() -> tuple[str, bytes]:
            """Block so the main loop's asyncio.wait hands off to stop_task."""
            await asyncio.sleep(3600)
            return ("", b"")

        sidecar._subscriber.recv_multipart = _blocks_forever
        task = asyncio.create_task(sidecar.start())
        await asyncio.sleep(0.05)
        await sidecar.stop()
        await asyncio.wait_for(task, timeout=2.0)

        assert sidecar._retry_task is not None
        await asyncio.sleep(0)
        assert cancelled.is_set()

    @pytest.mark.asyncio
    async def test_start_consumes_received_frame_and_dispatches(
        self, repo: SQLAlchemyRepository
    ) -> None:
        """A frame that arrives through recv_multipart is routed via the dispatcher."""
        user = "019dbb34-f439-77bd-afa8-ee5321d60307"
        await _seed_user(repo, user)
        await _seed_device(repo, user)
        reg = RuleRegistry()
        reg.register(_SingleRowRule(user_public_id=user, topic_prefix="test."))
        sidecar, apns = _make_sidecar(repo, registry=reg)

        received = asyncio.Event()

        async def _one_frame_then_hang() -> tuple[str, bytes]:
            """Return one frame, then block so the main loop sees the dispatch."""
            if not received.is_set():
                received.set()
                return ("test.anything", b"{}")
            await asyncio.sleep(5.0)
            return ("", b"")

        sidecar._subscriber.recv_multipart = _one_frame_then_hang
        task = asyncio.create_task(sidecar.start())
        await asyncio.wait_for(received.wait(), timeout=1.0)
        await asyncio.sleep(0.1)
        await sidecar.stop()
        await asyncio.wait_for(task, timeout=2.0)

        history = await repo.list_recent_alerts_for_user(user, limit=10, before=None)
        assert len(history) == 1
        apns.send.assert_awaited_once()


class TestPersistAndFanoutRow:
    """Edge cases for ``_persist_and_fanout_row``."""

    @pytest.mark.asyncio
    async def test_missing_read_back_row_logs_warning_and_skips(
        self,
        repo: SQLAlchemyRepository,
        caplog: pytest.LogCaptureFixture,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """If the just-inserted event is gone on read-back, fanout is skipped."""
        user = "019dbb34-f439-77bd-afa8-ee5321d60307"
        await _seed_user(repo, user)
        await _seed_device(repo, user)
        sidecar, apns = _make_sidecar(repo)

        async def _returns_none(_pid: str) -> AlertEventRow | None:
            """Simulate SCD2 close racing the read-back."""
            return None

        monkeypatch.setattr(repo, "get_alert_event_by_public_id", _returns_none)
        alert_row: AlertEventInsertRow = {
            "user_public_id": user,
            "operator_public_id": None,
            "wallet_public_id": None,
            "alert_type": "order_fill_full",
            "priority": "medium",
            "is_safety_critical": False,
            "title": "t",
            "body": "b",
            "dedup_key": "x",
            "thread_key": None,
            "source_topic": "test.x",
        }

        handler_id = logger.add(caplog.handler, format="{message}", level="WARNING")
        try:
            await sidecar._persist_and_fanout_row(alert_row, _ts())
        finally:
            logger.remove(handler_id)

        warnings = [r for r in caplog.records if "missing on read-back" in r.getMessage()]
        assert len(warnings) == 1
        apns.send.assert_not_awaited()


class TestRetryQueueLoop:
    """``_process_retry_queue_loop`` picks up deliveries whose timers fired."""

    @pytest.mark.asyncio
    async def test_retry_loop_reattempts_ready_row(
        self, repo: SQLAlchemyRepository, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A queued row with a past next_attempt_at is retried on wake."""
        user = "019dbb34-f439-77bd-afa8-ee5321d60307"
        await _seed_user(repo, user)
        device_pid = await _seed_device(repo, user)
        event = await _seed_alert_event(repo, user)
        delivery_pid = await _seed_queued_delivery(
            repo,
            event_public_id=event["public_id"],
            device_public_id=device_pid,
            user_public_id=user,
        )
        await repo.update_delivery_retry_schedule(
            delivery_pid,
            attempt_count=1,
            next_attempt_at=datetime(2020, 1, 1, tzinfo=UTC),
            error_reason="server_error",
            transition_at=_ts(),
            session_id="prep",
            sequence_id=99,
        )
        ok = ApnsSendResult(status_code=200, status="success", apns_id="apns-ok", description="")
        sidecar, apns = _make_sidecar(repo, send_result=ok)

        monkeypatch.setattr("snapper.application.notify.sidecar._RETRY_LOOP_INTERVAL_S", 0.05)
        task = asyncio.create_task(sidecar._process_retry_queue_loop())
        await asyncio.sleep(0.15)
        await sidecar.stop()
        await asyncio.wait_for(task, timeout=2.0)

        still_queued = await repo.list_queued_deliveries_all()
        assert still_queued == []
        apns.send.assert_awaited()
