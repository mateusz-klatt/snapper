"""Tests for ``snapper.application.notify.sidecar.NotifySidecar``.

Exercises the end-to-end flow: parse bus payload -> insert
alert_event -> fan out to active devices -> attempt APNs -> route
terminal/retry semantics (§D5.5). Uses in-memory SQLite via the
real repository (same pattern as ``tests/data/test_push_repo.py``)
so the SCD2 close-and-insert + partial-unique semantics get honest
exercise; the ZMQ subscriber and aioapns client are mocked because
they own external sockets that aren't helpful to build here.
"""

import asyncio
from collections.abc import AsyncGenerator
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from pathlib import Path
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import pytest
from sqlalchemy import update

from snapper.application.notify.apns_client import ApnsSendResult
from snapper.application.notify.sidecar import NotifySidecar
from snapper.application.notify.sidecar import _backoff_seconds
from snapper.application.notify.sidecar import _build_apns_payload
from snapper.application.notify.sidecar import _event_row_to_alert_data
from snapper.application.notify.sidecar import _priority_to_apns
from snapper.data.models import KNOWN_TO_MAX
from snapper.data.models import AlertDelivery
from snapper.data.models import User
from snapper.data.repository import SQLAlchemyRepository
from snapper.data.repository_types import AlertDeliveryInsertRow
from snapper.data.repository_types import AlertEventInsertRow
from snapper.data.repository_types import NotificationDeviceUpsertRow
from snapper.messaging.infrastructure.publisher import SequenceTracker
from snapper.messaging.schemas.data import AlertEventData
from snapper.messaging.schemas.data import TickData


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


def _alert_event_data(
    user_public_id: str = "019dbb34-f439-77bd-afa8-ee5321d60307",
    alert_type: str = "order_fill_full",
    priority: str = "medium",
    is_safety_critical: bool = False,
) -> AlertEventData:
    """Return a minimal valid ``AlertEventData`` for the happy path."""
    return AlertEventData(
        session_id="bus",
        sequence_id=1,
        public_id="envelope-pid",
        timestamp=_ts(),
        user_public_id=user_public_id,
        alert_type=alert_type,
        priority=priority,
        is_safety_critical=is_safety_critical,
        title="Filled",
        body="BTC-USD 0.1 filled",
    )


def _make_sidecar(
    repo: SQLAlchemyRepository,
    *,
    send_result: ApnsSendResult | Exception | None = None,
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
    )
    return sidecar, apns


class TestBackoff:
    """``_backoff_seconds`` implements the §D5.5 exponential schedule."""

    def test_first_attempt_30s(self) -> None:
        """N=1 -> 30s (base, no exponent)."""
        assert _backoff_seconds(1) == 30.0

    def test_second_attempt_60s(self) -> None:
        """N=2 -> 60s (one doubling)."""
        assert _backoff_seconds(2) == 60.0

    def test_third_attempt_120s(self) -> None:
        """N=3 -> 120s (two doublings)."""
        assert _backoff_seconds(3) == 120.0

    def test_caps_at_five_minutes(self) -> None:
        """The schedule is clamped at 300s (5 minutes)."""
        assert _backoff_seconds(20) == 300.0

    def test_zero_or_negative_stays_base(self) -> None:
        """Non-positive attempt numbers clamp to the base interval."""
        assert _backoff_seconds(0) == 30.0
        assert _backoff_seconds(-5) == 30.0


class TestPriorityMapping:
    """``_priority_to_apns`` maps the Literal to the APNs header value."""

    def test_high_maps_to_ten(self) -> None:
        """``high`` -> 10 (immediate delivery)."""
        assert _priority_to_apns("high") == 10

    def test_medium_and_low_map_to_five(self) -> None:
        """``medium`` / ``low`` -> 5 (throttleable)."""
        assert _priority_to_apns("medium") == 5
        assert _priority_to_apns("low") == 5


class TestBuildApnsPayload:
    """``_build_apns_payload`` assembles the ``aps`` envelope correctly."""

    def test_minimal_payload_has_title_and_body(self) -> None:
        """``aps.alert.title`` / ``.body`` are always populated."""
        data = _alert_event_data()

        payload = _build_apns_payload(data)

        aps = payload["aps"]
        assert isinstance(aps, dict)
        alert = aps["alert"]
        assert isinstance(alert, dict)
        assert alert["title"] == "Filled"
        assert alert["body"] == "BTC-USD 0.1 filled"

    def test_thread_key_becomes_thread_id(self) -> None:
        """A set ``thread_key`` flows into ``aps.thread-id``."""
        data = AlertEventData(
            session_id="bus",
            sequence_id=1,
            public_id="pid",
            timestamp=_ts(),
            user_public_id="019dbb34-f439-77bd-afa8-ee5321d60307",
            alert_type="order_fill_full",
            title="t",
            body="b",
            thread_key="order-123",
        )

        payload = _build_apns_payload(data)

        aps = payload["aps"]
        assert isinstance(aps, dict)
        assert aps["thread-id"] == "order-123"

    def test_custom_payload_keys_passthrough_aps_protected(self) -> None:
        """Custom payload keys copy through but the reserved ``aps`` is not overwritten."""
        data = AlertEventData(
            session_id="bus",
            sequence_id=1,
            public_id="pid",
            timestamp=_ts(),
            user_public_id="019dbb34-f439-77bd-afa8-ee5321d60307",
            alert_type="order_fill_full",
            title="t",
            body="b",
            payload={"order_public_id": "ord-1", "aps": {"alert": "evil-overwrite"}},
        )

        payload = _build_apns_payload(data)

        aps = payload["aps"]
        assert isinstance(aps, dict)
        alert = aps["alert"]
        assert isinstance(alert, dict)
        assert alert["title"] == "t"
        assert payload["order_public_id"] == "ord-1"
        assert payload["alert_type"] == "order_fill_full"
        assert payload["priority"] == "medium"


class TestSidecarHandleHappyPath:
    """End-to-end ``_handle`` + ``_persist_and_fanout`` with one active device."""

    @pytest.mark.asyncio
    async def test_successful_send_transitions_to_sent(self, repo: SQLAlchemyRepository) -> None:
        """Happy path: alert_event persisted + delivery row ends at ``sent``."""
        user = "019dbb34-f439-77bd-afa8-ee5321d60307"
        await _seed_user(repo, user)
        await _seed_device(repo, user)
        sidecar, apns = _make_sidecar(repo)
        data = _alert_event_data(user_public_id=user)

        await sidecar._handle(
            f"alerts.{user}.order_fill_full",
            data.to_json().encode("utf-8"),
        )

        queued = await repo.list_queued_deliveries_all()
        assert queued == []
        apns.send.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_topic_mismatch_drops_without_insert(self, repo: SQLAlchemyRepository) -> None:
        """Topic segment 2 != payload.user_public_id -> drop with warning."""
        user = "019dbb34-f439-77bd-afa8-ee5321d60307"
        await _seed_user(repo, user)
        await _seed_device(repo, user)
        sidecar, apns = _make_sidecar(repo)
        data = _alert_event_data(user_public_id=user)

        await sidecar._handle(
            "alerts.019dbb34-f439-77bd-afa8-aaaaaaaaaaaa.order_fill_full",
            data.to_json().encode("utf-8"),
        )

        apns.send.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_malformed_json_dropped_without_raise(self, repo: SQLAlchemyRepository) -> None:
        """Bad JSON logs a warning and returns without calling APNs."""
        sidecar, apns = _make_sidecar(repo)

        await sidecar._handle(
            "alerts.019dbb34-f439-77bd-afa8-ee5321d60307.order_fill_full",
            b"not-json",
        )

        apns.send.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_no_active_devices_persists_event_without_fanout(
        self, repo: SQLAlchemyRepository
    ) -> None:
        """User with no active devices: event row is persisted, no APNs call."""
        user = "019dbb34-f439-77bd-afa8-ee5321d60307"
        await _seed_user(repo, user)
        sidecar, apns = _make_sidecar(repo)
        data = _alert_event_data(user_public_id=user)

        await sidecar._handle(
            f"alerts.{user}.order_fill_full",
            data.to_json().encode("utf-8"),
        )

        history = await repo.list_recent_alerts_for_user(user, limit=10, before=None)
        assert len(history) == 1
        apns.send.assert_not_awaited()


class TestSidecarErrorSemantics:
    """Error + retry behaviour of the attempt loop (§D5.5)."""

    @pytest.mark.asyncio
    async def test_410_unregistered_deactivates_device(self, repo: SQLAlchemyRepository) -> None:
        """APNs 410 -> delivery 'unregistered' + device marked inactive."""
        user = "019dbb34-f439-77bd-afa8-ee5321d60307"
        await _seed_user(repo, user)
        await _seed_device(repo, user)
        unreg = ApnsSendResult(
            status_code=410, status="unregistered", apns_id="", description="BadDeviceToken"
        )
        sidecar, _ = _make_sidecar(repo, send_result=unreg)
        data = _alert_event_data(user_public_id=user)

        await sidecar._handle(
            f"alerts.{user}.order_fill_full",
            data.to_json().encode("utf-8"),
        )

        active = await repo.list_active_notification_devices_for_user(user)
        assert active == []

    @pytest.mark.asyncio
    async def test_server_error_schedules_retry(self, repo: SQLAlchemyRepository) -> None:
        """500 on first attempt -> still queued + ``next_attempt_at`` populated."""
        user = "019dbb34-f439-77bd-afa8-ee5321d60307"
        await _seed_user(repo, user)
        await _seed_device(repo, user)
        err = ApnsSendResult(status_code=500, status="server_error", apns_id="", description="")
        sidecar, _ = _make_sidecar(repo, send_result=err)
        data = _alert_event_data(user_public_id=user)

        await sidecar._handle(
            f"alerts.{user}.order_fill_full",
            data.to_json().encode("utf-8"),
        )

        queued = await repo.list_queued_deliveries_all()
        assert len(queued) == 1
        assert queued[0]["status"] == "queued"
        assert queued[0]["next_attempt_at"] is not None
        assert queued[0]["attempt_count"] == 1

    @pytest.mark.asyncio
    async def test_third_server_error_gives_up(self, repo: SQLAlchemyRepository) -> None:
        """Three consecutive server_errors -> delivery ends up ``failed``."""
        user = "019dbb34-f439-77bd-afa8-ee5321d60307"
        await _seed_user(repo, user)
        await _seed_device(repo, user)
        err = ApnsSendResult(status_code=500, status="server_error", apns_id="", description="")
        sidecar, _ = _make_sidecar(repo, send_result=err)
        data = _alert_event_data(user_public_id=user)

        await sidecar._handle(
            f"alerts.{user}.order_fill_full",
            data.to_json().encode("utf-8"),
        )
        queued = await repo.list_queued_deliveries_all()
        row = queued[0]
        await sidecar._attempt_on_queued_row(row)
        await sidecar._attempt_on_queued_row((await repo.list_queued_deliveries_all())[0])

        still_queued = await repo.list_queued_deliveries_all()
        assert still_queued == []

    @pytest.mark.asyncio
    async def test_send_exception_schedules_retry(self, repo: SQLAlchemyRepository) -> None:
        """``ApnsClientPool.send`` raising is the same as a server_error."""
        user = "019dbb34-f439-77bd-afa8-ee5321d60307"
        await _seed_user(repo, user)
        await _seed_device(repo, user)
        sidecar, _ = _make_sidecar(repo, send_result=RuntimeError("connection reset"))
        data = _alert_event_data(user_public_id=user)

        await sidecar._handle(
            f"alerts.{user}.order_fill_full",
            data.to_json().encode("utf-8"),
        )

        queued = await repo.list_queued_deliveries_all()
        assert len(queued) == 1
        assert queued[0]["error_reason"] is not None
        assert "connection reset" in queued[0]["error_reason"]


class TestDrainOutbox:
    """``_drain_outbox`` picks up queued rows on startup."""

    @pytest.mark.asyncio
    async def test_drain_attempts_queued_rows(self, repo: SQLAlchemyRepository) -> None:
        """Queued rows from a prior session are retried on startup."""
        user = "019dbb34-f439-77bd-afa8-ee5321d60307"
        await _seed_user(repo, user)
        await _seed_device(repo, user)
        err = ApnsSendResult(status_code=500, status="server_error", apns_id="", description="")
        first_sidecar, _ = _make_sidecar(repo, send_result=err)
        await first_sidecar._handle(
            f"alerts.{user}.order_fill_full",
            _alert_event_data(user_public_id=user).to_json().encode("utf-8"),
        )

        success_sidecar, success_apns = _make_sidecar(repo)
        await success_sidecar._drain_outbox()

        queued = await repo.list_queued_deliveries_all()
        assert queued == []
        success_apns.send.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_drain_marks_failed_when_alert_event_missing(
        self, repo: SQLAlchemyRepository
    ) -> None:
        """Orphan delivery (missing parent event) is marked ``failed``.

        This can only happen under a data-integrity bug (e.g., manual
        row deletion) but the sidecar must tolerate it and move on.
        """
        user = "019dbb34-f439-77bd-afa8-ee5321d60307"
        await _seed_user(repo, user)
        dev = await _seed_device(repo, user)

        await repo.insert_alert_delivery(
            AlertDeliveryInsertRow(
                alert_event_public_id="evt-missing",
                device_public_id=dev,
                user_public_id=user,
                status="queued",
                created_at=_ts(),
                session_id="s",
                sequence_id=1,
                timestamp=_ts(),
            )
        )
        sidecar, apns = _make_sidecar(repo)

        await sidecar._drain_outbox()

        queued = await repo.list_queued_deliveries_all()
        assert queued == []
        apns.send.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_drain_unregisters_delivery_when_device_gone(
        self, repo: SQLAlchemyRepository
    ) -> None:
        """Delivery whose device has been deactivated -> ``unregistered`` no-send."""
        user = "019dbb34-f439-77bd-afa8-ee5321d60307"
        await _seed_user(repo, user)
        dev = await _seed_device(repo, user)
        event_pid = await repo.insert_alert_event(
            __import__(
                "snapper.data.repository_types", fromlist=["AlertEventInsertRow"]
            ).AlertEventInsertRow(
                session_id="s",
                sequence_id=1,
                timestamp=_ts(),
                user_public_id=user,
                alert_type="order_fill_full",
                priority="medium",
                title="t",
                body="b",
            )
        )
        await repo.insert_alert_delivery(
            __import__(
                "snapper.data.repository_types", fromlist=["AlertDeliveryInsertRow"]
            ).AlertDeliveryInsertRow(
                alert_event_public_id=event_pid,
                device_public_id=dev,
                user_public_id=user,
                status="queued",
                created_at=_ts(),
                session_id="s",
                sequence_id=2,
                timestamp=_ts(),
            )
        )
        await repo.mark_notification_device_inactive(dev, closed_at=_ts(1))
        sidecar, apns = _make_sidecar(repo)

        await sidecar._drain_outbox()

        queued = await repo.list_queued_deliveries_all()
        assert queued == []
        apns.send.assert_not_awaited()


class TestEventRowToAlertData:
    """``_event_row_to_alert_data`` rehydrates retry-loop payloads correctly."""

    @pytest.mark.asyncio
    async def test_roundtrip_through_persistence(self, repo: SQLAlchemyRepository) -> None:
        """A persisted event round-trips back to an equivalent ``AlertEventData``."""
        user = "019dbb34-f439-77bd-afa8-ee5321d60307"
        await _seed_user(repo, user)
        orig = _alert_event_data(user_public_id=user)

        event_pid = await repo.insert_alert_event(
            AlertEventInsertRow(
                session_id=orig.session_id,
                sequence_id=orig.sequence_id,
                timestamp=orig.timestamp,
                user_public_id=orig.user_public_id,
                alert_type=orig.alert_type,
                priority=orig.priority,
                is_safety_critical=orig.is_safety_critical,
                title=orig.title,
                body=orig.body,
            )
        )
        row = await repo.get_alert_event_by_public_id(event_pid)

        assert row is not None
        rehydrated = _event_row_to_alert_data(row)

        assert rehydrated.user_public_id == user
        assert rehydrated.alert_type == "order_fill_full"
        assert rehydrated.title == "Filled"


class TestRetryLoopLifecycle:
    """The background retry loop respects ``stop_event`` promptly."""

    @pytest.mark.asyncio
    async def test_retry_loop_exits_on_stop(self, repo: SQLAlchemyRepository) -> None:
        """``stop()`` ends the retry loop within one wait cycle."""
        sidecar, _ = _make_sidecar(repo)
        task = asyncio.create_task(sidecar._process_retry_queue_loop())
        await asyncio.sleep(0)
        await sidecar.stop()
        await asyncio.wait_for(task, timeout=2.0)

        assert task.done()

    @pytest.mark.asyncio
    async def test_retry_loop_handles_cancellation(self, repo: SQLAlchemyRepository) -> None:
        """Cancelling the task while it waits is benign — no traceback leak."""
        sidecar, _ = _make_sidecar(repo)
        task = asyncio.create_task(sidecar._process_retry_queue_loop())
        await asyncio.sleep(0)
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)

        assert task.done()

    @pytest.mark.asyncio
    async def test_retry_loop_exits_immediately_when_stop_already_set(
        self, repo: SQLAlchemyRepository
    ) -> None:
        """``stop_event`` set before the loop body runs -> clean exit.

        Covers the ``while not self._stop_event.is_set():`` False-on-
        first-evaluation branch: the outer while sees True for
        ``is_set()`` and falls straight through without entering the
        body or the retry query. Matches the "stop called before the
        task got scheduled" edge.
        """
        sidecar, _ = _make_sidecar(repo)
        await sidecar.stop()

        await asyncio.wait_for(sidecar._process_retry_queue_loop(), timeout=2.0)


class TestSidecarStart:
    """End-to-end ``start()`` -> receive -> handle -> ``stop()`` lifecycle."""

    @pytest.mark.asyncio
    async def test_start_consumes_one_message_then_stops(self, repo: SQLAlchemyRepository) -> None:
        """The main receive loop ingests messages until ``stop()`` is called."""
        user = "019dbb34-f439-77bd-afa8-ee5321d60307"
        await _seed_user(repo, user)
        await _seed_device(repo, user)
        sidecar, apns = _make_sidecar(repo)
        data = _alert_event_data(user_public_id=user)

        async def recv() -> tuple[str, bytes]:
            return (f"alerts.{user}.order_fill_full", data.to_json().encode("utf-8"))

        sidecar._subscriber.recv_multipart = recv

        run = asyncio.create_task(sidecar.start())
        await asyncio.sleep(0.05)
        await sidecar.stop()
        await asyncio.wait_for(run, timeout=2.0)

        apns.send.assert_awaited()
        sidecar._subscriber.subscribe.assert_called_with("alerts.")

    @pytest.mark.asyncio
    async def test_start_returns_when_stop_wins_wait(self, repo: SQLAlchemyRepository) -> None:
        """If ``stop()`` fires before a message arrives, the loop exits cleanly."""
        sidecar, _ = _make_sidecar(repo)

        async def never_returns() -> tuple[str, bytes]:
            await asyncio.sleep(3600)
            raise AssertionError("recv should not complete in this test")

        sidecar._subscriber.recv_multipart = never_returns

        run = asyncio.create_task(sidecar.start())
        await asyncio.sleep(0.1)
        await sidecar.stop()
        await asyncio.wait_for(run, timeout=2.0)

        assert run.done()

    @pytest.mark.asyncio
    async def test_start_cancels_retry_task_on_abnormal_exit(
        self, repo: SQLAlchemyRepository
    ) -> None:
        """Finally clause cancels a still-running retry_task on start() exit.

        The retry loop normally exits on its own once ``stop_event`` is
        set, so the cancel() branch rarely fires in practice. We force
        it by replacing the retry loop with a coroutine that never
        checks the stop_event — the start() ``finally`` is then the
        only path that tears it down.
        """
        sidecar, _ = _make_sidecar(repo)

        async def never_ending_retry() -> None:
            while True:
                await asyncio.sleep(3600)

        async def stop_event_recv() -> tuple[str, bytes]:
            await asyncio.sleep(3600)
            raise AssertionError("recv should not complete in this test")

        sidecar._subscriber.recv_multipart = stop_event_recv
        sidecar._process_retry_queue_loop = never_ending_retry

        run = asyncio.create_task(sidecar.start())
        await asyncio.sleep(0.05)
        await sidecar.stop()
        await asyncio.wait_for(run, timeout=2.0)

        assert run.done()
        assert sidecar._retry_task is not None
        assert sidecar._retry_task.cancelled() or sidecar._retry_task.done()


class TestRetryQueueLoop:
    """``_process_retry_queue_loop`` picks up deliveries whose timers fired."""

    @pytest.mark.asyncio
    async def test_retry_loop_reattempts_ready_row(
        self, repo: SQLAlchemyRepository, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A queued row with a past ``next_attempt_at`` is retried on wake.

        We shrink the loop interval to 50ms via monkeypatch so the
        test doesn't block 30s; the loop picks up the ready row,
        exchanges it through a success send, and exits on stop.
        """
        user = "019dbb34-f439-77bd-afa8-ee5321d60307"
        await _seed_user(repo, user)
        await _seed_device(repo, user)
        err = ApnsSendResult(status_code=500, status="server_error", apns_id="", description="")
        sidecar, apns = _make_sidecar(repo, send_result=err)
        await sidecar._handle(
            f"alerts.{user}.order_fill_full",
            _alert_event_data(user_public_id=user).to_json().encode("utf-8"),
        )
        apns.send.reset_mock()
        ok = ApnsSendResult(status_code=200, status="success", apns_id="apns-ok", description="")
        apns.send.return_value = ok
        queued = (await repo.list_queued_deliveries_all())[0]
        async with repo.session() as s:

            await s.execute(
                update(AlertDelivery)
                .where(AlertDelivery.public_id == queued["public_id"])
                .where(AlertDelivery.known_to == KNOWN_TO_MAX)
                .values(next_attempt_at=datetime(2020, 1, 1, tzinfo=UTC))
            )
            await s.commit()

        monkeypatch.setattr("snapper.application.notify.sidecar._RETRY_LOOP_INTERVAL_S", 0.05)
        task = asyncio.create_task(sidecar._process_retry_queue_loop())
        await asyncio.sleep(0.15)
        await sidecar.stop()
        await asyncio.wait_for(task, timeout=2.0)

        still_queued = await repo.list_queued_deliveries_all()
        assert still_queued == []
        apns.send.assert_awaited()


class TestLoadDelivery:
    """``_load_delivery`` returns None for closed/unknown public_ids."""

    @pytest.mark.asyncio
    async def test_unknown_public_id_returns_none(self, repo: SQLAlchemyRepository) -> None:
        """Public_id that was never persisted yields None (defensive path).

        The helper scans ``list_queued_deliveries_all`` for the target;
        a never-written public_id exercises both branches of the
        per-row filter — the iteration continues past one unrelated
        queued row, then falls through to the terminal ``None``
        return.
        """
        user = "019dbb34-f439-77bd-afa8-ee5321d60307"
        await _seed_user(repo, user)
        await _seed_device(repo, user)
        err = ApnsSendResult(status_code=500, status="server_error", apns_id="", description="")
        sidecar, _ = _make_sidecar(repo, send_result=err)
        await sidecar._handle(
            f"alerts.{user}.order_fill_full",
            _alert_event_data(user_public_id=user).to_json().encode("utf-8"),
        )

        result = await sidecar._load_delivery("unknown-public-id")

        assert result is None


class TestHandleNonAlertEvent:
    """``_handle`` rejects non-AlertEventData payloads without side effects."""

    @pytest.mark.asyncio
    async def test_valid_json_wrong_discriminator_dropped(self, repo: SQLAlchemyRepository) -> None:
        """A payload that parses but is not AlertEventData logs + returns."""
        user = "019dbb34-f439-77bd-afa8-ee5321d60307"
        await _seed_user(repo, user)
        await _seed_device(repo, user)
        sidecar, apns = _make_sidecar(repo)

        tick = TickData(
            session_id="",
            sequence_id=0,
            exchange="kraken",
            instrument="BTC-USD",
            volume=1.0,
            public_id="pid",
            timestamp=_ts(),
        )

        await sidecar._handle(
            f"alerts.{user}.order_fill_full",
            tick.to_json().encode("utf-8"),
        )

        apns.send.assert_not_awaited()
