"""Tests for ``snapper.application.notify.sidecar.NotifySidecar`` rule-dispatch flow.

Exercises the rule-dispatch pipeline: receive a domain event (orders.events.* /
plans.decisions.* / system.heartbeats.* / bus.portfolio_drift_episode) ->
``_dispatch`` runs rules ->
each produced ``AlertEventInsertRow`` is persisted + routed via the
4-level precedence cascade + fanned out to matching devices via APNs.
Uses in-memory SQLite via the real repository so SCD2 semantics get
honest exercise; the ZMQ subscriber, rule registry, and aioapns client
are mocked or swapped in per test.
"""

import asyncio
import errno
import socket
from collections.abc import AsyncGenerator
from collections.abc import Callable
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from pathlib import Path
from typing import cast
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import httpx
import pytest
from aioapns.common import NotificationResult
from aioapns.exceptions import MaxAttemptsExceeded
from loguru import logger
from sqlalchemy import select

from snapper.application.notify.apns_client import ApnsSendResult
from snapper.application.notify.apns_client import _map_result
from snapper.application.notify.portfolio_drift_recovery import PortfolioDriftRecoveryScanner
from snapper.application.notify.push_beta import PushBetaConfig
from snapper.application.notify.rules.base import AlertRule
from snapper.application.notify.rules.base import RuleRegistry
from snapper.application.notify.sidecar import _NETWORK_BACKOFF_CAP_S
from snapper.application.notify.sidecar import NotifySidecar
from snapper.application.notify.sidecar import _backoff_seconds
from snapper.application.notify.sidecar import _build_apns_payload
from snapper.application.notify.sidecar import _DeliveryTarget
from snapper.application.notify.sidecar import _expired_error_reason
from snapper.application.notify.sidecar import _format_exception_reason
from snapper.application.notify.sidecar import _OutageDelay
from snapper.application.notify.sidecar import _PrefetchedBatchCaches
from snapper.application.notify.sidecar import _priority_to_apns
from snapper.core.json_types import JsonObject
from snapper.data.models import KNOWN_TO_MAX
from snapper.data.models import AlertDelivery
from snapper.data.models import NotificationDevice
from snapper.data.models import User
from snapper.data.repository import Repository
from snapper.data.repository import SQLAlchemyRepository
from snapper.data.repository_types import AlertDeliveryInsertRow
from snapper.data.repository_types import AlertDeliveryRow
from snapper.data.repository_types import AlertEventInsertRow
from snapper.data.repository_types import AlertEventRow
from snapper.data.repository_types import DeviceAlertPrefUpsertRow
from snapper.data.repository_types import NotificationDeviceUpsertRow
from snapper.messaging.infrastructure.publisher import MessagePublisher
from snapper.messaging.infrastructure.publisher import SequenceTracker
from snapper.messaging.schemas.data import AlertEventData


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


async def _seed_user(
    repo: SQLAlchemyRepository,
    user_public_id: str,
    *,
    default_language: str | None = None,
) -> None:
    """Insert a minimal active ``User`` row the fanout will target.

    ``default_language`` lets a test scenario pin the SCD2 row to a
    specific catalog language code (the i18n path) without going
    through the full update path.
    """
    async with repo.session() as s:
        s.add(
            User(
                public_id=user_public_id,
                username=f"u_{user_public_id}",
                email=f"{user_public_id}@example.test",
                password_hash="x",
                role="viewer",
                default_language=default_language,
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
    """Minimal rule emitting exactly one alert per dispatch — for flow tests.

    Optional ``row_payload`` lets a test pin localization loc_keys on
    the emitted row so we can exercise the live-fanout localization path
    (``sidecar._persist_and_fanout_row`` must look up the recipient's
    ``default_language`` and pass it through ``_attempt_once``).
    """

    def __init__(
        self,
        *,
        user_public_id: str,
        topic_prefix: str = "test.",
        alert_type: str = "order_fill_full",
        priority: str = "medium",
        safety_critical: bool = False,
        row_payload: dict[str, object] | None = None,
    ) -> None:
        self.alert_type = alert_type
        self.subscribe_topic_prefixes = (topic_prefix,)
        self.priority = priority
        self.is_safety_critical = safety_critical
        self.thread_key_prefix = "test"
        self.suppression_window_seconds = 0
        self._user = user_public_id
        self._counter = 0
        self._row_payload = row_payload

    async def evaluate(
        self, topic: str, payload: bytes, repo: Repository, now: datetime
    ) -> list[AlertEventInsertRow]:
        self._counter += 1
        row = AlertEventInsertRow(
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
        if self._row_payload is not None:
            row["payload"] = cast(JsonObject, self._row_payload)
        return [row]


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


class _FakeMessagePublisher:
    """In-memory ``MessagePublisher`` stand-in that captures every send.

    Mirrors the real publisher's surface area sufficiently for the
    sidecar to publish a frame: exposes a ``tracker`` property and an
    async ``send`` that records the (topic, data) tuple. Tests inspect
    ``sends`` to assert the publish path ran with the expected
    arguments and ordering relative to the APNs fanout.
    """

    def __init__(self) -> None:
        self._tracker = SequenceTracker()
        self.sends: list[tuple[str, AlertEventData]] = []

    @property
    def tracker(self) -> SequenceTracker:
        return self._tracker

    async def send(self, stream_key: str, data: AlertEventData) -> None:
        self.sends.append((stream_key, data))


def _pin_clock(sidecar: NotifySidecar, at: datetime) -> None:
    """Pin ``sidecar._clock`` to a deterministic timestamp for virtual-time tests."""
    sidecar._clock = lambda: at


def _make_sidecar(
    repo: SQLAlchemyRepository,
    *,
    send_result: ApnsSendResult | Exception | None = None,
    registry: RuleRegistry | None = None,
    portfolio_drift_recovery_scanner: PortfolioDriftRecoveryScanner | None = None,
    clock: Callable[[], datetime] | None = None,
) -> tuple[NotifySidecar, MagicMock]:
    """Construct a sidecar with a mock subscriber + mock APNs pool.

    The injected publisher is a ``_FakeMessagePublisher`` reachable via
    ``sidecar._publisher`` for tests that assert on the web (WebSocket)
    fanout path; tests that don't care about it ignore the
    attribute. ``_publisher`` shares its ``SequenceTracker`` with the
    sidecar exactly like the production CLI wiring.
    """
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
    publisher = _FakeMessagePublisher()
    sidecar = NotifySidecar(
        subscriber=subscriber,
        repo=repo,
        apns=apns,
        apns_topic="ie.klatt.snapper",
        tracker=publisher.tracker,
        publisher=cast(MessagePublisher, publisher),
        registry=registry,
        portfolio_drift_recovery_scanner=portfolio_drift_recovery_scanner,
    )
    if clock is not None:
        sidecar._clock = clock
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
    """``_backoff_seconds`` implements the exponential schedule."""

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

    def test_network_cap_extends_to_fifteen_minutes(self) -> None:
        """Network backoff honours the higher 15-minute cap.

        Given: High retry attempt counts under a network outage cap.
        When: ``_backoff_seconds`` is called with ``cap_s=_NETWORK_BACKOFF_CAP_S``.
        Then: Delays scale 240s, 480s, and cap at 900s (15 min).
        """
        assert _backoff_seconds(4, cap_s=_NETWORK_BACKOFF_CAP_S) == 240.0
        assert _backoff_seconds(5, cap_s=_NETWORK_BACKOFF_CAP_S) == 480.0
        assert _backoff_seconds(6, cap_s=_NETWORK_BACKOFF_CAP_S) == 900.0
        assert _backoff_seconds(20, cap_s=_NETWORK_BACKOFF_CAP_S) == 900.0


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

    def test_user_language_none_keeps_english_title_body(self) -> None:
        """No-preference path emits stored EN title/body.

        Given: An AlertEventRow with loc_key/loc_args populated and
            ``user_language=None``.
        When: ``_build_apns_payload`` is called.
        Then: ``aps.alert.title``/``body`` use the EN columns; the
            catalog is NOT consulted.
        """
        row = self._row(
            title="Order filled",
            body="BUY 100 BTCUSD @ $50000.00 filled on Kraken",
            payload={
                "title_loc_key": "alerts.title.order_fill_full",
                "body_loc_key": "alerts.body.order_fill_full",
                "body_loc_args": ["BUY", "100", "BTCUSD", "50000.00", "Kraken"],
            },
        )
        payload = _build_apns_payload(row, user_language=None)
        aps = payload["aps"]
        assert isinstance(aps, dict)
        alert = aps["alert"]
        assert isinstance(alert, dict)
        assert alert["title"] == "Order filled"
        assert alert["body"] == "BUY 100 BTCUSD @ $50000.00 filled on Kraken"

    @pytest.mark.parametrize("language", [None, "en", "pl", "pt", "no", "sr", "zh", "my-MM"])
    def test_quoted_fill_push_is_safe_without_client_catalog(self, language: str | None) -> None:
        """Given a quote-aware fill, APNs carries resolved text and no client loc-key.

        A recipient with an older application receives the correct explicit
        currency and precision without consulting its bundled dollar template.
        """
        row = self._row(
            title="Order filled",
            body="BUY 1 ETH/BTC @ 0.00234 BTC filled on venue",
            payload={
                "title_loc_key": "alerts.title.order_fill_full",
                "body_loc_key": "alerts.body.order_fill_full_quoted",
                "body_loc_args": ["BUY", "1", "ETH/BTC", "0.00234 BTC", "venue"],
            },
        )
        payload = _build_apns_payload(row, user_language=language)
        aps = payload["aps"]
        assert isinstance(aps, dict)
        alert = aps["alert"]
        assert isinstance(alert, dict)
        body = alert["body"]
        assert isinstance(body, str)
        assert "0.00234 BTC" in body
        assert "$" not in body
        assert "loc-key" not in alert
        assert "body_loc_key" not in payload
        assert "body_loc_args" not in payload

    def test_user_language_pl_resolves_polish_title_body(self) -> None:
        """PL user with loc_key set gets Polish APNs payload.

        Given: An AlertEventRow with body_loc_key + body_loc_args set
            and ``user_language='pl'``.
        When: ``_build_apns_payload`` is called.
        Then: ``aps.alert.title``/``body`` render in Polish via the
            backend catalog; ``%@`` placeholders are substituted in
            source order.
        """
        row = self._row(
            title="Order filled",
            body="BUY 100 BTCUSD @ $50000.00 filled on Kraken",
            payload={
                "title_loc_key": "alerts.title.order_fill_full",
                "body_loc_key": "alerts.body.order_fill_full",
                "body_loc_args": ["BUY", "100", "BTCUSD", "50000.00", "Kraken"],
            },
        )
        payload = _build_apns_payload(row, user_language="pl")
        aps = payload["aps"]
        assert isinstance(aps, dict)
        alert = aps["alert"]
        assert isinstance(alert, dict)
        assert alert["title"] == "Zlecenie zrealizowane"
        body_pl = alert["body"]
        assert isinstance(body_pl, str)
        assert "zrealizowane na Kraken" in body_pl

    def test_user_language_ar_renders_rtl_template(self) -> None:
        """Arabic recipient gets RTL APNs title/body.

        Given: A ``critical_system_error`` row with loc_keys set and
            ``user_language='ar'``.
        When: ``_build_apns_payload`` is called.
        Then: title + body render in Arabic; ``%lld`` placeholder for
            the threshold count is substituted as an int.
        """
        row = self._row(
            title="System degraded: executor",
            body="executor/kraken reported warning for 3 consecutive heartbeats",
            alert_type="critical_system_error",
            payload={
                "title_loc_key": "alerts.title.critical_system_error",
                "title_loc_args": ["executor"],
                "body_loc_key": "alerts.body.critical_system_error",
                "body_loc_args": ["executor", "kraken", "warning", 3],
            },
        )
        payload = _build_apns_payload(row, user_language="ar")
        aps = payload["aps"]
        assert isinstance(aps, dict)
        alert = aps["alert"]
        assert isinstance(alert, dict)
        assert alert["title"] != "System degraded: executor"
        body_ar = alert["body"]
        assert isinstance(body_ar, str)
        assert body_ar != "executor/kraken reported warning for 3 consecutive heartbeats"
        assert "executor" in body_ar

    def test_user_language_set_but_payload_missing_loc_keys_falls_back(self) -> None:
        """Legacy rows without loc_keys emit EN even with a PL user.

        Given: An AlertEventRow whose payload lacks
            ``title_loc_key``/``body_loc_key`` (predates localization,
            or a rule that opted out).
        When: ``_build_apns_payload`` is called with
            ``user_language='pl'``.
        Then: The stored EN columns drive ``aps.alert`` — fallback
            keeps push delivery working for legacy data.
        """
        row = self._row(
            title="Custom title",
            body="Custom body",
            payload={"deep_link_path": "/x"},
        )
        payload = _build_apns_payload(row, user_language="pl")
        aps = payload["aps"]
        assert isinstance(aps, dict)
        alert = aps["alert"]
        assert isinstance(alert, dict)
        assert alert["title"] == "Custom title"
        assert alert["body"] == "Custom body"

    def test_loc_key_unknown_in_catalog_falls_back_to_en(self) -> None:
        """Unrecognized loc_key (catalog miss) falls back to EN.

        Given: A loc_key that doesn't exist in the backend catalog.
        When: ``_build_apns_payload`` is called with a non-EN language.
        Then: The EN ``event.title``/``body`` are used — the catalog
            ``render`` returns the key itself on miss, which the
            resolver detects and substitutes back to EN to avoid
            shipping the literal key as an alert title.
        """
        row = self._row(
            title="Custom title",
            body="Custom body",
            payload={
                "title_loc_key": "alerts.title.nonexistent",
                "body_loc_key": "alerts.body.nonexistent",
            },
        )
        payload = _build_apns_payload(row, user_language="pl")
        aps = payload["aps"]
        assert isinstance(aps, dict)
        alert = aps["alert"]
        assert isinstance(alert, dict)
        assert alert["title"] == "Custom title"
        assert alert["body"] == "Custom body"

    def test_loc_args_must_be_lists_to_render(self) -> None:
        """Malformed loc_args (non-list) falls back to EN.

        Given: ``body_loc_args`` is a string (corrupt payload).
        When: ``_build_apns_payload`` is called.
        Then: Fallback to EN — avoids a crash on bad inputs and
            surfaces the bug in the EN render.
        """
        row = self._row(
            title="Custom title",
            body="Custom body",
            payload={
                "title_loc_key": "alerts.title.order_fill_full",
                "body_loc_key": "alerts.body.order_fill_full",
                "body_loc_args": "should-be-a-list",
            },
        )
        payload = _build_apns_payload(row, user_language="pl")
        aps = payload["aps"]
        assert isinstance(aps, dict)
        alert = aps["alert"]
        assert isinstance(alert, dict)
        assert alert["title"] == "Custom title"
        assert alert["body"] == "Custom body"

    def test_loc_args_arity_mismatch_falls_back_to_en(self) -> None:
        """Wrong argument count for a real catalog key falls back to EN.

        Given: ``body_loc_key`` resolves to a template that takes 5
            placeholders, but ``body_loc_args`` only supplies 2.
        When: ``_build_apns_payload`` runs with a non-EN language.
        Then: ``catalog.render`` raises ``ValueError`` internally; the
            resolver catches it and emits the stored EN columns so the
            user still gets a (legible) push instead of a crash that
            leaves the delivery stuck in retry.
        """
        row = self._row(
            title="Filled fallback",
            body="Filled body fallback",
            payload={
                "title_loc_key": "alerts.title.order_fill_full",
                "body_loc_key": "alerts.body.order_fill_full",
                "body_loc_args": ["BUY", "100"],
            },
        )
        payload = _build_apns_payload(row, user_language="pl")
        aps = payload["aps"]
        assert isinstance(aps, dict)
        alert = aps["alert"]
        assert isinstance(alert, dict)
        assert alert["title"] == "Filled fallback"
        assert alert["body"] == "Filled body fallback"

    def test_loc_keys_are_stripped_from_apns_custom_payload(self) -> None:
        """Internal localization fields never reach the APNs wire payload.

        Given: An event payload that carries the
            ``title_loc_key``/``body_loc_key``/``title_loc_args``/
            ``body_loc_args`` set emitted by the localizing notify rules.
        When: ``_build_apns_payload`` assembles the dict.
        Then: The APNs custom payload contains the public deep link
            but NOT the four localization keys — they're a backend-only
            contract once title/body are resolved server-side, and
            the 4 KB APNs budget would otherwise re-pay for the same
            exchange/instrument values already in ``aps.alert.body``.
        """
        row = self._row(
            payload={
                "deep_link_path": "/orders/1",
                "title_loc_key": "alerts.title.order_fill_full",
                "title_loc_args": ["unused"],
                "body_loc_key": "alerts.body.order_fill_full",
                "body_loc_args": ["BUY", "100", "BTCUSD", "50000.00", "Kraken"],
            },
        )
        payload = _build_apns_payload(row, user_language="pl")
        assert payload["deep_link_path"] == "/orders/1"
        assert "title_loc_key" not in payload
        assert "title_loc_args" not in payload
        assert "body_loc_key" not in payload
        assert "body_loc_args" not in payload

    def test_partial_catalog_miss_falls_back_both_fields_to_en(self) -> None:
        """All-or-nothing locale resolution: title-miss forces body to EN too.

        Given: A ``title_loc_key`` that exists in the catalog but
            a ``body_loc_key`` that does not (drift between rule and
            catalog after a key rename, say).
        When: ``_build_apns_payload`` runs with ``user_language='pl'``.
        Then: The push goes out in EN for BOTH fields — a mixed PL
            title + EN body would look broken to the user, so we treat
            either miss as a full catalog drop.
        """
        row = self._row(
            title="EN title",
            body="EN body",
            payload={
                "title_loc_key": "alerts.title.order_fill_full",
                "body_loc_key": "alerts.body.does_not_exist",
                "body_loc_args": [],
            },
        )
        payload = _build_apns_payload(row, user_language="pl")
        aps = payload["aps"]
        assert isinstance(aps, dict)
        alert = aps["alert"]
        assert isinstance(alert, dict)
        assert alert["title"] == "EN title"
        assert alert["body"] == "EN body"


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
        sidecar, apns = _make_sidecar(repo, registry=reg, clock=lambda: _ts())

        await sidecar._dispatch("test.something", b"{}", _ts())

        history = await repo.list_recent_alerts_for_user(user, limit=10, before=None)
        assert len(history) == 1
        apns.send.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_dispatch_publishes_alert_event_frame_before_apns(
        self, repo: SQLAlchemyRepository
    ) -> None:
        """The web (WebSocket) fanout publishes onto the bus before APNs delivery.

        Given: A matching rule + one device for the recipient user.
        When: ``_dispatch`` runs the persist + fanout chain.
        Then:
            (a) The publisher captured exactly one frame on topic
                ``alerts.{user}.{alert_type}``.
            (b) The frame's ``user_public_id`` / ``alert_type`` / ``title`` /
                ``body`` mirror the persisted ``alert_events`` row, and
                provenance is stamped (``session_id`` non-empty,
                ``sequence_id`` allocated by the shared tracker).
            (c) ``MessagePublisher.send`` is called before
                ``ApnsClientPool.send`` — proves the publish step
                landed in ``_persist_and_fanout_row`` ahead of the
                APNs path, matching the web-fanout latency-sensitivity
                rationale.
        """
        user = "019dbb34-f439-77bd-afa8-ee5321d60307"
        await _seed_user(repo, user)
        await _seed_device(repo, user)
        reg = RuleRegistry()
        reg.register(_SingleRowRule(user_public_id=user, topic_prefix="test."))
        sidecar, apns = _make_sidecar(repo, registry=reg, clock=lambda: _ts())
        order_log: list[str] = []
        publisher = cast("_FakeMessagePublisher", sidecar._publisher)
        original_send = publisher.send

        async def recording_send(stream_key: str, data: AlertEventData) -> None:
            order_log.append("publish")
            await original_send(stream_key, data)

        publisher.send = recording_send

        async def recording_apns_send(**_kwargs: object) -> ApnsSendResult:
            order_log.append("apns")
            return ApnsSendResult(
                status_code=200, status="success", apns_id="apns-xyz", description=""
            )

        apns.send = AsyncMock(side_effect=recording_apns_send)

        await sidecar._dispatch("test.something", b"{}", _ts())

        assert len(publisher.sends) == 1
        sent_topic, sent_frame = publisher.sends[0]
        assert sent_topic == f"alerts.{user}.order_fill_full"
        assert sent_frame.type == "alert_event"
        assert sent_frame.user_public_id == user
        assert sent_frame.alert_type == "order_fill_full"
        assert sent_frame.title == "Fixture"
        assert sent_frame.body == "Fixture body"
        assert sent_frame.session_id != ""
        assert sent_frame.sequence_id > 0
        assert order_log == ["publish", "apns"]

    @pytest.mark.asyncio
    async def test_live_fanout_resolves_user_language_from_repo(
        self, repo: SQLAlchemyRepository
    ) -> None:
        """The first-attempt push (live fanout) localizes to the recipient.

        Given: A user pinned to ``default_language='pl'`` and a rule
            emitting a row with the localization loc_key contract.
        When: ``_dispatch`` runs the live path
            (``_persist_and_fanout_row`` → ``_attempt_once``).
        Then: ``apns.send`` receives an ``aps.alert.title`` rendered
            via the Polish catalog — proves the live path looks up the
            user's language from the repo, not just the drain/retry
            loop which previously read from a prefetched cache.
        """
        user = "019dbb34-f439-77bd-afa8-ee5321d60307"
        await _seed_user(repo, user, default_language="pl")
        await _seed_device(repo, user)
        reg = RuleRegistry()
        reg.register(
            _SingleRowRule(
                user_public_id=user,
                topic_prefix="test.",
                row_payload={
                    "title_loc_key": "alerts.title.order_fill_full",
                    "body_loc_key": "alerts.body.order_fill_full",
                    "body_loc_args": [
                        "BUY",
                        "100",
                        "BTCUSD",
                        "50000.00",
                        "Kraken",
                    ],
                },
            )
        )
        sidecar, apns = _make_sidecar(repo, registry=reg, clock=lambda: _ts())

        await sidecar._dispatch("test.something", b"{}", _ts())

        apns.send.assert_awaited_once()
        sent_payload = apns.send.await_args.kwargs["payload"]
        aps = sent_payload["aps"]
        alert = aps["alert"]
        assert alert["title"] == "Zlecenie zrealizowane"
        assert "zrealizowane na Kraken" in alert["body"]

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
        sidecar, apns = _make_sidecar(repo, registry=reg, clock=lambda: _ts())

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
        sidecar, apns = _make_sidecar(repo, registry=reg, clock=lambda: _ts())

        await sidecar._dispatch("test.topic", b"{}", _ts())

        history = await repo.list_recent_alerts_for_user(user, limit=10, before=None)
        assert len(history) == 1
        apns.send.assert_not_awaited()


class TestScopeRevalidationDispatch:
    """Administrative revocation events route to the scope revalidator."""

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

    @pytest.mark.asyncio
    async def test_membership_revoked_routed_to_revalidator(
        self, repo: SQLAlchemyRepository
    ) -> None:
        """Desk detach frames bypass alert rules and hit the membership handler."""
        sidecar, _ = _make_sidecar(repo)
        sidecar._scope_revalidator = MagicMock()
        sidecar._scope_revalidator.handle_membership_revoked = AsyncMock()

        await sidecar._dispatch("admin.membership_revoked", b"{}", _ts())

        sidecar._scope_revalidator.handle_membership_revoked.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_scope_revoked_handler_exception_does_not_kill_loop(
        self, repo: SQLAlchemyRepository
    ) -> None:
        """An exception from the scope-revoked handler is caught + logged.

        A DB race in bulk cancel (e.g. IntegrityError on a partial-unique index)
        previously bubbled out of the admin-topic handler and killed
        the sidecar's receive loop. ``_dispatch`` now wraps both
        admin handlers in try/except so a single event-handling
        failure is a logged warning, not a process death.
        """
        sidecar, _ = _make_sidecar(repo)
        sidecar._scope_revalidator = MagicMock()
        sidecar._scope_revalidator.handle_scope_revoked = AsyncMock(
            side_effect=RuntimeError("db race boom")
        )

        await sidecar._dispatch("admin.scope_revoked", b"{}", _ts())

    @pytest.mark.asyncio
    async def test_user_deactivated_handler_exception_does_not_kill_loop(
        self, repo: SQLAlchemyRepository
    ) -> None:
        """Same exception-isolation guard applies to ``admin.user_deactivated``."""
        sidecar, _ = _make_sidecar(repo)
        sidecar._scope_revalidator = MagicMock()
        sidecar._scope_revalidator.handle_user_deactivated = AsyncMock(
            side_effect=RuntimeError("db race boom")
        )

        await sidecar._dispatch("admin.user_deactivated", b"{}", _ts())

    @pytest.mark.asyncio
    async def test_membership_revoked_handler_exception_does_not_kill_loop(
        self, repo: SQLAlchemyRepository
    ) -> None:
        """A failed desk cancellation is isolated from the receive loop."""
        sidecar, _ = _make_sidecar(repo)
        sidecar._scope_revalidator = MagicMock()
        sidecar._scope_revalidator.handle_membership_revoked = AsyncMock(
            side_effect=RuntimeError("db race boom")
        )

        await sidecar._dispatch("admin.membership_revoked", b"{}", _ts())


class TestScopePreSendSkip:
    """Scope pre-send skip: ``should_skip_send`` gates the APNs call in ``_attempt_once``."""

    @pytest.mark.asyncio
    async def test_attempt_aborts_when_bump_loses_to_concurrent_cancel(
        self, repo: SQLAlchemyRepository
    ) -> None:
        """``_bump_attempt`` returning None aborts the APNs call.

        Between the ``should_skip_send`` check and the attempt-count bump, an
        admin handler can cancel the queued delivery. The attempt-count
        bump then sees the row is no longer queued and returns None.
        Without the guard in ``_attempt_once``, the sidecar would still
        call ``_apns.send`` for an already-cancelled delivery.
        """
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
        await repo.mark_delivery_cancelled(
            delivery_pid,
            reason="scope_revoked",
            transition_at=_ts(1),
            session_id="admin",
            sequence_id=1,
        )
        sidecar, apns = _make_sidecar(repo, clock=lambda: _ts(2))
        device = (await repo.list_active_notification_devices_for_user(user))[0]

        await sidecar._attempt_once(delivery_pid, device, event, _ts(2))

        apns.send.assert_not_awaited()

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
        sidecar, apns = _make_sidecar(repo, clock=lambda: _ts())
        sidecar._scope_revalidator = MagicMock()
        sidecar._scope_revalidator.should_skip_send = AsyncMock(return_value=True)
        device = (await repo.list_active_notification_devices_for_user(user))[0]

        await sidecar._attempt_once(delivery_pid, device, event, _ts())

        apns.send.assert_not_awaited()
        queued = await repo.list_queued_deliveries_all()
        assert queued == []

    @pytest.mark.asyncio
    async def test_detach_during_attempt_reservation_blocks_apns_admission(
        self, repo: SQLAlchemyRepository
    ) -> None:
        """A detach committed during the bump prevents the APNs call from starting.

        Given: The first scope check passes but the post-bump check observes a
            revoked desk membership.
        When: The sidecar reserves the durable attempt and reaches send admission.
        Then: It cancels the delivery without invoking the APNs client.
        """
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
        sidecar, apns = _make_sidecar(repo, clock=lambda: _ts())
        sidecar._scope_revalidator = MagicMock()
        sidecar._scope_revalidator.should_skip_send = AsyncMock(side_effect=[False, True])
        device = (await repo.list_active_notification_devices_for_user(user))[0]

        await sidecar._attempt_once(delivery_pid, device, event, _ts())

        assert sidecar._scope_revalidator.should_skip_send.await_count == 2
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
        sidecar, _ = _make_sidecar(repo, send_result=unreg, clock=lambda: _ts())
        device = (await repo.list_active_notification_devices_for_user(user))[0]

        await sidecar._attempt_once(delivery_pid, device, event, _ts())

        active = await repo.list_active_notification_devices_for_user(user)
        assert active == []

    @pytest.mark.asyncio
    async def test_400_bad_device_token_deactivates_device_without_retry(
        self, repo: SQLAlchemyRepository
    ) -> None:
        """APNs 400 BadDeviceToken deactivates the device immediately without retrying."""
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
        mapped = _map_result(NotificationResult("", "400", "BadDeviceToken", None))
        sidecar, _ = _make_sidecar(repo, send_result=mapped, clock=lambda: _ts())
        device = (await repo.list_active_notification_devices_for_user(user))[0]

        await sidecar._attempt_once(delivery_pid, device, event, _ts())

        assert await repo.list_active_notification_devices_for_user(user) == []
        counts = await repo.count_deliveries_by_status()
        assert counts == {"unregistered": 1}

    @pytest.mark.asyncio
    async def test_400_device_token_not_for_topic_logs_error_and_keeps_device_active(
        self,
        repo: SQLAlchemyRepository,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """APNs 400 DeviceTokenNotForTopic logs ERROR, fails delivery, and keeps device active."""
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
        mapped = _map_result(NotificationResult("", "400", "DeviceTokenNotForTopic", None))
        sidecar, _ = _make_sidecar(repo, send_result=mapped, clock=lambda: _ts())
        device = (await repo.list_active_notification_devices_for_user(user))[0]

        handler_id = logger.add(caplog.handler, format="{message}", level="ERROR")
        try:
            await sidecar._attempt_once(delivery_pid, device, event, _ts())
        finally:
            logger.remove(handler_id)

        active = await repo.list_active_notification_devices_for_user(user)
        assert [d["public_id"] for d in active] == [device_pid]
        assert await repo.list_queued_deliveries_all() == []
        assert await repo.count_deliveries_by_status() == {"failed": 1}
        async with repo.session() as s:
            delivery_row = (
                await s.execute(
                    select(AlertDelivery).where(
                        AlertDelivery.public_id == delivery_pid,
                        AlertDelivery.known_to == KNOWN_TO_MAX,
                    )
                )
            ).scalar_one()
        assert delivery_row.status == "failed"
        assert delivery_row.error_reason == "config_error:DeviceTokenNotForTopic"
        errors = [
            r
            for r in caplog.records
            if r.levelname == "ERROR" and "APNs configuration error" in r.getMessage()
        ]
        assert len(errors) == 1
        assert "DeviceTokenNotForTopic" in errors[0].getMessage()
        assert device_pid in errors[0].getMessage()

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "raw_description",
        [
            "BadTopic",
            "MissingDeviceToken",
            "",
            None,
            "<html>not-json</html>",
        ],
    )
    async def test_400_other_reasons_leave_device_active_and_schedule_retry(
        self,
        repo: SQLAlchemyRepository,
        raw_description: str | None,
    ) -> None:
        """Non-unregistered APNs 400 reasons stay 'other', keep device active, and retry."""
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
        mapped = _map_result(NotificationResult("", "400", raw_description, None))
        assert mapped.status == "other"
        sidecar, _ = _make_sidecar(repo, send_result=mapped, clock=lambda: _ts())
        device = (await repo.list_active_notification_devices_for_user(user))[0]

        await sidecar._attempt_once(delivery_pid, device, event, _ts())

        active = await repo.list_active_notification_devices_for_user(user)
        assert [d["public_id"] for d in active] == [device_pid]
        queued = await repo.list_queued_deliveries_all()
        assert len(queued) == 1
        assert queued[0]["status"] == "queued"
        assert queued[0]["attempt_count"] == 1
        assert await repo.count_deliveries_by_status() == {"queued": 1}

    @pytest.mark.asyncio
    async def test_410_path_writes_inactive_successor_row(self, repo: SQLAlchemyRepository) -> None:
        """Regression: 410 writes a tombstone successor, not a gap."""
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
        sidecar, _ = _make_sidecar(repo, send_result=unreg, clock=lambda: _ts())
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
        sidecar, _ = _make_sidecar(repo, send_result=err, clock=lambda: _ts())
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
        sidecar, _ = _make_sidecar(repo, send_result=err, clock=lambda: _ts())
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
        sidecar, _ = _make_sidecar(
            repo, send_result=RuntimeError("connection reset"), clock=lambda: _ts()
        )
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
        """Retry exhaust emits ``logger.warning`` before terminal mark."""
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
        sidecar, _ = _make_sidecar(repo, send_result=err, clock=lambda: _ts())
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
        sidecar, apns = _make_sidecar(repo, clock=lambda: _ts(5))

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
        sidecar, apns = _make_sidecar(repo, clock=lambda: _ts(2))

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
        sidecar, apns = _make_sidecar(repo, clock=lambda: _ts())

        await sidecar._drain_outbox(_ts())

        queued = await repo.list_queued_deliveries_all()
        assert queued == []
        apns.send.assert_not_awaited()


class TestAttemptOnQueuedRowNoCache:
    """``_attempt_on_queued_row`` falls back to per-row repo lookups when caches are absent."""

    @pytest.mark.asyncio
    async def test_per_row_device_lookup_when_devices_cache_missing(
        self, repo: SQLAlchemyRepository
    ) -> None:
        """Calling without a ``devices_cache`` triggers the per-user fallback fetch.

        Given: A queued delivery for a registered user/device and an event
            cache that hits but no devices_cache passed,
        When: ``_attempt_on_queued_row`` is invoked directly (the
            cancellation handler path that processes a single delivery
            without a surrounding batch),
        Then: The per-user
            ``list_active_notification_devices_for_user`` fallback runs
            and APNs is invoked exactly once.
        """
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
        sidecar, apns = _make_sidecar(repo, clock=lambda: _ts(5))
        row = await repo.get_delivery_by_public_id(delivery_pid)
        assert row is not None

        await sidecar._attempt_on_queued_row(row, _ts(5))

        apns.send.assert_awaited_once()


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
    async def test_start_with_pre_set_stop_event_skips_receive_loop(
        self, repo: SQLAlchemyRepository, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A sidecar stopped before start exits without opening a receive wait."""
        sidecar, _ = _make_sidecar(repo)

        async def _instant_retry_loop() -> None:
            """Complete immediately so start can reach its finally branch."""
            return None

        monkeypatch.setattr(sidecar, "_process_retry_queue_loop", _instant_retry_loop)
        sidecar._subscriber.recv_multipart = AsyncMock()

        await sidecar.stop()
        await asyncio.wait_for(sidecar.start(), timeout=1.0)

        sidecar._subscriber.recv_multipart.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_start_and_stop_manage_injected_drift_recovery_scanner(
        self,
        repo: SQLAlchemyRepository,
    ) -> None:
        """The notify process owns the recovery scanner's full lifecycle."""
        scanner = MagicMock(spec=PortfolioDriftRecoveryScanner)
        started = asyncio.Event()

        async def start_scanner() -> None:
            """Signal that the injected scanner reached its start boundary."""
            started.set()

        scanner.start = AsyncMock(side_effect=start_scanner)
        scanner.stop = AsyncMock()
        sidecar, _ = _make_sidecar(
            repo,
            portfolio_drift_recovery_scanner=cast(PortfolioDriftRecoveryScanner, scanner),
        )

        async def _blocks_forever() -> tuple[str, bytes]:
            """Keep the receive loop alive until the test stops the process."""
            await asyncio.sleep(3600)
            return ("", b"")

        sidecar._subscriber.recv_multipart = _blocks_forever
        task = asyncio.create_task(sidecar.start())
        await asyncio.wait_for(started.wait(), timeout=1.0)
        await sidecar.stop()
        await asyncio.wait_for(task, timeout=2.0)

        scanner.start.assert_awaited_once()
        assert scanner.stop.await_count >= 1

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
            "admin.membership_revoked",
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
        retry_running = asyncio.Event()

        async def _unstoppable_retry_loop() -> None:
            """Signal `retry_running`, then sleep indefinitely; only a direct task.cancel() ends us.

            Without the explicit ``retry_running.set()`` rendezvous, the
            assertion below is racy: ``start()`` does an ``await``-heavy
            outbox drain before ``create_task(_process_retry_queue_loop)``,
            so a fixed sleep in the test can return before the retry task
            has even been scheduled. If the test then calls ``stop()`` and
            the finally-block cancel() lands on a task that has not yet
            started its coroutine, the task transitions straight to
            cancelled without entering the ``try`` body — the
            ``CancelledError`` handler never fires and ``cancelled`` stays
            unset.
            """
            retry_running.set()
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
        await asyncio.wait_for(retry_running.wait(), timeout=2.0)
        await sidecar.stop()
        await asyncio.wait_for(task, timeout=2.0)

        assert sidecar._retry_task is not None
        assert sidecar._retry_task.done()
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
        sidecar, apns = _make_sidecar(repo, registry=reg, clock=lambda: _ts())

        received = asyncio.Event()
        dispatched = asyncio.Event()

        async def _one_frame_then_hang() -> tuple[str, bytes]:
            """Observe the next receive only after the first dispatch completes."""
            if not received.is_set():
                received.set()
                return ("test.anything", b"{}")
            dispatched.set()
            await asyncio.sleep(5.0)
            return ("", b"")

        sidecar._subscriber.recv_multipart = _one_frame_then_hang
        task = asyncio.create_task(sidecar.start())
        try:
            await asyncio.wait_for(dispatched.wait(), timeout=1.0)
        finally:
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

    @pytest.mark.asyncio
    async def test_atomic_drift_dedup_loser_skips_every_fanout(
        self,
        repo: SQLAlchemyRepository,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A concurrent drift claim loser cannot publish or route a duplicate."""
        sidecar, apns = _make_sidecar(repo)
        insert = AsyncMock(return_value=None)
        read_back = AsyncMock()
        monkeypatch.setattr(sidecar, "_insert_alert_event", insert)
        monkeypatch.setattr(repo, "get_alert_event_by_public_id", read_back)
        alert_row = AlertEventInsertRow(
            user_public_id="user-loser",
            operator_public_id="operator-1",
            wallet_public_id="wallet-1",
            alert_type="drift",
            priority="high",
            is_safety_critical=True,
            title="Portfolio drift detected",
            body="Drift",
            dedup_key="drift.episode-loser",
            thread_key="snapper.drift.episode-loser",
            source_topic="bus.portfolio_drift_episode",
        )

        await sidecar._persist_and_fanout_row(alert_row, _ts())

        insert.assert_awaited_once_with(alert_row, _ts())
        read_back.assert_not_awaited()
        apns.send.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_drift_insert_uses_atomic_repository_claim(
        self,
        repo: SQLAlchemyRepository,
    ) -> None:
        """Both recovery and bus drift rows use the serialized insert primitive."""
        sidecar, _ = _make_sidecar(repo)
        alert_row = AlertEventInsertRow(
            user_public_id="user-atomic-sidecar",
            operator_public_id="operator-1",
            wallet_public_id="wallet-1",
            alert_type="drift",
            priority="high",
            is_safety_critical=True,
            title="Portfolio drift detected",
            body="Drift",
            dedup_key="drift.episode-sidecar",
            thread_key="snapper.drift.episode-sidecar",
            source_topic="bus.portfolio_drift_episode",
        )

        first = await sidecar._insert_alert_event(alert_row, _ts())
        second = await sidecar._insert_alert_event(alert_row, _ts(1))

        assert first is not None
        assert second is None


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
        assert (
            await repo.begin_delivery_attempt(
                delivery_pid, transition_at=_ts(), session_id="prep", sequence_id=98
            )
            == 1
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
        sidecar, apns = _make_sidecar(repo, send_result=ok, clock=lambda: _ts(1))

        monkeypatch.setattr("snapper.application.notify.sidecar._RETRY_LOOP_INTERVAL_S", 0.05)
        task = asyncio.create_task(sidecar._process_retry_queue_loop())
        await asyncio.sleep(0.15)
        await sidecar.stop()
        await asyncio.wait_for(task, timeout=2.0)

        still_queued = await repo.list_queued_deliveries_all()
        assert still_queued == []
        apns.send.assert_awaited()


@pytest.mark.asyncio
async def test_bump_uses_repository_committed_number_without_snapshot_read(
    repo: SQLAlchemyRepository, monkeypatch: pytest.MonkeyPatch
) -> None:
    """It returns three without a read.

    Given two committed attempts,
    When sidecar allocates,
    Then it returns three without a read.
    """
    delivery_pid = await _seed_queued_delivery(
        repo, event_public_id="event", device_public_id="device", user_public_id="user"
    )
    for number in (1, 2):
        assert (
            await repo.begin_delivery_attempt(
                delivery_pid, transition_at=_ts(number), session_id="competitor", sequence_id=number
            )
            == number
        )
    sidecar, _ = _make_sidecar(repo)
    read = AsyncMock(side_effect=AssertionError("Sidecar must not read an attempt snapshot"))
    monkeypatch.setattr(repo, "get_delivery_by_public_id", read)

    assert await sidecar._bump_attempt(delivery_pid, _ts(3)) == 3

    read.assert_not_awaited()
    rows = await repo.list_queued_deliveries_all()
    assert len(rows) == 1
    assert rows[0]["attempt_count"] == 3
    assert rows[0]["last_attempt_at"] == _ts(3)


class TestNetworkOutageRetryAndExpiry:
    """Network-class errors retry with 15 min cap until TTL; rejects stop at 3."""

    @pytest.mark.asyncio
    async def test_dispatch_writes_default_six_hour_expires_at(
        self, repo: SQLAlchemyRepository
    ) -> None:
        """New queued APNs rows record a 6 h ``expires_at`` at creation time.

        Given: A user with an active device and a sidecar failing with ``MaxAttemptsExceeded``.
        When: ``_dispatch`` creates and attempts the delivery at ``_ts()``.
        Then: The queued ``alert_deliveries`` row stores ``expires_at == _ts() + 6h``.
        """
        user = "019dbb34-f439-77bd-afa8-ee5321d60307"
        await _seed_user(repo, user)
        await _seed_device(repo, user)
        reg = RuleRegistry()
        reg.register(_SingleRowRule(user_public_id=user, topic_prefix="test."))
        sidecar, _ = _make_sidecar(
            repo, registry=reg, send_result=MaxAttemptsExceeded(), clock=lambda: _ts()
        )

        await sidecar._dispatch("test.outage", b"{}", _ts())

        queued = await repo.list_queued_deliveries_all()
        assert len(queued) == 1
        assert queued[0]["expires_at"] == _ts() + timedelta(hours=6)
        assert queued[0]["error_reason"] == "exception: MaxAttemptsExceeded"

    @pytest.mark.asyncio
    async def test_network_outage_retries_past_three_attempts_until_recovery(
        self,
        repo: SQLAlchemyRepository,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Network failures survive past attempt 3 and log one recovery warning.

        Given: A delivery queued during an outage failing with DNS, proxy,
            timeout, and ``MaxAttemptsExceeded`` errors across 5 attempts.
        When: The outage ends on attempt 6 and a second delivery also succeeds.
        Then: The delivery succeeds on attempt 6 with backoff reaching >300s,
            and exactly one recovery WARNING summarizes ``1 deliveries delayed``.
        """
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
        apns.send = AsyncMock(
            side_effect=[
                socket.gaierror(-3, "temporary DNS failure"),
                httpx.ProxyError("proxy unreachable"),
                TimeoutError("connect timed out"),
                MaxAttemptsExceeded(),
                OSError(errno.ENETUNREACH, "network down"),
                ApnsSendResult(
                    status_code=200, status="success", apns_id="apns-rec", description=""
                ),
                ApnsSendResult(
                    status_code=200, status="success", apns_id="apns-next", description=""
                ),
            ]
        )
        device = (await repo.list_active_notification_devices_for_user(user))[0]

        handler_id = logger.add(caplog.handler, format="{message}", level="WARNING")
        try:
            for idx, minute in enumerate((0, 1, 2, 4, 8), start=1):
                _pin_clock(sidecar, _ts(minute))
                await sidecar._attempt_once(delivery_pid, device, event, _ts(minute))
                queued = await repo.list_queued_deliveries_all()
                assert len(queued) == 1
                assert queued[0]["attempt_count"] == idx
            _pin_clock(sidecar, _ts(23))
            await sidecar._attempt_once(delivery_pid, device, event, _ts(23))
            second_pid = await _seed_queued_delivery(
                repo,
                event_public_id=event["public_id"],
                device_public_id=device_pid,
                user_public_id=user,
            )
            _pin_clock(sidecar, _ts(24))
            await sidecar._attempt_once(second_pid, device, event, _ts(24))
        finally:
            logger.remove(handler_id)

        assert await repo.list_queued_deliveries_all() == []
        assert await repo.count_deliveries_by_status() == {"sent": 2}
        recovery_logs = [
            r.getMessage()
            for r in caplog.records
            if r.levelname == "WARNING" and "delayed by an outage" in r.getMessage()
        ]
        assert len(recovery_logs) == 1
        assert "1 deliveries delayed by an outage in env=sandbox" in recovery_logs[0]

    @pytest.mark.asyncio
    async def test_custom_expires_at_honored_and_clamped_before_expiry(
        self, repo: SQLAlchemyRepository
    ) -> None:
        """Explicit ``expires_at`` clamps retry schedules and expires to ``failed``.

        Given: A queued row with ``expires_at = _ts(3)`` (3 minutes from creation).
        When: Attempt 1 fails with ``MaxAttemptsExceeded`` at ``_ts(2)`` (backoff 30s
            fits) and attempt 2 fails at ``_ts(2) + 45s`` (backoff 60s overshoots),
            then the retry loop checks at ``_ts(3)``.
        Then: ``next_attempt_at`` clamps to ``expires_at``, and at ``_ts(3)`` the row
            transitions to ``failed`` with reason starting ``expired during outage``
            without issuing a post-expiry APNs call.
        """
        user = "019dbb34-f439-77bd-afa8-ee5321d60307"
        await _seed_user(repo, user)
        device_pid = await _seed_device(repo, user)
        event = await _seed_alert_event(repo, user)
        expiry = _ts(3)
        delivery_pid = await repo.insert_alert_delivery(
            AlertDeliveryInsertRow(
                session_id="t",
                sequence_id=10,
                timestamp=_ts(),
                alert_event_public_id=event["public_id"],
                device_public_id=device_pid,
                user_public_id=user,
                status="queued",
                expires_at=expiry,
                created_at=_ts(),
            )
        )
        sidecar, apns = _make_sidecar(repo, send_result=MaxAttemptsExceeded())
        row = await repo.get_delivery_by_public_id(delivery_pid)
        assert row is not None

        _pin_clock(sidecar, _ts(2))
        await sidecar._attempt_on_queued_row(row, _ts(2))
        row_after_one = await repo.get_delivery_by_public_id(delivery_pid)
        assert row_after_one is not None
        assert row_after_one["next_attempt_at"] == _ts(2) + timedelta(seconds=30)

        _pin_clock(sidecar, _ts(2) + timedelta(seconds=45))
        await sidecar._attempt_on_queued_row(row_after_one, _ts(2) + timedelta(seconds=45))
        row_after_two = await repo.get_delivery_by_public_id(delivery_pid)
        assert row_after_two is not None
        assert row_after_two["next_attempt_at"] == expiry

        calls_before_expiry = apns.send.await_count
        _pin_clock(sidecar, expiry)
        await sidecar._attempt_on_queued_row(row_after_two, expiry)
        assert apns.send.await_count == calls_before_expiry
        final_row = await repo.get_delivery_by_public_id(delivery_pid)
        assert final_row is not None
        assert final_row["status"] == "failed"
        assert final_row["error_reason"] is not None
        assert final_row["error_reason"].startswith("expired during outage")

    @pytest.mark.asyncio
    async def test_network_failure_past_ttl_marks_failed_expired_during_outage(
        self, repo: SQLAlchemyRepository
    ) -> None:
        """A legacy row without ``expires_at`` expires 6 h after ``created_at``.

        Given: A queued row with ``expires_at=None`` created at ``_ts()``.
        When: A network failure happens at ``_ts() + 6h`` via ``_attempt_once``
            and when ``expires_at`` is explicit on the DB row or the row vanishes.
        Then: Past TTL transitions to ``failed`` with ``expired during outage``;
            a missing DB row falls back cleanly without crashing.
        """
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
        sidecar, _ = _make_sidecar(repo, send_result=MaxAttemptsExceeded(), clock=lambda: _ts(360))
        device = (await repo.list_active_notification_devices_for_user(user))[0]

        await sidecar._attempt_once(delivery_pid, device, event, _ts(360))
        row = await repo.get_delivery_by_public_id(delivery_pid)
        assert row is not None
        assert row["status"] == "failed"
        assert row["error_reason"] == "expired during outage"

        explicit_pid = await repo.insert_alert_delivery(
            AlertDeliveryInsertRow(
                session_id="t",
                sequence_id=11,
                timestamp=_ts(),
                alert_event_public_id=event["public_id"],
                device_public_id=device_pid,
                user_public_id=user,
                status="queued",
                expires_at=_ts(5),
                created_at=_ts(),
            )
        )
        _pin_clock(sidecar, _ts(10))
        await sidecar._attempt_once(explicit_pid, device, event, _ts(10))
        explicit_row = await repo.get_delivery_by_public_id(explicit_pid)
        assert explicit_row is not None
        assert explicit_row["status"] == "failed"

        legacy_pid = await _seed_queued_delivery(
            repo,
            event_public_id=event["public_id"],
            device_public_id=device_pid,
            user_public_id=user,
        )
        legacy_row = await repo.get_delivery_by_public_id(legacy_pid)
        assert legacy_row is not None
        assert legacy_row.get("expires_at") is None
        _pin_clock(sidecar, _ts(360))
        await sidecar._attempt_on_queued_row(legacy_row, _ts(360))
        legacy_after = await repo.get_delivery_by_public_id(legacy_pid)
        assert legacy_after is not None
        assert legacy_after["status"] == "failed"
        assert legacy_after["error_reason"] == "expired during outage"

        missing_target = _DeliveryTarget(
            delivery_public_id="nonexistent-delivery",
            device=device,
            event=event,
        )
        assert await sidecar._resolve_delivery_deadline(missing_target, _ts(1)) == _ts(1)
        assert _expired_error_reason(None) == "expired during outage"
        assert _format_exception_reason(MaxAttemptsExceeded()) == "exception: MaxAttemptsExceeded"

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "outcome",
        [
            ApnsSendResult(
                status_code=413, status="other", apns_id="", description="PayloadTooLarge"
            ),
            RuntimeError("non-network explosion"),
        ],
        ids=["http-413-reject", "non-network-exception"],
    )
    async def test_rejects_and_non_network_errors_give_up_after_three_attempts(
        self,
        repo: SQLAlchemyRepository,
        outcome: ApnsSendResult | Exception,
    ) -> None:
        """HTTP 4xx rejects and non-network exceptions keep the 3-attempt limit.

        Given: A queued delivery encountering HTTP 413 ``PayloadTooLarge`` or a
            non-network ``RuntimeError``.
        When: Three consecutive attempts fail even within the 6 h TTL.
        Then: The delivery transitions to terminal ``failed`` on attempt 3.
        """
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
        sidecar, _ = _make_sidecar(repo, send_result=outcome)
        device = (await repo.list_active_notification_devices_for_user(user))[0]

        for minute in (0, 1, 2):
            _pin_clock(sidecar, _ts(minute))
            await sidecar._attempt_once(delivery_pid, device, event, _ts(minute))

        final_row = await repo.get_delivery_by_public_id(delivery_pid)
        assert final_row is not None
        assert final_row["status"] == "failed"
        assert final_row["attempt_count"] == 3

    @pytest.mark.asyncio
    async def test_send_timeout_recovers_retry_loop_without_stalling_next_row(
        self,
        repo: SQLAlchemyRepository,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A stalled ``apns.send`` times out as network-class and yields to the next row.

        Given: Two queued deliveries with ``next_attempt_at`` in the past where the
            first call hangs past ``_APNS_SEND_TIMEOUT_S`` and the second succeeds.
        When: ``_process_retry_queue_loop`` ticks once.
        Then: Row 1 is rescheduled as a network retry (``TimeoutError``) instead of
            wedging the loop, and row 2 is delivered in the same tick.
        """
        user = "019dbb34-f439-77bd-afa8-ee5321d60307"
        await _seed_user(repo, user)
        device_pid = await _seed_device(repo, user)
        event = await _seed_alert_event(repo, user)
        past = datetime(2020, 1, 1, tzinfo=UTC)
        hanging_pid = await repo.insert_alert_delivery(
            AlertDeliveryInsertRow(
                session_id="t",
                sequence_id=1,
                timestamp=_ts(),
                alert_event_public_id=event["public_id"],
                device_public_id=device_pid,
                user_public_id=user,
                status="queued",
                attempt_count=3,
                next_attempt_at=past,
                expires_at=datetime(2099, 1, 1, tzinfo=UTC),
                created_at=_ts(),
            )
        )
        healthy_pid = await repo.insert_alert_delivery(
            AlertDeliveryInsertRow(
                session_id="t",
                sequence_id=2,
                timestamp=_ts(),
                alert_event_public_id=event["public_id"],
                device_public_id=device_pid,
                user_public_id=user,
                status="queued",
                next_attempt_at=past + timedelta(seconds=5),
                expires_at=datetime(2099, 1, 1, tzinfo=UTC),
                created_at=_ts(),
            )
        )
        sidecar, apns = _make_sidecar(repo)
        call_count = 0

        async def _hang_then_succeed(**_kwargs: object) -> ApnsSendResult:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                await asyncio.sleep(5.0)
            return ApnsSendResult(
                status_code=200, status="success", apns_id="apns-ok", description=""
            )

        apns.send = AsyncMock(side_effect=_hang_then_succeed)
        original_recovery = sidecar._maybe_log_outage_recovery

        async def _stop_after_first_tick_recovery(
            env: str, recovered_delivery_public_id: str, now: datetime, outcome_sequence: int
        ) -> None:
            await original_recovery(env, recovered_delivery_public_id, now, outcome_sequence)
            await sidecar.stop()

        monkeypatch.setattr(sidecar, "_maybe_log_outage_recovery", _stop_after_first_tick_recovery)
        monkeypatch.setattr("snapper.application.notify.sidecar._APNS_SEND_TIMEOUT_S", 0.02)
        monkeypatch.setattr("snapper.application.notify.sidecar._RETRY_LOOP_INTERVAL_S", 0.01)

        await asyncio.wait_for(sidecar._process_retry_queue_loop(), timeout=2.0)

        row_one = await repo.get_delivery_by_public_id(hanging_pid)
        row_two = await repo.get_delivery_by_public_id(healthy_pid)
        assert row_one is not None
        assert row_one["status"] == "queued"
        assert row_one["attempt_count"] == 4
        assert row_one["error_reason"] == "exception: TimeoutError"
        assert row_two is not None
        assert row_two["status"] == "sent"
        assert row_one["next_attempt_at"] is not None
        assert row_two["last_attempt_at"] is not None
        assert (
            row_two["last_attempt_at"]
            <= row_one["next_attempt_at"]
            < row_two["last_attempt_at"] + timedelta(seconds=5)
        )


class TestTerminalSuppressionAndIsolation:
    """Suppressions leave no queued rows and per-row exceptions do not stop drain."""

    @pytest.mark.asyncio
    async def test_routing_cascade_suppression_at_dispatch_queues_zero_rows(
        self, repo: SQLAlchemyRepository
    ) -> None:
        """Routing suppression during live fanout inserts zero delivery rows.

        Given: A user with an active device whose pref disables ``order_fill_full``.
        When: ``_dispatch`` evaluates the alert.
        Then: The ``alert_event`` persists, zero ``alert_deliveries`` are queued,
            and ``apns.send`` is not called.
        """
        user = "019dbb34-f439-77bd-afa8-ee5321d60307"
        await _seed_user(repo, user)
        device_pid = await _seed_device(repo, user)
        await repo.upsert_device_alert_pref(
            DeviceAlertPrefUpsertRow(
                session_id="seed",
                sequence_id=2,
                timestamp=_ts(),
                device_public_id=device_pid,
                alert_type="order_fill_full",
                enabled=False,
            )
        )
        reg = RuleRegistry()
        reg.register(_SingleRowRule(user_public_id=user, topic_prefix="test."))
        sidecar, apns = _make_sidecar(repo, registry=reg, clock=lambda: _ts())

        await sidecar._dispatch("test.suppressed", b"{}", _ts())

        assert await repo.count_deliveries_by_status() == {}
        apns.send.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_queued_delivery_suppressed_by_prefs_transitions_cancelled_scope(
        self, repo: SQLAlchemyRepository
    ) -> None:
        """Queued rows suppressed by later preference changes become ``cancelled_scope``.

        Given: Two queued APNs deliveries: one evaluated through ``_drain_outbox``
            after the user disabled the alert type, and one evaluated directly via
            ``_attempt_on_queued_row`` after ``push_beta`` excluded the user.
        When: Drain and per-row retry check routing admission.
        Then: Both transition to terminal ``cancelled_scope`` with reason
            ``routing_suppressed`` and zero APNs sends occur.
        """
        user_pref = "019dbb34-f439-77bd-afa8-ee5321d60307"
        user_beta = "019dbb34-f439-77bd-afa8-ee5321d60308"
        await _seed_user(repo, user_pref)
        await _seed_user(repo, user_beta)
        dev_pref = await _seed_device(repo, user_pref, device_token="a" * 64)
        dev_beta = await _seed_device(repo, user_beta, device_token="b" * 64)
        evt_pref = await _seed_alert_event(repo, user_pref)
        evt_beta = await _seed_alert_event(repo, user_beta)
        pid_pref = await _seed_queued_delivery(
            repo,
            event_public_id=evt_pref["public_id"],
            device_public_id=dev_pref,
            user_public_id=user_pref,
        )
        pid_beta = await _seed_queued_delivery(
            repo,
            event_public_id=evt_beta["public_id"],
            device_public_id=dev_beta,
            user_public_id=user_beta,
        )
        await repo.upsert_device_alert_pref(
            DeviceAlertPrefUpsertRow(
                session_id="seed",
                sequence_id=3,
                timestamp=_ts(),
                device_public_id=dev_pref,
                alert_type="order_fill_full",
                enabled=False,
            )
        )
        sidecar, apns = _make_sidecar(repo)
        sidecar._push_beta_provider = lambda: PushBetaConfig(
            enabled=True, user_public_ids=(user_pref,)
        )

        row_beta = await repo.get_delivery_by_public_id(pid_beta)
        assert row_beta is not None
        _pin_clock(sidecar, _ts(1))
        await sidecar._attempt_on_queued_row(row_beta, _ts(1))

        _pin_clock(sidecar, _ts(2))
        await sidecar._drain_outbox(_ts(2))

        apns.send.assert_not_awaited()
        for pid in (pid_pref, pid_beta):
            final_row = await repo.get_delivery_by_public_id(pid)
            assert final_row is not None
            assert final_row["status"] == "cancelled_scope"
            assert final_row["error_reason"] == "routing_suppressed"

    @pytest.mark.asyncio
    async def test_drain_isolates_unexpected_per_row_exception(
        self,
        repo: SQLAlchemyRepository,
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """An unexpected per-row error during drain logs a warning and processes remaining rows.

        Given: Two queued deliveries where the first raises ``RuntimeError`` inside
            ``_attempt_on_queued_row`` and the second is healthy.
        When: ``_drain_outbox`` runs.
        Then: The error is logged as a warning and the second delivery still succeeds.
        """
        user = "019dbb34-f439-77bd-afa8-ee5321d60307"
        await _seed_user(repo, user)
        device_pid = await _seed_device(repo, user)
        event = await _seed_alert_event(repo, user)
        bad_pid = await _seed_queued_delivery(
            repo,
            event_public_id=event["public_id"],
            device_public_id=device_pid,
            user_public_id=user,
        )
        good_pid = await _seed_queued_delivery(
            repo,
            event_public_id=event["public_id"],
            device_public_id=device_pid,
            user_public_id=user,
        )
        sidecar, apns = _make_sidecar(repo, clock=lambda: _ts(1))
        original = sidecar._attempt_on_queued_row

        async def _boom_on_first(
            row: AlertDeliveryRow,
            now: datetime,
            *,
            caches: _PrefetchedBatchCaches | None = None,
        ) -> None:
            if row["public_id"] == bad_pid:
                raise RuntimeError("transient db glitch")
            await original(row, now, caches=caches)

        monkeypatch.setattr(sidecar, "_attempt_on_queued_row", _boom_on_first)
        handler_id = logger.add(caplog.handler, format="{message}", level="WARNING")
        try:
            await sidecar._drain_outbox(_ts(1))
        finally:
            logger.remove(handler_id)

        good_row = await repo.get_delivery_by_public_id(good_pid)
        assert good_row is not None
        assert good_row["status"] == "sent"
        apns.send.assert_awaited_once()
        assert any("unexpectedly" in r.getMessage() for r in caplog.records)

    @pytest.mark.asyncio
    async def test_attempt_on_queued_row_noop_when_apns_disabled(
        self, repo: SQLAlchemyRepository
    ) -> None:
        """An APNs-disabled sidecar leaves queued APNs rows untouched.

        Given: A queued APNs delivery and a ``NotifySidecar`` constructed with ``apns=None``.
        When: ``_attempt_on_queued_row`` runs.
        Then: The delivery remains ``queued``.
        """
        user = "019dbb34-f439-77bd-afa8-ee5321d60307"
        await _seed_user(repo, user)
        device_pid = await _seed_device(repo, user)
        event = await _seed_alert_event(repo, user)
        pid = await _seed_queued_delivery(
            repo,
            event_public_id=event["public_id"],
            device_public_id=device_pid,
            user_public_id=user,
        )
        row = await repo.get_delivery_by_public_id(pid)
        assert row is not None
        sidecar = NotifySidecar(
            MagicMock(),
            repo,
            None,
            "",
            SequenceTracker(),
            cast(MessagePublisher, _FakeMessagePublisher()),
        )

        _pin_clock(sidecar, _ts(1))
        await sidecar._attempt_on_queued_row(row, _ts(1))

        after = await repo.get_delivery_by_public_id(pid)
        assert after is not None
        assert after["status"] == "queued"

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "exc",
        [
            FileNotFoundError(errno.ENOENT, "missing key"),
            PermissionError(errno.EACCES, "unreadable key"),
        ],
        ids=["file-not-found", "permission-error"],
    )
    async def test_filesystem_os_errors_stay_on_three_attempt_path(
        self, repo: SQLAlchemyRepository, exc: OSError
    ) -> None:
        """Filesystem ``OSError`` subclasses give up after 3 attempts.

        Given: A queued APNs delivery encountering ``FileNotFoundError`` or
            ``PermissionError`` on every attempt.
        When: Three consecutive attempts run inside the 6 h TTL.
        Then: The delivery transitions to terminal ``failed`` at attempt 3.
        """
        user = "019dbb34-f439-77bd-afa8-ee5321d60307"
        await _seed_user(repo, user)
        device_pid = await _seed_device(repo, user)
        event = await _seed_alert_event(repo, user)
        pid = await _seed_queued_delivery(
            repo,
            event_public_id=event["public_id"],
            device_public_id=device_pid,
            user_public_id=user,
        )
        sidecar, _ = _make_sidecar(repo, send_result=exc)
        device = (await repo.list_active_notification_devices_for_user(user))[0]

        for minute in (0, 1, 2):
            _pin_clock(sidecar, _ts(minute))
            await sidecar._attempt_once(pid, device, event, _ts(minute))

        after = await repo.get_delivery_by_public_id(pid)
        assert after is not None
        assert after["status"] == "failed"
        assert after["attempt_count"] == 3

    @pytest.mark.asyncio
    async def test_recovery_expedites_future_outage_backlog_for_next_tick(
        self,
        repo: SQLAlchemyRepository,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Recovery sets ``next_attempt_at = now`` on still-queued outage rows.

        Given: Outage-delayed rows where one future-scheduled row is queued at
            ``+15 min``, one already-ready row has ``next_attempt_at <= now``,
            one row already transitioned to ``failed``, and one row has a future
            ``next_attempt_at`` with ``error_reason=None``.
        When: Another delivery succeeds and triggers ``_maybe_log_outage_recovery``.
        Then: Still-queued future rows are SCD2-rescheduled to ``next_attempt_at == now``
            without altering ``attempt_count`` or terminal rows, and one retry-loop
            tick drains them immediately without waiting 15 minutes.
        """
        user = "019dbb34-f439-77bd-afa8-ee5321d60307"
        await _seed_user(repo, user)
        device_pid = await _seed_device(repo, user)
        event = await _seed_alert_event(repo, user)
        device = (await repo.list_active_notification_devices_for_user(user))[0]

        recovery_now = datetime.now(UTC)
        setup_now = recovery_now - timedelta(minutes=10)
        delayed_future_pid = await repo.insert_alert_delivery(
            AlertDeliveryInsertRow(
                session_id="t",
                sequence_id=1,
                timestamp=setup_now,
                alert_event_public_id=event["public_id"],
                device_public_id=device_pid,
                user_public_id=user,
                status="queued",
                created_at=setup_now,
            )
        )
        delayed_ready_pid = await repo.insert_alert_delivery(
            AlertDeliveryInsertRow(
                session_id="t",
                sequence_id=2,
                timestamp=setup_now,
                alert_event_public_id=event["public_id"],
                device_public_id=device_pid,
                user_public_id=user,
                status="queued",
                created_at=setup_now,
            )
        )
        delayed_terminal_pid = await repo.insert_alert_delivery(
            AlertDeliveryInsertRow(
                session_id="t",
                sequence_id=3,
                timestamp=setup_now,
                alert_event_public_id=event["public_id"],
                device_public_id=device_pid,
                user_public_id=user,
                status="queued",
                created_at=setup_now,
            )
        )
        delayed_null_reason_pid = await repo.insert_alert_delivery(
            AlertDeliveryInsertRow(
                session_id="t",
                sequence_id=4,
                timestamp=setup_now,
                alert_event_public_id=event["public_id"],
                device_public_id=device_pid,
                user_public_id=user,
                status="queued",
                created_at=setup_now,
            )
        )
        trigger_pid = await repo.insert_alert_delivery(
            AlertDeliveryInsertRow(
                session_id="t",
                sequence_id=5,
                timestamp=setup_now,
                alert_event_public_id=event["public_id"],
                device_public_id=device_pid,
                user_public_id=user,
                status="queued",
                created_at=setup_now,
            )
        )

        sidecar, apns = _make_sidecar(
            repo, send_result=MaxAttemptsExceeded(), clock=lambda: setup_now
        )
        for pid in (delayed_future_pid, delayed_ready_pid, delayed_terminal_pid, trigger_pid):
            await sidecar._attempt_once(pid, device, event, setup_now)

        future_next = recovery_now + timedelta(minutes=15)
        await repo.update_delivery_retry_schedule(
            delayed_future_pid,
            attempt_count=1,
            next_attempt_at=future_next,
            error_reason="exception: MaxAttemptsExceeded",
            transition_at=recovery_now - timedelta(minutes=1),
            session_id="prep",
            sequence_id=10,
        )
        await repo.update_delivery_retry_schedule(
            delayed_ready_pid,
            attempt_count=1,
            next_attempt_at=recovery_now - timedelta(seconds=5),
            error_reason="exception: MaxAttemptsExceeded",
            transition_at=recovery_now - timedelta(minutes=1),
            session_id="prep",
            sequence_id=11,
        )
        await repo.mark_delivery_failed(
            delayed_terminal_pid,
            error_reason="expired during outage",
            transition_at=recovery_now - timedelta(seconds=1),
            session_id="prep",
            sequence_id=12,
        )
        await repo.update_delivery_retry_schedule(
            delayed_null_reason_pid,
            attempt_count=0,
            next_attempt_at=future_next,
            error_reason=None,
            transition_at=recovery_now - timedelta(minutes=1),
            session_id="prep",
            sequence_id=13,
        )
        sidecar._record_outage_delay(
            "sandbox",
            delayed_null_reason_pid,
            _OutageDelay(0, setup_now + timedelta(hours=6), setup_now, 0),
        )

        apns.send = AsyncMock(
            return_value=ApnsSendResult(
                status_code=200, status="success", apns_id="apns-ok", description=""
            )
        )
        sidecar._clock = lambda: datetime.now(UTC)
        await sidecar._attempt_once(trigger_pid, device, event, recovery_now)
        after_recovery = datetime.now(UTC)

        expedited = await repo.get_delivery_by_public_id(delayed_future_pid)
        assert expedited is not None
        assert expedited["status"] == "queued"
        assert expedited["attempt_count"] == 1
        assert expedited["next_attempt_at"] is not None
        assert recovery_now <= expedited["next_attempt_at"] <= after_recovery

        expedited_null = await repo.get_delivery_by_public_id(delayed_null_reason_pid)
        assert expedited_null is not None
        assert expedited_null["next_attempt_at"] is not None
        assert recovery_now <= expedited_null["next_attempt_at"] <= after_recovery
        assert expedited_null["error_reason"] is None

        still_ready = await repo.get_delivery_by_public_id(delayed_ready_pid)
        assert still_ready is not None
        assert still_ready["next_attempt_at"] == recovery_now - timedelta(seconds=5)

        monkeypatch.setattr("snapper.application.notify.sidecar._RETRY_LOOP_INTERVAL_S", 0.03)
        task = asyncio.create_task(sidecar._process_retry_queue_loop())
        await asyncio.sleep(0.12)
        await sidecar.stop()
        await asyncio.wait_for(task, timeout=2.0)

        assert await repo.list_queued_deliveries_all() == []
        assert await repo.count_deliveries_by_status() == {"sent": 4, "failed": 1}


class TestReviewRegressionFixes:
    """Targeted regression tests for deadline freshness, SCD2 recovery, routing, and drain."""

    @pytest.mark.asyncio
    async def test_legacy_null_expires_at_expired_before_send_never_calls_apns(
        self, repo: SQLAlchemyRepository
    ) -> None:
        """Legacy rows without ``expires_at`` expire at ``created_at + 6 h`` before sending.

        Given: A queued legacy delivery with ``expires_at=None`` and a prior
            error reason created at ``_ts(0)``.
        When: ``_attempt_once`` and ``_attempt_on_queued_row`` run at ``_ts(360)``.
        Then: Neither invokes ``apns.send``, and both transition to ``failed``
            preserving the prior error reason in ``expired during outage: ...``.
        """
        user = "019dbb34-f439-77bd-afa8-ee5321d60307"
        await _seed_user(repo, user)
        device_pid = await _seed_device(repo, user)
        event = await _seed_alert_event(repo, user)
        device = (await repo.list_active_notification_devices_for_user(user))[0]

        pid_queued = await _seed_queued_delivery(
            repo,
            event_public_id=event["public_id"],
            device_public_id=device_pid,
            user_public_id=user,
        )
        await repo.update_delivery_retry_schedule(
            pid_queued,
            attempt_count=0,
            next_attempt_at=_ts(10),
            error_reason="exception: TimeoutError",
            transition_at=_ts(1),
            session_id="prep",
            sequence_id=1,
        )
        pid_direct = await _seed_queued_delivery(
            repo,
            event_public_id=event["public_id"],
            device_public_id=device_pid,
            user_public_id=user,
        )

        sidecar, apns = _make_sidecar(repo, clock=lambda: _ts(360))
        row_queued = await repo.get_delivery_by_public_id(pid_queued)
        assert row_queued is not None
        assert row_queued.get("expires_at") is None
        await sidecar._attempt_on_queued_row(row_queued, _ts(360))
        await sidecar._attempt_once(pid_direct, device, event, _ts(360))

        apns.send.assert_not_awaited()
        after_queued = await repo.get_delivery_by_public_id(pid_queued)
        after_direct = await repo.get_delivery_by_public_id(pid_direct)
        assert after_queued is not None and after_queued["status"] == "failed"
        assert after_queued["error_reason"] == "expired during outage: exception: TimeoutError"
        assert after_direct is not None and after_direct["status"] == "failed"
        assert after_direct["error_reason"] == "expired during outage"

    @pytest.mark.asyncio
    async def test_row_expiring_during_earlier_send_in_tick_expires_without_sending(
        self, repo: SQLAlchemyRepository, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A delivery whose TTL elapses during an earlier row's slow send is not sent.

        Given: Two queued rows ready at ``_ts(0)`` where row 1 takes 40 s to send
            and row 2 expires at ``_ts(0) + 20 s``.
        When: ``_process_retry_queue_loop`` executes one tick.
        Then: Only row 1 calls ``apns.send``, while row 2 checks fresh UTC and
            expires to ``failed`` without calling ``apns.send``.
        """
        user = "019dbb34-f439-77bd-afa8-ee5321d60307"
        await _seed_user(repo, user)
        device_pid = await _seed_device(repo, user)
        event = await _seed_alert_event(repo, user)
        pid_one = await repo.insert_alert_delivery(
            AlertDeliveryInsertRow(
                session_id="t",
                sequence_id=1,
                timestamp=_ts(0),
                alert_event_public_id=event["public_id"],
                device_public_id=device_pid,
                user_public_id=user,
                status="queued",
                next_attempt_at=_ts(0) - timedelta(seconds=10),
                expires_at=_ts(60),
                created_at=_ts(0),
            )
        )
        pid_two = await repo.insert_alert_delivery(
            AlertDeliveryInsertRow(
                session_id="t",
                sequence_id=2,
                timestamp=_ts(0),
                alert_event_public_id=event["public_id"],
                device_public_id=device_pid,
                user_public_id=user,
                status="queued",
                next_attempt_at=_ts(0) - timedelta(seconds=5),
                expires_at=_ts(0) + timedelta(seconds=20),
                created_at=_ts(0),
            )
        )

        current_time = _ts(0)
        sidecar, apns = _make_sidecar(repo, clock=lambda: current_time)

        async def _slow_send(**_kwargs: object) -> ApnsSendResult:
            nonlocal current_time
            current_time = _ts(0) + timedelta(seconds=40)
            return ApnsSendResult(
                status_code=200, status="success", apns_id="apns-1", description=""
            )

        apns.send = AsyncMock(side_effect=_slow_send)
        original_safe = sidecar._safe_attempt_on_queued_row

        async def _stop_after_second(
            row: AlertDeliveryRow,
            now: datetime,
            *,
            caches: _PrefetchedBatchCaches | None = None,
        ) -> None:
            await original_safe(row, now, caches=caches)
            if row["public_id"] == pid_two:
                await sidecar.stop()

        monkeypatch.setattr(sidecar, "_safe_attempt_on_queued_row", _stop_after_second)
        monkeypatch.setattr("snapper.application.notify.sidecar._RETRY_LOOP_INTERVAL_S", 0.01)

        await asyncio.wait_for(sidecar._process_retry_queue_loop(), timeout=2.0)

        apns.send.assert_awaited_once()
        row_one = await repo.get_delivery_by_public_id(pid_one)
        row_two = await repo.get_delivery_by_public_id(pid_two)
        assert row_one is not None and row_one["status"] == "sent"
        assert row_two is not None and row_two["status"] == "failed"
        assert row_two["error_reason"] == "expired during outage"

    @pytest.mark.asyncio
    async def test_row_expiring_during_bump_expires_at_send_admission_without_sending(
        self, repo: SQLAlchemyRepository
    ) -> None:
        """A delivery expiring between attempt bump and send admission does not send.

        Given: A queued delivery whose ``expires_at`` is ``_ts(5)`` and a clock
            returning ``_ts(4)`` at admission/bump then ``_ts(6)`` at send check.
        When: ``_attempt_once`` runs.
        Then: ``apns.send`` is not called and the delivery fails as expired.
        """
        user = "019dbb34-f439-77bd-afa8-ee5321d60307"
        await _seed_user(repo, user)
        device_pid = await _seed_device(repo, user)
        event = await _seed_alert_event(repo, user)
        device = (await repo.list_active_notification_devices_for_user(user))[0]
        pid = await repo.insert_alert_delivery(
            AlertDeliveryInsertRow(
                session_id="t",
                sequence_id=1,
                timestamp=_ts(0),
                alert_event_public_id=event["public_id"],
                device_public_id=device_pid,
                user_public_id=user,
                status="queued",
                expires_at=_ts(5),
                created_at=_ts(0),
            )
        )
        ticks = iter([_ts(4), _ts(4), _ts(6), _ts(6)])
        sidecar, apns = _make_sidecar(repo, clock=lambda: next(ticks))

        await sidecar._attempt_once(pid, device, event, _ts(4))

        apns.send.assert_not_awaited()
        final_row = await repo.get_delivery_by_public_id(pid)
        assert final_row is not None
        assert final_row["status"] == "failed"
        assert final_row["attempt_count"] == 1
        assert final_row["error_reason"] == "expired during outage"

    @pytest.mark.asyncio
    async def test_failure_backoff_and_transition_at_computed_from_fresh_failure_utc(
        self, repo: SQLAlchemyRepository
    ) -> None:
        """Backoff and ``next_attempt_at`` use fresh UTC captured after send failure.

        Given: A queued delivery whose send attempt starts at ``_ts(0)`` and
            fails with ``MaxAttemptsExceeded`` after 25 s.
        When: ``_attempt_once`` handles the failure.
        Then: ``last_attempt_at`` and ``timestamp`` record ``_ts(0) + 25 s``,
            and ``next_attempt_at`` adds the 30 s backoff to that failure time.
        """
        user = "019dbb34-f439-77bd-afa8-ee5321d60307"
        await _seed_user(repo, user)
        device_pid = await _seed_device(repo, user)
        event = await _seed_alert_event(repo, user)
        device = (await repo.list_active_notification_devices_for_user(user))[0]
        pid = await _seed_queued_delivery(
            repo,
            event_public_id=event["public_id"],
            device_public_id=device_pid,
            user_public_id=user,
        )

        current_time = _ts(0)
        sidecar, apns = _make_sidecar(repo, clock=lambda: current_time)

        async def _slow_failure(**_kwargs: object) -> ApnsSendResult:
            nonlocal current_time
            current_time = _ts(0) + timedelta(seconds=25)
            raise MaxAttemptsExceeded()

        apns.send = AsyncMock(side_effect=_slow_failure)

        await sidecar._attempt_once(pid, device, event, _ts(0))

        row = await repo.get_delivery_by_public_id(pid)
        assert row is not None
        expected_failure_at = _ts(0) + timedelta(seconds=25)
        assert row["last_attempt_at"] == expected_failure_at
        assert row["timestamp"] == expected_failure_at
        assert row["next_attempt_at"] == expected_failure_at + timedelta(seconds=30)

    @pytest.mark.asyncio
    async def test_network_failure_crossing_deadline_during_send_marks_expired(
        self, repo: SQLAlchemyRepository
    ) -> None:
        """A delivery whose TTL expires while ``apns.send`` is in flight fails as expired.

        Given: A queued delivery with ``expires_at = _ts(0) + 10 s`` that starts
            sending at ``_ts(0)`` before expiry and raises ``TimeoutError`` at
            ``_ts(0) + 30 s``.
        When: ``_attempt_once`` processes the post-send network exception.
        Then: Because fresh failure UTC is past ``expires_at``, it transitions
            directly to ``failed`` with ``expired during outage: exception: TimeoutError``.
        """
        user = "019dbb34-f439-77bd-afa8-ee5321d60307"
        await _seed_user(repo, user)
        device_pid = await _seed_device(repo, user)
        event = await _seed_alert_event(repo, user)
        device = (await repo.list_active_notification_devices_for_user(user))[0]
        pid = await repo.insert_alert_delivery(
            AlertDeliveryInsertRow(
                session_id="t",
                sequence_id=1,
                timestamp=_ts(0),
                alert_event_public_id=event["public_id"],
                device_public_id=device_pid,
                user_public_id=user,
                status="queued",
                expires_at=_ts(0) + timedelta(seconds=10),
                created_at=_ts(0),
            )
        )

        current_time = _ts(0)
        sidecar, apns = _make_sidecar(repo, clock=lambda: current_time)

        async def _timeout_past_expiry(**_kwargs: object) -> ApnsSendResult:
            nonlocal current_time
            current_time = _ts(0) + timedelta(seconds=30)
            raise TimeoutError("apns send timed out")

        apns.send = AsyncMock(side_effect=_timeout_past_expiry)

        await sidecar._attempt_once(pid, device, event, _ts(0))

        final_row = await repo.get_delivery_by_public_id(pid)
        assert final_row is not None
        assert final_row["status"] == "failed"
        assert (
            final_row["error_reason"]
            == "expired during outage: exception: TimeoutError: apns send timed out"
        )

    @pytest.mark.asyncio
    async def test_recovery_expedites_retry_committed_after_success(
        self, repo: SQLAlchemyRepository, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A success during a pending retry write still makes that delivery due.

        Given: A network failure waiting to persist backoff at the delivery's TTL.
        When: A concurrent delivery succeeds in the same APNs environment before that write.
        Then: Recovery sees the committed retry and makes it due before the TTL.
        """
        user = "019dbb34-f439-77bd-afa8-ee5321d60307"
        await _seed_user(repo, user)
        device_pid = await _seed_device(repo, user)
        event = await _seed_alert_event(repo, user)
        device = (await repo.list_active_notification_devices_for_user(user))[0]
        deadline = _ts() + timedelta(seconds=20)
        delayed_pid = await repo.insert_alert_delivery(
            AlertDeliveryInsertRow(
                session_id="t",
                sequence_id=1,
                timestamp=_ts(),
                alert_event_public_id=event["public_id"],
                device_public_id=device_pid,
                user_public_id=user,
                status="queued",
                created_at=_ts(),
                expires_at=deadline,
            )
        )
        trigger_pid = await _seed_queued_delivery(
            repo,
            event_public_id=event["public_id"],
            device_public_id=device_pid,
            user_public_id=user,
        )
        sidecar, apns = _make_sidecar(repo, clock=_ts)
        apns.send = AsyncMock(side_effect=[MaxAttemptsExceeded(), apns.send.return_value])
        write_started = asyncio.Event()
        allow_write = asyncio.Event()
        recovery_started = asyncio.Event()
        persist = sidecar._persist_retry_schedule
        recover = sidecar._maybe_log_outage_recovery
        get_delivery = repo.get_delivery_by_public_id

        async def pause_first_write(
            pid: str,
            attempt_number: int,
            next_attempt_at: datetime,
            error_reason: str | None,
            now: datetime,
        ) -> bool:
            """Pause the original outage write while the other delivery succeeds."""
            if not write_started.is_set():
                assert next_attempt_at == deadline
                write_started.set()
                await allow_write.wait()
            return await persist(pid, attempt_number, next_attempt_at, error_reason, now)

        async def observe_recovery(env: str, pid: str, now: datetime, sequence: int) -> None:
            """Signal that the successful delivery reached the recovery path."""
            recovery_started.set()
            await recover(env, pid, now, sequence)

        monkeypatch.setattr(sidecar, "_persist_retry_schedule", pause_first_write)
        monkeypatch.setattr(sidecar, "_maybe_log_outage_recovery", observe_recovery)

        recovered_at = _ts() + timedelta(seconds=5)
        async with asyncio.timeout(10), asyncio.TaskGroup() as group:
            group.create_task(sidecar._attempt_once(delayed_pid, device, event, _ts()))
            await write_started.wait()
            pending = await get_delivery(delayed_pid)
            assert pending is not None and pending["next_attempt_at"] is None

            async def read_delivery(pid: str) -> AlertDeliveryRow | None:
                """Return the visible pending version before releasing the paused write."""
                if pid == delayed_pid and not allow_write.is_set():
                    return pending
                return await get_delivery(pid)

            monkeypatch.setattr(repo, "get_delivery_by_public_id", read_delivery)
            _pin_clock(sidecar, recovered_at)
            group.create_task(sidecar._attempt_once(trigger_pid, device, event, recovered_at))
            await recovery_started.wait()
            allow_write.set()

        delayed = await get_delivery(delayed_pid)
        assert delayed is not None and delayed["status"] == "queued"
        assert delayed["attempt_count"] == 1
        assert delayed["next_attempt_at"] == recovered_at

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("same_delivery", "backoff_seconds"), [(False, 30), (True, 60)], ids=["different", "same"]
    )
    async def test_recovery_leaves_newer_failures_in_an_existing_window(
        self,
        repo: SQLAlchemyRepository,
        monkeypatch: pytest.MonkeyPatch,
        same_delivery: bool,
        backoff_seconds: int,
    ) -> None:
        """An earlier success expedites only failures it could have recovered.

        Given: An outage window and a successful send with a paused sent-state write.
        When: Another network failure commits its schedule before the success is recorded.
        Then: Any original delay is expedited and the recent delay waits for a later success.
        """
        user = "019dbb34-f439-77bd-afa8-ee5321d60307"
        await _seed_user(repo, user)
        device_pid = await _seed_device(repo, user)
        event = await _seed_alert_event(repo, user)
        device = (await repo.list_active_notification_devices_for_user(user))[0]
        older_pid, success_pid, other_pid, later_success_pid = [
            await _seed_queued_delivery(
                repo,
                event_public_id=event["public_id"],
                device_public_id=device_pid,
                user_public_id=user,
            )
            for _ in range(4)
        ]
        newer_pid = older_pid if same_delivery else other_pid
        sidecar, apns = _make_sidecar(repo, clock=_ts)
        success = apns.send.return_value
        apns.send = AsyncMock(
            side_effect=[MaxAttemptsExceeded(), success, MaxAttemptsExceeded(), success]
        )
        success_waiting = asyncio.Event()
        allow_success = asyncio.Event()
        mark_sent = repo.mark_delivery_sent

        async def pause_first_success(
            public_id: str,
            apns_id: str,
            *,
            transition_at: datetime,
            session_id: str,
            sequence_id: int,
        ) -> None:
            """Hold the first success after APNs responded and before its database write."""
            if public_id == success_pid:
                success_waiting.set()
                await allow_success.wait()
            await mark_sent(
                public_id,
                apns_id,
                transition_at=transition_at,
                session_id=session_id,
                sequence_id=sequence_id,
            )

        monkeypatch.setattr(repo, "mark_delivery_sent", pause_first_success)
        await sidecar._attempt_once(older_pid, device, event, _ts())
        success_at = _ts() + timedelta(seconds=5)
        newer_error_at = _ts() + timedelta(seconds=35 if same_delivery else 10)
        _pin_clock(sidecar, success_at)
        async with asyncio.timeout(10), asyncio.TaskGroup() as group:
            group.create_task(sidecar._attempt_once(success_pid, device, event, success_at))
            await success_waiting.wait()
            _pin_clock(sidecar, newer_error_at)
            await sidecar._attempt_once(newer_pid, device, event, newer_error_at)
            newer_before = await repo.get_delivery_by_public_id(newer_pid)
            assert newer_before is not None
            assert newer_before["next_attempt_at"] == newer_error_at + timedelta(
                seconds=backoff_seconds
            )
            allow_success.set()

        if not same_delivery:
            older = await repo.get_delivery_by_public_id(older_pid)
            assert older is not None and older["next_attempt_at"] == newer_error_at
        assert await repo.get_delivery_by_public_id(newer_pid) == newer_before
        assert set(sidecar._outage_windows["sandbox"].delayed) == {newer_pid}

        later_success_at = newer_error_at + timedelta(seconds=5)
        _pin_clock(sidecar, later_success_at)
        await sidecar._attempt_once(later_success_pid, device, event, later_success_at)

        newer = await repo.get_delivery_by_public_id(newer_pid)
        assert newer is not None and newer["next_attempt_at"] == later_success_at
        assert sidecar._outage_windows == {}

    @pytest.mark.asyncio
    async def test_recovery_preserves_later_attempt_throttle_schedule(
        self, repo: SQLAlchemyRepository
    ) -> None:
        """Recovery cannot shorten backoff from a later HTTP 429.

        Given: An initial network failure followed by a throttled second attempt.
        When: A different delivery succeeds in the same APNs environment.
        Then: Recovery leaves the second attempt's retry version and deadline alone.
        """
        user = "019dbb34-f439-77bd-afa8-ee5321d60307"
        await _seed_user(repo, user)
        device_pid = await _seed_device(repo, user)
        event = await _seed_alert_event(repo, user)
        device = (await repo.list_active_notification_devices_for_user(user))[0]
        delayed_pid, trigger_pid = [
            await _seed_queued_delivery(
                repo,
                event_public_id=event["public_id"],
                device_public_id=device_pid,
                user_public_id=user,
            )
            for _ in range(2)
        ]
        sidecar, apns = _make_sidecar(repo, clock=_ts)
        throttled = ApnsSendResult(
            status_code=429, status="throttled", apns_id="", description="TooManyRequests"
        )
        apns.send = AsyncMock(
            side_effect=[MaxAttemptsExceeded(), throttled, apns.send.return_value]
        )

        await sidecar._attempt_once(delayed_pid, device, event, _ts())
        _pin_clock(sidecar, _ts(1))
        await sidecar._attempt_once(delayed_pid, device, event, _ts(1))
        scheduled = await repo.get_delivery_by_public_id(delayed_pid)
        assert scheduled is not None and scheduled["attempt_count"] == 2
        assert scheduled["next_attempt_at"] == _ts(2)
        assert scheduled["error_reason"] == "TooManyRequests"

        recovered_at = _ts(1) + timedelta(seconds=5)
        _pin_clock(sidecar, recovered_at)
        await sidecar._attempt_once(trigger_pid, device, event, recovered_at)

        assert await repo.get_delivery_by_public_id(delayed_pid) == scheduled

    @pytest.mark.asyncio
    async def test_recovery_bounds_reads_and_keeps_evicted_schedule(
        self, repo: SQLAlchemyRepository, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Recovery skips expired and evicted delays.

        Given: A full backlog that evicted a delivery whose schedule ends at its TTL.
        When: A success closes the outage after one retained delivery expires.
        Then: Recovery reads only the live retained delivery and leaves the evicted schedule.
        """
        monkeypatch.setattr("snapper.application.notify.sidecar._MAX_OUTAGE_EXPEDITE_IDS", 2)
        user = "019dbb34-f439-77bd-afa8-ee5321d60307"
        await _seed_user(repo, user)
        device_pid = await _seed_device(repo, user)
        event = await _seed_alert_event(repo, user)
        device = (await repo.list_active_notification_devices_for_user(user))[0]
        evicted_pid = await _seed_queued_delivery(
            repo,
            event_public_id=event["public_id"],
            device_public_id=device_pid,
            user_public_id=user,
        )
        deadline = _ts() + timedelta(hours=6)
        outage_at = deadline - timedelta(seconds=20)
        recovered_at = deadline - timedelta(seconds=5)
        sidecar, _ = _make_sidecar(repo, send_result=MaxAttemptsExceeded(), clock=lambda: outage_at)
        await sidecar._attempt_once(evicted_pid, device, event, outage_at)
        sidecar._record_outage_delay(
            "sandbox", "expired", _OutageDelay(1, recovered_at, outage_at, 1)
        )
        sidecar._record_outage_delay("sandbox", "retained", _OutageDelay(1, deadline, outage_at, 2))
        assert set(sidecar._outage_windows["sandbox"].delayed) == {"expired", "retained"}

        get_delivery = repo.get_delivery_by_public_id
        lookups = AsyncMock(return_value=None)
        monkeypatch.setattr(repo, "get_delivery_by_public_id", lookups)
        _pin_clock(sidecar, recovered_at)
        await sidecar._maybe_log_outage_recovery("sandbox", "recovered-other", recovered_at, 3)

        lookups.assert_awaited_once_with("retained")
        evicted = await get_delivery(evicted_pid)
        assert evicted is not None and evicted["next_attempt_at"] == deadline

    @pytest.mark.asyncio
    async def test_older_success_leaves_newer_outage_tracked(
        self, repo: SQLAlchemyRepository, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A success sampled before a new outage cannot close that outage.

        Given: An outage recorded after the transport timestamp of a successful send.
        When: The success reaches recovery processing after the new outage starts.
        Then: Its backlog stays tracked without an expedited lookup.
        """
        sidecar, _ = _make_sidecar(repo, clock=lambda: _ts(2))
        delay = _OutageDelay(1, _ts(360), _ts(1), 2)
        sidecar._record_outage_delay("sandbox", "delayed", delay)
        lookups = AsyncMock(return_value=None)
        monkeypatch.setattr(repo, "get_delivery_by_public_id", lookups)

        await sidecar._maybe_log_outage_recovery("sandbox", "older-success", _ts(2), 1)

        lookups.assert_not_awaited()
        window = sidecar._outage_windows["sandbox"]
        assert window.started_at == _ts(1)
        assert window.delayed == {"delayed": delay}

    @pytest.mark.asyncio
    async def test_refused_network_retry_write_is_not_tracked(
        self, repo: SQLAlchemyRepository, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A network retry write that loses its race creates no in-memory delay.

        Given: A network exception and a retry write that refuses the attempted schedule.
        When: The sidecar handles the failed send.
        Then: It tracks no outage delay for the uncommitted schedule.
        """
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
        device = (await repo.list_active_notification_devices_for_user(user))[0]
        sidecar, _ = _make_sidecar(repo, send_result=MaxAttemptsExceeded(), clock=_ts)
        persist = AsyncMock(return_value=False)
        monkeypatch.setattr(sidecar, "_persist_retry_schedule", persist)

        await sidecar._attempt_once(delivery_pid, device, event, _ts())

        persist.assert_awaited_once()
        assert sidecar._outage_windows == {}

    @pytest.mark.asyncio
    @pytest.mark.parametrize("recovery_minute", [3, 1], ids=["advanced", "rolled-back"])
    async def test_expedite_outage_backlog_writes_fresh_utc_after_newer_version(
        self, repo: SQLAlchemyRepository, recovery_minute: int
    ) -> None:
        """Recovery uses fresh UTC for retry eligibility and preserves SCD2 order.

        Given: An outage delay written at ``_ts(2)`` and a later APNs success
            observed with a wall clock rolled back to ``_ts(0)``.
        When: Recovery expedites the row with a fresh clock sample before or after
            the active version timestamp.
        Then: The delivery is immediately due and the new version does not predate
            its predecessor.
        """
        user = "019dbb34-f439-77bd-afa8-ee5321d60307"
        await _seed_user(repo, user)
        device_pid = await _seed_device(repo, user)
        event = await _seed_alert_event(repo, user)
        delayed_pid = await _seed_queued_delivery(
            repo,
            event_public_id=event["public_id"],
            device_public_id=device_pid,
            user_public_id=user,
        )
        await repo.begin_delivery_attempt(
            delayed_pid, transition_at=_ts(1), session_id="s", sequence_id=1
        )
        await repo.update_delivery_retry_schedule(
            delayed_pid,
            attempt_count=1,
            next_attempt_at=_ts(15),
            error_reason="exception: MaxAttemptsExceeded",
            transition_at=_ts(2),
            session_id="s",
            sequence_id=2,
        )
        current_time = _ts(recovery_minute)
        transition_at = max(current_time, _ts(2))
        sidecar, _ = _make_sidecar(repo, clock=lambda: current_time)
        sidecar._record_outage_delay("sandbox", delayed_pid, _OutageDelay(1, _ts(360), _ts(2), 0))

        await sidecar._maybe_log_outage_recovery("sandbox", "recovered-other", _ts(), 1)

        expedited = await repo.get_delivery_by_public_id(delayed_pid)
        assert expedited is not None
        assert expedited["timestamp"] == transition_at
        assert expedited["next_attempt_at"] == current_time

        async with repo.session() as s:
            versions = (
                (
                    await s.execute(
                        select(AlertDelivery)
                        .where(AlertDelivery.public_id == delayed_pid)
                        .order_by(AlertDelivery.id)
                    )
                )
                .scalars()
                .all()
            )
        assert [v.timestamp for v in versions] == [_ts(), _ts(1), _ts(2), transition_at]
        assert [v.known_to for v in versions] == [_ts(1), _ts(2), transition_at, KNOWN_TO_MAX]

    @pytest.mark.asyncio
    async def test_expired_delivery_fails_before_routing_even_with_malformed_timezone(
        self, repo: SQLAlchemyRepository
    ) -> None:
        """Expiry is enforced before routing, and malformed pref timezones do not stall retries.

        Given: A device preference with malformed ``timezone='/Bad/Zone'`` and quiet
            hours covering the evaluation timestamp, plus one expired and one
            non-expired queued APNs delivery.
        When: ``_attempt_on_queued_row`` runs on both rows at ``_ts(10)``.
        Then: The expired row transitions to ``failed`` (not cancelled by routing),
            and the non-expired row evaluates quiet hours in UTC without raising.
        """
        user = "019dbb34-f439-77bd-afa8-ee5321d60307"
        await _seed_user(repo, user)
        device_pid = await _seed_device(repo, user)
        event = await _seed_alert_event(repo, user)
        await repo.upsert_device_alert_pref(
            DeviceAlertPrefUpsertRow(
                session_id="seed",
                sequence_id=3,
                timestamp=_ts(0),
                device_public_id=device_pid,
                alert_type="order_fill_full",
                enabled=True,
                min_priority="medium",
                quiet_hours_start_min=0,
                quiet_hours_end_min=1439,
                timezone="/Bad/Zone",
            )
        )
        expired_pid = await repo.insert_alert_delivery(
            AlertDeliveryInsertRow(
                session_id="t",
                sequence_id=1,
                timestamp=_ts(0),
                alert_event_public_id=event["public_id"],
                device_public_id=device_pid,
                user_public_id=user,
                status="queued",
                expires_at=_ts(5),
                created_at=_ts(0),
            )
        )
        active_pid = await repo.insert_alert_delivery(
            AlertDeliveryInsertRow(
                session_id="t",
                sequence_id=2,
                timestamp=_ts(0),
                alert_event_public_id=event["public_id"],
                device_public_id=device_pid,
                user_public_id=user,
                status="queued",
                expires_at=_ts(60),
                created_at=_ts(0),
            )
        )
        sidecar, apns = _make_sidecar(repo, clock=lambda: _ts(10))

        expired_row = await repo.get_delivery_by_public_id(expired_pid)
        active_row = await repo.get_delivery_by_public_id(active_pid)
        assert expired_row is not None and active_row is not None
        await sidecar._attempt_on_queued_row(expired_row, _ts(10))
        await sidecar._attempt_on_queued_row(active_row, _ts(10))

        apns.send.assert_not_awaited()
        expired_after = await repo.get_delivery_by_public_id(expired_pid)
        active_after = await repo.get_delivery_by_public_id(active_pid)
        assert expired_after is not None and expired_after["status"] == "failed"
        assert expired_after["error_reason"] == "expired during outage"
        assert active_after is not None and active_after["status"] == "cancelled_scope"
        assert active_after["error_reason"] == "routing_suppressed"

    @pytest.mark.asyncio
    async def test_drain_outbox_honors_future_next_attempt_at_and_expires_past_ttl(
        self, repo: SQLAlchemyRepository
    ) -> None:
        """Startup ``_drain_outbox`` respects ``next_attempt_at`` while expiring stale rows.

        Given: Three queued deliveries at ``_ts(5)``: one with ``next_attempt_at``
            in the future and valid TTL, one ready for retry, and one with
            future ``next_attempt_at`` whose ``expires_at`` already passed.
        When: ``_drain_outbox`` runs at ``_ts(5)``.
        Then: The future-backoff row remains ``queued`` untouched, the ready row
            sends and transitions to ``sent``, and the expired row becomes ``failed``.
        """
        user = "019dbb34-f439-77bd-afa8-ee5321d60307"
        await _seed_user(repo, user)
        device_pid = await _seed_device(repo, user)
        event = await _seed_alert_event(repo, user)

        future_pid = await repo.insert_alert_delivery(
            AlertDeliveryInsertRow(
                session_id="t",
                sequence_id=1,
                timestamp=_ts(0),
                alert_event_public_id=event["public_id"],
                device_public_id=device_pid,
                user_public_id=user,
                status="queued",
                next_attempt_at=_ts(15),
                expires_at=_ts(60),
                created_at=_ts(0),
            )
        )
        ready_pid = await repo.insert_alert_delivery(
            AlertDeliveryInsertRow(
                session_id="t",
                sequence_id=2,
                timestamp=_ts(0),
                alert_event_public_id=event["public_id"],
                device_public_id=device_pid,
                user_public_id=user,
                status="queued",
                next_attempt_at=_ts(4),
                expires_at=_ts(60),
                created_at=_ts(0),
            )
        )
        expired_future_next_pid = await repo.insert_alert_delivery(
            AlertDeliveryInsertRow(
                session_id="t",
                sequence_id=3,
                timestamp=_ts(0),
                alert_event_public_id=event["public_id"],
                device_public_id=device_pid,
                user_public_id=user,
                status="queued",
                next_attempt_at=_ts(15),
                expires_at=_ts(3),
                created_at=_ts(0),
            )
        )
        sidecar, apns = _make_sidecar(repo, clock=lambda: _ts(5))

        await sidecar._drain_outbox(_ts(5))

        apns.send.assert_awaited_once()
        future_after = await repo.get_delivery_by_public_id(future_pid)
        ready_after = await repo.get_delivery_by_public_id(ready_pid)
        expired_after = await repo.get_delivery_by_public_id(expired_future_next_pid)
        assert future_after is not None and future_after["status"] == "queued"
        assert ready_after is not None and ready_after["status"] == "sent"
        assert expired_after is not None and expired_after["status"] == "failed"
        assert expired_after["error_reason"] == "expired during outage"

    @pytest.mark.asyncio
    async def test_expired_apns_rows_cleaned_up_when_apns_disabled(
        self, repo: SQLAlchemyRepository
    ) -> None:
        """Expiry cleanup runs in drain even when APNs sending is disabled.

        Given: A ``NotifySidecar`` with ``apns=None`` and two queued APNs rows:
            one expired at ``_ts(2)`` and one valid until ``_ts(60)``.
        When: ``_drain_outbox`` runs at ``_ts(5)``.
        Then: The expired row transitions to ``failed`` while the valid row stays ``queued``.
        """
        user = "019dbb34-f439-77bd-afa8-ee5321d60307"
        await _seed_user(repo, user)
        device_pid = await _seed_device(repo, user)
        event = await _seed_alert_event(repo, user)
        expired_pid = await repo.insert_alert_delivery(
            AlertDeliveryInsertRow(
                session_id="t",
                sequence_id=1,
                timestamp=_ts(0),
                alert_event_public_id=event["public_id"],
                device_public_id=device_pid,
                user_public_id=user,
                status="queued",
                expires_at=_ts(2),
                created_at=_ts(0),
            )
        )
        valid_pid = await repo.insert_alert_delivery(
            AlertDeliveryInsertRow(
                session_id="t",
                sequence_id=2,
                timestamp=_ts(0),
                alert_event_public_id=event["public_id"],
                device_public_id=device_pid,
                user_public_id=user,
                status="queued",
                expires_at=_ts(60),
                created_at=_ts(0),
            )
        )
        sidecar = NotifySidecar(
            MagicMock(),
            repo,
            None,
            "",
            SequenceTracker(),
            cast(MessagePublisher, _FakeMessagePublisher()),
        )
        _pin_clock(sidecar, _ts(5))

        await sidecar._drain_outbox(_ts(5))

        expired_after = await repo.get_delivery_by_public_id(expired_pid)
        valid_after = await repo.get_delivery_by_public_id(valid_pid)
        assert expired_after is not None and expired_after["status"] == "failed"
        assert expired_after["error_reason"] == "expired during outage"
        assert valid_after is not None and valid_after["status"] == "queued"

    @pytest.mark.asyncio
    async def test_attempt_delivery_expires_when_clock_crosses_deadline_during_scope_check(
        self, repo: SQLAlchemyRepository
    ) -> None:
        """Clock crossing ``expires_at`` during the post-bump scope read prevents APNs send.

        Given: A queued delivery expiring at ``_ts(10)`` and a clock that reads
            ``_ts(9)`` at pre-bump, bump, and post-bump scope check start, then
            advances to ``_ts(11)`` while the post-bump ``should_skip_send`` awaits.
        When: ``_attempt_once`` executes.
        Then: Fresh UTC sampled after the post-bump scope read sees
            ``_ts(11) >= _ts(10)``, transitions the delivery to ``failed``
            (``expired during outage``), and never calls ``apns.send``.
        """
        user = "019dbb34-f439-77bd-afa8-ee5321d60307"
        await _seed_user(repo, user)
        device_pid = await _seed_device(repo, user)
        event = await _seed_alert_event(repo, user)
        delivery_pid = await repo.insert_alert_delivery(
            AlertDeliveryInsertRow(
                session_id="t",
                sequence_id=1,
                timestamp=_ts(0),
                alert_event_public_id=event["public_id"],
                device_public_id=device_pid,
                user_public_id=user,
                status="queued",
                expires_at=_ts(10),
                created_at=_ts(0),
            )
        )
        device = (await repo.list_active_notification_devices_for_user(user))[0]

        current_now = _ts(9)
        sidecar, apns = _make_sidecar(repo, clock=lambda: current_now)
        scope_calls = 0

        async def _advance_on_second_scope_check(
            _event: AlertEventRow, _repo: Repository, _now: datetime
        ) -> bool:
            nonlocal scope_calls, current_now
            scope_calls += 1
            if scope_calls == 2:
                current_now = _ts(11)
            return False

        sidecar._scope_revalidator = MagicMock()
        sidecar._scope_revalidator.should_skip_send = AsyncMock(
            side_effect=_advance_on_second_scope_check
        )

        await sidecar._attempt_once(delivery_pid, device, event, _ts(9))

        apns.send.assert_not_awaited()
        assert scope_calls == 2
        row = await repo.get_delivery_by_public_id(delivery_pid)
        assert row is not None
        assert row["status"] == "failed"
        assert row["attempt_count"] == 1
        assert row["timestamp"] == _ts(11)
        assert row["error_reason"] == "expired during outage"

    @pytest.mark.asyncio
    async def test_outage_recovery_scoped_per_environment_does_not_expedite_other_env(
        self, repo: SQLAlchemyRepository
    ) -> None:
        """A recovery in ``prod`` does not clear or expedite an active ``sandbox`` outage.

        Given: A queued ``sandbox`` delivery delayed by ``MaxAttemptsExceeded``
            until ``+30s``, a queued ``prod`` delivery, and a queued second
            ``sandbox`` delivery.
        When: The ``prod`` delivery succeeds at ``+10s``, and later the second
            ``sandbox`` delivery succeeds at ``+15s``.
        Then: The ``prod`` success leaves the ``sandbox`` outage window intact
            and the delayed ``sandbox`` row un-expedited at ``+30s``, whereas
            the subsequent ``sandbox`` success clears the ``sandbox`` window
            and expedites its delayed row to ``+15s``.
        """
        user = "019dbb34-f439-77bd-afa8-ee5321d60307"
        await _seed_user(repo, user)
        sandbox_dev_pid = await _seed_device(repo, user, device_token="a" * 64, env="sandbox")
        prod_dev_pid = await repo.upsert_notification_device(
            NotificationDeviceUpsertRow(
                session_id="seed",
                sequence_id=2,
                timestamp=_ts(0),
                user_public_id=user,
                device_token="b" * 64,
                device_id="dev-prod",
                env="prod",
                registered_at=_ts(0),
            )
        )
        devices = {
            row["env"]: row for row in await repo.list_active_notification_devices_for_user(user)
        }
        event = await _seed_alert_event(repo, user)
        sandbox_delayed_pid = await _seed_queued_delivery(
            repo,
            event_public_id=event["public_id"],
            device_public_id=sandbox_dev_pid,
            user_public_id=user,
        )
        prod_ok_pid = await _seed_queued_delivery(
            repo,
            event_public_id=event["public_id"],
            device_public_id=prod_dev_pid,
            user_public_id=user,
        )
        sandbox_recovery_pid = await _seed_queued_delivery(
            repo,
            event_public_id=event["public_id"],
            device_public_id=sandbox_dev_pid,
            user_public_id=user,
        )

        current_now = _ts(0)
        sidecar, apns = _make_sidecar(
            repo,
            send_result=MaxAttemptsExceeded(),
            clock=lambda: current_now,
        )
        await sidecar._attempt_once(sandbox_delayed_pid, devices["sandbox"], event, current_now)

        delayed_after_outage = await repo.get_delivery_by_public_id(sandbox_delayed_pid)
        assert delayed_after_outage is not None
        assert delayed_after_outage["next_attempt_at"] == _ts(0) + timedelta(seconds=30)
        assert list(sidecar._outage_windows) == ["sandbox"]

        current_now = _ts(0) + timedelta(seconds=10)
        apns.send = AsyncMock(
            return_value=ApnsSendResult(
                status_code=200, status="success", apns_id="apns-prod-ok", description=""
            )
        )
        await sidecar._attempt_once(prod_ok_pid, devices["prod"], event, current_now)

        still_delayed = await repo.get_delivery_by_public_id(sandbox_delayed_pid)
        assert still_delayed is not None
        assert still_delayed["next_attempt_at"] == _ts(0) + timedelta(seconds=30)
        assert "sandbox" in sidecar._outage_windows
        assert "prod" not in sidecar._outage_windows

        current_now = _ts(0) + timedelta(seconds=15)
        await sidecar._attempt_once(sandbox_recovery_pid, devices["sandbox"], event, current_now)

        expedited = await repo.get_delivery_by_public_id(sandbox_delayed_pid)
        assert expedited is not None
        assert expedited["next_attempt_at"] == current_now
        assert sidecar._outage_windows == {}

    @pytest.mark.asyncio
    async def test_apns_rejected_schedule_reapplies_once_on_stale_clock_rollback(
        self, repo: SQLAlchemyRepository
    ) -> None:
        """A stale-timestamp schedule refusal re-applies once while unscheduled on same attempt.

        Given: A queued APNs delivery whose active version bumped to
            ``attempt_count == 1`` at ``_ts(10)`` with ``next_attempt_at=None``.
        When: ``_persist_retry_schedule`` is called with a rolled-back ``now=_ts(5)``
            while the sidecar clock reads ``_ts(8)``.
        Then: The rejected initial write re-reads the active version, logs at
            INFO, and retries once using ``max(_ts(8), _ts(10)) = _ts(10)``,
            persisting ``next_attempt_at=_ts(15)``.
        """
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
        assert (
            await repo.begin_delivery_attempt(
                delivery_pid, transition_at=_ts(10), session_id="s", sequence_id=1
            )
            == 1
        )
        sidecar, _ = _make_sidecar(repo, clock=lambda: _ts(8))
        info_logs: list[str] = []
        handler_id = logger.add(info_logs.append, level="INFO")
        try:
            await sidecar._persist_retry_schedule(delivery_pid, 1, _ts(15), "server_error", _ts(5))
        finally:
            logger.remove(handler_id)

        updated = await repo.get_delivery_by_public_id(delivery_pid)
        assert updated is not None
        assert updated["status"] == "queued"
        assert updated["attempt_count"] == 1
        assert updated["timestamp"] == _ts(10)
        assert updated["next_attempt_at"] == _ts(15)
        assert updated["error_reason"] == "server_error"
        assert any("reapplying rejected retry schedule" in msg for msg in info_logs)

    @pytest.mark.asyncio
    async def test_apns_rejected_schedule_skips_reapply_on_lost_race(
        self, repo: SQLAlchemyRepository
    ) -> None:
        """A schedule refusal lost to a concurrent attempt logs INFO and does not overwrite.

        Given: A queued APNs delivery originally at ``attempt_count == 1`` whose
            active version has already advanced to ``attempt_count == 2``.
        When: The stale attempt-1 writer calls ``_persist_retry_schedule``.
        Then: Re-reading sees ``attempt_count == 2 != 1``, logs at INFO, skips
            the re-apply write, and leaves ``attempt_count == 2`` with
            ``next_attempt_at=None``.
        """
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
        assert (
            await repo.begin_delivery_attempt(
                delivery_pid, transition_at=_ts(10), session_id="s1", sequence_id=1
            )
            == 1
        )
        assert (
            await repo.begin_delivery_attempt(
                delivery_pid, transition_at=_ts(11), session_id="s2", sequence_id=2
            )
            == 2
        )
        sidecar, _ = _make_sidecar(repo, clock=lambda: _ts(12))
        info_logs: list[str] = []
        handler_id = logger.add(info_logs.append, level="INFO")
        try:
            await sidecar._persist_retry_schedule(delivery_pid, 1, _ts(20), "server_error", _ts(5))
        finally:
            logger.remove(handler_id)

        current = await repo.get_delivery_by_public_id(delivery_pid)
        assert current is not None
        assert current["attempt_count"] == 2
        assert current["next_attempt_at"] is None
        assert any("skipping rejected retry schedule after lost race" in msg for msg in info_logs)
