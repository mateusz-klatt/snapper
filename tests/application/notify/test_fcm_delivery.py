"""Actual SQLite outbox witnesses with an in-process synthetic provider."""

import asyncio
from collections.abc import AsyncGenerator
from collections.abc import Awaitable
from collections.abc import Callable
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from pathlib import Path
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from uuid import uuid4

import pytest
from sqlalchemy import select

from snapper.api.schemas.android_devices import AndroidPushConfiguration
from snapper.api.schemas.android_devices import RegisterAndroidDeviceBody
from snapper.application.notify.fcm_client import FcmClient
from snapper.application.notify.fcm_client import FcmHint
from snapper.application.notify.fcm_client import FcmSendResult
from snapper.application.notify.fcm_delivery import FcmDeliveryService
from snapper.application.notify.push_beta import PushBetaConfig
from snapper.application.notify.scope_revalidation import ScopeRevalidator
from snapper.application.notify.sidecar import NotifySidecar
from snapper.data.android_devices import RegistrationWrite
from snapper.data.android_devices import registration_revision
from snapper.data.models import KNOWN_TO_MAX
from snapper.data.models import AlertDelivery
from snapper.data.models import User
from snapper.data.repository import AndroidConfigurationStore
from snapper.data.repository import AndroidDeviceStore
from snapper.data.repository import SQLAlchemyRepository
from snapper.data.repository_types import AlertDeliveryRow
from snapper.data.repository_types import AlertEventInsertRow
from snapper.data.repository_types import AlertEventRow
from snapper.data.repository_types import NotificationDeviceUpsertRow
from snapper.data.repository_types import UserAlertDefaultUpsertRow
from snapper.messaging.infrastructure.publisher import SequenceTracker

_NOW = datetime(2026, 10, 16, tzinfo=UTC)
_OWNER = str(uuid4())


@pytest.fixture
async def repo(tmp_path: Path) -> AsyncGenerator[SQLAlchemyRepository]:
    """Seed only a synthetic active viewer in an exclusively owned database."""
    repository = SQLAlchemyRepository(f"sqlite+aiosqlite:///{tmp_path / 'fcm.db'}")
    await repository.create_all()
    async with repository.session() as session:
        session.add(
            User(
                public_id=_OWNER,
                username="synthetic",
                password_hash="unused",
                role="viewer",
                is_active=True,
                created_at=_NOW,
                session_id=str(uuid4()),
                sequence_id=1,
                timestamp=_NOW,
                known_to=KNOWN_TO_MAX,
            )
        )
        await session.commit()
    try:
        yield repository
    finally:
        await repository.engine.dispose()


async def _service(repo: SQLAlchemyRepository) -> tuple[FcmDeliveryService, MagicMock]:
    """Activate a project and register a synthetic SDK FID before constructing the sender."""
    configuration = AndroidPushConfiguration(
        configuration_id=str(uuid4()),
        package_name="ch.snapper.android",
        project_id="synthetic",
        application_id="app",
        api_key="public",
        messaging_sender_id="123",
    )
    write = RegistrationWrite(_OWNER, str(uuid4()), 1, _NOW)
    await AndroidConfigurationStore(repo).activate(configuration, None, write)
    await AndroidDeviceStore(repo).register(
        RegisterAndroidDeviceBody(
            protocol="fcm_fid",
            configuration_id=configuration.configuration_id,
            package_name=configuration.package_name,
            installation_id=str(uuid4()),
            binding_id=str(uuid4()),
            expected_revision=registration_revision(None),
            firebase_installation_id="synthetic-fid",
            expected_user_public_id=_OWNER,
        ),
        write,
    )
    sender = MagicMock(spec=FcmClient)
    sender.configuration = configuration
    sender.send = AsyncMock(return_value=FcmSendResult("accepted", "accepted", receipt="receipt"))
    tracker = SequenceTracker()
    service = FcmDeliveryService(repo, sender, tracker, ScopeRevalidator(tracker))
    service.clock = lambda: _NOW + timedelta(seconds=1)
    return service, sender


async def _event(repo: SQLAlchemyRepository) -> AlertEventRow:
    """Create a high-priority event while keeping its private text out of provider calls."""
    public_id = await repo.insert_alert_event(
        AlertEventInsertRow(
            session_id=str(uuid4()),
            sequence_id=1,
            timestamp=_NOW,
            user_public_id=_OWNER,
            alert_type="drift",
            priority="high",
            title="Private synthetic title",
            body="Private synthetic account details",
        )
    )
    event = await repo.get_alert_event_by_public_id(public_id)
    assert event is not None
    return event


async def _delivery(repo: SQLAlchemyRepository) -> AlertDelivery:
    """Read the current durable result without assuming a provider outcome."""
    async with repo.session() as session:
        rows = (
            await session.scalars(select(AlertDelivery).order_by(AlertDelivery.id.desc()))
        ).all()
    assert rows
    return rows[0]


@pytest.mark.asyncio
async def test_accepted_hint_retains_exact_bound_history_and_provider_receipt(
    repo: SQLAlchemyRepository,
) -> None:
    """FCM acceptance persists only a provider receipt and immutable opaque references.

    Given: An active viewer, Android binding and private alert in SQLite.
    When: The synthetic provider accepts the hint.
    Then: SCD2 history retains binding and expiry while recording a separate provider receipt.
    """
    service, sender = await _service(repo)
    event = await _event(repo)
    await service.fanout(event, _NOW, None)
    row = await _delivery(repo)
    assert row.status == "sent" and row.attempt_count == 1
    assert row.provider == "fcm" and row.apns_id is None
    assert row.provider_receipt == "receipt"
    assert row.expires_at == _NOW + timedelta(hours=24)
    args = sender.send.await_args
    assert args is not None
    assert args.args[0] == "synthetic-fid"
    hint = args.args[1]
    assert isinstance(hint, FcmHint)
    assert hint.registration_public_id == row.device_public_id
    assert hint.binding_id == row.binding_id
    assert hint.alert_public_id == event["public_id"]
    assert "Private" not in repr(hint)
    async with repo.session() as session:
        versions = (await session.scalars(select(AlertDelivery))).all()
    assert len(versions) == 3
    assert {version.binding_id for version in versions} == {row.binding_id}
    assert {version.expires_at for version in versions} == {row.expires_at}


@pytest.mark.asyncio
async def test_rotation_during_oauth_prevents_fid_send_and_cancels_outbox(
    repo: SQLAlchemyRepository,
) -> None:
    """A late OAuth response cannot send after startup activated the next configuration.

    Given: A charged hint waiting for OAuth under the original configuration.
    When: A replacement configuration activates before final admission.
    Then: Admission fails and the old outbox row becomes cancelled.
    """
    service, sender = await _service(repo)
    event = await _event(repo)

    async def delayed_send(
        fid: str, hint: FcmHint, admitted: Callable[[], Awaitable[bool]]
    ) -> FcmSendResult:
        """Commit rotation in the gap reserved for acquiring provider authorization."""
        assert fid == "synthetic-fid" and hint.alert_public_id == event["public_id"]
        replacement = service.sender.configuration.model_copy(
            update={"configuration_id": str(uuid4())}
        )
        await AndroidConfigurationStore(repo).activate(
            replacement,
            (await AndroidConfigurationStore(repo).revision()).revision,
            RegistrationWrite(_OWNER, str(uuid4()), 2, _NOW + timedelta(seconds=1)),
        )
        assert not await admitted()
        return FcmSendResult("cancelled", "binding_or_scope_changed")

    sender.send.side_effect = delayed_send
    await service.fanout(event, _NOW, None)
    assert (await _delivery(repo)).status == "cancelled_scope"
    assert await AndroidDeviceStore(repo).list_owned(_OWNER) == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "outcome,expected",
    [
        ("retry", "queued"),
        ("unregistered", "unregistered"),
        ("permanent", "failed"),
        ("expired", "failed"),
    ],
)
async def test_provider_outcomes_preserve_original_expiry(
    repo: SQLAlchemyRepository,
    outcome: str,
    expected: str,
) -> None:
    """Retry schedules do not renew the immutable 24-hour hint lifetime.

    Given: An admitted binding and representative provider outcomes.
    When: The outbox processes one attempt.
    Then: Status and retry timing follow the result without renewing expiry.
    """
    service, sender = await _service(repo)
    results = {
        "retry": FcmSendResult(
            "retry", "provider_unavailable", retry_at=_NOW + timedelta(minutes=4)
        ),
        "unregistered": FcmSendResult("unregistered", "unregistered"),
        "permanent": FcmSendResult("permanent", "provider_rejected"),
        "expired": FcmSendResult("expired", "expired"),
    }
    sender.send.return_value = results[outcome]
    await service.fanout(await _event(repo), _NOW, None)
    row = await _delivery(repo)
    assert row.status == expected
    assert row.expires_at == _NOW + timedelta(hours=24)
    if outcome == "retry":
        assert row.next_attempt_at == _NOW + timedelta(minutes=4)
    if outcome == "unregistered":
        assert await AndroidDeviceStore(repo).list_owned(_OWNER) == []


@pytest.mark.asyncio
async def test_post_claim_database_failures_cannot_exceed_three_provider_attempts(
    repo: SQLAlchemyRepository,
) -> None:
    """Charged attempts survive admission failures and cap the next restart before sending.

    Given: A provider path failing after durable attempt allocation.
    When: Recovery retries the same hint through the attempt budget.
    Then: The fourth admission is refused and only three provider attempts were invoked.
    """
    service, sender = await _service(repo)
    sender.send.side_effect = RuntimeError("synthetic admission database failure")
    with pytest.raises(RuntimeError, match="synthetic admission"):
        await service.fanout(await _event(repo), _NOW, None)
    for number in (2, 3):
        current = await _delivery(repo)
        row = await repo.get_delivery_by_public_id(current.public_id)
        assert row is not None
        with pytest.raises(RuntimeError, match="synthetic admission"):
            await service.attempt(row, _NOW + timedelta(seconds=number))
    current = await _delivery(repo)
    row = await repo.get_delivery_by_public_id(current.public_id)
    assert row is not None and row["attempt_count"] == 3
    await service.attempt(row, _NOW + timedelta(seconds=4))
    assert sender.send.await_count == 3
    assert (await _delivery(repo)).status == "failed"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "change",
    [
        "owner",
        "operator",
        "wallet",
        "expired",
        "missing_event",
        "configuration",
        "retired_binding",
        "quiet_preferences",
        "rollout",
    ],
)
async def test_retry_revalidates_bound_identity_scope_expiry_and_current_policy(
    repo: SQLAlchemyRepository,
    monkeypatch: pytest.MonkeyPatch,
    change: str,
) -> None:
    """A queued hint cannot escape a later authority, expiry or preference change.

    Given: A queued retry whose original provider call was transiently unsuccessful.
    When: One current identity, scope, expiry or policy fact changes.
    Then: The next attempt terminates without another provider call.
    """
    service, sender = await _service(repo)
    sender.send.return_value = FcmSendResult("retry", "provider_unavailable")
    event = await _event(repo)
    await service.fanout(event, _NOW, None)
    current = await _delivery(repo)
    row = await repo.get_delivery_by_public_id(current.public_id)
    assert row is not None
    await _change_admission(service, row, event, monkeypatch, change)
    await service.attempt(row, _NOW + timedelta(seconds=2))
    assert sender.send.await_count == 1
    assert (await _delivery(repo)).status in {"cancelled_scope", "failed"}


async def _change_admission(
    service: FcmDeliveryService,
    row: AlertDeliveryRow,
    event: AlertEventRow,
    monkeypatch: pytest.MonkeyPatch,
    change: str,
) -> None:
    """Change one independent current admission fact after the initial retry was persisted."""
    if change in {"owner", "operator", "wallet"}:
        changed = _changed_scope(event, change)
        monkeypatch.setattr(
            service.repo, "get_alert_event_by_public_id", AsyncMock(return_value=changed)
        )
    elif change == "expired":
        row["expires_at"] = _NOW
    elif change == "missing_event":
        monkeypatch.setattr(
            service.repo, "get_alert_event_by_public_id", AsyncMock(return_value=None)
        )
    elif change == "configuration":
        row["configuration_id"] = str(uuid4())
    elif change == "retired_binding":
        await service.repo.deactivate_notification_device_scd2(
            row["device_public_id"],
            reason="user_unregistered",
            timestamp=_NOW + timedelta(seconds=2),
            session_id=str(uuid4()),
            sequence_id=1,
        )
    elif change == "quiet_preferences":
        await service.repo.upsert_user_alert_default(
            UserAlertDefaultUpsertRow(
                user_public_id=_OWNER,
                alert_type=event["alert_type"],
                enabled=False,
                min_priority="low",
                session_id=str(uuid4()),
                sequence_id=1,
                timestamp=_NOW + timedelta(seconds=2),
            )
        )
    else:
        service.push_beta_provider = lambda: PushBetaConfig(enabled=True)


@pytest.mark.asyncio
async def test_expiring_or_exhausted_retry_is_terminal_without_expiry_renewal(
    repo: SQLAlchemyRepository,
) -> None:
    """A Retry-After beyond the original lifetime never schedules another send.

    Given: A provider Retry-After beyond the original hint lifetime.
    When: The retry result is persisted.
    Then: The delivery fails terminally and keeps its original expiry.
    """
    service, sender = await _service(repo)
    sender.send.return_value = FcmSendResult(
        "retry", "provider_unavailable", retry_at=_NOW + timedelta(days=2)
    )
    await service.fanout(await _event(repo), _NOW, None)
    row = await _delivery(repo)
    assert row.status == "failed"
    assert row.expires_at == _NOW + timedelta(hours=24)


def _changed_scope(event: AlertEventRow, change: str) -> AlertEventRow:
    """Build a corrected event with a different captured authority component."""
    changed = event.copy()
    if change == "owner":
        changed["user_public_id"] = str(uuid4())
    elif change == "operator":
        changed["operator_public_id"] = str(uuid4())
    else:
        changed["wallet_public_id"] = str(uuid4())
    return changed


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "old_outcome", ["cancelled", "unregistered", "permanent", "accepted", "retry"]
)
async def test_obsolete_oauth_admission_cannot_cancel_newer_charged_attempt(
    repo: SQLAlchemyRepository,
    old_outcome: str,
) -> None:
    """A superseded worker's cancelled result leaves the current worker free to send.

    Given: Two charged attempts with held asynchronous provider completions.
    When: The obsolete attempt finishes before the current one.
    Then: Its outcome cannot terminate the newer attempt, which records its own receipt.
    """
    service, sender = await _service(repo)
    event = await _event(repo)
    first_held, second_held, release_first, release_second = (
        asyncio.Event(),
        asyncio.Event(),
        asyncio.Event(),
        asyncio.Event(),
    )
    calls = 0

    async def send(
        fid: str, hint: FcmHint, admitted: Callable[[], Awaitable[bool]]
    ) -> FcmSendResult:
        """Hold both OAuth phases and resolve the obsolete one first."""
        nonlocal calls
        assert fid == "synthetic-fid" and hint.alert_public_id == event["public_id"]
        calls += 1
        if calls == 1:
            first_held.set()
            await release_first.wait()
            assert not await admitted()
            results = {
                "cancelled": FcmSendResult("cancelled", "binding_or_scope_changed"),
                "unregistered": FcmSendResult("unregistered", "unregistered"),
                "permanent": FcmSendResult("permanent", "provider_rejected"),
                "accepted": FcmSendResult("accepted", "accepted", receipt="obsolete"),
                "retry": FcmSendResult("retry", "provider_unavailable"),
            }
            return results[old_outcome]
        second_held.set()
        await release_second.wait()
        assert await admitted()
        return FcmSendResult("accepted", "accepted", receipt="second-attempt")

    sender.send.side_effect = send
    first = asyncio.create_task(service.fanout(event, _NOW, None))
    second: asyncio.Task[None] | None = None
    try:
        await asyncio.wait_for(first_held.wait(), 5)
        current = await _delivery(repo)
        row = await repo.get_delivery_by_public_id(current.public_id)
        assert row is not None
        second = asyncio.create_task(service.attempt(row, _NOW + timedelta(seconds=1)))
        await asyncio.wait_for(second_held.wait(), 5)
        release_first.set()
        await asyncio.wait_for(first, 5)
        pending = await _delivery(repo)
        assert pending.status == "queued" and pending.attempt_count == 2
        release_second.set()
        await asyncio.wait_for(second, 5)
        result = await _delivery(repo)
        assert result.status == "sent" and result.provider_receipt == "second-attempt"
    finally:
        release_first.set()
        release_second.set()
        await asyncio.gather(first, *([] if second is None else [second]), return_exceptions=True)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "condition", ["rollout", "configuration", "foreign_config", "disabled_pref", "missing_readback"]
)
async def test_fanout_refusals_never_dispatch_a_provider_message(
    repo: SQLAlchemyRepository,
    monkeypatch: pytest.MonkeyPatch,
    condition: str,
) -> None:
    """Independent policy and stale-snapshot refusals leave the sender untouched.

    Given: Independent rollout, configuration, preference or readback refusal conditions.
    When: The sidecar attempts to allocate and send a hint.
    Then: No provider message is dispatched.
    """
    service, sender = await _service(repo)
    event = await _event(repo)
    beta = PushBetaConfig(enabled=True) if condition == "rollout" else None
    if condition == "configuration":
        await AndroidConfigurationStore(repo).disable(
            (await AndroidConfigurationStore(repo).revision()).revision,
            RegistrationWrite(_OWNER, str(uuid4()), 1, _NOW + timedelta(seconds=1)),
        )
    elif condition == "foreign_config":
        devices = await AndroidDeviceStore(repo).list_owned(_OWNER)
        foreign = devices[0].model_copy(update={"configuration_id": str(uuid4())})
        monkeypatch.setattr(AndroidDeviceStore, "list_owned", AsyncMock(return_value=[foreign]))
    elif condition == "disabled_pref":
        await repo.upsert_user_alert_default(
            UserAlertDefaultUpsertRow(
                user_public_id=_OWNER,
                alert_type="drift",
                enabled=False,
                min_priority="low",
                session_id=str(uuid4()),
                sequence_id=1,
                timestamp=_NOW,
            )
        )
    elif condition == "missing_readback":
        monkeypatch.setattr(repo, "get_delivery_by_public_id", AsyncMock(return_value=None))
    await service.fanout(event, _NOW, beta)
    sender.send.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("condition", ["terminal", "stale_budget", "retired_after_claim"])
async def test_claim_boundary_rejects_terminal_stale_budget_or_retired_target(
    repo: SQLAlchemyRepository,
    monkeypatch: pytest.MonkeyPatch,
    condition: str,
) -> None:
    """Database changes after an old queue snapshot cannot admit another provider request.

    Given: An old queue snapshot and a later terminal, budget or retirement change.
    When: The old snapshot reaches the durable claim boundary.
    Then: The changed state prevents another provider request.
    """
    service, sender = await _service(repo)
    sender.send.return_value = FcmSendResult("retry", "provider_unavailable")
    await service.fanout(await _event(repo), _NOW, None)
    current = await _delivery(repo)
    row = await repo.get_delivery_by_public_id(current.public_id)
    assert row is not None
    if condition == "terminal":
        await repo.mark_delivery_failed(
            row["public_id"],
            "synthetic",
            transition_at=_NOW,
            session_id=str(uuid4()),
            sequence_id=1,
        )
    elif condition == "stale_budget":
        for _ in range(2):
            await repo.begin_delivery_attempt(
                row["public_id"], transition_at=_NOW, session_id=str(uuid4()), sequence_id=1
            )
    else:

        async def retiring_claim(
            public_id: str, *, transition_at: datetime, session_id: str, sequence_id: int
        ) -> int | None:
            """Retire the exact registration after charging the attempt."""
            count = await SQLAlchemyRepository.begin_delivery_attempt(
                repo,
                public_id,
                transition_at=transition_at,
                session_id=session_id,
                sequence_id=sequence_id,
            )
            await repo.deactivate_notification_device_scd2(
                row["device_public_id"],
                reason="user_unregistered",
                timestamp=transition_at,
                session_id=session_id,
                sequence_id=sequence_id,
            )
            assert count is None or isinstance(count, int)
            return count

        monkeypatch.setattr(repo, "begin_delivery_attempt", retiring_claim)
    service.push_beta_provider = lambda: PushBetaConfig()
    await service.attempt(row, _NOW + timedelta(seconds=3))
    assert sender.send.await_count == 1


@pytest.mark.asyncio
async def test_scope_cancellation_preserves_bound_android_delivery_metadata(
    repo: SQLAlchemyRepository,
) -> None:
    """The shared scope-event cancellation path retains configuration/binding and original expiry.

    Given: A queued Android delivery with immutable identity and expiry.
    When: The shared user-scope cancellation path runs.
    Then: The cancelled successor preserves provider, configuration, binding and expiry.
    """
    service, sender = await _service(repo)
    sender.send.return_value = FcmSendResult("retry", "provider_unavailable")
    await service.fanout(await _event(repo), _NOW, None)
    before = await _delivery(repo)
    await repo.cancel_pending_deliveries_for_user(
        _OWNER,
        transition_at=_NOW + timedelta(seconds=2),
        session_id=str(uuid4()),
        sequence_id=1,
    )
    after = await _delivery(repo)
    assert after.status == "cancelled_scope"
    assert (after.configuration_id, after.binding_id, after.expires_at, after.provider) == (
        before.configuration_id,
        before.binding_id,
        before.expires_at,
        "fcm",
    )


@pytest.mark.asyncio
async def test_real_sidecar_fcm_only_fanout_and_retry_never_enter_apns(
    repo: SQLAlchemyRepository,
) -> None:
    """Actual sidecar routing admits FCM independently and preserves queued APNs when disabled.

    Given: A real sidecar with an injected Android sender and APNs disabled.
    When: It fans out an alert, retries the FCM row and encounters APNs work.
    Then: FCM completes independently while APNs remains untouched.
    """
    service, sender = await _service(repo)
    sender.send.return_value = FcmSendResult("retry", "provider_unavailable")
    publisher = MagicMock(tracker=service.tracker, send=AsyncMock())
    sidecar = NotifySidecar(MagicMock(), repo, None, "", service.tracker, publisher)
    sidecar.configure_android(sender)
    assert sidecar._android is not None
    sidecar._android.clock = service.clock
    await sidecar._persist_and_fanout_row(
        AlertEventInsertRow(
            user_public_id=_OWNER,
            alert_type="drift",
            priority="high",
            title="Synthetic",
            body="Private",
            dedup_key="synthetic-drift",
        ),
        _NOW,
    )
    current = await _delivery(repo)
    assert current.status == "queued" and current.provider == "fcm"
    row = await repo.get_delivery_by_public_id(current.public_id)
    assert row is not None
    unconfigured = NotifySidecar(MagicMock(), repo, None, "", service.tracker, publisher)
    await unconfigured._attempt_on_queued_row(row, _NOW)
    assert sender.send.await_count == 1
    sender.send.return_value = FcmSendResult("accepted", "accepted", receipt="retry-receipt")
    await sidecar._attempt_on_queued_row(row, _NOW + timedelta(seconds=2))
    assert (await _delivery(repo)).provider_receipt == "retry-receipt"
    await repo.upsert_notification_device(
        NotificationDeviceUpsertRow(
            user_public_id=_OWNER,
            device_token="synthetic-apns",
            device_id="synthetic-device",
            env="sandbox",
            registered_at=_NOW,
            session_id=str(uuid4()),
            sequence_id=1,
            timestamp=_NOW,
        )
    )
    device = (await repo.list_active_notification_devices_for_user(_OWNER))[0]
    event = await repo.get_alert_event_by_public_id(row["alert_event_public_id"])
    assert event is not None
    await sidecar._attempt_once(row["public_id"], device, event, _NOW)
    assert sender.send.await_count == 2
