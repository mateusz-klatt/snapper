"""Opt-in live-PostgreSQL witness for atomic queued APNs admission.

Verifies real PostgreSQL ``REPEATABLE READ`` snapshot semantics and ``SQLSTATE
40001`` serialization conflicts across two independent engines executing
``SQLAlchemyRepository.admit_and_charge_apns_delivery`` concurrently.

Every test SKIPS unless the dedicated ``SNAPPER_PG_WITNESS_URL`` environment
variable is a PostgreSQL URL whose database name ends with ``_witness`` or
``_scratch`` (never falls back to ``DB_URL`` or ``TEST_DB_URL``), so default
SQLite runs and coverage gates remain unaffected and non-scratch databases are
protected. Opt in against a migrated scratch database via::

    SNAPPER_PG_WITNESS_URL=postgresql+asyncpg://snapper:example@localhost:5432/snapper_witness pytest tests/data/test_apns_admission_pg_witness.py

Each test allocates random UUID7 identifiers and deletes only its own
synthetic fixture rows on teardown.
"""

import os
from collections.abc import AsyncIterator
from collections.abc import Mapping
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from unittest.mock import patch
from uuid import uuid4
from uuid import uuid7

import pytest
from sqlalchemy import delete
from sqlalchemy import select
from sqlalchemy.engine import URL
from sqlalchemy.engine import make_url
from sqlalchemy.exc import ArgumentError
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql.base import Executable

from snapper.application.notify.routing import allows_device_alert
from snapper.application.notify.sidecar import _is_pg_serialization_failure
from snapper.data.models import KNOWN_TO_MAX
from snapper.data.models import AlertDelivery
from snapper.data.models import AlertEvent
from snapper.data.models import DeviceAlertPref
from snapper.data.models import NotificationDevice
from snapper.data.models import User
from snapper.data.models import UserAlertDefault
from snapper.data.repository import SQLAlchemyRepository
from snapper.data.repository import _DeliveryTransitionSpec
from snapper.data.repository_types import AlertDeliveryInsertRow
from snapper.data.repository_types import AlertEventInsertRow
from snapper.data.repository_types import AlertEventRow
from snapper.data.repository_types import ApnsAdmissionCharge
from snapper.data.repository_types import ApnsAdmissionContext
from snapper.data.repository_types import ApnsAdmissionRefusal
from snapper.data.repository_types import ApnsAdmissionRequest
from snapper.data.repository_types import DeviceAlertPrefUpsertRow
from snapper.data.repository_types import NotificationDeviceUpsertRow

_T0 = datetime(2026, 7, 17, 12, 0, tzinfo=UTC)
_T1 = _T0 + timedelta(minutes=1)
_T2 = _T0 + timedelta(minutes=2)
_T3 = _T0 + timedelta(minutes=3)
_WITNESS_ENV_VAR = "SNAPPER_PG_WITNESS_URL"


def _resolve_witness_url() -> tuple[URL | None, str]:
    """Validate ``SNAPPER_PG_WITNESS_URL`` and enforce a dedicated witness/scratch DB name."""
    raw = os.environ.get(_WITNESS_ENV_VAR, "").strip()
    if not raw:
        return (
            None,
            f"live-PostgreSQL witness: set {_WITNESS_ENV_VAR}=postgresql+asyncpg://.../<db>_witness to opt in",
        )
    try:
        url = make_url(raw)
    except ArgumentError:
        return None, f"{_WITNESS_ENV_VAR} is not a valid SQLAlchemy URL"
    if url.get_backend_name() != "postgresql":
        return (
            None,
            f"{_WITNESS_ENV_VAR} must use a postgresql backend (got {url.get_backend_name()!r})",
        )
    db_name = (url.database or "").strip()
    if not db_name.endswith(("_witness", "_scratch")):
        return (
            None,
            f"{_WITNESS_ENV_VAR} refuses to run unless database name ends with '_witness' or '_scratch' (got {db_name!r})",
        )
    return url.set(drivername="postgresql+asyncpg"), ""


_WITNESS_URL, _SKIP_REASON = _resolve_witness_url()

pytestmark = [
    pytest.mark.skipif(_WITNESS_URL is None, reason=_SKIP_REASON),
]


@dataclass(frozen=True, slots=True)
class _PgAdmissionFixture:
    """Two independent PostgreSQL repositories and isolated UUID fixture ids."""

    repo_primary: SQLAlchemyRepository
    repo_rival: SQLAlchemyRepository
    user_public_id: str
    device_public_id: str
    delivery_public_id: str
    event: AlertEventRow


async def _cleanup_fixture_rows(
    repo: SQLAlchemyRepository,
    *,
    user_public_id: str,
    device_public_id: str,
) -> None:
    """Delete synthetic fixture rows owned by one witness run."""
    async with repo.session() as session:
        await session.execute(
            delete(AlertDelivery).where(AlertDelivery.user_public_id == user_public_id)
        )
        await session.execute(
            delete(DeviceAlertPref).where(DeviceAlertPref.device_public_id == device_public_id)
        )
        await session.execute(
            delete(UserAlertDefault).where(UserAlertDefault.user_public_id == user_public_id)
        )
        await session.execute(delete(AlertEvent).where(AlertEvent.user_public_id == user_public_id))
        await session.execute(
            delete(NotificationDevice).where(NotificationDevice.user_public_id == user_public_id)
        )
        await session.execute(delete(User).where(User.public_id == user_public_id))
        await session.commit()


@pytest.fixture
async def pg_fixture() -> AsyncIterator[_PgAdmissionFixture]:
    """Provision two independent PostgreSQL engines with an isolated queued APNs delivery."""
    url = _WITNESS_URL
    assert url is not None
    url_str = url.render_as_string(hide_password=False)
    repo_primary = SQLAlchemyRepository(url_str)
    repo_rival = SQLAlchemyRepository(url_str)
    user_pid = str(uuid7())
    seed_session = str(uuid7())
    device_pid = ""
    try:
        async with repo_primary.session() as session:
            session.add(
                User(
                    public_id=user_pid,
                    username=f"pg_wit_{uuid4().hex[:12]}",
                    email=f"{user_pid}@witness.test",
                    password_hash="x",
                    role="viewer",
                    created_at=_T0,
                    session_id=seed_session,
                    sequence_id=1,
                    timestamp=_T0,
                    known_to=KNOWN_TO_MAX,
                )
            )
            await session.commit()
        device_pid = await repo_primary.upsert_notification_device(
            NotificationDeviceUpsertRow(
                user_public_id=user_pid,
                device_token=f"pg-apns-tok-{uuid4().hex}",
                device_id=f"pg-ios-{uuid4().hex[:12]}",
                env="prod",
                registered_at=_T0,
                session_id=seed_session,
                sequence_id=2,
                timestamp=_T0,
            )
        )
        event_pid = await repo_primary.insert_alert_event(
            AlertEventInsertRow(
                session_id=seed_session,
                sequence_id=3,
                timestamp=_T0,
                user_public_id=user_pid,
                alert_type="order_fill_full",
                priority="medium",
                title="Filled",
                body="PG witness fill alert",
            )
        )
        event = await repo_primary.get_alert_event_by_public_id(event_pid)
        assert event is not None
        delivery_pid = await repo_primary.insert_alert_delivery(
            AlertDeliveryInsertRow(
                session_id=seed_session,
                sequence_id=4,
                timestamp=_T0,
                alert_event_public_id=event_pid,
                device_public_id=device_pid,
                user_public_id=user_pid,
                status="queued",
                expires_at=_T0 + timedelta(hours=1),
                created_at=_T0,
            )
        )
        yield _PgAdmissionFixture(
            repo_primary=repo_primary,
            repo_rival=repo_rival,
            user_public_id=user_pid,
            device_public_id=device_pid,
            delivery_public_id=delivery_pid,
            event=event,
        )
    finally:
        await _cleanup_fixture_rows(
            repo_primary,
            user_public_id=user_pid,
            device_public_id=device_pid,
        )
        await repo_rival.engine.dispose()
        await repo_primary.engine.dispose()


async def _load_pg_delivery_versions(
    repo: SQLAlchemyRepository, delivery_public_id: str
) -> list[AlertDelivery]:
    """Return ordered SCD2 versions of ``delivery_public_id`` from PostgreSQL."""
    async with repo.session() as session:
        result = await session.execute(
            select(AlertDelivery)
            .where(AlertDelivery.public_id == delivery_public_id)
            .order_by(AlertDelivery.id.asc())
        )
        return list(result.scalars().all())


@pytest.mark.asyncio
async def test_pg_repeatable_read_admits_snapshot_and_next_call_refuses_routing_suppressed(
    pg_fixture: _PgAdmissionFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Concurrent opt-out committed between reads is hidden under ``REPEATABLE READ`` then caught next.

    Given: Two independent PostgreSQL engines sharing a queued APNs delivery whose
        initial snapshot permits routing.
    When: Engine 1 pauses between its ``notification_devices`` read and
        ``device_alert_prefs`` read while engine 2 commits a deny
        ``DeviceAlertPref``, and then engine 1 runs a second admission call.
    Then: The first admission completes from its consistent snapshot with
        ``attempt == 1``, and the second call reads the committed opt-out and
        refuses ``routing_suppressed`` at ``attempt_count == 1`` without charging.
    """
    original_execute = AsyncSession.execute
    committed_opt_out = False

    async def _interleave_opt_out(
        session: AsyncSession,
        statement: Executable,
        params: Sequence[Mapping[str, object]] | Mapping[str, object] | None = None,
    ) -> object:
        nonlocal committed_opt_out
        result = await original_execute(session, statement, params)
        if not committed_opt_out and "FROM notification_devices" in str(statement):
            committed_opt_out = True
            await pg_fixture.repo_rival.upsert_device_alert_pref(
                DeviceAlertPrefUpsertRow(
                    session_id=str(uuid7()),
                    sequence_id=10,
                    timestamp=_T1,
                    device_public_id=pg_fixture.device_public_id,
                    alert_type="order_fill_full",
                    enabled=False,
                )
            )
        return result

    monkeypatch.setattr(AsyncSession, "execute", _interleave_opt_out)
    captured_contexts: list[ApnsAdmissionContext] = []

    def _admits_at_t1(ctx: ApnsAdmissionContext) -> bool:
        captured_contexts.append(ctx)
        return allows_device_alert(
            pg_fixture.device_public_id,
            pg_fixture.event,
            ctx["device_prefs"],
            ctx["user_defaults"],
            _T1,
        )

    first = await pg_fixture.repo_primary.admit_and_charge_apns_delivery(
        ApnsAdmissionRequest(
            delivery_public_id=pg_fixture.delivery_public_id,
            user_public_id=pg_fixture.user_public_id,
            device_public_id=pg_fixture.device_public_id,
            transition_at=_T1,
            session_id=str(uuid7()),
            sequence_id=20,
            admits=_admits_at_t1,
        )
    )

    assert committed_opt_out is True
    assert isinstance(first, ApnsAdmissionCharge)
    assert first.attempt == 1
    assert captured_contexts[0]["device_prefs"] == []

    second = await pg_fixture.repo_primary.admit_and_charge_apns_delivery(
        ApnsAdmissionRequest(
            delivery_public_id=pg_fixture.delivery_public_id,
            user_public_id=pg_fixture.user_public_id,
            device_public_id=pg_fixture.device_public_id,
            transition_at=_T2,
            session_id=str(uuid7()),
            sequence_id=21,
            admits=lambda ctx: allows_device_alert(
                pg_fixture.device_public_id,
                pg_fixture.event,
                ctx["device_prefs"],
                ctx["user_defaults"],
                _T2,
            ),
        )
    )

    versions = await _load_pg_delivery_versions(
        pg_fixture.repo_primary, pg_fixture.delivery_public_id
    )
    assert second == ApnsAdmissionRefusal(
        refusal="routing_suppressed", attempt_count=1, version_id=versions[-1].id
    )
    assert [(v.status, v.attempt_count) for v in versions] == [("queued", 0), ("queued", 1)]


@pytest.mark.asyncio
async def test_pg_repeatable_read_concurrent_close_raises_sqlstate_40001(
    pg_fixture: _PgAdmissionFixture,
) -> None:
    """Concurrent delivery close under ``REPEATABLE READ`` raises real ``SQLSTATE 40001``.

    Given: Two independent PostgreSQL engines sharing a queued APNs delivery at
        ``attempt_count == 0``.
    When: Engine 1 begins admission and passes ``admits``, then engine 2 closes
        and transitions that same queued delivery to ``cancelled_scope`` before
        engine 1 executes its guarded ``UPDATE``.
    Then: PostgreSQL raises a ``DBAPIError`` that
        ``_is_pg_serialization_failure`` classifies as ``True``, engine 1 charges
        nothing, and the database retains engine 2's ``cancelled_scope`` version.
    """
    original_commit_transition = pg_fixture.repo_primary._commit_delivery_transition_in_session
    rival_session_id = str(uuid7())

    async def _close_from_rival_before_update(
        session: AsyncSession,
        existing: AlertDelivery,
        spec: _DeliveryTransitionSpec,
    ) -> int | None:
        await pg_fixture.repo_rival.mark_delivery_cancelled(
            pg_fixture.delivery_public_id,
            reason="pg_rival_cancel",
            transition_at=_T1,
            session_id=rival_session_id,
            sequence_id=30,
        )
        return await original_commit_transition(session, existing, spec)

    with (
        patch.object(
            pg_fixture.repo_primary,
            "_commit_delivery_transition_in_session",
            side_effect=_close_from_rival_before_update,
        ),
        pytest.raises(DBAPIError) as exc_info,
    ):
        await pg_fixture.repo_primary.admit_and_charge_apns_delivery(
            ApnsAdmissionRequest(
                delivery_public_id=pg_fixture.delivery_public_id,
                user_public_id=pg_fixture.user_public_id,
                device_public_id=pg_fixture.device_public_id,
                transition_at=_T2,
                session_id=str(uuid7()),
                sequence_id=31,
                admits=lambda _ctx: True,
            )
        )

    assert _is_pg_serialization_failure(exc_info.value) is True
    active = await pg_fixture.repo_primary.get_delivery_by_public_id(pg_fixture.delivery_public_id)
    assert active is not None
    assert active["status"] == "cancelled_scope"
    assert active["error_reason"] == "pg_rival_cancel"
    assert active["attempt_count"] == 0
    versions = await _load_pg_delivery_versions(
        pg_fixture.repo_primary, pg_fixture.delivery_public_id
    )
    assert [(v.status, v.attempt_count) for v in versions] == [
        ("queued", 0),
        ("cancelled_scope", 0),
    ]


@pytest.mark.asyncio
async def test_pg_repeatable_read_concurrent_schedule_rewrite_40001_recovers_on_retry(
    pg_fixture: _PgAdmissionFixture,
) -> None:
    """Concurrent schedule rewrite under ``REPEATABLE READ`` raises ``40001`` and recovers next call.

    Given: Two independent PostgreSQL engines sharing a queued APNs delivery at
        ``attempt_count == 0``.
    When: Engine 1 begins admission and passes ``admits``, then engine 2 rewrites
        the delivery's retry schedule (still queued and due at ``attempt_count == 0``)
        and commits before engine 1 executes its guarded ``UPDATE``.
    Then: Engine 1 raises a real ``SQLSTATE 40001`` serialization failure with
        zero attempt charge leaving the row queued and due, and a subsequent
        ``admit_and_charge_apns_delivery`` call admits and charges attempt 1.
    """
    original_commit_transition = pg_fixture.repo_primary._commit_delivery_transition_in_session
    rival_session_id = str(uuid7())

    async def _rewrite_schedule_from_rival_before_update(
        session: AsyncSession,
        existing: AlertDelivery,
        spec: _DeliveryTransitionSpec,
    ) -> int | None:
        rewritten = await pg_fixture.repo_rival.update_delivery_retry_schedule(
            pg_fixture.delivery_public_id,
            attempt_count=0,
            next_attempt_at=_T1,
            error_reason="rival_schedule_rewrite",
            transition_at=_T1,
            session_id=rival_session_id,
            sequence_id=40,
        )
        assert rewritten is True
        return await original_commit_transition(session, existing, spec)

    with (
        patch.object(
            pg_fixture.repo_primary,
            "_commit_delivery_transition_in_session",
            side_effect=_rewrite_schedule_from_rival_before_update,
        ),
        pytest.raises(DBAPIError) as exc_info,
    ):
        await pg_fixture.repo_primary.admit_and_charge_apns_delivery(
            ApnsAdmissionRequest(
                delivery_public_id=pg_fixture.delivery_public_id,
                user_public_id=pg_fixture.user_public_id,
                device_public_id=pg_fixture.device_public_id,
                transition_at=_T2,
                session_id=str(uuid7()),
                sequence_id=41,
                admits=lambda _ctx: True,
            )
        )

    assert _is_pg_serialization_failure(exc_info.value) is True
    after_conflict = await pg_fixture.repo_primary.get_delivery_by_public_id(
        pg_fixture.delivery_public_id
    )
    assert after_conflict is not None
    assert after_conflict["status"] == "queued"
    assert after_conflict["attempt_count"] == 0
    assert after_conflict["next_attempt_at"] == _T1
    assert after_conflict["error_reason"] == "rival_schedule_rewrite"
    due_rows = await pg_fixture.repo_primary.list_deliveries_ready_for_retry(_T2)
    assert any(row["public_id"] == pg_fixture.delivery_public_id for row in due_rows)

    recovered = await pg_fixture.repo_primary.admit_and_charge_apns_delivery(
        ApnsAdmissionRequest(
            delivery_public_id=pg_fixture.delivery_public_id,
            user_public_id=pg_fixture.user_public_id,
            device_public_id=pg_fixture.device_public_id,
            transition_at=_T3,
            session_id=str(uuid7()),
            sequence_id=42,
            admits=lambda _ctx: True,
        )
    )

    assert isinstance(recovered, ApnsAdmissionCharge)
    assert recovered.attempt == 1
    versions = await _load_pg_delivery_versions(
        pg_fixture.repo_primary, pg_fixture.delivery_public_id
    )
    assert [(v.status, v.attempt_count, v.next_attempt_at) for v in versions] == [
        ("queued", 0, None),
        ("queued", 0, _T1),
        ("queued", 1, None),
    ]
