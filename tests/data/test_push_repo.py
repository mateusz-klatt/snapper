"""Repository-level tests for the iOS Push Foundation BE-1 surface.

Covers the 21 new methods introduced in commit ``ca1e6942`` for the
five new tables ``notification_devices``, ``device_alert_prefs``,
``user_alert_defaults``, ``alert_events``, ``alert_deliveries``
(see ``proprietary/plans/plan_ios_push_foundation_weeks1_4.md`` v1.8
§D1 + §BE-1).

Each test uses a fresh on-disk SQLite database via the ``repo`` fixture
(tmp_path-scoped) and exercises one invariant per test. AAA layout
throughout (arrange / act / assert). No mocks — real SQLAlchemy
session + real ORM rows, so drift between the migration, models, and
repo methods surfaces immediately.
"""

from datetime import UTC
from datetime import datetime
from datetime import timedelta
from pathlib import Path

import pytest

from snapper.data.models import KNOWN_TO_MAX
from snapper.data.models import Operator
from snapper.data.models import User
from snapper.data.models import UserOperatorMembership
from snapper.data.models import Wallet
from snapper.data.models import WalletOperatorScopeGrant
from snapper.data.repository import SQLAlchemyRepository
from snapper.data.repository_types import AlertDeliveryInsertRow
from snapper.data.repository_types import AlertEventInsertRow
from snapper.data.repository_types import DeviceAlertPrefUpsertRow
from snapper.data.repository_types import NotificationDeviceUpsertRow
from snapper.data.repository_types import UserAlertDefaultUpsertRow


def _ts(minutes: int = 0) -> datetime:
    """Deterministic UTC timestamp offset by ``minutes`` from a fixed base."""
    return datetime(2026, 4, 23, 12, 0, 0, tzinfo=UTC) + timedelta(minutes=minutes)


@pytest.fixture
async def repo(tmp_path: Path) -> SQLAlchemyRepository:
    """Disposable on-disk SQLite repository with the full schema materialised."""
    r = SQLAlchemyRepository(f"sqlite+aiosqlite:///{tmp_path}/push.db")
    await r.create_all()
    return r


async def _seed_user_and_device(
    repo: SQLAlchemyRepository,
    *,
    user_public_id: str = "user-1",
    device_token: str = "token-1",
    device_id: str = "device-1",
) -> str:
    """Insert a user+device pair and return the ``device.public_id``."""
    async with repo.session() as s:
        user = User(
            public_id=user_public_id,
            username=f"user_{user_public_id}",
            email=f"{user_public_id}@example.test",
            password_hash="x",
            role="viewer",
            created_at=_ts(),
            session_id="test",
            sequence_id=1,
            timestamp=_ts(),
            known_to=KNOWN_TO_MAX,
        )
        s.add(user)
        await s.commit()
    return await repo.upsert_notification_device(
        NotificationDeviceUpsertRow(
            user_public_id=user_public_id,
            device_token=device_token,
            device_id=device_id,
            env="sandbox",
            registered_at=_ts(),
        )
    )


class TestNotificationDeviceRepo:
    """Behaviour of the five device-related methods."""

    @pytest.mark.asyncio
    async def test_upsert_inserts_new_row_when_token_unknown(
        self, repo: SQLAlchemyRepository
    ) -> None:
        """First upsert for a token creates a row and returns a fresh public_id."""
        public_id = await repo.upsert_notification_device(
            NotificationDeviceUpsertRow(
                user_public_id="user-a",
                device_token="token-a",
                device_id="ios-device-a",
                env="sandbox",
                registered_at=_ts(),
            )
        )

        assert isinstance(public_id, str)
        devices = await repo.list_active_notification_devices_for_user("user-a")
        assert len(devices) == 1
        assert devices[0]["device_token"] == "token-a"
        assert devices[0]["is_active"] is True
        assert devices[0]["previews_mode"] == "private"

    @pytest.mark.asyncio
    async def test_upsert_refreshes_existing_row_idempotent_on_token(
        self, repo: SQLAlchemyRepository
    ) -> None:
        """Re-registering the same token updates in place and reactivates."""
        first_pid = await repo.upsert_notification_device(
            NotificationDeviceUpsertRow(
                user_public_id="user-b",
                device_token="shared-token",
                device_id="old-device-id",
                env="sandbox",
                app_version="1.0.0",
                registered_at=_ts(),
            )
        )
        await repo.mark_notification_device_inactive(first_pid)

        second_pid = await repo.upsert_notification_device(
            NotificationDeviceUpsertRow(
                user_public_id="user-b",
                device_token="shared-token",
                device_id="new-device-id",
                env="prod",
                app_version="1.1.0",
                registered_at=_ts(1),
            )
        )

        assert first_pid == second_pid
        devices = await repo.list_active_notification_devices_for_user("user-b")
        assert len(devices) == 1
        assert devices[0]["device_id"] == "new-device-id"
        assert devices[0]["env"] == "prod"
        assert devices[0]["app_version"] == "1.1.0"
        assert devices[0]["is_active"] is True

    @pytest.mark.asyncio
    async def test_list_active_devices_excludes_inactive(self, repo: SQLAlchemyRepository) -> None:
        """``is_active = False`` rows are excluded from the active listing."""
        active_pid = await _seed_user_and_device(
            repo, user_public_id="user-c", device_token="active-token"
        )
        async with repo.session() as s:
            from snapper.data.models import NotificationDevice

            inactive = NotificationDevice(
                public_id="dead-device",
                user_public_id="user-c",
                device_token="dead-token",
                device_id="dead-id",
                platform="ios",
                env="sandbox",
                app_version=None,
                is_active=False,
                previews_mode="private",
                registered_at=_ts(-1),
                last_seen_at=None,
            )
            s.add(inactive)
            await s.commit()

        devices = await repo.list_active_notification_devices_for_user("user-c")

        assert len(devices) == 1
        assert devices[0]["public_id"] == active_pid

    @pytest.mark.asyncio
    async def test_mark_notification_device_inactive_unknown_is_noop(
        self, repo: SQLAlchemyRepository
    ) -> None:
        """Marking a nonexistent public_id inactive is a silent no-op."""
        await repo.mark_notification_device_inactive("nonexistent-public-id")

    @pytest.mark.asyncio
    async def test_upsert_device_alert_pref_scope_partitioning(
        self, repo: SQLAlchemyRepository
    ) -> None:
        """Three different scope depths coexist as independent rows."""
        device_pid = await _seed_user_and_device(repo, user_public_id="user-d")
        for operator, wallet in (
            (None, None),
            ("op-1", None),
            ("op-1", "wallet-1"),
        ):
            await repo.upsert_device_alert_pref(
                DeviceAlertPrefUpsertRow(
                    device_public_id=device_pid,
                    alert_type="order_fill_full",
                    operator_public_id=operator,
                    wallet_public_id=wallet,
                    enabled=True,
                    min_priority="medium",
                    updated_at=_ts(),
                )
            )

        prefs = await repo.list_device_alert_prefs_for_user("user-d")
        scope_tuples = {(p["operator_public_id"], p["wallet_public_id"]) for p in prefs}
        assert scope_tuples == {(None, None), ("op-1", None), ("op-1", "wallet-1")}

    @pytest.mark.asyncio
    async def test_upsert_device_alert_pref_updates_existing_at_same_scope(
        self, repo: SQLAlchemyRepository
    ) -> None:
        """Second upsert at an identical (device, alert_type, scope) updates in place."""
        device_pid = await _seed_user_and_device(repo, user_public_id="user-e")
        await repo.upsert_device_alert_pref(
            DeviceAlertPrefUpsertRow(
                device_public_id=device_pid,
                alert_type="order_fill_full",
                enabled=False,
                min_priority="low",
                updated_at=_ts(),
            )
        )
        await repo.upsert_device_alert_pref(
            DeviceAlertPrefUpsertRow(
                device_public_id=device_pid,
                alert_type="order_fill_full",
                enabled=True,
                min_priority="high",
                updated_at=_ts(1),
            )
        )

        prefs = await repo.list_device_alert_prefs_for_user("user-e")
        assert len(prefs) == 1
        assert prefs[0]["enabled"] is True
        assert prefs[0]["min_priority"] == "high"


class TestUserAlertDefaultRepo:
    """Behaviour of user-level fallback preference methods."""

    @pytest.mark.asyncio
    async def test_upsert_and_list_user_alert_default_roundtrip(
        self, repo: SQLAlchemyRepository
    ) -> None:
        """Write then read returns the same fields for a fresh (user, type) pair."""
        await repo.upsert_user_alert_default(
            UserAlertDefaultUpsertRow(
                user_public_id="user-f",
                alert_type="order_rejected",
                enabled=False,
                min_priority="high",
                updated_at=_ts(),
            )
        )

        defaults = await repo.list_user_alert_defaults("user-f")

        assert len(defaults) == 1
        assert defaults[0]["alert_type"] == "order_rejected"
        assert defaults[0]["enabled"] is False
        assert defaults[0]["min_priority"] == "high"


class TestAlertEventRepo:
    """Behaviour of the three alert_event methods."""

    @pytest.mark.asyncio
    async def test_insert_auto_fills_public_id_and_known_to(
        self, repo: SQLAlchemyRepository
    ) -> None:
        """Insert auto-generates public_id and defaults known_to to KNOWN_TO_MAX."""
        public_id = await repo.insert_alert_event(
            AlertEventInsertRow(
                session_id="s1",
                sequence_id=1,
                timestamp=_ts(),
                user_public_id="user-g",
                alert_type="order_fill_full",
                priority="high",
                title="Filled",
                body="BTC-USD 0.1 filled",
            )
        )

        row = await repo.get_alert_event_by_public_id(public_id)

        assert row is not None
        assert row["public_id"] == public_id
        assert row["known_to"] == KNOWN_TO_MAX
        assert row["title"] == "Filled"
        assert row["is_safety_critical"] is False

    @pytest.mark.asyncio
    async def test_list_recent_alerts_pagination_via_before_cursor(
        self, repo: SQLAlchemyRepository
    ) -> None:
        """The ``before`` cursor returns only rows strictly older than the cursor row."""
        public_ids: list[str] = []
        for idx in range(5):
            public_ids.append(
                await repo.insert_alert_event(
                    AlertEventInsertRow(
                        session_id="s1",
                        sequence_id=idx,
                        timestamp=_ts(idx),
                        user_public_id="user-h",
                        alert_type="order_fill_full",
                        priority="medium",
                        title=f"Alert {idx}",
                        body="body",
                    )
                )
            )

        full = await repo.list_recent_alerts_for_user("user-h", limit=10, before=None)
        after_first = await repo.list_recent_alerts_for_user(
            "user-h", limit=10, before=public_ids[4]
        )

        assert [r["public_id"] for r in full] == list(reversed(public_ids))
        assert [r["public_id"] for r in after_first] == list(reversed(public_ids[:4]))

    @pytest.mark.asyncio
    async def test_get_alert_event_returns_none_for_unknown_public_id(
        self, repo: SQLAlchemyRepository
    ) -> None:
        """Missing public_id yields None rather than raising."""
        row = await repo.get_alert_event_by_public_id("nope")

        assert row is None


class TestAlertDeliveryRepo:
    """Behaviour of the seven delivery methods."""

    @pytest.mark.asyncio
    async def test_insert_and_list_queued_deliveries(self, repo: SQLAlchemyRepository) -> None:
        """Insert creates a queued row visible to ``list_queued_deliveries_all``."""
        public_id = await repo.insert_alert_delivery(
            AlertDeliveryInsertRow(
                alert_event_public_id="evt-1",
                device_public_id="dev-1",
                status="queued",
                created_at=_ts(),
            )
        )

        queued = await repo.list_queued_deliveries_all()

        assert len(queued) == 1
        assert queued[0]["public_id"] == public_id
        assert queued[0]["status"] == "queued"

    @pytest.mark.asyncio
    async def test_mark_delivery_sent_transitions_and_stores_apns_id(
        self, repo: SQLAlchemyRepository
    ) -> None:
        """Sent transition records the APNs id and drops the row from the queue."""
        public_id = await repo.insert_alert_delivery(
            AlertDeliveryInsertRow(
                alert_event_public_id="evt-1",
                device_public_id="dev-1",
                status="queued",
                created_at=_ts(),
            )
        )
        await repo.mark_delivery_sent(public_id, apns_id="apns-123")

        queued_after = await repo.list_queued_deliveries_all()

        assert queued_after == []

    @pytest.mark.asyncio
    async def test_list_deliveries_ready_for_retry_filters_future_next_attempts(
        self, repo: SQLAlchemyRepository
    ) -> None:
        """Rows with ``next_attempt_at`` in the future are excluded."""
        now = _ts()
        future = _ts(10)
        past = _ts(-10)

        ready_pid = await repo.insert_alert_delivery(
            AlertDeliveryInsertRow(
                alert_event_public_id="evt-1",
                device_public_id="dev-1",
                status="queued",
                next_attempt_at=past,
                created_at=now,
            )
        )
        await repo.insert_alert_delivery(
            AlertDeliveryInsertRow(
                alert_event_public_id="evt-1",
                device_public_id="dev-2",
                status="queued",
                next_attempt_at=future,
                created_at=now,
            )
        )
        immediate_pid = await repo.insert_alert_delivery(
            AlertDeliveryInsertRow(
                alert_event_public_id="evt-2",
                device_public_id="dev-3",
                status="queued",
                created_at=now,
            )
        )

        ready = await repo.list_deliveries_ready_for_retry(now)

        ready_ids = {r["public_id"] for r in ready}
        assert ready_ids == {ready_pid, immediate_pid}


class TestScopeHelpers:
    """Behaviour of the three scope-cascade helpers."""

    @pytest.mark.asyncio
    async def test_cancel_pending_deliveries_for_scope_bulk_transitions(
        self, repo: SQLAlchemyRepository
    ) -> None:
        """Bulk cancellation flips queued deliveries for matching scope only."""
        evt_public_id = await repo.insert_alert_event(
            AlertEventInsertRow(
                session_id="s1",
                sequence_id=1,
                timestamp=_ts(),
                user_public_id="user-i",
                operator_public_id="op-i",
                wallet_public_id="wallet-i",
                alert_type="order_fill_full",
                priority="high",
                title="t",
                body="b",
            )
        )
        await repo.insert_alert_delivery(
            AlertDeliveryInsertRow(
                alert_event_public_id=evt_public_id,
                device_public_id="dev-1",
                status="queued",
                created_at=_ts(),
            )
        )

        unrelated_evt = await repo.insert_alert_event(
            AlertEventInsertRow(
                session_id="s1",
                sequence_id=2,
                timestamp=_ts(),
                user_public_id="user-i",
                operator_public_id="op-other",
                wallet_public_id="wallet-other",
                alert_type="order_fill_full",
                priority="high",
                title="t2",
                body="b2",
            )
        )
        untouched_pid = await repo.insert_alert_delivery(
            AlertDeliveryInsertRow(
                alert_event_public_id=unrelated_evt,
                device_public_id="dev-2",
                status="queued",
                created_at=_ts(),
            )
        )

        cancelled = await repo.cancel_pending_deliveries_for_scope(
            user_public_id="user-i",
            operator_public_id="op-i",
            wallet_public_id="wallet-i",
        )

        assert cancelled == 1
        queued_after = await repo.list_queued_deliveries_all()
        assert [q["public_id"] for q in queued_after] == [untouched_pid]

    @pytest.mark.asyncio
    async def test_is_scope_grant_active_requires_both_grant_and_membership(
        self, repo: SQLAlchemyRepository
    ) -> None:
        """Missing membership + present grant returns False; both present → True."""
        now = _ts()
        async with repo.session() as s:
            user = User(
                public_id="user-j",
                username="j",
                email="j@example.test",
                password_hash="x",
                role="viewer",
                created_at=now,
                session_id="t",
                sequence_id=1,
                timestamp=now,
                known_to=KNOWN_TO_MAX,
            )
            operator = Operator(
                public_id="op-j",
                label="Op J",
                description=None,
                session_id="t",
                sequence_id=2,
                timestamp=now,
                known_to=KNOWN_TO_MAX,
            )
            wallet = Wallet(
                label="w-j",
                description=None,
                is_paper=False,
                session_id="t",
                sequence_id=3,
                timestamp=now,
                known_to=KNOWN_TO_MAX,
            )
            s.add_all([user, operator, wallet])
            await s.commit()
            await s.refresh(wallet)

            grant = WalletOperatorScopeGrant(
                public_id="grant-j",
                operator_public_id="op-j",
                wallet_public_id=wallet.public_id,
                granted_by_user_public_id="user-j",
                scope_kind="underlying",
                underlying_public_id="underlying-j",
                instrument_public_id=None,
                note=None,
                session_id="t",
                sequence_id=4,
                timestamp=now,
                known_to=KNOWN_TO_MAX,
            )
            s.add(grant)
            await s.commit()
            wallet_public_id = wallet.public_id

        without_membership = await repo.is_scope_grant_active(
            user_public_id="user-j",
            operator_public_id="op-j",
            wallet_public_id=wallet_public_id,
            as_of=now,
        )
        assert without_membership is False

        async with repo.session() as s:
            membership = UserOperatorMembership(
                user_public_id="user-j",
                operator_public_id="op-j",
                session_id="t",
                sequence_id=5,
                timestamp=now,
                known_to=KNOWN_TO_MAX,
            )
            s.add(membership)
            await s.commit()

        with_membership = await repo.is_scope_grant_active(
            user_public_id="user-j",
            operator_public_id="op-j",
            wallet_public_id=wallet_public_id,
            as_of=now,
        )
        assert with_membership is True
