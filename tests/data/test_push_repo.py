"""Repository-level tests for the iOS Push Foundation surface.

Covers the 21 SCD2 methods introduced for the five new
bitemporal tables ``notification_devices``, ``device_alert_prefs``,
``user_alert_defaults``, ``alert_events``, ``alert_deliveries``.
Per project invariant (``feedback_bitemporal_all_tables``)
every row lifecycle is SCD2 close-and-insert; reads gate on
``known_to == KNOWN_TO_MAX`` via partial active indexes.

Each test uses a fresh on-disk SQLite database via the ``repo`` fixture
(tmp_path-scoped, engine disposed after yield). AAA layout.
"""

import asyncio
from collections.abc import AsyncGenerator
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy import select as _select
from sqlalchemy import update as _update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from snapper.data import repository as repository_module
from snapper.data.models import KNOWN_TO_MAX
from snapper.data.models import AlertDelivery
from snapper.data.models import AlertEvent
from snapper.data.models import DeviceAlertPref
from snapper.data.models import NotificationDevice
from snapper.data.models import Operator
from snapper.data.models import User
from snapper.data.models import UserOperatorMembership
from snapper.data.models import Wallet
from snapper.data.models import WalletOperatorScopeGrant
from snapper.data.repository import SQLAlchemyRepository
from snapper.data.repository_types import AlertDeliveryInsertRow
from snapper.data.repository_types import AlertEventInsertRow
from snapper.data.repository_types import AlertListCursor
from snapper.data.repository_types import DeviceAlertPrefUpsertRow
from snapper.data.repository_types import NotificationDeviceUpsertRow
from snapper.data.repository_types import UserAlertDefaultUpsertRow


def _ts(minutes: int = 0) -> datetime:
    """Deterministic UTC timestamp offset by ``minutes`` from a fixed base."""
    return datetime(2026, 4, 23, 12, 0, 0, tzinfo=UTC) + timedelta(minutes=minutes)


@pytest.fixture
async def repo(tmp_path: Path) -> AsyncGenerator[SQLAlchemyRepository]:
    """Disposable on-disk SQLite repository with the full schema materialised."""
    r = SQLAlchemyRepository(f"sqlite+aiosqlite:///{tmp_path}/push.db")
    await r.create_all()
    try:
        yield r
    finally:
        await r.engine.dispose()


async def _seed_user_and_device(
    repo: SQLAlchemyRepository,
    *,
    user_public_id: str = "user-1",
    device_token: str = "token-1",
    device_id: str = "device-1",
    sequence_id: int = 1,
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
            session_id="test",
            sequence_id=sequence_id,
            timestamp=_ts(),
        )
    )


class TestNotificationDeviceRepo:
    """SCD2 behaviour of the five device-related methods."""

    @pytest.mark.asyncio
    async def test_upsert_inserts_new_row_when_token_unknown(
        self, repo: SQLAlchemyRepository
    ) -> None:
        """First upsert for a token creates an active SCD2 row and returns its public_id."""
        public_id = await repo.upsert_notification_device(
            NotificationDeviceUpsertRow(
                user_public_id="user-a",
                device_token="token-a",
                device_id="ios-device-a",
                env="sandbox",
                registered_at=_ts(),
                session_id="s1",
                sequence_id=1,
                timestamp=_ts(),
            )
        )

        devices = await repo.list_active_notification_devices_for_user("user-a")

        assert isinstance(public_id, str)
        assert len(devices) == 1
        assert devices[0]["device_token"] == "token-a"
        assert devices[0]["known_to"] == KNOWN_TO_MAX
        assert devices[0]["previews_mode"] == "private"

    @pytest.mark.asyncio
    async def test_upsert_scd2_closes_prior_version_and_preserves_public_id(
        self, repo: SQLAlchemyRepository
    ) -> None:
        """Re-registering the same token closes the prior version and reuses public_id."""
        first_pid = await repo.upsert_notification_device(
            NotificationDeviceUpsertRow(
                user_public_id="user-b",
                device_token="shared-token",
                device_id="old-device-id",
                env="sandbox",
                app_version="1.0.0",
                registered_at=_ts(),
                session_id="s1",
                sequence_id=1,
                timestamp=_ts(),
            )
        )
        second_pid = await repo.upsert_notification_device(
            NotificationDeviceUpsertRow(
                user_public_id="user-b",
                device_token="shared-token",
                device_id="new-device-id",
                env="prod",
                app_version="1.1.0",
                registered_at=_ts(1),
                session_id="s1",
                sequence_id=2,
                timestamp=_ts(1),
            )
        )

        active = await repo.list_active_notification_devices_for_user("user-b")
        async with repo.session() as s:

            all_rows = (
                (
                    await s.execute(
                        _select(NotificationDevice).where(NotificationDevice.public_id == first_pid)
                    )
                )
                .scalars()
                .all()
            )

        assert first_pid == second_pid
        assert len(active) == 1
        assert active[0]["device_id"] == "new-device-id"
        assert active[0]["env"] == "prod"
        assert len(all_rows) == 2
        closed = [r for r in all_rows if r.known_to != KNOWN_TO_MAX]
        assert len(closed) == 1
        assert closed[0].known_to == _ts(1)

    @pytest.mark.asyncio
    async def test_list_active_excludes_scd2_closed_devices(
        self, repo: SQLAlchemyRepository
    ) -> None:
        """Tombstone successor rows are not returned by ``list_active``."""
        active_pid = await _seed_user_and_device(
            repo, user_public_id="user-c", device_token="active-token", sequence_id=1
        )
        inserted = await repo.deactivate_notification_device_scd2(
            active_pid,
            reason="unregistered",
            timestamp=_ts(5),
            session_id="s-410",
            sequence_id=99,
        )
        assert inserted is True

        devices = await repo.list_active_notification_devices_for_user("user-c")

        assert devices == []

    @pytest.mark.asyncio
    async def test_deactivate_notification_device_scd2_unknown_is_noop(
        self, repo: SQLAlchemyRepository
    ) -> None:
        """Deactivating a nonexistent public_id is a silent idempotent no-op."""
        inserted = await repo.deactivate_notification_device_scd2(
            "nonexistent-public-id",
            reason="unregistered",
            timestamp=_ts(3),
            session_id="s-noop",
            sequence_id=1,
        )
        assert inserted is False

    @pytest.mark.asyncio
    async def test_list_device_alert_prefs_excludes_tombstoned_device(
        self, repo: SQLAlchemyRepository
    ) -> None:
        """Prefs attached to a 410'd device must not leak back.

        Tombstone successor rows remain at ``known_to = KNOWN_TO_MAX``,
        so a join filtering only on ``known_to`` would return
        prefs owned by an unregistered device. The join must also
        require ``token_status = 'active'``.
        """
        device_pid = await _seed_user_and_device(
            repo,
            user_public_id="user-tombstone-prefs",
            device_token="tombstoned-token",
            sequence_id=1,
        )
        await repo.upsert_device_alert_pref(
            DeviceAlertPrefUpsertRow(
                device_public_id=device_pid,
                alert_type="order_fill_full",
                enabled=True,
                min_priority="medium",
                session_id="s1",
                sequence_id=2,
                timestamp=_ts(1),
            )
        )
        pre_deactivate = await repo.list_device_alert_prefs_for_user("user-tombstone-prefs")
        assert len(pre_deactivate) == 1

        await repo.deactivate_notification_device_scd2(
            device_pid,
            reason="unregistered",
            timestamp=_ts(5),
            session_id="s-410",
            sequence_id=99,
        )

        post_deactivate = await repo.list_device_alert_prefs_for_user("user-tombstone-prefs")
        assert post_deactivate == []

    @pytest.mark.asyncio
    async def test_upsert_device_alert_pref_scope_partitioning(
        self, repo: SQLAlchemyRepository
    ) -> None:
        """Three different scope depths coexist as independent active rows."""
        device_pid = await _seed_user_and_device(repo, user_public_id="user-d")
        for idx, (operator, wallet) in enumerate(
            ((None, None), ("op-1", None), ("op-1", "wallet-1"))
        ):
            await repo.upsert_device_alert_pref(
                DeviceAlertPrefUpsertRow(
                    device_public_id=device_pid,
                    alert_type="order_fill_full",
                    operator_public_id=operator,
                    wallet_public_id=wallet,
                    enabled=True,
                    min_priority="medium",
                    session_id="s1",
                    sequence_id=idx + 1,
                    timestamp=_ts(idx),
                )
            )

        prefs = await repo.list_device_alert_prefs_for_user("user-d")
        scope_tuples = {(p["operator_public_id"], p["wallet_public_id"]) for p in prefs}

        assert scope_tuples == {(None, None), ("op-1", None), ("op-1", "wallet-1")}

    @pytest.mark.asyncio
    async def test_upsert_device_alert_pref_scd2_updates_existing_scope(
        self, repo: SQLAlchemyRepository
    ) -> None:
        """Second upsert at identical scope closes previous active version + inserts new."""
        device_pid = await _seed_user_and_device(repo, user_public_id="user-e")
        await repo.upsert_device_alert_pref(
            DeviceAlertPrefUpsertRow(
                device_public_id=device_pid,
                alert_type="order_fill_full",
                enabled=False,
                min_priority="low",
                session_id="s1",
                sequence_id=1,
                timestamp=_ts(),
            )
        )
        await repo.upsert_device_alert_pref(
            DeviceAlertPrefUpsertRow(
                device_public_id=device_pid,
                alert_type="order_fill_full",
                enabled=True,
                min_priority="high",
                session_id="s1",
                sequence_id=2,
                timestamp=_ts(1),
            )
        )

        prefs = await repo.list_device_alert_prefs_for_user("user-e")

        assert len(prefs) == 1
        assert prefs[0]["enabled"] is True
        assert prefs[0]["min_priority"] == "high"
        assert prefs[0]["known_to"] == KNOWN_TO_MAX

    @pytest.mark.asyncio
    async def test_deactivate_device_alert_pref_scd2_closes_active_row_in_place(
        self, repo: SQLAlchemyRepository
    ) -> None:
        """Active row drops out of list + closed projection is returned."""
        device_pid = await _seed_user_and_device(repo, user_public_id="user-revoke-1")
        pref_pid = await repo.upsert_device_alert_pref(
            DeviceAlertPrefUpsertRow(
                device_public_id=device_pid,
                alert_type="margin_warning",
                enabled=True,
                min_priority="high",
                session_id="s1",
                sequence_id=1,
                timestamp=_ts(),
            )
        )
        revoke_at = _ts(1)

        closed = await repo.deactivate_device_alert_pref_scd2(
            pref_pid, device_public_id=device_pid, timestamp=revoke_at
        )

        assert closed is not None
        assert closed["public_id"] == pref_pid
        assert closed["alert_type"] == "margin_warning"
        assert closed["min_priority"] == "high"
        post = await repo.list_device_alert_prefs_for_user("user-revoke-1")
        assert post == []

    @pytest.mark.asyncio
    async def test_deactivate_device_alert_pref_scd2_idempotent_returns_none(
        self, repo: SQLAlchemyRepository
    ) -> None:
        """Second close-call on the already-closed pref returns None."""
        device_pid = await _seed_user_and_device(repo, user_public_id="user-revoke-2")
        pref_pid = await repo.upsert_device_alert_pref(
            DeviceAlertPrefUpsertRow(
                device_public_id=device_pid,
                alert_type="critical_system_error",
                session_id="s1",
                sequence_id=1,
                timestamp=_ts(),
            )
        )
        await repo.deactivate_device_alert_pref_scd2(
            pref_pid, device_public_id=device_pid, timestamp=_ts(1)
        )

        second = await repo.deactivate_device_alert_pref_scd2(
            pref_pid, device_public_id=device_pid, timestamp=_ts(2)
        )

        assert second is None

    @pytest.mark.asyncio
    async def test_deactivate_device_alert_pref_scd2_blocks_foreign_device(
        self, repo: SQLAlchemyRepository
    ) -> None:
        """Closing with a wrong ``device_public_id`` returns None — no leak across owners."""
        own_device = await _seed_user_and_device(repo, user_public_id="user-revoke-3")
        pref_pid = await repo.upsert_device_alert_pref(
            DeviceAlertPrefUpsertRow(
                device_public_id=own_device,
                alert_type="order_rejected",
                session_id="s1",
                sequence_id=1,
                timestamp=_ts(),
            )
        )

        result = await repo.deactivate_device_alert_pref_scd2(
            pref_pid,
            device_public_id="some-other-device-public-id",
            timestamp=_ts(1),
        )

        assert result is None
        post = await repo.list_device_alert_prefs_for_user("user-revoke-3")
        assert len(post) == 1
        assert post[0]["public_id"] == pref_pid

    @pytest.mark.asyncio
    async def test_deactivate_device_alert_pref_scd2_releases_unique_index_slot(
        self, repo: SQLAlchemyRepository
    ) -> None:
        """After close, a fresh upsert at the same scope tuple succeeds."""
        device_pid = await _seed_user_and_device(repo, user_public_id="user-revoke-4")
        first_pid = await repo.upsert_device_alert_pref(
            DeviceAlertPrefUpsertRow(
                device_public_id=device_pid,
                alert_type="order_fill_full",
                operator_public_id="op-x",
                enabled=False,
                min_priority="low",
                session_id="s1",
                sequence_id=1,
                timestamp=_ts(),
            )
        )
        await repo.deactivate_device_alert_pref_scd2(
            first_pid, device_public_id=device_pid, timestamp=_ts(1)
        )

        second_pid = await repo.upsert_device_alert_pref(
            DeviceAlertPrefUpsertRow(
                device_public_id=device_pid,
                alert_type="order_fill_full",
                operator_public_id="op-x",
                enabled=True,
                min_priority="high",
                session_id="s1",
                sequence_id=2,
                timestamp=_ts(2),
            )
        )

        assert second_pid != first_pid
        post = await repo.list_device_alert_prefs_for_user("user-revoke-4")
        assert len(post) == 1
        assert post[0]["public_id"] == second_pid
        assert post[0]["enabled"] is True


class TestUserAlertDefaultRepo:
    """SCD2 behaviour of user-level fallback preference methods."""

    @pytest.mark.asyncio
    async def test_upsert_and_list_user_alert_default_roundtrip(
        self, repo: SQLAlchemyRepository
    ) -> None:
        """Write then read returns the active SCD2 row with expected fields."""
        await repo.upsert_user_alert_default(
            UserAlertDefaultUpsertRow(
                user_public_id="user-f",
                alert_type="order_rejected",
                enabled=False,
                min_priority="high",
                session_id="s1",
                sequence_id=1,
                timestamp=_ts(),
            )
        )

        defaults = await repo.list_user_alert_defaults("user-f")

        assert len(defaults) == 1
        assert defaults[0]["alert_type"] == "order_rejected"
        assert defaults[0]["enabled"] is False
        assert defaults[0]["min_priority"] == "high"
        assert defaults[0]["known_to"] == KNOWN_TO_MAX


class TestAlertEventRepo:
    """SCD2 behaviour of the three alert_event methods."""

    @pytest.mark.asyncio
    async def test_insert_auto_fills_public_id_and_known_to(
        self, repo: SQLAlchemyRepository
    ) -> None:
        """Insert auto-generates public_id; known_to defaults to KNOWN_TO_MAX."""
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
        assert row["is_safety_critical"] is False

    @pytest.mark.asyncio
    async def test_list_recent_alerts_composite_cursor_pagination(
        self, repo: SQLAlchemyRepository
    ) -> None:
        """Opaque ``AlertListCursor`` returns strictly-earlier rows; unknown -> []."""
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
        after_latest = await repo.list_recent_alerts_for_user(
            "user-h",
            limit=10,
            before=AlertListCursor(timestamp=_ts(4), public_id=public_ids[4]),
        )
        unknown = await repo.list_recent_alerts_for_user(
            "user-h",
            limit=10,
            before=AlertListCursor(timestamp=_ts(999), public_id="nonexistent-cursor"),
        )

        assert [r["public_id"] for r in full] == list(reversed(public_ids))
        assert [r["public_id"] for r in after_latest] == list(reversed(public_ids[:4]))
        assert [r["public_id"] for r in unknown] == list(reversed(public_ids))

    @pytest.mark.asyncio
    async def test_list_recent_alerts_cursor_stable_after_anchor_revision(
        self, repo: SQLAlchemyRepository
    ) -> None:
        """Keyset cursor stays correct even if the anchor is SCD2-revised.

        Before R3 the cursor was just ``public_id`` and the repo re-read
        the anchor's current ``(timestamp, public_id)`` — a mid-session
        revision that moved the anchor's timestamp would silently shift
        pagination. With the opaque cursor, we filter on the snapshotted
        pair directly, so paging is immune to anchor churn.
        """
        public_ids: list[str] = []
        for idx in range(3):
            public_ids.append(
                await repo.insert_alert_event(
                    AlertEventInsertRow(
                        session_id="s1",
                        sequence_id=idx,
                        timestamp=_ts(idx),
                        user_public_id="user-stable",
                        alert_type="order_fill_full",
                        priority="medium",
                        title=f"Alert {idx}",
                        body="body",
                    )
                )
            )
        anchor = AlertListCursor(timestamp=_ts(2), public_id=public_ids[2])
        async with repo.session() as s:

            await s.execute(
                AlertEvent.__table__.update()
                .where(AlertEvent.public_id == public_ids[2])
                .where(AlertEvent.known_to == KNOWN_TO_MAX)
                .values(timestamp=_ts(-100))
            )
            await s.commit()

        page = await repo.list_recent_alerts_for_user("user-stable", limit=10, before=anchor)

        assert [r["public_id"] for r in page] == [
            public_ids[1],
            public_ids[0],
            public_ids[2],
        ]

    @pytest.mark.asyncio
    async def test_get_alert_event_returns_none_for_unknown_public_id(
        self, repo: SQLAlchemyRepository
    ) -> None:
        """Missing public_id yields None."""
        row = await repo.get_alert_event_by_public_id("nope")

        assert row is None

    @pytest.mark.asyncio
    async def test_list_alert_events_with_dedup_key_window(
        self, repo: SQLAlchemyRepository
    ) -> None:
        """Only events matching (user, dedup_key, timestamp >= since) return.

        Drives the rule-side suppression helper: a 2-event
        fixture — one inside the window, one outside — verifies the
        since-bound is inclusive on the lower edge + exact key match
        on dedup_key.
        """
        inside = await repo.insert_alert_event(
            AlertEventInsertRow(
                session_id="s1",
                sequence_id=1,
                timestamp=_ts(5),
                user_public_id="user-dedup",
                alert_type="order_fill_full",
                priority="medium",
                title="Filled",
                body="BTC-USD 0.1 filled",
                dedup_key="order_fill_full.coid-1",
            )
        )
        await repo.insert_alert_event(
            AlertEventInsertRow(
                session_id="s1",
                sequence_id=2,
                timestamp=_ts(1),
                user_public_id="user-dedup",
                alert_type="order_fill_full",
                priority="medium",
                title="Filled",
                body="BTC-USD 0.1 filled",
                dedup_key="order_fill_full.coid-1",
            )
        )
        await repo.insert_alert_event(
            AlertEventInsertRow(
                session_id="s1",
                sequence_id=3,
                timestamp=_ts(5),
                user_public_id="user-dedup",
                alert_type="order_fill_full",
                priority="medium",
                title="Filled",
                body="other coid",
                dedup_key="order_fill_full.coid-2",
            )
        )

        within_window = await repo.list_alert_events_with_dedup_key(
            "user-dedup", "order_fill_full.coid-1", since=_ts(3)
        )
        different_user = await repo.list_alert_events_with_dedup_key(
            "user-other", "order_fill_full.coid-1", since=_ts(0)
        )

        assert len(within_window) == 1
        assert within_window[0]["public_id"] == inside
        assert different_user == []

    @pytest.mark.asyncio
    async def test_list_alert_events_with_dedup_key_empty_when_no_match(
        self, repo: SQLAlchemyRepository
    ) -> None:
        """Unknown (user, dedup_key) pair returns an empty list, not None."""
        result = await repo.list_alert_events_with_dedup_key(
            "ghost-user", "order_fill_full.no-such-key", since=_ts(0)
        )
        assert result == []


class TestAlertDeliveryRepo:
    """SCD2 behaviour of the seven delivery methods."""

    @pytest.mark.asyncio
    async def test_insert_and_list_queued_deliveries(self, repo: SQLAlchemyRepository) -> None:
        """SCD2 insert creates an active queued row."""
        public_id = await repo.insert_alert_delivery(
            AlertDeliveryInsertRow(
                alert_event_public_id="evt-1",
                device_public_id="dev-1",
                user_public_id="user-i",
                status="queued",
                created_at=_ts(),
                session_id="s1",
                sequence_id=1,
                timestamp=_ts(),
            )
        )

        queued = await repo.list_queued_deliveries_all()

        assert len(queued) == 1
        assert queued[0]["public_id"] == public_id
        assert queued[0]["status"] == "queued"
        assert queued[0]["known_to"] == KNOWN_TO_MAX

    @pytest.mark.asyncio
    async def test_mark_delivery_sent_transitions_via_scd2_close_insert(
        self, repo: SQLAlchemyRepository
    ) -> None:
        """Sent transition closes queued + inserts new active row with status=sent."""
        public_id = await repo.insert_alert_delivery(
            AlertDeliveryInsertRow(
                alert_event_public_id="evt-1",
                device_public_id="dev-1",
                user_public_id="user-i",
                status="queued",
                created_at=_ts(),
                session_id="s1",
                sequence_id=1,
                timestamp=_ts(),
            )
        )
        await repo.mark_delivery_sent(
            public_id,
            apns_id="apns-123",
            transition_at=_ts(1),
            session_id="s1",
            sequence_id=2,
        )

        queued_after = await repo.list_queued_deliveries_all()

        assert queued_after == []

    @pytest.mark.asyncio
    async def test_list_deliveries_ready_for_retry_filters_future_next_attempts(
        self, repo: SQLAlchemyRepository
    ) -> None:
        """Rows with ``next_attempt_at`` strictly in the future are excluded."""
        now = _ts()
        future = _ts(10)
        past = _ts(-10)

        ready_pid = await repo.insert_alert_delivery(
            AlertDeliveryInsertRow(
                alert_event_public_id="evt-1",
                device_public_id="dev-1",
                user_public_id="user-j",
                status="queued",
                next_attempt_at=past,
                created_at=now,
                session_id="s1",
                sequence_id=1,
                timestamp=now,
            )
        )
        await repo.insert_alert_delivery(
            AlertDeliveryInsertRow(
                alert_event_public_id="evt-1",
                device_public_id="dev-2",
                user_public_id="user-j",
                status="queued",
                next_attempt_at=future,
                created_at=now,
                session_id="s1",
                sequence_id=2,
                timestamp=now,
            )
        )
        immediate_pid = await repo.insert_alert_delivery(
            AlertDeliveryInsertRow(
                alert_event_public_id="evt-2",
                device_public_id="dev-3",
                user_public_id="user-j",
                status="queued",
                created_at=now,
                session_id="s1",
                sequence_id=3,
                timestamp=now,
            )
        )

        ready = await repo.list_deliveries_ready_for_retry(now)

        ready_ids = {r["public_id"] for r in ready}
        assert ready_ids == {ready_pid, immediate_pid}


class TestListUsersWithPermission:
    """Rule 4 fan-out helper."""

    @pytest.mark.asyncio
    async def test_returns_users_with_matching_role(self, repo: SQLAlchemyRepository) -> None:
        """Only active users whose role grants the permission are returned."""
        async with repo.session() as s:
            s.add_all(
                [
                    User(
                        public_id="admin-user",
                        username="admin",
                        email="admin@example.test",
                        password_hash="x",
                        role="admin",
                        created_at=_ts(),
                        session_id="seed",
                        sequence_id=1,
                        timestamp=_ts(),
                        known_to=KNOWN_TO_MAX,
                    ),
                    User(
                        public_id="viewer-user",
                        username="viewer",
                        email="viewer@example.test",
                        password_hash="x",
                        role="viewer",
                        created_at=_ts(),
                        session_id="seed",
                        sequence_id=2,
                        timestamp=_ts(),
                        known_to=KNOWN_TO_MAX,
                    ),
                ]
            )
            await s.commit()

        result = await repo.list_users_with_permission("read:system_status")

        assert set(result) == {"admin-user", "viewer-user"}

    @pytest.mark.asyncio
    async def test_unknown_permission_returns_empty(self, repo: SQLAlchemyRepository) -> None:
        """A permission no role grants yields an empty list (no error)."""
        result = await repo.list_users_with_permission("unknown:permission")
        assert result == []


class TestCancelDeliveriesForUser:
    """``cancel_pending_deliveries_for_user`` admin kill-switch helper."""

    @pytest.mark.asyncio
    async def test_cancels_every_queued_delivery_for_user(self, repo: SQLAlchemyRepository) -> None:
        """All queued rows with matching user_public_id transition to cancelled_scope."""
        device_pid = await _seed_user_and_device(repo, user_public_id="u-kill")
        for idx in range(2):
            await repo.insert_alert_delivery(
                AlertDeliveryInsertRow(
                    alert_event_public_id=f"evt-{idx}",
                    device_public_id=device_pid,
                    user_public_id="u-kill",
                    status="queued",
                    created_at=_ts(),
                    session_id="s",
                    sequence_id=idx + 1,
                    timestamp=_ts(),
                )
            )

        cancelled = await repo.cancel_pending_deliveries_for_user(
            "u-kill",
            transition_at=_ts(5),
            session_id="s-admin",
            sequence_id=99,
        )

        assert cancelled == 2
        still_queued = await repo.list_queued_deliveries_all()
        assert still_queued == []

    @pytest.mark.asyncio
    async def test_ignores_other_users(self, repo: SQLAlchemyRepository) -> None:
        """Deliveries for a different user are untouched."""
        await _seed_user_and_device(
            repo, user_public_id="u-keep", device_token="keep-tok", sequence_id=1
        )
        await _seed_user_and_device(
            repo, user_public_id="u-kill", device_token="kill-tok", sequence_id=2
        )
        await repo.insert_alert_delivery(
            AlertDeliveryInsertRow(
                alert_event_public_id="evt-keep",
                device_public_id="any",
                user_public_id="u-keep",
                status="queued",
                created_at=_ts(),
                session_id="s",
                sequence_id=1,
                timestamp=_ts(),
            )
        )

        cancelled = await repo.cancel_pending_deliveries_for_user(
            "u-kill",
            transition_at=_ts(5),
            session_id="s-admin",
            sequence_id=1,
        )

        assert cancelled == 0
        still_queued = await repo.list_queued_deliveries_all()
        assert len(still_queued) == 1


class TestBulkCancelRaceSafety:
    """Bulk cancel helpers are race-safe against concurrent transitions."""

    @pytest.mark.asyncio
    async def test_scope_cancel_skips_row_already_transitioned(
        self, repo: SQLAlchemyRepository
    ) -> None:
        """A row transitioned to ``sent`` mid-cancel is skipped silently.

        Closes ``gpt-5.3-codex`` final-review Critical #1: a naive
        close+insert on a row the retry loop already transitioned to
        ``sent`` collided on the partial-unique
        ``(public_id, known_to=MAX)`` index and raised
        ``IntegrityError``. The R1 pattern does an atomic close
        ``UPDATE ... WHERE status='queued' AND known_to=MAX`` and
        only inserts the ``cancelled_scope`` successor when the
        rowcount is 1. A winner that already transitioned the row
        leaves our close with rowcount==0 → skip, no exception.
        """
        user = "u-race"
        device_pid = await _seed_user_and_device(repo, user_public_id=user)
        delivery_pid = await repo.insert_alert_delivery(
            AlertDeliveryInsertRow(
                alert_event_public_id="evt-race",
                device_public_id=device_pid,
                user_public_id=user,
                operator_public_id="op-race",
                wallet_public_id="wal-race",
                status="queued",
                created_at=_ts(),
                session_id="s",
                sequence_id=1,
                timestamp=_ts(),
            )
        )
        await repo.mark_delivery_sent(
            delivery_pid,
            apns_id="apns-early",
            transition_at=_ts(1),
            session_id="s-retry",
            sequence_id=2,
        )

        cancelled = await repo.cancel_pending_deliveries_for_scope(
            user_public_id=user,
            operator_public_id="op-race",
            wallet_public_id="wal-race",
            transition_at=_ts(2),
            session_id="s-admin",
            sequence_id=3,
        )

        assert cancelled == 0
        async with repo.session() as s:
            all_rows = list(
                (
                    await s.execute(
                        _select(AlertDelivery).where(AlertDelivery.public_id == delivery_pid)
                    )
                )
                .scalars()
                .all()
            )
        active_statuses = {r.status for r in all_rows if r.known_to == KNOWN_TO_MAX}
        assert active_statuses == {"sent"}

    @pytest.mark.asyncio
    async def test_user_cancel_skips_row_already_transitioned(
        self, repo: SQLAlchemyRepository
    ) -> None:
        """``cancel_pending_deliveries_for_user`` inherits the same guard."""
        user = "u-race-2"
        device_pid = await _seed_user_and_device(repo, user_public_id=user)
        delivery_pid = await repo.insert_alert_delivery(
            AlertDeliveryInsertRow(
                alert_event_public_id="evt-race-2",
                device_public_id=device_pid,
                user_public_id=user,
                status="queued",
                created_at=_ts(),
                session_id="s",
                sequence_id=1,
                timestamp=_ts(),
            )
        )
        await repo.mark_delivery_failed(
            delivery_pid,
            error_reason="concurrent failure",
            transition_at=_ts(1),
            session_id="s-retry",
            sequence_id=2,
        )

        cancelled = await repo.cancel_pending_deliveries_for_user(
            user,
            transition_at=_ts(2),
            session_id="s-admin",
            sequence_id=3,
        )

        assert cancelled == 0

    @pytest.mark.asyncio
    async def test_bulk_cancel_handles_mid_session_close_race(
        self, repo: SQLAlchemyRepository, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Between SELECT and per-row atomic UPDATE, a competitor can close.

        This test mimics a strict concurrency race that ``_scd2_transition_delivery``
        already handles: the SELECT snapshots an active queued row,
        but by the time the per-row atomic close UPDATE runs, a
        competitor has already closed it. The rowcount==0 branch must
        skip the successor insert so the partial-unique
        ``(public_id, known_to=MAX)`` index is not stressed. We
        reproduce the race by closing the row via a raw SQL UPDATE
        after the ORM has cached its ``AlertDelivery`` instance, then
        invoking the bulk helper.
        """
        user = "u-race-3"
        device_pid = await _seed_user_and_device(repo, user_public_id=user)
        delivery_pid = await repo.insert_alert_delivery(
            AlertDeliveryInsertRow(
                alert_event_public_id="evt-race-3",
                device_public_id=device_pid,
                user_public_id=user,
                operator_public_id="op-3",
                wallet_public_id="wal-3",
                status="queued",
                created_at=_ts(),
                session_id="s",
                sequence_id=1,
                timestamp=_ts(),
            )
        )

        async def _race_close_between_select_and_update(
            s: AsyncSession,
            *,
            user_public_id: str,
            operator_public_id: str,
            wallet_public_id: str,
        ) -> list[AlertDelivery]:
            rows = (
                (
                    await s.execute(
                        _select(AlertDelivery).where(
                            AlertDelivery.status == "queued",
                            AlertDelivery.known_to == KNOWN_TO_MAX,
                            AlertDelivery.user_public_id == user_public_id,
                            AlertDelivery.operator_public_id == operator_public_id,
                            AlertDelivery.wallet_public_id == wallet_public_id,
                        )
                    )
                )
                .scalars()
                .all()
            )
            async with repo.session() as competitor:
                await competitor.execute(
                    _update(AlertDelivery)
                    .where(
                        AlertDelivery.public_id == delivery_pid,
                        AlertDelivery.known_to == KNOWN_TO_MAX,
                    )
                    .values(known_to=_ts(1))
                )
                await competitor.commit()
            return list(rows)

        monkeypatch.setattr(
            repository_module.SQLAlchemyRepository,
            "_select_queued_deliveries_by_scope",
            staticmethod(_race_close_between_select_and_update),
        )
        cancelled = await repo.cancel_pending_deliveries_for_scope(
            user_public_id=user,
            operator_public_id="op-3",
            wallet_public_id="wal-3",
            transition_at=_ts(2),
            session_id="s-admin",
            sequence_id=3,
        )

        assert cancelled == 0

    @pytest.mark.asyncio
    async def test_user_bulk_cancel_handles_mid_session_close_race(
        self, repo: SQLAlchemyRepository, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """``cancel_pending_deliveries_for_user`` honours the same race guard."""
        user = "u-race-4"
        device_pid = await _seed_user_and_device(repo, user_public_id=user)
        delivery_pid = await repo.insert_alert_delivery(
            AlertDeliveryInsertRow(
                alert_event_public_id="evt-race-4",
                device_public_id=device_pid,
                user_public_id=user,
                status="queued",
                created_at=_ts(),
                session_id="s",
                sequence_id=1,
                timestamp=_ts(),
            )
        )

        async def _race_close(s: AsyncSession, user_public_id: str) -> list[AlertDelivery]:
            rows = (
                (
                    await s.execute(
                        _select(AlertDelivery).where(
                            AlertDelivery.status == "queued",
                            AlertDelivery.known_to == KNOWN_TO_MAX,
                            AlertDelivery.user_public_id == user_public_id,
                        )
                    )
                )
                .scalars()
                .all()
            )
            async with repo.session() as competitor:
                await competitor.execute(
                    _update(AlertDelivery)
                    .where(
                        AlertDelivery.public_id == delivery_pid,
                        AlertDelivery.known_to == KNOWN_TO_MAX,
                    )
                    .values(known_to=_ts(1))
                )
                await competitor.commit()
            return list(rows)

        monkeypatch.setattr(
            repository_module.SQLAlchemyRepository,
            "_select_queued_deliveries_by_user",
            staticmethod(_race_close),
        )
        cancelled = await repo.cancel_pending_deliveries_for_user(
            user,
            transition_at=_ts(2),
            session_id="s-admin",
            sequence_id=3,
        )

        assert cancelled == 0


class TestCountDeliveriesByStatus:
    """Status aggregation for ``GET /api/metrics/notifications``."""

    @pytest.mark.asyncio
    async def test_counts_only_active_scd2_rows_per_status(
        self, repo: SQLAlchemyRepository
    ) -> None:
        """Each terminal status gets its own count; closed SCD2 versions excluded."""
        device_pid = await _seed_user_and_device(repo, user_public_id="u-m")
        sent_pid = await repo.insert_alert_delivery(
            AlertDeliveryInsertRow(
                alert_event_public_id="evt-s",
                device_public_id=device_pid,
                user_public_id="u-m",
                status="queued",
                created_at=_ts(),
                session_id="s",
                sequence_id=1,
                timestamp=_ts(),
            )
        )
        queued_pid = await repo.insert_alert_delivery(
            AlertDeliveryInsertRow(
                alert_event_public_id="evt-q",
                device_public_id=device_pid,
                user_public_id="u-m",
                status="queued",
                created_at=_ts(),
                session_id="s",
                sequence_id=2,
                timestamp=_ts(),
            )
        )
        await repo.mark_delivery_sent(
            sent_pid,
            apns_id="apns-1",
            transition_at=_ts(1),
            session_id="s",
            sequence_id=3,
        )

        counts = await repo.count_deliveries_by_status()

        assert counts.get("sent") == 1
        assert counts.get("queued") == 1
        assert queued_pid

    @pytest.mark.asyncio
    async def test_empty_outbox_returns_empty_dict(self, repo: SQLAlchemyRepository) -> None:
        """No rows → empty dict (callers must ``.get(..., 0)`` for zero-default)."""
        counts = await repo.count_deliveries_by_status()
        assert counts == {}


class TestScopeHelpers:
    """Behaviour of the three scope-cascade helpers."""

    @pytest.mark.asyncio
    async def test_cancel_pending_deliveries_filters_by_delivery_scope_cols(
        self, repo: SQLAlchemyRepository
    ) -> None:
        """Cancel bulk-transitions matching scope via denormalised cols (no event JOIN).

        This is the closure for the Copilot R1 finding on SCD2-join
        correctness: even if the source ``alert_event`` is later closed
        or its scope changes, the scope-cancel path filters on the
        delivery row's own denormalised columns.
        """
        matching_pid = await repo.insert_alert_delivery(
            AlertDeliveryInsertRow(
                alert_event_public_id="evt-1",
                device_public_id="dev-1",
                user_public_id="user-k",
                operator_public_id="op-k",
                wallet_public_id="wallet-k",
                status="queued",
                created_at=_ts(),
                session_id="s1",
                sequence_id=1,
                timestamp=_ts(),
            )
        )
        untouched_pid = await repo.insert_alert_delivery(
            AlertDeliveryInsertRow(
                alert_event_public_id="evt-2",
                device_public_id="dev-2",
                user_public_id="user-k",
                operator_public_id="op-other",
                wallet_public_id="wallet-other",
                status="queued",
                created_at=_ts(),
                session_id="s1",
                sequence_id=2,
                timestamp=_ts(),
            )
        )

        cancelled = await repo.cancel_pending_deliveries_for_scope(
            user_public_id="user-k",
            operator_public_id="op-k",
            wallet_public_id="wallet-k",
            transition_at=_ts(5),
            session_id="s1",
            sequence_id=3,
        )

        queued_after = await repo.list_queued_deliveries_all()

        assert cancelled == 1
        assert [q["public_id"] for q in queued_after] == [untouched_pid]
        assert matching_pid != untouched_pid

    @pytest.mark.asyncio
    async def test_is_scope_grant_active_requires_both_grant_and_membership(
        self, repo: SQLAlchemyRepository
    ) -> None:
        """Missing membership + present grant -> False; both present -> True."""
        now = _ts()
        async with repo.session() as s:
            user = User(
                public_id="user-l",
                username="l",
                email="l@example.test",
                password_hash="x",
                role="viewer",
                created_at=now,
                session_id="t",
                sequence_id=1,
                timestamp=now,
                known_to=KNOWN_TO_MAX,
            )
            operator = Operator(
                public_id="op-l",
                label="Op L",
                description=None,
                session_id="t",
                sequence_id=2,
                timestamp=now,
                known_to=KNOWN_TO_MAX,
            )
            wallet = Wallet(
                label="w-l",
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
                public_id="grant-l",
                operator_public_id="op-l",
                wallet_public_id=wallet.public_id,
                granted_by_user_public_id="user-l",
                scope_kind="underlying",
                underlying_public_id="underlying-l",
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
            user_public_id="user-l",
            operator_public_id="op-l",
            wallet_public_id=wallet_public_id,
            as_of=now,
        )

        async with repo.session() as s:
            s.add(
                UserOperatorMembership(
                    user_public_id="user-l",
                    operator_public_id="op-l",
                    session_id="t",
                    sequence_id=5,
                    timestamp=now,
                    known_to=KNOWN_TO_MAX,
                )
            )
            await s.commit()

        with_membership = await repo.is_scope_grant_active(
            user_public_id="user-l",
            operator_public_id="op-l",
            wallet_public_id=wallet_public_id,
            as_of=now,
        )

        assert without_membership is False
        assert with_membership is True

    @pytest.mark.asyncio
    async def test_is_scope_grant_active_false_when_grant_missing(
        self, repo: SQLAlchemyRepository
    ) -> None:
        """No grant at all -> False (early exit branch)."""
        result = await repo.is_scope_grant_active(
            user_public_id="unknown-user",
            operator_public_id="unknown-op",
            wallet_public_id="unknown-wallet",
            as_of=_ts(),
        )

        assert result is False

    @pytest.mark.asyncio
    async def test_list_users_with_operator_membership_returns_distinct_user_ids(
        self, repo: SQLAlchemyRepository
    ) -> None:
        """Distinct active memberships under an operator produce their user public_ids."""
        now = _ts()
        async with repo.session() as s:
            s.add_all(
                [
                    User(
                        public_id="user-mem-a",
                        username="mem_a",
                        email="mem_a@example.test",
                        password_hash="x",
                        role="viewer",
                        created_at=now,
                        session_id="t",
                        sequence_id=1,
                        timestamp=now,
                        known_to=KNOWN_TO_MAX,
                    ),
                    User(
                        public_id="user-mem-b",
                        username="mem_b",
                        email="mem_b@example.test",
                        password_hash="x",
                        role="viewer",
                        created_at=now,
                        session_id="t",
                        sequence_id=2,
                        timestamp=now,
                        known_to=KNOWN_TO_MAX,
                    ),
                    Operator(
                        public_id="op-mem",
                        label="Op Mem",
                        description=None,
                        session_id="t",
                        sequence_id=3,
                        timestamp=now,
                        known_to=KNOWN_TO_MAX,
                    ),
                    UserOperatorMembership(
                        user_public_id="user-mem-a",
                        operator_public_id="op-mem",
                        session_id="t",
                        sequence_id=4,
                        timestamp=now,
                        known_to=KNOWN_TO_MAX,
                    ),
                    UserOperatorMembership(
                        user_public_id="user-mem-b",
                        operator_public_id="op-mem",
                        session_id="t",
                        sequence_id=5,
                        timestamp=now,
                        known_to=KNOWN_TO_MAX,
                    ),
                ]
            )
            await s.commit()

        users = await repo.list_users_with_operator_membership("op-mem", as_of=now)

        assert set(users) == {"user-mem-a", "user-mem-b"}


class TestConcurrencyInvariants:
    """Close Copilot R2 findings: atomic close + idempotent upsert convergence.

    We cannot easily drive true concurrent commits against a single
    SQLite file (writes serialise), but we can drive the "lost-race"
    branches by simulating their pre-conditions (an already-closed
    active row for the transition path; a winner row already committed
    for the upsert path). That is enough to prove the atomic-close
    predicate and IntegrityError recovery code is entered and behaves
    correctly.
    """

    @pytest.mark.asyncio
    async def test_mark_delivery_sent_idempotent_when_already_sent(
        self, repo: SQLAlchemyRepository
    ) -> None:
        """Retry after a successful send is a no-op (no spurious successor row).

        Before R3 a racing timeout+retry could both pass the
        ``require_queued`` guard in Python and both emit close+insert;
        the second would collide on the active-``public_id`` partial
        unique index and leak an IntegrityError. Now the second call
        short-circuits on ``existing.status == new_status``.
        """
        public_id = await repo.insert_alert_delivery(
            AlertDeliveryInsertRow(
                alert_event_public_id="evt-1",
                device_public_id="dev-1",
                user_public_id="user-concurrency-1",
                status="queued",
                created_at=_ts(),
                session_id="s1",
                sequence_id=1,
                timestamp=_ts(),
            )
        )
        await repo.mark_delivery_sent(
            public_id,
            apns_id="apns-a",
            transition_at=_ts(1),
            session_id="s1",
            sequence_id=2,
        )

        await repo.mark_delivery_sent(
            public_id,
            apns_id="apns-b",
            transition_at=_ts(2),
            session_id="s1",
            sequence_id=3,
        )

        async with repo.session() as s:

            rows = (
                (
                    await s.execute(
                        _select(AlertDelivery).where(AlertDelivery.public_id == public_id)
                    )
                )
                .scalars()
                .all()
            )
        assert len(rows) == 2
        active = [r for r in rows if r.known_to == KNOWN_TO_MAX]
        assert len(active) == 1
        assert active[0].status == "sent"
        assert active[0].apns_id == "apns-a"

    @pytest.mark.asyncio
    async def test_mark_delivery_sent_noop_when_row_preclosed(
        self, repo: SQLAlchemyRepository
    ) -> None:
        """Lost-race branch: atomic close predicate returns rowcount==0.

        We simulate the loser's view by closing the active row via a
        raw UPDATE before the second ``mark_delivery_sent`` fires. The
        second call must see no active row and return False without
        inserting a successor (otherwise it would create a
        dangling-status row and/or collide with the partial-unique).
        """
        public_id = await repo.insert_alert_delivery(
            AlertDeliveryInsertRow(
                alert_event_public_id="evt-1",
                device_public_id="dev-1",
                user_public_id="user-concurrency-2",
                status="queued",
                created_at=_ts(),
                session_id="s1",
                sequence_id=1,
                timestamp=_ts(),
            )
        )

        async with repo.session() as s:
            await s.execute(
                _update(AlertDelivery)
                .where(AlertDelivery.public_id == public_id)
                .where(AlertDelivery.known_to == KNOWN_TO_MAX)
                .values(known_to=_ts(1))
            )
            await s.commit()

        await repo.mark_delivery_sent(
            public_id,
            apns_id="apns-late",
            transition_at=_ts(2),
            session_id="s1",
            sequence_id=2,
        )

        async with repo.session() as s:

            rows = (
                (
                    await s.execute(
                        _select(AlertDelivery).where(AlertDelivery.public_id == public_id)
                    )
                )
                .scalars()
                .all()
            )
        assert len(rows) == 1
        assert rows[0].status == "queued"
        assert rows[0].known_to != KNOWN_TO_MAX

    @pytest.mark.asyncio
    async def test_update_delivery_retry_schedule_emits_successor_with_same_status(
        self, repo: SQLAlchemyRepository
    ) -> None:
        """Pure schedule-bump (queued->queued) still emits a successor row.

        The same-status short-circuit in ``_scd2_transition_delivery``
        must not fire when any ``*_override`` is provided — otherwise
        ``update_delivery_retry_schedule`` becomes a no-op and the
        retry counter never advances.
        """
        public_id = await repo.insert_alert_delivery(
            AlertDeliveryInsertRow(
                alert_event_public_id="evt-1",
                device_public_id="dev-1",
                user_public_id="user-sched",
                status="queued",
                created_at=_ts(),
                session_id="s1",
                sequence_id=1,
                timestamp=_ts(),
            )
        )

        await repo.update_delivery_retry_schedule(
            public_id,
            attempt_count=2,
            next_attempt_at=_ts(30),
            error_reason="transient",
            transition_at=_ts(1),
            session_id="s1",
            sequence_id=2,
        )

        async with repo.session() as s:

            rows = (
                (
                    await s.execute(
                        _select(AlertDelivery).where(AlertDelivery.public_id == public_id)
                    )
                )
                .scalars()
                .all()
            )
        assert len(rows) == 2
        active = [r for r in rows if r.known_to == KNOWN_TO_MAX][0]
        assert active.status == "queued"
        assert active.attempt_count == 2
        assert active.next_attempt_at == _ts(30)
        assert active.error_reason == "transient"

    @pytest.mark.asyncio
    async def test_upsert_notification_device_converges_on_competitor_winner(
        self, repo: SQLAlchemyRepository
    ) -> None:
        """Upsert races converge idempotently on the winner's public_id.

        We pre-insert a "winner" row directly (simulating a competing
        transaction that got there first) and then call the public
        upsert. The retry loop must rollback its initial insert
        attempt, re-read the winner's active row, close it, and
        insert a new active version keyed on the winner's stable
        ``public_id`` — never leaking IntegrityError to the caller.
        """
        async with repo.session() as s:
            s.add(
                User(
                    public_id="user-race",
                    username="u_race",
                    email="race@example.test",
                    password_hash="x",
                    role="viewer",
                    created_at=_ts(),
                    session_id="t",
                    sequence_id=1,
                    timestamp=_ts(),
                    known_to=KNOWN_TO_MAX,
                )
            )
            s.add(
                NotificationDevice(
                    public_id="winner-pid",
                    session_id="t",
                    sequence_id=2,
                    timestamp=_ts(),
                    known_to=KNOWN_TO_MAX,
                    user_public_id="user-race",
                    device_token="shared-token",
                    device_id="dev-race-winner",
                    platform="ios",
                    env="sandbox",
                    app_version=None,
                    previews_mode="private",
                    registered_at=_ts(),
                    last_seen_at=None,
                )
            )
            await s.commit()

        returned_pid = await repo.upsert_notification_device(
            NotificationDeviceUpsertRow(
                public_id="loser-pid",
                session_id="s-caller",
                sequence_id=1,
                timestamp=_ts(1),
                user_public_id="user-race",
                device_token="shared-token",
                device_id="dev-race-caller",
                env="sandbox",
                registered_at=_ts(1),
            )
        )

        assert returned_pid == "winner-pid"
        devices = await repo.list_active_notification_devices_for_user("user-race")
        assert len(devices) == 1
        assert devices[0]["public_id"] == "winner-pid"
        assert devices[0]["device_id"] == "dev-race-caller"

    @pytest.mark.asyncio
    async def test_upsert_notification_device_concurrent_same_token_recovery(
        self,
        repo: SQLAlchemyRepository,
    ) -> None:
        """Two concurrent same-token upserts converge via IntegrityError retry.

        Both tasks SELECT no active row simultaneously, generate
        distinct candidate ``public_id``s, and race their INSERTs.
        The partial unique index on ``(device_token) WHERE
        known_to=MAX`` rejects the loser; the retry loop then
        re-reads the winner and close+inserts a new active version
        keyed on the winner's ``public_id``. Both tasks return the
        same ``public_id`` — idempotent from the caller's view.
        """
        async with repo.session() as s:
            s.add(
                User(
                    public_id="user-concurrent",
                    username="u_concurrent",
                    email="concurrent@example.test",
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

        async def do_upsert(seq: int) -> str:
            return await repo.upsert_notification_device(
                NotificationDeviceUpsertRow(
                    session_id="s-conc",
                    sequence_id=seq,
                    timestamp=_ts(seq),
                    user_public_id="user-concurrent",
                    device_token="concurrent-token",
                    device_id=f"dev-{seq}",
                    env="sandbox",
                    registered_at=_ts(seq),
                )
            )

        results = await asyncio.gather(
            do_upsert(1),
            do_upsert(2),
            do_upsert(3),
            do_upsert(4),
            do_upsert(5),
        )

        assert len(set(results)) == 1
        devices = await repo.list_active_notification_devices_for_user("user-concurrent")
        assert len(devices) == 1

    @pytest.mark.asyncio
    async def test_upsert_device_alert_pref_concurrent_same_scope_recovery(
        self,
        repo: SQLAlchemyRepository,
    ) -> None:
        """Concurrent same-scope pref upserts converge via IntegrityError retry."""
        device_pid = await _seed_user_and_device(
            repo, user_public_id="user-pref-conc", device_token="t-pref-conc"
        )

        async def do_upsert(seq: int) -> None:
            await repo.upsert_device_alert_pref(
                DeviceAlertPrefUpsertRow(
                    session_id="s-conc",
                    sequence_id=seq,
                    timestamp=_ts(seq),
                    device_public_id=device_pid,
                    alert_type="order_fill_full",
                )
            )

        await asyncio.gather(do_upsert(1), do_upsert(2), do_upsert(3), do_upsert(4))

        prefs = await repo.list_device_alert_prefs_for_user("user-pref-conc")
        assert len(prefs) == 1
        assert prefs[0]["alert_type"] == "order_fill_full"

    @pytest.mark.asyncio
    async def test_upsert_user_alert_default_concurrent_same_key_recovery(
        self,
        repo: SQLAlchemyRepository,
    ) -> None:
        """Concurrent same-(user, alert_type) upserts converge via retry."""
        async with repo.session() as s:
            s.add(
                User(
                    public_id="user-def-conc",
                    username="u_def_conc",
                    email="defconc@example.test",
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

        async def do_upsert(seq: int) -> None:
            await repo.upsert_user_alert_default(
                UserAlertDefaultUpsertRow(
                    session_id="s-conc",
                    sequence_id=seq,
                    timestamp=_ts(seq),
                    user_public_id="user-def-conc",
                    alert_type="order_fill_full",
                )
            )

        await asyncio.gather(do_upsert(1), do_upsert(2), do_upsert(3), do_upsert(4))

        defaults = await repo.list_user_alert_defaults("user-def-conc")
        assert len(defaults) == 1
        assert defaults[0]["alert_type"] == "order_fill_full"

    @pytest.mark.asyncio
    async def test_mark_delivery_sent_loses_close_race_returns_false(
        self,
        repo: SQLAlchemyRepository,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Deterministically exercise the ``rowcount=0`` close-race branch.

        Installs an ``AsyncSession.execute`` interceptor that, exactly
        once, pre-closes the active ``alert_deliveries`` row via a
        side-session immediately before ``_scd2_transition_delivery``'s
        own conditional UPDATE fires. The method's UPDATE then sees
        ``known_to != MAX`` (we changed it) and returns ``rowcount=0``,
        so the helper rolls back and ``mark_delivery_sent`` exits
        without inserting a successor row — no spurious version, no
        IntegrityError leaks to the caller.
        """
        public_id = await repo.insert_alert_delivery(
            AlertDeliveryInsertRow(
                alert_event_public_id="evt-conc",
                device_public_id="dev-conc",
                user_public_id="user-delivery-conc",
                status="queued",
                created_at=_ts(),
                session_id="s1",
                sequence_id=1,
                timestamp=_ts(),
            )
        )

        original_execute = AsyncSession.execute
        sabotaged = {"count": 0}

        async def patched_execute(
            self_session: AsyncSession,
            statement: Any,
            *args: Any,
            **kwargs: Any,
        ) -> Any:
            stmt_text = str(statement)
            if (
                sabotaged["count"] == 0
                and "UPDATE alert_deliveries" in stmt_text
                and "SET known_to" in stmt_text
            ):
                sabotaged["count"] += 1
                side_cm = repo.session()
                side = await side_cm.__aenter__()
                try:
                    await original_execute(
                        side,
                        _update(AlertDelivery)
                        .where(
                            AlertDelivery.public_id == public_id,
                            AlertDelivery.known_to == KNOWN_TO_MAX,
                        )
                        .values(known_to=_ts(99)),
                    )
                    await side.commit()
                finally:
                    await side_cm.__aexit__(None, None, None)
            return await original_execute(self_session, statement, *args, **kwargs)

        monkeypatch.setattr(AsyncSession, "execute", patched_execute)

        await repo.mark_delivery_sent(
            public_id,
            apns_id="apns-loser",
            transition_at=_ts(2),
            session_id="s-conc",
            sequence_id=2,
        )

        assert sabotaged["count"] == 1
        async with repo.session() as s:
            rows = (
                (
                    await s.execute(
                        _select(AlertDelivery).where(AlertDelivery.public_id == public_id)
                    )
                )
                .scalars()
                .all()
            )
        active = [r for r in rows if r.known_to == KNOWN_TO_MAX]
        assert active == []
        assert all(r.status == "queued" for r in rows)

    @pytest.mark.asyncio
    async def test_upsert_notification_device_close_race_branch(
        self, repo: SQLAlchemyRepository, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Deterministically exercise the ``rowcount=0`` close-race branch.

        We install a session-``execute`` interceptor that, exactly once,
        pre-closes the active row via a side-session immediately before
        the method's own UPDATE fires. The method's UPDATE then sees
        ``known_to != MAX`` (we changed it) and returns rowcount=0,
        sending the loop through the ``last_error = close-race``
        continue branch. The second attempt takes the clean path and
        converges.
        """
        seed_pid = await repo.upsert_notification_device(
            NotificationDeviceUpsertRow(
                user_public_id="user-close-race",
                device_token="close-race-token",
                device_id="dev-seed",
                env="sandbox",
                registered_at=_ts(),
                session_id="s-seed",
                sequence_id=1,
                timestamp=_ts(),
            )
        )

        original_execute = AsyncSession.execute
        sabotaged = {"count": 0}

        async def patched_execute(
            self_session: AsyncSession,
            statement: Any,
            *args: Any,
            **kwargs: Any,
        ) -> Any:
            stmt_text = str(statement)
            if (
                sabotaged["count"] == 0
                and "UPDATE notification_devices" in stmt_text
                and "SET known_to" in stmt_text
            ):
                sabotaged["count"] += 1

                side_cm = repo.session()
                side = await side_cm.__aenter__()
                try:
                    await original_execute(
                        side,
                        _update(NotificationDevice)
                        .where(
                            NotificationDevice.public_id == seed_pid,
                            NotificationDevice.known_to == KNOWN_TO_MAX,
                        )
                        .values(known_to=_ts(99)),
                    )
                    await side.commit()
                finally:
                    await side_cm.__aexit__(None, None, None)
            return await original_execute(self_session, statement, *args, **kwargs)

        monkeypatch.setattr(AsyncSession, "execute", patched_execute)

        await repo.upsert_notification_device(
            NotificationDeviceUpsertRow(
                user_public_id="user-close-race",
                device_token="close-race-token",
                device_id="dev-second",
                env="sandbox",
                registered_at=_ts(2),
                session_id="s-second",
                sequence_id=2,
                timestamp=_ts(2),
            )
        )

        assert sabotaged["count"] == 1
        devices = await repo.list_active_notification_devices_for_user("user-close-race")
        assert len(devices) == 1
        assert devices[0]["device_id"] == "dev-second"

    @pytest.mark.asyncio
    async def test_upsert_notification_device_exhausts_retries_and_raises(
        self, repo: SQLAlchemyRepository, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Retry budget exhausted -> the last close-race error propagates.

        We intercept every close UPDATE on ``notification_devices`` and
        return a synthetic ``rowcount=0`` result without actually
        closing anything, so each attempt's SELECT still finds the
        original active row and each UPDATE reports a lost close race.
        After ``max_attempts`` iterations the loop exits and raises
        the last recorded close-race ``RuntimeError``.
        """
        await repo.upsert_notification_device(
            NotificationDeviceUpsertRow(
                user_public_id="u-exhaust",
                device_token="exhaust-token",
                device_id="dev-seed",
                env="sandbox",
                registered_at=_ts(),
                session_id="s-seed",
                sequence_id=1,
                timestamp=_ts(),
            )
        )

        original_execute = AsyncSession.execute

        class _ZeroRowcountResult:
            rowcount = 0

        async def patched_execute(
            self_session: AsyncSession,
            statement: Any,
            *args: Any,
            **kwargs: Any,
        ) -> Any:
            stmt_text = str(statement)
            if "UPDATE notification_devices" in stmt_text and "SET known_to" in stmt_text:
                return _ZeroRowcountResult()
            return await original_execute(self_session, statement, *args, **kwargs)

        monkeypatch.setattr(AsyncSession, "execute", patched_execute)

        with pytest.raises(RuntimeError, match="close-race"):
            await repo.upsert_notification_device(
                NotificationDeviceUpsertRow(
                    user_public_id="u-exhaust",
                    device_token="exhaust-token",
                    device_id="dev-never",
                    env="sandbox",
                    registered_at=_ts(2),
                    session_id="s-never",
                    sequence_id=2,
                    timestamp=_ts(2),
                )
            )

    @pytest.mark.asyncio
    async def test_upsert_notification_device_integrityerror_retry(
        self, repo: SQLAlchemyRepository, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """IntegrityError on first commit -> rollback + retry -> success."""
        async with repo.session() as s:
            s.add(
                User(
                    public_id="u-nd-ie",
                    username="u_nd_ie",
                    email="nd_ie@example.test",
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

        original_commit = AsyncSession.commit
        tripped = {"n": 0}

        async def patched_commit(self_session: AsyncSession) -> None:
            if tripped["n"] == 0:
                tripped["n"] += 1
                await self_session.rollback()
                raise IntegrityError("synthetic", params=None, orig=Exception("partial-unique"))
            return await original_commit(self_session)

        monkeypatch.setattr(AsyncSession, "commit", patched_commit)

        pid = await repo.upsert_notification_device(
            NotificationDeviceUpsertRow(
                user_public_id="u-nd-ie",
                device_token="nd-ie-token",
                device_id="dev-ie",
                env="sandbox",
                registered_at=_ts(),
                session_id="s-ie",
                sequence_id=1,
                timestamp=_ts(),
            )
        )

        assert tripped["n"] == 1
        devices = await repo.list_active_notification_devices_for_user("u-nd-ie")
        assert len(devices) == 1
        assert devices[0]["public_id"] == pid

    @pytest.mark.asyncio
    async def test_upsert_device_alert_pref_close_race_branch(
        self, repo: SQLAlchemyRepository, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Close-race branch in ``upsert_device_alert_pref`` fires deterministically."""
        device_pid = await _seed_user_and_device(
            repo, user_public_id="u-pref-race", device_token="pref-race-token"
        )
        await repo.upsert_device_alert_pref(
            DeviceAlertPrefUpsertRow(
                session_id="s-seed",
                sequence_id=1,
                timestamp=_ts(),
                device_public_id=device_pid,
                alert_type="order_fill_full",
            )
        )

        original_execute = AsyncSession.execute
        sabotaged = {"count": 0}

        async def patched_execute(
            self_session: AsyncSession,
            statement: Any,
            *args: Any,
            **kwargs: Any,
        ) -> Any:
            stmt_text = str(statement)
            if (
                sabotaged["count"] == 0
                and "UPDATE device_alert_prefs" in stmt_text
                and "SET known_to" in stmt_text
            ):
                sabotaged["count"] += 1

                side_cm = repo.session()
                side = await side_cm.__aenter__()
                try:
                    await original_execute(
                        side,
                        _update(DeviceAlertPref)
                        .where(
                            DeviceAlertPref.device_public_id == device_pid,
                            DeviceAlertPref.alert_type == "order_fill_full",
                            DeviceAlertPref.known_to == KNOWN_TO_MAX,
                        )
                        .values(known_to=_ts(99)),
                    )
                    await side.commit()
                finally:
                    await side_cm.__aexit__(None, None, None)
            return await original_execute(self_session, statement, *args, **kwargs)

        monkeypatch.setattr(AsyncSession, "execute", patched_execute)

        await repo.upsert_device_alert_pref(
            DeviceAlertPrefUpsertRow(
                session_id="s-second",
                sequence_id=2,
                timestamp=_ts(2),
                device_public_id=device_pid,
                alert_type="order_fill_full",
                enabled=False,
            )
        )

        assert sabotaged["count"] == 1
        prefs = await repo.list_device_alert_prefs_for_user("u-pref-race")
        assert len(prefs) == 1
        assert prefs[0]["enabled"] is False

    @pytest.mark.asyncio
    async def test_upsert_device_alert_pref_exhausts_retries_and_raises(
        self, repo: SQLAlchemyRepository, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Close-race exhaust path in ``upsert_device_alert_pref``."""
        device_pid = await _seed_user_and_device(
            repo, user_public_id="u-pref-exhaust", device_token="pref-exhaust-token"
        )
        await repo.upsert_device_alert_pref(
            DeviceAlertPrefUpsertRow(
                session_id="s-seed",
                sequence_id=1,
                timestamp=_ts(),
                device_public_id=device_pid,
                alert_type="order_fill_full",
            )
        )

        original_execute = AsyncSession.execute

        class _ZeroRowcountResult:
            rowcount = 0

        async def patched_execute(
            self_session: AsyncSession,
            statement: Any,
            *args: Any,
            **kwargs: Any,
        ) -> Any:
            stmt_text = str(statement)
            if "UPDATE device_alert_prefs" in stmt_text and "SET known_to" in stmt_text:
                return _ZeroRowcountResult()
            return await original_execute(self_session, statement, *args, **kwargs)

        monkeypatch.setattr(AsyncSession, "execute", patched_execute)

        with pytest.raises(RuntimeError, match="close-race"):
            await repo.upsert_device_alert_pref(
                DeviceAlertPrefUpsertRow(
                    session_id="s-never",
                    sequence_id=2,
                    timestamp=_ts(2),
                    device_public_id=device_pid,
                    alert_type="order_fill_full",
                    enabled=False,
                )
            )

    @pytest.mark.asyncio
    async def test_upsert_device_alert_pref_integrityerror_retry(
        self, repo: SQLAlchemyRepository, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Integrity-error on commit -> rollback + retry -> successful commit."""
        device_pid = await _seed_user_and_device(
            repo, user_public_id="u-pref-ie", device_token="pref-ie-token"
        )

        original_commit = AsyncSession.commit
        tripped = {"n": 0}

        async def patched_commit(self_session: AsyncSession) -> None:
            if tripped["n"] == 0:
                tripped["n"] += 1
                await self_session.rollback()
                raise IntegrityError("synthetic", params=None, orig=Exception("partial-unique"))
            return await original_commit(self_session)

        monkeypatch.setattr(AsyncSession, "commit", patched_commit)

        await repo.upsert_device_alert_pref(
            DeviceAlertPrefUpsertRow(
                session_id="s-ie",
                sequence_id=1,
                timestamp=_ts(),
                device_public_id=device_pid,
                alert_type="order_fill_full",
            )
        )

        assert tripped["n"] == 1
        prefs = await repo.list_device_alert_prefs_for_user("u-pref-ie")
        assert len(prefs) == 1

    @pytest.mark.asyncio
    async def test_upsert_user_alert_default_close_race_exhausts(
        self, repo: SQLAlchemyRepository, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Close-race init + final raise in ``upsert_user_alert_default``.

        Same synthetic-``rowcount=0`` sabotage as the
        ``notification_devices`` exhaust test.
        """
        async with repo.session() as s:
            s.add(
                User(
                    public_id="u-def-exhaust",
                    username="u_def_exhaust",
                    email="def_exhaust@example.test",
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
        await repo.upsert_user_alert_default(
            UserAlertDefaultUpsertRow(
                session_id="s-seed",
                sequence_id=1,
                timestamp=_ts(),
                user_public_id="u-def-exhaust",
                alert_type="order_fill_full",
            )
        )

        original_execute = AsyncSession.execute

        class _ZeroRowcountResult:
            rowcount = 0

        async def patched_execute(
            self_session: AsyncSession,
            statement: Any,
            *args: Any,
            **kwargs: Any,
        ) -> Any:
            stmt_text = str(statement)
            if "UPDATE user_alert_defaults" in stmt_text and "SET known_to" in stmt_text:
                return _ZeroRowcountResult()
            return await original_execute(self_session, statement, *args, **kwargs)

        monkeypatch.setattr(AsyncSession, "execute", patched_execute)

        with pytest.raises(RuntimeError, match="close-race"):
            await repo.upsert_user_alert_default(
                UserAlertDefaultUpsertRow(
                    session_id="s-never",
                    sequence_id=2,
                    timestamp=_ts(2),
                    user_public_id="u-def-exhaust",
                    alert_type="order_fill_full",
                    enabled=False,
                )
            )

    @pytest.mark.asyncio
    async def test_upsert_user_alert_default_integrityerror_retry(
        self, repo: SQLAlchemyRepository, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Integrity-error on commit -> rollback + retry -> successful commit."""
        async with repo.session() as s:
            s.add(
                User(
                    public_id="u-def-ie",
                    username="u_def_ie",
                    email="def_ie@example.test",
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

        original_commit = AsyncSession.commit
        tripped = {"n": 0}

        async def patched_commit(self_session: AsyncSession) -> None:
            if tripped["n"] == 0:
                tripped["n"] += 1
                await self_session.rollback()
                raise IntegrityError("synthetic", params=None, orig=Exception("partial-unique"))
            return await original_commit(self_session)

        monkeypatch.setattr(AsyncSession, "commit", patched_commit)

        await repo.upsert_user_alert_default(
            UserAlertDefaultUpsertRow(
                session_id="s-ie",
                sequence_id=1,
                timestamp=_ts(),
                user_public_id="u-def-ie",
                alert_type="order_fill_full",
            )
        )

        assert tripped["n"] == 1
        defaults = await repo.list_user_alert_defaults("u-def-ie")
        assert len(defaults) == 1

    @pytest.mark.asyncio
    async def test_mark_delivery_failed_unregistered_cancelled_transitions(
        self, repo: SQLAlchemyRepository
    ) -> None:
        """Every terminal ``mark_delivery_*`` transitions queued -> {target}."""

        async def fresh_delivery(suffix: str) -> str:
            return await repo.insert_alert_delivery(
                AlertDeliveryInsertRow(
                    alert_event_public_id=f"evt-{suffix}",
                    device_public_id=f"dev-{suffix}",
                    user_public_id=f"u-{suffix}",
                    status="queued",
                    created_at=_ts(),
                    session_id="s1",
                    sequence_id=1,
                    timestamp=_ts(),
                )
            )

        failed_pid = await fresh_delivery("fail")
        unreg_pid = await fresh_delivery("unreg")
        cancel_pid = await fresh_delivery("cancel")

        await repo.mark_delivery_failed(
            failed_pid,
            error_reason="5xx-maxed",
            transition_at=_ts(1),
            session_id="s1",
            sequence_id=2,
        )
        await repo.mark_delivery_unregistered(
            unreg_pid,
            transition_at=_ts(1),
            session_id="s1",
            sequence_id=3,
        )
        await repo.mark_delivery_cancelled(
            cancel_pid,
            reason="grant-revoked",
            transition_at=_ts(1),
            session_id="s1",
            sequence_id=4,
        )

        queued_after = await repo.list_queued_deliveries_all()
        assert queued_after == []
