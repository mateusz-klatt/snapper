"""Repository-level tests for the push-notification persistence surface.

Covers the 21 SCD2 methods introduced for the five new
bitemporal tables ``notification_devices``, ``device_alert_prefs``,
``user_alert_defaults``, ``alert_events``, ``alert_deliveries``.
Per the bitemporal/SCD2 invariant, every row lifecycle is SCD2
close-and-insert; reads gate on ``known_to == KNOWN_TO_MAX`` via
partial active indexes.

Each test uses a fresh on-disk SQLite database via the ``repo`` fixture
(tmp_path-scoped, engine disposed after yield). AAA layout.
"""

import asyncio
import inspect
from collections.abc import AsyncGenerator
from collections.abc import Mapping
from collections.abc import Sequence
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from io import StringIO
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import PropertyMock
from unittest.mock import patch

import pytest
from sqlalchemy import select as _select
from sqlalchemy import update as _update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql.base import Executable

from snapper.auth.domain.permissions import Permission
from snapper.auth.domain.roles import UserRole
from snapper.auth.schemas.principal import AuthPrincipal
from snapper.auth.tokens import TokenManager
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
from snapper.data.repository_types import ApnsAdmissionCharge
from snapper.data.repository_types import ApnsAdmissionContext
from snapper.data.repository_types import ApnsAdmissionRefusal
from snapper.data.repository_types import ApnsAdmissionRequest
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
    async def test_list_active_notification_devices_for_users_bulk(
        self, repo: SQLAlchemyRepository
    ) -> None:
        """Bulk variant returns dict[user_public_id, list[device]]; missing users → empty list.

        Given: One active device for user-bulk-1, one for user-bulk-2, none for user-bulk-3,
        When: list_active_notification_devices_for_users([user-1, user-2, user-3]) is called,
        Then: Returned dict has all three keys; user-1 and user-2 carry their device,
            user-3 maps to an empty list.
        """
        await _seed_user_and_device(
            repo, user_public_id="user-bulk-1", device_token="tok-bulk-1", sequence_id=10
        )
        await _seed_user_and_device(
            repo, user_public_id="user-bulk-2", device_token="tok-bulk-2", sequence_id=20
        )
        result = await repo.list_active_notification_devices_for_users(
            ["user-bulk-1", "user-bulk-2", "user-bulk-3"]
        )
        assert set(result) == {"user-bulk-1", "user-bulk-2", "user-bulk-3"}
        assert len(result["user-bulk-1"]) == 1
        assert result["user-bulk-1"][0]["device_token"] == "tok-bulk-1"
        assert len(result["user-bulk-2"]) == 1
        assert result["user-bulk-2"][0]["device_token"] == "tok-bulk-2"
        assert result["user-bulk-3"] == []

    @pytest.mark.asyncio
    async def test_list_active_notification_devices_for_users_empty(
        self, repo: SQLAlchemyRepository
    ) -> None:
        """Empty input returns empty dict without a round-trip."""
        result = await repo.list_active_notification_devices_for_users([])
        assert result == {}

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
    async def test_atomic_dedup_insert_returns_none_after_first_claim(
        self,
        repo: SQLAlchemyRepository,
    ) -> None:
        """A committed same-user dedup identity suppresses the next insert."""
        row = AlertEventInsertRow(
            session_id="s1",
            sequence_id=1,
            timestamp=_ts(),
            user_public_id="user-atomic",
            alert_type="drift",
            priority="high",
            title="Portfolio drift detected",
            body="Drift",
            dedup_key="drift.episode-1",
        )

        first = await repo.insert_alert_event_if_dedup_absent(row)
        second = await repo.insert_alert_event_if_dedup_absent(row)

        assert first is not None
        assert second is None
        matches = await repo.list_alert_events_with_dedup_key(
            "user-atomic",
            "drift.episode-1",
            since=_ts(),
        )
        assert [match["public_id"] for match in matches] == [first]

    @pytest.mark.asyncio
    async def test_atomic_dedup_insert_serializes_repository_instances(
        self,
        repo: SQLAlchemyRepository,
    ) -> None:
        """Concurrent SQLite repository instances produce one claim winner."""
        competitor = SQLAlchemyRepository(repo.db_url)
        row = AlertEventInsertRow(
            session_id="s1",
            sequence_id=1,
            timestamp=_ts(),
            user_public_id="user-race",
            alert_type="drift",
            priority="high",
            title="Portfolio drift detected",
            body="Drift",
            dedup_key="drift.episode-race",
        )
        try:
            results = await asyncio.gather(
                repo.insert_alert_event_if_dedup_absent(row),
                competitor.insert_alert_event_if_dedup_absent(row),
            )
        finally:
            await competitor.engine.dispose()

        assert sum(result is not None for result in results) == 1
        matches = await repo.list_alert_events_with_dedup_key(
            "user-race",
            "drift.episode-race",
            since=_ts(),
        )
        assert len(matches) == 1

    @pytest.mark.asyncio
    async def test_atomic_dedup_insert_requires_key(
        self,
        repo: SQLAlchemyRepository,
    ) -> None:
        """Atomic insertion rejects rows without a logical dedup identity."""
        row = AlertEventInsertRow(
            session_id="s1",
            sequence_id=1,
            timestamp=_ts(),
            user_public_id="user-no-key",
            alert_type="drift",
            priority="high",
            title="Portfolio drift detected",
            body="Drift",
        )

        with pytest.raises(ValueError, match="requires dedup_key"):
            await repo.insert_alert_event_if_dedup_absent(row)

    @pytest.mark.asyncio
    async def test_postgresql_atomic_dedup_lock_uses_advisory_transaction_lock(
        self,
        repo: SQLAlchemyRepository,
    ) -> None:
        """PostgreSQL contenders serialize on a transaction advisory key."""
        session = AsyncMock()
        with patch.object(type(repo), "dialect_name", new_callable=lambda: "postgresql"):
            await repo._acquire_alert_event_dedup_lock(
                session,
                "user-pg",
                "drift.episode-pg",
            )

        statement, parameters = session.execute.await_args.args
        assert "pg_advisory_xact_lock" in str(statement)
        assert parameters == {"identity": "user-pg\x1fdrift.episode-pg"}

    @pytest.mark.asyncio
    async def test_unknown_atomic_dedup_lock_dialect_fails_closed(
        self,
        repo: SQLAlchemyRepository,
    ) -> None:
        """An unsupported database cannot silently run a racy claim."""
        session = AsyncMock()
        with (
            patch.object(type(repo), "dialect_name", new_callable=lambda: "mysql"),
            pytest.raises(NotImplementedError, match="mysql"),
        ):
            await repo._acquire_alert_event_dedup_lock(
                session,
                "user-mysql",
                "drift.episode-mysql",
            )
        session.execute.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_get_alert_events_by_public_ids_bulk_lookup(
        self, repo: SQLAlchemyRepository
    ) -> None:
        """Bulk variant returns dict keyed by public_id; missing IDs are absent.

        Given: Two active alert_events,
        When: get_alert_events_by_public_ids is called with both IDs plus a missing one,
        Then: Returned dict carries both hits keyed by public_id; the
            missing ID is absent (NOT mapped to None).
        """
        pid_a = await repo.insert_alert_event(
            AlertEventInsertRow(
                session_id="s1",
                sequence_id=1,
                timestamp=_ts(),
                user_public_id="user-bulk-a",
                alert_type="order_fill_full",
                priority="high",
                title="A",
                body="body A",
            )
        )
        pid_b = await repo.insert_alert_event(
            AlertEventInsertRow(
                session_id="s1",
                sequence_id=2,
                timestamp=_ts(),
                user_public_id="user-bulk-b",
                alert_type="order_fill_full",
                priority="high",
                title="B",
                body="body B",
            )
        )
        result = await repo.get_alert_events_by_public_ids([pid_a, pid_b, "nope"])
        assert set(result) == {pid_a, pid_b}
        assert result[pid_a]["title"] == "A"
        assert result[pid_b]["title"] == "B"

    @pytest.mark.asyncio
    async def test_get_alert_events_by_public_ids_empty_returns_empty_dict(
        self, repo: SQLAlchemyRepository
    ) -> None:
        """Empty input returns empty dict without a round-trip."""
        result = await repo.get_alert_events_by_public_ids([])
        assert result == {}

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

        An earlier design used just ``public_id`` as the cursor and the
        repo re-read the anchor's current ``(timestamp, public_id)`` — a
        mid-session revision that moved the anchor's timestamp would
        silently shift pagination. With the opaque cursor, we filter on
        the snapshotted pair directly, so paging is immune to anchor churn.
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
    async def test_get_delivery_by_public_id_returns_active_row(
        self, repo: SQLAlchemyRepository
    ) -> None:
        """Indexed lookup returns the active SCD2 version of one delivery row.

        Given: One queued delivery inserted via insert_alert_delivery,
        When: get_delivery_by_public_id is called with its public_id,
        Then: It returns the row in O(1) (no scan), with known_to == KNOWN_TO_MAX.
        Missing public_ids return None.
        """
        public_id = await repo.insert_alert_delivery(
            AlertDeliveryInsertRow(
                alert_event_public_id="evt-direct",
                device_public_id="dev-direct",
                user_public_id="user-direct",
                status="queued",
                created_at=_ts(),
                session_id="s1",
                sequence_id=1,
                timestamp=_ts(),
            )
        )
        hit = await repo.get_delivery_by_public_id(public_id)
        assert hit is not None
        assert hit["public_id"] == public_id
        assert hit["status"] == "queued"
        assert hit["known_to"] == KNOWN_TO_MAX
        miss = await repo.get_delivery_by_public_id("nonexistent-pid")
        assert miss is None

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
    """Permission-based user fan-out helper."""

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
    async def test_downscoped_token_does_not_suppress_role_fan_out(
        self, repo: SQLAlchemyRepository
    ) -> None:
        """Notification recipients remain principal-wide when one token is narrow.

        Given: An admin principal holding an access token scoped only to
            ``READ_MARKET_DATA``,
        When: Notification fan-out resolves users whose roles grant
            ``READ_SYSTEM_STATUS``,
        Then: The admin remains a recipient despite that credential excluding
            the fan-out permission.
        """
        async with repo.session() as s:
            s.add(
                User(
                    public_id="downscoped-admin-user",
                    username="downscoped-admin",
                    email="downscoped-admin@example.test",
                    password_hash="x",
                    role=UserRole.ADMIN.value,
                    created_at=_ts(),
                    session_id="seed",
                    sequence_id=1,
                    timestamp=_ts(),
                    known_to=KNOWN_TO_MAX,
                )
            )
            await s.commit()
        token_manager = TokenManager()
        principal = AuthPrincipal(
            username="downscoped-admin",
            role=UserRole.ADMIN,
            user_public_id="downscoped-admin-user",
        )
        token_pair = token_manager.create_tokens(
            principal,
            permissions=[Permission.READ_MARKET_DATA],
        )
        token_claims = token_manager.decode_fresh_token(token_pair.access_token)

        recipients = await repo.list_users_with_permission(Permission.READ_SYSTEM_STATUS.value)

        assert token_claims.permissions == [Permission.READ_MARKET_DATA.value]
        assert recipients == ["downscoped-admin-user"]

    @pytest.mark.asyncio
    async def test_excludes_deactivated_users(self, repo: SQLAlchemyRepository) -> None:
        """Deactivated users never receive fan-out alert rows.

        Given: Two admins with current SCD2 rows, one deactivated
            (``is_active=False`` — deactivation supersedes the row but
            keeps it current),
        When: The fan-out helper resolves ``read:system_status``,
        Then: Only the active admin is returned.
        """
        async with repo.session() as s:
            s.add_all(
                [
                    User(
                        public_id="active-admin",
                        username="active-admin",
                        email="active-admin@example.test",
                        password_hash="x",
                        role="admin",
                        created_at=_ts(),
                        session_id="seed",
                        sequence_id=1,
                        timestamp=_ts(),
                        known_to=KNOWN_TO_MAX,
                    ),
                    User(
                        public_id="deactivated-admin",
                        username="deactivated-admin",
                        email="deactivated-admin@example.test",
                        password_hash="x",
                        role="admin",
                        is_active=False,
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

        assert result == ["active-admin"]

    @pytest.mark.asyncio
    async def test_unknown_permission_returns_empty(self, repo: SQLAlchemyRepository) -> None:
        """A permission no role grants yields an empty list (no error)."""
        result = await repo.list_users_with_permission("unknown:permission")
        assert result == []


class TestGetDefaultLanguagesForUsers:
    """Bulk ``user.default_language`` lookup used by the APNs sidecar."""

    @pytest.mark.asyncio
    async def test_returns_default_language_per_user(self, repo: SQLAlchemyRepository) -> None:
        """Mixed coverage: one user has PL, one has NULL, one missing.

        Given: ``user-lang-pl`` with ``default_language='pl'``,
            ``user-lang-none`` with ``default_language=NULL``, and a
            third public_id ``user-lang-missing`` that has no User row.
        When: ``get_default_languages_for_users`` is called with all
            three IDs.
        Then: The returned dict carries every key — PL value, None for
            the user with NULL, None for the missing user.
        """
        async with repo.session() as s:
            s.add_all(
                [
                    User(
                        public_id="user-lang-pl",
                        username="user_pl",
                        email=None,
                        password_hash="x",
                        role="viewer",
                        default_language="pl",
                        created_at=_ts(),
                        session_id="seed",
                        sequence_id=1,
                        timestamp=_ts(),
                        known_to=KNOWN_TO_MAX,
                    ),
                    User(
                        public_id="user-lang-none",
                        username="user_none",
                        email=None,
                        password_hash="x",
                        role="viewer",
                        default_language=None,
                        created_at=_ts(),
                        session_id="seed",
                        sequence_id=2,
                        timestamp=_ts(),
                        known_to=KNOWN_TO_MAX,
                    ),
                ]
            )
            await s.commit()
        result = await repo.get_default_languages_for_users(
            ["user-lang-pl", "user-lang-none", "user-lang-missing"]
        )
        assert result == {
            "user-lang-pl": "pl",
            "user-lang-none": None,
            "user-lang-missing": None,
        }

    @pytest.mark.asyncio
    async def test_empty_input_returns_empty_dict(self, repo: SQLAlchemyRepository) -> None:
        """Empty input short-circuits — no DB round-trip.

        Given: An empty user_public_id list.
        When: ``get_default_languages_for_users`` is called.
        Then: Returns ``{}`` without touching the DB.
        """
        result = await repo.get_default_languages_for_users([])
        assert result == {}

    @pytest.mark.asyncio
    async def test_deduplicates_repeated_ids(self, repo: SQLAlchemyRepository) -> None:
        """Duplicate input IDs collapse to one DB row, one dict entry.

        Given: A user with default_language='de' and a list of IDs
            containing the same public_id three times.
        When: ``get_default_languages_for_users`` is called.
        Then: The result has exactly one key with the expected value.
        """
        async with repo.session() as s:
            s.add(
                User(
                    public_id="user-lang-dup",
                    username="user_dup",
                    email=None,
                    password_hash="x",
                    role="viewer",
                    default_language="de",
                    created_at=_ts(),
                    session_id="seed",
                    sequence_id=3,
                    timestamp=_ts(),
                    known_to=KNOWN_TO_MAX,
                )
            )
            await s.commit()
        result = await repo.get_default_languages_for_users(
            ["user-lang-dup", "user-lang-dup", "user-lang-dup"]
        )
        assert result == {"user-lang-dup": "de"}


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

        Regression coverage for a correctness fix: a naive
        close+insert on a row the retry loop already transitioned to
        ``sent`` collided on the partial-unique
        ``(public_id, known_to=MAX)`` index and raised
        ``IntegrityError``. The current pattern does an atomic close
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

    @pytest.mark.asyncio
    async def test_membership_cancel_does_not_count_lost_atomic_transition(
        self, repo: SQLAlchemyRepository
    ) -> None:
        """A membership cancel skips a row won by a concurrent transition.

        Given: A matching queued delivery whose guarded transition reports a race loss.
        When: Membership-scoped cancellation processes the delivery snapshot.
        Then: It reports no cancellation and leaves the queued row available.
        """
        delivery_pid = await repo.insert_alert_delivery(
            AlertDeliveryInsertRow(
                alert_event_public_id="evt-membership-race",
                device_public_id="dev-membership-race",
                user_public_id="user-membership-race",
                operator_public_id="op-membership-race",
                wallet_public_id="wallet-membership-race",
                status="queued",
                created_at=_ts(),
                session_id="s",
                sequence_id=1,
                timestamp=_ts(),
            )
        )
        transition = AsyncMock(return_value=False)
        with patch.object(repo, "_atomic_transition_queued_delivery", new=transition):
            cancelled = await repo.cancel_pending_deliveries_for_membership(
                membership=("user-membership-race", "op-membership-race"),
                detached_at=_ts(),
                transition_at=_ts(1),
                session_id="s-admin",
                sequence_id=2,
            )

        assert cancelled == 0
        transition.assert_awaited_once()
        assert [row["public_id"] for row in await repo.list_queued_deliveries_all()] == [
            delivery_pid
        ]

    @pytest.mark.asyncio
    async def test_bulk_cancel_clamps_earlier_transition_at_than_retry_version(
        self, repo: SQLAlchemyRepository
    ) -> None:
        """Bulk cancel never closes an active version before its start timestamp.

        Given: A queued delivery whose retry transition committed at ``_ts(5)``
            (``t1``).
        When: ``cancel_pending_deliveries_for_scope`` runs with caller timestamp
            ``_ts(2)`` (``t0 < t1``).
        Then: The delivery cancels (``cancelled == 1``), closes at ``_ts(5)``
            so no SCD2 interval ends before it starts, and opens its
            ``cancelled_scope`` successor at ``_ts(5)``.
        """
        user = "u-bulk-clamp"
        device_pid = await _seed_user_and_device(repo, user_public_id=user)
        delivery_pid = await repo.insert_alert_delivery(
            AlertDeliveryInsertRow(
                alert_event_public_id="evt-bulk-clamp",
                device_public_id=device_pid,
                user_public_id=user,
                operator_public_id="op-clamp",
                wallet_public_id="wal-clamp",
                status="queued",
                created_at=_ts(0),
                session_id="s",
                sequence_id=1,
                timestamp=_ts(0),
            )
        )
        assert (
            await repo.begin_delivery_attempt(
                delivery_pid, transition_at=_ts(5), session_id="worker", sequence_id=2
            )
            == 1
        )

        cancelled = await repo.cancel_pending_deliveries_for_scope(
            user_public_id=user,
            operator_public_id="op-clamp",
            wallet_public_id="wal-clamp",
            transition_at=_ts(2),
            session_id="s-admin",
            sequence_id=3,
        )

        assert cancelled == 1
        async with repo.session() as s:
            rows = (
                (
                    await s.execute(
                        _select(AlertDelivery)
                        .where(AlertDelivery.public_id == delivery_pid)
                        .order_by(AlertDelivery.id)
                    )
                )
                .scalars()
                .all()
            )
        assert len(rows) == 3
        for row in rows:
            assert row.known_to >= row.timestamp
        assert rows[1].status == "queued"
        assert rows[1].timestamp == _ts(5)
        assert rows[1].known_to == _ts(5)
        assert rows[2].status == "cancelled_scope"
        assert rows[2].timestamp == _ts(5)
        assert rows[2].known_to == KNOWN_TO_MAX


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

        This is the closure for an SCD2-join correctness finding: even
        if the source ``alert_event`` is later closed or its scope
        changes, the scope-cancel path filters on the delivery row's
        own denormalised columns.
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
    async def test_membership_cancel_honours_inclusive_detach_boundary(
        self, repo: SQLAlchemyRepository
    ) -> None:
        """Old and equal rows cancel while later and other-desk rows survive."""
        old_pid = await repo.insert_alert_delivery(
            AlertDeliveryInsertRow(
                alert_event_public_id="evt-old",
                device_public_id="dev-1",
                user_public_id="user-desk",
                operator_public_id="op-detached",
                wallet_public_id="wallet-a",
                status="queued",
                created_at=_ts(1),
                session_id="s1",
                sequence_id=1,
                timestamp=_ts(1),
            )
        )
        equal_pid = await repo.insert_alert_delivery(
            AlertDeliveryInsertRow(
                alert_event_public_id="evt-equal",
                device_public_id="dev-1",
                user_public_id="user-desk",
                operator_public_id="op-detached",
                wallet_public_id="wallet-a",
                status="queued",
                created_at=_ts(2),
                session_id="s1",
                sequence_id=2,
                timestamp=_ts(2),
            )
        )
        new_pid = await repo.insert_alert_delivery(
            AlertDeliveryInsertRow(
                alert_event_public_id="evt-new",
                device_public_id="dev-1",
                user_public_id="user-desk",
                operator_public_id="op-detached",
                wallet_public_id="wallet-a",
                status="queued",
                created_at=_ts(3),
                session_id="s1",
                sequence_id=3,
                timestamp=_ts(3),
            )
        )
        other_desk_pid = await repo.insert_alert_delivery(
            AlertDeliveryInsertRow(
                alert_event_public_id="evt-other-desk",
                device_public_id="dev-1",
                user_public_id="user-desk",
                operator_public_id="op-surviving",
                wallet_public_id="wallet-b",
                status="queued",
                created_at=_ts(1),
                session_id="s1",
                sequence_id=4,
                timestamp=_ts(1),
            )
        )

        cancelled = await repo.cancel_pending_deliveries_for_membership(
            membership=("user-desk", "op-detached"),
            detached_at=_ts(2),
            transition_at=_ts(5),
            session_id="s-admin",
            sequence_id=5,
        )
        duplicate_cancelled = await repo.cancel_pending_deliveries_for_membership(
            membership=("user-desk", "op-detached"),
            detached_at=_ts(2),
            transition_at=_ts(6),
            session_id="s-admin",
            sequence_id=6,
        )

        queued_after = await repo.list_queued_deliveries_all()
        async with repo.session() as s:
            rows = list(
                (
                    await s.execute(
                        _select(AlertDelivery).where(
                            AlertDelivery.public_id.in_([old_pid, equal_pid])
                        )
                    )
                )
                .scalars()
                .all()
            )
        active_cancelled = [row for row in rows if row.known_to == KNOWN_TO_MAX]
        assert cancelled == 2
        assert duplicate_cancelled == 0
        assert {row["public_id"] for row in queued_after} == {new_pid, other_desk_pid}
        assert {row.public_id for row in active_cancelled} == {old_pid, equal_pid}
        assert {row.status for row in active_cancelled} == {"cancelled_scope"}
        assert {row.timestamp for row in active_cancelled} == {_ts(5)}

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
        membership_without_row = await repo.is_operator_membership_active(
            user_public_id="user-l",
            operator_public_id="op-l",
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
        membership_with_row = await repo.is_operator_membership_active(
            user_public_id="user-l",
            operator_public_id="op-l",
            as_of=now,
        )

        assert without_membership is False
        assert with_membership is True
        assert membership_without_row is False
        assert membership_with_row is True

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
    """Atomic close + idempotent upsert convergence under concurrency.

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

        Earlier a racing timeout+retry could both pass the
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

        Given two committed attempts, when the current attempt schedules
        a retry, then the queued successor retains the allocated count.
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

        for attempt in (1, 2):
            assert (
                await repo.begin_delivery_attempt(
                    public_id, transition_at=_ts(), session_id="attempt", sequence_id=attempt
                )
                == attempt
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
        assert len(rows) == 4
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

        s5778_value_1 = NotificationDeviceUpsertRow(
            user_public_id="u-exhaust",
            device_token="exhaust-token",
            device_id="dev-never",
            env="sandbox",
            registered_at=_ts(2),
            session_id="s-never",
            sequence_id=2,
            timestamp=_ts(2),
        )
        with pytest.raises(RuntimeError, match="close-race"):
            await repo.upsert_notification_device(s5778_value_1)

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

        s5778_value_1 = DeviceAlertPrefUpsertRow(
            session_id="s-never",
            sequence_id=2,
            timestamp=_ts(2),
            device_public_id=device_pid,
            alert_type="order_fill_full",
            enabled=False,
        )
        with pytest.raises(RuntimeError, match="close-race"):
            await repo.upsert_device_alert_pref(s5778_value_1)

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

        s5778_value_1 = UserAlertDefaultUpsertRow(
            session_id="s-never",
            sequence_id=2,
            timestamp=_ts(2),
            user_public_id="u-def-exhaust",
            alert_type="order_fill_full",
            enabled=False,
        )
        with pytest.raises(RuntimeError, match="close-race"):
            await repo.upsert_user_alert_default(s5778_value_1)

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


@pytest.mark.asyncio
async def test_device_retry_warning_redacts_token_and_database_error_payload(
    repo: SQLAlchemyRepository,
) -> None:
    """Exhaust real upsert retries without logging token or SQL exception parameters."""
    token = "distinctive-fake-push-token-never-log"
    payload = "sensitive-simulated-sql-error-payload"
    error = IntegrityError(
        "INSERT notification_devices", {"device_token": token}, RuntimeError(payload)
    )
    row = NotificationDeviceUpsertRow(
        user_public_id="log-test-user",
        device_token=token,
        device_id="log-test-device",
        env="sandbox",
        registered_at=_ts(),
        session_id="log-test-session",
        sequence_id=1,
        timestamp=_ts(),
    )
    output = StringIO()
    sink = repository_module.logger.add(output, format="{message}", level="WARNING")
    try:
        with (
            patch.object(AsyncSession, "commit", new=AsyncMock(side_effect=error)) as commit,
            pytest.raises(IntegrityError) as raised,
        ):
            await repo.upsert_notification_device(row)
        assert raised.value is error
        assert commit.await_count == 20
        warning = output.getvalue()
        assert "upsert_notification_device: sustained contention exhausted" in warning
        assert "20-attempt retry budget" in warning
        assert "IntegrityError" in warning
        assert token not in warning
        assert payload not in warning
        assert "INSERT notification_devices" not in warning
    finally:
        repository_module.logger.remove(sink)


class TestApnsAdmissionContextRepo:
    """Single-session APNs admission read returns device, device prefs, and user defaults."""

    @pytest.mark.asyncio
    async def test_returns_target_device_filtered_prefs_and_user_defaults(
        self, repo: SQLAlchemyRepository
    ) -> None:
        """Combined read scopes device prefs to the target device and defaults to the user.

        Given: Two active APNs devices for one user with separate prefs (including
            a superseded SCD2 pref version), another user's device and default,
            and active user defaults.
        When: ``get_apns_admission_context`` is called for the first user and device.
        Then: It returns the target device, only that device's active prefs, and
            only that user's active defaults.
        """
        dev_one = await _seed_user_and_device(
            repo,
            user_public_id="user-adm-1",
            device_token="tok-adm-1",
            device_id="ios-adm-1",
            sequence_id=1,
        )
        dev_two = await repo.upsert_notification_device(
            NotificationDeviceUpsertRow(
                user_public_id="user-adm-1",
                device_token="tok-adm-2",
                device_id="ios-adm-2",
                env="sandbox",
                registered_at=_ts(),
                session_id="test",
                sequence_id=2,
                timestamp=_ts(),
            )
        )
        dev_other = await _seed_user_and_device(
            repo,
            user_public_id="user-adm-2",
            device_token="tok-adm-other",
            device_id="ios-adm-other",
            sequence_id=3,
        )
        await repo.upsert_device_alert_pref(
            DeviceAlertPrefUpsertRow(
                device_public_id=dev_one,
                alert_type="order_fill_full",
                enabled=True,
                min_priority="low",
                session_id="s1",
                sequence_id=1,
                timestamp=_ts(1),
            )
        )
        await repo.upsert_device_alert_pref(
            DeviceAlertPrefUpsertRow(
                device_public_id=dev_one,
                alert_type="order_fill_full",
                enabled=False,
                min_priority="high",
                session_id="s1",
                sequence_id=2,
                timestamp=_ts(2),
            )
        )
        await repo.upsert_device_alert_pref(
            DeviceAlertPrefUpsertRow(
                device_public_id=dev_two,
                alert_type="order_fill_full",
                enabled=True,
                min_priority="medium",
                session_id="s1",
                sequence_id=3,
                timestamp=_ts(1),
            )
        )
        await repo.upsert_device_alert_pref(
            DeviceAlertPrefUpsertRow(
                device_public_id=dev_other,
                alert_type="order_fill_full",
                enabled=True,
                min_priority="medium",
                session_id="s1",
                sequence_id=4,
                timestamp=_ts(1),
            )
        )
        await repo.upsert_user_alert_default(
            UserAlertDefaultUpsertRow(
                user_public_id="user-adm-1",
                alert_type="order_rejected",
                enabled=True,
                min_priority="medium",
                session_id="s1",
                sequence_id=5,
                timestamp=_ts(1),
            )
        )
        await repo.upsert_user_alert_default(
            UserAlertDefaultUpsertRow(
                user_public_id="user-adm-1",
                alert_type="order_rejected",
                enabled=False,
                min_priority="high",
                session_id="s1",
                sequence_id=6,
                timestamp=_ts(2),
            )
        )
        await repo.upsert_user_alert_default(
            UserAlertDefaultUpsertRow(
                user_public_id="user-adm-2",
                alert_type="order_rejected",
                enabled=True,
                min_priority="low",
                session_id="s1",
                sequence_id=7,
                timestamp=_ts(1),
            )
        )

        ctx = await repo.get_apns_admission_context("user-adm-1", dev_one)

        assert ctx["device"] is not None
        assert ctx["device"]["public_id"] == dev_one
        assert ctx["device"]["device_token"] == "tok-adm-1"
        assert len(ctx["device_prefs"]) == 1
        assert ctx["device_prefs"][0]["device_public_id"] == dev_one
        assert ctx["device_prefs"][0]["enabled"] is False
        assert ctx["device_prefs"][0]["min_priority"] == "high"
        assert len(ctx["user_defaults"]) == 1
        assert ctx["user_defaults"][0]["user_public_id"] == "user-adm-1"
        assert ctx["user_defaults"][0]["enabled"] is False
        assert ctx["user_defaults"][0]["min_priority"] == "high"

    @pytest.mark.asyncio
    async def test_returns_none_for_inactive_device(self, repo: SQLAlchemyRepository) -> None:
        """Inactive devices return None device without leaking tombstoned device prefs.

        Given: A deactivated APNs device that still has active pref and user-default rows.
        When: ``get_apns_admission_context`` is called for that user and device.
        Then: ``device`` is ``None`` and ``device_prefs`` and ``user_defaults`` are empty.
        """
        device_pid = await _seed_user_and_device(
            repo,
            user_public_id="user-adm-inactive",
            device_token="tok-adm-inactive",
            sequence_id=1,
        )
        await repo.upsert_device_alert_pref(
            DeviceAlertPrefUpsertRow(
                device_public_id=device_pid,
                alert_type="order_fill_full",
                enabled=True,
                session_id="s1",
                sequence_id=1,
                timestamp=_ts(1),
            )
        )
        await repo.upsert_user_alert_default(
            UserAlertDefaultUpsertRow(
                user_public_id="user-adm-inactive",
                alert_type="order_fill_full",
                enabled=True,
                session_id="s1",
                sequence_id=2,
                timestamp=_ts(1),
            )
        )
        await repo.deactivate_notification_device_scd2(
            device_pid,
            reason="user_unregistered",
            timestamp=_ts(2),
            session_id="s2",
            sequence_id=3,
        )

        ctx = await repo.get_apns_admission_context("user-adm-inactive", device_pid)

        assert ctx == {"device": None, "device_prefs": [], "user_defaults": []}

    @pytest.mark.asyncio
    async def test_returns_none_for_other_users_device(self, repo: SQLAlchemyRepository) -> None:
        """A device owned by another user is never admitted across users.

        Given: An active APNs device owned by ``user-adm-owner`` and a separate user.
        When: ``get_apns_admission_context`` is queried with ``user-adm-caller`` and that device.
        Then: ``device`` is ``None`` and ``device_prefs`` and ``user_defaults`` are empty.
        """
        owner_device = await _seed_user_and_device(
            repo,
            user_public_id="user-adm-owner",
            device_token="tok-adm-owner",
            sequence_id=1,
        )
        await repo.upsert_device_alert_pref(
            DeviceAlertPrefUpsertRow(
                device_public_id=owner_device,
                alert_type="order_fill_full",
                enabled=True,
                session_id="s1",
                sequence_id=1,
                timestamp=_ts(1),
            )
        )
        await repo.upsert_user_alert_default(
            UserAlertDefaultUpsertRow(
                user_public_id="user-adm-caller",
                alert_type="order_fill_full",
                enabled=True,
                session_id="s1",
                sequence_id=2,
                timestamp=_ts(1),
            )
        )

        ctx = await repo.get_apns_admission_context("user-adm-caller", owner_device)

        assert ctx == {"device": None, "device_prefs": [], "user_defaults": []}

    @pytest.mark.asyncio
    async def test_returns_none_for_fcm_or_missing_device(self, repo: SQLAlchemyRepository) -> None:
        """Android FCM registrations and missing device IDs are excluded from APNs admission.

        Given: An active ``provider='fcm'`` device registration and an unknown device ID.
        When: ``get_apns_admission_context`` is called for each.
        Then: Both return ``device=None`` with empty prefs and defaults.
        """
        async with repo.session() as s:
            s.add(
                NotificationDevice(
                    public_id="dev-fcm-1",
                    session_id="s1",
                    sequence_id=1,
                    timestamp=_ts(),
                    known_to=KNOWN_TO_MAX,
                    user_public_id="user-adm-fcm",
                    provider="fcm",
                    configuration_id="cfg-1",
                    binding_id="bind-1",
                    installation_id="inst-1",
                    firebase_installation_id="fid-1",
                    package_name="ch.snapper.android",
                    device_token=None,
                    device_id="android-1",
                    platform="android",
                    env=None,
                    registered_at=_ts(),
                    token_status="active",
                )
            )
            await s.commit()

        fcm_ctx = await repo.get_apns_admission_context("user-adm-fcm", "dev-fcm-1")
        missing_ctx = await repo.get_apns_admission_context("user-adm-fcm", "dev-does-not-exist")

        assert fcm_ctx == {"device": None, "device_prefs": [], "user_defaults": []}
        assert missing_ctx == {"device": None, "device_prefs": [], "user_defaults": []}


async def _delivery_versions(repo: SQLAlchemyRepository, public_id: str) -> list[AlertDelivery]:
    """Return every SCD2 version of ``public_id`` ordered by surrogate primary key."""
    async with repo.session() as s:
        rows = (
            await s.execute(
                _select(AlertDelivery)
                .where(AlertDelivery.public_id == public_id)
                .order_by(AlertDelivery.id)
            )
        ).scalars()
        return list(rows.all())


async def _seed_queued_apns_delivery(
    repo: SQLAlchemyRepository,
    *,
    user_public_id: str,
    device_public_id: str,
    next_attempt_at: datetime | None = None,
) -> str:
    """Insert a queued APNs delivery row with deterministic test provenance."""
    return await repo.insert_alert_delivery(
        AlertDeliveryInsertRow(
            alert_event_public_id="evt-atomic",
            device_public_id=device_public_id,
            user_public_id=user_public_id,
            status="queued",
            next_attempt_at=next_attempt_at,
            created_at=_ts(0),
            session_id="seed",
            sequence_id=1,
            timestamp=_ts(0),
        )
    )


async def _seed_admission_filter_fixture(
    repo: SQLAlchemyRepository,
) -> tuple[str, str]:
    """Seed two users, three devices, and active/superseded prefs and defaults."""
    dev_one = await _seed_user_and_device(
        repo,
        user_public_id="user-atomic-1",
        device_token="tok-atomic-1",
        device_id="ios-atomic-1",
        sequence_id=1,
    )
    dev_two = await repo.upsert_notification_device(
        NotificationDeviceUpsertRow(
            user_public_id="user-atomic-1",
            device_token="tok-atomic-2",
            device_id="ios-atomic-2",
            env="sandbox",
            registered_at=_ts(0),
            session_id="s1",
            sequence_id=2,
            timestamp=_ts(0),
        )
    )
    dev_other = await _seed_user_and_device(
        repo,
        user_public_id="user-atomic-2",
        device_token="tok-atomic-other",
        device_id="ios-atomic-other",
        sequence_id=3,
    )
    for seq, (dev, ts, enabled, prio) in enumerate(
        [
            (dev_one, _ts(1), True, "low"),
            (dev_one, _ts(2), False, "high"),
            (dev_two, _ts(1), True, "medium"),
            (dev_other, _ts(1), True, "medium"),
        ],
        start=1,
    ):
        await repo.upsert_device_alert_pref(
            DeviceAlertPrefUpsertRow(
                device_public_id=dev,
                alert_type="order_fill_full",
                enabled=enabled,
                min_priority=prio,
                session_id="s-pref",
                sequence_id=seq,
                timestamp=ts,
            )
        )
    for seq, (user, ts, enabled, prio) in enumerate(
        [
            ("user-atomic-1", _ts(1), True, "medium"),
            ("user-atomic-1", _ts(2), False, "high"),
            ("user-atomic-2", _ts(1), True, "low"),
        ],
        start=1,
    ):
        await repo.upsert_user_alert_default(
            UserAlertDefaultUpsertRow(
                user_public_id=user,
                alert_type="order_rejected",
                enabled=enabled,
                min_priority=prio,
                session_id="s-def",
                sequence_id=seq,
                timestamp=ts,
            )
        )
    return dev_one, await _seed_queued_apns_delivery(
        repo, user_public_id="user-atomic-1", device_public_id=dev_one
    )


def _make_pg_admission_session(statements: list[str], *, fail_commit: bool) -> AsyncMock:
    """Build a mock ``AsyncSession`` capturing SQL statements for PostgreSQL admission."""
    delivery_model = AlertDelivery(
        id=1,
        public_id="del-pg",
        session_id="s",
        sequence_id=1,
        timestamp=_ts(0),
        known_to=KNOWN_TO_MAX,
        alert_event_public_id="evt-pg",
        device_public_id="dev-pg",
        user_public_id="user-pg",
        status="queued",
        attempt_count=0,
        provider="apns",
        created_at=_ts(0),
    )
    device_model = NotificationDevice(
        public_id="dev-pg",
        session_id="s",
        sequence_id=1,
        timestamp=_ts(0),
        known_to=KNOWN_TO_MAX,
        user_public_id="user-pg",
        device_token="tok-pg",
        device_id="ios-pg",
        platform="ios",
        env="prod",
        previews_mode="private",
        registered_at=_ts(0),
        token_status="active",
    )
    scalar_results = iter([delivery_model, device_model])

    async def _fake_execute(statement: object) -> MagicMock:
        sql_text = str(statement)
        statements.append(sql_text)
        if sql_text.startswith("SET TRANSACTION"):
            return MagicMock()
        if sql_text.startswith("UPDATE alert_deliveries"):
            return MagicMock(rowcount=1)
        if "FROM alert_deliveries" in sql_text or "FROM notification_devices" in sql_text:
            return MagicMock(scalar_one_or_none=MagicMock(return_value=next(scalar_results)))
        return MagicMock(scalars=MagicMock(return_value=MagicMock(all=MagicMock(return_value=[]))))

    return AsyncMock(
        spec=AsyncSession,
        add=MagicMock(),
        execute=AsyncMock(side_effect=_fake_execute),
        commit=AsyncMock(side_effect=RuntimeError("40001 serialization") if fail_commit else None),
        rollback=AsyncMock(),
        close=AsyncMock(),
    )


class TestAdmitAndChargeApnsDeliveryRepo:
    """Atomic queued APNs admission and attempt allocation inside one transaction."""

    @pytest.mark.asyncio
    async def test_admits_increments_attempt_once_and_returns_fresh_device(
        self, repo: SQLAlchemyRepository
    ) -> None:
        """Admitting predicate increments attempt once and returns refreshed device.

        Given: A queued APNs delivery whose device was refreshed to token ``tok-fresh``.
        When: ``admit_and_charge_apns_delivery`` runs with a predicate returning ``True``.
        Then: It commits attempt 1 once and returns ``ApnsAdmissionCharge`` with the fresh device.
        """
        device_pid = await _seed_user_and_device(
            repo, user_public_id="user-adm-ok", device_token="tok-old", sequence_id=1
        )
        delivery_pid = await _seed_queued_apns_delivery(
            repo, user_public_id="user-adm-ok", device_public_id=device_pid
        )
        await repo.upsert_notification_device(
            NotificationDeviceUpsertRow(
                user_public_id="user-adm-ok",
                device_token="tok-fresh",
                device_id="device-1",
                env="prod",
                registered_at=_ts(0),
                session_id="refresh",
                sequence_id=2,
                timestamp=_ts(1),
            )
        )

        outcome = await repo.admit_and_charge_apns_delivery(
            ApnsAdmissionRequest(
                delivery_public_id=delivery_pid,
                user_public_id="user-adm-ok",
                device_public_id=device_pid,
                transition_at=_ts(2),
                session_id="worker",
                sequence_id=10,
                admits=lambda _ctx: True,
            )
        )

        assert isinstance(outcome, ApnsAdmissionCharge)
        assert outcome.attempt == 1
        assert outcome.device["device_token"] == "tok-fresh"
        assert outcome.device["env"] == "prod"
        versions = await _delivery_versions(repo, delivery_pid)
        assert [v.attempt_count for v in versions] == [0, 1]
        assert versions[0].known_to == _ts(2)
        assert versions[1].known_to == KNOWN_TO_MAX
        assert versions[1].last_attempt_at == _ts(2)
        assert versions[1].status == "queued"

    @pytest.mark.asyncio
    async def test_predicate_false_refuses_routing_suppressed_without_increment(
        self, repo: SQLAlchemyRepository
    ) -> None:
        """Predicate returning False refuses ``routing_suppressed`` without incrementing.

        Given: A queued APNs delivery with an active device and future ``next_attempt_at``.
        When: ``admit_and_charge_apns_delivery`` runs with a predicate returning ``False``.
        Then: It returns ``routing_suppressed`` and leaves the queued row and attempt unchanged.
        """
        device_pid = await _seed_user_and_device(
            repo, user_public_id="user-adm-sup", device_token="tok-sup", sequence_id=1
        )
        delivery_pid = await _seed_queued_apns_delivery(
            repo,
            user_public_id="user-adm-sup",
            device_public_id=device_pid,
            next_attempt_at=_ts(5),
        )

        outcome = await repo.admit_and_charge_apns_delivery(
            ApnsAdmissionRequest(
                delivery_public_id=delivery_pid,
                user_public_id="user-adm-sup",
                device_public_id=device_pid,
                transition_at=_ts(2),
                session_id="worker",
                sequence_id=10,
                admits=lambda _ctx: False,
            )
        )

        assert outcome == ApnsAdmissionRefusal(refusal="routing_suppressed")
        versions = await _delivery_versions(repo, delivery_pid)
        assert len(versions) == 1
        assert versions[0].attempt_count == 0
        assert versions[0].status == "queued"
        assert versions[0].next_attempt_at == _ts(5)

    @pytest.mark.asyncio
    async def test_deactivated_device_refuses_device_inactive_without_increment(
        self, repo: SQLAlchemyRepository
    ) -> None:
        """Deactivated device refuses ``device_inactive`` before invoking predicate.

        Given: A queued APNs delivery whose target device was deactivated.
        When: ``admit_and_charge_apns_delivery`` executes.
        Then: It returns ``device_inactive``, skips the predicate, and leaves attempt unchanged.
        """
        device_pid = await _seed_user_and_device(
            repo, user_public_id="user-adm-off", device_token="tok-off", sequence_id=1
        )
        delivery_pid = await _seed_queued_apns_delivery(
            repo, user_public_id="user-adm-off", device_public_id=device_pid
        )
        await repo.deactivate_notification_device_scd2(
            device_pid,
            reason="user_unregistered",
            timestamp=_ts(1),
            session_id="deact",
            sequence_id=2,
        )
        predicate_called = False

        def _admits(_ctx: ApnsAdmissionContext) -> bool:
            nonlocal predicate_called
            predicate_called = True
            return True

        outcome = await repo.admit_and_charge_apns_delivery(
            ApnsAdmissionRequest(
                delivery_public_id=delivery_pid,
                user_public_id="user-adm-off",
                device_public_id=device_pid,
                transition_at=_ts(2),
                session_id="worker",
                sequence_id=10,
                admits=_admits,
            )
        )

        assert outcome == ApnsAdmissionRefusal(refusal="device_inactive")
        assert predicate_called is False
        versions = await _delivery_versions(repo, delivery_pid)
        assert len(versions) == 1
        assert versions[0].attempt_count == 0

    @pytest.mark.asyncio
    @pytest.mark.parametrize("status", ["missing", "cancelled_scope", "sent"])
    async def test_non_queued_or_missing_delivery_refuses_not_queued(
        self, repo: SQLAlchemyRepository, status: str
    ) -> None:
        """Missing, cancelled, or terminal deliveries refuse ``not_queued`` without increment.

        Given: A delivery that is missing, ``cancelled_scope``, or already ``sent``.
        When: ``admit_and_charge_apns_delivery`` executes.
        Then: It returns ``not_queued`` without mutating delivery history.
        """
        device_pid = await _seed_user_and_device(
            repo, user_public_id="user-adm-term", device_token=f"tok-{status}", sequence_id=1
        )
        delivery_pid = await _seed_queued_apns_delivery(
            repo, user_public_id="user-adm-term", device_public_id=device_pid
        )
        if status == "cancelled_scope":
            await repo.mark_delivery_cancelled(
                delivery_pid,
                reason="user_deactivated",
                transition_at=_ts(1),
                session_id="admin",
                sequence_id=2,
            )
        elif status == "sent":
            await repo.mark_delivery_sent(
                delivery_pid,
                apns_id="apns-done",
                transition_at=_ts(1),
                session_id="worker",
                sequence_id=2,
            )
        else:
            delivery_pid = "missing-delivery"
        before = await _delivery_versions(repo, delivery_pid)

        outcome = await repo.admit_and_charge_apns_delivery(
            ApnsAdmissionRequest(
                delivery_public_id=delivery_pid,
                user_public_id="user-adm-term",
                device_public_id=device_pid,
                transition_at=_ts(2),
                session_id="worker",
                sequence_id=10,
                admits=lambda _ctx: True,
            )
        )

        assert outcome == ApnsAdmissionRefusal(refusal="not_queued")
        after = await _delivery_versions(repo, delivery_pid)
        assert [(r.id, r.status, r.attempt_count) for r in after] == [
            (r.id, r.status, r.attempt_count) for r in before
        ]

    @pytest.mark.asyncio
    async def test_concurrent_close_between_select_and_update_refuses_not_queued(
        self, repo: SQLAlchemyRepository, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A concurrent close between SELECT and UPDATE refuses ``not_queued`` without increment.

        Given: A queued APNs delivery admitted by SELECT and the predicate.
        When: A competing transaction cancels the row right before the close UPDATE runs.
        Then: The close affects zero rows, rolls back, returns ``not_queued``, and preserves
            the winner's ``cancelled_scope`` version with ``attempt_count == 0``.
        """
        device_pid = await _seed_user_and_device(
            repo, user_public_id="user-adm-race", device_token="tok-race", sequence_id=1
        )
        delivery_pid = await _seed_queued_apns_delivery(
            repo, user_public_id="user-adm-race", device_public_id=device_pid
        )
        original_execute = AsyncSession.execute
        raced = False

        async def _intercept(
            session: AsyncSession,
            statement: Executable,
            params: Sequence[Mapping[str, object]] | Mapping[str, object] | None = None,
        ) -> object:
            """Commit a competing close right before the guarded close UPDATE executes."""
            nonlocal raced
            if not raced and str(statement).startswith("UPDATE alert_deliveries"):
                raced = True
                await repo.mark_delivery_cancelled(
                    delivery_pid,
                    reason="user_deactivated",
                    transition_at=_ts(1),
                    session_id="admin",
                    sequence_id=9,
                )
            return await original_execute(session, statement, params)

        monkeypatch.setattr(AsyncSession, "execute", _intercept)

        outcome = await repo.admit_and_charge_apns_delivery(
            ApnsAdmissionRequest(
                delivery_public_id=delivery_pid,
                user_public_id="user-adm-race",
                device_public_id=device_pid,
                transition_at=_ts(2),
                session_id="worker",
                sequence_id=10,
                admits=lambda _ctx: True,
            )
        )

        assert raced is True
        assert outcome == ApnsAdmissionRefusal(refusal="not_queued")
        versions = await _delivery_versions(repo, delivery_pid)
        assert [v.status for v in versions] == ["queued", "cancelled_scope"]
        assert [v.attempt_count for v in versions] == [0, 0]

    @pytest.mark.asyncio
    async def test_predicate_exception_rolls_back_and_propagates_without_increment(
        self, repo: SQLAlchemyRepository
    ) -> None:
        """An exception raised by the admission predicate rolls back with no increment.

        Given: A queued APNs delivery and an active device.
        When: The synchronous ``admits`` predicate raises ``RuntimeError``.
        Then: The exception propagates and ``alert_deliveries`` remains at attempt 0.
        """
        device_pid = await _seed_user_and_device(
            repo, user_public_id="user-adm-err", device_token="tok-err", sequence_id=1
        )
        delivery_pid = await _seed_queued_apns_delivery(
            repo, user_public_id="user-adm-err", device_public_id=device_pid
        )

        def _raising_admits(_ctx: ApnsAdmissionContext) -> bool:
            raise RuntimeError("predicate exploded")

        with pytest.raises(RuntimeError, match="predicate exploded"):
            await repo.admit_and_charge_apns_delivery(
                ApnsAdmissionRequest(
                    delivery_public_id=delivery_pid,
                    user_public_id="user-adm-err",
                    device_public_id=device_pid,
                    transition_at=_ts(2),
                    session_id="worker",
                    sequence_id=10,
                    admits=_raising_admits,
                )
            )

        versions = await _delivery_versions(repo, delivery_pid)
        assert len(versions) == 1
        assert versions[0].attempt_count == 0
        assert versions[0].status == "queued"

    @pytest.mark.asyncio
    async def test_admission_context_filters_prefs_to_target_device(
        self, repo: SQLAlchemyRepository
    ) -> None:
        """Predicate receives active prefs scoped strictly to the target device and user.

        Given: Two devices for one user, another user's device, and superseded/active prefs.
        When: ``admit_and_charge_apns_delivery`` runs for the first device.
        Then: Only that device's active prefs and that user's active defaults reach the predicate.
        """
        dev_one, delivery_pid = await _seed_admission_filter_fixture(repo)
        captured: list[ApnsAdmissionContext] = []

        def _capture(ctx: ApnsAdmissionContext) -> bool:
            captured.append(ctx)
            return True

        outcome = await repo.admit_and_charge_apns_delivery(
            ApnsAdmissionRequest(
                delivery_public_id=delivery_pid,
                user_public_id="user-atomic-1",
                device_public_id=dev_one,
                transition_at=_ts(3),
                session_id="worker",
                sequence_id=10,
                admits=_capture,
            )
        )

        assert isinstance(outcome, ApnsAdmissionCharge)
        assert len(captured) == 1
        ctx = captured[0]
        assert len(ctx["device_prefs"]) == 1
        assert ctx["device_prefs"][0]["device_public_id"] == dev_one
        assert ctx["device_prefs"][0]["enabled"] is False
        assert len(ctx["user_defaults"]) == 1
        assert ctx["user_defaults"][0]["user_public_id"] == "user-atomic-1"
        assert ctx["user_defaults"][0]["min_priority"] == "high"

    @pytest.mark.asyncio
    async def test_sqlite_does_not_emit_repeatable_read_isolation(
        self, repo: SQLAlchemyRepository, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """SQLite execution skips PostgreSQL ``SET TRANSACTION ISOLATION LEVEL``.

        Given: A real SQLite repository and queued APNs delivery.
        When: ``admit_and_charge_apns_delivery`` runs while recording executed statements.
        Then: No ``REPEATABLE READ`` statement is executed on SQLite.
        """
        device_pid = await _seed_user_and_device(
            repo, user_public_id="user-adm-sq", device_token="tok-sq", sequence_id=1
        )
        delivery_pid = await _seed_queued_apns_delivery(
            repo, user_public_id="user-adm-sq", device_public_id=device_pid
        )
        original_execute = AsyncSession.execute
        statements: list[str] = []

        async def _record_execute(
            session: AsyncSession,
            statement: Executable,
            params: Sequence[Mapping[str, object]] | Mapping[str, object] | None = None,
        ) -> object:
            statements.append(str(statement))
            return await original_execute(session, statement, params)

        monkeypatch.setattr(AsyncSession, "execute", _record_execute)

        outcome = await repo.admit_and_charge_apns_delivery(
            ApnsAdmissionRequest(
                delivery_public_id=delivery_pid,
                user_public_id="user-adm-sq",
                device_public_id=device_pid,
                transition_at=_ts(1),
                session_id="worker",
                sequence_id=5,
                admits=lambda _ctx: True,
            )
        )

        assert isinstance(outcome, ApnsAdmissionCharge)
        assert not any("REPEATABLE READ" in stmt for stmt in statements)

    @pytest.mark.asyncio
    @pytest.mark.parametrize("fail_commit", [False, True], ids=["success", "pg-serialization"])
    async def test_postgresql_sets_repeatable_read_and_propagates_errors(
        self, repo: SQLAlchemyRepository, fail_commit: bool
    ) -> None:
        """PostgreSQL starts the transaction at ``REPEATABLE READ`` and rolls back on failure.

        Given: A repository reporting dialect ``postgresql`` with a mocked session factory.
        When: ``admit_and_charge_apns_delivery`` runs normally or fails during commit.
        Then: ``SET TRANSACTION ISOLATION LEVEL REPEATABLE READ`` is emitted first, and any
            commit failure triggers session rollback and propagates without returning a charge.
        """
        statements: list[str] = []
        session = _make_pg_admission_session(statements, fail_commit=fail_commit)
        request = ApnsAdmissionRequest(
            delivery_public_id="del-pg",
            user_public_id="user-pg",
            device_public_id="dev-pg",
            transition_at=_ts(1),
            session_id="worker",
            sequence_id=2,
            admits=lambda _ctx: True,
        )

        with (
            patch.object(
                SQLAlchemyRepository,
                "dialect_name",
                new_callable=PropertyMock,
                return_value="postgresql",
            ),
            patch.object(repo, "session_factory", return_value=session),
        ):
            if fail_commit:
                with pytest.raises(RuntimeError, match="40001 serialization"):
                    await repo.admit_and_charge_apns_delivery(request)
                session.rollback.assert_awaited_once()
            else:
                outcome = await repo.admit_and_charge_apns_delivery(request)
                assert isinstance(outcome, ApnsAdmissionCharge)
                assert outcome.attempt == 1
                assert outcome.device["device_token"] == "tok-pg"

        assert statements[0] == "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"
        assert "FROM alert_deliveries" in statements[1]
        assert "FROM notification_devices" in statements[2]

    @pytest.mark.asyncio
    async def test_admits_predicate_called_synchronously_once_without_await(
        self, repo: SQLAlchemyRepository
    ) -> None:
        """Repository invokes ``request.admits`` synchronously once without awaiting it.

        Given: A queued APNs delivery, active APNs device, and plain synchronous function.
        When: ``admit_and_charge_apns_delivery`` runs with that synchronous predicate.
        Then: ``inspect.iscoroutinefunction`` is ``False``, the predicate is invoked
            synchronously exactly once with an ``ApnsAdmissionContext``, and the
            delivery increments once.
        """
        device_pid = await _seed_user_and_device(
            repo, user_public_id="user-adm-sync", device_token="tok-sync", sequence_id=1
        )
        delivery_pid = await _seed_queued_apns_delivery(
            repo, user_public_id="user-adm-sync", device_public_id=device_pid
        )
        calls: list[ApnsAdmissionContext] = []

        def _sync_admits(ctx: ApnsAdmissionContext) -> bool:
            calls.append(ctx)
            return True

        assert inspect.iscoroutinefunction(_sync_admits) is False
        outcome = await repo.admit_and_charge_apns_delivery(
            ApnsAdmissionRequest(
                delivery_public_id=delivery_pid,
                user_public_id="user-adm-sync",
                device_public_id=device_pid,
                transition_at=_ts(1),
                session_id="worker",
                sequence_id=3,
                admits=_sync_admits,
            )
        )

        assert isinstance(outcome, ApnsAdmissionCharge)
        assert outcome.attempt == 1
        assert len(calls) == 1
        assert calls[0]["device"] is not None
        assert calls[0]["device"]["public_id"] == device_pid
        assert isinstance(calls[0]["device_prefs"], list)
        assert isinstance(calls[0]["user_defaults"], list)
