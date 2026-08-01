"""Tests for ``snapper.application.notify.scope_revalidation.ScopeRevalidator``."""

from datetime import UTC
from datetime import datetime
from datetime import timedelta
from typing import Literal
from typing import cast
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import pytest

from snapper.application.notify.scope_revalidation import ScopeRevalidator
from snapper.auth.domain.permissions import Permission
from snapper.data.models import KNOWN_TO_MAX
from snapper.data.repository_types import AlertEventRow
from snapper.messaging.infrastructure.publisher import SequenceTracker
from snapper.messaging.schemas.admin import MembershipRevokedData
from snapper.messaging.schemas.data import ScopeRevokedData
from snapper.messaging.schemas.data import UserDeactivatedData


def _now() -> datetime:
    """Fixed UTC noon timestamp for deterministic cache tests."""
    return datetime(2026, 4, 24, 12, 0, 0, tzinfo=UTC)


def _scope_revoked_payload(
    *,
    operator: str = "op-1",
    wallet: str = "wal-1",
    scope_kind: str = "underlying",
) -> bytes:
    """Serialise a ScopeRevokedData envelope as it arrives on the bus."""
    event = ScopeRevokedData(
        session_id="admin",
        sequence_id=1,
        public_id="evt-1",
        timestamp=_now(),
        grant_public_id="grant-1",
        operator_public_id=operator,
        wallet_public_id=wallet,
        scope_kind=cast(Literal["underlying", "instrument"], scope_kind),
        underlying_public_id="und-1" if scope_kind == "underlying" else None,
        instrument_public_id=None if scope_kind == "underlying" else "inst-1",
        revoked_at=_now(),
    )
    return event.to_json().encode("utf-8")


def _user_deactivated_payload(user: str = "user-1") -> bytes:
    """Serialise a UserDeactivatedData envelope."""
    event = UserDeactivatedData(
        session_id="admin",
        sequence_id=1,
        public_id="evt-2",
        timestamp=_now(),
        user_public_id=user,
        deactivated_at=_now(),
    )
    return event.to_json().encode("utf-8")


def _membership_revoked_payload(
    *,
    user: str = "user-1",
    operator: str = "op-1",
) -> bytes:
    """Serialise a MembershipRevokedData envelope."""
    event = MembershipRevokedData(
        session_id="admin",
        sequence_id=1,
        public_id="evt-3",
        timestamp=_now(),
        membership_public_id="membership-1",
        user_public_id=user,
        username="viewer",
        operator_public_id=operator,
        detached_at=_now(),
        revoked_by_user_public_id="admin-1",
        promoted_operator_public_id=None,
        reason="desk_membership_revoked",
    )
    return event.to_json().encode("utf-8")


def _alert(
    *,
    operator: str | None = "op-1",
    wallet: str | None = "wal-1",
    safety_critical: bool = False,
) -> AlertEventRow:
    """Build an AlertEventRow fixture."""
    return cast(
        AlertEventRow,
        {
            "public_id": "evt",
            "session_id": "s",
            "sequence_id": 1,
            "timestamp": _now(),
            "known_to": KNOWN_TO_MAX,
            "user_public_id": "user-1",
            "operator_public_id": operator,
            "wallet_public_id": wallet,
            "alert_type": "order_fill_full",
            "priority": "medium",
            "is_safety_critical": safety_critical,
            "title": "t",
            "body": "b",
            "payload": None,
            "dedup_key": None,
            "thread_key": None,
            "source_topic": None,
        },
    )


class TestHandleScopeRevoked:
    """Cascade: primes the cache + cancels matching deliveries per affected user."""

    @pytest.mark.asyncio
    async def test_malformed_payload_dropped(self) -> None:
        """Broken JSON never raises; repo is not touched."""
        rev = ScopeRevalidator(SequenceTracker())
        repo = MagicMock()

        await rev.handle_scope_revoked(b"not-json", repo, _now())

        repo.list_users_with_operator_membership.assert_not_called()

    @pytest.mark.asyncio
    async def test_primes_stale_scope_cache(self) -> None:
        """The (operator, wallet) pair gets a wake-up entry in the cache."""
        rev = ScopeRevalidator(SequenceTracker())
        repo = MagicMock()
        repo.list_users_with_operator_membership = AsyncMock(return_value=[])
        repo.cancel_pending_deliveries_for_scope = AsyncMock(return_value=0)

        await rev.handle_scope_revoked(_scope_revoked_payload(), repo, _now())

        assert rev._stale_scope_cache == {("op-1", "wal-1"): _now()}

    @pytest.mark.asyncio
    async def test_cancels_per_affected_user(self) -> None:
        """Every user holding membership in the revoked operator gets cancelled."""
        rev = ScopeRevalidator(SequenceTracker())
        repo = MagicMock()
        repo.list_users_with_operator_membership = AsyncMock(return_value=["u-a", "u-b"])
        repo.cancel_pending_deliveries_for_scope = AsyncMock(return_value=2)

        await rev.handle_scope_revoked(_scope_revoked_payload(), repo, _now())

        assert repo.cancel_pending_deliveries_for_scope.await_count == 2
        users_called = {
            call.kwargs["user_public_id"]
            for call in repo.cancel_pending_deliveries_for_scope.await_args_list
        }
        assert users_called == {"u-a", "u-b"}

    @pytest.mark.asyncio
    async def test_user_with_no_pending_deliveries_is_quiet(self) -> None:
        """Cancel returning 0 does NOT log info — keeps noise floor low."""
        rev = ScopeRevalidator(SequenceTracker())
        repo = MagicMock()
        repo.list_users_with_operator_membership = AsyncMock(return_value=["u-a"])
        repo.cancel_pending_deliveries_for_scope = AsyncMock(return_value=0)

        await rev.handle_scope_revoked(_scope_revoked_payload(), repo, _now())

        repo.cancel_pending_deliveries_for_scope.assert_awaited_once()


class TestHandleUserDeactivated:
    """``admin.user_deactivated`` bulk-cancels every queued delivery for the user."""

    @pytest.mark.asyncio
    async def test_malformed_payload_dropped(self) -> None:
        """Broken JSON never raises; repo is not touched."""
        rev = ScopeRevalidator(SequenceTracker())
        repo = MagicMock()

        await rev.handle_user_deactivated(b"not-json", repo, _now())

        repo.cancel_pending_deliveries_for_user.assert_not_called()

    @pytest.mark.asyncio
    async def test_cancels_all_for_user(self) -> None:
        """The deactivated user's public_id reaches the repo cancel helper."""
        rev = ScopeRevalidator(SequenceTracker())
        repo = MagicMock()
        repo.cancel_pending_deliveries_for_user = AsyncMock(return_value=3)

        await rev.handle_user_deactivated(_user_deactivated_payload(user="u-kill"), repo, _now())

        repo.cancel_pending_deliveries_for_user.assert_awaited_once()
        assert (
            repo.cancel_pending_deliveries_for_user.await_args.kwargs["user_public_id"] == "u-kill"
        )

    @pytest.mark.asyncio
    async def test_user_without_pending_deliveries_is_quiet(self) -> None:
        """Zero cancels produce no error and complete the repository call."""
        rev = ScopeRevalidator(SequenceTracker())
        repo = MagicMock()
        repo.cancel_pending_deliveries_for_user = AsyncMock(return_value=0)

        await rev.handle_user_deactivated(_user_deactivated_payload(), repo, _now())

        repo.cancel_pending_deliveries_for_user.assert_awaited_once()


class TestHandleMembershipRevoked:
    """``admin.membership_revoked`` only cancels deliveries for the detached desk."""

    @pytest.mark.asyncio
    async def test_malformed_payload_dropped(self) -> None:
        """Broken JSON never reaches the repository."""
        rev = ScopeRevalidator(SequenceTracker())
        repo = MagicMock()

        await rev.handle_membership_revoked(b"not-json", repo, _now())

        repo.cancel_pending_deliveries_for_membership.assert_not_called()

    @pytest.mark.asyncio
    async def test_cancels_only_user_and_operator_pair(self) -> None:
        """Identity, detach boundary, and handling transition reach the repository."""
        rev = ScopeRevalidator(SequenceTracker())
        repo = MagicMock()
        repo.cancel_pending_deliveries_for_membership = AsyncMock(return_value=2)
        handled_at = _now() + timedelta(minutes=5)

        await rev.handle_membership_revoked(
            _membership_revoked_payload(user="u-detached", operator="op-detached"),
            repo,
            handled_at,
        )

        repo.cancel_pending_deliveries_for_membership.assert_awaited_once()
        kwargs = repo.cancel_pending_deliveries_for_membership.await_args.kwargs
        assert kwargs["membership"] == ("u-detached", "op-detached")
        assert kwargs["detached_at"] == _now()
        assert kwargs["transition_at"] == handled_at

    @pytest.mark.asyncio
    async def test_absent_pending_delivery_completes_quietly(self) -> None:
        """An already-drained desk has nothing to transition."""
        rev = ScopeRevalidator(SequenceTracker())
        repo = MagicMock()
        repo.cancel_pending_deliveries_for_membership = AsyncMock(return_value=0)

        await rev.handle_membership_revoked(_membership_revoked_payload(), repo, _now())

        repo.cancel_pending_deliveries_for_membership.assert_awaited_once()


class TestShouldSkipSend:
    """Pre-send revalidation + safety-critical bypass + TTL cache semantics."""

    @pytest.mark.asyncio
    async def test_no_scope_never_skips(self) -> None:
        """Alerts without operator or wallet scope never hit the DB."""
        rev = ScopeRevalidator(SequenceTracker())
        repo = MagicMock()

        skip = await rev.should_skip_send(_alert(operator=None, wallet=None), repo, _now())

        assert skip is False

    @pytest.mark.asyncio
    async def test_safety_critical_always_rechecks_db(self) -> None:
        """Safety-critical alerts skip the cache and always call is_scope_grant_active."""
        rev = ScopeRevalidator(SequenceTracker())
        repo = MagicMock()
        repo.is_operator_membership_active = AsyncMock(return_value=True)
        repo.is_scope_grant_active = AsyncMock(return_value=False)
        repo.list_users_with_permission = AsyncMock(return_value=[])

        skip = await rev.should_skip_send(_alert(safety_critical=True), repo, _now())

        assert skip is True
        repo.is_scope_grant_active.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_safety_critical_active_scope_sends_without_global_lookup(self) -> None:
        """An active membership and wallet grant need no global-authority fallback."""
        rev = ScopeRevalidator(SequenceTracker())
        repo = MagicMock()
        repo.is_operator_membership_active = AsyncMock(return_value=True)
        repo.is_scope_grant_active = AsyncMock(return_value=True)
        repo.list_users_with_permission = AsyncMock()

        skip = await rev.should_skip_send(_alert(safety_critical=True), repo, _now())

        assert skip is False
        repo.list_users_with_permission.assert_not_called()

    @pytest.mark.asyncio
    async def test_non_critical_cache_miss_skips_db(self) -> None:
        """Cold cache → no DB call → send proceeds (bounded staleness)."""
        rev = ScopeRevalidator(SequenceTracker())
        repo = MagicMock()
        repo.is_operator_membership_active = AsyncMock(return_value=True)
        repo.is_scope_grant_active = AsyncMock(return_value=True)

        skip = await rev.should_skip_send(_alert(), repo, _now())

        assert skip is False
        repo.is_scope_grant_active.assert_not_called()

    @pytest.mark.asyncio
    async def test_non_critical_cache_hit_rechecks_db(self) -> None:
        """Cache flagged scope → DB check → cancel if grant is gone."""
        rev = ScopeRevalidator(SequenceTracker())
        rev._stale_scope_cache[("op-1", "wal-1")] = _now()
        repo = MagicMock()
        repo.is_operator_membership_active = AsyncMock(return_value=True)
        repo.is_scope_grant_active = AsyncMock(return_value=False)
        repo.list_users_with_permission = AsyncMock(return_value=[])

        skip = await rev.should_skip_send(_alert(), repo, _now())

        assert skip is True
        repo.is_scope_grant_active.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_non_critical_cache_expired_skips_db(self) -> None:
        """Cache entry older than TTL is ignored — cache effectively clears."""
        rev = ScopeRevalidator(SequenceTracker())
        rev._stale_scope_cache[("op-1", "wal-1")] = _now() - timedelta(minutes=5)
        repo = MagicMock()
        repo.is_operator_membership_active = AsyncMock(return_value=True)
        repo.is_scope_grant_active = AsyncMock()

        skip = await rev.should_skip_send(_alert(), repo, _now())

        assert skip is False
        repo.is_scope_grant_active.assert_not_called()

    @pytest.mark.asyncio
    async def test_non_critical_cache_hit_but_grant_still_active_sends(self) -> None:
        """Cache hit + grant still active → send proceeds."""
        rev = ScopeRevalidator(SequenceTracker())
        rev._stale_scope_cache[("op-1", "wal-1")] = _now()
        repo = MagicMock()
        repo.is_operator_membership_active = AsyncMock(return_value=True)
        repo.is_scope_grant_active = AsyncMock(return_value=True)

        skip = await rev.should_skip_send(_alert(), repo, _now())

        assert skip is False

    @pytest.mark.asyncio
    async def test_missing_membership_and_global_authority_skips(self) -> None:
        """A user with neither desk membership nor global authority fails closed."""
        rev = ScopeRevalidator(SequenceTracker())
        repo = MagicMock()
        repo.is_operator_membership_active = AsyncMock(return_value=False)
        repo.is_scope_grant_active = AsyncMock()
        repo.list_users_with_permission = AsyncMock(return_value=[])

        skip = await rev.should_skip_send(_alert(), repo, _now())

        assert skip is True
        repo.is_scope_grant_active.assert_not_called()
        repo.list_users_with_permission.assert_awaited_once_with(
            Permission.IMPERSONATE_OPERATOR.value
        )

    @pytest.mark.asyncio
    async def test_global_admin_without_membership_sends(self) -> None:
        """Global operator authority preserves an ADMIN's cross-desk delivery."""
        rev = ScopeRevalidator(SequenceTracker())
        repo = MagicMock()
        repo.is_operator_membership_active = AsyncMock(return_value=False)
        repo.is_scope_grant_active = AsyncMock()
        repo.list_users_with_permission = AsyncMock(return_value=["user-1"])

        skip = await rev.should_skip_send(
            _alert(safety_critical=True),
            repo,
            _now(),
        )

        assert skip is False
        repo.is_scope_grant_active.assert_not_called()
        repo.list_users_with_permission.assert_awaited_once_with(
            Permission.IMPERSONATE_OPERATOR.value
        )

    @pytest.mark.asyncio
    async def test_global_admin_bypasses_missing_wallet_grant(self) -> None:
        """Global authority also survives a membership-bound wallet-grant result."""
        rev = ScopeRevalidator(SequenceTracker())
        repo = MagicMock()
        repo.is_operator_membership_active = AsyncMock(return_value=True)
        repo.is_scope_grant_active = AsyncMock(return_value=False)
        repo.list_users_with_permission = AsyncMock(return_value=["user-1"])

        skip = await rev.should_skip_send(
            _alert(safety_critical=True),
            repo,
            _now(),
        )

        assert skip is False
        repo.list_users_with_permission.assert_awaited_once_with(
            Permission.IMPERSONATE_OPERATOR.value
        )

    @pytest.mark.asyncio
    async def test_active_membership_without_wallet_scope_sends(self) -> None:
        """A desk-scoped alert without a wallet only needs the membership check."""
        rev = ScopeRevalidator(SequenceTracker())
        repo = MagicMock()
        repo.is_operator_membership_active = AsyncMock(return_value=True)
        repo.is_scope_grant_active = AsyncMock()

        skip = await rev.should_skip_send(_alert(wallet=None), repo, _now())

        assert skip is False
        repo.is_scope_grant_active.assert_not_called()

    @pytest.mark.asyncio
    async def test_membership_fallback_uses_fresh_pre_send_time(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A detach after dispatch start is visible to the final authority read."""
        fresh_as_of = _now() + timedelta(minutes=1)
        monkeypatch.setattr(
            "snapper.application.notify.scope_revalidation._authority_now",
            lambda: fresh_as_of,
        )
        rev = ScopeRevalidator(SequenceTracker())
        repo = MagicMock()
        repo.is_operator_membership_active = AsyncMock(return_value=False)
        repo.list_users_with_permission = AsyncMock(return_value=[])

        skip = await rev.should_skip_send(_alert(), repo, _now())

        assert skip is True
        assert repo.is_operator_membership_active.await_args.kwargs["as_of"] == fresh_as_of
