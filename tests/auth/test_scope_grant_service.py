"""Tests for  ``ScopeGrantService.revoke_grant`` orchestration.

The DB-state mutations (SCD2 close + advisory lock) are integration-
tested in ``tests/data/test_scope_grants.py::TestRevokeScopeGrant``.
These tests focus on the service contract: ordering (repository first,
publish second), single-publisher invariant, payload schema, graceful
degradation on missing/failing publisher, and statelessness.
"""

from collections.abc import Generator
from datetime import UTC
from datetime import datetime
from unittest.mock import AsyncMock

import pytest

from snapper.auth.scope_grant_service import ScopeGrantService
from snapper.data.repository import ScopeGrantNotFoundError
from snapper.data.repository_types import ScopeGrantRow
from snapper.messaging.schemas.data import ScopeRevokedData


class _RecordingPublisher:
    """Stub MessagePublisher capturing every (topic, payload) pair."""

    def __init__(self) -> None:
        self.sent: list[tuple[str, ScopeRevokedData]] = []

    async def send(self, stream_key: str, data: ScopeRevokedData) -> None:
        """Record the (topic, payload) pair for assertion."""
        self.sent.append((stream_key, data))


class _RaisingPublisher(_RecordingPublisher):
    """Publisher whose `send` raises after recording the call."""

    async def send(self, stream_key: str, data: ScopeRevokedData) -> None:
        """Record the call then raise to simulate a broker hiccup."""
        await super().send(stream_key, data)
        raise RuntimeError("broker unreachable")


def _make_row(
    *,
    grant_public_id: str = "grant-1",
    operator_public_id: str = "op-1",
    wallet_public_id: str = "wal-1",
    scope_kind: str = "underlying",
    underlying_public_id: str | None = "under-btc",
    instrument_public_id: str | None = None,
    revoked_at: datetime | None = None,
) -> ScopeGrantRow:
    """Build a closed-row projection matching ``repository.revoke_scope_grant``."""
    if revoked_at is None:
        revoked_at = datetime.now(UTC)
    return ScopeGrantRow(
        public_id=grant_public_id,
        operator_public_id=operator_public_id,
        wallet_public_id=wallet_public_id,
        granted_by_user_public_id="user-admin",
        scope_kind=scope_kind,
        underlying_public_id=underlying_public_id,
        instrument_public_id=instrument_public_id,
        note="seed",
        timestamp=revoked_at,
        known_to=revoked_at,
        session_id="sess-seed",
        sequence_id=1,
    )


@pytest.fixture(autouse=True)
def _reset_singleton() -> Generator[None]:
    """Ensure each test gets a clean ScopeGrantService instance."""
    ScopeGrantService.clear_instance()
    yield
    ScopeGrantService.clear_instance()


class TestRevokeGrantOrchestration:
    """Tests for ``ScopeGrantService.revoke_grant`` happy path + publisher."""

    @pytest.mark.asyncio
    async def test_revoke_grant_publishes_after_commit(self) -> None:
        """Publisher is called exactly once with the admin.scope_revoked topic.

        Given: repository.revoke_scope_grant returns a closed-row projection,
        When: service.revoke_grant runs with a recording publisher,
        Then: publisher.send was awaited once with topic='admin.scope_revoked'
            and the ScopeRevokedData payload carries every field from the
            closed row plus the revoker metadata.
        """
        service = ScopeGrantService()
        revoked_at = datetime.now(UTC)
        closed = _make_row(revoked_at=revoked_at)
        service.repository = AsyncMock()
        service.repository.revoke_scope_grant = AsyncMock(return_value=closed)
        publisher = _RecordingPublisher()
        service.set_msg_publisher(publisher)

        returned = await service.revoke_grant(
            grant_public_id="grant-1",
            revoked_by_user_public_id="user-admin",
            reason="alice left",
            now=revoked_at,
        )
        assert returned is closed
        assert len(publisher.sent) == 1
        topic, payload = publisher.sent[0]
        assert topic == "admin.scope_revoked"
        assert payload.grant_public_id == "grant-1"
        assert payload.operator_public_id == "op-1"
        assert payload.wallet_public_id == "wal-1"
        assert payload.scope_kind == "underlying"
        assert payload.underlying_public_id == "under-btc"
        assert payload.instrument_public_id is None
        assert payload.revoked_at == revoked_at
        assert payload.revoked_by_user_public_id == "user-admin"
        assert payload.reason == "alice left"

    @pytest.mark.asyncio
    async def test_revoke_grant_without_publisher_still_closes(self) -> None:
        """Missing publisher logs warning but does not raise or roll back.

        When the lifespan has not yet attached a publisher (tests, API-only
        startup race), revoke_grant still drives the repository close and
        returns the projected row. Fanout is best-effort.
        """
        service = ScopeGrantService()
        closed = _make_row()
        service.repository = AsyncMock()
        service.repository.revoke_scope_grant = AsyncMock(return_value=closed)
        service.set_msg_publisher(None)

        returned = await service.revoke_grant(
            grant_public_id="grant-1",
            revoked_by_user_public_id="user-admin",
            reason=None,
            now=datetime.now(UTC),
        )
        assert returned is closed
        service.repository.revoke_scope_grant.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_revoke_grant_publisher_failure_does_not_rollback(self) -> None:
        """Broker hiccup is swallowed; the commit stays final.

        The single-publisher rule says the DB close is source of truth;
        a failed send logs an exception but must not raise out of the
        service (per UserService._publish_user_deactivated convention).
        """
        service = ScopeGrantService()
        closed = _make_row()
        service.repository = AsyncMock()
        service.repository.revoke_scope_grant = AsyncMock(return_value=closed)
        publisher = _RaisingPublisher()
        service.set_msg_publisher(publisher)

        returned = await service.revoke_grant(
            grant_public_id="grant-1",
            revoked_by_user_public_id="user-admin",
            reason=None,
            now=datetime.now(UTC),
        )
        assert returned is closed
        assert len(publisher.sent) == 1

    @pytest.mark.asyncio
    async def test_revoke_grant_propagates_repository_not_found(self) -> None:
        """NotFoundError from the repository bubbles up without a publish.

        Publisher must NOT be called when the close fails — there's
        nothing to revoke.
        """
        service = ScopeGrantService()
        service.repository = AsyncMock()
        service.repository.revoke_scope_grant = AsyncMock(
            side_effect=ScopeGrantNotFoundError("no such grant")
        )
        publisher = _RecordingPublisher()
        service.set_msg_publisher(publisher)

        with pytest.raises(ScopeGrantNotFoundError):
            await service.revoke_grant(
                grant_public_id="grant-none",
                revoked_by_user_public_id="user-admin",
                reason=None,
                now=datetime.now(UTC),
            )
        assert publisher.sent == []

    @pytest.mark.asyncio
    async def test_revoke_grant_invalid_scope_kind_from_repo_raises(self) -> None:
        """A bogus scope_kind value from the repo surfaces as ValueError.

        Defense-in-depth: should the repository ever return a row with a
        malformed scope_kind (schema drift, bad migration), the service
        refuses to publish garbage.
        """
        service = ScopeGrantService()
        closed = _make_row(scope_kind="weird")
        service.repository = AsyncMock()
        service.repository.revoke_scope_grant = AsyncMock(return_value=closed)
        publisher = _RecordingPublisher()
        service.set_msg_publisher(publisher)

        with pytest.raises(ValueError, match="invalid scope_kind"):
            await service.revoke_grant(
                grant_public_id="grant-1",
                revoked_by_user_public_id="user-admin",
                reason=None,
                now=datetime.now(UTC),
            )
        assert publisher.sent == []

    @pytest.mark.asyncio
    async def test_revoke_grant_instrument_scope_populates_instrument_field(
        self,
    ) -> None:
        """instrument-scoped grants flow instrument_public_id through to the event."""
        service = ScopeGrantService()
        closed = _make_row(
            scope_kind="instrument",
            underlying_public_id=None,
            instrument_public_id="inst-btcusd",
        )
        service.repository = AsyncMock()
        service.repository.revoke_scope_grant = AsyncMock(return_value=closed)
        publisher = _RecordingPublisher()
        service.set_msg_publisher(publisher)

        await service.revoke_grant(
            grant_public_id="grant-1",
            revoked_by_user_public_id="user-admin",
            reason=None,
            now=datetime.now(UTC),
        )
        _topic, payload = publisher.sent[0]
        assert payload.scope_kind == "instrument"
        assert payload.underlying_public_id is None
        assert payload.instrument_public_id == "inst-btcusd"


class TestSingleton:
    """Tests for the ``get_scope_grant_service`` factory + singleton semantics."""

    def test_singleton_returns_same_instance(self) -> None:
        """Two calls to get_instance return the same object."""
        first = ScopeGrantService.get_instance()
        second = ScopeGrantService.get_instance()
        assert first is second

    def test_clear_instance_resets_singleton(self) -> None:
        """clear_instance drops the cached instance for test isolation."""
        first = ScopeGrantService.get_instance()
        ScopeGrantService.clear_instance()
        second = ScopeGrantService.get_instance()
        assert first is not second

    def test_set_publisher_none_clears_reference(self) -> None:
        """set_msg_publisher(None) allows later explicit clearing."""
        service = ScopeGrantService()
        publisher = _RecordingPublisher()
        service.set_msg_publisher(publisher)
        assert service._msg_publisher is publisher
        service.set_msg_publisher(None)
        assert service._msg_publisher is None

    def test_direct_constructor_is_idempotent(self) -> None:
        """Calling ``ScopeGrantService()`` twice returns the same singleton.

        Exercises both the ``__new__`` cached-instance branch and the
        ``__init__`` early-return guard so repeated bare constructor
        calls don't double-wire the repository / tracker references.
        """
        first = ScopeGrantService()
        first._msg_publisher = _RecordingPublisher()
        second = ScopeGrantService()
        assert first is second
        assert second._msg_publisher is first._msg_publisher


class TestStatelessness:
    """Pins the §D10 "no coordinator-owned mutable state" invariant."""

    @pytest.mark.asyncio
    async def test_two_successive_revokes_do_not_leak_state(self) -> None:
        """Two revokes on different grants use only their own inputs.

        Each ``revoke_grant`` call must be self-contained: no cached
        principal, no cached grant identity, no accumulating scratch.
        Verifies by exercising two back-to-back revokes on distinct
        grants/operators/wallets and asserting each emitted event
        carries ONLY the fields of its corresponding call.
        """
        service = ScopeGrantService()
        publisher = _RecordingPublisher()
        service.set_msg_publisher(publisher)
        now_a = datetime.now(UTC)
        now_b = now_a

        row_a = _make_row(
            grant_public_id="grant-A",
            operator_public_id="op-A",
            wallet_public_id="wal-A",
            scope_kind="underlying",
            underlying_public_id="under-btc",
            instrument_public_id=None,
            revoked_at=now_a,
        )
        row_b = _make_row(
            grant_public_id="grant-B",
            operator_public_id="op-B",
            wallet_public_id="wal-B",
            scope_kind="instrument",
            underlying_public_id=None,
            instrument_public_id="inst-eth",
            revoked_at=now_b,
        )
        service.repository = AsyncMock()
        service.repository.revoke_scope_grant = AsyncMock(side_effect=[row_a, row_b])

        await service.revoke_grant(
            grant_public_id="grant-A",
            revoked_by_user_public_id="user-A",
            reason="A reason",
            now=now_a,
        )
        await service.revoke_grant(
            grant_public_id="grant-B",
            revoked_by_user_public_id="user-B",
            reason="B reason",
            now=now_b,
        )
        assert len(publisher.sent) == 2
        _, payload_a = publisher.sent[0]
        _, payload_b = publisher.sent[1]
        assert payload_a.grant_public_id == "grant-A"
        assert payload_a.operator_public_id == "op-A"
        assert payload_a.revoked_by_user_public_id == "user-A"
        assert payload_a.reason == "A reason"
        assert payload_a.scope_kind == "underlying"
        assert payload_a.underlying_public_id == "under-btc"
        assert payload_a.instrument_public_id is None
        assert payload_b.grant_public_id == "grant-B"
        assert payload_b.operator_public_id == "op-B"
        assert payload_b.revoked_by_user_public_id == "user-B"
        assert payload_b.reason == "B reason"
        assert payload_b.scope_kind == "instrument"
        assert payload_b.underlying_public_id is None
        assert payload_b.instrument_public_id == "inst-eth"
