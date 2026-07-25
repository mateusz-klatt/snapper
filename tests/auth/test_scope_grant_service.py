"""Tests for ``ScopeGrantService`` orchestration across all three events.

The DB-state mutations (SCD2 close + insert + advisory lock) are
integration-tested in ``tests/data/test_scope_grants.py``. These
tests focus on the service contract: ordering (repository first,
publish second), single-publisher invariant, payload schema, graceful
degradation on missing/failing publisher, and statelessness for
create_grant / handover / revoke_grant.
"""

from collections.abc import Generator
from datetime import UTC
from datetime import datetime
from typing import Any
from unittest.mock import AsyncMock

import pytest

from snapper.auth.domain.roles import UserRole
from snapper.auth.schemas.principal import AuthPrincipal
from snapper.auth.scope_grant_service import ScopeGrantService
from snapper.auth.scope_grant_service import get_scope_grant_service
from snapper.data.repository import ScopeGrantNotFoundError
from snapper.data.repository_types import CreateScopeGrantRequest
from snapper.data.repository_types import ScopeGrantRow
from snapper.messaging.schemas.data import ScopeGrantedData
from snapper.messaging.schemas.data import ScopeHandedOverData


class _RecordingPublisher:
    """Stub MessagePublisher capturing every (topic, payload) pair."""

    def __init__(self) -> None:
        self.sent: list[tuple[str, Any]] = []

    async def send(self, stream_key: str, data: Any) -> None:
        """Record the (topic, payload) pair for assertion."""
        self.sent.append((stream_key, data))


class _RaisingPublisher(_RecordingPublisher):
    """Publisher whose `send` raises after recording the call."""

    async def send(self, stream_key: str, data: Any) -> None:
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


def _make_insert_request(
    *,
    granted_by_user_public_id: str = "user-admin",
    scope_kind: str = "underlying",
    underlying_public_id: str | None = "under-btc",
    instrument_public_id: str | None = None,
    note: str | None = "seed-note",
) -> CreateScopeGrantRequest:
    """Build a ``CreateScopeGrantRequest`` for create_grant tests."""
    return CreateScopeGrantRequest(
        operator_public_id="op-1",
        wallet_public_id="wal-1",
        granted_by_user_public_id=granted_by_user_public_id,
        scope_kind=scope_kind,
        underlying_public_id=underlying_public_id,
        instrument_public_id=instrument_public_id,
        note=note,
        session_id="sess-rest",
        sequence_id=1,
        timestamp=datetime.now(UTC),
    )


class TestCreateGrantOrchestration:
    """Tests for ``ScopeGrantService.create_grant`` happy path + publisher."""

    @pytest.mark.asyncio
    async def test_create_grant_publishes_after_commit(self) -> None:
        """Publisher is called exactly once with admin.scope_granted topic.

        The repository creates the new row; the service then emits the
        wake-up event carrying grant identity + creator metadata.
        """
        service = ScopeGrantService()
        granted_at = datetime.now(UTC)
        created = _make_row()
        service.repository = AsyncMock()
        service.repository.create_scope_grant = AsyncMock(return_value=created)
        publisher = _RecordingPublisher()
        service.set_msg_publisher(publisher)
        insert = _make_insert_request()

        returned = await service.create_grant(insert, now=granted_at)
        assert returned is created
        assert len(publisher.sent) == 1
        topic, payload = publisher.sent[0]
        assert topic == "admin.scope_granted"
        assert isinstance(payload, ScopeGrantedData)
        assert payload.grant_public_id == "grant-1"
        assert payload.operator_public_id == "op-1"
        assert payload.wallet_public_id == "wal-1"
        assert payload.scope_kind == "underlying"
        assert payload.underlying_public_id == "under-btc"
        assert payload.granted_at == granted_at
        assert payload.granted_by_user_public_id == "user-admin"
        assert payload.reason == "seed-note"

    @pytest.mark.asyncio
    async def test_create_grant_without_publisher_still_inserts(self) -> None:
        """Missing publisher logs warning but does not raise or roll back."""
        service = ScopeGrantService()
        created = _make_row()
        service.repository = AsyncMock()
        service.repository.create_scope_grant = AsyncMock(return_value=created)
        service.set_msg_publisher(None)
        insert = _make_insert_request()

        returned = await service.create_grant(insert, now=datetime.now(UTC))
        assert returned is created
        service.repository.create_scope_grant.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_create_grant_publisher_failure_does_not_rollback(self) -> None:
        """Broker hiccup is swallowed; the insert stays final."""
        service = ScopeGrantService()
        created = _make_row()
        service.repository = AsyncMock()
        service.repository.create_scope_grant = AsyncMock(return_value=created)
        publisher = _RaisingPublisher()
        service.set_msg_publisher(publisher)
        insert = _make_insert_request()

        returned = await service.create_grant(insert, now=datetime.now(UTC))
        assert returned is created
        assert len(publisher.sent) == 1

    @pytest.mark.asyncio
    async def test_create_grant_invalid_scope_kind_from_repo_raises(self) -> None:
        """A bogus scope_kind from the repo surfaces as ValueError; no publish."""
        service = ScopeGrantService()
        created = _make_row(scope_kind="weird")
        service.repository = AsyncMock()
        service.repository.create_scope_grant = AsyncMock(return_value=created)
        publisher = _RecordingPublisher()
        service.set_msg_publisher(publisher)
        insert = _make_insert_request()

        with pytest.raises(ValueError, match="invalid scope_kind"):
            await service.create_grant(insert, now=datetime.now(UTC))
        assert publisher.sent == []


class TestHandoverOrchestration:
    """Tests for ``ScopeGrantService.handover`` happy path + publisher."""

    @pytest.mark.asyncio
    async def test_handover_publishes_after_commit(self) -> None:
        """Publisher is called once with admin.scope_handed_over topic.

        Both operator IDs are emitted so audit consumers can identify
        the from + to parties without a second DB round-trip.
        """
        service = ScopeGrantService()
        handover_at = datetime.now(UTC)
        closed = _make_row(operator_public_id="op-from")
        new_row = _make_row(
            grant_public_id="grant-2",
            operator_public_id="op-to",
        )
        service.repository = AsyncMock()
        service.repository.handover_grant = AsyncMock(return_value=(closed, new_row))
        publisher = _RecordingPublisher()
        service.set_msg_publisher(publisher)

        out_closed, out_new = await service.handover(
            grant_public_id="grant-1",
            destination_operator_public_id="op-to",
            handover_by_user_public_id="user-admin",
            reason="rebalance",
            session_id="sess-rest",
            sequence_id=7,
            now=handover_at,
        )
        assert out_closed is closed
        assert out_new is new_row
        assert len(publisher.sent) == 1
        topic, payload = publisher.sent[0]
        assert topic == "admin.scope_handed_over"
        assert isinstance(payload, ScopeHandedOverData)
        assert payload.grant_public_id == "grant-2"
        assert payload.from_operator_public_id == "op-from"
        assert payload.to_operator_public_id == "op-to"
        assert payload.wallet_public_id == "wal-1"
        assert payload.scope_kind == "underlying"
        assert payload.handover_at == handover_at
        assert payload.handover_by_user_public_id == "user-admin"
        assert payload.reason == "rebalance"

    @pytest.mark.asyncio
    async def test_handover_without_publisher_still_completes(self) -> None:
        """Missing publisher logs warning but transaction stays final."""
        service = ScopeGrantService()
        closed = _make_row(operator_public_id="op-from")
        new_row = _make_row(grant_public_id="grant-2", operator_public_id="op-to")
        service.repository = AsyncMock()
        service.repository.handover_grant = AsyncMock(return_value=(closed, new_row))
        service.set_msg_publisher(None)

        await service.handover(
            grant_public_id="grant-1",
            destination_operator_public_id="op-to",
            handover_by_user_public_id="user-admin",
            reason=None,
            session_id="sess",
            sequence_id=1,
            now=datetime.now(UTC),
        )
        service.repository.handover_grant.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_handover_publisher_failure_does_not_rollback(self) -> None:
        """Broker hiccup is swallowed; the SCD2 transaction stays final."""
        service = ScopeGrantService()
        closed = _make_row(operator_public_id="op-from")
        new_row = _make_row(grant_public_id="grant-2", operator_public_id="op-to")
        service.repository = AsyncMock()
        service.repository.handover_grant = AsyncMock(return_value=(closed, new_row))
        publisher = _RaisingPublisher()
        service.set_msg_publisher(publisher)

        await service.handover(
            grant_public_id="grant-1",
            destination_operator_public_id="op-to",
            handover_by_user_public_id="user-admin",
            reason=None,
            session_id="sess",
            sequence_id=1,
            now=datetime.now(UTC),
        )
        assert len(publisher.sent) == 1

    @pytest.mark.asyncio
    async def test_handover_invalid_scope_kind_from_repo_raises(self) -> None:
        """Bogus scope_kind on the new row surfaces as ValueError; no publish."""
        service = ScopeGrantService()
        closed = _make_row(operator_public_id="op-from")
        new_row = _make_row(
            grant_public_id="grant-2",
            operator_public_id="op-to",
            scope_kind="weird",
        )
        service.repository = AsyncMock()
        service.repository.handover_grant = AsyncMock(return_value=(closed, new_row))
        publisher = _RecordingPublisher()
        service.set_msg_publisher(publisher)

        with pytest.raises(ValueError, match="invalid scope_kind"):
            await service.handover(
                grant_public_id="grant-1",
                destination_operator_public_id="op-to",
                handover_by_user_public_id="user-admin",
                reason=None,
                session_id="sess",
                sequence_id=1,
                now=datetime.now(UTC),
            )
        assert publisher.sent == []


class TestSingleton:
    """Tests for the ``get_scope_grant_service`` factory + singleton semantics."""

    def test_singleton_returns_same_instance(self) -> None:
        """Two calls to get_instance return the same object."""
        first = ScopeGrantService.get_instance()
        second = ScopeGrantService.get_instance()
        assert first is second

    def test_get_scope_grant_service_factory_returns_singleton(self) -> None:
        """The module-level factory delegates to ``ScopeGrantService.get_instance``."""
        first = get_scope_grant_service()
        second = get_scope_grant_service()
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


class TestHasGrantForDelegate:
    """Tests for ``ScopeGrantService.has_grant_for_delegate`` forwarder."""

    @pytest.mark.asyncio
    async def test_has_grant_for_delegate_forwards_to_repository(self) -> None:
        """Service forwards args + verdict from the repository.

        Given a service with a stubbed repository,
        When has_grant_for_delegate is called,
        Then the repository receives the kwargs verbatim and the verdict
            propagates back unchanged.
        """
        service = ScopeGrantService()
        as_of = datetime(2026, 4, 25, 12, 0, 0, tzinfo=UTC)
        captured: dict[str, object] = {}

        async def _repo_call(
            *,
            delegate_public_id: str,
            wallet_public_id: str,
            instrument_public_id: str,
            as_of: datetime,
        ) -> bool:
            captured["delegate"] = delegate_public_id
            captured["wallet"] = wallet_public_id
            captured["instrument"] = instrument_public_id
            captured["as_of"] = as_of
            return True

        service.repository.has_grant_for_delegate = AsyncMock(side_effect=_repo_call)
        verdict = await service.has_grant_for_delegate(
            delegate_public_id="del-1",
            wallet_public_id="wal-1",
            instrument_public_id="inst-1",
            as_of=as_of,
        )
        assert verdict is True
        assert captured == {
            "delegate": "del-1",
            "wallet": "wal-1",
            "instrument": "inst-1",
            "as_of": as_of,
        }

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


class TestListAccessibleWalletPublicIds:
    """Wrapper for ``list_accessible_wallets_for_operators`` + ``list_active_wallets``.

    Drives the v0.7.0 RBAC-symmetry filter for ``orders.events.*`` and
    ``portfolio.accounts.*`` WS frames. Since the read/trade split it is
    the OPERATOR plane alone and is deliberately NARROWER than the REST
    read surfaces, which resolve through ``list_readable_wallets_for_user``
    — the method's docstring records why the read grants stop at REST.
    """

    @pytest.mark.asyncio
    async def test_admin_returns_all_active_wallets(self) -> None:
        """ADMIN bypasses operator-scope filtering.

        Given an ADMIN principal,
        When list_accessible_wallet_public_ids is called,
        Then the service queries ``list_active_wallets`` (not the
            operator-scoped variant) and returns every active wallet's
            public_id.
        """
        service = ScopeGrantService()
        as_of = datetime(2026, 5, 7, 12, 0, 0, tzinfo=UTC)
        admin = AuthPrincipal(
            username="admin-x",
            role=UserRole.ADMIN,
            user_public_id="user-admin",
            operator_public_ids=["op-1"],
        )
        rows = [{"public_id": "wal-A"}, {"public_id": "wal-B"}, {"public_id": "wal-C"}]
        service.repository.list_active_wallets = AsyncMock(return_value=rows)
        service.repository.list_accessible_wallets_for_operators = AsyncMock()

        result = await service.list_accessible_wallet_public_ids(principal=admin, as_of=as_of)

        assert result == {"wal-A", "wal-B", "wal-C"}
        active_mock: AsyncMock = service.repository.list_active_wallets
        active_mock.assert_awaited_once_with(as_of=as_of)
        scoped_mock: AsyncMock = service.repository.list_accessible_wallets_for_operators
        scoped_mock.assert_not_called()

    @pytest.mark.asyncio
    async def test_viewer_with_empty_operator_set_returns_empty(self) -> None:
        """Non-ADMIN with no operator memberships -> empty set, no DB hit.

        Given a VIEWER with no operator memberships,
        When list_accessible_wallet_public_ids is called,
        Then the empty set is returned without any repository query, and
            in particular WITHOUT consulting the read-grant union: the WS
            frame filters stay on the operator plane, so a personal read
            grant widens the REST snapshot but not the live deltas. This
            asymmetry is deliberate — the same method also serves
            AI_DELEGATE sockets, whose principal carries the delegate's
            OWN generated ``user_public_id`` (the owner is recorded only
            as ``created_by_user_public_id``), so a read grant written
            against that id would become live trade-plane socket
            visibility rather than a read-only widening.
        """
        service = ScopeGrantService()
        viewer = AuthPrincipal(
            username="viewer-x",
            role=UserRole.VIEWER,
            user_public_id="user-viewer",
            operator_public_ids=[],
        )
        service.repository.list_active_wallets = AsyncMock()
        service.repository.list_accessible_wallets_for_operators = AsyncMock()
        service.repository.list_readable_wallets_for_user = AsyncMock()

        result = await service.list_accessible_wallet_public_ids(
            principal=viewer, as_of=datetime.now(UTC)
        )

        assert result == set()
        active_mock: AsyncMock = service.repository.list_active_wallets
        active_mock.assert_not_called()
        scoped_mock: AsyncMock = service.repository.list_accessible_wallets_for_operators
        scoped_mock.assert_not_called()
        readable_mock: AsyncMock = service.repository.list_readable_wallets_for_user
        readable_mock.assert_not_called()

    @pytest.mark.asyncio
    async def test_viewer_with_operators_uses_scoped_query(self) -> None:
        """VIEWER with operator memberships forwards to the scoped repo query.

        Wallet rows are projected down to a public_id set.
        """
        service = ScopeGrantService()
        as_of = datetime(2026, 5, 7, 12, 0, 0, tzinfo=UTC)
        viewer = AuthPrincipal(
            username="viewer-x",
            role=UserRole.VIEWER,
            user_public_id="user-viewer",
            operator_public_ids=["op-1", "op-2"],
        )
        service.repository.list_active_wallets = AsyncMock()
        service.repository.list_accessible_wallets_for_operators = AsyncMock(
            return_value=[{"public_id": "wal-A"}, {"public_id": "wal-B"}]
        )

        result = await service.list_accessible_wallet_public_ids(principal=viewer, as_of=as_of)

        assert result == {"wal-A", "wal-B"}
        active_mock: AsyncMock = service.repository.list_active_wallets
        active_mock.assert_not_called()
        scoped_mock: AsyncMock = service.repository.list_accessible_wallets_for_operators
        scoped_mock.assert_awaited_once_with(operator_public_ids=["op-1", "op-2"], as_of=as_of)


class TestStatelessness:
    """Pins the "no coordinator-owned mutable state" invariant."""

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
