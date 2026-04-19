"""Tests for the Day 3b kill-switch flow (plan §3.6.1).

Covers :meth:`UserService.deactivate_user` — the SOLE publisher of
``admin.user_deactivated`` per the single-publisher rule. The method
SCD2-flips ``users.is_active=False``, drives
:meth:`TokenManager.revoke_user_sessions` synchronously, commits, then
publishes the bus event.

The DB-state mutations (`close_and_insert` + token revocation) are
already integration-tested in
``tests/auth/test_revoke_user_sessions.py`` (Day 3a primitive). These
tests focus on the new orchestration contract: ordering, single-publisher
invariant, payload schema, graceful degradation on missing/failing
publisher.
"""

from datetime import UTC
from datetime import datetime
from typing import Any
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest
from fastapi import HTTPException

from snapper.api.schemas.base import StrictDataSchema
from snapper.auth.domain.roles import UserRole
from snapper.auth.routes import deactivate_user as deactivate_route
from snapper.auth.schemas.principal import AuthPrincipal
from snapper.auth.schemas.requests import DeactivateUserBody
from snapper.auth.schemas.requests import DeactivateUserRequest
from snapper.auth.schemas.user import UserProfile
from snapper.auth.tokens import TokenManager
from snapper.auth.user_service import UserService
from snapper.data.models import KNOWN_TO_MAX
from snapper.messaging.infrastructure.publisher import SequenceTracker
from snapper.messaging.schemas.data import UserDeactivatedData


class _RecordingPublisher:
    """Stub MessagePublisher capturing every (topic, payload) pair.

    Mirrors the surface of `MessagePublisher.send` used by
    `UserService._publish_user_deactivated` so tests can substitute it
    via `set_msg_publisher` without binding a real ZMQ socket.
    """

    def __init__(self) -> None:
        self.sent: list[tuple[str, StrictDataSchema[Any]]] = []

    async def send(self, stream_key: str, data: StrictDataSchema[Any]) -> None:
        self.sent.append((stream_key, data))


class _RaisingPublisher(_RecordingPublisher):
    """Publisher whose `send` raises after recording the call.

    Confirms that a broker hiccup never rolls back a committed
    deactivation — the local in-process kill switch has already won.
    """

    async def send(self, stream_key: str, data: StrictDataSchema[Any]) -> None:
        await super().send(stream_key, data)
        raise RuntimeError("broker unreachable")


def _make_db_user(public_id: str, *, is_active: bool = True) -> Any:
    """Return a MagicMock User row with the columns `deactivate_user` reads."""
    db_user = MagicMock()
    db_user.id = 7
    db_user.public_id = public_id
    db_user.session_id = "seed-session"
    db_user.sequence_id = 1
    db_user.username = f"user-{public_id}"
    db_user.email = f"user-{public_id}@example.com"
    db_user.password_hash = "bcrypt-fake"
    db_user.role = "viewer"
    db_user.is_active = is_active
    db_user.created_at = datetime(2026, 1, 1, tzinfo=UTC)
    db_user.timestamp = datetime(2026, 1, 1, tzinfo=UTC)
    db_user.known_to = KNOWN_TO_MAX
    return db_user


def _build_service(
    *, existing_db_user: Any | None, revoke_count: int = 0
) -> tuple[UserService, AsyncMock, AsyncMock, AsyncMock, MagicMock]:
    """Construct a UserService whose repo + token manager are wired to mocks.

    Returns:
        Tuple of (service, session_mock, commit_mock, revoke_mock,
        token_manager_stub). The token_manager_stub is reused as the
        return value of `get_token_manager` in the autouse fixture so
        the production code path resolves to the same instance.
    """
    UserService.clear_instance()
    TokenManager.clear_instance()
    TokenManager._initialized = False
    repo = MagicMock()
    session_mock = AsyncMock()
    execute_result = MagicMock()
    execute_result.scalar_one_or_none = MagicMock(return_value=existing_db_user)
    session_mock.execute = AsyncMock(return_value=execute_result)
    commit_mock = AsyncMock()
    session_mock.commit = commit_mock
    session_ctx = MagicMock()
    session_ctx.__aenter__ = AsyncMock(return_value=session_mock)
    session_ctx.__aexit__ = AsyncMock(return_value=False)
    repo.session = MagicMock(return_value=session_ctx)
    revoke_mock = AsyncMock(return_value=revoke_count)
    token_manager_stub = MagicMock()
    token_manager_stub.revoke_user_sessions = revoke_mock
    with patch("snapper.auth.user_service.get_repository", return_value=repo):
        service = UserService()
    return service, session_mock, commit_mock, revoke_mock, token_manager_stub


@pytest.fixture(autouse=True)
def _patch_token_manager_lookup() -> Any:
    """Keep `get_token_manager` patched throughout each test body.

    `UserService.deactivate_user` resolves the manager via
    `get_token_manager()` at call time. Tests substitute their own
    stub through `_build_service` and then bind it on this patch so
    the production code path uses the supplied AsyncMock.
    """
    with patch("snapper.auth.user_service.get_token_manager") as mock:
        yield mock


class TestDeactivateUserOrchestration:
    """Coverage for the canonical kill-switch flow (plan §3.6.1)."""

    @pytest.mark.asyncio
    async def test_happy_path_publishes_bus_event_with_expected_payload(
        self, _patch_token_manager_lookup: MagicMock
    ) -> None:
        """A successful deactivation publishes exactly one bus event.

        Given: an active user row + a configured publisher,
        When: deactivate_user runs with a reason,
        Then: (a) returns True; (b) publisher.send called once on
            topic `admin.user_deactivated`; (c) payload carries
            `user_public_id`, `deactivated_at`, `reason` per §3.6.5;
            (d) `payload.deactivated_at == payload.timestamp`.
        """
        existing = _make_db_user("user-1")
        service, _session, _commit, _revoke, tm = _build_service(existing_db_user=existing)
        _patch_token_manager_lookup.return_value = tm
        publisher = _RecordingPublisher()
        service.set_msg_publisher(publisher)
        with patch("snapper.auth.user_service.close_and_insert", new=AsyncMock()):
            result = await service.deactivate_user("user-1", reason="policy_violation")
        assert result is True
        assert len(publisher.sent) == 1
        topic, payload = publisher.sent[0]
        assert topic == "admin.user_deactivated"
        assert isinstance(payload, UserDeactivatedData)
        assert payload.user_public_id == "user-1"
        assert payload.reason == "policy_violation"
        assert payload.deactivated_at == payload.timestamp

    @pytest.mark.asyncio
    async def test_returns_false_for_unknown_user_no_publish_no_revoke(
        self, _patch_token_manager_lookup: MagicMock
    ) -> None:
        """Ghost user → False, no token revocation, no event emitted.

        Given: the SCD2 lookup returns None (no active row),
        When: deactivate_user runs,
        Then: returns False, neither `revoke_user_sessions` nor the
            publisher are touched. Skipping revoke prevents the kill
            switch from running for users that no longer exist —
            matches the existing `delete_user` semantics.
        """
        service, _session, commit, revoke, tm = _build_service(existing_db_user=None)
        _patch_token_manager_lookup.return_value = tm
        publisher = _RecordingPublisher()
        service.set_msg_publisher(publisher)
        with patch("snapper.auth.user_service.close_and_insert", new=AsyncMock()) as close_insert:
            result = await service.deactivate_user("ghost-user", reason=None)
        assert result is False
        assert publisher.sent == []
        revoke.assert_not_awaited()
        close_insert.assert_not_called()
        commit.assert_not_called()

    @pytest.mark.asyncio
    async def test_publish_runs_after_session_commit(
        self, _patch_token_manager_lookup: MagicMock
    ) -> None:
        """Publish runs AFTER the session commits so subscribers see committed state.

        Given: a publisher that records whether the mocked
            session.commit has already fired,
        When: deactivate_user runs,
        Then: the recorded value is True at publish time — proving
            the bus event lands strictly after the commit. This is
            the §3.6.1 invariant that prevents subscribers from
            seeing a stale `is_active=True` snapshot.
        """
        existing = _make_db_user("user-2")
        service, _session, commit, _revoke, tm = _build_service(existing_db_user=existing)
        _patch_token_manager_lookup.return_value = tm
        commit_observed = [False]

        async def _flag_commit() -> None:
            commit_observed[0] = True

        commit.side_effect = _flag_commit
        publish_observation: list[bool] = []

        class _ObservingPublisher(_RecordingPublisher):
            async def send(self, stream_key: str, data: StrictDataSchema[Any]) -> None:
                publish_observation.append(commit_observed[0])
                await super().send(stream_key, data)

        service.set_msg_publisher(_ObservingPublisher())
        with patch("snapper.auth.user_service.close_and_insert", new=AsyncMock()):
            await service.deactivate_user("user-2", reason="test")
        assert publish_observation == [True]
        commit.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_publisher_unavailable_logs_and_does_not_raise(
        self, _patch_token_manager_lookup: MagicMock
    ) -> None:
        """Missing publisher → in-process kill switch still wins.

        Given: a UserService whose `_msg_publisher` is None
            (lifespan never injected one — single-instance dev case),
        When: deactivate_user runs,
        Then: the call returns True (DB flip + token revocation
            land); no exception escapes; no broadcast attempted.
        """
        existing = _make_db_user("user-3")
        service, _session, commit, revoke, tm = _build_service(existing_db_user=existing)
        _patch_token_manager_lookup.return_value = tm
        with patch("snapper.auth.user_service.close_and_insert", new=AsyncMock()):
            result = await service.deactivate_user("user-3", reason=None)
        assert result is True
        revoke.assert_awaited_once_with("user-3", service.repository)
        commit.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_publisher_failure_does_not_roll_back_deactivation(
        self, _patch_token_manager_lookup: MagicMock
    ) -> None:
        """A broker hiccup must not undo a committed deactivation."""
        existing = _make_db_user("user-4")
        service, _session, commit, revoke, tm = _build_service(existing_db_user=existing)
        _patch_token_manager_lookup.return_value = tm
        publisher = _RaisingPublisher()
        service.set_msg_publisher(publisher)
        with patch("snapper.auth.user_service.close_and_insert", new=AsyncMock()):
            result = await service.deactivate_user("user-4", reason="leaked_key")
        assert result is True
        revoke.assert_awaited_once_with("user-4", service.repository)
        commit.assert_awaited_once()
        assert len(publisher.sent) == 1

    @pytest.mark.asyncio
    async def test_reason_none_propagates_to_payload(
        self, _patch_token_manager_lookup: MagicMock
    ) -> None:
        """`reason=None` survives the round-trip through the DTO."""
        existing = _make_db_user("user-5")
        service, _session, _commit, _revoke, tm = _build_service(existing_db_user=existing)
        _patch_token_manager_lookup.return_value = tm
        publisher = _RecordingPublisher()
        service.set_msg_publisher(publisher)
        with patch("snapper.auth.user_service.close_and_insert", new=AsyncMock()):
            await service.deactivate_user("user-5", reason=None)
        _, payload = publisher.sent[0]
        assert isinstance(payload, UserDeactivatedData)
        assert payload.reason is None

    @pytest.mark.asyncio
    async def test_revoke_called_before_session_commit(
        self, _patch_token_manager_lookup: MagicMock
    ) -> None:
        """Token revocation runs BEFORE the user-row commit (§3.6.1 step 2).

        The plan ordering is: SCD2 close+insert (uncommitted) →
        revoke_user_sessions (its own committed sub-tx) → user commit
        → bus publish. If the user commit fails, tokens stay revoked
        — the safer failure mode (kill switch wins).
        """
        existing = _make_db_user("user-6")
        service, _session, commit, revoke, tm = _build_service(existing_db_user=existing)
        _patch_token_manager_lookup.return_value = tm
        ordering: list[str] = []

        async def _record_revoke(_uid: str, _repo: Any) -> int:
            ordering.append("revoke")
            return 0

        revoke.side_effect = _record_revoke

        async def _record_commit() -> None:
            ordering.append("commit")

        commit.side_effect = _record_commit
        publisher = _RecordingPublisher()
        service.set_msg_publisher(publisher)
        with patch("snapper.auth.user_service.close_and_insert", new=AsyncMock()):
            await service.deactivate_user("user-6", reason=None)
        assert ordering == ["revoke", "commit"]


def _make_rest_request() -> MagicMock:
    """Build a stand-in FastAPI Request whose app.state has a rest_tracker.

    `_mint_provenance` reads `request.app.state.rest_tracker` to stamp
    the MessageResponse envelope. Tests don't care about the exact
    provenance values — only that the call succeeds.
    """
    mock_request = MagicMock()
    mock_request.app.state.rest_tracker = SequenceTracker()
    return mock_request


def _make_principal(username: str) -> AuthPrincipal:
    """Authenticated principal with MANAGE_USERS at the route boundary."""
    return AuthPrincipal(
        username=username,
        role=UserRole.ADMIN,
        is_active=True,
        user_public_id="admin-public-id",
    )


def _make_request_envelope(reason: str | None) -> DeactivateUserRequest:
    """Wrap `reason` in the canonical envelope per `DeactivateUserBody`."""
    return DeactivateUserRequest(
        public_id="env-public-id",
        timestamp=datetime(2026, 4, 19, tzinfo=UTC),
        session_id="env-session",
        sequence_id=1,
        payload=DeactivateUserBody(reason=reason),
    )


class TestDeactivateUserRoute:
    """Coverage for the auth/routes.py deactivate endpoint wiring."""

    @pytest.mark.asyncio
    async def test_route_resolves_username_to_public_id_and_forwards_reason(
        self,
    ) -> None:
        """Happy path: route looks up profile, forwards public_id + reason.

        Given: an authenticated admin and a target username with an
            active row,
        When: the route runs with `reason="manual_compromise"`,
        Then: `UserService.deactivate_user` is awaited with the
            resolved `public_id` and the supplied `reason` — proving
            the username→public_id translation happens at the route
            layer (so the service contract stays public_id-only per
            plan §3.6.1).
        """
        target_profile = UserProfile(
            session_id="t-sid",
            sequence_id=1,
            public_id="target-public-id",
            timestamp=datetime(2026, 1, 1, tzinfo=UTC),
            username="target",
            role=UserRole.VIEWER,
            is_active=True,
            created_at=datetime(2026, 1, 1, tzinfo=UTC),
        )
        mock_service = AsyncMock()
        mock_service.get_user_by_id = AsyncMock(return_value=target_profile)
        mock_service.deactivate_user = AsyncMock(return_value=True)
        with patch("snapper.auth.routes.get_user_service", return_value=mock_service):
            response = await deactivate_route(
                request=_make_rest_request(),
                user_id="target",
                current_user=_make_principal("admin"),
                _csrf=None,
                body=_make_request_envelope("manual_compromise"),
            )
        assert response.payload == "User 'target' has been deactivated"
        mock_service.get_user_by_id.assert_awaited_once_with("target")
        mock_service.deactivate_user.assert_awaited_once_with(
            "target-public-id", "manual_compromise"
        )

    @pytest.mark.asyncio
    async def test_route_returns_400_when_caller_targets_self(self) -> None:
        """Self-deactivation guard returns 400 BEFORE any service call.

        Critical safety invariant: an admin cannot lock themselves
        out. The check fires before either the lookup or the
        deactivate call, so neither is invoked when self-targeting.
        """
        mock_service = AsyncMock()
        with (
            patch("snapper.auth.routes.get_user_service", return_value=mock_service),
            pytest.raises(HTTPException) as exc_info,
        ):
            await deactivate_route(
                request=_make_rest_request(),
                user_id="admin",
                current_user=_make_principal("admin"),
                _csrf=None,
                body=_make_request_envelope(None),
            )
        assert exc_info.value.status_code == 400
        mock_service.get_user_by_id.assert_not_called()
        mock_service.deactivate_user.assert_not_called()

    @pytest.mark.asyncio
    async def test_route_returns_404_when_target_user_absent(self) -> None:
        """Route returns 404 when the target username has no active row.

        Given: `get_user_by_id` returns None (the username does not
            map to an active user),
        When: the route runs,
        Then: HTTPException 404 is raised AND `deactivate_user` is
            never called — the kill-switch primitive does not run for
            ghost targets, so token revocation cannot fire on a wrong
            user.
        """
        mock_service = AsyncMock()
        mock_service.get_user_by_id = AsyncMock(return_value=None)
        mock_service.deactivate_user = AsyncMock()
        with (
            patch("snapper.auth.routes.get_user_service", return_value=mock_service),
            pytest.raises(HTTPException) as exc_info,
        ):
            await deactivate_route(
                request=_make_rest_request(),
                user_id="ghost",
                current_user=_make_principal("admin"),
                _csrf=None,
                body=_make_request_envelope("audit"),
            )
        assert exc_info.value.status_code == 404
        mock_service.deactivate_user.assert_not_called()

    @pytest.mark.asyncio
    async def test_route_returns_404_when_service_reports_race_loss(self) -> None:
        """Race-window 404: lookup found a user but service-side deactivation no-op'd.

        Defends against the gap between the lookup and the SCD2
        close+insert when another admin closed the row first. The
        service returns False, the route surfaces 404 instead of a
        stale 200 success.
        """
        target_profile = UserProfile(
            session_id="t-sid",
            sequence_id=1,
            public_id="raced-public-id",
            timestamp=datetime(2026, 1, 1, tzinfo=UTC),
            username="raced",
            role=UserRole.VIEWER,
            is_active=True,
            created_at=datetime(2026, 1, 1, tzinfo=UTC),
        )
        mock_service = AsyncMock()
        mock_service.get_user_by_id = AsyncMock(return_value=target_profile)
        mock_service.deactivate_user = AsyncMock(return_value=False)
        with (
            patch("snapper.auth.routes.get_user_service", return_value=mock_service),
            pytest.raises(HTTPException) as exc_info,
        ):
            await deactivate_route(
                request=_make_rest_request(),
                user_id="raced",
                current_user=_make_principal("admin"),
                _csrf=None,
                body=_make_request_envelope(None),
            )
        assert exc_info.value.status_code == 404
