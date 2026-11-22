"""Tests for the kill-switch flow.

Covers :meth:`UserService.deactivate_user` — the SOLE publisher of
``admin.user_deactivated`` per the single-publisher rule. The method
SCD2-flips ``users.is_active=False``, drives
:meth:`TokenManager.revoke_user_sessions` synchronously, commits, then
publishes the bus event.

The DB-state mutations (`close_and_insert` + token revocation) are
already integration-tested in
``tests/auth/test_revoke_user_sessions.py``. These
tests focus on the new orchestration contract: ordering, single-publisher
invariant, payload schema, graceful degradation on missing/failing
publisher.
"""

from collections.abc import Generator
from datetime import UTC
from datetime import datetime
from typing import Literal
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import call
from unittest.mock import patch

import pytest
from fastapi import FastAPI
from fastapi import HTTPException
from fastapi.testclient import TestClient

from snapper.api.schemas.base import MessageResponse
from snapper.auth.dependencies import require_authentication
from snapper.auth.dependencies import validate_csrf_token
from snapper.auth.domain.roles import UserRole
from snapper.auth.routes import deactivate_user as deactivate_route
from snapper.auth.routes import router
from snapper.auth.routes import update_user as update_route
from snapper.auth.schemas.principal import AuthPrincipal
from snapper.auth.schemas.requests import DeactivateUserBody
from snapper.auth.schemas.requests import DeactivateUserRequest
from snapper.auth.schemas.requests import UpdateUserBody
from snapper.auth.schemas.requests import UpdateUserRequest
from snapper.auth.schemas.responses import UserResponse
from snapper.auth.schemas.user import UserProfile
from snapper.auth.tokens import TokenManager
from snapper.auth.user_service import UserAuthorityConflictError
from snapper.auth.user_service import UserService
from snapper.data.models import KNOWN_TO_MAX
from snapper.data.models import User
from snapper.data.repository import SQLAlchemyRepository
from snapper.data.repository import TemporalVersionConflictError
from snapper.messaging.infrastructure.publisher import SequenceTracker
from snapper.messaging.schemas.data import UserDeactivatedData

_BCRYPT_FAKE_DIGEST = "bcrypt-fake"
_WriterPath = Literal[
    "deactivate",
    "update_deactivate",
    "update_profile",
    "preferences",
    "change_password",
    "admin_reset_password",
    "reset_password_by_username",
]
_TX_START_STATEMENTS = frozenset(
    {
        "BEGIN IMMEDIATE",
        "SET TRANSACTION ISOLATION LEVEL READ COMMITTED",
    }
)


async def _invoke_writer_path(service: UserService, path: _WriterPath, target: MagicMock) -> None:
    """Invoke one UserService SCD2 writer path for transaction-start verification.

    Args:
        service: UserService under test.
        path: Logical writer operation to exercise.
        target: Mock target user row supplying public_id and username.
    """
    if path == "deactivate":
        assert await service.deactivate_user(target.public_id, reason=None)
    elif path == "update_deactivate":
        result = await service.update_user(target.username, is_active=False)
        assert result is not None and not result.is_active
    elif path == "update_profile":
        assert await service.update_user(target.username, email="updated@example.com") is not None
    elif path == "preferences":
        assert await service.update_self_preferences(target.username, "pl") is not None
    elif path == "change_password":
        assert await service.change_password(target.username, "old", "new") is True
    elif path == "admin_reset_password":
        await service.admin_reset_password(target.username, "new")
    else:
        await service.reset_password_by_username(target.username, "new")


class _RecordingPublisher:
    """Stub MessagePublisher capturing every (topic, payload) pair.

    Mirrors the surface of `MessagePublisher.send` used by
    `UserService._publish_user_deactivated` so tests can substitute it
    via `set_msg_publisher` without binding a real ZMQ socket.
    """

    def __init__(self) -> None:
        self.sent: list[tuple[str, UserDeactivatedData]] = []

    async def send(self, stream_key: str, data: UserDeactivatedData) -> None:
        self.sent.append((stream_key, data))


class _RaisingPublisher(_RecordingPublisher):
    """Publisher whose `send` raises after recording the call.

    Confirms that a broker hiccup never rolls back a committed
    deactivation — the local in-process kill switch has already won.
    """

    async def send(self, stream_key: str, data: UserDeactivatedData) -> None:
        await super().send(stream_key, data)
        raise RuntimeError("broker unreachable")


def _make_db_user(public_id: str, *, is_active: bool = True) -> MagicMock:
    """Return a MagicMock User row with the columns `deactivate_user` reads."""
    db_user = MagicMock()
    db_user.id = 7
    db_user.public_id = public_id
    db_user.session_id = "seed-session"
    db_user.sequence_id = 1
    db_user.username = f"user-{public_id}"
    db_user.email = f"user-{public_id}@example.com"
    db_user.password_hash = _BCRYPT_FAKE_DIGEST
    db_user.role = "viewer"
    db_user.is_active = is_active
    db_user.default_language = "pl"
    db_user.created_at = datetime(2026, 1, 1, tzinfo=UTC)
    db_user.timestamp = datetime(2026, 1, 1, tzinfo=UTC)
    db_user.known_to = KNOWN_TO_MAX
    return db_user


def _build_service(
    *, existing_db_user: MagicMock | None, revoke_count: int = 0
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
    session_mock.scalar.return_value = None
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
def _patch_token_manager_lookup() -> Generator[MagicMock]:
    """Keep `get_token_manager` patched throughout each test body.

    `UserService.deactivate_user` resolves the manager via
    `get_token_manager()` at call time. Tests substitute their own
    stub through `_build_service` and then bind it on this patch so
    the production code path uses the supplied AsyncMock.
    """
    with patch("snapper.auth.user_service.get_token_manager") as mock:
        yield mock


class TestDeactivateUserOrchestration:
    """Coverage for the canonical kill-switch flow."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize("has_grants", [False, True])
    async def test_postgresql_locks_grant_owners_before_the_target(
        self, has_grants: bool, _patch_token_manager_lookup: MagicMock
    ) -> None:
        """Deactivate only after locking the owners of the target's active grants.

        Given a PostgreSQL user with or without incoming grants.
        When an administrator deactivates the user.
        Then the transaction sets isolation and locks any owners before the target.
        """
        target = _make_db_user("target")
        service, session, commit, revoke, manager = _build_service(existing_db_user=target)
        _patch_token_manager_lookup.return_value = manager
        owners = [User(public_id="owner", known_to=KNOWN_TO_MAX)]
        session.scalars.side_effect = [["owner"], owners] if has_grants else [[]]
        with (
            patch.object(service.repository, "dialect_name", "postgresql"),
            patch("snapper.auth.user_service.close_and_insert_existing", new=AsyncMock()),
        ):
            assert await service.deactivate_user(target.public_id, reason=None)
        statements = [
            call.args[0] for call in session.method_calls if call[0] in {"execute", "scalars"}
        ]
        assert str(statements[0]) == "SET TRANSACTION ISOLATION LEVEL READ COMMITTED"
        assert "oauth_grants.delegate_user_public_id = (SELECT users.public_id" in str(
            statements[1]
        )
        assert "oauth_grants.revoked_at IS NULL" in str(statements[1])
        if has_grants:
            assert "ORDER BY users.public_id FOR UPDATE" in str(statements[2])
            assert statements[2].get_execution_options()["populate_existing"]
        target_lock = statements[3 if has_grants else 2]
        assert "users.public_id = " in str(target_lock) and "FOR UPDATE" in str(target_lock)
        revoke.assert_awaited_once_with(target.public_id, service.repository)
        commit.assert_awaited_once()

    @pytest.mark.asyncio
    @pytest.mark.parametrize("second_conflict", [False, True])
    async def test_postgresql_retries_a_closed_grant_owner_before_revocation(
        self, second_conflict: bool, _patch_token_manager_lookup: MagicMock
    ) -> None:
        """A stale grant owner forces a new deactivation transaction.

        Given a grant whose owner version closes while deactivation waits on its lock.
        When the lock returns that closed row.
        Then the next transaction prepares authority again before deciding whether to write.

        Args:
            second_conflict: Whether the second owner lock still observes a closed row.
            _patch_token_manager_lookup: Mocked legacy token manager lookup.
        """
        target = _make_db_user("delegate")
        service, session, commit, revoke, manager = _build_service(existing_db_user=target)
        _patch_token_manager_lookup.return_value = manager
        closed_owner = User(public_id="owner", known_to=datetime(2020, 1, 1, tzinfo=UTC))
        current_owner = User(public_id="owner", known_to=KNOWN_TO_MAX)
        session.scalars.side_effect = [["owner"], [closed_owner]]
        first_context = service.repository.session.return_value
        second, second_context = _lock_attempt(target, None)
        second.scalars.side_effect = [
            ["owner"],
            [closed_owner if second_conflict else current_owner],
        ]
        with (
            patch.object(service.repository, "dialect_name", "postgresql"),
            patch.object(
                service.repository, "session", side_effect=[first_context, second_context]
            ),
            patch("snapper.auth.user_service.close_and_insert_existing", new=AsyncMock()) as mutate,
        ):
            if second_conflict:
                with pytest.raises(UserAuthorityConflictError, match="OAuth grant owner changed"):
                    await service.deactivate_user(target.public_id, reason=None)
            else:
                assert await service.deactivate_user(target.public_id, reason=None)
        first_context.__aexit__.assert_awaited_once()
        assert first_context.__aexit__.await_args.args[0] is UserAuthorityConflictError
        session.execute.assert_awaited_once()
        commit.assert_not_awaited()
        assert second.scalars.await_count == 2
        assert mutate.await_count == int(not second_conflict)
        assert revoke.await_count == int(not second_conflict)
        assert second.commit.await_count == int(not second_conflict)

    @pytest.mark.asyncio
    async def test_happy_path_publishes_bus_event_with_expected_payload(
        self, _patch_token_manager_lookup: MagicMock
    ) -> None:
        """A successful deactivation publishes exactly one bus event.

        Given: an active user row + a configured publisher,
        When: deactivate_user runs with a reason,
        Then: (a) returns True; (b) publisher.send called once on
            topic `admin.user_deactivated`; (c) payload carries
            `user_public_id`, `deactivated_at`, `reason`;
            (d) `payload.deactivated_at == payload.timestamp`.
        """
        existing = _make_db_user("user-1")
        service, _session, _commit, _revoke, tm = _build_service(existing_db_user=existing)
        _patch_token_manager_lookup.return_value = tm
        publisher = _RecordingPublisher()
        service.set_msg_publisher(publisher)
        with patch("snapper.auth.user_service.close_and_insert_existing", new=AsyncMock()):
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
        with patch(
            "snapper.auth.user_service.close_and_insert_existing", new=AsyncMock()
        ) as close_insert:
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
            the bus event lands strictly after the commit. This
            invariant prevents subscribers from seeing a stale
            `is_active=True` snapshot.
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
            async def send(self, stream_key: str, data: UserDeactivatedData) -> None:
                publish_observation.append(commit_observed[0])
                await super().send(stream_key, data)

        service.set_msg_publisher(_ObservingPublisher())
        with patch("snapper.auth.user_service.close_and_insert_existing", new=AsyncMock()):
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
        with patch("snapper.auth.user_service.close_and_insert_existing", new=AsyncMock()):
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
        with patch("snapper.auth.user_service.close_and_insert_existing", new=AsyncMock()):
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
        with patch("snapper.auth.user_service.close_and_insert_existing", new=AsyncMock()):
            await service.deactivate_user("user-5", reason=None)
        _, payload = publisher.sent[0]
        assert isinstance(payload, UserDeactivatedData)
        assert payload.reason is None

    @pytest.mark.asyncio
    async def test_revoke_called_before_session_commit(
        self, _patch_token_manager_lookup: MagicMock
    ) -> None:
        """Token revocation runs BEFORE the user-row commit.

        The ordering is: SCD2 close+insert (uncommitted) →
        revoke_user_sessions (its own committed sub-tx) → user commit
        → bus publish. If the user commit fails, tokens stay revoked
        — the safer failure mode (kill switch wins).
        """
        existing = _make_db_user("user-6")
        service, _session, commit, revoke, tm = _build_service(existing_db_user=existing)
        _patch_token_manager_lookup.return_value = tm
        ordering: list[str] = []

        async def _record_revoke(_uid: str, _repo: object) -> int:
            ordering.append("revoke")
            return 0

        revoke.side_effect = _record_revoke

        async def _record_commit() -> None:
            ordering.append("commit")

        commit.side_effect = _record_commit
        publisher = _RecordingPublisher()
        service.set_msg_publisher(publisher)
        with patch("snapper.auth.user_service.close_and_insert_existing", new=AsyncMock()):
            await service.deactivate_user("user-6", reason=None)
        assert ordering == ["revoke", "commit"]

    @pytest.mark.asyncio
    async def test_revoke_failure_does_not_publish_and_propagates(
        self, _patch_token_manager_lookup: MagicMock
    ) -> None:
        """Token revocation failure rolls back uncommitted user mutation + skips publish.

        This is a load-bearing failure contract — if revoke raises,
        the SCD2 close+insert (still uncommitted) MUST be discarded by
        the session's `__aexit__` rollback, the bus event MUST NOT
        fire (the deactivation never landed), and the exception MUST
        propagate so the route returns 5xx instead of a fake 200.
        """
        existing = _make_db_user("user-revoke-fail")
        service, _session, commit, revoke, tm = _build_service(existing_db_user=existing)
        _patch_token_manager_lookup.return_value = tm
        revoke.side_effect = RuntimeError("blacklist seed failed")
        publisher = _RecordingPublisher()
        service.set_msg_publisher(publisher)
        with (
            patch("snapper.auth.user_service.close_and_insert_existing", new=AsyncMock()),
            pytest.raises(RuntimeError, match="blacklist seed failed"),
        ):
            await service.deactivate_user("user-revoke-fail", reason="bad")
        commit.assert_not_called()
        assert publisher.sent == []

    @pytest.mark.asyncio
    async def test_commit_failure_does_not_publish_and_propagates(
        self, _patch_token_manager_lookup: MagicMock
    ) -> None:
        """Commit failure leaves tokens revoked + skips publish + propagates.

        If the user commit fails AFTER token revocation already
        committed in its own subtransaction, the kill switch wins
        (tokens stay revoked, user row stays active) — the safer
        failure mode. The bus event MUST NOT fire because no
        `admin.user_deactivated` actually happened from a subscriber's
        POV. The exception MUST propagate so the route returns 5xx.
        """
        existing = _make_db_user("user-commit-fail")
        service, _session, commit, revoke, tm = _build_service(existing_db_user=existing)
        _patch_token_manager_lookup.return_value = tm
        commit.side_effect = RuntimeError("DB commit lost connection")
        publisher = _RecordingPublisher()
        service.set_msg_publisher(publisher)
        with (
            patch("snapper.auth.user_service.close_and_insert_existing", new=AsyncMock()),
            pytest.raises(RuntimeError, match="DB commit lost connection"),
        ):
            await service.deactivate_user("user-commit-fail", reason="net flap")
        revoke.assert_awaited_once_with("user-commit-fail", service.repository)
        assert publisher.sent == []


class TestDeactivateUserRaceLoss:
    """Race-loss safety verified against a real in-memory aiosqlite DB.

    An earlier revision of `deactivate_user` filtered by
    `where_active_now(User)` only — it did NOT filter by
    `User.is_active`. So if Admin B deactivated a user between Admin
    A's route lookup and Admin A's service call, the service would
    find the new SCD2 row (is_active=False but still known_to=MAX),
    process it again, re-revoke (no-op), and re-publish a duplicate
    `admin.user_deactivated` event. The fix adds `User.is_active` +
    `with_for_update()` to the SELECT so the query returns None for
    already-inactive users → the route surfaces 404, not 200.
    """

    @pytest.mark.asyncio
    async def test_already_inactive_user_returns_false_no_revoke_no_publish(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Real-DB SELECT honours the `is_active=True` filter in deactivate_user."""
        UserService.clear_instance()
        TokenManager.clear_instance()
        monkeypatch.setattr(TokenManager, "_initialized", False)
        repo = SQLAlchemyRepository("sqlite+aiosqlite:///:memory:")
        await repo.create_all()
        seed_time = datetime(2026, 1, 1, tzinfo=UTC)
        async with repo.session() as s:
            s.add(
                User(
                    public_id="user-already-inactive",
                    session_id="seed",
                    sequence_id=1,
                    timestamp=seed_time,
                    known_to=KNOWN_TO_MAX,
                    username="ghost",
                    email="ghost@example.com",
                    password_hash=_BCRYPT_FAKE_DIGEST,
                    role="viewer",
                    is_active=False,
                    created_at=seed_time,
                )
            )
            await s.commit()
        with patch("snapper.auth.user_service.get_repository", return_value=repo):
            service = UserService()
        publisher = _RecordingPublisher()
        service.set_msg_publisher(publisher)
        revoke_mock = AsyncMock(return_value=0)
        tm_stub = MagicMock()
        tm_stub.revoke_user_sessions = revoke_mock
        with patch("snapper.auth.user_service.get_token_manager", return_value=tm_stub):
            result = await service.deactivate_user("user-already-inactive", reason=None)
        assert result is False
        assert publisher.sent == []
        revoke_mock.assert_not_awaited()
        async with repo.session() as s:
            count = (
                await s.execute(
                    User.__table__.select().where(
                        User.public_id == "user-already-inactive",
                        User.known_to == KNOWN_TO_MAX,
                    )
                )
            ).all()
        assert len(count) == 1


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


async def _call_authority_route(
    operation: Literal["deactivate", "update"], target: MagicMock
) -> MessageResponse | UserResponse:
    """Call the real admin route for a SCD2 authority reduction.

    Args:
        operation: Admin route to invoke.
        target: User whose authority the route reduces.

    Returns:
        The route's success envelope.
    """
    if operation == "deactivate":
        return await deactivate_route(
            request=_make_rest_request(),
            user_id=target.username,
            current_user=_make_principal("admin"),
            _csrf=None,
            body=_make_request_envelope("policy"),
        )
    return await update_route(
        request=_make_rest_request(),
        user_id=target.username,
        current_user=_make_principal("admin"),
        _csrf=None,
        user_data=UpdateUserRequest(
            public_id="env-public-id",
            timestamp=datetime(2026, 4, 19, tzinfo=UTC),
            session_id="env-session",
            sequence_id=1,
            payload=UpdateUserBody(is_active=False),
        ),
    )


def _lock_attempt(
    locked_user: MagicMock | None, visible_id: int | None
) -> tuple[AsyncMock, MagicMock]:
    """Build a session whose locked select and unlocked probe have independent results.

    Args:
        locked_user: Entity returned from the locking select.
        visible_id: Primary key visible in the probe's newer snapshot.

    Returns:
        The session and its context manager.
    """
    session = AsyncMock()
    session.execute.return_value = MagicMock(scalar_one_or_none=MagicMock(return_value=locked_user))
    session.scalar.return_value = visible_id
    context = MagicMock()
    context.__aenter__.return_value = session
    context.__aexit__.return_value = False
    return session, context


def _assert_authority_probe(session: AsyncMock, active_only: bool) -> None:
    """Verify the probe has the locked select's predicates without its lock.

    Args:
        session: Losing transaction that executed both queries.
        active_only: Whether to expect deactivation's active-user filter.
    """
    lock = next(
        awaited.args[0]
        for awaited in session.execute.await_args_list
        if "FOR UPDATE" in str(awaited.args[0])
    )
    session.scalar.assert_awaited_once()
    probe = session.scalar.await_args.args[0]
    assert "FOR UPDATE" in str(lock)
    assert lock.get_execution_options()["populate_existing"] is True
    assert "FOR UPDATE" not in str(probe)
    assert str(probe).startswith("SELECT users.id ")
    assert str(probe.whereclause) == str(lock.whereclause)
    target_column = "public_id" if active_only else "username"
    assert f"users.{target_column} = " in str(probe.whereclause)
    assert ("AND users.is_active" in str(probe.whereclause)) is active_only
    assert "users.timestamp <= " in str(probe.whereclause)
    assert "users.known_to > " in str(probe.whereclause)


class TestUserAuthorityRetry:
    """Exercise the real admin routes over mocked transaction snapshots."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize("operation", ["deactivate", "update"])
    @pytest.mark.parametrize("status", [200, 404, 409])
    async def test_routes_handle_lost_lock(
        self,
        operation: Literal["deactivate", "update"],
        status: int,
        _patch_token_manager_lookup: MagicMock,
    ) -> None:
        """Only a visible successor calls for a second transaction.

        Given an empty or closed locking result with an optional visible successor.
        When an administrator deactivates or sets is_active to false.
        Then the route returns success after retry, 404 without a successor, or 409 after two conflicts.

        Args:
            operation: Route to exercise.
            status: Expected HTTP status.
            _patch_token_manager_lookup: Mocked legacy token manager lookup.
        """
        target = _make_db_user("target")
        locked = _make_db_user("target")
        locked.email = "current@example.com"
        successor = _make_db_user("target", is_active=False)
        service, _, _, revoke, manager = _build_service(existing_db_user=target)
        service.set_msg_publisher(publisher := _RecordingPublisher())
        _patch_token_manager_lookup.return_value = manager
        deactivation = operation == "deactivate"
        _, preflight = _lock_attempt(None, target.id)
        closed = _make_db_user("target")
        closed.known_to = datetime(2020, 1, 1, tzinfo=UTC)
        first, first_context = _lock_attempt(None, None if status == 404 else target.id)
        second, second_context = (
            _lock_attempt(locked, None) if status == 200 else _lock_attempt(closed, target.id)
        )
        second.scalars.return_value = ["late-jti"]
        manager.attach_mock(second.commit, "commit_attempt")
        contexts = [preflight, first_context] if deactivation else [first_context]
        if status != 404:
            contexts.append(second_context)
        with (
            patch("snapper.auth.routes.get_user_service", return_value=service),
            patch.object(
                service, "get_user_by_id", return_value=service._db_user_to_auth_user(target)
            ),
            patch.object(service.repository, "dialect_name", "sqlite"),
            patch.object(service.repository, "session", side_effect=contexts) as open_session,
            patch(
                "snapper.auth.user_service.close_and_insert_existing", return_value=successor
            ) as mutate,
        ):
            if status == 200:
                response = await _call_authority_route(operation, target)
                assert response.payload in (
                    f"User '{target.username}' has been deactivated",
                    service._db_user_to_auth_user(successor),
                )
            else:
                with pytest.raises(HTTPException) as raised:
                    await _call_authority_route(operation, target)
                assert raised.value.status_code == status
        assert open_session.call_count == len(contexts)
        _assert_authority_probe(first, active_only=operation == "deactivate")
        first.commit.assert_not_awaited()
        if status == 409:
            _assert_authority_probe(second, active_only=operation == "deactivate")
        if status == 200:
            second.commit.assert_awaited_once()
            mutate.assert_awaited_once()
            values = mutate.await_args_list[0].kwargs["new_values"]
            assert values["is_active"] is False
            assert values["email"] == locked.email
        else:
            second.commit.assert_not_awaited()
            mutate.assert_not_awaited()
        deactivated = status == 200 and deactivation
        assert revoke.await_count == int(deactivation)
        assert manager.blacklist_token_immediately.call_count == int(deactivated)
        if deactivated:
            assert manager.mock_calls[-2:] == [
                call.commit_attempt(),
                call.blacklist_token_immediately("late-jti"),
            ]
        assert len(publisher.sent) == int(deactivated)

    @pytest.mark.asyncio
    async def test_delete_user_propagates_two_lock_conflicts(self) -> None:
        """Soft deletion surfaces the conflict from its second transaction.

        Given two locking reads that miss a visible successor.
        When the caller deletes the user by username.
        Then the second conflict propagates without committing either attempt.
        """
        service, _, _, _, _ = _build_service(existing_db_user=None)
        first, first_context = _lock_attempt(None, 1)
        second, second_context = _lock_attempt(None, 2)
        with (
            patch.object(
                service.repository, "session", side_effect=[first_context, second_context]
            ),
            pytest.raises(UserAuthorityConflictError) as raised,
        ):
            await service.delete_user("target")
        _assert_authority_probe(first, active_only=False)
        _assert_authority_probe(second, active_only=False)
        assert first_context.__aexit__.await_args.args[0] is UserAuthorityConflictError
        assert second_context.__aexit__.await_args.args[1] is raised.value
        first.commit.assert_not_awaited()
        second.commit.assert_not_awaited()

    @pytest.mark.asyncio
    @pytest.mark.parametrize("dialect", ["sqlite", "postgresql"])
    @pytest.mark.parametrize(
        "path",
        [
            "deactivate",
            "update_deactivate",
            "update_profile",
            "preferences",
            "change_password",
            "admin_reset_password",
            "reset_password_by_username",
        ],
    )
    async def test_user_writers_emit_single_transaction_start_per_attempt(
        self,
        dialect: Literal["sqlite", "postgresql"],
        path: _WriterPath,
        _patch_token_manager_lookup: MagicMock,
    ) -> None:
        """Every User SCD2 writer emits exactly one dialect transaction start first.

        Given a SQLite or PostgreSQL repository mock and an active target user.
        When any of the User SCD2 writer paths executes a write attempt.
        Then BEGIN IMMEDIATE (SQLite) or READ COMMITTED (PostgreSQL) is first and runs once.

        Args:
            dialect: Database dialect name under test.
            path: User SCD2 writer operation under test.
            _patch_token_manager_lookup: Mocked legacy token manager lookup.
        """
        target = _make_db_user("target")
        successor = _make_db_user(
            "target", is_active=path not in {"deactivate", "update_deactivate"}
        )
        service, _, _, _, manager = _build_service(existing_db_user=target)
        _patch_token_manager_lookup.return_value = manager
        write_session, write_context = _lock_attempt(target, None)
        write_session.scalars.return_value = []
        auxiliary_sessions: list[AsyncMock] = []
        contexts: list[MagicMock] = []
        if dialect == "sqlite" and path == "deactivate":
            preflight, preflight_context = _lock_attempt(None, target.id)
            auxiliary_sessions.append(preflight)
            contexts.append(preflight_context)
        if path == "change_password":
            read_session, read_context = _lock_attempt(None, target.id)
            read_session.scalar.return_value = target.password_hash
            auxiliary_sessions.append(read_session)
            contexts.append(read_context)
        contexts.append(write_context)
        with (
            patch.object(service.repository, "dialect_name", dialect),
            patch.object(service.repository, "session", side_effect=contexts),
            patch.object(service, "_verify_password", return_value=True),
            patch.object(service, "hash_password", return_value=_BCRYPT_FAKE_DIGEST),
            patch("snapper.auth.user_service.close_and_insert_existing", return_value=successor),
        ):
            await _invoke_writer_path(service, path, target)
        expected_start = (
            "BEGIN IMMEDIATE"
            if dialect == "sqlite"
            else "SET TRANSACTION ISOLATION LEVEL READ COMMITTED"
        )
        executed = [str(awaited.args[0]) for awaited in write_session.execute.await_args_list]
        assert executed[0] == expected_start
        assert [stmt for stmt in executed if stmt in _TX_START_STATEMENTS] == [expected_start]
        for auxiliary in auxiliary_sessions:
            aux_executed = [str(awaited.args[0]) for awaited in auxiliary.execute.await_args_list]
            assert not any(stmt in _TX_START_STATEMENTS for stmt in aux_executed)


@pytest.mark.parametrize(
    ("path", "command_type", "payload", "expected"),
    [
        pytest.param(
            "/api/auth/me/update",
            "update_auth_me_request",
            {"default_language": "en"},
            {"default_language": "en"},
            id="preferences",
        ),
        pytest.param(
            "/api/auth/users/user-target/change-password",
            "change_password_request",
            {"current_password": "test-current", "new_password": "test-replacement"},
            "Password changed successfully",
            id="change-password",
        ),
        pytest.param(
            "/api/auth/users/user-target/admin-reset-password",
            "admin_reset_password_request",
            {"new_password": "test-replacement"},
            "Password reset successfully for user user-target",
            id="admin-reset-password",
        ),
    ],
)
@pytest.mark.parametrize("conflicts", [1, 2], ids=["retry-succeeds", "retry-conflicts"])
def test_account_write_routes_resolve_authority_conflicts(
    path: str,
    command_type: str,
    payload: dict[str, str],
    expected: dict[str, str] | str,
    conflicts: int,
) -> None:
    """Account write routes preserve the response contract through a retry.

    Given one or two transactions whose locked read misses a visible row.
    When a caller submits a preference or password change over HTTP.
    Then the route returns its success envelope after one conflict or a 409 after two.

    Args:
        path: Account write URL.
        command_type: Request envelope discriminator.
        payload: Request body inside the envelope.
        expected: Success payload or expected fields of that payload.
        conflicts: Number of transaction snapshots that show a conflict.
    """
    target = _make_db_user("target")
    successor = _make_db_user("target")
    successor.default_language = "en"
    service, _, _, _, _ = _build_service(existing_db_user=target)
    read_session = AsyncMock()
    read_session.scalar.return_value = target.password_hash
    read_context = MagicMock()
    read_context.__aenter__.return_value = read_session
    read_context.__aexit__.return_value = False
    first, first_context = _lock_attempt(None, target.id)
    second, second_context = (
        _lock_attempt(target, None) if conflicts == 1 else _lock_attempt(None, target.id)
    )
    contexts = (
        [read_context, first_context, read_context, second_context]
        if command_type == "change_password_request"
        else [first_context, second_context]
    )
    principal = _make_principal(target.username)
    app = FastAPI()
    app.state.rest_tracker = SequenceTracker()
    app.include_router(router, prefix="/api")
    app.dependency_overrides[require_authentication] = lambda: principal
    app.dependency_overrides[validate_csrf_token] = lambda: None
    with (
        patch("snapper.auth.routes.get_user_service", return_value=service),
        patch.object(service.repository, "session", side_effect=contexts) as open_session,
        patch.object(service, "_verify_password", return_value=True),
        patch.object(service, "hash_password", return_value=_BCRYPT_FAKE_DIGEST),
        patch(
            "snapper.auth.user_service.close_and_insert_existing", return_value=successor
        ) as mutate,
        TestClient(app) as client,
    ):
        response = client.post(
            path,
            json={
                "type": command_type,
                "public_id": "env-public-id",
                "session_id": "env-session",
                "sequence_id": 1,
                "timestamp": "2026-04-19T00:00:00Z",
                "payload": payload,
            },
        )
    assert open_session.call_count == len(contexts)
    if command_type == "change_password_request":
        assert read_session.scalar.await_count == 2
        read_session.commit.assert_not_awaited()
    first.scalar.assert_awaited_once()
    first.commit.assert_not_awaited()
    if conflicts == 2:
        assert response.status_code == 409
        assert response.json() == {"detail": "User version changed. Retry the request."}
        second.scalar.assert_awaited_once()
        second.commit.assert_not_awaited()
        mutate.assert_not_awaited()
        return
    assert response.status_code == 200, response.text
    second.scalar.assert_not_awaited()
    second.commit.assert_awaited_once()
    mutate.assert_awaited_once()
    if isinstance(expected, str):
        assert MessageResponse.model_validate_json(response.content).payload == expected
    else:
        profile = UserResponse.model_validate_json(response.content).payload
        assert profile.username == target.username
        assert profile.default_language == expected["default_language"]


def test_account_write_route_maps_repeated_temporal_version_conflict_to_409() -> None:
    """Repeated TemporalVersionConflictError from close_and_insert maps to HTTP 409.

    Given close_and_insert raises TemporalVersionConflictError on both write attempts.
    When an authenticated caller posts an account preference update over HTTP.
    Then the route returns HTTP 409 with the canonical version-conflict detail.
    """
    target = _make_db_user("target")
    service, _, _, _, _ = _build_service(existing_db_user=target)
    first, first_context = _lock_attempt(target, None)
    second, second_context = _lock_attempt(target, None)
    conflict = TemporalVersionConflictError(
        "users",
        target.id,
        datetime(2026, 1, 1, tzinfo=UTC),
        datetime(2026, 1, 2, tzinfo=UTC),
    )
    principal = _make_principal(target.username)
    app = FastAPI()
    app.state.rest_tracker = SequenceTracker()
    app.include_router(router, prefix="/api")
    app.dependency_overrides[require_authentication] = lambda: principal
    app.dependency_overrides[validate_csrf_token] = lambda: None
    with (
        patch("snapper.auth.routes.get_user_service", return_value=service),
        patch.object(service.repository, "session", side_effect=[first_context, second_context]),
        patch(
            "snapper.auth.user_service.close_and_insert_existing",
            new=AsyncMock(side_effect=[conflict, conflict]),
        ) as mutate,
        TestClient(app) as client,
    ):
        response = client.post(
            "/api/auth/me/update",
            json={
                "type": "update_auth_me_request",
                "public_id": "env-public-id",
                "session_id": "env-session",
                "sequence_id": 1,
                "timestamp": "2026-04-19T00:00:00Z",
                "payload": {"default_language": "en"},
            },
        )
    assert response.status_code == 409
    assert response.json() == {"detail": "User version changed. Retry the request."}
    assert mutate.await_count == 2
    first.commit.assert_not_awaited()
    second.commit.assert_not_awaited()


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
            layer (so the service contract stays public_id-only).
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
        rest_request = _make_rest_request()
        admin_principal = _make_principal("admin")
        request_envelope = _make_request_envelope(None)

        with (
            patch("snapper.auth.routes.get_user_service", return_value=mock_service),
            pytest.raises(HTTPException) as exc_info,
        ):
            await deactivate_route(
                request=rest_request,
                user_id="admin",
                current_user=admin_principal,
                _csrf=None,
                body=request_envelope,
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
        rest_request = _make_rest_request()
        admin_principal = _make_principal("admin")
        request_envelope = _make_request_envelope("audit")

        with (
            patch("snapper.auth.routes.get_user_service", return_value=mock_service),
            pytest.raises(HTTPException) as exc_info,
        ):
            await deactivate_route(
                request=rest_request,
                user_id="ghost",
                current_user=admin_principal,
                _csrf=None,
                body=request_envelope,
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
        rest_request = _make_rest_request()
        admin_principal = _make_principal("admin")
        request_envelope = _make_request_envelope(None)

        with (
            patch("snapper.auth.routes.get_user_service", return_value=mock_service),
            pytest.raises(HTTPException) as exc_info,
        ):
            await deactivate_route(
                request=rest_request,
                user_id="raced",
                current_user=admin_principal,
                _csrf=None,
                body=request_envelope,
            )
        assert exc_info.value.status_code == 404
