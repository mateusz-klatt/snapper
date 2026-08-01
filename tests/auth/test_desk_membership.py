"""Tests for permission-gated human VIEWER desk attachment."""

from datetime import UTC
from datetime import datetime
from datetime import timedelta
from pathlib import Path
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest
from fastapi import HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from starlette.routing import Match

from snapper.auth.domain.permissions import Permission
from snapper.auth.domain.roles import UserRole
from snapper.auth.routes import attach_viewer_to_desk
from snapper.auth.routes import detach_viewer_from_desk
from snapper.auth.routes import list_desk_members
from snapper.auth.routes import router
from snapper.auth.schemas.principal import AuthPrincipal
from snapper.auth.schemas.user import UserProfile
from snapper.auth.user_service import DeskMembershipAuthorizationError
from snapper.auth.user_service import UserService
from snapper.data.models import Operator
from snapper.data.models import User
from snapper.data.models import UserOperatorMembership
from snapper.data.models import Wallet
from snapper.data.repository import DeskMembershipNotFoundError
from snapper.data.repository import DeskMembershipTargetError
from snapper.data.repository import SQLAlchemyRepository
from snapper.data.repository_types import DeskMembershipAttach
from snapper.data.repository_types import DeskMembershipDetach
from snapper.messaging.infrastructure.publisher import SequenceTracker
from snapper.messaging.schemas.admin import MembershipRevokedData


@pytest.fixture
async def desk_repository(tmp_path: Path) -> SQLAlchemyRepository:
    """Build a two-desk, two-wallet world that exposes isolation defects."""
    repository = SQLAlchemyRepository(f"sqlite+aiosqlite:///{tmp_path}/desk-membership.db")
    await repository.create_all()
    now = datetime.now(UTC) - timedelta(minutes=5)
    async with repository.session() as session:
        session.add_all(
            [
                Operator(
                    public_id="00000000-0000-7000-8000-000000000001",
                    label="desk-a",
                    description=None,
                    timestamp=now,
                    session_id="fixture",
                    sequence_id=1,
                ),
                Operator(
                    public_id="00000000-0000-7000-8000-000000000002",
                    label="desk-b",
                    description=None,
                    timestamp=now,
                    session_id="fixture",
                    sequence_id=2,
                ),
                Wallet(
                    public_id="00000000-0000-7000-8000-000000000011",
                    label="wallet-a",
                    description=None,
                    is_paper=True,
                    timestamp=now,
                    session_id="fixture",
                    sequence_id=3,
                ),
                Wallet(
                    public_id="00000000-0000-7000-8000-000000000012",
                    label="wallet-b",
                    description=None,
                    is_paper=True,
                    timestamp=now,
                    session_id="fixture",
                    sequence_id=4,
                ),
                _user("viewer", "00000000-0000-7000-8000-000000000021", "viewer", now, True),
                _user(
                    "second-viewer",
                    "00000000-0000-7000-8000-000000000022",
                    "viewer",
                    now,
                    True,
                ),
                _user(
                    "delegate",
                    "00000000-0000-7000-8000-000000000023",
                    "ai_delegate",
                    now,
                    True,
                ),
                _user(
                    "inactive-viewer",
                    "00000000-0000-7000-8000-000000000024",
                    "viewer",
                    now,
                    False,
                ),
                _user("admin", "00000000-0000-7000-8000-000000000025", "admin", now, True),
                _user(
                    "operator",
                    "00000000-0000-7000-8000-000000000026",
                    "operator",
                    now,
                    True,
                ),
                _user(
                    "operator-zero",
                    "00000000-0000-7000-8000-000000000027",
                    "operator",
                    now,
                    True,
                ),
            ]
        )
        await session.commit()
    return repository


def _user(
    username: str,
    public_id: str,
    role: str,
    timestamp: datetime,
    is_active: bool,
) -> User:
    """Build one fixture user."""
    return User(
        public_id=public_id,
        username=username,
        email=None,
        password_hash="unused",
        role=role,
        is_active=is_active,
        created_at=timestamp,
        timestamp=timestamp,
        session_id="fixture",
        sequence_id=10,
    )


def _attach(username: str, desk: str, sequence_id: int) -> DeskMembershipAttach:
    """Build deterministic attachment provenance."""
    return DeskMembershipAttach(
        username=username,
        operator_public_id=desk,
        timestamp=datetime.now(UTC),
        session_id="attach-test",
        sequence_id=sequence_id,
    )


def _detach(username: str, desk: str, sequence_id: int) -> DeskMembershipDetach:
    """Build deterministic detachment provenance."""
    return DeskMembershipDetach(
        username=username,
        operator_public_id=desk,
        timestamp=datetime.now(UTC),
        session_id="detach-test",
        sequence_id=sequence_id,
    )


@pytest.mark.asyncio
async def test_repository_first_membership_is_primary_and_repeat_is_idempotent(
    desk_repository: SQLAlchemyRepository,
) -> None:
    """The first membership is primary and repeating the pair returns it.

    Given: A VIEWER with no memberships in a two-desk world.
    When: The same desk attachment is requested twice.
    Then: One primary membership exists and both calls return it.
    """
    desk_a = "00000000-0000-7000-8000-000000000001"
    first = await desk_repository.attach_viewer_to_desk(_attach("viewer", desk_a, 1))
    repeated = await desk_repository.attach_viewer_to_desk(_attach("viewer", desk_a, 2))
    assert first["is_primary"] is True
    assert repeated == first
    memberships = await desk_repository.get_user_operator_memberships(
        "00000000-0000-7000-8000-000000000021", datetime.now(UTC)
    )
    assert memberships == [first]


@pytest.mark.asyncio
async def test_repository_second_membership_is_not_primary(
    desk_repository: SQLAlchemyRepository,
) -> None:
    """Only the first of two desk memberships becomes primary.

    Given: A VIEWER with no memberships and two active desks.
    When: The VIEWER is attached to each desk in turn.
    Then: The first row is primary and the second is not.
    """
    desk_a = "00000000-0000-7000-8000-000000000001"
    desk_b = "00000000-0000-7000-8000-000000000002"
    first = await desk_repository.attach_viewer_to_desk(_attach("viewer", desk_a, 1))
    second = await desk_repository.attach_viewer_to_desk(_attach("viewer", desk_b, 2))
    assert first["is_primary"] is True
    assert second["is_primary"] is False


@pytest.mark.asyncio
async def test_repository_rejects_delegate_inactive_user_and_inactive_desk(
    desk_repository: SQLAlchemyRepository,
) -> None:
    """Only active human VIEWER users and active desks are accepted.

    Given: Delegate, desk-manager, inactive VIEWER, and inactive-desk targets.
    When: Each target is passed to the repository attachment primitive.
    Then: Every invalid target is rejected with an explicit domain error.
    """
    desk_a = "00000000-0000-7000-8000-000000000001"
    with pytest.raises(DeskMembershipTargetError, match="AI delegate membership"):
        await desk_repository.attach_viewer_to_desk(_attach("delegate", desk_a, 1))
    for username in ("operator", "admin"):
        with pytest.raises(DeskMembershipTargetError, match="human VIEWER"):
            await desk_repository.attach_viewer_to_desk(_attach(username, desk_a, 1))
    with pytest.raises(DeskMembershipNotFoundError, match="user"):
        await desk_repository.attach_viewer_to_desk(_attach("inactive-viewer", desk_a, 2))
    async with desk_repository.session() as session:
        desk = Operator(
            public_id="00000000-0000-7000-8000-000000000099",
            label="inactive-desk",
            description=None,
            timestamp=datetime.now(UTC) - timedelta(minutes=2),
            known_to=datetime.now(UTC) - timedelta(minutes=1),
            session_id="fixture",
            sequence_id=20,
        )
        session.add(desk)
        await session.commit()
    with pytest.raises(DeskMembershipNotFoundError, match="desk"):
        await desk_repository.attach_viewer_to_desk(
            _attach("viewer", "00000000-0000-7000-8000-000000000099", 3)
        )


@pytest.mark.asyncio
async def test_service_enforces_capability_and_target_desk_membership_with_two_desks(
    desk_repository: SQLAlchemyRepository,
) -> None:
    """Token and live membership scope both fence desk management.

    Given: Two desks and principals with zero scope, desk-A token scope, or a narrowed token.
    When: They attempt attachments inside and outside their effective authority.
    Then: Capability, signed desk ceiling, and live membership are all required.
    """
    desk_a = "00000000-0000-7000-8000-000000000001"
    desk_b = "00000000-0000-7000-8000-000000000002"
    async with desk_repository.session() as session:
        session.add(
            UserOperatorMembership(
                user_public_id="00000000-0000-7000-8000-000000000026",
                operator_public_id=desk_a,
                is_primary=True,
                timestamp=datetime.now(UTC) - timedelta(minutes=1),
                session_id="fixture",
                sequence_id=30,
            )
        )
        await session.commit()
    with patch("snapper.auth.user_service.get_repository", return_value=desk_repository):
        UserService.clear_instance()
        service = UserService()
    no_memberships = AuthPrincipal(
        username="operator-zero",
        role=UserRole.OPERATOR,
        user_public_id="00000000-0000-7000-8000-000000000027",
        operator_public_ids=[],
    )
    desk_a_operator = no_memberships.model_copy(
        update={
            "username": "operator",
            "user_public_id": "00000000-0000-7000-8000-000000000026",
            "operator_public_ids": [desk_a],
        }
    )
    narrowed = desk_a_operator.model_copy(
        update={
            "permissions": [Permission.READ_MARKET_DATA.value],
            "permission_scope_version": 3,
        }
    )
    with pytest.raises(DeskMembershipAuthorizationError, match="token scope"):
        await service.attach_viewer_to_desk(no_memberships, desk_b, "viewer")
    stale_claim = no_memberships.model_copy(update={"operator_public_ids": [desk_a]})
    with pytest.raises(DeskMembershipAuthorizationError, match="Current membership"):
        await service.attach_viewer_to_desk(stale_claim, desk_a, "viewer")
    missing_identity = no_memberships.model_copy(update={"user_public_id": ""})
    with pytest.raises(DeskMembershipAuthorizationError, match="Active caller identity"):
        await service.attach_viewer_to_desk(missing_identity, desk_a, "viewer")
    async with desk_repository.session() as session:
        session.add(
            UserOperatorMembership(
                user_public_id="00000000-0000-7000-8000-000000000026",
                operator_public_id=desk_b,
                is_primary=False,
                timestamp=datetime.now(UTC),
                session_id="new-membership",
                sequence_id=31,
            )
        )
        await session.commit()
    with pytest.raises(DeskMembershipAuthorizationError, match="token scope"):
        await service.attach_viewer_to_desk(desk_a_operator, desk_b, "viewer")
    with pytest.raises(DeskMembershipAuthorizationError, match="permission"):
        await service.attach_viewer_to_desk(narrowed, desk_a, "viewer")
    membership = await service.attach_viewer_to_desk(desk_a_operator, desk_a, "viewer")
    assert membership["operator_public_id"] == desk_a
    UserService.clear_instance()


@pytest.mark.asyncio
async def test_service_rejects_inactive_and_unrecognized_live_manager_roles(
    desk_repository: SQLAlchemyRepository,
) -> None:
    """Canonical identity and role corruption both fail closed.

    Given: Valid OPERATOR token ceilings for an inactive user and a user with an unknown role.
    When: Either principal tries to read a desk directory.
    Then: Live authorization rejects both before consulting desk membership.
    """
    desk_a = "00000000-0000-7000-8000-000000000001"
    corrupt_public_id = "00000000-0000-7000-8000-000000000028"
    async with desk_repository.session() as session:
        session.add(
            _user(
                "corrupt-manager",
                corrupt_public_id,
                "unrecognized",
                datetime.now(UTC) - timedelta(minutes=1),
                True,
            )
        )
        await session.commit()
    with patch("snapper.auth.user_service.get_repository", return_value=desk_repository):
        UserService.clear_instance()
        service = UserService()
    inactive_principal = AuthPrincipal(
        username="inactive-viewer",
        role=UserRole.OPERATOR,
        user_public_id="00000000-0000-7000-8000-000000000024",
        operator_public_ids=[desk_a],
    )
    corrupt_principal = inactive_principal.model_copy(
        update={
            "username": "corrupt-manager",
            "user_public_id": corrupt_public_id,
        }
    )

    with pytest.raises(DeskMembershipAuthorizationError, match="Active caller identity"):
        await service.list_desk_members(inactive_principal, desk_a)
    with pytest.raises(DeskMembershipAuthorizationError, match="Live user role"):
        await service.list_desk_members(corrupt_principal, desk_a)
    UserService.clear_instance()


@pytest.mark.asyncio
async def test_service_global_admin_may_attach_outside_memberships(
    desk_repository: SQLAlchemyRepository,
) -> None:
    """Global ADMIN is the explicit exception to target-desk membership.

    Given: An ADMIN principal with zero memberships in a two-desk world.
    When: The ADMIN attaches a VIEWER to desk B.
    Then: The attachment succeeds under the documented global exception.
    """
    desk_b = "00000000-0000-7000-8000-000000000002"
    with patch("snapper.auth.user_service.get_repository", return_value=desk_repository):
        UserService.clear_instance()
        service = UserService()
    admin = AuthPrincipal(
        username="admin",
        role=UserRole.ADMIN,
        user_public_id="00000000-0000-7000-8000-000000000025",
        operator_public_ids=[],
    )
    membership = await service.attach_viewer_to_desk(admin, desk_b, "second-viewer")
    assert membership["operator_public_id"] == desk_b
    UserService.clear_instance()


@pytest.mark.asyncio
async def test_service_narrowed_admin_keeps_structural_global_bypass(
    desk_repository: SQLAlchemyRepository,
) -> None:
    """ADMIN keeps non-downscopable impersonation despite an explicit scope.

    Given: An ADMIN principal narrowed to one desk and the membership permission.
    When: The principal lists the members of its scoped desk.
    Then: The empty directory is returned under the structural global bypass.
    """
    desk_a = "00000000-0000-7000-8000-000000000001"
    with patch("snapper.auth.user_service.get_repository", return_value=desk_repository):
        UserService.clear_instance()
        service = UserService()
    narrowed_admin = AuthPrincipal(
        username="admin",
        role=UserRole.ADMIN,
        user_public_id="00000000-0000-7000-8000-000000000025",
        operator_public_ids=[desk_a],
        permissions=[Permission.MANAGE_DESK_MEMBERSHIPS.value],
        permission_scope_version=3,
    )
    members = await service.list_desk_members(narrowed_admin, desk_a)
    assert members == []
    UserService.clear_instance()


@pytest.mark.asyncio
async def test_service_rejects_operator_token_after_live_role_demotion(
    desk_repository: SQLAlchemyRepository,
) -> None:
    """An OPERATOR JWT cannot retain desk-management capability after demotion.

    Given: A signed OPERATOR principal with a current membership in desk A.
    When: The canonical database role is demoted to VIEWER before a desk read.
    Then: Live role authorization rejects the still-valid OPERATOR token.
    """
    desk_a = "00000000-0000-7000-8000-000000000001"
    operator_public_id = "00000000-0000-7000-8000-000000000026"
    async with desk_repository.session() as session:
        session.add(
            UserOperatorMembership(
                user_public_id=operator_public_id,
                operator_public_id=desk_a,
                is_primary=True,
                timestamp=datetime.now(UTC) - timedelta(minutes=1),
                session_id="demotion-test",
                sequence_id=1,
            )
        )
        await session.commit()
    with patch("snapper.auth.user_service.get_repository", return_value=desk_repository):
        UserService.clear_instance()
        service = UserService()
    stale_operator = AuthPrincipal(
        username="operator",
        role=UserRole.OPERATOR,
        user_public_id=operator_public_id,
        operator_public_ids=[desk_a],
    )
    assert await service.update_user("operator", role=UserRole.VIEWER) is not None
    with pytest.raises(DeskMembershipAuthorizationError, match="Live user role"):
        await service.list_desk_members(stale_operator, desk_a)
    UserService.clear_instance()


@pytest.mark.asyncio
async def test_service_rechecks_global_bypass_after_admin_demotion(
    desk_repository: SQLAlchemyRepository,
) -> None:
    """An ADMIN JWT loses global bypass when the live role becomes OPERATOR.

    Given: A signed ADMIN principal whose database role becomes OPERATOR.
    When: It tries to use the old global bypass outside live memberships.
    Then: Desk authorization requires current membership and rejects the call.
    """
    desk_b = "00000000-0000-7000-8000-000000000002"
    admin_public_id = "00000000-0000-7000-8000-000000000025"
    with patch("snapper.auth.user_service.get_repository", return_value=desk_repository):
        UserService.clear_instance()
        service = UserService()
    stale_admin = AuthPrincipal(
        username="admin",
        role=UserRole.ADMIN,
        user_public_id=admin_public_id,
        operator_public_ids=[desk_b],
    )
    assert await service.update_user("admin", role=UserRole.OPERATOR) is not None
    with pytest.raises(DeskMembershipAuthorizationError, match="Current membership"):
        await service.list_desk_members(stale_admin, desk_b)
    UserService.clear_instance()


@pytest.mark.asyncio
async def test_service_lists_only_the_requested_desk_projection(
    desk_repository: SQLAlchemyRepository,
) -> None:
    """The service returns a data-minimized profile for exactly one desk.

    Given: A VIEWER attached to two active desks.
    When: An ADMIN lists the members of the second desk.
    Then: Only its minimized desk-scoped VIEWER projection is returned.
    """
    desk_a = "00000000-0000-7000-8000-000000000001"
    desk_b = "00000000-0000-7000-8000-000000000002"
    await desk_repository.attach_viewer_to_desk(_attach("viewer", desk_a, 1))
    await desk_repository.attach_viewer_to_desk(_attach("viewer", desk_b, 2))
    with patch("snapper.auth.user_service.get_repository", return_value=desk_repository):
        UserService.clear_instance()
        service = UserService()
    admin = AuthPrincipal(
        username="admin",
        role=UserRole.ADMIN,
        user_public_id="00000000-0000-7000-8000-000000000025",
    )
    members = await service.list_desk_members(admin, desk_b, as_of=datetime.now(UTC))
    assert len(members) == 1
    assert members[0].username == "viewer"
    assert members[0].email is None
    assert members[0].default_language is None
    assert members[0].operator_public_ids == [desk_b]
    assert members[0].primary_operator_public_id is None
    UserService.clear_instance()


@pytest.mark.asyncio
async def test_service_detach_revokes_immediately_and_publishes_after_commit(
    desk_repository: SQLAlchemyRepository,
) -> None:
    """A successful detach applies both local barriers and emits one admin event.

    Given: A VIEWER membership with token barriers and an event publisher.
    When: An ADMIN detaches the VIEWER and repeats the already-completed request.
    Then: Sessions, cache invalidation, and publication happen exactly once.
    """
    desk_a = "00000000-0000-7000-8000-000000000001"
    await desk_repository.attach_viewer_to_desk(_attach("viewer", desk_a, 1))
    token_manager = MagicMock()
    token_manager.revoke_user_sessions = AsyncMock(return_value=2)
    token_manager.invalidate_user_cache = MagicMock(return_value=1)
    publisher = MagicMock()
    publisher.send = AsyncMock()
    with (
        patch("snapper.auth.user_service.get_repository", return_value=desk_repository),
        patch("snapper.auth.user_service.get_token_manager", return_value=token_manager),
    ):
        UserService.clear_instance()
        service = UserService()
        service.set_msg_publisher(publisher)
        admin = AuthPrincipal(
            username="admin",
            role=UserRole.ADMIN,
            user_public_id="00000000-0000-7000-8000-000000000025",
        )
        result = await service.detach_viewer_from_desk(admin, desk_a, "viewer")
        repeated = await service.detach_viewer_from_desk(admin, desk_a, "viewer")
    assert result is not None
    assert repeated is None
    token_manager.revoke_user_sessions.assert_awaited_once_with(
        "00000000-0000-7000-8000-000000000021",
        desk_repository,
        immediate=True,
    )
    assert token_manager.invalidate_user_cache.call_count == 2
    publisher.send.assert_awaited_once()
    topic, payload = publisher.send.await_args.args
    assert topic == "admin.membership_revoked"
    assert isinstance(payload, MembershipRevokedData)
    assert payload.user_public_id == "00000000-0000-7000-8000-000000000021"
    assert payload.operator_public_id == desk_a
    assert payload.revoked_by_user_public_id == "00000000-0000-7000-8000-000000000025"
    UserService.clear_instance()


@pytest.mark.asyncio
async def test_service_detach_converges_without_publisher_and_after_publish_failure(
    desk_repository: SQLAlchemyRepository,
) -> None:
    """The committed database remains authoritative when event delivery is unavailable.

    Given: Two VIEWER memberships and unavailable or failing event delivery.
    When: An ADMIN detaches each VIEWER from its desk.
    Then: Both committed memberships remain removed despite the delivery conditions.
    """
    desk_a = "00000000-0000-7000-8000-000000000001"
    desk_b = "00000000-0000-7000-8000-000000000002"
    await desk_repository.attach_viewer_to_desk(_attach("viewer", desk_a, 1))
    await desk_repository.attach_viewer_to_desk(_attach("second-viewer", desk_b, 2))
    token_manager = MagicMock()
    token_manager.revoke_user_sessions = AsyncMock(return_value=1)
    token_manager.invalidate_user_cache = MagicMock(return_value=1)
    publisher = MagicMock()
    publisher.send = AsyncMock(side_effect=RuntimeError("broker unavailable"))
    with (
        patch("snapper.auth.user_service.get_repository", return_value=desk_repository),
        patch("snapper.auth.user_service.get_token_manager", return_value=token_manager),
    ):
        UserService.clear_instance()
        service = UserService()
        admin = AuthPrincipal(
            username="admin",
            role=UserRole.ADMIN,
            user_public_id="00000000-0000-7000-8000-000000000025",
        )
        first = await service.detach_viewer_from_desk(admin, desk_a, "viewer")
        service.set_msg_publisher(publisher)
        second = await service.detach_viewer_from_desk(admin, desk_b, "second-viewer")
    assert first is not None
    assert second is not None
    publisher.send.assert_awaited_once()
    assert (
        await desk_repository.get_user_operator_memberships(
            first.user_public_id,
            datetime.now(UTC),
        )
        == []
    )
    assert (
        await desk_repository.get_user_operator_memberships(
            second.user_public_id,
            datetime.now(UTC),
        )
        == []
    )
    UserService.clear_instance()


@pytest.mark.asyncio
async def test_service_detach_commit_failure_revokes_without_publishing(
    desk_repository: SQLAlchemyRepository,
) -> None:
    """A rolled-back membership close keeps credentials revoked and emits no event.

    Given: A VIEWER membership whose detach transaction cannot commit.
    When: An ADMIN attempts to detach the VIEWER.
    Then: Credentials stay revoked, the membership remains, and no event is published.
    """
    desk_a = "00000000-0000-7000-8000-000000000001"
    user_public_id = "00000000-0000-7000-8000-000000000021"
    await desk_repository.attach_viewer_to_desk(_attach("viewer", desk_a, 1))
    token_manager = MagicMock()
    token_manager.revoke_user_sessions = AsyncMock(return_value=2)
    token_manager.invalidate_user_cache = MagicMock(return_value=1)
    publisher = MagicMock()
    publisher.send = AsyncMock()

    async def fail_commit(_session: AsyncSession) -> None:
        raise RuntimeError("forced membership commit failure")

    with (
        patch("snapper.auth.user_service.get_repository", return_value=desk_repository),
        patch("snapper.auth.user_service.get_token_manager", return_value=token_manager),
        patch.object(AsyncSession, "commit", new=fail_commit),
    ):
        UserService.clear_instance()
        service = UserService()
        service.set_msg_publisher(publisher)
        admin = AuthPrincipal(
            username="admin",
            role=UserRole.ADMIN,
            user_public_id="00000000-0000-7000-8000-000000000025",
        )
        with pytest.raises(RuntimeError, match="forced membership commit failure"):
            await service.detach_viewer_from_desk(admin, desk_a, "viewer")
    token_manager.revoke_user_sessions.assert_awaited_once_with(
        user_public_id,
        desk_repository,
        immediate=True,
    )
    token_manager.invalidate_user_cache.assert_called_once_with(user_public_id)
    publisher.send.assert_not_awaited()
    memberships = await desk_repository.get_user_operator_memberships(
        user_public_id,
        datetime.now(UTC),
    )
    assert [(row["operator_public_id"], row["is_primary"]) for row in memberships] == [
        (desk_a, True)
    ]
    UserService.clear_instance()


@pytest.mark.asyncio
async def test_repository_preserves_one_primary_across_attachment_decisions(
    desk_repository: SQLAlchemyRepository,
) -> None:
    """Separate attachment decisions preserve one primary membership.

    Given: A VIEWER with no memberships and two active desks.
    When: Both serialized repository decisions attach the VIEWER.
    Then: Exactly one of the two durable rows is primary.
    """
    desk_a = "00000000-0000-7000-8000-000000000001"
    desk_b = "00000000-0000-7000-8000-000000000002"
    first = await desk_repository.attach_viewer_to_desk(_attach("second-viewer", desk_a, 1))
    second = await desk_repository.attach_viewer_to_desk(_attach("second-viewer", desk_b, 2))
    assert sum(row["is_primary"] for row in [first, second]) == 1
    async with desk_repository.session() as session:
        rows = (
            (
                await session.execute(
                    UserOperatorMembership.__table__.select().where(
                        UserOperatorMembership.user_public_id
                        == "00000000-0000-7000-8000-000000000022"
                    )
                )
            )
            .mappings()
            .all()
        )
    assert len(rows) == 2


@pytest.mark.asyncio
async def test_repository_lists_human_members_and_excludes_delegate(
    desk_repository: SQLAlchemyRepository,
) -> None:
    """The desk directory exposes human members without delegate lifecycle rows.

    Given: A desk containing ADMIN, OPERATOR, VIEWER, and DELEGATE memberships.
    When: The repository lists the human desk directory.
    Then: Only the three human roles are returned as primary members of that desk.
    """
    desk_a = "00000000-0000-7000-8000-000000000001"
    now = datetime.now(UTC)
    await desk_repository.attach_viewer_to_desk(_attach("viewer", desk_a, 1))
    async with desk_repository.session() as session:
        session.add_all(
            [
                UserOperatorMembership(
                    user_public_id="00000000-0000-7000-8000-000000000025",
                    operator_public_id=desk_a,
                    is_primary=True,
                    timestamp=now,
                    session_id="directory",
                    sequence_id=1,
                ),
                UserOperatorMembership(
                    user_public_id="00000000-0000-7000-8000-000000000026",
                    operator_public_id=desk_a,
                    is_primary=True,
                    timestamp=now,
                    session_id="directory",
                    sequence_id=2,
                ),
                UserOperatorMembership(
                    user_public_id="00000000-0000-7000-8000-000000000023",
                    operator_public_id=desk_a,
                    is_primary=True,
                    timestamp=now,
                    session_id="directory",
                    sequence_id=3,
                ),
            ]
        )
        await session.commit()
    members = await desk_repository.list_human_desk_members(desk_a, datetime.now(UTC))
    assert [member["username"] for member in members] == ["admin", "operator", "viewer"]
    assert all(member["operator_public_id"] == desk_a for member in members)
    assert all(member["is_primary"] is True for member in members)


@pytest.mark.asyncio
async def test_repository_detach_primary_revokes_before_close_and_promotes_next(
    desk_repository: SQLAlchemyRepository,
) -> None:
    """Primary detach revokes first and deterministically promotes the oldest survivor.

    Given: A VIEWER whose primary desk is followed by one surviving membership.
    When: The repository detaches the primary membership.
    Then: Authority is revoked before close and the surviving desk becomes primary.
    """
    desk_a = "00000000-0000-7000-8000-000000000001"
    desk_b = "00000000-0000-7000-8000-000000000002"
    user_public_id = "00000000-0000-7000-8000-000000000021"
    await desk_repository.attach_viewer_to_desk(_attach("viewer", desk_a, 1))
    await desk_repository.attach_viewer_to_desk(_attach("viewer", desk_b, 2))
    callback_users: list[str] = []

    async def revoke_authority(target_user_public_id: str) -> None:
        callback_users.append(target_user_public_id)
        before_close = await desk_repository.get_user_operator_memberships(
            target_user_public_id,
            datetime.now(UTC),
        )
        assert {row["operator_public_id"] for row in before_close} == {desk_a, desk_b}

    result = await desk_repository.detach_viewer_from_desk(
        _detach("viewer", desk_a, 3),
        revoke_authority,
    )
    assert result is not None
    assert result.promoted_operator_public_id == desk_b
    assert callback_users == [user_public_id]
    memberships = await desk_repository.get_user_operator_memberships(
        user_public_id,
        datetime.now(UTC),
    )
    assert [(row["operator_public_id"], row["is_primary"]) for row in memberships] == [
        (desk_b, True)
    ]


@pytest.mark.asyncio
async def test_repository_detach_is_idempotent_and_does_not_revoke_twice(
    desk_repository: SQLAlchemyRepository,
) -> None:
    """Repeating an already-completed detach is a no-op without another revocation.

    Given: A VIEWER with one desk membership and a revocation callback.
    When: The same membership is detached twice.
    Then: The first detach succeeds and the repeated detach does not revoke again.
    """
    desk_a = "00000000-0000-7000-8000-000000000001"
    await desk_repository.attach_viewer_to_desk(_attach("viewer", desk_a, 1))
    revoked: list[str] = []

    async def revoke_authority(user_public_id: str) -> None:
        revoked.append(user_public_id)

    first = await desk_repository.detach_viewer_from_desk(
        _detach("viewer", desk_a, 2), revoke_authority
    )
    repeated = await desk_repository.detach_viewer_from_desk(
        _detach("viewer", desk_a, 3), revoke_authority
    )
    assert first is not None
    assert repeated is None
    assert revoked == ["00000000-0000-7000-8000-000000000021"]


@pytest.mark.asyncio
async def test_repository_revocation_failure_preserves_membership(
    desk_repository: SQLAlchemyRepository,
) -> None:
    """A failed credential barrier rolls the detachment decision back.

    Given: A VIEWER membership whose authority revocation callback fails.
    When: The repository attempts to detach the membership.
    Then: The failure propagates and the original membership remains active.
    """
    desk_a = "00000000-0000-7000-8000-000000000001"
    user_public_id = "00000000-0000-7000-8000-000000000021"
    await desk_repository.attach_viewer_to_desk(_attach("viewer", desk_a, 1))

    async def failed_revoke(_user_public_id: str) -> None:
        raise RuntimeError("token inventory unavailable")

    with pytest.raises(RuntimeError, match="token inventory unavailable"):
        await desk_repository.detach_viewer_from_desk(_detach("viewer", desk_a, 2), failed_revoke)
    memberships = await desk_repository.get_user_operator_memberships(
        user_public_id,
        datetime.now(UTC),
    )
    assert [row["operator_public_id"] for row in memberships] == [desk_a]


def _route_request() -> MagicMock:
    """Build a request carrying the REST provenance tracker."""
    request = MagicMock()
    request.app.state.rest_tracker = SequenceTracker()
    return request


@pytest.mark.parametrize("method", ["POST", "DELETE"])
def test_membership_mutation_routes_accept_username_path_separators(method: str) -> None:
    """Slash-containing exact usernames reach both mutation endpoints.

    Given: The membership router and a username containing a path separator.
    When: Starlette matches either membership mutation method.
    Then: The full route matches and preserves the complete username parameter.
    """
    scope = {
        "type": "http",
        "method": method,
        "path": "/auth/desks/desk-a/members/viewer/name",
        "root_path": "",
    }
    matches = [route.matches(scope) for route in router.routes]
    matching_scopes = [child for match, child in matches if match is Match.FULL]
    assert any(
        child["path_params"]
        == {
            "operator_public_id": "desk-a",
            "username": "viewer/name",
        }
        for child in matching_scopes
    )


@pytest.mark.asyncio
async def test_route_attaches_by_username_and_returns_confirmation() -> None:
    """The CSRF-gated route delegates its desk and username unchanged.

    Given: An authenticated caller admitted by permission and CSRF dependencies.
    When: The attach-by-username route is invoked.
    Then: The service receives exact identities and the route confirms attachment.
    """
    principal = AuthPrincipal(username="operator", role=UserRole.OPERATOR)
    service = MagicMock()
    service.attach_viewer_to_desk = AsyncMock()
    with patch("snapper.auth.routes.get_user_service", return_value=service):
        response = await attach_viewer_to_desk(
            request=_route_request(),
            operator_public_id="desk-a",
            username="viewer",
            current_user=principal,
            _csrf=None,
        )
    assert response.payload == "User 'viewer' is attached to the desk"
    service.attach_viewer_to_desk.assert_awaited_once_with(
        principal=principal,
        operator_public_id="desk-a",
        username="viewer",
    )


@pytest.mark.asyncio
async def test_route_lists_desk_members_without_manage_users() -> None:
    """The desk-scoped directory uses its own permission and response envelope.

    Given: An admitted OPERATOR and one projected member from the service.
    When: The list-members route is invoked for the desk.
    Then: The route returns a counted envelope and delegates the exact request.
    """
    principal = AuthPrincipal(username="operator", role=UserRole.OPERATOR)
    member = UserProfile(
        public_id="user-viewer",
        timestamp=datetime.now(UTC),
        session_id="member",
        sequence_id=1,
        username="viewer",
        role=UserRole.VIEWER,
        created_at=datetime.now(UTC),
        operator_public_ids=["desk-a"],
        primary_operator_public_id="desk-a",
    )
    service = MagicMock()
    service.list_desk_members = AsyncMock(return_value=[member])
    with patch("snapper.auth.routes.get_user_service", return_value=service):
        response = await list_desk_members(
            request=_route_request(),
            operator_public_id="desk-a",
            current_user=principal,
            as_of=None,
        )
    assert response.payload == [member]
    assert response.count == 1
    service.list_desk_members.assert_awaited_once_with(
        principal=principal,
        operator_public_id="desk-a",
        as_of=None,
    )


@pytest.mark.asyncio
async def test_route_detaches_by_username_and_returns_confirmation() -> None:
    """The CSRF-gated DELETE delegates exact identities to the service.

    Given: An authenticated caller admitted by permission and CSRF dependencies.
    When: The detach-by-username route is invoked.
    Then: The service receives exact identities and the route confirms detachment.
    """
    principal = AuthPrincipal(username="operator", role=UserRole.OPERATOR)
    service = MagicMock()
    service.detach_viewer_from_desk = AsyncMock()
    with patch("snapper.auth.routes.get_user_service", return_value=service):
        response = await detach_viewer_from_desk(
            request=_route_request(),
            operator_public_id="desk-a",
            username="viewer",
            current_user=principal,
            _csrf=None,
        )
    assert response.payload == "User 'viewer' is detached from the desk"
    service.detach_viewer_from_desk.assert_awaited_once_with(
        principal=principal,
        operator_public_id="desk-a",
        username="viewer",
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("error", "expected_status"),
    [
        (DeskMembershipAuthorizationError("outside desk"), 403),
        (DeskMembershipNotFoundError("missing"), 404),
        (DeskMembershipTargetError("delegate"), 422),
    ],
)
async def test_route_maps_membership_errors(
    error: Exception,
    expected_status: int,
) -> None:
    """Service and repository failures map to stable HTTP statuses.

    Given: Each attachment domain error from the service boundary.
    When: The route handles the failed attachment.
    Then: The response status preserves authorization, absence, or target semantics.
    """
    principal = AuthPrincipal(username="operator", role=UserRole.OPERATOR)
    service = MagicMock()
    service.attach_viewer_to_desk = AsyncMock(side_effect=error)
    with (
        patch("snapper.auth.routes.get_user_service", return_value=service),
        pytest.raises(HTTPException) as exc_info,
    ):
        await attach_viewer_to_desk(
            request=_route_request(),
            operator_public_id="desk-b",
            username="viewer",
            current_user=principal,
            _csrf=None,
        )
    assert exc_info.value.status_code == expected_status
    assert exc_info.value.detail == str(error)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("error", "expected_status"),
    [
        (DeskMembershipAuthorizationError("outside desk"), 403),
        (DeskMembershipNotFoundError("missing desk"), 404),
    ],
)
async def test_list_route_maps_membership_errors(
    error: Exception,
    expected_status: int,
) -> None:
    """The desk directory preserves authorization and absence statuses.

    Given: Each list-members authorization or missing-desk service error.
    When: The route handles the failed directory request.
    Then: The response preserves the corresponding forbidden or not-found status.
    """
    principal = AuthPrincipal(username="operator", role=UserRole.OPERATOR)
    service = MagicMock()
    service.list_desk_members = AsyncMock(side_effect=error)
    with (
        patch("snapper.auth.routes.get_user_service", return_value=service),
        pytest.raises(HTTPException) as exc_info,
    ):
        await list_desk_members(
            request=_route_request(),
            operator_public_id="desk-b",
            current_user=principal,
            as_of=None,
        )
    assert exc_info.value.status_code == expected_status
    assert exc_info.value.detail == str(error)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("error", "expected_status"),
    [
        (DeskMembershipAuthorizationError("outside desk"), 403),
        (DeskMembershipNotFoundError("missing"), 404),
        (DeskMembershipTargetError("delegate"), 422),
    ],
)
async def test_detach_route_maps_membership_errors(
    error: Exception,
    expected_status: int,
) -> None:
    """The detach route preserves authorization, absence, and target statuses.

    Given: Each detachment domain error from the service boundary.
    When: The route handles the failed detachment.
    Then: The response preserves authorization, absence, or target semantics.
    """
    principal = AuthPrincipal(username="operator", role=UserRole.OPERATOR)
    service = MagicMock()
    service.detach_viewer_from_desk = AsyncMock(side_effect=error)
    with (
        patch("snapper.auth.routes.get_user_service", return_value=service),
        pytest.raises(HTTPException) as exc_info,
    ):
        await detach_viewer_from_desk(
            request=_route_request(),
            operator_public_id="desk-b",
            username="viewer",
            current_user=principal,
            _csrf=None,
        )
    assert exc_info.value.status_code == expected_status
    assert exc_info.value.detail == str(error)
