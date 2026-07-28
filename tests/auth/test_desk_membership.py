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

from snapper.auth.domain.permissions import Permission
from snapper.auth.domain.roles import UserRole
from snapper.auth.routes import attach_viewer_to_desk
from snapper.auth.schemas.principal import AuthPrincipal
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
from snapper.messaging.infrastructure.publisher import SequenceTracker


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

    Given: Delegate, inactive VIEWER, and inactive-desk targets.
    When: Each target is passed to the repository attachment primitive.
    Then: Every invalid target is rejected with an explicit domain error.
    """
    desk_a = "00000000-0000-7000-8000-000000000001"
    with pytest.raises(DeskMembershipTargetError, match="AI delegate membership"):
        await desk_repository.attach_viewer_to_desk(_attach("delegate", desk_a, 1))
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
    """Zero-membership and desk-A principals cannot attach into desk B.

    Given: Two desks and principals with zero scope, desk-A scope, or a narrowed token.
    When: They attempt attachments inside and outside their effective authority.
    Then: Both capability and target membership are required for the successful call.
    """
    desk_a = "00000000-0000-7000-8000-000000000001"
    desk_b = "00000000-0000-7000-8000-000000000002"
    with patch("snapper.auth.user_service.get_repository", return_value=desk_repository):
        UserService.clear_instance()
        service = UserService()
    no_memberships = AuthPrincipal(
        username="operator-zero",
        role=UserRole.OPERATOR,
        user_public_id="00000000-0000-7000-8000-000000000031",
        operator_public_ids=[],
    )
    desk_a_operator = no_memberships.model_copy(update={"operator_public_ids": [desk_a]})
    narrowed = desk_a_operator.model_copy(
        update={
            "permissions": [Permission.READ_MARKET_DATA.value],
            "permission_scope_version": 3,
        }
    )
    with pytest.raises(DeskMembershipAuthorizationError, match="Current membership"):
        await service.attach_viewer_to_desk(no_memberships, desk_b, "viewer")
    with pytest.raises(DeskMembershipAuthorizationError, match="Current membership"):
        await service.attach_viewer_to_desk(desk_a_operator, desk_b, "viewer")
    with pytest.raises(DeskMembershipAuthorizationError, match="permission"):
        await service.attach_viewer_to_desk(narrowed, desk_a, "viewer")
    membership = await service.attach_viewer_to_desk(desk_a_operator, desk_a, "viewer")
    assert membership["operator_public_id"] == desk_a
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
        user_public_id="00000000-0000-7000-8000-000000000032",
        operator_public_ids=[],
    )
    membership = await service.attach_viewer_to_desk(admin, desk_b, "second-viewer")
    assert membership["operator_public_id"] == desk_b
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


def _route_request() -> MagicMock:
    """Build a request carrying the REST provenance tracker."""
    request = MagicMock()
    request.app.state.rest_tracker = SequenceTracker()
    return request


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
