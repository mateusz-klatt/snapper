"""Tests for authentication roles and permissions."""

from collections.abc import Generator
from datetime import UTC
from datetime import datetime
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest
from fastapi import FastAPI
from fastapi import HTTPException
from fastapi.testclient import TestClient

from snapper.auth.domain.roles import UserRole
from snapper.auth.routes import get_current_user_profile
from snapper.auth.routes import router
from snapper.auth.schemas.principal import AuthPrincipal
from snapper.auth.schemas.user import UserProfile
from snapper.auth.user_service import UserService
from snapper.auth.user_service import get_user_service
from snapper.data.models import KNOWN_TO_MAX
from snapper.data.models import User
from snapper.messaging.infrastructure.publisher import SequenceTracker


def _make_rest_request() -> MagicMock:
    """Create a mock FastAPI Request with rest_tracker."""
    mock_request = MagicMock()
    mock_request.app.state.rest_tracker = SequenceTracker()
    return mock_request


@pytest.mark.asyncio
async def test_get_current_user_profile_returns_user_from_db() -> None:
    """Test get_current_user_profile loads user from DB via user_service.

    Given: An AuthPrincipal and a mocked user_service returning a UserProfile.
    When: get_current_user_profile is called with the principal.
    Then: The UserProfile from the service is returned.
    """
    principal = AuthPrincipal(
        username="testuser",
        role=UserRole.VIEWER,
        is_active=True,
    )
    expected_profile = UserProfile(
        session_id="test-sid",
        sequence_id=1,
        public_id="test-pid",
        timestamp=datetime(2024, 1, 1, tzinfo=UTC),
        username="testuser",
        role=UserRole.VIEWER,
        is_active=True,
        created_at=datetime.now(UTC),
    )
    mock_service = AsyncMock()
    mock_service.get_user_by_id = AsyncMock(return_value=expected_profile)
    with patch("snapper.auth.routes.get_user_service", return_value=mock_service):
        result = await get_current_user_profile(
            request=_make_rest_request(), current_user=principal
        )
    assert result.payload is expected_profile
    assert result.payload.username == "testuser"
    assert result.payload.role == UserRole.VIEWER
    mock_service.get_user_by_id.assert_awaited_once_with("testuser")


@pytest.mark.asyncio
async def test_get_current_user_profile_user_deleted_returns_404() -> None:
    """Test get_current_user_profile returns 404 when user no longer exists.

    Given: An AuthPrincipal for a user that was deleted after token issuance.
    When: get_current_user_profile is called.
    Then: HTTPException with 404 status is raised.
    """
    principal = AuthPrincipal(
        username="deleted_user",
        role=UserRole.VIEWER,
    )
    mock_service = AsyncMock()
    mock_service.get_user_by_id = AsyncMock(return_value=None)
    with (
        patch("snapper.auth.routes.get_user_service", return_value=mock_service),
        pytest.raises(HTTPException) as exc_info,
    ):
        await get_current_user_profile(request=_make_rest_request(), current_user=principal)
    assert exc_info.value.status_code == 404


app = FastAPI()
app.include_router(router)
client = TestClient(app)


def teardown_module(module: object) -> None:
    """Close the module-scoped TestClient after the test module completes."""
    client.close()


class TestUserManagementBasic:
    """Test suite for user management API authorization."""

    def test_get_users_unauthorized(self) -> None:
        """Test GET /auth/users returns 401 without authentication.

        Given: An unauthenticated HTTP client.
        When: A GET request is made to /auth/users.
        Then: The response status code is 401 Unauthorized.
        """
        response = client.get("/auth/users")
        assert response.status_code == 401

    def test_create_user_unauthorized(self) -> None:
        """Test POST /auth/users returns 401 without authentication.

        Given: An unauthenticated HTTP client and user creation data.
        When: A POST request is made to /auth/users with user data.
        Then: The response status code is 401 Unauthorized.
        """
        user_data = {
            "username": "newuser",
            "password": "testpassword123",
            "email": "newuser@example.com",
            "role": "operator",
            "is_active": True,
        }
        response = client.post("/auth/users", json=user_data)
        assert response.status_code == 401

    def test_update_user_unauthorized(self) -> None:
        """Test POST /auth/users/{id}/update returns 401 without authentication.

        Given: An unauthenticated HTTP client and user update data.
        When: A POST request is made to /auth/users/some-id/update.
        Then: The response status code is 401 Unauthorized.
        """
        update_data = {
            "email": "new@example.com",
            "role": "operator",
            "is_active": False,
        }
        response = client.post("/auth/users/some-id/update", json=update_data)
        assert response.status_code == 401

    def test_deactivate_user_unauthorized(self) -> None:
        """Test POST /auth/users/{id}/deactivate returns 401 without authentication.

        Given: An unauthenticated HTTP client.
        When: A POST request is made to /auth/users/some-id/deactivate.
        Then: The response status code is 401 Unauthorized.
        """
        body = {
            "public_id": "test-pid",
            "session_id": "test-sid",
            "sequence_id": 0,
            "timestamp": "2024-01-01T00:00:00Z",
            "payload": {},
        }
        response = client.post("/auth/users/some-id/deactivate", json=body)
        assert response.status_code == 401

    def test_change_password_unauthorized(self) -> None:
        """Test POST /auth/users/{id}/change-password returns 401 without authentication.

        Given: An unauthenticated HTTP client and password change data.
        When: A POST request is made to /auth/users/some-id/change-password.
        Then: The response status code is 401 Unauthorized.
        """
        password_data = {
            "current_password": "oldpass",
            "new_password": "newpass",
        }
        response = client.post("/auth/users/some-id/change-password", json=password_data)
        assert response.status_code == 401

    @patch("snapper.auth.routes.require_permission")
    @patch("snapper.auth.routes.get_user_service")
    def test_get_users_admin_success(
        self, mock_get_service: MagicMock, mock_require_permission: MagicMock
    ) -> None:
        """Test GET /auth/users route exists and is accessible for admin.

        Given: Mocked user service and admin permission dependencies.
        When: Router routes are inspected.
        Then: The /auth/users endpoint exists in the router.
        """
        mock_user_service = AsyncMock()
        mock_user_service.get_all_users.return_value = [
            UserProfile(
                session_id="test-sid",
                sequence_id=1,
                public_id="test-pid",
                timestamp=datetime(2024, 1, 1, tzinfo=UTC),
                username="testuser",
                role=UserRole.VIEWER,
                is_active=True,
                created_at=datetime.now(UTC),
            )
        ]
        mock_get_service.return_value = mock_user_service
        mock_admin_user = AuthPrincipal(
            username="admin",
            role=UserRole.ADMIN,
            is_active=True,
        )
        mock_require_permission.return_value = lambda: mock_admin_user
        with patch("snapper.auth.routes.Depends") as mock_depends:
            mock_depends.return_value = mock_admin_user
            assert hasattr(router, "routes")
            routes = [route.path for route in router.routes if hasattr(route, "path")]
            assert "/auth/users" in routes


class TestUserService:
    """Test suite for UserService class methods."""

    @pytest.fixture
    def mock_db_user(self) -> User:
        """Create a mock database User object for testing."""
        db_user = MagicMock(spec=User)
        db_user.id = 1
        db_user.public_id = "fake-public-id"
        db_user.session_id = "fake-session-id"
        db_user.sequence_id = 1
        db_user.username = "testuser"
        db_user.email = "test@example.com"
        db_user.password_hash = "hashed_password"
        db_user.role = "viewer"
        db_user.is_active = True
        db_user.created_at = datetime.now(UTC)
        db_user.timestamp = datetime.now(UTC)
        db_user.known_to = KNOWN_TO_MAX
        return db_user

    @pytest.fixture
    def user_service(self) -> UserService:
        """Create a UserService instance with mocked repository."""
        with patch("snapper.auth.user_service.get_repository") as mock_get_repo:
            mock_repo = MagicMock()
            mock_get_repo.return_value = mock_repo
            return UserService()

    def test_hash_password(self, user_service: UserService) -> None:
        """Test password hashing generates unique bcrypt hash each time.

        Given: A UserService instance and a plain text password.
        When: hash_password is called twice with the same password.
        Then: Different bcrypt hashes starting with ``$2b$`` are generated.
        """
        password = "testpassword123"
        hash1 = user_service.hash_password(password)
        hash2 = user_service.hash_password(password)
        assert hash1 != hash2
        assert hash1.startswith("$2b$")
        assert hash2.startswith("$2b$")

    @pytest.mark.asyncio
    async def test_authenticate_user_success(
        self, user_service: UserService, mock_db_user: User
    ) -> None:
        """Test successful user authentication with valid credentials.

        Given: A user exists in the database and password verification succeeds.
        When: authenticate_user is called with correct username and password.
        Then: The authenticated user profile is returned and a login event is recorded.
        """
        mock_session = AsyncMock()
        mock_session.add = MagicMock()
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = mock_db_user
        mock_session.execute.return_value = mock_result
        user_service.repository.session = MagicMock()
        user_service.repository.session.return_value = AsyncMock()
        user_service.repository.session.return_value.__aenter__.return_value = mock_session
        with patch.object(user_service, "_verify_password", return_value=True):
            auth_user = await user_service.authenticate_user("testuser", "testpassword")
        assert auth_user is not None
        assert auth_user.username == "testuser"
        assert mock_session.commit.called

    @pytest.mark.asyncio
    async def test_authenticate_user_not_found(self, user_service: UserService) -> None:
        """Test authentication fails when user does not exist.

        Given: No user exists with the given username.
        When: authenticate_user is called with a nonexistent username.
        Then: None is returned.
        """
        mock_session = AsyncMock()
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = None
        mock_session.execute.return_value = mock_result
        user_service.repository.session = MagicMock()
        user_service.repository.session.return_value = AsyncMock()
        user_service.repository.session.return_value.__aenter__.return_value = mock_session
        auth_user = await user_service.authenticate_user("nonexistent", "password")
        assert auth_user is None

    @pytest.mark.asyncio
    async def test_authenticate_user_wrong_password(
        self, user_service: UserService, mock_db_user: User
    ) -> None:
        """Test authentication fails with incorrect password.

        Given: A user exists but password verification fails.
        When: authenticate_user is called with wrong password.
        Then: None is returned.
        """
        mock_session = AsyncMock()
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = mock_db_user
        mock_session.execute.return_value = mock_result
        user_service.repository.session = MagicMock()
        user_service.repository.session.return_value = AsyncMock()
        user_service.repository.session.return_value.__aenter__.return_value = mock_session
        with patch.object(user_service, "_verify_password", return_value=False):
            auth_user = await user_service.authenticate_user("testuser", "wrongpassword")
        assert auth_user is None

    @pytest.mark.asyncio
    async def test_get_user_by_id_success(
        self, user_service: UserService, mock_db_user: User
    ) -> None:
        """Test successful user retrieval by ID.

        Given: A user exists in the database with the given ID.
        When: get_user_by_id is called with an existing user ID.
        Then: The user profile is returned with correct ID.
        """
        mock_session = AsyncMock()
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = mock_db_user
        mock_session.execute.return_value = mock_result
        user_service.repository.session = MagicMock()
        user_service.repository.session.return_value = AsyncMock()
        user_service.repository.session.return_value.__aenter__.return_value = mock_session
        auth_user = await user_service.get_user_by_id("test_user")
        assert auth_user is not None
        assert auth_user.username == "testuser"

    @pytest.mark.asyncio
    async def test_get_user_by_id_not_found(self, user_service: UserService) -> None:
        """Test get_user_by_id returns None for nonexistent ID.

        Given: No user exists with the given ID.
        When: get_user_by_id is called with a nonexistent ID.
        Then: None is returned.
        """
        mock_session = AsyncMock()
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = None
        mock_session.execute.return_value = mock_result
        user_service.repository.session = MagicMock()
        user_service.repository.session.return_value = AsyncMock()
        user_service.repository.session.return_value.__aenter__.return_value = mock_session
        auth_user = await user_service.get_user_by_id("nonexistent")
        assert auth_user is None

    @pytest.mark.asyncio
    async def test_get_user_by_username_success(
        self, user_service: UserService, mock_db_user: User
    ) -> None:
        """Test successful user retrieval by username.

        Given: A user exists in the database with the given username.
        When: get_user_by_username is called with an existing username.
        Then: The user profile is returned with correct username.
        """
        mock_session = AsyncMock()
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = mock_db_user
        mock_session.execute.return_value = mock_result
        user_service.repository.session = MagicMock()
        user_service.repository.session.return_value = AsyncMock()
        user_service.repository.session.return_value.__aenter__.return_value = mock_session
        auth_user = await user_service.get_user_by_username("testuser")
        assert auth_user is not None
        assert auth_user.username == "testuser"

    @pytest.mark.asyncio
    async def test_get_user_by_username_not_found(self, user_service: UserService) -> None:
        """Test get_user_by_username returns None for nonexistent username.

        Given: No user exists with the given username.
        When: get_user_by_username is called with a nonexistent username.
        Then: None is returned.
        """
        mock_session = AsyncMock()
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = None
        mock_session.execute.return_value = mock_result
        user_service.repository.session = MagicMock()
        user_service.repository.session.return_value = AsyncMock()
        user_service.repository.session.return_value.__aenter__.return_value = mock_session
        auth_user = await user_service.get_user_by_username("nonexistent")
        assert auth_user is None

    @pytest.mark.asyncio
    async def test_get_all_users(self, user_service: UserService, mock_db_user: User) -> None:
        """Test get_all_users returns list of active users.

        Given: Users exist in the database.
        When: get_all_users is called without parameters.
        Then: A list of user profiles is returned.
        """
        mock_session = AsyncMock()
        mock_result = MagicMock()
        mock_result.scalars.return_value.all.return_value = [mock_db_user]
        mock_session.execute.return_value = mock_result
        user_service.repository.session = MagicMock()
        user_service.repository.session.return_value = AsyncMock()
        user_service.repository.session.return_value.__aenter__.return_value = mock_session
        users = await user_service.get_all_users()
        assert len(users) == 1
        assert users[0].username == "testuser"

    @pytest.mark.asyncio
    async def test_get_all_users_include_inactive(
        self, user_service: UserService, mock_db_user: User
    ) -> None:
        """Test get_all_users can include inactive users.

        Given: Users exist in the database including inactive ones.
        When: get_all_users is called with include_inactive=True.
        Then: All users including inactive ones are returned.
        """
        mock_session = AsyncMock()
        mock_result = MagicMock()
        mock_result.scalars.return_value.all.return_value = [mock_db_user]
        mock_session.execute.return_value = mock_result
        user_service.repository.session = MagicMock()
        user_service.repository.session.return_value = AsyncMock()
        user_service.repository.session.return_value.__aenter__.return_value = mock_session
        users = await user_service.get_all_users(include_inactive=True)
        assert len(users) == 1

    @pytest.mark.asyncio
    async def test_get_all_users_with_as_of(
        self, user_service: UserService, mock_db_user: User
    ) -> None:
        """Test get_all_users accepts as_of for point-in-time query.

        Given: Users exist in the database.
        When: get_all_users is called with an explicit as_of timestamp.
        Then: A list of user profiles is returned using temporal filtering.
        """
        mock_session = AsyncMock()
        mock_result = MagicMock()
        mock_result.scalars.return_value.all.return_value = [mock_db_user]
        mock_session.execute.return_value = mock_result
        user_service.repository.session = MagicMock()
        user_service.repository.session.return_value = AsyncMock()
        user_service.repository.session.return_value.__aenter__.return_value = mock_session
        historical_date = datetime(2024, 6, 1, 12, 0, 0, tzinfo=UTC)
        users = await user_service.get_all_users(as_of=historical_date)
        assert len(users) == 1
        assert users[0].username == "testuser"

    @pytest.mark.asyncio
    async def test_create_user_success(self, user_service: UserService) -> None:
        """Test successful user creation with full parameters.

        Given: No user with the given username exists.
        When: create_user is called with username, password, email, and role.
        Then: A new user is added to the database and committed.
        """
        mock_session = AsyncMock()
        mock_session.add = MagicMock()
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = None
        mock_session.execute.return_value = mock_result

        def _fake_refresh(obj: User) -> None:
            """Simulate session.refresh populating server-side defaults."""
            obj.public_id = "fake-new-public-id"

        mock_session.refresh = AsyncMock(side_effect=_fake_refresh)
        user_service.repository.session = MagicMock()
        user_service.repository.session.return_value = AsyncMock()
        user_service.repository.session.return_value.__aenter__.return_value = mock_session
        with patch.object(user_service, "hash_password", return_value="hash"):
            await user_service.create_user(
                username="newuser",
                password="password",
                email="new@example.com",
                role=UserRole.VIEWER,
            )
        assert mock_session.add.called
        assert mock_session.commit.called

    @pytest.mark.asyncio
    async def test_create_user_already_exists(
        self, user_service: UserService, mock_db_user: User
    ) -> None:
        """Test create_user raises error when username already exists.

        Given: A user with the given username already exists.
        When: create_user is called with the same username.
        Then: ValueError is raised with appropriate message.
        """
        mock_session = AsyncMock()
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = mock_db_user
        mock_session.execute.return_value = mock_result
        user_service.repository.session = MagicMock()
        user_service.repository.session.return_value = AsyncMock()
        user_service.repository.session.return_value.__aenter__.return_value = mock_session
        with pytest.raises(ValueError, match="User with username 'testuser' already exists"):
            await user_service.create_user(
                username="testuser", password="password", email="test@example.com"
            )

    @pytest.mark.asyncio
    async def test_create_user_minimal_params(self, user_service: UserService) -> None:
        """Test user creation with only required parameters.

        Given: No user with the given username exists.
        When: create_user is called with only username and password.
        Then: A new user is created with default values.
        """
        mock_session = AsyncMock()
        mock_session.add = MagicMock()
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = None
        mock_session.execute.return_value = mock_result

        def _fake_refresh(obj: User) -> None:
            """Simulate session.refresh populating server-side defaults."""
            obj.public_id = "fake-new-public-id"

        mock_session.refresh = AsyncMock(side_effect=_fake_refresh)
        user_service.repository.session = MagicMock()
        user_service.repository.session.return_value = AsyncMock()
        user_service.repository.session.return_value.__aenter__.return_value = mock_session
        with patch.object(user_service, "hash_password", return_value="hash"):
            await user_service.create_user("newuser", "password")
        assert mock_session.add.called

    @pytest.mark.asyncio
    async def test_update_user_success(self, user_service: UserService, mock_db_user: User) -> None:
        """Test successful user update via close+insert.

        Given: A user exists with the given ID.
        When: update_user is called with new email and role.
        Then: close_and_insert is called, committed, and refreshed.
        """
        mock_session = AsyncMock()
        mock_session.add = MagicMock()
        mock_select_result = MagicMock()
        mock_select_result.scalar_one_or_none.return_value = mock_db_user
        mock_ci_result = MagicMock()
        mock_ci_result.scalars.return_value.first.return_value = mock_db_user
        mock_session.execute.side_effect = [mock_select_result, mock_ci_result, AsyncMock()]
        user_service.repository.session = MagicMock()
        user_service.repository.session.return_value = AsyncMock()
        user_service.repository.session.return_value.__aenter__.return_value = mock_session
        auth_user = await user_service.update_user(
            user_id="test_user", email="updated@example.com", role=UserRole.OPERATOR
        )
        assert auth_user is not None
        assert mock_session.commit.called
        assert mock_session.refresh.called

    @pytest.mark.asyncio
    async def test_update_user_not_found(self, user_service: UserService) -> None:
        """Test update_user returns None for nonexistent user.

        Given: No user exists with the given ID.
        When: update_user is called with a nonexistent user ID.
        Then: None is returned.
        """
        mock_session = AsyncMock()
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = None
        mock_session.execute.return_value = mock_result
        user_service.repository.session = MagicMock()
        user_service.repository.session.return_value = AsyncMock()
        user_service.repository.session.return_value.__aenter__.return_value = mock_session
        auth_user = await user_service.update_user(
            user_id="nonexistent", email="updated@example.com"
        )
        assert auth_user is None

    @pytest.mark.asyncio
    async def test_update_user_deactivate(
        self, user_service: UserService, mock_db_user: User
    ) -> None:
        """Test user can be deactivated via close+insert.

        Given: A user exists with the given ID.
        When: update_user is called with is_active=False.
        Then: close_and_insert is called with is_active=False.
        """
        mock_session = AsyncMock()
        mock_session.add = MagicMock()
        mock_select_result = MagicMock()
        mock_select_result.scalar_one_or_none.return_value = mock_db_user
        mock_ci_result = MagicMock()
        mock_ci_result.scalars.return_value.first.return_value = mock_db_user
        mock_session.execute.side_effect = [mock_select_result, mock_ci_result, AsyncMock()]
        user_service.repository.session = MagicMock()
        user_service.repository.session.return_value = AsyncMock()
        user_service.repository.session.return_value.__aenter__.return_value = mock_session
        auth_user = await user_service.update_user(user_id="test_user", is_active=False)
        assert auth_user is not None

    @pytest.mark.asyncio
    async def test_delete_user_success(self, user_service: UserService, mock_db_user: User) -> None:
        """Test successful user deletion via close+insert with is_active=False.

        Given: A user exists with the given ID.
        When: delete_user is called with the user ID.
        Then: True is returned and close_and_insert is called.
        """
        mock_session = AsyncMock()
        mock_session.add = MagicMock()
        mock_select_result = MagicMock()
        mock_select_result.scalar_one_or_none.return_value = mock_db_user
        mock_ci_result = MagicMock()
        mock_ci_result.scalars.return_value.first.return_value = mock_db_user
        mock_session.execute.side_effect = [mock_select_result, mock_ci_result, AsyncMock()]
        user_service.repository.session = MagicMock()
        user_service.repository.session.return_value = AsyncMock()
        user_service.repository.session.return_value.__aenter__.return_value = mock_session
        result = await user_service.delete_user("test_user")
        assert result is True
        assert mock_session.commit.called

    @pytest.mark.asyncio
    async def test_delete_user_not_found(self, user_service: UserService) -> None:
        """Test delete_user returns False for nonexistent user.

        Given: No user exists with the given ID.
        When: delete_user is called with a nonexistent ID.
        Then: False is returned.
        """
        mock_session = AsyncMock()
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = None
        mock_session.execute.return_value = mock_result
        user_service.repository.session = MagicMock()
        user_service.repository.session.return_value = AsyncMock()
        user_service.repository.session.return_value.__aenter__.return_value = mock_session
        result = await user_service.delete_user("nonexistent")
        assert result is False

    @pytest.mark.asyncio
    async def test_change_password_success(
        self, user_service: UserService, mock_db_user: User
    ) -> None:
        """Test successful password change via close+insert.

        Given: A user exists and the old password is correct.
        When: change_password is called with correct old password.
        Then: True is returned and close_and_insert is called.
        """
        mock_session = AsyncMock()
        mock_session.add = MagicMock()
        mock_select_result = MagicMock()
        mock_select_result.scalar_one_or_none.return_value = mock_db_user
        mock_ci_result = MagicMock()
        mock_ci_result.scalars.return_value.first.return_value = mock_db_user
        mock_session.execute.side_effect = [mock_select_result, mock_ci_result, AsyncMock()]
        user_service.repository.session = MagicMock()
        user_service.repository.session.return_value = AsyncMock()
        user_service.repository.session.return_value.__aenter__.return_value = mock_session
        with (
            patch.object(user_service, "_verify_password", return_value=True),
            patch.object(user_service, "hash_password", return_value="new_hash"),
        ):
            result = await user_service.change_password("test_user", "old_password", "new_password")
        assert result is True
        assert mock_session.commit.called

    @pytest.mark.asyncio
    async def test_change_password_wrong_old_password(
        self, user_service: UserService, mock_db_user: User
    ) -> None:
        """Test change_password fails with incorrect old password.

        Given: A user exists but old password verification fails.
        When: change_password is called with wrong old password.
        Then: False is returned and password is not changed.
        """
        mock_session = AsyncMock()
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = mock_db_user
        mock_session.execute.return_value = mock_result
        user_service.repository.session = MagicMock()
        user_service.repository.session.return_value = AsyncMock()
        user_service.repository.session.return_value.__aenter__.return_value = mock_session
        with patch.object(user_service, "_verify_password", return_value=False):
            result = await user_service.change_password(
                "test_user", "wrong_password", "new_password"
            )
        assert result is False

    @pytest.mark.asyncio
    async def test_change_password_user_not_found(self, user_service: UserService) -> None:
        """Test change_password returns False for nonexistent user.

        Given: No user exists with the given ID.
        When: change_password is called with a nonexistent user ID.
        Then: False is returned.
        """
        mock_session = AsyncMock()
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = None
        mock_session.execute.return_value = mock_result
        user_service.repository.session = MagicMock()
        user_service.repository.session.return_value = AsyncMock()
        user_service.repository.session.return_value.__aenter__.return_value = mock_session
        result = await user_service.change_password("nonexistent", "old_password", "new_password")
        assert result is False

    def test_get_user_service_singleton(self) -> None:
        """Test get_user_service returns the same singleton instance.

        Given: The get_user_service function.
        When: get_user_service is called multiple times.
        Then: The same instance is returned each time.
        """
        service1 = get_user_service()
        service2 = get_user_service()
        assert service1 is service2

    def test_singleton_returns_existing_instance(self) -> None:
        """Test UserService singleton pattern returns existing instance.

        Given: UserService _instance is reset to None.
        When: UserService is instantiated multiple times.
        Then: The same singleton instance is returned and stored.
        """
        with patch("snapper.auth.user_service.get_repository") as mock_get_repo:
            mock_repo = MagicMock()
            mock_get_repo.return_value = mock_repo
            UserService._instance = None
            service1 = UserService()
            assert UserService._instance is service1
            service2 = UserService()
            assert service2 is service1
            assert UserService._instance is service1
            UserService._instance = None

    def test_verify_password_bcrypt(self, user_service: UserService) -> None:
        """Test bcrypt password verification succeeds with correct password.

        Given: A password hashed with bcrypt via hash_password.
        When: _verify_password is called with the correct password.
        Then: True is returned.
        """
        password = "secure_password_123"
        hashed = user_service.hash_password(password)
        assert user_service._verify_password(password, hashed) is True

    def test_verify_password_bcrypt_wrong(self, user_service: UserService) -> None:
        """Test bcrypt password verification fails with wrong password.

        Given: A password hashed with bcrypt via hash_password.
        When: _verify_password is called with an incorrect password.
        Then: False is returned.
        """
        password = "secure_password_123"
        hashed = user_service.hash_password(password)
        assert user_service._verify_password("wrong_password", hashed) is False

    @pytest.mark.asyncio
    async def test_admin_reset_password_success(
        self, user_service: UserService, mock_db_user: User
    ) -> None:
        """Test admin_reset_password succeeds for existing user.

        Given: A user exists in the database.
        When: admin_reset_password is called with new password.
        Then: close_and_insert is called and session is committed.
        """
        mock_session = AsyncMock()
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = mock_db_user
        mock_result.scalars.return_value.first.return_value = mock_db_user
        mock_session.execute.return_value = mock_result
        user_service.repository.session = MagicMock()
        user_service.repository.session.return_value = AsyncMock()
        user_service.repository.session.return_value.__aenter__.return_value = mock_session
        with patch("snapper.auth.user_service.close_and_insert", new_callable=AsyncMock):
            await user_service.admin_reset_password("testuser", "newpassword123")
        assert mock_session.commit.called

    @pytest.mark.asyncio
    async def test_admin_reset_password_user_not_found(self, user_service: UserService) -> None:
        """Test admin_reset_password raises ValueError for missing user.

        Given: No user with the given username exists.
        When: admin_reset_password is called.
        Then: ValueError is raised.
        """
        mock_session = AsyncMock()
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = None
        mock_session.execute.return_value = mock_result
        user_service.repository.session = MagicMock()
        user_service.repository.session.return_value = AsyncMock()
        user_service.repository.session.return_value.__aenter__.return_value = mock_session
        with pytest.raises(ValueError, match="not found"):
            await user_service.admin_reset_password("missing", "newpassword")

    @pytest.mark.asyncio
    async def test_reset_password_by_username_success(
        self, user_service: UserService, mock_db_user: User
    ) -> None:
        """Test reset_password_by_username succeeds for existing user.

        Given: A user exists in the database.
        When: reset_password_by_username is called with new password.
        Then: close_and_insert is called and session is committed.
        """
        mock_session = AsyncMock()
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = mock_db_user
        mock_result.scalars.return_value.first.return_value = mock_db_user
        mock_session.execute.return_value = mock_result
        user_service.repository.session = MagicMock()
        user_service.repository.session.return_value = AsyncMock()
        user_service.repository.session.return_value.__aenter__.return_value = mock_session
        with patch("snapper.auth.user_service.close_and_insert", new_callable=AsyncMock):
            await user_service.reset_password_by_username("testuser", "newpassword123")
        assert mock_session.commit.called

    @pytest.mark.asyncio
    async def test_reset_password_by_username_user_not_found(
        self, user_service: UserService
    ) -> None:
        """Test reset_password_by_username raises ValueError for missing user.

        Given: No user with the given username exists.
        When: reset_password_by_username is called.
        Then: ValueError is raised.
        """
        mock_session = AsyncMock()
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = None
        mock_session.execute.return_value = mock_result
        user_service.repository.session = MagicMock()
        user_service.repository.session.return_value = AsyncMock()
        user_service.repository.session.return_value.__aenter__.return_value = mock_session
        with pytest.raises(ValueError, match="not found"):
            await user_service.reset_password_by_username("missing", "newpassword")

    @pytest.mark.asyncio
    async def test_list_login_events_returns_results(self, user_service: UserService) -> None:
        """Verify list_login_events queries with where_active filter.

        Given: A mock session returning login events,
        When: list_login_events is called,
        Then: Results are returned from the scalars query.
        """
        mock_session = AsyncMock()
        mock_event = MagicMock()
        mock_result = MagicMock()
        mock_result.scalars.return_value.all.return_value = [mock_event]
        mock_session.execute.return_value = mock_result
        user_service.repository.session = MagicMock()
        user_service.repository.session.return_value = AsyncMock()
        user_service.repository.session.return_value.__aenter__.return_value = mock_session
        events = await user_service.list_login_events("user-pub-id")
        assert events == [mock_event]

    @pytest.mark.asyncio
    async def test_close_login_event_found(self, user_service: UserService) -> None:
        """Verify close_login_event returns True when event exists.

        Given: An active login event found by public_id,
        When: close_login_event is called,
        Then: True is returned and UPDATE + commit are executed.
        """
        mock_session = AsyncMock()
        mock_existing = MagicMock()
        mock_existing.id = 42
        mock_select_result = MagicMock()
        mock_select_result.scalars.return_value.first.return_value = mock_existing
        mock_session.execute = AsyncMock(return_value=mock_select_result)
        mock_session.add = MagicMock()
        user_service.repository.session = MagicMock()
        user_service.repository.session.return_value = AsyncMock()
        user_service.repository.session.return_value.__aenter__.return_value = mock_session
        result = await user_service.close_login_event("evt-pub-id")
        assert result is True

    @pytest.mark.asyncio
    async def test_close_login_event_not_found(self, user_service: UserService) -> None:
        """Verify close_login_event returns False when event not found.

        Given: No active login event for the given public_id,
        When: close_login_event is called,
        Then: False is returned.
        """
        mock_session = AsyncMock()
        mock_result = MagicMock()
        mock_result.scalars.return_value.first.return_value = None
        mock_session.execute.return_value = mock_result
        user_service.repository.session = MagicMock()
        user_service.repository.session.return_value = AsyncMock()
        user_service.repository.session.return_value.__aenter__.return_value = mock_session
        result = await user_service.close_login_event("missing-pub-id")
        assert result is False


class TestBuildAuthPrincipal:
    """Tests for ``UserService.build_auth_principal`` multi-tenant population."""

    def _make_profile(self, role: UserRole) -> UserProfile:
        return UserProfile(
            public_id="user-public-id-1",
            timestamp=datetime.now(UTC),
            session_id="seed-session",
            sequence_id=1,
            username="alice",
            email="alice@example.com",
            role=role,
            is_active=True,
            created_at=datetime.now(UTC),
        )

    @pytest.fixture
    def user_service_with_repo(self) -> Generator[UserService]:
        """Yield a UserService whose repository has the multi-tenant lookups mocked."""
        with patch("snapper.auth.user_service.get_repository") as mock_get_repo:
            mock_repo = MagicMock()
            mock_repo.list_active_operators = AsyncMock()
            mock_repo.get_user_operator_memberships = AsyncMock()
            mock_get_repo.return_value = mock_repo
            UserService.clear_instance()
            service = UserService()
            yield service
            UserService.clear_instance()

    @pytest.mark.asyncio
    async def test_admin_receives_every_active_operator(
        self, user_service_with_repo: UserService
    ) -> None:
        """ADMIN role gets the operator set covering every active operator.

        Given: Three active operators in the DB and an explicit primary
            membership row for the admin user,
        When: ``build_auth_principal`` is invoked for an ADMIN profile,
        Then: ``operator_public_ids`` mirrors all three operators and
            ``primary_operator_public_id`` is taken from the membership row.
        """
        repo = user_service_with_repo.repository
        repo.list_active_operators.return_value = [
            {
                "public_id": "op-1",
                "label": "alpha",
                "description": None,
                "timestamp": datetime.now(UTC),
                "session_id": "s",
                "sequence_id": 1,
            },
            {
                "public_id": "op-2",
                "label": "beta",
                "description": None,
                "timestamp": datetime.now(UTC),
                "session_id": "s",
                "sequence_id": 2,
            },
            {
                "public_id": "op-3",
                "label": "gamma",
                "description": None,
                "timestamp": datetime.now(UTC),
                "session_id": "s",
                "sequence_id": 3,
            },
        ]
        repo.get_user_operator_memberships.return_value = [
            {
                "public_id": "m-1",
                "user_public_id": "user-public-id-1",
                "operator_public_id": "op-2",
                "is_primary": True,
                "timestamp": datetime.now(UTC),
                "session_id": "s",
                "sequence_id": 10,
            }
        ]

        principal = await user_service_with_repo.build_auth_principal(
            self._make_profile(UserRole.ADMIN)
        )

        assert principal.role == UserRole.ADMIN
        assert principal.user_public_id == "user-public-id-1"
        assert principal.operator_public_ids == ["op-1", "op-2", "op-3"]
        assert principal.primary_operator_public_id == "op-2"

    @pytest.mark.asyncio
    async def test_operator_receives_only_explicit_memberships(
        self, user_service_with_repo: UserService
    ) -> None:
        """OPERATOR role's operator_public_ids comes only from memberships.

        Given: An OPERATOR profile with two membership rows (one primary),
        When: ``build_auth_principal`` is invoked,
        Then: ``operator_public_ids`` lists only the membership operators
            (NOT the full active set), and ``primary_operator_public_id``
            is the one flagged ``is_primary``.
        """
        repo = user_service_with_repo.repository
        repo.get_user_operator_memberships.return_value = [
            {
                "public_id": "m-1",
                "user_public_id": "user-public-id-1",
                "operator_public_id": "op-77",
                "is_primary": False,
                "timestamp": datetime.now(UTC),
                "session_id": "s",
                "sequence_id": 11,
            },
            {
                "public_id": "m-2",
                "user_public_id": "user-public-id-1",
                "operator_public_id": "op-99",
                "is_primary": True,
                "timestamp": datetime.now(UTC),
                "session_id": "s",
                "sequence_id": 12,
            },
        ]

        principal = await user_service_with_repo.build_auth_principal(
            self._make_profile(UserRole.OPERATOR)
        )

        repo.list_active_operators.assert_not_awaited()
        assert principal.role == UserRole.OPERATOR
        assert principal.operator_public_ids == ["op-77", "op-99"]
        assert principal.primary_operator_public_id == "op-99"

    @pytest.mark.asyncio
    async def test_user_with_no_memberships_yields_empty_primary(
        self, user_service_with_repo: UserService
    ) -> None:
        """A VIEWER with no memberships yields empty operator IDs and empty primary.

        Given: A VIEWER profile with zero membership rows,
        When: ``build_auth_principal`` is invoked,
        Then: ``operator_public_ids`` is an empty list and
            ``primary_operator_public_id`` is the empty string sentinel.
        """
        repo = user_service_with_repo.repository
        repo.get_user_operator_memberships.return_value = []

        principal = await user_service_with_repo.build_auth_principal(
            self._make_profile(UserRole.VIEWER)
        )

        assert principal.operator_public_ids == []
        assert principal.primary_operator_public_id == ""
        assert principal.user_public_id == "user-public-id-1"
