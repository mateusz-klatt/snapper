"""Tests for settings routes and schemas."""

from datetime import UTC
from datetime import datetime
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest
from fastapi import HTTPException

from snapper.api.schemas.settings import SettingRead
from snapper.api.schemas.settings import SettingUpdate
from snapper.auth.domain.roles import UserRole
from snapper.auth.schemas.user import UserProfile
from snapper.config.settings_routes import delete_setting
from snapper.config.settings_routes import get_all_settings
from snapper.config.settings_routes import get_setting_categories
from snapper.config.settings_routes import update_setting


class MockRepository:
    """Mock repository with session accessor."""

    def __init__(self) -> None:
        """Initialize the instance."""
        self.session = MagicMock()


class MockSession:
    """Mock async database session for settings tests."""

    def __init__(self) -> None:
        """Initialize the instance."""
        self.execute = AsyncMock()
        self.delete = AsyncMock()
        self.commit = AsyncMock()

    async def __aenter__(self) -> MockSession:
        """Magic method."""
        return self

    async def __aexit__(self, exc_type: type, exc_val: Exception, exc_tb: object) -> None:
        """Magic method."""
        pass


class MockResult:
    """Mock database result with scalars/scalar methods."""

    def __init__(self, data: list[MagicMock]) -> None:
        """Initialize the instance."""
        self.data = data

    def scalars(self) -> MockScalars:
        """Return MockScalars wrapper."""
        return MockScalars(self.data)

    def scalar_one_or_none(self) -> MagicMock | None:
        """Return first item or None."""
        return self.data[0] if self.data else None

    def scalar_one(self) -> MagicMock:
        """Return first item or raise."""
        if not self.data:
            raise ValueError("No data")
        return self.data[0]

    def fetchall(self) -> list[tuple[str]]:
        """Return all categories as tuples."""
        return [(item.category,) for item in self.data]


class MockScalars:
    """Mock scalars result wrapper."""

    def __init__(self, data: list[MagicMock]) -> None:
        """Initialize the instance."""
        self.data = data

    def all(self) -> list[MagicMock]:
        """Return all items."""
        return self.data


class TestSettingsRoutes:
    """Tests for settings API routes and schemas."""

    @staticmethod
    def _make_user(role: UserRole = UserRole.ADMIN) -> UserProfile:
        return UserProfile(
            id="test_id",
            username="test_user",
            email="test@example.com",
            role=role,
            is_active=True,
        )

    def test_setting_response_model(self) -> None:
        """Verify SettingRead response model maps fields correctly.

        Given: A SettingRead model with all fields populated,
        When: The model is instantiated,
        Then: All fields are accessible and contain expected values.
        """
        timestamp = datetime.now(UTC)
        response = SettingRead(
            key="test_key",
            value="test_value",
            category="test_category",
            description="Test description",
            updated_at=timestamp,
            updated_by="test_user",
        )
        assert response.key == "test_key"
        assert response.value == "test_value"
        assert response.category == "test_category"
        assert response.description == "Test description"
        assert response.updated_at == timestamp
        assert response.updated_by == "test_user"

    def test_setting_update_request_model(self) -> None:
        """Verify SettingUpdate request model validation.

        Given: A SettingUpdate model with update fields,
        When: The model is instantiated,
        Then: All fields are validated and accessible.
        """
        request = SettingUpdate(
            value="new_value",
            category="new_category",
            description="New description",
        )
        assert request.value == "new_value"
        assert request.category == "new_category"
        assert request.description == "New description"

    @pytest.mark.asyncio
    async def test_get_all_settings_no_filter(self) -> None:
        """Verify get_all_settings returns all settings without filter.

        Given: Multiple settings exist in database,
        When: get_all_settings is called without category filter,
        Then: All settings are returned.
        """
        mock_user = self._make_user(UserRole.ADMIN)
        mock_settings = MagicMock()
        mock_settings.db_url = "sqlite:///:memory:"
        mock_repository = MockRepository()
        mock_session = MockSession()
        mock_setting1 = MagicMock()
        mock_setting1.key = "key1"
        mock_setting1.value = "value1"
        mock_setting1.category = "category1"
        mock_setting1.description = "desc1"
        mock_setting1.updated_at = datetime.now(UTC)
        mock_setting1.updated_by = "user1"
        mock_setting2 = MagicMock()
        mock_setting2.key = "key2"
        mock_setting2.value = "value2"
        mock_setting2.category = "category2"
        mock_setting2.description = "desc2"
        mock_setting2.updated_at = datetime.now(UTC)
        mock_setting2.updated_by = "user2"
        mock_session.execute.return_value = MockResult([mock_setting1, mock_setting2])
        mock_repository.session.return_value = mock_session
        with (
            patch("snapper.config.settings_routes.get_settings", return_value=mock_settings),
            patch("snapper.config.settings_routes.get_repository", return_value=mock_repository),
        ):
            result = await get_all_settings(user=mock_user)
        assert len(result) == 2
        assert result[0].key == "key1"
        assert result[0].value == "value1"
        assert result[1].key == "key2"
        assert result[1].value == "value2"

    @pytest.mark.asyncio
    async def test_get_all_settings_with_category_filter(self) -> None:
        """Verify get_all_settings filters by category.

        Given: Settings with different categories exist,
        When: get_all_settings is called with category filter,
        Then: Only settings matching the category are returned.
        """
        mock_user = self._make_user(UserRole.ADMIN)
        mock_settings = MagicMock()
        mock_settings.db_url = "sqlite:///:memory:"
        mock_repository = MockRepository()
        mock_session = MockSession()
        mock_setting = MagicMock()
        mock_setting.key = "key1"
        mock_setting.value = "value1"
        mock_setting.category = "auth"
        mock_setting.description = "desc1"
        mock_setting.updated_at = datetime.now(UTC)
        mock_setting.updated_by = "user1"
        mock_session.execute.return_value = MockResult([mock_setting])
        mock_repository.session.return_value = mock_session
        with (
            patch("snapper.config.settings_routes.get_settings", return_value=mock_settings),
            patch("snapper.config.settings_routes.get_repository", return_value=mock_repository),
        ):
            result = await get_all_settings(category="auth", user=mock_user)
        assert len(result) == 1
        assert result[0].category == "auth"

    @pytest.mark.asyncio
    async def test_get_setting_categories(self) -> None:
        """Verify get_setting_categories returns unique categories.

        Given: Settings with different categories exist,
        When: get_setting_categories is called,
        Then: All unique category names are returned.
        """
        mock_user = self._make_user(UserRole.ADMIN)
        mock_settings = MagicMock()
        mock_settings.db_url = "sqlite:///:memory:"
        mock_repository = MockRepository()
        mock_session = MockSession()
        mock_setting1 = MagicMock()
        mock_setting1.category = "auth"
        mock_setting2 = MagicMock()
        mock_setting2.category = "system"
        mock_session.execute.return_value = MockResult([mock_setting1, mock_setting2])
        mock_repository.session.return_value = mock_session
        with (
            patch("snapper.config.settings_routes.get_settings", return_value=mock_settings),
            patch("snapper.config.settings_routes.get_repository", return_value=mock_repository),
        ):
            result = await get_setting_categories(user=mock_user)
        assert result.categories == ["auth", "system"]

    @pytest.mark.asyncio
    async def test_update_setting_found(self) -> None:
        """Verify update_setting updates existing setting.

        Given: An existing setting in database,
        When: update_setting is called with new values,
        Then: Setting is updated and returned with new values.
        """
        mock_user = self._make_user(UserRole.ADMIN)
        mock_settings = MagicMock()
        mock_settings.db_url = "sqlite:///:memory:"
        mock_settings.zmq_broker_xpub = "tcp://localhost:5555"
        mock_settings_service = AsyncMock()
        mock_repository = MockRepository()
        mock_session = MockSession()
        mock_setting = MagicMock()
        mock_setting.key = "existing_key"
        mock_setting.value = "updated_value"
        mock_setting.category = "updated_category"
        mock_setting.description = "Updated description"
        mock_setting.updated_at = datetime.now(UTC)
        mock_setting.updated_by = "test_user"
        mock_session.execute.return_value = MockResult([mock_setting])
        mock_repository.session.return_value = mock_session
        request = SettingUpdate(
            value="updated_value",
            category="updated_category",
            description="Updated description",
        )
        with (
            patch("snapper.config.settings_routes.get_settings", return_value=mock_settings),
            patch(
                "snapper.config.settings_routes.get_settings_service",
                return_value=mock_settings_service,
            ),
            patch("snapper.config.settings_routes.get_repository", return_value=mock_repository),
        ):
            result = await update_setting(
                key="existing_key", request=request, user=mock_user, _csrf=None
            )
        mock_settings_service.update_setting.assert_called_once_with(
            key="existing_key",
            value="updated_value",
            category="updated_category",
            description="Updated description",
            updated_by="test_user",
        )
        assert result.key == "existing_key"
        assert result.value == "updated_value"

    @pytest.mark.asyncio
    async def test_update_setting_not_found(self) -> None:
        """Verify update_setting raises 404 for non-existent setting.

        Given: No setting with the specified key exists,
        When: update_setting is called with that key,
        Then: HTTPException with 404 status is raised.
        """
        mock_user = self._make_user(UserRole.ADMIN)
        mock_settings = MagicMock()
        mock_settings.db_url = "sqlite:///:memory:"
        mock_settings.zmq_broker_xpub = "tcp://localhost:5555"
        mock_settings_service = AsyncMock()
        mock_repository = MockRepository()
        mock_session = MockSession()
        mock_session.execute.return_value = MockResult([])
        mock_repository.session.return_value = mock_session
        request = SettingUpdate(value="updated_value", description=None)
        with (
            patch("snapper.config.settings_routes.get_settings", return_value=mock_settings),
            patch(
                "snapper.config.settings_routes.get_settings_service",
                return_value=mock_settings_service,
            ),
            patch("snapper.config.settings_routes.get_repository", return_value=mock_repository),
            pytest.raises(HTTPException) as exc_info,
        ):
            await update_setting(key="nonexistent_key", request=request, user=mock_user, _csrf=None)
        assert exc_info.value.status_code == 404

    @pytest.mark.asyncio
    async def test_delete_setting_found(self) -> None:
        """Verify delete_setting removes existing setting.

        Given: An existing setting in database,
        When: delete_setting is called with that key,
        Then: Setting is deleted and success message is returned.
        """
        mock_user = self._make_user(UserRole.ADMIN)
        mock_settings = MagicMock()
        mock_settings.db_url = "sqlite:///:memory:"
        mock_repository = MockRepository()
        mock_session = MockSession()
        mock_setting = MagicMock()
        mock_setting.key = "delete_key"
        mock_session.execute.return_value = MockResult([mock_setting])
        mock_repository.session.return_value = mock_session
        with (
            patch("snapper.config.settings_routes.get_settings", return_value=mock_settings),
            patch("snapper.config.settings_routes.get_repository", return_value=mock_repository),
        ):
            result = await delete_setting(key="delete_key", user=mock_user, _csrf=None)
        mock_session.delete.assert_called_once_with(mock_setting)
        mock_session.commit.assert_called_once()
        assert result.message == "Setting 'delete_key' deleted successfully"

    @pytest.mark.asyncio
    async def test_delete_setting_not_found(self) -> None:
        """Verify delete_setting raises 404 for non-existent setting.

        Given: No setting with the specified key exists,
        When: delete_setting is called with that key,
        Then: HTTPException with 404 status is raised.
        """
        mock_user = self._make_user(UserRole.ADMIN)
        mock_settings = MagicMock()
        mock_settings.db_url = "sqlite:///:memory:"
        mock_repository = MockRepository()
        mock_session = MockSession()
        mock_session.execute.return_value = MockResult([])
        mock_repository.session.return_value = mock_session
        with (
            patch("snapper.config.settings_routes.get_settings", return_value=mock_settings),
            patch("snapper.config.settings_routes.get_repository", return_value=mock_repository),
            pytest.raises(HTTPException) as exc_info,
        ):
            await delete_setting(key="nonexistent_key", user=mock_user, _csrf=None)
        assert exc_info.value.status_code == 404
