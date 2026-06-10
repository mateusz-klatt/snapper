"""Tests for settings routes and schemas."""

from datetime import UTC
from datetime import datetime
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest
from fastapi import HTTPException

from snapper.api.schemas.settings import PushBetaUsersBody
from snapper.api.schemas.settings import RemoveSettingBody
from snapper.api.schemas.settings import RemoveSettingRequest
from snapper.api.schemas.settings import SettingRead
from snapper.api.schemas.settings import SettingUpdate
from snapper.api.schemas.settings import SettingUpdateBody
from snapper.api.schemas.settings import UpdatePushBetaUsersCommand
from snapper.application.notify.push_beta import PUSH_BETA_SETTING_KEY
from snapper.auth.domain.roles import UserRole
from snapper.auth.schemas.principal import AuthPrincipal
from snapper.config.settings_routes import get_all_settings
from snapper.config.settings_routes import get_public_feature_flags
from snapper.config.settings_routes import get_push_beta_users
from snapper.config.settings_routes import get_setting_categories
from snapper.config.settings_routes import remove_setting
from snapper.config.settings_routes import set_push_beta_users
from snapper.config.settings_routes import set_setting
from snapper.messaging.infrastructure.publisher import SequenceTracker


class MockRepository:
    """Mock repository with session accessor and read methods."""

    def __init__(self) -> None:
        """Initialize the instance."""
        self.session = MagicMock()
        self._settings: list[dict[str, object]] = []
        self._categories: list[str] = []

    def set_settings(self, settings: list[MagicMock]) -> None:
        """Configure settings data for get_settings calls."""
        self._settings = [
            {
                "public_id": s.public_id,
                "timestamp": s.timestamp,
                "session_id": s.session_id,
                "sequence_id": s.sequence_id,
                "key": s.key,
                "value": s.value,
                "category": s.category,
                "description": s.description,
                "updated_by": s.updated_by,
            }
            for s in settings
        ]

    def set_categories(self, categories: list[str]) -> None:
        """Configure categories data for get_setting_categories calls."""
        self._categories = categories

    async def get_settings(
        self, as_of: object, category: str | None = None
    ) -> list[dict[str, object]]:
        """Return configured settings, optionally filtered by category."""
        if category:
            return [s for s in self._settings if s["category"] == category]
        return self._settings

    async def get_setting_categories(self, as_of: object) -> list[str]:
        """Return configured categories."""
        return sorted(self._categories)


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
    def _make_user(role: UserRole = UserRole.ADMIN) -> AuthPrincipal:
        return AuthPrincipal(
            username="test_user",
            email="test@example.com",
            role=role,
            is_active=True,
        )

    @staticmethod
    def _make_mock_setting(**overrides: object) -> MagicMock:
        """Create a mock Setting row with provenance fields."""
        mock = MagicMock()
        mock.public_id = "test-public-id"
        mock.session_id = "test-session-id"
        mock.sequence_id = 1
        mock.timestamp = datetime.now(UTC)
        for key, value in overrides.items():
            setattr(mock, key, value)
        return mock

    @staticmethod
    def _make_rest_request() -> MagicMock:
        """Create a mock FastAPI Request with rest_tracker."""
        mock_request = MagicMock()
        mock_request.app.state.rest_tracker = SequenceTracker()
        return mock_request

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
            session_id="test-sid",
            sequence_id=1,
            public_id="test-public-id",
            timestamp=timestamp,
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
            session_id="test-sid",
            sequence_id=1,
            public_id="test-pid",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            payload=SettingUpdateBody(
                value="new_value",
                category="new_category",
                description="New description",
            ),
        )
        assert request.payload.value == "new_value"
        assert request.payload.category == "new_category"
        assert request.payload.description == "New description"

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
        mock_setting1 = self._make_mock_setting(
            key="key1",
            value="value1",
            category="category1",
            description="desc1",
            updated_by="user1",
        )
        mock_setting2 = self._make_mock_setting(
            key="key2",
            value="value2",
            category="category2",
            description="desc2",
            updated_by="user2",
        )
        mock_repository.set_settings([mock_setting1, mock_setting2])
        with (
            patch("snapper.config.settings_routes.get_settings", return_value=mock_settings),
            patch("snapper.config.settings_routes.get_repository", return_value=mock_repository),
        ):
            result = await get_all_settings(request=self._make_rest_request(), user=mock_user)
        assert result.count == 2
        assert result.payload[0].key == "key1"
        assert result.payload[0].value == "value1"
        assert result.payload[1].key == "key2"
        assert result.payload[1].value == "value2"

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
        mock_setting = self._make_mock_setting(
            key="key1",
            value="value1",
            category="auth",
            description="desc1",
            updated_by="user1",
        )
        mock_repository.set_settings([mock_setting])
        with (
            patch("snapper.config.settings_routes.get_settings", return_value=mock_settings),
            patch("snapper.config.settings_routes.get_repository", return_value=mock_repository),
        ):
            result = await get_all_settings(
                request=self._make_rest_request(), category="auth", user=mock_user
            )
        assert result.count == 1
        assert result.payload[0].category == "auth"

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
        mock_repository.set_categories(["auth", "system"])
        with (
            patch("snapper.config.settings_routes.get_settings", return_value=mock_settings),
            patch("snapper.config.settings_routes.get_repository", return_value=mock_repository),
        ):
            result = await get_setting_categories(
                request=self._make_rest_request(),
                user=mock_user,
            )
        assert result.payload == ["auth", "system"]

    @pytest.mark.asyncio
    async def test_set_setting_found(self) -> None:
        """Verify set_setting updates existing setting.

        Given: An existing setting in database,
        When: set_setting is called with new values,
        Then: Setting is updated and returned with new values.
        """
        mock_user = self._make_user(UserRole.ADMIN)
        mock_settings = MagicMock()
        mock_settings.db_url = "sqlite:///:memory:"
        mock_settings.zmq_broker_xpub = "tcp://localhost:5555"
        mock_settings_service = AsyncMock()
        mock_repository = MockRepository()
        mock_session = MockSession()
        mock_setting = self._make_mock_setting(
            key="existing_key",
            value="updated_value",
            category="updated_category",
            description="Updated description",
            updated_by="test_user",
        )
        mock_session.execute.return_value = MockResult([mock_setting])
        mock_repository.session.return_value = mock_session
        request = SettingUpdate(
            session_id="test-sid",
            sequence_id=1,
            public_id="test-pid",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            payload=SettingUpdateBody(
                value="updated_value",
                category="updated_category",
                description="Updated description",
            ),
        )
        with (
            patch("snapper.config.settings_routes.get_settings", return_value=mock_settings),
            patch(
                "snapper.config.settings_routes.get_settings_service",
                return_value=mock_settings_service,
            ),
            patch("snapper.config.settings_routes.get_repository", return_value=mock_repository),
        ):
            result = await set_setting(
                http_request=self._make_rest_request(),
                key="existing_key",
                body=request,
                user=mock_user,
                _csrf=None,
            )
        mock_settings_service.update_setting.assert_called_once_with(
            key="existing_key",
            value="updated_value",
            category="updated_category",
            description="Updated description",
            updated_by="test_user",
        )
        assert result.payload.key == "existing_key"
        assert result.payload.value == "updated_value"

    @pytest.mark.asyncio
    async def test_set_setting_not_found(self) -> None:
        """Verify set_setting raises 404 for non-existent setting.

        Given: No setting with the specified key exists,
        When: set_setting is called with that key,
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
        request = SettingUpdate(
            session_id="test-sid",
            sequence_id=1,
            public_id="test-pid",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            payload=SettingUpdateBody(
                value="updated_value",
                description=None,
            ),
        )
        with (
            patch("snapper.config.settings_routes.get_settings", return_value=mock_settings),
            patch(
                "snapper.config.settings_routes.get_settings_service",
                return_value=mock_settings_service,
            ),
            patch("snapper.config.settings_routes.get_repository", return_value=mock_repository),
            pytest.raises(HTTPException) as exc_info,
        ):
            await set_setting(
                http_request=self._make_rest_request(),
                key="nonexistent_key",
                body=request,
                user=mock_user,
                _csrf=None,
            )
        assert exc_info.value.status_code == 404

    @pytest.mark.asyncio
    async def test_remove_setting_found(self) -> None:
        """Verify remove_setting temporally closes existing setting.

        Given: An existing setting in database,
        When: remove_setting is called with that key,
        Then: Setting is closed via update (known_to=now) and success message is returned.
        """
        mock_user = self._make_user(UserRole.ADMIN)
        mock_settings = MagicMock()
        mock_settings.db_url = "sqlite:///:memory:"
        mock_repository = MockRepository()
        mock_session = MockSession()
        mock_setting = MagicMock()
        mock_setting.key = "delete_key"
        mock_setting.id = 42
        mock_session.execute.return_value = MockResult([mock_setting])
        mock_repository.session.return_value = mock_session
        body = RemoveSettingRequest(
            session_id="test-sid",
            sequence_id=0,
            public_id="test-pid",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            payload=RemoveSettingBody(),
        )
        with (
            patch("snapper.config.settings_routes.get_settings", return_value=mock_settings),
            patch("snapper.config.settings_routes.get_repository", return_value=mock_repository),
        ):
            mock_request = MagicMock()
            mock_request.app.state.rest_tracker = SequenceTracker()
            result = await remove_setting(
                request=mock_request, key="delete_key", _body=body, user=mock_user, _csrf=None
            )
        assert mock_session.execute.await_count == 2
        mock_session.commit.assert_called_once()
        assert result.payload == "Setting 'delete_key' deleted successfully"

    @pytest.mark.asyncio
    async def test_remove_setting_not_found(self) -> None:
        """Verify remove_setting raises 404 for non-existent setting.

        Given: No setting with the specified key exists,
        When: remove_setting is called with that key,
        Then: HTTPException with 404 status is raised.
        """
        mock_user = self._make_user(UserRole.ADMIN)
        mock_settings = MagicMock()
        mock_settings.db_url = "sqlite:///:memory:"
        mock_repository = MockRepository()
        mock_session = MockSession()
        mock_session.execute.return_value = MockResult([])
        mock_repository.session.return_value = mock_session
        body = RemoveSettingRequest(
            session_id="test-sid",
            sequence_id=0,
            public_id="test-pid",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
            payload=RemoveSettingBody(),
        )
        with (
            patch("snapper.config.settings_routes.get_settings", return_value=mock_settings),
            patch("snapper.config.settings_routes.get_repository", return_value=mock_repository),
            pytest.raises(HTTPException) as exc_info,
        ):
            mock_request = MagicMock()
            mock_request.app.state.rest_tracker = SequenceTracker()
            await remove_setting(
                request=mock_request, key="nonexistent_key", _body=body, user=mock_user, _csrf=None
            )
        assert exc_info.value.status_code == 404


class TestPublicFeatureFlags:
    """Tests for the public ``GET /settings/features`` endpoint."""

    @pytest.mark.asyncio
    async def test_default_on_when_flag_absent(self) -> None:
        """Settings service default path → ``ai_integration_enabled=True``.

        The ``ai_integration_enabled`` flag defaults to on so a fresh
        install exposes the AI Integration navigation entry without
        any manual DB flip. Flipping the setting to ``false``
        remains the way to hide the feature.
        """
        mock_request = MagicMock()
        mock_request.app.state.rest_tracker = SequenceTracker()
        mock_settings_service = MagicMock()
        mock_settings_service.get_setting.side_effect = lambda key, default=None: default
        mock_request.app.state.settings_service = mock_settings_service
        response = await get_public_feature_flags(request=mock_request)
        assert response.payload.ai_integration_enabled is True
        mock_settings_service.get_setting.assert_called_once_with(
            "ai_integration_enabled", default=True
        )

    @pytest.mark.asyncio
    async def test_returns_false_when_flag_explicitly_disabled(self) -> None:
        """Operator flipped the flag to False → endpoint surfaces False."""
        mock_request = MagicMock()
        mock_request.app.state.rest_tracker = SequenceTracker()
        mock_settings_service = MagicMock()
        mock_settings_service.get_setting.return_value = False
        mock_request.app.state.settings_service = mock_settings_service
        response = await get_public_feature_flags(request=mock_request)
        assert response.payload.ai_integration_enabled is False

    @pytest.mark.asyncio
    async def test_returns_true_when_flag_enabled(self) -> None:
        """Settings service returns True → response reflects it."""
        mock_request = MagicMock()
        mock_request.app.state.rest_tracker = SequenceTracker()
        mock_settings_service = MagicMock()
        mock_settings_service.get_setting.return_value = True
        mock_request.app.state.settings_service = mock_settings_service
        response = await get_public_feature_flags(request=mock_request)
        assert response.payload.ai_integration_enabled is True

    @pytest.mark.asyncio
    async def test_response_envelope_has_provenance(self) -> None:
        """Response envelope carries session/sequence/public_id/timestamp."""
        mock_request = MagicMock()
        tracker = SequenceTracker()
        mock_request.app.state.rest_tracker = tracker
        mock_settings_service = MagicMock()
        mock_settings_service.get_setting.return_value = True
        mock_request.app.state.settings_service = mock_settings_service
        response = await get_public_feature_flags(request=mock_request)
        assert response.session_id == tracker.session_id
        assert response.sequence_id >= 1
        assert response.public_id
        assert response.timestamp is not None
        assert response.type == "feature_flags_response"

    @pytest.mark.asyncio
    async def test_returns_true_when_settings_service_missing(self) -> None:
        """Lifespan-not-ready (state.settings_service absent) → True.

        The endpoint must not raise when the FastAPI lifespan hasn't
        finished wiring ``app.state.settings_service``; callers that
        hit the route during startup see the default-on state so
        the frontend can render the AI Integration nav entry
        optimistically.
        """
        mock_request = MagicMock()
        mock_request.app.state = MagicMock(spec=["rest_tracker"])
        mock_request.app.state.rest_tracker = SequenceTracker()
        response = await get_public_feature_flags(request=mock_request)
        assert response.payload.ai_integration_enabled is True


class TestPushBetaUsersRoutes:
    """Tests for ``/settings/push-beta/users`` admin endpoints."""

    @staticmethod
    def _make_user(role: UserRole = UserRole.ADMIN) -> AuthPrincipal:
        return AuthPrincipal(
            username="admin_user",
            email="admin@example.com",
            role=role,
            is_active=True,
        )

    @staticmethod
    def _make_request() -> MagicMock:
        mock_request = MagicMock()
        mock_request.app.state.rest_tracker = SequenceTracker()
        return mock_request

    @pytest.mark.asyncio
    async def test_get_returns_disabled_default_when_setting_absent(self) -> None:
        """Setting absent → response surfaces enabled=False + empty list.

        A fresh install must see the gate as disabled so push delivery
        proceeds for every authenticated user (legacy default).
        """
        mock_settings = MagicMock()
        mock_settings.db_url = "sqlite:///:memory:"
        mock_settings.zmq_broker_xsub = "tcp://localhost:5555"
        mock_settings_service = MagicMock()
        mock_settings_service.get_setting.return_value = None
        with (
            patch("snapper.config.settings_routes.get_settings", return_value=mock_settings),
            patch(
                "snapper.config.settings_routes.get_settings_service",
                AsyncMock(return_value=mock_settings_service),
            ),
        ):
            response = await get_push_beta_users(
                request=self._make_request(), user=self._make_user()
            )
        assert response.payload.enabled is False
        assert response.payload.user_public_ids == []
        mock_settings_service.get_setting.assert_called_once_with(PUSH_BETA_SETTING_KEY)

    @pytest.mark.asyncio
    async def test_get_returns_decoded_setting(self) -> None:
        """Setting present → response surfaces the decoded JSON config."""
        mock_settings = MagicMock()
        mock_settings.db_url = "sqlite:///:memory:"
        mock_settings.zmq_broker_xsub = "tcp://localhost:5555"
        mock_settings_service = MagicMock()
        mock_settings_service.get_setting.return_value = (
            '{"enabled": true, "user_public_ids": ["user-a", "user-b"]}'
        )
        with (
            patch("snapper.config.settings_routes.get_settings", return_value=mock_settings),
            patch(
                "snapper.config.settings_routes.get_settings_service",
                AsyncMock(return_value=mock_settings_service),
            ),
        ):
            response = await get_push_beta_users(
                request=self._make_request(), user=self._make_user()
            )
        assert response.payload.enabled is True
        assert response.payload.user_public_ids == ["user-a", "user-b"]

    @pytest.mark.asyncio
    async def test_post_writes_canonical_json_to_settings_service(self) -> None:
        """POST replaces the entire allowlist via canonical JSON.

        The serialised value sorts + dedups user ids so re-POSTing
        the same logical set does not churn the SCD2 history.
        """
        mock_settings = MagicMock()
        mock_settings.db_url = "sqlite:///:memory:"
        mock_settings.zmq_broker_xsub = "tcp://localhost:5555"
        mock_settings_service = AsyncMock()
        command = UpdatePushBetaUsersCommand(
            session_id="client-sid",
            sequence_id=1,
            public_id="client-pid",
            timestamp=datetime(2026, 4, 25, tzinfo=UTC),
            payload=PushBetaUsersBody(
                enabled=True,
                user_public_ids=["user-b", "user-a", "user-a"],
            ),
        )
        with (
            patch("snapper.config.settings_routes.get_settings", return_value=mock_settings),
            patch(
                "snapper.config.settings_routes.get_settings_service",
                AsyncMock(return_value=mock_settings_service),
            ),
        ):
            response = await set_push_beta_users(
                request=self._make_request(),
                user=self._make_user(),
                _csrf=None,
                body=command,
            )
        update_call = mock_settings_service.update_setting.await_args
        assert update_call.kwargs["key"] == PUSH_BETA_SETTING_KEY
        assert update_call.kwargs["value"] == (
            '{"enabled":true,"user_public_ids":["user-a","user-b"]}'
        )
        assert update_call.kwargs["category"] == "notifications"
        assert update_call.kwargs["updated_by"] == "admin_user"
        assert response.payload.enabled is True
        assert response.payload.user_public_ids == ["user-a", "user-b"]

    @pytest.mark.asyncio
    async def test_post_with_empty_allowlist_serialises_empty_list(self) -> None:
        """``enabled=True`` with an empty allowlist silences every push.

        The admin contract: explicit empty list = "nobody is in the
        beta yet" — used during an incremental rollout where the gate is
        flipped on before users are added.
        """
        mock_settings = MagicMock()
        mock_settings.db_url = "sqlite:///:memory:"
        mock_settings.zmq_broker_xsub = "tcp://localhost:5555"
        mock_settings_service = AsyncMock()
        command = UpdatePushBetaUsersCommand(
            session_id="client-sid",
            sequence_id=1,
            public_id="client-pid",
            timestamp=datetime(2026, 4, 25, tzinfo=UTC),
            payload=PushBetaUsersBody(enabled=True, user_public_ids=[]),
        )
        with (
            patch("snapper.config.settings_routes.get_settings", return_value=mock_settings),
            patch(
                "snapper.config.settings_routes.get_settings_service",
                AsyncMock(return_value=mock_settings_service),
            ),
        ):
            response = await set_push_beta_users(
                request=self._make_request(),
                user=self._make_user(),
                _csrf=None,
                body=command,
            )
        update_call = mock_settings_service.update_setting.await_args
        assert update_call.kwargs["value"] == '{"enabled":true,"user_public_ids":[]}'
        assert response.payload.enabled is True
        assert response.payload.user_public_ids == []
