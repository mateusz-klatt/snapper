"""Tests for database API endpoints."""

from unittest.mock import AsyncMock

from fastapi.testclient import TestClient

from snapper.auth.dependencies import require_authentication
from snapper.auth.dependencies import validate_csrf_token
from snapper.auth.domain.roles import UserRole
from snapper.auth.schemas.user import UserProfile
from snapper.data.repository import Repository
from snapper.server.app import create_app
from snapper.server.app import get_repository_dependency


class TestDatabaseEndpoints:
    """Tests for database API endpoints (orders, signals, executions)."""

    def setup_method(self) -> None:
        """Initialize test client with mocked auth dependencies."""
        self.app = create_app()

        def skip_csrf_validation() -> None:
            return None

        def skip_authentication() -> UserProfile:
            return UserProfile(id="test_id", username="test_user", role=UserRole.ADMIN)

        self.app.dependency_overrides[validate_csrf_token] = skip_csrf_validation
        self.app.dependency_overrides[require_authentication] = skip_authentication
        self.client = TestClient(self.app)

    def teardown_method(self) -> None:
        """Clear dependency overrides after each test."""
        self.app.dependency_overrides.clear()

    def test_get_orders_endpoint_basic(self) -> None:
        """Verify GET /orders endpoint returns 200 with list.

        Given: An authenticated client with CSRF bypassed,
        When: GET /snapper/api/orders is called,
        Then: Response is 200 OK with a list body.
        """
        response = self.client.get("/snapper/api/orders")
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)

    def test_get_orders_with_parameters(self) -> None:
        """Verify GET /orders accepts query parameters.

        Given: An authenticated client,
        When: GET /orders is called with symbol, limit, and offset,
        Then: Response is 200 OK with filtered results.
        """
        response = self.client.get("/snapper/api/orders?symbol=BTCUSD&limit=50&offset=10")
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)

    def test_get_signals_endpoint_basic(self) -> None:
        """Verify GET /signals endpoint returns 200 with list.

        Given: An authenticated client,
        When: GET /snapper/api/signals is called,
        Then: Response is 200 OK with a list body.
        """
        response = self.client.get("/snapper/api/signals")
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)

    def test_get_signals_with_parameters(self) -> None:
        """Verify GET /signals accepts query parameters.

        Given: An authenticated client,
        When: GET /signals is called with filtering parameters,
        Then: Response is 200 OK with filtered results.
        """
        response = self.client.get(
            "/snapper/api/signals?instrument=BTCUSD&strategy=test_strat&hours=48&limit=50"
        )
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)

    def test_get_executions_endpoint_basic(self) -> None:
        """Verify GET /executions endpoint returns 200 with list.

        Given: An authenticated client,
        When: GET /snapper/api/executions is called,
        Then: Response is 200 OK with a list body.
        """
        response = self.client.get("/snapper/api/executions")
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)

    def test_get_executions_with_parameters(self) -> None:
        """Verify GET /executions accepts limit parameter.

        Given: An authenticated client,
        When: GET /executions is called with limit parameter,
        Then: Response is 200 OK with limited results.
        """
        response = self.client.get("/snapper/api/executions?limit=25")
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)

    def test_get_positions_endpoint_basic(self) -> None:
        """Verify GET /positions endpoint returns 200 with list.

        Given: An authenticated client,
        When: GET /snapper/api/positions is called,
        Then: Response is 200 OK with a list body.
        """
        response = self.client.get("/snapper/api/positions")
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)

    def test_dependency_injection_is_used(self) -> None:
        """Verify endpoints use injected repository dependency.

        Given: A mock repository that raises an exception,
        When: Any database endpoint is called,
        Then: Response is 500 with error detail.
        """
        mock_repository = AsyncMock(spec=Repository)
        mock_repository.session.return_value.__aenter__.side_effect = Exception(
            "Mock repository error"
        )
        self.app.dependency_overrides[get_repository_dependency] = lambda: mock_repository
        endpoints = [
            "/snapper/api/orders",
            "/snapper/api/signals",
            "/snapper/api/executions",
            "/snapper/api/positions",
        ]
        for endpoint in endpoints:
            response = self.client.get(endpoint)
            assert response.status_code == 500
            assert "detail" in response.json()

    def test_database_connection_errors(self) -> None:
        """Verify database errors return proper error messages.

        Given: A mock repository that raises database connection error,
        When: Database endpoints are called,
        Then: Response is 500 with specific error messages.
        """
        mock_repository = AsyncMock(spec=Repository)
        mock_session = AsyncMock()
        mock_repository.session.return_value.__aenter__.return_value = mock_session
        mock_repository.session.return_value.__aexit__.return_value = None
        mock_session.execute.side_effect = Exception("Database connection failed")
        self.app.dependency_overrides[get_repository_dependency] = lambda: mock_repository
        endpoint_messages = {
            "/snapper/api/orders": "Failed to fetch orders",
            "/snapper/api/signals": "Failed to fetch signals",
            "/snapper/api/executions": "Failed to fetch executions",
            "/snapper/api/positions": "Failed to fetch positions",
        }
        for endpoint, expected_message in endpoint_messages.items():
            response = self.client.get(endpoint)
            assert response.status_code == 500
            assert response.json()["detail"] == expected_message

    def test_parameter_validation(self) -> None:
        """Verify query parameter validation returns 422.

        Given: An authenticated client,
        When: Invalid parameters are provided to endpoints,
        Then: Response is 422 Unprocessable Entity.
        """
        response = self.client.get("/snapper/api/orders?limit=0")
        assert response.status_code == 422
        response = self.client.get("/snapper/api/orders?limit=2000")
        assert response.status_code == 422
        response = self.client.get("/snapper/api/orders?offset=-1")
        assert response.status_code == 422
