"""Tests for GET /api/underlyings/{ticker}/continuous endpoint."""

from collections.abc import AsyncGenerator
from datetime import UTC
from datetime import datetime
from typing import Any
from unittest.mock import AsyncMock
from unittest.mock import patch

from fastapi import FastAPI
from fastapi.testclient import TestClient

from snapper.application.services.continuous_contract_builder import BuildResult
from snapper.application.services.continuous_contract_builder import RollPointInfo
from snapper.auth.dependencies import require_authentication
from snapper.auth.dependencies import validate_csrf_token
from snapper.auth.domain.roles import UserRole
from snapper.auth.schemas.principal import AuthPrincipal
from snapper.data.repository_types import ContinuousCandleRow
from snapper.data.repository_types import UnderlyingAssetRow
from snapper.server.app import create_app
from snapper.server.app import get_repository_dependency


async def _noop_lifespan(_app: FastAPI) -> AsyncGenerator[None]:
    """Disable application lifespan for endpoint-only tests."""
    yield


def _ts() -> datetime:
    """Return a fixed timestamp for deterministic test data."""
    return datetime(2026, 1, 1, tzinfo=UTC)


def _make_underlying(ticker: str = "SPX") -> UnderlyingAssetRow:
    """Build an UnderlyingAssetRow fixture."""
    return UnderlyingAssetRow(
        public_id="ua-1",
        ticker=ticker,
        name={"en": "S&P 500"},
        asset_class="index",
        sector=None,
        description=None,
        timestamp=_ts(),
        session_id="s1",
        sequence_id=1,
        instrument_count=2,
    )


def _make_candle(
    open_at: datetime,
    close: float = 100.0,
    source: str = "ESM6",
) -> ContinuousCandleRow:
    """Build a ContinuousCandleRow fixture."""
    return ContinuousCandleRow(
        open_at=open_at,
        timeframe="1d",
        open=close - 2.0,
        high=close + 5.0,
        low=close - 5.0,
        close=close,
        volume=1000.0,
        vwap=close,
        trades=100,
        source_contract=source,
        adjustment_factor=None,
    )


_BASE_PARAMS = (
    "?exchange=kraken_equities&contract_family=ES&timeframe=1d"
    "&start=2026-01-01T00%3A00%3A00%2B00%3A00"
    "&end=2026-01-05T00%3A00%3A00%2B00%3A00"
)


def _create_client(mock_repo: Any) -> TestClient:
    """Create test client with auth bypassed and mock repository injected."""
    app = create_app()
    app.router.lifespan_context = _noop_lifespan

    def skip_csrf() -> None:
        return None

    def skip_auth() -> AuthPrincipal:
        return AuthPrincipal(username="test_user", role=UserRole.ADMIN)

    app.dependency_overrides[validate_csrf_token] = skip_csrf
    app.dependency_overrides[require_authentication] = skip_auth
    app.dependency_overrides[get_repository_dependency] = lambda: mock_repo
    return TestClient(app)


class TestGetContinuousSeries:
    """Tests for GET /api/underlyings/{ticker}/continuous."""

    def test_success_full_series(self) -> None:
        """Given valid ticker and successful build, return 200 with candle payload.

        When: Builder produces a complete series with no failed roll,
        Then: Response type is continuous_candle_list with all candles.
        """
        d1 = datetime(2026, 1, 1, tzinfo=UTC)
        d2 = datetime(2026, 1, 2, tzinfo=UTC)
        build_result = BuildResult(
            candles=[_make_candle(d1, 100.0), _make_candle(d2, 102.0)],
            contracts_used=["ESM6", "ESU6"],
            roll_points=[],
            failed_roll=None,
        )
        repo = AsyncMock()
        repo.get_underlying_by_ticker = AsyncMock(return_value=_make_underlying())

        with patch("snapper.server.app.ContinuousContractBuilder") as mock_builder_cls:
            mock_builder = AsyncMock()
            mock_builder.build = AsyncMock(return_value=build_result)
            mock_builder_cls.return_value = mock_builder
            client = _create_client(repo)
            response = client.get(f"/api/underlyings/SPX/continuous{_BASE_PARAMS}")

        assert response.status_code == 200
        data = response.json()
        assert data["type"] == "continuous_candle_list"
        assert data["count"] == 2
        assert len(data["payload"]) == 2
        assert data["payload"][0]["source_contract"] == "ESM6"
        client.close()

    def test_partial_series_with_failed_roll(self) -> None:
        """Given build result with failed_roll, return 200 with partial response.

        When: Builder truncates at a failed roll point,
        Then: Response type is continuous_series_partial with failed_roll detail.
        """
        d1 = datetime(2026, 1, 1, tzinfo=UTC)
        failed = RollPointInfo(
            from_contract="ESM6",
            to_contract="ESU6",
            roll_at=datetime(2026, 1, 3, tzinfo=UTC),
            adjustment=None,
        )
        build_result = BuildResult(
            candles=[_make_candle(d1, 100.0)],
            contracts_used=["ESM6", "ESU6"],
            roll_points=[],
            failed_roll=failed,
        )
        repo = AsyncMock()
        repo.get_underlying_by_ticker = AsyncMock(return_value=_make_underlying())

        with patch("snapper.server.app.ContinuousContractBuilder") as mock_builder_cls:
            mock_builder = AsyncMock()
            mock_builder.build = AsyncMock(return_value=build_result)
            mock_builder_cls.return_value = mock_builder
            client = _create_client(repo)
            response = client.get(f"/api/underlyings/SPX/continuous{_BASE_PARAMS}")

        assert response.status_code == 200
        data = response.json()
        assert data["type"] == "continuous_partial"
        assert data["count"] == 1
        assert "failed_roll" in data
        assert data["failed_roll"]["from_contract"] == "ESM6"
        assert data["failed_roll"]["to_contract"] == "ESU6"
        assert "message" in data
        assert "truncated" in data["message"]
        client.close()

    def test_as_of_aware_is_normalized_to_utc_for_underlying_lookup(self) -> None:
        """Given aware as_of, convert it to UTC before repository lookup.

        When: Caller passes an aware datetime query parameter,
        Then: Repository receives the UTC-normalized timestamp.
        """
        build_result = BuildResult(
            candles=[],
            contracts_used=[],
            roll_points=[],
            failed_roll=None,
        )
        repo = AsyncMock()
        repo.get_underlying_by_ticker = AsyncMock(return_value=_make_underlying())

        with patch("snapper.server.app.ContinuousContractBuilder") as mock_builder_cls:
            mock_builder = AsyncMock()
            mock_builder.build = AsyncMock(return_value=build_result)
            mock_builder_cls.return_value = mock_builder
            client = _create_client(repo)
            response = client.get(
                f"/api/underlyings/SPX/continuous{_BASE_PARAMS}&as_of=2026-01-05T12:30:00Z"
            )

        assert response.status_code == 200
        assert repo.get_underlying_by_ticker.await_args.args == (
            "SPX",
            datetime(2026, 1, 5, 12, 30, tzinfo=UTC),
        )
        client.close()

    def test_as_of_naive_is_assumed_utc_for_underlying_lookup(self) -> None:
        """Given naive as_of, assume UTC before repository lookup.

        When: Caller passes a naive datetime query parameter,
        Then: Repository receives the same instant with UTC tzinfo attached.
        """
        build_result = BuildResult(
            candles=[],
            contracts_used=[],
            roll_points=[],
            failed_roll=None,
        )
        repo = AsyncMock()
        repo.get_underlying_by_ticker = AsyncMock(return_value=_make_underlying())

        with patch("snapper.server.app.ContinuousContractBuilder") as mock_builder_cls:
            mock_builder = AsyncMock()
            mock_builder.build = AsyncMock(return_value=build_result)
            mock_builder_cls.return_value = mock_builder
            client = _create_client(repo)
            response = client.get(
                f"/api/underlyings/SPX/continuous{_BASE_PARAMS}&as_of=2026-01-05T12:30:00"
            )

        assert response.status_code == 200
        assert repo.get_underlying_by_ticker.await_args.args == (
            "SPX",
            datetime(2026, 1, 5, 12, 30, tzinfo=UTC),
        )
        client.close()

    def test_underlying_not_found_returns_404(self) -> None:
        """Given nonexistent ticker, return 404.

        When: Repository returns None for get_underlying_by_ticker,
        Then: 404 with ticker in detail message.
        """
        repo = AsyncMock()
        repo.get_underlying_by_ticker = AsyncMock(return_value=None)
        client = _create_client(repo)
        response = client.get(f"/api/underlyings/NOPE/continuous{_BASE_PARAMS}")
        assert response.status_code == 404
        assert "NOPE" in response.json()["detail"]
        client.close()

    def test_invalid_method_returns_400(self) -> None:
        """Given invalid adjustment method, return 400.

        When: method=badmethod is passed as query parameter,
        Then: 400 with descriptive detail.
        """
        repo = AsyncMock()
        client = _create_client(repo)
        params = _BASE_PARAMS + "&method=badmethod"
        response = client.get(f"/api/underlyings/SPX/continuous{params}")
        assert response.status_code == 400
        assert "Invalid method" in response.json()["detail"]
        client.close()

    def test_value_error_returns_400(self) -> None:
        """Given builder raises ValueError, return 400.

        When: Builder raises ValueError (e.g. non-positive price for ratio),
        Then: 400 with the error message in detail.
        """
        repo = AsyncMock()
        repo.get_underlying_by_ticker = AsyncMock(return_value=_make_underlying())

        with patch("snapper.server.app.ContinuousContractBuilder") as mock_builder_cls:
            mock_builder = AsyncMock()
            mock_builder.build = AsyncMock(side_effect=ValueError("Non-positive price at roll"))
            mock_builder_cls.return_value = mock_builder
            client = _create_client(repo)
            response = client.get(f"/api/underlyings/SPX/continuous{_BASE_PARAMS}")

        assert response.status_code == 400
        assert "Non-positive" in response.json()["detail"]
        client.close()

    def test_unexpected_error_returns_500(self) -> None:
        """Given unexpected exception during build, return 500.

        When: Builder raises an unexpected RuntimeError,
        Then: 500 with generic error message.
        """
        repo = AsyncMock()
        repo.get_underlying_by_ticker = AsyncMock(return_value=_make_underlying())

        with patch("snapper.server.app.ContinuousContractBuilder") as mock_builder_cls:
            mock_builder = AsyncMock()
            mock_builder.build = AsyncMock(side_effect=RuntimeError("DB down"))
            mock_builder_cls.return_value = mock_builder
            client = _create_client(repo)
            response = client.get(f"/api/underlyings/SPX/continuous{_BASE_PARAMS}")

        assert response.status_code == 500
        assert "Failed to build continuous series" in response.json()["detail"]
        client.close()

    def test_date_range_too_large_returns_400(self) -> None:
        """Given date range exceeding 10 years, return 400.

        When: start and end span more than 3650 days,
        Then: 400 with date range error.
        """
        repo = AsyncMock()
        client = _create_client(repo)
        params = (
            "?exchange=kraken_equities&contract_family=ES&timeframe=1d"
            "&start=2010-01-01T00:00:00Z&end=2026-01-01T00:00:00Z"
            "&method=panama"
        )
        response = client.get(f"/api/underlyings/SPX/continuous{params}")
        assert response.status_code == 400
        assert "Date range too large" in response.json()["detail"]
        client.close()

    def test_rejects_negative_rollover_days(self) -> None:
        """R2: rollover_days_before < 0 must be rejected with HTTP 422.

        When: caller passes rollover_days_before=-1,
        Then: FastAPI Query(ge=0) fails validation before the handler runs.
        """
        repo = AsyncMock()
        client = _create_client(repo)
        response = client.get(
            f"/api/underlyings/SPX/continuous{_BASE_PARAMS}&rollover_days_before=-1"
        )
        assert response.status_code == 422
        client.close()

    def test_rejects_excessive_rollover_days(self) -> None:
        """R2: rollover_days_before > 365 must be rejected with HTTP 422.

        When: caller passes rollover_days_before=366,
        Then: FastAPI Query(le=365) fails validation before the handler runs.
        """
        repo = AsyncMock()
        client = _create_client(repo)
        response = client.get(
            f"/api/underlyings/SPX/continuous{_BASE_PARAMS}&rollover_days_before=366"
        )
        assert response.status_code == 422
        client.close()

    def test_returns_200_empty_for_unknown_contract_family(self) -> None:
        """R4: valid underlying + empty builder result returns 200 with empty payload.

        Pins the contract that the endpoint never 404s on "no contracts in
        range" — only on "underlying not found". The OpenAPI description
        was tightened to match (dropping the "or no contracts" clause),
        so this test guards against future refactors that accidentally
        introduce a 404 for empty-series cases.
        """
        build_result = BuildResult(
            candles=[],
            contracts_used=[],
            roll_points=[],
            failed_roll=None,
        )
        repo = AsyncMock()
        repo.get_underlying_by_ticker = AsyncMock(return_value=_make_underlying())

        with patch("snapper.server.app.ContinuousContractBuilder") as mock_builder_cls:
            mock_builder = AsyncMock()
            mock_builder.build = AsyncMock(return_value=build_result)
            mock_builder_cls.return_value = mock_builder
            client = _create_client(repo)
            response = client.get(f"/api/underlyings/SPX/continuous{_BASE_PARAMS}")

        assert response.status_code == 200
        data = response.json()
        assert data["type"] == "continuous_candle_list"
        assert data["count"] == 0
        assert data["payload"] == []
        client.close()
