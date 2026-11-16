"""HTTP witnesses for the bounded read-only market-snapshot freshness route."""

from datetime import UTC
from datetime import datetime
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from snapper.api.schemas.market_snapshot_freshness import MarketSnapshotFreshnessResponse
from snapper.application.market_snapshot_refresh.freshness import MAX_REQUESTED_INSTRUMENTS
from snapper.auth.dependencies import require_authentication
from snapper.auth.domain.permissions import Permission
from snapper.auth.domain.roles import UserRole
from snapper.auth.schemas.principal import AuthPrincipal
from snapper.data.repository import Repository
from snapper.data.repository_types import MarketSnapshotFreshnessRow
from snapper.data.repository_types import MarketSnapshotRefreshTargetRow
from snapper.messaging.infrastructure.publisher import SequenceTracker
from snapper.server.dependencies import get_repository_dependency
from snapper.server.metrics_routes import router

_PATH = "/api/metrics/market-snapshots/freshness"


def _repository() -> MagicMock:
    """Return a typed-boundary mock with one main instrument and one FX candidate.

    Returns:
        Repository mock whose async methods have independent await counters.
    """
    targets: list[MarketSnapshotRefreshTargetRow] = [
        {
            "instrument_public_id": "main",
            "native_symbol": "BTC/EUR",
            "exchange": "kraken",
            "base": "BTC",
            "quote": "EUR",
            "asset_type": "crypto",
        },
        {
            "instrument_public_id": "fx",
            "native_symbol": "EUR/USD",
            "exchange": "kraken",
            "base": "EUR",
            "quote": "USD",
            "asset_type": "forex",
        },
    ]
    timestamp = datetime.now(UTC)
    rows: list[MarketSnapshotFreshnessRow] = [
        {"instrument_public_id": "main", "timestamp": timestamp, "last_price": 100.0},
        {"instrument_public_id": "fx", "timestamp": timestamp, "last_price": 1.2},
    ]
    repository = MagicMock(spec=Repository)
    repository.resolve_market_snapshot_refresh_targets = AsyncMock(return_value=targets)
    repository.get_active_market_snapshot_freshness = AsyncMock(return_value=rows)
    return repository


def _principal(role: UserRole = UserRole.AUDITOR) -> AuthPrincipal:
    """Build a narrowly scoped read-only principal for the metrics route.

    Args:
        role: Role carrying the explicit system-status permission.

    Returns:
        Authenticated principal with one current permission grant.
    """
    return AuthPrincipal(
        username="freshness-reader",
        user_public_id="reader",
        role=role,
        permissions=[Permission.READ_SYSTEM_STATUS.value],
        permission_scope_version=4,
    )


def _app(repository: MagicMock, principal: AuthPrincipal | None) -> FastAPI:
    """Build a minimal app using the actual metrics router and permission dependency.

    Args:
        repository: Controlled repository read boundary.
        principal: Authenticated principal, or null to exercise missing auth.

    Returns:
        App without any production lifespan or background process.
    """
    app = FastAPI()
    app.include_router(router, prefix="/api")
    app.state.rest_tracker = SequenceTracker()
    app.dependency_overrides[get_repository_dependency] = lambda: repository
    if principal is not None:
        app.dependency_overrides[require_authentication] = lambda: principal
    return app


@pytest.mark.parametrize("role", [UserRole.VIEWER, UserRole.AUDITOR])
def test_read_permission_returns_expanded_scope_without_csrf(role: UserRole) -> None:
    """Return explicit dependency evidence to an authorized read-only principal.

    Given: A system-status reader and one EUR-quoted requested instrument.
    When: The reader issues GET without any mutation or CSRF token.
    Then: Both the instrument and FX dependency appear in a valid envelope.
    """
    repository = _repository()
    client = TestClient(_app(repository, _principal(role)))

    response = client.get(
        _PATH, params=[("instrument_public_ids", "main"), ("fx_exchanges", "kraken")]
    )

    assert response.status_code == 200
    result = MarketSnapshotFreshnessResponse.model_validate_json(response.content)
    assert result.payload.freshness_ready
    assert result.payload.requested_instrument_public_ids == ["main"]
    assert result.payload.configured_fx_exchanges == ["kraken"]
    assert result.payload.excluded_fx_instruments == []
    assert len(result.payload.scope_digest) == 64
    assert [item.instrument_public_id for item in result.payload.instruments] == ["fx", "main"]
    assert result.payload.unresolved_fx_currencies == []
    assert result.public_id != result.payload.public_id
    repository.resolve_market_snapshot_refresh_targets.assert_awaited_once_with(["main"])
    repository.get_active_market_snapshot_freshness.assert_awaited_once_with(["fx", "main"])


def test_permission_denial_occurs_before_snapshot_reads() -> None:
    """Reject an authenticated principal whose grant excludes system status.

    Given: An auditor token with an explicitly empty permission grant.
    When: It requests freshness for an otherwise valid explicit scope.
    Then: Authorization returns 403 before either repository read executes.
    """
    repository = _repository()
    principal = _principal().model_copy(update={"permissions": []})
    client = TestClient(_app(repository, principal))

    response = client.get(_PATH, params={"instrument_public_ids": "main", "fx_exchanges": "kraken"})

    assert response.status_code == 403
    repository.resolve_market_snapshot_refresh_targets.assert_not_awaited()
    repository.get_active_market_snapshot_freshness.assert_not_awaited()


def test_missing_authentication_refuses_before_snapshot_reads() -> None:
    """Reject unauthenticated requests through the real authentication dependency.

    Given: A request with neither an access cookie nor a bearer token.
    When: It requests freshness for a concrete instrument identity.
    Then: The route returns 401 without either repository query.
    """
    repository = _repository()
    client = TestClient(_app(repository, None))

    response = client.get(_PATH, params={"instrument_public_ids": "main", "fx_exchanges": "kraken"})

    assert response.status_code == 401
    repository.resolve_market_snapshot_refresh_targets.assert_not_awaited()
    repository.get_active_market_snapshot_freshness.assert_not_awaited()


@pytest.mark.parametrize("supplied", [False, True])
def test_missing_or_blank_scope_never_becomes_all_instruments(supplied: bool) -> None:
    """Refuse missing and blank explicit scope before touching the repository.

    Given: An authorized request whose instrument list is omitted or blank.
    When: The GET parameters are validated by the route and policy.
    Then: The request fails with 422 or 400, with neither repository read.
    """
    repository = _repository()
    client = TestClient(_app(repository, _principal()))

    params = {"fx_exchanges": "kraken"}
    if supplied:
        params["instrument_public_ids"] = ""
    response = client.get(_PATH, params=params)

    assert response.status_code == (400 if supplied else 422)
    repository.resolve_market_snapshot_refresh_targets.assert_not_awaited()
    repository.get_active_market_snapshot_freshness.assert_not_awaited()


def test_oversized_scope_is_rejected_by_query_validation() -> None:
    """Bound the raw repeated-query list before metadata expansion begins.

    Given: More instrument parameters than the explicit-scope request ceiling.
    When: An authorized caller submits the GET request.
    Then: Validation returns 422 without a metadata or snapshot query.
    """
    repository = _repository()
    client = TestClient(_app(repository, _principal()))
    params = [("instrument_public_ids", "main")] * (MAX_REQUESTED_INSTRUMENTS + 1)
    params.append(("fx_exchanges", "kraken"))

    response = client.get(_PATH, params=params)

    assert response.status_code == 422
    repository.resolve_market_snapshot_refresh_targets.assert_not_awaited()
    repository.get_active_market_snapshot_freshness.assert_not_awaited()


def test_unknown_requested_id_returns_400_before_snapshot_query() -> None:
    """Do not omit an unresolved requested identity from an apparently ready scope.

    Given: Metadata resolution does not contain one caller-requested identity.
    When: An authorized caller asks for that unknown instrument.
    Then: The route reports 400 and never performs its snapshot read.
    """
    repository = _repository()
    client = TestClient(_app(repository, _principal()))

    response = client.get(
        _PATH, params={"instrument_public_ids": "unknown", "fx_exchanges": "kraken"}
    )

    assert response.status_code == 400
    repository.get_active_market_snapshot_freshness.assert_not_awaited()


def test_fresh_main_with_absent_fx_snapshot_returns_not_ready() -> None:
    """Retain absent FX evidence in a successful diagnostic response.

    Given: A fresh main instrument and a resolved FX dependency without a row.
    When: The reader asks for freshness of the main instrument.
    Then: The response includes missing FX evidence and freshness_ready false.
    """
    repository = _repository()
    rows: list[MarketSnapshotFreshnessRow] = [
        {"instrument_public_id": "main", "timestamp": datetime.now(UTC), "last_price": 100.0}
    ]
    repository.get_active_market_snapshot_freshness.return_value = rows
    client = TestClient(_app(repository, _principal()))

    response = client.get(_PATH, params={"instrument_public_ids": "main", "fx_exchanges": "kraken"})

    assert response.status_code == 200
    result = MarketSnapshotFreshnessResponse.model_validate_json(response.content)
    assert not result.payload.freshness_ready
    assert result.payload.instruments[0].instrument_public_id == "fx"
    assert result.payload.instruments[0].reason == "missing_snapshot"


@pytest.mark.parametrize("fx_exchange", [None, "", " ", "kraken ", "Kraken", "polygon"])
def test_missing_or_invalid_fx_configuration_never_queries(fx_exchange: str | None) -> None:
    """Require explicit supported FX configuration in addition to the main scope.

    Given: A valid requested main instrument but absent or invalid FX venue parameters.
    When: The authorized caller requests the freshness endpoint.
    Then: It returns 422 or 400 before metadata or snapshot reads can start.
    """
    repository = _repository()
    client = TestClient(_app(repository, _principal()))
    params = {"instrument_public_ids": "main"}
    if fx_exchange is not None:
        params["fx_exchanges"] = fx_exchange

    response = client.get(_PATH, params=params)

    assert response.status_code == (422 if fx_exchange is None else 400)
    repository.resolve_market_snapshot_refresh_targets.assert_not_awaited()
    repository.get_active_market_snapshot_freshness.assert_not_awaited()


def test_oversized_fx_parameter_list_never_queries() -> None:
    """Bound raw repeated FX parameters before route-level policy evaluation.

    Given: An otherwise valid request with too many repeated FX exchange parameters.
    When: FastAPI validates the explicit query lists.
    Then: It returns 422 without performing either bounded repository operation.
    """
    repository = _repository()
    client = TestClient(_app(repository, _principal()))
    params = [("instrument_public_ids", "main")]
    params.extend([("fx_exchanges", "kraken")] * (MAX_REQUESTED_INSTRUMENTS + 1))

    response = client.get(_PATH, params=params)

    assert response.status_code == 422
    repository.resolve_market_snapshot_refresh_targets.assert_not_awaited()
    repository.get_active_market_snapshot_freshness.assert_not_awaited()


def test_response_audits_excluded_fx_metadata_without_reading_its_price() -> None:
    """Expose unselected Polygon FX metadata while reading only configured required rows.

    Given: Fresh Kraken main and FX prices plus an eligible Polygon metadata candidate.
    When: The reader explicitly selects Kraken FX twice in repeated parameters.
    Then: Configuration is deduplicated and Polygon appears only in the exclusion audit.
    """
    repository = _repository()
    targets: list[MarketSnapshotRefreshTargetRow] = (
        repository.resolve_market_snapshot_refresh_targets.return_value
    )
    targets.append(
        {
            "instrument_public_id": "polygon-fx",
            "native_symbol": "C:EURUSD",
            "exchange": "polygon",
            "base": "EUR",
            "quote": "USD",
            "asset_type": "forex",
        }
    )
    client = TestClient(_app(repository, _principal()))

    response = client.get(
        _PATH,
        params=[
            ("instrument_public_ids", "main"),
            ("fx_exchanges", "kraken"),
            ("fx_exchanges", "kraken"),
        ],
    )

    assert response.status_code == 200
    result = MarketSnapshotFreshnessResponse.model_validate_json(response.content)
    assert result.payload.freshness_ready
    assert result.payload.configured_fx_exchanges == ["kraken"]
    excluded = result.payload.excluded_fx_instruments[0]
    assert excluded.instrument_public_id == "polygon-fx"
    assert excluded.native_symbol == "C:EURUSD"
    assert (excluded.exchange, excluded.base, excluded.quote) == ("polygon", "EUR", "USD")
    assert excluded.reason == "fx_venue_not_selected"
    repository.get_active_market_snapshot_freshness.assert_awaited_once_with(["fx", "main"])
