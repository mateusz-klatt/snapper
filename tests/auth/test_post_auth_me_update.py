"""Tests for ``POST /api/auth/me/update`` — caller-scoped preferences endpoint.

Covers:

- 401 when unauthenticated (the same gate every other ``/auth/me``-style
  endpoint shares).
- 422 when ``default_language`` is not in
  :data:`snapper.i18n.supported_languages.SUPPORTED_LANGUAGES`.
- 200 happy path returns the updated profile with the new
  ``default_language`` field populated.
- 200 with ``default_language=None`` clears the preference (alert
  pipeline reverts to English emission for the user).
- Service-layer 404 fallthrough when the row vanishes between auth
  resolution and the SCD2 close+insert.
"""

from collections.abc import Generator
from datetime import UTC
from datetime import datetime
from unittest.mock import AsyncMock
from unittest.mock import patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from snapper.auth.dependencies import require_authentication
from snapper.auth.dependencies import validate_csrf_token
from snapper.auth.domain.roles import UserRole
from snapper.auth.routes import router
from snapper.auth.schemas.principal import AuthPrincipal
from snapper.auth.schemas.user import UserProfile
from snapper.messaging.infrastructure.publisher import SequenceTracker

_PRINCIPAL = AuthPrincipal(
    username="testuser",
    role=UserRole.VIEWER,
    is_active=True,
)


def _build_request_envelope(default_language: str | None) -> dict[str, object]:
    """Construct an envelope-shaped POST body matching ``UpdateAuthMeRequest``."""
    return {
        "type": "update_auth_me_request",
        "public_id": "01910000-0000-7000-8000-000000000001",
        "session_id": "01910000-0000-7000-8000-000000000002",
        "sequence_id": 1,
        "timestamp": "2026-05-17T00:00:00Z",
        "payload": {"default_language": default_language},
    }


def _make_authenticated_app(mock_service: AsyncMock) -> FastAPI:
    """Build a FastAPI app with the auth + CSRF guards overridden to the test principal.

    Mirrors the pattern used by the admin-update tests where the route
    must run end-to-end (validation + service dispatch + response shape)
    rather than calling the handler function directly.
    """
    app = FastAPI()
    app.state.rest_tracker = SequenceTracker()
    app.include_router(router)
    app.dependency_overrides[require_authentication] = lambda: _PRINCIPAL
    app.dependency_overrides[validate_csrf_token] = lambda: None
    app.user_service_patch = patch(
        "snapper.auth.routes.get_user_service", return_value=mock_service
    )
    app.user_service_patch.start()
    return app


@pytest.fixture
def updated_profile() -> UserProfile:
    """Default service-layer return value used by happy-path assertions."""
    return UserProfile(
        session_id="test-sid",
        sequence_id=1,
        public_id="test-pid",
        timestamp=datetime(2026, 5, 17, tzinfo=UTC),
        username="testuser",
        role=UserRole.VIEWER,
        is_active=True,
        created_at=datetime(2026, 1, 1, tzinfo=UTC),
        default_language="pl",
    )


@pytest.fixture
def authenticated_client(
    updated_profile: UserProfile,
) -> Generator[tuple[TestClient, AsyncMock]]:
    """Yield a TestClient with auth + CSRF + user_service mocked.

    The default mock returns ``updated_profile``; tests can override via
    ``mock_service.update_self_preferences.return_value``.
    """
    mock_service = AsyncMock()
    mock_service.update_self_preferences = AsyncMock(return_value=updated_profile)
    app = _make_authenticated_app(mock_service)
    client = TestClient(app)
    try:
        yield client, mock_service
    finally:
        app.user_service_patch.stop()
        client.close()


def test_unauthenticated_caller_is_rejected() -> None:
    """Unauthenticated POST returns 401.

    Given: A TestClient with no auth dependencies overridden.
    When: A POST to ``/auth/me/update`` carrying a valid envelope fires.
    Then: The response is 401 — the same gate every self-service
        endpoint shares.
    """
    app = FastAPI()
    app.state.rest_tracker = SequenceTracker()
    app.include_router(router)
    client = TestClient(app)
    try:
        response = client.post("/auth/me/update", json=_build_request_envelope("pl"))
        assert response.status_code == 401
    finally:
        client.close()


def test_unknown_language_code_returns_422(
    authenticated_client: tuple[TestClient, AsyncMock],
) -> None:
    """Validation rejects codes not in SUPPORTED_LANGUAGES.

    Given: An authenticated TestClient + a request body with an unknown
        ``default_language`` (``"xyz"``).
    When: POST to ``/auth/me/update`` fires.
    Then: The response is 422 and the service layer is never called —
        the schema-level field validator catches typos and drift.
    """
    client, mock_service = authenticated_client
    response = client.post("/auth/me/update", json=_build_request_envelope("xyz"))
    assert response.status_code == 422
    mock_service.update_self_preferences.assert_not_called()


def test_happy_path_returns_updated_profile(
    authenticated_client: tuple[TestClient, AsyncMock],
) -> None:
    """200 round-trip preserves the new ``default_language``.

    Given: An authenticated TestClient + service mock returning a
        profile with ``default_language="pl"``.
    When: POST to ``/auth/me/update`` with ``"pl"`` payload fires.
    Then: The response is 200, the body's payload carries ``"pl"``, and
        the service was awaited with the resolved username.
    """
    client, mock_service = authenticated_client
    response = client.post("/auth/me/update", json=_build_request_envelope("pl"))
    assert response.status_code == 200
    body = response.json()
    assert body["payload"]["default_language"] == "pl"
    mock_service.update_self_preferences.assert_awaited_once_with(
        user_id="testuser", default_language="pl"
    )


def test_null_clears_preference(
    authenticated_client: tuple[TestClient, AsyncMock],
    updated_profile: UserProfile,
) -> None:
    """Passing ``default_language=None`` is a valid clear operation.

    Given: A user with a previously-set ``default_language``.
    When: POST to ``/auth/me/update`` with payload ``{"default_language": null}``.
    Then: The response is 200 with ``default_language=null`` — the
        alert pipeline reverts to English emission for that user.
    """
    client, mock_service = authenticated_client
    mock_service.update_self_preferences.return_value = updated_profile.model_copy(
        update={"default_language": None}
    )
    response = client.post("/auth/me/update", json=_build_request_envelope(None))
    assert response.status_code == 200
    assert response.json()["payload"]["default_language"] is None
    mock_service.update_self_preferences.assert_awaited_once_with(
        user_id="testuser", default_language=None
    )


def test_service_returns_none_yields_404(
    authenticated_client: tuple[TestClient, AsyncMock],
) -> None:
    """Service returning ``None`` surfaces as 404 to the caller.

    Given: An authenticated TestClient + service mock returning ``None``
        (the SCD2 close+insert found no active row).
    When: POST to ``/auth/me/update`` fires.
    Then: The response is 404 instead of silently returning an empty
        body — the only realistic trigger is a concurrent admin
        deactivation between auth-dep resolution and update.
    """
    client, mock_service = authenticated_client
    mock_service.update_self_preferences.return_value = None
    response = client.post("/auth/me/update", json=_build_request_envelope("pl"))
    assert response.status_code == 404
