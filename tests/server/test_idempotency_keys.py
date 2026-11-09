"""Order entry points reject keys that exceed the shared storage width."""

from datetime import UTC
from datetime import datetime
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from pydantic import BaseModel
from pydantic import ValidationError

from snapper.api.schemas.brackets import BracketCreateBody
from snapper.api.schemas.orders import CreateOrderBody
from snapper.api.schemas.trailing_stops import TrailingStopCreateBody
from snapper.auth.dependencies import require_authentication
from snapper.auth.dependencies import validate_csrf_token
from snapper.auth.domain.roles import UserRole
from snapper.auth.schemas.principal import AuthPrincipal
from snapper.core.idempotency import IDEMPOTENCY_KEY_MAX_LENGTH
from snapper.core.json_types import JsonObject
from snapper.server.dependencies import get_caps_enforcer_dependency
from snapper.server.dependencies import get_repository_dependency
from snapper.server.execution_plan_routes import router as bracket_router
from snapper.server.order_routes import router as order_router
from snapper.server.trailing_stop_routes import router as trailing_router

_CREATE_ORDER: JsonObject = {
    "instrument": "BTC-USD",
    "instrument_public_id": "instrument-1",
    "exchange": "kraken",
    "side": "buy",
    "order_type": "limit",
    "quantity": 1,
    "price": 2,
}
_CREATE_BRACKET: JsonObject = {"position_cycle_public_id": "cycle-1", "sl_price": 1}
_CREATE_TRAILING: JsonObject = {"position_cycle_public_id": "cycle-1", "trailing_pct": 2}


@pytest.mark.parametrize(
    "model,payload",
    [
        (CreateOrderBody, _CREATE_ORDER),
        (BracketCreateBody, _CREATE_BRACKET),
        (TrailingStopCreateBody, _CREATE_TRAILING),
    ],
)
@pytest.mark.parametrize("key", [None, "", "x" * IDEMPOTENCY_KEY_MAX_LENGTH])
def test_create_body_accepts_idempotency_key_limit(
    model: type[BaseModel], payload: JsonObject, key: str | None
) -> None:
    """Accept optional keys and preserve every character at the storage boundary.

    Given: a supported create body with an omitted, empty, or 64-character key.
    When: the request schema validates its payload.
    Then: the key retains its exact value.
    """
    body = model.model_validate({**payload, "idempotency_key": key})
    assert body.model_dump()["idempotency_key"] == key


@pytest.mark.parametrize(
    "model,payload",
    [
        (CreateOrderBody, _CREATE_ORDER),
        (BracketCreateBody, _CREATE_BRACKET),
        (TrailingStopCreateBody, _CREATE_TRAILING),
    ],
)
def test_create_body_rejects_idempotency_key_above_limit(
    model: type[BaseModel], payload: JsonObject
) -> None:
    """Reject keys above the shared maximum in each create-body schema.

    Given: a supported create body with a 65-character key.
    When: the request schema validates its payload.
    Then: validation identifies the offending key.
    """
    with pytest.raises(ValidationError, match="idempotency_key"):
        model.model_validate({**payload, "idempotency_key": "x" * 65})


@pytest.mark.parametrize(
    "route,command_type,payload",
    [
        ("/api/orders", "create_order_command", _CREATE_ORDER),
        ("/api/execution-plans", "create_bracket_command", _CREATE_BRACKET),
        ("/api/trailing-stops", "create_trailing_stop_command", _CREATE_TRAILING),
    ],
)
@pytest.mark.parametrize("key", ["x" * 65, "ą" * 65])
def test_oversized_http_idempotency_key_has_no_repository_effects(
    route: str, command_type: str, payload: JsonObject, key: str
) -> None:
    """Return 422 before reading or persisting an oversized create key.

    Given: authenticated create requests whose key exceeds 64 characters.
    When: each HTTP entry point validates its request.
    Then: HTTP 422 identifies idempotency_key and no repository method runs.
    """
    repo = AsyncMock()
    app = FastAPI()
    app.include_router(order_router, prefix="/api")
    app.include_router(bracket_router, prefix="/api")
    app.include_router(trailing_router, prefix="/api")
    app.dependency_overrides[require_authentication] = lambda: AuthPrincipal(
        username="test_user", role=UserRole.ADMIN
    )
    app.dependency_overrides[validate_csrf_token] = lambda: None
    app.dependency_overrides[get_repository_dependency] = lambda: repo
    app.dependency_overrides[get_caps_enforcer_dependency] = MagicMock
    request: JsonObject = {
        "type": command_type,
        "session_id": "session-1",
        "sequence_id": 1,
        "public_id": "request-1",
        "timestamp": datetime.now(UTC).isoformat(),
        "payload": {**payload, "idempotency_key": key},
    }
    with TestClient(app) as client:
        response = client.post(route, json=request)
    assert response.status_code == 422
    assert response.json()["detail"][0]["loc"] == ["body", "payload", "idempotency_key"]
    assert repo.mock_calls == []
