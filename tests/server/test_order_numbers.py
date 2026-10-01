"""REST rejection of invalid manual-order amounts before database activity."""

import json
from unittest.mock import AsyncMock

import pytest

from snapper.core.json_types import JsonValue
from tests.server.test_order_routes import _create_client
from tests.server.test_order_routes import _create_order_body


@pytest.mark.parametrize(
    "field,value",
    [
        ("quantity", float("inf")),
        ("quantity", float("nan")),
        ("quantity", 0),
        ("quantity", True),
        ("quantity", 10**1000),
        ("price", -1),
        ("price", float("nan")),
        ("stop_price", -float("inf")),
        ("leverage", True),
        ("leverage", 1.0),
        ("leverage", 1.5),
        ("leverage", 0),
        ("leverage", -1),
        ("leverage", 2147483648),
    ],
)
def test_rest_invalid_numbers_before_repository(field: str, value: JsonValue) -> None:
    """Return a JSON 422 without repository lookups or writes.

    Given: a REST order body containing an unsafe amount or leverage.
    When: the client posts the body to the order endpoint.
    Then: a JSON 422 response is returned without any repository calls.
    """
    repo = AsyncMock()
    body = _create_order_body()
    body["payload"][field] = value
    client = _create_client(repo)
    try:
        response = client.post(
            "/api/orders", content=json.dumps(body), headers={"content-type": "application/json"}
        )
    finally:
        client.close()
    assert response.status_code == 422
    assert response.json()["detail"]
    assert repo.mock_calls == []
