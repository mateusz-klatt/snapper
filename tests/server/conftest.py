"""Shared fixtures for ``tests/server``.

The order-entry capability guard introduced for TradFi P3 Day 3
(``snapper.server._capability_guard.require_tradable``) fails closed
for instruments without a ``SymbolExchangeCapability(can_trade=True)``
row. Existing route tests use placeholder instrument strings such as
``"inst-1"`` that are not seeded in the test symbol mapper cache, so
without a bypass they would all return HTTP 422.

The ``bypass_capability_guard`` autouse fixture makes the guard a
no-op for every test in this package. Tests that need to exercise
the guard (new Day-3 regression tests) mark themselves with
``@pytest.mark.capability_guard`` so the bypass yields without
patching.
"""

from collections.abc import Generator
from unittest.mock import AsyncMock
from unittest.mock import patch

import pytest

_GUARD_IMPORT_PATHS: tuple[str, ...] = (
    "snapper.server.order_routes.require_tradable",
    "snapper.server.execution_plan_routes.require_tradable",
    "snapper.server.trailing_stop_routes.require_tradable",
)


@pytest.fixture(autouse=True)
def bypass_capability_guard(request: pytest.FixtureRequest) -> Generator[None]:
    """Replace ``require_tradable`` in each submit-route module with a no-op.

    Scope: function. Autouse across ``tests/server``. Tests that need to
    exercise the guard mark themselves with ``@pytest.mark.capability_guard``
    and the fixture becomes a pass-through.
    """
    if "capability_guard" in request.keywords:
        yield
        return
    with (
        patch(_GUARD_IMPORT_PATHS[0], new_callable=AsyncMock),
        patch(_GUARD_IMPORT_PATHS[1], new_callable=AsyncMock),
        patch(_GUARD_IMPORT_PATHS[2], new_callable=AsyncMock),
    ):
        yield
