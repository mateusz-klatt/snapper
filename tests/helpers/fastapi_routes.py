"""FastAPI route-introspection helpers for tests.

FastAPI 0.137 keeps included routers as route-table nodes and expands their
effective routes lazily. These helpers expose the effective ``APIRoute``-like
view that route-registration and CSRF meta-tests need without depending on
whether the app flattened routers eagerly.
"""

from collections.abc import Iterable
from collections.abc import Iterator
from typing import Protocol
from typing import cast

from fastapi import FastAPI
from fastapi.dependencies.models import Dependant
from fastapi.routing import APIRoute


class FastAPIRouteView(Protocol):
    """Route-like view carrying the fields used by test introspection."""

    path: str
    methods: set[str] | None
    dependant: Dependant | None


class _EffectiveCandidateProvider(Protocol):
    """Route-table node that can expose lazily expanded FastAPI routes."""

    def effective_candidates(self) -> Iterable[object]:
        """Return effective route contexts for an included router."""


def iter_fastapi_routes(app: FastAPI) -> Iterator[FastAPIRouteView]:
    """Yield all effective FastAPI HTTP routes registered on ``app``.

    Args:
        app: FastAPI application whose routes should be inspected.

    Yields:
        Route-like objects with effective prefixed paths, methods, and
        dependency graphs. Mounted sub-apps and static routes are skipped.
    """
    for route in app.routes:
        yield from _iter_route_node(route)


def iter_fastapi_route_paths(app: FastAPI) -> set[str]:
    """Return effective FastAPI HTTP route paths registered on ``app``.

    Args:
        app: FastAPI application whose routes should be inspected.

    Returns:
        Effective route paths, including prefixes from ``include_router``.
    """
    return {route.path for route in iter_fastapi_routes(app)}


def _iter_route_node(route: object) -> Iterator[FastAPIRouteView]:
    """Yield effective FastAPI HTTP routes under a route-table node.

    Args:
        route: A direct route-table entry or a FastAPI effective route context.

    Yields:
        FastAPI route-like views for HTTP routes.
    """
    if isinstance(route, APIRoute):
        yield route
        return
    if hasattr(route, "effective_candidates"):
        provider = cast(_EffectiveCandidateProvider, route)
        for candidate in provider.effective_candidates():
            yield from _iter_route_node(candidate)
        return
    if isinstance(getattr(route, "dependant", None), Dependant):
        yield cast(FastAPIRouteView, route)
