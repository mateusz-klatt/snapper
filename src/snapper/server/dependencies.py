"""Shared FastAPI dependencies for the REST API layer.

Keeps small dependency callables out of the main ``server/app.py`` so
new route modules (Phase 0d wallet / operator / scope grant routes,
future Manual Orders routes) can import them without pulling the
full application factory and introducing a circular import.

Currently only ``get_repository_dependency`` lives here. Other
cross-cutting dependencies should be added on demand as new routers
need them — avoid introducing catch-all helpers that routers do not
actually consume.
"""

from snapper.config.settings import get_settings
from snapper.data.repository import Repository
from snapper.data.repository import get_repository


def get_repository_dependency() -> Repository:
    """FastAPI dependency returning the cached repository.

    Looks up the configured database URL via ``get_settings`` and
    returns the cached ``Repository`` instance from
    ``get_repository``. The cache is process-wide so every request
    shares the same engine / connection pool.

    Returns:
        Repository instance for the configured database.
    """
    settings = get_settings()
    return get_repository(settings.db_url)
