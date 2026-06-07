"""Shared FastAPI dependencies for the REST API layer.

Keeps small dependency callables out of the main ``server/app.py`` so
new route modules (wallet / operator / scope grant routes
and future route modules) can import them without pulling the
full application factory and introducing a circular import.
Currently only ``get_repository_dependency`` lives here. Other
cross-cutting dependencies should be added on demand as new routers
need them — avoid introducing catch-all helpers that routers do not
actually consume.
"""

from snapper.application.pricing.usd_converter import USDConverter
from snapper.application.trade.caps_enforcer import TradingCapsEnforcer
from snapper.config.settings import get_settings
from snapper.data.repository import Repository
from snapper.data.repository import SQLAlchemyRepository
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


_CAPS_ENFORCER_CACHE: dict[str, TradingCapsEnforcer] = {}


def get_caps_enforcer_dependency() -> TradingCapsEnforcer:
    """FastAPI dependency returning the process-singleton enforcer.

    Lazily constructs :class:`TradingCapsEnforcer` +
    :class:`USDConverter` on first call and caches both for the
    lifetime of the process. Subsequent calls return the same
    instance, so concurrent HTTP requests share the per-user
    :class:`asyncio.Lock` dict that prevents cap-check TOCTOU.

    Returns:
        The cached :class:`TradingCapsEnforcer` singleton.

    Raises:
        RuntimeError: when the configured repository is not a
            :class:`SQLAlchemyRepository` — the caps enforcer needs
            a live async session factory to query cap state.
    """
    cached = _CAPS_ENFORCER_CACHE.get("singleton")
    if cached is not None:
        return cached
    settings = get_settings()
    repository = get_repository(settings.db_url)
    if not isinstance(repository, SQLAlchemyRepository):
        raise RuntimeError(
            "get_caps_enforcer_dependency requires SQLAlchemyRepository; "
            f"got {type(repository).__name__}"
        )
    pricing = USDConverter(repository=repository)
    enforcer = TradingCapsEnforcer(repository=repository, pricing=pricing)
    _CAPS_ENFORCER_CACHE["singleton"] = enforcer
    return enforcer


def reset_caps_enforcer_singleton() -> None:
    """Clear the cached enforcer — test hook."""
    _CAPS_ENFORCER_CACHE.pop("singleton", None)
