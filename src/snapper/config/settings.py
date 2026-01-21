"""Settings module providing cached access to application configuration.

This module serves as the main entry point for configuration access,
exporting factory functions that provide cached singleton instances
of settings objects.

Factory Functions:
    - ``get_bootstrap_settings()``: Cached bootstrap settings from env.
    - ``get_settings()``: Cached AppSettings with bootstrap only.
    - ``get_settings_with_service(service)``: AppSettings with DB access.

Example:
    Basic usage::

        from snapper.config.settings import get_settings

        settings = get_settings()
        print(settings.server_port)  # 8000

    With database settings::

        from snapper.config.settings import get_settings_with_service

        settings = get_settings_with_service(settings_service)
        api_key = settings.kraken_api_key
"""

from functools import lru_cache
from typing import Any

from snapper.application.services.settings import SettingsService
from snapper.application.services.settings import get_settings_service
from snapper.config.app import AppSettings
from snapper.config.bootstrap import BootstrapSettingsLoader

__all__ = [
    "AppSettings",
    "BootstrapSettingsLoader",
    "SettingsService",
    "get_bootstrap_settings",
    "get_settings",
    "get_settings_service",
    "get_settings_with_service",
]


@lru_cache(maxsize=1)
def get_bootstrap_settings() -> BootstrapSettingsLoader:
    """Get cached bootstrap settings instance.

    Returns:
        Singleton BootstrapSettingsLoader with environment configuration.
    """
    return BootstrapSettingsLoader()


@lru_cache(maxsize=1)
def get_settings() -> AppSettings:
    """Get cached AppSettings instance without database access.

    This function returns a singleton AppSettings that can only access
    bootstrap (environment) settings. Attempting to access database-backed
    settings will raise RuntimeError.

    Use ``get_settings_with_service()`` when database settings are needed.

    Returns:
        Cached AppSettings instance with bootstrap configuration only.
    """
    bootstrap = get_bootstrap_settings()
    return AppSettings(bootstrap)


def get_settings_with_service(settings_service: Any) -> AppSettings:
    """Create AppSettings with database access.

    Unlike ``get_settings()``, this function creates a new instance each time
    (not cached) because the settings_service may change during runtime.

    Args:
        settings_service: Initialized SettingsService for database access.

    Returns:
        New AppSettings instance with both bootstrap and database access.
    """
    bootstrap = get_bootstrap_settings()
    return AppSettings(bootstrap, settings_service)
