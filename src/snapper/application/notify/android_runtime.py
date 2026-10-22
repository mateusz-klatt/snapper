"""Explicit runtime configuration and credentials; absent configuration stays disabled."""

import os
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from datetime import UTC
from datetime import datetime
from pathlib import Path
from uuid import uuid7

import httpx
from fastapi import FastAPI
from pydantic import Field
from pydantic import ValidationError

from snapper.api.schemas.android_devices import AndroidPushConfiguration
from snapper.api.schemas.android_devices import UuidText
from snapper.api.schemas.base import StrictBody
from snapper.application.notify.android_config import AndroidPushRuntime
from snapper.application.notify.fcm_client import FcmClient
from snapper.application.notify.fcm_client import FcmServiceAccount
from snapper.config.android_push import CONFIG_FILE_ENV as ANDROID_PUSH_CONFIG_FILE_ENV
from snapper.data.android_devices import RegistrationWrite
from snapper.data.repository import AndroidConfigurationStore
from snapper.data.repository import Repository


class AndroidPushRuntimeFile(StrictBody):
    """Operator-only settings; never serialize this object into an API response."""

    enabled: bool = False
    apns_enabled: bool = True
    configuration: AndroidPushConfiguration | None = None
    expected_configuration_revision: UuidText | None = None
    credentials_file: str | None = Field(default=None, repr=False)


def _utc_now() -> datetime:
    """Provide an injectable UTC clock to the provider adapter."""
    return datetime.now(UTC)


def _read_settings(path: Path) -> AndroidPushRuntimeFile:
    """Parse only the explicit runtime file, suppressing raw validation input."""
    try:
        return AndroidPushRuntimeFile.model_validate_json(path.read_bytes())
    except (OSError, ValidationError):
        raise ValueError("Invalid Android push runtime configuration") from None


def _read_credentials(settings: AndroidPushRuntimeFile, directory: Path) -> FcmServiceAccount:
    """Read the explicitly referenced file without ambient cloud credential discovery."""
    if settings.credentials_file is None:
        raise ValueError("Android push requires an explicit credential file")
    try:
        return FcmServiceAccount.model_validate_json(
            (directory / settings.credentials_file).read_bytes()
        )
    except (OSError, ValidationError):
        raise ValueError("Invalid Android push service-account file") from None


async def build_android_runtime(repo: Repository, path: str | None) -> AndroidPushRuntime:
    """Validate sender configuration before atomically activating its immutable project ID.

    Args:
        repo: Repository used for atomic configuration-head admission.
        path: Explicit runtime JSON path, or None for local Android disable.

    Returns:
        Validated runtime with its owned sender, or a disabled runtime.
    """
    if not path:
        return AndroidPushRuntime()
    location = Path(path).resolve()
    settings = _read_settings(location)
    if not settings.enabled:
        await AndroidConfigurationStore(repo).disable(
            settings.expected_configuration_revision,
            RegistrationWrite("", str(uuid7()), 1, _utc_now()),
        )
        return AndroidPushRuntime(apns_enabled=settings.apns_enabled)
    configuration = settings.configuration
    if configuration is None or configuration.package_name != "ch.snapper.android":
        raise ValueError("Android push requires the production package configuration")
    credentials = _read_credentials(settings, location.parent)
    client = httpx.AsyncClient(timeout=10.0, follow_redirects=False, trust_env=False)
    try:
        sender = FcmClient(configuration, credentials, client, _utc_now)
        write = RegistrationWrite("", str(uuid7()), 1, _utc_now())
        await AndroidConfigurationStore(repo).activate(
            configuration, settings.expected_configuration_revision, write
        )
        return AndroidPushRuntime(configuration, True, sender, settings.apns_enabled)
    except BaseException:
        await client.aclose()
        raise


@asynccontextmanager
async def android_runtime_context(
    repo: Repository, path: str | None
) -> AsyncGenerator[AndroidPushRuntime]:
    """Close private sender resources even when later composition fails.

    Args:
        repo: Repository used for configuration admission.
        path: Explicit runtime JSON path, or None for local Android disable.

    Yields:
        The runtime whose sender is closed when the context exits.
    """
    runtime = await build_android_runtime(repo, path)
    try:
        yield runtime
    finally:
        if runtime.sender is not None:
            await runtime.sender.aclose()


@asynccontextmanager
async def android_runtime_lifespan(app: FastAPI, repo: Repository) -> AsyncGenerator[None]:
    """Activate at startup; GET handlers never mutate configuration state.

    Args:
        app: API application receiving the public runtime capability.
        repo: Repository used for configuration admission.

    Yields:
        Control while the runtime is available to API handlers.
    """
    async with android_runtime_context(repo, os.getenv(ANDROID_PUSH_CONFIG_FILE_ENV)) as runtime:
        app.state.android_push_runtime = runtime
        try:
            yield
        finally:
            app.state.android_push_runtime = AndroidPushRuntime()
