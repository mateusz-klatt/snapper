"""Explicit runtime files, safe failures and sender lifetime without provider traffic."""

import json
import traceback
from collections.abc import AsyncGenerator
from pathlib import Path
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from fastapi import FastAPI

from snapper.application.notify.android_runtime import android_runtime_context
from snapper.application.notify.android_runtime import android_runtime_lifespan
from snapper.application.notify.android_runtime import build_android_runtime
from snapper.data.repository import AndroidConfigurationStore
from snapper.data.repository import SQLAlchemyRepository


@pytest.fixture
async def repo(tmp_path: Path) -> AsyncGenerator[SQLAlchemyRepository]:
    """Isolate runtime activation from any existing database."""
    repository = SQLAlchemyRepository(f"sqlite+aiosqlite:///{tmp_path / 'runtime.db'}")
    await repository.create_all()
    try:
        yield repository
    finally:
        await repository.engine.dispose()


def _write_runtime(directory: Path) -> Path:
    """Create synthetic files with a relative credential path resolved from the JSON."""
    directory.mkdir()
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    pem = key.private_bytes(
        serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption()
    ).decode()
    (directory / "synthetic-key.json").write_text(
        json.dumps(
            {
                "type": "service_account",
                "project_id": "synthetic",
                "client_email": "synthetic@example.invalid",
                "private_key": pem,
                "private_key_id": "key",
                "token_uri": "https://oauth2.googleapis.com/token",
            }
        )
    )
    path = directory / "runtime.json"
    path.write_text(
        json.dumps(
            {
                "enabled": True,
                "apns_enabled": False,
                "credentials_file": "synthetic-key.json",
                "configuration": {
                    "configuration_id": str(uuid4()),
                    "package_name": "ch.snapper.android",
                    "project_id": "synthetic",
                    "application_id": "app",
                    "api_key": "public",
                    "messaging_sender_id": "123",
                },
            }
        )
    )
    return path


@pytest.mark.asyncio
async def test_runtime_relative_path_and_local_absence_do_not_disable_active_sender(
    repo: SQLAlchemyRepository,
    tmp_path: Path,
) -> None:
    """Only explicit predecessor-matched disable can retire a shared active configuration.

    Given: An explicit runtime file with a relative synthetic credential path.
    When: The runtime activates, an absent configuration is loaded, and explicit disable is applied.
    Then: Only explicit revision-matched disable retires the shared configuration.
    """
    path = _write_runtime(tmp_path / "settings")
    async with android_runtime_context(repo, str(path)) as runtime:
        configuration = runtime.available_configuration
        assert configuration is not None and runtime.sender is not None
        assert not runtime.apns_enabled
        absent = await build_android_runtime(repo, None)
        assert absent.available_configuration is None and absent.apns_enabled
        assert await AndroidConfigurationStore(repo).matches(configuration)
        path.write_text(
            json.dumps(
                {
                    "enabled": False,
                    "expected_configuration_revision": (
                        await AndroidConfigurationStore(repo).revision()
                    ).revision,
                }
            )
        )
        disabled = await build_android_runtime(repo, str(path))
        assert disabled.available_configuration is None
        assert not await AndroidConfigurationStore(repo).matches(configuration)


@pytest.mark.asyncio
async def test_lifespan_always_resets_public_capability_and_closes_sender(
    repo: SQLAlchemyRepository,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Even composition failure clears readiness and releases the runtime-owned HTTP pool.

    Given: An API lifespan with an admitted synthetic sender.
    When: Composition raises after the runtime becomes available.
    Then: The sender closes once and public readiness resets.
    """
    path = _write_runtime(tmp_path / "settings")
    monkeypatch.setenv("SNAPPER_ANDROID_PUSH_CONFIG_FILE", str(path))
    app = FastAPI()
    closed = AsyncMock()
    with pytest.raises(RuntimeError, match="synthetic startup failure"):
        async with android_runtime_lifespan(app, repo):
            sender = app.state.android_push_runtime.sender
            original = sender.aclose

            async def close_sender() -> None:
                """Record shutdown while still closing the actual local HTTP client."""
                await closed()
                await original()

            sender.aclose = close_sender
            raise RuntimeError("synthetic startup failure")
    closed.assert_awaited_once()
    assert app.state.android_push_runtime.available_configuration is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "failure",
    [
        "invalid_json",
        "missing_file",
        "missing_configuration",
        "wrong_package",
        "missing_credentials",
        "bad_credentials",
        "wrong_project",
        "invalid_key",
        "token_url",
    ],
)
async def test_configuration_failures_never_expose_private_input(
    repo: SQLAlchemyRepository,
    tmp_path: Path,
    failure: str,
) -> None:
    """Inspect actual formatted exceptions for accidental raw Pydantic credential disclosure.

    Given: Malformed runtime or credential input containing a synthetic private marker.
    When: Runtime construction validates the input and fails.
    Then: Formatted exceptions contain neither the marker nor raw Pydantic input.
    """
    path = _write_runtime(tmp_path / "settings")
    raw = json.loads(path.read_text())
    credential_path = path.parent / "synthetic-key.json"
    credentials = json.loads(credential_path.read_text())
    marker = "SYNTHETIC_PRIVATE_KEY_MUST_NEVER_LEAK"
    if failure == "invalid_json":
        path.write_text(marker)
    elif failure == "missing_file":
        path.unlink()
    elif failure == "wrong_package":
        raw["configuration"]["package_name"] = "other.package"
    elif failure in {"missing_configuration", "missing_credentials"}:
        raw["configuration" if failure == "missing_configuration" else "credentials_file"] = None
    elif failure == "bad_credentials":
        credential_path.write_text(marker)
    elif failure == "wrong_project":
        credentials["project_id"] = "other"
    elif failure == "invalid_key":
        credentials["private_key"] = marker
    else:
        credentials["token_uri"] = "https://example.invalid/" + marker
    if failure not in {"invalid_json", "missing_file"}:
        path.write_text(json.dumps(raw))
    if failure in {"wrong_project", "invalid_key", "token_url"}:
        credential_path.write_text(json.dumps(credentials))
    with pytest.raises(ValueError) as caught:
        await build_android_runtime(repo, str(path))
    formatted = "".join(traceback.format_exception(caught.value))
    assert marker not in formatted
    assert "BEGIN PRIVATE KEY" not in formatted
    assert "ValidationError" not in formatted


@pytest.mark.asyncio
async def test_absent_runtime_context_has_no_sender_to_close(repo: SQLAlchemyRepository) -> None:
    """Ordinary APNs-only composition does not activate Android configuration.

    Given: An empty configuration store and no runtime filename.
    When: An ordinary runtime context opens and closes.
    Then: APNs defaults remain enabled and no Android head is created.
    """
    async with android_runtime_context(repo, None) as runtime:
        assert runtime.sender is None and runtime.apns_enabled
    assert (await AndroidConfigurationStore(repo).revision()).revision is None
