"""HTTP contract tests for disabled-by-default, owner-bound Android registration."""

from collections.abc import AsyncGenerator
from datetime import UTC
from datetime import datetime
from pathlib import Path
from uuid import uuid4

import httpx
import pytest
from fastapi import FastAPI
from fastapi import HTTPException

from snapper.api.schemas.android_devices import AndroidPushConfiguration
from snapper.api.schemas.android_devices import RegisterAndroidDeviceBody
from snapper.api.schemas.android_devices import RegisterAndroidDeviceCommand
from snapper.api.schemas.devices import DeviceAlertPrefBody
from snapper.api.schemas.devices import RevokeDevicePrefBody
from snapper.api.schemas.devices import RevokeDevicePrefCommand
from snapper.api.schemas.devices import UpdateDevicePrefCommand
from snapper.application.notify.android_config import AndroidPushRuntime
from snapper.auth.dependencies import require_authentication
from snapper.auth.dependencies import validate_csrf_token
from snapper.auth.domain.roles import UserRole
from snapper.auth.schemas.principal import AuthPrincipal
from snapper.data.android_devices import RegistrationWrite
from snapper.data.android_devices import registration_revision
from snapper.data.repository import AndroidConfigurationStore
from snapper.data.repository import SQLAlchemyRepository
from snapper.messaging.infrastructure.publisher import SequenceTracker
from snapper.server.android_device_routes import get_android_push_runtime
from snapper.server.android_device_routes import router
from snapper.server.dependencies import get_repository_dependency
from snapper.server.device_routes import router as legacy_router

_OWNER = str(uuid4())
_CONFIG = str(uuid4())
_INSTALLATION = str(uuid4())


@pytest.fixture
async def api(tmp_path: Path) -> AsyncGenerator[tuple[FastAPI, httpx.AsyncClient]]:
    """Use real isolated SQLite and a minimal app with explicit synthetic authentication."""
    repo = SQLAlchemyRepository(f"sqlite+aiosqlite:///{tmp_path / 'routes.db'}")
    await repo.create_all()
    configuration = _runtime().configuration
    assert configuration is not None
    await AndroidConfigurationStore(repo).activate(
        configuration, None, RegistrationWrite("", str(uuid4()), 1, datetime.now(UTC))
    )
    app = FastAPI()
    app.state.rest_tracker = SequenceTracker()
    app.include_router(router, prefix="/api")
    app.include_router(legacy_router, prefix="/api")
    app.dependency_overrides[get_repository_dependency] = lambda: repo
    app.dependency_overrides[require_authentication] = lambda: AuthPrincipal(
        username="synthetic-viewer",
        role=UserRole.VIEWER,
        user_public_id=_OWNER,
    )
    app.dependency_overrides[validate_csrf_token] = lambda: None
    try:
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as client:
            yield app, client
    finally:
        await repo.engine.dispose()


def _runtime(sender: bool = True) -> AndroidPushRuntime:
    """Public synthetic options cannot send through any real provider."""
    return AndroidPushRuntime(
        AndroidPushConfiguration(
            configuration_id=_CONFIG,
            package_name="ch.snapper.android",
            project_id="synthetic",
            application_id="synthetic-app",
            api_key="synthetic-public-key",
            messaging_sender_id="123",
        ),
        sender,
    )


@pytest.mark.asyncio
async def test_public_options_without_sender_are_unavailable(
    api: tuple[FastAPI, httpx.AsyncClient],
) -> None:
    """Never advertise delivery readiness based on client configuration alone.

    Given: Public Firebase options with sender readiness initially absent.
    When: The authenticated client requests metadata before and after readiness.
    Then: Only the admitted sender configuration is advertised.
    """
    app, client = api
    response = await client.get("/api/devices/android/config")
    assert response.status_code == 200
    assert response.json()["payload"]["availability"] == "unavailable"
    app.dependency_overrides[get_android_push_runtime] = lambda: _runtime(False)
    assert (await client.get("/api/devices/android/config")).json()["payload"][
        "configuration"
    ] is None
    app.dependency_overrides[get_android_push_runtime] = _runtime
    payload = (await client.get("/api/devices/android/config")).json()["payload"]
    assert payload["availability"] == "configured"
    assert payload["configuration"]["protocol"] == "fcm_fid"
    assert "credential" not in str(payload)


@pytest.mark.asyncio
async def test_http_registration_envelope_and_private_inventory(
    api: tuple[FastAPI, httpx.AsyncClient],
) -> None:
    """The caller can prepare and register without a wallet or trading grant.

    Given: An authenticated synthetic owner and configured Android sender.
    When: The client registers an FID and reads its inventory.
    Then: Envelopes contain owned registration identity without exposing the FID.
    """
    app, client = api
    app.dependency_overrides[get_android_push_runtime] = _runtime
    state = await client.get(
        "/api/devices/android/registration-state",
        params={
            "configuration_id": _CONFIG,
            "installation_id": _INSTALLATION,
        },
    )
    revision = state.json()["payload"]["revision"]
    body = {
        "type": "register_android_device_command",
        "public_id": str(uuid4()),
        "session_id": str(uuid4()),
        "sequence_id": 1,
        "timestamp": datetime.now(UTC).isoformat(),
        "payload": {
            "protocol": "fcm_fid",
            "configuration_id": _CONFIG,
            "package_name": "ch.snapper.android",
            "installation_id": _INSTALLATION,
            "binding_id": str(uuid4()),
            "expected_revision": revision,
            "firebase_installation_id": "synthetic-FID",
            "expected_user_public_id": _OWNER,
        },
    }
    response = await client.post("/api/devices/android", json=body)
    assert response.status_code == 200, response.text
    payload = response.json()["payload"]
    assert payload["user_public_id"] == _OWNER
    assert payload["platform"] == "android"
    assert "firebase_installation_id" not in payload
    inventory = (await client.get("/api/devices/android")).json()
    assert inventory["count"] == 1
    assert inventory["payload"][0]["public_id"] == payload["public_id"]
    app.dependency_overrides[require_authentication] = lambda: AuthPrincipal(
        username="other",
        role=UserRole.VIEWER,
        user_public_id=str(uuid4()),
    )
    assert (await client.get("/api/devices/android")).json()["count"] == 0
    foreign = (
        await client.get(
            "/api/devices/android/registration-state",
            params={
                "configuration_id": _CONFIG,
                "installation_id": _INSTALLATION,
            },
        )
    ).json()["payload"]
    assert foreign["current_binding_id"] is None
    assert foreign["revision"] != revision
    assert (await client.post("/api/devices/android", json=body)).status_code == 409


@pytest.mark.asyncio
async def test_http_routes_require_authentication(api: tuple[FastAPI, httpx.AsyncClient]) -> None:
    """Metadata remains authenticated despite containing only public SDK options.

    Given: Android routes with authentication refusing the request.
    When: Unauthenticated metadata, state, registration and inventory requests are made.
    Then: Each protected route rejects access.
    """
    app, client = api

    def denied() -> AuthPrincipal:
        """Model the existing authentication dependency refusing a missing session."""
        raise HTTPException(401, "authentication_required")

    app.dependency_overrides[require_authentication] = denied
    assert (await client.get("/api/devices/android/config")).status_code == 401
    assert (await client.get("/api/devices/android")).status_code == 401


def _command() -> RegisterAndroidDeviceCommand:
    """Mint a valid synthetic command for admission and CSRF witnesses."""
    return RegisterAndroidDeviceCommand(
        public_id=str(uuid4()),
        session_id=str(uuid4()),
        sequence_id=1,
        timestamp=datetime.now(UTC),
        payload=RegisterAndroidDeviceBody(
            protocol="fcm_fid",
            configuration_id=_CONFIG,
            package_name="ch.snapper.android",
            installation_id=_INSTALLATION,
            binding_id=str(uuid4()),
            expected_revision=registration_revision(None),
            firebase_installation_id="synthetic-FID",
            expected_user_public_id=_OWNER,
        ),
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("field", "value", "code"),
    [
        (
            "configuration_id",
            "22222222-2222-4222-8222-222222222222",
            "android_push_configuration_changed",
        ),
        ("package_name", "other.package", "android_push_package_mismatch"),
    ],
)
async def test_wrong_configuration_refused_without_registration(
    api: tuple[FastAPI, httpx.AsyncClient],
    field: str,
    value: str,
    code: str,
) -> None:
    """Client-controlled metadata cannot select another project's sender.

    Given: An authenticated registration intent with a mismatched configuration identity.
    When: The command is submitted.
    Then: The server returns a known refusal without storing a registration.
    """
    app, client = api
    app.dependency_overrides[get_android_push_runtime] = _runtime
    command = _command()
    command = command.model_copy(
        update={"payload": command.payload.model_copy(update={field: value})}
    )
    response = await client.post("/api/devices/android", json=command.model_dump(mode="json"))
    assert response.status_code == 409
    assert response.json()["detail"] == code
    assert (await client.get("/api/devices/android")).json()["count"] == 0


@pytest.mark.asyncio
async def test_unavailable_csrf_and_stale_revision_refuse_dispatch(
    api: tuple[FastAPI, httpx.AsyncClient],
) -> None:
    """Unavailable sender, failed CSRF and stale first submission produce known refusals.

    Given: Unavailable sender, failed CSRF or obsolete installation revision conditions.
    When: Registration is attempted under each condition.
    Then: The server refuses the command before replacing current state.
    """
    app, client = api
    command = _command()
    response = await client.post("/api/devices/android", json=command.model_dump(mode="json"))
    assert response.status_code == 503
    assert response.json()["detail"] == "android_push_unavailable"
    app.dependency_overrides[get_android_push_runtime] = _runtime

    def denied() -> None:
        """Require the existing mutation CSRF dependency to run."""
        raise HTTPException(403, "csrf_required")

    app.dependency_overrides[validate_csrf_token] = denied
    assert (
        await client.post("/api/devices/android", json=command.model_dump(mode="json"))
    ).status_code == 403
    app.dependency_overrides[validate_csrf_token] = lambda: None
    assert (
        await client.post("/api/devices/android", json=command.model_dump(mode="json"))
    ).status_code == 200
    stale = _command()
    response = await client.post("/api/devices/android", json=stale.model_dump(mode="json"))
    assert response.status_code == 409
    assert response.json()["detail"] == "android_push_revision_changed"


@pytest.mark.asyncio
async def test_cross_installation_fid_conflict_does_not_disclose_counterparty(
    api: tuple[FastAPI, httpx.AsyncClient],
) -> None:
    """A second installation cannot displace another registration sharing its FID.

    Given: An FID already bound to another installation.
    When: A second installation submits the same FID.
    Then: The conflict neither replaces the binding nor discloses its owner.
    """
    app, client = api
    app.dependency_overrides[get_android_push_runtime] = _runtime
    original = _command()
    response = await client.post("/api/devices/android", json=original.model_dump(mode="json"))
    registration_id = response.json()["payload"]["public_id"]
    other = _command()
    other = other.model_copy(
        update={"payload": other.payload.model_copy(update={"installation_id": str(uuid4())})}
    )
    response = await client.post("/api/devices/android", json=other.model_dump(mode="json"))
    assert response.status_code == 409
    assert response.json() == {"detail": "android_push_binding_conflict"}
    assert "synthetic-FID" not in response.text
    assert _OWNER not in response.text
    inventory = (await client.get("/api/devices/android")).json()
    assert inventory["count"] == 1
    assert inventory["payload"][0]["public_id"] == registration_id


@pytest.mark.asyncio
async def test_android_uses_owned_preferences_and_revocation_without_appearing_in_apns_list(
    api: tuple[FastAPI, httpx.AsyncClient],
) -> None:
    """Legacy ownership routes cover Android while the legacy inventory remains APNs-only.

    Given: An accepted Android registration owned by the authenticated caller.
    When: The caller edits preferences, revokes the binding and retries it.
    Then: Shared ownership routes work, APNs inventory excludes it and retired replay is refused.
    """
    app, client = api
    app.dependency_overrides[get_android_push_runtime] = _runtime
    command = _command()
    receipt = (
        await client.post("/api/devices/android", json=command.model_dump(mode="json"))
    ).json()
    public_id = receipt["payload"]["public_id"]
    assert (await client.get("/api/devices")).json()["count"] == 0
    pref = UpdateDevicePrefCommand(
        session_id=str(uuid4()),
        public_id=str(uuid4()),
        sequence_id=1,
        timestamp=datetime.now(UTC),
        payload=DeviceAlertPrefBody(
            alert_type="order_fill_full",
            enabled=False,
            quiet_hours_start_min=1320,
            quiet_hours_end_min=420,
            timezone="Europe/Warsaw",
        ),
    )
    response = await client.patch(
        f"/api/devices/{public_id}/prefs", json=pref.model_dump(mode="json")
    )
    assert response.status_code == 200
    assert response.json()["payload"]["enabled"] is False
    pref_id = response.json()["payload"]["public_id"]
    assert (await client.get(f"/api/devices/{public_id}/prefs")).json()["count"] == 1
    revoke = RevokeDevicePrefCommand(
        session_id=str(uuid4()),
        public_id=str(uuid4()),
        sequence_id=2,
        timestamp=datetime.now(UTC),
        payload=RevokeDevicePrefBody(),
    )
    assert (
        await client.post(
            f"/api/devices/{public_id}/prefs/{pref_id}/revoke", json=revoke.model_dump(mode="json")
        )
    ).status_code == 200
    assert (await client.get(f"/api/devices/{public_id}/prefs")).json()["count"] == 0
    assert (await client.delete(f"/api/devices/{public_id}")).status_code == 200
    assert (await client.get("/api/devices/android")).json()["count"] == 0
    assert (await client.delete(f"/api/devices/{public_id}")).status_code == 404
    assert (await client.post("/api/devices/android", json=command.model_dump(mode="json"))).json()[
        "detail"
    ] == "android_push_binding_retired"


@pytest.mark.asyncio
async def test_retired_process_configuration_is_not_advertised_as_available(
    api: tuple[FastAPI, httpx.AsyncClient],
) -> None:
    """A still-running API process cannot advertise its sender after a global disable.

    Given: An API process retaining options for a globally disabled configuration.
    When: The authenticated client requests public metadata.
    Then: The response is unavailable and the read does not mutate the disabled revision.
    """
    app, client = api
    app.state.android_push_runtime = _runtime()
    repo = app.dependency_overrides[get_repository_dependency]()
    assert isinstance(repo, SQLAlchemyRepository)
    store = AndroidConfigurationStore(repo)
    before = await store.revision()
    await store.disable(before.revision, RegistrationWrite("", str(uuid4()), 2, datetime.now(UTC)))
    disabled = await store.revision()
    response = await client.get("/api/devices/android/config")
    assert response.status_code == 200
    assert response.json()["payload"]["availability"] == "unavailable"
    assert response.json()["payload"]["configuration"] is None
    assert await store.revision() == disabled
