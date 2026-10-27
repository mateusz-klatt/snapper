"""Authenticated Android registration isolated from legacy APNs routing."""

from datetime import UTC
from datetime import datetime
from typing import Annotated
from uuid import uuid7

from fastapi import APIRouter
from fastapi import Depends
from fastapi import HTTPException
from fastapi import Request

from snapper.api.schemas.android_devices import AndroidDeviceListResponse
from snapper.api.schemas.android_devices import AndroidDeviceResponse
from snapper.api.schemas.android_devices import AndroidPushConfigInfo
from snapper.api.schemas.android_devices import AndroidPushConfigResponse
from snapper.api.schemas.android_devices import AndroidRegistrationStateInfo
from snapper.api.schemas.android_devices import AndroidRegistrationStateResponse
from snapper.api.schemas.android_devices import RegisterAndroidDeviceCommand
from snapper.api.schemas.android_devices import UuidText
from snapper.application.notify.android_config import AndroidPushRuntime
from snapper.auth.dependencies import require_authentication
from snapper.auth.dependencies import require_permission
from snapper.auth.dependencies import validate_csrf_token
from snapper.auth.domain.permissions import Permission
from snapper.auth.schemas.principal import AuthPrincipal
from snapper.data.android_devices import AndroidRegistrationError
from snapper.data.android_devices import RegistrationWrite
from snapper.data.android_devices import registration_revision
from snapper.data.repository import AndroidConfigurationStore
from snapper.data.repository import AndroidDeviceStore
from snapper.data.repository import Repository
from snapper.messaging.infrastructure.publisher import SequenceTracker
from snapper.server.dependencies import get_repository_dependency
from snapper.server.json_body import json_body
from snapper.server.json_body import openapi_schema

router = APIRouter(prefix="/devices/android", tags=["devices"])


def get_android_push_runtime(request: Request) -> AndroidPushRuntime:
    """Remain unavailable until the composition root supplies a working sender.

    Args:
        request: Request whose application owns the admitted runtime.

    Returns:
        The configured application runtime, or a disabled default.
    """
    runtime: AndroidPushRuntime = getattr(
        request.app.state, "android_push_runtime", AndroidPushRuntime()
    )
    return runtime


def _stamp(request: Request, owner: str) -> RegistrationWrite:
    """Allocate server provenance without trusting the client's envelope clock."""
    tracker: SequenceTracker = request.app.state.rest_tracker
    return RegistrationWrite(
        owner, tracker.session_id, tracker.next_sequence("rest.devices.android"), datetime.now(UTC)
    )


@router.get("/config")
async def android_push_config(
    request: Request,
    principal: Annotated[AuthPrincipal, Depends(require_authentication)],
    runtime: Annotated[AndroidPushRuntime, Depends(get_android_push_runtime)],
    repo: Annotated[Repository, Depends(get_repository_dependency)],
) -> AndroidPushConfigResponse:
    """Return only public configuration and honest sender availability.

    Args:
        request: Request supplying response provenance.
        principal: Authenticated caller; no trading permission is required.
        runtime: Process-local public options and sender readiness.
        repo: Repository used to verify the persisted configuration head.

    Returns:
        Public configuration metadata, unavailable when local and persisted state differ.
    """
    stamp = _stamp(request, principal.user_public_id)
    configuration = runtime.available_configuration
    if configuration is not None and not await AndroidConfigurationStore(repo).matches(
        configuration
    ):
        configuration = None
    payload = AndroidPushConfigInfo(
        session_id=stamp.session_id,
        sequence_id=stamp.sequence_id,
        timestamp=stamp.timestamp,
        public_id=str(uuid7()),
        availability="configured" if configuration else "unavailable",
        configuration=configuration,
    )
    return AndroidPushConfigResponse(
        session_id=stamp.session_id,
        sequence_id=stamp.sequence_id,
        timestamp=stamp.timestamp,
        public_id=str(uuid7()),
        payload=payload,
    )


@router.get("/registration-state")
async def android_registration_state(
    request: Request,
    configuration_id: UuidText,
    installation_id: UuidText,
    principal: Annotated[AuthPrincipal, Depends(require_authentication)],
    repo: Annotated[Repository, Depends(get_repository_dependency)],
) -> AndroidRegistrationStateResponse:
    """Read an opaque revision without revealing a different account's binding.

    Args:
        request: Request supplying response provenance.
        configuration_id: Immutable configuration whose installation head is read.
        installation_id: App-local opaque installation identity.
        principal: Authenticated caller used to limit binding disclosure.
        repo: Repository used for the read-only installation lookup.

    Returns:
        Opaque installation revision and the binding ID only when owned by the caller.
    """
    row = await AndroidDeviceStore(repo).state(configuration_id, installation_id)
    binding = (
        row.binding_id
        if row is not None and row.user_public_id == principal.user_public_id
        else None
    )
    stamp = _stamp(request, principal.user_public_id)
    payload = AndroidRegistrationStateInfo(
        session_id=stamp.session_id,
        sequence_id=stamp.sequence_id,
        timestamp=stamp.timestamp,
        public_id=str(uuid7()),
        revision=registration_revision(row),
        current_binding_id=binding,
    )
    return AndroidRegistrationStateResponse(
        session_id=stamp.session_id,
        sequence_id=stamp.sequence_id,
        timestamp=stamp.timestamp,
        public_id=str(uuid7()),
        payload=payload,
    )


def _admit(command: RegisterAndroidDeviceCommand, runtime: AndroidPushRuntime, owner: str) -> None:
    """Refuse wrong owner or configuration before any registration write."""
    body = command.payload
    configuration = runtime.available_configuration
    if body.expected_user_public_id != owner:
        raise HTTPException(409, "android_push_owner_changed")
    if configuration is None:
        raise HTTPException(503, "android_push_unavailable")
    if body.configuration_id != configuration.configuration_id:
        raise HTTPException(409, "android_push_configuration_changed")
    if body.package_name != configuration.package_name:
        raise HTTPException(409, "android_push_package_mismatch")


@router.post(
    "",
    openapi_extra=openapi_schema(RegisterAndroidDeviceCommand),
    dependencies=[Depends(validate_csrf_token)],
)
async def register_android_device(
    request: Request,
    command: Annotated[
        RegisterAndroidDeviceCommand, Depends(json_body(RegisterAndroidDeviceCommand))
    ],
    principal: Annotated[
        AuthPrincipal, Depends(require_permission(Permission.MANAGE_SELF_ACCOUNT))
    ],
    repo: Annotated[Repository, Depends(get_repository_dependency)],
    runtime: Annotated[AndroidPushRuntime, Depends(get_android_push_runtime)],
) -> AndroidDeviceResponse:
    """Register one FID with a compare-and-replace installation revision.

    Args:
        request: Request supplying server response provenance.
        command: Typed FID registration intent and expected installation revision.
        principal: Authenticated owner checked against the submitted expected user.
        repo: Repository used for atomic registration CAS.
        runtime: Process-local configuration and sender readiness.

    Returns:
        The accepted owned registration receipt without its FID.
    """
    _admit(command, runtime, principal.user_public_id)
    stamp = _stamp(request, principal.user_public_id)
    try:
        payload = await AndroidDeviceStore(repo).register(command.payload, stamp)
    except AndroidRegistrationError as error:
        raise HTTPException(409, str(error)) from error
    return AndroidDeviceResponse(
        session_id=stamp.session_id,
        sequence_id=stamp.sequence_id,
        timestamp=stamp.timestamp,
        public_id=str(uuid7()),
        payload=payload,
    )


@router.get("")
async def list_android_devices(
    request: Request,
    principal: Annotated[AuthPrincipal, Depends(require_authentication)],
    repo: Annotated[Repository, Depends(get_repository_dependency)],
) -> AndroidDeviceListResponse:
    """List only the caller's Android registrations, without their FIDs.

    Args:
        request: Request supplying response provenance.
        principal: Authenticated owner whose active registrations are listed.
        repo: Repository used for the ownership-constrained inventory.

    Returns:
        Active Android registrations and count, without FIDs.
    """
    payload = await AndroidDeviceStore(repo).list_owned(principal.user_public_id)
    stamp = _stamp(request, principal.user_public_id)
    return AndroidDeviceListResponse(
        session_id=stamp.session_id,
        sequence_id=stamp.sequence_id,
        timestamp=stamp.timestamp,
        public_id=str(uuid7()),
        payload=payload,
        count=len(payload),
    )
