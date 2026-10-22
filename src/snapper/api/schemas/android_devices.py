"""Android FID registration contracts, separate from legacy APNs payloads."""

from datetime import datetime
from typing import Annotated
from typing import Literal

from pydantic import Field

from snapper.api.schemas.base import PayloadListResponse
from snapper.api.schemas.base import PayloadRequest
from snapper.api.schemas.base import PayloadResponse
from snapper.api.schemas.base import StrictBody
from snapper.api.schemas.base import StrictDataSchema

UuidText = Annotated[
    str,
    Field(pattern=r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"),
]


class AndroidPushConfiguration(StrictBody):
    """Public Firebase options; no sending credential is exposed."""

    configuration_id: UuidText
    protocol: Literal["fcm_fid"] = "fcm_fid"
    package_name: str = Field(min_length=1, max_length=255)
    project_id: str = Field(min_length=1, max_length=255)
    application_id: str = Field(min_length=1, max_length=255)
    api_key: str = Field(min_length=1, max_length=255)
    messaging_sender_id: str = Field(min_length=1, max_length=255)


class AndroidPushConfigInfo(StrictDataSchema[Literal["android_push_config_info"]]):
    """Availability requires both public configuration and a configured sender."""

    type: Literal["android_push_config_info"] = "android_push_config_info"
    availability: Literal["configured", "unavailable"]
    configuration: AndroidPushConfiguration | None


class AndroidPushConfigResponse(
    PayloadResponse[Literal["android_push_config_response"], AndroidPushConfigInfo]
):
    """Authenticated configuration response envelope."""

    type: Literal["android_push_config_response"] = "android_push_config_response"


class RegisterAndroidDeviceBody(StrictBody):
    """Explicit FID protocol and local binding; ownership comes from authentication."""

    protocol: Literal["fcm_fid"]
    configuration_id: UuidText
    package_name: str = Field(min_length=1, max_length=255)
    installation_id: UuidText
    binding_id: UuidText
    expected_revision: str = Field(min_length=64, max_length=64, pattern=r"^[0-9a-f]{64}$")
    firebase_installation_id: str = Field(min_length=1, max_length=256, pattern=r"^[!-~]+$")
    expected_user_public_id: UuidText
    app_version: str | None = Field(default=None, max_length=32)


class RegisterAndroidDeviceCommand(
    PayloadRequest[Literal["register_android_device_command"], RegisterAndroidDeviceBody]
):
    """Nonreplaying Android registration command envelope."""

    type: Literal["register_android_device_command"] = "register_android_device_command"


class AndroidDeviceInfo(StrictDataSchema[Literal["android_device_info"]]):
    """Owned registration projection that does not disclose the FID."""

    type: Literal["android_device_info"] = "android_device_info"
    user_public_id: UuidText
    installation_id: UuidText
    binding_id: UuidText
    configuration_id: UuidText
    protocol: Literal["fcm_fid"] = "fcm_fid"
    platform: Literal["android"] = "android"
    package_name: str
    status: Literal["active"] = "active"
    registered_at: datetime
    last_seen_at: datetime | None


class AndroidDeviceResponse(PayloadResponse[Literal["android_device_response"], AndroidDeviceInfo]):
    """Single owned Android registration envelope."""

    type: Literal["android_device_response"] = "android_device_response"


class AndroidDeviceListResponse(
    PayloadListResponse[Literal["android_device_list_response"], AndroidDeviceInfo]
):
    """Android-only inventory preserving the APNs list contract."""

    type: Literal["android_device_list_response"] = "android_device_list_response"


class AndroidRegistrationStateInfo(StrictDataSchema[Literal["android_registration_state_info"]]):
    """Opaque optimistic revision without disclosing another account's binding."""

    type: Literal["android_registration_state_info"] = "android_registration_state_info"
    revision: str
    current_binding_id: UuidText | None


class AndroidRegistrationStateResponse(
    PayloadResponse[Literal["android_registration_state_response"], AndroidRegistrationStateInfo]
):
    """Read-only preparation response; does not allocate a challenge or mutate state."""

    type: Literal["android_registration_state_response"] = "android_registration_state_response"
