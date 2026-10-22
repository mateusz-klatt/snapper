"""Atomic Android registration bindings with immutable retirement history."""

import hashlib
from dataclasses import dataclass
from datetime import datetime
from typing import Literal

from snapper.api.schemas.android_devices import AndroidDeviceInfo
from snapper.data.models import NotificationDevice


class AndroidRegistrationError(Exception):
    """A stable, known pre-dispatch registration refusal."""


@dataclass(frozen=True, slots=True)
class RegistrationWrite:
    """Authenticated ownership and provenance captured at the route boundary."""

    owner: str
    session_id: str
    sequence_id: int
    timestamp: datetime


def registration_revision(row: NotificationDevice | None) -> str:
    """Expose an opaque revision stable across heartbeat and preference changes."""
    value = "absent" if row is None else f"{row.public_id}:{row.token_status}"
    return hashlib.sha256(value.encode()).hexdigest()


def _required(value: str | None) -> str:
    """Reject an incomplete provider identity rather than inventing one."""
    if value is None:
        raise ValueError("Android registration identity is incomplete")
    return value


def android_device_info(row: NotificationDevice) -> AndroidDeviceInfo:
    """Validate the Android projection without returning its FID."""
    return AndroidDeviceInfo(
        session_id=row.session_id,
        sequence_id=row.sequence_id,
        timestamp=row.timestamp,
        public_id=row.public_id,
        user_public_id=row.user_public_id,
        installation_id=_required(row.installation_id),
        binding_id=_required(row.binding_id),
        configuration_id=_required(row.configuration_id),
        package_name=_required(row.package_name),
        registered_at=row.registered_at,
        last_seen_at=row.last_seen_at,
    )


class AndroidConfigurationError(Exception):
    """A stale or non-immutable configuration cannot become the active sender."""


@dataclass(frozen=True, slots=True)
class AndroidConfigurationRevision:
    """Read-only operator state; disabled decisions retain an opaque revision."""

    revision: str | None
    active_configuration_id: str | None


@dataclass(frozen=True, slots=True)
class AndroidDeliveryResult:
    """A terminal provider outcome applies only to its charged outbox attempt."""

    status: Literal["sent", "failed", "unregistered", "cancelled_scope"]
    expected_attempt_count: int
    reason: str | None = None
    receipt: str | None = None
