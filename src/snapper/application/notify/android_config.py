"""Explicit public Android configuration with a separately admitted sender."""

from dataclasses import dataclass
from dataclasses import field

from snapper.api.schemas.android_devices import AndroidPushConfiguration
from snapper.application.notify.fcm_client import FcmClient


@dataclass(frozen=True, slots=True)
class AndroidPushRuntime:
    """Injected capability; defaults cannot advertise unsupported push delivery.

    Sender readiness is set by the server composition root only after it builds
    a sender for the exact configuration project. Public options alone never
    enable registration. Credential material is deliberately absent here.
    """

    configuration: AndroidPushConfiguration | None = None
    sender_configured: bool = False
    sender: FcmClient | None = field(default=None, repr=False)
    apns_enabled: bool = True

    @property
    def available_configuration(self) -> AndroidPushConfiguration | None:
        """Return public options only when a matching sender is configured.

        Returns:
            Public options when a matching sender is configured, otherwise None.
        """
        return self.configuration if self.sender_configured else None
