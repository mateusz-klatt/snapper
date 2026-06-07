"""APNs (Apple Push Notification service) configuration loader.

Reads the nine ``apns_*`` settings seeded via the proprietary TOML
profile (``proprietary/data/seed/{dev,prod}.toml``, category
``apns``) and hydrates them into an ``ApnsConfig`` dataclass the
``ApnsClientPool`` can consume.

The private key is stored as a base64-encoded PEM
string in ``apns_private_key_p8_base64`` so it round-trips cleanly
through the TOML seed. Decoding yields a PEM string that aioapns /
PyJWT accepts verbatim — no filesystem tempfile is required despite
the misleading ``key: str | None`` argument name in
``aioapns.APNs.__init__``.
"""

import base64
from dataclasses import dataclass
from typing import cast

from snapper.application.services.settings import SettingsService


@dataclass(frozen=True, slots=True)
class ApnsConfig:
    """Fully-hydrated APNs configuration for ``ApnsClientPool`` construction.

    Attributes:
        team_id: Apple Developer team ID.
        key_id: APNs auth key id.
        bundle_id: iOS app bundle identifier (reverse-DNS form).
        topic: APNs topic — normally identical to ``bundle_id`` for
            alert pushes.
        environment: ``sandbox``, ``production`` or
            ``sandbox_and_production`` — when the latter, the pool
            instantiates both clients and routes by the device's own
            ``env`` column.
        private_key_pem: Decoded PKCS#8 PEM string suitable for
            passing to ``aioapns.APNs(key=...)``. Already trimmed of
            whitespace; includes the ``BEGIN PRIVATE KEY``/``END
            PRIVATE KEY`` markers and the newline-separated base64
            payload.
    """

    team_id: str
    key_id: str
    bundle_id: str
    topic: str
    environment: str
    private_key_pem: str


_APNS_SETTINGS_KEYS: tuple[str, ...] = (
    "apns_team_id",
    "apns_key_id",
    "apns_bundle_id",
    "apns_topic",
    "apns_environment",
    "apns_private_key_p8_base64",
)


def load_apns_config(settings: SettingsService) -> ApnsConfig:
    """Load the APNs config from a ready ``SettingsService``.

    Reads the six required keys (four simple strings, one environment
    enum, one base64-encoded PEM) and returns a fully-hydrated
    ``ApnsConfig``. Missing keys raise ``ValueError`` with a dense
    message listing every missing key — if the sidecar can't
    authenticate, we fail loud at startup rather than producing
    cryptic APNs errors at first-send time.

    Args:
        settings: A ``SettingsService`` whose cache has already been
            hydrated via ``_load_all_settings`` (called automatically
            on service construction).

    Returns:
        ``ApnsConfig`` with the PEM string decoded from base64.

    Raises:
        ValueError: If any of the six required settings keys are
            missing, empty, or base64 decoding fails.
    """
    missing = [key for key in _APNS_SETTINGS_KEYS if not settings.get_setting(key)]
    if missing:
        raise ValueError(
            "APNs config missing required settings: "
            f"{', '.join(missing)}. Seed them via "
            "proprietary/data/seed/{dev,prod}.toml under category='apns'."
        )
    environment = cast(str, settings.get_setting("apns_environment"))
    if environment not in {"sandbox", "production", "sandbox_and_production"}:
        raise ValueError(
            f"apns_environment must be sandbox|production|"
            f"sandbox_and_production, got {environment!r}."
        )
    key_b64 = cast(str, settings.get_setting("apns_private_key_p8_base64"))
    try:
        pem = base64.b64decode(key_b64.encode("ascii")).decode("ascii")
    except ValueError as exc:
        raise ValueError(
            f"apns_private_key_p8_base64 failed to decode as base64-encoded PEM: {exc}"
        ) from exc
    if "BEGIN PRIVATE KEY" not in pem:
        raise ValueError(
            "Decoded APNs private key does not contain a PKCS#8 PEM "
            "BEGIN marker — verify the seed payload was base64-"
            "encoded from the raw .p8 file."
        )
    return ApnsConfig(
        team_id=cast(str, settings.get_setting("apns_team_id")),
        key_id=cast(str, settings.get_setting("apns_key_id")),
        bundle_id=cast(str, settings.get_setting("apns_bundle_id")),
        topic=cast(str, settings.get_setting("apns_topic")),
        environment=environment,
        private_key_pem=pem,
    )
