"""Push-beta rollout gate helpers (iOS-5 sub-scope b).

When admins want to roll out APNs pushes to a subset of users
without ripping the rule registry apart, the push-beta gate
intercepts ``application/notify/routing.route_alert_to_devices``
before the per-device cascade runs:

- ``enabled = False`` (default) — every authenticated user gets
  pushes (legacy behaviour pre-iOS-5).
- ``enabled = True`` — only users whose ``user_public_id`` is in
  ``user_public_ids`` receive pushes; everyone else is silently
  dropped at the routing layer regardless of their per-device prefs.

Storage is one row in the ``settings`` table keyed
``push_beta_config`` carrying a JSON-encoded
``{"enabled": bool, "user_public_ids": [str, ...]}``. ``parse_push_beta_config``
decodes the cached string into the dataclass and tolerates absent
/ malformed values by falling back to the disabled default —
operators must NEVER find themselves without pushes because the
setting was mis-edited; the gate is opt-in by construction.
"""

import json
from dataclasses import dataclass
from dataclasses import field

from loguru import logger

PUSH_BETA_SETTING_KEY: str = "push_beta_config"
"""Key under which the push-beta config is stored in the
``settings`` table (and cached on ``SettingsService``)."""


PUSH_BETA_SETTING_CATEGORY: str = "notifications"
"""Category bucket — surfaces in the admin Settings UI alongside
the other notification-tier settings.
"""


@dataclass(frozen=True, slots=True)
class PushBetaConfig:
    """Decoded push-beta configuration."""

    enabled: bool = False
    user_public_ids: tuple[str, ...] = field(default_factory=tuple)

    def includes(self, user_public_id: str | None) -> bool:
        """Return True iff the user should receive pushes.

        Args:
            user_public_id: Caller's UUID7 to match against the
                allowlist. ``None`` is treated as "no scope" — denied
                when the gate is enabled, admitted when disabled.

        Returns:
            ``True`` when the gate is disabled (legacy / default
            open) or when the gate is enabled and the user is on the
            allowlist. ``False`` otherwise.
        """
        if not self.enabled:
            return True
        if user_public_id is None:
            return False
        return user_public_id in self.user_public_ids


_DISABLED_FALLBACK = PushBetaConfig()


def parse_push_beta_config(raw: str | None) -> PushBetaConfig:
    """Decode the ``push_beta_config`` JSON string from settings cache.

    Args:
        raw: Value retrieved via
            ``SettingsService.get_setting(PUSH_BETA_SETTING_KEY)``.
            ``None`` (setting absent) and any malformed JSON fall back
            to a disabled-default config so a misedit can never silence
            the entire push system.

    Returns:
        Frozen ``PushBetaConfig`` parsed from the JSON string, or the
        disabled-default fallback when the input is ``None``,
        non-string, non-JSON, missing keys, or ill-typed.
    """
    if not raw:
        return _DISABLED_FALLBACK
    try:
        decoded = json.loads(raw)
    except json.JSONDecodeError:
        logger.warning(
            "push_beta_config: malformed JSON in settings; falling back to disabled-default"
        )
        return _DISABLED_FALLBACK
    if not isinstance(decoded, dict):
        logger.warning(
            "push_beta_config: expected JSON object, got {kind}; falling back to disabled-default",
            kind=type(decoded).__name__,
        )
        return _DISABLED_FALLBACK
    enabled = decoded.get("enabled")
    user_public_ids = decoded.get("user_public_ids")
    if not isinstance(enabled, bool):
        logger.warning("push_beta_config: 'enabled' missing/non-bool; falling back")
        return _DISABLED_FALLBACK
    if not isinstance(user_public_ids, list) or not all(
        isinstance(item, str) for item in user_public_ids
    ):
        logger.warning("push_beta_config: 'user_public_ids' missing/non-string-list; falling back")
        return _DISABLED_FALLBACK
    return PushBetaConfig(enabled=enabled, user_public_ids=tuple(user_public_ids))


def serialize_push_beta_config(config: PushBetaConfig) -> str:
    """Encode a ``PushBetaConfig`` for storage in the settings table.

    Sorts + dedups ``user_public_ids`` so two semantically-equal
    configs produce byte-identical JSON — preventing churn on the
    SCD2 history when the admin POSTs the same set in a different
    order or with duplicates.

    Args:
        config: Configuration to encode.

    Returns:
        Compact JSON string with sorted top-level keys + sorted
        deduped allowlist, suitable for the ``settings.value``
        column.
    """
    return json.dumps(
        {
            "enabled": config.enabled,
            "user_public_ids": sorted(set(config.user_public_ids)),
        },
        separators=(",", ":"),
        sort_keys=True,
    )
