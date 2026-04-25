"""Tests for the push-beta rollout gate helpers (iOS-5 sub-scope b)."""

import json

from snapper.application.notify.push_beta import PUSH_BETA_SETTING_KEY
from snapper.application.notify.push_beta import PushBetaConfig
from snapper.application.notify.push_beta import parse_push_beta_config
from snapper.application.notify.push_beta import serialize_push_beta_config


class TestPushBetaConfigIncludes:
    """``PushBetaConfig.includes(user_public_id)`` truth table."""

    def test_disabled_gate_lets_everyone_through(self) -> None:
        """Disabled gate is the legacy default — every caller is admitted."""
        config = PushBetaConfig()
        assert config.includes("user-a") is True
        assert config.includes(None) is True

    def test_enabled_gate_admits_only_listed_users(self) -> None:
        """Enabled gate matches the allowlist by exact user_public_id."""
        config = PushBetaConfig(enabled=True, user_public_ids=("user-a", "user-b"))
        assert config.includes("user-a") is True
        assert config.includes("user-b") is True
        assert config.includes("user-c") is False

    def test_enabled_gate_denies_none_user(self) -> None:
        """Defensive: alerts without user scope must not slip through a beta gate."""
        config = PushBetaConfig(enabled=True, user_public_ids=("user-a",))
        assert config.includes(None) is False


class TestParsePushBetaConfig:
    """JSON parsing + fallback semantics for the cached setting value."""

    def test_none_returns_disabled_default(self) -> None:
        """Setting absent (cache miss) -> disabled default."""
        config = parse_push_beta_config(None)
        assert config.enabled is False
        assert config.user_public_ids == ()

    def test_empty_string_returns_disabled_default(self) -> None:
        """Empty value -> disabled default (no parse attempt)."""
        config = parse_push_beta_config("")
        assert config.enabled is False
        assert config.user_public_ids == ()

    def test_malformed_json_returns_disabled_default(self) -> None:
        """Mis-edit of the setting cannot silence the entire push system."""
        config = parse_push_beta_config("{not-json")
        assert config.enabled is False

    def test_non_object_json_returns_disabled_default(self) -> None:
        """JSON arrays and primitives at top level fall back to disabled."""
        config = parse_push_beta_config('["just-a-list"]')
        assert config.enabled is False

    def test_missing_enabled_key_returns_disabled_default(self) -> None:
        """Required ``enabled`` key absent -> disabled default."""
        config = parse_push_beta_config(json.dumps({"user_public_ids": ["u-1"]}))
        assert config.enabled is False

    def test_non_bool_enabled_returns_disabled_default(self) -> None:
        """``enabled`` typed as string truthy -> still disabled (strict)."""
        config = parse_push_beta_config(json.dumps({"enabled": "true", "user_public_ids": []}))
        assert config.enabled is False

    def test_non_list_user_ids_returns_disabled_default(self) -> None:
        """``user_public_ids`` typed as scalar -> disabled (strict)."""
        config = parse_push_beta_config(json.dumps({"enabled": True, "user_public_ids": "user-a"}))
        assert config.enabled is False

    def test_non_string_inside_user_ids_returns_disabled_default(self) -> None:
        """List elements typed as ints -> disabled (strict element type check)."""
        config = parse_push_beta_config(json.dumps({"enabled": True, "user_public_ids": [123]}))
        assert config.enabled is False

    def test_well_formed_json_round_trips(self) -> None:
        """Canonical JSON parses into the matching dataclass shape."""
        config = parse_push_beta_config(
            json.dumps({"enabled": True, "user_public_ids": ["user-a", "user-b"]})
        )
        assert config.enabled is True
        assert config.user_public_ids == ("user-a", "user-b")


class TestSerializePushBetaConfig:
    """Serialise -> parse round trip + canonicalisation."""

    def test_round_trip_preserves_intent(self) -> None:
        """Serialise then parse yields the original (enabled, ids) pair."""
        original = PushBetaConfig(enabled=True, user_public_ids=("user-a", "user-b"))
        decoded = parse_push_beta_config(serialize_push_beta_config(original))
        assert decoded.enabled is True
        assert decoded.user_public_ids == ("user-a", "user-b")

    def test_serialisation_sorts_and_dedups_user_ids(self) -> None:
        """Two semantically-equal configs serialise to byte-identical JSON.

        Prevents churn on the SCD2 history when the admin POSTs the
        same allowlist in a different order or with duplicates.
        """
        a = PushBetaConfig(enabled=True, user_public_ids=("user-b", "user-a", "user-a"))
        b = PushBetaConfig(enabled=True, user_public_ids=("user-a", "user-b"))
        assert serialize_push_beta_config(a) == serialize_push_beta_config(b)


class TestSettingKeyConstant:
    """Setting-key constant pinned to the wire contract."""

    def test_setting_key_pinned(self) -> None:
        """Pin the setting key so accidental rename is caught at test time."""
        assert PUSH_BETA_SETTING_KEY == "push_beta_config"
