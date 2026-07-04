"""Tests for restart-policy resolution and config building."""

from unittest.mock import MagicMock

from snapper.application.process_manager.config_resolver import build_process_config_from_dict
from snapper.application.process_manager.config_resolver import resolve_restart_policy
from snapper.application.process_manager.models import ProcessRegistryEntry
from snapper.core.types import ProcessLifecycleEnum
from snapper.core.types import ProcessModeEnum
from snapper.core.types import ProcessRestartPolicyEnum
from snapper.core.types import ProcessRoleEnum


class TestResolveRestartPolicy:
    """Test suite for resolve_restart_policy."""

    def test_none_returns_on_failure(self) -> None:
        """None resolves to the ON_FAILURE default."""
        assert resolve_restart_policy(None, "proc") == ProcessRestartPolicyEnum.ON_FAILURE

    def test_enum_passthrough(self) -> None:
        """An enum value passes through unchanged."""
        assert (
            resolve_restart_policy(ProcessRestartPolicyEnum.ALWAYS, "proc")
            == ProcessRestartPolicyEnum.ALWAYS
        )

    def test_string_value_resolves(self) -> None:
        """A valid string resolves to the matching enum."""
        assert resolve_restart_policy("never", "proc") == ProcessRestartPolicyEnum.NEVER

    def test_invalid_value_warns_and_defaults(self) -> None:
        """An invalid value falls back to ON_FAILURE."""
        assert resolve_restart_policy("bogus", "proc") == ProcessRestartPolicyEnum.ON_FAILURE


class TestBuildProcessConfigRestartPolicy:
    """Test suite for restart_policy resolution in build_process_config_from_dict."""

    def test_restart_policy_from_config_dict(self) -> None:
        """A restart_policy present in the config dict is honored."""
        config = build_process_config_from_dict(
            "proc",
            {"class": "a.B", "restart_policy": "always"},
            None,
        )
        assert config.restart_policy == ProcessRestartPolicyEnum.ALWAYS

    def test_restart_policy_defaults_without_entry(self) -> None:
        """Restart policy defaults to ON_FAILURE when absent and no entry exists."""
        config = build_process_config_from_dict("proc", {"class": "a.B"}, None)
        assert config.restart_policy == ProcessRestartPolicyEnum.ON_FAILURE

    def test_restart_nonce_from_config_dict(self) -> None:
        """A restart_nonce present in the config dict is carried onto the model."""
        config = build_process_config_from_dict(
            "proc",
            {"class": "a.B", "restart_nonce": "op-restart-1"},
            None,
        )
        assert config.restart_nonce == "op-restart-1"

    def test_restart_nonce_defaults_to_none(self) -> None:
        """A config without restart_nonce yields None (never restarted via the control plane)."""
        config = build_process_config_from_dict("proc", {"class": "a.B"}, None)
        assert config.restart_nonce is None

    def test_restart_policy_inherited_from_entry(self) -> None:
        """When the config omits restart_policy, it is inherited from the registry entry."""
        entry = ProcessRegistryEntry(
            class_ref=MagicMock(),
            class_path="a.B",
            method="start",
            description="",
            priority=0,
            lifecycle=ProcessLifecycleEnum.LONG_RUNNING,
            role=ProcessRoleEnum.CORE,
            tags=(),
            parameters_model=None,
            parameters_schema=None,
            enabled=False,
            mode=ProcessModeEnum.THREAD,
            restart_policy=ProcessRestartPolicyEnum.ALWAYS,
        )
        config = build_process_config_from_dict("proc", {"class": "a.B"}, entry)
        assert config.restart_policy == ProcessRestartPolicyEnum.ALWAYS
